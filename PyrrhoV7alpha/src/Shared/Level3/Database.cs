using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Threading;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level3
{
    /// <summary>
    /// To facilitate shareability of level 3 objects we define this base class
    /// to hold multiple properties of the database objects, using a fixed static
    /// set of negative longs as system keys. Full lists of these keys are in
    /// the SourceIntro document: the actual longs used will be different 
    /// each time the server is built.
    /// User keys can also be used, and these will be positive longs.
    /// Almost all perties of database objects are handled this way. The exceptions are
    /// records,roles,loadpos (for Database)
    /// defpos (for DBObject)
    /// kind (for Domain)
    /// </summary>
    internal abstract class Basis
    {
        internal static long _uid = -500;
        internal const long Name = -50; // string
        static long _dbg = 0;
        internal long dbg = ++_dbg;
        // negative keys are for system data, positive for user-defined data
        public readonly BTree<long, object> mem;
        public virtual long lexeroffset => 0;
        protected Basis (BTree<long,object> m)
        {
            mem = m;
        }
        internal Basis(params (long, object)[] m) : this(_Mem(m)) { }

        static BTree<long,object> _Mem((long,object)[] m)
        {
            var mm = BTree<long, object>.Empty;
            foreach (var b in m)
                mm += b;
            return mm;
        }
        internal abstract Basis New(BTree<long, object> m);
        public static Basis operator+(Basis b,(long,object)x)
        {
            return b.New(b.mem + x);
        }
        internal virtual Basis Relocate(Writer wr)
        {
            return this;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            if (mem.Contains(Name)) { sb.Append(" Name="); sb.Append((string)mem[Name]); }
            return sb.ToString();
        }
    }
    public enum ExecuteStatus { Parse, Obey, Drop,Rename }

    /// <summary>
    /// Counter-intuitively, a logical database (embodied by the durable contents of the transaction log)
    /// is represented in the engine by a great many immutable instances of this class as physical records
    /// are processed by the engine. New instances are not created by copying! Instead, 
    /// every modification at database or transaction level creates a new instance.
    /// The first instance of the database is created when the server first reads physical data from the 
    /// transaction log, and is initialised to contain the system objects (database _system). 
    /// The entire contents of the transaction log file are then processed so that at the conclusion of
    /// Load() there is an instance that represents the committed state of the database.
    /// When a new connection thread is started it is given the latest committed version of the database.
    /// 
    /// Prior to version 7, every Pyrrho database began with user and role records. From version 7 on,
    /// this is not required: the database user for a new empty database is the account running the engine,
    /// and this account is initially allowed to use the the schema role (with key 0). 
    /// 
    /// Creation of a new database by a connected user other than the server account can be permitted
    /// using command-line flags (it is not the default), and in that case the creator user is 
    /// written at the start of the database and becomes the owner. Otherwise the server account must
    /// create the new database and grant ownership to a user. (Owner is a user, all other privileges
    /// are for roles.)
    /// 
    /// Data manipulation is always carried out within transactions, and user-defined datatypes
    /// can obviously play their part there. 
    /// 
    /// From version 7, the latest version of every record is held in memory.
    /// </summary>
    internal class Database : Basis
    {
        static long _did = 0;
        internal long did = ++_did;
        protected static BTree<string, FileStream> dbfiles = BTree<string, FileStream>.Empty;
        protected static BTree<string, Database> databases = BTree<string, Database>.Empty;
        internal static Database _system = null;
        internal readonly long loadpos;
        public override long lexeroffset => loadpos;
        internal const long
            Cascade = -390, // bool (only used for Transaction subclass)
            Curated = -53, // long
            _ExecuteStatus = -54, // ExecuteStatus
            Guest = -55, // Role
            Levels = -56, // BTree<Level,long>
            LevelUids = -57, // BTree<long,Level>
            NextTid = -58, // long:  will be used for next transaction
            Owner = -59, // long
            Roles = -60, // BTree<string,long>
            Types = -61;  // BTree<Domain,Domain>
        internal virtual long uid => -1;
        public string name => (string)(mem[Name] ?? "");
        internal FileStream df => dbfiles[name];
        internal long curated => (long)(mem[Curated]??-1L);
        internal long owner => (long)(mem[Owner]??-1L);
        internal virtual long nextTid => Transaction.TransPos;
        internal BTree<string, long> roles =>
            (BTree<string, long>)mem[Roles] ?? BTree<string, long>.Empty;
        internal Role schemaRole => (mem[DBObject.Definer] is Role r)?r
            :(Role)mem[(long)mem[DBObject.Definer]];
        internal Role guestRole => (Role)mem[Guest];
        internal virtual Role role => schemaRole;
        internal virtual User user => (User)mem[owner];
        internal virtual bool autoCommit => true;
        internal virtual string source => "";
        internal bool cascade => (bool)(mem[Cascade] ?? false);
        /// <summary>
        /// The type system needs to be able to consider ad-hoc data types, i.e. not reified in
        /// any physical database. When data with any data type is committed to a database this must be
        /// prepared by reifying the data type in the database. All data types decalred in
        /// table definitions etc are automatically reified, and the resulting type from an SQL computation
        /// is refified if necessary to the subtype of the value.
        /// The compare function for Domain does not look at defpos: this means that the types
        /// tree allows us to find the detabase's version of a Domain with given properties
        /// </summary>
        public BTree<Domain, Domain> types => (BTree<Domain, Domain>)mem[Types];// key==value for all entries
        public BTree<Level, long> levels => (BTree<Level, long>)mem[Levels];
        public BTree<long, Level> cache => (BTree<long, Level>)mem[LevelUids];
        public ExecuteStatus parse => (ExecuteStatus)(mem[_ExecuteStatus]??ExecuteStatus.Obey);
        public BTree<long, object> objects => mem;
        static Database()
        {
            var su = new User(-500, new BTree<long, object>(Name,
                    System.Security.Principal.WindowsIdentity.GetCurrent().Name));
            var sr = new Role("$Schema", DBObject.Definer, BTree<long, object>.Empty +
                    (su.defpos, su) +
                    (Owner, su.defpos));
            var gu = new Role("$Guest", Guest, BTree<long, object>.Empty);
            _system = new Database("System", su, sr, gu);
            Domain.StandardTypes();
            SystemRowSet.Kludge();
            Domain.RdfTypes();
        }
        Database(string n,User su,Role sr,Role gu) 
            : base((Levels,BTree<Level,long>.Empty),(LevelUids,BTree<long,Level>.Empty),
                  (Name,n),(Owner,su.defpos),(sr.defpos,sr),(su.defpos,su),
                  (Guest,gu),(Roles,BTree<string,long>.Empty+(sr.name,sr.defpos)+(gu.name,gu.defpos)),
                  (Types,BTree<Domain,Domain>.Empty))
        {
            loadpos = 0;
        }
        public Database(string n,FileStream f):base(_system.mem+(Name,n))
        {
            dbfiles += (n, f);
            loadpos = 5;
        }
        protected Database(long c, BTree<long,object> m):base(m)
        {
            loadpos = c;
        }
        public virtual Database New(long c, BTree<long,object> m)
        {
            return new Database(c, m);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Database(loadpos, m);
        }
        public static Database operator +(Database d, (long, object) x)
        {
            return d.New(d.loadpos, d.mem + x);
        }
        public static Database operator -(Database d,long x)
        {
            return d.New(d.loadpos, d.mem - x);
        }
        /// <summary>
        /// Default action for adding a DBObject (goes into mem)
        /// If it has a Domain, this is the definer's type for the object
        /// </summary>
        /// <param name="d"></param>
        /// <param name="x"></param>
        /// <returns></returns>
        public static Database operator +(Database d, (DBObject, long) x)
        {
            var (ob,curpos) = x;
            return d.New(curpos,d.mem+(ob.defpos,ob));
        }
        public static Database operator +(Database d, (ObInfo, long) x)
        {
            var (ob, curpos) = x;
            var ro = d.role;
            return d.New(curpos, d.mem + (ro.defpos,ro+ob));
        }
        public static Database operator +(Database d, (Level, long) x)
        {
            return d.New(d.loadpos, 
                d.mem+(Levels,d.levels + (x.Item1, x.Item2))+
                (LevelUids,d.cache + (x.Item2, x.Item1)));
        }
        public static Database operator +(Database d,(Role,long)x)
        {
            var (ro, curpos) = x;
            return d+ (Roles, d.roles + (ro.name, ro.defpos))+(ro.defpos,ro);
        }
        public static Database Get(string fn)
        {
            var f = dbfiles[fn];
            if (f == null)
                    return null;
            for (; ; )
            {
                var r = databases[fn];
                if (r != null)
                    return r;
                // otherwise the database is loading
                Thread.Sleep(1000);
            }
        }
        internal FileStream File()
        {
            return dbfiles[name];
        }
        public virtual Transaction Transact(long t,string sce,bool? auto=null)
        {
            // if not new, this database may be out of date: ensure we get the latest
            var r = databases[name] ?? this;
            return new Transaction(r,t,sce,auto??autoCommit);
        }
        public DBObject GetObject(string n)
        {
            return objects[role.dbobjects[n]] as DBObject;
        }
        public Procedure GetProcedure(string n,int a)
        {
            return (role.procedures[n] is BTree<int,long> pt &&
                pt.Contains(a))? objects[pt[a]] as Procedure:null;
        }
        /// <summary>
        /// Build a ReadConstraint object.
        /// </summary>
        /// <param name="q">The database object concerned</param>
        /// <returns></returns>
        internal virtual ReadConstraint _ReadConstraint(Context cx,DBObject d)
        {
            return null;
        }
        public virtual (Database,long) RdrClose(Context cx)
        {
            return (this,Transaction.TransPos);
        }
        /// <summary>
        /// Load the database
        /// </summary>
        public virtual Database Load()
        {
            var rdr = new Reader(this);
            rdr.context = new Context(this);
            Physical p;
            lock (df) //(consistency)
            {
                var db = rdr.db;
                for (int counter = 0; ; counter++)
                {
                    p = rdr.Create();
                    if (p == null)
                        break;
                    try
                    {
                        if (p is PTransaction pt)
                        {
                            rdr.trans = pt;
                            rdr.role = (Role)rdr.db.mem[pt.ptrole];
                            rdr.user = (User)rdr.db.mem[pt.ptuser];
                        } 
                        else
                            (rdr.db,rdr.role) = p.Install(rdr.db, rdr.role, rdr.Position);
                    }
                    catch (Exception) { }
                }
            }
            var d = rdr.db;
            databases += (name, d);
            rdr.db = d;
            return d;
        }
        /// <summary>
        /// Accessor: determine if there is anything to commit
        /// </summary>
        public virtual bool WorkToCommit { get { return false; } }
        /// <summary>
        /// Accessor: get the start of the work to be committed
        /// </summary>
        public virtual long WorkPos { get { return long.MaxValue; } }
        public virtual void Audit(Audit a) { }
        /// <summary>
        /// Accessor: a level2 record by position
        /// </summary>
        /// <param name="pos">a given position</param>
        /// <returns>the physical record</returns>
        public Physical GetD(long pos)
        {
            return new Reader(this,pos).Create();
        }
        public Physical Get(ref long pos)
        {
            var rdr = new Reader(this, pos);
            var r = rdr.Create();
            pos = rdr.Position;
            return r;
        }
        internal virtual void Execute(Role r, string id,string[] path, int p, string etag)
        { }
        internal virtual void Post(RowSet r, string s)
        { }
        internal virtual void Put(RowSet r, string s)
        { }
        internal virtual void Delete(RowSet r)
        { }
         /// <summary>
        /// Prepare for a CONTINUE condition handler (implemented in Participant)
        /// </summary>
        /// <returns>The updated Participant</returns>
        internal virtual Transaction Mark()
        {
            return null;
        }
        /// <summary>
        /// Commit the physical data
        /// </summary>
        internal virtual (Database,long) Commit(Context cx)
        {
            return (this,Transaction.TransPos);
        }

        internal (Database,long) Install()
        {
            var db = new Database(df.Length, mem);
            databases += (name, db);
            return (db,Transaction.TransPos);
        }
        internal virtual (Database,long) Rollback(object e)
        {
            return (this,Transaction.TransPos);
        }
        public virtual DBException Exception(string sig, params object[] obs)
        {
            return new DBException(sig, obs);
        }
    }
 }

