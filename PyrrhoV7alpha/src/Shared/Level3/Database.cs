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
        // negative keys are for system data, positive for user-defined data
        public readonly BTree<long, object> mem;
        public string name => (string)(mem[Name]??"");
        public virtual long lexeroffset => 0;
        protected Basis (BTree<long,object> m) { mem = m; }
        internal Basis (params (long,object)[] m)
        {
            var mm = BTree<long, object>.Empty;
            foreach (var b in m)
                mm += b;
            mem = mm;
        }
        internal abstract Basis New(BTree<long, object> m);
        public static Basis operator+(Basis b,(long,object)x)
        {
            return b.New(b.mem + x);
        }
        public override string ToString()
        {
            return GetType().Name + " Name=" + name;
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
    /// Database objects are almost all accessed from the Role. There are always at least two
    /// of these called schemaRole and guestRole. During connection, a transaction is created
    /// by a user with a given role: this is maintained by the transaction context stack.
    /// 
    /// Prior to version 7, every Pyrrho database began with user and role records. From version 7
    /// on this, is not required: the database user for a new empty database is the account running the engine,
    /// and this account is initially allowed to use the the schema role (with key Role.Schema). 
    /// Creation of a new database by a connected user other than the server account can be permitted
    /// using command-line flags (it is not the default), and in that case the creator user is 
    /// written at the start of the database and becomes the owner. Otherwise the server account must
    /// create the new database and grant ownership to a user.
    /// 
    /// The schemaRole is different in several ways from user-defined roles. The system types belong
    /// to the schemaRole, and any object will therefore be accessible from the schemaRole. 
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
        internal readonly BTree<long, Role> roles;
        internal readonly long loadpos;
        public override long lexeroffset => loadpos;
        internal const long
            Curated = -54, // long
            NextTid = -55, // long:  will be used for next transaction
            _ExecuteStatus = -56, // ExecuteStatus
            Guest = -57, // Rolw
            Levels = -58, // BTree<Level,long>
            LevelUids = -59, // BTree<long,Level>
            Owner = -60, // long
            Schema = -61, // long (used always to be 5)
            Types = -62;  // BTree<Domain,Domain>
        internal virtual long uid => -1;
        internal FileStream df => dbfiles[name];
        internal long curated => (long)(mem[Curated]??-1L);
        internal long owner => (long)(mem[Owner]??-1L);
        internal virtual long nextTid => Transaction.TransPos;
        internal long schema => (long)mem[Schema];
        internal Role schemaRole => roles[schema];
        internal Role guestRole => roles[Guest];
        internal virtual Role role => schemaRole;
        internal virtual User user => roles[owner] as User;
        internal virtual bool autoCommit => true;
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
        static Database()
        {
            var su = new User(-63, new BTree<long, object>(Name,
                    System.Security.Principal.WindowsIdentity.GetCurrent().Name));
            var sr = new Role("$Schema", Schema, BTree<long, object>.Empty +
                    (su.defpos, su) +
                    (Owner, su.defpos));
            _system = new Database("System", su, sr);
            Domain.StandardTypes();
            SystemRowSet.Kludge();
            Domain.RdfTypes();
        }
        Database(string n,User su,Role sr) 
            : base((Levels,BTree<Level,long>.Empty),(LevelUids,BTree<long,Level>.Empty),
                  (Name,n),(Owner,su.defpos),(Schema,sr.defpos),
                  (Types,BTree<Domain,Domain>.Empty))
        {
            loadpos = 0;
            roles = BTree<long, Role>.Empty + (Schema, sr)+(su.defpos,su)+
                (Guest, new Role("$Guest", Guest, BTree<long, object>.Empty));
        }
        public Database(string n,FileStream f):base(_system.mem+(Name,n))
        {
            dbfiles += (n, f);
            loadpos = 5;
            roles = _system.roles;
        }
        protected Database(BTree<long, Role> rs, long c, BTree<long,object> m):base(m)
        {
            loadpos = c;
            roles = rs;
        }
        public virtual Database New(BTree<long, Role> rs, long c, BTree<long,object> m)
        {
            return new Database(rs, c, m);
        }
        /// <summary>
        /// A DBObject added to Database goes into mem by default
        /// </summary>
        /// <param name="ob"></param>
        /// <param name="rs"></param>
        /// <param name="c"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        public virtual Database New(DBObject ob,long p)
        {
            return new Database(roles + (role.defpos, role + ob),p, mem);
        }
        public static Database operator+(Database d,(Role,long)x)
        {
            return d.New(d.roles + (x.Item1.defpos, x.Item1), x.Item2, d.mem);
        }
        /// <summary>
        /// Add an object to a role
        /// </summary>
        /// <param name="d"></param>
        /// <param name="x"></param>
        /// <returns></returns>
        public static Database operator +(Database d, (Role,DBObject,long) x)
        {
            return d.New(d.roles + (x.Item1.defpos, x.Item1 + x.Item2), x.Item3, d.mem);
        }
        /// <summary>
        /// If x.Item1 <0, store a property object.
        /// If x.Item1 >=0, store the affected defpos.
        /// </summary>
        /// <param name="d"></param>
        /// <param name="x"></param>
        /// <returns></returns>
        public static Database operator+(Database d,(long,object)x)
        {
            return d.New(d.roles, d.loadpos, d.mem + x);
        }
        /// <summary>
        /// Default action for adding a DBObject
        /// </summary>
        /// <param name="d"></param>
        /// <param name="x"></param>
        /// <returns></returns>
        public static Database operator+(Database d,(DBObject,long) x)
        {
            return d.New(x.Item1,x.Item2);
        }
        public static Database operator- (Database d,Role r)
        {
            return d.New(d.roles - r.defpos, d.loadpos, d.mem-r.defpos);
        }
        /// <summary>
        /// Drop an object from a role
        /// </summary>
        /// <param name="d"></param>
        /// <param name="x"></param>
        /// <returns></returns>
        public static Database operator -(Database d, (Role,DBObject,long) x)
        {
            return d.New(d.roles + (x.Item1.defpos, x.Item1-x.Item2), x.Item3, d.mem);
        }
        public static Database operator-(Database d,DBObject ob)
        {
            return d.New(d.roles + (d.role.defpos, d.role - ob), d.loadpos, d.mem);
        }
        public static Database operator +(Database d, (Level, long) x)
        {
            return d.New(d.roles, d.loadpos, 
                d.mem+(Levels,d.levels + (x.Item1, x.Item2))+
                (LevelUids,d.cache + (x.Item2, x.Item1)));
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
        public virtual Transaction Transact(long t,bool? auto=null)
        {
            // if not new, this database may be out of date: ensure we get the latest
            var r = databases[name] ?? this;
            return new Transaction(r,t,auto??autoCommit);
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
        /// <summary>
        /// Mutator: Perform actions for Drop of a database object
        /// Side effects depend on action and CheckDependent
        /// </summary>
        /// <param name="ob">The database object to drop</param>
        /// <param name="act">CASCADE or RESTRICT</param>
        internal virtual void OnDrop(DBObject ob, Sqlx act)
        {
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
                            rdr.trans = pt;
                        else if (p is TriggeredAction ta)
                            rdr._role = rdr.db.roles[((Trigger)rdr._role.objects[ta.trigger]).definer];
                        else
                            rdr.db = p.Install(rdr.db, rdr.role, rdr.Position);
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
        public Domain GetDomain(long pos)
        {
            var ob = (DBObject)role.objects[pos];
            if (ob is Domain dm)
                return dm;
            else if (ob is Index ix)
                return ix.keyType;
            throw new PEException("PE284");
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
            var db = new Database(roles, df.Length, mem);
            databases += (name, db);
            return (db,Transaction.TransPos);
        }
        internal virtual (Database,long) Rollback(object e)
        {
            return (this,Transaction.TransPos);
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new Database(roles, loadpos, m);
        }
        public virtual DBException Exception(string sig, params object[] obs)
        {
            return new DBException(sig, obs);
        }
    }
 }

