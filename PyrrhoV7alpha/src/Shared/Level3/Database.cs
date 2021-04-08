using System;
using System.Text;
using System.IO;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Threading;
using System.Security.Principal;
using System.Security.AccessControl;
using System.Runtime.CompilerServices;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
    /// <summary>
    /// To facilitate shareability of level 3 objects we define this base class
    /// to hold multiple properties of the database objects, using a fixed static
    /// set of negative longs as system keys. Full lists of these keys are in
    /// the SourceIntro document: the actual longs used will be different 
    /// (in principle, each time the server is built).
    /// User keys can also be used, and these will be positive longs.
    /// Almost all properties of database objects are handled this way. The exceptions are
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
        /// <summary>
        /// Deep Fix of uids following Commit or View.Instance
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal virtual Basis Fix(Context cx)
        {
            return this;
        }
        /// <summary>
        /// Relocation for Commit
        /// </summary>
        /// <param name="wr"></param>
        /// <returns></returns>
        internal virtual Basis _Relocate(Writer wr)
        {
            return this;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            if (mem.Contains(Name)) { sb.Append(" Name="); sb.Append((string)mem[Name]); }
            return sb.ToString();
        }
        // Tailor REST string for differnt remote DBMS
        internal virtual string ToString(string sg, CTree<string,CTree<long, long>> cs, Context cx)
        {
            return ToString();
        }
    }
    // for view instancing and RowSet Review: Ok = ob key, RK rs key, OV ob value, RV rs value
    [Flags] internal enum VIC { None = 0, OK = 1, RK = 2, OV = 4, RV = 8 }
    public enum ExecuteStatus { Parse, Obey, Prepare }

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
        internal readonly long did = ++_did;
        protected static BTree<string, FileStream> dbfiles = BTree<string, FileStream>.Empty;
        internal static BTree<string, Database> databases = BTree<string, Database>.Empty;
        /// <summary>
        /// The _system database contains primitive domains and system tables and columns.
        /// These objects are inherited by any new database, and the _system._role uid
        /// becomes the schema role uid (obviously evolves to have all of the database objects).
        /// If there no users or roles defined in a database, the _system role uid is used
        /// </summary>
        internal static Database _system = null;
        internal readonly long loadpos;
        public override long lexeroffset => loadpos;
        internal const long
            Cascade = -227, // bool (only used for Transaction subclass)
            ColTracker = -318, // BTree<long,BTree<long,long>> colpos, ppos, dompos
            _Connection = -261, // BTree<string,string>: the session details
            Curated = -53, // long
            Format = -54,  // int (50 for Pyrrho v5,v6; 51 for Pyrrho v7)
            Guest = -55, // long: a role holding all grants to PUBLIC
            Public = -311, // long: always -1L, a dummy user ID
            Levels = -56, // BTree<Level,long>
            LevelUids = -57, // BTree<long,Level>
            Log = -188,     // BTree<long,Physical.Type>
            NextStmt = -393, // long: uncommitted compiled statements
            NextPrep = -394, // long: highwatermark of prepared statements for this connection
            NextPos = -395, // long: next proposed Physical record
            NextId = -58, // long:  will be used for next transaction
            Owner = -59, // long: the defpos of the owner user for the database
            Role = -285, // Role: the current role (e.g. an executable's definer)
            _Role = -302, // long: role.defpos, initially set to the session role
            Roles = -60, // BTree<string,long>
            _Schema = -291, // long: (always the same as _system._role) the owner role for the database
            SchemaKey = -286, // long: highwatermark for schema changes
            Types = -61, // CTree<Domain,long>
            TypeTracker = -315, // BTree<long,BTree<long,Domain>> colpos,modpos
            User = -277, // User: always the connection user
            _User = -301; // long: user.defpos, always the connection user, maybe uncommitted
        internal virtual long uid => -1;
        public string name => (string)(mem[Name] ?? "");
        internal FileStream df => dbfiles[name];
        internal long curated => (long)(mem[Curated]??-1L);
        internal long nextPrep => (long)(mem[NextPrep] ?? PyrrhoServer.Preparing);
        internal long nextStmt => (long)(mem[NextStmt] ?? 
            throw new PEException("PE777"));
        internal virtual long nextPos => Transaction.TransPos;
        internal long nextId => (long)(mem[NextId] ?? Transaction.Analysing);
        internal BTree<string, long> roles =>
            (BTree<string, long>)mem[Roles] ?? BTree<string, long>.Empty;
        // NB The following 8 entries have default values supplied by _system
        internal Role schema => (Role)mem[(long)mem[_Schema]];
        internal Role guest => (Role)mem[Guest];
        internal long _role => (long)mem[_Role];
        internal long owner => (long)mem[Owner];
        internal long _user => (long)mem[_User];
        internal Role role => (Role)objects[_role];
        internal User user => (User)objects[_user]; 
        internal virtual bool autoCommit => true;
        internal virtual string source => "";
        internal bool cascade => (bool)(mem[Cascade] ?? false);
        internal int format => (int)(mem[Format] ?? 0);
        internal long schemaKey => (long)(mem[SchemaKey] ?? 0L);
        public BTree<Domain, long> types => (BTree<Domain, long>)mem[Types];
        public BTree<Level, long> levels => (BTree<Level, long>)mem[Levels];
        public BTree<long, Level> cache => (BTree<long, Level>)mem[LevelUids];
        public BTree<long, Physical.Type> log =>
            (BTree<long, Physical.Type>)mem[Log] ?? BTree<long, Physical.Type>.Empty;
        public BTree<long, object> objects => mem;
        public BTree<string, string> conn => (BTree<string,string>)mem[_Connection];
        public BTree<long, BTree<long, Domain>> typeTracker =>
            (BTree<long, BTree<long, Domain>>)mem[TypeTracker] ?? BTree<long, BTree<long, Domain>>.Empty;
        public BTree<long, BTree<long, long>> colTracker =>
            (BTree<long, BTree<long, long>>)mem[ColTracker] ?? BTree<long, BTree<long, long>>.Empty;
        /// <summary>
        /// This code sets up the _system Database.
        /// It contains two roles ($Schema and _public), 
        /// the predefined types and system tables.
        /// </summary>
        static Database()
        {
            var su = new User(--_uid, new BTree<long, object>(Name,
                    WindowsIdentity.GetCurrent().Name));
            var sr = new Role("$Schema",--_uid,BTree<long, object>.Empty +
                    (_User, su.defpos) +  (Owner, su.defpos));
            var gu = new Role("GUEST", Guest, BTree<long, object>.Empty);
            _system = new Database("System", su, sr, gu)+(_Schema,sr.defpos);
            SystemRowSet.Kludge(); 
            Domain.RdfTypes();
            Context._system = new Context(_system);
        }
        /// <summary>
        /// The creates the _system database
        /// </summary>
        /// <param name="n"></param>
        /// <param name="su"></param>
        /// <param name="sr"></param>
        /// <param name="gu"></param>
        Database(string n,User su,Role sr,Role gu) 
            : base((Levels,BTree<Level,long>.Empty),(LevelUids,BTree<long,Level>.Empty),
                  (Name,n),(sr.defpos,sr),(su.defpos,su),(gu.defpos,gu),
                  // the 7 entries without defaults start here
                  (_Role,sr.defpos),(Role, sr),(DBObject.Definer,sr.defpos),
                  (_User,su.defpos),(User,su),(Owner,su.defpos),
                  (Guest,gu),(gu.defpos,gu),
                  (Types,BTree<Domain,long>.Empty),
                  (Roles,BTree<string,long>.Empty+(sr.name,sr.defpos)+(gu.name,gu.defpos)),                
                  (NextStmt,Transaction.Executables))
        {
            loadpos = 0;
        }
        /// <summary>
        /// Each named Database starts off with the _system definitions
        /// </summary>
        /// <param name="n"></param>
        /// <param name="f"></param>
        public Database(string n,FileStream f):base(_system.mem+(Name,n)
            +(Format,_Format(f)))
        {
            dbfiles += (n, f);
            loadpos = 5;
        }
        /// <summary>
        /// After that all changes to a named database are made using the
        /// operator+ methods defined below
        /// </summary>
        /// <param name="c">The current load position</param>
        /// <param name="m">All the other properties</param>
        internal Database(long c, BTree<long,object> m):base(m)
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
        public static Database operator +(Database d, (Level, long) x)
        {
            return d.New(d.loadpos, 
                d.mem+(Levels,d.levels + (x.Item1, x.Item2))+
                (LevelUids,d.cache + (x.Item2, x.Item1)));
        }
        public static Database operator +(Database d,(Role,long) x)
        {
            var (ro, p) = x;
            return d+(ro.defpos,ro)+(SchemaKey,p);
        }
        public static Database operator+(Database d,(long,Domain,long)x)
        {
            var (dp, dm, curpos) = x;
            return d.New(curpos, d.mem + (dp, dm));
        }
        public static Database Get(BTree<string,string> cs)
        {
            var fn = cs["Files"];
            var f = dbfiles[fn];
            if (f == null)
                try
                {
                    var fp = PyrrhoStart.path + fn;
                    if (!File.Exists(fp))
                        return null;
                    var db = new Database(fn, new FileStream(fp,
                        FileMode.Open, FileAccess.ReadWrite, FileShare.None));
                    db.Load();
                    f = dbfiles[fn];
                }
                catch (Exception) { }
            if (f == null)
                    return null;
            for (; ; )
            {
                var r = databases[fn];
                if (r != null) // add the connectionString for the session
                    return r + (_Connection, cs);
                // otherwise the database is loading
                Thread.Sleep(1000);
            }
        }
        internal virtual void Add(Context cx,Physical ph,long lp)
        {
            ph.Install(cx, lp);
        }
        internal FileStream _File()
        {
            return dbfiles[name];
        }
        internal static FileStream _File(string n)
        {
            return dbfiles[n];
        }
        static int _Format(FileStream f)
        {
            var bs = new byte[5];
            return (f.Read(bs, 0, 5)==5)?bs[4]:0;
        }
        /// <summary>
        /// Start a new Transtion if necessary (Transaction override does very little)
        /// </summary>
        /// <param name="t">usually nextId except for Prepared statements</param>
        /// <param name="sce"></param>
        /// <param name="auto"></param>
        /// <returns></returns>
        public virtual Transaction Transact(long t, string sce, bool? auto = null)
        {
            // if not new, this database may be out of date: ensure we get the latest
            // and add the connection for the session
            var r = databases[name] + (_Connection, conn);
            if (r == null || r.loadpos < loadpos)
                r = this; // this is more recent!
            // ensure a valid user and role combination
            // 1. Default:
            Role ro = guest;
            var user = conn["User"];
            User u = objects[roles[user]] as User;
            if (u == null)// 2. if the user is unknown
            {
                // Has the schema role any users?
                var users = false;
                for (var b=log.PositionAt(0L);(!users) && b!=null; b=b.Next())
                    if (b.value() == Physical.Type.PUser)
                    {
                        var up = b.key();
                        if (schema.infos[up] is ObInfo si
                            && si.priv.HasFlag(Grant.Privilege.UseRole))
                            users = true;
                    }
                if (users) // 2a make an uncommitted user
                    u = new User(user); // added to the new Transaction below
                else {  // 2b 
                    if (user == WindowsIdentity.GetCurrent().Name) //2bi
                    {
                        u = (User)objects[_system._user]
                            ?? throw new PEException("PE855");
                        ro = schema // allow the server account use the schema role
                            ?? throw new PEException("PE856");
                    }
                    else // 2bii deny access
                        throw new DBException("42105");
                }
            }
            if (conn["Role"] is string rn) // 3. has a specific role been requested?
            {
                ro = (Role)objects[roles[rn]]
                    ?? throw new DBException("42105"); // 3a
                if (role.infos[guest.defpos] is ObInfo oi
                    && oi.priv.HasFlag(Grant.Privilege.UseRole)) // 3b public role
                    goto done;
                if (owner == u.defpos && ro == schema) // 3ci1
                    goto done;
                throw new DBException("42105");
            }
            // 4. No specific role requested
            if (u.defpos == owner)
                ro = schema; // 4ai
            else if (u.defpos >= 0 && u.defpos < Transaction.TransPos)
            {
                // 4aii See if the use can access just one role 
                for (var b = roles.First(); b != null; b = b.Next())
                {
                    var br = (Role)objects[b.value()];
                    if (br.infos[u.defpos] is ObInfo bi
                        && bi.priv.HasFlag(Grant.Privilege.UseRole))
                    {
                        if (ro == guest)
                            ro = br;   // we found one
                        else
                        {
                            ro = guest; // we found another, so leave it as guest
                            break;
                        }
                    }
                }
            }
            else
                ro = guest;
            done:
            var tr = new Transaction(r, t, sce, auto ?? autoCommit) + (NextPrep, nextPrep);
            if (u.defpos==-1L) // make a PUser for ad-hoc User, in case of Audit or Grant
            { 
                var cx = new Context(tr);
                var pu = new PUser(user, tr.nextPos, cx);
                u = new User(pu, this);
                tr.Add(cx,pu,loadpos);
                tr = (Transaction)cx.db;
            }
            tr = tr + (_User, u.defpos) + (_Role, ro.defpos);
            return tr;
        }
        public DBObject GetObject(string n)
        {
            return objects[role.dbobjects[n]] as DBObject;
        }
        public ObInfo GetObInfo(string n)
        {
            return role.infos[role.dbobjects[n]] as ObInfo;
        }
        public Procedure GetProcedure(string n,int a)
        {
            return (role.procedures[n] is BTree<int,long> pt &&
                pt.Contains(a))? objects[pt[a]] as Procedure:null;
        }
        public Domain Find(Domain dm)
        {
            if (!types.Contains(dm))
                throw new PEException("PE555");
            return (Domain)objects[types[dm]];
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
        public virtual Database RdrClose(Context cx)
        {
            return cx.db - NextId;
        }
        /// <summary>
        /// Load the database
        /// </summary>
        public virtual Database Load()
        {
            var rdr = new Reader(new Context(this));
            Physical p;
            lock (df) //(consistency)
            {
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
                            rdr.context.db += (Role, pt.ptrole);
                            rdr.context.db += (User, pt.ptuser);
                            // these two fields for reading of old objects from the log
                            // not used (at all) during Load(): so set them to the above
                            rdr.role = rdr.context.role;
                            rdr.user = rdr.context.user;
                            rdr.trans = pt;
                        }
                        rdr.Add(p);
                        rdr.role = rdr.context.db.role;
                    }
                    catch (Exception) { }
                    rdr.context.db += (Log, rdr.context.db.log + (p.ppos, p.type));
                }
            }
            var d = rdr.context.db;
            databases += (name, d);
            rdr.context.db = d;
            return d;
        }
        internal (Physical, long) _NextPhysical(long pp,PTransaction trans=null)
        {
            try
            {
                var rdr = new ReaderBase(this, pp);
                var ph = rdr.Create();
                pp = (int)rdr.Position;
                if (ph == null)
                    return (null, -1);
                return (ph, pp);
            } catch(Exception)
            {
                throw new DBException("22003");
            }
        }
        internal Physical GetPhysical(long pp)
        {
            try
            {
                var ph = new ReaderBase(this, pp).Create();
                return ph;
            }
            catch (Exception)
            {
                throw new DBException("22003");
            }
        }
        public virtual void Audit(Audit a,Context cx) { }
        internal virtual int AffCount(Context cx)
        {
            var aff = cx.affected;
            if (aff == null)
                return 0;
            var r = 0L;
            for (var b = aff.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Table)
                    r += b.value().Count;
            return (int)r;
        }
        /// <summary>
        /// Accessor: a level2 record by position
        /// </summary>
        /// <param name="pos">a given position</param>
        /// <returns>the physical record</returns>
        public Physical GetD(long pos)
        {
            return new Reader(new Context(this),pos).Create();
        }
        public Physical Get(ref long pos)
        {
            var rdr = new Reader(new Context(this), pos);
            var r = rdr.Create();
            pos = rdr.Position;
            return r;
        }
        internal virtual void Execute(Role r, string id,string[] path, int p, string etag)
        { }
        internal virtual Context Post(Context cx, RowSet  r, string s)
        { return cx;  }
        internal virtual Context Put(Context cx, RowSet r, string s)
        { return cx;  }
        internal virtual Context Delete(Context cx, RowSet r)
        { return cx; }
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
        internal virtual Database Commit(Context cx)
        {
            return this - NextId;
        }

        internal Database Install(long pos)
        {
            var db = new Database(pos, mem);
            databases += (name, db);
            return db;
        }
        internal virtual Database Rollback(object e)
        {
            return this - NextId;
        }
        public virtual DBException Exception(string sig, params object[] obs)
        {
            return new DBException(sig, obs);
        }
    }
 }

