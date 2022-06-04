using System;
using System.Text;
using System.IO;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Threading;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
        internal readonly long dbg = ++_dbg;
        // negative keys are for system obs, positive for user-defined obs
        internal readonly BTree<long, object> mem;
        public string name => (string)mem[Name]??"";
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
#if ASSERTRANGE
        /// <summary>
        /// Alarm bells should ring if any committed DBObject in
        /// db.objects has any property with a non-shareable uid. 
        /// If db is a Transaction, this means #,%,@ uids
        /// If db is a Database, it means !,#,%,@ uids.
        /// These methods don't know if db is a Transaction,
        /// (and if it is, e.g. a table has uncommitted records etc)
        /// but we can test a good many uids anyway.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="m"></param>
        internal void AssertRange<K,V>(ATree<K,V> m) 
        {
            for (var b=m.First();b!=null;b=b.Next())
            {
                if (b.key() is long p)
                    AssertRange(p);
                var o = b.value();
                if (o is long p0)
                    AssertRange(p0);
                if (o is Basis bo)
                    AssertRange(bo.mem);
            }
        }
        internal static void AssertRange(long p)
        {
            if (p > Transaction.Analysing)
                throw new PEException("PE101");
        }
#endif
        internal virtual Basis Fix(Context cx)
        {
            return _Fix(cx);
        }
        /// <summary>
        /// Deep Fix of uids following Commit or View.Instance
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal virtual Basis _Fix(Context cx)
        {
            return this;
        }
        /// <summary>
        /// Relocation for Commit
        /// </summary>
        /// <param name="wr"></param>
        /// <returns></returns>
        internal virtual Basis _Relocate(Context cx)
        {
            return this;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            if (mem.Contains(Name)) { sb.Append(" Name="); sb.Append(name); }
            return sb.ToString();
        }
        [Flags]
        internal enum Remotes { None=0, Selects=1, Operands=2 }
        /// <summary>
        /// Tailor REST string for differnt remote DBMS
        /// </summary>
        /// <param name="sg">SqlAgent</param>
        /// <param name="rf">role in remote query</param>
        /// <param name="cs">remote columns</param>
        /// <param name="ns">remote names</param>
        /// <param name="cx">Context</param>
        /// <returns></returns>
        internal virtual string ToString(string sg, Remotes rf, CList<long> cs, 
            CTree<long,string> ns, Context cx)
        {
            return ToString();
        }
    }
    public enum ExecuteStatus { Parse, Obey, Prepare, Compile, SubView }

    /// <summary>
    /// Counter-intuitively, a logical database (embodied by the durable contents of the transaction log)
    /// is represented in the engine by a great many immutable instances of this class as physical records
    /// are processed by the engine. New instances are not created by copying! Instead, 
    /// every modification at database or transaction level creates a new instance.
    /// The first instance of the database is created when the server first reads physical obs from the 
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
    /// shareable as of 26 April 2021
    /// </summary>
    internal class Database : Basis
    {
        static long _did = 0;
        internal readonly long did = ++_did;
        protected static BTree<string, FileStream> dbfiles = BTree<string, FileStream>.Empty;
        internal static BTree<string, Database> databases = BTree<string, Database>.Empty;
        internal static object _lock = new object();
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
            Curated = -53, // long
            Format = -54,  // int (50 for Pyrrho v5,v6; 51 for Pyrrho v7)
            Guest = -55, // long: a role holding all grants to PUBLIC
            Public = -311, // long: always -1L, a dummy user ID
            LastModified = -279, // DateTime
            Levels = -56, // BTree<Level,long>
            LevelUids = -57, // BTree<long,Level>
            Log = -188,     // BTree<long,Physical.Type>
            NextId = -58, // long:  will be used for next transaction
            NextPos = -395, // long: next proposed Physical record
            NextStmt = -393, // long: next space in compiled range
            Owner = -59, // long: the defpos of the owner user for the database
            Procedures = -95, // CTree<long,string> Procedure
            Role = -285, // Role: the current role (e.g. an executable's definer)
            _Role = -302, // long: role.defpos, initially set to the session role
            Roles = -60, // BTree<string,long>
            _Schema = -291, // long: (always the same as _system._role) the owner role for the database
            SchemaKey = -286, // long: highwatermark for schema changes
            Types = -61, // CTree<Domain,long>
            User = -277, // User: always the connection user
            _User = -301,// long: user.defpos, always the connection user, maybe uncommitted
            Users = -287; // BTree<string,long> users defined in the database
        internal virtual long uid => -1;
        internal FileStream df => dbfiles[name];
        internal long curated => (long)(mem[Curated]??-1L);
        internal long nextStmt => (long)(mem[NextStmt] ?? 
            throw new PEException("PE777"));
        internal virtual long nextPos => Transaction.TransPos;
        internal long nextId => (long)(mem[NextId] ?? Transaction.Analysing);
        internal BTree<string, long> roles =>
            (BTree<string, long>)mem[Roles] ?? BTree<string, long>.Empty;
        public BTree<string, long> users =>
            (BTree<string, long>)mem[Users] ?? BTree<string, long>.Empty;
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
        internal int format => (int)(mem[Format] ?? 0);
        internal long schemaKey => (long)(mem[SchemaKey] ?? 0L);
        public BTree<Domain, long> types => (BTree<Domain, long>)mem[Types];
        public BTree<Level, long> levels => (BTree<Level, long>)mem[Levels];
        public BTree<long, Level> cache => (BTree<long, Level>)mem[LevelUids];
        public DateTime lastModified => (DateTime)mem[LastModified];
        public BTree<long, Physical.Type> log =>
            (BTree<long, Physical.Type>)mem[Log] ?? BTree<long, Physical.Type>.Empty;
        public BTree<long, object> objects => mem;
        public CTree<long, string> procedures =>
            (CTree<long, string>)mem[Procedures] ?? CTree<long, string>.Empty;
        /// <summary>
        /// This code sets up the _system Database.
        /// It contains two roles ($Schema and _public), 
        /// the predefined types and system tables.
        /// </summary>
        static Database()
        {
            var su = new User(--_uid, new BTree<long, object>(Name,
                    Environment.UserDomainName + "\\" + Environment.UserName));
            var sr = new Role("$Schema",--_uid,BTree<long, object>.Empty +
                    (_User, su.defpos) +  (Owner, su.defpos));
            var gu = new Role("GUEST", Guest, BTree<long, object>.Empty);
            _system = new Database("System", su, sr, gu)+(_Schema,sr.defpos);
            Context._system = new Context(_system);
            Domain.StandardTypes();
            SystemRowSet.Kludge();
        }
        internal static void Kludge() { }
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
        public Database(string n,string path,FileStream f):base(_system.mem+(Name,n)
            +(Format,_Format(f))+(LastModified,File.GetLastWriteTimeUtc(path))
            +(NextStmt,Transaction.Executables))
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
            return d.New(p,d.mem+(ro.defpos,ro)+(SchemaKey,p));
        }
        public static Database operator+(Database d,(long,Domain,long)x)
        {
            var (dp, dm, curpos) = x;
            return d.New(curpos, d.mem + (dp, dm));
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
        public static Database Get(BTree<string, string> cs)
        {
            var fn = cs["Files"];
            var f = dbfiles[fn];
            if (f == null)
                try
                {
                    var fp = PyrrhoStart.path + fn;
                    if (!File.Exists(fp))
                    {
                        Console.WriteLine("Database not found " + fp);
                        return null;
                    }
                    var db = new Database(fn, fp, new FileStream(fp,
                        FileMode.Open, FileAccess.ReadWrite, FileShare.None));
                    if (PyrrhoStart.VerboseMode)
                        Console.WriteLine("Database file found: " + fp);
                    db.Load();
                    f = dbfiles[fn];
                }
                catch (Exception) { }
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
        /// <summary>
        /// Start a new Transtion if necessary (Transaction override does very little)
        /// </summary>
        /// <param name="t">usually nextId except for Prepared statements</param>
        /// <param name="sce"></param>
        /// <param name="auto"></param>
        /// <returns></returns>
        public virtual Transaction Transact(long t, string sce,Connection con,bool? auto = null)
        {
            // if not new, this database may be out of date: ensure we get the latest
            // and add the connection for the session
            var r = databases[name];
            if (r == null || r.loadpos < loadpos)
                r = this; // this is more recent!
            // ensure a valid user and role combination
            // 1. Default:
            Role ro = guest;
            var user = con.props["User"];
            User u = objects[roles[user]] as User;
            if (u == null)// 2. if the user is unknown
            {
                // Has the schema role any users?
                var users = r.mem.Contains(Users);
                if (users) // 2a make an uncommitted user
                    u = new User(user); // added to the new Transaction below
                else {  // 2b 
                    if (user == Environment.UserDomainName+"\\"+Environment.UserName) //2bi
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
            if (con.props["Role"] is string rn) // 3. has a specific role been requested?
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
            var tr = new Transaction(r, t, sce, auto ?? autoCommit)
                + (Transaction.StartTime,DateTime.Now);
            if (u.defpos==-1L) // make a PUser for ad-hoc User, in case of Audit or Grant
            { 
                var cx = new Context(tr);
                var pu = new PUser(user, tr.nextPos, cx);
                u = new User(pu, this);
                tr.Add(cx,pu,loadpos);
                tr = (Transaction)cx.db;
            }
            tr = tr + (_User, u.defpos) + (_Role, ro.defpos)
                      + (LastModified, DateTime.UtcNow); // transaction start time
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
        public virtual Database RdrClose(ref Context cx)
        {
            return this;
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
                    if (rdr.context.db.mem.Contains(Log))
                        rdr.context.db += (Log, rdr.context.db.log + (p.ppos, p.type));
                }
            }
            var d = rdr.context.db + (NextStmt,rdr.context.nextStmt);
            if (PyrrhoStart.VerboseMode)
                Console.WriteLine("Database " + name + " loaded to " + rdr.Position);
            lock (_lock)
                databases += (name, d);
            return d;
        }
        internal Database BuildLog()
        {
            if (mem.Contains(Log))
                return this;
            var rdr = new Reader(new Context(this),5L);
            Physical p;
            lock (df) //(consistency)
            {
                for (int counter = 0; ; counter++)
                {
                    p = rdr.Create();
                    if (p == null)
                        break;
                    rdr.context.db += (Log, rdr.context.db.log + (p.ppos, p.type));
                }
            }
            return rdr.context.db;
        }
        internal (Physical, long) _NextPhysical(long pp,PTransaction trans=null)
        {
            try
            {
                var rdr = new Reader(this, pp);
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
                var ph = new Reader(this, pp).Create();
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
            var aff = cx?.affected;
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
        internal virtual Context Post(Context cx, TableRowSet  r, string s)
        { return cx;  }
        internal virtual Context Put(Context cx, TableRowSet r, string s)
        { return cx;  }
        internal virtual Context Delete(Context cx, TableRowSet r)
        { return cx; }
        /// <summary>
        /// Commit the physical obs
        /// </summary>
        internal virtual Database Commit(Context cx)
        {
            return databases[name] 
                + (LastModified, DateTime.UtcNow);
        }
        internal Database Rollback()
        {
            return databases[name];
        }
        public virtual DBException Exception(string sig, params object[] obs)
        {
            return new DBException(sig, obs);
        }
    }
 }

