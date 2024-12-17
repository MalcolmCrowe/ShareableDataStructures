using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Common;
using Pyrrho.Level5;
using System.Xml;
using System.Security.AccessControl;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

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
    internal abstract class Basis(BTree<long,object> m)
    {
        public BTree<long, object> mem => m;   // negative keys are for system obs, positive for user-defined obs
        internal static long _uid = -500;
        static long _dbg = 0;
        internal readonly long dbg = ++_dbg;
        public virtual long lexeroffset => 0;
        internal Basis(params (long, object)[] m) : this(_Mem(m)) { }
        static BTree<long,object> _Mem((long,object)[] m)
        {
            var mm = BTree<long, object>.Empty;
            foreach (var b in m)
                mm += b;
            return mm;
        }
        internal abstract Basis New(BTree<long, object> m);
        internal virtual Basis Fix(Context cx)
        {
            return New(_Fix(cx,mem));
        }
        internal virtual Basis Apply(Context cx,BTree<long,object> m)
        {
            return New(_Apply(cx,m,mem));
        }
        /// <summary>
        /// Deep Fix of uids following Commit or View.Instance
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        protected virtual BTree<long,object> _Fix(Context cx,BTree<long,object> m)
        {
            return m;
        }
        /// <summary>
        /// Context.MergeColumn uses a different cascade, can't use depths.
        /// </summary>
        /// <param name="dn">DBObjects we have done</param>
        /// <param name="was">An old TableColumn uid</param>
        /// <param name="now">A new one</param>
        /// <returns></returns>
        internal virtual Basis ShallowReplace(Context cx,long was,long now)
        {
            return this;
        }
        protected virtual BTree<long,object> _Apply(Context cx,BTree<long,object>am,BTree<long,object>m)
        {
            return m;
        }
        public override string ToString()
        {
            return GetType().Name;
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
        internal virtual string ToString(string sg, Remotes rf, BList<long?> cs, 
            CTree<long,string> ns, Context cx)
        {
            return ToString();
        }
    }
    [Flags]
    public enum ExecuteStatus 
    { Parse=1, Obey=2, Graph=4, GraphType=8, Prepare=16, Compile=32, Commit=64, Detach=128 }

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
    /// and this account is initially allowed to use the the rowType role. 
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
        internal static object _lock = new();
        internal static Database Empty = new();
        /// <summary>
        /// The _system database contains primitive domains and system tables and columns.
        /// These objects are inherited by any new database, and the _system._role uid
        /// becomes the rowType role uid (obviously evolves to have all of the database objects).
        /// If there no users or roles defined in a database, the _system role uid is used
        /// </summary>
        internal static Database _system = Empty;
        internal readonly long length;
        public override long lexeroffset => length;
        internal const long
            Arriving = -469, // CTree<long,CTree<long,bool>> TNode,TEdge 7.03
            Catalog = -247, // BTree<string,long?> DBObject
            Curated = -53, // long
            Format = -54,  // int (50 for Pyrrho v5,v6; 51 for Pyrrho v7)
            Guest = -55, // long: a role holding all grants to PUBLIC
            JoinedNodes = -430,// CTree<long,CTree<Domain,bool>> JoinedNodeType
            KeyLabels = -461, // BTree<CTree<string,bool>,long?> NodeType 
            LastModified = -279, // DateTime
            Leaving = -466, // CTree<long,CTree<long,bool>> TNode,TEdge 7.03
            Levels = -56, // BTree<Level,long?>
            LevelUids = -57, // BTree<long,Level>
            Log = -188,     // BTree<long,Physical.Type>
            NextId = -58, // long:  will be used for next transaction
            NextPos = -395, // long: next proposed Physical record
            NextStmt = -393, // long: next space in compiled range
            Owner = -59, // long: the defpos of the owner user for the database
            Prefixes = -375, // BTree<string,long?> UDT
            Procedures = -95, // CTree<long,string> Procedure
            Public = -311, // long: always -1L, a dummy user Id
            Role = -285, // Role: the current role (e.g. an executable's definer)
            Roles = -60, // BTree<string,long?>
            _Schema = -291, // long: the owner role for the database
            Suffixes = -376, // BTree<string,long?> UDT
            Types = -61, // BTree<Domain,long?> should only be used for system types and unnamed types
            UnlabelledNodeTypes = -237, // BTree<CTree<long,bool>,long?> Unlabelled NodeType and EdgeType by propertyset
            User = -277, // User: always the connection user
            Users = -287; // BTree<string,long?> users defined in the database
        internal virtual long uid => -1;
        public string name => (string)(mem[ObInfo.Name]??throw new PEException("PE1001"));
        internal FileStream df => dbfiles[name]??throw new PEException("PE1002");
        internal BTree<string, long?> catalog =>
            (BTree<string, long?>)(mem[Catalog] ?? new BTree<string, long?>("/",0L));
        internal long curated => (long)(mem[Curated]??-1L);
        internal long nextStmt => (long)(mem[NextStmt] ?? 
            throw new PEException("PE777"));
        internal virtual long nextPos => Transaction.TransPos;
        internal long nextId => (long)(mem[NextId] ?? Transaction.Analysing);
        internal BTree<string, long?> roles =>
            (BTree<string, long?>?)mem[Roles] ?? BTree<string, long?>.Empty;
        public BTree<string, long?> users =>
            (BTree<string, long?>?)mem[Users] ?? BTree<string, long?>.Empty;
        // NB The following 8 entries have default values supplied by _system
        internal Role schema => (Role)(mem[(long)(mem[_Schema]??-1L)] ?? throw new PEException("PE1003"));
        internal Role guest => (Role)(mem[Guest]??throw new PEException("PE1003"));
        internal long owner => (long)(mem[Owner] ?? throw new PEException("PE1005"));
        internal Role role => (Role)(mem[Role] ?? guest);
        internal User? user => (User?)mem[User];
        internal virtual bool autoCommit => true;
        internal virtual string source => "";
        internal int format => (int)(mem[Format] ?? 0);
        internal BTree<Domain, long?> types => (BTree<Domain, long?>?)mem[Types]??BTree<Domain,long?>.Empty;
        public BTree<Level, long?> levels => (BTree<Level, long?>?)mem[Levels]??BTree<Level,long?>.Empty;
        public BTree<long, Level> cache => (BTree<long, Level>?)mem[LevelUids]??BTree<long,Level>.Empty;
        public DateTime? lastModified => (DateTime?)mem[LastModified];
        public BTree<long, Physical.Type> log =>
            (BTree<long, Physical.Type>?)mem[Log] ?? BTree<long, Physical.Type>.Empty;
        public BTree<long, object> objects => mem;
        public CTree<long, string> procedures =>
            (CTree<long, string>?)mem[Procedures] ?? CTree<long, string>.Empty;
        internal BTree<CTree<long, bool>, long?> unlabelledNodeTypes =>
    (BTree<CTree<long, bool>, long?>)(mem[UnlabelledNodeTypes] ?? BTree<CTree<long, bool>, long?>.Empty);
        public BTree<string, long?> prefixes =>
            (BTree<string, long?>?)mem[Prefixes] ?? BTree<string, long?>.Empty;
        public BTree<string, long?> suffixes =>
            (BTree<string, long?>?)mem[Suffixes] ?? BTree<string, long?>.Empty;
        internal CTree<long,CTree<Domain,bool>> joinedNodes =>
            (CTree<long, CTree<Domain, bool>>)(mem[JoinedNodes] ?? CTree<long, CTree<Domain, bool>>.Empty);  
        internal static Role schemaRole;
        internal static Role guestRole;
        /// <summary>
        /// This code sets up the _system Database.
        /// It contains two roles ($RowType and _public), 
        /// the predefined types and system tables.
        /// </summary>
        static Database()
        {
            var su = new User(--_uid, new BTree<long, object>(ObInfo.Name,
                    Environment.UserDomainName + "\\" + Environment.UserName));
            schemaRole = new Role("$Schema",--_uid,BTree<long, object>.Empty +
                    (User, su) +  (Owner, su.defpos));
            guestRole = new Role("GUEST", Guest, BTree<long, object>.Empty);
            _system = new Database("System", su, schemaRole, guestRole)+(_Schema,schemaRole.defpos);
            Domain.Kludge();
            SystemRowSet.Kludge();
        }
        Database():base(BTree<long,object>.Empty) { }
        /// <summary>
        /// The creates the _system database
        /// </summary>
        /// <param name="n"></param>
        /// <param name="su"></param>
        /// <param name="sr"></param>
        /// <param name="gu"></param>
        Database(string n,User su,Role sr,Role gu) 
            : base((Levels,BTree<Level,long?>.Empty),(LevelUids,BTree<long,Level>.Empty),
                  (ObInfo.Name,n),(sr.defpos,sr),(su.defpos,su),(gu.defpos,gu),
                  // the 7 entries without defaults start here
                  (Role, sr),(DBObject.Definer,sr.defpos),
                  (User,su),(Owner,su.defpos),
                  (Guest,gu),(gu.defpos,gu),
                  (Types,BTree<Domain,long?>.Empty),
                  (Roles,BTree<string,long?>.Empty+(sr.name??"",sr.defpos)+(gu.name??"",gu.defpos)),                
                  (NextStmt,Transaction.Executables))
        {
            length = 0;
        }
        /// <summary>
        /// Each named Database starts off with the _system definitions
        /// </summary>
        /// <param name="n"></param>
        /// <param name="f"></param>
        public Database(string n,string path,FileStream f)
            :base((_system??throw new PEException("PE1006")).mem+(ObInfo.Name,n)
            +(Format,_Format(f))+(LastModified,File.GetLastWriteTimeUtc(path))
            +(NextStmt,Transaction.Executables))
        {
            dbfiles += (n, f);
            length = 5;
        }
        internal Database(Database d,long len) :base (d.mem)
        {
            length = len;
        }
        protected Database(Database d,BTree<long,object> m) :base(m)
        {
            length = d.length;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Database(this,m);
        }
        public static Database operator +(Database d, (long, object) x)
        {
            var (dp, ob) = x;
            if (d.mem[dp] == ob)
                return d;
            return (Database)d.New(d.mem + x);
        }
        public static Database operator -(Database d,long x)
        {
            return (Database)d.New(d.mem - x);
        }
        /// <summary>
        /// Default action for adding a DBObject (goes into mem)
        /// If it has a Domain, this is the definer's type for the object
        /// </summary>
        /// <param name="d"></param>
        /// <param name="x"></param>
        /// <returns></returns>
        public static Database operator +(Database d, DBObject x)
        {
            var m = d.mem;
            if (d.mem[x.defpos] == x)
                return d;
            m += (x.defpos, x);
            return (Database)d.New(m);
        }
        public static Database operator +(Database d, (Level,long) x)
        {
            return (Database)d.New(d.mem+(Levels,d.levels + x)+
                (LevelUids,d.cache + (x.Item2, x.Item1)));
        }
        public static Database operator +(Database d,Role x)
        {
            return (Database)d.New(d.mem+(x.defpos,x)+(Role,x));
        }
        public static Database operator+(Database d,Domain dm)
        {
            var m = d.mem;
            if (dm.name.Length == 0 || dm.defpos == -1L)
            {
                var ts = d.types + (dm, dm.defpos);
                m += (Types, ts);
            }
            if (dm.defpos!=-1L)
                m += (dm.defpos, dm);
            return (Database)d.New(m);
        }
        internal virtual DBObject? Add(Context cx,Physical ph)
        {
            return ph.Install(cx);
        }
        internal FileStream _File()
        {
            return dbfiles[name]??throw new PEException("PE1007");
        }
        internal static FileStream _File(string n)
        {
            return dbfiles[n]??throw new PEException("PE1007");
        }
        static int _Format(FileStream f)
        {
            var bs = new byte[5];
            return (f.Read(bs, 0, 5)==5)?bs[4]:0;
        }
        public static Database? Get(BTree<string, string> cs)
        {
            var fn = cs["Files"]??throw new DBException("80000");
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
        public virtual Transaction Transact(long t, Connection con,bool? auto = null)
        {
            // if not new, this database may be out of date: ensure we get the latest
            // and add the connection for the session
            var r = databases[name];
            if (r == null || r.length < length)
                r = this; // this is more recent!
            // ensure a valid user and role combination
            // 1. Default:
            Role ro = guest;
            var user = con.props["User"] ?? throw new DBException("80000");
            if (objects[roles[user] ?? -1L] is not User u)// 2. if the user is unknown
            {
                // Has the rowType role any users?
                var users = r.mem.Contains(Users);
                if (users) // 2a make an uncommitted user
                    u = new User(user); // added to the new Transaction below
                else
                {  // 2b 
                    if (user == Environment.UserDomainName + "\\" + Environment.UserName) //2bi
                    {
                        var sysdb = _system ?? throw new PEException("PE1009");
                        u = sysdb.user ?? throw new PEException("PE855");
                        ro = schema // allow the server account use the rowType role
                            ?? throw new PEException("PE856");
                    }
                    else // 2bii deny access
                        throw new DBException("42105");
                }
            }
            if (con.props["Role"] is string rn) // 3. has a specific role been requested?
            {
                ro = (Role)(objects[roles[rn]??-1L]
                    ?? throw new DBException("42105").Add(Qlx.ROLE)); // 3a
                if (u is not null && ro.infos[u.defpos] is ObInfo ou
                        && ou.priv.HasFlag(Grant.Privilege.UseRole)) // user has usage
                    goto done;
                if (ro.infos[guest.defpos] is ObInfo oi
                    && oi.priv.HasFlag(Grant.Privilege.UseRole)) // 3b public role
                    goto done;
                if (u is not null && owner == u.defpos && ro == schema) // 3ci1
                    goto done;
                throw new DBException("42105").Add(Qlx.USER);
            }
            // 4. No specific role requested
            if (u.defpos == owner)
                ro = schema; // 4ai
            else if (u.defpos >= 0 && u.defpos < Transaction.TransPos)
            {
                // 4aii See if the use can access just one role 
                for (var b = roles.First(); b != null; b = b.Next())
                    if (objects[b.value()??-1L] is Role br)
                    {
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
            if (u == null)
                throw new DBException("42105").Add(Qlx.USER);
            var tr = new Transaction(r, t, auto ?? autoCommit);
            if (u.defpos==-1L) // make a PUser for ad-hoc User, in case of Audit or Grant
            { 
                var cx = new Context(tr);
                var pu = new PUser(user, tr.nextPos, cx);
                u = new User(pu, this);
                tr.Add(cx,pu);
                tr = (Transaction)(cx.db ?? throw new PEException("PE1012"));
            }
            tr = tr + (User, u) + (Role, ro)
                      + (LastModified, DateTime.UtcNow); // transaction start time
            return tr;
        }
        public DBObject? GetObject(string n,Role r)
        {
            return r.dbobjects.Contains(n)? objects[r.dbobjects[n]??-1L] as DBObject : null;
        }
        internal CList<Domain> Signature(Procedure proc)
        {
            var r = CList<Domain>.Empty;
            for (var b = proc.ins.First(); b != null; b = b.Next())
                if (b.value() is long p && proc.framing.obs[p] is DBObject ob)
                    r += ob.domain;
            return r;
        }
        internal CList<Domain> Signature(Context cx,BList<long?> ins)
        {
            var r = CList<Domain>.Empty;
            for (var b = ins.First(); b != null; b = b.Next())
                if (b.value() is long p && cx._Ob(p) is DBObject ob)
                    r += ob.domain;
            return r;
        }
        internal static CList<Domain> Signature(Domain ins)
        {
            var r = CList<Domain>.Empty;
            for (var b = ins.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    r += ins.representation[p] ?? throw new PEException("PE0098");
            return r;
        }
        public Procedure? GetProcedure(string n,CList<Domain> a)
        {
            return (role.procedures[n] is BTree<CList<Domain>, long?> pt &&
                pt.Contains(a)) ? objects[pt[a] ?? -1L] as Procedure
                : null;
        }
        public Domain? Find(Domain dm)
        {
            if ((dm.defpos != -1L && dm.defpos<Transaction.Analysing) || (dm.name?.Length > 0 && !char.IsLetter(dm.name[0])))
                return dm;
            return objects[types[dm] ?? -1L] as Domain;
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
            var kludge = Domain.Content;
            var rdr = new Reader(new Context(this));
            Physical? p;
            lock (df) //(consistency)
            {
                for (int counter = 0; ; counter++)
                {
                    rdr.context.obs = ObTree.Empty;
                    rdr.context.depths = BTree<int, ObTree>.Empty;
                    rdr.context.defs = BTree<long,Names>.Empty;
                    rdr.context.names = Names.Empty;
                    p = rdr.Create();
                    if (p is EndOfFile)
                        break;
                    try
                    {
                        if (p is PTransaction pt && rdr.context is Context cx && cx.db is Database dr)
                        {
                            // these two fields for reading of old objects from the log
                            // not used (at all) during Load()
                            rdr.role = pt.ptrole;
                            rdr.user = pt.ptuser; 
                            rdr.trans = pt;
                            dr += (Role, rdr.role);
                            dr += (User,rdr.user??Level3.User.None);
                        }
                        rdr.Add(p);
                    }
                    catch (Exception) { }
                   // if (rdr.context?.db.mem[Log] is BTree<long,Physical.Type> log)
                        rdr.context.db += (Log, rdr.context.db.log + (p.ppos, p.type));
                }
                rdr.context.db = new Database(rdr.context.db,df.Length);
            }
            var d = rdr.context?.db ?? throw new PEException("PE1013");
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
            Physical? p;
            lock (df) //(consistency)
            {
                for (int counter = 0; ; counter++)
                {
                    p = rdr.Create();
                    if (p == null || p is EndOfFile)
                        break;
                    if (rdr.context.db is Database dr)
                        rdr.context.db += (Log, rdr.context.db.log + (p.ppos, p.type));
                }
            }
            return rdr.context.db ?? throw new PEException("PE014");
        }
        internal (Physical?, long) _NextPhysical(long pp)
        {
            try
            {
                var rdr = new Reader(this, pp);
                var ph = rdr.Create();
                pp = (int)rdr.Position;
                if (ph is EndOfFile)
                    return (null, -1L);
                return (ph, pp);
            } catch(Exception)
            {
                throw new DBException("22003");
            }
        }
        internal (Physical,long) GetPhysical(long pp)
        {
            try
            {
                var rdr = new Reader(this, pp);
                var ph = rdr.Create();
                var ppos = rdr.Position;
                return (ph,ppos);
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
                if (cx.db.objects[b.key()] is Table && b.value() is BTree<long,long?> tt)
                    r += tt.Count;
            return (int)r;
        }
        internal static void UpdateWith(Database d)
        {
            lock (_lock)
                databases += (d.name, d);
        }
        /// <summary>
        /// Accessor: a level2 record by position
        /// </summary>
        /// <param name="pos">a given position</param>
        /// <returns>the physical record</returns>
        public Physical? GetD(long pos)
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
            return (databases[name] ?? throw new PEException("PE1010"))
                + (LastModified, DateTime.UtcNow);
        }
        internal Database Rollback()
        {
            return databases[name] ?? throw new PEException("PE1011");
        }
        public virtual DBException Exception(string sig, params object[] obs)
        {
            return new DBException(sig, obs);
        }
    }
 }

