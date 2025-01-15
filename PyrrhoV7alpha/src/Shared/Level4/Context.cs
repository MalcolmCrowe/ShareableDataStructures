using System.Net;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level1;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
    /// <summary>
    /// A Context contains the main obs structures for analysing the meaning of SQL. 
    /// Each Context maintains an indexed set of its enclosed Contexts (named blocks, tables, routine instances etc).
    /// RowSet and Activation are the families of subclasses of Context. 
    /// Contexts and Activations form a stack in the normal programming implementation sense for execution,
    /// linked downwards by staticLink and dynLink. There is a pushdown Variables structure, so that 
    /// this[Ident] gives access to the TypedValue for that Ident at the top level. 
    /// (The SQL programming language does not need access to deeper levels of the activation stack.)
    /// A Context or Activation can have a pointer to the start of the currently executing query (called query).
    /// During parsing Queries are constructed: the query currently being worked on is called cur in the staticLink Context,
    /// parsing proceeds left to right, with nested RowSet instances backward loinked using the enc field. 
    /// 
    /// All Context information is volatile and scoped to within the current transaction,for which it may contain a snapshot.
    /// Ideally Context should be immutable (a subclass of Basis), but we do not do this because QlValue::Eval can affect
    /// the current ReadConstraint and ETag, and these changes must be placed somewhere. The Context is the simplest.
    /// 
    /// So Context is mutable: but places we have the separate Contexts are important. 
    ///     1. The first step in Commit is to create Reader and a Writer based on the Database and not the Transaction
    ///     Then wr.cx.db is extended by the committed objects from the Transaction (see Physical.Commit)
    ///     2. During Parsing, the parsing context contains the evolving Transaction (i.e. cx.db is tr)
    ///     3. During Execution we can have temporary Contexts/Activations for doing aggregations, triggers,
    ///     recursion and exception handling
    /// </summary>
    internal class Context
    {
        static long _cxid = 0L;
        internal long nid = 0L; // for creating new identifiers for export
        internal static Context _system => new(Database._system);
        internal readonly long cxid = ++_cxid;
        public User? user => db.user;
        public Role role => db.role;
        internal Context? next, parent = null; // contexts form a stack (by nesting or calling)
        internal Rvv affected = Rvv.Empty;
        public BTree<long, TRow> cursors = BTree<long, TRow>.Empty;
        internal CTree<long, bool> restRowSets = CTree<long, bool>.Empty;
        public long nextHeap = -1L, parseStart = -1L;
        internal Domain? result = null,lastret = null;
        internal CTree<long, TypedValue> values = CTree<long, TypedValue>.Empty;
        public TypedValue val = TNull.Value;
        public Domain valueType => result ?? val?.dataType ?? Domain.Null;
        internal Database db = Database.Empty;
        internal Connection conn;
        internal string? url = null;
        internal Transaction? tr => db as Transaction;
        internal BList<TriggerActivation> deferred = BList<TriggerActivation>.Empty;
        internal BList<Exception> warnings = BList<Exception>.Empty;
        internal ObTree obs = ObTree.Empty;
        // these 3 fields help with Fix(dp) during instancing (for Views)
        internal long instDFirst = -1L; // first uid for instance dest
        internal long instSFirst = -1L; // first uid in framing
        internal long instSLast = -1L; // last uid in framing
        internal BTree<int, ObTree> depths = BTree<int, ObTree>.Empty;
        internal CTree<long, CTree<long, bool>> forReview = CTree<long, CTree<long, bool>>.Empty; // QlValue,RowSet
        internal BTree<long, BList<BTree<long, object>>> forApply = BTree<long, BList<BTree<long, object>>>.Empty;// RowSet, props
        internal CTree<long, Domain> groupCols = CTree<long, Domain>.Empty; // Domain; GroupCols for a Domain with Aggs
        internal BTree<long, BTree<TRow, BTree<long, Register>>> funcs = BTree<long, BTree<TRow, BTree<long, Register>>>.Empty; // Agg GroupCols
        internal BTree<long, BTree<long, TableRow>> newTables = BTree<long, BTree<long, TableRow>>.Empty;
        internal BTree<Domain, long?> newTypes = BTree<Domain, long?>.Empty; // uncommitted types
        internal BTree<long,Names> defs = BTree<long,Names>.Empty; // lexical scopes at lower levels
        internal TRow? path = null;
        internal Qlx inclusionMode = Qlx.ANY;
        internal CTree<long,CTree<long,CTree<long,(int,CTree<long,TypedValue>)>>> paths // shortest/longest
            = CTree<long,CTree<long,CTree<long,(int,CTree<long,TypedValue>)>>>.Empty; // GqlPath,TNode,TNode,Bindings
        internal Names names = Names.Empty; // QlValue names at current level
        internal Names dnames = Names.Empty; // non-QlValue (e.g. Domain, TableColumn) names at current level
        internal Names anames = Names.Empty; // ambient names
        internal int sD => (int)defs.Count; // used for forgetting blocks of names
        internal long offset = 0L; // set in Framing._Relocate, constant during relocation of compiled objects
        internal Graph? graph = null; // current graph, set by USE
        internal Schema? schema = null; // current rowType, set by AT
        internal bool ParsingMatch = false;
        internal CTree<long, long> undefined = CTree<long, long>.Empty;
        // bindings, and cache of RowSetPredicate RowSet (not initialised from parent, not copied to parent on exit)
        public BTree<long, Domain> bindings = BTree<long,Domain>.Empty;
        // UnHeap things for Procedure, Trigger, and Constraint bodies
        internal BTree<long, long?> uids = BTree<long, long?>.Empty;
        /// <summary>
        /// Used in Replace cascade
        /// </summary>
        internal ObTree done = ObTree.Empty;
        internal BList<(DBObject, DBObject)> todo = BList<(DBObject, DBObject)>.Empty;
        internal bool replacing = false;
        /// <summary>
        /// Used for prepared statements
        /// </summary>
        internal CList<long> qParams = CList<long>.Empty;
        /// <summary>
        /// The current or latest statement
        /// </summary>
        public Executable? exec = null;
        /// <summary>
        /// local syntax namespace defined in XMLNAMESPACES or elsewhere,
        /// indexed by prefix
        /// </summary>
        internal BTree<string, string> nsps = BTree<string, string>.Empty;
        /// <summary>
        /// Used for View processing: lexical positions of ends of columns
        /// </summary>
        internal BList<Ident> viewAliases = BList<Ident>.Empty;
        internal ExecuteStatus parse = ExecuteStatus.Obey;
        internal CTree<long, bool> rdC = CTree<long, bool>.Empty; // read TableColumns defpos
        internal CTree<long, CTree<long, bool>> rdS = CTree<long, CTree<long, bool>>.Empty; // specific read TableRow defpos
        internal BTree<Audit, bool> auds = BTree<Audit, bool>.Empty;
        internal CTree<long, TypedValue> binding = CTree<long, TypedValue>.Empty;
        internal BTree<long, long?> newnodes = BTree<long, long?>.Empty;
        public int rconflicts = 0, wconflicts = 0;
        /// <summary>
        /// We only send versioned information if 
        /// a) the pseudocolumn VERSIONING has been requested or
        /// b) Protocol.Get has been used (POCO Versioned library) or
        /// c) there is a RestRowSet or
        /// d) we are preparing an HttpService Response
        /// </summary>
        internal bool versioned = false;
        internal Context(Database db, Connection? con = null)
        {
            next = null;
            cxid = db.lexeroffset;
            conn = con ?? new Connection();
            obs = ObTree.Empty;
            nextHeap = conn.nextPrep;
            parseStart = 0L;
            this.db = db;
        }
        internal Context(Database db, Context cx)
        {
            next = null;
            cxid = db.lexeroffset;
            conn = cx.conn;
            if (conn.props["Schema"] is string sn)
                cx.schema = cx.db.objects[cx.role.schemas[sn] ?? -1] as Schema;
            if (conn.props["Graph"] is string gn)
                cx.graph = cx.db.objects[cx.role.graphs[gn] ?? -1] as Graph;
            nextHeap = conn.nextPrep;
            parseStart = 0L;
            this.db = db;
            if (db is Transaction)
                rdC = cx.rdC;
        }
        internal Context(Context cx)
        {
            next = cx;
            db = cx.db;
            conn = cx.conn;
            nextHeap = cx.nextHeap;
            parseStart = cx.parseStart;
            values = cx.values;
            obs = cx.obs;
            anames = cx.anames;
            defs = cx.defs;
            names = cx.names;
            depths = cx.depths;
            obs = cx.obs;
            cursors = cx.cursors;
            val = cx.val;
            parent = cx.parent; // for triggers
            if (cx.parse.HasFlag(ExecuteStatus.Detach))
                parse = cx.parse;
            rdC = cx.rdC;
            nid = cx.nid;
            restRowSets = cx.restRowSets;
            groupCols = cx.groupCols;
            // and maybe some more?
        }
        internal Context(Context c, Role r, User u) : this(c)
        {
            db = db + (Database.Role, r) + (Database.User, u);
        }
        internal (DBObject?,Ident?) Lookup(Ident n)
        {
            var p = names[n.ToString()];
            if (p!=0L && _Ob(p) is DBObject ob)
                return (ob, null);
            var (o, s) = Lookup(names, n);
            if (o is null && defs[Lookup(n.ident)?.defpos ?? -1L] is Names f && n.sub is not null)
                (o, s) = Lookup(f, n.sub);
            return (o,s);  
        }
        internal DBObject? Lookup(string n)
        {
            return _Ob(names[n]);
        }
        internal DBObject? Lookup(Names ns,string n)
        {
            return ns.Contains(n)?_Ob(ns[n]):null;
        }
        internal DBObject? Lookup(BTree<long,Names> s, string n)
        {
            for (var b = s.Last(); b != null; b = b.Previous())
                if (b.value() is Names t
                    && t.Contains(n) && _Ob(t[n]) is DBObject ob)
                    return ob;
            return null;
        }
        internal (DBObject?, Ident?) Lookup(Names t, Ident n,long f=-1L)
        {
            var p = t[n.ident];
            if (p!=0L && _Ob(p) is DBObject ob)
            {
                var ns = defs[ob.defpos]??ob.names;
                if (ob is GqlNode g)
                {
                    if (n.sub?.ToString() is string nm
                    && g.domain is Domain gd && gd.names[nm] is long np
                    && gd.representation[np] is Domain dt)
                        return (Add(new SqlField(n.uid, nm, -1, g.defpos, dt, g.defpos)), null);
                    if (n.sub?.ident == "POSITION")
                        return (Add(new SqlFunction(n.uid, this, Qlx.POSITION, g, null, null, Qlx.NO)), null);
                }
                if (ob is QlValue || ob is RowSet || ob is Procedure)
                { 
                    if (ns != Names.Empty && n.sub is Ident os)
                        return Lookup(ns, os);
                    return (ob, n.sub);
                }
                else
                {
                    var nf = GetUid();
                    var q = new QlInstance(nf, this, n.ident, f, ob);
                    Add(q);
                    ns = ob.domain.infos[role.defpos]?.names ?? Names.Empty;
                    if (ns != Names.Empty && n.sub is Ident ps)
                        return Lookup(ns, ps, nf);
                    return (q, null);
                }
            }
            return (null, n);
        }
        internal Names ApplyDone(Names ns)
        {
            var r = Names.Empty;
            for (var b = ns.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (done[p] is DBObject nb)
                    r += (b.key(), nb.defpos);
                else
                    r += (b.key(), p);
            }
            return r;
        }
        internal BTree<long,Names> ApplyDone(BTree<long,Names> ds)
        {
            var r = BTree<long,Names>.Empty;
            for (var b = ds.First(); b != null; b = b.Next())
                r += (b.key(),ApplyDone(b.value()));
            return r;
        }
        internal bool Known(string n)
        {
            return Lookup(n) is not null;
        }
        internal bool Known(BList<BTree<string,(int,long)>>? nl,string n)
        {
            for (var b=nl?.Last();b!=null;b=b.Previous())
                if (b.value() is BTree<string,(int,long)> t && t.Contains(n))
                    return true;
            return false;
        }
            /*        /// <summary>.
        /// If the selectdepth has changed it is unlikely to be the right identification,
        /// so leave it to resolve later.
        /// </summary>
        /// <param name="lp"></param>
        /// <param name="n"></param>
        /// <param name="d"></param>
        /// <returns></returns>
        internal (DBObject?, Ident?) Lookup(long lp, Ident n, int d)
        {
            var (bc, _, sub) = defs[(n, d, BList<Iix>.Empty)]; // chain lookup
            DBObject? rr, r = null;
            Ident? s = n;
            for (var b = bc.First(); b != null; b = b.Next())
            {
                rr = r;
                var ix = b.value();
                if (_Ob(ix.dp) is DBObject ob)
                {
                    if (ix.sd < sD && (ob is SqlReview||
                        (ob.GetType().Name=="QlValue" && (!parse.HasFlag(ExecuteStatus.Graph))
                            &&ob.domain.kind!=Qlx.PATH))) // an undefined identifier from a lower level
                        return (null, n);
                    (r, s) = ob._Lookup(lp, this, n, n.sub, rr);
                    if (r!=ob && (r is SqlField||(r is SqlValueExpr se && se.op==Qlx.DOT)))
                    {
                        if (s is null)
                        {
                            undefined -= n.iix.dp;
                            undefined -= lp;
                        }
                        break;
                    }
                    (r,s) = ob._Lookup(lp, this, n, sub, rr);
                    lp = GetUid();
                    if (sub!=null)
                        n = sub;
                    continue;
                }
                if (ix.sd < sD) // an undefined identifier from a lower level
                    return (null, n);
            }
            return (r,s);
        }
*/
        internal CTree<long, bool> Needs(CTree<long, bool> nd,
        RowSet rs, Domain dm)
        {
            var s = (long)(rs.mem[RowSet._CountStar] ?? -1L);
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && p != s)
                    for (var c = obs[p]?.Needs(this, rs.defpos).First(); c != null; c = c.Next())
                    {
                        var u = c.key();
                        nd += (u, true);
                    }
            return nd;
        }
        internal string Alias()
        {
            return "C_" + (++nid);
        }
  /*      internal Iix Ix(long u)
        {
            return new Iix(u, this, u);
        }
        internal Iix Ix(long l, long d)
        {
            return new Iix(l, this, d);
        } */
        internal CTree<long, bool> Needs(CTree<long, bool> nd, CList<long> rt)
        {
            for (var b = rt?.First(); b != null; b = b.Next())
                nd += obs[b.value()]?.Needs(this) ?? CTree<long, bool>.Empty;
            return nd;
        }
        /// <summary>
        /// During Parsing
        /// </summary>
        /// <param name="ic"></param>
        /// <param name="xp"></param>
        /// <returns></returns>
        internal DBObject? Get(Ident ic, Domain xp)
        {
            DBObject? v = null;
            var (l1, l2) = Lookup(ic);
            if (l1 is QlValue s0 && l2 is null)
                v = s0;
            else v = Lookup(ic.ident) as Domain;
            if (v != null)
            {
                if (v.domain is Domain dv && !xp.CanTakeValueOf(dv))
                    throw new DBException("42000", ic);
                if (v.defpos >= Transaction.HeapStart)
                    for (var f = obs[v.from] as RowSet; f != null;)
                    {
                        for (var b = f.matching[v.defpos]?.First(); b != null; b = b.Next())
                            if (obs[b.key()] is QlValue mv && mv.defpos < Transaction.HeapStart)
                                return mv;
                        var ff = f;
                        f = obs[f.from] as RowSet;
                        if (f == ff)
                            break;
                    }
            }
            return v;
        }
        internal void CheckRemote(string url, string etag)
        {
            var rq = new HttpRequestMessage(HttpMethod.Head, url);
            rq.Headers.Add("UserAgent", "Pyrrho " + PyrrhoStart.Version[1]);
            var cr = (user?.name ?? "") + ":";
            var d = Convert.ToBase64String(Encoding.UTF8.GetBytes(cr));
            rq.Headers.Add("UseDefaultCredentials", "false");
            rq.Headers.Add("Authorization", "Basic " + d);
            rq.Headers.Add("If-Match", etag);
            var rs = PyrrhoStart.htc.Send(rq);
            if (rs.StatusCode == HttpStatusCode.OK)
            {
                var e = rs.Headers.ETag?.ToString() ?? "";
                if (e != "" && e != etag)
                    throw new DBException("40082");
            }
            else
            {
                throw new DBException("40082", url);
            }
        }
        internal long GetPos()
        {
            if (db == null)
                return -1L;
            switch (parse)
            {
                case ExecuteStatus.Parse:
                case ExecuteStatus.Compile:
                case ExecuteStatus.Compile | ExecuteStatus.Graph:
                    {
                        var r = db.nextStmt;
                        db += (Database.NextStmt, r + 1);
                        return r;
                    }
                default: return db.nextPos;
            }
        }
  /*      internal Iix GetIid()
        {
            var u = GetUid();
            return Ix(u);
        } */
        internal long GetUid()
        {
            if (db == null)
                return -1L;
            if (parse.HasFlag(ExecuteStatus.Parse) || parse.HasFlag(ExecuteStatus.Compile))
            {
                var r = db.nextStmt;
                db += (Database.NextStmt, r + 1);
                return r;
            }
            else
                return nextHeap++;
        }
        internal long GetUid(int n)
        {
            long r;
            if (db == null)
                return -1L;
            if (parse.HasFlag(ExecuteStatus.Parse) || parse.HasFlag(ExecuteStatus.Compile))
            {
                r = db.nextStmt;
                db += (Database.NextStmt, r + n);
                return r;
            }
            else
            {
                r = nextHeap;
                nextHeap += n;
                return r;
            }
        }
        internal long GetPrevUid()
        {
            if (db == null)
                return -1L;
            return (parse.HasFlag(ExecuteStatus.Parse) || parse.HasFlag(ExecuteStatus.Compile))?
                db.nextStmt - 1:  nextHeap - 1;
        }
        internal string NewNode(long pp, string v)
        {
            if (long.TryParse(v, out var c) && c >= Transaction.TransPos)
            {
                if (newnodes[c] is long q)
                    return q.ToString();
                else
                {
                    newnodes += (c, pp);
                    return pp.ToString();
                }
            }
            return v;
        }
        internal int _DepthV(long? p, int d)
        {
            if (p is not long q || d < 0)
                return d;
            return (obs[q] is DBObject ob) ? Math.Max(ob.depth + 1, d) : d;
        }
        internal int _DepthBV(CList<long>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                d = _DepthV(b.value(), d);
            return d;
        }
        internal int _DepthBBV(CList<CList<long>>?t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                d = _DepthBV(b.value(), d);
            return d;
        }
        internal int _DepthTUb(CTree<UpdateAssignment, bool>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
            {
                var u = b.key();
                d = _DepthV(u.val, _DepthV(u.vbl, d));
            }
            return d;
        }
        internal int _DepthBPVX<X>(BList<(long, X)>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
            {
                var u = b.value().Item1;
                d = _DepthV(u, d);
            }
            return d;
        }
        internal int _DepthBPXV<X>(BList<(X, long)>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
            {
                var u = b.value().Item2;
                d = _DepthV(u, d);
            }
            return d;
        }
        internal int _DepthBPVV(BList<(long, long)>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
            {
                var (u, v) = b.value();
                d = _DepthV(u, _DepthV(v, d));
            }
            return d;
        }
        internal int _DepthTVX<X>(BTree<long, X> t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                d = _DepthV(b.key(), d);
            return d;
        }
        internal static int _DepthBO(BList<QlValue> t, int d)
        {
            for (var b = t.First(); b != null; b = b.Next())
                d = Math.Max(b.value().depth + 1, d);
            return d;
        }
        internal int _DepthTXV<V>(BTree<V, long?> t, int d) where V : IComparable
        {
            for (var b = t.First(); b != null; b = b.Next())
                d = _DepthV(b.value(), d);
            return d;
        }
        internal static int _DepthTDb(CTree<Domain, bool> t, int d)
        {
            for (var b = t.First(); b != null; b = b.Next())
                d = Math.Max(b.key().depth + 1, d);
            return d;
        }
        internal int _DepthTVBt(BTree<long, BList<TypedValue>> t, int d)
        {
            for (var b = t.First(); b != null; b = b.Next())
                d = _DepthV(b.key(), d);
            return d;
        }
        internal int _DepthTVD(CTree<long, Domain>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                d = _DepthV(b.key(), Math.Max(b.value().depth + 1, d));
            return d;
        }
        internal int _DepthTVV(BTree<long, long?>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                d = _DepthV(b.key(), _DepthV(b.value(), d));
            return d;
        }
        internal int _DepthTsl(Names? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                if (b.value() is long p)
                    d = _DepthV(p, d);
            return d;
        }
        internal int _DepthTlPiD(BTree<long,(int,Domain)> tg, int d)
        {
            for (var b = tg?.First(); b != null; b = b.Next())
            {
                var (i, dm) = b.value();
                if (db.objects[b.key()] is EdgeType et)
                    d = Math.Max(et.depth, d);
                d = Math.Max(dm.depth, d);
            }
            return d;
        }
        internal static int _DepthLT<T>(CList<T> s, int d) where T : TypedValue
        {
            for (var b = s.First(); b != null; b = b.Next())
                if (b.value() is TypedValue v)
                    d = Math.Max(v.dataType.depth + 1, d);
            return d;
        }
        internal static int _DepthLPlT<T>(Domain dm, BList<(long,T)> s, int d) where T : TypedValue
        {
            for (var b = s.First(); b != null; b = b.Next())
                if (b.value().Item2 is TypedValue v && v.dataType.defpos!=dm.defpos)
                    d = Math.Max(v.dataType.depth + 1, d);
            return d;
        }
        internal int _DepthG(GenerationRule ge, int d)
        {
            if (obs[ge.exp] is QlValue sv)
                d = Math.Max(sv.depth + 1, d);
            return d;
        }
        internal int _DepthTDTVb(CTree<Domain, CTree<long, bool>> t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                d = _DepthTVX(b.value(), Math.Max(b.key().depth + 1, d));
            return d;
        }
        internal int _DepthTVTVb(CTree<long, CTree<long, bool>>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                d = _DepthTVX(b.value(), _DepthV(b.key(), d));
            return d;
        }
        // define an object at a lower level (happens with select statement)
        internal void Define(long lp,DBObject ob)
        {
            var nn = BTree<long,Names>.Empty;
            for (var b = defs.First(); b != null && b.key() < lp; b = b.Next())
            {
                var k = b.key();
                var u = b.value();
                if (k == lp)
                    nn += (k, u + (ob.name, ob.defpos));
                else if (u.Contains(ob.name) && obs[u[ob.name]] is SqlReview ud)
                {
                    Replace(ud, ob);
                    nn += (k, u - ob.name);
                }
                else
                    nn += (k, u);
            }
            defs = nn;
        }
        internal BTree<long, QlValue> Map(CList<long> s)
        {
            var r = BTree<long, QlValue>.Empty;
            for (var b = s.First(); b != null; b = b.Next())
                if (b.value() is long p && obs[p] is QlValue sp)
                    r += (p, sp);
            return r;
        }
        internal static bool HasItem(CList<long> rt, long p)
        {
            for (var b = rt.First(); b != null; b = b.Next())
                if (b.value() == p)
                    return true;
            return false;
        }
        internal virtual void AddValue(DBObject s, TypedValue tv)
        {
            if (tv is Cursor)
                Console.WriteLine("AddValue??");
            s.Set(this, tv);
        }
        internal DBObject? Add(Physical ph)
        {
            if (ph == null || db == null)
                throw new DBException("42105").Add(Qlx.DATABASE);
            if (PyrrhoStart.DebugMode && db is Transaction)
                Console.WriteLine(ph.ToString());
            return db.Add(this, ph);
        }
        /// <summary>
        /// Add the framing objects without relocation, but taking account of their depths.
        /// This avoids the simpler cx.obs+= which ignores depths issues.
        /// </summary>
        /// <param name="fr"></param>
        internal void Add(Framing fr)
        {
            for (var b = fr.obs.First(); b != null; b = b.Next())
                Add(b.value());
        }
        internal void AddPost(string u, string tn, string s, string us, long vp, PTrigger.TrigType tp)
        {
            if (db is Transaction tr)
            {
                for (var b = tr.physicals.Last(); b != null; b = b.Previous())
                    if (b.value() is Physical ph)
                        switch (ph.type)
                        {
                            case Physical.Type.PTransaction:
                                goto ins;
                            case Physical.Type.Post:
                                {
                                    var p = (Post)ph;
                                    if (p.url == u && p.target == tn && p.user == us && vp == p._vw)
                                    {
                                        p.sql += ("," + s);
                                        return;
                                    }
                                    goto ins;
                                }
                        }
                    ins:
                Add(new Post(u, tn, s, us, vp, tp, db.nextPos, this));
            }
        }
        internal DBObject Add(DBObject ob)
        {
            if (obs[ob.defpos] is DBObject nb && nb.dbg>=ob.dbg)
                return nb;
            if (obs[ob.defpos] is DBObject oo && oo.depth != ob.depth)
                if (depths[oo.depth] is ObTree de)
                    depths += (oo.depth, de - ob.defpos);
            if (ob.defpos != -1L && obs[ob.defpos]?.dbg!=ob.dbg) 
                _Add(ob);
            if (ob.defpos >= Transaction.HeapStart)
                done += (ob.defpos, ob);
            if (ob is Executable e)
                exec = e;
            return ob;
        }
        internal void AddObs(Table tb)
        {
            if (!obs.Contains(tb.defpos))
            {
                Add(tb);
                if (tb is UDType ut)
                {
                    for (var b = ut.super.First();b!=null;b=b.Next())
                        if (b.key() is Table su)
                            AddObs(su);
                    for (var b = ut.subtypes.First();b!=null;b=b.Next())
                        if (db.objects[b.key()] is Table st)
                            AddObs(st);
                    for (var b = ut.methods.First(); b != null; b = b.Next())
                        if (!obs.Contains(b.key()) && db.objects[b.key()] is DBObject ob)
                            Add(ob);
                }
            }
        }
        /// <summary>
        /// Add an object to the database/transaction 
        /// </summary>
        /// <param name="ob"></param>
        internal void Install(DBObject ob)
        {
            db += ob;
            Add(ob);
            if (ob.mem.Contains(DBObject._Framing))
            {
                // For compiled objects the parser creates many identifiers with executable uids
                // during the parsing transaction.
                // But the only objects with such uids that are persistent are in framings.
                var t = ob.framing.obs.Last()?.key() ?? -1L;
                if (t > db.nextStmt)
                    db += (Database.NextStmt, t + 1);
                // (We don't care if this process leaves gaps in the uid tree)
            }
        }
        internal Context ForConstraintParse()
        {
            // Set up the information for parsing the generation rule
            // The table domain and cx.defs should contain the columns so far defined
            var cx = new Context(this) { parse = ExecuteStatus.Compile };
            var rs = CTree<long, Domain>.Empty;
            Ident? ti = null;
     //       Table? tb = null;
            for (var b = cx.obs?.PositionAt(0); b != null; b = b.Next())
                if (role != null && b.value() is DBObject ob 
                    && ob.infos[role.defpos] is ObInfo oi && oi.name is string nm)
                {
                    var ic = new Ident(nm, b.key());
                    var dm = cx._Dom(b.key());
                    if (dm == null)
                        continue;
                    if (dm.kind == Qlx.TABLE || dm is NodeType)
                        ti = ic;
                    else if (ti != null)
                    {
                        ic = new Ident(ti, ic);
                        rs += (b.key(), dm);
                    }
                    if (ob is QlValue)
                        cx.Add(nm, ob);
                }
      //     if (tb != null)
      //          cx.Add(tb + (Table.TableCols, rs));
            return cx;
        }
        internal static bool Match(CList<long> a, CList<long> b)
        {
            return Compare(a, b)==0;
        }
        internal static int Compare(CList<long> a, CList<long> b)
        {
            var c = a.Count.CompareTo(b.Count);
            if (c!=0)
                return c;
            var ab = a.First();
            for (var bb = b.First(); ab != null && bb != null; ab = ab.Next(), bb = bb.Next())
            {
                if (ab.value() is long ap)
                {
                    if (bb.value() is long bp)
                    {
                        c = ap.CompareTo(bp);
                        if (c != 0)
                            return c;
                    }
                    else return -1;
                }
                else return 1;
            }
            return 0;
        }
        internal static int Compare(long? a,long? b)
        {
            if (a is long ap)
            {
                if (b is long bp)
                    return ap.CompareTo(bp);
                else return -1;
            }
            else return 1;
        }
        internal static bool CanCall(CList<Domain>m,CList<Domain>p)
        {
            var mb = m.First();
            var pb = p.First();
            for (; mb != null && pb != null; mb = mb.Next(), pb = pb.Next())
                if (!mb.value().CanTakeValueOf(pb.value()))
                    return false;
            return mb == null && pb == null;
        }
        internal Domain _Dom(Domain dm, long p, Domain d)
        {
            var rs = dm.representation;
            if (rs[p] == d)
                return dm;
            if (dm.defpos < Transaction.Executables)
            {
                var nd = (Domain)dm.Relocate(GetUid());
                if (nd is EdgeType ne && nd.defpos != dm.defpos)
                    ne.Fix(this);
                dm = nd;
            }
            var rt = dm.rowType;
            if (!rt.Has(p))
                rt += p;
            dm =(Domain)dm.New(dm.mem+ (Domain.Representation, rs + (p, d))
                + (Domain.RowType, rt));
            if (db.Find(dm) is Domain r)
                return r;
            return (Domain)Add(dm);
        }
        internal Domain _Dom(long dp, params (long, object)[] xs)
        {
            var ob = (obs[dp] ?? (DBObject?)db.objects[dp]);
            var dm = ob as Domain ?? ob?.domain ?? Domain.Null;
            var m = dm.mem;
            var ch = false;
            foreach (var x in xs)
            {
                var (k, v) = x;
                m += x;
                ch = ch || dm.mem[k] == v;
            }
            if (!ch)
                return dm;
            if (dm.defpos < Transaction.Executables)
            {
                var nd = (Domain)dm.Relocate(GetUid());
                if (nd is EdgeType ne && nd.defpos != dm.defpos)
                    ne.Fix(this);
                dm = nd;
            }
            var r = new Domain(dm.defpos, m);
            Add(r);
            return r;
        }
        internal BTree<long, object> Name(Ident n, BList<Ident> ch, BTree<long,object>? m=null)
        {
            var mm = (m ?? BTree<long, object>.Empty);
            mm += (DBObject.Infos, new BTree<long, ObInfo>(role.defpos, new ObInfo(n.ident)));
            mm += (DBObject.Definer, role.defpos);
            mm += (QlValue.Chain, ch);
            return mm + (ObInfo.Name, n.ident) + (DBObject._Ident, n);
        }
        internal DBObject _Add(DBObject ob)
        {
            if (ob.from>=0 && !ob.Verify(this))
                throw new DBException("42112", ob);
            if (ob.defpos != -1 && obs[ob.defpos]?.dbg!=ob.dbg)
            {
                ob._Add(this);
                var dp = depths[ob.depth] ?? ObTree.Empty;
                depths += (ob.depth, dp + (ob.defpos, ob));
            }
            return ob;
        }
        /// <summary>
        /// In the _Replace algorithm we need to ensure that the depths information is kept consistent.
        /// </summary>
        /// <param name="m"></param>
        /// <param name="p"></param>
        /// <param name="o"></param>
        /// <returns></returns>
        internal BTree<long,object> Add(BTree<long,object>m,long p, object o)
        {
            if (m[p] == o)
                return m;
            var md = (int)(m[DBObject._Depth] ?? 0);
            m += (DBObject._Depth, _Depth(p, o, md));
            return m + (p, o);
        }
        internal BTree<long,object> DoDepth(BTree<long,object>m)
        {
            var d = (int)(m[DBObject._Depth] ?? 0);
            for (var b = m.First(); b != null; b = b.Next())
                d = _Depth(b.key(), b.value(), d);
            return m + (DBObject._Depth, d);
        }
        internal int _Depth(long p, object o, int d)
        {
            if (o is long q)
            {
                if (obs[q] is DBObject ob && ob.depth >= d)
                    d = ob.depth + 1;
            }
            else
                d = p switch
                {
                    HandlerStatement.Action => Math.Max(((HandlerStatement)o).depth, d),
                    Domain.Aggs => _DepthTVX((CTree<long, bool>)o, d),
                    SqlValueArray._Array => _DepthBV((CList<long>)o, d),
                    RowSet.Assig => _DepthTUb((CTree<UpdateAssignment, bool>)o, d),
                    SqlCall.Parms => _DepthBV((CList<long>)o, d),
                    SqlCaseSimple.Cases => _DepthBPVV((BList<(long, long)>)o, d),
                    Domain.DefaultRowValues => _DepthTVX((CTree<long, TypedValue>)o, d),
                    ConditionalStatement.Else => _DepthBV((CList<long>)o, d),
                    ConditionalStatement.Elsif => _DepthBV((CList<long>)o, d),
                    ExplicitRowSet.ExplRows => _DepthBPVX((BList<(long, TRow)>)o, d),
                    RowSet.Filter => _DepthTVX((CTree<long, TypedValue>)o, d),
                    SqlFunction.Filter => _DepthTVX((CTree<long, bool>)o, d),
                    TransitionRowSet.ForeignKeys => _DepthTVX((CTree<long, bool>)o, d),
                    TableColumn.Generated => _DepthG((GenerationRule)o, d),
                    RowSet.Groupings => _DepthBV((CList<long>)o, d),
                    RowSet.GroupIds => _DepthTVD((CTree<long, Domain>)o, d),
                    RowSet.Having => _DepthTVX((CTree<long, bool>)o, d),
                    Table.Indexes => _DepthTDTVb((CTree<Domain, CTree<long, bool>>)o, d),
                    RowSet.ISMap => _DepthTVV((BTree<long, long?>)o, d),
                    JoinRowSet.JoinCond => _DepthTVX((CTree<long, bool>)o, d),
                    JoinRowSet.JoinUsing => _DepthTVV((BTree<long, long?>)o, d),
                    GetDiagnostics.List => _DepthTVX((CTree<long, Qlx>)o, d),
                    MultipleAssignment.List => _DepthBV((CList<long>)o, d),
                    RowSet._Matches => _DepthTVX((CTree<long, TypedValue>)o, d),
                    RowSet.Matching => _DepthTVTVb((CTree<long, CTree<long, bool>>)o, d),
                    Grouping.Members => _DepthTVX((CTree<long, int>)o, d),
                    ObInfo._Names => _DepthTsl((Names)o, d),
                    RestView.NamesMap => _DepthTVX((CTree<long, string>)o, d),
                    Domain.NodeTypes => _DepthTDb((CTree<Domain, bool>)o, d),
                    JoinRowSet.OnCond => _DepthTVV((BTree<long, long?>)o, d),
                    FetchStatement.Outs => _DepthBV((CList<long>)o, d),
                    PreparedStatement.QMarks => _DepthBV((CList<long>)o, d),
                    SelectRowSet.RdCols => _DepthTVX((CTree<long, bool>)o, d),
                    RowSet.Referenced => _DepthTVX((CTree<long, bool>)o, d),
                    Level3.Index.References => _DepthTVBt((BTree<long, BList<TypedValue>>)o, d),
                    Domain.Representation => _DepthTVD((CTree<long, Domain>)o, d),
                    RowSet.RestRowSetSources => _DepthTVX((CTree<long, bool>)o, d),
                    Domain.RowType => _DepthBV((CList<long>)o, d),
                    SignalStatement.SetList => _DepthTXV((BTree<Qlx, long?>)o, d),
                    GroupSpecification.Sets => _DepthBV((CList<long>)o, d),
                    RowSet.SIMap => _DepthTXV((BTree<long, long?>)o, d),
                    SqlRowSet.SqlRows => _DepthBV((CList<long>)o, d),
                    InstanceRowSet.SRowType => _DepthBV((CList<long>)o, d),
                    NestedStatement.Stms => _DepthBV((CList<long>)o, d),
                    ForwardReference.Subs => _DepthTVX((CTree<long, bool>)o, d),
                    ConditionalStatement.Then => _DepthBV((CList<long>)o, d),
                    Domain.UnionOf => _DepthTDb((CTree<Domain, bool>)o, d),
                    RestRowSetUsing.UsingCols => _DepthBV((CList<long>)o, d),
                    RowSet.UsingOperands => _DepthTVV((BTree<long, long?>)o, d),
                    AssignmentStatement.Val => Math.Max(((AssignmentStatement)o).depth, d),
                    AssignmentStatement.Vbl => Math.Max(((AssignmentStatement)o).depth, d),
                    WhileStatement.What => _DepthBV((CList<long>)o, d),
                    SimpleCaseStatement.Whens => _DepthBV((CList<long>)o, d),
                    RowSet._Where => _DepthTVX((CTree<long, bool>)o, d),
                    QuantifiedPredicate.Vals => _DepthBV((CList<long>)o, d),
                    MatchStatement.MatchList => _DepthBV((CList<long>)o, d),
                    MatchStatement.Truncating => _DepthTlPiD((BTree<long, (int, Domain)>)o, d),
                    GqlMatch.MatchAlts => _DepthBV((CList<long>)o, d),
                    GqlMatchAlt.MatchExps => _DepthBV((CList<long>)o, d),
                    _ => Math.Max(d, 1)
                };
            return d;
        }
        internal void _Remove(long dp)
        {
            obs -= dp;
            for (var b = depths.First(); b != null; b = b.Next())
                if (b.value() is ObTree ot)
                    depths += (b.key(), ot - dp);
        }
        internal void Replace(DBObject was, DBObject now)
        {
            if (!now.Verify(this))
                throw new DBException("42112", now);
            if (now is EdgeType ne && was.defpos != now.defpos)
                ne.Fix(this);
            _Add(now);
            ATree<int, (DBObject, DBObject)> a = todo;
            ATree<int, (DBObject, DBObject)>.Add(ref a, 0, (was, now));
            todo = (BList<(DBObject, DBObject)>)a;
            if (!replacing)
                while (todo.Length > 0)
                {
                    replacing = true;
                    var (w, n) = todo[0];
                    todo -= 0;
                    var ob = _Ob(w.defpos);
                    if (ob == null)
                        Console.WriteLine("Replace target removed: " + w.ToString());
                    else
                        DoReplace(w, n);
                }
            replacing = false;
        }
        internal void Later(long dp,BTree<long,object> mm)
        {
            var lm = forApply[dp] ?? BList<BTree<long, object>>.Empty;
            forApply += (dp, lm + mm);
        }
        internal void NowTry()
        {
            if (replacing || undefined != CTree<long, long>.Empty)
                return;
            for (var b = forApply.First(); b != null; b = b.Next())
            {
                if (obs[b.key()] is not RowSet rs)
                    throw new DBException("42000","NowTry");
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (c.value() is BTree<long, object> cm)
                        rs = (RowSet)Add(rs.Apply(cm, this));
            }
            forApply = BTree<long, BList<BTree<long, object>>>.Empty;
        }
        /// <summary>
        /// Update the query processing context in a cascade to implement a single replacement!
        /// </summary>
        /// <param name="was"></param>
        /// <param name="now"></param>
        void DoReplace(DBObject was, DBObject now)
        {
            if (db == null)
                return;
            done = new ObTree(now.defpos, now);
            if (was.defpos != now.defpos)
                done += (was.defpos, now);
            // scan by depth to perform the replacement
            var ldpos = db.length;
            var excframing = parse.HasFlag(ExecuteStatus.Compile) &&
                (was.defpos < Transaction.Executables || was.defpos >= Transaction.HeapStart);
            for (var b = depths.First(); b != null; b = b.Next())
                if (b.value() is ObTree bv)
                {
                    for (var c = bv.PositionAt(ldpos); c != null; c = c.Next())
                    {
                        var k = c.key();
                        if (excframing && k >= Transaction.Executables
                            && k < Transaction.HeapStart)
                            continue;
                        if (c.value() is DBObject op)
                        {
                            if (done[op.defpos] is DBObject oq && op.dbg != oq.dbg)
                            {
                                if (oq.depth != k)
                                    bv -= op.defpos;
                                op = oq;
                            }
                            var cv = op.Replace(this, was, now); // may update done
                            if (cv != op)
                                bv += (k, cv);
                        }
                    }
                    for (var c = forReview.First(); c != null; c = c.Next())
                    {
                        var k = c.key();
                        if (obs[k]?.depth != b.key())
                            continue;
                        if ((done[k] ?? obs[k]) is DBObject nv)
                        {
                            var nk = nv.defpos;
                            if (k != nk)
                            {
                                forReview -= k;
                                forReview += (nk, c.value());
                            }
                        }
                    }
                    if (bv != b.value())
                        depths += (b.key(), bv);
                }
            for (var b = done.First(); b != null; b = b.Next())
                if (b.value() is DBObject ob)
                    obs += (b.key(), ob); // depths??
            defs = ApplyDone(defs);
            names = ApplyDone(names);
            for (var b=forApply.First();b is not null;b=b.Next())
            {
                var ls = BList<BTree<long, object>>.Empty;
                for (var c = b.value().First(); c != null; c = c.Next())
                    ls += Replaced(c.value(),was.defpos);
                forApply += (b.key(), ls);
            }
            if (was.defpos != now.defpos && ((was.defpos>=Transaction.TransPos  && was.defpos < Transaction.Executables)
                || was.defpos >= Transaction.HeapStart))
                _Remove(was.defpos);
        }
        internal long ObReplace(long dp, DBObject was, DBObject now)
        {
            if (dp < Transaction.TransPos || !obs.Contains(dp))
                return dp;
            if (done[dp] is DBObject nb)
                return nb.defpos;
            if (obs[dp] == null)
                throw new PEException("PE111");
            return obs[dp]?.Replace(this, was, now)?.defpos ??
                throw new PEException("PE111");
        }
        internal long Replaced(long p)
        {
            return (done[p] is DBObject ob) ? ob.defpos : p;
        }
        internal long ReplacedNT(long p)
        {
            if (done[p] is NodeType nd) return nd.defpos;
            if (db.objects[p] is NodeType nt && nt.nodeTypes != CTree<Domain, bool>.Empty)
                return FindOrCreate(ReplacedTDb(nt.nodeTypes)).defpos;
            return p;
        }
        internal BTree<K, long?> Replaced<K>(BTree<K, long?> ms) where K : IComparable
        {
            var r = BTree<K, long?>.Empty;
            var ch = false;
            for (var b = ms.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var m = Replaced(p);
                    ch = ch || m != p;
                    r += (b.key(), m);
                }
            return ch ? r : ms;
        }
        internal CTree<long, V> Replaced<V>(CTree<long, V> ms) where V : IComparable
        {
            var r = CTree<long, V>.Empty;
            var ch = false;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var m = Replaced(k);
                ch = ch || m != k;
                r += (m, b.value());
            }
            return ch ? r : ms;
        }
        internal CTree<Domain, CTree<long, bool>> ReplacedTDTlb(CTree<Domain, CTree<long, bool>> xs)
        {
            var r = CTree<Domain, CTree<long, bool>>.Empty;
            var ch = false;
            for (var b = xs.First(); b != null; b = b.Next())
                if (b.value() is CTree<long, bool> c)
                {
                    var k = b.key();
                    var nk = k.Replaced(this);
                    var nc = ReplacedTlb(c);
                    ch = ch || k.CompareTo(nk) != 0 || c.CompareTo(nc) != 0;
                    r += (nk, nc);
                }
            return ch ? r : xs;
        }
        internal CList<long> ReplacedLl(CList<long> ks)
        {
            var r = CList<long>.Empty;
            var ch = false;
            for (var b = ks.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var np = done[p]?.defpos ?? p;
                    ch = ch || p != np;
                    r += np;
                }
            return ch ? r : ks;
        }
        internal CList<CList<long>> ReplacedLLl(CList<CList<long>> ks)
        {
            var r = CList<CList<long>>.Empty;
            var ch = false;
            for (var b = ks.First(); b != null; b = b.Next())
                if (b.value() is CList<long> p)
                {
                    var np = ReplacedLl(p);
                    ch = ch || p != np;
                    r += np;
                }
            return ch ? r : ks;
        }
        internal CTree<long, TypedValue> ReplacedTlV(CTree<long, TypedValue> vt)
        {
            var r = CTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = vt.First(); b != null; b = b.Next())
                if (b.value() is TypedValue v)
                {
                    var k = b.key();
                    var p = Replaced(k);
                    var nv = v.Replaced(this);
                    if (p != b.key() || nv != v)
                        ch = true;
                    r += (p, nv);
                }
            return ch ? r : vt;
        }
        internal CTree<long, bool> ReplacedTlb(CTree<long, bool> wh)
        {
            var r = CTree<long, bool>.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = done[k]?.defpos ?? k;
                ch = ch || k != nk;
                r += (nk, b.value());
            }
            return ch ? r : wh;
        }
        internal BTree<long,TableRow> ReplacedTlR(BTree<long,TableRow>rs, long w)
        {
            var r = BTree<long, TableRow>.Empty;
            var ch = false;
            var n = done[w]?.defpos ?? w;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var tr = b.value();
                var np = ReplacedNT(tr.tabledefpos);
                var nr = new TableRow(tr,np,ReplacedTlV(tr.vals)); 
                ch = b.value() != nr;
                r += (k, nr);
            }
            return ch ? r : rs;
        }
        internal CTree<long, Domain> ReplacedTlD(CTree<long, Domain> rs)
        {
            var r = CTree<long, Domain>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
                if (b.value() is Domain od)
                {
                    var k = b.key();
                    var p = Replaced(k);
                    var d = od.Replaced(this);
                    ch = ch || p != k || d != b.value();
                    r += (p, d);
                }
            return ch ? r : rs;
        }
        internal CTree<Domain, bool> ReplacedTDb(CTree<Domain, bool> ts)
        {
            var r = CTree<Domain, bool>.Empty;
            var ch = false;
            for (var b = ts.First(); b != null; b = b.Next())
            {
                var d = b.key();
                var nd = d.Replaced(this);
                ch = ch || d != nd;
                r += (nd, b.value());
            }
            return ch ? r : ts;
        }
        internal CTree<long, CTree<long, bool>> ReplacedTTllb(CTree<long, CTree<long, bool>> ma)
        {
            var r = CTree<long, CTree<long, bool>>.Empty;
            var ch = false;
            for (var b = ma.First(); b != null; b = b.Next())
                if (b.value() is CTree<long, bool> c)
                {
                    var k = b.key();
                    var p = Replaced(k);
                    var v = ReplacedTlb(c);
                    ch = ch || p != k || v != b.value();
                    r += (p, v);
                }
            return ch ? r : ma;
        }
        internal BTree<long, long?> ReplacedTll(BTree<long, long?> wh)
        {
            var r = BTree<long, long?>.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var k = b.key();
                    var nk = done[k]?.defpos ?? k;
                    var np = done[p]?.defpos ?? p;
                    ch = ch || k != nk || p != np;
                    r += (nk, np);
                }
            return ch ? r : wh;
        }
        internal CTree<UpdateAssignment, bool> ReplacedTUb(CTree<UpdateAssignment, bool> us)
        {
            var r = CTree<UpdateAssignment, bool>.Empty;
            var ch = false;
            for (var b = us.First(); b != null; b = b.Next())
            {
                var ua = b.key();
                if (done.Contains(ua.val) || done.Contains(ua.vbl))
                    ua = new UpdateAssignment(Replaced(ua.vbl), Replaced(ua.val));
                ch = ch || ua != b.key();
                r += (ua, true);
            }
            return ch ? r : us;
        }
        internal BList<(long, long)> ReplacedBll(BList<(long, long)> ds)
        {
            var r = BList<(long, long)>.Empty;
            var ch = false;
            for (var b = ds.First(); b != null; b = b.Next())
            {
                var (v, p) = b.value();
                var nv = Replaced(v);
                var np = Replaced(p);
                ch = ch || nv != v || np != p;
                r += (nv, np);
            }
            return ch ? r : ds;
        }
        internal BTree<long, BList<TypedValue>> ReplacedBBlV(BTree<long, BList<TypedValue>> t)
        {
            var r = BTree<long, BList<TypedValue>>.Empty;
            var ch = false;
            for (var b = t.First(); b != null; b = b.Next())
                if (b.value() is BList<TypedValue> v)
                {
                    var k = b.key();
                    var nk = Replaced(k);
                    var nv = ReplacedBV(v);
                    ch = ch || nk != k || nv != v;
                    r += (k, nv);
                }
            return ch ? r : t;
        }
        internal BList<TypedValue> ReplacedBV(BList<TypedValue> vs)
        {
            var r = BList<TypedValue>.Empty;
            var ch = false;
            for (var b = vs.First(); b != null; b = b.Next())
                if (b.value() is TypedValue v)
                {
                    var nv = v.Replaced(this);
                    ch = ch || nv != v;
                    r += nv;
                }
            return ch ? r : vs;
        }
        internal BList<(long, TRow)> ReplacedBlT(BList<(long, TRow)> rs)
        {
            var s = BList<(long, TRow)>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var (p, q) = b.value();
                var nq = (TRow)q.Replaced(this);
                ch = ch || q != nq;
                s += (p, nq);
            }
            return ch ? s : rs;
        }
        internal Names ReplacedTsl(Names wh)
        {
            var r = Names.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var k = b.key();
                var nb = done[p];
                var nk = nb?.alias ?? k;
                var np = nb?.defpos ?? p;
                ch = ch || p != np || k != nk;
                r += (nk,np);
            }
            return ch ? r : wh;
        }
        internal CTree<long, string> ReplacedTls(CTree<long, string> wh)
        {
            var r = CTree<long, string>.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.key() is long p)
                {
                    var k = b.value();
                    var nb = done[p];
                    var nk = nb?.alias ?? k;
                    var np = nb?.defpos ?? p;
                    ch = ch || p != np || k != nk;
                    r += (np, nk);
                }
            return ch ? r : wh;
        }
        internal CTree<long, TypedValue> ReplaceTlT(CTree<long, TypedValue> wh,
            DBObject so, DBObject sv)
        {
            var r = CTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.value() is TypedValue v)
                {
                    var k = b.key();
                    var nk = done[k]?.defpos ?? k;
                    var nv = v.Replace(this, so, sv);
                    ch = ch || k != nk || v != nv;
                    r += (nk, nv);
                }
            return ch ? r : wh;
        }
        internal CTree<long,TypedValue> ShallowReplace(CTree<long, TypedValue> fl,long was,long now)
        {
            for (var b=fl.First();b!=null;b=b.Next())
            {
                var k = b.key();
                if (k == was)
                {
                    fl -= k;
                    k = now;
                }
                var v = b.value().ShallowReplace(this, was, now);
                if (k != b.key() || v != b.value())
                    fl += (k, v);
            }
            return fl;
        }
        internal CTree<Domain, bool> ShallowReplace(CTree<Domain, bool> fl, long was, long now)
        {
            var r = CTree<Domain, bool>.Empty;
            for (var b = fl.First(); b != null; b = b.Next())
                r += ((Domain)b.key().ShallowReplace(this,was,now),true);
            return r;
        }
        /// <summary>
        /// drop was if present in representation
        /// </summary>
        /// <param name="rs"></param>
        /// <param name="was"></param>
        /// <param name="now"></param>
        /// <returns></returns>
        internal CTree<long, Domain> ShallowReplace(CTree<long, Domain> rs, long was, long now)
        {
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (k == was)
                {
                    rs -= k;
                    if (_Ob(now) is not TableColumn tc)
                        throw new PEException("PE106101");
                    rs += (now, tc.domain);
                    continue;
                }
                var v = (Domain)b.value().ShallowReplace(this, was, now);
                if (v != b.value())
                    rs += (k, v);
            }
            return rs;
        }
        internal static CTree<long, bool> ShallowReplace(CTree<long, bool> ag, long was, long now)
        {
            for (var b = ag.First(); b != null; b = b.Next())
                if (b.key() == was)
                {
                    ag -= was;
                    ag += (now, true);
                }
            return ag;
        }
        internal static CList<long> ShallowReplace(CList<long> rt, long was, long now)
        {
            var r = CList<long>.Empty;
            var ch = false;
            for (var b = rt.First(); b != null; b = b.Next())
                if (b.value() == was)
                {
                    ch = true;
                    r += now;
                }
                else
                    r += b.value();
            return ch ? r : rt;
        }
        BTree<long, bool> RestRowSets(long p)
        {
            var r = BTree<long, bool>.Empty;
            if (obs[p] is RowSet rs)
            {
                if (rs is RestRowSet)
                    r += (p, true);
                else
                    for (var b = rs.Sources(this).First(); b != null; b = b.Next())
                        r += RestRowSets(b.key());
            }
            return r;
        }
        /// <summary>
        /// Propagate prepared statement parameters into their referencing TypedValues
        /// </summary>
        internal void QParams()
        {
            for (var b = obs.PositionAt(Transaction.HeapStart); b != null; b = b.Next())
                if (b.value() is DBObject ob)
                    Add(ob.QParams(this));
        }
        internal CTree<long, TypedValue> QParams(CTree<long, TypedValue> f)
        {
            var r = CTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = f.First(); b != null; b = b.Next())
            {
                var v = b.value();
                if (v is TQParam tq)
                {
                    ch = true;
                    v = values[tq.qid] ?? TNull.Value;
                }
                r += (b.key(), v);
            }
            return ch ? r : f;
        }
        internal DBObject _Replace(long dp, DBObject was, DBObject now)
        {
            if (done[dp] is Domain ob)
                return ob;
            return (_Ob(dp)?.Replace(this, was, now)
                ?? throw new PEException("PE629"));
        }
        internal DBObject? _Ob(long dp)
        {
            return done[dp] ?? obs[dp] ?? (DBObject?)db.objects[dp];
        }
        internal DBObject? _Od(long dp)
        {
            return db.objects[dp] as DBObject ?? obs[dp];
        }
  /*      internal Iix Fix(Iix iix)
        {
            return new Iix(iix, Fix(iix.dp));
        } */
        /// <summary>
        /// Support for cx.Replace(was,now,m) during Parsing.
        /// Update a separate tree of properties accordingly.
        /// We don't worry about _Depth because that will be fixed in 
        /// the main Replace implementation, Apply, constructors etc.
        /// </summary>
        /// <param name="m">A tree of properties</param>
        /// <param name="w">special case for MergeColumn/TableRow</param>
        /// <returns>The updated tree</returns>
        internal BTree<long, object> Replaced(BTree<long, object> m,long w = -1L)
        {
            var r = BTree<long, object>.Empty;
            for (var b = m.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = b.value();
                if (v is not null)
                    switch (k)
                    {
                        case HandlerStatement.Action: v = Replaced((long)v); break;
                        case Level3.Index.Adapter: v = Replaced((long)v); break;
                        case Domain.Aggs: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case SqlValueArray._Array: v = ReplacedLl((CList<long>)v); break;
                        case SqlSelectArray.ArrayValuedQE: v = Replaced((long)v); break;
                        case RowSet.Assig: v = ReplacedTUb((CTree<UpdateAssignment, bool>)v); break;
                        case Procedure.Body: v = Replaced((long)v); break;
                        case CallStatement.Call: v = Replaced((long)v); break;
                        case SqlCaseSimple.CaseElse: v = Replaced((long)v); break;
                        case SqlCaseSimple.Cases: v = ReplacedBll((BList<(long, long)>)v); break;
                        case WhenPart.Cond: v = Replaced((long)v); break;
                        case ForStatement.CountCol: v = Replaced((long)v); break;
                        case Check.Condition: v = Replaced((long)v); break;
                        case FetchStatement.Cursor: v = Replaced((long)v); break;
                        case RowSet._Data: v = Replaced((long)v); break;
                        case Domain.DefaultRowValues: v = ReplacedTlV((CTree<long, TypedValue>)v); break;
                        case SqlTreatExpr._Diff: v = Replaced((BTree<long,object>)v,w); break;
                        case ConditionalStatement.Else: v = ReplacedLl((CList<long>)v); break;
                        case ConditionalStatement.Elsif: v = ReplacedLl((CList<long>)v); break;
                        case LikePredicate.Escape: v = Replaced((long)v); break;
                        case ExplicitRowSet.ExplRows: v = ReplacedBlT((BList<(long, TRow)>)v); break;
                        case RowSet.Filter: v = ReplacedTlV((CTree<long, TypedValue>)v); break;
                        case SqlFunction.Filter: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case GenerationRule.GenExp: v = Replaced((long)v); break;
                        case RowSet.Group: v = Replaced((long)v); break;
                        case RowSet.Groupings: v = ReplacedLl((CList<long>)v); break;
                        case RowSet.GroupIds: v = ReplacedTlD((CTree<long, Domain>)v); break;
                        case RowSet.Having: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case QuantifiedPredicate.High: v = Replaced((long)v); break;
                        case Table.Indexes: v = ReplacedTDTlb((CTree<Domain, CTree<long, bool>>)v); break;
                        case LocalVariableDec.Init: v = Replaced((long)v); break;
                        case SqlInsert.InsCols: v = ReplacedLl((CList<long>)v); break;
                        case Procedure.Inverse: v = Replaced((long)v); break;
                        case RowSet.ISMap: v = ReplacedTll((BTree<long, long?>)v); break;
                        case JoinRowSet.JFirst: v = Replaced((long)v); break;
                        case JoinRowSet.JoinCond: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case JoinRowSet.JoinUsing: v = ReplacedTll((BTree<long, long?>)v); break;
                        case JoinRowSet.JSecond: v = Replaced((long)v); break;
                        case Table.KeyCols: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case Level3.Index.Keys: v = ((Domain)v).Replaced(this); break;
                //        case GqlNode._Label: v = ((DBObject)v).Replaced(this); break;
                        case MergeRowSet._Left: v = Replaced((long)v); break;
                        case QlValue.Left: v = Replaced((long)v); break;
                        case MemberPredicate.Lhs: v = Replaced((long)v); break;
                        case GetDiagnostics.List: v = Replaced((CTree<long, Qlx>)v); break;
                        case MultipleAssignment.List: v = ReplacedLl((CList<long>)v); break;
                        case QuantifiedPredicate.Low: v = Replaced((long)v); break;
                        case RowSet._Matches: v = ReplacedTlV((CTree<long, TypedValue>)v); break;
                        case RowSet.Matching: v = ReplacedTTllb((CTree<long, CTree<long, bool>>)v); break;
                        case Grouping.Members: v = Replaced((CTree<long, int>)v); break;
                        case ObInfo._Names: v = ReplacedTsl((Names)v); break;
                        case RestView.NamesMap: v = ReplacedTls((CTree<long,string>)v); break;
                        case NullPredicate.NVal: v = Replaced((long)v); break;
                        case JoinRowSet.OnCond: v = ReplacedTll((BTree<long, long?>)v); break;
                        case SqlFunction.Op1: v = Replaced((long)v); break;
                        case SqlFunction.Op2: v = Replaced((long)v); break;
                        case SimpleCaseStatement._Operand: v = Replaced((long)v); break;
                        case WindowSpecification.Order: v = ((Domain)v).Replaced(this); break;
                        case Domain.OrderFunc: v = Replaced((long)v); break;
                        case FetchStatement.Outs: v = ReplacedLl((CList<long>)v); break;
                        case SqlCall.Parms: v = ReplacedLl((CList<long>)v); break;
                        case Procedure.Params: v = ReplacedLl((CList<long>)v); break;
                        case WindowSpecification.PartitionType: v = ((Domain)v).Replaced(this); break;
                        //    case RowSet.Periods:        v = Replaced
                        case SqlCall.ProcDefPos: v = Replaced((long)v); break;
                        case PreparedStatement.QMarks: v = ReplacedLl((CList<long>)v); break;
                        case SelectRowSet.RdCols: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case RowSet.Referenced: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case Level3.Index.References: v = ReplacedBBlV((BTree<long, BList<TypedValue>>)v); break;
                        case Level3.Index.RefIndex: v = Replaced((long)v); break;
                        case Domain.Representation: v = ReplacedTlD((CTree<long, Domain>)v); break;
                        case RowSet.RestRowSetSources: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case RestRowSetUsing.RestTemplate: v = Replaced((long)v); break;
                        case ReturnStatement.Ret: v = Replaced((long)v); break;
                        case Schema._Graphs: v = ReplacedTlb((CTree<long,bool>)v); break;
                        case MultipleAssignment.Rhs: v = Replaced((long)v); break;
                        case MergeRowSet._Right: v = Replaced((long)v); break;
                        case QlValue.Right: v = Replaced((long)v); break;
                        case RowSet.RowOrder: v = ReplacedLl((CList<long>)v); break;
                        case Domain.RowType: v = ReplacedLl((CList<long>)v); break;
                        case RowSet.RSTargets: v = ReplacedTll((BTree<long, long?>)v); break;
                        case SqlDefaultConstructor.Sce: v = Replaced((long)v); break;
                        case ConditionalStatement.Search: v = Replaced((long)v); break;
                        case ForSelectStatement.Sel: v = Replaced((long)v); break;
                        case QuantifiedPredicate._Select: v = Replaced((long)v); break;
                        case SignalStatement.SetList: v = Replaced((BTree<Qlx, long?>)v); break;
                        case GroupSpecification.Sets: v = ReplacedLl((CList<long>)v); break;
                        case RowSet.SIMap: v = ReplacedTll((BTree<long, long?>)v); break;
                        case RowSet._Source: v = Replaced((long)v); break;
                        case SqlCursor.Spec: v = Replaced((long)v); break;
                        case SqlRowSet.SqlRows: v = ReplacedLl((CList<long>)v); break;
                        case InstanceRowSet.SRowType: v = ReplacedLl((CList<long>)v); break;
                        case NestedStatement.Stms: v = ReplacedLl((CList<long>)v); break;
                        case QlValue.Sub: v = Replaced((long)v); break;
                        case SqlValueArray.Svs: v = Replaced((long)v); break;
                        case Level3.Index.TableDefPos: v = Replaced((long)v); break;
                        case Table.TableRows: v = ReplacedTlR((BTree<long, TableRow>)v,w); break;
                        case RowSet.Target: v = Replaced((long)v); break;
                        case ConditionalStatement.Then: v = ReplacedLl((CList<long>)v); break;
                        case SqlTreatExpr.TreatExpr: v = Replaced((long)v); break;
                        case QueryStatement.Result: v = Replaced((long)v); break;
                        case Domain.UnionOf: v = ReplacedTDb((CTree<Domain, bool>)v); break;
                        case RestRowSetUsing.UrlCol: v = Replaced((long)v); break;
                        case RestRowSetUsing.UsingCols: v = ReplacedLl((CList<long>)v); break;
                        case RowSet.UsingOperands: v = ReplacedTll((BTree<long, long?>)v); break;
                        case AssignmentStatement.Val: v = Replaced((long)v); break;
                        case AssignmentStatement.Vbl: v = Replaced((long)v); break;
                        case WhileStatement.What: v = Replaced((long)v); break;
                        case SimpleCaseStatement.Whens: v = ReplacedLl((CList<long>)v); break;
                        case FetchStatement.Where: v = Replaced((long)v); break;
                        case RowSet._Where: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case WindowSpecification.WQuery: v = Replaced((long)v); break;
                        case SqlFunction._Val: v = Replaced((long)v); break;
                        case QuantifiedPredicate.Vals: v = ReplacedLl((CList<long>)v); break;
                        case SqlInsert.Value: v = Replaced((long)v); break;
                        case SelectRowSet.ValueSelect: v = Replaced((long)v); break;
                        case SqlCall.Var: v = Replaced((long)v); break;
                        case SqlFunction.Window: v = Replaced((long)v); break;
                        case SqlFunction.WindowId: v = Replaced((long)v); break;
                        default: break;
                    }
                if (v is not null)
                    r += (k, v);
            }
            return r;
        }
        internal long Fix(long dp)
        {
            // Take care about Relocate on Commit
            if (parse == ExecuteStatus.Commit && dp > Transaction.TransPos
                && !uids.Contains(dp))
                undefined += (dp, (int)undefined.Count);
            // This is needed for Commit and for ppos->instance
            if (uids[dp] is long ep && ep != 0L)
                return ep;
            if (dp < Transaction.TransPos || db == null)
                return dp;
            // See notes in SourceIntro 3.4.2
            var r = dp;
            if (parse.HasFlag(ExecuteStatus.Graph) || parse.HasFlag(ExecuteStatus.GraphType)
                || parse.HasFlag(ExecuteStatus.Obey))
            {
                if (instDFirst > 0 && dp > instSFirst
                    && dp <= instSLast)
                    r = dp - instSFirst + instDFirst;
                if (r >= nextHeap)
                    nextHeap = r + 1;
            }
            else
            {
                if (instDFirst > 0 && dp > instSFirst
                        && dp <= instSLast)
                {
                    r = dp - instSFirst + instDFirst;
                    if (r >= db.nextStmt)
                        db += (Database.NextStmt, r + 1);
                }
                else
                    r = done[dp]?.defpos ?? dp;
            }
 //           if (!db.objects.Contains(r))
 //               r = -1L;
            return r;
        }
        internal long FixNT(long p)
        {
            if (db.objects[p] is NodeType nt && nt.nodeTypes.Count!=0 && !done.Contains(nt.defpos))
            {
                var ns = FixTDb(nt.nodeTypes);
                if (ns != nt.nodeTypes)
                    done += (nt.defpos,
                        new NodeType(nt.defpos, new BTree<long, object>(Domain.Kind, nt.kind) + (Domain.NodeTypes, ns)));
            }
            return Fix(p);
        }
        internal void AddDefs(Domain dm)
        {
            names += (dm.NameFor(this), dm.defpos);
            if (dm.infos[role.defpos] is ObInfo di)
            {
                for (var b = dm.rowType.First(); b != null; b = b.Next())
                    if (db.objects[b.value()] is Domain cd)
                        AddDefs(cd);
                defs += (dm.defpos, di.names);
            }
        }
        /// <summary>
        /// As a valueType of Alter Type we need to merge two TableColumns. We can't use the Replace machinery
        /// above since everyting will have depth 1. So we need a new set of transformers.
        /// These are called ShallowReplace because that is what they do on DBObjects.
        /// We do rely on Domains with columns all having positive uids, and no forward column references
        /// However, DBObjects frequently have embedded Domains with later defpos so we do all Domains first.
        /// The algorithm traverses all database objects with defpos>=0 in sequence. 
        /// All transformed objects are guaranteed to have unchanged defpos.
        /// As of v7 it seems to be true that these conditions ensure no stackoverflow (?)
        /// </summary>
        /// <param name="was">A TableColumn uid</param>
        /// <param name="now">A TableColumn uid guaranteed less than was</param>
        internal bool MergeColumn(long was, long now)
        {
            var od = db;
            for (var b = db.objects.PositionAt(0L); b != null; b = b.Next())
                if (b.value() is Domain d && d.kind!=Qlx.Null)
                {
                    var nb = (DBObject)d.ShallowReplace(this, was, now);
                    if (nb != d)
                        db += (b.key(), nb);
                }
            for (var b = db.objects.PositionAt(0L);b!=null;b=b.Next())
                if (b.value() is DBObject ob && ob is not Domain)
                {
                    var nb = (DBObject)ob.ShallowReplace(this, was, now);
                    if (nb != ob)
                        db += (b.key(), nb);
                }
            return db != od;
        }
        internal void DefineForward(string? n)
        {
            if (n == null)
                return;
            if (Lookup(n) is ForwardReference fr)
            {
                for (var b = fr.subs.First(); b != null; b = b.Next())
                    if (obs[b.key()] is DBObject ob && ob.chain is BList<Ident> ch)
                    {
                        var nc = BList<Ident>.Empty;
                        for (var c = ch.First(); c != null; c = c.Next())
                            if (c.value().ident != n)
                                nc += c.value();
                        obs += (ob.defpos, ob + (DBObject.Chain, nc));
                    }
                obs -= fr.defpos;
            }
        }
        internal void DefineStructures(Table? ob,RowSet rs)
        {
            if (ob == null)
                return;
            if (ob.infos[role.defpos] is ObInfo ti)
                for (var b = ti.names.First(); b != null; b = b.Next())
                    if (Lookup(b.key()) is ForwardReference fr
                            && b.value() is long dp
                            && ob.representation[dp] is UDType)
                    {
                        for (var c = fr.subs.First(); c != null; c = c.Next())
                            if (obs[c.key()] is QlValue sv && sv.name is string nm)
                                Replace(sv, new SqlField(sv.defpos, nm, -1, dp, sv.domain, ob.defpos));
                        obs -= fr.defpos;
                    }
        }
        internal void Add(string name,DBObject ob)
        {
            if (name == null)
                return;
            names += (name, ob.defpos);
            var ns = (ob.defpos >= Transaction.TransPos) ? ob.names : ob.infos[role.defpos]?.names;
            if (ns?.Count > 0 && !defs.Contains(ob.defpos))
                defs += (ob.defpos, ns);
        }
        internal void Add(Names ns)
        {
            for (var b = ns.First(); b != null; b = b.Next())
                if (_Ob(b.value()) is DBObject ob && !names.Contains(b.key()))
                    Add(b.key(), ob);
        }
        internal void AddParams(Procedure pr)
        {
            if (role != null && pr.infos[role.defpos] is ObInfo oi && oi.name != null)
            {
                var pi = new Ident(oi.name, 0); 
                for (var b = pr.ins.First(); b != null; b = b.Next())
                    if (b.value() is long p && obs[p] is FormalParameter pp) {
                        var pn = new Ident(pp.NameFor(this), 0);
                        names += (pp.NameFor(this), pp.defpos);
                        values += (p, TNull.Value); // for KnownBy
                        Add(pp);
                    }
                names += (oi.name, pr.defpos);
                defs += (pr.defpos, names-oi.name);
            }
        }
        internal long UnlabelledNodeSuper(CTree<string,bool> ps)
        {
            var rp = -1L;
            int n = 0;
            for (var b = role.unlabelledNodeTypesInfo.First(); b != null; b = b.Next())
            {
                var nn = 0;
                for (var c = b.key().First(); c != null; c = c.Next())
                    if (ps.Contains(c.key()))
                        nn++;
                if (nn > n && b.value() is long p)
                {
                    rp = p; n = nn;
                }
            }
            return rp;
        }
        internal long UnlabelledEdgeSuper(long lt,long at,CTree<string, bool> ps)
        {
            var rp = -1L;
            int n = 0;
            for (var b = role.unlabelledEdgeTypesInfo.First(); b != null; b = b.Next())
            {
                var nn = 0;
                for (var c = b.key().First(); c != null; c = c.Next())
                    if (ps.Contains(c.key()))
                        nn++;
                if (nn > n && b.value() is long p)
                {
                    rp = p; n = nn;
                }
            }
            return rp;
        }
        internal NodeType FindOrCreate(CTree<Domain,bool>ts)
        {
            var kind = ts.First()?.key()?.kind ?? Qlx.NODETYPE;
            var u = new NodeType(-1L, BTree<long, object>.Empty + (Domain.Kind, kind) + (Domain.NodeTypes, ts));
            if (db.objects[db.types[u] ?? -1L] is NodeType nt)
                return nt;
            var nst = db.nextStmt;
            u = (NodeType)u.Relocate(nst);
            db += (Database.NextStmt, nst + 1);
            db += (Database.Types, db.types + (u, nst));
            return u;
        }
        internal NodeType FindOrCreate(CTree<long, bool> ts)
        {
            var kind = Qlx.NODETYPE;
            var ds = CTree<Domain,bool>.Empty;
            for (var b = ts.First(); b != null; b = b.Next())
                if (db.objects[b.key()] is NodeType n)
                {
                    ds += (n, true);
                    kind = n.kind;
                }
            var u = new NodeType(-1L, BTree<long, object>.Empty + (Domain.Kind, kind) + (Domain.NodeTypes, ds));
            if (db.objects[db.types[u] ?? -1L] is NodeType nt)
                return nt;
            var nst = db.nextStmt;
            u = (NodeType)u.Relocate(nst);
            db += (Database.NextStmt, nst + 1);
            db += (Database.Types, db.types + (u, nst));
            return u;
        }
        internal NodeType? Find(CTree<Domain, bool> ts)
        {
            var kind = ts.First()?.key()?.kind ?? Qlx.NODETYPE;
            var u = new NodeType(-1L, BTree<long, object>.Empty + (Domain.Kind, kind) + (Domain.NodeTypes, ts));
            return db.objects[db.types[u] ?? -1L] as NodeType;
        }
        internal CTree<Domain, bool> NodeTypes(long dp)
        {
            var d = db.objects[dp] as NodeType ?? throw new PEException("PE50401");
            if (d.nodeTypes.Count > 1)
                return d.nodeTypes;
            return new CTree<Domain, bool>(d, true);
        }
        internal NodeType? FindNodeType(string nm,CTree<string,QlValue> dc)
        {
            if (nm != "")
                return db.objects[role.nodeTypes[nm]??-1L] as NodeType;
            var pn = CTree<string, bool>.Empty;
            for (var b = dc.First(); b != null; b = b.Next())
                pn += (b.key(), true);
            return db.objects[role.unlabelledNodeTypesInfo[pn] ?? -1L] as NodeType;
        }
        internal CTree<Domain,bool> FindEdgeType(string nm, long lt, long at, CTree<string, QlValue> dc,
            BTree<long,object> m, TMetadata md)
        {
            var r = CTree<Domain, bool>.Empty;
            if (nm != "" && role.edgeTypes[nm] is long el) 
            {
                EdgeType? et = null;
                if (db.objects[el] is EdgeType ee
                    && ee.leavingType == lt && ee.arrivingType == at)
                    et = ee;
                if (db.objects[el] is Domain eu && eu.kind == Qlx.UNION)
                    for (var c = eu.unionOf.First(); et is null && c != null; c = c.Next())
                        if (db.objects[c.key().defpos] is EdgeType ef
                            && ef.leavingType == lt && ef.arrivingType == at)
                            et = ef;
                return (et is null)?r : r+(et.Build(this,null,0L,m,md), true);
            }
            var pn = CTree<string, bool>.Empty;
            for (var b = dc.First(); b != null; b = b.Next())
                pn += (b.key(), true);
            if (db.objects[role.unlabelledEdgeTypesInfo[pn] ?? -1L] is EdgeType ut && ut.name==nm) 
                r +=(ut.Build(this,null,0L,m,md), true);
            return r;
        }
        internal string? NameFor(long p)
        {
            var ob = _Ob(p);
            if (ob is null)
                return null;
            return ob.infos[role.defpos]?.name ??
                (string?)ob.mem[DBObject._Alias] ??
                (string?)ob.mem[ObInfo.Name];
        }
        internal string? GName(long? p)
        {
            if (_Ob(p??-1L) is not QlValue sv)
                return null;
            if (sv.name is string s)
                return s;
            var tv = sv.Eval(this);
            if (tv is TGParam tp && binding[tp.uid] is TChar tb)
                return tb.value;
            if (tv is TChar tc)
                return tc.value;
            if (tv is TTypeSpec tt)
                return tt._dataType.name;
            return null;
        }
        internal TypedValue? GConstrain(long? p)
        {
            if (_Ob(p ?? -1L) is not QlValue sv)
                return null;
            var tv = sv.Eval(this);
            if (tv is TGParam tp)
                tv = binding[tp.uid];
            if (tv is TNull)
                return null;
            return tv;
        }
        internal NodeType? GType(long? p)
        {
            return (GName(p) is string s && role.dbobjects[s] is long sp) ? _Ob(sp) as NodeType : null;
        }
        internal Grant.Privilege Priv(long p)
        {
            return (_Ob(p) is DBObject ob && ob.infos[role.defpos] is ObInfo oi) ?
                oi.priv : Grant.Privilege.NoPrivilege;
        }
        /// <summary>
        /// If there is a handler for No Data signal, raise it
        /// </summary>
        internal virtual void NoData()
        {
            // no action
        }
        /// <summary>
        /// Type information for the SQL standard Diagnostics area: 
        /// see Tables 30 and 31 of the SQL standard 
        /// </summary>
        /// <param name="w">The diagnostic identifier</param>
        /// <returns></returns>
        public static Domain InformationItemType(Qlx w)
        {
            return w switch
            {
                Qlx.COMMAND_FUNCTION_CODE => Domain.Int,
                Qlx.DYNAMIC_FUNCTION_CODE => Domain.Int,
                Qlx.NUMBER => Domain.Int,
                Qlx.ROW_COUNT => Domain.Int,
                Qlx.TRANSACTION_ACTIVE => Domain.Int,// always 1
                Qlx.TRANSACTIONS_COMMITTED => Domain.Int,
                Qlx.TRANSACTIONS_ROLLED_BACK => Domain.Int,
                Qlx.CONDITION_NUMBER => Domain.Int,
                Qlx.MESSAGE_LENGTH => Domain.Int,//derived from MESSAGE_TEXT 
                Qlx.MESSAGE_OCTET_LENGTH => Domain.Int,// derived from MESSAGE_OCTET_LENGTH
                Qlx.PARAMETER_ORDINAL_POSITION => Domain.Int,
                _ => Domain.Char,
            };
        }
        internal void CheckMetadata(long p,TypedValue v)
        {
            if (_Ob(p) is DBObject ob && ob.infos[role.defpos] is ObInfo oi)
                for (var b=oi.metadata.First();b is not null;b=b.Next())
                    switch (b.key())
                    {
                        case Qlx.MIN:
                            {
                                if (b.value().ToInt() > v.Cardinality())
                                    throw new DBException("21000");
                                break;
                            }
                        case Qlx.MAX:
                            {
                                if (b.value().ToInt() < v.Cardinality())
                                    throw new DBException("21000");
                                break;
                            }
                    }
        }
        internal void CheckMultiplicity(long p, TypedValue v)
        {
            if (_Ob(p) is DBObject ob && ob.infos[role.defpos] is ObInfo oi)
                for (var b = oi.metadata.First(); b != null; b = b.Next())
                    switch (b.key())
                    {
                        case Qlx.MINVALUE:
                            {
                                if (b.value().ToInt() > v.Cardinality())
                                    throw new DBException("21000");
                                break;
                            }
                        case Qlx.MAXVALUE:
                            {
                                if (b.value().ToInt() < v.Cardinality())
                                    throw new DBException("21000");
                                break;
                            }
                    }
        }
        internal Procedure? GetProcedure(long lp,string n,CList<Domain> a)
        {
            var proc = db.GetProcedure(n, a);
            if (proc == null)
                return null;
            if (obs[proc.defpos] is Procedure p)
                return p;
            var pi = (Procedure)proc.Instance(lp,this);
            Add(pi);
            return pi;
        }
        internal Activation GetActivation()
        {
            for (var c = this; c != null; c = c.next)
                if (c is Activation ac)
                    return ac;
            return new Activation(this,"");
        }
        internal DBObject? GetObject(string n)
        {
            return db.GetObject(n,role);
        }
        internal virtual Context SlideDown()
        {
            if (next == null)
                return this;
            next.values += values;
            next.warnings += warnings;
            next.deferred += deferred;
            next.val = val;
            next.nextHeap = nextHeap;
            next.lastret = lastret;
            if (db != next.db)
            {
                var nd = db;
                if (db.role != next.db.role)
                    nd += (Database.Role, next.db.role);
                next.db = nd; // adopt the transaction changes done by this
            }
            return next;
        }
        internal void DoneCompileParse(Context cx)
        {
            if (cx != this)
            {
                for (var b = cx.obs.First(); b != null; b = b.Next())
                    _Add(b.value());
                db = cx.db;
            }
        }
        /// <summary>
        /// Used when changes have been made to a newly-created TableColumn
        /// </summary>
        /// <param name="pc"></param>
        /// <param name="tc"></param>
        /// <param name="nst"></param>
        /// <returns></returns>
        internal TableColumn Modify(PColumn3 pc, TableColumn tc)
        {
            var tr = (Transaction)db;
            db += (Transaction.Physicals, tr.physicals + (pc.ppos, pc));
            db += (tc.defpos, tc);
            return tc;
        }
        /// <summary>
        /// Used when changes have been made to a newly-created Table
        /// </summary>
        /// <param name="tb"></param>
        /// <param name="nst"></param>
        /// <returns></returns>
        internal Table Modify(Table tb, long nst)
        {
            var fr = new Framing(this, nst);
            tb += (DBObject._Framing, tb.framing + (Framing.Obs, tb.framing.obs + fr.obs));
            db += (tb.defpos, tb);
            Add(tb);
            return tb;
        }
        /// <summary>
        /// Used when changes have been made to a newly-created UDType.
        /// Cascade.
        /// </summary>
        /// <param name="ut"></param>
        /// <returns></returns>
        internal UDType? Modify(UDType? ut)
        {
            if (ut == null)
                return null;
            var tr = (Transaction)db;
            var m = ut.mem;
            if (tr.physicals[ut.defpos] is PType pt)
            {
                var ch = false;
                var r = CTree<Domain, bool>.Empty;
                for (var b = ut.super.First(); b != null; b = b.Next())
                    if (b.key() is UDType su && Modify(su) is Domain ns)
                    {
                        r += (ns, true);
                        ch = ch || (su != ns && ns != null);
                    }
                if (ch)
                    m += (Domain.Under, r);
                if (obs[pt.defpos] is UDType at)
                {
                    var rs = at.representation;
                    var am = at.mem;
                    for (var b = rs.First(); b != null; b = b.Next())
                    {
                        var nd = Modify(b.value() as UDType);
                        if (b.value() != nd && nd != null)
                            rs += (nd.defpos, nd);
                    }
                    if (rs != at.representation)
                        am += (Domain.Representation, rs);
                    if (at.defpos == ut.defpos)
                        am += m;
                    if (am != at.mem)
                        at = (UDType)Add(new UDType(at.defpos, am));
                    if (at.defpos == ut.defpos)
                        ut = at;
                }
            }
            return ut;
        }
        internal Domain GroupCols(CList<long> gs, Domain dm)
        {
            var gc = CTree<long, Domain>.Empty;
            if (dm is RowSet rs)
                for (var b = gs.First(); b != null; b = b.Next())
                    if (b.value() is long p && rs != null)
                    {
                        if (obs[p] is QlValue v && v.KnownBy(this, rs))
                            gc += (p, _Dom(p));
                        if (obs[p] is Grouping gg)
                            for (var c = gg.keys.First(); c != null; c = c.Next())
                                if (c.value() is long cp && _Ob(cp) is QlValue ce)
                                {
                                    if (dm.representation.Contains(cp) ||
                                        (rs != null && ce.KnownBy(this, rs)))
                                        gc += (cp, ce.domain);
                                    for (var d = rs?.matching.PositionAt(cp); d != null; d = d.Next())
                                        for (var e = d.value().First(); e != null; e = e.Next())
                                            if (dm.representation.Contains(e.key()))
                                                gc += (cp, ce.domain);
                                }
                    }
            var rt = CList<long>.Empty;
            for (var b = gc.First(); b != null; b = b.Next())
                rt += b.key();
            return (rt.Length==0)?Domain.Row:new Domain(-1L, this, Qlx.ROW, gc, rt);
        }
        // debugging
        public override string ToString()
        {
            return GetType().Name + " "+ cxid;
        }
        internal virtual Context FindCx(long c)
        {
            return this;
        }
        internal virtual TriggerActivation FindTriggerActivation(long tabledefpos)
        {
            return next?.FindTriggerActivation(tabledefpos)
                ?? throw new PEException("PE600");
        }
        internal Ident? FixI(Ident? id)
        {
            if (id == null)
                return null;
            return new Ident(id.ident, Fix(id.uid), FixI(id.sub));
        }
        internal CList<long> FixLl(CList<long> ord)
        {
            var r = CList<long>.Empty;
            var ch = false;
            for (var b = ord.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var f = Fix(p);
                    if (f <= 0)
                        continue;
                    if (p != f)
                        ch = true;
                    r += f;
                }
            return ch ? r : ord;
        }
        internal CList<CList<long>> FixLLl(CList<CList<long>> stms)
        {
            var r = CList<CList<long>>.Empty;
            var ch = false;
            for (var b = stms.First(); b != null; b = b.Next())
                if (b.value() is CList<long> rr)
                {
                    var f = FixLl(rr);
                    if (rr != f)
                        ch = true;
                    r += f;
                }
            return ch ? r : stms;
        }
        internal BList<Domain> FixBD(BList<Domain> ds)
        {
            var r = BList<Domain>.Empty;
            var ch = false;
            for (var b = ds.First(); b != null; b = b.Next())
                if (b.value() is Domain d)
                {
                    var nd = (Domain)d.Fix(this);
                    if (d != nd)
                        ch = true;
                    r += nd;
                }
            return ch ? r : ds;
        }
        internal BList<Grouping> FixBG(BList<Grouping> gs)
        {
            var r = BList<Grouping>.Empty;
            var ch = false;
            for (var b = gs.First(); b != null; b = b.Next())
                if (b.value() is Grouping og)
                {
                    var g = (Grouping)og.Fix(this);
                    ch = ch || g != b.value();
                    r += g;
                }
            return ch ? r : gs;
        }
        internal CTree<UpdateAssignment,bool> FixTub(CTree<UpdateAssignment,bool> us)
        {
            var r = CTree<UpdateAssignment,bool>.Empty;
            var ch = false;
            for (var b = us.First(); b != null; b = b.Next())
            {
                var u = (UpdateAssignment)b.key().Fix(this);
                ch = ch || u != b.key();
                r += (u,true);
            }
            return ch ? r : us;
        }
        internal BList<TypedValue> FixBV(BList<TypedValue> key)
        {
            var r = BList<TypedValue>.Empty;
            var ch = false;
            for (var b = key.First(); b != null; b = b.Next())
                if (b.value() is TypedValue p)
                {
                    var f = p.Fix(this);
                    if (p != f)
                        ch = true;
                    r += f;
                }
            return ch ? r : key;
        }
        internal CTree<long,V> Fix<V>(CTree<long,V> ms) where V:IComparable
        {
            var r = CTree<long, V>.Empty;
            var ch = false;
            for (var b=ms.First();b is not null;b=b.Next())
            {
                var k = b.key();
                var m = Fix(k);
                ch = ch || m != k;
                r += (m, b.value());
            }
            return ch ? r : ms;
        }
        internal BTree<K,long?> Fix<K>(BTree<K, long?> ms) where K:IComparable
        {
            var r = BTree<K,long?>.Empty;
            var ch = false;
            for (var b = ms.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var m = Fix(p);
                    ch = ch || m != p;
                    r += (b.key(), m);
                }
            return ch ? r : ms;
        }
        internal CTree<long,bool> FixTlb(CTree<long,bool> fi)
        {
            var r = CTree<long,bool>.Empty;
            var ch = false;
            for (var b = fi.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                if (p != b.key())
                    ch = true;
                r += (p,true);
            }
            return ch ? r : fi;
        }
        internal BTree<long,ObInfo> Fix(BTree<long,ObInfo> oi)
        {
            var r = BTree<long, ObInfo>.Empty;
            var ch = false;
            for (var b=oi.First();b is not null;b=b.Next())
            {
                var k = b.key();
                var ns = (ObInfo)b.value().Fix(this);
               var nk = Fix(k);
                if (k != nk || ns!=b.value())
                    ch = true;
                r += (nk, ns);
            }
            return ch? r: oi;
        }
        internal CTree<long, TypedValue> FixTlV(CTree<long, TypedValue> vt)
        {
            var r = CTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = vt.First(); b != null; b = b.Next())
                if (b.value() is TypedValue ov)
                {
                    var k = b.key();
                    var p = Fix(k);
                    var v = ov.Fix(this);
                    if (p != b.key() || v != b.value())
                        ch = true;
                    r += (p, v);
                }
            return ch ? r : vt;
        }
        internal CTree<string,TypedValue> FixTsV(CTree<string, TypedValue> a)
        {
            var r = CTree<string, TypedValue>.Empty;
            var ch = false;
            for (var b = a.First(); b != null; b = b.Next())
                if (b.value() is TypedValue ov)
                {
                    var p = b.key();
                    var v = ov.Fix(this);
                    ch = ch || v != b.value();
                    r += (p, v);
                }
            return ch?r:a;
        }
        internal CTree<int, TypedValue> FixTiV(CTree<int, TypedValue> a)
        {
            var r = CTree<int, TypedValue>.Empty;
            var ch = false;
            for (var b = a.First(); b != null; b = b.Next())
                if (b.value() is TypedValue ov)
                {
                    var p = b.key();
                    var v = ov.Fix(this);
                    ch = ch || v != b.value();
                    r += (p, v);
                }
            return ch ? r : a;
        }
        internal BTree<TypedValue,long?> FixTVl(BTree<TypedValue,long?> mu)
        {
            var r = BTree<TypedValue,long?>.Empty;
            var ch = false;
            for (var b = mu.First(); b != null; b = b.Next())
            if (b.value() is long v){
                var p = b.key().Fix(this);
                var q = uids[v]??-1L;
                ch = ch || p != b.key() || q != b.value();
                r += (p, q);
            }
            return ch? r : mu;
        }
        internal CTree<TypedValue, bool> FixTVb(CTree<TypedValue, bool> mu)
        {
            var r = CTree<TypedValue,bool>.Empty;
            var ch = false;
            for (var b = mu.First(); b != null; b = b.Next())
                {
                    var p = b.key().Fix(this);
                    ch = ch || p != b.key();
                    r += (p, true);
                }
            return ch ? r : mu;
        }
        internal Names FixTsl(Names cs)
        {
            var r = Names.Empty;
            for (var b = cs.First(); b != null; b = b.Next())
            if (b.value() is long p)
                r += (b.key(), Fix(p));
            return r;
        }
        internal BList<(long,long)> FixLll(BList<(long,long)> cs)
        {
            var r = BList<(long,long)>.Empty;
            for (var b = cs.First();b is not null; b=b.Next())
            {
                var (w, x) = b.value();
                r += (Fix(w), Fix(x));
            }
            return r;
        }
        internal CTree<long,CTree<long,bool>> FixTlTlb(CTree<long, CTree<long,bool>> ma)
        {
            var r = CTree<long, CTree<long,bool>>.Empty;
            var ch = false;
            for (var b = ma.First(); b != null; b = b.Next())
                if (b.value() is CTree<long, bool> x)
                {
                    var k = b.key();
                    var p = Fix(k);
                    var v = FixTlb(x);
                    ch = ch || p != k || v != b.value();
                    r += (p, v);
                }
            return ch ? r : ma;
        }
        internal CTree<Domain, bool> FixTDb(CTree<Domain, bool> su)
        {
            var r = CTree<Domain, bool>.Empty;
            var ch = false;
            for (var b = su.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var d = db.objects[k.defpos] as Domain??k;
                var nk = (Domain)k.Fix(this);
                ch = ch || (nk != k);
                r += (nk, true);
            }
            return ch ? r : su;
        }
        internal CTree<Domain,CTree<long,bool>> FixTDTlb(CTree<Domain,CTree<long,bool>> xs)
        {
            var r = CTree<Domain, CTree<long, bool>>.Empty;
            var ch = false;
            for (var b = xs.First(); b != null; b = b.Next())
                if (b.value() is CTree<long, bool> x)
                {
                    var k = b.key();
                    var nk = (Domain?)k?.Fix(this);
                    var v = FixTlb(x);
                    ch = ch || (nk != k || v != b.value());
                    if (nk is not null)
                        r += (nk, v);
                }
            return ch ? r : xs;
        }
        internal CTree<long,Domain> FixTlD(CTree<long,Domain> rs)
        {
            var r = CTree<long, Domain>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
                if (b.value() is Domain od)
                {
                    var k = b.key();
                    var p = Fix(k);
                    if (p < 0)
                        continue;
                    var d = (Domain)od.Fix(this);
                    ch = ch || p != k || d != b.value();
                    r += (p, d);
                }
            return ch?r:rs;
        }
        internal CTree<long, CTree<Domain, Domain>> FixTlTDD(CTree<long, CTree<Domain,Domain>> rs)
        {
            var r = CTree<long, CTree<Domain,Domain>>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var s = CTree<Domain, Domain>.Empty;
                for (var c=b.value().First();c is not null;c=c.Next())
                {
                    var p = c.key();
                    var np = (Domain)p.Fix(this);
                    var d = c.value();
                    var nd = (Domain)d.Fix(this);
                    ch = ch || p != np || d != nd;
                    s += (np, nd);
                }
                var k = b.key();
                var nk = Fix(k);
                ch = ch || k != nk;
                r += (nk, s);
            }
            return ch ? r : rs;
        }
        internal CTree<long, CTree<long, CTree<long, bool>>> FixTlTlTlb(CTree<long, CTree<long, CTree<long, bool>>> es)
        {
            var r = CTree<long, CTree<long, CTree<long, bool>>>.Empty;
            var ch = false;
            for (var b = es.First(); b != null; b = b.Next())
                if (b.value() is CTree<long, CTree<long, bool>> bt)
                {
                    var bk = Fix(b.key());
                    var bv = FixTlTlb(bt);
                    ch = ch || bk != b.key() || bv != bt;
                    r += (bk, r[bk]??CTree<long,CTree<long,bool>>.Empty + bv);
                }
            return ch ? r : es;
        }
        internal BTree<long, long?> FixTll(BTree<long, long?> rs)
        {
            var r = BTree<long, long?>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
                if (b.value() is long v)
                {
                    var k = b.key();
                    var p = Fix(k);
                    var d = Fix(v);
                    ch = ch || p != k || d != v;
                    r += (p, d);
                }
            return ch ? r : rs;
        } 
        internal BTree<long, (long, long)> FixBlll(BTree<long, (long, long)> ds)
        {
            var r = BTree<long, (long, long)>.Empty;
            var ch = false;
            for (var b = ds.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = Fix(k);
                var (v, p) = b.value();
                var nv = Fix(v);
                var np = Fix(p);
                ch = ch || nk != k || nv != v || np != p;
                r += (k, (nv, np));
            }
            return ch ? r : ds;
        }
        internal BTree<long, long?> FixV(BTree<long, long?> rs)
        {
            var r = BTree<long, long?>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
                if (b.value() is long v)
                {
                    var k = b.key();
                    var d = Fix(v);
                    ch = ch || d != v;
                    r += (k, d);
                }
            return ch ? r : rs;
        }
        internal CTree<PTrigger.TrigType, CTree<long, bool>> FixTTElb(CTree<PTrigger.TrigType, CTree<long, bool>> t)
        {
            var r = CTree<PTrigger.TrigType, CTree<long, bool>>.Empty;
            for (var b = t.First(); b != null; b = b.Next())
                if (b.value() is CTree<long, bool> x)
                {
                    var p = b.key();
                    r += (p, FixTlb(x));
                }
            return r;
        }
        internal BTree<long,TableRow> FixTlR(BTree<long,TableRow> t) 
        {
            for (var b=t.First();b!=null;b=b.Next())
            {
                var k = b.key();
                var nk = Fix(k);
                if (k != nk)
                    t -= k;
                var r = b.value();
                var nr = r.Fix(this);
                if (nk>0 && !r.Equals(nr))
                    t += (nk, nr);
            }
            return t;
        }
        internal TypedValue? Node(Domain? dm, long lI)
        {
            dm = db.objects[dm?.defpos ?? -1L] as Domain;
            if (dm is EdgeType et && et.tableRows[lI] is TableRow tr)
                return et.Node(this, tr);
            if (dm is NodeType nt && nt.tableRows[lI] is TableRow tn)
                return nt.Node(this, tn);
            return TNull.Value;
        }
    }

    internal class Connection
    {
        // uid range for prepared statements is HeapStart=0x7000000000000000-0x7fffffffffffffff
        long _nextPrep = Transaction.HeapStart;
        internal long nextPrep => _nextPrep;
        internal readonly TCPStream? _tcp = null;
        internal readonly bool caseSensitive = false;
        internal bool refIdsToPos = false;
        internal DateTime awake = DateTime.Now;
        internal readonly byte[] awakeBuf = [0, 1, (byte)Responses.Continue]; 
        /// <summary>
        /// A tree of prepared statements, whose object persist
        /// in the base context for the connection.
        /// </summary>
        BTree<string, PreparedStatement> _prepared = BTree<string, PreparedStatement>.Empty;
        /// <summary>
        ///  A tree of prepared edge-interventions, to replace edge e with source type s
        ///  and destination type d with edge type n
        /// </summary>
        internal CTree<string, CTree<string, CTree<string, string>>> edgeTypes =
             CTree<string, CTree<string, CTree<string, string>>>.Empty;
        internal BTree<string, PreparedStatement> prepared => _prepared;
        /// <summary>
        /// Connection string details
        /// </summary>
        internal BTree<string, string> props = BTree<string, string>.Empty;
        public Connection(BTree<string,string>? cs=null)
        {
            if (cs != null)
                props = cs;
            if (props["CaseSensitive"] == "true")
                caseSensitive = true;
        }
        internal Connection(TCPStream tcp,BTree<string,string> cs)
        {
            _tcp = tcp;
            props = cs;
            if (props["CaseSensitive"] == "true")
                caseSensitive = true;
        }
        internal void Awake()
        {
            if (DateTime.Now - awake >TimeSpan.FromSeconds(20))
            {
                _tcp?.SendAwake();
                awake = DateTime.Now;
            }
        }
        public void Add(string nm,PreparedStatement ps)
        {
            _prepared += (nm, ps);
            if (ps.framing.obs.Last() is ABookmark<long,DBObject> b)
                _nextPrep = b.key();
        }
        public void Add(string nm, string n)
        {
            var i = nm.IndexOf('(');
            var e = nm[..i]; // a (conflicting) edge type nmame
            var j = nm.IndexOf(',');
            var s = nm.Substring(i + 1, j - i - 1); // its source node type
            var d = nm.Substring(j + 1, s.Length - j - 2); // its dest node type
            var t = edgeTypes[e] ?? CTree<string, CTree<string, string>>.Empty;
            var u = t[s] ?? CTree<string, string>.Empty;
            u += (d, n);  // rename it to n
            t +=  (s, u);
            edgeTypes += (e, t);
        }
    }
    /// <summary>
    /// The Framing of an DBObject contains executable-range objects only,
    /// which were created by the Compiled classes on load or during parsing.
    /// </summary>
    internal class Framing : Basis // for compiled code
    {
        internal const long
            Obs = -449;     // ObTree 
        public ObTree obs => 
            (ObTree?)mem[Obs]??ObTree.Empty;
        public Domain valueType => (Domain)(mem[Executable.ValueType] ?? Domain.Null);
        public Rvv? withRvv => (Rvv?)mem[Rvv.RVV];
        internal static Framing Empty = new();
        Framing() { }
        Framing(BTree<long,object> m) :base(m) 
        { }
        internal Framing(Context cx,long nst) : base(_Mem(cx,nst))
        { }
        static BTree<long, object> _Mem(Context cx,long nst)
        {
            var r = BTree<long, object>.Empty;
            if (cx.result is RowSet rs)
                r += (Executable.ValueType, rs);
            else if (cx.val is TypedValue rv && rv!=TNull.Value)
                r += (Executable.ValueType, rv.dataType);
            if (cx.affected is not null)
                 r+= (Rvv.RVV, cx.affected);
            var os = ObTree.Empty;
            for (var b = cx.obs.PositionAt(Transaction.Executables); b != null; b = b.Next())
            {
                var k = b.key();
                if (cx.parse!=ExecuteStatus.Prepare &&
                    (k < nst || k >= Transaction.HeapStart)) // we only want new executables
                    continue;
                if (b.value() is DBObject v)
                    os += (k, v);
            }
            return r + (Obs, os);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Framing(m);
        }
        public static Framing operator+(Framing f,(long,object)x)
        {
            var (dp, ob) = x;
            if (f.mem[dp] == ob)
                return f;
            return new Framing(f.mem + x);
        }
        public static Framing operator+(Framing f,DBObject ob)
        {
            var p = ob.defpos;
            if (p < Transaction.TransPos)
                return f;
            return f + (Obs, f.obs + (p, ob));
        }
        public static Framing operator+(Framing f,Framing nf)
        {
            return f + (Obs, f.obs + nf.obs);
        }
        public static Framing operator-(Framing f,long p)
        {
            return f + (Obs, f.obs - p);
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            var r = base._Fix(cx, mem);
            var os = obs;
            for (var b = obs.First(); b != null; b = b.Next())
                if (b.value() is DBObject ob)
                {
                    var k = b.key();
                    var nk = cx.Fix(k);
                    var nb = (DBObject)ob.Fix(cx);
                    if (k != nk)
                    {
                        nb = nb.Relocate(nk);
                        if (k < Transaction.Executables || k >= Transaction.HeapStart) // don't remove virtual columns
                            os -= k;  // or RestView
                                     //         cx.Remove(ob); typically ob will not be in cx
                    }
                    if (nb != ob)
                    {
                        os += (ob.defpos,nb);
                        cx.Add(nb);
                    }
                }
            r += (Obs, os);
            r += (Executable.ValueType, (Domain)valueType.Fix(cx));
            return r; 
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = " (";
            for (var b = obs.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.key())); 
                sb.Append(' '); sb.Append(b.value());
            }
            sb.Append(')');
            if (valueType.kind!=Qlx.Null)
            {
                sb.Append(" DataType ");sb.Append(valueType);
            }
            return sb.ToString();
        }
    }
    internal class Register
    {
        /// <summary>
        /// The current partition
        /// </summary>
        internal RTree? wtree = null;
        /// <summary>
        /// The bookmark for the current row
        /// </summary>
        internal Cursor? wrb = null;
        /// the valueType of COUNT
        /// </summary>
        internal long count = 0L;
        /// <summary>
        /// The row number
        /// </summary>
        internal long row = -1L;
        /// <summary>
        /// the results of MAX, MIN, FIRST, LAST, ARRAY, RESTRICT
        /// </summary>
        internal TypedValue? acc;
        /// <summary>
        /// the results of XMLAGG
        /// </summary>
        internal StringBuilder? sb = null;
        /// <summary>
        ///  the sort of sum/max/min we have
        /// </summary>
        internal Domain sumType = Domain.Null;
        /// <summary>
        ///  the sum of long
        /// </summary>
        internal long sumLong = 0L;
        /// <summary>
        /// the sum of INTEGER
        /// </summary>
        internal Integer? sumInteger = null;
        /// <summary>
        /// the sum of double
        /// </summary>
        internal double sum1, acc1;
        /// <summary>
        /// the sum of Decimal
        /// </summary>
        internal Numeric? sumDecimal = null;
        /// <summary>
        /// the boolean valueType so far
        /// </summary>
        internal bool? bval = null;
        /// <summary>
        /// a multiset for accumulating things
        /// </summary>
        internal TMultiset? mset = null;
        internal Register(Context cx,TRow key,SqlFunction sf)
        {
            var oc = cx.values;
            var dp = sf.window;
            if (dp >=0L && cx.obs[dp] is WindowSpecification ws)
            {
                var t1 = cx.funcs[sf.from] ?? BTree<TRow, BTree<long, Register>>.Empty;
                var t2 = t1[key] ?? BTree<long, Register>.Empty;
                t2 += (sf.defpos, this);
                t1 += (key, t2);
                cx.funcs += (sf.from, t1);  // prevent stack oflow
                var dm = (ws.order!=Domain.Row)?ws.order:ws.partition;
                if (dm != null && cx.obs[sf.from] is RowSet fm
                    && cx.obs[fm.source] is RowSet sce)
                {
                    wtree = new RTree(dp, cx, dm,
                       TreeBehaviour.Allow, TreeBehaviour.Allow);
                    for (var e = sce.First(cx); e != null; e = e.Next(cx))
                    {
                        var vs = CTree<long, TypedValue>.Empty;
                        cx.cursors += (dp, e);
                        for (var b = dm.rowType.First(); b != null; b = b.Next())
                            if (b.value() is long p && cx.obs[p] is QlValue s)
                                vs += (s.defpos, s.Eval(cx));
                        var rw = new TRow(dm, vs);
                        RTree.Add(ref wtree, rw, cx.cursors);
                    }
                }
            }
            cx.values = oc;
        }
        public override string ToString()
        {
            var s = new StringBuilder("{");
            if (count != 0L) { s.Append(count); s.Append(' '); }
            if (sb != null) { s.Append(sb); s.Append(' '); }
            if (row>=0) { s.Append(row); s.Append(' '); }
            switch (sumType.kind)
            {
                case Qlx.COLLECT:
                case Qlx.EVERY:
                case Qlx.FUSION:
                case Qlx.INTERSECTION:
                    if (mset is not null) s.Append(mset);
                    break;
                case Qlx.INT:
                case Qlx.INTEGER:
                    if (sumInteger is Integer i)
                        s.Append(i.ToString());
                    else
                        s.Append(sumLong);
                    break;
                case Qlx.REAL:
                    s.Append(sum1); break;
                case Qlx.NUMERIC:
                    if (sumDecimal is not null)
                        s.Append(sumDecimal); 
                    break;
                case Qlx.BOOLEAN:
                    s.Append(bval); break;
                case Qlx.MAX:
                case Qlx.MIN:
                case Qlx.RESTRICT:
                    s.Append(acc); break;
                case Qlx.STDDEV_POP:
                case Qlx.STDDEV_SAMP:
                    s.Append(sum1); s.Append(' ');
                    s.Append(acc1); 
                    break;
            }
            s.Append('}');
            return s.ToString();
        }
    }
    /// <summary>
    /// A period specification occurs in a table reference: AS OF/BETWEEN/FROM
    /// </summary>
    internal class PeriodSpec
    {
        /// <summary>
        /// the name of the period
        /// </summary>
        public readonly string periodname;
        /// <summary>
        /// AS, BETWEEN, SYMMETRIC, FROM
        /// </summary>
        public readonly Qlx kind = Qlx.NO;  
        /// <summary>
        /// The first point in time specified
        /// </summary>
        public readonly QlValue? time1 = null;
        /// <summary>
        /// The second point in time specified
        /// </summary>
        public readonly QlValue? time2 = null;
        internal PeriodSpec(string n,Qlx k,QlValue? t1,QlValue? t2)
        {
            periodname = n; kind = k; time1 = t1; time2 = t2;
        }
    }
    /// <summary>
    /// No transaction history here
    /// </summary>
    internal class ETag :Basis 
    {
        internal const long
            AssertMatch = -453,  // string Rvv for If-Match
            HighWaterMark = -463, // DB length for If-Match W/
            AssertUnmodifiedSince = -451; // HttpDate
        internal long highWaterMark => (long)(mem[HighWaterMark] ?? -1L); // Database length
        internal THttpDate? assertUnmodifiedSince => (THttpDate?)mem[AssertUnmodifiedSince];
        internal Rvv assertMatch => (Rvv?)mem[AssertMatch]??Rvv.Empty;
        internal static ETag Empty = new(BTree<long, object>.Empty); 
        public ETag(Database d,Rvv rv) :base(_Mem(d,rv))
        { }
        static BTree<long,object> _Mem(Database d,Rvv rv)
        {
            var r = BTree<long, object>.Empty +  (AssertMatch, rv) + (HighWaterMark, d.length);
            if (d.lastModified is DateTime dt)
                r += (AssertUnmodifiedSince, new THttpDate(dt));
            return r;
        }
        protected ETag(BTree<long, object> m) : base(m) { }
        public static ETag operator+ (ETag e,(long,object) x)
        {
            var (dp, ob) = x;
            if (e.mem[dp] == ob)
                return e;
            return new ETag(e.mem + x);
        }
        public static ETag operator-(ETag e,long p)
        {
            return new ETag(e.mem - p);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ETag(m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(assertMatch); sb.Append('@');
            sb.Append(highWaterMark); sb.Append('=');
            sb.Append(assertUnmodifiedSince);
            return sb.ToString();
        }
    }
}
