using System.Net;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level1;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

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
    /// Ideally Context should be immutable (a subclass of Basis), but we do not do this because SqlValue::Eval can affect
    /// the current ReadConstraint and ETag, and these changes must be placed somewhere. The Context is the simplest.
    /// 
    /// So Context is mutable: but places we have the separate Contexts are important. 
    ///     1. During the UnLex relocations, with _Relocate(cx,nc) nc is the new context containing the Unlexed objects
    ///     2, The first step in Commit is to create Reader and a Writer based on the Database and not the Transaction
    ///     Then wr.cx.db is extended by the committed objects from the Transaction (see Physical.Commit)
    ///     3. During Parsing, the parsing context contains the evolving Transaction (i.e. cx.db is tr)
    ///     4. During Execution we can have temporary Contexts/Activations for doing aggregations, triggers,
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
        public BTree<long, Cursor> cursors = BTree<long, Cursor>.Empty;
        internal CTree<long, bool> restRowSets = CTree<long, bool>.Empty;
        public long nextHeap = -1L, parseStart = -1L;
        public TypedValue val = TNull.Value;
        internal Database db = Database.Empty;
        internal Connection conn;
        internal string? url = null;
        internal Transaction? tr => db as Transaction;
        internal BList<TriggerActivation> deferred = BList<TriggerActivation>.Empty;
        internal BList<Exception> warnings = BList<Exception>.Empty;
        internal ObTree obs = ObTree.Empty;
        // these five fields help with Fix(dp) during instancing (for Views)
        internal long instDFirst = -1L; // first uid for instance dest
        internal long instSFirst = -1L; // first uid in framing
        internal long instSLast = -1L; // last uid in framing
        internal BTree<int, ObTree> depths = BTree<int, ObTree>.Empty;
        internal CTree<long, TypedValue> values = CTree<long, TypedValue>.Empty;
        internal BTree<long, long?> instances = BTree<long, long?>.Empty;
        internal CTree<long, CTree<long, bool>> forReview = CTree<long, CTree<long, bool>>.Empty; // SqlValue,RowSet
        internal BTree<long, BList<BTree<long, object>>> forApply = BTree<long, BList<BTree<long, object>>>.Empty;// RowSet, props
        internal bool inHttpService = false;
        internal CTree<long, Domain> groupCols = CTree<long, Domain>.Empty; // Domain; GroupCols for a Domain with Aggs
        internal BTree<long, BTree<TRow, BTree<long, Register>>> funcs = BTree<long, BTree<TRow, BTree<long, Register>>>.Empty; // Agg GroupCols
        internal BTree<long, BTree<long, TableRow>> newTables = BTree<long, BTree<long, TableRow>>.Empty;
        internal BTree<Domain, long?> newTypes = BTree<Domain, long?>.Empty; // uncommitted types
        internal CTree<long, TGParam> nodes = CTree<long, TGParam>.Empty; 
        internal CTree<long, TList> paths = CTree<long, TList>.Empty; // of trails by SqlPath
        internal TList trail = new TList(Domain.NodeType); // of nodes in current trail
        /// <summary>
        /// Left-to-right accumulation of definitions during a parse: accessed only by RowSet
        /// </summary>
        internal Ident.Idents defs = Ident.Idents.Empty;
        internal BTree<int, (long, Ident.Idents)> defsStore = BTree<int, (long, Ident.Idents)>.Empty; // saved defs for previous level
        internal int sD => (int)defsStore.Count; // see IncSD() and DecSD() below
        internal long offset = 0L; // set in Framing._Relocate, constant during relocation of compiled objects
        internal long lexical = 0L; // current select block, set in incSD()
        internal CTree<long, int> undefined = CTree<long, int>.Empty;
        // UnHeap things for Procedure, Trigger, and Constraint bodies
        internal BTree<long, long?> uids = BTree<long, long?>.Empty;
        internal long result = -1L;
        /// <summary>
        /// Used in Replace cascade
        /// </summary>
        internal ObTree done = ObTree.Empty;
        internal BList<(DBObject, DBObject)> todo = BList<(DBObject, DBObject)>.Empty;
        internal static BTree<long, Func<object, object>> fixer = BTree<long, Func<object, object>>.Empty;
        internal bool replacing = false;
        /// <summary>
        /// Used for prepared statements
        /// </summary>
        internal BList<long?> qParams = BList<long?>.Empty;
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
        internal CTree<long, TypedValue> binding = CTree<long, TypedValue>.Empty; // bound TGParams
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
            instances = cx.instances;
            defsStore = cx.defsStore;
            obs = cx.obs;
            defs = cx.defs;
            depths = cx.depths;
            obs = cx.obs;
            cursors = cx.cursors;
            val = cx.val;
            parent = cx.parent; // for triggers
            rdC = cx.rdC;
            nid = cx.nid;
            restRowSets = cx.restRowSets;
            groupCols = cx.groupCols;
            inHttpService = cx.inHttpService;
            // and maybe some more?
        }
        internal Context(Context c, Role r, User u) : this(c)
        {
            if (db != null)
                db = db + (Database.Role, r) + (Database.User, u);
        }
        /// <summary>
        /// This Lookup is from the ParseVarOrColumn above.
        /// If the selectdepth has changed it is unlikely to be the right identification,
        /// so leave it to resolved later.
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
                    if (ix.sd < sD && ob.GetType().Name == "SqlValue") // an undefined identifier from a lower level
                        return (null, n);
                    (r,s) = ob._Lookup(lp, this, n.ident, sub, rr);
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
        internal Iix Ix(long u)
        {
            return new Iix(u, this, u);
        }
        internal Iix Ix(long l, long d)
        {
            return new Iix(l, this, d);
        }
        internal CTree<long, bool> Needs(CTree<long, bool> nd, BList<long?> rt)
        {
            for (var b = rt?.First(); b != null; b = b.Next())
                nd += obs[b.value() ?? -1L]?.Needs(this) ?? CTree<long, bool>.Empty;
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
            if (ic.Length > 0 && defs.Contains(ic.ToString())
                    && obs[defs[ic.ToString()]?[ic.iix.sd].Item1.dp ?? -1L] is SqlValue s0)
                v = s0;
            else if (defs.Contains(ic.ident))
                v = obs[defs[ic.ident]?[ic.iix.sd].Item1.dp ?? -1L] as Domain;
            if (v != null)
            {
                if (v.domain is Domain dv && !xp.CanTakeValueOf(dv))
                    throw new DBException("42000", ic);
                if (v.defpos >= Transaction.HeapStart)
                    for (var f = obs[v.from] as RowSet; f != null;)
                    {
                        for (var b = f.matching[v.defpos]?.First(); b != null; b = b.Next())
                            if (obs[b.key()] is SqlValue mv && mv.defpos < Transaction.HeapStart)
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
            rq.Headers.Add("If-Match", "\"" + etag + "\"");
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
                    {
                        var r = db.nextStmt;
                        db += (Database.NextStmt, r + 1);
                        return r;
                    }
                default: return db.nextPos;
            }
        }
        internal Iix GetIid()
        {
            var u = GetUid();
            return Ix(u);
        }
        internal long GetUid()
        {
            if (db == null)
                return -1L;
            switch (parse)
            {
                case ExecuteStatus.Parse:
                case ExecuteStatus.Compile:
                    {
                        var r = db.nextStmt;
                        db += (Database.NextStmt, r + 1);
                        return r;
                    }
                default:
                    return nextHeap++;
            }
        }
        internal long GetUid(int n)
        {
            long r;
            if (db == null)
                return -1L;
            switch (parse)
            {
                case ExecuteStatus.Parse:
                case ExecuteStatus.Compile:
                    r = db.nextStmt;
                    db += (Database.NextStmt, r + n);
                    return r;
                default:
                    r = nextHeap;
                    nextHeap += n;
                    return r;
            }
        }
        internal long GetPrevUid()
        {
            if (db == null)
                return -1L;
            return parse switch
            {
                ExecuteStatus.Parse or ExecuteStatus.Compile => db.nextStmt - 1,
                _ => nextHeap - 1,
            };
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
        internal int _DepthBV(BList<long?>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                d = _DepthV(b.value(), d);
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
        internal int _DepthBO(BList<SqlValue> t, int d)
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
        internal int _DepthTDb(CTree<Domain, bool> t, int d)
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
        internal int _DepthTsPil(BTree<string, (int, long?)>? t, int d)
        {
            for (var b = t?.First(); b != null; b = b.Next())
                if (b.value().Item2 is long p)
                    d = _DepthV(p, d);
            return d;
        }
        internal int _DepthLT<T>(CList<T> s, int d) where T : TypedValue
        {
            for (var b = s.First(); b != null; b = b.Next())
                if (b.value() is TypedValue v)
                    d = Math.Max(v.dataType.depth + 1, d);
            return d;
        }
        internal int _DepthLPlT<T>(Domain dm, BList<(long,T)> s, int d) where T : TypedValue
        {
            for (var b = s.First(); b != null; b = b.Next())
                if (b.value().Item2 is TypedValue v && v.dataType.defpos!=dm.defpos)
                    d = Math.Max(v.dataType.depth + 1, d);
            return d;
        }
        internal int _DepthG(GenerationRule ge, int d)
        {
            if (obs[ge.exp] is SqlValue sv)
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
        /// <summary>
        /// Symbol management stack
        /// </summary>
        internal void IncSD(Ident id)
        {
            defsStore += (sD, (id.iix.lp, defs));
            lexical = id.iix.lp;
        }
        /// <summary>
        /// Deal with end-of-Scope things 
        /// </summary>
        /// <param name="sm">The select tree at this level</param>
        /// <param name="tm">The source table expression</param>
        internal void DecSD(Domain? sm = null, RowSet? te = null)
        {
            // we don't want to lose the current defs right away.
            // they will be needed for OrderBy and ForSelect bodies
            // Look at any undefined symbols in sm that are NOT in tm
            // and identify them with symbols at the lower level.
            var sd = sD;
            var (oldlx, ldefs) = defsStore[sd - 1];
            for (var b = undefined.First(); b != null; b = b.Next())
                if (obs[b.key()] is DBObject ob)
                    ob.Resolve(this, te?.defpos ?? result, BTree<long, object>.Empty);
            for (var b = sm?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && obs[p] is SqlValue uv && uv.name is string n)
                {
                    if (uv.GetType().Name == "SqlValue" &&  // there is an unfdefined entry for n at the current level
                        te?.names.Contains(n) != true)      // which is not in tm
                    {
                        Iix dv = Iix.None;
                        var x = defsStore.Last()?.value().Item2;
                        if (x?.Contains(n) == true) // there is an entry for the previous level
                        {
                            Ident.Idents ds;
                            (dv, ds) = x[(n, sd - 1)];
                            defs += (n, dv, ds);   // update it
                        }
                        if (dv.dp >= 0 && obs[dv.dp] is SqlValue lv) // what is the current entry for n
                        {
                            if (lv.domain.kind == Sqlx.CONTENT || lv.GetType().Name == "SqlValue") // it has unknown type
                            {
                                var nv = (Domain)uv.Relocate(lv.defpos);
                                Replace(lv, nv);
                                Replace(uv, nv);
                            }
                            else if (dv.dp >= Transaction.HeapStart) // was thought unreferenced at lower level
                                Replace(uv, lv);
                        }
                    }
                    // export the current level to the next one down
                    if (sd > 2 && uv.defpos < Transaction.Executables && defs.Contains(n))
                        if (defs[n] is BTree<int, (Iix, Ident.Idents)> x && uv.name != null)
                            if (x.Contains(sd))
                            {
                                var tx = x[sd].Item1;
                                ldefs += (uv.name, new Iix(tx.lp, sd - 1, tx.dp), Ident.Idents.Empty);
                            }
                }
            defsStore -= (sd - 1);
            // demote forward references for later resolution
            for (var b = defs.First(); b != null; b = b.Next())
            {
                var n = b.key();
                if (defs[n] is BTree<int, (Iix, Ident.Idents)> t && t.Contains(sd))// there is an entry for n at the current level
                {
                    var (px, cs) = t[sd];
                    if (cs != null && cs != Ident.Idents.Empty    // re-enter forward references to be resolved at a lower level
                        && obs[px.dp] is ForwardReference fr)
                    {
                        for (var c = cs.First(); c != null; c = c.Next())
                            if (c.value() is BTree<int, (Iix, Ident.Idents)> x && x.Contains(sd)) {
                                var cn = c.key();
                                var (cx, cc) = x[sd];
                                cs += (cn, new Iix(cx.lp, px.sd - 1, cx.dp), cc);
                                fr += (ForwardReference.Subs, fr.subs + (cx.dp, true));
                                Add(fr);
                            }
                        ldefs += (n, new Iix(px.lp, px.sd - 1, px.dp), cs);
                    }
                }
            }
            if (ldefs.Count != 0)
                defs = ldefs;
            lexical = oldlx;
        }
        internal BTree<long, SqlValue> Map(BList<long?> s)
        {
            var r = BTree<long, SqlValue>.Empty;
            for (var b = s.First(); b != null; b = b.Next())
                if (b.value() is long p && obs[p] is SqlValue sp)
                    r += (p, sp);
            return r;
        }
        internal static bool HasItem(BList<long?> rt, long p)
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
        internal DBObject? Add(Physical ph, long lp = 0)
        {
            if (ph == null || db == null)
                throw new DBException("42105");
            if (lp == 0)
                lp = db.loadpos;
            if (PyrrhoStart.DebugMode && db is Transaction)
                Console.WriteLine(ph.ToString());
            return db.Add(this, ph, lp);
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
                    if (ut.super is Table su)
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
        internal void Install(DBObject ob, long p)
        {
            db += (ob, p);
            Add(ob);
  //          var dm = ob.domain;
 //          if (dm != null && ob.domain.defpos!=ob.defpos && ob.domain.defpos >= 0 
  //              && ob.domain.defpos < Transaction.TransPos)
   //             Add(dm);
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
                if (role != null && b.value()?.infos[role.defpos] is ObInfo oi && oi.name is string nm)
                {
                    var ox = Ix(b.key());
                    var ic = new Ident(nm, ox);
                    var dm = cx._Dom(b.key());
                    if (dm == null)
                        continue;
                    if (dm.kind == Sqlx.TABLE || dm is NodeType)
                        ti = ic;
                    else if (ti != null)
                    {
                        cx.defs += (ic, ox);
                        ic = new Ident(ti, ic);
                        rs += (b.key(), dm);
                    }
                    cx.defs += (ic, ox);
                }
      //     if (tb != null)
      //          cx.Add(tb + (Table.TableCols, rs));
            return cx;
        }
        internal static bool Match(BList<long?> a, BList<long?> b)
        {
            return Compare(a, b)==0;
        }
        internal static int Compare(BList<long?> a, BList<long?> b)
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
                dm = (Domain)dm.Relocate(GetUid());
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
                dm = (Domain)dm.Relocate(GetUid());
            var r = new Domain(dm.defpos, m);
            Add(r);
            return r;
        }
        internal BTree<long, object> Name(Ident n, BTree<long,object>? m=null)
        {
            var mm = (m ?? BTree<long, object>.Empty);
            mm += (DBObject.Infos, new BTree<long, ObInfo>(role.defpos, new ObInfo(n.ident)));
            mm += (DBObject.Definer, role.defpos);
            return mm + (ObInfo.Name, n.ident) + (DBObject._Ident, n);
        }
        internal DBObject _Add(DBObject ob)
        {
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
                    HandlerStatement.Action => Math.Max(((HandlerStatement)o).depth,d),
                    Domain.Aggs => _DepthTVX((CTree<long, bool>)o, d),
                    SqlValueArray._Array => _DepthBV((BList<long?>)o, d),
                    RowSet.Assig => _DepthTUb((CTree<UpdateAssignment, bool>)o, d),
                    SqlXmlValue.Attrs => _DepthBPXV((BList<(SqlXmlValue.XmlName, long)>)o, d),
                    SqlCall.Parms => _DepthBV((BList<long?>)o, d),
                    SqlCaseSimple.Cases => _DepthBPVV((BList<(long, long)>)o, d),
                    SqlXmlValue.Children => _DepthBV((BList<long?>)o, d),
                    Domain.DefaultRowValues => _DepthTVX((CTree<long, TypedValue>)o, d),
                    IfThenElse.Else => _DepthBV((BList<long?>)o, d),
                    SimpleCaseStatement.Else => _DepthBV((BList<long?>)o, d),
                    IfThenElse.Elsif => _DepthBV((BList<long?>)o, d),
                    ExplicitRowSet.ExplRows => _DepthBPVX((BList<(long, TRow)>)o, d),
                    RowSet.Filter => _DepthTVX((CTree<long, TypedValue>)o, d),
                    SqlFunction.Filter => _DepthTVX((CTree<long, bool>)o, d),
                    TransitionRowSet.ForeignKeys => _DepthTVX((CTree<long, bool>)o, d),
                    TableColumn.Generated => _DepthG((GenerationRule)o, d),
                    RowSet.Groupings => _DepthBV((BList<long?>)o, d),
                    RowSet.GroupIds => _DepthTVD((CTree<long, Domain>)o, d),
                    RowSet.Having => _DepthTVX((CTree<long, bool>)o, d),
                    Table.Indexes => _DepthTDTVb((CTree<Domain, CTree<long, bool>>)o, d),
                    RowSet.ISMap => _DepthTVV((BTree<long, long?>)o, d),
                    JoinRowSet.JoinCond => _DepthTVX((CTree<long, bool>)o, d),
                    JoinRowSet.JoinUsing => _DepthTVV((BTree<long, long?>)o, d),
                    GetDiagnostics.List => _DepthTVX((CTree<long, Sqlx>)o, d),
                    MultipleAssignment.List => _DepthBV((BList<long?>)o, d),
                    RowSet._Matches => _DepthTVX((CTree<long, TypedValue>)o, d),
                    RowSet.Matching => _DepthTVTVb((CTree<long, CTree<long, bool>>)o, d),
                    Grouping.Members => _DepthTVX((CTree<long, int>)o, d),
                    ObInfo.Names => _DepthTsPil((BTree<string, (int, long?)>)o, d),
                    RestView.NamesMap => _DepthTVX((CTree<long, string>)o, d),
                    JoinRowSet.OnCond => _DepthTVV((BTree<long, long?>)o, d),
                    FetchStatement.Outs => _DepthBV((BList<long?>)o, d),
                    SelectSingle.Outs => _DepthBV((BList<long?>)o, d),
                    PreparedStatement.QMarks => _DepthBV((BList<long?>)o, d),
                    SelectRowSet.RdCols => _DepthTVX((CTree<long, bool>)o, d),
                    RowSet.Referenced => _DepthTVX((CTree<long, bool>)o, d),
                    Level3.Index.References => _DepthTVBt((BTree<long, BList<TypedValue>>)o, d),
                    Domain.Representation => _DepthTVD((CTree<long, Domain>)o, d),
                    RowSet.RestRowSetSources => _DepthTVX((CTree<long, bool>)o, d),
                    SqlCall.Result => Math.Max(((Domain)o).depth, d),
                    Domain.RowType => _DepthBV((BList<long?>)o, d),
                    SignalStatement.SetList => _DepthTXV((BTree<Sqlx, long?>)o, d),
                    GroupSpecification.Sets => _DepthBV((BList<long?>)o, d),
                    RowSet.SIMap => _DepthTXV((BTree<long, long?>)o, d),
                    SqlRowSet.SqlRows => _DepthBV((BList<long?>)o, d),
                    InstanceRowSet.SRowType => _DepthBV((BList<long?>)o, d),
                    RowSet.Stem => _DepthTVX((CTree<long, bool>)o, d),
                    CompoundStatement.Stms => _DepthBV((BList<long?>)o, d),
                    ForSelectStatement.Stms => _DepthBV((BList<long?>)o, d),
                    WhenPart.Stms => _DepthBV((BList<long?>)o, d),
                    ForwardReference.Subs => _DepthTVX((CTree<long, bool>)o, d),
                    IfThenElse.Then => _DepthBV((BList<long?>)o, d),
                    Domain.UnionOf => _DepthTDb((CTree<Domain, bool>)o, d),
                    RestRowSetUsing.UsingCols => _DepthBV((BList<long?>)o, d),
                    RowSet.UsingOperands => _DepthTVV((BTree<long, long?>)o, d),
                    AssignmentStatement.Val => Math.Max(((AssignmentStatement)o).depth, d),
                    AssignmentStatement.Vbl => Math.Max(((AssignmentStatement)o).depth, d),
                    WhileStatement.What => _DepthBV((BList<long?>)o, d),
                    SimpleCaseStatement.Whens => _DepthBV((BList<long?>)o, d),
                    RowSet._Where => _DepthTVX((CTree<long, bool>)o, d),
                    RowSet.Windows => _DepthTVX((CTree<long, bool>)o, d),
                    QuantifiedPredicate.Vals => _DepthBV((BList<long?>)o, d),
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
            if (replacing || undefined != CTree<long, int>.Empty)
                return;
            for (var b = forApply.First(); b != null; b = b.Next())
            {
                if (obs[b.key()] is not RowSet rs)
                    throw new DBException("42000");
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
            var ldpos = db.loadpos;
            var excframing = parse != ExecuteStatus.Compile &&
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
            defs = defs.ApplyDone(this);
            for (var b=forApply.First();b is not null;b=b.Next())
            {
                var ls = BList<BTree<long, object>>.Empty;
                for (var c = b.value().First(); c != null; c = c.Next())
                    ls += Replaced(c.value());
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
        internal BList<long?> ReplacedLl(BList<long?> ks)
        {
            var r = BList<long?>.Empty;
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
        internal BTree<long,TableRow> ReplacedTlR(BTree<long,TableRow>rs,long w)
        {
            var r = BTree<long, TableRow>.Empty;
            var nt = uids[UDType.Under] ?? -1L;
            var n = uids[w] ?? w;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nr = new TableRow(b.value(), nt, w, n);
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
        internal BList<(SqlXmlValue.XmlName, long)> ReplacedLXl(BList<(SqlXmlValue.XmlName, long)> cs)
        {
            var r = BList<(SqlXmlValue.XmlName, long)>.Empty;
            var ch = false;
            for (var b = cs.First(); b != null; b = b.Next())
            {
                var (n, p) = b.value();
                var np = Replaced(p);
                ch = ch || p != np;
                r += (n, np);
            }
            return ch ? r : cs;
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
        internal BTree<string, (int,long?)> ReplacedTsPil(BTree<string, (int,long?)> wh)
        {
            var r = BTree<string, (int, long?)>.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
            {
                var (i, p) = b.value();
                var k = b.key();
                var nb = done[p??-1L];
                var nk = nb?.alias ?? k;
                var np = nb?.defpos ?? p;
                ch = ch || p != np || k != nk;
                r += (nk, (i,np));
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
                    continue;
                }
                var v = b.value().ShallowReplace1(this,was,now);
                if (v != b.value())
                    rs += (k, v);
            }
            return rs;
        }
        internal CTree<long, Domain> ShallowReplace1(CTree<long, Domain> rs, long was, long now)
        {
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (k == was)
                {
                    rs -= k;
                    k = now;
                }
                var v = b.value().ShallowReplace1(this, was, now);
                if (k!=b.key() || v != b.value())
                    rs += (k, v);
            }
            return rs;
        }
        internal CTree<long, bool> ShallowReplace(CTree<long, bool> ag, long was, long now)
        {
            for (var b = ag.First(); b != null; b = b.Next())
                if (b.key() == was)
                {
                    ag -= was;
                    ag += (now, true);
                }
            return ag;
        }
        internal BList<long?> ShallowReplace(BList<long?> rt,long was,long now)
        {
            var r = BList<long?>.Empty;
            var ch = false;
            for (var b = rt.First(); b != null; b = b.Next())
                if (b.value() == was)
                    ch = true; // don't add now
                else
                    r += b.value();
            return ch ? r : rt;
        }
        internal BList<long?> ShallowReplace1(BList<long?> rt, long was, long now)
        {
            var r = BList<long?>.Empty;
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
                    v = values[tq.qid.dp] ?? TNull.Value;
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
        internal Domain? _Dm(long dp)
        {
            return (done[dp] ?? obs[dp] ?? (DBObject?)db.objects[dp]) as Domain;
        }
        internal Iix Fix(Iix iix)
        {
            return new Iix(iix, Fix(iix.dp));
        }
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
                        case SqlValueArray._Array: v = ReplacedLl((BList<long?>)v); break;
                        case SqlSelectArray.ArrayValuedQE: v = Replaced((long)v); break;
                        case RowSet.Assig: v = ReplacedTUb((CTree<UpdateAssignment, bool>)v); break;
                        case SqlXmlValue.Attrs: v = ReplacedLXl((BList<(SqlXmlValue.XmlName, long)>)v); break;
                        case Procedure.Body: v = Replaced((long)v); break;
                        case SqlCall.Parms: v = ReplacedLl((BList<long?>)v); break;
                        case CallStatement.Call: v = Replaced((long)v); break;
                        case SqlCaseSimple.CaseElse: v = Replaced((long)v); break;
                        case SqlCaseSimple.Cases: v = ReplacedBll((BList<(long, long)>)v); break;
                        case SqlXmlValue.Children: v = ReplacedLl((BList<long?>)v); break;
                        case WhenPart.Cond: v = Replaced((long)v); break;
                        case Check.Condition: v = Replaced((long)v); break;
                        case SqlXmlValue._Content: v = Replaced((long)v); break;
                        case FetchStatement.Cursor: v = Replaced((long)v); break;
                        case RowSet._Data: v = Replaced((long)v); break;
                        case Domain.DefaultRowValues: v = ReplacedTlV((CTree<long, TypedValue>)v); break;
                        case SqlTreatExpr._Diff: v = Replaced((BTree<long,object>)v); break;
                        case IfThenElse.Else: v = ReplacedLl((BList<long?>)v); break;
                        case SimpleCaseStatement.Else: v = ReplacedLl((BList<long?>)v); break;
                        case IfThenElse.Elsif: v = ReplacedLl((BList<long?>)v); break;
                        case LikePredicate.Escape: v = Replaced((long)v); break;
                        case ExplicitRowSet.ExplRows: v = ReplacedBlT((BList<(long, TRow)>)v); break;
                        case SqlValueSelect.Expr: v = Replaced((long)v); break;
                        case RowSet.Filter: v = ReplacedTlV((CTree<long, TypedValue>)v); break;
                        case SqlFunction.Filter: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case GenerationRule.GenExp: v = Replaced((long)v); break;
                        case RowSet.Group: v = Replaced((long)v); break;
                        case RowSet.Groupings: v = ReplacedLl((BList<long?>)v); break;
                        case RowSet.GroupIds: v = ReplacedTlD((CTree<long, Domain>)v); break;
                        case RowSet.Having: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case QuantifiedPredicate.High: v = Replaced((long)v); break;
                        case Table.Indexes: v = ReplacedTDTlb((CTree<Domain, CTree<long, bool>>)v); break;
                        case LocalVariableDec.Init: v = Replaced((long)v); break;
                        case SqlInsert.InsCols: v = ReplacedLl((BList<long?>)v); break;
                        case Procedure.Inverse: v = Replaced((long)v); break;
                        case RowSet.ISMap: v = ReplacedTll((BTree<long, long?>)v); break;
                        case JoinRowSet.JFirst: v = Replaced((long)v); break;
                        case JoinRowSet.JoinCond: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case JoinRowSet.JoinUsing: v = ReplacedTll((BTree<long, long?>)v); break;
                        case JoinRowSet.JSecond: v = Replaced((long)v); break;
                        case Table.KeyCols: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case Level3.Index.Keys: v = ((Domain)v).Replaced(this); break;
                        case MergeRowSet._Left: v = Replaced((long)v); break;
                        case SqlValue.Left: v = Replaced((long)v); break;
                        case MemberPredicate.Lhs: v = Replaced((long)v); break;
                        case GetDiagnostics.List: v = Replaced((CTree<long, Sqlx>)v); break;
                        case MultipleAssignment.List: v = ReplacedLl((BList<long?>)v); break;
                        case QuantifiedPredicate.Low: v = Replaced((long)v); break;
                        case RowSet._Matches: v = ReplacedTlV((CTree<long, TypedValue>)v); break;
                        case RowSet.Matching: v = ReplacedTTllb((CTree<long, CTree<long, bool>>)v); break;
                        case Grouping.Members: v = Replaced((CTree<long, int>)v); break;
                        case ObInfo.Names: v = ReplacedTsPil((BTree<string, (int,long?)>)v); break;
                        case RestView.NamesMap: v = ReplacedTls((CTree<long,string>)v); break;
                        case NullPredicate.NVal: v = Replaced((long)v); break;
                        case JoinRowSet.OnCond: v = ReplacedTll((BTree<long, long?>)v); break;
                        case SqlFunction.Op1: v = Replaced((long)v); break;
                        case SqlFunction.Op2: v = Replaced((long)v); break;
                        case SimpleCaseStatement._Operand: v = Replaced((long)v); break;
                        case WindowSpecification.Order: v = ((Domain)v).Replaced(this); break;
                        case Domain.OrderFunc: v = Replaced((long)v); break;
                        case FetchStatement.Outs: v = ReplacedLl((BList<long?>)v); break;
                        case SelectSingle.Outs: v = ReplacedLl((BList<long?>)v); break;
                        case Procedure.Params: v = ReplacedLl((BList<long?>)v); break;
                        case WindowSpecification.PartitionType: v = ((Domain)v).Replaced(this); break;
                        //    case RowSet.Periods:        v = Replaced
                        case SqlCall.ProcDefPos: v = Replaced((long)v); break;
                        case PreparedStatement.QMarks: v = ReplacedLl((BList<long?>)v); break;
                        case SelectRowSet.RdCols: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case RowSet.Referenced: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case Level3.Index.References: v = ReplacedBBlV((BTree<long, BList<TypedValue>>)v); break;
                        case Level3.Index.RefIndex: v = Replaced((long)v); break;
                        case Domain.Representation: v = ReplacedTlD((CTree<long, Domain>)v); break;
                        case RowSet.RestRowSetSources: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case RestRowSetUsing.RestTemplate: v = Replaced((long)v); break;
                        case SqlCall.Result: v = ((Domain)v).Replaced(this); break;
                        case ReturnStatement.Ret: v = Replaced((long)v); break;
                        case MemberPredicate.Rhs: v = Replaced((long)v); break;
                        case MultipleAssignment.Rhs: v = Replaced((long)v); break;
                        case MergeRowSet._Right: v = Replaced((long)v); break;
                        case SqlValue.Right: v = Replaced((long)v); break;
                        case RowSet.RowOrder: v = ReplacedLl((BList<long?>)v); break;
                        case Domain.RowType: v = ReplacedLl((BList<long?>)v); break;
                        case RowSetPredicate.RSExpr: v = Replaced((long)v); break;
                        case RowSet.RSTargets: v = ReplacedTll((BTree<long, long?>)v); break;
                        case SqlDefaultConstructor.Sce: v = Replaced((long)v); break;
                        case IfThenElse.Search: v = Replaced((long)v); break;
                        case WhileStatement.Search: v = Replaced((long)v); break;
                        case ForSelectStatement.Sel: v = Replaced((long)v); break;
                        case QuantifiedPredicate._Select: v = Replaced((long)v); break;
                        case SignalStatement.SetList: v = Replaced((BTree<Sqlx, long?>)v); break;
                        case GroupSpecification.Sets: v = ReplacedLl((BList<long?>)v); break;
                        case RowSet.SIMap: v = ReplacedTll((BTree<long, long?>)v); break;
                        case RowSet._Source: v = Replaced((long)v); break;
                        case SqlCursor.Spec: v = Replaced((long)v); break;
                        case SqlRowSet.SqlRows: v = ReplacedLl((BList<long?>)v); break;
                        case InstanceRowSet.SRowType: v = ReplacedLl((BList<long?>)v); break;
                        case RowSet.Stem: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case CompoundStatement.Stms: v = ReplacedLl((BList<long?>)v); break;
                        case ForSelectStatement.Stms: v = ReplacedLl((BList<long?>)v); break;
                        case WhenPart.Stms: v = ReplacedLl((BList<long?>)v); break;
         //               case Domain.Structure: v = Replaced((long)v); break;
                        case SqlValue.Sub: v = Replaced((long)v); break;
                        case SqlValueArray.Svs: v = Replaced((long)v); break;
                        case Level3.Index.TableDefPos: v = Replaced((long)v); break;
                        case Table.TableRows: v = ReplacedTlR((BTree<long, TableRow>)v,w); break;
                        case RowSet.Target: v = Replaced((long)v); break;
                        case IfThenElse.Then: v = ReplacedLl((BList<long?>)v); break;
                        case SqlTreatExpr.TreatExpr: v = Replaced((long)v); break;
                        case SelectStatement.Union: v = Replaced((long)v); break;
                        case Domain.UnionOf: v = ReplacedTDb((CTree<Domain, bool>)v); break;
                        //         case RowSet.UnReferenced:   v = ReplacedTlD((CTree<long, Domain>)v); break;
                        case RestRowSetUsing.UrlCol: v = Replaced((long)v); break;
                        case RestRowSetUsing.UsingCols: v = ReplacedLl((BList<long?>)v); break;
                        case RowSet.UsingOperands: v = ReplacedTll((BTree<long, long?>)v); break;
                        case AssignmentStatement.Val: v = Replaced((long)v); break;
                        case AssignmentStatement.Vbl: v = Replaced((long)v); break;
                        case WhileStatement.What: v = ReplacedLl((BList<long?>)v); break;
                        case SimpleCaseStatement.Whens: v = ReplacedLl((BList<long?>)v); break;
                        case FetchStatement.Where: v = Replaced((long)v); break;
                        case RowSet._Where: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case RowSet.Windows: v = ReplacedTlb((CTree<long, bool>)v); break;
                        case WindowSpecification.WQuery: v = Replaced((long)v); break;
                        case SqlFunction._Val: v = Replaced((long)v); break;
                        case UpdateAssignment.Val: v = Replaced((long)v); break;
                        case QuantifiedPredicate.Vals: v = ReplacedLl((BList<long?>)v); break;
                        case SqlInsert.Value: v = Replaced((long)v); break;
                        case SelectRowSet.ValueSelect: v = Replaced((long)v); break;
                        case SqlCall.Var: v = Replaced((long)v); break;
                        case QuantifiedPredicate.What: v = Replaced((long)v); break;
                        case QuantifiedPredicate.Where: v = Replaced((long)v); break;
                        case SqlFunction.Window: v = Replaced((long)v); break;
                        case SqlFunction.WindowId: v = Replaced((long)v); break;
                        case UpdateAssignment.Vbl: v = Replaced((long)v); break;
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
            if (dp < Transaction.Analysing || db == null)
                return dp;
            // See notes in SourceIntro 3.4.2
            var r = dp;
            switch (parse)
            {
                case ExecuteStatus.Parse:
                case ExecuteStatus.Compile:
                case ExecuteStatus.Prepare:
                    if (instDFirst > 0 && dp > instSFirst
                            && dp <= instSLast)
                    {
                        r = dp - instSFirst + instDFirst;
                        if (r >= db.nextStmt)
                            db += (Database.NextStmt, r + 1);
                    } else
                        r = done[dp]?.defpos ?? dp;
                    break;
                case ExecuteStatus.Obey:
                    if (instDFirst > 0 && dp > instSFirst
                        && dp <= instSLast)
                        r = dp - instSFirst + instDFirst;
                    if (r >= nextHeap)
                        nextHeap = r + 1;
                    break;
            }
            return r;
        }
        /// <summary>
        /// As a result of Alter Type we need to merge two TableColumns. We can't use the Replace machinery
        /// above since everyting will have depth 1. So we need a new set of transformers.
        /// These are called ShallowReplace because that is what they do on DBObjects.
        /// We do rely on Domains with columns all having positive uids, an no forward column references
        /// However, DBObjects frequently have embedded Domains with later defpos so we do all Domains first.
        /// The algorithm traverses all database objects with defpos>=0 in sequence. 
        /// All transformed objects are guaranteed to have unchanged defpos.
        /// As of v7 it seems to be true that these conditions ensure no stackoverflow (?)
        /// </summary>
        /// <param name="was">A TableColumn uid</param>
        /// <param name="now">A TableColumn uid guaranteed less than was</param>
        internal void MergeColumn(long was, long now)
        {
            for (var b = db.objects.PositionAt(0L); b != null; b = b.Next())
                if (b.value() is Domain d)
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
        }
        internal void AddDefs(Domain dm)
        {
            if (db == null)
                return;
            for (var b = dm.rowType.First(); b != null;// && b.key() < dm.display; 
                    b = b.Next())
                if (b.value() is long p)
                {
                    var ob = obs[p] ?? (DBObject?)db.objects[p];
                    if (ob == null || role == null)
                        return;
                    var n = (ob is SqlValue v) ? (v.alias ?? v.name)
                        : ob.infos[role.defpos]?.name;
                    if (n == null)
                        continue;
                    var ic = new Ident(n, new Iix(dm.defpos));
                    if (ob is TableColumn tc && db.objects[tc.tabledefpos] is EdgeType et)
                    {
                        if (tc.flags.HasFlag(PColumn.GraphFlags.LeaveCol)
                        && db.objects[et.leavingType] is NodeType lt)
                            AddDefs(ic, lt);
                        else if (tc.flags.HasFlag(PColumn.GraphFlags.ArriveCol)
                            && db.objects[et.arrivingType] is NodeType at)
                            AddDefs(ic, at);
                    }
                    else
                        AddDefs(ic, ob.domain);
                    defs += (ic, ic.iix);
                }
        }
        internal void AddDefs(Ident id, Domain dm, string? a = null)
        {
            if (db == null)
                return;
            defs += (id.ident, id.iix, Ident.Idents.Empty);
            Ident? ia = null;
            if (a != null)
            {
                defs += (a, id.iix, Ident.Idents.Empty);
                ia = new Ident(a, id.iix);
            }
            for (var b = dm?.rowType.First(); b != null;// && b.key() < dm.display; 
                    b = b.Next())
                if (b.value() is long p)
                {
                    var px = Ix(id.iix.lp, p);
                    var ob = obs[p] ?? (DBObject?)db.objects[p];
                    if (ob == null || role == null)
                        return;
                    var n = (ob is SqlValue v) ? (v.alias ?? v.name)
                        : ob.infos[role.defpos]?.name;
                    if (n == null)
                        continue;
                    var ic = new Ident(n, px);
                    var iq = new Ident(id, ic);
                    //        if (defs[iq].dp < 0)
                    defs += (iq, ic.iix);
                    defs += (ic, ic.iix);
                    if (ia != null)
                        defs += (new Ident(ia, ic), ic.iix);
                }
        }
        internal void AddDefs(Ident id, BList<long?> rt, string? a = null)
        {
            if (db == null)
                return;
            defs += (id.ident, id.iix, Ident.Idents.Empty);
            Ident? ia = null;
            if (a != null)
            {
                defs += (a, id.iix, Ident.Idents.Empty);
                ia = new Ident(a, id.iix);
            }
            for (var b = rt.First(); b != null;// && b.key() < dm.display; 
                    b = b.Next())
                if (b.value() is long p)
                {
                    var px = Ix(id.iix.lp, p);
                    var ob = obs[p] ?? (DBObject?)db.objects[p];
                    if (ob == null || role == null)
                        return;
                    var n = (ob is SqlValue v) ? (v.alias ?? v.name)
                        : ob.infos[role.defpos]?.name;
                    if (n == null)
                        continue;
                    var ic = new Ident(n, px);
                    var iq = new Ident(id, ic);
                    //        if (defs[iq].dp < 0)
                    defs += (iq, ic.iix);
                    defs += (ic, ic.iix);
                    if (ia != null)
                        defs += (new Ident(ia, ic), ic.iix);
                }
        }
        internal void UpdateDefs(Ident ic, RowSet rs, string? a)
        {
            var tn = ic.ident; // the object name
            Iix ix;
            Ident.Idents ids;
            // we begin by examining the f.u entries in cx.defs. If f matches n we will add to fu
            if (defs.Contains(tn)) // care: our name may have occurred earlier (for a different instance)
            {
                (ix, ids) = defs[(tn, ic.iix.sd)];
                if (obs[ix.dp] is ForwardReference)
                {
                    for (var b = ids.First(); b != null; b = b.Next())
                    {
                        if (b.value() is BTree<int, (Iix, Ident.Idents)> bt
                            && bt.Last() is ABookmark<int, (Iix, Ident.Idents)> ab
                            && _Ob(ab.value().Item1.dp) is SqlValue ov)
                        if (rs.names[b.key()].Item2 is long p && _Ob(p) is Domain nb)
                                ov.Define(this, p, rs, nb);
                    }
                }
            }
            if (a != null && defs.Contains(a)) // care: our name may have occurred earlier (for a different instance)
            {
                (ix, ids) = defs[(a, ic.iix.sd)];
                if (obs[ix.dp] is ForwardReference)

                    for (var b = ids.First(); b != null; b = b.Next())
                        if (b.value() is BTree<int, (Iix, Ident.Idents)> bt
                            && bt.Last() is ABookmark<int, (Iix, Ident.Idents)> ab
                            && _Ob(ab.value().Item1.dp) is SqlValue ov &&
                            rs.names[b.key()].Item2 is long p && _Ob(p) is Domain nb)
                            ov.Define(this, p, rs, nb);

            }
            ids = defs[(a ?? rs.name ?? "", sD)].Item2;
            for (var b = rs.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && obs[p] is SqlValue c)
                {
                    // update defs with the newly defined entries and their aliases
                    if (c.name != null)
                    {
                        var cix = new Iix(ic.iix.lp, this, c.defpos);
                        ids += (c.name, cix, Ident.Idents.Empty);
                        var pt = defs[c.name];
                        if (pt == null || pt[cix.sd].Item1 is Iix nx &&
                            nx.dp >= 0 && (nx.sd < ic.iix.sd || nx.lp == nx.dp))
                            defs += (c.name, cix, Ident.Idents.Empty);
                        if (c.alias != null)
                        {
                            ids += (c.alias, cix, Ident.Idents.Empty);
                            defs += (c.alias, cix, Ident.Idents.Empty);
                        }
                    }
                }
            defs += (a ?? rs.name ?? "", ic.iix, ids);
        }
        internal void AddParams(Procedure pr)
        {
            if (role != null && pr.infos[role.defpos] is ObInfo oi && oi.name != null)
            {
                var zx = Ix(0);
                var pi = new Ident(oi.name, zx);
                for (var b = pr.ins.First(); b != null; b = b.Next())
                    if (b.value() is long p && obs[p] is FormalParameter pp) {
                        var pn = new Ident(pp.NameFor(this), zx);
                        var pix = new Iix(pp.defpos);
                        defs += (pn, pix);
                        defs += (new Ident(pi, pn), pix);
                        values += (p, TNull.Value); // for KnownBy
                        Add(pp);
                    }
            }
        }
        internal CList<Domain> Signature(Procedure proc)
        {
            var r = CList<Domain>.Empty;
            Add(proc.framing);
            for (var b = proc.ins.First(); b != null; b = b.Next())
                if (b.value() is long p && _Dom(p) is Domain d)
                    r += d;
            return r;
        }
        internal CList<Domain> Signature(BList<long?> ins)
        {
            var r = CList<Domain>.Empty;
            for (var b = ins.First(); b != null; b = b.Next())
                if (b.value() is long p && _Dom(p) is Domain d)
                    r += d;
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

        internal string NameFor(long p)
        {
            var ob = _Ob(p);
            if (ob is null)
                return ("?dropped");
            return ob.infos[role.defpos]?.name ??
                (string?)ob.mem[DBObject._Alias] ??
                (string?)ob.mem[ObInfo.Name] ?? "?";
        }
        internal string? GName(long? p)
        {
            if (_Ob(p??-1L) is not SqlValue sv)
                return null;
            if (sv.GetType().Name=="SqlValue" && sv.name is string s)
                return s;
            var tv = sv.Eval(this);
            if (tv is TGParam tp && binding[tp.uid] is TChar tb)
                return tb.value;
            if (tv is TChar tc)
                return tc.value;
            return null;
        }
        internal TypedValue? GConstrain(long? p)
        {
            if (_Ob(p ?? -1L) is not SqlValue sv)
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
        public static Domain InformationItemType(Sqlx w)
        {
            return w switch
            {
                Sqlx.COMMAND_FUNCTION_CODE => Domain.Int,
                Sqlx.DYNAMIC_FUNCTION_CODE => Domain.Int,
                Sqlx.NUMBER => Domain.Int,
                Sqlx.ROW_COUNT => Domain.Int,
                Sqlx.TRANSACTION_ACTIVE => Domain.Int,// always 1
                Sqlx.TRANSACTIONS_COMMITTED => Domain.Int,
                Sqlx.TRANSACTIONS_ROLLED_BACK => Domain.Int,
                Sqlx.CONDITION_NUMBER => Domain.Int,
                Sqlx.MESSAGE_LENGTH => Domain.Int,//derived from MESSAGE_TEXT 
                Sqlx.MESSAGE_OCTET_LENGTH => Domain.Int,// derived from MESSAGE_OCTET_LENGTH
                Sqlx.PARAMETER_ORDINAL_POSITION => Domain.Int,
                _ => Domain.Char,
            };
        }
        internal void CheckMetadata(long p,TypedValue v)
        {
            if (_Ob(p) is DBObject ob && ob.infos[role.defpos] is ObInfo oi)
                for (var b=oi.metadata.First();b is not null;b=b.Next())
                    switch (b.key())
                    {
                        case Sqlx.MIN:
                            {
                                if (b.value().ToInt() > v.Cardinality())
                                    throw new DBException("21000");
                                break;
                            }
                        case Sqlx.MAX:
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
                        case Sqlx.MINVALUE:
                            {
                                if (b.value().ToInt() > v.Cardinality())
                                    throw new DBException("21000");
                                break;
                            }
                        case Sqlx.MAXVALUE:
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
                var ns = Modify(ut.super as UDType);
                if (ut.super != ns && ns != null)
                    m += (UDType.Under, ns);
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
                            at = (UDType)Add(new UDType(at.defpos,am));
                        if (at.defpos == ut.defpos)
                            ut = at;
                    }
            }
            return ut;
        }
        internal Domain GroupCols(BList<long?> gs,Domain dm)
        {
            var gc = CTree<long, Domain>.Empty;
            var rs = dm as RowSet;
            for (var b = gs.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    if (obs[p] is SqlValue v && rs != null && v.KnownBy(this, rs))
                        gc += (p, _Dom(p));
                    if (obs[p] is Grouping gg)
                        for (var c = gg.keys.First(); c != null; c = c.Next())
                            if (c.value() is long cp && _Dom(cp) is Domain cd)
                            {
                                if (dm.representation.Contains(cp))
                                    gc += (cp, cd);
                                for (var d = rs?.matching.PositionAt(cp); d != null; d = d.Next())
                                    for (var e = d.value().First(); e != null; e = e.Next())
                                        if (dm.representation.Contains(e.key()))
                                            gc += (cp, cd);
                            }
                }
            var rt = BList<long?>.Empty;
            for (var b = gc.First(); b != null; b = b.Next())
                rt += b.key();
            return new Domain(-1L, this, Sqlx.ROW, gc, rt);
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
            return new Ident(id.ident, Fix(id.iix), FixI(id.sub));
        }
        internal BList<long?> FixLl(BList<long?> ord)
        {
            var r = BList<long?>.Empty;
            var ch = false;
            for (var b = ord.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var f = Fix(p);
                    if (p != f)
                        ch = true;
                    r += f;
                }
            return ch ? r : ord;
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
        internal CList<TXml> FixLX(CList<TXml> cl)
        {
            var r = CList<TXml>.Empty;
            var ch = false;
            for (var b = cl.First(); b != null; b = b.Next())
                if (b.value() is TXml tx)
                {
                    var v = (TXml)tx.Fix(this);
                    ch = ch || v != b.value();
                    r += v;
                }
            return ch? r:cl;
        }
        internal BList<(SqlXmlValue.XmlName,long)> FixLXl(BList<(SqlXmlValue.XmlName, long)> cs)
        {
            var r = BList<(SqlXmlValue.XmlName, long)>.Empty;
            var ch = false;
            for (var b = cs.First(); b != null; b = b.Next())
            {
                var (n, p) = b.value();
                var np = Fix(p);
                ch = ch || p!=np;
                r += (n,np);
            }
            return ch ? r : cs;
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
        internal BTree<string,(int,long?)> FixTsPil(BTree<string,(int,long?)>cs)
        {
            var r = BTree<string, (int,long?)>.Empty;
            for (var b = cs.First(); b != null; b = b.Next())
            if (b.value().Item2 is long p)
                r += (b.key(), (b.value().Item1,Fix(p)));
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
        internal CTree<long,CTree<long,bool>> FixTTllb(CTree<long, CTree<long,bool>> ma)
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
    }

    internal class Connection
    {
        // uid range for prepared statements is HeapStart=0x7000000000000000-0x7fffffffffffffff
        long _nextPrep = Transaction.HeapStart;
        internal long nextPrep => _nextPrep;
        internal readonly Level1.TCPStream? _tcp = null;
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
        }
        internal Connection(TCPStream tcp,BTree<string,string> cs)
        {
            _tcp = tcp;
            props = cs;
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
            var e = nm.Substring(0,i); // a (conflicting) edge type nmame
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
            Obs = -449,     // ObTree 
            Result = -452;  // long
        public ObTree obs => 
            (ObTree?)mem[Obs]??ObTree.Empty;
        public long result => (long)(mem[Result]??-1L);
        public Rvv? withRvv => (Rvv?)mem[Rvv.RVV];
        internal static Framing Empty = new();
        Framing() { }
        Framing(BTree<long,object> m) :base(m) 
        { }
        internal Framing(Context cx,long nst) : base(_Mem(cx,nst))
        { }
        static BTree<long, object> _Mem(Context cx,long nst)
        {
            var r = BTree<long, object>.Empty + (Result, cx.result);
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
        internal Framing Relocate(Context cx)
        {
            var r = ObTree.Empty;
            for (var b = obs.First(); b != null; b = b.Next())
                if (b.value() is DBObject ob)
                {
                    var nb = (DBObject)ob.Fix(cx);
                    r += (nb.defpos, nb);
                }
            return new Framing(BTree<long, object>.Empty + (Obs, r) + (Result, cx.uids[result] ?? result));
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
            r += (Result, cx.Fix(result));
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
            if (result>=0)
            {
                sb.Append(" Result ");sb.Append(DBObject.Uid(result));
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
        /// the result of COUNT
        /// </summary>
        internal long count = 0L;
        /// <summary>
        /// The row number
        /// </summary>
        internal long row = -1L;
        /// <summary>
        /// the results of MAX, MIN, FIRST, LAST, ARRAY
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
        /// the boolean result so far
        /// </summary>
        internal bool bval = false;
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
                            if (b.value() is long p && cx.obs[p] is SqlValue s)
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
                case Sqlx.COLLECT:
                case Sqlx.EVERY:
                case Sqlx.FUSION:
                case Sqlx.INTERSECTION:
                    if (mset is not null) s.Append(mset);
                    break;
                case Sqlx.INT:
                case Sqlx.INTEGER:
                    if (sumInteger is Integer i)
                        s.Append(i.ToString());
                    else
                        s.Append(sumLong);
                    break;
                case Sqlx.REAL:
                    s.Append(sum1); break;
                case Sqlx.NUMERIC:
                    if (sumDecimal is not null)
                        s.Append(sumDecimal); 
                    break;
                case Sqlx.BOOLEAN:
                    s.Append(bval); break;
                case Sqlx.MAX:
                case Sqlx.MIN:
                    s.Append(acc); break;
                case Sqlx.STDDEV_POP:
                case Sqlx.STDDEV_SAMP:
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
        public readonly Sqlx kind = Sqlx.NO;  
        /// <summary>
        /// The first point in time specified
        /// </summary>
        public readonly SqlValue? time1 = null;
        /// <summary>
        /// The second point in time specified
        /// </summary>
        public readonly SqlValue? time2 = null;
        internal PeriodSpec(string n,Sqlx k,SqlValue? t1,SqlValue? t2)
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
            var r = BTree<long, object>.Empty +  (AssertMatch, rv) + (HighWaterMark, d.loadpos);
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
