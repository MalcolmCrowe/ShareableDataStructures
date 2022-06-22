using System;
using System.Configuration;
using System.Diagnostics.Eventing.Reader;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
        internal static Context _system;
        internal readonly long cxid = ++_cxid;
        public readonly int dbformat = 51;
        public User user => db.user;
        public Role role => db.role;
        internal Context next, parent = null; // contexts form a stack (by nesting or calling)
        internal Rvv affected = null;
        public BTree<long, Cursor> cursors = BTree<long, Cursor>.Empty;
        internal CTree<long, RowSet.Finder> finder = CTree<long, RowSet.Finder>.Empty;
        internal CTree<long, bool> restRowSets = CTree<long, bool>.Empty;
        public long nextHeap = -1L, nextStmt = -1L, parseStart = -1L, oldStmt = -1L;
        public TypedValue val = TNull.Value;
        internal Database db = null;
        internal Connection conn;
        internal string url = null;
        internal Transaction tr => db as Transaction;
        internal BList<TriggerActivation> deferred = BList<TriggerActivation>.Empty;
        internal BList<Exception> warnings = BList<Exception>.Empty;
        internal ObTree obs = ObTree.Empty;
        // these five fields help with Fix(dp) during instancing (for Views)
        internal long instDFirst = -1L; // first uid for instance dest
        internal long instSFirst = -1L; // first uid in framing
        internal long instSLast = -1L; // last uid in framing
        internal BTree<int, ObTree> depths = BTree<int, ObTree>.Empty;
        internal CTree<long, TypedValue> values = CTree<long, TypedValue>.Empty;
        internal CTree<long, long> instances = CTree<long, long>.Empty;
        internal CTree<long, CTree<long,bool>> awaits = CTree<long, CTree<long,bool>>.Empty; // SqlValue,RowSet
        internal bool inHttpService = false;
        internal BTree<int,Ident.Idents> defsStore = BTree<int,Ident.Idents>.Empty; // saved defs for previous level
        internal int sD => (int)defsStore.Count; // see IncSD() and DecSD() below
        internal CTree<Domain, Domain> groupCols = CTree<Domain, Domain>.Empty; // GroupCols for a Domain with Aggs
        internal BTree<long, BTree<TRow, BTree<long, Register>>> funcs = BTree<long, BTree<TRow, BTree<long, Register>>>.Empty; // Agg GroupCols
        internal BTree<long, BTree<long, TableRow>> newTables = BTree<long, BTree<long, TableRow>>.Empty;
        /// <summary>
        /// Left-to-right accumulation of definitions during a parse: accessed only by RowSet
        /// </summary>
        internal Ident.Idents defs = Ident.Idents.Empty;
        internal CTree<long,Iix> iim = CTree<long,Iix>.Empty;
        /// <summary>
        /// Lexical positions to DBObjects (if dbformat<51)
        /// </summary>
        public BTree<long, (string, long)> digest = BTree<long, (string, long)>.Empty;
        // UnHeap things for Procedure, Trigger, and Constraint bodies
        internal BTree<long, long?> uids = BTree<long, long?>.Empty;
        internal BTree<long, RowSet.Finder> needed = BTree<long, RowSet.Finder>.Empty;
        internal long result = -1L;
        // used SqlColRefs by From.defpos
        internal BTree<long, BTree<long, SqlValue>> used = BTree<long, BTree<long, SqlValue>>.Empty;
        /// <summary>
        /// Used in Replace cascade
        /// </summary>
        internal ObTree done = ObTree.Empty;
        /// <summary>
        /// Used for prepared statements
        /// </summary>
        internal CList<long> qParams = CList<long>.Empty;
        /// <summary>
        /// The current or latest statement
        /// </summary>
        public Executable exec = null;
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
        internal BTree<long, ReadConstraint> rdC = BTree<long, ReadConstraint>.Empty; // copied to and from Transaction
        internal BTree<Audit, bool> auds = BTree<Audit, bool>.Empty;
        public int rconflicts = 0, wconflicts = 0;
        /// <summary>
        /// We only send versioned information if 
        /// a) the pseudocolumn VERSIONING has been requested or
        /// b) Protocol.Get has been used (POCO Versioned library) or
        /// c) there is a RestRowSet or
        /// d) we are preparing an HttpService Response
        /// </summary>
        internal bool versioned = false;
        internal Context(Database db, Connection con = null)
        {
            next = null;
            cxid = db.lexeroffset;
            conn = con ?? new Connection();
            obs = ObTree.Empty;
            nextHeap = conn.nextPrep;
            dbformat = db.format;
            nextStmt = db.nextStmt;
            parseStart = 0L;
            this.db = db;
        }
        internal Context(Database db, Context cx)
        {
            next = null;
            cxid = db.lexeroffset;
            conn = cx.conn;
            nextHeap = conn.nextPrep;
            dbformat = db.format;
            nextStmt = db.nextStmt;
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
            nextStmt = db.nextStmt;
            parseStart = cx.parseStart;
            values = cx.values;
            instances = cx.instances;
            defsStore = cx.defsStore;
            obs = cx.obs;
            defs = cx.defs;
            depths = cx.depths;
            obs = cx.obs;
            finder = cx.finder;
            cursors = cx.cursors;
            val = cx.val;
            parent = cx.parent; // for triggers
            dbformat = cx.dbformat;
            rdC = cx.rdC;
            nid = cx.nid;
            restRowSets = cx.restRowSets;
            groupCols = cx.groupCols;
            inHttpService = cx.inHttpService;
            // and maybe some more?
        }
        internal Context(Context c, Role r, User u) : this(c)
        {
            db = db + (Database.Role, r.defpos) + (Database.User, u.defpos);
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
        internal (DBObject, Ident) Lookup(long lp, Ident n, int d)
        {
            var (ix, _, sub) = defs[(n, d)]; // chain lookup
            if (ix != null)
            {
                if (ix.sd < sD)
                    return (null, n);
                if (obs[ix.dp] is DBObject ob)
                    return ob._Lookup(lp, this, n.ident, sub);
            }
            return (null, sub);
        }
        internal (CTree<long, RowSet.Finder>, CTree<long, bool>)
    Needs((CTree<long, RowSet.Finder>, CTree<long, bool>) ln,
        RowSet rs, Domain dm)
        {
            var s = (long)(rs.mem[RowSet._CountStar] ?? -1L);
            var (nd, rc) = ln;
            var d = dm?.display ?? 0;
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (p != s)
                    for (var c = obs[p].Needs(this, rs.defpos).First(); c != null; c = c.Next())
                    {
                        var u = c.key();
                        nd += (u, c.value());
                    }
                if (b.key() < d && rs is InstanceRowSet ir)
                    rc += (ir.iSMap[p], true);
            }
            return (nd, rc);
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
        internal CTree<long, bool> Needs(CTree<long, bool> nd, CList<long> rt)
        {
            for (var b = rt?.First(); b != null; b = b.Next())
                nd += obs[b.value()].Needs(this);
            return nd;
        }
        internal (CTree<long, RowSet.Finder>, CTree<long, bool>)
            Needs((CTree<long, RowSet.Finder>, CTree<long, bool>) ln, RowSet rs,
                CList<long> rt)
        {
            var (nd, rc) = ln;
            for (var b = rt.First(); b != null; b = b.Next())
                for (var c = obs[b.value()].Needs(this, rs.defpos).First(); c != null; c = c.Next())
                {
                    var u = c.key();
                    nd += (u, new RowSet.Finder(u, rs.defpos));
                    if (rs is TableRowSet ir)
                        rc += (ir.iSMap[u], true);
                }
            return (nd, rc);
        }
        internal (CTree<long, RowSet.Finder>, CTree<long, bool>)
            Needs<V>((CTree<long, RowSet.Finder>, CTree<long, bool>) ln, RowSet rs,
            BTree<long, V> wh)
        {
            var (nd, rc) = ln;
            for (var b = wh?.First(); b != null; b = b.Next())
                for (var c = obs[b.key()].Needs(this, rs.defpos).First(); c != null; c = c.Next())
                {
                    var u = c.key();
                    nd += (u, c.value());
                    if (rs is TableRowSet ir)
                        rc += (ir.iSMap[u], true);
                }
            return (nd, rc);
        }
        /// <summary>
        /// During Parsing
        /// </summary>
        /// <param name="ic"></param>
        /// <param name="xp"></param>
        /// <returns></returns>
        internal DBObject Get(Ident ic, Domain xp)
        {
            DBObject v = null;
            if (ic.Length > 0 && defs.Contains(ic.ToString())
                    && obs[defs[ic.ToString()][ic.iix.sd].Item1.dp] is SqlValue s0)
                v = s0;
            else if (defs.Contains(ic.ident))
                v = obs[defs[ic.ident][ic.iix.sd].Item1.dp];
            if (v != null && !xp.CanTakeValueOf(_Dom(v)))
                throw new DBException("42000", ic);
            return v;
        }

        internal object Depth(CTree<long, Domain> rs)
        {
            var d = 1;
            for (var b = rs.First(); b != null; b = b.Next())
                d = Math.Max(d,
                    Math.Max(obs[b.key()]?.depth??0, b.value().depth) + 1);
            return d;
        }
        internal void CheckRemote(string url, string etag)
        {
            var rq = WebRequest.Create(url) as HttpWebRequest;
            rq.UserAgent = "Pyrrho " + PyrrhoStart.Version[1];
            var cr = user.name + ":";
            var d = Convert.ToBase64String(Encoding.UTF8.GetBytes(cr));
            rq.UseDefaultCredentials = false;
            rq.Headers.Add("Authorization: Basic " + d);
            rq.Headers.Add("If-Match: " + etag);
            rq.Method = "HEAD";
            HttpWebResponse rs = null;
            try
            {
                rs = rq.GetResponse() as HttpWebResponse;
                var e = rs.GetResponseHeader("ETag");
                if (e!="" && e!=etag)
                    throw new DBException("40082");
            }
            catch
            {
                throw new DBException("40082", url);
            }
            rs?.Close();
        }
        internal long GetPos()
        {
            switch(parse)
            {
                case ExecuteStatus.Parse: 
                case ExecuteStatus.Compile: 
                    return nextStmt++;
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
            switch(parse)
            {
                case ExecuteStatus.Parse:
                case ExecuteStatus.Compile:
                    return nextStmt++;
                default:
                    return nextHeap++;
            }
        }
        internal Iix GetIid(int n)
        {
            var u = GetUid(n);
            return Ix(u);
        }
        internal long GetUid(int n)
        {
            long r;
            switch (parse)
            {
                case ExecuteStatus.Parse:
                case ExecuteStatus.Compile:
                    r = nextStmt;
                    nextStmt += n;
                    return r;
                default:
                    r = nextHeap;
                    nextHeap += n;
                    return r;
            }
        }
        internal long GetPrevUid()
        {
            switch (parse)
            {
                case ExecuteStatus.Parse:
                case ExecuteStatus.Compile:
                    return nextStmt-1;
                default:
                    return nextHeap-1;
            }
        }
        PRow Filter(PRow f)
        {
            if (f == null)
                return null;
            var t = Filter(f._tail);
            if (f._head is TQParam q)
                return new PRow(values[q.qid.dp], t);
            return new PRow(f._head,t);
        }
        internal CTree<long,TypedValue> Filter(Table tb,CTree<long,bool> wh)
        {
            var r = CTree<long, TypedValue>.Empty;
            for (var b = wh.First(); b != null; b = b.Next())
                r += obs[b.key()].Add(this, r, tb);
            return r;
        }
        internal int Depth(CList<long> os,params DBObject[] ps)
        {
            var r = 1;
            for (var b=os?.First();b!=null;b=b.Next())
            {
                var d = obs[b.value()]?.depth??1;
                if (d >= r)
                    r = d + 1;
            }
            foreach (var p in ps)
            {
                var d = p?.depth??-1;
                if (d >= r)
                    r = d + 1;
            }
            return r;
        }
        internal int Depth(params DBObject[] ps)
        {
            var r = 1;
            foreach (var p in ps)
            {
                var d = p?.depth??-1;
                if (d >= r)
                    r = d + 1;
            }
            return r;
        }
        internal int Depth(BList<SqlValue> vs)
        {
            var r = 1;
            for (var b = vs?.First(); b != null; b = b.Next())
                if (b.value().depth >= r)
                    r = b.value().depth + 1;
            return r;
        }
        internal int Depth(CTree<long,bool> os, params DBObject[] ps)
        {
            var r = 1;
            for (var b = os?.First(); b != null; b = b.Next())
            {
                var d = obs[b.key()].depth;
                if (d >= r)
                    r = d + 1;
            }
            foreach (var p in ps)
            {
                var d = p?.depth??-1;
                if (d >= r)
                    r = d + 1;
            }
            return r;
        }
        internal ObInfo Inf(long dp)
        {
            if (role.infos.Contains(dp))
                return (ObInfo)role.infos[dp];
            return null;
        }
        /// <summary>
        /// Symbol management stack
        /// </summary>
        internal void IncSD()
        {
            defsStore += (sD, defs);
        }
        internal void DecSD()
        {
            // we don't want to lose the current defs right away.
            // they will be needed for OrderBy and ForSelect bodies
            // but at least restore the Ambiguous entries in defs
            // at level sD.
            // Also look at any undefined symbols at the cuurent level
            // and identify them with symbols at the lower level.
            var sd = sD;
            for (var b = defs.First(); b != null; b = b.Next())
            {
                var n = b.key();
                var t = defs[n];
                if (t.Contains(sd)) // there is an entry for n at the current level
                {
                    var (px, cs) = t[sd];
                    Iix dv = Iix.None;
                    Ident.Idents ds = Ident.Idents.Empty;
                    var x = defsStore.Last().value();
                    if (x?.Contains(n) == true) // there is an entry for the previous level
                        (dv, ds) = x[(n, sd - 1)];
                    if (px.dp < 0) // n is ambiguous at the current level
                        defs += (n, dv, ds);
                    else if (dv.dp >= 0 && obs[px.dp] is SqlValue uv && obs[dv.dp] is SqlValue lv) // what is the current entry for n
                    {
                        if (_Dom(uv).kind == Sqlx.CONTENT || uv.GetType().Name=="SqlValue") // it has unknown type
                            Replace(uv, lv); // use it instead of uv
                        else if (dv.dp >= Transaction.HeapStart) // was thought unreferenced at lower level
                            Replace(uv, lv);
                    }
                    if (cs != Ident.Idents.Empty    // re-enter forward references to be resolved at a lower level
                        && obs[px.dp] is ForwardReference)
                    {
                        defs += (n, new Iix(px.lp, px.sd - 1, px.dp), cs);
                        iim += (px.dp, px);
                    }
                }
            }
            defsStore -= (sd - 1);
        }
        internal BTree<long, SqlValue> Map(CList<long> s)
        {
            var r = BTree<long, SqlValue>.Empty;
            for (var b = s.First(); b != null; b = b.Next())
            {
                var p = b.value();
                r += (p, (SqlValue)obs[p]);
            }
            return r;
        }
        internal bool HasItem(CList<long> rt,long p)
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
        internal void Add(Physical ph, long lp = 0)
        {
            if (lp == 0)
                lp = db.loadpos;
            if (PyrrhoStart.DebugMode && db is Transaction)
                Console.WriteLine(ph.ToString());
            db.Add(this, ph, lp);
        }
        internal void Add(Framing fr)
        {
            obs += fr.obs;
            for (var b=fr.depths.First();b!=null;b=b.Next())
            {
                var d = b.key();
                var ds = depths[d] ?? ObTree.Empty;
                depths += (d, ds + fr.depths[d]);
            }
        }
        internal void AddPost(string u, string tn, string s, string us, long vp, PTrigger.TrigType tp)
        {
            for (var b = ((Transaction)db).physicals.Last(); b != null; b = b.Previous())
                switch (b.value().type)
                {
                    case Physical.Type.PTransaction:
                    case Physical.Type.PTransaction2:
                            goto ins;
                    case Physical.Type.Post:
                        {
                            Post p = (Post)b.value();
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
        internal DBObject Add(DBObject ob)
        {
            if (ob == null)
                return null;
            long rp;
            if (dbformat < 51)
            {
                if (ob is SqlValue sv && (rp = sv.target) > 0 && sv.defpos > Transaction.TransPos
                    && sv.name != "" && rp< Transaction.TransPos)
                    digest += (sv.defpos, (sv.name, rp));
                else if (ob is From fm && (rp = fm.target) > 0 && fm.defpos > Transaction.TransPos
                    && fm.name != "" && rp < Transaction.TransPos)
                    digest += (fm.defpos, (fm.name, rp));
            }
            if (obs[ob.defpos] is DBObject oo && oo.depth != ob.depth)
            {
                var de = depths[oo.depth];
                depths += (oo.depth, de - ob.defpos);
            }
            if (ob.defpos != -1L)
                _Add(ob);
            if (ob.defpos >= Transaction.HeapStart)
                done += (ob.defpos, ob);
           return ob;
        }
        /// <summary>
        /// Add an object to the database/transaction 
        /// </summary>
        /// <param name="ob"></param>
        internal void Install(DBObject ob, long p)
        {
            db += (ob, p);
            obs += (ob.defpos, ob);
            var dm = _Dom(ob);
            if (dm!=null && ob.domain>=0 && ob.domain<Transaction.TransPos)
                obs += (ob.domain, dm);
            if (ob.mem.Contains(DBObject._Framing))
            {
                var t = ob.framing.obs.Last()?.key()??-1L;
                if (t > nextStmt)
                    nextStmt = t;
                if (nextStmt > db.nextStmt)
                    db += (Database.NextStmt, nextStmt);
            }
        }
        internal Context ForConstraintParse(BTree<long,ObInfo>ns)
        {
            // Set up the information for parsing the generation rule
            // The table domain and cx.defs should contain the columns so far defined
            var cx = new Context(this);
            cx.parse = ExecuteStatus.Compile;
            var rs = CTree<long, Domain>.Empty;
            Ident ti = null;
            Table tb = null;
            for (var b = ns?.First(); b != null; b = b.Next())
            {
                var oi = b.value();
                var ox = Ix(b.key());
                var ic = new Ident(oi.name, ox);
                if (oi.dataType.kind == Sqlx.TABLE)
                {
                    ti = ic;
                    var p = b.key();
                    tb = (Table)db.objects[p];
                }
                else if (ti!=null)
                {
                    cx.defs += (ic, ox);
                    ic = new Ident(ti, ic);
                    rs += (b.key(), oi.dataType);
                }
                cx.defs += (ic, ox);
                cx.iim += (ic.iix.dp, ox);
            }
            if (tb!=null)
             cx.Add(tb + (Table.TableCols, rs));
            return cx;
        }
        internal Domain _DomAdd(Context cx,Domain dm,SqlValue sv)
        {
            var r = new Domain(GetUid(),cx,dm.kind,dm.representation+(sv.defpos,cx._Dom(sv)),
                dm.rowType+sv.defpos);
            obs += (r.defpos, r);
            return r;
        }
        internal Domain _Dom(DBObject ob)
        {
            if (ob == null)
                return null;
            if (ob is Domain)
                return (Domain)ob;
            if (ob.defpos!=-1L && ob.defpos < Transaction.Analysing &&
                ((ObInfo)role.infos[ob.defpos])?.dataType is Domain d && d.Length>0)
                return d;
            return (Domain)obs[ob.domain] ?? (Domain)db.objects[ob.domain] ?? Domain.Content;
        }
        internal Domain _Dom(long dp)
        {
            return (Domain)(obs[dp]??db.objects[dp]);
        }
        internal Domain _Dom(long dp,CList<long> rt)
        {
            var ob = obs[dp]??(DBObject)db.objects[dp];
            var dm = _Dom(ob);
            if (dm.rowType.CompareTo(rt) == 0)
                return dm;
            if (dm.defpos < Transaction.Executables)
                dm = (Domain)dm.Relocate(GetUid());
            var r = dm + (Domain.RowType, rt);
            obs += (r.defpos, dm);
            return r;
        }
        internal Domain _Dom(Domain dm,long p,Domain d)
        {
            var rs = dm.representation;
            if (rs[p] == d)
                return dm;
            if (dm.defpos < Transaction.Executables)
                dm = (Domain)dm.Relocate(GetUid());
            var rt = dm.rowType;
            if (!rt.Has(p))
                rt += p;
            var r = dm + (Domain.Representation, rs + (p, d))
                +(Domain.RowType,rt);
            obs += (r.defpos, r);
            return r;
        }
        internal Domain _Dom(long dp,params (long,object)[] xs)
        {
            var ob = obs[dp] ?? (DBObject)db.objects[dp];
            var dm = _Dom(ob);
            var m = dm.mem;
            var ch = false;
            foreach(var x in xs)
            {
                var (k, v) = x;
                m += x;
                ch = ch || dm.mem[k] == v;
            }
            if (!ch)
                return dm;
            if (dm.defpos < Transaction.Executables)
                dm = (Domain)dm.Relocate(GetUid());
            var r = (Domain)dm.New(m);
            obs += (r.defpos, r);
            return r;
        }
        internal DBObject _Add(DBObject ob,string alias=null)
        {
            if (ob != null && ob.defpos != -1)
            {
                ob._Add(this);
                var dp = depths[ob.depth] ?? ObTree.Empty;
                depths += (ob.depth, dp + (ob.defpos, ob));
            }
            return ob;
        }
        internal void _Remove(long dp)
        {
            obs -= dp;
            for (var b=depths.First();b!=null;b=b.Next())
                depths += (b.key(), b.value() - dp);
        }
        internal void AddRowSetsPair(SqlValue a,SqlValue b)
        {
            if (obs[a.from] is RowSet ra)
                obs += (a.from, ra + (a.defpos, b.defpos));
            if (obs[b.from] is RowSet rb)
                obs += (a.from, rb + (a.defpos, b.defpos));
        }
        /// <summary>
        /// Update the query processing context in a cascade to implement a single replacement!
        /// </summary>
        /// <param name="was"></param>
        /// <param name="now"></param>
        internal DBObject Replace(DBObject was, DBObject now, ObTree m=null,Domain dm=null)
        {
            if (dbformat<51)
                Add(now);
            if (was == now)
                return now;
            _Add(now);
            done = (m??ObTree.Empty)+(now.defpos,now);
            if (was.defpos != now.defpos)
                done += (was.defpos, now);
            // scan by depth to perform the replacement
            var ldpos = db.loadpos;
            for (var b = depths.First(); b != null; b = b.Next())
            {
                var bv = b.value();
                for (var c = bv.PositionAt(ldpos); c != null; c = c.Next())
                {
                    var p = c.value();
                    var cv = obs[p.defpos]._Replace(this, was, now); // may update done
                    if (cv != p)
                        bv += (c.key(), cv);
                }
                if (bv != b.value())
                    depths += (b.key(), bv);
            }
            for (var b=done.First();b!=null;b=b.Next())
                obs += (b.key(), b.value());
            defs = defs.ApplyDone(this);
            var r = now;
            if (dm != null && dm.defpos>=Transaction.Analysing)
            {
                var rs = CTree<long, Domain>.Empty;
                var rt = CList<long>.Empty;
                for (var b=dm.representation.First();b!=null;b=b.Next())
                {
                    var ob = done[b.key()];
                    rs += (ob.defpos, _Dom(ob));
                }
                for (var b = dm.rowType.First(); b != null; b = b.Next())
                    rt += done[b.value()].defpos;
                if (rs.CompareTo(dm.representation) != 0)
                    r = dm + (Domain.Representation, rs) + (Domain.RowType,rt);
                obs += (r.defpos, r);
                return r;
            }
            if (was.defpos != now.defpos)
                _Remove(was.defpos);
            return r;
        }

        internal CTree<long,bool> Operands(CTree<long, bool> where)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = where.First(); b != null; b = b.Next())
                r += ((SqlValue)obs[b.key()]).Operands(this);
            return r;
        }

        internal long ObReplace(long dp,DBObject was,DBObject now)
        {
            if (dp < Transaction.TransPos || !obs.Contains(dp))
                return dp;
            if (done.Contains(dp))
                return done[dp].defpos;
            if (obs[dp] == null)
                throw new PEException("Bad replace");
            return obs[dp]?._Replace(this, was, now)?.defpos??
                throw new PEException("Bad replace");
        }
        internal CTree<long,RowSet.Finder> Replaced(CTree<long,RowSet.Finder>fi)
        {
            var r = CTree<long, RowSet.Finder>.Empty;
            var ch = false;
            for (var b=fi.First();b!=null;b=b.Next())
            {
                var f = b.value();
                var c = f.col;
                var k = b.key();
                var nk = done[k]?.defpos??k;
                var nc = done[c]?.defpos??c;
                ch = ch || k != nk || c != nc;
                r += (nk, new RowSet.Finder(nc, f.rowSet));
            }
            return ch?r:fi;
        }
        internal BTree<long,Cursor> Replaced(BTree<long,Cursor> ec)
        {
            var r = BTree<long, Cursor>.Empty;
            var ch = false;
            for (var b = ec.First();b!=null;b=b.Next())
            {
                var c = b.value();
                var k = b.key();
                var nk = done[k]?.defpos ?? k;
                var nc = (Cursor)c.Replaced(this);
                ch = ch || k != nk || c != nc;
                r += (nk, nc);
            }
            return ch?r:ec;
        }
        internal CList<long> Replaced(CList<long> ks)
        {
            var r = CList<long>.Empty;
            var ch = false;
            for (var b = ks.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var np = done[p]?.defpos ?? p;
                ch = ch || p != np;
                r += np;
            }
            return ch?r:ks;
        }
        internal CTree<long,bool> Replaced(CTree<long,bool> wh)
        {
            var r = CTree<long,bool>.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = done[k]?.defpos ?? k;
                ch = ch || k != nk;
                r += (nk, b.value());
            }
            return ch?r:wh;
        }
        internal CTree<long,long> Replaced(CTree<long,long> wh)
        {
            var r = CTree<long, long>.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = b.value();
                var nk = done[k]?.defpos ?? k;
                var np = done[p]?.defpos ?? p;
                ch = ch || k != nk || p != np;
                r += (nk, np);
            }
            return ch?r:wh;
        }
        internal CTree<long, TypedValue> Replace(CTree<long, TypedValue> wh,
            DBObject so,DBObject sv)
        {
            var r = CTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = b.value();
                var nk = done[k]?.defpos ?? k;
                var nv = v.Replace(this,so,sv);
                ch = ch || k != nk || v != nv;
                r += (nk, nv);
            }
            return ch?r:wh;
        }
        internal CTree<string,CTree<long,long>> Replaced
            (CTree<string, CTree<long, long>> vc)
        {
            var r = CTree<string, CTree<long, long>>.Empty;
            var ch = false;
            for (var b = vc.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = b.value();
                var nv = Replaced(v);
                ch = ch || v != nv;
                r += (k, nv);
            }
            return ch?r:vc;
        }
        internal CTree<CList<long>,long> Replaced
            (CTree<CList<long>,long> xs)
        {
            var r = CTree<CList<long>, long>.Empty;
            var ch = false;
            for (var b = xs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = b.value();
                var nk = Replaced(k);
                var np = done[p]?.defpos ?? p;
                ch = ch || k != nk || p != np;
                r += (nk, np);
            }
            return ch?r:xs;
        }
        internal CTree<CList<long>, (long,CList<long>)> Replaced
    (CTree<CList<long>, (long,CList<long>)> xs)
        {
            var r = CTree<CList<long>, (long,CList<long>)>.Empty;
            var ch = false;
            for (var b = xs.First(); b != null; b = b.Next())
            {
                var (p, cs) = b.value();
                var k = b.key();
                var nk = Replaced(k);
                var np = done[p]?.defpos ?? p;
                var nc = Replaced(cs);
                ch = ch || k != nk || p != np || cs != nc;
                r += (nk, (np,nc));
            }
            return ch?r:xs;
        }
        BTree<long,bool> RestRowSets(long p)
        {
            var r = BTree<long, bool>.Empty;
            var s = (RowSet)obs[p];
            if (s is RestRowSet)
                r += (p, true);
            else
                for (var b = s.Sources(this).First(); b != null; b = b.Next())
                    r += RestRowSets(b.key());
            return r;
        }
        /// <summary>
        /// Propagate prepared statement parameters into their referencing TypedValues
        /// </summary>
        internal void QParams()
        {
            for (var b = obs.PositionAt(Transaction.HeapStart); b != null; b = b.Next())
                obs += (b.key(), b.value().QParams(this));
        }
        internal CTree<long,TypedValue> QParams(CTree<long,TypedValue> f)
        {
            var r = CTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = f.First(); b != null; b = b.Next())
            {
                var v = b.value();
                if (v is TQParam tq)
                {
                    ch = true;
                    v = values[tq.qid.dp];
                }
                r += (b.key(), v);
            }
            return ch ? r : f;
        }
         internal DBObject _Replace(long dp, DBObject was, DBObject now)
        {
            if (done.Contains(dp))
                return done[dp];
            return obs[dp]?._Replace(this, was, now);
        }
        internal DBObject _Ob(long dp)
        {
            return done[dp] ?? obs[dp]?? (DBObject)db.objects[dp];
        }
        internal Iix Fix(Iix iix)
        {
            return new Iix(iix, Fix(iix.dp));
        }
        internal long Fix(long dp)
        {
            // This is needed for Commit and for ppos->instance
            if (uids[dp] != null)
                return uids[dp].Value;
            if (dp < Transaction.Analysing)
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
                        if (r >= nextStmt)
                            nextStmt = r + 1;
                    } else
                        r = done[dp]?.defpos ?? dp;
                    break;
                case ExecuteStatus.Obey:
                    if (instDFirst>0 && dp>instSFirst 
                        && dp<=instSLast)
                        r = dp - instSFirst + instDFirst; 
                    if (r >= nextHeap)
                        nextHeap = r+1;
                    break;
            }
            return r;
        }
        internal void AddDefs(Ident id, Domain dm, string a=null)
        {
            defs += (id.ident, id.iix, Ident.Idents.Empty);
            iim += (id.iix.dp, id.iix);
            for (var b = dm?.rowType.First(); b != null;// && b.key() < dm.display; 
                b = b.Next())
            {
                var p = b.value();
                var px = Ix(id.iix.lp,p);
                var ob = obs[p] ?? (DBObject)db.objects[p];
                var n = (ob is SqlValue v) ? v.alias??v.name : Inf(p)?.name;
                if (n == null)
                    continue;
                var ic = new Ident(n, px);
                var iq = new Ident(id, ic);
                var ox = Ix(ob.defpos);
                if (defs[iq].dp < 0)
                {
                    defs += (iq, ox);
                    iim += (ob.defpos, ox);
                }
                if (defs[ic].dp < 0)
                {
                    defs += (ic, ox);
                    iim += (ob.defpos, ox); // one of them may succeed
                }
            }
        }
        internal void AddParams(Procedure pr)
        {
            var zx = Ix(0);
            var pi = new Ident(pr.name, zx);
            for (var b = pr.ins.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var pp = (FormalParameter)obs[p];
                var pn = new Ident(pp.name, zx);
                var pix = new Iix(pp.defpos);
                defs += (pn, pix);
                iim += (p, pix);
                defs += (new Ident(pi, pn), pix);
                values += (p, TNull.Value); // for KnownBy
                Add(pp);
            }
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
            switch (w)
            {
                case Sqlx.COMMAND_FUNCTION_CODE: return Domain.Int;
                case Sqlx.DYNAMIC_FUNCTION_CODE: return Domain.Int;
                case Sqlx.NUMBER: return Domain.Int;
                case Sqlx.ROW_COUNT: return Domain.Int;
                case Sqlx.TRANSACTION_ACTIVE: return Domain.Int; // always 1
                case Sqlx.TRANSACTIONS_COMMITTED: return Domain.Int; 
                case Sqlx.TRANSACTIONS_ROLLED_BACK: return Domain.Int; 
                case Sqlx.CONDITION_NUMBER: return Domain.Int;
                case Sqlx.MESSAGE_LENGTH: return Domain.Int; //derived from MESSAGE_TEXT 
                case Sqlx.MESSAGE_OCTET_LENGTH: return Domain.Int; // derived from MESSAGE_OCTET_LENGTH
                case Sqlx.PARAMETER_ORDINAL_POSITION: return Domain.Int;
            }
            return Domain.Char;
        }
        internal PRow MakeKey(CList<long>s)
        {
            PRow k = null;
            for (var b = s.Last(); b != null; b = b.Previous())
                k = new PRow(obs[b.value()].Eval(this), k);
            return k;
        }
        internal Procedure GetProcedure(long lp,string n,int a)
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
        internal virtual Context SlideDown()
        {
            if (next == null)
                return this;
            next.values += values;
            next.warnings += warnings;
            next.deferred += deferred;
            next.val = val;
            next.nextHeap = nextHeap;
            next.db = db; // adopt the transaction changes done by this
            return next;
        }
        internal void DoneCompileParse(Context cx)
        {
            if (cx != this)
                for (var b = cx.obs.First(); b != null; b = b.Next())
                    _Add(b.value());
            db = cx.db;
            nextStmt = cx.nextStmt;
        }
        internal Domain GroupCols(CList<long> gs)
        {
            var gc = CTree<long, Domain>.Empty;
            for (var b = gs.First(); b != null; b = b.Next())
            {
                var gg = (Grouping)obs[b.value()];
                for (var c = gg.keys.First(); c != null; c = c.Next())
                {
                    var p = c.value();
                    gc += (p,_Dom(obs[p]));
                }
            }
            var rt = CList<long>.Empty;
            for (var b = gc.First(); b != null; b = b.Next())
                rt += b.key();
            return new Domain(-1L,this,Sqlx.ROW,gc,rt);
        }
        internal Domain GroupCols(CTree<long, bool> gs)
        {
            var gc = CTree<long, Domain>.Empty;
            for (var b = gs.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var sv = (SqlValue)obs[p];
                gc += (p, _Dom(sv));
            }
            var rt = CList<long>.Empty;
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
        internal CList<long> RowType(CList<SqlValue> ls)
        {
            var r = CList<long>.Empty;
            for (var b = ls.First(); b != null; b = b.Next())
                r += b.value().defpos;
            return r;
        }
        internal Ident Fix(Ident id)
        {
            if (id == null)
                return null;
            return new Ident(id.ident, Fix(id.iix), Fix(id.sub));
        }
        internal BList<long> Fix(BList<long> ord)
        {
            var r = BList<long>.Empty;
            var ch = false;
            for (var b = ord?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = Fix(p);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : ord;
        }
        internal CList<long> Fix(CList<long> ord)
        {
            var r = CList<long>.Empty;
            var ch = false;
            for (var b = ord?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = Fix(p);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : ord;
        }
        internal BList<Domain> Fix(BList<Domain> ds)
        {
            var r = BList<Domain>.Empty;
            var ch = false;
            for (var b = ds?.First(); b != null; b = b.Next())
            {
                var d = b.value();
                var nd = (Domain)d._Fix(this);
                if (d != nd)
                    ch = true;
                r += nd;
            }
            return ch ? r : ds;
        }
        internal CList<UpdateAssignment> Fix(CList<UpdateAssignment> us)
        {
            var r = CList<UpdateAssignment>.Empty;
            var ch = false;
            for (var b=us.First();b!=null;b=b.Next())
            {
                var u = (UpdateAssignment)b.value()._Fix(this);
                ch = ch || u != b.value();
                r += u;
            }
            return ch ? r : us;
        }
        internal BList<Grouping> Fix(BList<Grouping> gs)
        {
            var r = BList<Grouping>.Empty;
            var ch = false;
            for (var b=gs.First();b!=null;b=b.Next())
            {
                var g = (Grouping)b.value()._Fix(this);
                ch = ch || g != b.value();
                r += g;
            }
            return ch ? r : gs;
        }
        internal CTree<UpdateAssignment,bool> Fix(CTree<UpdateAssignment,bool> us)
        {
            var r = CTree<UpdateAssignment,bool>.Empty;
            var ch = false;
            for (var b = us.First(); b != null; b = b.Next())
            {
                var u = (UpdateAssignment)b.key()._Fix(this);
                ch = ch || u != b.key();
                r += (u,true);
            }
            return ch ? r : us;
        }
        internal CTree<string,CTree<long,long>> Fix
            (CTree<string,CTree<long,long>> vc)
        {
            var r = CTree<string, CTree<long, long>>.Empty;
            for (var b = vc.First(); b != null; b = b.Next())
                r += (b.key(),Fix(b.value()));
            return r;
        }
        internal BList<Cursor> Fix(BList<Cursor> rws)
        {
            var r = BList<Cursor>.Empty;
            var ch = false;
            for (var b=rws.First();b!=null;b=b.Next())
            {
                var rr = b.value();
                var fr = rr?._Fix(this);
                ch = ch || fr != rr;
                r += fr;
            }
            return r;
        }
        internal BList<TypedValue> Fix(BList<TypedValue> key)
        {
            var r = BList<TypedValue>.Empty;
            var ch = false;
            for (var b = key?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = p.Fix(this);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : key;
        }
        internal CList<TypedValue> Fix(CList<TypedValue> key)
        {
            var r = CList<TypedValue>.Empty;
            var ch = false;
            for (var b = key?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var f = b.value().Fix(this);
                if (p != f)
                    ch = true;
                r += f;
            }
            return ch ? r : key;
        }
        internal BList<TRow> Fix(BList<TRow> rws)
        {
            var r = BList<TRow>.Empty;
            var ch = false;
            for (var b=rws?.First();b!=null;b=b.Next())
            {
                var v = (TRow)b.value().Fix(this);
                ch = ch || v != b.value();
                r += v;
            }
            return ch ? r : rws;
        }
        internal CTree<long,V> Fix<V>(CTree<long,V> ms) where V:IComparable
        {
            var r = CTree<long, V>.Empty;
            var ch = false;
            for (var b=ms?.First();b!=null;b=b.Next())
            {
                var k = b.key();
                var m = Fix(k);
                ch = ch || m != k;
                r += (m, b.value());
            }
            return ch ? r : ms;
        }
        internal CTree<K,long> Fix<K>(CTree<K, long> ms) where K:IComparable
        {
            var r = CTree<K,long>.Empty;
            var ch = false;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var m = Fix(p);
                ch = ch || m != p;
                r += (b.key(),m);
            }
            return ch ? r : ms;
        }
        internal CTree<CList<long>, (long,CList<long>)> Fix(CTree<CList<long>, (long,CList<long>)> ms)
        {
            var r = CTree<CList<long>, (long,CList<long>)>.Empty;
            var ch = false;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var (p,cs) = b.value();
                var m = Fix(p);
                ch = ch || m != p;
                var nc = Fix(cs);
                ch = ch || nc != cs;
                r += (b.key(), (m,nc));
            }
            return ch ? r : ms;
        }
        internal CTree<long,RowSet.Finder> Fix(CTree<long,RowSet.Finder> fi)
        {
            var r = CTree<long,RowSet.Finder>.Empty;
            var ch = false;
            for (var b = fi?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                var f = b.value().Fix(this);
                if (p != b.key() || (object)f!=(object)b.value())
                    ch = true;
                r += (p,f);
            }
            return ch ? r : fi;
        }
        internal BTree<long, Cursor> Fix(BTree<long, Cursor> vt)
        {
            var r = BTree<long, Cursor>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                var v = (Cursor)b.value().Fix(this);
                if (p != b.key() || v != b.value())
                    ch = true;
                r += (p, v);
            }
            return ch ? r : vt;
        }
        internal BList<BTree<long, Cursor>> Fix(BList<BTree<long, Cursor>> vt)
        {
            var r = BList<BTree<long, Cursor>>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var v = Fix(b.value());
                if (v != b.value())
                    ch = true;
                r += v;
            }
            return ch ? r : vt;
        }
        internal CTree<long, TypedValue> Fix(CTree<long, TypedValue> vt)
        {
            var r = CTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                var v = b.value().Fix(this);
                if (p != b.key() || v != b.value())
                    ch = true;
                r += (p, v);
            }
            return ch ? r : vt;
        }
        internal CTree<string,TypedValue> Fix(CTree<string, TypedValue> a)
        {
            var r = CTree<string, TypedValue>.Empty;
            var ch = false;
            for (var b = a?.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var v = b.value().Fix(this);
                ch = ch || v != b.value();
                r += (p, v);
            }
            return ch?r:a;
        }
        internal CList<TXml> Fix(CList<TXml> cl)
        {
            var r = CList<TXml>.Empty;
            var ch = false;
            for (var b = cl?.First(); b != null; b = b.Next())
            {
                var v = (TXml)b.value().Fix(this);
                ch = ch || v != b.value();
                r += v;
            }
            return ch? r:cl;
        }
        internal BList<(SqlXmlValue.XmlName,long)> Fix(BList<(SqlXmlValue.XmlName, long)> cs)
        {
            var r = BList<(SqlXmlValue.XmlName, long)>.Empty;
            var ch = false;
            for (var b = cs?.First(); b != null; b = b.Next())
            {
                var (n, p) = b.value();
                var np = Fix(p);
                ch = ch || p!=np;
                r += (n,np);
            }
            return ch ? r : cs;
        }
        internal CTree<TypedValue,long> Fix(CTree<TypedValue,long> mu)
        {
            var r = CTree<TypedValue,long>.Empty;
            var ch = false;
            for (var b = mu?.First(); b != null; b = b.Next())
            {
                var p = b.key().Fix(this);
                var q = uids[b.value()]??-1L;
                ch = ch || p != b.key() || q != b.value();
                r += (p, q);
            }
            return ch? r : mu;
        }
        internal BTree<string,long> Fix(BTree<string,long>cs)
        {
            var r = BTree<string, long>.Empty;
            for (var b = cs?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                r += (b.key(), uids[p]??p);
            }
            return r;
        }
        internal BList<(long,long)> Fix(BList<(long,long)> cs)
        {
            var r = BList<(long,long)>.Empty;
            for (var b = cs.First();b!=null; b=b.Next())
            {
                var (w, x) = b.value();
                r += (Fix(w), Fix(x));
            }
            return r;
        }
        internal CTree<long,CTree<long,bool>> Fix(CTree<long, CTree<long,bool>> ma)
        {
            var r = CTree<long, CTree<long,bool>>.Empty;
            var ch = false;
            for (var b = ma?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                var v = Fix(b.value());
                ch = ch || p != k || v != b.value();
                r += (p, v);
            }
            return ch ? r : ma;
        }
        internal CTree<CList<long>,CTree<long,bool>> Fix(CTree<CList<long>,CTree<long,bool>> xs)
        {
            var r = CTree<CList<long>, CTree<long, bool>>.Empty;
            var ch = false;
            for (var b = xs?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                var v = Fix(b.value());
                ch = ch || p != k || v != b.value();
                r += (p, v);
            }
            return ch ? r : xs;
        }
        internal ObTree Fix(ObTree os)
        {
            var r = ObTree.Empty;
            for (var b=os.First();b!=null;b=b.Next())
            {
                var nk = Fix(b.key());
                var nb = (DBObject)b.value()._Fix(this);
                r += (nk, nb);
            }
            return r;
        }
        internal CTree<long,Domain> Fix(CTree<long,Domain> rs)
        {
            var r = CTree<long, Domain>.Empty;
            var ch = false;
            for (var b=rs.First();b!=null;b=b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                var d = (Domain)b.value()._Fix(this);
                ch = ch || p != k || d != b.value();
                r += (p, d);
            }
            return ch?r:rs;
        }
        internal CTree<long, long> Fix(CTree<long, long> rs)
        {
            var r = CTree<long, long>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                var v = b.value();
                var d = Fix(v);
                ch = ch || p != k || d != v;
                r += (p, d);
            }
            return ch ? r : rs;
        }
        internal CTree<long, long> FixV(CTree<long, long> rs)
        {
            var r = CTree<long, long>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = b.value();
                var d = Fix(v);
                ch = ch || d != v;
                r += (k, d);
            }
            return ch ? r : rs;
        }
        internal BTree<long,(long,long)> Fix(BTree<long,(long,long)> ds)
        {
            var r = BTree<long,(long, long)>.Empty;
            var ch = false;
            for (var b = ds.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = Fix(k);
                var (v,p) = b.value();
                var nv = Fix(v);
                var np = Fix(p);
                ch = ch || nk!=k || nv!=v || np!=p;
                r += (k, (nv,np));
            }
            return ch?r:ds;
        }
        internal CTree<PTrigger.TrigType, CTree<long, bool>> Fix(CTree<PTrigger.TrigType, CTree<long, bool>> t)
        {
            var r = CTree<PTrigger.TrigType, CTree<long, bool>>.Empty;
            for (var b = t.First(); b != null; b = b.Next())
            {
                var p = b.key();
                r += (p, Fix(b.value()));
            }
            return r;
        }
    }

    internal class Connection
    {
        // uid range for prepared statements is HeapStart=0x7000000000000000-0x7fffffffffffffff
        long _nextPrep = Transaction.HeapStart;
        internal long nextPrep => _nextPrep;
        /// <summary>
        /// A list of prepared statements, whose object persist
        /// in the base context for the connection.
        /// </summary>
        BTree<string, PreparedStatement> _prepared = BTree<string, PreparedStatement>.Empty;
        internal BTree<string, PreparedStatement> prepared => _prepared;
        /// <summary>
        /// Connection string details
        /// </summary>
        internal BTree<string, string> props = BTree<string, string>.Empty;
        public Connection(BTree<string,string> cs=null)
        {
            if (cs != null)
                props = cs;
        }
        public void Add(string nm,PreparedStatement ps)
        {
            _prepared += (nm, ps);
            _nextPrep = ps.framing.obs.Last().key();
        }
    }
    /// <summary>
    /// The Framing of an DBObject contains executable-range objects only,
    /// which were created by the Compiled classes on load or during parsing.
    /// shareable as of 26 April 2021
    /// </summary>
    internal class Framing : Basis // for compiled code
    {
        internal const long
            Depths = -450,  // BTree<int,ObTree>
            Obs = -449,     // ObTree 
            Result = -452;  // long
        public ObTree obs => 
            (ObTree)mem[Obs]??ObTree.Empty;
        public BTree<int,ObTree> depths =>
            (BTree<int,ObTree>)mem[Depths]??BTree<int,ObTree>.Empty;
        public long result => (long)(mem[Result]??-1L);
        public Rvv withRvv => (Rvv)mem[Rvv.RVV];
        internal static Framing Empty = new Framing();
        //       public BTree<int, BTree<long, DBObject>> depths =>
        //           (BTree<int,BTree<1long,DBObject>>)mem[Depths]
        //           ??BTree<int,ObTree>.Empty;
        Framing() { }
        Framing(BTree<long,object> m) :base(m) 
        { }
        internal Framing(Context cx) : base(_Mem(cx))
        {
            cx.oldStmt = cx.db.nextStmt;
        }
        static BTree<long, object> _Mem(Context cx)
        {
            var r = BTree<long, object>.Empty + (Result, cx.result)
                  + (Rvv.RVV, cx.affected);
            var os = ObTree.Empty;
            for (var b = cx.obs.PositionAt(Transaction.Executables); b != null; b = b.Next())
            {
                var k = b.key();
                if (k >= Transaction.HeapStart && cx.parse != ExecuteStatus.Prepare)
                    throw new PEException("PE443");
                var v = b.value();
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
            return new Framing(f.mem + x);
        }
        public static Framing operator+(Framing f,DBObject ob)
        {
            var p = ob.defpos;
            var d = f.depths[ob.depth]??ObTree.Empty;
            return f + (Obs, f.obs + (p, ob)) + (Depths, f.depths + (ob.depth, d + (p, ob)));
        }
        public static Framing operator-(Framing f,long p)
        {
            return f + (Obs, f.obs - p);
        }
        internal override Basis _Relocate(Context cx)
        {
            if (this == Empty)
                return this;
            var r = (Framing)base._Relocate(cx);
            for (var b = r.obs.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var ob = (DBObject)b.value().Fix(cx);
                if (ob.defpos != p)
                    r -= p;
                r += ob;
            }
            r += (Result, cx.Fix(r.result));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = this;
            cx.obs += obs;
            for (var b = obs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = cx.Fix(k);
                var ob = b.value();
                var nb = (DBObject)ob.Fix(cx);
                if (k != nk)
                {
                    nb = nb.Relocate(nk);
                    if (k<Transaction.Executables||k>=Transaction.HeapStart) // don't remove virtual columns
                        r -= k;  // or RestView
           //         cx.Remove(ob); typically ob will not be in cx
                }
                if (nb != ob)
                {
                    r += ob;
                    cx.Add(nb);
                }
            }
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
            sb.Append(")");
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
        internal RTree wtree = null;
        /// <summary>
        /// The bookmark for the current row
        /// </summary>
        internal Cursor wrb = null;
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
        internal TypedValue acc;
        /// <summary>
        /// the results of XMLAGG
        /// </summary>
        internal StringBuilder sb = null;
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
        internal Integer sumInteger = null;
        /// <summary>
        /// the sum of double
        /// </summary>
        internal double sum1, acc1;
        /// <summary>
        /// the sum of Decimal
        /// </summary>
        internal Numeric sumDecimal;
        /// <summary>
        /// the boolean result so far
        /// </summary>
        internal bool bval = false;
        /// <summary>
        /// a multiset for accumulating things
        /// </summary>
        internal TMultiset mset = null;
        internal Register(Context cx,TRow key,SqlFunction sf)
        {
            var oc = cx.cursors;
            if (sf.window >=0L)
            {
                var t1 = cx.funcs[sf.from] ?? BTree<TRow, BTree<long, Register>>.Empty;
                var t2 = t1[key] ?? BTree<long, Register>.Empty;
                t2 += (sf.defpos, this);
                t1 += (key, t2);
                cx.funcs += (sf.from, t1);  // prevent stack oflow
                var dp = sf.window;
                var ws = (WindowSpecification)cx.obs[dp];
                var dm = ws.order;
                if (dm != null)
                {
                    wtree = new RTree(dp, cx, dm,
                       TreeBehaviour.Allow, TreeBehaviour.Allow);
                    var fm = (RowSet)cx.obs[sf.from];
                    var sce = (RowSet)cx.obs[fm.source];
                    for (var e = sce.First(cx); e != null; e = e.Next(cx))
                    {
                        var vs = CTree<long, TypedValue>.Empty;
                        cx.cursors += (dp, e);
                        for (var b = dm.rowType.First(); b != null; b = b.Next())
                        {
                            var s = cx.obs[b.value()];
                            vs += (s.defpos, s.Eval(cx));
                        }
                        var rw = new TRow(dm, vs);
                        RTree.Add(ref wtree, rw, cx.cursors);
                    }
                }
            }
            cx.cursors = oc;
        }
        public override string ToString()
        {
            var s = new StringBuilder("{");
            if (count != 0L) { s.Append(count); s.Append(" "); }
            if (sb != null) { s.Append(sb); s.Append(" "); }
            if (row>=0) { s.Append(row); s.Append(" "); }
            switch (sumType.kind)
            {
                case Sqlx.COLLECT:
                case Sqlx.EVERY:
                case Sqlx.FUSION:
                case Sqlx.INTERSECTION:
                    if (mset!=null) s.Append(mset);
                    break;
                case Sqlx.INT:
                case Sqlx.INTEGER:
                    if (sumInteger is Integer i)
                        s.Append(i.ToString());
                    else
                        s.Append(sumLong.ToString());
                    break;
                case Sqlx.REAL:
                    s.Append(sum1); break;
                case Sqlx.NUMERIC:
                    s.Append(sumDecimal); break;
                case Sqlx.BOOLEAN:
                    s.Append(bval); break;
                case Sqlx.MAX:
                case Sqlx.MIN:
                    s.Append(acc); break;
                case Sqlx.STDDEV_POP:
                case Sqlx.STDDEV_SAMP:
                    s.Append(sum1); s.Append(" ");
                    s.Append(acc1); 
                    break;
            }
            s.Append("}");
            return s.ToString();
        }
    }
    /// <summary>
    /// A period specification occurs in a table reference: AS OF/BETWEEN/FROM
    /// shareable as of 26 April 2021
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
        public readonly SqlValue time1 = null;
        /// <summary>
        /// The second point in time specified
        /// </summary>
        public readonly SqlValue time2 = null;
        internal PeriodSpec(string n,Sqlx k,SqlValue t1,SqlValue t2)
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
        internal string etag => (string)mem[RestRowSet.ETag]; // RFC7232 Rvv string
        internal long highWaterMark => (long)(mem[HighWaterMark] ?? -1L); // Database length
        internal THttpDate assertUnmodifiedSince => (THttpDate)mem[AssertUnmodifiedSince];
        internal Rvv assertMatch => (Rvv)mem[AssertMatch]??Rvv.Empty;
        internal static ETag Empty = new ETag(BTree<long, object>.Empty); 
        public ETag(Database d,Rvv rv) :base(BTree<long,object>.Empty
            + (AssertMatch,rv) + (HighWaterMark,d.loadpos) 
            + (AssertUnmodifiedSince, new THttpDate(d.lastModified)))
        { }
        protected ETag(BTree<long, object> m) : base(m) { }
        public static ETag operator+ (ETag e,(long,object) x)
        {
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
            sb.Append(assertMatch); sb.Append("@");
            sb.Append(highWaterMark); sb.Append("=");
            sb.Append(assertUnmodifiedSince);
            return sb.ToString();
        }
    }
    /// <summary>
    /// Two of the fields in this structure are used for ETag validation (RFC7232)
    /// The highwatermark is used if W is specified.
    /// Otherwise the whole of the Rvv string is used for validation.
    /// </summary>
    internal class HttpParams : Basis
    {
        internal const long
            _Authorization = -443, // string
            DefaultCredentials = -444, // bool
            Url = -147; // string
        // local databasename or url of form http://hostname/db/role/table
        internal string url => (string)mem[Url];
        internal bool defaultCredentials => (bool)(mem[DefaultCredentials]??false);
        internal string authorization => (string)mem[_Authorization];
        internal HttpParams(string u) :base(new BTree<long,object>(Url,u)) { }
        protected HttpParams(BTree<long,object>m) : base(m) { }
        public static HttpParams operator+ (HttpParams h,(long,object)x)
        {
            return new HttpParams(h.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new HttpParams(m);
        }
    }
}
