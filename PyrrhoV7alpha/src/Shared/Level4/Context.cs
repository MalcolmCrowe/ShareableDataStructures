using System;
using System.Configuration;
using System.Diagnostics.Eventing.Reader;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Xml.Linq;
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
        internal CTree<long, bool> restRowSets = CTree<long, bool>.Empty;
        public long nextHeap = -1L, parseStart = -1L, oldStmt = -1L;
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
        internal CTree<long,CTree<long,bool>> forReview = CTree<long, CTree<long,bool>>.Empty; // SqlValue,RowSet
        internal bool inHttpService = false;
        internal CTree<long, Domain> groupCols = CTree<long, Domain>.Empty; // Domain; GroupCols for a Domain with Aggs
        internal BTree<long, BTree<TRow, BTree<long, Register>>> funcs = BTree<long, BTree<TRow, BTree<long, Register>>>.Empty; // Agg GroupCols
        internal BTree<long, BTree<long, TableRow>> newTables = BTree<long, BTree<long, TableRow>>.Empty;
        /// <summary>
        /// Left-to-right accumulation of definitions during a parse: accessed only by RowSet
        /// </summary>
        internal Ident.Idents defs = Ident.Idents.Empty;
        internal BTree<int, (long, Ident.Idents)> defsStore = BTree<int, (long, Ident.Idents)>.Empty; // saved defs for previous level
        internal int sD => (int)defsStore.Count; // see IncSD() and DecSD() below
        internal long offset = 0L; // set in Framing._Relocate, constant during relocation of compiled objects
        internal long lexical = 0L; // current select block, set in incSD()
        internal CTree<long, bool> undefined = CTree<long, bool>.Empty;
        /// <summary>
        /// Lexical positions to DBObjects (if dbformat<51)
        /// </summary>
        public BTree<long, (string, long)> digest = BTree<long, (string, long)>.Empty;
        // UnHeap things for Procedure, Trigger, and Constraint bodies
        internal BTree<long, long?> uids = BTree<long, long?>.Empty;
        internal long result = -1L;
        /// <summary>
        /// Used in Replace cascade
        /// </summary>
        internal ObTree done = ObTree.Empty;
        internal BList<(DBObject, DBObject, BTree<long,object>)> todo = 
            BList<(DBObject, DBObject, BTree<long,object>)>.Empty;
        internal static BTree<long,Func<object,object>> fixer = BTree<long,Func<object,object>>.Empty;
        internal bool replacing = false;
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
        internal CTree<long, bool> rdC = CTree<long, bool>.Empty; // read TableColumns defpos
        internal CTree<long, CTree<long, bool>> rdS = CTree<long, CTree<long, bool>>.Empty; // specific read TableRow defpos
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
                if (obs[ix.dp] is DBObject ob)
                {
                    if (ix.sd < sD && ob.GetType().Name == "SqlValue") // an undefined identifier from a lower level
                        return (null, n);
                    return ob._Lookup(lp, this, n.ident, sub);
                }
                if (ix.sd < sD) // an undefined identifier from a lower level
                    return (null, n);
            }
            return (null, sub);
        }
        internal CTree<long, bool> Needs(CTree<long, bool> nd,
        RowSet rs, Domain dm)
        {
            var s = (long)(rs.mem[RowSet._CountStar] ?? -1L);
            for (var b = dm?.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (p != s)
                    for (var c = obs[p].Needs(this, rs.defpos).First(); c != null; c = c.Next())
                    {
                        var u = c.key();
                        nd += (u, true);
                    }
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
        internal CTree<long, bool> Needs(CTree<long, bool> nd, CList<long> rt)
        {
            for (var b = rt?.First(); b != null; b = b.Next())
                nd += obs[b.value()].Needs(this);
            return nd;
        }
        internal CTree<long, bool> Needs(CTree<long, bool> nd, RowSet rs,
                CList<long> rt)
        {
            for (var b = rt.First(); b != null; b = b.Next())
                for (var c = obs[b.value()].Needs(this, rs.defpos).First(); c != null; c = c.Next())
                {
                    var u = c.key();
                    nd += (u, true);
                }
            return nd;
        }
        internal CTree<long, bool> Needs<V>(CTree<long, bool> nd, RowSet rs,
            BTree<long, V> wh)
        {
            for (var b = wh?.First(); b != null; b = b.Next())
                for (var c = obs[b.key()].Needs(this, rs.defpos).First(); c != null; c = c.Next())
                {
                    var u = c.key();
                    nd += (u, true);
                }
            return nd;
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
                    Math.Max(obs[b.key()]?.depth ?? 0, b.value().depth) + 1);
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
                if (e != "" && e != etag)
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
                    r = db.nextStmt;
                    db += (Database.NextStmt,r + n);
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
                    return db.nextStmt - 1;
                default:
                    return nextHeap - 1;
            }
        }
        PRow Filter(PRow f)
        {
            if (f == null)
                return null;
            var t = Filter(f._tail);
            if (f._head is TQParam q)
                return new PRow(values[q.qid.dp], t);
            return new PRow(f._head, t);
        }
        internal CTree<long, TypedValue> Filter(Table tb, CTree<long, bool> wh)
        {
            var r = CTree<long, TypedValue>.Empty;
            for (var b = wh.First(); b != null; b = b.Next())
                r += obs[b.key()].Add(this, r, tb);
            return r;
        }
        internal int Depth(CList<long> os, params DBObject[] ps)
        {
            var r = 1;
            for (var b = os?.First(); b != null; b = b.Next())
            {
                var d = obs[b.value()]?.depth ?? 1;
                if (d >= r)
                    r = d + 1;
            }
            foreach (var p in ps)
            {
                var d = p?.depth ?? -1;
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
                var d = p?.depth ?? -1;
                if (d >= r)
                    r = d + 1;
            }
            return r;
        }
        internal int Depth(CTree<long, bool> os, params DBObject[] ps)
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
                var d = p?.depth ?? -1;
                if (d >= r)
                    r = d + 1;
            }
            return r;
        }
        /// <summary>
        /// Symbol management stack
        /// </summary>
        internal void IncSD(Ident id)
        {
            defsStore += (sD, (id.iix.lp,defs));
            lexical = id.iix.lp;
        }
        /// <summary>
        /// Deal with end-of-Scope things 
        /// </summary>
        /// <param name="sm">The select list at this level</param>
        /// <param name="tm">The source table expression</param>
        internal void DecSD(Domain sm = null,RowSet te = null)
        {
            // we don't want to lose the current defs right away.
            // they will be needed for OrderBy and ForSelect bodies
            // Look at any undefined symbols in sm that are NOT in tm
            // and identify them with symbols at the lower level.
            var sd = sD;
            var (oldlx,ldefs) = defsStore[sd-1];
            for (var b = undefined.First(); b != null; b = b.Next())
                if (obs[b.key()] is SqlValue sv)
                    sv.Resolve(this, te?.defpos ?? result,BTree<long,object>.Empty);
            for (var b = sm?.rowType.First();b!=null;b=b.Next())
            {
                var uv = (SqlValue)obs[b.value()];
                var n = uv.name;
                if (uv.GetType().Name == "SqlValue" &&  // there is an unfdefined entry for n at the current level
                    !te.names.Contains(n))      // which is not in tm
                {
                    Iix dv = Iix.None;
                    var x = defsStore.Last().value().Item2;
                    if (x?.Contains(n) == true) // there is an entry for the previous level
                    {
                        Ident.Idents ds;
                        (dv, ds) = x[(n, sd - 1)];
                        defs += (n, dv, ds);   // update it
                    }
                    if (dv.dp >= 0 && obs[dv.dp] is SqlValue lv) // what is the current entry for n
                    {
                        if (_Dom(lv).kind == Sqlx.CONTENT || lv.GetType().Name == "SqlValue") // it has unknown type
                        {
                            var nv = uv.Relocate(lv.defpos);
                            Replace(lv, nv);
                            Replace(uv, nv);
                        }
                        else if (dv.dp >= Transaction.HeapStart) // was thought unreferenced at lower level
                            Replace(uv, lv);
                    }
                }
                // export the current level to the next one down
                if (sd>2 && uv.defpos<Transaction.Executables && defs.Contains(n))
                {
                    var tx = defs[n][sd].Item1;
                    ldefs += (uv.name, new Iix(tx.lp, sd - 1, tx.dp), Ident.Idents.Empty);
                } 
            }
            defsStore -= (sd - 1);
            // demote forward references for later resolution
            for (var b = defs.First(); b != null; b = b.Next())
            {
                var n = b.key();
                var t = defs[n];
                if (t.Contains(sd))// there is an entry for n at the current level
                {
                    var (px, cs) = t[sd];
                    if (cs != Ident.Idents.Empty    // re-enter forward references to be resolved at a lower level
                        && obs[px.dp] is ForwardReference fr)
                    {
                        for (var c = cs.First();c!=null;c=c.Next())
                        {
                            var cn = c.key();
                            var (cx,cc) = c.value()[sd];
                            cs += (cn, new Iix(cx.lp, px.sd - 1, cx.dp),cc);
                            fr += (Domain.RowType, fr.subs + (cx.dp, true));
                            obs += (fr.defpos, fr);
                        }
                        ldefs += (n, new Iix(px.lp, px.sd - 1, px.dp), cs);
                    }
                }
            }
            if (ldefs.Count!=0)
                defs = ldefs;
            lexical = oldlx;
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
        internal bool HasItem(CList<long> rt, long p)
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
            if (ph == null)
                return;
            if (lp == 0)
                lp = db.loadpos;
            if (PyrrhoStart.DebugMode && db is Transaction)
                Console.WriteLine(ph.ToString());
            db.Add(this, ph, lp);
        }
        internal void Add(Framing fr)
        {
            obs += fr.obs;
            for (var b = fr.depths.First(); b != null; b = b.Next())
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
                    && sv.name != "" && rp < Transaction.TransPos)
                    digest += (sv.defpos, (sv.name, rp));
                else if (ob is RowSet fm && (rp = fm.target) > 0 && fm.defpos > Transaction.TransPos
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
            if (dm != null && ob.domain >= 0 && ob.domain < Transaction.TransPos)
                obs += (ob.domain, dm);
            if (ob.mem.Contains(DBObject._Framing))
            {
                var t = ob.framing.obs.Last()?.key() ?? -1L;
                if (t > db.nextStmt)
                    db +=(Database.NextStmt,t);
            }
        }
        internal Context ForConstraintParse()
        {
            // Set up the information for parsing the generation rule
            // The table domain and cx.defs should contain the columns so far defined
            var cx = new Context(this);
            cx.parse = ExecuteStatus.Compile;
            var rs = CTree<long, Domain>.Empty;
            Ident ti = null;
            Table tb = null;
            for (var b = cx.obs?.PositionAt(0); b != null; b = b.Next())
                if (b.value().infos[role.defpos] is ObInfo oi)
                {
                    var ox = Ix(b.key());
                    var ic = new Ident(oi.name, ox);
                    var dm = cx._Dom(b.key());
                    if (dm == null)
                        continue;
                    if (dm.kind == Sqlx.TABLE)
                    {
                        ti = ic;
                        tb = b.value() as Table;
                    }
                    else if (ti != null)
                    {
                        cx.defs += (ic, ox);
                        ic = new Ident(ti, ic);
                        rs += (b.key(), dm);
                    }
                    cx.defs += (ic, ox);
                }
            if (tb != null)
                cx.Add(tb + (Table.TableCols, rs));
            return cx;
        }
        internal Domain _DomAdd(Context cx, Domain dm, SqlValue sv)
        {
            var r = new Domain(GetUid(), cx, dm.kind, dm.representation + (sv.defpos, cx._Dom(sv)),
                dm.rowType + sv.defpos);
            obs += (r.defpos, r);
            return r;
        }
        internal Domain _Dom(DBObject ob)
        {
            if (ob == null)
                return null;
            if (ob is Domain)
                return (Domain)ob;
            if (ob is Table t)
                obs += t.framing.obs;
            return (Domain)obs[ob.domain] ?? (Domain)db.objects[ob.domain] ?? Domain.Content;
        }
        internal Domain _Dom(long dp)
        {
            var ob = _Ob(dp);
            return (ob is Domain d) ? d : (Domain)_Ob(ob?.domain ?? -1L);
        }
        internal Domain _Dom(long dp, CList<long> rt)
        {
            var ob = obs[dp] ?? (DBObject)db.objects[dp];
            var dm = _Dom(ob);
            if (dm.rowType.CompareTo(rt) == 0)
                return dm;
            if (dm.defpos < Transaction.Executables)
                dm = (Domain)dm.Relocate(GetUid());
            var r = dm + (Domain.RowType, rt);
            obs += (r.defpos, dm);
            return r;
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
            var r = dm + (Domain.Representation, rs + (p, d))
                + (Domain.RowType, rt);
            obs += (r.defpos, r);
            return r;
        }
        internal Domain _Dom(long dp, params (long, object)[] xs)
        {
            var ob = obs[dp] ?? (DBObject)db.objects[dp];
            var dm = _Dom(ob);
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
            var r = (Domain)dm.New(m);
            obs += (r.defpos, r);
            return r;
        }
        internal BTree<long, object> Name(Ident n, BTree<long, object> m = null)
        {
            return (m ?? BTree<long, object>.Empty)
                + (DBObject.Infos, new BTree<long, ObInfo>(role.defpos, new ObInfo(n.ident)))
                + (ObInfo.Name,n.ident) + (DBObject._Ident,n);
        }
        internal DBObject _Add(DBObject ob)
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
            for (var b = depths.First(); b != null; b = b.Next())
                depths += (b.key(), b.value() - dp);
        }
        internal void AddRowSetsPair(SqlValue a, SqlValue b)
        {
            if (obs[a.from] is RowSet ra)
                obs += (a.from, ra + (a.defpos, b.defpos));
            if (obs[b.from] is RowSet rb)
                obs += (a.from, rb + (a.defpos, b.defpos));
        }
        internal BTree<long,object> Replace(DBObject was, DBObject now, BTree<long,object> m = null)
        {
            m = m ?? BTree<long, object>.Empty;
            if (was == now)
                return m;
            _Add(now);
            ATree<int, (DBObject, DBObject, BTree<long,object>)> a = todo;
            ATree<int, (DBObject, DBObject, BTree<long,object>)>.Add(ref a, 0, (was, now, m));
            todo = (BList<(DBObject, DBObject,BTree<long,object>)>)a;
            var ours = false;
            if (!replacing)
                while (todo.Length > 0)
                {
                    replacing = true;
                    var (w, n, mm) = todo[0];
                    ours = mm == m;
                    todo -= 0;
                    var ob = _Ob(w.defpos);
                    if (ob == null)
                        Console.WriteLine("Replace target removed: " + w.ToString());
                    else // maybe some other tests for reasonable behaviour?
                        mm = DoReplace(w, n, mm);
                    if (ours)
                        m = mm;
                }
            replacing = false;
            return m;
        }
        int rct = 0;
        /// <summary>
        /// Update the query processing context in a cascade to implement a single replacement!
        /// </summary>
        /// <param name="was"></param>
        /// <param name="now"></param>
        BTree<long,object> DoReplace(DBObject was, DBObject now,BTree<long,object>m)
        {
            done = new ObTree(now.defpos,now);
            if (was.defpos != now.defpos)
                done += (was.defpos, now);
            // scan by depth to perform the replacement
            rct++;
            var ldpos = db.loadpos;
            var excframing = parse!=ExecuteStatus.Compile &&
                (was.defpos<Transaction.Executables || was.defpos>=Transaction.HeapStart);
            for (var b = depths.First(); b != null; b = b.Next())
            {
                var bv = b.value();
                for (var c = bv.PositionAt(ldpos); c != null; c = c.Next())
                {
                    var k = c.key();
                    if (excframing && k >= Transaction.Executables
                        && k < Transaction.HeapStart)
                        continue;
                    var p = c.value();
                    var cv = obs[p.defpos]._Replace(this, was, now); // may update done
                    if (cv != p)
                        bv += (k, cv);
                }
                for (var c = forReview.First();c!=null;c=c.Next())
                {
                    var k = c.key();
                    if (obs[k].depth != b.key())
                        continue;
                    var nv = done[k] ?? obs[k];
                    var nk = nv.defpos;
                    if (k!=nk)
                    {
                        forReview -= k;
                        forReview += (nk, c.value());
                    }
                }
                if (bv != b.value())
                    depths += (b.key(), bv);
            }
            m = Replaced(m);
            for (var b=done.First();b!=null;b=b.Next())
                obs += (b.key(), b.value());
            defs = defs.ApplyDone(this);
            if (was.defpos != now.defpos && (parse==ExecuteStatus.Compile ||
                was.defpos<Transaction.Executables || was.defpos>=Transaction.HeapStart))
               _Remove(was.defpos);
            return m;
        }
        internal long ObReplace(long dp, DBObject was, DBObject now)
        {
            if (dp < Transaction.TransPos || !obs.Contains(dp))
                return dp;
            if (done.Contains(dp))
                return done[dp].defpos;
            if (obs[dp] == null)
                throw new PEException("PE111");
            return obs[dp]?._Replace(this, was, now)?.defpos ??
                throw new PEException("PE111");
        }
        internal long Replaced(long p)
        {
            return done.Contains(p)? done[p].defpos: p;
        }
        internal CTree<K, long> Replaced<K>(CTree<K, long> ms) where K : IComparable
        {
            var r = CTree<K, long>.Empty;
            var ch = false;
            for (var b = ms.First(); b != null; b = b.Next())
            {
                var p = b.value();
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
            for (var b = ms?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var m = Replaced(k);
                ch = ch || m != k;
                r += (m, b.value());
            }
            return ch ? r : ms;
        }
        internal CTree<CList<long>,CTree<long,bool>> ReplacedTLllb(CTree<CList<long>,CTree<long,bool>> xs)
        {
            var r = CTree<CList<long>,CTree<long,bool>>.Empty;
            var ch = false;
            for (var b = xs.First(); b != null; b = b.Next())
            {
                var c = b.value();
                var k = b.key();
                var nk = ReplacedLl(k);
                var nc = ReplacedTlb(c);
                ch = ch || k.CompareTo(nk)!=0 || c.CompareTo(nc)!=0;
                r += (nk, nc);
            }
            return ch?r:xs;
        }
        internal CList<long> ReplacedLl(CList<long> ks)
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
        internal CTree<long, TypedValue> ReplacedTlV(CTree<long, TypedValue> vt)
        {
            var r = CTree<long, TypedValue>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Replaced(k);
                var v = b.value().Replaced(this);
                if (p != b.key() || v != b.value())
                    ch = true;
                r += (p, v);
            }
            return ch ? r : vt;
        }
        internal CTree<long,bool> ReplacedTlb(CTree<long,bool> wh)
        {
            var r = CTree<long,bool>.Empty;
            var ch = false;
            for (var b = wh?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var nk = done[k]?.defpos ?? k;
                ch = ch || k != nk;
                r += (nk, b.value());
            }
            return ch?r:wh;
        }
        internal CTree<long, Domain> ReplacedTlD(CTree<long, Domain> rs)
        {
            var r = CTree<long, Domain>.Empty;
            var ch = false;
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Replaced(k);
                var d = b.value().Replaced(this);
                ch = ch || p != k || d != b.value();
                r += (p, d);
            }
            return ch ? r : rs;
        }
        internal CTree<Domain,bool> ReplacedTDb(CTree<Domain,bool> ts)
        {
            var r = CTree<Domain, bool>.Empty;
            var ch = false;
            for (var b=ts.First();b!=null;b=b.Next())
            {
                var d = b.key();
                var nd = d.Replaced(this);
                ch = ch || d != nd;
                r += (nd,b.value());
            }
            return ch ? r: ts;
        }
        internal CTree<long, CTree<long, bool>> ReplacedTTllb(CTree<long, CTree<long, bool>> ma)
        {
            var r = CTree<long, CTree<long, bool>>.Empty;
            var ch = false;
            for (var b = ma?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Replaced(k);
                var v = ReplacedTlb(b.value());
                ch = ch || p != k || v != b.value();
                r += (p, v);
            }
            return ch ? r : ma;
        }
        internal CTree<long,long> ReplacedTll(CTree<long,long> wh)
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
        internal CTree<UpdateAssignment, bool> ReplacedTUb(CTree<UpdateAssignment, bool> us)
        {
            var r = CTree<UpdateAssignment, bool>.Empty;
            var ch = false;
            for (var b = us.First(); b != null; b = b.Next())
            {
                var ua = (UpdateAssignment)b.key();
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
            for (var b = cs?.First(); b != null; b = b.Next())
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
            {
                var k = b.key();
                var nk = Replaced(k);
                var v = b.value();
                var nv = ReplacedBV(v);
                ch = ch || nk != k || nv != v;
                r += (k,nv);
            }
            return ch ? r : t;
        }
        internal BList<TypedValue> ReplacedBV(BList<TypedValue> vs)
        {
            var r = BList<TypedValue>.Empty;
            var ch = false;
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var v = b.value();
                var nv = v.Replaced(this);
                ch = ch ||nv != v;
                r += nv;
            }
            return ch ? r : vs;
        }
        internal BList<(long,TRow)> ReplacedBlT(BList<(long,TRow)> rs)
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
        internal CTree<string, long> ReplacedTsl(CTree<string, long> wh)
        {
            var r = CTree<string, long>.Empty;
            var ch = false;
            for (var b = wh.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = b.value();
                var nb = done[p];
                var nk = nb?.alias ?? k;
                var np = nb?.defpos ?? p;
                ch = ch || p != np || k != nk;
                r += (nk, np);
            }
            return ch ? r : wh;
        }
        internal CTree<long, TypedValue> ReplaceTlT(CTree<long, TypedValue> wh,
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
        /// <summary>
        /// Support for cx.Replace(was,now,m) during Parsing.
        /// Update a separate list of properties accordingly.
        /// We don't worry about _Depth because that will be fixed in 
        /// the main Replace implementation, Apply, constructors etc.
        /// </summary>
        /// <param name="m">A list of properties</param>
        /// <returns>The updated list</returns>
        internal BTree<long,object> Replaced(BTree<long,object> m)
        {
            var r = BTree<long,object>.Empty;
            for (var b = m.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = b.value();
                switch (k)
                {
                    case HandlerStatement.Action: v = Replaced((long)v); break;
                    case Index.Adapter: v = Replaced((long)v); break;
                    case Domain.Aggs:       v = ReplacedTlb((CTree<long, bool>)v); break;
                    case SqlValueArray.Array:       v = ReplacedLl((CList<long>)v); break;
                    case SqlSelectArray.ArrayValuedQE:  v= Replaced((long)v); break;
                    case RowSet.Assig:      v = ReplacedTUb((CTree<UpdateAssignment, bool>)v); break;
                    case SqlXmlValue.Attrs: v = ReplacedLXl((BList<(SqlXmlValue.XmlName, long)>)v); break;
                    case Procedure.Body:    v = Replaced((long)v); break;
                    case CallStatement.Parms:   v = ReplacedLl((CList<long>)v); break;
                    case SqlCall.Call:          v = Replaced((long)v); break;
                    case SqlCaseSimple.CaseElse: v = Replaced((long)v); break;
                    case SqlCaseSimple.Cases:   v = ReplacedBll((BList<(long, long)>)v); break;
                    case SqlXmlValue.Children: v = ReplacedLl((CList<long>)v); break;
                    case SqlValue.Cols:     v = ReplacedLl((CList<long>)v); break;
                    case WhenPart.Cond: v = Replaced((long)v); break;
                    case Check.Condition:   v = Replaced((long)v); break;
                    case SqlXmlValue.Content:   v = Replaced((long)v); break;
                    case FetchStatement.Cursor: v = Replaced((long)v); break;
                    case RowSet._Data:          v = Replaced((long)v); break;
                    case Domain.DefaultRowValues: v = ReplacedTlV((CTree<long, TypedValue>)v); break;
                    case DBObject._Domain:  v = Replaced((long)v); break;
                    case IfThenElse.Else:   v = ReplacedLl((CList<long>)v); break;
                    case SimpleCaseStatement.Else:  v = ReplacedLl((CList<long>)v); break;
                    case IfThenElse.Elsif: v = ReplacedLl((CList<long>)v); break;
                    case LikePredicate.Escape: v = Replaced((long)v); break;
                    case ExplicitRowSet.ExplRows: v = ReplacedBlT((BList<(long,TRow)>)v); break;
                    case SqlValueSelect.Expr:   v = Replaced((long)v); break;
                    case SqlField.Field:    v = Replaced((long)v); break;
                    case UDType.Fields:     v = ReplacedLl((CList<long>)v); break;
                    case RowSet.Filter:     v = ReplacedTlV((CTree<long, TypedValue>)v); break;
                    case SqlFunction.Filter:    v = ReplacedTlb((CTree<long, bool>)v); break;
                    case GenerationRule.GenExp: v = Replaced((long)v); break;
                    case RowSet.Group:          v = Replaced((long)v); break;
                    case RowSet.Groupings: v = ReplacedLl((CList<long>)v); break;
                    case RowSet.GroupIds: v = ReplacedTlD((CTree<long,Domain>)v); break;
                    case RowSet.Having:     v = ReplacedTlb((CTree<long, bool>)v); break;
                    case QuantifiedPredicate.High: v = Replaced((long)v); break;
                    case LocalVariableDec.Init: v = Replaced((long)v); break;
                    case SqlInsert.InsCols:     v = ReplacedLl((CList<long>)v); break;
                    case Procedure.Inverse:     v = Replaced((long)v); break;
                    case RowSet.ISMap:      v = ReplacedTll((CTree<long, long>)v); break;
                    case JoinRowSet.JFirst: v = Replaced((long)v); break;
                    case JoinRowSet.JoinCond:   v = ReplacedTlb((CTree<long, bool>)v); break;
                    case JoinRowSet.JoinUsing:  v = ReplacedTll((CTree<long, long>)v); break;
                    case JoinRowSet.JSecond: v = Replaced((long)v); break;
                    case Index.Keys:            v = ReplacedLl((CList<long>)v); break;
                    case MergeRowSet._Left:     v = Replaced((long)v); break;
                    case SqlValue.Left:     v = Replaced((long)v); break;
                    case MemberPredicate.Lhs:   v = Replaced((long)v); break;
                    case GetDiagnostics.List:   v = Replaced((CTree<long, Sqlx>)v); break;
                    case MultipleAssignment.List:   v = ReplacedLl((CList<long>)v); break;
                    case QuantifiedPredicate.Low: v = Replaced((long)v); break;
                    case RowSet._Matches:       v = ReplacedTlV((CTree<long, TypedValue>)v); break;
                    case RowSet.Matching:       v = ReplacedTTllb((CTree<long, CTree<long, bool>>)v); break;
                    case Grouping.Members:      v = Replaced((CTree<long, int>)v); break;
                    case ObInfo.Names:          v = ReplacedTsl((CTree<string, long>)v); break;
                    case RowSet._Needed:        v = ReplacedTlb((CTree<long,bool>)v); break;
                    case NullPredicate.NVal:    v = Replaced((long)v); break;
                    case JoinRowSet.OnCond:  v = ReplacedTll((CTree<long, long>)v); break;
                    case SqlFunction.Op1:       v = Replaced((long)v); break;
                    case SqlFunction.Op2:       v = Replaced((long)v); break;
                    case SimpleCaseStatement._Operand: v = Replaced((long)v); break;
                    case WindowSpecification.Order:     v = Replaced((long)v); break;
                    case Domain.OrderFunc:  v = Replaced((long)v); break;
                    case RowSet.OrdSpec:    v = ReplacedLl((CList<long>)v); break;
                    case FetchStatement.Outs: v = ReplacedLl((CList<long>)v); break;
                    case SelectSingle.Outs:     v = ReplacedLl((CList<long>)v); break;
                    case Procedure.Params:      v = ReplacedLl((CList<long>)v); break;
                    case SqlField.Parent:       v = Replaced((long)v); break;
                    case WindowSpecification.PartitionType:     v = Replaced((long)v); break;
                //    case RowSet.Periods:        v = Replaced
                    case CallStatement.ProcDefPos: v = Replaced((long)v); break;
                    case PreparedStatement.QMarks:  v = ReplacedLl((CList<long>)v); break;
                    case SelectRowSet.RdCols:       v = ReplacedTlb((CTree<long, bool>)v); break;
                    case RowSet.Referenced:         v = ReplacedTlb((CTree<long, bool>)v); break;
                    case Index.References:      v = ReplacedBBlV((BTree<long, BList<TypedValue>>)v); break;
                    case Index.RefIndex:        v = Replaced((long)v); break;
                    case Domain.Representation: v = ReplacedTlD((CTree<long, Domain>)v); break;
                    case RowSet.RestRowSetSources:  v = ReplacedTlb((CTree<long, bool>)v); break;
                    case RestRowSetUsing.RestTemplate: v = Replaced((long)v); break;
                    case ReturnStatement.Ret:   v = Replaced((long)v); break;
                    case MemberPredicate.Rhs:   v = Replaced((long)v); break;
                    case MultipleAssignment.Rhs:    v = Replaced((long)v); break;
                    case MergeRowSet._Right:     v = Replaced((long)v); break;
                    case SqlValue.Right:     v = Replaced((long)v); break;
                    case RowSet.RowOrder:   v = ReplacedLl((CList<long>)v); break;
                    case Domain.RowType:    v = ReplacedLl((CList<long>)v); break;
                    case RowSetPredicate.RSExpr: v = Replaced((long)v); break;
                    case RowSet.RSTargets:      v = ReplacedTll((CTree<long, long>)v);  break;
                    case SqlDefaultConstructor.Sce: v = Replaced((long)v); break;
                    case IfThenElse.Search: v = Replaced((long)v); break;
                    case WhileStatement.Search: v = Replaced((long)v); break;
                    case ForSelectStatement.Sel:    v = Replaced((long)v); break;
                    case QuantifiedPredicate._Select: v = Replaced((long)v); break;
                    case SignalStatement.SetList: v = Replaced((CTree<Sqlx,long>)v); break;
                    case GroupSpecification.Sets: v = ReplacedLl((CList<long>)v); break;
                    case RowSet.SIMap:      v = ReplacedTll((CTree<long, long>)v); break;
                    case RowSet._Source:    v = Replaced((long)v); break;
                    case SqlCursor.Spec:            v = Replaced((long)v); break;
                    case SqlRowSet.SqlRows:         v = ReplacedLl((CList<long>)v); break;
                    case InstanceRowSet.SRowType:   v = ReplacedLl((CList<long>)v); break;
                    case RowSet.Stem:       v = ReplacedTlb((CTree<long, bool>)v); break;
                    case CompoundStatement.Stms:    v = ReplacedLl((CList<long>)v); break;
                    case ForSelectStatement.Stms: v = ReplacedLl((CList<long>)v); break;
                    case WhenPart.Stms:     v = ReplacedLl((CList<long>)v); break;
                    case Domain.Structure:  v = Replaced((long)v); break;
                    case SqlValue.Sub:     v = Replaced((long)v); break;
                    case SqlValueArray.Svs: v = Replaced((long)v); break;
                    case Index.TableDefPos: v = Replaced((long)v); break;
                    case RowSet.Target: v= Replaced((long)v); break;
                    case IfThenElse.Then: v = ReplacedLl((CList<long>)v); break;
                    case SqlTreatExpr.TreatExpr: v = Replaced((long)v); break;
                    case SelectStatement.Union: v = Replaced((long)v); break;
                    case Domain.UnionOf:    v = ReplacedTDb((CTree<Domain, bool>)v); break;
           //         case RowSet.UnReferenced:   v = ReplacedTlD((CTree<long, Domain>)v); break;
                    case RestRowSetUsing.UrlCol: v = Replaced((long)v); break;
                    case RestRowSetUsing.UsingCols: v = ReplacedLl((CList<long>)v); break;
                    case RowSet.UsingOperands:  v = ReplacedTll((CTree<long, long>)v); break;
                    case WhileStatement.What:   v = ReplacedLl((CList<long>)v); break;
                    case SimpleCaseStatement.Whens: v = ReplacedLl((CList<long>)v); break;
                    case FetchStatement.Where:  v = Replaced((long)v); break;
                    case RowSet._Where:     v = ReplacedTlb((CTree<long, bool>)v); break;
                    case RowSet.Windows:     v = ReplacedTlb((CTree<long, bool>)v); break;
                    case WindowSpecification.WQuery: v = Replaced((long)v); break;
                    case AssignmentStatement.Val:   v = Replaced((long)v); break;
                    case SqlFunction._Val: v = Replaced((long)v); break;
                    case UpdateAssignment.Val:   v = Replaced((long)v); break;
                    case QuantifiedPredicate.Vals: v = ReplacedLl((CList<long>)v); break;
                    case SqlInsert.Value:           v = Replaced((long)v); break;
                    case SelectRowSet.ValueSelect: v = Replaced((long)v); break;
                    case CallStatement.Var:         v = Replaced((long)v); break;
                    case AssignmentStatement.Vbl:   v = Replaced((long)v); break;
                    case QuantifiedPredicate.What: v = Replaced((long)v); break;
                    case QuantifiedPredicate.Where: v = Replaced((long)v); break;
                    case SqlFunction.Window: v = Replaced((long)v); break;
                    case SqlFunction.WindowId: v = Replaced((long)v); break;
                    case UpdateAssignment.Vbl:   v = Replaced((long)v); break;
                    default:                break;
                }
                r += (k, v);
            }
            return r;
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
                        if (r >= db.nextStmt)
                            db += (Database.NextStmt,r + 1);
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
            Ident ia = null;
            if (a != null)
            {
                defs += (a, id.iix, Ident.Idents.Empty);
                ia = new Ident(a, id.iix);
            }
            for (var b = dm?.rowType.First(); b != null;// && b.key() < dm.display; 
                b = b.Next())
            {
                var p = b.value();
                var px = Ix(id.iix.lp,p);
                var n = NameFor(p);
                if (n == null)
                    continue;
                var ic = new Ident(n, px);
                var iq = new Ident(id, ic);
                if (defs[iq].dp < 0)
                    defs += (iq, ic.iix);
                if (defs[ic].dp < 0)
                    defs += (ic, ic.iix);
                if (ia != null)
                    defs += (new Ident(ia, ic), ic.iix);
            }
        }
        internal void UpdateDefs(Ident ic, RowSet rs,string a)
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
                        if (b.value() != BTree<int, (Iix, Ident.Idents)>.Empty
                            && _Ob(b.value().Last().value().Item1.dp) is SqlValue ov)
                        {
                            var p = rs.names[b.key()];
                            ov.Define(this, ix, p, rs, _Ob(p));
                        }
                    }
                }
            }
            if (defs.Contains(a)) // care: our name may have occurred earlier (for a different instance)
            {
                (ix, ids) = defs[(a, ic.iix.sd)];
                if (obs[ix.dp] is ForwardReference)
                {
                    for (var b = ids.First(); b != null; b = b.Next())
                        if (b.value() != BTree<int, (Iix, Ident.Idents)>.Empty
                            && _Ob(b.value().Last().value().Item1.dp) is SqlValue ov)
                        {
                            var p = rs.names[b.key()];
                            ov.Define(this, ix, p, rs, _Ob(p));
                        }
                }
            }
            ids = defs[(a ?? rs.name, sD)].Item2;
            var dm = _Dom(rs);
            for (var b = dm.rowType.First(); b != null; b = b.Next())
            {
                // update defs with the newly defined entries and their aliases
                var c = (SqlValue)obs[b.value()];
                if (c.name != "")
                {
                    var cix = new Iix(ic.iix.lp, this, c.defpos);
                    ids += (c.name, cix, Ident.Idents.Empty);
                    var pt = defs[c.name];
                    if (pt==null || pt[cix.sd].Item1 is Iix nx && 
                        nx.dp >= 0 && (nx.sd < ic.iix.sd || nx.lp==nx.dp))
                        defs += (c.name, cix, Ident.Idents.Empty);
                    if (c.alias != null)
                    {
                        ids += (c.alias, cix, Ident.Idents.Empty);
                        defs += (c.alias, cix, Ident.Idents.Empty);
                    }
                }
            }
            defs += (a ?? rs.name, ic.iix, ids);
        }
        internal void AddParams(Procedure pr)
        {
            var zx = Ix(0);
            var pi = new Ident(pr.infos[role.defpos].name, zx);
            for (var b = pr.ins.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var pp = (FormalParameter)obs[p];
                var pn = new Ident(pp.name, zx);
                var pix = new Iix(pp.defpos);
                defs += (pn, pix);
                defs += (new Ident(pi, pn), pix);
                values += (p, TNull.Value); // for KnownBy
                Add(pp);
            }
        }
        internal string NameFor(long p)
        {
            var ob = _Ob(p) ?? throw new NullReferenceException();
            return ob.infos[role.defpos]?.name ??
                (string)ob.mem[DBObject._Alias] ??
                (string)ob.mem[ObInfo.Name] ??  "?";
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
        internal Procedure GetProcedure(long lp,string n,CList<Domain> a)
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
        internal CList<Domain> Signature(CList<long> ins)
        {
            var r = CList<Domain>.Empty;
            for (var b = ins.First(); b != null; b = b.Next())
                r += _Dom(b.value());
            return r;
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
        internal Ident FixI(Ident id)
        {
            if (id == null)
                return null;
            return new Ident(id.ident, Fix(id.iix), FixI(id.sub));
        }
        internal CList<long> FixLl(CList<long> ord)
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
        internal BList<Domain> FixBD(BList<Domain> ds)
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
        internal BList<Grouping> FixBG(BList<Grouping> gs)
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
        internal CTree<UpdateAssignment,bool> FixTub(CTree<UpdateAssignment,bool> us)
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
        internal BList<TypedValue> FixBV(BList<TypedValue> key)
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
        internal CTree<long,bool> FixTlb(CTree<long,bool> fi)
        {
            var r = CTree<long,bool>.Empty;
            var ch = false;
            for (var b = fi?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                if (p != b.key())
                    ch = true;
                r += (p,true);
            }
            return ch ? r : fi;
        }
        internal BTree<long, Cursor> FixBlC(BTree<long, Cursor> vt)
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
        internal BList<BTree<long, Cursor>> FixBBlC(BList<BTree<long, Cursor>> vt)
        {
            var r = BList<BTree<long, Cursor>>.Empty;
            var ch = false;
            for (var b = vt?.First(); b != null; b = b.Next())
            {
                var v = FixBlC(b.value());
                if (v != b.value())
                    ch = true;
                r += v;
            }
            return ch ? r : vt;
        }
        internal CTree<long, TypedValue> FixTlV(CTree<long, TypedValue> vt)
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
        internal CTree<string,TypedValue> FixTsV(CTree<string, TypedValue> a)
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
        internal CList<TXml> FixLX(CList<TXml> cl)
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
        internal BList<(SqlXmlValue.XmlName,long)> FixLXl(BList<(SqlXmlValue.XmlName, long)> cs)
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
        internal CTree<TypedValue,long> FixTVl(CTree<TypedValue,long> mu)
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
        internal CTree<string,long> FixTsl(CTree<string,long>cs)
        {
            var r = CTree<string, long>.Empty;
            for (var b = cs?.First(); b != null; b = b.Next())
            {
                var p = b.value();
                r += (b.key(), Fix(p));
            }
            return r;
        }
        internal BList<(long,long)> FixLll(BList<(long,long)> cs)
        {
            var r = BList<(long,long)>.Empty;
            for (var b = cs.First();b!=null; b=b.Next())
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
            for (var b = ma?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = Fix(k);
                var v = FixTlb(b.value());
                ch = ch || p != k || v != b.value();
                r += (p, v);
            }
            return ch ? r : ma;
        }
        internal CTree<CList<long>,CTree<long,bool>> FixTLTllb(CTree<CList<long>,CTree<long,bool>> xs)
        {
            var r = CTree<CList<long>, CTree<long, bool>>.Empty;
            var ch = false;
            for (var b = xs?.First(); b != null; b = b.Next())
            {
                var k = b.key();
                var p = FixLl(k);
                var v = FixTlb(b.value());
                ch = ch || p != k || v != b.value();
                r += (p, v);
            }
            return ch ? r : xs;
        }
        internal CTree<long,Domain> FixTlD(CTree<long,Domain> rs)
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
        internal CTree<long, long> FixTll(CTree<long, long> rs)
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
        internal CTree<PTrigger.TrigType, CTree<long, bool>> FixTTElb(CTree<PTrigger.TrigType, CTree<long, bool>> t)
        {
            var r = CTree<PTrigger.TrigType, CTree<long, bool>>.Empty;
            for (var b = t.First(); b != null; b = b.Next())
            {
                var p = b.key();
                r += (p, FixTlb(b.value()));
            }
            return r;
        }

        internal string ToString(CList<long> ins, CList<Domain> signature, Domain ret)
        {
            var sb = new StringBuilder();
            var cm = "";
            sb.Append('(');
            var a = ins.First();
            for (var b = signature.First(); a!=null && b != null; a=a.Next(), b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(NameFor(a.value()));
                sb.Append(' ');
                sb.Append(b.value().Name());
            }
            sb.Append(')');
            if (ret!=null && ret != Domain.Null)
            {
                sb.Append(" returns ");
                sb.Append(ret.Name());
            }
            return sb.ToString();
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
            if (ps.framing.obs.Last() is ABookmark<long,DBObject> b)
                _nextPrep = b.key();
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
        Framing() { }
        Framing(BTree<long,object> m) :base(m) 
        { }
        internal Framing(Context cx,long nst) : base(_Mem(cx,nst))
        {
            cx.oldStmt = cx.db.nextStmt;
        }
        static BTree<long, object> _Mem(Context cx,long nst)
        {
            var r = BTree<long, object>.Empty + (Result, cx.result)
                  + (Rvv.RVV, cx.affected);
            var os = ObTree.Empty;
            var ds = BTree<int, ObTree>.Empty;
            for (var b = cx.obs.PositionAt(Transaction.Executables); b != null; b = b.Next())
            {
                var k = b.key();
                if (cx.parse!=ExecuteStatus.Prepare &&
                    (k < nst || k >= Transaction.HeapStart)) // we only want new executables
                    continue;
                var v = b.value();
                var d = ds[v.depth] ?? ObTree.Empty;
                os += (k, v);
                ds += (v.depth, d + (k, v)); // only depth for new executables
            }
            return r + (Obs, os) + (Depths, ds);
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
            if (p < Transaction.TransPos)
                return f;
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
            var oc = cx.values;
            if (sf.window >=0L)
            {
                var t1 = cx.funcs[sf.from] ?? BTree<TRow, BTree<long, Register>>.Empty;
                var t2 = t1[key] ?? BTree<long, Register>.Empty;
                t2 += (sf.defpos, this);
                t1 += (key, t2);
                cx.funcs += (sf.from, t1);  // prevent stack oflow
                var dp = sf.window;
                var ws = (WindowSpecification)cx.obs[dp];
                var dm = cx._Dom(ws.order)??cx._Dom(ws.partition);
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
            cx.values = oc;
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
