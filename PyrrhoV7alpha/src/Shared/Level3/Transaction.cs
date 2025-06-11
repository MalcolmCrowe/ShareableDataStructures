using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System;
using System.Net;
using System.Transactions;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{
    /// <summary>
    /// DBObjects with transaction uids are add to the transaction's tree of objects.
    /// Transaction itself is not shareable because Physicals are mutable.
    /// 
    /// WARNING: Each new Physical for a transaction must be added to the Context
    /// so that Transaction gets a chance to update nextPos. Make sure you understand this fully
    /// before you add any code that creates a new Physical.
    /// shareable as at 26 April 2021
    /// </summary>
    internal class Transaction : Database
    {
        internal const long
            AutoCommit = -278, // bool
            Diagnostics = -280, // BTree<Qlx,TypedValue>
            _ETags = -374, // CTree<string,string> url, ETag
            Physicals = -79, // BTree<long,Physical>
            Posts = -398, // bool
            StartTime = -394, // DateTime
            Step = -276, // long
            TriggeredAction = -288; // long
        public BTree<Qlx, TypedValue> diagnostics =>
            (BTree<Qlx, TypedValue>)(mem[Diagnostics] ?? BTree<Qlx, TypedValue>.Empty);
        internal override long uid => (long)(mem[NextId] ?? -1L);
        public override long lexeroffset => uid;
        internal long step => (long)(mem[Step] ?? TransPos);
        internal override long nextPos => (long)(mem[NextPos] ?? TransPos);
        internal BTree<long, Physical> physicals =>
            (BTree<long, Physical>)(mem[Physicals] ?? BTree<long, Physical>.Empty);
        internal DateTime startTime => (DateTime?)mem[StartTime] ?? throw new PEException("PE48172");
        internal override bool autoCommit => (bool)(mem[AutoCommit] ?? true);
        internal bool posts => (bool)(mem[Posts] ?? false);
        internal long triggeredAction => (long)(mem[TriggeredAction] ?? -1L);
        internal CTree<string, string> etags =>
            (CTree<string, string>)(mem[_ETags] ?? CTree<string, string>.Empty);
        /// <summary>
        /// Physicals, SqlValues and Executables constructed by the transaction
        /// will use virtual positions above this mark (see PyrrhoServer.nextIid)
        /// </summary>
        public const long TransPos = 0x4000000000000000;
        public const long Analysing = 0x5000000000000000;
        public const long Executables = 0x6000000000000000;
        // actual start of Heap is given by conn.nextPrep for the connection (see Context(db))
        public const long HeapStart = 0x7000000000000000; //so heap starts after prepared statements
        /// <summary>
        /// As created from the Database: 
        /// via db.mem below we inherit its objects, and the session user and role
        /// </summary>
        /// <param name="db"></param>
        /// <param name="t"></param>
        /// <param name="auto"></param>
        internal Transaction(Database db, long t, bool auto)
            : base(db, db.mem + (NextId, t + 1) + (StartTime, DateTime.Now)
            + (AutoCommit, auto))
        { }
        protected Transaction(Transaction t, BTree<long, object> m)
            : base(t, m)
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new Transaction(this, m);
        }
        public override Transaction Transact(long t, Connection con, bool? auto = null)
        {
            var r = this;
            if (auto == false && autoCommit)
                r += (AutoCommit, false);
            r += (Step, r.nextPos);
            return r;
        }
        public override Database RdrClose(ref Context cx)
        {
            cx.values = CTree<long, TypedValue>.Empty;
            cx.cursors = BTree<long, TRow>.Empty;
            cx.obs = ObTree.Empty;
            cx.result = null;
            cx.binding = CTree<long, TypedValue>.Empty;
            // but keep rdC, etags
            if (!autoCommit)
                return this;
            else
            {
                var r = cx.db.Commit(cx);
                var aff = cx.affected;
                cx = new Context(r, cx.conn) { affected = aff };
                return r;
            }
        }
        internal override int AffCount(Context cx)
        {
            var c = 0;
            for (var b = ((Transaction)cx.db).physicals.PositionAt(step); b != null;
                b = b.Next())
                if (b.value() is Record || b.value() is Delete)
                    c++;
            return c;
        }
        public static Transaction operator +(Transaction d, (long, object) x)
        {
            var (dp, ob) = x;
            var m = d.mem;
            if (d.mem[dp] == ob)
                return d;
            return new Transaction(d, m + x);
        }
        public static Transaction operator +(Transaction d, DBObject ob)
        {
            return d + (ob.defpos, ob);
        }
        internal override DBObject? Add(Context cx, Physical ph)
        {
            if (cx.parse.HasFlag(ExecuteStatus.Parse) || cx.parse.HasFlag(ExecuteStatus.Prepare))
                return null;
            cx.db += (Physicals, physicals + (ph.ppos, ph));
            if (ph.ppos == cx.db.nextPos)
                cx.db += (NextPos, ph.ppos + 1);
            return ph.Install(cx);
        }
        /// <summary>
        /// We commit unknown users to the database if necessary for audit.
        /// There is a theoretical danger here that a conncurrent transaction will
        /// have committed the same user id. Watch out for this and take appropriate action.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="cx"></param>
        public override void Audit(Audit a, Context cx)
        {
            if (databases[name] is not Database db || dbfiles[name] is not FileStream df
                || a.user is not User u)
                return;
            var wr = new Writer(new Context(db), df);
            // u is from this transaction which has not been committed.
            // it is possible that a different user u is in the database: check for name.
            if (u.defpos > TransPos && u.name is not null && db.roles.Contains(u.name)
                && db.objects[db.roles[u.name] ?? -1L] is User du)
                a.user = du;
            lock (wr.file)
            {
                wr.oldStmt = wr.cx.db.nextStmt;
                wr.segment = wr.file.Position;
                a.Commit(wr, this);
                wr.PutBuf();
                df.Flush();
            }
        }
        internal override Database Commit(Context cx)
        {
            if (cx == null)
                return Rollback();
            var cph = BTree<long, Physical>.Empty;
            for (var b = physicals.First(); b != null; b = b.Next())
            {
                var cp = b.value();
                if ((!cp.ifNeeded) || cp.NeededFor(physicals))
                    cph += (b.key(), cp);
            }
            if (cph == BTree<long, Physical>.Empty &&
                (autoCommit || (cx.rdC.Count == 0 && (cx.db as Transaction)?.etags == null)))
                return Rollback();
            if (PyrrhoStart.ValidationMode)
            {
                var sb = new System.Text.StringBuilder();
                for (var b = physicals.First(); b != null; b = b.Next())
                    if (b.value() is Physical p && p is not PTransaction)
                        sb.Append(p);
                PyrrhoStart.validationLog?.WriteLine(sb.ToString());
            }
            // check for the case of an ad-hoc user that does not need to commit
            if (cph.Count == 1L && physicals.First()?.value() is PUser)
                return Rollback();
            for (var b = cx.deferred.First(); b != null; b = b.Next())
            {
                var ta = b.value();
                ta.defer = false;
                ta.db = this;
                ta.Exec();
            }
            var mt = CTree<Table, bool>.Empty; // tables with multiplicity indexes to check
            for (var b = cx.checkEdges.First(); b != null; b = b.Next())
                if (b.value() is TableRow tr && cx.db.objects[tr.tabledefpos] is Table et
                    && et.tableRows[b.key()] is TableRow nr)
                {
                    if (et.mindexes != CTree<long, long>.Empty)
                        mt += (et, true);
                    for (var c = (et.metadata[Qlx.EDGETYPE] as TSet)?.First();
                        c != null; c = c.Next())
                        if (c.Value() is TConnector tc && tc.cm is TMetadata tm)
                        {
                            var v = nr.vals[tc.cp];
                            if (v == null || v == TNull.Value)
                            {
                                if (tm[Qlx.OPTIONAL] == TBool.True) continue;
                                if (tm[Qlx.MIN]?.ToInt() == 0) continue;
                                throw new DBException("22G21", tc.cn);
                            }
                            if (tc.cd.kind == Qlx.SET)
                            {
                                var minc = tm[Qlx.MIN]?.ToInt();
                                var maxc = tm[Qlx.MAX]?.ToInt();
                                if (minc != null || maxc != null)
                                {
                                    var ct = v.Cardinality();
                                    if (minc != null && minc > ct)
                                        throw new DBException("22208", ((tc.cn == "") ? cx.NameFor(tc.cp) : tc.cn) ?? "");
                                    if (maxc != null && maxc < ct)
                                        throw new DBException("22209", ((tc.cn == "") ? cx.NameFor(tc.cp) : tc.cn) ?? "");
                                }
                            }
                        }
                }
            // check multiplicity constraints
            for (var b = mt.First(); b != null; b = b.Next())
            {
                var ta = b.key();
                for (var c = ta.mindexes.First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is TableColumn co &&
                        cx.db.objects[c.value()] is Index mx)
                    {
                        var minv = co.metadata[Qlx.MINVALUE].ToInt() ?? 0;
                        var maxv = co.metadata[Qlx.MAXVALUE].ToInt() ?? int.MaxValue;
                        var (l, h) = mx.Multiplicity();
                        if (minv < l)
                            throw new DBException("22206", co.NameFor(cx));
                        if (maxv > h)
                            throw new DBException("22207", co.NameFor(cx));
                    }
            }
            if (!autoCommit)
                for (var b = (cx.db as Transaction)?.etags.First(); b != null; b = b.Next())
                    if (b.key() != name)
                        cx.CheckRemote(b.key(), b.value());
            // Both rdr and wr access the database - not the transaction information
            if (databases[name] is not Database db || dbfiles[name] is not FileStream df)
                throw new PEException("PE0100");
            var rdr = new Reader(new Context(db), cx.db.length);
            var wr = new Writer(new Context(db), df);
            wr.cx.newnodes = cx.newnodes;
            wr.cx.nextHeap = cx.nextHeap; // preserve Compiled objects framing
            var tb = cph.First(); // start of the work we want to commit
            var since = rdr.GetAll();
            Physical? ph = null;
            PTransaction? pt = null;
            for (var pb = since.First(); pb != null; pb = pb.Next())
            {
                ph = pb.value();
                if (ph.type == Physical.Type.PTransaction)
                    pt = (PTransaction)ph;
                for (var b = (ph.supTables + (ph._table, true)).First(); b != null; b = b.Next())
                    if (cx.rdS[b.key()] is CTree<long, bool> ct)
                    {
                        if (ct.Contains(-1L))
                        {
                            cx.rconflicts++;
                            throw new DBException("40008", ph.supTables);
                        }
                        if (ct.Contains(ph.Affects) && pt is not null && ph.Conflicts(cx.rdC, pt) is Exception e)
                        {
                            cx.rconflicts++;
                            throw e;
                        }
                    }
                if (pt is not null)
                    for (var b = tb; b != null; b = b.Next())
                    {
                        var p = b.value();
                        var ce = ph.Conflicts(rdr.context.db, cx, p, pt);
                        if (ce is not null)
                        {
                            cx.wconflicts++;
                            throw ce;
                        }
                    }
            }
            lock (wr.file)
            {
                if (databases[name] is Database nd && nd != db)// may have moved on 
                    db = nd;
                rdr = new Reader(new Context(db), ph?.ppos ?? db.length) { locked = true };
                since = rdr.GetAll(); // resume where we had to stop above, use new file length
                for (var pb = since.First(); pb != null; pb = pb.Next())
                {
                    ph = pb.value();
                    PTransaction? pu = null;
                    if (ph.type == Physical.Type.PTransaction)
                        pu = (PTransaction)ph;
                    for (var b = ph.supTables.First(); b != null; b = b.Next())
                        if (cx.rdS[b.key()] is CTree<long, bool> ct)
                        {
                            if (ct.Contains(-1L))
                            {
                                cx.rconflicts++;
                                throw new DBException("4008", ph.supTables);
                            }
                            if (ct.Contains(ph.Affects) && pu is not null && ph.Conflicts(cx.rdC, pu) is Exception e)
                            {
                                cx.rconflicts++;
                                throw e;
                            }
                        }
                    if (pu is not null)
                        for (var b = tb; b != null; b = b.Next())
                        {
                            var ce = ph.Conflicts(rdr.context.db, cx, b.value(), pu);
                            if (ce != null)
                            {
                                cx.wconflicts++;
                                throw ce;
                            }
                        }
                }
                if (cph.Count == 0)
                    return Rollback();
                // deal with the special cases of first role and first user of the database
                var uu = user;
                var rl = role;
                if (uu?.defpos >= TransPos)
                    uu = null;
                if (rl.defpos >= TransPos)
                    rl = db.schema;
                pt = new PTransaction((int)physicals.Count, uu, rl, nextPos);
                cx.Add(pt);
                wr.segment = wr.file.Position;
                cx.parse = ExecuteStatus.Commit;
                var (tr, _) = pt.Commit(wr, this);
                var os = BTree<long, Physical>.Empty;
                cx.undefined = CTree<long, long>.Empty;
                for (var b = cph.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    cx.result = cx.obs[p.ppos] as RowSet;
                    cx.uids += (p.ppos, wr.Length);
                    (tr, _) = p.Commit(wr, tr);
                }
                cx.affected = (cx.affected ?? Rvv.Empty) + wr.cx.affected;
                cx.parse = ExecuteStatus.Obey;
                wr.cx.db += (LastModified, File.GetLastWriteTimeUtc(name));
                wr.cx.result = null;
                wr.cx.binding = CTree<long, TypedValue>.Empty;
                var at = CTree<long, Domain>.Empty;
                for (var b = wr.cx.uids.First(); b != null; b = b.Next())
                    if (wr.cx.db.objects[b.value() ?? -1L] is DBObject o)
                    {
                        if (o is Domain dm)
                            at = Supers(wr.cx, dm, at);
                        if (o is Index ix && wr.cx.db.objects[ix.reftabledefpos] is Domain dn)
                            at = Supers(wr.cx, dn, at);
                    }
                for (var b = at.First(); b != null; b = b.Next())
                    if (b.value() is Domain ad)
                    {
                        var na = (Domain)ad.Fix(wr.cx);
                        if (na.mem.ToString() != ad.mem.ToString())
                            wr.cx.db += na;
                    }
            }
            wr.PutBuf();
            df.Flush();
            var r = new Database(wr.cx.db, wr.Length);
            lock (_lock)
                databases += (name, r - Role - User);
            cx.db = r;
            return r;
        }
        static CTree<long, Domain> Supers(Context cx, Domain dm, CTree<long, Domain> at)
        {
            at += (dm.defpos, dm);
            for (var b = dm.super.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key().defpos] is Domain d)
                    at += (d.defpos, d);
            return at;
        }
        /// <summary>
        /// Contsruct a DBException and add in some diagnostics information
        /// </summary>
        /// <param name="sig">The name of the exception</param>
        /// <param name="obs">The objects for the format string</param>
        /// <returns>the DBException</returns>
        public override DBException Exception(string sig, params object[] obs)
        {
            var r = new DBException(sig, obs);
            for (var s = diagnostics.First(); s != null; s = s.Next())
                r.Add(s.key(), s.value());
            r.Add(Qlx.CONNECTION_NAME, new TChar(name));
#if !EMBEDDED
            r.Add(Qlx.SERVER_NAME, new TChar(PyrrhoStart.host));
#endif
            if (diagnostics[Qlx.TRANSACTIONS_COMMITTED] is TypedValue tc)
                r.Add(Qlx.TRANSACTIONS_COMMITTED, tc);
            if (diagnostics[Qlx.TRANSACTIONS_ROLLED_BACK] is TypedValue rb)
                r.Add(Qlx.TRANSACTIONS_ROLLED_BACK, rb);
            return r;
        }
        internal Context Execute(Executable e, Context cx)
        {
            if (!cx.parse.HasFlag(ExecuteStatus.Obey))
                return cx;
            for (var b = cx.undefined.First(); b != null; b = b.Next())
            {
                DBObject? qv = null;
                var ns = cx.names;
                if (cx.obs[b.key()] is DBObject uo)
                {
                    if (uo is SqlCall sc)
                        qv = sc.Resolve(cx);
                    if (qv is null)
                        for (var c = uo.chain?.First(); ns.Count != 0L && c != null; c = c.Next())
                            if (c.value() is Ident id && cx.obs[ns[id.ident].Item2] is DBObject q)
                            {
                                qv = q;
                                ns = cx.defs[q.defpos] ?? Names.Empty;
                            }
                    if (qv is not null)
                    {
                        cx.undefined -= b.key();
                        cx.Replace(uo, qv);
                        cx.NowTry();
                    }
                }
            }
            if (cx.undefined != CTree<long, long>.Empty)
                throw new DBException("42112", cx.obs[cx.undefined.First()?.key() ?? -1L]?.mem[ObInfo.Name] ?? "?");
            var a = new LabelledActivation(cx, e.label ?? "")
            {
                exec = e
            };
            var ac = e._Obey(a);
            if (a.signal != null)
            {
                var ex = Exception(a.signal.signal, a.signal.objects);
                for (var s = a.signal.setlist.First(); s != null; s = s.Next())
                    if (s.value() is long p && cx.obs[p] is QlValue v)
                        ex.Add(s.key(), v.Eval(cx));
                throw ex;
            }
            cx.result = ac.result;
            cx.obs = ac.obs;
            if (cx != ac)
            {
                cx.db = ac.db;
                cx.rdC = ac.rdC;
            }
            return cx;
        }
        /// <summary>
        /// For REST service: do what we should according to the path, mime type and posted obs
        /// </summary>
        /// <param name="method">GET/HEAD/PUT/POST/DELETE</param>
        /// <param name="path">The URL</param>
        /// <param name="mime">The mime type in the header</param>
        /// <param name="sdata">The posted obs if any</param>
        internal Context Execute(Context cx, long sk, string method, string dn, string[] path,
            string query, string? mime, string sdata)
        {
            var db = this;
            cx.parse |= ExecuteStatus.Http;
            int j, ln;
            if (sk != 0L)
            {
                j = 1; ln = 2;
            }
            else
            {
                j = 2; ln = 4;
            }
            if (path.Length >= ln)
            {
                RowSet? fm = null;
                if (cx.role != null && long.TryParse(path[j], out long t) && cx.db.objects[t] is Table tb
                    && tb.infos[cx.role.defpos] is ObInfo ti && ti.name != null)
                {
                    if (sk != 0L && ti.schemaKey > sk)
                        throw new DBException("2E307", ti.name);
                    fm = tb.RowSets(new Ident(ti.name, cx.GetUid()), cx, tb, cx.GetPrevUid(), 0L);
                    j++;
                }
                switch (method)
                {
                    case "HEAD":
                        cx.result = null;
                        cx.binding = CTree<long, TypedValue>.Empty;
                        break;
                    case "GET":
                        db.Execute(cx, fm, method, dn, path, query, j);
                        break;
                    case "DELETE":
                        {
                            db.Execute(cx, fm, method, dn, path, query, j);
                            if (cx.result is TableRowSet trd)
                                cx = db.Delete(cx, trd);
                            break;
                        }
                    case "PUT":
                        {
                            db.Execute(cx, fm, method, dn, path, query, j);
                            if (cx.result is TableRowSet trp)
                                cx = db.Put(cx, trp, sdata);
                            break;
                        }
                    case "POST":
                        {
                            db.Execute(cx, fm, method, dn, path, query, j);
                            if (cx.result is TableRowSet trt)
                                cx = db.Post(cx, trt, sdata);
                            break;
                        }
                }
            }
            else
            {
                switch (method)
                {
                    case "POST":
                        new Parser(cx).ParseQl(sdata);
                        break;
                }
            }
            return cx;
        }
        /// <summary>
        /// HTTP service implementation
        /// See sec 3.8.2 of the Pyrrho manual. The URL format is very flexible, with
        /// keywords such as table, procedure all optional. 
        /// URL encoding is used so that at this stage the URL can contain spaces etc.
        /// The URL is case-sensitive throughout, so use capitals a lot,
        /// except for the keywords specified in 3.8.2.
        /// Single quotes around string values and double quotes around identifiers
        /// are optional (they can be used to disambiguate column names from string values,
        /// or to include commas etc in string values).
        /// Expressions are allowed in procedure argument values,
        /// Where conditions can only be simple column compareop value (can be chained),
        /// and no other expressions are allowed.
        /// The query part of the URL is used for metadata flags, see section 7.2.
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="f">The rowset so far</param>
        /// <param name="method">GET, PUT, POST or DELETE</param>
        /// <param name="dn">The database name</param>
        /// <param name="path">The URL split into segments by /</param>
        /// <param name="query">The metadata flags part of the query</param>
        /// <param name="p">Where we are in the path</param>
        internal void Execute(Context cx, RowSet? f, string method, string dn, string[] path, string query, int p)
        {
            if ((p >= path.Length || path[p] == "") && f is not null)
            {
                cx.result = f;
                return;
            }
            string cp = path[p]; // Test cp against Selector and Processing specification in 3.8.2
            int off = 0;
            string[] sp = cp.Split(' ');
            CallStatement? fc = null;
            switch (sp[0])
            {
                case "edmx":
                    break;
                case "table":
                    {
                        var tbs = cp[(6 + off)..];
                        tbs = WebUtility.UrlDecode(tbs);
                        var tbn = new Ident(tbs, cx.GetUid());
                        if (cx.db == null || cx.db.role == null
                            || objects[cx.db.role.dbobjects[tbn.ident] ?? -1L] is not Table tb)
                            throw new DBException("42107", tbn).Mix();
                        f ??= tb.RowSets(tbn, cx, tb, tbn.uid, 0L);
                        var lp = uid + 6 + off;
                        break;
                    }
                case "procedure":
                    {
                        if (fc == null)
                        {
                            var pn = cp[(10 + off)..];
#if (!SILVERLIGHT) && (!ANDROID)
                            pn = WebUtility.UrlDecode(pn);
#endif
                            fc = new Parser(cx).ParseProcedureCall(pn);
                        }
                        fc._Obey(cx);
                        break;
                    }
                case "key":
                    {
                        if (f is TableRowSet ts && objects[f.target] is Table tb &&
                            tb.FindPrimaryIndex(cx) is Index ix)
                        {
                            var kn = 0;
                            var fl = CTree<long, TypedValue>.Empty;
                            while (kn < ix.keys.Length && p < path.Length)
                            {
                                var sk = path[p];
                                if (kn == 0)
                                    sk = sk[(4 + off)..];
#if (!SILVERLIGHT) && (!ANDROID)
                                sk = WebUtility.UrlDecode(sk);
#endif
                                if (cx.obs[ix.keys[kn] ?? -1L] is not TableColumn tc)
                                    throw new DBException("42112", kn);
                                if (tc.domain.TryParse(new Scanner(uid, sk.ToCharArray(), 0, cx), out TypedValue? kv) != null)
                                    break;
                                kn++;
                                p++;
                                if (ts.iSMap[tc.defpos] is long pp)
                                    fl += (pp, kv);
                            }
                            var rs = (RowSet)cx.Add(f + (cx, RowSet._Matches, fl));
                            cx.result = rs;
                            break;
                        }
                        goto case "where";
                    }
                case "where":
                    {
                        string ks = cp[(6 + off)..];
#if (!SILVERLIGHT) && (!ANDROID)
                        ks = WebUtility.UrlDecode(ks);
#endif
                        if (f == null)
                            throw new DBException("42000", ks).ISO();
                        string[] sk = [];
                        if (ks.Contains("={") || ks[0] == '{')
                            sk = [ks];
                        else
                            sk = ks.Split(',');
                        var n = sk.Length;
                        var psr = new Parser(cx);
                        var wt = CTree<long, bool>.Empty;
                        for (var si = 0; si < n; si++)
                        {
                            var v = psr.ParseSqlValue(new Ident(sk[si], cx.GetUid()), (DBObject._Domain, Domain.Bool));
                            var ls = v.Resolve(cx, f, BTree<long, object>.Empty, 0L).Item1;
                            for (var c = ls.First(); c != null; c = c.Next())
                                if (c.value() is QlValue cv)
                                    wt += cv.Disjoin(cx);
                        }
                        f = (RowSet)cx.Add(f + (cx, RowSet._Where, wt));
                        break;
                    }
                case "distinct":
                    {
                        if (cp.Length < 10 && cx.result is RowSet r)
                        {
                            cx.val = (TypedValue?)new DistinctRowSet(cx, r).First(cx) ?? TNull.Value;
                            break;
                        }
                        string[] ss = cp[9..].Split(',');
                        // ???
                        break;
                    }
                case "ascending":
                    {
                        if (cp.Length < 10)
                            throw new DBException("42161", "Column(s)", cp).Mix();
                        string[] ss = cp[9..].Split(',');
                        //??
                        break;
                    }
                case "descending":
                    {
                        if (cp.Length < 10)
                            throw new DBException("42161", "Column(s)", cp).Mix();
                        string[] ss = cp[9..].Split(',');
                        // ??
                        break;
                    }
                case "skip":
                    {
                        //                transaction.SetResults(new RowSetSection(transaction.valueType.rowSet, int.Parse(cp.Substring(5)), int.MaxValue));
                        break;
                    }
                case "count":
                    {
                        //                transaction.SetResults(new RowSetSection(transaction.valueType.rowSet, 0, int.Parse(cp.Substring(6))));
                        break;
                    }
                case "of":
                    {
                        var s = cp[(3 + off)..];
                        var ps = s.IndexOf('(');
                        var key = Array.Empty<string>();
                        if (ps > 0)
                        {
                            var cs = s.Substring(ps + 1, s.Length - ps - 2);
                            s = s[0..(ps - 1)];
                            key = cs.Split(',');
                        }
                        // ??
                        break;
                    }
                case "rvv":
                    {
                        var s = cp[(4 + off)..];
                        // ??
                        return; // do not break;
                    }
                case "select":
                    {
                        sp[0] = sp[0].Trim(' ');
                        if (f is TableRowSet fa && objects[fa.target] is Table ta && cx.role != null)
                        {
                            var cs = sp[0].Split(',');
                            var ns = BTree<string, long?>.Empty;
                            var ss = CList<long>.Empty;
                            for (var c = ta.rowType.First(); c != null; c = c.Next())
                                if (c.value() is long cc && ta.representation[cc] is DBObject oa
                                        && oa.infos[cx.role.defpos] is ObInfo ci && ci.name != null)
                                    ns += (ci.name, cc);
                            for (var i = 0; i < cs.Length; i++)
                                if (ns[cs[i]] is long np && fa.iSMap[np] is long fp)
                                    ss += fp;
                            if (ss != CList<long>.Empty)
                            {
                                var fd = new Domain(cx.GetUid(), cx, f.kind, f.representation, ss);
                                f = new SelectedRowSet(cx, 0L, f, f);
                                break;
                            }
                        }
                        throw new DBException("420000", cp);
                    }
                default:
                    {
                        var cn = sp[0];
                        cn = WebUtility.UrlDecode(cn);
                        if (QuotedIdent(cn))
                            cn = cn.Trim('"');
                        var ob = GetObject(cn, cx.role);
                        if (ob is Table tb)
                        {
                            off = -6;
                            goto case "table";
                        }
                        else if (ob is Role ro)
                        {
                            cx.db += (Role, ro);
                            Execute(cx, f, method, dn, path, query, p + 1);
                            return;
                        }
                        else if (ob is Procedure pn)
                        {
                            off = -10;
                            goto case "procedure";
                        }
                        if (cn.Contains(':'))
                        {
                            off -= 4;
                            goto case "rvv";
                        }
                        if (cn.Contains('=') || cn.Contains('<') || cn.Contains('>'))
                        {
                            off = -6;
                            goto case "where";
                        }
                        if (f is TableRowSet fa && objects[fa.target] is Table ta && cx.role != null)
                        {
                            var cs = sp[0].Split(',');
                            var ns = BTree<string, long?>.Empty;
                            var ss = CList<long>.Empty;
                            for (var c = ta.rowType.First(); c != null; c = c.Next())
                                if (c.value() is long cc && ta.representation[cc] is DBObject co
                                        && co.infos[cx.role.defpos] is ObInfo ci &&
                                        ci.name != null)
                                    ns += (ci.name, cc);
                            for (var i = 0; i < cs.Length; i++)
                                if (ns[cs[i]] is long np && fa.iSMap[np] is long fp)
                                    ss += fp;
                            if (ss != CList<long>.Empty)
                            {
                                var fd = new Domain(cx.GetUid(), cx, f.kind, f.representation, ss);
                                f = new SelectedRowSet(cx, 0L, fd, f);
                                break;
                            }
                            var ix = ta.FindPrimaryIndex(cx);
                            if (ix != null)
                            {
                                off -= 4;
                                goto case "key";
                            }
                        }
                        if (cx.val != TNull.Value)
                        {
                            off = -4;
                            goto case "key";
                        }
                        break;
                        //    throw new DBException("42107", sp[0]).Mix();
                    }
            }
            Execute(cx, f, method, dn, path, query, p + 1);
        }
        static bool QuotedIdent(string s)
        {
            var cs = s.ToCharArray();
            var n = cs.Length;
            if (n <= 3 || cs[0] != '"' || cs[n - 1] != '"')
                return false;
            for (var i = 1; i < n - 1; i++)
                if (!char.IsLetterOrDigit(cs[i]) && cs[i] != '_')
                    return false;
            return true;
        }
        internal override Context Put(Context cx, TableRowSet rs, string s)
        {
            var da = new TDocArray(s);
            if (objects[rs.target] is not Table tb || cx.obs.Last() is not ABookmark<long, DBObject> ab
                || cx.role == null)
                throw new PEException("PE49000");
            var ix = tb.FindPrimaryIndex(cx);
            var us = rs.assig;
            var ma = CTree<long, TypedValue>.Empty;
            var d = da[0];
            cx.nextHeap = ab.key() + 1;
            for (var c = rs.rowType.First(); c != null; c = c.Next())
                if (c.value() is long cc)
                {
                    var n = "";
                    var isk = false;
                    if (cx.obs[cc] is QlValue sv && sv.name != null)
                    {
                        n = sv.name;
                        if (sv is QlInstance sc && ix != null)
                            isk = ix.keys.rowType.Has(sc.sPos);
                    }
                    else if (cx._Ob(cc) is DBObject oc && oc.infos[cx.role.defpos] is ObInfo ci && ci.name != null)
                        n = ci.name;
                    if (d[n] is not TypedValue v)
                        throw new PEException("PE49203");
                    if (isk)
                        ma += (cc, v);
                    else
                    {
                        var sl = new SqlLiteral(cx.GetUid(), v);
                        cx._Add(sl);
                        us += (new UpdateAssignment(cc, sl.defpos), true);
                    }
                }
            rs += (cx, RowSet._Matches, ma);
            rs += (cx, RowSet.Assig, us);
            cx.Add(rs);
            rs = (TableRowSet)(cx.obs[rs.defpos] ?? throw new PEException("PE49202"));
            var ta = rs.Update(cx, rs)[rs.target];
            if (ta != null)
            {
                ta.db = cx.db;
                if (rs.First(cx) is Cursor cu)
                {
                    ta.cursors = cx.cursors + (rs.defpos, cu);
                    ta.EachRow(cx, 0);
                }
                cx.db = ta.db;
            }
            return cx;
        }
        internal override Context Post(Context cx, TableRowSet rs, string s)
        {
            var da = new TDocArray(s);
            var rws = BList<(long, TRow)>.Empty;
            for (var i = 0; i < da.Count; i++)
            {
                var d = da[i];
                var vs = CTree<long, TypedValue>.Empty;
                for (var c = rs.rowType.First(); c != null; c = c.Next())
                    if (c.value() is long p && cx.NameFor(p) is string n && d[n] is TypedValue v)
                        vs += (p, v);
                rws += (cx.GetUid(), new TRow(rs, vs));
            }
            var ers = new ExplicitRowSet(0L, cx.GetUid(), cx, rs, rws);
            cx.Add(ers);
            if (ers.First(cx) is Cursor cu)
            {
                if (ers.tree != null)
                    rs = rs + (Index.Tree, ers.tree) + (Index.Keys, ers.keys);
                if (rs.Insert(cx, rs, rs)[rs.target] is TargetActivation ta)
                {
                    ta.db = cx.db;
                    cx.cursors += (rs.defpos, cu);
                    ta.cursors = cx.cursors;
                    ta.EachRow(cx, 0);
                }
            }
            return cx;
        }
        internal override Context Delete(Context cx, TableRowSet r)
        {
            if (cx.obs[r.from] is RowSet fm)
            {
                var ts = r.Delete(cx, fm);
                if (ts.First()?.value() is TargetActivation ta)
                    for (var b = r.First(cx); b != null; b = b.Next(cx))
                        if (cx.cursors[r.target] is Cursor ib)
                        {
                            ta.db = cx.db;
                            cx.cursors += (fm.defpos, ib);
                            ta.EachRow(cx, b._pos);
                            cx.db = ta.db;
                        }
            }
            return base.Delete(cx, r);
        }
        /// <summary>
        /// Implement Grant or Revoke
        /// </summary>
        /// <param name="grant">true=grant,false=revoke</param>
        /// <param name="pr">the privilege</param>
        /// <param name="obj">the database object</param>
        /// <param name="grantees">a tree of grantees</param>
        static ExecuteList DoAccess(Context cx, bool grant, Grant.Privilege pr, long obj,
            BList<DBObject> grantees)
        {
            var es = ExecuteList.Empty;
            var np = cx.db.nextPos;
            if (grantees != BList<DBObject>.Empty) // PUBLIC
                for (var b = grantees.First(); b != null; b = b.Next())
                    if (b.value() is DBObject mk)
                    {
                        var gee = mk.defpos;
                        if (grant)
                            es += cx.Add(new Grant(pr, obj, gee, np++, cx));
                        else
                            es += cx.Add(new Revoke(pr, obj, gee, np++, cx));
                    }
            return es;
        }
        /// <summary>
        /// Implement Grant/Revoke on a tree of TableColumns
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="tb">the database</param>
        /// <param name="pr">the privileges</param>
        /// <param name="tb">the table</param>
        /// <param name="list">(Privilege,columnnames[])</param>
        /// <param name="grantees">a tree of grantees</param>
        static ExecuteList AccessColumns(Context cx, bool grant, Grant.Privilege pr, Table tb, PrivNames list, BList<DBObject> grantees)
        {
            var es = ExecuteList.Empty;
            var ne = list.cols != BTree<string, bool>.Empty;
            for (var b = tb.representation.First(); b != null; b = b.Next())
                if (b.value() is DBObject oc && oc.infos[cx.role.defpos] is ObInfo ci && ci.name != null
                    && !ne && list.cols.Contains(ci.name))
                {
                    list.cols -= ci.name;
                    es += DoAccess(cx, grant, pr, b.key(), grantees);
                }
            if (list.cols.First()?.key() is string cn)
                throw new DBException("42112", cn);
            return es;
        }
        /// <summary>
        /// Implement grant/revoke on a Role
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="roles">a tree of Roles (ids)</param>
        /// <param name="grantees">a tree of Grantees</param>
        /// <param name="opt">whether with ADMIN option</param>
		internal ExecuteList AccessRole(Context cx, bool grant, CList<string> rols, BList<DBObject> grantees, bool opt)
        {
            var es = ExecuteList.Empty;
            Grant.Privilege op;
            if (opt == grant) // grant with grant option or revoke
                op = Grant.Privilege.UseRole | Grant.Privilege.AdminRole;
            else if (opt && !grant) // revoke grant option for
                op = Grant.Privilege.AdminRole;
            else // grant
                op = Grant.Privilege.UseRole;
            for (var b = rols.First(); b is not null; b = b.Next())
                if (b.value() is string s && roles.Contains(s) && objects[roles[s] ?? -1L] is Role ro)
                    es += DoAccess(cx, grant, op, ro.defpos, grantees);
            return es;
        }
        /// <summary>
        /// Implement grant/revoke on a database obejct
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="privs">the privileges</param>
        /// <param name="dp">the database object defining position</param>
        /// <param name="grantees">a tree of grantees</param>
        /// <param name="opt">whether with GRANT option (grant) or GRANT for (revoke)</param>
        internal ExecuteList AccessObject(Context cx, bool grant, BList<PrivNames> privs, long dp, BList<DBObject> grantees, bool opt)
        {
            var es = ExecuteList.Empty;
            if (role is not null && objects[dp] is DBObject ob && ob.infos[role.defpos] is ObInfo gd)
            {
                Grant.Privilege defp = Grant.Privilege.NoPrivilege;
                if (!grant)
                    defp = (Grant.Privilege)0x3fffff;
                var p = defp; // the privilege being granted
                var gp = gd.priv; // grantor's privileges on the target object
                var changed = true;
                if (privs == null) // all (grantor's) privileges
                {
                    if (grant)
                    {
                        p = gp;
                        for (var cp = ob.domain.First(); cp != null; cp = cp.Next())
                            if (cp.value() is long c)
                            {
                                gp = gd.priv;
                                var pp = defp;
                                if (grant)
                                    pp = gp;
                                es += DoAccess(cx, grant, pp, c, grantees);
                            }
                    }
                }
                else
                    for (var b = privs.First(); b is not null; b = b.Next())
                        if (b.value() is PrivNames mk)
                        {
                            Grant.Privilege q = Grant.Privilege.NoPrivilege;
                            switch (mk.priv)
                            {
                                case Qlx.SELECT: q = Grant.Privilege.Select; break;
                                case Qlx.INSERT: q = Grant.Privilege.Insert; break;
                                case Qlx.DELETE: q = Grant.Privilege.Delete; break;
                                case Qlx.UPDATE: q = Grant.Privilege.Update; break;
                                case Qlx.REFERENCES: q = Grant.Privilege.References; break;
                                case Qlx.EXECUTE: q = Grant.Privilege.Execute; break;
                                case Qlx.TRIGGER: break; // ignore for now (?)
                                case Qlx.USAGE: q = Grant.Privilege.Usage; break;
                                case Qlx.OWNER:
                                    q = Grant.Privilege.Owner;
                                    if (!grant)
                                        throw Exception("4211A", mk).Mix();
                                    break;
                                default: throw Exception("4211A", mk).Mix();
                            }
                            Grant.Privilege pp = (Grant.Privilege)(((int)q) << 12);
                            if (opt == grant)
                                q |= pp;
                            else if (opt && !grant)
                                q = pp;
                            if (mk.cols.Count != 0L)
                            {
                                if (changed)
                                    changed = grant;
                                es += AccessColumns(cx, grant, q, (Table)ob, mk, grantees);
                            }
                            else
                                p |= q;
                        }
                if (changed)
                    es += DoAccess(cx, grant, p, ob?.defpos ?? 0, grantees);
            }
            return es;
        }
    }
    /// <summary>
    ///  better implementation of UNDO handler: copy the context stack as well as LocationTransaction states
    /// </summary>
    internal class ExecState
    {
        public Transaction mark;
        public Context stack;

        internal ExecState(Context cx,Transaction tr)
        {
            mark = tr;
            stack = cx;
        }
    }
}
