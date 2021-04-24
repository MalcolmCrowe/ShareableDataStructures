using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System;
using System.Net;
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
    /// DBObjects with transaction uids are add to the transaction's list of objects.
    /// Transaction itself is immutable and shareable.
    /// 
    /// WARNING: Each new Physical for a transaction must be added to the Context
    /// so that Transaction gets a chance to update nextPos. Make sure you understand this fully
    /// before you add any code that creates a new Physical.
    /// </summary>
    internal class Transaction : Database
    {
        internal const long
            AutoCommit = -278, // bool
            Deferred = -279, // BList<TriggerActivation>
            Diagnostics = -280, // BTree<Sqlx,TypedValue>
            _Mark = -281, // Transaction
            Physicals = -282, // BTree<long,Physical>
            ReadConstraint = -283, // BTree<long,ReadConstraint> Context has latest version
            Step = -276, // long
            StartTime = -287, //long
            TriggeredAction = -288, // long
            Warnings = -289; // BList<Exception>
        internal BTree<long,Physical> physicals => 
            (BTree<long,Physical>)mem[Physicals]?? BTree<long,Physical>.Empty;
        internal long startTime => (long)(mem[StartTime] ?? 0);
   //     internal BTree<CList<long>,BTree<CList<Domain>,Domain>> domains => 
   //         (BTree<CList<long>,BTree<CList<Domain>,Domain>>)mem[Domains]
   //         ??BTree<CList<long>,BTree<CList<Domain>,Domain>>.Empty;
        public BTree<Sqlx, TypedValue> diagnostics =>
            (BTree<Sqlx,TypedValue>)mem[Diagnostics]??BTree<Sqlx, TypedValue>.Empty;
        public BList<System.Exception> warnings =>
            (BList<System.Exception>)mem[Warnings]??BList<System.Exception>.Empty;
        public BTree<long, ReadConstraint> rdC =>
            (BTree<long, ReadConstraint>)mem[ReadConstraint] ?? BTree<long, ReadConstraint>.Empty;
        internal override long uid => (long)(mem[NextId]??-1L);
        public override long lexeroffset => uid;
        internal Transaction mark => (Transaction)mem[_Mark];
        internal long step => (long)(mem[Step] ?? TransPos);
        internal override long nextPos => (long)(mem[NextPos]??TransPos);
        internal override string source => (string)mem[CursorSpecification._Source];
        internal override bool autoCommit => (bool)(mem[AutoCommit]??true);
        internal long triggeredAction => (long)(mem[TriggeredAction]??-1L);
        internal BList<TriggerActivation> deferred =>
            (BList<TriggerActivation>)mem[Deferred] ?? BList<TriggerActivation>.Empty;
        /// <summary>
        /// Physicals, SqlValues and Executables constructed by the transaction
        /// will use virtual positions above this mark (see PyrrhoServer.nextIid)
        /// </summary>
        public const long TransPos = 0x4000000000000000;
        public const long Analysing = 0x5000000000000000;
        public const long Executables = 0x6000000000000000;
        // actual start of Heap is given by db.nextPrep for the connection (see Context(db))
        public const long HeapStart = 0x7000000000000000; //so heap starts after prepared statements
        readonly Database parent;
        /// <summary>
        /// As created from the Database: 
        /// via db.mem below we inherit its objects, and the session user and role
        /// </summary>
        /// <param name="db"></param>
        /// <param name="t"></param>
        /// <param name="sce"></param>
        /// <param name="auto"></param>
        internal Transaction(Database db,long t,string sce,bool auto) 
            :base(db.loadpos,db.mem+(StartTime,DateTime.Now.Ticks)+(NextId,t+1)
            +(NextStmt,db.nextStmt)+(AutoCommit,auto)
            +(CursorSpecification._Source,sce))
        {
            parent = db;
        }
        protected Transaction(Transaction t,long p, BTree<long, object> m)
            : base(p, m)
        {
            parent = t.parent;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Transaction(this,loadpos, m);
        }
        public override Database New(long c, BTree<long, object> m)
        {
            return new Transaction(this, c, m);
        }
        public override Transaction Transact(long t,string sce,bool? auto=null)
        {
            var r = this;
            if (auto == false && autoCommit)
                r += (AutoCommit, false);
            // Ensure the correct role amd user combination
            r += (Step, r.nextPos);
            if (t>=TransPos) // if sce is tranaction-local, we need to make space above nextIid
                r = r+ (NextId,t+1)+(CursorSpecification._Source,sce);
            return r;
        }
        public override Database RdrClose(Context cx)
        {
            cx.values = CTree<long, TypedValue>.Empty;
            cx.cursors = BTree<long, Cursor>.Empty;
            cx.data = BTree<long, RowSet>.Empty;
            cx.results = BTree<long, long>.Empty;
            cx.obs = BTree<long, DBObject>.Empty;
            cx.result = -1L;
            if (!autoCommit)
                return Unheap(cx);
            cx.etags = CTree<string, Rvv>.Empty;
            return cx.db.Commit(cx)+(NextPrep,nextPrep);
        }
        internal override int AffCount(Context cx)
        {
            var c = 0;
            for (var b = physicals.PositionAt(cx.physAtStepStart); b != null;
                b = b.Next())
                if (b.value() is Record || b.value() is Delete)
                    c++;
            return c;
        }
        /// <summary>
        /// Fix all heap uids in framing fields of compiled objects
        /// such as TableColumn (generation rule), Check, Trigger, Procedure, Method
        /// </summary>
        /// <param name="cx"></param>
        /// <returns>This Transaction with the compiled objects updated</returns>
        Database Unheap(Context cx)
        {
            for (var b = physicals.PositionAt(step); b != null; b = b.Next())
                b.value().Relocate(cx);
            return cx.db;
        }
        public static Transaction operator +(Transaction d, (long, object) x)
        {
            return new Transaction(d,d.loadpos, d.mem + x);
        }
        /// <summary>
        /// Default action for adding a DBObject
        /// </summary>
        /// <param name="d"></param>
        /// <param name="ob"></param>
        /// <returns></returns>
        public static Transaction operator +(Transaction d, DBObject ob)
        {
            return (Transaction)(d+(ob,d.loadpos));
        }
        public static Transaction operator+(Transaction d,Procedure p)
        {
            var ro = d.role + p;
            return (Transaction)(d + ro + (p,d.loadpos));
        }
        internal override void Add(Context cx,Physical ph, long lp)
        {
            if (cx.parse != ExecuteStatus.Obey)
                return;
            var d = cx.db as Transaction;
            cx.db = new Transaction(d, lp, d.mem+(Physicals, d.physicals + (ph.ppos, ph))
                + (NextPos, ph.ppos + 1));
            ph.Install(cx, lp);
        }
        /// <summary>
        /// Ensure that TriggeredAction effects get serialised after the event that triggers them. 
        /// </summary>
        /// <param name="rp">The triggering record</param>
        internal void FixTriggeredActions(CTree<PTrigger.TrigType,CTree<long,bool>> trigs,
            PTrigger.TrigType tgt, long rp)
        {
            for (var t = trigs.First(); t != null; t = t.Next())
                if (t.key().HasFlag(tgt))
                {
                    var tgs = t.value();
                    for (var b = physicals.First(); b != null; b = b.Next())
                        if (b.value() is TriggeredAction ta && tgs.Contains(ta.trigger) 
                                && ta.refPhys < 0)
                            ta.refPhys = rp;
                }
        }
        internal override Database Rollback(object e)
        {
            return parent.Rollback(e);
        }
        /// <summary>
        /// We commit unknown users to the database if necessary for audit.
        /// There is a theoretical danger here that a conncurrent transaction will
        /// have committed the same user id. Watch out for this and take appropriate action.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="cx"></param>
        public override void Audit(Audit a,Context cx)
        {
            var db = databases[name];
            var wr = new Writer(new Context(db), dbfiles[name]);
            if (a.user.defpos > TransPos && db.roles.Contains(a.user.name))
                a.user = (User)db.objects[db.roles[a.user.name]];
            lock (wr.file)
            {
                wr.stmtPos = databases[name].nextStmt; // may have changed!
                wr.oldStmt = wr.stmtPos;
                wr.segment = wr.file.Position;
                a.Commit(wr, this);
                wr.PutBuf();
                df.Flush();
            }
        }
        internal override Database Commit(Context cx)
        {
            if (physicals == BTree<long, Physical>.Empty && cx.rdC.Count==0)
                return parent.Commit(cx);
            // check for the case of an ad-hoc user that does not need to commit
            if (physicals.Count == 1L && physicals.First().value() is PUser)
                return parent.Commit(cx);
            for (var b=deferred.First();b!=null;b=b.Next())
            {
                var ta = b.value();
                ta.deferred = false;
                ta.db = this;
                ta.Exec();
            }
            // Both rdr and wr access the database - not the transaction information
            var db = databases[name];
            var rdr = new Reader(new Context(db), loadpos);
            var wr = new Writer(new Context(db), dbfiles[name]);
            var tb = physicals.First(); // start of the work we want to commit
            var since = rdr.GetAll();
            Physical ph = null;
            for (var pb=since.First(); pb!=null; pb=pb.Next())
            {
                ph = pb.value();
                PTransaction pt = null;
                if (ph.type == Physical.Type.PTransaction || ph.type == Physical.Type.PTransaction2)
                    pt = (PTransaction)ph;
                for (var cb = cx.rdC.First(); cb != null; cb = cb.Next())
                {
                    var ce = cb.value()?.Check(ph,pt);
                    if (ce != null)
                    {
                        cx.rconflicts++;
                        throw ce;
                    }
                }
                for (var b = tb; b != null; b = b.Next())
                {
                    var ce = ph.Conflicts(rdr.context.db, cx, b.value(), pt);
                    if (ce!=null)
                    {
                        cx.wconflicts++;
                        throw ce;
                    }
                }
            }
            lock (wr.file)
            {
                db = databases[name]; // may have moved on
                wr.stmtPos = db.nextStmt;
                wr.oldStmt = wr.stmtPos;
                var lb = db.log.PositionAt(ph?.ppos ?? loadpos)?.Next();
                if (lb != null)
                {
                    rdr = new Reader(new Context(db), lb.key());
                    rdr.locked = true;
                    since = rdr.GetAll(); // resume where we had to stop above, use new file length
                    for (var pb = since.First(); pb != null; pb = pb.Next())
                    {
                        ph = pb.value();
                        PTransaction pu = null;
                        if (ph.type == Physical.Type.PTransaction || ph.type == Physical.Type.PTransaction2)
                            pu = (PTransaction)ph;
                        for (var cb = cx.rdC.First(); cb != null; cb = cb.Next())
                        {
                            var ce = cb.value()?.Check(ph, pu);
                            if (ce != null)
                            {
                                cx.rconflicts++;
                                throw ce;
                            }
                        }
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
                }
                if (physicals.Count == 0)
                    return parent.Commit(cx);
                var pt = new PTransaction((int)physicals.Count, user.defpos, role.defpos,
                        nextPos, cx);
                cx.Add(pt);
                wr.segment = wr.file.Position;
                var (tr, _) = pt.Commit(wr, this);
                for (var b = physicals.First(); b != null; b = b.Next())
                    (tr, _) = b.value().Commit(wr, tr);
                cx.affected = (cx.affected ?? Rvv.Empty) + wr.cx.affected;
                cx.etags += (name, (cx.etags[name] ?? Rvv.Empty) + cx.affected);
                wr.PutBuf();
                df.Flush();
                wr.cx.db += (NextStmt, wr.cx.nextStmt);
                wr.cx.result = -1L;
                // we install the new version of the database, and then 
                // add the Connection information for the session
                return wr.cx.db.Install(wr.Length) + (_Connection, conn);
            }
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
            r.Add(Sqlx.CONNECTION_NAME, new TChar(name));
#if !EMBEDDED
            r.Add(Sqlx.SERVER_NAME, new TChar(PyrrhoStart.host));
#endif
            r.Add(Sqlx.TRANSACTIONS_COMMITTED, diagnostics[Sqlx.TRANSACTIONS_COMMITTED]);
            r.Add(Sqlx.TRANSACTIONS_ROLLED_BACK, diagnostics[Sqlx.TRANSACTIONS_ROLLED_BACK]);
            return r;
        }
        internal override Transaction Mark()
        {
            return this+(_Mark,this);
        }
        internal Context Execute(Executable e, Context cx)
        {
            if (cx.parse != ExecuteStatus.Obey)
                return cx;
            var a = new Activation(cx,e.label);
            a.exec = e;
            cx = e.Obey(a); // Obey must not call the Parser!
            if (a.signal != null)
            {
                var ex = Exception(a.signal.signal, a.signal.objects);
                for (var s = a.signal.setlist.First(); s != null; s = s.Next())
                    ex.Add(s.key(), cx.obs[s.value()].Eval(null));
                throw ex;
            }
            cx.result = -1L;
            return cx;
        }
        /// <summary>
        /// For REST service: do what we should according to the path, mime type and posted data
        /// </summary>
        /// <param name="method">GET/PUT/POST/DELETE</param>
        /// <param name="path">The URL</param>
        /// <param name="mime">The mime type in the header</param>
        /// <param name="sdata">The posted data if any</param>
        internal Context Execute(Context cx, string method, string id, string dn, string[] path, string mime, 
            string sdata, string[] ets)
        {
            var db = this;
            var etag = cx.etags[dn];
            if (ets != null)
                for (var i = 0; i < ets.Length; i++)
                {
                    var rv = Rvv.Parse(ets[i]);
                    if (!rv.Validate(this))
                    {
                        if (PyrrhoStart.DebugMode || PyrrhoStart.HTTPFeedbackMode)
                            Console.WriteLine("ETag invalid " + ets[i]);
                        throw new DBException("40000", etag);
                    }
                    etag += rv;
                    cx.etags += (dn,etag);
                }
            var es = etag?.ToString()??"";
            if (path.Length >= 4)
            {
                switch (method)
                {
                    case "GET":
                        db.Execute(cx, From._static, method, path, 2, es);
                        break;
                    case "DELETE":
                        db.Execute(cx, From._static,method, path, 2, es);
                        cx = db.Delete(cx, cx.data[cx.result]);
                        break;
                    case "PUT":
                        db.Execute(cx, From._static,method, path, 2, es);
                        cx = db.Put(cx,cx.data[cx.result], sdata);
        //                var rvr = tr.result.rowSet as RvvRowSet;
        //                tr.SetResults(rvr._rs);
                        break;
                    case "POST":
                        db.Execute(cx, From._static,id + ".", path, 2, es);
                        //                tr.stack = tr.result?.acts ?? BTree<string, Activation>.Empty;
                        cx = db.Post(cx, cx.data[cx.result],sdata);
                        break;
                }
            }
            else
            {
                switch (method)
                {
                    case "POST":
                        new Parser(cx).ParseSql(sdata, Domain.Content).Commit(cx);
                        break;
                }
            }
            return cx;
        }
        /// <summary>
        /// REST service implementation
        /// </summary>
        /// <param name="ro"></param>
        /// <param name="path"></param>
        /// <param name="p"></param>
        internal void Execute(Context cx, From f,string method, string[] path, int p, string etag)
        {
            if (p >= path.Length || path[p] == "")
            {
                //               f.Validate(etag);
                var rs = f.RowSets(cx, CTree<long, RowSet.Finder>.Empty);
                cx.result = rs.defpos;
                return;
            }
            string cp = path[p];
            int off = 0;
            string[] sp = cp.Split(' ');
            CallStatement fc = null;
            switch (sp[0])
            {
                case "edmx":
                    break;
                case "table":
                    {
                        var tbs = cp.Substring(6 + off);
                        tbs = WebUtility.UrlDecode(tbs);
                        var tbn = new Ident(tbs, 0);
                        var tb = objects[cx.db.role.dbobjects[tbn.ident]] as Table
                            ?? throw new DBException("42107", tbn).Mix();
                        f = new From(new Ident("",uid + 4 + off), cx, tb);
                            
                        //       if (schemaKey != 0 && schemaKey != ro.defs[f.target.defpos].lastChange)
                        //           throw new DBException("2E307", tbn).Mix();
                        //       f.PreAnalyse(transaction);
                        break;
                    }
                case "procedure":
                    {
                        if (fc == null)
                        {
                            var pn = cp.Substring(10 + off);
#if (!SILVERLIGHT) && (!ANDROID)
                            pn = WebUtility.UrlDecode(pn);
#endif
                            fc = new Parser(cx).ParseProcedureCall(pn,Domain.Content);
                        }
                        var pr = GetProcedure(fc.name,(int)fc.parms.Count) ??
                            throw new DBException("42108", fc.name).Mix();
                        pr.Exec(cx, fc.parms);
                        break;
                    }
                case "key":
                    {
                        var ix = (objects[f.target] as Table)?.FindPrimaryIndex(this);
                        if (ix != null)
                        {
                            var kt = (ObInfo)role.infos[ix.defpos];
                            var kn = 0;
                            while (kn < ix.keys.Count && p < path.Length)
                            {
                                var sk = path[p];
                                if (kn == 0)
                                    sk = sk.Substring(4 + off);
#if (!SILVERLIGHT) && (!ANDROID)
                                sk = WebUtility.UrlDecode(sk);
#endif
                                var tc = (TableColumn)kt.domain.ObjFor(cx,sk);
                                TypedValue kv = null;
                                var ft = tc.domain;
                                try
                                {
                                    kv = ft.Parse(uid,sk);
                                }
                                catch (System.Exception)
                                {
                                    break;
                                }
                                kn++;
                                p++;
                                var cond = new SqlValueExpr(1, cx, Sqlx.EQL,
                                    new SqlCopy(2,cx,"",f.defpos,tc.defpos),
                                    new SqlLiteral(3,cx,kv,ft),Sqlx.NO);
                                f = (From)f.AddCondition(cx,Query.Where,cond,false);
                            }
                            var rs = f.RowSets(cx, CTree<long, RowSet.Finder>.Empty);
                            cx.result = rs.defpos;
                            break;
                        }
                        string ks = cp.Substring(4 + off);
#if (!SILVERLIGHT) && (!ANDROID)
                        ks = WebUtility.UrlDecode(ks);
#endif
                        TRow key = null;
                        var dt = cx.val.dataType;
                        if (dt == null)
                            throw new DBException("42111", cp).Mix();
                        key = (TRow)dt.Parse(new Scanner(uid,ks.ToCharArray(),0));
                        break;
                    }
                case "where":
                    {
                        string ks = cp.Substring(6 + off);
#if (!SILVERLIGHT) && (!ANDROID)
                        ks = WebUtility.UrlDecode(ks);
#endif
                        if (f == null)
                            throw new DBException("42000", ks).ISO();
                        string[] sk = null;
                        if (ks.Contains("={") || ks[0] == '{')
                            sk = new string[] { ks };
                        else
                            sk = ks.Split(',');
                        var n = sk.Length;
                        var psr = new Parser(cx);
                        f = (From)f.AddCondition(cx,Query.Where,
                            psr.ParseSqlValue(sk[0], Domain.Bool).Disjoin(cx));
                        //           if (f.target.SafeName(this) == "User")
                        //           {
                        TypedValue[] wh = new TypedValue[n];
                        var sq = new Ident[n];
                        for (int j = 0; j < n; j++)
                        {
                            string[] lr = sk[j].Split('=');
                            if (lr.Length != 2)
                                throw new DBException("42000", sk[j]).ISO();
                            var cn = lr[0];
                            var sc = f.domain.ObjFor(cx,cn)
                                ?? throw new DBException("42112", cn).Mix();
                            var ct = sc.domain;
                            var cv = lr[1];
                            wh[j] = ct.Parse(uid,cv);
                            sq[j] = new Ident(cn, 0);
                        }
                        //               Authentication(transaction.result.rowSet, wh, sq); // 5.3 this is a no-op if the targetName is not User
                        //             }
                        break;
                    }
                case "select":
                    {
                        string ss = cp.Substring(8 + off);
                        string[] sk = cp.Split(',');
                        int n = sk.Length;
                        var qout = new CursorSpecification(uid+4+off)
                            +(CursorSpecification.Union,f.defpos)+(DBObject._From,f.from); // ???
                        var qin = f;
                        var rs = f.RowSets(cx, CTree<long, RowSet.Finder>.Empty);
                        cx.result = rs.defpos;
                        for (int j = 0; j < n; j++)
                        {
                            var cn = sk[j];
                            cn = WebUtility.UrlDecode(cn);
                            var cd = new Ident(cn, j);
                            qout += (Domain.RowType,qout.rowType + j);
                        }
                        break;
                    }
                case "distinct":
                    {
                        if (cp.Length < 10)
                        {
                            cx.val = new DistinctRowSet(cx,cx.data[cx.result]).First(cx);
                            break;
                        }
                        string[] ss = cp.Substring(9).Split(',');
                        // ???
                        break;
                    }
                case "ascending":
                    {
                        if (cp.Length < 10)
                            throw new DBException("42161", "Column(s)", cp).Mix();
                        string[] ss = cp.Substring(9).Split(',');
                        //??
                        break;
                    }
                case "descending":
                    {
                        if (cp.Length < 10)
                            throw new DBException("42161", "Column(s)", cp).Mix();
                        string[] ss = cp.Substring(9).Split(',');
                        // ??
                        break;
                    }
                case "skip":
                    {
                        //                transaction.SetResults(new RowSetSection(transaction.result.rowSet, int.Parse(cp.Substring(5)), int.MaxValue));
                        break;
                    }
                case "count":
                    {
                        //                transaction.SetResults(new RowSetSection(transaction.result.rowSet, 0, int.Parse(cp.Substring(6))));
                        break;
                    }
                case "of":
                    {
                        var s = cp.Substring(3 + off);
                        var ps = s.IndexOf('(');
                        var key = new string[0];
                        if (ps > 0)
                        {
                            var cs = s.Substring(ps + 1, s.Length - ps - 2);
                            s = s.Substring(0, ps - 1);
                            key = cs.Split(',');
                        }
                        // ??
                        break;
                    }
                case "rvv":
                    {
                        var s = cp.Substring(4 + off);
                        // ??
                        return; // do not break;
                    }
                default:
                    {
                        var cn = sp[0];
                        cn = WebUtility.UrlDecode(cn);
                        var ob = GetObject(cn);
                        if (ob is Table tb)
                        {
                            off = -6;
                            goto case "table";
                        }
                        else if (ob is Role ro)
                        {
                            cx.db += (Role, ro.defpos);
                            Execute(cx, f, method, path, p + 1, etag);
                            return;
                        }
                        if (cn.Contains(":"))
                        {
                            off -= 4;
                            goto case "rvv";
                        }
                        if (cn.Contains("="))
                        {
                            off = -6;
                            goto case "where";
                        }
                        var sv = new Parser(cx).ParseSqlValueItem(cn,Domain.Content);
                        if (sv is SqlProcedureCall pr)
                        {
                            fc = (CallStatement)cx.obs[pr.call];
                            var proc = cx.db.role.procedures[fc.name]?[(int)fc.parms.Count];
                            if (proc != null)
                            {
                                off = -10;
                                goto case "procedure";
                            }
                        }
                        if (f is From fa && objects[fa.target] is Table ta)
                        {
                            var ix = ta.FindPrimaryIndex(this);
                            if (ix != null)
                            {
                                off -= 4;
                                goto case "key";
                            }
                        }
                        if (GetObject(cn) != null)
                        {
                            off = -7;
                            goto case "select";
                        }
                        if (cx.val != null)
                        {
                            off = -4;
                            goto case "key";
                        }
                        throw new DBException("42107", sp[0]).Mix();
                    }
            }
            Execute(cx, f, method, path, p + 1, etag);
        }
        internal override Context Put(Context cx, RowSet r, string s)
        {
            var rs = (SelectedRowSet)r;
            var da = new TDocArray(s);
            var us = r.assig;
            var d = da[0];
            for (var c = rs.rt.First(); c != null; c = c.Next())
            {
                var n = "";
                if (cx.obs[c.value()] is SqlValue sc)
                    n = sc.name;
                else if (cx.db.role.infos[c.value()] is ObInfo ci)
                    n = ci.name;
                var sl = new SqlLiteral(cx.nextHeap++, cx, d[n]);
                cx._Add(sl);
                us += (new UpdateAssignment(c.value(), sl.defpos), true);
            }
            rs += (Query.Assig, us);
            return rs.Update(cx, rs, true);
        }
        internal override Context Post(Context cx, RowSet r, string s)
        {
            var rs = (SelectedRowSet)r;
            var da = new TDocArray(s);
            var rws = BList<(long, TRow)>.Empty;
            for (var i = 0; i < da.Count; i++)
            {
                var d = da[i];
                var vs = CTree<long, TypedValue>.Empty;
                for (var c = rs.rt.First(); c != null; c = c.Next())
                {
                    var n = "";
                    if (cx.obs[c.value()] is SqlValue sc)
                        n = sc.name;
                    else if (cx.db.role.infos[c.value()] is ObInfo ci)
                        n = ci.name;
                    vs += (c.value(), d[n]);
                }
                rws += (cx.nextHeap++, new TRow(r.domain,vs));
            }
            var ers = new ExplicitRowSet(cx.nextHeap++, r.domain, rws);
            cx.data += (ers.defpos, ers);
            rs += (RowSet._Source, ers.defpos);
            return rs.Insert(cx, rs, true, "", null);
        }
        /// <summary>
        /// Implement Grant or Revoke
        /// </summary>
        /// <param name="grant">true=grant,false=revoke</param>
        /// <param name="pr">the privilege</param>
        /// <param name="obj">the database object</param>
        /// <param name="grantees">a list of grantees</param>
        void DoAccess(Context cx, bool grant, Grant.Privilege pr, long obj,
            DBObject[] grantees)
        {
            var np = cx.db.nextPos;
            if (grantees != null) // PUBLIC
                foreach (var mk in grantees)
                {
                    long gee = -1;
                    gee = mk.defpos;
                    if (grant)
                        cx.Add(new Grant(pr, obj, gee, np++, cx));
                    else
                        cx.Add(new Revoke(pr, obj, gee, np++, cx));
                }
        }
        /// <summary>
        /// Implement Grant/Revoke on a list of TableColumns
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="tb">the database</param>
        /// <param name="pr">the privileges</param>
        /// <param name="tb">the table</param>
        /// <param name="list">(Privilege,columnnames[])</param>
        /// <param name="grantees">a list of grantees</param>
        void AccessColumns(Context cx,bool grant, Grant.Privilege pr, Table tb, PrivNames list, DBObject[] grantees)
        {
            var rt = role.infos[tb.defpos] as ObInfo;
            var ne = list.cols != BTree<string, bool>.Empty;
            for (var b = rt.domain.representation.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var ci = (ObInfo)cx.db.role.infos[p];
                if (ci == null 
                    || (ne && !list.cols.Contains(ci.name)))
                    continue;
                list.cols -= ci.name;
                DoAccess(cx,grant, pr, p, grantees);
            }
            if (list.cols.First()?.key() is string cn)
                throw new DBException("42112", cn);
        }
        /// <summary>
        /// Implement grant/revoke on a Role
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="roles">a list of Roles (ids)</param>
        /// <param name="grantees">a list of Grantees</param>
        /// <param name="opt">whether with ADMIN option</param>
		internal void AccessRole(Context cx,bool grant, string[] rols, DBObject[] grantees, bool opt)
        {
            var db = this;
            Grant.Privilege op = Grant.Privilege.NoPrivilege;
            if (opt == grant) // grant with grant option or revoke
                op = Grant.Privilege.UseRole | Grant.Privilege.AdminRole;
            else if (opt && !grant) // revoke grant option for
                op = Grant.Privilege.AdminRole;
            else // grant
                op = Grant.Privilege.UseRole;
            foreach (var s in rols)
            {
                if (!roles.Contains(s))
                    throw Exception("42135", s).Mix(); 
                var ro = (Role)objects[roles[s]];
                DoAccess(cx,grant, op, ro.defpos, grantees);
            }
        }
        /// <summary>
        /// Implement grant/revoke on a database obejct
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="privs">the privileges</param>
        /// <param name="dp">the database object defining position</param>
        /// <param name="grantees">a list of grantees</param>
        /// <param name="opt">whether with GRANT option (grant) or GRANT for (revoke)</param>
        internal void AccessObject(Context cx, bool grant, PrivNames[] privs, long dp, DBObject[] grantees, bool opt)
        {
            var db = this;
            var ob = (DBObject)objects[dp];
            var gd = (ObInfo)role.infos[dp];
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
                    for (var cp = gd.domain.rowType.First(); cp != null; cp = cp.Next())
                    {
                        var c = cp.value();
                        var ci = (ObInfo)role.infos[c];
                        gp = ci.priv;
                        var pp = defp;
                        if (grant)
                            pp = gp;
                        DoAccess(cx,grant, pp, c, grantees);
                    }
                }
            }
            else
                foreach (var mk in privs)
                {
                    Grant.Privilege q = Grant.Privilege.NoPrivilege;
                    switch (mk.priv)
                    {
                        case Sqlx.SELECT: q = Grant.Privilege.Select; break;
                        case Sqlx.INSERT: q = Grant.Privilege.Insert; break;
                        case Sqlx.DELETE: q = Grant.Privilege.Delete; break;
                        case Sqlx.UPDATE: q = Grant.Privilege.Update; break;
                        case Sqlx.REFERENCES: q = Grant.Privilege.References; break;
                        case Sqlx.EXECUTE: q = Grant.Privilege.Execute; break;
                        case Sqlx.TRIGGER: break; // ignore for now (?)
                        case Sqlx.USAGE: q = Grant.Privilege.Usage; break;
                        case Sqlx.OWNER:
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
                        AccessColumns(cx,grant, q, (Table)ob, mk, grantees);
                    }
                    else
                        p |= q;
                }
            if (changed)
                DoAccess(cx,grant, p, ob?.defpos ?? 0, grantees);
        }
        /// <summary>
        /// Called from the Parser.
        /// Create a new level 2 index associated with a referential constraint definition.
        /// We defer adding the Index to the Participant until we are sure all Columns are set up.
        /// </summary>
        /// <param name="tb">A table</param>
        /// <param name="name">The name for the index</param>
        /// <param name="key">The set of TableColumns defining the foreign key</param>
        /// <param name="refTable">The referenced table</param>
        /// <param name="refs">The set of TableColumns defining the referenced key</param>
        /// <param name="ct">The constraint type</param>
        /// <param name="afn">The adapter function if specified</param>
        /// <param name="cl">The set of Physicals being gathered by the parser</param>
        public void AddReferentialConstraint(Context cx,Table tb, Ident name,
            CList<long> key,Table rt, CList<long> refs, PIndex.ConstraintType ct, 
            string afn)
        {
            Index rx = null;
            if (refs == null || refs.Count == 0)
                rx = rt.FindPrimaryIndex(this);
            else
                rx = rt.FindIndex(this,refs);
            if (rx == null)
                throw new DBException("42111").Mix();
            if (rx.keys.Count != key.Count)
                throw new DBException("22207").Mix();
            var np = nextPos;
            var pc = new PIndex2(name.ident, tb.defpos, key, ct, rx.defpos, afn,0,
                np,cx);
            cx.Add(pc);
        }
    }
    /// <summary>
    ///  better implementation of UNDO handler: copy the context stack as well as LocationTransaction states
    /// </summary>
    internal class ExecState
    {
        public Transaction mark;
        public Context stack;

        internal ExecState(Context cx)
        {
            mark = cx.tr;
            stack = cx;
        }
    }
}
