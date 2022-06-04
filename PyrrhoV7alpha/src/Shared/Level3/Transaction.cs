using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System;
using System.Net;
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
    /// DBObjects with transaction uids are add to the transaction's list of objects.
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
            Diagnostics = -280, // BTree<Sqlx,TypedValue>
            _ETags = -462, // BTree<string,string> url, ETag
            Physicals = -250, // BTree<long,Physical>
            StartTime = -217, // DateTime
            Step = -276, // long
            TriggeredAction = -288; // long
        public BTree<Sqlx, TypedValue> diagnostics =>
            (BTree<Sqlx,TypedValue>)mem[Diagnostics]??BTree<Sqlx, TypedValue>.Empty;
        internal override long uid => (long)(mem[NextId]??-1L);
        public override long lexeroffset => uid;
        internal long step => (long)(mem[Step] ?? TransPos);
        internal override long nextPos => (long)(mem[NextPos]??TransPos);
        internal override string source => (string)mem[SelectStatement.SourceSQL];
        internal BTree<long, Physical> physicals =>
            (BTree<long,Physical>)mem[Physicals]??BTree<long, Physical>.Empty;
        internal DateTime startTime => (DateTime)mem[StartTime];
        internal override bool autoCommit => (bool)(mem[AutoCommit]??true);
        internal long triggeredAction => (long)(mem[TriggeredAction]??-1L);
        internal CTree<string,string> etags => 
            (CTree<string,string>)mem[_ETags]??CTree<string,string>.Empty;
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
        /// <param name="sce"></param>
        /// <param name="auto"></param>
        internal Transaction(Database db,long t,string sce,bool auto) 
            :base(db.loadpos,db.mem+(NextId,t+1)
            +(AutoCommit,auto)+(SelectStatement.SourceSQL,sce))
        { }
        protected Transaction(Transaction t,long p, BTree<long, object> m)
            : base(p, m)
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new Transaction(this,loadpos, m);
        }
        public override Database New(long c, BTree<long, object> m)
        {
            return new Transaction(this, c, m);
        }
        public override Transaction Transact(long t,string sce,Connection con,bool? auto=null)
        {
            var r = this;
            if (auto == false && autoCommit)
                r += (AutoCommit, false);
            // Ensure the correct role amd user combination
            r += (Step, r.nextPos);
            if (t>=TransPos) // if sce is tranaction-local, we need to make space above nextIid
                r = r+ (NextId,t+1)+(SelectStatement.SourceSQL,sce);
            return r;
        }
        public override Database RdrClose(ref Context cx)
        {
            cx.values = CTree<long, TypedValue>.Empty;
            cx.cursors = BTree<long, Cursor>.Empty;
            cx.obs = ObTree.Empty;
            cx.result = -1L;
            // but keep rdC, etags
            if (!autoCommit)
                return this;
            else
            {
                var r = cx.db.Commit(cx);
                var aff = cx.affected;
                cx = new Context(r,cx.conn);
                cx.affected = aff;
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
            if (cx.parse != ExecuteStatus.Obey && cx.parse!=ExecuteStatus.Compile)
                return;
            cx.db += (Physicals,physicals +(ph.ppos, ph));
            cx.db += (NextPos, ph.ppos + 1);
            ph.Install(cx, lp);
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
                wr.cx.nextStmt = databases[name].nextStmt; // may have changed!
                wr.oldStmt = wr.cx.nextStmt;
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
            if (physicals == BTree<long, Physical>.Empty && 
                (autoCommit || (cx.rdC.Count==0 && (cx.db as Transaction)?.etags==null)))
                return Rollback();
            // check for the case of an ad-hoc user that does not need to commit
            if (physicals.Count == 1L && physicals.First().value() is PUser)
                return Rollback();
            for (var b=cx.deferred.First();b!=null;b=b.Next())
            {
                var ta = b.value();
                ta.defer = false;
                ta.db = this;
                ta.Exec();
            }
            if (!autoCommit)
                for (var b = (cx.db as Transaction)?.etags.First(); b != null; b = b.Next())
                    if (b.key() != name)
                        cx.CheckRemote(b.key(),b.value()); 
            // Both rdr and wr access the database - not the transaction information
            var db = databases[name];
            var rdr = new Reader(new Context(db), loadpos);
            var wr = new Writer(new Context(db), dbfiles[name]);
            wr.cx.nextHeap = cx.nextHeap; // preserve Compiled objects framing
            wr.cx.nextStmt = cx.db.nextStmt;
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
                    var p = b.value();
                    var ce = ph.Conflicts(rdr.context.db, cx, p, pt);
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
                rdr = new Reader(new Context(db), ph?.ppos ?? loadpos); 
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
                if (physicals.Count == 0)
                    return Rollback();
                var pt = new PTransaction((int)physicals.Count, user.defpos, db._role,
                        nextPos, cx);
                cx.Add(pt);
                wr.segment = wr.file.Position;
                var (tr, _) = pt.Commit(wr, this);
                for (var b = physicals.First(); b != null; b = b.Next())
                {
                    (tr, _) = b.value().Commit(wr, tr);
                    if (PyrrhoStart.TutorialMode)
                        Console.WriteLine("Committed " + b.value());
                }
                cx.affected = (cx.affected ?? Rvv.Empty) + wr.cx.affected;
                wr.PutBuf();
                df.Flush();
                wr.cx.db += (NextStmt, cx.nextStmt); // not wr.cx.nextStmt
                wr.cx.db += (LastModified, System.IO.File.GetLastWriteTimeUtc(name));
                wr.cx.result = -1L;
                var r = new Database(wr.Length,wr.cx.db.mem);
                lock (_lock)
                    databases += (name, r);
                cx.db = r;
                return r;
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
        internal Context Execute(Executable e, Context cx)
        {
            if (cx.parse != ExecuteStatus.Obey)
                return cx;
            var a = new Activation(cx,e.label);
            a.exec = e;
            var ac = e.Obey(a); 
            if (a.signal != null)
            {
                var ex = Exception(a.signal.signal, a.signal.objects);
                for (var s = a.signal.setlist.First(); s != null; s = s.Next())
                    ex.Add(s.key(), cx.obs[s.value()].Eval(null));
                throw ex;
            }
            cx.result = -1L;
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
        internal Context Execute(Context cx, string method, string id, string dn, string[] path, 
            string query, string mime, string sdata)
        {
            var db = this;
            cx.inHttpService = true;
            if (path.Length >= 4)
            {
                RowSet fm = new TrivialRowSet(cx);
                var tn = path[3];
                if (cx.db.role.dbobjects.Contains(tn))
                {
                    var p = cx.db.role.dbobjects[tn];
                    if (cx.db.objects[p] is Table t)
                    {
                        var dp = cx.GetUid();
                        var ts = new TableRowSet(dp, cx, p, t.domain);
                        fm = new From(new Ident(tn, new Iix(dp)), cx, ts);
                    }
                }
                switch (method)
                {
                    case "HEAD":
                        cx.result = -1L;
                        break;
                    case "GET":
                        db.Execute(cx, fm, method, dn, path, query, 2);
                        break;
                    case "DELETE":
                        db.Execute(cx, fm, method, dn, path, query, 2);
                        cx = db.Delete(cx, (TableRowSet)cx.obs[cx.result]);
                        break;
                    case "PUT":
                        db.Execute(cx, fm, method, dn, path, query, 2);
                        cx = db.Put(cx,(TableRowSet)cx.obs[cx.result], sdata);
                        break;
                    case "POST":
                        db.Execute(cx, fm, id + ".", dn, path, query, 2);
                        cx = db.Post(cx, (TableRowSet)cx.obs[cx.result],sdata);
                        break;
                }
            }
            else
            {
                switch (method)
                {
                    case "POST":
                        new Parser(cx).ParseSql(sdata);
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
        internal void Execute(Context cx, RowSet f,string method, string dn, string[] path, string query, int p)
        {
            if (p >= path.Length || path[p] == "")
            {
                //               f.Validate(etag);
                var rs = f;
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
                        var tbn = new Ident(tbs, cx.Ix(0));
                        var tb = objects[cx.db.role.dbobjects[tbn.ident]] as Table
                            ?? throw new DBException("42107", tbn).Mix();
                        var lp = cx.Ix(uid + 6 + off);
                        var fm = new From(new Ident("", lp), cx, tb);
                        f = (TableRowSet)cx.obs[fm.source];
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
                        var ts = f as TableRowSet ?? cx.obs[f.target] as TableRowSet;
                        var ix = (objects[f.target] as Table)?.FindPrimaryIndex(this);
                        if (ix != null)
                        {
                            var kt = (ObInfo)role.infos[ix.defpos];
                            var kn = 0;
                            var fl = CTree<long, TypedValue>.Empty;
                            while (kn < ix.keys.Count && p < path.Length)
                            {
                                var sk = path[p];
                                if (kn == 0)
                                    sk = sk.Substring(4 + off);
#if (!SILVERLIGHT) && (!ANDROID)
                                sk = WebUtility.UrlDecode(sk);
#endif
                                var tc = (TableColumn)cx.obs[ix.keys[kn]];
                                TypedValue kv = null;
                                var ft = cx._Dom(tc);
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
                                fl += (ts.iSMap[tc.defpos], kv);
                            }
                            var rs = (RowSet)cx.Add(f + (RowSet._Matches,fl));
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
                        var iix = cx.Ix(cp.Length + Analysing);
                        f += (RowSet._Where,
                            psr.ParseSqlValue(new Ident(sk[0],iix), Domain.Bool).Disjoin(cx));
                        cx.Add(f);
                        break;
                    }
                case "distinct":
                    {
                        if (cp.Length < 10)
                        {
                            cx.val = new DistinctRowSet(cx,(RowSet)cx.obs[cx.result]).First(cx);
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
                            Execute(cx, f, method, dn, path, query, p + 1);
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
                        if (f is TableRowSet fa && objects[fa.target] is Table ta)
                        {
                            var cs = sp[0].Split(',');
                            var dm = cx._Dom(ta);
                            var ns = CTree<string, long>.Empty;
                            var ss = CList<long>.Empty;
                            for (var c = dm.rowType.First();c!=null;c=c.Next())
                            {
                                var ci = (ObInfo)role.infos[c.value()];
                                ns += (ci.name, c.value());
                            }
                            for (var i = 0; i<cs.Length &&  ns.Contains(cs[i]);i++)
                            {
                                ss += fa.iSMap[ns[cs[i]]];
                            }
                            if (ss!=CList<long>.Empty)
                            {
                                var df = cx._Dom(f);
                                var fd = new Domain(cx.GetUid(),cx,df.kind,df.representation,ss);
                                f = new SelectedRowSet(cx, fd.defpos, f);
                                break;
                            }
                            var ix = ta.FindPrimaryIndex(this);
                            if (ix != null)
                            {
                                off -= 4;
                                goto case "key";
                            }
                        }
                        if (cx.val != null)
                        {
                            off = -4;
                            goto case "key";
                        }
                        throw new DBException("42107", sp[0]).Mix();
                    }
            }
            Execute(cx, f, method, dn, path, query, p + 1);
        }
        internal override Context Put(Context cx, TableRowSet rs, string s)
        {
            var da = new TDocArray(s);
            var tb = (Table)objects[rs.target];
            var ix = tb.FindPrimaryIndex(this);
            var us = rs.assig;
            var ma = CTree<long,TypedValue>.Empty;
            var d = da[0];
            for (var c = cx._Dom(rs).rowType.First(); c != null; c = c.Next())
            {
                var n = "";
                var isk = false;
                if (cx.obs[c.value()] is SqlValue sv)
                {
                    n = sv.name;
                    if (sv is SqlCopy sc)
                        isk = ix.keys.Has(sc.copyFrom);
                }
                else if (cx.db.role.infos[c.value()] is ObInfo ci)
                    n = ci.name;
                if (isk)
                    ma += (c.value(), d[n]);  
                else
                {
                    var sl = new SqlLiteral(cx.GetUid(), cx, d[n]);
                    cx._Add(sl);
                    us += (new UpdateAssignment(c.value(), sl.defpos), true);
                }
            }
            rs += (RowSet._Matches, ma);
            rs += (RowSet.Assig, us);
            cx.Add(rs);
            rs = (TableRowSet)cx.obs[rs.defpos];
            var ta = rs.Update(cx, rs, true)[rs.target];
            ta.db = cx.db;
            rs.First(cx);
            ta.cursors = cx.cursors;
            ta.EachRow(0);
            cx.db = ta.db;
            return cx;
        }
        internal override Context Post(Context cx, TableRowSet rs, string s)
        {
            var da = new TDocArray(s);
            var rws = BList<(long, TRow)>.Empty;
            var dm = cx._Dom(rs);
            for (var i = 0; i < da.Count; i++)
            {
                var d = da[i];
                var vs = CTree<long, TypedValue>.Empty;
                for (var c = dm.rowType.First(); c != null; c = c.Next())
                {
                    var n = "";
                    if (cx.obs[c.value()] is SqlValue sc)
                        n = sc.name;
                    else if (cx.db.role.infos[c.value()] is ObInfo ci)
                        n = ci.name;
                    vs += (c.value(), d[n]);
                }
                rws += (cx.GetUid(), new TRow(dm, vs));
            }
            var ers = new ExplicitRowSet(cx.GetUid(), cx, dm, rws);
            cx.Add(ers);
            var cu = ers.First(cx);
            rs = rs + (Index.Tree,ers.tree) +(Index.Keys,ers.keys);
            var ta = rs.Insert(cx, rs, true, rs.sRowType)[rs.target];
            ta.db = cx.db;
            cx.cursors += (rs.defpos, cu);
            ta.cursors = cx.cursors;
            ta.EachRow(0);
            return cx;
        }
        internal override Context Delete(Context cx, TableRowSet r)
        {
            var fm = (From)cx.obs[r.from];
            var ts = r.Delete(cx, fm, true);
            var ta = ts.First().value();
            for (var b=r.First(cx);b!=null;b=b.Next(cx))
            {
                ta.db = cx.db;
                cx.cursors += (fm.defpos,cx.cursors[r.target]);
                ta.EachRow(b._pos);
                cx.db = ta.db;
            }
            return base.Delete(cx, r);
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
            for (var b = rt.dataType.representation.First(); b != null; b = b.Next())
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
                    for (var cp = gd.dataType.rowType.First(); cp != null; cp = cp.Next())
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
            var pc = new PIndex2(name.ident, tb.defpos, key, ct, rx.defpos, afn,0,
                nextPos,cx);
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
