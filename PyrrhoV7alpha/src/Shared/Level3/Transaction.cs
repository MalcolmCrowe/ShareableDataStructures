using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System;
using System.Net;

namespace Pyrrho.Level3
{
    /// <summary>
    /// DBObjects with transaction uids are add to the transaction's list of objects.
    /// Transaction itself should also be immutable and shareable
    /// </summary>
    internal class Transaction : Database
    {
        internal const long
            AutoCommit = -396, // bool
            Diagnostics = -294, // BTree<Sql,SqlValue>
            _Mark = -295, // Transaction
            Physicals = -296, // BTree<long,Physical>
            RefObj = -297, // DBObject
            Role = -298, // long
            SchemaKey = -299, // long
            StartTime = -300, //long
            User = -301, // long
            Warnings = -302; // BList<Exception>
        internal override Role role =>roles[_role];
        internal override User user =>(User)roles[_user];
        internal BTree<long,Physical> physicals => 
            (BTree<long,Physical>)mem[Physicals]?? BTree<long,Physical>.Empty;
        internal long _role => (long)mem[Role];
        internal long _user => (long)mem[User];
        internal long startTime => (long)(mem[StartTime] ?? 0);
        internal DBObject refObj => (DBObject)mem[RefObj];
        public BTree<Sqlx, TypedValue> diagnostics =>
            (BTree<Sqlx,TypedValue>)mem[Diagnostics]??BTree<Sqlx, TypedValue>.Empty;
        public BList<System.Exception> warnings =>
            (BList<System.Exception>)mem[Warnings]??BList<System.Exception>.Empty;
        long tid;
        internal override long uid => tid;
        public override long lexeroffset => tid;
        internal Transaction mark => (Transaction)mem[_Mark];
        internal long schemaKey => (long)(mem[SchemaKey]??0);
        internal override long nextTid => (long)mem[NextTid]; 
        internal override bool autoCommit => (bool)(mem[AutoCommit]??true);
        /// <summary>
        /// Physicals, SqlValues and Executables constructed by the transaction
        /// will use virtual positions above this mark (see PyrrhoServer.nextTid)
        /// </summary>
        public const long TransPos = 0x4000000000000000;
        readonly Database parent;
        internal Transaction(Database db,long t,bool auto) :base(db.roles,db.loadpos,db.mem
            +(Role,db.role.defpos)+(User,db.user.defpos)+(StartTime,System.DateTime.Now.Ticks)
            +(NextTid,t+1)+(AutoCommit,auto))
        {
            tid = t;
            parent = db;
        }
        protected Transaction(Transaction t,BTree<long, Role> r, long p, BTree<long, object> m)
            : base(r, p, m)
        {
            tid = t.uid;
            parent = t.parent;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Transaction(this,roles, loadpos, m);
        }
        public override Database New(BTree<long, Role> rs, long c, BTree<long, object> m)
        {
            return new Transaction(this,rs, c, m);
        }
        public override Database New(DBObject ob)
        {
            return new Transaction(this,roles+(role.defpos,role+ob),loadpos,mem+(ob.defpos,ob));
        }
        public override Transaction Transact(long t,bool? auto=null)
        {
            var r = this;
            if (auto == false && autoCommit)
                r += (AutoCommit, false);
            tid = t;
            return r + (NextTid,t+1);
        }
        public override Database RdrClose(Context cx)
        {
            if (!autoCommit)
                return this;
            return Commit(cx);
        }
        /// <summary>
        /// Add an object to a role
        /// </summary>
        /// <param name="d"></param>
        /// <param name="x"></param>
        /// <returns></returns>
        public static Transaction operator +(Transaction d, (Role, DBObject) x)
        {
            return new Transaction(d,d.roles + (x.Item1.defpos, x.Item1 + x.Item2), d.loadpos, d.mem);
        }
        public static Transaction operator +(Transaction d, (long, object) x)
        {
            return new Transaction(d,d.roles, d.loadpos, d.mem + x);
        }
        /// <summary>
        /// Default action for adding a DBObject
        /// </summary>
        /// <param name="d"></param>
        /// <param name="ob"></param>
        /// <returns></returns>
        public static Transaction operator +(Transaction d, DBObject ob)
        {
            return (Transaction)d.New(ob);
        }
        public static Transaction operator+(Transaction d,Physical p)
        {
            d += (Physicals, d.physicals + (p.ppos,p));
            return (Transaction)p.Install(d, d.role, d.loadpos);
        }
        public static Transaction operator-(Transaction t,long x)
        {
            return new Transaction(t,t.roles,t.loadpos,t.mem - x);
        }
        public static Transaction operator +(Transaction d, (Level, long) x)
        {
            return new Transaction(d,d.roles, d.loadpos,
                d.mem + (Levels, d.levels + (x.Item1, x.Item2)) +
                (LevelUids, d.cache + (x.Item2, x.Item1)));
        }
        /// <summary>
        /// Drop an object from a role
        /// </summary>
        /// <param name="d"></param>
        /// <param name="x"></param>
        /// <returns></returns>
        public static Transaction operator -(Transaction d, (Role, DBObject) x)
        {
            return new Transaction(d,d.roles + (x.Item1.defpos, x.Item1 - x.Item2), d.loadpos, d.mem);
        }
        public static Transaction operator -(Transaction d, DBObject ob)
        {
            return new Transaction(d,d.roles + (d.role.defpos, d.role - ob), d.loadpos, d.mem);
        }
        internal override ReadConstraint _ReadConstraint(Context cx, DBObject d)
        {
            Table t = d as Table;
            if (t == null || t.defpos < 0)
                return null;
            ReadConstraint r = cx.rdC[t.defpos];
            var db = this;
            if (r == null)
            {
                r = new ReadConstraint(cx, d.defpos);
                if (t != null)
                    r.check = new CheckUpdate(cx);
                cx.rdC+=(d.defpos, r);
            }
            return r;
        }
        internal override Database Rollback(object e)
        {
            return parent;
        }
        internal override Database Commit(Context cx)
        {
            if (physicals == BTree<long, Physical>.Empty)
                return this;
            // important: both rdr and wr access the database - not the transaction information
            var wr = new Writer(databases[name], dbfiles[name]);
            var rdr = new Reader(databases[name], this.loadpos);
            var tb = physicals.First(); // start of the work we want to commit
            var since = rdr.GetAll(wr.Length, rdr.limit);
            for (var i = 0; i < since.Count; i++)
            {
                for (var cb = cx.rdC.First(); cb != null; cb = cb.Next())
                {
                    var ce = cb.value().Check(since[i]);
                    if (ce != null)
                    {
                        cx.rconflicts++;
                        throw ce;
                    }
                }
                for (var b = tb; b != null; b = b.Next())
                {
                    var ck = since[i].Conflicts(rdr.db, this, b.value());
                    if (ck >= 0)
                    {
                        cx.wconflicts++;
                        throw new DBException("42000", ck, b.key(), "Transaction conflict " + ck 
                            + " on " + b.value());
                    }
                }
            }
            lock (wr.file)
            {
                since = rdr.GetAll(wr.Length, rdr.limit);
                for (var i = 0; i < since.Count; i++)
                {
                    for (var cb = cx.rdC.First(); cb != null; cb = cb.Next())
                    {
                        var ce = cb.value().Check(since[i]);
                        if (ce != null)
                        {
                            cx.rconflicts++;
                            throw ce;
                        }
                    }
                    for (var b = tb; b != null; b = b.Next())
                    {
                        var ck = since[i].Conflicts(rdr.db, this, b.value());
                        if (ck >= 0)
                        {
                            cx.wconflicts++;
                            throw new DBException("42000", ck, b.key(), "Transaction conflict " + ck 
                                + " on " + b.value());
                        }
                    }
                }
                var pt = new PTransaction((int)physicals.Count, user.defpos, role.defpos,this);
                wr.segment = wr.file.Position;
                pt.Commit(wr,this);
                for (var b = physicals.First(); b != null; b = b.Next())
                    b.value().Commit(wr,this);
                wr.PutBuf();
                cx.affected = wr.rvv;
                df.Flush();
                return wr.db.Install();
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
        internal Transaction Execute(Executable e, Context cx)
        {
            var tr = this;
            if (parse != ExecuteStatus.Obey)
                return this;
            Activation a = new Activation(cx, e.label);
            a.exec = e;
            tr = e.Obey(this, cx); // Obey must not call the Parser!
            if (a.signal != null)
            {
                var ex = Exception(a.signal.signal, a.signal.objects);
                for (var s = a.signal.setlist.First(); s != null; s = s.Next())
                    ex.Add(s.key(), s.value().Eval(this, null));
                throw ex;
            }
            return tr;
        }
        /// <summary>
        /// For REST service: do what we should according to the path, mime type and posted data
        /// </summary>
        /// <param name="method">GET/PUT/POST/DELETE</param>
        /// <param name="path">The URL</param>
        /// <param name="mime">The mime type in the header</param>
        /// <param name="sdata">The posted data if any</param>
        internal Transaction Execute(Context cx,string method, string id, string[] path, string mime, 
            string sdata, string etag)
        {
            var db = this;
            var tr = this;
            var ro = db.role;
            /*          if (etag != null)
                      {
                          var ss = etag.Split(';');
                          if (ss.Length > 1)
                              db.CheckRdC(ss[1]);
                      }
          */
            if (path.Length > 2)
            {
                switch (method)
                {
                    case "GET":
                        db.Execute(ro, id + ".", path, 2, etag);
                        break;
                    case "DELETE":
                        db.Execute(ro, id + ".", path, 2, etag);
                        db.Delete(cx.rb._rs);
                        break;
                    case "PUT":
                        db.Execute(ro, id + ".", path, 2, etag);
                        db.Put(cx.rb._rs, sdata);
        //                var rvr = tr.result.rowSet as RvvRowSet;
        //                tr.SetResults(rvr._rs);
                        break;
                    case "POST":
                        db.Execute(cx,ro, Table._static,id + ".", path, 2, etag);
        //                tr.stack = tr.result?.acts ?? BTree<string, Activation>.Empty;
        //                db.Post(tr.result?.rowSet, sdata);
                        break;
                }
            }
            else
            {
                switch (method)
                {
        //            case "GET":
        //                var f = new From(tr, id + "G", new Ident("Role$Class", 0, Ident.IDType.NoInput), null, Grant.Privilege.Select);
        //                f.Analyse(tr);
        //                SetResults(f.rowSet);
         //               break;
                    case "POST":
                        new Parser(tr).ParseProcedureStatement(sdata);
                        break;
                }
            }
            return tr;
        }
        /// <summary>
        /// REST service implementation
        /// </summary>
        /// <param name="ro"></param>
        /// <param name="path"></param>
        /// <param name="p"></param>
        internal void Execute(Context cx, Role ro, Query f,string id, string[] path, int p, string etag)
        {
            if (p >= path.Length || path[p] == "")
            {
                //               f.Validate(etag);
                cx.rb = f.RowSets(this, cx).First(cx);
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
                        f = new Query(uid+4+off, Domain.Null, tbn.ident)
                            ?? throw new DBException("42107", tbn).Mix();
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
                            fc = new Parser(this).ParseProcedureCall(pn);
                        }
                        var pr = role.GetProcedure(fc.name,(int)fc.parms.Count) ??
                            throw new DBException("42108", fc.name).Mix();
                        pr.Exec(this, cx, fc.parms);
                        break;
                    }
                case "key":
                    {
                        var ix = (f as Table)?.FindPrimaryIndex();
                        if (ix != null)
                        {
                            var kn = 0;
                            while (kn < ix.cols.Count && p < path.Length)
                            {
                                var sk = path[p];
                                if (kn == 0)
                                    sk = sk.Substring(4 + off);
#if (!SILVERLIGHT) && (!ANDROID)
                                sk = WebUtility.UrlDecode(sk);
#endif
                                var tc = ix.cols[kn] as TableColumn;
                                TypedValue kv = null;
                                var ft = tc.domain;
                                try
                                {
                                    kv = ft.Parse(sk);
                                }
                                catch (System.Exception)
                                {
                                    break;
                                }
                                kn++;
                                p++;
                                var cond = new SqlValueExpr(1, Sqlx.EQL,
                                    new Selector(tc.name, 2, tc.domain, 0),
                                    new SqlLiteral(3, kv), Sqlx.NO);
                                f += (Query.Where, f.where + (cond.defpos, cond));
                            }
                            cx.rb = f.RowSets(this, cx).First(cx);
                            break;
                        }
                        string ks = cp.Substring(4 + off);
#if (!SILVERLIGHT) && (!ANDROID)
                        ks = WebUtility.UrlDecode(ks);
#endif
                        TRow key = null;
                        var dt = cx.ret.dataType;
                        if (dt == null)
                            throw new DBException("42111", cp).Mix();
                        if (dt.columns == null)
                        {
                            key = new TRow(dt, dt.Parse(ks));
                        }
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
                        f += (Query.Where, new Parser(this).ParseSqlValue(sk[0], Domain.Bool).Disjoin());
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
                            var sc = f.rowType.names[cn] ??
                                throw new DBException("42112", cn).Mix();
                            var ct = sc.nominalDataType;
                            var cv = lr[1];
                            wh[j] = ct.Parse(cv);
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
                        var qout = new Query(uid+4+off, Domain.TableType);
                        cx.rb = f.RowSets(this, cx).First(cx);
                        var qin = cx.rb?._rs.qry;
                        for (int j = 0; j < n; j++)
                        {
                            var cn = sk[j];
                            cn = WebUtility.UrlDecode(cn);
                            var cd = new Ident(cn, 0);
                            qout.Add(qin.rowType.names[cd.ident], cd.ident);
                        }
                        break;
                    }
                case "distinct":
                    {
                        if (cx.rb == null)
                            cx.rb = f.RowSets(this, cx).First(cx);
                        if (cp.Length < 10)
                        {
                            cx.rb = new DistinctRowSet(cx,cx.rb._rs).First(cx);
                            break;
                        }
                        string[] ss = cp.Substring(9).Split(',');
                        // ???
                        break;
                    }
                case "ascending":
                    {
                        if (cx.rb == null)
                            cx.rb = f.RowSets(this, cx).First(cx);
                        if (cp.Length < 10)
                            throw new DBException("42161", "Column(s)", cp).Mix();
                        string[] ss = cp.Substring(9).Split(',');
                        //??
                        break;
                    }
                case "descending":
                    {
                        if (cx.rb == null)
                            cx.rb = f.RowSets(this, cx).First(cx);
                        if (cp.Length < 10)
                            throw new DBException("42161", "Column(s)", cp).Mix();
                        string[] ss = cp.Substring(9).Split(',');
                        var kc = new TColumn[ss.Length];
                        // ??
                        break;
                    }
                case "skip":
                    {
                        if (cx.rb == null)
                            cx.rb = f.RowSets(this, cx).First(cx);
                        //                transaction.SetResults(new RowSetSection(transaction.result.rowSet, int.Parse(cp.Substring(5)), int.MaxValue));
                        break;
                    }
                case "count":
                    {
                        if (cx.rb == null)
                            cx.rb = f.RowSets(this, cx).First(cx);
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
                        var ob = ro.GetObject(cn);
                        if (ob is Table tb)
                        {
                            off = -6;
                            goto case "table";
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
                        var sv = new Parser(this).ParseSqlValueItem(cn);
                        if (sv is SqlProcedureCall pr)
                        {
                            fc = pr.call;
                            var proc = ro.procedures[fc.name]?[(int)fc.parms.Count];
                            if (proc != null)
                            {
                                off = -10;
                                goto case "procedure";
                            }
                        }
                        if (f is Table ta)
                        {
                            var ix = ta.FindPrimaryIndex();
                            if (ix != null)
                            {
                                off -= 4;
                                goto case "key";
                            }
                        }
                        if (ro.GetObject(cn) != null)
                        {
                            off = -7;
                            goto case "select";
                        }
                        if (cx.rb != null)
                        {
                            off = -4;
                            goto case "key";
                        }
                        throw new DBException("42107", sp[0]).Mix();
                    }
            }
            Execute(ro, id + "." + p, path, p + 1, etag);
        }

        /// <summary>
        /// Implement Grant or Revoke
        /// </summary>
        /// <param name="grant">true=grant,false=revoke</param>
        /// <param name="pr">the privilege</param>
        /// <param name="obj">the database object</param>
        /// <param name="grantees">a list of grantees</param>
        Transaction DoAccess(bool grant, Grant.Privilege pr, long obj, DBObject[] grantees)
        {
            var d = this;
            if (grantees == null) // PUBLIC
            {
                if (grant)
                    d+=new Grant(pr, obj, -1L, tid, d);
                else
                    d+=new Revoke(pr, obj, -1L, tid, d);
                return d;
            }
            foreach (var mk in grantees)
            {
                long gee = -1;
                gee = mk.defpos;
                if (grant)
                    d+=new Grant(pr, obj, gee, tid,d);
                else
                    d+=new Revoke(pr, obj, gee, tid,d);
            }
            return d;
        }
        /// <summary>
        /// Implement Grant/Revoke on a list of TableColumns
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="tb">the database</param>
        /// <param name="pr">the privileges</param>
        /// <param name="tb">the table</param>
        /// <param name="list">a list of TableColumns</param>
        /// <param name="grantees">a list of grantees</param>
        void AccessColumns(bool grant, Grant.Privilege pr, Table tb, PrivNames list, DBObject[] grantees)
        {
            bool SelectCond = pr.HasFlag(Grant.Privilege.Select);
            bool InsertCond = pr.HasFlag(Grant.Privilege.Insert);
            bool UpdateCond = pr.HasFlag(Grant.Privilege.Update);
            int inserts = 0; // for testing whether any columns are permitted
            if (InsertCond)
                for (var cp = tb.rowType.names.First(); cp != null; cp = cp.Next())
                    if (cp.value() is TableColumn tc)
                        inserts++;
            for (int i = 0; i < list.names.Length; i++)
            {
                var cn = list.names[i];
                var co = tb.rowType.names[cn] as TableColumn;
                if (co == null)
                    throw Exception("42112", cn).Mix();
                if (SelectCond && ((co.generated != PColumn.GenerationRule.No) || co.notNull))
                    SelectCond = false;
                if (co.notNull)
                    inserts--;
                if (InsertCond && (co.generated != PColumn.GenerationRule.No))
                    throw Exception("28107", cn).Mix();
                DoAccess(grant, pr, co.defpos, grantees);
            }
            if (grant && SelectCond)
                throw Exception("28105").Mix();
            if (inserts > 0)
                throw Exception("28106").Mix();
        }
        /// <summary>
        /// Implement grant/revoke on a Role
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="roles">a list of Roles (ids)</param>
        /// <param name="grantees">a list of Grantees</param>
        /// <param name="opt">whether with ADMIN option</param>
		internal Transaction AccessRole(bool grant, string[] rols, DBObject[] grantees, bool opt)
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
                Role ro = null;
                for (var b = roles.First(); b != null; b = b.Next())
                    if (b.value().name == s)
                        ro = b.value();
                if (ro == null)
                    throw Exception("42135", s).Mix();
                db = DoAccess(grant, op, ro.defpos, grantees);
            }
            return db;
        }
        /// <summary>
        /// Implement grant/revoke on a database obejct
        /// </summary>
        /// <param name="grant">true=grant, false=revoke</param>
        /// <param name="priv">the privileges</param>
        /// <param name="ob">the database object defining position</param>
        /// <param name="grantees">a list of grantees</param>
        /// <param name="opt">whether with GRANT option (grant) or GRANT for (revoke)</param>
        internal void AccessObject(bool grant, PrivNames[] priv, long ob, DBObject[] grantees, bool opt)
        {
            var db = this;
            var t = (DBObject)role.objects[ob];
            Grant.Privilege defp = Grant.Privilege.NoPrivilege;
            if (!grant)
                defp = (Grant.Privilege)0x3fffff;
            var p = defp; // the privilege being granted
            var gp = t.priv; // grantor's privileges
            var changeTable = true;
            if (priv == null) // all (grantor's) privileges
            {
                if (grant)
                    p = gp;
                if (t is Table tb)
                    for (var cp = tb.rowType.columns.First(); cp != null; cp = cp.Next())
                    {
                        var tc = cp.value() as TableColumn;
                        gp = tc.priv;
                        var pp = defp;
                        if (grant)
                            pp = gp;
                        DoAccess(grant, pp, tc.defpos, grantees);
                    }
            }
            else
                foreach (var mk in priv)
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
                    Grant.Privilege pp = (Grant.Privilege)(((int)q) << 0x400);
                    if (opt == grant)
                        q |= pp;
                    else if (opt && !grant)
                        q = pp;
                    if (mk.names.Length != 0)
                    {
                        if (changeTable)
                            changeTable = grant;
                        AccessColumns(grant, q, (Table)t, mk, grantees);
                    }
                    else
                        p |= q;
                }
            if (changeTable)
                DoAccess(grant, p, t?.defpos ?? 0, grantees);
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
        public Transaction AddReferentialConstraint(Table tb, Ident name, BList<Selector> key,
                Table rt, BList<Selector> refs, PIndex.ConstraintType ct, string afn)
        {
            var r = this;
            Index rx = null;
            if (refs == null)
                rx = rt.FindPrimaryIndex();
            else
                rx = rt.FindIndex(refs);
            if (rx == null)
                throw new DBException("42111").Mix();
            if (rx.cols.Count != key.Count)
                throw new DBException("22207").Mix();
            if (afn == "")
            {
                PIndex pc = new PIndex(name.ident, tb.defpos, key, ct, rx.defpos, 
                    tid, this);
                r += pc;
            }
            else
            {
                // should really do some type checking here
                PIndex1 pc = new PIndex1(name.ident, tb.defpos, key, ct, rx.defpos, afn, 
                    tid, this);
                r += pc;
            }
            return r;
        }
    }
    /// <summary>
    ///  better implementation of UNDO handler: copy the context stack as well as LocationTransaction states
    /// </summary>
    internal class ExecState
    {
        public Transaction mark;
        public Context stack;

        internal ExecState(Transaction t,Context cx)
        {
            mark = t;
            stack = cx;
        }
    }
}
