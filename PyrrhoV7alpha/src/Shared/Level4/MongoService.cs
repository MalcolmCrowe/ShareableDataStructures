using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.IO;
using System.Net;
using System.Net.Sockets;
using PyrrhoBase;
using Pyrrho.Level2; // for Record
using Pyrrho.Level3; // for Database
using Pyrrho.Level4; // for Select
using Pyrrho.Level1; // for DataFile option
using Pyrrho.Common;

namespace Pyrrho
{
#if MONGO
    class MongoService
    {
#if !EMBEDDED
        static TcpListener tcp = null;
        internal static int port = 0;
        internal Socket client;
#endif
        Stream stream;
        internal IContext conn;
        ATree<long, RowSet> cursors = BTree<long, RowSet>.Empty;
        internal string tabname, colname, sysTable = null;
        internal Table table
        {
            get {
                if (tabname == "system")
                    return Context.systemTables[sysTable];
                if (tr == null)
                    return null;
                var n = new Ident(tabname);
                var tb = tr.local.GetTable(n);
                if (tb == null)
                {
                    tr.Execute("create table \"" + tabname + "\"(\"" + colname + "\" document)");
                    tb = tr.local.GetTable(n);
                }
                if (tb == null)
                    throw new DBException("42107", tabname);
                return tb;
            }
        }
        internal TableColumn column
        {
            get
            {
                if (tr == null)
                    return null;
                var tc = table.GetColumn(tr.local, new Ident(colname)) as TableColumn;
                if (tc == null)
                    throw new DBException("42112", colname);
                return tc;
            }
        }
        internal IContext tr = null;
        internal TDocument lastError = null;
        internal int lastreq = 0;
        internal List<Rvv> lastAffected = new List<Rvv>();
        static long _cid = 0;
        internal long cid = ++_cid;
        internal Context ctx;
        internal static SqlDataType MongoType = null;
#if !EMBEDDED
        internal MessageOpcode reply = MessageOpcode.Reply;
        internal MongoService(Socket c)
        {
            conn = new IContext("Mongod");
            lastError = new ReplyDocument(ctx);
            if (MongoType == null)
                MongoType = new SqlDataType(new SqlDataType[] { SqlDataType.Document }, new Ident[] { new Ident("doc", 0) });
            client = c;
            stream = new NetworkStream(client);
  //         ddb = new Debug(this);
        }
#endif
        internal MongoService(Context cx,string tbn)
        {
            conn = cx.transaction;
            tr = cx.transaction;
            ctx = cx;
            lastError = new ReplyDocument(ctx);
            if (MongoType == null)
                MongoType = new SqlDataType(new SqlDataType[] { SqlDataType.Document }, new Ident[] { new Ident( "doc",0) });
            stream = null;
            tabname = tbn;
            colname = "DOC";
        }
#if !EMBEDDED
        internal void Server()
        {
            conn = new IContext("");
            conn.user = PyrrhoStart.user;
            conn.Open();
            try
            {
                for (; ; )
                {
                    reply = MessageOpcode.Reply;
                    byte[] bn = new byte[4];
                    if (Read(bn, 0, 4) < 4)
                        return;
                    var i = 0;
                    var n = GetInt(bn, ref i);
                    if (n <= 4)
                        continue;
                    var bs = new byte[n];
                    for (int j = 0; j < 4; j++)
                        bs[j] = bn[j];
                    if (Read(bs, 4, n - 4) <n-4)
                        return;
                    i = 4;
                    lastreq = GetInt(bs, ref i);
                    i = 12;
                    try
                    {
                        var opcode = (MessageOpcode)GetInt(bs, ref i);
                        if (PyrrhoStart.DebugMode)
                        Console.WriteLine(opcode.ToString());
                        switch (opcode)
                        {
                            case MessageOpcode.Insert:
                                {
                                    var r = new InsertMessage(this, bs, lastreq, ref i);
                                    conn.MongoConnect(this, r.collectionName);
                                    if (tabname == "system")
                                    {
                                        SystemTableInsert(r);
                                        break;
                                    }
                                    var d = tr.local;
                                    var continueOnError = (r.flags & 0) == 1;
                                    foreach (var ins in r.insert)
                                        try
                                        {
                                            var fl = new BTree<long, TypedValue>(column.defpos, ins);
                                            var ph = new Record(table.defpos, fl, d);
                                            d.AddRecord(ph);
                                        }
                                        catch (DBException ex){
                                            if (!continueOnError)
                                                throw ex;
                                        }
                                    conn = tr.Commit();
                                    break;
                                }
                            case MessageOpcode.Update:
                                {
                                    var u = new UpdateMessage(this, bs, lastreq, ref i);
                                    conn.MongoConnect(this, u.collectionName);
                                    var d = tr.local;
                                    var sr = u.query;
                                    var fm = new From(this, u.query);
                                    fm.Analyse(tr.context);
                                    var upsert = (u.flags & 1) == 1;
                                    var multi = (u.flags&2)==2;
                                    // multi is allowed only if the update doc is all operators
                                    for (var c = u.update.content.First(); c!=null;c=c.Next()) 
                                        if (!c.value().name.ident.StartsWith("$"))
                                            multi = false;
                                    var nn = 0;
                                    for (var b = fm.rowSet.First();b!=null;b=b.Next())
                                    {
                                        var nd = (b.Value(colname) as TDocument).Update(tr,u.update);
                                        if (nd == null)
                                            continue;
                                        d.AddUpdate(new Update(b._pos,new BTree<long, TypedValue>(column.defpos, nd), table.defpos, d));
                                        nn++;
                                        if (!multi)
                                            break;
                                    }
                                    if (nn == 0 && upsert)
                                        d.AddRecord(new Record(table.defpos, new BTree<long, TypedValue>(column.defpos, u.update), d));
                                    conn = tr.Commit();
                                    break;
                                }
                            case MessageOpcode.Query:
                                {
                                    var q = new QueryMessage(this, bs, lastreq, ref i);
                                    if (q.databaseNameAndSuffix.EndsWith("$cmd"))
                                    {
                                        if (q.databaseNameAndSuffix != "admin.$cmd")
                                            conn.MongoConnect(this, q.databaseNameAndSuffix);
                                        QueryCommand(q);
                                    }
                                    else
                                    {
                                        conn.MongoConnect(this, q.databaseNameAndSuffix);
                                        if (tabname == "system")
                                            SystemTableQuery(q);
                                        else
                                        {
                                            var fm = new From(this, q.query["query"] as TDocument??q.query);
                                            Ordering(fm,q.query["orderby"] as TDocument);
                                            new ReplyMessage(this).WriteTo(this, stream, fm.rowSet, q);
                                        }
                                    }
                                    break;
                                }
                            case MessageOpcode.GetMore:
                                {
                                    var g = new GetMoreMessage(this, bs, lastreq, ref i);
                                    //         db.MongoConnect(this,g.collectionName);
                                    var rs = cursors[g.cursorId];
                                    var m = new ReplyMessage(this);
                                    if (rs == null)
                                    {
                                        m.doc = new TDocument(ctx);
                                        m.doc.Add("ok", false);
                                        m.WriteTo(this, stream);
                                    }
                                    else
                                        m.WriteTo(this, stream, rs, null);
                                    if (m.numberReturned == 0)
                                        ATree<long, RowSet>.Remove(ref cursors, g.cursorId);
                                    break;
                                }
                            case MessageOpcode.Delete:
                                {
                                    var dm = new DeleteMessage(this, bs, lastreq, ref i);
                                    conn.MongoConnect(this, dm.collectionName);
                                    var d = tr.local;
                                    var sr = dm.delete;
                                    var fm = new From(this, dm.delete);
                                    fm.Analyse(tr.context);
                                    var nn = 0;
                                    var singleRemove = (dm.flags & 1) == 1;
                                    for (var b = fm.rowSet.First();b!=null;b=b.Next())
                                    {
                                        d.AddDelete(new Delete(b._defpos, d));
                                        nn++;
                                        if (singleRemove)
                                            break;
                                    }
                                    conn = tr.Commit();
                                    break;
                                }
                            case MessageOpcode.KillCursors:
                                {
                                    var k = new KillCursorsMessage(this, bs, lastreq, ref i);
                                    foreach (var c in k.cursorIds)
                                        ATree<long, RowSet>.Remove(ref cursors, c);
                                    var m = new ReplyMessage(this);
                                    m.WriteTo(this, stream);
                                    break;
                                }
                            case MessageOpcode.Command:
                                {
                                    reply = MessageOpcode.CommandReply;
                                    var dbn = GetCString(bs, ref i);
                                    var cmd = GetCString(bs, ref i);
                                    var args = new TDocument(ctx, bs, ref i);
                                    var meta = new TDocument(ctx, bs, ref i);
                                    conn.MongoConnect(this, dbn+".$cmd");
                                    QueryCommand(new QueryMessage(ctx,args));
                                    break;
                                }
                        }
                    }
                    catch (DBException ex)
                    {
                        var m = new ReplyMessage(this);
                        var msg = Resx.Format(ex.signal, ex.objects);
                        m.doc.Add("err", msg);
                        m.doc.Add("errmsg", msg); // Mongo docs unclear
                        var code = 0;
                        int.TryParse(ex.signal, out code);
                        m.doc.Add("code", code);
                        lastError = m.doc;
                        lastAffected = conn.affected;
                        m.WriteTo(this,stream);
                    }
                    catch (NullReferenceException ex)
                    {
                        var m = new ReplyMessage(this);
                        m.doc.Add("err", "Null Reference");
                        m.doc.Add("errmsg", ex.Message); 
                        lastError = m.doc;
                        lastAffected = conn.affected;
                        m.WriteTo(this, stream);
                    }
                    stream.Flush();
                    if (PyrrhoStart.DebugMode)
                    Console.WriteLine("MainLoop");
                }
            }
            catch (Exception e)
            { throw e;
            }
            finally
            {
                stream.Close();
                client.Close();
            }
        }
#endif
        int Read(byte[] buf,int off,int len)
        {
            var n = 0;
            while (n<len)
            {
                var m = stream.Read(buf, off + n, len - n);
                if (m <= 0)
                {
                    Console.WriteLine("short read at " + (off + n));
                    return n;
                }
                n += m;
            }
            return n;
        }
        string GetLastComponent(TypedValue c)
        {
            var s = GetString(c);
            if (s==null)
                return null;
            var ss = c.ToString().Split('.');
            return ss[ss.Length-1];
        }
        string GetString(TypedValue c)
        {
            if (c == null || c.Val() == null || c.dataType.kind != Sqlx.CHAR)
                return null;
            return c.ToString();
        }
        TDocument GetDocument(TypedValue c)
        {
            if (c == null || c.Val() == null || c.dataType.kind != Sqlx.DOCUMENT)
                return null;
            return (TDocument)c;
        }
        bool? GetBool(TypedValue c)
        {
            if (c == null || c.Val() == null || c.dataType.kind != Sqlx.BOOLEAN)
                return null;
            return (bool)c.Val();
        }
#if !EMBEDDED
        private void SystemTableQuery(QueryMessage q)
        {
            var mark = tr.Mark();
            var db = tr.local;
            var ns = GetString(q.query["ns"]);
            var sb = new StringBuilder();
            switch (colname)
            {
                case "namespaces":
                    sb.Append("select {name: \"$A\" } from (select '" + db.name + ".'||\"Name\" as a from \"Role$Table\")");
                    if (ns != null && ns != "*")
                        sb.Append(" where a ='" + ns + "'");
                    break;
                case "indexes":
                    {
                        new Parser(tr).ParseSql("create function GetKey(ix char) returns document " +
                            "begin " +
                            "  declare b document;" +
                            "  declare p int;" +
                            "  set p = 1+(select position('.' in \"TableColumn\") from \"Role$IndexKey\" where \"IndexName\"=ix fetch first 1 rows only);"+
                            "  for select substring(\"TableColumn\" from p) as d,-1-2*position('Desc' in \"Flags\") as f"+
                            "          from \"Role$IndexKey\" where \"IndexName\"=ix order by \"Position\" do" +
                            "     set b = { '$set': [\"$D\", \"$F\"]}" +
                            "  end for;" +
                            "  return b" +
                            " end");
                        sb.Append("select {ns:'" + ns + "',v:1,name:'_id_',key:{'_id':1}} from static");
                        sb.Append(" union select {ns:'" + ns + "',v:1,name: \"$A\",key: \"$GETKEY(A)\",background: \"$B\",dropDups: \"$D\", sparse: \"$S\",unique: \"$U\"}" + 
                            " from (select \"Name\" as a, position('BACKGROUND' in \"Flags\")>=0 as b,"+
                            "position('DROPDUPS' in \"Flags\")>=0 as d,position('SPARSE' in \"Flags\")>=0 as s,position('Unique' in \"Flags\")>=0 as u"+
                            " from \"Role$Index\"");
                        if (ns != null && ns != "*")
                            sb.Append(" where '" + db.name + ".'||\"Table\"='" + ns + "'");
                        sb.Append(")");
                    }
                    break;
            }
            new Parser(tr).ParseSql(sb.ToString());
            new ReplyMessage(this).WriteTo(this,stream,tr.context.result,q);
            tr.Restore(mark);
        }
        void SystemTableInsert(InsertMessage m)
        {
            switch (colname)
            {
                case "indexes":
                    {
                        var ins = m.insert[0];
                        var ns = GetLastComponent(ins["ns"]);
                        var nm = GetString(ins["name"]);
                        var un = GetBool(ins["unique"])??false;
                        var bk = GetBool(ins["background"])??false;
                        var dd = GetBool(ins["dropDups"])??false;
                        var sp = GetBool(ins["sparse"])??false;
                        var kd = GetDocument(ins["key"]);
                        if (ns==null || nm==null || kd==null)
                            return;
                        var sb = new StringBuilder("create index \""+ns+"\" \""+nm+"\"");
                        if (un)
                            sb.Append(" unique");
                        var tb = tr.local.GetTable(new Ident(ns));
                        var dp = tb.GetDocumentColumn(tr.local);
                        if (dp < 0)
                            return;
                        var dc = "\""+ tr.local.objects[dp].CurrentName(tr.local)+"\"";
                        sb.Append("(");
                        var comma = "";
                        for (var kc = kd.content.First();kc!=null;kc=kc.Next())
                        {
                            var s = kc.value().typedValue is TInt && kc.value().typedValue.ToLong() == -1;
                            sb.Append(comma); comma = ",";
                            sb.Append(dc + ".\"" + kc.value().name.ident + "\"" + (s ? " desc" : ""));
                        }
                        sb.Append(")");
                        if (bk) sb.Append(" "+Sqlx.BACKGROUND);
                        if (dd) sb.Append(" "+Sqlx.DROPDUPS);
                        if (sp) sb.Append(" "+Sqlx.SPARSE);
                        var cmd = sb.ToString();
                        if (PyrrhoStart.DebugMode)
                        Console.WriteLine("Executing: "+cmd);
                        new Parser(tr).ParseSql(cmd);
                        break;
                    }
            }
            tr.Commit();
        }
#endif
        void Ordering(From f, TDocument ord)
        {
            if (ord == null)
                return;
            int n = (int)ord.content.Count;
            if (n == 0)
                return;
            var kt = new SqlDataType[n];
            var names = new Ident[n];
            for (int j=0;j<n;j++)
            {
                var s = ord.content[j];
                var t = SqlDataType.Content;
                if (s.typedValue.ToInt() == -1)
                    t = new SqlDataType(t, Sqlx.DESC, Sqlx.FIRST);
                kt[j] = t;
                names[j] = s.name;
            }
            var keyType = new SqlDataType(kt, names);
            var a = new RTree(f.rowSet, new TreeInfo(keyType, TreeBehaviour.Allow, TreeBehaviour.Allow));
            for (var b = f.rowSet.First();b!=null;b=b.Next())
            {
                var ks = new TypedValue[n];
                var rw = b.Value() as TRow;
                for (int j = n - 1; j >= 0; j--)
                    ks[j] = ((TDocument)rw[0])[names[j]];
                RTree.Add(ref a, new TRow(f, keyType, ks), rw);
            }
            f.rowSet = new ValueRowSet(f, a);
        }
        internal ReplyMessage AggregateCommand(QueryMessage q)
        {
            var x = q.databaseNameAndSuffix.IndexOf(".$cmd");
            var coll = q.databaseNameAndSuffix.Substring(0, x);
  //          ddb.Check("Start of Aggregate");
            var fm = new From(this, null); // aggregating so don't match the query
            fm.Analyse(tr.context);
            var rs = fm.rowSet;
            var pi = q.query["pipeline"] as TDocArray;
            for (var p = pi.content.First();p!=null;p=p.Next()) // will use array order
            {
                var pd = p.value() as TDocument;
                if (pd.Contains("$project"))
                    rs = ProjectPipeline(rs, fm, pd);
                else if (pd.Contains("$match"))
                    rs = MatchPipeline(rs, fm, pd);
                else if (pd.Contains("$limit"))
                    rs = LimitPipeline(rs, fm, pd);
                else if (pd.Contains("$skip"))
                    rs = SkipPipeline(rs, fm, pd);
                else if (pd.Contains("$unwind"))
                    rs = UnwindPipeline(rs, fm, pd);
                else if (pd.Contains("$group"))
                    rs = GroupPipeline(rs, fm, pd);
                else if (pd.Contains("$sort"))
                    rs = SortPipeline(rs, fm, pd);
                else if (pd.Contains("$geoNear"))
                    rs = GeoNearPipline(rs, fm, pd);
            }
            var r = new AggregateResultsMessage(this,rs);
#if !EMBEDDED
            if (stream!=null)
                r.WriteTo(this, stream);
#endif
            return r;
        }

        private RowSet GeoNearPipline(RowSet rs, From fm, TDocument pd)
        {
            throw new NotImplementedException();
        }

        private RowSet SortPipeline(RowSet rs, From fm, TDocument pd)
        {
            var spec = pd["$sort"]as TDocument;
            var dta = new SqlDataType[spec.content.Count];
            var cns = new Ident[spec.content.Count];
            int k = 0;
            int n = rs.rowType.Length;
            for (var f = spec.content.First();f!=null;f=f.Next())
            {
                var dt = SqlDataType.Content;
                var v = f.value();
                cns[k] = v.name;
                var i = fm.ColFor(v.name);
                if (i >= 0)
                    cns[k] = rs.rowType.names[i];
                if (v.typedValue.ToInt() < 0)
                    dt = new SqlDataType(dt, Sqlx.DESC, Sqlx.NULLS);
                dta[k++] = dt;
            }
            var ti = new TreeInfo(new SqlDataType(dta,cns),TreeBehaviour.Allow,TreeBehaviour.Allow);
            return new SortedRowSet(rs.qry,rs, ti);
        }

        private RowSet GroupPipeline(RowSet rs, From fm, TDocument pd)
        {
            var spec = pd["$group"].Build(fm) as SqlDocument;
            ATree<TypedValue, SqlDocument> rows = new CTree<TypedValue, SqlDocument>(fm,rs.keyType);
            for (var b = rs.First();b!=null;b=b.Next())
            {
                var cur = b.Value()[0];
                var k = cur[spec["_id"].ToString()];
                var v = rows[k];
                if (v == null)
                {
                    v = new SqlDocument(fm, SqlDataType.Document);
                    v.Add("_id", k.Build(fm));
                    foreach (var f in spec.doc)
                        if (f.Key != "_id")
                        {
                            var w = f.Value.Copy();
                            w.StartCounter(rs.qry);
                            v.Add(f.Key, w);
                        }
                    ATree<TypedValue, SqlDocument>.Add(ref rows, k, v);
                }
                foreach (var f in v.doc)
                    f.Value.AddIn(rs.qry);
            }
            ExplicitRowSet r = new ExplicitRowSet(fm);
            for (var e = rows.First();e!=null;e=e.Next())
                r.Add(new TRow(fm, rs.rowType, e.value().Eval(fm)));
            return r;
        }

        private RowSet UnwindPipeline(RowSet rs, From fm, TDocument pd)
        {
            return new UnwindRowSet(rs, pd["$unwind"].ToString());
        }

        private RowSet SkipPipeline(RowSet rs, From fm, TDocument pd)
        {
            rs.skip = pd["$limit"].ToInt().Value;
            return rs;
        }

        private RowSet LimitPipeline(RowSet rs, From fm, TDocument pd)
        {
            rs.limit = pd["$limit"].ToInt().Value;
            return rs;
        }

        private RowSet MatchPipeline(RowSet rs, From fm, TDocument pd)
        {
            return new WhereRowSet(rs, new TypedValue[] { pd["$match"] }, new string[] { fm.NameAt(0).ident });
        }

        private RowSet ProjectPipeline(RowSet rs, From fm, TDocument pd)
        {
            var pj = pd["$project"] as TDocument;
            // first look to see if we have the _id exclusion feature
            var ie = pj["_id"]; // but what do we do with it??
            return new SelectRowSet(fm.rowSet, pj.Build(fm));
        }
#if !EMBEDDED
        ReplyMessage AuthenticateCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage BuildInfoCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("bits", 51);
            rdoc.Add("gitVersion", "PyrrhoDB");
            rdoc.Add("sysInfo", PyrrhoStart.Version[1]);
            rdoc.Add("version", "2.1.0");
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage CloneCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage CollModCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage CollStatsCommand(QueryMessage q)
        {
            var tbname = q.query["collstats"].ToString();
            var tb = tr.local.GetTable(new Ident(tbname));
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            var ixc = 0;
            var xa = new TDocArray(ctx);
            for (var xp = tr.local.indexes.First();xp!=null;xp=xp.Next())
            {
                var ix = tr.local.objects[xp.key()] as Index;
                if (ix.tabledefpos != tb.defpos)
                    continue;
                ixc++;
                var ka = new TDocArray(ctx);
                foreach (var cp in ix.cols)
                {
                    var tc = tr.local.objects[cp] as Selector;
                    var ki = new TDocument(ctx);
                    ki.Add("col", new TChar(tc.CurrentName(tr.local)));
                    ka.Add(ki);
                }
                var xi = new TDocument(ctx);
                xi.Add("key", ka);
                xi.Add("size", ix.rows.Count);
                xa.Add(xi);
            }
            rdoc.Add("nindexes", ixc);
            var nd = 0L;
            var sz = 0L;
            for (var cp = tr.local.role.defs[tb.defpos].props.First();cp!=null;cp=cp.Next())
            if (cp.value().HasValue) 
            {
                var te = tr.local.pb.documents[cp.value().Value];
                if (te == null)
                    continue;
                nd += te.Count;
                for (var dc = te.First();dc!=null;dc=dc.Next())
                    sz += dc.value().ToBytes(null).Length;
            }
            rdoc.Add("count", nd); //64bit
            rdoc.Add("indexSizes", xa);
            rdoc.Add("totalIndexSize", 1); // 64bit
            rdoc.Add("numExtents", 1);
            rdoc.Add("lastExtentSize", sz); //64bit
            rdoc.Add("paddingFactor", 1.0);
            var df = tr.local.role.defs[tb.defpos];
            rdoc.Add("storageSize", df.MaxStorageSize);
            rdoc.Add("max", df.MaxDocuments);
            rdoc.Add("size", sz);
            rdoc.Add("avgObjSize", (nd>0)?sz/nd:0);
            rdoc.Add("ns", tbname);
            var ca = df != null && df.Has(Sqlx.CAPPED);
            rdoc.Add("capped", ca);
            rdoc.Add("systemFlags", 1);
            var u2 = df != null && df.Has(Sqlx.USEPOWEROF2SIZES);
            rdoc.Add("userFlags", u2?1:0);
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage CountCommand(QueryMessage q)
        {
            var d = q.query;
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            var x = q.databaseNameAndSuffix.IndexOf(".$cmd");
            var coll = q.databaseNameAndSuffix.Substring(0, x);
            var tv = d["count"];
            var s = tv.ToString();
            if (s != null)
                coll += "." + s;
            tr.MongoConnect(this, coll);
            var db = tr.local;
            tv = d["query"];
            var qy = tv as TDocument;
            var f = new From(this, qy);
            f.Analyse(tr.context);
            conn.result = f.rowSet;
            rdoc.Add("n", GetCount(f));
            rdoc.Add("ok", true);
            return m;
        }
        long GetCount(Query f)
        {
            var count = 0L;
            for (var b = f.rowSet.First();b!=null;b=b.Next())
                count++;
            return count;
        }
        ReplyMessage CreateCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            var x = q.databaseNameAndSuffix.IndexOf(".$cmd");
            var coll = q.databaseNameAndSuffix.Substring(0, x);
            var tv = q.query["create"];
            var s = tv.ToString();
            if (s != null)
                coll += "." + s;
            tr.MongoConnect(this, coll);
            var cp = q.query["capped"];
            var md = new Metadata();
            md.Add(Sqlx.CAPPED);
            var mx = q.query["max"];
            if (mx != null && mx is TInt)
                md.MaxDocuments = mx.ToInt().Value;
            var sz = q.query["size"];
            if (sz != null && sz is TInt)
                md.MaxStorageSize = sz.ToLong().Value;
            if (cp != null || mx!=null || sz!=null)
                tr.local.Add(new PMetadata2(tabname, md, 0,table.defpos, tr.local));
            conn = tr.Commit();
            tr = new IContext(conn);
            rdoc.Add("ok", true);
            return m;
        }
        /// <summary>
        /// Because of TransactionIsolation we report very little current activity!
        /// </summary>
        /// <param name="q"></param>
        /// <returns></returns>
        ReplyMessage CurrentOpCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            var ip = new TDocArray(ctx);
            var v = new TDocument(ctx);
            v.Add("desc", tr.local.transName+tr.cid);
            v.Add("threadId", 0);
            v.Add("connectionId", tr.cid);
            v.Add("opid", q.requestId);
            v.Add("ok", true);
            v.Add("active", true);
            v.Add("secs_running", 0);
            v.Add("microsecs_unning", 0);
            v.Add("op", "query");
            v.Add("ns", tr.local.name + "." + tabname);
            var pd = new TDocument(ctx);
            pd.Add("done", 0);
            pd.Add("total", 1);
            v.Add("progress", pd);
            v.Add("query", q.query);
            v.Add("planSummary", "");
            var ep = client.LocalEndPoint as IPEndPoint;
            v.Add("client", ep.Address.ToString());
            v.Add("locks", new TDocArray(ctx));
            v.Add("waitingforLock", false);
            v.Add("msg", "");
            v.Add("killPending", false);
            v.Add("numYields", 0);
            v.Add("lockStats", new TDocArray(ctx));
            ip.Add(v);
            rdoc.Add("inprog", ip);
            rdoc.Add("ok", true);
            return m;
        }
        /// <summary>
        /// Because of TransactionIsolation we report very little current activity!
        /// </summary>
        /// <param name="q"></param>
        /// <returns></returns>
        ReplyMessage ExplainCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            var v = new TDocument(ctx);
            v.Add("executionSuccess", true);
            v.Add("nReturned", 0);
            v.Add("executionTimeMillis", 0);
            v.Add("totalKeysExamined", 0);
            v.Add("totalDocsExamined", 0);
            v.Add("executionStages", new TDocument(ctx));
            v.Add("allPlansExecution", new TDocument(ctx));
            rdoc.Add("executionStats", v);
            rdoc.Add("ok", true);
            return m;
        }

        ReplyMessage DbStatsCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("avgObjSize", 1.0);
            rdoc.Add("collections", 0);
            rdoc.Add("dataSize", 0); // 64bit
            rdoc.Add("numExtents", 1);
            rdoc.Add("fileSize", 0); // 64bit
            rdoc.Add("indexes", tr.local.indexes.Count+tr.local.pb.documents.Count);
            rdoc.Add("indexSize", 1); // 64bit
            var docCount = 0L;
            for (var di=tr.local.pb.documents.First();di!=null;di=di.Next())
                docCount += di.value().Count;
            rdoc.Add("objects", docCount); //64bit
            rdoc.Add("storageSize", 1);
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage DeleteIndexesCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            var x = q.databaseNameAndSuffix.IndexOf(".$cmd");
            var coll = q.databaseNameAndSuffix.Substring(0, x);
            var tv = q.query["deleteIndexes"];
            var sc = tv.ToString();
            var db = tr.local;
            Table tb = null;
            if (sc != null)
            {
                coll += "." + sc;
                tb = db.GetTable(new Ident(sc));
            }
            tr.MongoConnect(this, coll);
            db = tr.local; // needs to be done again as quite a lot happens inside MongoConnect
            tv = q.query["index"];
            var si = tv.ToString();
            var k = 0;
            var dropped = 0;
            if (tb!=null)
                for (var col = db.pb.documents.First();col!=null;col=col.Next())
                {
                    var pc = db.pb.GetS(col.key()) as PColumn;
                    if (pc.tabledefpos == tb.defpos)
                        k++;
                }
            for (var ix = db.indexes.First();ix!=null;ix=ix.Next())
            {
                var inx = db.GetS(ix.key()) as PIndex;
                // only looking at Mongo indexes
                if ((inx.flags & (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.ForeignKey)) != PIndex.ConstraintType.NoType)
                    continue;
                k++;
                if (tb != null && inx.tabledefpos != tb.defpos)
                    continue;
                if (inx.name.ident == si || si == "*")
                {
                    db.Add(new Drop(inx, db));
                    dropped++;
                }
            }
            conn = tr.Commit();
            tr = new IContext(conn);
            rdoc.Add("nIndexes", k); // the number of indexes BEFORE removing
            rdoc.Add("msg", "all indexes deleted for collection");
            rdoc.Add("ns", sc);
            if (si=="*" || dropped!=0)
                rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage DistinctCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = new TDocument(ctx);
            var ddoc = new TDocArray(ctx);
            var qq = q.query["query"];
            var excm = "select distinct \"" + colname + "\".\"" + q.query["key"].ToString() + "\" from \"" + tabname+"\"";
            if (qq != null)
                excm += " where \"" + colname + "\"=" + qq.ToString();
            tr.Execute(excm);
            for (var b=tr.result.First();b!=null; b=b.Next())
                ddoc.Add(b.Value()[0]);
            rdoc.Add("values",  ddoc);
            rdoc.Add("ok", true);
            m.doc = rdoc;
            return m;
        }
        ReplyMessage DropCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            var x = q.databaseNameAndSuffix.IndexOf(".$cmd");
            var coll = q.databaseNameAndSuffix.Substring(0, x);
            var tv = q.query["drop"];
            var s = tv.ToString();
            if (s != null)
                coll += "." + s;
            tr.MongoConnect(this, coll);
            var db = tr.local;
            var k = 0;
            for (var ix = db.indexes.First();ix!=null;ix=ix.Next())
            {
                var inx = db.GetS(ix.key()) as PIndex;
                if (inx.defpos == table.defpos)
                    k++;
            }
            db.Add(new Drop(db.GetD(table.defpos), db));
            conn = tr.Commit();
            tr = new IContext(conn);
            rdoc.Add("nIndexes", k);
            rdoc.Add("msg", "all indexes deleted for collection");
            rdoc.Add("ns", s);
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage DropDatabaseCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            var x = q.databaseNameAndSuffix.IndexOf(".$cmd");
            var coll = q.databaseNameAndSuffix.Substring(0, x);
            var db = tr.local.database;
            conn = tr.Commit();
            tr = new IContext(conn); // drop database is not transacted
            rdoc.Add("nIndexes", db.indexes.Count);
            rdoc.Add("msg", "all indexes deleted for collection");
            rdoc.Add("ns", coll);
            rdoc.Add("ok", true);
            Database.DetachDatabase(db.pb.df.name);
            var oldName = PyrrhoStart.path + db.pb.df.name;
            var newName = oldName + "." + DateTime.Now.Ticks;
            try
            {
                File.Move(oldName, newName);
            }
            catch (Exception ex)
            {
                throw new DBException("2E203", ex.Message,oldName);
            }
            return m;
        }
        ReplyMessage EvalCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var fn = q.query["$eval"].Build(tr.context);
            var args = q.query["args"].Build(tr.context);
            var ex = new SqlValueExpr(tr.context, Sqlx.CALL, fn, args, Sqlx.NO);
            var rdoc = m.doc;
            rdoc.Add("retval", ex);
            return m;
        }
        ReplyMessage FileMd5Command(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage FindAndModifyCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var coll = q.query["findAndModify"].ToString();
            conn.MongoConnect(this,coll);
            var fm = new From(this, q.query["query"] as TDocument);
            fm.Analyse(ctx);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage GeoNearCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage GeoSearchCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage GetNonceCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("nonce", "2375531c32080ae8");
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage GroupCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        private ReplyMessage InsertCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var x = q.databaseNameAndSuffix.IndexOf(".$cmd");
            var coll = q.databaseNameAndSuffix.Substring(0, x);
            tabname = q.query["insert"].ToString();
            conn.MongoConnect(this, coll+"."+tabname);
            var docs = q.query["documents"] as TDocArray;
            bool ordd = q.query.GetBool("ordered", true);
            var errs = new TDocArray(ctx);
            var ctr = 0;
            if (docs != null)
                for (var d =docs.content.First();d!=null; d=d.Next())
                    try
                    {
                        var db = tr.local;
                        var fl = new BTree<long, TypedValue>(column.defpos, d.value());
                        var ph = new Record(table.defpos, fl, db);
                        db.AddRecord(ph);
                        ctr++;
                    }
                    catch (DBException e)
                    {
                        var er = new TDocument(ctx);
                        er.Add("index", ctr);
                        var code = 0;
                        if (int.TryParse(e.signal, out code))
                            er.Add("code", code);
                        er.Add("errmsg", Resx.Format(e.signal, e.objects));
                        errs.Add(er);
                        if (ordd)
                            break;
                    }
                    catch (Exception e)
                    {
                        var er = new TDocument(ctx);
                        er.Add("index", ctr);
                        er.Add("errmsg", e.Message);
                        errs.Add(er);
                        if (ordd)
                            break;
                    }
            conn = tr.Commit();
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            rdoc.Add("n", ctr);
            if (errs.content.Count > 0)
                rdoc.Add("writeErrors", errs);
            return m;
        }
        private ReplyMessage DeleteCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            conn.MongoConnect(this, q.databaseNameAndSuffix);
            tabname = q.query["delete"].ToString();
            var docs = q.query["deletes"] as TDocArray;
            bool ordd = q.query.GetBool("ordered", true);
            var errs = new TDocArray(ctx);
            var ctr = 0;
            for (var d = docs.content.First();d!=null;d=d.Next())
                try
                {
                    var db = tr.local;
                    var dd = d.value() as TDocument;
                    var qd = dd["q"] as TDocument;
                    var ld = dd["limit"] as TInt;
                    var limit = ld?.value ?? 0;
                    var ct = 0;
                    var fm = new From(this, qd);
                    fm.Analyse(ctx);
                    var rs = fm.rowSet;
                    for (var b = rs.First();b!=null;b=b.Next())
                    {
                        if (b._recpos!=0)
                            db.Add(new Delete(b._recpos, db));
                        ctr++;
                        ct++;
                        if (limit != 0 && ct > limit)
                            break;
                    }
                }
                catch (DBException e)
                {
                    var er = new TDocument(ctx);
                    er.Add("index", ctr);
                    var code = 0;
                    if (int.TryParse(e.signal, out code))
                        er.Add("code", code);
                    er.Add("errmsg", Resx.Format(e.signal, e.objects));
                    errs.Add(er);
                    if (ordd)
                        break;
                }
                catch (Exception e)
                {
                    var er = new TDocument(ctx);
                    er.Add("index", ctr);
                    er.Add("errmsg", e.Message);
                    errs.Add(er);
                    if (ordd)
                        break;
                }
            conn = tr.Commit();
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            rdoc.Add("n", ctr);
            if (errs.content.Count > 0)
                rdoc.Add("writeErrors", errs);
            return m;
        }
        private ReplyMessage UpdateCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            tabname = q.query["update"].ToString();
            var x = q.databaseNameAndSuffix.IndexOf(".$cmd");
            var coll = q.databaseNameAndSuffix.Substring(0, x);
            conn.MongoConnect(this, coll + "." + tabname);
            var docs = q.query["updates"] as TDocArray;
            bool ordd = q.query.GetBool("ordered", true);
            var errs = new TDocArray(ctx);
            var ctr = 0;
            if (docs != null)
                for (var d = docs.content.First();d != null;d=d.Next())
                    try
                    {
                        var db = tr.local;
                        var v = d.value() as TDocument;
                        var query = v["q"] as TDocument;
                        var update = v["u"] as TDocument;
                        var upsert = v.GetBool("upsert",false);
                        var fm = new From(this, query, update);
                        fm.Analyse(tr.context);
                        var nn = 0;
                        var multi = v.GetBool("multi", false);
                        // multi is allowed only if the update doc is all operators
                        for (var c = update.content.First();c!=null;c=c.Next())
                            if (!c.value().name.ident.StartsWith("$"))
                                multi = false;
                        for (var b=fm.rowSet.First();b!=null;b=b.Next())
                        {
                            var nd = b.Value()[colname] as TDocument;
                            if (nd == null)
                                continue;
                            db.AddUpdate(new Update(b._defpos,new BTree<long, TypedValue>(column.defpos, nd), table.defpos, db));
                            nn++;
                            if (!multi)
                                break;
                        }
                        if (nn==0 && upsert)
                            db.AddRecord(new Record(table.defpos,new BTree<long, TypedValue>(column.defpos, update),db));
                        ctr++;
                    }
                    catch (DBException e)
                    {
                        var er = new TDocument(ctx);
                        er.Add("index", ctr);
                        var code = 0;
                        if (int.TryParse(e.signal, out code))
                            er.Add("code", code);
                        er.Add("errmsg", Resx.Format(e.signal, e.objects));
                        errs.Add(er);
                        if (ordd)
                            break;
                    }
                    catch (Exception e)
                    {
                        var er = new TDocument(ctx);
                        er.Add("index", ctr);
                        er.Add("errmsg", e.Message);
                        errs.Add(er);
                        if (ordd)
                            break;
                    }
            conn = tr.Commit();
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            rdoc.Add("n", ctr);
            if (errs.content.Count > 0)
                rdoc.Add("writeErrors", errs);
            return m;
        }

        ReplyMessage OptionsCommand(QueryMessage q)
        {
            var qy = new From(this, q.query["$query"] as TDocument);
            qy.profile = new QueryProfile();
            var m = new ReplyMessage(this);
            for (var f = q.query.content.First(); f != null; f = f.Next())
            {
                var v = f.value();
                switch (v.name.ident)
                {
                    case "$comment": qy.profile.comment = v.typedValue.ToString(); break;
                    case "$explain": qy.profile.explain = (bool)v.Val(); break;
                    case "$hint": qy.profile.hint = v.ToString(); break;
                    case "$max": qy.profile.max = v.typedValue as TDocument; break;
                    case "$min": qy.profile.min = v.typedValue as TDocument; break;
                    case "$orderby": qy.profile.sort = v.typedValue as TDocument; break;
                    case "$returnKey": qy.profile.returnKey = (bool)v.Val(); break;
                    case "$showDiskLoc": qy.profile.showDiskLoc = (bool)v.Val(); break;
                    case "$snapshot": qy.profile.snapshot = (bool)v.Val(); break;
                    case "$natural": qy.profile.natural = (bool)v.Val(); break;
                }
            }
            var t = DateTime.Now;
            qy.Analyse(tr.context);
            var rdoc = m.doc;
            if (qy.profile.explain??false)
            {
                for (var b = qy.rowSet.First();b!=null;b=b.Next())
                    ;
                qy.profile.millis = (DateTime.Now - t).Milliseconds;
                var explain = new TDocument(ctx);
                explain.Add("cursor", (qy.profile.index == null)?"BasicCursor":("BtreeCursor "+qy.profile.index.name));
                explain.Add("n", GetCount(qy));
                explain.Add("nscanned", qy.profile.nscannedobjects);
                explain.Add("nscannedObjects", qy.profile.nscannedobjects);
                explain.Add("scanAndOrder", qy.profile.scanAndOrder);
                explain.Add("indexOnly", qy.rowSet is IndexRowSet);
                explain.Add("nYields", 0L);
                explain.Add("nChunkSkips", 0L);
                explain.Add("millis", qy.profile.millis);
                explain.Add("server", PyrrhoStart.cfg.hp.ToString());
                if (qy.profile.max!=null || qy.profile.min!=null)
                {
                    var ib = new TDocument(ctx);
                    if (qy.profile.min != null)
                        ib.Add("start", qy.profile.min);
                    if (qy.profile.max != null)
                        ib.Add("end", qy.profile.max);
                    explain.Add("indexNBounds", ib);
                }
                rdoc.Add("explain", explain);
                rdoc.Add("n", 1);
            } 
            else
                rdoc.Add("n", GetCount(qy));
            rdoc.Add("ok", true); 
            return m;
        }
        ReplyMessage WhatsMyUriCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("you", "127.0.0.1:27017");
            return m;
        }
        ReplyMessage GetLogCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("totalLinesWritten", 0);
            rdoc.Add("log", new TDocArray(ctx));
            return m;
        }
        ReplyMessage ReplSetGetStatusCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("set","");
            rdoc.Add("date", "20150-01-10T07:00:00Z");
            rdoc.Add("myState", 0);
            rdoc.Add("members", new TDocArray(ctx));
            return m;
        }
        ReplyMessage GetLastErrorCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            m.doc = lastError;
            lastError = new ReplyDocument(ctx);
            return m;
        }
        ReplyMessage IsMasterCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("arbiters", new TDocArray(ctx));
            rdoc.Add("hosts", new TDocArray(ctx));
            rdoc.Add("arbiterOnly", false);
            rdoc.Add("isMaster", true);
            rdoc.Add("passive", true);
            rdoc.Add("isreplicaset", false);
            rdoc.Add("issecondary", false);
            rdoc.Add("maxMessageSizeBytes", 16 * 1024);
            rdoc.Add("minWireVersion", 2);
            rdoc.Add("maxWireVersion", 3); // I haven't done the CommandRequest stuff yet
            rdoc.Add("msg", "Hi!");
            var hp = new HostPort(PyrrhoStart.cfg.hp.host,port);
            rdoc.Add("me", hp.ToString());
            rdoc.Add("passives", 0);
            rdoc.Add("primary", hp.ToString());
            rdoc.Add("ok", true);
            return m;
        }

        private ReplyMessage ListDatabasesCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var dbs = new TDocArray(ctx);
            m.doc.Add("databases", dbs);
            string[] files;
#if (SILVERLIGHT)
                files = DbStorage.store.GetFileNames("*");
#else
            var pth = (PyrrhoStart.path != "")?PyrrhoStart.path:Directory.GetCurrentDirectory();
            files = Directory.GetFiles(pth);
#endif
            for (int j = 0; j < files.Length; j++)
            {
                string s = files[j];
                if (!s.EndsWith(DbData.ext))
                    continue;
                int mm = s.LastIndexOf("\\");
                if (mm >= 0)
                    s = s.Substring(mm + 1);
                mm = s.LastIndexOf("/");
                if (mm >= 0)
                    s = s.Substring(mm + 1);
                int n = s.Length - 4;
                if (s.IndexOf(".", 0, n) >= 0)
                    continue;
                var d = new TDocument(ctx);
                d.Add("name", s);
                var fi = new FileInfo(pth+"\\"+files[j]);
                d.Add("sizeOnDisk",fi.Length);
                d.Add("empty",fi.Length<60);
                dbs.Add(d);
            }

            m.doc.Add("ok", true);
            return m;
        }

        ReplyMessage MapReduceCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage PingCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage ProfileCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage ReIndexCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage RenameCollectionCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage SaslStartCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage SaslContinueCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage SetParameterCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            rdoc.Add("ok", true);
            return m;
        }
        ReplyMessage ValidateCommand(QueryMessage q)
        {
            var m = new ReplyMessage(this);
            var rdoc = m.doc;
            var v = rdoc; // new TDocument(ctx);
            v.Add("ns", tr.local.name);
            v.Add("firstExtent", 0);
            v.Add("lastExtent", tr.local.posAtStart);
            v.Add("extentCount", 1); 
            v.Add("extents", TNull.Value); // full is not implemented yet
            var size = 0;
            var tb = table;
            var nrecs = tb.tableRows.Count;
            for (var d = tb.tableRows.First();d!=null;d=d.Next())
            {
                var r = tr.local.GetD(d.key()) as Record;
                if (r == null)
                    continue;
                var rd = r.Field(column.defpos) as TDocument;
                if (rd == null)
                    continue;
                size += rd.nbytes;
            }
            v.Add("dataSize", size);
            v.Add("nrecords", nrecs);
            v.Add("lastExtentSize", tr.local.posAtStart);
            v.Add("padding", 1.0);
            v.Add("firstExtentDetails", TNull.Value); // hmm
            v.Add("objectsFound", nrecs);
            v.Add("deleteCount", 0);
            v.Add("deleteSize", 0);
            var inxs = new TDocument(ctx);
            for (var x =tr.local.indexes.First();x!=null;x=x.Next())
            {
                var ix = tr.local.objects[x.key()] as Index;
                if (ix == null || ix.tabledefpos != tb.defpos)
                    continue;
                inxs.Add(ix.name, ix.rows.Count);
            }
            v.Add("keysPerIndex", inxs);
            v.Add("valid", true);
            // rdoc.Add("validate", v);
            rdoc.Add("ok", true);
            return m;
        }
        /// <summary>
        /// Additional document fields not yet implemented
        /// $readPreference { mode , tags: [..] }
        ///    mode is one of 'Primary', 'Secondary', 'Primaryreferred', 'SecondaryPreferred', 'Nearest'
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="q"></param>
        private void QueryCommand(QueryMessage q)
        {
            var d = q.query;
            ReplyMessage m = null;
            try
            {
                if (d.Contains("aggregate")) // =_name, pipeline=
                    AggregateCommand(q);
                else if (d.Contains("authenticate")) // =1, user=, nonce=, digest= key=
                    m = AuthenticateCommand(q);
                else if (d.Contains("buildinfo"))
                    m = BuildInfoCommand(q);
                else if (d.Contains("clone")) // =from_host
                    m = CloneCommand(q);
                else if (d.Contains("collMod")) // usePowerOf2Sizes=
                    m = CollModCommand(q);
                else if (d.Contains("collstats")) // =_name
                    m = CollStatsCommand(q);
                else if (d.Contains("count")) // =_name, query, limit, skip
                    m = CountCommand(q);
                else if (d.Contains("create")) // =collectionname
                    m = CreateCommand(q);
                else if (d.Contains("currentOp")) // ??
                    m = CurrentOpCommand(q);
                else if (d.Contains("dbstats")) // ??
                    m = DbStatsCommand(q);
                else if (d.Contains("delete"))
                    m = DeleteCommand(q);
                else if (d.Contains("deleteIndexes")) // =_name, index=
                    m = DeleteIndexesCommand(q);
                else if (d.Contains("distinct")) // =_name, key, query
                    m = DistinctCommand(q);
                else if (d.Contains("drop")) // =collectionName collectionNameInSession
                    m = DropCommand(q);
                else if (d.Contains("dropdatabase"))
                    m = DropDatabaseCommand(q);
                else if (d.Contains("explain"))
                    m = ExplainCommand(q); 
                else if (d.Contains("filemd5")) // =files_id, root=
                    m = FileMd5Command(q);
                else if (d.Contains("findAndModify")) // =_name, query, sort, update, fields, new, upsert, remove
                    m = FindAndModifyCommand(q);
                else if (d.Contains("geoSearch")) // =_name, near
                    m = GeoSearchCommand(q);
                else if (d.Contains("geoNear")) // =_name, near, num, query
                    m = GeoNearCommand(q);
                else if (d.Contains("getLog"))
                    m = GetLogCommand(q);
                else if (d.Contains("getlasterror"))
                    m = GetLastErrorCommand(q);
                else if (d.Contains("getnonce"))
                    m = GetNonceCommand(q);
                else if (d.Contains("group")) // { ns, condition, $keyf, initial, reduce, finalize } 
                    // or }{ ns, condition, key, initial, $reduce, finalize }
                    // or 
                    m = GroupCommand(q);
                else if (d.Contains("insert"))
                    m = InsertCommand(q);
                else if (d.Contains("isMaster"))
                    m = IsMasterCommand(q);
                else if (d.Contains("listDatabases"))
                    m = ListDatabasesCommand(q);
                else if (d.Contains("mapreduce")) // =_name, map= reduce=
                    m = MapReduceCommand(q);
                else if (d.Contains("ping"))
                    m = PingCommand(q);
                else if (d.Contains("profile")) // =level slowms was ok
                    m = ProfileCommand(q);
                else if (d.Contains("reIndex")) // =_name
                    m = ReIndexCommand(q);
                else if (d.Contains("renameCollection")) // =_name.oldname, to, dropTarget
                    m = RenameCollectionCommand(q);
                else if (d.Contains("replSetGetStatus"))
                    m = ReplSetGetStatusCommand(q);
                else if (d.Contains("saslContinue")) // conversationId=, payload=
                    m = SaslContinueCommand(q);
                else if (d.Contains("saslStart")) // mechanism=, payload=
                    m = SaslStartCommand(q);
                else if (d.Contains("setParameter")) // textSearchEnabled=
                    m = SetParameterCommand(q);
                else if (d.Contains("update"))
                    m = UpdateCommand(q);
                else if (d.Contains("validate")) // =_name
                    m = ValidateCommand(q);
                else if (d.Contains("whatsmyuri"))
                    m = WhatsMyUriCommand(q);
                else if (d.Contains("$query"))
                    m = OptionsCommand(q);
                else if (d.Contains("$eval")) // =code, args, nolock
                    m = EvalCommand(q);
                else
                {
                    lastError.Add("err", "Unknown command");
                    lastError.Add("errmsg", "Unknown command");
                    lastError.Add("command", d);
                    m = new ReplyMessage(this);
                    m.doc.Add("ok", false);
                }
            }
            catch (DBException ex)
            {
                var msg = Resx.Format(ex.signal, ex.objects);
                lastError.Add("err", msg);
                lastError.Add("errmsg", msg);
                var code = 0;
                int.TryParse(ex.signal, out code);
                lastError.Add("code", code);
                lastError.Add("command", d);
                lastAffected = conn.affected;
                m = new ReplyMessage(this);
                m.doc.Add("ok", false);
            }
            catch (Exception ex)
            {
                lastError.Add("err", ex.Message);
                lastError.Add("errmsg", ex.Message);
                lastError.Add("code", 0);
                lastError.Add("command", d);
                lastAffected = conn.affected;
                m = new ReplyMessage(this);
                m.doc.Add("ok", false);
            }
            if (m!=null)
                m.WriteTo(this,stream);
            return;
        }
        static int GetInt(byte[] buf, ref int off)
        {
            int n = 0;
            for (int j = 3; j >=0; j--)
                n = (n << 8) + buf[off+j];
            off += 4;
            return n;
        }
        static void PutInt(byte[] buf, ref int off,int val)
        {
            for (int i = 0; i < 4; i++)
            {
                buf[off++] = (byte)(val & 0xff);
                val >>= 8;
            }
        }
        static long GetLong(byte[] buf, ref int off)
        {
            int n = 0;
            for (int j = 7; j >=0; j--)
                n = (n << 8) + buf[off+j];
            off += 8;
            return n;
        }
        static void PutLong(byte[] buf, ref int off, long val)
        {
            for (int i = 0; i < 8; i++)
            {
                buf[off++] = (byte)(val & 0xff);
                val >>= 8;
            }
        }
        static string GetCString(byte[] buf, ref int off)
        {
            var bs = new List<byte>();
            for (; ; )
            {
                var c = buf[off++];
                if (c == 0)
                    break;
                bs.Add(c);
            }
            return Encoding.UTF8.GetString(bs.ToArray());
        }
        static void PutCString(byte[] buf, ref int off, string s)
        {
            var bs = Encoding.UTF8.GetBytes(s);
            for (int i = 0; i < bs.Length;i++ )
                buf[off++] = bs[i];
            buf[off++] = 0;
        }
        static string GetString(byte[] buf, ref int off)
        {
            var n = GetInt(buf, ref off);
            var s = Encoding.UTF8.GetString(buf, off, n-1); // exclude trailing \0
            off += n;
            return s;
        }
        static void PutString(byte[] buf, ref int off, string s)
        {
            var bs = Encoding.UTF8.GetBytes(s);
            PutInt(buf,ref off,bs.Length+1); // +1 for trailing \0
            for (int i = 0; i < bs.Length; i++) 
                buf[off++] = bs[i];
            buf[off++] = 0;
        }

        internal static void Run()
        {
            int cid = 0;
            try
            {
                var ad = IPAddress.Parse(PyrrhoStart.cfg.hp.host);
                tcp = new TcpListener(ad, port);
            }
            catch
            {
                if (PyrrhoStart.DebugMode)
                Console.WriteLine("Access denied for MongoDB port");
            }
            if (tcp != null)
                Console.WriteLine("MongoDB service started on port "+port);
            tcp.Start();
            for (; ; )
                try
                {
                    Socket client = tcp.AcceptSocket();
                    var t = new Thread(new ThreadStart(new MongoService(client).Server));
                    t.Name = "T" + (++cid);
                    t.Start();
                }
                catch (Exception)
                { }
        }
        internal enum MessageOpcode
        {
            Reply = 1, Message = 1000, Update = 2001, Insert = 2002, Query = 2004,
            GetMore = 2005, Delete = 2006, KillCursors = 2007, Command = 2010, CommandReply = 2011
        }
        internal class MessageHeader
        {
            static int rid = 0;
            public int length;
            public int requestId;
            public int responseTo;
            public MessageOpcode opcode;
            internal MessageHeader(int to) 
            {
                requestId = ++rid;
                responseTo = to;
                opcode = MessageOpcode.Reply;
            }
            internal MessageHeader(int len,int rid,int rto,MessageOpcode op)
            {
                length = len;
                requestId = rid;
                responseTo = rto;
                opcode = op;
            }
            internal void ToBuffer(byte[] buf, out int i)
            {
                i = 0;
                PutInt(buf, ref i, length);
                PutInt(buf, ref i, requestId);
                PutInt(buf, ref i, responseTo);
                PutInt(buf, ref i, (int)opcode);
            }
        }
#endif
        internal class ReplyDocument : TDocument
        {
            internal ReplyDocument(Context cx)
                : base(cx)
            {
                Add("ok", true);
            }
        }
        internal class ReplyMessage
#if !EMBEDDED
            : MessageHeader
#endif
        {
            public int responseFlags = 0; // bits: 0 CursorNotFound, 1 QueryFailure, 2 ShardConfigState
                                          // 3: AwaitCapable
            public long cursorId = 0;
            public int startingFrom = 0;
            public int numberReturned = 1;
            public TDocument doc;
            internal ReplyMessage(MongoService ms)
#if !EMBEDDED
                : base(ms.lastreq)
#endif
            {
#if !EMBEDDED
                opcode = ms.reply;
#endif
                doc = new ReplyDocument(ms.ctx);
            }
#if !EMBEDDED
            internal void WriteTo(MongoService ms, Stream str)
            {
                if (doc.content.Count == 0)
                    doc.Add("ok", true);
                if (PyrrhoStart.DebugMode)
                Console.WriteLine("["+ms.cid+":"+responseTo+"] Sending " + doc.ToString());
                var bs = doc.ToBytes(null);
                length = bs.Length+36;
                var buf = new byte[length];
                int i;
                ToBuffer(buf, out i);
                PutInt(buf, ref i, responseFlags);
                PutLong(buf, ref i, cursorId);
                PutInt(buf, ref i, startingFrom);
                PutInt(buf, ref i, numberReturned);
                for (int j = 0; j < bs.Length; j++)
                    buf[i++] = bs[j];
                str.Write(buf, 0, buf.Length);
                if (PyrrhoStart.DebugMode)
                Console.WriteLine("" + buf.Length + " bytes");
                str.Flush();
            }
            internal void WriteTo(MongoService ms, Stream str, RowSet rs, QueryMessage q)
            {
                var n = 48;
                startingFrom = 0;
                cursorId = ms.cid;
                var more = false;
                var dl = new List<TDocument>();
                var nc = rs.rowType.Length;
                var ctr = 0;
                if (PyrrhoStart.DebugMode)
                {
                    Console.Write("[" + ms.cid + "] Sending");
                    if (q.fields != null)
                        Console.Write(" Proj by " + q.fields.ToString());
                    Console.WriteLine(" [");
                }
                for (var b = rs.First();b!=null;b=b.Next())
                {
                    if ((q.numberToReturn < 0 && ctr > 0) || (q.numberToReturn > 0 && ctr >= q.numberToReturn))
                        break;
                    ctr++;
                    TDocument ri = null;
                    var v = b.Value();
                    if (nc == 1)
                        ri = v[ms.colname] as TDocument;
                    else
                    {
                        ri = new TDocument(ms.ctx);
                        for(int j=0;j<rs.rowType.Length;j++)
                        {
                            var nm = rs.qry.NameAt(j).ident;
                            var col = v[nm];
                            ri.Add(nm, col);
                        }
                    }
                    var bs = ri.ToBytes(q.fields);
                    if (bs.Length > 16 * 1024 - 32)
                        throw new DBException("3D005", "Length " + bs.Length);
                    if (n + bs.Length > 16 * 1024)
                    {
                        more = true;
                        break;
                    }
                    n += bs.Length;
                    if (PyrrhoStart.DebugMode)
                    Console.WriteLine(ri.ToString());
                    dl.Add(ri);
                }
                if (PyrrhoStart.DebugMode)
                    Console.WriteLine("]");
                length = n;
                if (more)
                    ATree<long, RowSet>.Add(ref ms.cursors, cursorId, rs);
                var buf = new byte[n];
                int i;
                if (dl.Count == 0 || !more)
                    cursorId = 0;
                ToBuffer(buf, out i);
                PutInt(buf, ref i, responseFlags);
                PutLong(buf, ref i, cursorId);
                PutInt(buf, ref i, startingFrom);
                PutInt(buf, ref i, dl.Count);
                foreach (var ri in dl)
                {
                    var bt = ri.ToBytes(q.fields);
                    for (int j = 0; j < bt.Length; j++)
                        buf[i++] = bt[j];
                }
                str.Write(buf, 0, buf.Length);
            }
#endif
        }
        internal class AggregateResultsMessage : ReplyMessage
        {
            internal bool more = false;
            internal AggregateResultsMessage(MongoService ms,RowSet rs) :base(ms)
            {
                var ra = new TDocArray(ms.ctx);
                var nc = rs.rowType.Length;
 //               var k = 0;
 //               var n = 0;
                for (var b=rs.First();b!=null;b=b.Next())
                {
                    /*                    var ri = new Document(ms.ctx);
                                        for (int i = 0; i < rs.nominalDataType.Length;i++ )
                                        { */
                    var v = b.Value();
                        var tv = v[0]; //rs[i];
/*                        ri.Add(rs.nominalDataType.names[i], tv);
                    }
                    var bs = ri.ToBytes();
                    if (bs.Length > 16 * 1024 - 32)
                        throw new DBException("3D005", "Length " + bs.Length);
                    if (n + bs.Length > 16 * 1024)
                    {
                        more = true;
                        break;
                    }
                    n += bs.Length;
                    var na = "" + k++;
                    n += na.Length + 7; 8?
                    ra.Add(ri);
 */
                        ra.Add(tv);
                }
                doc.Add("result", ra);
                doc.Add("ok", true);
            }
        }
#if !EMBEDDED
        internal class UpdateMessage : MessageHeader
        {
            public int flags; // bit0=upsert, bit1=MultiUpdate
            public string collectionName;
            public TDocument query;
            public TDocument update;
            internal UpdateMessage(MongoService ms,byte[] buf, int rid, ref int off) : base(buf.Length,rid,0,MessageOpcode.Update)
            {
                GetInt(buf, ref off);
                collectionName = GetCString(buf, ref off);
                flags = GetInt(buf, ref off);
                query = new TDocument(ms.ctx,buf, ref off);
                update = new TDocument(ms.ctx,buf, ref off);
                if (PyrrhoStart.DebugMode)
                {
                    Console.WriteLine("[" + ms.cid + "] Got Update for " + query.ToString());
                    Console.WriteLine("[" + ms.cid + "]   of " + update.ToString());
                }
            }
        }
        internal class InsertMessage : MessageHeader
        {
            public int flags; // bit0: ContinueOnError
            public string collectionName;
            public List<TDocument> insert = new List<TDocument>();
            internal InsertMessage(MongoService ms,byte[] buf, int rid, ref int off): base(buf.Length,rid,0,MessageOpcode.Insert)
            {
                flags = GetInt(buf, ref off);
                collectionName = GetCString(buf, ref off);
                var counter = 0;
                while (off < buf.Length)
                {
                    var ins = new TDocument(ms.ctx, buf, ref off);
                    insert.Add(ins);
                    counter++;
                }
                if (PyrrhoStart.DebugMode)
                Console.WriteLine("[" + ms.cid + "] Got Insert " + insert.ToString());
            }
        }
#endif
        internal class QueryMessage
#if !EMBEDDED
        : MessageHeader
#endif
        {
            public int flags; // bits 1 TailableCursor, 2 SlaveOk, 3 OplogReplay, 4 NoCursorTimeout
                        // 5 AwaitData, 6 Exhaust, 7 Partial
            public string databaseNameAndSuffix;
            public int numberToSkip;
            public int numberToReturn;
            public TDocument query;
            public TDocument fields = null;
#if !EMBEDDED
            internal QueryMessage(MongoService ms,byte[] buf, int rid, ref int off): base(buf.Length,rid,0,MessageOpcode.Query)
            {
                flags = GetInt(buf, ref off);
                databaseNameAndSuffix = GetCString(buf, ref off);
                numberToSkip = GetInt(buf, ref off);
                numberToReturn = GetInt(buf, ref off);
                query = new TDocument(ms.ctx, buf, ref off);
                if (off<buf.Length)
                    fields = new TDocument(ms.ctx, buf, ref off);
                if (PyrrhoStart.DebugMode)
                {
                    Console.Write(databaseNameAndSuffix + " [" + ms.cid + "] Got Query " + query.ToString());
                    if (fields != null)
                        Console.Write(" proj=" + fields.ToString());
                    Console.WriteLine();
                }
            }
#endif
            internal QueryMessage(Context cx,TDocument doc)
#if !EMBEDDED
                : base(0,0,0,MessageOpcode.Query)
#endif
            {
                flags = 0;
                databaseNameAndSuffix = cx.transaction.local.name + ".$cmd";
                query = doc;
                numberToSkip = 0;
                numberToReturn = 0;
            }
        }
#if !EMBEDDED
        internal class GetMoreMessage : MessageHeader
        {
            public string collectionName;
            public int numberToReturn;
            public long cursorId;
            internal GetMoreMessage(MongoService ms,byte[] buf, int rid, ref int off) :base(buf.Length,rid,0,MessageOpcode.GetMore)
            {
                GetInt(buf, ref off); // ZERO
                collectionName = GetCString(buf, ref off);
                numberToReturn = GetInt(buf, ref off);
                cursorId = GetLong(buf, ref off);
            }
        }
        internal class DeleteMessage : MessageHeader
        {
            public int flags;
            public string collectionName;
            public TDocument delete;
            internal DeleteMessage(MongoService ms,byte[] buf, int rid, ref int off) :base(buf.Length,rid,0,MessageOpcode.Delete)
            {
                GetInt(buf, ref off); // ZERO
                collectionName = GetCString(buf, ref off);
                flags = GetInt(buf, ref off); // bits: 0 SingleRemove
                delete = new TDocument(ms.ctx, buf, ref off);
                if (PyrrhoStart.DebugMode)
                Console.WriteLine("[" + ms.cid + "] Got Delete " + delete.ToString());
            }
        }
        internal class KillCursorsMessage  :MessageHeader
        {
            public int nCursorIds;
            public List<long> cursorIds = new List<long>();
            internal KillCursorsMessage(MongoService ms, byte[] buf, int rid, ref int off):base(buf.Length,rid,0,MessageOpcode.KillCursors)
            {
                nCursorIds = GetInt(buf, ref off);
                for(int i=0;i<nCursorIds;i++)
                    cursorIds.Add(GetLong(buf,ref off));
            }

        }
#endif
    }
#endif
}
