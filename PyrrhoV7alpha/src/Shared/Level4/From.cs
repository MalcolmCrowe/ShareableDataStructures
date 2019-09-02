using System;
using System.Collections.Generic;
using PyrrhoBase;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.Text;
using Pyrrho.Level1;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    /// <summary>
    /// A From clause. This is a subclass of Query so that the analysis machinery can be used.
    /// The select list for the FROM is built from the sources and entered into the context so that
    /// TableColumns can be looked up in the context. SqlValue conditions and oredrings are propoagated down into the
    /// From clause so that indexes can be selected for maximum efficiency.
    /// </summary>
    internal class From : SelectQuery
    {
        public override string Tag => "FM";
        /// <summary>
        /// compute this from constant parts of the where expression if present
        /// </summary>
        internal ATree<long, TypedValue> filter = BTree<long, TypedValue>.Empty;
        /// <summary>
        /// The list of accessible TableColumns (for the current user)
        /// </summary>
        internal Selector[] accessibleCols = new Selector[0];
        /// <summary>
        /// The object being selected from
        /// </summary>
        internal DBObject target;
        internal Ident name;
        /// <summary>
        /// Check if our name matches the given one
        /// </summary>
        /// <param name="n">a name</param>
        /// <returns></returns>
        internal override bool NameMatches(Ident n)
        {
            return n.HeadsMatch(alias)==true || n?.HeadsMatch(name) ==true || base.NameMatches(n);
        }
        public Index index = null;
        public Query source = null; // for Views
        public UpdateAssignment[] assigns = null;
        public RowSet data = null; // for Insert
        public long oldTable = 0L, newTable = 0L; // for triggers
        public ATree<long,TypedValue> oldRow = null, newRow = null; // for triggers
        internal Domain accessibleDataType = null;
        public long newRec = 0;
        /// <summary>
        /// for debugging
        /// </summary>
        static int _uid = 0;
        internal int fid = ++_uid;
        internal Correlation corr = null;
        protected From(From f, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) : base(f, ref cs,ref vs)
        {
            filter = f.filter;
            accessibleCols = f.accessibleCols;
            accessibleDataType = f.accessibleDataType;
            target = f.target;
            name = f.name;
            data = f.data; // ??? tr will be wrong
            if (f.assigns != null)
            {
                assigns = new UpdateAssignment[f.assigns.Length];
                for (var i = 0; i < assigns.Length; i++)
                    assigns[i] = f.assigns[i].Copy(ref vs);
            }
            enc = f.enc;
            index = f.index;
            if (f.source != null)
                source = (CursorSpecification)f.source.Copy(ref cs,ref vs);
            CopyContexts(f, cs, vs);
        }
        /// <summary>
        /// Constructor: Used when table properties are changed, to check against existing rows
        /// </summary>
        /// <param name="cx">A parsing context</param>
        /// <param name="tb">The Table</param>
        public From(Transaction tr, string i, Table tb)
            : base(tr, i, TableType.Table,tb.NameInSession(tr))
        {
            target = tb;
            SetAccessibleCols(tr, tb, Grant.Privilege.Select);
            Scols();
        }
#if MONGO
        /// <summary>
        /// Constructor for the MongoService
        /// </summary>
        /// <param name="ms"></param>
        /// <param name="doc">the query</param>
        public From(MongoService ms,TDocument doc) :base(ms.tr,Domain.Table)
        {
            database = ms.tr.front;
            var table = ms.table;
            if (table != null)
            {
                _defpos = table.defpos;
                accessibleCols = new Selector[] {ms.column};
                AddTable(ms.tr, null, new Ident(ms.tabname));
                Analyse(this);
                if (doc != null)
                {
                    var c = new SqlValueExpr(this, Sqlx.EQL,
                           new SqlName(this, new Ident(ms.colname), Domain.Document),
                                    doc.Build(this), Sqlx.NO);
                    rowSet = new SelectRowSet(rowSet, c);
                }
                else if (ms.conn.result != null)
                    rowSet = ms.conn.result;
                rowSet.Initialise();
            }
        }
        public From(MongoService ms,TDocument doc,TDocument update) : this(ms,doc)
        {
            rowSet = new UpdateRowSet(ms, rowSet, update);
        }
#endif
        /// <summary>
        /// Constructor: a FROM for UNNEST or subquery cases
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="rs">The rowset</param>
        /// <param name="cr">The correlatuion info (table and column aliases</param>
        public From(Context cx, string id, RowSet rs, Correlation cr)
            : base(cx, id, rs.rowType, cr?.tablealias??rs?.qry?.alias)
        {
            var dt = rs?.rowType ?? Domain.Row;
            if (cr == null || cr.cols == null || cr.cols.Length == 0)
                for (int i = 0; i < dt.Length; i++)
                    new SqlTypeColumn(cx, this, dt[i], dt.names[i], true, true);
            else
                for (int i = 0; i < cr.cols.Length; i++)
                {
                    var n = cr.cols[i];
                    Domain ct = null;
                    var j = dt.names.Get(n,out Ident s);
                    if (j.HasValue)
                        ct = dt[j.Value]?[s];
                    if (ct == null)
                        throw new DBException("42112", n.ident).Mix();
                    new SqlTypeColumn(cx, this, ct, n, true, true);
                }
            nominalDataType = rs.rowType;
            accessibleDataType = nominalDataType;
            source = rs.qry;
            rowSet = new ExportedRowSet(rs.tr,this,rs,rs.rowType);
            display = rowSet.rowType.Length;
            Scols();
            Resolve(cx,alias);
            analysed = rs.tr.cid;
            for (var i = 0; i < names.Length; i++)
                new SqlTypeColumn(cx, this, nominalDataType, names[i], false, false);
            cx.context.lookup = BTree<Ident,ATree<int,ATree<long,SqlValue>>>.Empty;
            ATree<string, Context>.Remove(ref cx.context.contexts, blockid);
            cur = enc;
        }
        /// <summary>
        /// Constructor: a FROM for an array of SqlRows
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="rs">The TRows</param>
        /// <param name="cr">The correlation info (table and column aliases</param>
        public From(Transaction tr, string id, TRow[] rs, Correlation cr)
            : base(tr, id, RowType.Null, cr?.tablealias)
        {
            if (rs.Length == 0)
                throw new PEException("PE884");
            var ers = new ExplicitRowSet(tr,this);
            for (int i = 1; i < rs.Length; i++)
                ers.Add(rs[i]);
            rowSet = ers;
            analysed = tr.cid;
            display = rowSet.rowType.Length;
            Scols();
        }
        /// <summary>
        /// Constructor: a FROM clause from the parser
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="n">The name: a table or view</param>
        /// <param name="cr">The correlation set</param>
        /// <param name="how">What sort of access is required</param>
        public From(Transaction tr, string i, Ident n, Correlation cr, Grant.Privilege how)
            : base(tr, i, RowType.Null, cr?.tablealias ?? n)
        {
            Database d = null;
            name = n;
            ATree<string, Context>.Add(ref staticLink.contexts, name.ident, this);
            if (name.ident.IndexOf("$") >= 0)
            {
#if !EMBEDDED && !LOCAL
                if (nam[0] == 'L' && (!PyrrhoServer.HasServerRole(tr.db.name, Level1.ServerRole.Storage,
                    database.cd.remoteServer==null)))
                    throw new DBException("42107", name).Mix();
#endif
                SystemRowSet.Kludge();
                Table tb = systemTables[name.ident];
                target = tb ?? throw new DBException("42107", name).Mix();
                d = Database.SysDatabase;
            }
            else
            {
                // check in case name refers to a RowSet variable
                for (var ac = tr.context as Activation; ac != null; ac = ac.staticLink as Activation)
                    if (ac.vars[name]?.Eval(tr,null) is TRowSet ts)
                    {
                        nominalDataType = ts.rowSet.rowType;
                        accessibleDataType = nominalDataType;
                        rowSet = ts.rowSet;
                        if (ts.rowSet.qry is From tf && tf.target is Table tb)
                        {
                            target = tb;
                            var db = tr.Db(tf.target.dbix);
                            SetAccessibleCols(tr,db,tb,how);
                            ApplyCorrelation(tr,db,cr, name);
                        }
                        else
                            for (var c = rowSet.qry.defs.First(); c != null; c = c.Next())
                                Ident.Tree<SqlValue>.Add(ref defs, c.key(), c.value());
                        Scols();
                        Resolve(tr,name);
                        Resolve(tr,cr?.tablealias);
                        return;
                    }
                // look for a real table, or view
                target = tr.GetTable(name, out d); // guaranteed to be a Table
                if (target == null)
                {
                    View vw = tr.GetView(name, out d);
                    if (vw != null && vw.httpProblem)
                        throw new DBException("40001").ISO();
                    defs1 = staticLink.defs;
                    target = vw?.Copy();
                    if (vw != null)
                        accessibleDataType = d.Tracker(vw.defpos).type as RowType;
                }
            }
            // If the access is subject to enforcement and the user is not the security admin
            // we check the clearance: if refused, for select report NOT FOUND
            // for others report ACCESS DENIED
            if (d!=Database.SysDatabase && (d==null || target==null || (target is Table tx && tx.enforcement.HasFlag(how)
                && d._User.defpos != d.owner && !d._User.clearance.ClearanceAllows(tx.classification))))
                throw (how==Grant.Privilege.Select)?
                    new DBException("42107", name): // table not found
                    new DBException("42105"); // access denied
            simpleQuery = this;
            target?.Setup(tr,this, how);
            ApplyCorrelation(tr,d, cr, name);
            Scols();
            if (target!=null)
                name.Set(target.dbix, target.defpos, Ident.IDType.Table);
            Resolve(tr,name);
            Resolve(tr,cr?.tablealias);
        }
        /// <summary>
        /// Constructor: an explicit source expression
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="v">A rowset expression</param>
        internal From(Transaction tr, string i, SqlValue v) : base(tr, i, v.nominalDataType as RowType)
        {
            target = null;
            v.RowSet(tr,this);
            display = v.nominalDataType.Length;
            Scols();
        }
        /// <summary>
        /// Constructor: From a procedure call
        /// </summary>
        /// <param name="cx">The transaction</param>
        /// <param name="n">The procedure name</param>
        /// <param name="r">The parameters</param>
        /// <param name="k">The row type for the results</param>
        /// <param name="cr">The correlation</param>
        public From(Transaction tr,string id,Ident n, SqlValue[] r, RowType k, Correlation cr)
                : base(tr, id, k, cr?.tablealias ?? n)
        {
            n = n.Suffix(r.Length); // check
            Procedure proc = Procedure.Get(tr,n, out Database db);
            target = proc ?? throw new DBException("42108", n).Mix();
            nominalDataType = db.pb.Tracker(db._Role.defpos,target.defpos).type as RowType;
            var rt = proc.Returns(db);
            if (rt.names.Length == 0)
                rt = new RowType(Sqlx.ROW,new Domain[] { rt }, new Ident[] { new Ident("",0) });
            var ret = proc.Exec(tr, db.dbix, n, r);
            if (ret == null)
                throw new DBException("22004").Mix();
            if (!(ret is TRowSet))
                ret = new TRowSet(tr,this, rt, ret.ToArray());
            rowSet = (RowSet)ret.Val(tr);
            n.segpos = proc.defpos;
            n.dbix = proc.dbix;
            nominalDataType = ret.dataType.LimitBy(nominalDataType) as RowType;
            for (int i=0;i<nominalDataType.Length;i++)
                new SqlTypeColumn(tr,this, nominalDataType[i], nominalDataType.names[i], true, true);
            ApplyCorrelation(tr,db, cr, n);
            Scols();
            Resolve(tr,n);
            Resolve(tr,cr?.tablealias);
        }
        /// <summary>
        /// Constructor: FROM ROWS(nnn): access the log entries for insertion, update and deletion of rows in the specified table.
        /// The table is assumed to be in the first or only connected database.
        /// </summary>
        /// <param name="cx">The prasing context</param>
        /// <param name="td">The table defining position</param>
        /// <param name="ta">An alias for the table</param>
        public From(Transaction tr, string i, long td, string ta)
            : base(tr, i, RowType.Table)
        {
            newTable = td;
            var db = tr.Db(0);
            Table ctable = (Table)db.objects[newTable];
            if (ctable == null)
                throw new DBException("42131", "" + newTable).Mix();
            if ((db.owner != 0 && db.Transuserid != db.owner && db._Role.defpos != ctable.owner))
                throw new DBException("42105", ctable.NameInSession(db)).Mix();
            var ttable = new SystemTable(new Ident("" + td,0));
            ATree<long, RoleObject>.Add(ref db._Role.defs, ttable.defpos, new RoleObject(0, ttable.name));
            target = ttable;
            AddColumn(tr,db,"Pos", Sqlx.INTEGER);
            AddColumn(tr,db,"Action", Sqlx.CHAR);
            AddColumn(tr,db,"DefPos", Sqlx.INTEGER);
            AddColumn(tr,db,"Transaction", Sqlx.INTEGER);
            AddColumn(tr,db,"Timestamp", Sqlx.TIMESTAMP);
            AddColsFrom(ttable, (Table)db.objects[newTable]);
            SetAccessibleCols(tr,db,(Table)target, Grant.Privilege.Select);
            rowSet = new LogTableSelect(tr,this);
            nominalDataType = rowSet.rowType;
            display = nominalDataType.Length;
            Scols();
        }
        /// <summary>
        /// Constructor: FROM ROWS(rrr,ccc): access the log information for insertion, update and deletion of row rrr and ident ccc.
        /// The table is assumed to be in the first or only connected database.
        /// </summary>
        /// <param name="cx">The parsing context</param>
        /// <param name="rd">The row's defining position</param>
        /// <param name="cd">The ident's defining position</param>
        /// <param name="ta">The table alias</param>
        public From(Transaction tr, string i, long rd, long cd, Ident ta)
            : base(tr, i, RowType.Table) // LogRowColReference
        {
            var db = Database.Get(tr);
            TableColumn tc = (TableColumn)db.objects[cd];
            if (tc == null)
                throw new DBException("42131", cd.ToString()).Mix();
            newTable = tc.tabledefpos;
            Table ctable = (Table)db.objects[newTable];
            if (db._User == null || (db.owner != 0 && db._User.defpos != db.owner && db._User.defpos != ctable.owner))
                throw new DBException("42105").Mix();
            var ttable = new SystemTable(new Ident("" + rd + ":" + cd,0));
            ATree<long, RoleObject>.Add(ref db._Role.defs, ttable.defpos, new RoleObject(0, ttable.name));
            target = ttable;
            AddColumn(tr,db,"Pos", Sqlx.INTEGER);
            AddColumn(tr,db,"Value", Sqlx.CHAR);
            AddColumn(tr,db,"StartTransaction", Sqlx.INTEGER);
            AddColumn(tr,db,"StartTimestamp", Sqlx.TIMESTAMP);
            AddColumn(tr,db,"EndTransaction", Sqlx.INTEGER);
            AddColumn(tr,db,"EndTimestamp", Sqlx.TIMESTAMP);
            ApplyCorrelation(tr,db, null, null);
            SetAccessibleCols(tr,db,(Table)target, Grant.Privilege.Select);
            rowSet = new LogRowColSelect(tr,this, rd, tc, ttable);
            nominalDataType = rowSet.rowType;
            display = nominalDataType.Length;
            Scols();
        }
         /// <summary>
        /// FROM STATIC
        /// </summary>
        /// <param name="q">the query</param>
        public From(Context cnx, Query q, string id)
            : base(cnx, id, TableType.Table)
        {
            target = null;
            Scols();
            display = q.display;
        }
        internal override Query Copy(ref ATree<string,Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new From(this,ref cs,ref vs);
        }
        void Resolve(Context cx,Ident n)
        {            // resolve all unresolved SqlNames matching this
            if (n == null)
                return;
            for (Query q = this; q != null; q = q.enc)
                if (q is SelectQuery qs)
                {
                    SqlName nx = null;
                    for (var s = qs?.unresolved; s != null; s = nx)
                    {
                        nx = s.next; // s.next may get clobbered in s.Resolved
                        if (s.name.CompareTo(n) == 0 && defs[s.name.sub] is SqlValue v)
                        {
                            s.name.Set(v, cx);
                            s.Resolved(cx,this,v);
                            Add(v);
                        }
                        if (ValFor(s.name) is SqlValue sv)
                        {
                            s.name.Set(sv, cx);
                            s.Resolved(cx,this,sv);
                            Add(sv);
                        }
                    }
                    if (q is QuerySpecification)
                        break;
                }
        }
        /// <summary>
        /// Calculate the accessible columns for the given table and privilege
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="tb">the table</param>
        /// <param name="p">the privilege</param>
        internal void SetAccessibleCols(Transaction tr,Table tb,Grant.Privilege p)
        {
            accessibleCols = tb.AccessibleColumns(tr, p);
            if (accessibleCols == null || accessibleCols.Length==0)
                throw new DBException("2E111",tr.role.name,tb.NameInSession(tr));
            var coldts = new Domain[accessibleCols.Length];
            var nms = new Ident[accessibleCols.Length];
            names = new Idents();
            for (int i = 0; i < accessibleCols.Length; i++)
            {
                var ac = accessibleCols[i];
                var dt = ac.dataType(tr.role.defpos);
                coldts[i] = dt;
                nms[i] = ac.CurrentName(tr);
                new SqlTypeColumn(tr, this, coldts[i], nms[i], true, true);
            }
            nominalDataType = new RowType(Sqlx.ROW,coldts, nms)
            {
                name = tb.NameInSession(db)
            };
            accessibleDataType = nominalDataType;
            display = nms.Length;
            ATree<string, Context>.Add(ref staticLink.contexts, alias.ident, this);
        }
        /// <summary>
        /// Accessor
        /// </summary>
        /// <returns>the computed accesible columns</returns>
        internal override Selector[] AccessibleCols()
        {
            return accessibleCols;
        }
        /// <summary>
        /// Allow assignment to a result column
        /// </summary>
        /// <param name="p">The column name</param>
        /// <returns>A target for allowing assignment</returns>
        internal override Target LVal(Transaction tr, Ident p)
        {
            if (rowSet != null)
            {
                var i = nominalDataType.names[p];
                if (i.HasValue)
                {
                    p.Set(tr,nominalDataType.names[i.Value]);
                    return new Target(tr, p, nominalDataType[i.Value]);
                }
            }
            return base.LVal(tr,p);
        }
        internal override SqlValue Lookup0(Context cnx,Ident name)
        {
            if (source?.Lookup0(cnx, name) is SqlValue sv)
            {
                if (sv is SqlTypeColumn st && st.name.ident==name.ident && st.blockid==blockid)
                    return sv;
                name.Set(sv.name.dbix, sv.name.segpos, Ident.IDType.Column);
                return new SqlTypeColumn(cnx, sv.nominalDataType, name, true, false,this);
            }
            return base.Lookup0(cnx,name);
        }
        internal override bool Knows(SqlTypeColumn c)
        {
            return 
                PosFor(accessibleDataType?.names,c.name) != null
                || ((target as View)?.ViewDef?.Knows(c) == true)
#if !EMBEDDED
             || PosFor((target as RestView)?.usingTableType?.names,c.name) != null
#endif
             ;
        }
        internal override void Selects(Transaction tr, Query q)
        {
            source?.AddMatches(tr, this);
            base.Selects(tr, q);
        }
        internal override void Conditions(Transaction tr, Query q)
        {
            if (source != null)
                MoveConditions(ref where, tr, source);
            base.Conditions(tr, q);
            if (filter.Count == 0 && target!=null)
                filter = matches[target.dbix]??BTree<long,TypedValue>.Empty;
        }
        /// <summary>
        /// Construct the associated rowsets. If the table is partitioned we collect RemoteRowset information as required.
        /// Otherwise, at this level this amounts mostly to deciding which index to use for the rows.
        /// The best index selects on any where clauses and/or respects any ordering we want to do.
        /// We might want to do ordering because it is specified in the query, because it is needed for a part, or for aggregation.
        /// </summary>
        internal override void RowSets(Transaction tr)
        {
            (target as View)?.Selects(tr, this); // I now think we need to have done JoinCondition stuff first
            (target as View)?.Conditions(tr, this);
            var db = tr.Db(target?.dbix??0);
            if (rowSet != null && rowSet.tr==tr) // the work already done by From (View, Proc etc)
            {
                rowSet.AddMatch(this.matches);
                Ordering(false);
                return;
            }
            if (target==null)
            {
                rowSet = new TrivialRowSet(tr,this, Eval() as TRow);
                return;
            }
            if (target is View vw)
            {
                vw.RowSets(tr,this);
                return;
            }
            ReadConstraint readC = db._ReadConstraint(target);
            int matches = 0;
            PRow match = null;
            Index index = null;
#if MONGO
            if (profile != null && profile.hint != "")
                index = database.GetIndex(profile.hint);
#endif
            if (index == null)
            {
                // At this point we have a table/alias, 
                // we want to find the Index best meeting our needs
                // Score: Add 10^n for n ordType cols occurring in order, 
                // add n+1 for each filter column occurring in position k-n 
                // in an index with k cols.
                // Find the index with highest score, then set up
                // the match Link for it
                if (periods.Contains(target.defpos))
                {
                    // a periodspecification has been supplied for this table.
                    var ps = periods[target.defpos];
                    if (ps.kind == Sqlx.NO)
                    {
                        // simply use the appropriate time versioning index for this table
                        var tb = (Table)target;
                        var ix = (ps.periodname == "SYSTEM_TIME") ? Sqlx.SYSTEM_TIME : Sqlx.APPLICATION;
                        var pd = tb.FindPeriodDef(db, ix);
                        if (pd != null)
                            index = (Index)db.GetObject(pd.indexdefpos);
                    }
                    else
                    {
                        // We need to evaluate the points in time and find/build the appropriate indexes
                        ps.Setup(tr,this);
                        var psk = new PeriodVersion(ps.kind, (DateTime)ps.time1.Val(tr,rowSet), (DateTime)ps.time2.Val(tr,rowSet), 0);
                        object rows = versionedIndexes[psk];
                        var tb = (Table)target;
                        var ix = db.FindPrimaryIndex(tb);
                        if (ix != null)
                        {
                            if (rows == null)
                            {
                                rows = ix.Build(tr,this, ps);
                                ATree<PeriodVersion, object>.AddNN(ref versionedIndexes, psk, rows);
                            }
                            index = new Index(tr, ix, (MTree)rows, ix.subTypes);
                        }
                        else
                        {
                            if (rows == null)
                            {
                                rows = tb.Build(tr,this, ps);
                                ATree<PeriodVersion, object>.AddNN(ref versionedIndexes, psk, rows);
                            }
                            target = new Table(tb);
                            ((Table)target).tableRows = (ATree<long, bool>)rows;
                        }
                    }
                }
                else
                {
                    int bs = 0;		 // score for best index
                    for (var p = db.indexes.First();p!= null;p=p.Next())
                    {
                        var x = db.objects[p.key()] as Index;
                        if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey || x.tabledefpos != target.defpos)
                            continue;
                        var dt = db.GetDataType(x.defpos);
                        int sc = 0;
                        int nm = 0;
                        int n = 0;
                        PRow pr = null;
                        int sb = 1;
                        for (int j = x.cols.Length-1; j >=0; j--)
                        {
                            for (var fd = filter.First();fd!= null;fd=fd.Next())
                            {
                                if (x.cols[j] == fd.key())
                                {
                                    sc += 9 - j;
                                    nm++;
                                    pr = new PRow(fd.value(), pr);
                                    goto nextj;
                                }
                            }
                            var ob = db.objects[x.cols[j]];
                            var nam = ob.NameInSession(db);
                            if (ordSpec != null && n < ordSpec.items.Count)
                            {
                                var ok = ordSpec[n];
                                var sr = ValFor(nam);
                                if (ok != null && ok.what.MatchExpr(this,sr))
                                {
                                    n++;
                                    sb *= 10;
                                }
                            }
                            pr = new PRow(TNull.Value, pr);
                            nextj:;
                        }
                        sc += sb;
                        if (sc > bs)
                        {
                            index = x;
                            matches = nm;
                            match = pr;
                            bs = sc;
                        }
                    }
                }
            }
            var svo = ordSpec;
            KType(db,index);
            if (index != null && index.rows != null)
            {
#if MONGO
                if (profile != null)
                    profile.index = index;
#endif
                rowSet = new IndexRowSet(tr,this, index,match);
                if (readC != null)
                {
                    if (matches == index.cols.Length &&
                        (index.flags & (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique)) != PIndex.ConstraintType.NoType)
                        readC.Singleton(index, match);
                    else
                        readC.Block();
                }
            }
            else
            {
                if (target is Table tb)
                {
                    if (tb.tableRows != null)
                        rowSet = new TableRowSet(tr,this);
                    else
                    {
                        index = db.FindPrimaryIndex(tb);
                        KType(db,index);
                        if (index != null && index.rows != null)
                            rowSet = new IndexRowSet(tr,this, index, null);
#if !EMBEDDED && !LOCAL
                        else
                            rowSet = new RemoteRowSet(this, database.AsParticipant.cd.Async as AsyncStream);
#endif
                    }
                }
                if (readC != null)
                    readC.Block();
            }
            ordSpec = svo;
            Ordering(false);
            nominalDataType = rowSet.rowType;
        }
        public string WhereString(ATree<long, SqlValue> svs, ATree<int, ATree<long, TypedValue>> mts, Context cnx, Record pre)
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = svs?.First(); b != null; b = b.Next())
            {
                var sw = b.value().ToString1(cnx, pre);
                if (sw.Length > 1)
                {
                    sb.Append(cm); cm = " and ";
                    sb.Append(sw);
                }
            }
            for (var d = mts?.First(); d != null; d = d.Next())
                for (var b = d.value().First(); b != null; b = b.Next())
                {
                    var nm = (cnx as Transaction).databases[d.key()]._Role.defs[b.key()].name;
                    if (accessibleDataType[nm] == null)
                        continue;
                    sb.Append(cm); cm = " and ";
                    sb.Append(nm.ident);
                    sb.Append("=");
                    var tv = b.value();
                    if (tv.dataType.kind == Sqlx.CHAR)
                        sb.Append("'" + tv.ToString() + "'");
                    else
                        sb.Append(tv.ToString());
                }
            return sb.ToString();
        }
        internal override int? FillHere(Ident n)
        {
            if (target != null)
            {
                n.segpos = target.defpos;
                n.dbix = target.dbix;
            }
            n.type = (name==null || n.ident==name.ident)?Ident.IDType.Table:Ident.IDType.Alias;
            return 0;
        }
        /// <summary>
        /// Compute the effective key type for a given index
        /// </summary>
        /// <param name="x">The index</param>
        internal void KType(Database db,Index x)
        {
            ordSpec = null;
            if (x == null)
                return;
            int n = x.cols.Length;
            var r = new OrderSpec();
            for (int j = 0; j < n; j++)
            {
                var cp = x.cols[j];
                var dr = Sqlx.ASC;
                if (cp < 0) { cp = -cp; dr = Sqlx.DESC; }
                var ob = db.objects[cp];
                var id = ob.NameInSession(db);
                var sv = (id != null) ? ValFor(id) : null;
                if (sv == null)
                    return;
                r.itemsAdd(this,new OrderItem(sv)
                {
                    ascDesc = dr
                });
            }
            ordSpec = r;
        }
        /// <summary>
        /// Process the correlation set
        /// <param name="cr">The correlation</param>
        /// <param name="name">The table name (renaming support)</param>
        /// </summary>
        internal void ApplyCorrelation(Transaction tr,Database db,Correlation cr, Ident name)
        {
            if (target.type==DBTree.Tables && rowSet == null && cr != null && cr.cols.Length>0)
            {
                if (accessibleDataType == null || accessibleDataType.Length==0)
                    throw tr.Exception("2E111", tr._Role(db).name.ToString(), "" + target.defpos).Mix();
                var coldts = new Domain[cr.cols.Length];
                var ndt = nominalDataType;
                for (int i = 0; i < cr.cols.Length; i++)
                {
                    var s = cr.cols[i].ToString();
                    var j = ndt.names[s];
                    if (j.HasValue)
                    {
                        cr.cols[i].Set(target.dbix, ndt.names[j.Value].Defpos(Ident.IDType.Column), Ident.IDType.Column);
                        coldts[i] = ndt[j.Value];
                        goto found;
                    }
                    throw new DBException("42112", s);
                    found:;
                }
                nominalDataType = new RowType(Sqlx.ROW,coldts, cr.cols);
            }
            if (display==0)
                display = nominalDataType.Length;
        }
        /// <summary>
        /// Add a column to a LogTable
        /// </summary>
        /// <param name="name">The column name</param>
        /// <param name="dt">The type</param>
        void AddColumn(Context cnx,Database db,string name, Sqlx dt) 
        {
            var tb = (SystemTable)target;
            Selector c = null;
            var n = new Ident(name, 0);
            c = tb._GetColumn(cnx,db,n);
            if (c == null)
            {
                c = new SystemTableColumn(tb.shell, name, (int)db._Role.defs[tb.defpos].props.Count, dt);
                ATree<Ident,long?>.Add(ref db._Role.defs[tb.defpos].props, n, c.defpos);
            }
            SystemRowSet.AddColumn(tb, name, dt);
        }
        /// <summary>
        /// Add the base table TableColumns to a LogTable
        /// </summary>
        /// <param name="t">The base table</param>
        void AddColsFrom(SystemTable tt, Table t) 
        {
            PhysBase pb = SystemTable.defaultDb.pb;
            long pos = pb.startData;
            var cl = new List<long>(); // watch out for and don't include Alters
            while (pos < pb.Ppos)
            {
                Physical ph = pb.Get(pos, ref pos);
                if (ph is PColumn pc && pc.tabledefpos == t.defpos && !cl.Contains(pc.defpos))
                {
                    SystemRowSet.AddColumn(tt, "" + pc.defpos, Sqlx.CHAR);
                    cl.Add(pc.defpos);
                }
            }
        }
        internal override void AddRestViews(CursorSpecification q)
        {
            if (target is RestView rv)
                ATree<long, RestView>.Add(ref q.restViews, cxid, rv);
            source?.AddRestViews(q);
        }
        /// <summary>
        /// Add a given where condition to this Query
        /// </summary>
        /// <param name="cond">The where condition</param>
        /// <param name="needed"></param>
        /// <param name="rqC"></param>
        /// <param name="qw"></param>
        /// <returns></returns>
        internal override void AddCondition(Transaction tr, ATree<long,SqlValue> cond, UpdateAssignment[] assigs, RowSet data)
        {
            for(var b=cond.First();b!=null;b=b.Next())
            {
                var c = b.value();
                if (c.IsFrom(tr, this, false))
                    AddCondition(tr, c);
                if (c != null && c.kind == Sqlx.EQL)
                {
                    SqlValue.Setup(tr, this, c, Domain.Bool);
                    if (c is SqlValueExpr sqe)
                    {
                        if (sqe.left.name is Ident sl && sl.Defpos() > 0 && sqe.right is SqlLiteral rg &&
                            !filter.Contains(sl.Defpos()))
                            ATree<long, TypedValue>.Add(ref filter, sl.Defpos(), rg.val);
                        if (sqe.right.name is Ident sr && sr.Defpos() > 0 && sqe.left is SqlLiteral lf &&
                            !filter.Contains(sr.Defpos()))
                            ATree<long, TypedValue>.Add(ref filter, sr.Defpos(), lf.val);
                    }
                }
                source?.ImportCondition(tr, c);
            }
            rowSet?.AddCondition(tr, cond, cxid);
            base.AddCondition(tr, cond, assigs, data);
            if (assigns == null)
                assigns = assigs;
            if (rowSet == null)
                rowSet = data;
        }
        internal override void DistributeAssigns(ATree<UpdateAssignment,bool> assigs)
        {
            if (assigs != null)
            {
                var na = new List<UpdateAssignment>();
                for (var b =assigs.First();b!=null;b=b.Next())
                    if (b.key().vbl.blockid==blockid)
                        na.Add(b.key());
                assigns = new UpdateAssignment[na.Count];
                for (int i = 0; i < na.Count; i++)
                    assigns[i] = na[i];
            }
        }
        /// <summary>
        /// The compilcation here is that generation rule dependency may not actually suit left to right evaluation.
        /// The SQL standard has restrictive rules about the syntax of generation rules. I prefer a more dynamic approach;
        /// the current implementation here uses null values with exception 22004
        /// </summary>
        /// <param name="rec">A Record</param>
        /// <returns>A TRow containing the selected values</returns>
        internal TRow RowFor(Context cnx,Record rec)
        {
            var tr = cnx as Transaction;
            var tb = target as Table;
            var db = tr.Db(tb.dbix);
            var vs = new TypedValue[nominalDataType.Length];
            for (int j = 0; j < vs.Length; j++)
                vs[j] = null;
            var todo = vs.Length;
            while (todo > 0)
            {
                var donesome = false;
                for (int j = 0; j < vs.Length; j++)// j < selects.Count; j++)
                    if (vs[j] == null)
                    try
                    {
                        var sc = db.objects[cols[j].name.Defpos()] as Selector;
                        switch (sc.Generated)
                        {
                            case PColumn.GenerationRule.No:
                                vs[j] = rec?.Field(sc.defpos) ?? TNull.Value;
                                todo--;
                                donesome = true;
                                break;
                            case PColumn.GenerationRule.Expression:
                                {
                                    SqlValue expr = null; ;
                                    var a = tr.PushRole(db, sc.definer, sc.owner);
                                    var dt = db.GetDataType(sc.domaindefpos);
                                    var needed = BTree<SqlValue, Ident>.Empty; // ignored
                                    expr = new Parser(tr).ParseSqlValue(sc.defaultValue, dt);
                                    SqlValue.Setup(tr,this,expr, db.GetDataType(sc.defpos));
                                    tr.PopTo(a);
                                    if (expr.Eval(tr,rowSet) is TypedValue ev)
                                        vs[j] = ev;
                                    else
                                        return null;
                                    todo--; donesome = true;
                                    }
                                break;
                            case PColumn.GenerationRule.RowStart:
                                {
                                    var pd = db.objects[tb.systemTime] as PeriodDef;
                                    var dt = db.GetDataType(pd.startColDefpos);
                                    vs[j] = rec.Field(pd.startColDefpos);
                                    todo--; donesome = true;
                                }
                                break;
                            case PColumn.GenerationRule.RowEnd:
                                {
                                    var pd = db.objects[tb.systemTime] as PeriodDef;
                                    var dt = db.GetDataType(pd.endColDefpos);
                                    vs[j] = rec.Field(pd.endColDefpos);
                                    todo--; donesome = true;
                                }
                                break;
                        }
                    }
                    catch (DBException e)
                    {
                        if (e.signal != "22004")
                            throw e;
                    }
                if (!donesome)
                    throw new DBException("22004", tb.NameInSession(db));
            }
            return new TRow(tr, nominalDataType, vs);
        }
        /// <summary>
        /// Optimise retrievals
        /// </summary>
        /// <param name="rec"></param>
        /// <returns></returns>
        internal bool CheckMatch(Transaction tr,Record rec)
        {
            var dx = name?.dbix ?? target.dbix;
            if (rec != null)
                for(var e = matches[dx]?.First();e!= null;e=e.Next())
                { 
                    var v = rec.Field(e.key());
                    var m = e.value();
                    if (v != null && m!=null && m.dataType.Compare(tr, m, v) != 0)
                        return false;
                }
            return true;
        }
        internal override Ident FindAliasFor(Ident n)
        {
            return (names[n]!=null)? (alias??name): null;
        }
        internal override int Insert(string prov,RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl, bool autokey = false)
        {
            // We might have been called just to execute this first checking stage
            var dfs = defs;
            Analyse(data.tr);
            defs = dfs;
            if (data.tr.parse != ExecuteStatus.Obey)
                return 0;
            var d = data.tr.Db(target.dbix) as Transaction;
            // Get our bearings and check permissions
            if (d.schemaKey != 0 && d.schemaKey != d._Role.defs[target.defpos].lastChange)
                throw new DBException("2E307", target.NameInSession(d)).Mix();
            return target.Insert(this, prov, data, eqs, rs,cl,autokey);
        }
        internal override int Update(Transaction tr,ATree<string,bool> ur, Adapters eqs, List<RowSet> rs)
        {
            if (assigns == null)
                return 0;
            if (tr.parse != ExecuteStatus.Obey)
                return 0;
            var d = tr[this].AsParticipant;
            if (d.schemaKey != 0 && d.schemaKey != d._Role.defs[target.defpos].lastChange)
                throw new DBException("2E307", target.NameInSession(d)).Mix();
            var needed = BTree<SqlValue, Ident>.Empty;
            foreach (var sa in assigns)
                sa.SetupValues(tr, this);
            return target.Update(tr,this, ur, eqs, rs);
        }
        internal override int Delete(Transaction tr,ATree<string,bool> dr, Adapters eqs)
        {
            var d = tr[this].AsParticipant;
            if (d.schemaKey != 0 && d.schemaKey != d._Role.defs[target.defpos].lastChange)
                throw new DBException("2E307", target.NameInSession(d)).Mix();
            return target.Delete(tr,this,dr,eqs);
        }
        internal override void Close(Transaction tr)
        {
            var d = tr.Db(target.dbix).AsParticipant;
            base.Close(tr);
        }
        /// <summary>
        /// A readable version of this query
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return (alias == null) ? "From" : alias.ident;
        }
        internal void Validate(string etag)
        {
            if (etag== null || etag.Length==0)
                return;
            var row = rowSet?.First()?.Value();
            if (row?.etag == null)
                return;
            if (etag.CompareTo(row.etag.ToString()) != 0)
                throw new DBException("40082").Mix();
        }
    }
    /// <summary>
    /// The interesting bit here is that if we have something like "insert into a(b,c) select d,e from f"
    /// the table-valued subquery silently gets its columns renamed to b,c and types coerced to match a, 
    /// and then the resulting columns get reordered to become candidate rows of a so that trigger processing
    /// etc can proceed.
    /// This is a bit more complex than "insert into a values(..." and requires some planning.
    /// The current approach is that in the above example nominalDataType is a's row type, nominaltype is for (b,c)
    /// and rows is a subquery before the renaming. 
    /// The renaming, reordering and coercion steps complicate the coding.
    /// </summary>
    internal class SqlInsert : Executable
    {
        internal From from;
        /// <summary>
        /// Provenance information if supplied
        /// </summary>
        public string provenance = null;
        internal Domain valuesDataType = null;
        internal bool autokey;
        internal Level classification = Level.D;
        /// <summary>
        /// Constructor: an INSERT statement from the parser.
        /// </summary>
        /// <param name="cx">The parsing context</param>
        /// <param name="name">The name of the table to insert into</param>
        public SqlInsert(Transaction tr, Ident name, Correlation cr, string prov,string i,bool ak)
            : base(Type.Insert,i)
        {
            from = new From(tr, i, name, cr, Grant.Privilege.Insert);
            if (from.cols.Count == 0 && !(from.target is RestView))
                throw new DBException("2E111", tr.user, name.ident).Mix();
            provenance = prov;
            valuesDataType = from.nominalDataType;
            autokey = ak;
            from.nominalDataType = from.accessibleDataType;
        }
        protected SqlInsert(SqlInsert s, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) :base(s,ref cs,ref vs)
        {
            from = (From)s.from.Copy(ref cs,ref vs);
            provenance = s.provenance;
            autokey = s.autokey;
            classification = s.classification;
            valuesDataType = s.valuesDataType;
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new SqlInsert(this,ref cs,ref vs);
        }
        public override void Obey(Transaction tr)
        {
            tr.Execute(this);
        }
    }
    /// <summary>
    /// QuerySearch is for DELETE and UPDATE 
    /// </summary>
    internal class QuerySearch : Executable
    {
        internal From from;
        internal QuerySearch(Transaction tr, Ident ic, Correlation cr, Grant.Privilege how,string i)
            : this(Type.DeleteWhere,tr,ic,cr,how,i)
        {
            from.assigns = new UpdateAssignment[0]; // detected for HttpService for DELETE verb
        }
        /// <summary>
        /// Constructor: a DELETE or UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The parsing context</param>
        protected QuerySearch(Type et,Transaction tr, Ident ic, Correlation cr, Grant.Privilege how,string i)
            : base(et,i)
        {
            from = new From(tr, i, ic, cr, how);
            if (from.accessibleDataType.Length == 0)
                throw new DBException("2E111", tr.user, ic.ident).Mix();
        }
        protected QuerySearch(QuerySearch q, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            : base(q,ref cs,ref vs)
        {
            from = (From)q.from.Copy(ref cs,ref vs);
        }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new QuerySearch(this,ref cs,ref vs);
        }
        /// <summary>
        /// A readable version of the delete statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var db = from.rowSet.tr.Db(from.target.dbix);
            var sb = new StringBuilder("DELETE FROM ");
            if (from.target != null)
                sb.Append(from.target.NameInOwner(db));
            from.CondString(sb, from.where, " where ");
            return sb.ToString();
        }
        public override void Obey(Transaction tr)
        {
            tr.Execute(this);
        }
    }
    /// <summary>
    /// Implement a searched UPDATE statement as a kind of QuerySearch
    /// </summary>
    internal class UpdateSearch : QuerySearch
    {
        /// <summary>
        /// Constructor: A searched UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The context</param>
        public UpdateSearch(Transaction tr, Ident ic, Correlation ca, Grant.Privilege how,string id)
            : base(Type.UpdateWhere, tr, ic, ca, how,id)
        {
        }
        protected UpdateSearch(UpdateSearch u, ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs) 
            : base(u, ref cs,ref vs)
        { }
        internal override Executable Copy(ref ATree<string, Context> cs, ref ATree<long, SqlValue> vs)
        {
            return new UpdateSearch(this,ref cs,ref vs);
        }
        /// <summary>
        /// A readable version of the update statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (from.target == null)
                return "UpdateSearch";
            var db = from.rowSet.tr.Db(from.target.dbix) as Transaction;
            var sb = new StringBuilder();
            sb.Append("UPDATE " + from.target.NameInOwner(db) + " SET ");
            var c = "";
            foreach (var a in from.assigns)
            {
                sb.Append(c + a.ToString());
                c = ", ";
            }
            from.CondString(sb, from.where, " where ");
            return sb.ToString();
        }
        public override void Obey(Transaction tr)
        {
            tr.Execute(this); // we need this override even though it looks like the base's one!
        }
    }
}