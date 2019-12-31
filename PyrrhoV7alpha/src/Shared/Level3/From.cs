using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level3
{
    internal class From : Query
    {
        internal const long
            Assigns = -150, // BList<UpdateAssignment>
            Source = -151, // Query (for Views)
            Static = -152, // From (defpos for STATIC)
            Target = -153; // DBObject (a table or view)
        internal BList<UpdateAssignment> assigns =>
            (BList<UpdateAssignment>)mem[Assigns] ?? BList<UpdateAssignment>.Empty;
        internal Query source => (Query)mem[Source];
        internal long target => (long)(mem[Target]??-1L);
        internal readonly static From _static = new From();
        From() : base(Static, BTree<long,object>.Empty) { }
        public From(long dp, Table tb, ObInfo ti) : this(dp, BTree<long, object>.Empty
            + (Target, tb.defpos) + (Display,ti.Length) + (_Domain,Domain.TableType)
            + (Name, ti.name) +(RowType,ti))
        {
    //        if (dp >= 0 && dp < Transaction.TransPos)
    //            throw new PEException("PE000");will happen when deserialising Triggers
        }
        protected From(long defpos, BTree<long, object> m) : base(defpos, m)
        {
     //       if (defpos >= 0 && defpos < Transaction.TransPos)
     //           throw new PEException("PE000"); 
        }
        public static From operator+(From f,(long,object) x)
        {
            return (From)f.New(f.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new From(defpos, m);
        }
        internal override DBObject Replace(Context cx, DBObject was, DBObject now)
        {
            var r = (From)base.Replace(cx,was,now);
            var ch = false;
            var so = r.source?.Replace(cx, was, now);
            if (so != r.source)
            {
                ch = true;
                r += (Source, so);
            }
            var ua = BList<UpdateAssignment>.Empty;
            for (var b = assigns?.First(); b != null; b = b.Next())
                ua += b.value().Replace(cx, was, now);
            if (ua != assigns)
                r += (Assigns, ua);
            if (now is SqlCol sc && sc.tableCol.tabledefpos==target)
            {
                var bt = cx.used[defpos] ?? BTree<long, SqlCol>.Empty;
                bt += (sc.defpos, sc);
                cx.used += (defpos, bt);
                r += (RowType,r.rowType.Replace(cx,was,now));
                ch = true;
            }
            if (ch)
                cx.Add(r);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new From(dp,mem);
        }
        internal override SqlValue ToSql(Ident id,Database db)
        {
            return new SqlTable(id.iix,name,this);
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (From)base.Frame(cx);
            var so = r.source?.Frame(cx);
            if (so != r.source)
                r += (Source, so);
            var ua = BList<UpdateAssignment>.Empty;
            for (var b = assigns?.First(); b != null; b = b.Next())
                ua += b.value().Frame(cx);
            if (ua != assigns)
                r += (Assigns, ua);
            return r;
        }
        internal ObInfo KeyType(Transaction tr)
        {
            return (tr.objects[target] is Table tb && tb.FindPrimaryIndex(tr) is Index ix) ?
             (ObInfo)tr.role.obinfos[ix.defpos] : null;
        }
        internal override bool Uses(long t)
        {
            return target==t;
        }
        /// <summary>
        /// Optimise retrievals
        /// </summary>
        /// <param name="rec"></param>
        /// <returns></returns>
        internal bool CheckMatch(Transaction tr, Context cx, TableRow rec)
        {
            if (rec != null)
                for (var e = matches?.First(); e != null; e = e.Next())
                {
                    var v = rec.fields[e.key().defpos];
                    var m = e.value();
                    if (v != null && m != null && m.dataType.Compare(m, v) != 0)
                        return false;
                }
            return true;
        }
        internal override RowSet RowSets(Database tr, Context cx)
        {
            if (defpos == Static)
                return new TrivialRowSet(tr as Transaction, cx, this, new TRow(rowType, cx.values));
            RowSet rowSet = null;
            //           if (target == null)
            //               return new TrivialRowSet(tr, cx, this, Eval(tr, cx) as TRow ?? TRow.Empty);
            //         if (target is View vw)
            //             return vw.RowSets(tr, cx, this);

            // ReadConstraints only apply in explicit transactions 
            ReadConstraint readC = tr.autoCommit?null
                :tr._ReadConstraint(cx, (DBObject)tr.objects[target]);
            int matches = 0;
            PRow match = null;
            Index index = null;
            // At this point we have a table/alias, 
            // we want to find the Index best meeting our needs
            // Score: Add 10^n for n ordType cols occurring in order, 
            // add n+1 for each filter column occurring in position k-n 
            // in an index with k cols.
            // Find the index with highest score, then set up
            // the match Link for it
       /*     if (periods.Contains(target.defpos))
            {
                // a periodspecification has been supplied for this table.
                var ps = periods[target.defpos];
                if (ps.kind == Sqlx.NO)
                {
                    // simply use the appropriate time versioning index for this table
                    var tb = (Table)target;
                    var pp = (ps.periodname == "SYSTEM_TIME") ? tb.systemPS : tb.applicationPS;
                    var pd = (PeriodDef)tr.objects[pp];
                    if (pd != null)
                        index = tb.indexes[pp]; // I doubt this
                }
            } 
            else */
            if(tr.objects[target] is Table ta)
            {
                int bs = 0;      // score for best index
                for (var p = ta.indexes.First(); p != null; p = p.Next())
                {
                    var x = (Index)tr.objects[p.value()];
                    if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey 
                        || x.tabledefpos != target)
                        continue;
                    var dt = (ObInfo)tr.role.obinfos[x.defpos];
                    int sc = 0;
                    int nm = 0;
                    int n = 0;
                    PRow pr = null;
                    var havematch = false;
                    int sb = 1;
                    for (int j = (int)dt.columns.Count - 1; j >= 0; j--)
                    {
                        for (var fd = filter.First(); fd != null; fd = fd.Next())
                        {
                            if (dt.columns[j].defpos == fd.key())
                            {
                                sc += 9 - j;
                                nm++;
                                pr = new PRow(fd.value(), pr);
                                havematch = true;
                                goto nextj;
                            }
                        }
                        var ob = dt.columns[j];
                        if (ordSpec != null && n < ordSpec.items.Count)
                        {
                            var ok = ordSpec.items[n];
                            var sr = ValFor(cx,ok);
                            if (ok != null && ok.MatchExpr(this, sr))
                            {
                                n++;
                                sb *= 10;
                            }
                        }
                        pr = new PRow(TNull.Value, pr);
                    nextj:;
                    }
                    if (!havematch)
                        pr = null;
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
            if (index != null && index.rows != null)
            {
                rowSet = new IndexRowSet(tr, cx, this, index, match);
                if (readC != null)
                {
                    if (matches == (int)rowSet.keyType.columns.Count &&
                        (index.flags & (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique)) 
                            != PIndex.ConstraintType.NoType)
                        readC.Singleton(index, match);
                    else
                        readC.Block();
                }
            }
            else
            {
                if (tr.objects[target] is SystemTable st)
                    rowSet = new SystemRowSet(tr, cx, this);
                else if (tr.objects[target] is Table tb)
                {
                    index = tb.FindPrimaryIndex(tr);
                    if (index != null && index.rows != null)
                        rowSet = new IndexRowSet(tr, cx, this, index, null);
                    else
                        rowSet = new TableRowSet(tr, cx, this);
                }
                if (readC != null)
                    readC.Block();
            }
            if (readC!=null)
                cx.rdC += (target, readC);
            return rowSet;
        }
        internal override Transaction Insert(Transaction tr, Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs, Level cl)
        {
            return ((DBObject)tr.objects[target]).Insert(tr, _cx, this, prov, data, eqs, rs, cl);
        }
        internal override Transaction Delete(Transaction tr, Context cx, BTree<string, bool> dr, Adapters eqs)
        {
            return ((DBObject)tr.objects[target]).Delete(tr, cx, this, dr, eqs);
        }
        internal override Transaction Update(Transaction tr, Context cx, BTree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            return ((DBObject)tr.objects[target]).Update(tr, cx, this, ur, eqs, rs);
        }
        /// <summary>
        /// Accessor: Check a new table check constraint
        /// </summary>
        /// <param name="tr">Transaction</param>
        /// <param name="c">The new Check constraint</param>
        internal void TableCheck(Transaction tr, PCheck c)
        {
            var cx = new Context(tr);
            var trs = new TableRowSet(tr,cx,this+(Where,c.test));
            if (trs.First(cx) != null)
                throw new DBException("44000", c.check).ISO();
        }
        /// <summary>
        /// Build a period window
        /// </summary>
        /// <param name="fm">The query</param>
        /// <param name="ps">The period spec</param>
        /// <returns>The ATree of row positions</returns>
        public BTree<long, TRow> Build(Transaction tr, Context cx, Query fm, PeriodSpec ps)
        {
            var r = BTree<long, TRow>.Empty;
            if (tr.objects[target] is Table tb)
            for (var e = new WindowRowSet(tr,cx,fm,ps).First(cx); e != null; e = e.Next(cx))
            {
                var ts = (Period)e.row[new Ident(ps.periodname,0)].Val();
                var dt = ts.start.dataType;
                var time1 = ps.time1.Eval(tr, cx);
                var time2 = ps.time2.Eval(tr, cx);
                switch (ps.kind)
                {
                    case Sqlx.AS:
                        if (!(dt.Compare(ts.start, time1) <= 0
                  && dt.Compare(ts.end, time1) > 0)) continue; break;
                    case Sqlx.BETWEEN:
                        if (!(dt.Compare(ts.start, time2) <= 0
             && dt.Compare(ts.end, time1) > 0)) continue; break;
                    case Sqlx.FROM:
                        if (!(dt.Compare(ts.start, time2) < 0
                && dt.Compare(ts.end, time1) > 0)) continue; break;
                }
                r += (e.Rec().defpos, e.row);
            }
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (defpos == _static.defpos) sb.Append(" STATIC");
            if (mem.Contains(Assigns)){ sb.Append(" Assigns:"); sb.Append(assigns); }
            if (mem.Contains(Source)) { sb.Append(" Source:"); sb.Append(source); }
            if (mem.Contains(Target)) { sb.Append(" Target="); sb.Append(Uid(target)); }
            return sb.ToString();
        }
    }
    internal class FromOldTable: From
    {
        internal long trs => (long)(mem[SqlOldRowCol.TransitionRowSet] ?? -1L);
        public FromOldTable(Ident id, From f)
            : base(id.iix,f.mem
                  +(SqlOldRowCol.TransitionRowSet,f.defpos)
                  +(Name,id.ident)) { }
        protected FromOldTable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static FromOldTable operator+(FromOldTable f,(long,object)x)
        {
            return (FromOldTable)f.New(f.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new FromOldTable(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new FromOldTable(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (FromOldTable)base.Relocate(wr);
            r += (SqlOldRowCol.TransitionRowSet, wr.Fix(trs));
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (FromOldTable)base.Frame(cx);
            r += (SqlOldRowCol.TransitionRowSet, cx.data[trs]);
            return r;
        }
        internal RowSet Change(RowSet rs,Context cx)
        {
            if (rs is TableRowSet trs && trs.from.defpos == defpos)
                return new OldTableRowSet(this,trs,cx);
            if (rs is IndexRowSet irs && irs.from.defpos == defpos)
                return new IndexRowSet(rs._tr, cx, this,
                    cx.FindTriggerActivation(target).oldIndexes[irs.index.defpos],
                    irs.filter);
            if (rs is GroupingRowSet grs)
                return new GroupingRowSet(cx, rs.qry, Change(grs.source, cx), 
                    grs.groups,grs.having);
            if (rs is MergeRowSet mrs)
                return new MergeRowSet(cx, rs.qry, Change(mrs.left, cx),
                    Change(mrs.right, cx), mrs.distinct, mrs.oper);
            if (rs is ExportedRowSet ers)
                return new ExportedRowSet(cx, Change(ers.source, cx), ers.rtyp);
            if (rs is SelectedRowSet srs)
                return new SelectedRowSet(rs._tr, cx, rs.qry, Change(rs, cx));
            if (rs is EvalRowSet frs)
                return new EvalRowSet(rs._tr, cx, rs.qry, Change(frs.source,cx), frs.having);
            if (rs is DistinctRowSet drs)
                return new DistinctRowSet(cx, Change(rs, cx));
            if (rs is OrderedRowSet ors)
                return new OrderedRowSet(cx, rs.qry, Change(ors.source, cx), 
                    ors.ordSpec,ors.distinct);
            if (rs is SortedRowSet yrs)
                return new SortedRowSet(cx, rs.qry, Change(yrs.source, cx), rs.keyType,
                    yrs.treeInfo);
            if (rs is RowSetSection rsx)
                return new RowSetSection(cx, Change(rsx.source, cx), rsx.offset, rsx.count);
            if (rs is JoinRowSet jrs)
                return new JoinRowSet(cx, rs.qry, Change(jrs.first, cx), Change(jrs.second, cx));
            return rs;
        }
        internal override RowSet RowSets(Database tr, Context cx)
        {
            var t = cx.data[((FromOldTable)cx.defs[name].Item1).trs];
            return t;
    //        if (t == null)
    //            return null;
    //        return Change(base.RowSets(t._tr, cx),cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Trs: "); sb.Append(Uid(trs));
            return sb.ToString();
        }
    }
    /// <summary>
    /// The interesting bit here is that if we have something like "insert into a(b,c) select d,e from f"
    /// the table-valued subquery silently gets its columns renamed to b,c and types coerced to match a, 
    /// and then the resulting columns get reordered to become candidate rows of a so that trigger processing
    /// etc can proceed.
    /// This is a bit more complex than "insert into a values(..." and requires some planning.
    /// The current approach is that in the above example domain is a's row type, nominaltype is for (b,c)
    /// and rows is a subquery before the renaming. 
    /// The renaming, reordering and coercion steps complicate the coding.
    /// </summary>
    internal class SqlInsert : Executable
    {
        internal const long
            _Table = -154, // From
            Provenance = -155, //string
            Value = -156; // SqlValue
        internal From from => (From)mem[_Table];
        /// <summary>
        /// Provenance information if supplied
        /// </summary>
        public string provenance => (string)mem[Provenance];
        public SqlValue value => (SqlValue)mem[Value];
        /// <summary>
        /// Constructor: an INSERT statement from the parser.
        /// </summary>
        /// <param name="cx">The parsing context</param>
        /// <param name="name">The name of the table to insert into</param>
        public SqlInsert(long dp, Database tr,Context cx,Ident name, Correlation cr, 
            string prov, SqlValue v) : base(dp+1,_Mem(dp,tr,cx,name,cr)+ (Provenance, prov)+(Value,v))
        {
            if (from.rowType.Length == 0)
                throw new DBException("2E111", tr.user, name.ident).Mix();
        }
        protected SqlInsert(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlInsert operator+(SqlInsert s,(long,object)x)
        {
            return new SqlInsert(s.defpos, s.mem + x);
        }
        static BTree<long,object> _Mem(long dp,Database db,Context cx,Ident name,Correlation cr)
        {
            var tb = db.GetObject(name.ident) as Table ??
                throw new DBException("42107", name.ident);
            var rt = (ObInfo)((ObInfo)db.role.obinfos[tb.defpos]).Relocate(dp);
            var fm = new From(dp, tb, rt);
            if (cr!=null)
                fm += (Query.RowType,cr.Pick(rt.For(db, tb, Grant.Privilege.Insert)));
            fm = (From)fm.AddCols(cx,fm);
            return BTree<long, object>.Empty + (_Table, fm);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = base.Replace(cx,so,sv);
            var fm = from.Replace(cx, so, sv);
            if (fm != from)
                r += (_Table, fm);
            return cx.Add(r);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlInsert(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r =  (SqlInsert)base.Relocate(wr);
            var tb = from.Relocate(wr);
            if (tb != from)
                r += (_Table, tb);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlInsert)base.Frame(cx);
            var tb = from.Frame(cx);
            if (tb != from)
                r += (_Table, tb);
            var vl = value.Frame(cx);
            if (vl != value)
                r += (Value, vl);
            return cx.Add(r);
        }
        public override Transaction Obey(Transaction tr, Context cx)
        {
            value.RowSet(defpos, tr, cx, from);
            Level cl = tr.user.clearance;
            var ta = tr.objects[from.target] as Table;
            if (tr.user.defpos != tr.owner 
                && ta.enforcement.HasFlag(Grant.Privilege.Insert)
                && !cl.ClearanceAllows(from.classification))
                throw new DBException("42105");
            return from.Insert(tr,cx,provenance, cx.data[defpos], new Common.Adapters(),
                new List<RowSet>(), classification);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Table: "); sb.Append(from);
            sb.Append(" Value: "); sb.Append(value);
            if (provenance != null)
            { sb.Append(" Provenance: "); sb.Append(provenance); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// QuerySearch is for DELETE and UPDATE 
    /// </summary>
    internal class QuerySearch : Executable
    {
        internal From table => (From)mem[SqlInsert._Table];
        internal BList<UpdateAssignment> assigs =>
            (BList<UpdateAssignment>)mem[From.Assigns] ?? BList<UpdateAssignment>.Empty;
        internal QuerySearch(long dp,Database db, Context cx,Ident ic, Correlation cr, 
            Grant.Privilege how) : this(Type.DeleteWhere,dp,db,cx,ic,cr,how,BList<UpdateAssignment>.Empty)
            // detected for HttpService for DELETE verb
        { }
        /// <summary>
        /// Constructor: a DELETE or UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The parsing context</param>
        protected QuerySearch(Type et,long dp,Database db, Context cx,Ident ic, Correlation cr, 
            Grant.Privilege how, BList<UpdateAssignment> ua=null)
            : base(dp,_Mem(dp,db,cx,ic,cr,ua)+(_Type,et)+(From.Assigns,ua))
        {
            if (table.rowType.Length == 0)
                throw new DBException("2E111", db.user, ic.ident).Mix();
        }
        protected QuerySearch(long dp,BTree<long,object>m) :base(dp,m) { }
        public static QuerySearch operator+(QuerySearch q,(long,object)x)
        {
            return (QuerySearch)q.New(q.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new QuerySearch(defpos,m);
        }
        static BTree<long, object> _Mem(long dp,Database db, Context cx, Ident name, Correlation cr, 
            BList<UpdateAssignment> ua)
        {
            var tb = db.GetObject(name.ident) as Table ??
                throw new DBException("42107", name.ident);
            var rt = db.role.obinfos[tb.defpos] as ObInfo;
            var dt = rt.For(db as Transaction, tb, Grant.Privilege.Insert);
            if (cr != null)
                dt = cr.Pick(dt);
            return BTree<long, object>.Empty + (SqlInsert._Table, new From(name.iix,tb,dt));
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (QuerySearch)base.Replace(cx, so, sv);
            var tb = table.Replace(cx, so, sv);
            if (tb != table)
                r += (SqlInsert._Table, tb);
            var ua = BList<UpdateAssignment>.Empty;
            for (var b = assigs?.First(); b != null; b = b.Next())
                ua += b.value().Replace(cx, so, sv);
            if (ua != assigs)
                r += (From.Assigns, ua);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new QuerySearch(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (QuerySearch)base.Relocate(wr);
            var tb = table.Relocate(wr);
            if (tb != table)
                r += (SqlInsert._Table, tb);
            var ua = BList<UpdateAssignment>.Empty;
            for (var b = assigs?.First(); b != null; b = b.Next())
                ua += (UpdateAssignment)b.value().Relocate(wr);
            if (ua != assigs)
                r += (From.Assigns, ua);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (QuerySearch)base.Frame(cx);
            var tb = table.Frame(cx);
            if (tb != table)
                r += (SqlInsert._Table, tb);
            var ua = BList<UpdateAssignment>.Empty;
            for (var b = assigs?.First(); b != null; b = b.Next())
                ua += b.value().Frame(cx);
            if (ua != assigs)
                r += (From.Assigns, ua);
            return cx.Add(r);
        }
        /// <summary>
        /// A readable version of the delete statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder("DELETE FROM ");
            if (table != null)
                sb.Append(table.name);
            table.CondString(sb, table.where, " where ");
            return sb.ToString();
        }
        public override Transaction Obey(Transaction tr, Context cx)
        {
            return table.Delete(tr, cx, BTree<string, bool>.Empty, new Adapters());
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
        public UpdateSearch(long dp, Database db, Context cx, Ident ic, Correlation ca, 
            Grant.Privilege how)
            : base(Type.UpdateWhere, dp, db, cx, ic, ca, how)
        {  }
        protected UpdateSearch(long dp, BTree<long, object> m) : base(dp, m) { }
        public static UpdateSearch operator+(UpdateSearch u,(long,object)x)
        {
            return (UpdateSearch)u.New(u.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UpdateSearch(defpos,m);
        }
        /// <summary>
        /// A readable version of the update statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("UPDATE " + table.name + " SET ");
            var c = "";
            for (var a =table.assigns.First();a!=null;a=a.Next())
            {
                sb.Append(c); sb.Append(a.value());
                c = ", ";
            }
            table.CondString(sb, table.where, " where ");
            return sb.ToString();
        }
        public override Transaction Obey(Transaction tr,Context cx)
        {
            return table.Update(tr, cx, BTree<string, bool>.Empty, new Adapters(),
                new List<RowSet>());
        }
        internal override DBObject Relocate(long dp)
        {
            return new UpdateSearch(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (UpdateSearch)base.Relocate(wr);
            var tb = table.Relocate(wr);
            if (tb != table)
                r += (SqlInsert._Table, tb);
            var ua = BList<UpdateAssignment>.Empty;
            for (var b = assigs?.First(); b != null; b = b.Next())
                ua += (UpdateAssignment)b.value().Relocate(wr);
            if (ua != assigs)
                r += (From.Assigns, ua);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (UpdateSearch)base.Frame(cx);
            var tb = table.Frame(cx);
            if (tb != table)
                r += (SqlInsert._Table, tb);
            var ua = BList<UpdateAssignment>.Empty;
            for (var b = assigs?.First(); b != null; b = b.Next())
                ua += b.value().Frame(cx);
            if (ua != assigs)
                r += (From.Assigns, ua);
            return cx.Add(r);
        }
    }
}