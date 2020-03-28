using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
        public From(Ident ic, Context cx, Table tb, QuerySpecification q=null,
            Selection qn=null, Grant.Privilege pr=Grant.Privilege.Select,
            Correlation cr=null) : base(ic.iix, _Mem(ic,cx, tb,q,qn,pr,cr))
        { }
        public From(long dp,Context cx,CallStatement pc,Correlation cr=null)
            :base(dp,_Mem(dp,cx,pc,cr))
        { }
        protected From(long defpos, BTree<long, object> m) : base(defpos, m)
        { }
        public static From operator+(From f,(long,object) x)
        {
            return (From)f.New(f.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new From(defpos, m);
        }
        /// <summary>
        /// The main task here is to compute the rowType for the new From. 
        /// The supplied ObInfo has been created from the names of table columns
        /// found during query resolution.
        /// All columns in the From's rowtype will be SqlCopy. None will have table uids.
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="db"></param>
        /// <param name="tb"></param>
        /// <param name="qn">Names required</param>
        /// <param name="pr"></param>
        /// <param name="cr">Aliases supplied if any</param>
        /// <returns></returns>
        static BTree<long,object> _Mem(Ident ic,Context cx,Table tb, QuerySpecification q, 
            Selection qn=null, Grant.Privilege pr = Grant.Privilege.Select, Correlation cr=null)
        {
            var ti = (ObInfo)cx.db.schemaRole.obinfos[tb.defpos];
            var ri = (ObInfo)cx.db.role.obinfos[tb.defpos];
            var us = BTree<long, bool>.Empty;
            var s = new Selection(ic.iix, ic.ident??ti.name);
            var de = 0;
            for (var b = qn?.First(); b != null; b = b.Next())
            {
                var n = b.value();
                var iq = ri.map[n.name];
                if (iq != null)
                {
                    var ci = ri[iq.Value];
                    var cp = n.defpos;
                    us += (ci.defpos, true);
                    s += (SqlValue)cx._Add(new SqlCopy(cp, n.name, ci, 
                            q?.defpos??ic.iix, ci.defpos));
                    de = 1;
                }
            }
            var ds = s.Length;
            // add the columns we can see in case they are referred to later
            if (s.Length < ri.Length)
            {
                for (var b = ri.columns.First(); b != null; b = b.Next())
                {
                    var ci = b.value();
                    if (!us.Contains(ci.defpos))
                    {
                        var tc = (TableColumn)cx.db.objects[ci.defpos];
                        s += new SqlTableCol(ci.defpos, ci.name, ic.iix, tc);
                        us += (ci.defpos, true);
                    }
                }
            }
            if (ds == 0)
                ds = s.Length;
            // add the columns we can't see in case they are somehow used later 
            if (s.Length < ti.Length)
            {
                for (var b = ti.columns.First(); b != null; b = b.Next())
                {
                    var ci = b.value();
                    if (!us.Contains(ci.defpos))
                    {
                        var tc = (TableColumn)cx.db.objects[ci.defpos];
                        s += new SqlTableCol(ci.defpos, "?"+ci.name, ic.iix, tc);
                    }
                }
            }
            if (cr != null)
            {
                s = cr.Pick(ic,s);
                ds = s.Length;
            }
            return BTree<long,object>.Empty
                   + (Target, tb.defpos) + (Display,ds) + (_Domain,Domain.TableType)
                   + (Name, s.name) +(RowType,s) + (Depth,de+1);
        }
        static BTree<long,object> _Mem(long dp,Context cx,CallStatement pc,Correlation cr)
        {
            var proc = (Procedure)cx.db.objects[pc.procdefpos];
            var disp = cr?.cols.Count ?? proc.retType.Length;
            var s = new Selection(dp,cr?.tablealias.ident??pc.name);
            for (var b=proc.retType.columns.First();b!=null;b=b.Next())
                s += new SqlRowSetCol(b.value(),dp);
            return BTree<long, object>.Empty
                + (Target,pc.procdefpos) + (Display,disp) + (_Domain,Domain.TableType)
                + (Name, proc.name) + (RowType,s);
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (From)base._Replace(cx,was,now);
            var ch = false;
            var so = r.source?._Replace(cx, was, now);
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
            if (ch)
                cx.Add(r);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new From(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (From)base.Relocate(wr);
            var ua = BList<UpdateAssignment>.Empty;
            var ch = false;
            for (var b=assigns.First();b!=null;b=b.Next())
            {
                var a = (UpdateAssignment)b.value().Relocate(wr);
                ua += a;
                if (a != b.value())
                    ch = true;
            }
            if (ch)
                r += (Assigns, ua);
            var sc = source?.Relocate(wr);
            if (sc != source)
                r += (Source, sc);
            var tg = wr.Fix(target);
            if (tg != target)
                r += (Target, tg);
            return r;
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
            return cx.Add(r,true);
        }
        internal Selection KeyType(Transaction tr)
        {
            if (tr.objects[target] is Table tb && tb.FindPrimaryIndex(tr) is Index ix)
            {
                var s = new Selection(tb.defpos,"");
                var oi = (ObInfo)tr.role.obinfos[ix.defpos];
                for (var b = oi.columns.First(); b != null; b = b.Next())
                {
                    var c = b.value();
                    s += rowType[rowType.map[c.name]??-1]
                        ??new SqlTableCol(b.key(),c.name,defpos, // for autokey
                        (TableColumn)tr.objects[c.defpos]);
                }
                return s;
            }
            return rowType;
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
        internal bool CheckMatch(Context cx, TableRow rec)
        {
            if (rec != null)
                for (var e = matches?.First(); e != null; e = e.Next())
                {
                    var v = rec.vals[e.key().defpos];
                    var m = e.value();
                    if (v != null && m != null && m.dataType.Compare(m, v) != 0)
                        return false;
                }
            return true;
        }
        internal override RowSet RowSets(Context cx)
        {
            //        if (cx.data.Contains(defpos))
            //            return cx.data[defpos];
            var inf = rowType.info;
            if (defpos == Static)
                return new TrivialRowSet(defpos,cx, inf, new TRow(inf.domain, cx.values));
            RowSet rowSet = null;
            //           if (target == null)
            //               return new TrivialRowSet(tr, cx, this, Eval(tr, cx) as TRow ?? TRow.Empty);
            //         if (target is View vw)
            //             return vw.RowSets(tr, cx, this);

            // ReadConstraints only apply in explicit transactions 
            ReadConstraint readC = cx.db.autoCommit?null
                :cx.db._ReadConstraint(cx, (DBObject)cx.db.objects[target]);
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
            if (cx.data.Contains(defpos))
                return cx.data[defpos];
            var tr = cx.db;
            var ta = tr.objects[target] as Table;
            if(ta!=null)
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
                            if (cx.obs[fd.key()] is SqlCopy co 
                                && co.copyFrom==dt.columns[j].defpos)
                            {
                                sc += 9 - j;
                                nm++;
                                pr = new PRow(fd.value(), pr);
                                havematch = true;
                                goto nextj;
                            }
                        }
                        if (ordSpec != null && n < ordSpec.items.Length)
                        {
                            var ok = ordSpec.items[n];
                            if (ok != null)
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
                rowSet = new SelectedRowSet(cx,this,new IndexRowSet(cx, ta, index, match));
                if (readC != null)
                {
                    if (matches == index.keys.Length &&
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
                    rowSet = new SystemRowSet(cx, this);
                else if (tr.objects[target] is Table tb)
                {
                    index = tb.FindPrimaryIndex(cx.db);
                    if (index != null && index.rows != null)
                        rowSet = new SelectedRowSet(cx,this,
                            new IndexRowSet(cx,tb, index));
                    else
                        rowSet = new SelectedRowSet(cx,this,
                            new TableRowSet(cx,tb.defpos));
                }
                if (readC != null)
                    readC.Block();
            }
            if (readC!=null)
                cx.rdC += (target, readC);
            return rowSet;
        }
        internal override Context Insert(Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs, Level cl)
        {
            return ((DBObject)_cx.db.objects[target]).Insert(_cx, this, prov, data, eqs, rs, cl);
        }
        internal override Context Delete(Context cx, BTree<string, bool> dr, Adapters eqs)
        {
            return ((DBObject)cx.db.objects[target]).Delete(cx, this, dr, eqs);
        }
        internal override Context Update(Context cx, BTree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            return ((DBObject)cx.db.objects[target]).Update(cx, this, ur, eqs, rs);
        }
        /// <summary>
        /// Accessor: Check a new table check constraint
        /// </summary>
        /// <param name="tr">Transaction</param>
        /// <param name="c">The new Check constraint</param>
        internal void TableCheck(Transaction tr, PCheck c)
        {
            var cx = new Context(tr);
            var trs = new TableRowSet(cx,target);
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
            for (var e = new WindowRowSet(cx,fm,ps).First(cx); e != null; e = e.Next(cx))
            {
                var ts = (Period)e[new Ident(ps.periodname,0)].Val();
                var dt = ts.start.dataType;
                var time1 = ps.time1.Eval(cx);
                var time2 = ps.time2.Eval(cx);
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
                r += (e.Rec().defpos, e);
            }
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (defpos == _static.defpos) sb.Append(" STATIC");
            if (mem.Contains(_Alias)) { sb.Append(" Alias "); sb.Append(alias); }
            if (mem.Contains(Assigns)){ sb.Append(" Assigns:"); sb.Append(assigns); }
            if (mem.Contains(Source)) { sb.Append(" Source:"); sb.Append(source); }
            if (mem.Contains(Target)) { sb.Append(" Target="); sb.Append(Uid(target)); }
            return sb.ToString();
        }
    }
    internal class FromOldTable: From
    {
        internal const long
            TRSPos = -315; // long
        internal long trs => (long)(mem[TRSPos] ?? -1L);
        public FromOldTable(Ident id, From f)
            : base(id.iix,_Mem(id,f)) { }
        protected FromOldTable(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Ident id,From f)
        {
            var oi = f.rowType.info;
            var ot = new Selection(id.iix, id.ident);
            for (var b = f.rowType.First(); b != null; b = b.Next())
                ot += b.value() + (SqlValue._From, id.iix);
            return BTree<long, object>.Empty + (RowType,ot)
                   + (Target, f.target) + (Display, oi.Length) + (_Domain, Domain.TableType)
                   + (Name, id.ident) + (TRSPos,f.defpos);
        }
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
            r += (TRSPos, wr.Fix(trs));
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (FromOldTable)base.Frame(cx);
            r += (SqlOldRowCol.TransitionRowSet, cx.data[trs]);
            return cx.Add(r,true);
        }
        internal RowSet Change(RowSet rs,Context cx)
        {
            if (rs is TableRowSet trs && trs.tabledefpos == defpos)
                return new OldTableRowSet(this,trs.tabledefpos,cx);
            if (rs is IndexRowSet irs && irs.table.defpos == defpos)
                return new IndexRowSet(cx, irs.table,
                    cx.FindTriggerActivation(target).oldIndexes[irs.index.defpos]);
            if (rs is GroupingRowSet grs)
                return new GroupingRowSet(cx, this, Change(grs.source, cx), 
                    grs.groups,grs.having);
            if (rs is MergeRowSet mrs)
                return new MergeRowSet(cx, this, Change(mrs.left, cx),
                    Change(mrs.right, cx), mrs.distinct, mrs.oper);
            if (rs is SelectedRowSet srs)
                return new SelectedRowSet(cx, (QuerySpecification)cx.obs[srs.defpos], Change(rs, cx));
            if (rs is EvalRowSet frs)
                return new EvalRowSet(cx, (QuerySpecification)cx.obs[frs.defpos], Change(frs.source,cx), frs.having);
            if (rs is DistinctRowSet drs)
                return new DistinctRowSet(cx, Change(rs, cx));
            if (rs is OrderedRowSet ors)
                return new OrderedRowSet(cx, Change(ors.source, cx), 
                    ors.ordSpec,ors.distinct);
            if (rs is SortedRowSet yrs)
                return new SortedRowSet(cx, Change(yrs.source, cx), rs.keyInfo,
                    yrs.treeInfo);
            if (rs is RowSetSection rsx)
                return new RowSetSection(cx, Change(rsx.source, cx), rsx.offset, rsx.count);
            if (rs is JoinRowSet jrs)
                return new JoinRowSet(cx, jrs.join, Change(jrs.first, cx), Change(jrs.second, cx));
            return rs;
        }
        internal override RowSet RowSets(Context cx)
        {
            return cx.data[trs];
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
        public SqlInsert(long dp,From fm,string prov, SqlValue v) 
           : base(dp,BTree<long,object>.Empty + (_Table,fm) + (Provenance, prov)+(Value,v))
        { }
        protected SqlInsert(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlInsert operator+(SqlInsert s,(long,object)x)
        {
            return new SqlInsert(s.defpos, s.mem + x);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = base._Replace(cx,so,sv);
            var fm = from._Replace(cx, so, sv);
            if (fm != from)
                r += (_Table, fm);
            cx.done += (defpos, r);
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
            var vl = value.Relocate(wr);
            if (vl != value)
                r += (Value, vl);
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
            return cx.Add(r,true);
        }
        public override Context Obey(Context cx)
        {
            var r = value.RowSet(from.defpos,cx,from.rowType.info);
            Level cl = cx.db.user?.clearance??Level.D;
            var ta = cx.db.objects[from.target] as Table;
            if (cx.db.user!=null && cx.db.user.defpos != cx.db.owner 
                && ta.enforcement.HasFlag(Grant.Privilege.Insert)
                && !cl.ClearanceAllows(from.classification))
                throw new DBException("42105");
            return from.Insert(cx,provenance, r, new Common.Adapters(),
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
        internal QuerySearch(long dp,Context cx,Ident ic,Table tb, 
            Selection qn,Correlation cr, Grant.Privilege how) 
            : this(Type.DeleteWhere,dp,cx,ic,tb,qn,cr,how,BList<UpdateAssignment>.Empty)
            // detected for HttpService for DELETE verb
        { }
        protected QuerySearch(Type et, long dp, Context cx, Ident ic, Table tb,
            Selection qn, Correlation cr,
            Grant.Privilege how, BList<UpdateAssignment> ua = null)
            : this(et, dp, cx, 
                  new From(ic, cx, tb, null, qn, Grant.Privilege.Insert, cr),
                 tb, qn, cr,how)
        { }
        /// <summary>
        /// Constructor: a DELETE or UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The parsing context</param>
        protected QuerySearch(Type et,long dp,Context cx,From f,Table tb, 
            Selection qn, Correlation cr, 
            Grant.Privilege how, BList<UpdateAssignment> ua=null)
            : base(dp,BTree<long, object>.Empty + (SqlInsert._Table,f)
                  +(Depth,f.depth+1)+(_Type,et)+(From.Assigns,ua))
        {
            if (table.rowType.Length == 0)
                throw new DBException("2E111", cx.db.user, qn.name).Mix();
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
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QuerySearch)base._Replace(cx, so, sv);
            var tb = table._Replace(cx, so, sv);
            if (tb != table)
                r += (SqlInsert._Table, tb);
            cx.done += (defpos, r);
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
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (QuerySearch)base.Frame(cx);
            var tb = table.Frame(cx);
            if (tb != table)
                r += (SqlInsert._Table, tb);
            return cx.Add(r,true);
        }
        /// <summary>
        /// A readable version of the delete statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder("DELETE FROM ");
            if (table != null)
                sb.Append(Uid(table.defpos));
            table.CondString(sb, table.where, " where ");
            return sb.ToString();
        }
        public override Context Obey(Context cx)
        {
            return table.Delete(cx, BTree<string, bool>.Empty, new Adapters());
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
        public UpdateSearch(long dp, Context cx, Ident ic, Table tb,
            Selection qn, Correlation ca, Grant.Privilege how)
            : base(Type.UpdateWhere, dp, cx, ic, tb, qn, ca, how)
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
        public override Context Obey(Context cx)
        {
            return table.Update(cx, BTree<string, bool>.Empty, new Adapters(),
                new List<RowSet>());
        }
        internal override DBObject Relocate(long dp)
        {
            return new UpdateSearch(dp, mem);
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (UpdateSearch)base.Frame(cx);
            var tb = table.Frame(cx);
            if (tb != table)
                r += (SqlInsert._Table, tb);
            return cx.Add(r,true);
        }
    }
}