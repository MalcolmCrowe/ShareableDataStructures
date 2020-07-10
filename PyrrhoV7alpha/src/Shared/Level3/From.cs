using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
using System;
using System.Configuration;
using System.Resources;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
    internal class From : Query
    {
        internal const long
            Assigns = -150, // BList<UpdateAssignment>
            Source = -151, // long (a Query for Views)
            Static = -152, // From (defpos for STATIC)
            Target = -153; // long (a table or view)
        internal BList<UpdateAssignment> assigns =>
            (BList<UpdateAssignment>)mem[Assigns] ?? BList<UpdateAssignment>.Empty;
        internal long source => (long)(mem[Source]??-1L);
        internal long target => (long)(mem[Target]??-1L);
        internal readonly static From _static = new From();
        From() : base(Static) { }
        public From(Ident ic, Context cx, Table tb, QuerySpecification q=null,
            Grant.Privilege pr=Grant.Privilege.Select, string a= null, BList<Ident> cr = null) 
            : base(ic.iix, _Mem(ic,cx, tb,q,pr,a,cr))
        { }
        protected From(Ident ic, Context cx, Table tb, BTree<long, object> mem)
            : base(ic.iix, mem + (_Mem(ic, cx, tb, null, Grant.Privilege.Select, null, null),false)) { }
        public From(long dp,Context cx,CallStatement pc,CList<long> cr=null)
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
        /// All columns in the From's rowtype will be SqlCopy. None will have table uids.
        /// We want to ensure that the From rowtype is different
        /// for each occurrence of a table in the query.
        /// If there are no stars in the select list, then we will find which columns
        /// are needed in the select list, and these will have unique uids.
        /// If there is a star in the select list that might be for this table,
        /// we append a list of all columns to the query and construct a rowType from
        /// that.
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="db"></param>
        /// <param name="tb"></param>
        /// <param name="q">The query with the select list</param>
        /// <param name="pr"></param>
        /// <param name="cr">Aliases supplied if any</param>
        /// <returns></returns>
        static BTree<long, object> _Mem(Ident ic, Context cx, Table tb, QuerySpecification q,
           Grant.Privilege pr = Grant.Privilege.Select, string a=null,BList<Ident> cr = null)
        {
            var cs = RowType.Empty;
            var vs = BList<SqlValue>.Empty;
            var de = 1; // we almost always have some columns
            var ti = tb.Struct(cx);
            cx._Add(tb);
            cx.AddCols(ic, ti);
            var mp = BTree<long, bool>.Empty;
            if (cr == null)
            {
                var ma = BTree<string, TableColumn>.Empty;
                for (var b = ti?.First(); b != null; b = b.Next())
                {
                    var p = b.value().Item1;
                    if (cx.obs[p] is SqlCopy sc)
                        p = sc.copyFrom;
                    var tc = (TableColumn)cx.db.objects[p];
                    var ci = (ObInfo)cx.role.infos[tc.defpos];
                    ma += (ci.name, tc);
                }
                // we want to add everything from ti that matches cx.stars or q.Needs
                if (q != null)
                {
                    var qn = q.Needs(cx, BTree<long,bool>.Empty);
                    for (var b = qn.First(); b != null; b = b.Next())
                    {
                        var p = b.key();
                        if (q != null && cx.obs[p] is SqlValue uv && uv.domain == Domain.Content)
                        {
                            var tc = ma[uv.name];
                            if (tc == null)
                                continue;
                            var nv = new SqlCopy(uv.defpos, cx, uv.name, ic.iix, tc.defpos);
                            if (uv.alias != null)
                                nv += (_Alias, uv.alias);
                            cx.Replace(uv, nv);
                            q = (QuerySpecification)cx.obs[q.defpos];
                            cs += (nv.defpos,nv.domain);
                            vs += nv;
                            mp += (tc.defpos, true);
                        }
                    }
                }
                for (var sb = cx.stars.First(); sb != null; sb = sb.Next())
                {
                    var (i, dp) = sb.value();
                    var rt = q.rowType ?? RowType.Empty;
                    if (tb.defpos == dp || dp < 0)
                        for (var b = ti?.First(); b != null; b = b.Next())
                        {
                            var (p,d) = b.value();
                            var ci = cx.NameFor(p);
                            var u = cx.GetUid();
                            cs += (u,d);
                            var sv = new SqlCopy(u, cx, ci, ic.iix, p);
                            cx.Add(sv);
                            q += (cx,sv);
                            rt += (u,d);
                            vs += sv;
                            mp += (p, true);
                        }
                    q = (QuerySpecification)cx.Add(q + (_RowType, rt));
                }
            }
            else
                for (var b = cr.First(); b != null; b = b.Next())
                {
                    var c = b.value();
                    var tc = (TableColumn)cx.obs[cx.defs[c].Item1]
                        ?? throw new DBException("42112", c.ident);
                    cs += (c.iix,tc.domain);
                    var sv = new SqlCopy(c.iix, cx, c.ident, ic.iix, tc.defpos);
                    cx.Add(sv);
                    vs += sv;
                    mp += (tc.defpos, true);
                }
            for (var b = ti?.First(); b != null; b = b.Next())
            {
                var (p,d) = b.value();
                if (cx.obs[p] is SqlCopy sc)
                    p = sc.copyFrom;
                if (mp.Contains(p))
                    continue;
                var ci = cx.NameFor(p);
                var u = cx.GetUid();
                cs += (u,d);
                var sv = new SqlCopy(u, cx, ci, ic.iix, p);
                cx.Add(sv);
                vs += sv;
            }
            var dm = new Domain(-1,Domain.TableType,vs);
            if (q!=null)
                cx.Add(q + (QuerySpecification.Scope, q.scope + dm.representation));
            return BTree<long, object>.Empty + (Name, ic.ident)
                   + (Target, tb.defpos) + (_Domain, dm) 
                   + (_RowType, cs) + (Depth, de + 1);
        }
        static BTree<long,object> _Mem(long dp,Context cx,CallStatement pc,CList<long> cr=null)
        {
            var proc = (Procedure)cx.db.objects[pc.procdefpos];
            cx._Add(proc);
            var disp = cr?.Length ?? proc.retType.Length;
            var s = RowType.Empty;
            for (var b = proc.rowType?.First(); b != null; b = b.Next())
            {
                var (p,dm) = b.value();
                var ci = cx.NameFor(p);
                s += (cx.Add(new SqlCopy(p,cx,ci,dp,b.key())).defpos,dm);
            }
            return BTree<long, object>.Empty
                + (Target,pc.procdefpos) + (Display,disp) + (_Domain,proc.domain)
                + (Name, proc.name) + (_RowType,s);
        }
        internal override TypedValue Eval(Context cx)
        {
            return cx.cursors[defpos];
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (From)base._Replace(cx,was,now);
            var ch = (r!=this);
            if (cx._Replace(r.source,was,now) is Query so && so.defpos != r.source)
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
        internal override Basis _Relocate(Writer wr)
        {
            var r = (From)base._Relocate(wr);
            var ua = BList<UpdateAssignment>.Empty;
            var ch = false;
            for (var b=assigns.First();b!=null;b=b.Next())
            {
                var a = (UpdateAssignment)b.value()._Relocate(wr);
                ua += a;
                if (a != b.value())
                    ch = true;
            }
            if (ch)
                r += (Assigns, ua);
            if (wr.cx.Fixed(source) is Query sc && sc.defpos != source)
                r += (Source, sc);
            var tg = wr.Fix(target);
            if (tg != target)
                r += (Target, tg);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (From)base._Relocate(cx);
            var ua = BList<UpdateAssignment>.Empty;
            var ch = false;
            for (var b = assigns.First(); b != null; b = b.Next())
            {
                var a = (UpdateAssignment)b.value()._Relocate(cx);
                ua += a;
                if (a != b.value())
                    ch = true;
            }
            if (ch)
                r += (Assigns, ua);
            if (cx.Fixed(source) is Query sc && sc.defpos != source)
                r += (Source, sc);
            var tg = cx.Unheap(target);
            if (tg != target)
                r += (Target, tg);
            return r;
        }
        internal override SqlValue ToSql(Ident id,Database db)
        {
            return new SqlTable(id.iix,this);
        }
        internal override bool Uses(Context cx,long t)
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
                    var v = rec.vals[e.key()];
                    var m = e.value();
                    if (v != null && m != null && m.dataType.Compare(m, v) != 0)
                        return false;
                }
            return true;
        }
        internal override RowSet RowSets(Context cx, BTree<long, RowSet.Finder> fi)
        {
            //        if (cx.data.Contains(defpos))
            //            return cx.data[defpos];
            if (defpos == Static)
                return new TrivialRowSet(defpos,cx,rowType, new TRow(rowType,domain, cx.values),-1,fi);
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
                     if (ps.op == Sqlx.NO)
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
            var ta = cx.obs[target] as Table;
            if(ta!=null)
            {
                if (cx.data[target] is RowSet tt)
                    return tt;
                int bs = 0;      // score for best index
                for (var p = ta.indexes.First(); p != null; p = p.Next())
                {
                    var x = (Index)tr.objects[p.value()];
                    if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey 
                        || x.tabledefpos != target)
                        continue;
                    var dt = (ObInfo)tr.role.infos[x.defpos];
                    int sc = 0;
                    int nm = 0;
                    int n = 0;
                    PRow pr = null;
                    var havematch = false;
                    int sb = 1;
                    var j = dt.Length - 1; 
                    for (var b=dt.rowType?.Last();b!=null;b=b.Previous(), j--)
                    {
                        var c = b.value().Item1;
                        for (var fd = filter.First(); fd != null; fd = fd.Next())
                        {
                            if (cx.obs[fd.key()] is SqlCopy co 
                                && co.copyFrom==c)
                            {
                                sc += 9 - j;
                                nm++;
                                pr = new PRow(fd.value(), pr);
                                havematch = true;
                                goto nextj;
                            }
                        }
                        if (n < ordSpec.items.Length)
                        {
                            var ok = ordSpec.items[n];
                            if (ok != -1L)
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
                rowSet = new SelectedRowSet(cx,this,new IndexRowSet(cx, ta, index, match,fi),fi);
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
                    rowSet = new SystemRowSet(cx, st);
                else if (cx.obs[target] is Table tb)
                {
                    index = tb.FindPrimaryIndex(cx.db);
                    if (index != null && index.rows != null)
                        rowSet = new SelectedRowSet(cx, this,
                            new IndexRowSet(cx, tb, index,null,fi),fi);
                    else
                        rowSet = new SelectedRowSet(cx, this,
                            new TableRowSet(cx, tb.defpos,fi),fi);
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
            var trs = new TableRowSet(cx,target,BTree<long,RowSet.Finder>.Empty);
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
                var ts = (Period)e[new Ident(ps.periodname,0,Sqlx.COLUMN)].Val();
                var dt = ts.start.dataType;
                var time1 = ps.time1.Eval(cx);
                var time2 = ps.time2.Eval(cx);
                switch (ps.op)
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
            _Table = -154, // long From
            Provenance = -155, //string
            Value = -156; // long SqlValue
        internal long target => (long)(mem[_Table]??-1L);
        /// <summary>
        /// Provenance information if supplied
        /// </summary>
        public string provenance => (string)mem[Provenance];
        public long value => (long)(mem[Value]??-1L);
        /// <summary>
        /// Constructor: an INSERT statement from the parser.
        /// </summary>
        /// <param name="cx">The parsing context</param>
        /// <param name="name">The name of the table to insert into</param>
        public SqlInsert(long dp,From fm,string prov, SqlValue v) 
           : base(dp,BTree<long,object>.Empty + (_Table,fm.defpos) + (Provenance, prov)+(Value,v.defpos))
        { }
        protected SqlInsert(long dp, BTree<long, object> m) : base(dp, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlInsert(defpos,m);
        }
        public static SqlInsert operator+(SqlInsert s,(long,object)x)
        {
            return new SqlInsert(s.defpos, s.mem + x);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = base._Replace(cx,so,sv);
            var fm = cx.Replace(target, so, sv);
            if (fm != target)
                r += (_Table, fm);
            cx.done += (defpos, r);
            return cx.Add(r);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlInsert(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r =  (SqlInsert)base._Relocate(wr);
            var tb = wr.Fixed(target).defpos;
            if (tb != target)
                r += (_Table, tb);
            var vl = wr.Fixed(value).defpos;
            if (vl != value)
                r += (Value, vl);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (SqlInsert)base._Relocate(cx);
            var tb = cx.Fixed(target).defpos;
            if (tb != target)
                r += (_Table, tb);
            var vl = cx.Fixed(value).defpos;
            if (vl != value)
                r += (Value, vl);
            return r;
        }
        public override Context Obey(Context cx)
        {
            var fm = (From)cx.obs[target];
            var r = ((SqlValue)cx.obs[value]).RowSet(target,cx,fm.domain.defpos,fm.rowType);
            Level cl = cx.db.user?.clearance??Level.D;
            var ta = cx.db.objects[fm.target] as Table;
            if (cx.db.user!=null && cx.db.user.defpos != cx.db.owner 
                && ta.enforcement.HasFlag(Grant.Privilege.Insert)
                && !cl.ClearanceAllows(fm.classification))
                throw new DBException("42105");
            return fm.Insert(cx,provenance, r, new Common.Adapters(),
                new List<RowSet>(), classification);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Table: "); sb.Append(Uid(target));
            sb.Append(" Value: "); sb.Append(Uid(value));
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
        internal long table => (long)(mem[SqlInsert._Table]??-1L);
        internal QuerySearch(long dp,Context cx,Ident ic,Table tb,Grant.Privilege how) 
            : this(Type.DeleteWhere,dp,cx,ic,tb,how)
            // detected for HttpService for DELETE verb
        { }
        protected QuerySearch(Type et, long dp, Context cx, Ident ic, Table tb,
            Grant.Privilege how, BList<Ident> cr = null,BList<UpdateAssignment> ua = null)
            : this(et, dp, cx, 
                  (From)cx.Add(new From(ic, cx, tb, null, Grant.Privilege.Insert,null,cr)),
                 tb, how)
        { }
        /// <summary>
        /// Constructor: a DELETE or UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The parsing context</param>
        protected QuerySearch(Type et,long dp,Context cx,From f,Table tb,
            Grant.Privilege how, BList<UpdateAssignment> ua=null)
            : base(dp,BTree<long, object>.Empty + (SqlInsert._Table,f.defpos)
                  +(Depth,f.depth+1)+(_Type,et)+(From.Assigns,ua))
        {
            if (f.rowType.Length == 0)
                throw new DBException("2E111", cx.db.user, dp).Mix();
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
            var tb = cx.Replace(r.table, so, sv);
            if (tb != r.table)
                r += (SqlInsert._Table, tb);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Relocate(long dp)
        {
            return new QuerySearch(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (QuerySearch)base._Relocate(wr);
            var tb = wr.Fixed(table).defpos;
            if (tb != table)
                r += (SqlInsert._Table, tb);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (QuerySearch)base._Relocate(cx);
            var tb = cx.Fixed(table).defpos;
            if (tb != table)
                r += (SqlInsert._Table, tb);
            return r;
        }
        /// <summary>
        /// A readable version of the delete statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (table != -1L)
                sb.Append(Uid(table));
            return sb.ToString();
        }
        public override Context Obey(Context cx)
        {
            return ((From)cx.obs[table]).Delete(cx, BTree<string, bool>.Empty, new Adapters());
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
            Grant.Privilege how)
            : base(Type.UpdateWhere, dp, cx, ic, tb, how)
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
            var sb = new StringBuilder(base.ToString());
            sb.Append(" ");sb.Append(Uid(table));
            return sb.ToString();
        }
        public override Context Obey(Context cx)
        {
            return ((From)cx.obs[table]).Update(cx, BTree<string, bool>.Empty, new Adapters(),
                new List<RowSet>());
        }
        internal override DBObject Relocate(long dp)
        {
            return new UpdateSearch(dp, mem);
        }
    }
}