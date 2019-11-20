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
        internal DBObject target => (DBObject)mem[Target];
        internal readonly static From _static = new From();
        From() : base(Static, BTree<long,object>.Empty) { }
        public From(long dp, Table tb, ObInfo ti) : this(dp, BTree<long, object>.Empty
            + (Target, tb) + (Display,ti.Length) + (_Domain,Domain.TableType)
            + (Name, ti.name) +(RowType,ti))
        {
            if (dp >= 0 && dp < Transaction.TransPos)
                throw new PEException("PE000");
        }
        protected From(long defpos, BTree<long, object> m) : base(defpos, m)
        {
            if (defpos >= 0 && defpos < Transaction.TransPos)
                throw new PEException("PE000");
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
            var r = this;
            var ch = false;
            var so = r.source?.Replace(cx, was, now);
            if (so != r.source)
            {
                ch = true;
                r += (Source, so);
            }
            if (now is SqlCol sc && sc.tableCol.tabledefpos==target.defpos)
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
        internal ObInfo KeyType(Transaction tr)
        {
            return (target is Table tb && tb.FindPrimaryIndex(tr) is Index ix) ?
             (ObInfo)tr.role.obinfos[ix.defpos] : null;
        }
        internal override bool Uses(long t)
        {
            return target.defpos==t;
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
                    var v = (TypedValue)rec.fields[e.key().defpos];
                    var m = e.value();
                    if (v != null && m != null && m.dataType.Compare(m, v) != 0)
                        return false;
                }
            return true;
        }
        internal override RowSet RowSets(Transaction tr, Context cx)
        {
            if (defpos == Static)
                return new TrivialRowSet(tr, cx, this, new TRow(rowType, cx.values));
            RowSet rowSet = null;
            //           if (target == null)
            //               return new TrivialRowSet(tr, cx, this, Eval(tr, cx) as TRow ?? TRow.Empty);
            //         if (target is View vw)
            //             return vw.RowSets(tr, cx, this);

            // ReadConstraints only apply in explicit transactions 
            ReadConstraint readC = tr.autoCommit?null:tr._ReadConstraint(cx, target);
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
            if(target is Table ta)
            {
                int bs = 0;      // score for best index
                for (var p = ta.indexes.First(); p != null; p = p.Next())
                {
                    var x = (Index)tr.objects[p.value()];
                    if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey || x.tabledefpos != target.defpos)
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
                if (target is SystemTable st)
                    rowSet = new SystemRowSet(tr, cx, this);
                else if (target is Table tb)
                {
                    if (tb.tableRows != null)
                        rowSet = new TableRowSet(tr, cx, this);
                    else
                    {
                        index = tb.FindPrimaryIndex(tr);
                        if (index != null && index.rows != null)
                            rowSet = new IndexRowSet(tr, cx, this, index, null);
                    }
                }
                if (readC != null)
                    readC.Block();
            }
            cx.rdC += (target.defpos, readC);
            return rowSet;
        }
        internal override Transaction Insert(Transaction tr, Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs, Level cl)
        {
            return target.Insert(tr, _cx, this, prov, data, eqs, rs, cl);
        }
        internal override Transaction Delete(Transaction tr, Context cx, BTree<string, bool> dr, Adapters eqs)
        {
            return target.Delete(tr, cx, this, dr, eqs);
        }
        internal override Transaction Update(Transaction tr, Context cx, BTree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            return target.Update(tr, cx, this, ur, eqs, rs);
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
            if (target is Table tb)
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
            if (mem.Contains(Target)) { sb.Append(" Target="); sb.Append(Uid(target.defpos)); }
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
            ValuesDataType = -156; // Domain
        internal From from => (From)mem[_Table];
        /// <summary>
        /// Provenance information if supplied
        /// </summary>
        public string provenance => (string)mem[Provenance];
        internal Domain valuesDataType => (Domain)mem[ValuesDataType];
        /// <summary>
        /// Constructor: an INSERT statement from the parser.
        /// </summary>
        /// <param name="cx">The parsing context</param>
        /// <param name="name">The name of the table to insert into</param>
        public SqlInsert(Transaction tr,Context cx,Lexer lx, Ident name, Correlation cr, string prov)
            : base(lx,_Mem(lx.Position,tr,cx,name,cr)+ (Provenance, prov))
        {
            if (from.rowType.Length == 0)
                throw new DBException("2E111", tr.user, name.ident).Mix();
        }
        protected SqlInsert(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlInsert operator+(SqlInsert s,(long,object)x)
        {
            return new SqlInsert(s.defpos, s.mem + x);
        }
        static BTree<long,object> _Mem(long dp,Transaction tr,Context cx,Ident name,Correlation cr)
        {
            var tb = tr.GetObject(name.ident) as Table ??
                throw new DBException("42107", name.ident);
            var rt = (ObInfo)((ObInfo)tr.role.obinfos[tb.defpos]).Relocate(dp);
            var fm = new From(dp, tb, rt);
            if (cr!=null)
                fm += (Query.RowType,cr.Pick(rt.For(tr, tb, Grant.Privilege.Insert)));
            fm = (From)fm.AddCols(cx,fm);
            return BTree<long, object>.Empty + (_Table, fm) + (ValuesDataType, tb);
        }
        public override Transaction Obey(Transaction tr, Context cx)
        {
            Level cl = tr.user.clearance;
            var ta = from.target as Table;
            if (tr.user.defpos != tr.owner 
                && ta.enforcement.HasFlag(Grant.Privilege.Insert)
                && !cl.ClearanceAllows(from.classification))
                throw new DBException("42105");
            return from.Insert(tr,cx,provenance, cx.data, new Common.Adapters(),
                new List<RowSet>(), classification);
        }
    }
    /// <summary>
    /// QuerySearch is for DELETE and UPDATE 
    /// </summary>
    internal class QuerySearch : Executable
    {
        internal From table => (From)mem[SqlInsert._Table];
        internal QuerySearch(Lexer lx,Transaction tr, Context cx,Ident ic, Correlation cr, 
            Grant.Privilege how) : this(Type.DeleteWhere,lx,tr,cx,ic,cr,how,BList<UpdateAssignment>.Empty)
            // detected for HttpService for DELETE verb
        { }
        /// <summary>
        /// Constructor: a DELETE or UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The parsing context</param>
        protected QuerySearch(Type et,Lexer lx,Transaction tr, Context cx,Ident ic, Correlation cr, 
            Grant.Privilege how, BList<UpdateAssignment> ua=null)
            : base(lx,_Mem(lx.Position,tr,cx,ic,cr,ua)+(_Type,et)+(From.Assigns,ua))
        {
            if (table.rowType.Length == 0)
                throw new DBException("2E111", tr.user, ic.ident).Mix();
        }
        protected QuerySearch(long dp,BTree<long,object>m) :base(dp,m) { }
        public static QuerySearch operator+(QuerySearch q,(long,object)x)
        {
            return new QuerySearch(q.defpos, q.mem + x);
        }
        static BTree<long, object> _Mem(long dp,Transaction tr, Context cx, Ident name, Correlation cr, 
            BList<UpdateAssignment> ua)
        {
            var tb = tr.GetObject(name.ident) as Table ??
                throw new DBException("42107", name.ident);
            var rt = tr.role.obinfos[tb.defpos] as ObInfo;
            var dt = rt.For(tr, tb, Grant.Privilege.Insert);
            if (cr != null)
                dt = cr.Pick(dt);
            return BTree<long, object>.Empty + (SqlInsert._Table, new From(dp,tb,dt));
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
            return tr.Execute(this,cx);
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
        public UpdateSearch(Lexer lx,Transaction tr, Context cx, Ident ic, Correlation ca, 
            Grant.Privilege how)
            : base(Type.UpdateWhere, lx, tr, cx, ic, ca, how)
        {  }
        protected UpdateSearch(long dp, BTree<long, object> m) : base(dp, m) { }
        public static UpdateSearch operator+(UpdateSearch u,(long,object)x)
        {
            return new UpdateSearch(u.defpos, u.mem + x);
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
            return tr.Execute(this,cx); // we need this override even though it looks like the base's one!
        }
    }
}