using System;
using System.Collections.Generic;
using System.Diagnostics;
using Pyrrho.Common;
using System.IO;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
namespace Pyrrho.Level4
{
    /// <summary>
    /// A RowSet is the result of a stage of query processing. 
    /// We take responsibility for cx.values in the outs list
    /// IMMUTABLE
    /// </summary>
    internal abstract class RowSet : TypedValue
    {
        internal readonly long defpos;
        internal readonly ObInfo info, keyInfo;
        internal readonly OrderSpec rowOrder; 
        internal readonly BTree<long, SqlValue> where; 
        internal readonly BTree<SqlValue,TypedValue> matches;
        internal readonly BTree<long, BTree<long, bool>> matching;
        internal readonly GroupSpecification grouping;
        /// <summary>
        /// The info gives the list of SqlValues that we will place in the cursor.
        /// In a Context there may be several rowsets being calculated independently
        /// (e.g. a join of subqueries), and it can combine sources into a from association
        /// saying which rowset (and so which cursor) contains the current values.
        /// We could simply search along cx.data to pick the rowset we want.
        /// But it would be tighter if we could manage to combine the infos into a
        /// quick lookup tree. Such a finder is then a property of the RowSet.
        /// Before we compute a Cursor, we can place the source finder in the  Context, 
        /// so that the evaluation can use values retrieved from the correct cursors. 
        /// 
        /// RowSet uids are carefully managed:
        /// SystemRowSet uids match the relevant System Table (thus a negative uid)
        /// TableRowSet uids match the table defining position
        /// IndexRowSet uids match the index defpos
        /// EmptyRowSet uid is -1
        /// The following rowsets always have physical or lexical uids and
        /// rowtype information comes from the syntax:
        /// (physical can occur for queries in stored procedures and triggers):
        /// TrivialRowSet, SelectedRowSet, JoinRowSet, MergeRowSet, 
        /// SelectRowSet, EvakRowSet, GroupRowSet, TableExpRowSet, ExplicitRowSet,
        /// TransitionRowSet always is #0 (Transaction.Analysing)
        /// All other rowSets get their defining position by cx.nextPid++
        /// </summary>
        internal readonly BTree<long, long> finder; // what we provide
        internal readonly BTree<long, long> _finder; // what we consume
        internal virtual int display => info.Length; // after Build if any
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dp">The uid for the RowSet</param>
        /// <param name="rt">The way for a cursor to calculate a row</param>
        /// <param name="kt">The way for a cursor to calculate a key (by default same as rowType)</param>
        /// <param name="or">The ordering of rows in this rowset</param>
        /// <param name="wh">A set of boolean conditions on row values</param>
        /// <param name="ma">A filter for row column values</param>
        protected RowSet(long dp, Context cx, ObInfo inf,BTree<long,long> fin=null, ObInfo kt=null,
            BTree<long,SqlValue> wh=null,OrderSpec or=null,BTree<SqlValue,TypedValue>ma=null,
            BTree<long,BTree<long,bool>> mg=null,GroupSpecification gp=null) :base(inf.domain)
        {
            defpos = dp;
            info = inf;
            keyInfo = kt??info;
            rowOrder = or??OrderSpec.Empty;
            where = wh??BTree<long,SqlValue>.Empty;
            matches = ma??BTree<SqlValue,TypedValue>.Empty;
            matching = mg ?? BTree<long, BTree<long, bool>>.Empty;
            grouping = gp;
            fin = fin ?? BTree<long, long>.Empty;
            _finder = fin;
            for (var b = inf.columns.First(); b != null; b = b.Next())
                fin += (b.value().defpos, dp);
            finder = fin;
            cx.data += (dp, this);
            cx.val = this;
        }
        protected RowSet(long dp, Context cx, RowSet rs) 
            : this(dp, cx, rs, (ObInfo)rs.info.Relocate(dp), (ObInfo)rs.keyInfo.Relocate(dp)) { }
        RowSet(long dp,Context cx,RowSet rs,ObInfo inf,ObInfo keyInf) :base(inf.domain)
        {
            defpos = dp;
            info = inf;
            keyInfo = keyInf;
            rowOrder = rs.rowOrder;
            where = rs.where;
            matches = rs.matches;
            matching = rs.matching;
            grouping = rs.grouping;
            cx.data += (dp, this);
            cx.val = this;
        }
        protected RowSet(RowSet rs,long a,long b):base(rs.info.domain)
        {
            defpos = rs.defpos;
            info = rs.info;
            keyInfo = rs.keyInfo;
            rowOrder = rs.rowOrder;
            where = rs.where;
            matches = rs.matches;
            var m = rs.matching;
            var ma = m[a] ?? BTree<long, bool>.Empty;
            var mb = m[b] ?? BTree<long, bool>.Empty;
            matching = m + (a, ma + (b, true)) + (b, mb + (a, true));
            grouping = rs.grouping;
        }
        internal abstract RowSet New(long dp, Context cx);
        internal abstract RowSet New(long a, long b);
        public static RowSet operator+(RowSet rs,(long,long)x)
        {
            return rs.New(x.Item1, x.Item2);
        }
        internal virtual int? Count => null; // number of rows is usually unknown
        public override bool IsNull => false;
        internal override object Val()
        {
            return this;
        }
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        protected virtual object Build(Context cx)
        {
            if (PyrrhoStart.StrategyMode)
                Strategy(0);
            return null;
        }
        /// <summary>
        /// Relocation is possible only for RowSets that are not Built.
        /// Those that are will correctly throw a NotImplementedException
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal RowSet Relocate(long dp,Context cx)
        {
            return New(dp, cx);
        }
        internal virtual RowSet Source => this; 
        /// <summary>
        /// Test if the given source RowSet matches the requested ordering
        /// </summary>
        /// <returns>whether the orderings match</returns>
        protected bool SameOrder(Query q,RowSet sce)
        {
            if (rowOrder == null || rowOrder.items.Length == 0)
                return true;
            return rowOrder.SameAs(q,sce?.rowOrder);
        }
        /// <summary>
        /// Compute schema information (for the client) for this row set.
        /// 
        /// </summary>
        /// <param name="flags">The column flags to be filled in</param>
        internal void Schema(Context cx, int[] flags)
        {
            int m = info.Length;
            bool addFlags = true;
            var adds = new int[flags.Length];
            // see if we are going to add index flags stuff
            var j = 0;
            for (var ib = keyInfo.columns.First(); ib != null; ib = ib.Next(), j++)
            {
                var found = false;
                for (var b = info.columns.First(); b != null; b = b.Next())
                    if (b.value().defpos == ib.value().defpos)
                    {
                        adds[b.key()] = (j + 1) << 4;
                        found = true;
                        break;
                    }
                if (!found)
                    addFlags = false;
            }
            for (int i = 0; i < flags.Length; i++)
            {
                var dc = info[i].domain;
                if (dc.kind == Sqlx.SENSITIVE)
                    dc = dc.elType.domain;
                flags[i] = dc.Typecode() + (addFlags ? adds[i] : 0);
                if (cx.db.objects[info[i].defpos] is TableColumn tc)
                    flags[i] += ((tc.notNull) ? 0x100 : 0) +
                        ((tc.generated != GenerationRule.None) ? 0x200 : 0);
            }
        }
        TableColumn Col(SqlValue v)
        {
            if (v is SqlTableCol sc)
                return sc.tableCol;
            if (v is SqlValueExpr se && se.kind == Sqlx.DOT)
                return Col(se.right);
            return TableColumn.Doc;
        }
        /// <summary>
        /// Bookmarks are used to traverse rowsets
        /// </summary>
        /// <returns>a bookmark for the first row if any</returns>
        public abstract Cursor First(Context _cx);
        /// <summary>
        /// Find a bookmark for the given key
        /// </summary>
        /// <param name="key">a key</param>
        /// <returns>a bookmark or null if it is not there</returns>
        public virtual Cursor PositionAt(Context _cx,PRow key)
        {
            var dt = keyInfo ?? info;
            for (var bm = First(_cx); bm!=null; bm=bm.Next(_cx))
            {
                var k = key;
                for (int i = 0; i < dt.Length && k != null; i++, k = k._tail)
                    if (info[i].Eval(_cx).CompareTo(k._head) != 0)
                        goto next;
                return bm;
                next:;
            }
            return null;
        }
        internal virtual void _Strategy(StringBuilder sb,int indent)
        {
            Conds(sb, where, " where ");
            Matches(sb);
        }
        internal void Conds(StringBuilder sb,BTree<long,SqlValue> conds,string cm)
        {
            for (var b = conds.First(); b != null; b = b.Next())
            {
                sb.Append(cm);
                sb.Append(b.value());
                cm = " and ";
            }
        }
        internal void Matches(StringBuilder sb)
        {
            var cm = " match ";
            for (var b = matches.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.key()); sb.Append("=");
                sb.Append(b.value());
            }
        }
        internal void Strategy(int indent)
        {
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(' '); sb.Append(info);
            _Strategy(sb, 0);
            return sb.ToString();
        }
    }
    /// <summary>
    /// 1. Throughout the code RowSets are navigated using Cursors instead of
    /// Enumerators of any sort. The great advantage of bookmarks generally is
    /// that they are immutable. (E.g. the ABookmark classes always refer to a
    /// snapshot of the tree at the time of constructing the bookmark.)
    /// 2. The Rowset field is readonly but NOT immutable as it contains the Context _cx.
    /// _cx.values is updated to provide shortcuts to current values.
    /// 3. The TypedValues obtained by Value(..) are mostly immutable also, but
    /// updates to row columns (e.g. during trigger operations) are managed by 
    /// allowing the Cursor to override the usual results of 
    /// evaluating a row column: this is needed in (at least) the following 
    /// circumstances (a) UPDATE (b) INOUT and OUT parameters that are passed
    /// column references (c) triggers.
    /// 4. In view of the anxieties of 3. these overrides are explicit
    /// in the Cursor itself.
    /// </summary>
    internal abstract class Cursor : TRow
    {
        public readonly long _rowsetpos;
        public readonly int _pos;
        public readonly ObInfo _info;
        public readonly long _defpos;
        public Cursor(Context _cx,RowSet rs,int pos,long defpos,TRow rw)
            :base(_Row(rs.info,rw))
        {
            _rowsetpos = rs.defpos;
            _pos = pos;
            _defpos = defpos;
            _info = rs.info;
            _cx.cursors += (rs.defpos, this);
        }
        // a more detailed version for trigger-side transition cursors
        protected Cursor(Context cx,long rd,Selection sel,int pos,long defpos,TRow rw)
            :base(_Row(sel.info,rw))
        {
            _rowsetpos = rd;
            _pos = pos;
            _defpos = defpos;
            _info = sel.info;
            cx.cursors += (rd, this);
        }
        public Cursor(Context _cx, long rp, int pos, long defpos, ObInfo info, TRow rw)
    : base(_Row(info, rw))
        {
            _rowsetpos = rp;
            _pos = pos;
            _defpos = defpos;
            _info = info;
            _cx.cursors += (rp, this);
        }
        static TRow _Row(ObInfo oi, TRow rw)
        {
            if (oi.domain == rw.dataType)
                return rw;
            var vs = BTree<long, TypedValue>.Empty;
            for (var b = oi.columns.First(); b != null; b = b.Next())
            {
                var ci = b.value();
                vs += (ci.defpos, rw[b.key()]);
            }
            return new TRow(oi.domain, vs);
        }
        internal bool Matches(Context cx)
        {
            var rs = cx.data[_rowsetpos];
            for (var b = rs.matches.First(); b != null; b = b.Next())
                if (b.key().Eval(cx).CompareTo(b.value()) != 0)
                    return false;
            for (var b = rs.where.First(); b != null; b = b.Next())
                if (b.value().Eval(cx) != TBool.True)
                    return false;
            return true;
        }
        public abstract Cursor Next(Context _cx);
        /// <summary>
        /// These methods are for join processing
        /// </summary>
        /// <returns></returns>
        public virtual MTreeBookmark Mb()
        {
            return null; // throw new PEException("PE543");
        }
        public virtual Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
        {
            return null; // throw new PEException("PE544");
        }
        internal abstract TableRow Rec();
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        internal override object Val()
        {
            throw new NotImplementedException();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');sb.Append(DBObject.Uid(_rowsetpos));
            return sb.ToString();
        }
    }
    internal class TrivialRowSet: RowSet
    {
        internal readonly TRow row;
        internal readonly long rc = -1;
        internal readonly TrivialCursor here;
        internal TrivialRowSet(long dp,Context cx, TRow r=null, long rc = -1)  
            :base(dp, cx, ObInfo.For(r))
        {
            row = r??new TRow(ObInfo.Any);
            here = new TrivialCursor(cx,this,0,rc);
            cx.data+=(defpos,this);
        }
        internal TrivialRowSet(long dp, Context cx, ObInfo inf,TRow r = null, long rc = -1)
    : base(dp, cx, inf,null,null,null,null,null,cx.Copy(inf.domain,r?.dataType))
        {
            row = r ?? new TRow(ObInfo.Any);
            here = new TrivialCursor(cx, this, 0, rc);
            cx.data += (defpos, this);
        }
        internal TrivialRowSet(long dp,Context cx, ObInfo nf, Record rec) 
            : this(dp,cx, nf, new TRow(nf.domain,rec.fields), rec.defpos)
        { }
        protected TrivialRowSet(TrivialRowSet rs, long a, long b) : base(rs, a, b)
        {
            row = rs.row;
            rc = rs.rc;
            here = rs.here;
        }
        internal override RowSet New(long a, long b)
        {
            return new TrivialRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            return new TrivialRowSet(dp,cx,row,rc);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Trivial ");
            sb.Append(row.ToString());
            base._Strategy(sb, indent);
        }
        internal override int? Count => 1;

        public override bool IsNull => throw new NotImplementedException();

        public override Cursor First(Context _cx)
        {
            return here;
        }
        public override Cursor PositionAt(Context _cx,PRow key)
        {
            for (int i = 0; key != null; i++, key = key._tail)
                if (row[i].CompareTo(key._head) != 0)
                    return null;
            return here;
        }

        internal class TrivialCursor : Cursor
        {
            readonly TrivialRowSet trs;
            internal TrivialCursor(Context _cx, TrivialRowSet t,long d,long r) 
                :base(_cx,t,0,d,t.row)
            {
                trs = t;
            }
            public override Cursor Next(Context _cx)
            {
                return null;
            }
            internal override TableRow Rec()
            {
                return null;
            }
        }
    }
    /// <summary>
    /// A rowset consisting of selected Columns from another rowset (e.g. SELECT A,B FROM C).
    /// NB we assume that the rowtype contains only simple SqlCopy expressions
    /// </summary>
    internal class SelectedRowSet : RowSet
    {
        internal readonly Selection rowType;
        internal readonly RowSet rIn;
        /// <summary>
        /// This constructor builds a rowset for the given Table
        /// directly using its defpos, rowType, ordering, where and match info.
        /// Suggestion here is to use the source keyType. Maybe the source ordering too?
        /// </summary>
        internal SelectedRowSet(Context cx,Query q,RowSet r)
            :base(q.defpos, cx, q.rowType.info,r.finder,null,q.where,q.ordSpec,q.matches)
        {
            rIn = r;
            rowType = q.rowType;
            Build(cx);
        }
        protected SelectedRowSet(SelectedRowSet rs, long a, long b) : base(rs, a, b)
        {
            rIn = rs.rIn;
        }
        internal override RowSet New(long a, long b)
        {
            return new SelectedRowSet(this, a, b);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Selected ");
            sb.Append(DBObject.Uid(defpos));
            base._Strategy(sb, indent);
            rIn.Strategy(indent);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override RowSet Source => rIn;
        protected override object Build(Context cx)
        {
            for (var b = info.columns.First(); b != null; b = b.Next())
                b.value()?.Build(cx,this);
            return null;
        }
        public override Cursor First(Context _cx)
        {
            return SelectedCursor.New(_cx,this);
        }
        public override Cursor PositionAt(Context _cx, PRow key)
        {
            if (rIn is IndexRowSet irs)
                return new SelectedCursor(_cx, this, IndexRowSet.IndexCursor.New(_cx, irs, key),0);
            return base.PositionAt(_cx, key);
        }
        internal class SelectedCursor : Cursor
        {
            readonly SelectedRowSet _srs;
            internal readonly Cursor _bmk; // for rIn
            internal SelectedCursor(Context _cx,SelectedRowSet srs, Cursor bmk, int pos) 
                : base(_cx,srs, pos, bmk._defpos, _Row(srs,bmk)) 
            {
                _bmk = bmk;
                _srs = srs;
            }
            static TRow _Row(SelectedRowSet srs,TRow bmk)
            {
                var vs = BTree<long, TypedValue>.Empty;
                var rt = srs.rowType;
                for (var b = rt.First(); b != null; b = b.Next())
                    if (b.value() is SqlCopy sc)
                        vs += (sc.defpos, bmk[sc.copyFrom]);
                return new TRow(rt.info.domain, vs);
            }
            internal static SelectedCursor New(Context _cx,SelectedRowSet srs)
            {
                var ox = _cx.from;
                _cx.from = srs.finder; // just for SelectedRowSet
                for (var bmk = srs.rIn.First(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new SelectedCursor(_cx,srs, bmk, 0);
                    if (rb.Matches(_cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                var ox = _cx.from;
                _cx.from = _srs.finder; // just for SelectedRowSet
                for (var bmk = _bmk.Next(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new SelectedCursor(_cx,_srs, bmk, _pos + 1);
                    if (rb.Matches(_cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                return _bmk.Rec();
            }
        }
    }
    internal class SelectRowSet : RowSet
    {
        internal readonly RowSet rIn;
        internal readonly QuerySpecification qry;
        /// <summary>
        /// This constructor builds a rowset for the given QuerySpec
        /// directly using its defpos, rowType, ordering, where and match info.
        /// Note we cannot assume that columns are simple SqlCopy.
        /// Suggestion here is to use the source keyType. Maybe the source ordering too?
        /// </summary>
        internal SelectRowSet(Context cx, QuerySpecification q, RowSet r)
            : base(q.defpos, cx, q.rowType.info, r.finder, null, q.where, q.ordSpec, 
                  q.matches,Context.Copy(q.matching))
        {
            rIn = r;
            qry = q;
            Build(cx);
        }
        protected SelectRowSet(SelectRowSet rs, long a, long b) : base(rs, a, b)
        {
            rIn = rs.rIn;
        }
        internal override RowSet New(long a, long b)
        {
            return new SelectRowSet(this, a, b);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Select ");
            sb.Append(DBObject.Uid(defpos));
            base._Strategy(sb, indent);
            rIn.Strategy(indent);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override RowSet Source => rIn;
        protected override object Build(Context cx)
        {
            for (var b = info.columns.First(); b != null; b = b.Next())
                b.value()?.Build(cx, this);
            return null;
        }
        public override Cursor First(Context _cx)
        {
            return SelectCursor.New(_cx, this);
        }
        internal class SelectCursor : Cursor
        {
            readonly SelectRowSet _srs;
            readonly Cursor _bmk; // for rIn, not used directly for Eval
            SelectCursor(Context _cx, SelectRowSet srs, Cursor bmk, int pos)
                : base(_cx, srs, pos, bmk._defpos, _Row(_cx, bmk, srs))
            {
                _bmk = bmk;
                _srs = srs;
            }
            static TRow _Row(Context cx, Cursor bmk, SelectRowSet srs)
            {
                var ox = cx.from;
                cx.copy = srs.matching;
                cx.from = srs._finder;
                var vs = BList<TypedValue>.Empty;
                for (var b = srs.qry.rowType.First(); b != null; b = b.Next())
                {
                    var s = b.value(); 
                    var v = s?.Eval(cx)??TNull.Value;
                    if (v == TNull.Value && bmk[b.value().defpos] is TypedValue tv 
                        && tv != TNull.Value) 
                        // tv would be the right value but something has gone wrong
                        throw new PEException("PE788");
                    vs += v;
                }
                cx.from = ox;
                return new TRow(srs.info, vs);
            }
            internal static SelectCursor New(Context _cx, SelectRowSet srs)
            {
                for (var bmk = srs.rIn.First(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new SelectCursor(_cx, srs, bmk, 0);
                    if (rb.Matches(_cx))
                        return rb;
                }
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                for (var bmk = _bmk.Next(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new SelectCursor(_cx, _srs, bmk, _pos + 1);
                    for (var b = _srs.info.columns.First(); b != null; b = b.Next())
                        ((SqlValue)_cx.obs[b.value().defpos]).OnRow(rb);
                    if (rb.Matches(_cx))
                        return rb;
                }
                return null;
            }
            internal override TableRow Rec()
            {
                return _bmk.Rec();
            }
        }
    }

    internal class EvalRowSet : RowSet
    {
        /// <summary>
        /// The RowSet providing the data for aggregation. 
        /// </summary>
		internal readonly RowSet source;
        /// <summary>
        /// The having search condition for the aggregation
        /// </summary>
		internal readonly BTree<long,SqlValue> having;
        internal readonly TRow row;
        internal override RowSet Source => source;
        /// <summary>
        /// Constructor: Build a rowSet that aggregates data from a given source
        /// </summary>
        /// <param name="rs">The source data</param>
        /// <param name="h">The having condition</param>
		public EvalRowSet(Context cx,QuerySpecification q, RowSet rs, BTree<long,SqlValue> h) 
            : base(q.defpos, cx, q.rowType.info,rs.finder,null,FixWhere(rs),q.ordSpec,q.matches,
                  Context.Copy(q.matching))
        {
            source = rs;
            having = h;
            row = (TRow)Build(cx);
            cx.values += (defpos, row); // EvalRowSet has a single row
        }
        protected EvalRowSet(EvalRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            having = rs.having;
            row = rs.row;
        }
        internal override RowSet New(long a, long b)
        {
            return new EvalRowSet(this, a, b);
        }
        static BTree<long,SqlValue> FixWhere(RowSet rs)
        {
            bool gp = false;
            if (rs.grouping is GroupSpecification g)
                for (var gs = g.sets.First();gs!=null;gs=gs.Next())
                    gs.value().Grouped(rs.where, ref gp);
            return gp?rs.where:BTree<long, SqlValue>.Empty;
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Eval ");
            sb.Append(info);
            var cm = " having ";
            for (var b=having.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = " and ";
                sb.Append(b.value().ToString());
            }
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        public override Cursor First(Context _cx)
        {
            return new EvalBookmark(_cx,this);
        }

        protected override object Build(Context _cx)
        {
            var cx = new Context(_cx);
            cx.copy = matching;
            cx.from = _finder;
            var k = new TRow(info);
            for (var b = info.columns.First(); b != null; b = b.Next())
                ((SqlValue)_cx.obs[b.value().defpos]).AddReg(cx, defpos, k);
            cx.data += (defpos, this);
            for (int i = 0; i < info.Length; i++)
                ((SqlValue)_cx.obs[info[i].defpos]).StartCounter(cx,this);
            var ebm = source.First(cx);
            if (ebm != null)
            {
                for (; ebm != null; ebm = ebm.Next(cx))
                    if ((!ebm.IsNull) && Query.Eval(having,cx))
                        for (int i = 0; i < info.Length; i++)
                            ((SqlValue)_cx.obs[info[i].defpos]).AddIn(cx,ebm,null);
            }
            var cols = info;
            var vs = BList<TypedValue>.Empty;
            for (int i = 0; i < cols.Length; i++)
            {
                var s = cols[i];
                vs += s.Eval(cx);
            }
            return new TRow(info,vs);
        }
         internal class EvalBookmark : Cursor
        {
            readonly EvalRowSet _ers;
            internal EvalBookmark(Context _cx, EvalRowSet ers) 
                : base(_cx, ers, 0, 0, (TRow)_cx.values[ers.defpos])
            {
                _ers = ers;
            }
            public override Cursor Next(Context _cx)
            {
                return null; // just one row in the rowset
            }
            internal override TableRow Rec()
            {
                return null;
            }
        }
    }

    /// <summary>
    /// A TableRowSet consists of all of the *accessible* TableColumns from a Table
    /// </summary>
    internal class TableRowSet : RowSet
    {
        internal readonly long tabledefpos;
        internal readonly int? count;
        /// <summary>
        /// Constructor: a rowset defined by a base table without a primary key.
        /// Independent of role, user, command.
        /// Context must have a suitable tr field
        /// </summary>
        internal TableRowSet(Context cx, long t)
            : base(t, cx, (ObInfo)cx.db.schemaRole.obinfos[t])
        {
            tabledefpos = t;
            count = (int?)(cx.db.objects[t] as Table)?.tableRows.Count;
        }
        TableRowSet(long dp,Context cx,TableRowSet trs):base(dp,cx,trs)
        {
            tabledefpos = trs.tabledefpos;
            count = trs.count;
        }
        protected TableRowSet(TableRowSet rs, long a, long b) : base(rs, a, b)
        {
            tabledefpos = rs.tabledefpos;
            count = rs.count;
        }
        internal override RowSet New(long a, long b)
        {
            return new TableRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            return new TableRowSet(dp, cx, this);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("TableRowSet ");
            sb.Append(DBObject.Uid(tabledefpos));
            base._Strategy(sb, indent);
        }
        internal override int? Count => count;
        public override Cursor First(Context _cx)
        {
            return TableCursor.New(_cx,this);
        }
        internal class TableCursor : Cursor
        {
            internal readonly Table _table;
            internal readonly TableRowSet _trs;
            internal readonly ABookmark<long, TableRow> _bmk;
            protected TableCursor(Context _cx, TableRowSet trs, Table tb, int pos, ObInfo inf,
                ABookmark<long, TableRow> bmk) 
                : base(_cx,trs.defpos, pos, bmk.key(), inf, _Row(inf,bmk.value()))
            {
                _bmk = bmk; _table = tb; _trs = trs;
            }
            static TRow _Row(ObInfo inf,TableRow rw)
            {
                var vs = BTree<long, TypedValue>.Empty;
                for (var b=inf.columns.First();b!=null;b=b.Next())
                {
                    var p = b.value().defpos;
                    vs += (p, rw.vals[p]);
                }
                return new TRow(inf.domain, vs);
            }
            internal static TableCursor New(Context _cx, TableRowSet trs)
            {
                var table = _cx.db.objects[trs.tabledefpos] as Table;
                for (var b = table.tableRows.First(); b != null; b = b.Next())
                {
                    var rec = b.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user != null && _cx.db.user.defpos != table.definer
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new TableCursor(_cx, trs, table, 0, trs.info, b);
                }
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                var bmk = _bmk;
                var rec = (TableRow)_bmk.value();
                var table = _table;
                for (;;) 
                {
                    if (bmk != null)
                        bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) && 
                        _cx.db.user.defpos!=table.definer 
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new TableCursor(_cx,_trs,_table, _pos + 1, _info, bmk);
                }
            }
            internal override TableRow Rec()
            {
                return _bmk.value();
            }
        }
    }
    internal class OldTableRowSet : TableRowSet
    {
        internal new readonly int count;
        internal OldTableRowSet(FromOldTable f, long t, Context cx)
            : base(cx, t) 
        {
            var ta = cx.FindTriggerActivation(f.target);
            count = (int)ta.oldRows.Count;
        }
        internal override int? Count => count;
        public override Cursor First(Context _cx)
        {
            return OldTableCursor.New(_cx, this);
        }
        internal class OldTableCursor : TableCursor
        {
            OldTableCursor(Context _cx,TableRowSet ors,Table tb,int pos,ObInfo inf,
                ABookmark<long,TableRow> bmk) :base(_cx,ors,tb,pos,inf,bmk)
            { }
            internal new static TableCursor New(Context _cx, TableRowSet trs)
            {
                var table = _cx.db.objects[trs.tabledefpos] as Table;
                var ta = _cx.FindTriggerActivation(table.defpos);
                for (var b = ta.oldRows.First(); b != null; b = b.Next())
                {
                    var rec = b.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user.defpos != table.definer
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new OldTableCursor(_cx, trs,table, 0, trs.info, b);
                }
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                var bmk = _bmk;
                var rec = _bmk.value();
                var table = _table;
                for (; ; )
                {
                    if (bmk != null)
                        bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) &&
                        _cx.db.user.defpos != table.definer
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new OldTableCursor(_cx,_trs, _table, _pos + 1, _info, bmk);
                }
            }
        }
    }
    /// <summary>
    /// A RowSet defined by an Index (e.g. the primary key for a table)
    /// </summary>
    internal class IndexRowSet : RowSet
    {
        internal readonly Table table;
        /// <summary>
        /// The Index to use
        /// </summary>
        internal readonly Index index;
        internal readonly PRow filter;
        /// <summary>
        /// Constructor: A rowset for a table using a given index. 
        /// Independent of role, user, command.
        /// Context must have a suitable tr field.
        /// </summary>
        /// <param name="f">the from part</param>
        /// <param name="x">the index</param>
        internal IndexRowSet(Context cx, Table tb, Index x, PRow filt=null) 
            : base(x.defpos,cx,_Info(x.defpos,cx,tb))
        {
            table = tb;
            index = x;
            filter = filt;
        }
        IndexRowSet(Context cx,IndexRowSet irs) :base(irs.defpos,cx,irs.info)
        {
            table = irs.table;
            index = irs.index;
            filter = irs.filter;
        }
        protected IndexRowSet(IndexRowSet irs, long a, long b) : base(irs, a, b)
        {
            table = irs.table;
            index = irs.index;
            filter = irs.filter;
        }
        static ObInfo _Info(long dp,Context cx,Table tb)
        {
            var oi = (ObInfo)cx.db.schemaRole.obinfos[tb.defpos];
            return (ObInfo)oi.Relocate(dp);
        }
        internal override RowSet New(long a, long b)
        {
            return new IndexRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            return new IndexRowSet(cx,this);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("IndexRowSet ");
            sb.Append(table);
            base._Strategy(sb, indent);
        }
        internal override int? Count => (int?)index.rows.Count;
        public override Cursor First(Context _cx)
        {
            return IndexCursor.New(_cx,this);
        }
        /// <summary>
        /// We assume the key matches our key type, and that the filter is null
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public override Cursor PositionAt(Context _cx,PRow key)
        {
            return IndexCursor.New(_cx,this, key);
        }
        internal class IndexCursor : Cursor
        {
            internal readonly Table _table;
            internal readonly MTreeBookmark _bmk;
            internal readonly TableRow _rec;
            internal readonly PRow _key;
            IndexCursor(Context _cx, Table tb, int pos, ObInfo inf, MTreeBookmark bmk,PRow key=null)
                : this(_cx, tb, pos, bmk, inf, tb.tableRows[bmk.Value().Value],key) { }
            IndexCursor(Context _cx, Table tb, int pos, MTreeBookmark bmk, ObInfo inf, TableRow trw,
                PRow key)
                : base(_cx, tb.defpos, pos, trw.defpos, inf, _Row(inf,trw))
            {
                _bmk = bmk; _table = tb; _rec = trw; _key = key;
            }
            static TRow _Row(ObInfo inf, TableRow rw)
            {
                var vs = BTree<long, TypedValue>.Empty;
                for (var b = inf.columns.First(); b != null; b = b.Next())
                {
                    var p = b.value().defpos;
                    vs += (p, rw.vals[p]);
                }
                return new TRow(inf.domain, vs);
            }
            public override MTreeBookmark Mb()
            {
                return _bmk;
            }
            internal override TableRow Rec()
            {
                return _rec;
            }
            public override Cursor ResetToTiesStart(Context _cx, MTreeBookmark mb)
            {
                return new IndexCursor(_cx,_table, _pos + 1, _info, mb);
            }
            internal static IndexCursor New(Context _cx,IndexRowSet irs, PRow key = null)
            {
                var table = irs.table;
                for (var bmk = irs.index.rows.PositionAt(key ?? irs.filter); bmk != null;
                    bmk = bmk.Next())
                {
                    var iq = bmk.Value();
                    if (!iq.HasValue)
                        continue;
                    var rec = table.tableRows[iq.Value];
                    if (rec == null || (table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db.user.defpos != table.definer
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification)))
                        continue;
                    return new IndexCursor(_cx,table, 0, irs.info, bmk, key??irs.filter);
                }
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                var bmk = _bmk;
                for (; ; )
                {
                    bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (!bmk.Value().HasValue)
                        continue;
                    var rec = _table.tableRows[bmk.Value().Value];
                    if (_table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _cx.db.user.defpos != _table.definer
                        && !_cx.db.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    return new IndexCursor(_cx, _table, _pos + 1, _info, bmk);
                }
            }
        }
    }

    /// <summary>
    /// A rowset for distinct values
    /// </summary>
    internal class DistinctRowSet : RowSet
    {
        readonly MTree rtree;
        internal RowSet source;
        /// <summary>
        /// constructor: a distinct rowset
        /// </summary>
        /// <param name="r">a source rowset</param>
        internal DistinctRowSet(Context _cx,RowSet r) 
            : base(_cx.nextHeap++,_cx,r.info,r.finder,r.keyInfo,r.where)
        {
            source = r;
            rtree = (MTree)Build(_cx);
        }
        protected DistinctRowSet(DistinctRowSet rs,long a,long b):base(rs,a,b)
        {
            source = rs.source;
            rtree = rs.rtree;
        }
        internal override RowSet New(long a, long b)
        {
            return new DistinctRowSet(this,a,b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException(); // Relocate before build
        }

        internal override RowSet Source => source;
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Distinct");
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }

        protected override object Build(Context cx)
        {
            var rtree = new MTree(new TreeInfo(source.keyInfo, TreeBehaviour.Allow, TreeBehaviour.Allow));
            var vs = BList<TypedValue>.Empty;
            for (var a = source.First(cx); a != null; a = a.Next(cx))
            {
                for (var ti = rtree.info; ti != null; ti = ti.tail)
                    vs+= a[ti.head];
                MTree.Add(ref rtree, new PRow(vs), 0);
            }
            return rtree;
        }

        public override Cursor First(Context _cx)
        {
            return DistinctCursor.New(_cx,this);
        }

        internal class DistinctCursor : Cursor
        {
            readonly DistinctRowSet _drs;
            readonly MTreeBookmark _bmk;
            DistinctCursor(Context cx,DistinctRowSet drs,int pos,MTreeBookmark bmk) 
                :base(cx,drs,pos,0,new TRow(drs.keyInfo,bmk.key()))
            {
                _bmk = bmk;
                _drs = drs;
            }
            internal static DistinctCursor New(Context _cx,DistinctRowSet drs)
            {
                var ox = _cx.from;
                _cx.from = drs._finder;
                for (var bmk = drs.rtree.First(); bmk != null; bmk = bmk.Next() as MTreeBookmark)
                {
                    var rb = new DistinctCursor(_cx,drs,0, bmk);
                    if (rb.Matches(_cx) && Query.Eval(drs.where, _cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                var ox = _cx.from;
                _cx.from = _drs._finder;
                for (var bmk = _bmk.Next() as MTreeBookmark; bmk != null; 
                    bmk = bmk.Next() as MTreeBookmark)
                {
                    var rb = new DistinctCursor(_cx, _drs,_pos + 1, bmk);
                    if (rb.Matches(_cx) && Query.Eval(_drs.where, _cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    internal class OrderedRowSet : RowSet
    {
        internal readonly RTree tree;
        internal readonly RowSet source;
        internal readonly OrderSpec ordSpec;
        internal readonly bool distinct;
        internal override RowSet Source => source;
        public OrderedRowSet(Context _cx,RowSet r,OrderSpec os,bool dct)
            :base(_cx.nextHeap++, _cx, r.info,r.finder,os.info,r.where,os,r.matches)
        {
            source = r;
            distinct = dct;
            ordSpec = os;
            tree = (RTree)Build(_cx);
        }
        protected OrderedRowSet(OrderedRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            distinct = rs.distinct;
            ordSpec = rs.ordSpec;
            tree = rs.tree;
        }
        internal override RowSet New(long a, long b)
        {
            return new OrderedRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Ordered ");
            if (distinct)
                sb.Append("distinct ");
            sb.Append(keyInfo);
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        internal override int? Count => (int)tree.rows.Count;
        protected override object Build(Context cx)
        {
            var _cx = new Context(cx);
            _cx.from = _finder; 
            var tree = new RTree(defpos,keyInfo, distinct ? TreeBehaviour.Ignore : TreeBehaviour.Allow, 
                TreeBehaviour.Allow);
            for (var e = source.First(_cx); e != null; e = e.Next(_cx))
            {
                var vs = BList<TypedValue>.Empty;
                for (var b = rowOrder.items.First(); b != null; b = b.Next())
                {
                    var s = b.value();
                    vs += s.Eval(_cx);
                }
                RTree.Add(ref tree, new TRow(rowOrder.info, vs), e);
            }
            return tree;
        }
        public override Cursor First(Context _cx)
        {
            return OrderedCursor.New(_cx, this, RTreeBookmark.New(_cx, tree, info));
        }
        internal class OrderedCursor : Cursor
        {
            readonly OrderedRowSet _ors;
            readonly Cursor _rb;
            internal OrderedCursor(Context cx,OrderedRowSet ors,Cursor rb)
                :base(cx,ors,rb._pos,rb._defpos,rb)
            {
                cx.cursors += (ors.defpos, this);
                _ors = ors; _rb = rb;
            }
            internal static OrderedCursor New(Context cx,OrderedRowSet ors,Cursor rb)
            {
                if (rb == null)
                    return null;
                return new OrderedCursor(cx, ors, rb);
            }
            public override Cursor Next(Context _cx)
            {
                return New(_cx, _ors, _rb.Next(_cx));
            }
            internal override TableRow Rec()
            {
                return _rb.Rec();
            }
        }
    }
    internal class EmptyRowSet : RowSet
    {
        public static readonly EmptyRowSet Value = new EmptyRowSet();
        EmptyRowSet() : base(-1,null,null) { }
        internal EmptyRowSet(long dp, Context cx) : base(dp, cx, Value) { }
        protected EmptyRowSet(EmptyRowSet rs, long a, long b) : base(rs, a, b)
        { }
        internal override RowSet New(long a, long b)
        {
            return new EmptyRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            return new EmptyRowSet(dp, cx);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Empty");
            base._Strategy(sb, indent);
        }
        internal override int? Count => 0;
        public override Cursor First(Context _cx)
        {
            return null;
        }
    }
    /// <summary>
    /// A rowset for SqlRows
    /// </summary>
    internal class SqlRowSet : RowSet
    {
        internal readonly BList<SqlRow> rows;
        internal SqlRowSet(Context cx,ObInfo inf, BList<SqlRow> rs) 
            : base(cx.nextHeap++, cx, inf)
        {
            rows = rs;
        }
        protected SqlRowSet(SqlRowSet rs, long a, long b) : base(rs, a, b)
        {
            rows = rs.rows;
        }
        internal override RowSet New(long a, long b)
        {
            return new SqlRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("SqlRows ");
            sb.Append(rows.Length);
            base._Strategy(sb, indent);
        }
        public override Cursor First(Context _cx)
        {
            return SqlCursor.New(_cx,this);
        }
        internal class SqlCursor : Cursor
        {
            readonly SqlRowSet _srs;
            SqlCursor(Context _cx,SqlRowSet rs,int pos)
                : base(_cx,rs,pos,0,(TRow)rs.info.Eval(_cx))
            {
                _srs = rs;
            }
            internal static SqlCursor New(Context _cx,SqlRowSet rs)
            {
                for (var i = 0; i < rs.rows.Length; i++)
                {
                    var rb = new SqlCursor(_cx,rs, i);
                    if (rb.Matches(_cx) && Query.Eval(rs.where, _cx))
                        return rb;
                }
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                for (var i = _pos + 1; i < _srs.rows.Length; i++)
                {
                    var rb = new SqlCursor(_cx,_srs, i);
                    if (rb.Matches(_cx) && Query.Eval(_srs.where, _cx))
                        return rb;
                }
                return null;
            }
            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    internal class TableExpRowSet : RowSet
    {
        internal readonly RowSet source;
        internal readonly Selection needed;
        internal TableExpRowSet(long dp, Context cx, Selection ne,RowSet sc, BTree<long, SqlValue> wh, 
            BTree<SqlValue, TypedValue> ma)
            : base(dp, cx, sc.info, sc.finder, sc.keyInfo, _Where(sc, wh), sc.rowOrder, _Matches(sc, ma)) 
        {
            source = sc;
            needed = ne;
        }
        protected TableExpRowSet(TableExpRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            needed = rs.needed;
        }
        internal override RowSet New(long a, long b)
        {
            return new TableExpRowSet(this, a, b);
        }
        static BTree<long, SqlValue> _Where(RowSet sc, BTree<long, SqlValue> wh)
        {
            var r = sc.where;
            for (var b = wh?.First(); b != null; b = b.Next())
            {
                var p = b.key();
                if (!sc.where.Contains(p))
                    r += (p, b.value());
            }
            return r;
        }
        static BTree<SqlValue, TypedValue> _Matches(RowSet sc, BTree<SqlValue, TypedValue> ma)
        {
            var r = BTree<SqlValue, TypedValue>.Empty;
            for (var b = ma?.First(); b != null; b = b.Next())
            {
                var s = b.key();
                if (!sc.matches.Contains(s))
                    r += (s, b.value());
            }
            return r;
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        public override Cursor First(Context _cx)
        {
            return TableExpCursor.New(_cx,this);
        }
        internal class TableExpCursor : Cursor
        {
            internal readonly TableExpRowSet _trs;
            internal readonly Cursor _bmk;
            TableExpCursor(Context cx,TableExpRowSet trs,Cursor bmk,int pos) 
                :base(cx,trs,pos,bmk._defpos,bmk)
            {
                _trs = trs;
                _bmk = bmk;
            }
            public static TableExpCursor New(Context cx,TableExpRowSet trs)
            {
                var ox = cx.from;
                cx.from = trs._finder;
                for (var bmk=trs.source.First(cx);bmk!=null;bmk=bmk.Next(cx))
                {
                    var rb = new TableExpCursor(cx, trs, bmk, 0);
                    if (rb.Matches(cx) && Query.Eval(trs.where, cx))
                    {
                        cx.from = ox;
                        return rb;
                    }
                }
                cx.from = ox;
                return null;
            }
            public override Cursor Next(Context cx)
            {
                var ox = cx.from;
                cx.from = _trs._finder;
                for (var bmk = _bmk.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new TableExpCursor(cx, _trs, bmk, _pos+1);
                    if (rb.Matches(cx) && Query.Eval(_trs.where, cx))
                    {
                        cx.from = ox;
                        return rb;
                    }
                }
                cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                return _bmk.Rec();
            }
        }
    }
    /// <summary>
    /// a row set for TRows
    /// </summary>
    internal class ExplicitRowSet : RowSet
    {
        internal readonly BList<(long,TRow)> rows;
        /// <summary>
        /// constructor: a set of explicit rows
        /// </summary>
        /// <param name="rt">a row type</param>
        /// <param name="r">a a set of TRows from q</param>
        internal ExplicitRowSet(long dp,Context cx,ObInfo rt,BList<(long,TRow)>r)
            : base(dp,cx,rt)
        {
            rows = r;
        }
        internal ExplicitRowSet(long dp,Context cx,RowSet sce,ObInfo oi)
            : base(dp, cx, _Info(dp,cx,oi), sce.finder)
        { }
        protected ExplicitRowSet(ExplicitRowSet rs, long a, long b) : base(rs, a, b)
        {
            rows = rs.rows;
        }
        internal override RowSet New(long a, long b)
        {
            return new ExplicitRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            return new ExplicitRowSet(dp,cx,(ObInfo)info.Relocate(dp),rows);
        }
        static ObInfo _Info(long dp,Context cx,ObInfo oi)
        {
            var r = ObInfo.Any;
            for (var b = oi.columns.First(); b != null; b = b.Next())
                r += new SqlRowSetCol(b.value(), dp);
            return r;
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Explicit");
            base._Strategy(sb, indent);
            if (Count < 10)
                for (var r =rows.First();r!=null;r=r.Next())
                {
                    var s = new StringBuilder();
                    for (var i = 0; i < indent; i++)
                        s.Append(' ');
                    s.Append(r.value());
                }
            else
                sb.Append("" + Count + " rows");
        }
        internal override int? Count => (int)rows.Count;
        public override Cursor First(Context _cx)
        {
            return ExplicitCursor.New(_cx,this,0);
        }

        internal class ExplicitCursor : Cursor
        {
            readonly ExplicitRowSet _ers;
            readonly ABookmark<int,(long,TRow)> _prb;
            ExplicitCursor(Context _cx, ExplicitRowSet ers,ABookmark<int,(long,TRow)>prb,int pos) 
                :base(_cx,ers,pos,prb.value().Item1,prb.value().Item2)
            {
                _ers = ers;
                _prb = prb;
            }
            static BTree<long,TypedValue> _Vals(ExplicitRowSet ers,ABookmark<int,(long,TRow)>prb)
            {
                var rw = prb.value().Item2;
                var vs = BTree<long, TypedValue>.Empty;
                var i = 0;
                for (var b = ers.info.columns.First(); b != null; b = b.Next(), i++)
                    vs += (b.value().defpos, rw[i]);
                return vs;
            }
            internal static ExplicitCursor New(Context _cx,ExplicitRowSet ers,int i)
            {
                var ox = _cx.from;
                _cx.from = ers._finder;
                for (var b=ers.rows.First();b!=null;b=b.Next())
                {
                    var rb = new ExplicitCursor(_cx,ers, b, 0);
                    if (rb.Matches(_cx) && Query.Eval(ers.where, _cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                var ox = _cx.from;
                _cx.from = _ers._finder;
                for (var prb = _prb.Next(); prb!=null; prb=prb.Next())
                {
                    var rb = new ExplicitCursor(_cx,_ers, prb, _pos+1);
                    if (rb.Matches(_cx) && Query.Eval(_ers.where, _cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                return null;
            }
        }
    }
    /// <summary>
    /// Deal with execution of Triggers. The main complication here is that triggers execute in definer's role
    /// so the column names and data types will be different for different triggers.
    /// (Of course many trigger actions will be to different tables.)
    /// Another nuisance is that we need to manage our own copies of autokeys, as we may be preparing
    /// many new rows for a table.
    /// </summary>
    internal class TransitionRowSet : RowSet
    {
        internal readonly BTree<long, TypedValue> defaults = BTree<long, TypedValue>.Empty; 
        internal readonly From from; // will be a SqlInsert, QuerySearch or UpdateSearch
        internal readonly ObInfo targetInfo; 
        internal readonly long indexdefpos = -1L;
        internal readonly PTrigger.TrigType _tgt;
        internal readonly BTree<long, TriggerActivation> tb, ti, ta, td;
        internal readonly Transaction oldTr;
        internal readonly Adapters _eqs;
        internal TransitionRowSet(Context cx,From q, PTrigger.TrigType tg, Adapters eqs)
            : base(Transaction.Analysing,cx, (ObInfo)cx.db.role.obinfos[q.target], _Finder(cx,q), null,q.where)
        {
            from = q;
            _eqs = eqs;
            oldTr = cx.tr;
            var tr = cx.db;
            var t = tr.objects[from.target] as Table;
            indexdefpos = t.FindPrimaryIndex(tr)?.defpos ?? -1L;
            // check now about conflict with generated columns
            if (t.Denied(cx,Grant.Privilege.Insert))
                throw new DBException("42105",q);
            var dt = q.rowType; // data rowType
            targetInfo = tr.schemaRole.obinfos[t.defpos] as ObInfo; 
            if (tg != PTrigger.TrigType.Delete)
            {
                for (int i = 0; i < dt.Length; i++) // at this point q is the insert statement, simpleQuery is the base table
                {
                    var s = dt[i];
                    var c = (s is SqlCopy sc) ? sc.copyFrom : s.defpos;
                    if (tr.objects[c] is TableColumn tc && tc.generated != GenerationRule.None)
                            throw new DBException("0U000", dt[i].name).Mix();
                }
                for (int i = 0; i < targetInfo.Length; i++)
                    if (targetInfo.columns[i] is ObInfo oc)
                    {
                        cx.from += (oc.defpos, defpos);
                        var tc = (TableColumn)tr.objects[oc.defpos];
                        var tv = tc.defaultValue ?? tc.domain.defaultValue;
                        if (tv != TNull.Value)
                        {
                            defaults += (tc.defpos, tv);
                            cx.values += (tc.defpos, tv);
                        }
                    }
            }
            else 
                targetInfo = ObInfo.Any;
            _tgt = tg;
            tb = Setup(cx, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.Before]);
            ti = Setup(cx, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.Instead]);
            ta = Setup(cx, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.After]);
            td = Setup(cx, t.triggers[_tgt | PTrigger.TrigType.Deferred]);
        }
        static BTree<long,long> _Finder(Context cx,Query q)
        {
            var r = BTree<long, long>.Empty;
            var rp = cx.nextHeap - 1;
            for (var b = cx.data.First(); b != null; b = b.Next())
                for (var c = b.value()?.finder?.First(); c != null; c = c.Next())
                    r += (c.key(), c.value());
            for (var b = q.rowType.First(); b != null; b = b.Next())
                r += (b.value().defpos, q.defpos);
            return r;
        }
        protected TransitionRowSet(TransitionRowSet rs, long a, long b) : base(rs, a, b)
        {
            defaults = rs.defaults;
            from = rs.from;
            targetInfo = rs.targetInfo;
            indexdefpos = rs.indexdefpos;
            _tgt = rs._tgt;
            tb = rs.tb;
            ti = rs.ti;
            ta = rs.ta;
            td = rs.td;
            oldTr = rs.oldTr;
            _eqs = rs._eqs;
        }
        internal override RowSet New(long a, long b)
        {
            return new TransitionRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        public override Cursor First(Context _cx)
        {
            return TransitionCursor.New(_cx,this);
        }
        /// <summary>
        /// Implement the autokey feature: if a key column is an integer type,
        /// the engine will pick a suitable unused value. 
        /// The works cleverly for multi-column indexes. 
        /// The transition rowset adds to a private copy of the index as there may
        /// be several rows to add, and the null column(s!) might not be the first key column.
        /// </summary>
        /// <param name="fl"></param>
        /// <param name="ix"></param>
        /// <returns></returns>
        void CheckPrimaryKey(Context cx)
        {
            var ix = (Index)cx.db.objects[indexdefpos];
            if (ix == null)
                return;
            var k = BList<TypedValue>.Empty;
            for (var i = 0; i < (int)ix.keys.Count; i++)
            {
                var sc = ix.keys[i];
                var v = cx.values[sc.defpos];
                if (v == null || v == TNull.Value)
                {
                    if (sc.domain.kind != Sqlx.INTEGER)
                        throw new DBException("22004");
                    v = ix.rows.NextKey(k, 0, i);
                    if (v == TNull.Value)
                        v = new TInt(0);
                    cx.values += (sc.defpos, v);
                }
                k += v;
            }
        }
        /// <summary>
        /// Set up activations for executing a set of triggers
        /// </summary>
        /// <param name="q"></param>
        /// <param name="tgs"></param>
        /// <returns></returns>
        BTree<long, TriggerActivation> Setup(Context _cx,BTree<long, Trigger> tgs)
        {
            var r = BTree<long, TriggerActivation>.Empty;
            var cx = new Context(_cx.tr);
            if (tgs != null)
                for (var tg = tgs.First(); tg != null; tg = tg.Next())
                    r +=(tg.key(), new TriggerActivation(cx,this, tg.value()));
            return r;
        }
        /// <summary>
        /// Perform the triggers in a set
        /// </summary>
        /// <param name="acts"></param>
        (Context,TransitionCursor,bool) Exec(Context _cx, BTree<long, TriggerActivation> acts)
        {
            var r = false;
            if (acts == null)
                return (_cx,_cx.cursors[defpos] as TransitionCursor, r);
            var row = (TransitionCursor)_cx.cursors[defpos];
            bool skip;
            for (var a = acts.First(); a != null; a = a.Next())
            {
                (_cx, skip) = a.value().Exec(_cx, row);
                r = r || skip;
            }
            _cx.val = TBool.For(r);
            return (_cx,(TransitionCursor)_cx.cursors[defpos],r);
        }
        internal (Context,TransitionCursor,bool) InsertSA(Context _cx)
        { return Exec(_cx,ta); }
        internal (Context, TransitionCursor, bool) InsertSB(Context _cx)
        { return Exec(_cx, tb); }
        internal (Context, TransitionCursor, bool) UpdateSA(Context _cx)
        { return Exec(_cx,ta); }
        internal (Context, TransitionCursor, bool) UpdateSB(Context _cx)
        { return Exec( Exec(_cx,tb).Item1,ti); }
        internal (Context, TransitionCursor, bool) DeleteSB(Context _cx)
        { return Exec(Exec(_cx,tb).Item1,ti); }
        internal (Context, TransitionCursor, bool) DeleteSA(Context _cx)
        { return Exec(_cx, td); }
        internal class TransitionCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor _fbm; // transition-side only
            internal readonly BTree<long,TypedValue> _oldVals;
            /// <summary>
            /// There may be several triggers of any type, so we manage a set of transition activations for each.
            /// These are for table before, table instead, table after, row before, row instead, row after.
            /// </summary>
            internal readonly BTree<long, TriggerActivation> rb, ri, ra;
            internal TransitionCursor(Context cx, TransitionRowSet trs, int pos, Cursor fbm)
               : base(cx, trs, pos, fbm._defpos,_Row(trs,fbm))
            {
                _trs = trs as TransitionRowSet;
                _fbm = fbm;
                _oldVals = values;
                for (var b = values.First(); b != null; b = b.Next())
                    cx.values += (b.key(), b.value());
            }
            static TRow _Row(TransitionRowSet trs,Cursor fb)
            {
                var vs = BTree<long, TypedValue>.Empty;
                for (var b=trs.from.rowType.First();b!=null;b=b.Next())
                {
                    var s = b.value();
                    if (fb[s.defpos] is TypedValue v)
                        vs += ((s is SqlCopy sc)?sc.copyFrom:s.defpos, v);
                }
                return new TRow(trs.info.domain, vs);
            }
            // calculate the trigger-side version of the transition cursor
            internal TransitionCursor(Context cx, Selection sel, 
                TransitionCursor fbm)
            : base(cx, sel.defpos, sel, fbm._pos, fbm._defpos, _Row(sel,fbm))
            {
                _trs = fbm._trs as TransitionRowSet;
                _fbm = null; // trigger-side does not access _fbm
                _oldVals = values;
            }
            static TRow _Row(Selection sel,Cursor fb)
            {
                var vs = BTree<long, TypedValue>.Empty;
                for (var b = sel.First(); b != null; b = b.Next())
                {
                    var s = b.value();
                    var p = (s is SqlCopy sc) ? sc.copyFrom : s.defpos;
                        vs += (s.defpos, fb[p]);
                }
                return new TRow(sel.info.domain, vs);
            }
           internal TransitionCursor(Context cx, TransitionCursor cu, 
                BTree<long,TypedValue> vs)
                : base(cx, cu._trs, cu._pos, cu._defpos, new TRow(cu._info.domain,vs))
            {
                _trs = cu._trs;
                _fbm = cu._fbm;
                _oldVals = values;
            }
            internal TransitionCursor(Context cx, Selection sel, TransitionCursor cu,
            BTree<long, TypedValue> vs)
            : base(cx, sel.defpos, sel, cu._pos, cu._defpos, new TRow(sel.info.domain, vs))
            {
                _trs = cu._trs;
                _fbm = null;
                _oldVals = values;
            }
            public static TransitionCursor operator+(TransitionCursor cu,
                (Context,Selection,long,TypedValue)x)
            {
                var (cx, sel,p, tv) = x;
                return new TransitionCursor(cx, sel, cu, cu.values + (p, tv));
            }
            internal TransitionCursor(TransitionCursor trb,Context cx)
                :base(cx,trb._trs,trb._pos,trb._fbm._defpos,
                     new TRow(trb._trs.targetInfo.domain,cx.values))
            {
                _trs = trb._trs;
                _fbm = trb._fbm;
                _oldVals = values;
                var tb = cx.db.objects[_trs.from.target] as Table;
                // Get the trigger sets and set up the activations
                rb = trb.rb??Setup(cx, tb.triggers[_trs._tgt | PTrigger.TrigType.EachRow 
                    | PTrigger.TrigType.Before]);
                ri = trb.ri??Setup(cx, tb.triggers[_trs._tgt | PTrigger.TrigType.EachRow 
                    | PTrigger.TrigType.Instead]);
                ra = trb.ra??Setup(cx, tb.triggers[_trs._tgt | PTrigger.TrigType.EachRow 
                    | PTrigger.TrigType.After]);
            }
            static TransitionCursor New(Context cx,TransitionRowSet trs, int pos,
                 Cursor fbm)
            { 
                var dt = trs.from.rowType;
                var ti = trs.targetInfo;
                for (var b = ti.columns.First(); b != null; b = b.Next())
                    cx.values -= b.value().defpos;
                for (var b = trs.defaults.First(); b != null; b = b.Next())
                    if (cx.values[b.key()] == null || cx.values[b.key()] == TNull.Value)
                        cx.values += (b.key(), b.value());
                var trc = new TransitionCursor(cx, trs, pos, fbm);
                for (int i = 0; i < trs.from.display; i++)
                {
                    var sl = dt[i];
                    var fb = fbm._info.columns[i];
                    TypedValue tv = fbm[fb.defpos];
                    if (sl is SqlProcedureCall sv)
                    {
                        var fp = sv.call.procdefpos;
                        var m = trs._eqs.Match(fp, sl.defpos);
                        if (m.HasValue)
                        {
                            if (m.Value == 0)
                                tv = sl.Eval(cx);
                            else // there's an adapter function
                            {
                                // tv = fn (fbm[j])
                                var pr = cx.db.objects[m.Value] as Procedure;
                                var ac = new CalledActivation(cx, pr, Domain.Null);
                                tv = pr.body.Eval(ac);
                            }
                        }
                    }
                    cx.values += (sl.defpos, tv);
                }
                for (var b = dt.First(); b != null; b = b.Next())
                {
                    var sl = b.value();
                    var tv = cx.values[sl.defpos];
                    if (tv == null || tv==TNull.Value)
                        tv = trs.defaults[sl.defpos] ?? TNull.Value;
                    cx.values += (sl.Defpos(), tv);
                }
                if (trs.indexdefpos>0)
                    trs.CheckPrimaryKey(cx);
                trc = new TransitionCursor(trc,cx);
                for (int i = 0; i < ti.Length; i++)
                    if (ti.columns[i] is ObInfo sc && cx.db.objects[sc.defpos] is TableColumn tc)
                    {
                        switch (tc.generated.gen)
                        {
                            case Generation.Expression:
                                cx.values += (tc.defpos, tc.generated.exp.Eval(cx));
                                break;
                        }
                        if (tc.notNull && !cx.values.Contains(tc.defpos))
                            throw new DBException("22206", ti.columns[i].name);
                    }
                return new TransitionCursor(trc, cx);
            }
            internal static TransitionCursor New(Context _cx,TransitionRowSet trs)
            {
                var ox = _cx.from;
                _cx.from = trs._finder;
                for (var fbm = _cx.data[trs.from.defpos]?.First(_cx); fbm != null;
                    fbm = fbm.Next(_cx))
                    if (fbm.Matches(_cx) && Query.Eval(trs.from.where, _cx))
                    {
                        _cx.from = ox;
                        return New(_cx, trs, 0, fbm);
                    }
                _cx.from = ox;
               return null;
            }
            /// <summary>
            /// Set up activations for executing a set of triggers
            /// </summary>
            /// <param name="q"></param>
            /// <param name="tgs"></param>
            /// <returns></returns>
            BTree<long, TriggerActivation> Setup(Context cx, BTree<long, Trigger> tgs)
            {
                var r = BTree<long, TriggerActivation>.Empty;
                cx = new Context(cx); // for this trs
                if (tgs != null)
                    for (var tg = tgs.First(); tg != null; tg = tg.Next())
                    {
                        var trg = tg.value();
                        var ta = new TriggerActivation(cx,_trs, trg);
                        r +=(tg.key(), ta);
                    }
                return r;
            }
            public override Cursor Next(Context _cx)
            {
                var ox = _cx.from;
                _cx.from = _trs._finder;
                var from = _trs.from;
                if(from.where.First()?.value() is SqlValue sv && sv.domain.kind == Sqlx.CURRENT)
                        return null;
                var t = _cx.db.objects[_trs.from.target] as Table;
                for (var fbm = _fbm.Next(_cx); fbm != null; fbm = fbm.Next(_cx))
                {
                    var ret = New(_cx,_trs, 0, fbm);
                    for (var b = from.where.First(); b != null; b = b.Next())
                        if (b.value().Eval(_cx) != TBool.True)
                            goto skip;
                    _cx.from = ox;
                    return ret;
                    skip:;
                }
                _cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                return _fbm.Rec();
            }
            /// <summary>
            /// Some convenience functions for calling from Transaction.Execute(..)
            /// </summary>
            internal (Context,TransitionCursor,bool) InsertRA(Context _cx)
            { return _trs.Exec(_cx,ra); }
            internal (Context,TransitionCursor,bool) InsertRB(Context _cx)
            { return _trs.Exec( _trs.Exec(_cx,rb).Item1,ri); }
            internal (Context,TransitionCursor,bool) UpdateRA(Context _cx)
            { return _trs.Exec(_cx,ra); }
            internal (Context,TransitionCursor,bool) UpdateRB(Context _cx)
            { var (cx,_,_) = _trs.Exec(_cx,rb); return _trs.Exec(cx,ri); }
            internal (Context,TransitionCursor,bool) DeleteRB(Context _cx)
            { ; return _trs.Exec(_trs.Exec(_cx,rb).Item1,ri); }

        }
    }
    internal class SortedRowSet : RowSet
    {
        internal MTree tree;
        internal readonly RowSet source;
        internal readonly TreeInfo treeInfo;
        List<TRow> rows = new List<TRow>();
        List<Rvv> rvvs = new List<Rvv>();
        internal override RowSet Source => source;
        internal SortedRowSet(Context _cx,RowSet s, ObInfo kt, TreeInfo ti)
            : base(_cx.nextHeap++,_cx, s.info, s.finder, kt,s.where)
        {
            source = s;
            treeInfo = ti;
            tree = (MTree)Build(_cx);
        }
        protected SortedRowSet(SortedRowSet rs, long a, long b) : base(rs, a, b)
        {
            tree = rs.tree;
            source = rs.source;
            treeInfo = rs.treeInfo;
            rows = rs.rows;
            rvvs = rs.rvvs;
        }
        internal override RowSet New(long a, long b)
        {
            return new SortedRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Sorted ");
            sb.Append(keyInfo.ToString());
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        internal override int? Count => rows.Count;
        protected override object Build(Context cx)
        {
            var ox = cx.from;
            cx.from = source.finder;
            var tree = new MTree(treeInfo);
            for (var a = source.First(cx);a!= null;a=a.Next(cx))
            {
                PRow k = null;
                for (int i = keyInfo.Length-1;i>=0;i--)
                    k = new PRow(keyInfo[i].Eval(cx), k);
                MTree.Add(ref tree, k, rows.Count);
                var vs = BTree<long, TypedValue>.Empty;
                for (var b=source.info.columns.First();b!=null;b=b.Next())
                {
                    var s = b.value();
                    vs += (s.defpos, s.Eval(cx));
                }
                rows.Add(new TRow(source.info.domain,vs));
            }
            cx.from = ox;
            cx.data -= source.defpos;
            return tree;
        }
        public override Cursor First(Context _cx)
        {
            return SortedCursor.New(_cx,this);
        }
        public override Cursor PositionAt(Context _cx,PRow key)
        {
            return SortedCursor.New(_cx,this,key);
        }
        internal class SortedCursor : Cursor
        {
            readonly SortedRowSet _srs;
            readonly MTreeBookmark _mbm;
            SortedCursor(Context _cx, SortedRowSet srs, int pos, MTreeBookmark mbm, long dpos)
                : base(_cx, srs, pos, dpos,srs.rows[(int)(mbm.Value()??-1)])
            {
                _srs = srs;
                _mbm = mbm;
            }
            public override MTreeBookmark Mb()
            {
                return _mbm;
            }
            internal static SortedCursor New(Context _cx,SortedRowSet srs)
            {
                var ox = _cx.from;
                _cx.from = srs._finder;
                for (MTreeBookmark mbm = srs.tree.First();mbm!=null;mbm=mbm.Next())
                {
                    //           var rvv = srs.rvvs[(int)mbm.Value().Value];
                    //           var d = (rvv != null) ? rvv.def : 0;
                    var rb = new SortedCursor(_cx, srs, 0, mbm, 0);// d);
                    if (rb.Matches(_cx) && Query.Eval(srs.where, _cx))
                    {
                        _cx.from = ox;
                        return rb;
                    }
                }
                _cx.from = ox;
                return null;
            }
            internal static SortedCursor New(Context _cx,SortedRowSet srs,PRow key)
            {
                for (var mbm = srs.tree.PositionAt(key); mbm != null; mbm = mbm.Next())
                    if (mbm.Value().HasValue)
                    {
                        //            var rvv = srs.rvvs[(int)mbm.Value().Value];
                        //            var d = (rvv != null) ? rvv.def : 0;
                        return new SortedCursor(_cx, srs, 0, mbm, 0); // d);
                    }
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                for (var mbm = _mbm.Next(); mbm != null; mbm = mbm.Next())
                {
                    //         var rvv = ((SortedRowSet)_rs).rvvs[(int)mbm.Value().Value];
                    //         var d = (rvv != null) ? rvv.def : 0;
                    var rb = new SortedCursor(_cx, _srs, _pos + 1, mbm, 0); // d);
                    if (rb.Matches(_cx) && Query.Eval(_srs.where, _cx))
                        return rb;
                }
                return null;
            }

            internal override TableRow Rec()
            {
                return null;
            }
        }
    }
    /// <summary>
    /// RoutineCallRowSet is a table-valued routine call
    /// </summary>
    internal class RoutineCallRowSet : RowSet
    {
        readonly Query from;
        readonly Procedure proc;
        readonly BList<SqlValue> actuals;
        readonly RowSet rowSet;
        internal RoutineCallRowSet(Context cx,From f,Procedure p, BList<SqlValue> r) 
            :base(cx.nextHeap++,cx,f.rowType.info,null,null,f.where)
        {
            from = f;
            proc = p;
            actuals = r;
            rowSet = (RowSet)Build(cx);
        }
        protected RoutineCallRowSet(RoutineCallRowSet rs, long a, long b) : base(rs, a, b)
        {
            from = rs.from;
            proc = rs.proc;
            actuals = rs.actuals;
            rowSet = rs.rowSet;
        }
        internal override RowSet New(long a, long b)
        {
            return new RoutineCallRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("RoutineCall ");
            sb.Append(DBObject.Uid(proc.defpos));
            var cm = '(';
            for(var b = actuals.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ',';
                sb.Append(b.value());
            }
            base._Strategy(sb, indent);
        }
        protected override object Build(Context cx)
        {
            var _cx = new Context(cx);
            cx = proc.Exec(_cx, actuals);
            return cx.val;
        }
        public override Cursor First(Context _cx)
        {
            return rowSet.First(_cx);
        }
    }
    internal class RowSetSection : RowSet
    {
        internal readonly RowSet source;
        internal readonly int offset,count;
        internal RowSetSection(Context _cx,RowSet s, int o, int c)
            : base(_cx.nextHeap++,_cx,s.info,null,s.keyInfo,s.where)
        {
            source = s; offset = o; count = c;
        }
        protected RowSetSection(RowSetSection rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
            offset = rs.offset;
            count = rs.count;
        }
        internal override RowSet New(long a, long b)
        {
            return new RowSetSection(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            return new RowSetSection(cx, source, offset, count);
        }
        internal override RowSet Source => source;
        public override Cursor First(Context _cx)
        {
            var b = source.First(_cx);
            for (int i = 0; b!=null && i < offset; i++)
                b = b.Next(_cx);
            return b;
        }
    }
    internal class DocArrayRowSet : RowSet
    {
        internal readonly BList<SqlValue> vals;
        internal DocArrayRowSet(Context cx,Query q, SqlRowArray d)
            : base(cx.nextHeap++,cx,q.rowType.info,null,null,q.where)
        {
            var vs = BList<SqlValue>.Empty;
            if (d != null)
                for(int i=0;i<d.rows.Count;i++)
                    vs += d.rows[i];
            vals = vs;
        }
        DocArrayRowSet(Context cx,DocArrayRowSet ds) :base(ds.defpos,cx,ds)
        {
            vals = ds.vals;
        }
        protected DocArrayRowSet(DocArrayRowSet rs, long a, long b) : base(rs, a, b)
        {
            vals = rs.vals;
        }
        internal override RowSet New(long a, long b)
        {
            return new DocArrayRowSet(this, a, b);
        }
        internal override RowSet New(long dp, Context cx)
        {
            return new DocArrayRowSet(cx,this);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("DocArray");
            base._Strategy(sb, indent);
            for (var b = vals.First(); b != null; b = b.Next())
            {
                var s = new StringBuilder();
                for (var i = 0; i < indent; i++)
                    s.Append(' ');
                s.Append(b.value());
                s.Append(": ");
                s.Append(b.value().ToString());
                Console.WriteLine(s.ToString());
            }
        }
        public override Cursor First(Context _cx)
        {
            return DocArrayBookmark.New(_cx,this);
        }

        internal class DocArrayBookmark : Cursor
        {
            readonly DocArrayRowSet _drs;
            readonly ABookmark<int, SqlValue> _bmk;

            DocArrayBookmark(Context _cx,DocArrayRowSet drs, ABookmark<int, SqlValue> bmk) 
                :base(_cx,drs,bmk.key(),0,_Row(_cx,bmk.value())) 
            {
                _drs = drs;
                _bmk = bmk;
            }
            static TRow _Row(Context cx,SqlValue sv)
            {
                return new TRow(sv.domain,new BTree<long,TypedValue>(sv.defpos,(TDocument)sv.Eval(cx)));
            }
            internal static DocArrayBookmark New(Context _cx,DocArrayRowSet drs)
            {
                var bmk = drs.vals.First();
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,drs, bmk);
            }
            public override Cursor Next(Context _cx)
            {
                var bmk = ABookmark<int, SqlValue>.Next(_bmk, _drs.vals);
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,_drs, bmk);
            }

            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    internal class WindowRowSet : RowSet
    {
        public override Cursor First(Context _cx)
        {
            throw new NotImplementedException();
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        readonly Query from;
        readonly PeriodSpec pSpec;
        internal WindowRowSet(Context cx,Query f,PeriodSpec ps)
            :base(cx.nextHeap++,cx,f.rowType.info,null,null,f.where,null)
        {
            from = f;
            f.Audit(cx.db.nextPos,cx, f);
            pSpec = ps;
        }
        protected WindowRowSet(WindowRowSet rs, long a, long b) : base(rs, a, b)
        {
            from = rs.from;
            pSpec = rs.pSpec;
        }
        internal override RowSet New(long a, long b)
        {
            return new WindowRowSet(this, a, b);
        }
    }
}