using System;
using System.Collections.Generic;
using System.Diagnostics;
using Pyrrho.Common;
using System.IO;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Level3;
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
    /// A RowSet is the result of a stage of query processing, so is a collection of rows
    /// </summary>
    internal abstract class RowSet : TypedValue
    {
        /// <summary>
        /// debugging
        /// </summary>
        static long _uid =0;
        internal readonly long uid = ++_uid;
        internal Transaction _tr;
        /// <summary>
        /// the query this rowset belongs to (most queries have at most one)
        /// </summary>
        internal Query qry;
        /// <summary>
        /// the key type for the results (default is the row type)
        /// </summary>
        internal readonly Domain keyType;
        internal Domain rowType
        {
            get { return dataType; }
        }
        /// <summary>
        /// ordering information
        /// </summary>
        internal readonly OrderSpec rowOrder;
        /// <summary>
        /// a set of filters
        /// </summary>
        internal BTree<SqlValue,TypedValue> matches = BTree<SqlValue,TypedValue>.Empty;
        /// <summary>
        /// limitations on fetching
        /// </summary>
        internal int skip=0, limit=0;
        /// <summary>
        /// Try to discourage use of qry.cols once the rowSet is Built.
        /// </summary>
        internal bool building = true;
        protected new virtual void Build(Context _cx)
        { }
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="q">the hosting query</param>
        /// <param name="n">optional the nominal data type for the rows</param>
        /// <param name="k">optional: a key type</param>
        protected RowSet(Transaction tr,Context cx,Query q,Domain n=null,Domain k=null,OrderSpec os=null)
            :base(n ?? q.rowType??Domain.Value)
        {
            _tr = tr;
            qry = q;
            keyType = k ?? rowType;
            matches = q?.matches??BTree<SqlValue,TypedValue>.Empty;
            rowOrder = os;
        }
        internal virtual int? Count => null; // number of rows is usually unknown
        public abstract string keywd();
        /// <summary>
        /// The key for given row
        /// </summary>
        /// <param name="bmk">a bookmark</param>
        /// <returns>the key</returns>
        public virtual PRow Key(RowBookmark bmk)
        {
            PRow r = null;
            for (var i = keyType.Length - 1; i >= 0; i--)
                r = new PRow(bmk.row[keyType.columns[i].defpos], r);
            return r;
        }
        public override bool IsNull => false;
        internal override object Val()
        {
            return this;
        }
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        internal void AddMatch(BTree<SqlValue,TypedValue> mts)
        {
            for (var a = mts.First(); a != null; a = a.Next())
                    AddMatch(a.key(), a.value());
        }
        /// <summary>
        /// Test if the given source RowSet matches the requested ordering
        /// </summary>
        /// <returns>whether the orderings match</returns>
        protected bool SameOrder(RowSet sce)
        {
            if (rowOrder == null || rowOrder.items.Count == 0)
                return true;
            return rowOrder.SameAs(qry,sce?.rowOrder);
        }
        /// <summary>
        /// Compute schema information (for the client) for this row set.
        /// 
        /// </summary>
        /// <param name="flags">The column flags to be filled in</param>
        internal void Schema(Domain dt, int[] flags)
        {
            int m = dt.Length;
            bool addFlags = true;
            var adds = new int[flags.Length];
            // see if we are going to add index flags stuff
            var fm = qry as Table;
            if (fm != null)
            {
                var ix = fm.FindPrimaryIndex();
                if (ix != null)
                    for (int i = 0; i < ix.cols.Count; i++)
                    {
                        var cp = ix.cols[i].seq;
                        var rev = cp < 0;
                        if (rev)
                            cp = -cp;
                        var j = dt.columns[cp].seq;
                        if (j<0)
                        {
                            addFlags = false;
                            break;
                        }
                        adds[j] = ((i + 1) << 4) + (rev ? 0x400 : 0);
                    }
            }
            for (int i = 0; i < flags.Length; i++)
            {
                var dc = dt.columns[i].nominalDataType;
                var tc = (TableColumn)_tr.role.objects[dt.columns[i].defpos];
                if (dc.kind == Sqlx.SENSITIVE)
                    dc = dc.elType;
                flags[i] = dc.Typecode() + (addFlags ? adds[i] : 0);
                if (tc!=null)
                flags[i] += (tc.notNull ? 0x100 : 0) +
                    ((tc.generated != PColumn.GenerationRule.No) ? 0x200 : 0);
            }
        }
        internal virtual TypedValue Eval(Context _cx,Selector sel)
        {
            return _cx.values[sel.defpos];
        }
        /// <summary>
        /// For simple queries the filters will be added by other means.
        /// Override this method for grouped and exported rowsets at least
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="cond"></param>
        internal virtual void AddCondition(Context _cx,BTree<long,SqlValue> cond, long done)
        {
            qry.AddCondition(_tr,_cx,cond, null, null);
        }
        /// <summary>
        /// Bookmarks are used to traverse rowsets
        /// </summary>
        /// <returns>a bookmark for the first row if any</returns>
        protected abstract RowBookmark _First(Context _cx);
        /// <summary>
        /// First needs to be overridden in RowSets with additional search conditions such as GroupingRowSet
        /// </summary>
        /// <returns></returns>
        public virtual RowBookmark First(Context _cx)
        {
#if !EMBEDDED
            if (PyrrhoStart.StrategyMode)
                Strategy(0);
#endif
            _cx.Add(qry);
            return _First(_cx);
        }
        /// <summary>
        /// Find a bookmark for the given key
        /// </summary>
        /// <param name="key">a key</param>
        /// <returns>a bookmark or null if it is not there</returns>
        public virtual RowBookmark PositionAt(Context _cx,PRow key)
        {
            var dt = keyType ?? rowType;
            for (var bm = First(_cx); bm!=null; bm=bm.Next(_cx))
            {
                var k = key;
                for (int i = 0; i < dt.Length && k != null; i++, k = k._tail)
                    if (bm.row[dt.columns[i].defpos].CompareTo(k._head) != 0)
                        goto next;
                return bm;
                next:;
            }
            return null;
        }
        internal virtual RowSet Source()
        {
            return null;
        }
        internal virtual void _Strategy(StringBuilder sb,int indent)
        {
            Conds(sb, qry.where, " where ");
            Matches(sb);
            Console.WriteLine(sb.ToString());
        }
        internal void Cols(StringBuilder sb,Index x)
        {
            var ro = _tr.role;
            var cm = "(";
            for (var i=0;i<x.cols.Count;i++)
            {
                sb.Append(cm); cm = ",";
                sb.Append(ro.dbobjects[x.cols[i].name]);
            }
            sb.Append(")");
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

        internal virtual void AddMatch(SqlValue v, TypedValue typedValue)
        {
            matches+=(v, typedValue);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("RowSet ");
            sb.Append(rowType);
            _Strategy(sb, 0);
            return sb.ToString();
        }
    }
    /// <summary>
    /// 1. Throughout the code RowSets are navigated using RowBookmarks instead of
    /// Enumerators of any sort. The great advantage of bookmarks generally is
    /// that they are immutable. (E.g. the ABookmark classes always refer to a
    /// snapshot of the tree at the time of constructing the bookmark.)
    /// 2. The Rowset field is readonly but NOT immutable as it contains the Context _cx.
    /// _cx.values is updated to provide shortcuts to current values.
    /// 3. The TypedValues obtained by Value(..) are mostly immutable also, but
    /// updates to row columns (e.g. during trigger operations) are managed by 
    /// allowing the RowBookmark to override the usual results of 
    /// evaluating a row column: this is needed in (at least) the following 
    /// circumstances (a) UPDATE (b) INOUT and OUT parameters that are passed
    /// column references (c) triggers.
    /// 4. In view of the anxieties of 3. these overrides are explicit
    /// in the RowBookmark itself.
    /// </summary>
    internal abstract class RowBookmark 
    {
        public readonly RowSet _rs;
        public readonly int _pos;
        public readonly long _defpos;
        /// <summary>
        /// row and key must be correct at the end of First() and Next().
        /// </summary>
        public abstract TRow row { get; }
        public abstract TRow key { get; }
        public RowBookmark(Context _cx,RowSet rs,int pos,long defpos)
        {
            _rs = rs;
            _pos = pos;
            _defpos = defpos;
        }
        public abstract RowBookmark Next(Context _cx);
        /// <summary>
        /// These methods are for join processing
        /// </summary>
        /// <returns></returns>
        public virtual MTreeBookmark Mb()
        {
            return null; // throw new PEException("PE543");
        }
        public virtual RowBookmark ResetToTiesStart(Context _cx, MTreeBookmark mb)
        {
            return null; // throw new PEException("PE544");
        }
        internal abstract TableRow Rec();
        internal BList<SqlValue> Grouping()
        {
            return row?.grouping;
        }
        public bool Matches()
        {
            if (Rec() is TableRow r)
            {
                for (var b = _rs.matches.First(); b != null; b = b.Next())
                    if (r.fields[b.key().defpos] is TypedValue tv && tv.CompareTo(b.value()) != 0)
                        return false;
                return true;
            }
            if (_rs.matches.Count == 0)
                return true;
            var dt = _rs.rowType;
            for (var b = _rs.matches.First(); b != null; b = b.Next())
            {
                for (var i = 0; i < dt.Length; i++)
                {
                    var nm = dt.columns[i].defpos;
                    if (row[nm] is TypedValue tv && tv.CompareTo(b.value()) != 0)
                        return false;
                    break;
                }
            }
            return true;
        }
    }
    internal class TrivialRowSet: RowSet
    {
        public override string keywd()
        {
            return " Trivial ";
        }
        readonly TRow row;
        readonly TrivialRowBookmark here;
        public static TrivialRowSet Static = new TrivialRowSet(null,null, null, new TRow());
        internal TrivialRowSet(Transaction tr,Context cx, Query q, TRow r, long d=0, long rc=0)  
            :base(tr,cx, q,q?.rowType??Domain.Null,r.dataType)
        {
            row = r;
            here = new TrivialRowBookmark(cx,this,d,rc);
            cx?.Add(q, here);
        }
        internal TrivialRowSet(Transaction tr,Context cx, Query fm, Record rec) 
            : base(tr, cx, fm, fm?.rowType??Domain.Null, null)
        {
            row = new TRow(fm.rowType, rec.fields);
            here = new TrivialRowBookmark(cx,this, 0, rec.defpos);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Trivial ");
            sb.Append(row.ToString());
            base._Strategy(sb, indent);
        }
        internal override int? Count => 1;

        public override bool IsNull => throw new NotImplementedException();

        protected override RowBookmark _First(Context _cx)
        {
            return here;
        }
        public override RowBookmark PositionAt(Context _cx,PRow key)
        {
            for (int i = 0; key != null; i++, key = key._tail)
                if (row[i].CompareTo(key._head) != 0)
                    return null;
            return here;
        }

        internal override object Val()
        {
            throw new NotImplementedException();
        }

        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }

        internal class TrivialRowBookmark : RowBookmark
        {
            readonly TrivialRowSet trs;
            internal TrivialRowBookmark(Context _cx, TrivialRowSet t,long d,long r) :base(_cx,t,0,d)
            {
                trs = t;
            }

            public override TRow row => trs.row;

            public override TRow key => trs.row;

            public override RowBookmark Next(Context _cx)
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
    /// Used to establish a new context and rowType for results
    /// </summary>
    internal class ExportedRowSet : RowSet
    {
        public override string keywd()
        {
            return " Exported ";
        }
        internal readonly RowSet source;
        internal readonly Domain rtyp;
        internal ExportedRowSet(Context _cx,RowSet rs, Domain rt,Domain kt=null) :base(rs._tr,_cx,rs.qry,rt,kt)
        {
            source = rs;
            rtyp = rt;
            if (!rt.CanTakeValueOf(rs.rowType))
                throw new DBException("22005", rt, rs.rowType);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Exported ");
            sb.Append(qry);
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        protected override RowBookmark _First(Context _cx)
        {
            return ExportedBookmark.New(_cx,this);
        }
        internal class ExportedBookmark : RowBookmark
        {
            readonly RowBookmark _bmk;
            readonly ExportedRowSet _ers;
            readonly TRow _row, _key;
            public override TRow row => _row;
            public override TRow key =>  _key;
            ExportedBookmark(Context _cx,ExportedRowSet ers,int pos, RowBookmark bmk)
                :base(_cx,ers,pos,bmk._defpos)
            {
                _ers = ers;
                _bmk = bmk;
                _row = new TRow(_rs.qry.rowType, _cx.values);
                _key = new TRow(_rs.keyType, _cx.values);
                _cx.Add(ers.qry, this);
            }

            internal static ExportedBookmark New(Context _cx,ExportedRowSet e)
            {
                RowBookmark bmk;
                for (bmk = e.source.First(_cx);bmk!=null;bmk=bmk.Next(_cx))
                {
                    var rb = new ExportedBookmark(_cx,e, 0, bmk);
                    if (!rb.Matches())
                        goto skip;
                    for (var b = e.qry.where.First(); b != null; b = b.Next())
                        if (b.value().Eval(e._tr,_cx) != TBool.True)
                            goto skip;
                    return rb;
                    skip:;
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var bmk = _bmk.Next(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new ExportedBookmark(_cx,_ers, _pos + 1, bmk);
                    for (var b = _rs.qry.where.First(); b != null; b = b.Next())
                        if (b.value().Eval(_ers._tr,_cx) != TBool.True)
                            goto skip;
                    return rb;
                    skip:;
                }
                return null;
            }

        internal override TableRow Rec()
        {
            return _bmk.Rec();
        }
    }
    }
    /// <summary>
    /// A rowset consisting of selected Columns from another rowset (e.g. SELECT A,B FROM C)
    /// </summary>
    internal class SelectedRowSet : RowSet
    {
        public override string keywd()
        {
            return " Selected ";
        }
        internal readonly RowSet rIn;
        /// <summary>
        /// Constructor for the selected TableColumns rowset. At this stage all of the SqlValue have been resolved.
        /// So all of the selects in qout are guaranteed to be in qin.
        /// </summary>
        internal SelectedRowSet(Transaction tr,Context cx,Query q,RowSet r)
            :base(tr,cx,q,q.rowType,_Type(r, q),r.qry.ordSpec)
        {
            rIn = r;
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Selected ");
            sb.Append(qry);
            base._Strategy(sb, indent);
            rIn.qry.RowSets(_tr,new Context(_tr)).Strategy(indent);
        }
        protected override void Build(Context _cx)
        {
            building = false;
            for (var b = qry.cols.First(); b != null; b = b.Next())
                b.value().Build(_cx,this);
        }
        public override RowBookmark First(Context _cx)
        {
            if (building)
                Build(_cx);
            return base.First(_cx);
        }
        protected override RowBookmark _First(Context _cx)
        {
            return SelectedRowBookmark.New(_cx,this);
        }
        static Domain _Type(RowSet rin,Query qout)
        {
            var kt = rin.keyType;
            for (int i = 0; i < kt.columns.Count; i++)
                if (!qout.scols.Contains(kt.columns[i]))
                    return null;
            return kt;
        }
        internal override TypedValue Eval(Context _cx,Selector stc)
        {
            return rIn.Eval(_cx,stc);
        }
        internal class SelectedRowBookmark : RowBookmark
        {
            readonly SelectedRowSet _srs;
            readonly RowBookmark _bmk; // for rIn
            readonly TRow _row, _key;
            long valueInProgress = -1;

            public override TRow row => _row;

            public override TRow key => _key;

            SelectedRowBookmark(Context _cx,SelectedRowSet srs, RowBookmark bmk, int pos) 
                : base(_cx,srs, pos, bmk._defpos)
            {
                _bmk = bmk;
                _srs = srs;
                var vs =Value(_cx);
                _row = new TRow(_rs.qry.rowType,vs);
                _key = new TRow(_rs.keyType, vs);
                _cx.Add(srs.qry, this);
            }
            /// <summary>
            /// WE override the base implementation in order to use the right column names
            /// </summary>
            /// <returns></returns>
            BTree<long, TypedValue> Value(Context _cx)
            {
                var dt = _rs.rowType;
                var r = BTree<long, TypedValue>.Empty;
                for (int i = 0; i < dt.Length; i++)
                {
                    TypedValue v = null;
                    var id = _rs.qry.rowType.columns[i].defpos; // nb
                    if (valueInProgress == id && id>0) // avoid infinite recursion!
                        return null;
                    valueInProgress = id;
                    if (_rs.qry is SelectQuery qs && i < qs.Size) // always will be
                        v = qs.cols[i].Eval(_rs._tr, _cx);
                    if (v!=null)
                        r += (id,v);
                }
                return r;
            }
            internal static SelectedRowBookmark New(Context _cx,SelectedRowSet srs)
            {
                for (var bmk = srs.rIn.First(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new SelectedRowBookmark(_cx,srs, bmk, 0);
                    if (rb.row.values!=BTree<long,TypedValue>.Empty &&
                        rb.Matches() && Query.Eval(srs.qry.where,srs._tr,_cx))
                        return rb;
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var bmk = _bmk.Next(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new SelectedRowBookmark(_cx,_srs, bmk, _pos + 1);
                    for (var b = _rs.qry.cols.First(); b != null; b = b.Next())
                        b.value().OnRow(rb);
                    if (rb.row.values != BTree<long, TypedValue>.Empty && 
                        rb.Matches() && Query.Eval(_rs.qry.where, _rs._tr,_cx))
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
        public override string keywd()
        {
            return " Eval ";
        }
        /// <summary>
        /// The RowSet providing the data for aggregation. 
        /// </summary>
		internal RowSet source;
        /// <summary>
        /// The having search condition for the aggregation
        /// </summary>
		internal BTree<long,SqlValue> having;
        private TRow row,key;
        /// <summary>
        /// Constructor: Build a rowSet that aggregates data from a given source
        /// </summary>
        /// <param name="rs">The source data</param>
        /// <param name="h">The having condition</param>
		public EvalRowSet(Transaction tr,Context cx,Query qout, RowSet rs, BTree<long,SqlValue> h) 
            : base(rs._tr,cx,qout)
        {
            source = rs;
            having = h;
            var wh = BTree<SqlValue, TypedValue>.Empty;
            var qin = rs.qry;
            bool gp = false;
            if ((qin as TableExpression)?.group is GroupSpecification g)
                for (var gs = g.sets.First();gs!=null;gs=gs.Next())
                    gs.value().Grouped(qry.where, ref gp);
            if (!gp)
                qry += (Query.Where,BTree<long, SqlValue>.Empty);
            Build(cx);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Eval");
            sb.Append(qry);
            var cm = " having ";
            for (var b=having.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = " and ";
                sb.Append(b.value().ToString());
            }
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        public override RowBookmark First(Context _cx)
        {
            Build(_cx);
            return _First(_cx);
        }
        protected override RowBookmark _First(Context _cx)
        {
            return new EvalBookmark(_cx,this);
        }

        protected override void Build(Context _cx)
        {
            building = true;
            var vs = BTree<long, TypedValue>.Empty;
            for (int i = 0; i < rowType.Length; i++)
                qry.ValAt(i).StartCounter(_cx,this);
            var ebm = source.First(_cx);
            if (ebm != null)
            {
                for (; ebm != null; ebm = ebm.Next(_cx))
                    if (Query.Eval(having,_tr,_cx))
                        for (int i = 0; i < rowType.Length; i++)
                            qry.ValAt(i).AddIn(_cx,ebm);
            }
            for (int i = 0; i < rowType.Length; i++)
            {
                var s = qry.cols[i];
                vs += (s.defpos,s.Eval(_tr, _cx));
            }
            row = new TRow(rowType,vs);
            key = new TRow(keyType, vs);
            building = false;
        }

        internal class EvalBookmark : RowBookmark
        {
            readonly EvalRowSet _ers;
            internal EvalBookmark(Context _cx, EvalRowSet ers) : base(_cx,ers, 0, 0)
            {
                _ers = ers;
            }
            public override TRow row => _ers.row;

            public override TRow key => _ers.key;

            public override RowBookmark Next(Context _cx)
            {
                return null; // just one row in the rowset
            }
            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }

    /// <summary>
    /// A TableRowSet consists of all of the *accessible* TableColumns from a Table
    /// </summary>
    internal class TableRowSet : RowSet
    {
        public override string keywd()
        {
            return " Table ";
        }
        readonly Table table;
        /// <summary>
        /// Constructor: a rowset defined by a base table without a primary key
        /// </summary>
        /// <param name="f">the from</param>
        internal TableRowSet(Transaction tr, Context cx,Table f) : base(tr,cx,f)
        {
            table = f;
            f.Audit(tr,f);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Table ");
            sb.Append(table.ToString());
            sb.Append(' ');
            sb.Append(table);
            base._Strategy(sb, indent);
        }
        internal override int? Count => (int?)table.tableRows.Count;
        protected override RowBookmark _First(Context _cx)
        {
            return TableRowBookmark.New(_cx,this);
        }
        internal class TableRowBookmark : RowBookmark
        {
            readonly ABookmark<long, TableRow> _bmk;
            internal readonly TRow _row, _key;
            public override TRow row => _row;
            public override TRow key => _key;
            TableRowBookmark(Context _cx, RowSet trs, int pos,
                ABookmark<long, TableRow> bmk) : base(_cx,trs, pos, bmk.key())
            {
                _bmk = bmk;
                _row = new TRow(_rs.rowType, bmk.value().fields);
                _key = new TRow(_rs.keyType, bmk.value().fields);
                _cx.Add(trs.qry, this);
            }
            internal static TableRowBookmark New(Context _cx, TableRowSet trs)
            {
                var table = trs.qry as Table;
                for (var b = table.tableRows.First(); b != null; b = b.Next())
                {
                    var rec = b.value();
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) && 
                        trs._tr.user.defpos != table.definer 
                        && !trs._tr.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    if (table.CheckMatch(trs._tr, _cx,rec))
                    {
                        var bm = new TableRowBookmark(_cx,trs, 0, b);
                        // because where won't evaluate correctly until we have a bookmark for the query
                        if (Query.Eval(table.where,trs._tr, _cx))
                            return bm;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                var bmk = _bmk;
                var rec = _bmk.value();
                var table = _rs.qry as Table;
                TableRowBookmark ret = null;
                for (;;) // loop until we find a local or remote match record or we give up
                {
                    if (bmk != null)
                        bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (table.enforcement.HasFlag(Grant.Privilege.Select) && 
                        _rs._tr.user.defpos!=table.definer 
                        && !_rs._tr.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    ret = new TableRowBookmark(_cx,_rs, _pos + 1, bmk);
                    if ((!table.CheckMatch(_rs._tr,_cx,rec)) 
                        || !Query.Eval(table.where,_rs._tr,_cx))
                        continue;
                    break;  // got a local row
                }
                return ret;
            }
            internal override TableRow Rec()
            {
                return _bmk.value();
            }
        }
#if !EMBEDDED && !LOCAL
        internal class RemoteRowBookmark : RowBookmark
        {
            readonly long _curs, _curl;
            readonly internal Record _rec;
            RemoteRowBookmark(RowSet trs,long pos,long curs,long curl,Record rec) :base(trs,pos,rec.defpos,rec.ppos)
            {
                _curs = curs; _curl = curl; _rec = rec;
            }
            internal static RemoteRowBookmark New(TableRowSet trs,Database p)
            {
                RemoteRowBookmark r = null;
                try
                {
                    var proxy = (p as Participant).cd.Async as AsyncStream;
                    var from = trs.qry as From;
                    var lk = proxy.GetLock();
                    lock (lk)
                    {
                        lk.OnLock(false, "GetTable", proxy.Conn());
                        proxy.Write(Protocol.GetTable);
                        proxy.PutLong(from.target.defpos);
                        proxy.PutLong(from.matches.Count);
                        for (var ma = from.matches.First(); ma != null; ma = ma.Next())
                        {
                            proxy.PutLong(ma.key());
                            proxy.PutCell(trs.qry, ma.value().dataType, ma.value());
                        }
                        proxy.Flush();
                        if (proxy.ReadResponse() != Responses.TableCursor)
                            throw new DBException("24101").ISO();
                        var curs = proxy.GetLong();
                        while (r==null)
                        {
                            proxy.Write(Protocol.TableNext);
                            proxy.PutLong(curs);
                            proxy.Flush();
                            if (proxy.ReadResponse() == Responses.TableDone)
                                break;
                            var curl = proxy.GetLong();
                            var rec = proxy.GetRecord(p.pb);
                            if (trs.from.CheckMatch(rec))
                                r = new RemoteRowBookmark(trs, 0, curs, curl, rec);
                        }
                        lk.Unlock(true);
                    }
                }
                catch
                {
                    throw new DBException("08006").ISO();
                }
                return r;
            }
            public override RowBookmark Next()
            {
                var from = _rs.qry as From;
                var part = from.database as Participant;
                var curs = _curs;
                var curl = _curl;
                Record rec;
                RowBookmark ret = null;
                try
                {
                    while(ret==null)
                    {
                        var proxy = part.cd.Async as AsyncStream;
                        proxy.Write(Protocol.TableNext);
                        proxy.PutLong(curs);
                        proxy.Flush();
                        if (proxy.ReadResponse() == Responses.TableDone)
                            break;
                        curl = proxy.GetLong();
                        rec = proxy.GetRecord(part.pb);
                        ret = new RemoteRowBookmark(_rs, _pos+1, curs, curl,  rec);
                        if (!(_rs.qry as From).CheckMatch(rec))
                            ret = null;
                    }
                }
                catch
                {
                    throw new DBException("08006").ISO();
                }
                return ret;
            }
            internal override TypedValue this[int i]
            {
                get
                {
                    var dt = _rs.rowType;
                    return _rec.Field(dt.names[i].defpos) ?? TNull.Value;
                }
            }
            public override Rvvs _Rvv()
            {
                var from = _rs.qry as From;
                return Rvvs.New(new Rvv(null,0,from.database.name, _rec.ppos, _curl));
            }
        }
#endif
    }
    /// <summary>
    /// A RowSet defined by an Index (e.g. the primary key for a table)
    /// </summary>
    internal class IndexRowSet : RowSet
    {
        public override string keywd()
        {
            return " Index ";
        }
        /// <summary>
        /// The From part
        /// </summary>
        readonly Table from;
        /// <summary>
        /// The Index to use
        /// </summary>
        readonly Index index;
        readonly PRow filter;
        /// <summary>
        /// Constructor: A rowset for a table using a given index
        /// </summary>
        /// <param name="f">the from part</param>
        /// <param name="x">the index</param>
        internal IndexRowSet(Transaction tr, Context cx, Table f, Index x, PRow m)
            : base(tr, cx, f, f.rowType, x.rows.info.keyType, new OrderSpec(x.rows.info.keyType))
        {
            from = f;
            index = x;
            filter = m;
            f.Audit(tr, x, m);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Index");
            if (filter?.ToString() is string s && s != "()")
            {
                sb.Append(" filter ");
                sb.Append(s);
            }
            sb.Append(' ');
            sb.Append(from);
            base._Strategy(sb, indent);
        }
        internal override int? Count => (int?)index.rows.Count;
        protected override RowBookmark _First(Context _cx)
        {
            return IndexRowBookmark.New(_cx,this);
        }
        /// <summary>
        /// We assume the key matches our key type, and that the filter is null
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public override RowBookmark PositionAt(Context _cx,PRow key)
        {
            return IndexRowBookmark.New(_cx,this, key);
        }
        internal class IndexRowBookmark : RowBookmark
        {
            internal readonly IndexRowSet _irs;
            internal readonly MTreeBookmark _bmk;
            internal readonly TableRow _rec;
            internal readonly TRow _row, _key;
            public bool IsNull => false;

            public override TRow row => _row;

            public override TRow key => _key;

            IndexRowBookmark(Context _cx, IndexRowSet irs, int pos, MTreeBookmark bmk)
                : base(_cx,irs, pos, bmk.Value().Value)
            {
                _bmk = bmk; _irs = irs;
                _rec = irs.from.tableRows[_defpos];
                _row = new TRow(_rs.rowType, _rec.fields);
                _key = new TRow(_rs.keyType, _rec.fields);
                _cx.Add(irs.qry, this);
            }
            public override MTreeBookmark Mb()
            {
                return _bmk;
            }
            internal override TableRow Rec()
            {
                return _rec;
            }
            public override RowBookmark ResetToTiesStart(Context _cx, MTreeBookmark mb)
            {
                return new IndexRowBookmark(_cx,_irs, _pos + 1, mb);
            }
            internal static IndexRowBookmark New(Context _cx,IndexRowSet irs, PRow key = null)
            {
                for (var bmk = irs.index.rows.PositionAt(key ?? irs.filter); bmk != null;
                    bmk = bmk.Next())
                {
                    var iq = bmk.Value();
                    if (!iq.HasValue)
                        continue;
                    var rec = irs.from.tableRows[iq.Value];
                    if (rec == null || (irs.from.enforcement.HasFlag(Grant.Privilege.Select)
                        && irs._tr.user.defpos != irs.from.definer
                        && !irs._tr.user.clearance.ClearanceAllows(rec.classification)))
                        continue;
                    // not sure (yet) which of the two following tests works best
                    for (var m = irs.matches?.First(); m != null; m = m.Next())
                        if (rec.fields.Contains(m.key().defpos)
                            && m.value().CompareTo(rec.fields[m.key().defpos]) != 0)
                            goto skip;
                    if (irs.from.CheckMatch(irs._tr, _cx, rec))
                    {
                        var bm = new IndexRowBookmark(_cx,irs, 0, bmk);
                        // because where won't evaluate until we have a bookmark for the query
                        if (Query.Eval(irs.from.where, irs._tr, _cx))
                            return bm;
                    }
                skip:;
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                var bmk = _bmk;
                var rec = _rec;
                var table = _rs.qry as Table;
                for (; ; )
                {
                    bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    if (!bmk.Value().HasValue)
                        continue;
                    rec = table.tableRows[bmk.Value().Value];
                    if (table.enforcement.HasFlag(Grant.Privilege.Select)
                        && _rs._tr.user.defpos != table.definer
                        && !_rs._tr.user.clearance.ClearanceAllows(rec.classification))
                        continue;
                    // not sure (yet) which of the two following tests works best
                    for (var m = _rs.matches?.First(); m != null; m = m.Next())
                        if (rec.fields.Contains(m.key().defpos)
                            && m.value().CompareTo(rec.fields[m.key().defpos]) != 0)
                            goto skip;
                    if (table.CheckMatch(_rs._tr, _cx, rec))
                    {
                        var bm = new IndexRowBookmark(_cx,_irs, _pos + 1, bmk);
                        // because where won't evaluate correctly until we update the bookmark for the query
                        if (Query.Eval(table.where, _rs._tr, _cx))
                            return bm;
                    }
                skip:;
                }
            }
        }
    }

    /// <summary>
    /// A rowset for distinct values
    /// </summary>
    internal class DistinctRowSet : RowSet
    {
        public override string keywd()
        {
            return " Distinct ";
        }
        MTree rtree;
        RowSet source;
        /// <summary>
        /// constructor: a distinct rowset
        /// </summary>
        /// <param name="r">a source rowset</param>
        internal DistinctRowSet(Context _cx,RowSet r)
            : base(r._tr,_cx,r.qry,r.rowType,r.keyType)
        {
            var wh = BTree<SqlValue, TypedValue>.Empty;
            Build(r.qry.defpos);
            source = r;
        }

        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Distinct");
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }

        protected override void Build(Context _cx)
        {
            rtree = new MTree(new TreeInfo(source.keyType, TreeBehaviour.Allow, TreeBehaviour.Allow));
            var vs = BList<TypedValue>.Empty;
            var i = 0;
            for (var a = source.First(_cx); a != null; a = a.Next(_cx),i++)
            {
                for (var ti = rtree.info; ti != null; ti = ti.tail)
                    vs+=(i,a.row[ti.head]);
                MTree.Add(ref rtree, new PRow(vs), 0);
            }
        }

        protected override RowBookmark _First(Context _cx)
        {
            return DistinctRowBookmark.New(_cx,this);
        }

        internal class DistinctRowBookmark : RowBookmark
        {
            readonly MTreeBookmark _bmk;
            readonly TRow _row;
            DistinctRowBookmark(Context _cx, DistinctRowSet drs,int pos,MTreeBookmark bmk) 
                :base(_cx,drs,pos,0)
            {
                _bmk = bmk;
                _row = new TRow(drs.rowType, bmk.key());
                _cx.Add(drs.qry, this);
            }

            public override TRow row => _row;

            public override TRow key => _row;

            internal static DistinctRowBookmark New(Context _cx,DistinctRowSet drs)
            {
                for (var bmk = drs.rtree.First(); bmk != null; bmk = bmk.Next())
                {
                    var rb = new DistinctRowBookmark(_cx,drs, 0, bmk);
                    if (rb.Matches() && Query.Eval(drs.qry.where, drs._tr, _cx))
                        return rb;
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var bmk = _bmk.Next(); bmk != null; bmk = bmk.Next())
                {
                    var rb = new DistinctRowBookmark(_cx,_rs as DistinctRowSet, _pos + 1, bmk);
                    if (rb.Matches() && Query.Eval(_rs.qry.where, _rs._tr,_cx))
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
    internal class OrderedRowSet : RowSet
    {
        public override string keywd()
        {
            return " Ordered ";
        }
        internal RTree tree = null;
        internal RowSet source;
        readonly bool distinct;
        public OrderedRowSet(Context _cx,Query q,RowSet r,OrderSpec os,bool dct)
            :base(r._tr,_cx,q,r.rowType,os.KeyType(r.rowType),os)
        {
            source = r;
            distinct = dct;
            building = !SameOrder(r);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Ordered ");
            if (distinct)
                sb.Append("distinct ");
            sb.Append(keyType);
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        internal override int? Count => tree.rows.Count;
        protected override RowBookmark _First(Context _cx)
        {
            if (building)
            {
                tree = new RTree(source, new TreeInfo(keyType, distinct ? TreeBehaviour.Ignore : TreeBehaviour.Allow, TreeBehaviour.Allow));
                for (var e = source.First(_cx); e != null; e = e.Next(_cx))
                {
                    var ks = new TypedValue[keyType.Length];
                    var rw = e.row;
                    for (int j = (int)rowOrder.items.Count - 1; j >= 0; j--)
                        ks[j] = rowOrder.items[j].what.Eval(source._tr,_cx);
                    RTree.Add(ref tree, new TRow(keyType, ks), rw);
                }
                building = false;
            }
            return (tree==null)?source.First(_cx):RTreeBookmark.New(_cx,tree);
        }
        public override RowBookmark PositionAt(Context _cx,PRow key)
        {
            return tree.PositionAt(_cx,key);
        }
    }
    internal class EmptyRowSet : RowSet
    {
        public override string keywd()
        {
            return " Empty ";
        }
        public static readonly EmptyRowSet Value = new EmptyRowSet();
        EmptyRowSet() : base(null,null,null) { }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Empty");
            base._Strategy(sb, indent);
        }
        internal override int? Count => 0;
        protected override RowBookmark _First(Context _cx)
        {
            return null;
        }
    }
    /// <summary>
    /// A rowset for SqlRows
    /// </summary>
    internal class SqlRowSet : RowSet
    {
        public override string keywd()
        {
            return " Sql ";
        }
        internal readonly SqlRow[] rows;
        internal SqlRowSet(Transaction tr,Context cx,Query q, Domain dt, SqlRow[] rs) 
            : base(tr,cx, q, dt, dt)
        {
            rows = rs;
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("SqlRows ");
            sb.Append(rows.Length);
            base._Strategy(sb, indent);
        }
        protected override RowBookmark _First(Context _cx)
        {
            return SqlRowBookmark.New(_cx,this);
        }
        internal class SqlRowBookmark : RowBookmark
        {
            readonly SqlRowSet _srs;
            readonly TRow _row;

            public override TRow row => _row;

            public override TRow key => _row;

            SqlRowBookmark(Context _cx,SqlRowSet rs,int pos): base(_cx,rs,pos,0)
            {
                _srs = rs;
                _row = _srs.rows[_pos].Eval(_srs._tr, _cx) as TRow;
                _cx.Add(rs.qry, this);
            }
            internal static SqlRowBookmark New(Context _cx,SqlRowSet rs)
            {
                for (var i = 0; i < rs.rows.Length; i++)
                {
                    var rb = new SqlRowBookmark(_cx,rs, i);
                    if (rb.Matches() && Query.Eval(rs.qry.where, rs._tr, _cx))
                        return rb;
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var i = _pos + 1; i < _srs.rows.Length; i++)
                {
                    var rb = new SqlRowBookmark(_cx,_srs, i);
                    if (rb.Matches() && Query.Eval(_srs.qry.where, _srs._tr, _cx))
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
    /// <summary>
    /// a row set for TRows
    /// </summary>
    internal class ExplicitRowSet : RowSet
    {
        public override string keywd()
        {
            return " Explicit ";
        }
        internal BList<(long,TRow)> rows = BList<(long,TRow)>.Empty;
        /// <summary>
        /// constructor: a set of explicit rows
        /// </summary>
        /// <param name="q">a query</param>
        /// <param name="r">a row type</param>
        internal ExplicitRowSet(Transaction tr,Context cx,Query q,Domain kT=null)
            : base(tr,cx,q,q.rowType,kT)
        {
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
                    Console.WriteLine(s.ToString());
                }
            else
                Console.WriteLine("" + Count + " rows");
        }
        internal override int? Count => (int)rows.Count;
        /// <summary>
        /// Add a row to the set.
        /// </summary>
        /// <param name="r">the row to add</param>
        internal void Add((long,TRow) r)
        {
            rows+=r;
        }

        protected override RowBookmark _First(Context _cx)
        {
            return ExplicitRowBookmark.New(_cx,this,0);
        }

        internal class ExplicitRowBookmark : RowBookmark
        {
            readonly ExplicitRowSet _ers;
            readonly int _i;
            readonly TRow _row;

            public override TRow row => _row;

            public override TRow key => _row;

            ExplicitRowBookmark(Context _cx, ExplicitRowSet ers,int pos,int i) 
                :base(_cx,ers,pos,ers.rows[i].Item1)
            {
                _ers = ers;
                _i = i;
                _row = ers.rows[i].Item2;
                _cx.Add(ers.qry, this);
            }
            internal static ExplicitRowBookmark New(Context _cx,ExplicitRowSet ers,int i)
            {
                if (i<0) 
                    return null;
                for (;i<ers.rows.Count;i++)
                {
                    var rb = new ExplicitRowBookmark(_cx,ers, 0, i);
                    if (rb.Matches() && Query.Eval(ers.qry.where, ers._tr, _cx))
                        return rb;
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                var ers = _rs as ExplicitRowSet;
                for (var i = _i+1; i < ers.rows.Count; i++)
                {
                    var rb = new ExplicitRowBookmark(_cx,ers, _pos+1, i);
                    if (rb.Matches() && Query.Eval(ers.qry.where, ers._tr, _cx))
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
    /// Deal with execution of Triggers. The main complication here is that triggers execute in definer's role
    /// so the column names and data types will be different for different triggers.
    /// (Of course many trigger actions will be to different tables.)
    /// </summary>
    internal class TransitionRowSet : RowSet
    {
        public override string keywd()
        {
            return " Transition ";
        }
        internal readonly BTree<long, TypedValue> defaults = BTree<long, TypedValue>.Empty; 
        internal readonly Query table; // will be a SqlInsert, QuerySearch or UpdateSearch
        readonly PTrigger.TrigType _tgt;
        internal readonly BTree<long, TriggerActivation> tb, ti, ta;
        internal readonly Selector pkauto;
        internal readonly Index index;
        internal readonly Adapters _eqs;
        internal readonly bool autokey;
        internal TransitionRowSet(Transaction tr,Context cx,Query q, PTrigger.TrigType tg, Adapters eqs,bool autokey)
            : base(tr,cx,q)
        {
            table = q;
            _eqs = eqs;
            var t = table as Table ?? table.simpleQuery as Table;
            // Lookup the autoidentity if any (since we allow override for this)
            index = t.FindPrimaryIndex();
            pkauto = null;
            var tx0 = index?.cols[0] as Selector;
            if (index != null && index.cols.Count == 1 && 
                index.cols[0].domain.kind == Sqlx.INTEGER)
                pkauto = tx0;
            // check now about conflict with generated columns
            if (q.Denied(tr,Grant.Privilege.Insert))
                throw new DBException("42105",q);
            var dt = q.rowType;
            for (int i = 0; i < dt.Length; i++) // at this point q is the insert statement, simpleQuery is the base table
                if (dt.columns[i] is TableColumn tc)
                {
                    if (tc != pkauto && tc.generated != PColumn.GenerationRule.No)
                        throw (tr as Transaction).Exception("0U000", tc.name).Mix();
                    var df = tc.domain.defaultValue;
                    if (tc != pkauto && tc.generated == PColumn.GenerationRule.No)
                        defaults += (tc.defpos, df);
                }
            _tgt = tg;
            tb = Setup(tr, q, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.Before]);
            ti = Setup(tr, q, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.Instead]);
            ta = Setup(tr, q, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.After]);
        }
        protected override RowBookmark _First(Context _cx)
        {
            return TransitionRowBookmark.New(_cx,this);
        }

        /// <summary>
        /// Set up activations for executing a set of triggers
        /// </summary>
        /// <param name="q"></param>
        /// <param name="tgs"></param>
        /// <returns></returns>
        BTree<long, TriggerActivation> Setup(Transaction tr,Query q, BTree<long, Trigger> tgs)
        {
            var r = BTree<long, TriggerActivation>.Empty;
            var cx = new Context(tr);
            if (tgs != null)
                for (var tg = tgs.First(); tg != null; tg = tg.Next())
                    r +=(tg.key(), new TriggerActivation(cx,this, tg.value()));
            return r;
        }
        /// <summary>
        /// Perform the triggers in a set
        /// </summary>
        /// <param name="acts"></param>
        Transaction Exec(Transaction tr,Context _cx, BTree<long, TriggerActivation> acts)
        {
            var r = false;
            for (var a = acts.First(); a != null; a = a.Next())
            {
                var nt = a.value().Exec(tr, _cx);
                if (nt != _tr)
                    r = true;
                tr = nt;
            }
            _cx.ret = TBool.For(r);
            return tr;
        }
        internal Transaction InsertSA(Transaction tr,Context _cx)
        { return Exec(tr, _cx,ta); }
        internal Transaction InsertSB(Transaction tr,Context _cx)
        { tr = Exec(tr,_cx,tb); return Exec(tr,_cx,ti); }
        internal Transaction UpdateSA(Transaction tr,Context _cx)
        { return Exec(tr,_cx,ta); }
        internal Transaction UpdateSB(Transaction tr,Context _cx)
        { tr = Exec(tr,_cx,tb); return Exec(tr,_cx,ti); }
        internal Transaction DeleteSB(Transaction tr,Context _cx)
        { tr = Exec(tr,_cx,tb); return Exec(tr,_cx,ti); }
        internal class TransitionRowBookmark : RowBookmark
        {
            readonly TransitionRowSet _trs;
            readonly RowBookmark _fbm;
            readonly TRow _row, _key;
            /// <summary>
            /// There may be several triggers of any type, so we manage a set of transitition activations for each.
            /// These are for table before, table instead, table after, row before, row instead, row after.
            /// </summary>
            internal readonly BTree<long, TriggerActivation> rb, ri, ra;
            internal readonly BTree<long,TypedValue> oldRow = BTree<long, TypedValue>.Empty; // computed from Session role
            internal readonly BTree<long, TypedValue> newRow = BTree<long, TypedValue>.Empty; // computed from Session role
            public override TRow row => _row;
            public override TRow key => _key;
            TransitionRowBookmark(Context _cx,TransitionRowSet trs, int pos, RowBookmark fbm) 
                : base(_cx,trs, pos, fbm._defpos)
            {
                _trs = trs;
                _fbm = fbm;
                var dt = trs.qry.rowType;
                var oldRow = BTree<long, TypedValue>.Empty;
                if (trs.qry.cols == BList<SqlValue>.Empty)
                    oldRow = fbm.row.values;
                else
                {
                    for (int i = 0; i < trs.qry.display; i++)
                    {
                        TypedValue tv = fbm.row[i];
                        var sl = trs.qry.cols[i];
                        if (sl is SqlProcedureCall sv)
                        {
                            var fp = sv.call.proc.defpos;
                            var m = _trs._eqs.Match(fp, sl.defpos);
                            if (m.HasValue)
                            {
                                if (m.Value == 0)
                                    tv = fbm.row[sl.defpos];
                                else // there's an adapter function
                                {
                                    // tv = fn (fbm[j])
                                    var pr = trs._tr.role.objects[m.Value] as Procedure;
                                    var ac = new CalledActivation(trs._tr, _cx, pr, Domain.Null);
                                    tv = pr.body.Eval(_trs._tr, ac);
                                }
                            }
                        }
                        if (tv == null)
                            tv = trs.defaults[sl.defpos];
                        tv = sl.nominalDataType.Coerce(tv);
                        oldRow += (sl.defpos, tv);
                    }
                }
      //          if (trs.index != null)
      //              trs._tr.CheckPrimaryKey(trs._tr,ref oldRow, trs.index,trs.autokey);
                newRow = oldRow;
                _row = new TRow(dt, oldRow);
                _key = new TRow(trs.keyType, oldRow);
                _cx.Add(trs.qry,this);
                var q = trs.table as Table;
                // Get the trigger sets and set up the activations
                rb = Setup(trs._tr,q, q.triggers[trs._tgt | PTrigger.TrigType.EachRow | PTrigger.TrigType.Before]);
                ri = Setup(trs._tr,q, q.triggers[trs._tgt | PTrigger.TrigType.EachRow | PTrigger.TrigType.Instead]);
                ra = Setup(trs._tr,q, q.triggers[trs._tgt | PTrigger.TrigType.EachRow | PTrigger.TrigType.After]);
            }
            internal static TransitionRowBookmark New(Context _cx,TransitionRowSet trs)
            {
                var from = trs.qry;
                for (var fbm = _cx.rb; fbm != null; fbm = fbm.Next(_cx))
                {
                    if (fbm.Matches() && Query.Eval(from.where,trs._tr,_cx))
                        return new TransitionRowBookmark(_cx,trs, 0, fbm);
                }
                return null;
            }
            /// <summary>
            /// Set up activations for executing a set of triggers
            /// </summary>
            /// <param name="q"></param>
            /// <param name="tgs"></param>
            /// <returns></returns>
            BTree<long, TriggerActivation> Setup(Transaction tr,Query q, BTree<long, Trigger> tgs)
            {
                var r = BTree<long, TriggerActivation>.Empty;
                var cx = new Context(tr);
                if (tgs != null)
                    for (var tg = tgs.First(); tg != null; tg = tg.Next())
                    {
                        var trg = tg.value();
                        var ta = new TriggerActivation(cx,_trs, trg);
                        r +=(tg.key(), ta);
                    }
                return r;
            }
            public override RowBookmark Next(Context _cx)
            {
                var from = _trs.qry;
                if(from.where.First()?.value() is SqlValue sv && sv.nominalDataType.kind == Sqlx.CURRENT)
                        return null;
                for (var fbm = _fbm.Next(_cx); fbm != null; fbm = fbm.Next(_cx))
                {
                    var ret = new TransitionRowBookmark(_cx,_trs, 0, fbm);
                    for (var b = from.where.First(); b != null; b = b.Next())
                        if (b.value().Eval(_trs._tr, _cx) != TBool.True)
                            goto skip;
                    return ret;
                    skip:;
                }
                return null;
            }
            internal override TableRow Rec()
            {
                return _fbm.Rec();
            }
            /// <summary>
            /// Some convenience functions for calling from Transaction.Execute(..)
            /// </summary>
            internal Transaction InsertRA(Transaction tr,Context _cx)
            { return _trs.Exec(tr, _cx,ra); }
            internal Transaction InsertRB(Transaction tr,Context _cx)
            { _cx.row = _cx.rb.row;  tr = _trs.Exec(tr,_cx,rb); return _trs.Exec(tr,_cx,ri); }
            internal Transaction UpdateRA(Transaction tr,Context _cx)
            { return _trs.Exec(tr,_cx,ra); }
            internal Transaction UpdateRB(Transaction tr,Context _cx)
            { tr = _trs.Exec(tr,_cx,rb); return _trs.Exec(tr,_cx,ri); }
            internal Transaction DeleteRB(Transaction tr,Context _cx)
            { tr = _trs.Exec(tr,_cx,rb); return _trs.Exec(tr,_cx,ri); }

        }
    }
    internal class SelectedKeyRowSet : RowSet
    {
        public override string keywd()
        {
            return " SelectedKey ";
        }
        readonly RowSet source;
        readonly TRow key;
        internal SelectedKeyRowSet(Context _cx,Query q, RowSet r, TRow k)
            : base(r._tr, _cx, q, r.rowType, r.keyType)
        {
            source = r;
            key = k;
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("SelectedKey ");
            sb.Append(key.ToString());
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        protected override RowBookmark _First(Context _cx)
        {
            return new SelectedKeyRowBookmark(_cx,this,source.PositionAt(_cx,new PRow(key)));
        }
        internal class SelectedKeyRowBookmark : RowBookmark
        {
            readonly SelectedKeyRowSet _skr;
            readonly RowBookmark _bmk;
            readonly Domain _dt;
            readonly Domain _qt;
            readonly TRow _row,_key;
            internal SelectedKeyRowBookmark(Context _cx,SelectedKeyRowSet rs, RowBookmark bmk) 
                : base(_cx,rs, bmk._pos, bmk._defpos)
            {
                _skr = rs;
                _bmk = bmk;
                _qt = rs.rowType;
                _dt = rs.keyType ?? _qt;
            }

            public override TRow row => _row;

            public override TRow key => _key;

            public override RowBookmark Next(Context _cx)
            {
                var rb = _bmk.Next(_cx);
                var k = new PRow(_skr.key);
                for (int i = 0; i < _dt.Length && k != null; i++, k = k._tail)
                    for (int j = 0; j < _qt.Length; j++)
                    {
                        var n = _qt.columns[j].defpos;
                        if (rb.row[n].CompareTo(k._head) != 0)
                            return null;
                    }
                return new SelectedKeyRowBookmark(_cx,_skr, rb);
            }
            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
    internal class SortedRowSet : RowSet
    {
        public override string keywd()
        {
            return " Sorted ";
        }
        internal MTree tree;
        RowSet source;
        TreeInfo info;
        List<TRow> rows = new List<TRow>();
        List<Rvv> rvvs = new List<Rvv>();
        internal SortedRowSet(Context _cx,Query q, RowSet s, TreeInfo ti)
            : base(s._tr, _cx, q, s.rowType, ti.headType)
        {
            tree = new MTree(ti);
            source = s;
            info = ti;
            Build(q.defpos);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Sorted ");
            sb.Append(keyType.ToString());
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        internal override int? Count => rows.Count;
        protected override void Build(Context _cx)
        {
            for (var a = source.First(_cx);a!= null;a=a.Next(_cx))
            {
                var v = a.row;
                PRow k = null;
                for (int i = keyType.Length-1;i>=0;i--)
                    k = new PRow(v[keyType.columns[i].defpos], k);
                MTree.Add(ref tree, k, rows.Count);
                rows.Add(v);
            }
        }

        protected override RowBookmark _First(Context _cx)
        {
            return SortedRowBookmark.New(_cx,this);
        }
        public override RowBookmark PositionAt(Context _cx,PRow key)
        {
            return SortedRowBookmark.New(_cx,this,key);
        }
        internal class SortedRowBookmark : RowBookmark
        {
            readonly MTreeBookmark _mbm;
            readonly TRow _row, _key;
            SortedRowBookmark(Context _cx, RowSet srs,int pos,MTreeBookmark mbm,long dpos) 
                :base(_cx,srs,pos,dpos)
            {
                _mbm = mbm;
            }

            public override TRow row => _row;

            public override TRow key => _key;

            internal static SortedRowBookmark New(Context _cx,SortedRowSet srs)
            {
                for (MTreeBookmark mbm = srs.tree.First();mbm!=null;mbm=mbm.Next())
                {
                    var rvv = srs.rvvs[(int)mbm.Value().Value];
                    var d = (rvv != null) ? rvv.def : 0;
                    var rb = new SortedRowBookmark(_cx,srs, 0, mbm, d);
                    if (rb.Matches() && Query.Eval(srs.qry.where, srs._tr,_cx))
                        return rb;
                }
                    return null;
            }
            internal static SortedRowBookmark New(Context _cx,SortedRowSet srs,PRow key)
            {
                for (var mbm = srs.tree.PositionAt(key); mbm != null; mbm = mbm.Next())
                    if (mbm.Value().HasValue)
                    {
                        var rvv = srs.rvvs[(int)mbm.Value().Value];
                        var d = (rvv != null) ? rvv.def : 0;
                        return new SortedRowBookmark(_cx,srs, 0, mbm, d);
                    }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var mbm = _mbm.Next(); mbm != null; mbm = mbm.Next())
                {
                    var rvv = ((SortedRowSet)_rs).rvvs[(int)mbm.Value().Value];
                    var d = (rvv != null) ? rvv.def : 0;
                    var rb = new SortedRowBookmark(_cx,_rs, _pos + 1, mbm, d);
                    if (rb.Matches() && Query.Eval(_rs.qry.where, _rs._tr, _cx))
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
    /// <summary>
    /// RoutineCallRowSet is a table-valued routine call
    /// </summary>
    internal class RoutineCallRowSet : RowSet
    {
        public override string keywd()
        {
            return " RoutineCall ";
        }
        Query from;
        Procedure proc;
        BList<SqlValue> actuals;
        RowSet rowSet;
        internal RoutineCallRowSet(Transaction tr,Context cx,Query f,Procedure p, BList<SqlValue> r) 
            :base(tr,cx,f,f.rowType)
        {
            from = f;
            proc = p;
            actuals = r;
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("RoutineCall ");
            sb.Append(proc.name);
            var cm = '(';
            for(var b = actuals.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ',';
                sb.Append(b.value());
            }
            base._Strategy(sb, indent);
        }
        protected override RowBookmark _First(Context _cx)
        {
            _tr = proc.Exec(_tr,_cx,actuals);
            if (_cx.data == null)
                throw new DBException("22004").Mix();
            rowSet = _cx.data;
            return rowSet.First(_cx);
        }
    }
    internal class RowSetSection : RowSet
    {
        public override string keywd()
        {
            return " Section ";
        }
        RowSet source;
        readonly int offset,count;
        internal RowSetSection(Context _cx,RowSet s, int o, int c)
            : base(s._tr,_cx,s.qry,s.rowType,s.keyType)
        {
            source = s; offset = o; count = c;
        }

        protected override RowBookmark _First(Context _cx)
        {
            var b = source.First(_cx);
            for (int i = 0; b!=null && i < offset; i++)
                b = b.Next(_cx);
            return b;
        }
    }
#if REFLECTION
    internal class ReflectionRowSet : RowSet
    {
        public override string keywd()
        {
            return " Reflection ";
        }
        RowSet source;
        Query from;
        Ident[] cols;
        Index index1 = null, index2 = null;
        Table mtable = null;
        internal ReflectionRowSet(RowSet s, Query f, string[] c)
            : base(s._tr,s._cx,s.qry)
        {
            source = s;
            from = f;
            cols = new Ident[c.Length];
            for (var i = 0; i < c.Length; i++)
                cols[i] = new Ident(c[i],0);
            Build();
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Reflection ");
            sb.Append(from.ToString());
            var cm = "(";
            foreach (var id in cols)
            {
                sb.Append(cm); cm = "'";
                sb.Append(id.ToString());
            }
            sb.Append(")");
            if (index1 != null)
                sb.Append(" "+index1.ToString());
            if (index2 != null)
                sb.Append(" "+index2.ToString());
            if (mtable != null)
                sb.Append(" " + mtable.ToString());
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        protected override void Build()
        {
            var tb = qry as Table;
            for (var ip = tb.indexes.First(); ip != null; ip = ip.Next())
            {
                var ix = ip.value();
                // get our bearings: Case 1: f has a foreign key referencing s.table
                // cols if specified gives the columns in f making up this foreign key
                // index1 has reftable s.table and table f.table
                if (ix.reftabledefpos == from.defpos && ix.tabledefpos == from.defpos && cols != null)
                {
                    if (cols.Length != ix.cols.Count)
                        goto skip;
                    for (int i = 0; i < ix.cols.Count; i++)
                    {
                        var tc = from.ValFor(cols[i]);
                        if (ix.cols[i] != tc.name.Defpos())
                            goto skip;
                    }
                    index1 = ix;
                    return;
                }

                // case 2: there is a table M with foreign keys for s.table and f.table
                // cols if specified contains just one entry naming M (not a column at all)
                // index1 has reftable f.table and table M, index2 has reftable f.table and table M
                if (ix.reftabledefpos == from.target.defpos)
                {
                    mtable = db.GetObject(ix.tabledefpos) as Table;
                    if (cols != null && ix.tabledefpos != from.ValFor(cols[0]).name.Defpos()) // wow.. i doubt if this can work
                        continue;
                    for (var ip2 = db.indexes.First(); ip2 != null; ip2 = ip2.Next())
                    {
                        var ix2 = db.GetObject(ip2.key()) as Index;
                        if (ix2.reftabledefpos == from.target.defpos && ix2.tabledefpos == mtable.defpos)
                        {
                            index1 = ix;
                            index2 = ix2;
                            return;
                        }
                    }
                }
                skip:;
            }
            throw new DBException("42111").Mix();
        }
        protected override RowBookmark _First()
        {
            qry.row = null;
            if (index2 != null)
                return ManyManyRowBookmark.New(this);
            return ReflectionRowBookmark.New(this);
        }

        internal class ReflectionRowBookmark : RowBookmark
        {
            readonly RowBookmark _smk;
            readonly MTreeBookmark _mbm;
            ReflectionRowBookmark(ReflectionRowSet rrs,long pos,RowBookmark smk,
                MTreeBookmark mbm,long dpos) :base(rrs,pos,dpos)
            {
                _smk = smk; _mbm = mbm;
            }
            internal static ReflectionRowBookmark New(ReflectionRowSet rrs)
            {
                for (var smk = rrs.source.First(); smk != null; smk = smk.Next())
                    for (var mbm = rrs.index1.rows.PositionAt(rrs.tr,rrs.source.Key(smk));
                        mbm!= null;mbm=mbm.Next(rrs.tr))
                        if (mbm.Value().HasValue)
                            return new ReflectionRowBookmark(rrs, 0, smk, mbm, mbm.Value().Value);
                return null;
            }
            public override RowBookmark Next()
            {
                var mbm = _mbm;
                var smk = _smk;
                var rrs = _rs as ReflectionRowSet;
                for (;;)
                {
                    if (mbm != null)
                        mbm = mbm.Next(_rs.tr);
                    if (mbm != null)
                        break;
                    smk = smk.Next();
                    if (smk == null)
                        return Null();
                    mbm = rrs.index1.rows.PositionAt(rrs.tr,rrs.source.Key(_smk));
                    if (mbm != null)
                        break;
                }
                var d = mbm.Value() ?? 0;
                return new ReflectionRowBookmark(rrs, _pos + 1, _smk, mbm, d);
            }
            internal override TypedValue Get(Ident n)
            {
                return _smk.Get(n);
            }
            public override Rvvs _Rvv()
            {
                return (_rs.qry as From).row._Rvv();
            }

            internal override void Close(Transaction tr)
            {
                _smk.Close(tr);
                base.Close(tr);
            }
        }
        internal class ManyManyRowBookmark : RowBookmark
        {
            readonly RowBookmark _smk;
            readonly MTreeBookmark _mbm;
            ManyManyRowBookmark(ReflectionRowSet rrs, long pos, RowBookmark smk, 
                MTreeBookmark mbm,long dpos) :base(rrs,pos,dpos)
            {
                _smk = smk; _mbm = mbm;
            }
            internal static ManyManyRowBookmark New(ReflectionRowSet rrs)
            {
                for (var smk = rrs.source.First(); smk != null; smk = smk.Next())
                    for (var mbm = rrs.index1.rows.PositionAt(rrs.tr,rrs.source.Key(smk)); mbm != null; mbm = mbm.Next(rrs.tr))
                        if (mbm.Value().HasValue)
                            return new ManyManyRowBookmark(rrs, 0, smk, mbm, mbm.Value().Value);
                return null;
            }
            public override RowBookmark Next()
            {
                var mbm = _mbm;
                var smk = _smk;
                var rrs = _rs as ReflectionRowSet;
                for (;;)
                {
                    if (mbm != null)
                        mbm = mbm.Next(_rs.tr);
                    if (mbm != null)
                    {
                        if (mbm.Value().HasValue)
                            break;
                        continue;
                    }
                    smk = smk.Next();
                    if (smk == null)
                        return Null();
                    mbm = rrs.index1.rows.PositionAt(rrs.tr,rrs.source.Key(_smk));
                    if (mbm != null && mbm.Value().HasValue)
                        break;
                }
                return new ManyManyRowBookmark(rrs, _pos + 1, smk, mbm,mbm.Value().Value);
            }
            internal override TypedValue Get(Ident n)
            {
                var rrs = _rs as ReflectionRowSet;
                var d = rrs.tr.Db(rrs.from.target.dbix);
                var rc = d.GetD(_mbm.Value().Value) as Record; // in M
                var k2 = rc.MakeKey(rrs.index2.cols);
                var rc2 = d.GetD(rrs.index2.rows.Get(_rs.tr,k2).Value) as Record; // in f.table
                return rrs.from.RowFor(rrs.tr, rc2)?[n];
            }
            public override Rvvs _Rvv()
            {
                var rrs = _rs as ReflectionRowSet;
                var d = rrs.tr.Db(rrs.from.target.dbix);
                var rc = d.GetD(_mbm.Value().Value) as Record; // in M
                var k2 = rc.MakeKey(rrs.index2.cols);
                var rc2 = d.GetD(rrs.index2.rows.Get(_rs.tr,k2).Value) as Record; // in f.table
                return rrs.from.RowFor(rrs.tr,rc2)?.rvv;
            }

            internal override void Close(Transaction tr)
            {
                _smk.Close(tr);
                base.Close(tr);
            }
        }
    }
#endif
    internal class DocArrayRowSet : RowSet
    {
        public override string keywd()
        {
            return " DocArray ";
        }
        internal BList<SqlValue> vals = BList<SqlValue>.Empty;
        internal Domain dt = Domain.Content;
        internal DocArrayRowSet(Transaction tr,Context cx,Query q, SqlDocArray d)
            : base(tr,cx,q)
        {
            if (d != null)
                for(int i=0;i<d.Length;i++)
                    Add(d[""+i]);
        }
        internal void Add(SqlValue c)
        {
            if (dt.kind == Sqlx.Null)
                dt = c.nominalDataType;
            else if (dt.kind != c.nominalDataType.kind)
                throw new DBException("22005T", dt.kind.ToString(), c.nominalDataType.kind.ToString()).ISO();
            vals+=c;
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
        protected override RowBookmark _First(Context _cx)
        {
            return DocArrayBookmark.New(_cx,this);
        }

        internal class DocArrayBookmark : RowBookmark
        {
            readonly ABookmark<int, SqlValue> _bmk;
            readonly TRow _row, _key;

            DocArrayBookmark(Context _cx,DocArrayRowSet drs, ABookmark<int, SqlValue> bmk) 
                :base(_cx,drs,bmk.key(),0)
            {
                _bmk = bmk;
                _row = new TRow(Domain.Content, bmk.value().Eval(drs._tr, _cx));
                _key = _row;
            }

            public override TRow row => _row;

            public override TRow key => _key;

            internal static DocArrayBookmark New(Context _cx,DocArrayRowSet drs)
            {
                var bmk = drs.vals.First();
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,drs, bmk);
            }
            public override RowBookmark Next(Context _cx)
            {
                var drs = _rs as DocArrayRowSet;
                var bmk = ABookmark<int, SqlValue>.Next(_bmk, drs.vals);
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,drs, bmk);
            }

            internal override TableRow Rec()
            {
                throw new NotImplementedException();
            }
        }
    }
}