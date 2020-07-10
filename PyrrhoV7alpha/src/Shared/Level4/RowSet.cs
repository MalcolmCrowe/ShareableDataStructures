using System;
using System.Collections.Generic;
using Pyrrho.Common;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Level3;
using System.CodeDom.Compiler;
using System.IO;
using System.Runtime.CompilerServices;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
namespace Pyrrho.Level4
{
    /// <summary>
    /// A RowSet is the result of a stage of query processing. 
    /// We take responsibility for cx.values in the finder list.
    /// 
    /// RowSet is a subclass of TypedValue so inherits domain and a column ordering.
    /// RowSet Domains have fully specified representation and defpos matches the
    /// rowSet.
    /// TableRowSet, IndexRowSet and TransitionRowSet access physical columns
    /// and ignore the column ordering.
    /// Other RowSets must not contain any physical uids in their column uids
    /// or within their domain's representation.
    /// 
    /// RowSet defpos (uids) are carefully managed:
    /// SystemRowSet uids match the relevant System Table (thus a negative uid)
    /// TableRowSet uids match the table defining position
    /// IndexRowSet uids match the index defpos
    /// TransitionRowSet is somewhere on the heap (accessible transition and target contexts)
    /// EmptyRowSet uid is -1
    /// The following rowsets always have physical or lexical uids and
    /// rowtype and domain information comes from the syntax:
    /// TrivialRowSet, SelectedRowSet, JoinRowSet, MergeRowSet, 
    /// SelectRowSet, EvalRowSet, GroupRowSet, TableExpRowSet, ExplicitRowSet.
    /// All other rowSets get their defining position by cx.nextId++
    /// 
    /// IMMUTABLE
    /// </summary>
    internal abstract class RowSet : TypedValue
    {
        internal readonly long defpos; // uid see above
        internal readonly RowType rt; 
        internal readonly RowType keys; // defpos -1 unless same as RowSet
        internal readonly OrderSpec rowOrder; 
        internal readonly BTree<long, bool> where; 
        internal readonly BTree<long,TypedValue> matches;
        internal readonly BTree<long, BTree<long, bool>> matching;
        internal readonly GroupSpecification grouping;
        /// <summary>
        /// We use rowSet uid instead of actual RowSet because of order of construction
        /// </summary>
        internal readonly struct Finder 
        {
            public readonly long col;
            public readonly long rowSet; 
            public Finder(long c, long r) { col = c; rowSet = r; }
            public override string ToString()
            {
                return DBObject.Uid(rowSet) + "[" + DBObject.Uid(col) + "]";
            }
        }
        /// <summary>
        /// The finder maps the list of SqlValues whose values are in cursors,
        /// to a Finder.
        /// In a Context there may be several rowsets being calculated independently
        /// (e.g. a join of subqueries), and it can combine sources into a from association
        /// saying which rowset (and so which cursor) contains the current values.
        /// We could simply search along cx.data to pick the rowset we want.
        /// Before we compute a Cursor, we place the source finder in the  Context, 
        /// so that the evaluation can use values retrieved from the source cursors.
        /// </summary>
        internal readonly BTree<long, Finder> finder; // what we provide
        internal virtual int display => rt.Length; // after Build if any
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dp">The uid for the RowSet</param>
        /// <param name="rt">The way for a cursor to calculate a row</param>
        /// <param name="kt">The way for a cursor to calculate a key (by default same as rowType)</param>
        /// <param name="or">The ordering of rows in this rowset</param>
        /// <param name="wh">A set of boolean conditions on row values</param>
        /// <param name="ma">A filter for row column values</param>
        protected RowSet(long dp, Context cx, Domain dt, RowType qt,
            BTree<long,Finder> fin=null, RowType kt=null, 
            BTree<long,bool> wh=null,OrderSpec or=null,
            BTree<long,TypedValue>ma=null, 
            BTree<long,BTree<long,bool>> mg=null,
            GroupSpecification gp=null) :base(dt)
        {
            defpos = dp;
            rt = qt;
            keys = kt ?? rt;
            rowOrder = or??OrderSpec.Empty;
            where = wh??BTree<long,bool>.Empty;
            matches = ma??BTree<long,TypedValue>.Empty;
            matching = mg ?? BTree<long, BTree<long, bool>>.Empty;
            grouping = gp;
            fin = fin ?? BTree<long, Finder>.Empty;
            for (var b = rt?.First(); b != null; b = b.Next())
            {
                var p = b.value().Item1;
                fin += (p, new Finder(p,defpos));
            }
            finder = fin;
            cx.data += (dp, this);
            cx.val = this;
        }
        protected RowSet(long dp, Context cx, RowSet rs) 
            : this(dp, cx, rs, rs.rt, rs.keys) { }
        RowSet(long dp,Context cx,RowSet rs,RowType inf,RowType keyInf) :base(rs.dataType)
        {
            defpos = dp;
            rt = inf;
            keys = keyInf;
            rowOrder = rs.rowOrder;
            where = rs.where;
            matches = rs.matches;
            matching = rs.matching;
            grouping = rs.grouping;
            cx.data += (dp, this);
            cx.val = this;
        }
        protected RowSet(RowSet rs,long a,long b):base(rs.dataType)
        {
            defpos = rs.defpos;
            rt = rs.rt;
            keys = rs.keys;
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
        internal virtual bool TableColsOk => false;
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
        public string NameFor(Context cx, int i)
        {
            var p = rt[i].Item1;
            var sv = cx.obs[p];
            var n = sv?.alias ?? (string)sv?.mem[Basis.Name];
            return n ?? (cx.db.role.infos[p] as ObInfo)?.name ?? ("Col"+i);
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
        protected bool SameOrder(Context cx,Query q,RowSet sce)
        {
            if (rowOrder == null || rowOrder.items.Length == 0)
                return true;
            return rowOrder.SameAs(cx,q,sce?.rowOrder);
        }
        /// <summary>
        /// Compute schema information (for the client) for this row set.
        /// 
        /// </summary>
        /// <param name="flags">The column flags to be filled in</param>
        internal void Schema(Context cx, int[] flags)
        {
            int m = rt.Length;
            bool addFlags = true;
            var adds = new int[flags.Length];
            // see if we are going to add index flags stuff
            var j = 0;
            for (var ib = keys.First(); ib != null; 
                ib = ib.Next(), j++)
            {
                var found = false;
                for (var b = rt.First(); b != null; 
                    b = b.Next())
                    if (b.key() == ib.key())
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
                var cp = rt[i].Item1;
                var dc = dataType.representation[cp];
                if (dc.prim == Sqlx.SENSITIVE)
                    dc = dc.elType;
                flags[i] = dc.Typecode() + (addFlags ? adds[i] : 0);
                if (cx.db.objects[cp] is TableColumn tc)
                    flags[i] += ((tc.notNull) ? 0x100 : 0) +
                        ((tc.generated != GenerationRule.None) ? 0x200 : 0);
            }
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
            for (var bm = First(_cx); bm!=null; bm=bm.Next(_cx))
            {
                var k = key;
                for (var b=keys.First();b!=null;b=b.Next())
                    if (_cx.obs[b.value().Item1].Eval(_cx).CompareTo(k._head) != 0)
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
        internal void Conds(StringBuilder sb,BTree<long,bool> conds,string cm)
        {
            for (var b = conds.First(); b != null; b = b.Next())
            {
                sb.Append(cm);
                sb.Append(DBObject.Uid(b.key()));
                cm = " and ";
            }
        }
        internal void Matches(StringBuilder sb)
        {
            var cm = " match ";
            for (var b = matches.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(b.key())); sb.Append("=");
                sb.Append(b.value());
            }
        }
        internal void Strategy(int indent)
        {
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(' ');sb.Append(DBObject.Uid(defpos));
            sb.Append(rt);
            var cm = "";
            if (keys!=rt && keys.Count!=0)
            {
                cm = " key (";
                for (var b = keys.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(DBObject.Uid(b.value().Item1));
                }
                sb.Append(")");
            }
            cm = "(";
            sb.Append(" Finder: ");
            for (var b=finder.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = "],";
                sb.Append(DBObject.Uid(b.key()));
                var f = b.value();
                sb.Append("="); sb.Append(DBObject.Uid(f.rowSet));
                sb.Append('['); sb.Append(DBObject.Uid(f.col));
            }
            sb.Append("])");
            return sb.ToString();
        }
        internal virtual string ToString(Context cx,int n)
        {
            return ToString();
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
        public readonly long _defpos;
        public Cursor(Context _cx,RowSet rs,int pos,long defpos,TRow rw)
            :base(rs,rw.values)
        {
            _rowsetpos = rs.defpos;
            _pos = pos;
            _defpos = defpos;
            _cx.cursors += (rs.defpos, this);
        }
        public Cursor(Context _cx, RowSet rs, int pos, long defpos, TypedValue[] vs)
            : base(rs, vs)
        {
            _rowsetpos = rs.defpos;
            _pos = pos;
            _defpos = defpos;
            _cx.cursors += (rs.defpos, this);
        }
        // a more detailed version for trigger-side transition cursors
        protected Cursor(Context cx,long rd,int pos,long defpos,TRow rw)
            :base(cx.data[rd],rw.values)
        {
            _rowsetpos = rd;
            _pos = pos;
            _defpos = defpos;
            cx.cursors += (rd, this);
        }
        public Cursor(Context _cx, long rp, int pos, long defpos, RowType cols, Domain dt, TRow rw)
    : base(cols,dt,rw.values)
        {
            _rowsetpos = rp;
            _pos = pos;
            _defpos = defpos;
            _cx.cursors += (rp, this);
        }
        protected Cursor(Cursor cu,Context cx,long p,TypedValue v) 
            :base (cu.columns,cu.dataType,cu.values+(p,v))
        {
            _rowsetpos = cu._rowsetpos;
            _pos = cu._pos;
            _defpos = cu._defpos;
            cx.cursors += (cu._rowsetpos, this);
        }
        public static Cursor operator+(Cursor cu,(Context,long,TypedValue)x)
        {
            return cu.New(x.Item1,x.Item2,x.Item3);
        }
        protected abstract Cursor New(Context cx,long p, TypedValue v);
        internal bool Matches(Context cx)
        {
            var rs = cx.data[_rowsetpos];
            for (var b = rs.matches.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Eval(cx).CompareTo(b.value()) != 0)
                    return false;
            for (var b = rs.where.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Eval(cx) != TBool.True)
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
        internal string NameFor(Context cx,int i)
        {
            var rs = cx.data[_rowsetpos];
            var p = rs.rt[i].Item1;
            var ob = (SqlValue)cx.obs[p];
            return ob.name;
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
        internal TrivialRowSet(long dp, Context cx, RowType cols, TRow r, 
            long rc, BTree<long,Finder> fi) : base(dp, cx, r.dataType,cols,fi)
        {
            row = r??new TRow(ObInfo.Any);
            here = new TrivialCursor(cx,this,0,rc);
            cx.data+=(defpos,this);
        }
        internal TrivialRowSet(long dp, Context cx, RowType cols, 
            Domain dt, TRow r, long rc, BTree<long,Finder>fi)
            : base(dp, cx, dt, cols,fi)
        {
            row = r ?? new TRow(cols,dt,BTree<long,TypedValue>.Empty);
            here = new TrivialCursor(cx, this, 0, rc);
            cx.data += (defpos, this);
        }
        internal TrivialRowSet(long dp,Context cx, Record rec) 
            : this(dp,cx, cx.Signature(rec.tabledefpos), 
                  cx.Dom(rec.tabledefpos), 
                  new TRow(cx.db.role.infos[rec.tabledefpos] as ObInfo, rec.fields), 
                  rec.defpos,BTree<long,Finder>.Empty)
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
            return new TrivialRowSet(dp,cx,rt,row,rc,finder);
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Trivial ");
            sb.Append(row.ToString());
            base._Strategy(sb, indent);
        }
        internal override int? Count => 1;

        public override bool IsNull => throw new NotImplementedException();

        internal override bool TableColsOk => true;

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
            TrivialCursor(TrivialCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
            {
                trs = cu.trs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TrivialCursor(this, cx, p, v);
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
        internal readonly RowType rowType;
        internal readonly RowSet source;
        // qSmap if it existed would be the same as this.finder
        internal readonly BTree<long, Finder> sQmap;
        internal readonly BTree<long, DBObject> _obs;
        internal readonly Domain _domain;
        /// <summary>
        /// This constructor builds a rowset for the given Table
        /// directly using its defpos, rowType, ordering, where and match info.
        /// Suggestion here is to use the source keyType. Maybe the source ordering too?
        /// </summary>
        internal SelectedRowSet(Context cx,Query q,RowSet r,BTree<long,Finder> fi)
            :base(q.defpos, cx, q.domain,q.rowType, _Fin(cx,q,fi),null,q.where,
                 q.ordSpec,q.matches)
        {
            source = r;
            rowType = q.rowType;
            var sq = BTree<long, Finder>.Empty;
            for (var b=rowType?.First();b!=null;b=b.Next())
            {
                var p = b.value().Item1;
                if (cx.obs[p] is SqlCopy sc)
                {
                    var s = sc.copyFrom;
                    sq += (p, new Finder(s,source.defpos));
                }
            }
            sQmap = sq;
            _obs = cx.obs;
            _domain = q.domain;
            Build(cx);
        }
        protected SelectedRowSet(SelectedRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
        }
        static BTree<long, Finder> _Fin(Context cx, Query q, BTree<long,Finder>fi)
        {
            for (var b = q.rowType?.First(); b != null; b = b.Next())
            {
                var p = b.value().Item1;
                if (cx.obs[p] is SqlCopy sc)
                    p = sc.copyFrom;
                fi += (b.value().Item1, new Finder(p, q.defpos));
            }
            return fi;
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
            source.Strategy(indent);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override RowSet Source => source;
        protected override object Build(Context cx)
        {
            for (var i=0; i<rowType.Length;i++)
                cx.obs[rowType[i].Item1]?.Build(cx,this);
            return null;
        }
        public override Cursor First(Context _cx)
        {
            return SelectedCursor.New(_cx,this);
        }
        public override Cursor PositionAt(Context _cx, PRow key)
        {
            if (source is IndexRowSet irs)
                return new SelectedCursor(_cx, this, IndexRowSet.IndexCursor.New(_cx, irs, key),0);
            return base.PositionAt(_cx, key);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (sQmap != null)
            {
                var cm = " sQmap: (";
                for (var b = sQmap.First(); b != null; b = b.Next())
                {
                    var f = b.value();
                    sb.Append(cm); cm = "],";
                    sb.Append(DBObject.Uid(b.key()));
                    sb.Append("="); sb.Append(DBObject.Uid(f.rowSet));
                    sb.Append('['); sb.Append(DBObject.Uid(f.col));
                }
            }
            sb.Append("])");
            return sb.ToString();
        }
        internal class SelectedCursor : Cursor
        {
            readonly SelectedRowSet _srs;
            internal readonly Cursor _bmk; // for rIn
            internal SelectedCursor(Context _cx,SelectedRowSet srs, Cursor bmk, int pos) 
                : base(_cx,srs, pos, bmk._defpos, new TRow(srs,srs.sQmap,bmk.values)) 
            {
                _bmk = bmk;
                _srs = srs;
            }
            SelectedCursor(SelectedCursor cu,Context cx,long p,TypedValue v)
                :base(cu,cx,cu._srs.sQmap[p].col,v)
            {
                _bmk = cu._bmk;
                _srs = cu._srs;
            }
            internal static SelectedCursor New(Context _cx,SelectedRowSet srs)
            {
                var ox = _cx.from;
                _cx.from += srs.source.finder; 
                for (var bmk = srs.source.First(_cx); bmk != null; bmk = bmk.Next(_cx))
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
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SelectedCursor(this, cx, p, v);
            }
            public override Cursor Next(Context _cx)
            {
                var ox = _cx.from;
                _cx.from += _srs.finder; // just for SelectedRowSet
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
        internal readonly RowSet source;
        internal readonly QuerySpecification qry;
        /// <summary>
        /// This constructor builds a rowset for the given QuerySpec
        /// directly using its defpos, rowType, ordering, where and match info.
        /// Note we cannot assume that columns are simple SqlCopy.
        /// Suggestion here is to use the source keyType. Maybe the source ordering too?
        /// </summary>
        internal SelectRowSet(Context cx, QuerySpecification q, RowSet r)
            : base(q.defpos, cx, q.domain, q.rowType, r.finder, null, q.where, q.ordSpec, 
                  q.matches,q.matching)
        {
            source = r;
            qry = q;
            Build(cx);
        }
        protected SelectRowSet(SelectRowSet rs, long a, long b) : base(rs, a, b)
        {
            source = rs.source;
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
            source.Strategy(indent);
        }
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override RowSet Source => source;
        protected override object Build(Context cx)
        {
            for (var i=0;i<qry.rowType.Length;i++)
                cx.obs[qry.rowType[i].Item1].Build(cx, this);
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
            SelectCursor(SelectCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _bmk = cu._bmk;
                _srs = cu._srs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SelectCursor(this, cx,p, v);
            }
            static TRow _Row(Context cx, Cursor bmk, SelectRowSet srs)
            {
                var ox = cx.from;
                cx.copy = srs.matching;
                cx.from += srs.source.finder;
                var vs = BTree<long,TypedValue>.Empty;
                for (var b = srs.qry.rowType?.First(); b != null; b = b.Next())
                {
                    var p = b.value().Item1;
                    var s = cx.obs[p]; 
                    var v = s?.Eval(cx)??TNull.Value;
                    if (v == TNull.Value && bmk[p] is TypedValue tv 
                        && tv != TNull.Value) 
                        // tv would be the right value but something has gone wrong
                        throw new PEException("PE788");
                    vs += (p,v);
                }
                cx.from = ox;
                return new TRow(srs, vs);
            }
            internal static SelectCursor New(Context _cx, SelectRowSet srs)
            {
                for (var bmk = srs.source.First(_cx); bmk != null; bmk = bmk.Next(_cx))
                {
                    var rb = new SelectCursor(_cx, srs, bmk, 0);
                    if (rb.Matches(_cx))
                        return rb;
                }
                return null;
            }
            public override Cursor Next(Context cx)
            {
                for (var bmk = _bmk.Next(cx); bmk != null; bmk = bmk.Next(cx))
                {
                    var rb = new SelectCursor(cx, _srs, bmk, _pos + 1);
                    for (var b = _srs.dataType.representation.First(); b != null; b = b.Next())
                        ((SqlValue)cx.obs[b.key()]).OnRow(cx,rb);
                    if (rb.Matches(cx))
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
		internal readonly BTree<long,bool> having;
        internal readonly TRow row;
        internal override RowSet Source => source;
        /// <summary>
        /// Constructor: Build a rowSet that aggregates data from a given source
        /// </summary>
        /// <param name="rs">The source data</param>
        /// <param name="h">The having condition</param>
		public EvalRowSet(Context cx,QuerySpecification q, RowSet rs) 
            : base(q.defpos, cx, q.domain, q.rowType,rs.finder,null,null,
                  q.ordSpec,q.matches,q.matching)
        {
            source = rs;
            having = q.where;
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
        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append("Eval ");
            sb.Append(rt);
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
            var tg = BTree<long, Register>.Empty;
            var cx = new Context(_cx);
            cx.copy = matching;
            cx.from += source.finder;
            var oi = rt;
            var k = new TRow(this,BTree<long,TypedValue>.Empty);
            cx.data += (defpos, this);
            for (var b=rt.First(); b!=null; b=b.Next())
                tg = ((SqlValue)_cx.obs[b.value().Item1]).StartCounter(cx,this,tg);
            var ebm = source.First(cx);
            if (ebm != null)
            {
                for (; ebm != null; ebm = ebm.Next(cx))
                    if ((!ebm.IsNull) && Query.Eval(having,cx))
                        for (var b = rt.First(); b != null; b = b.Next())
                            tg = ((SqlValue)_cx.obs[b.value().Item1]).AddIn(cx,ebm,tg);
            }
            var cols = oi;
            var vs = BTree<long,TypedValue>.Empty;
            cx.funcs = tg;
            for (int i = 0; i < cols.Length; i++)
            {
                var s = cols[i].Item1;
                vs += (s,_cx.obs[s].Eval(cx));
            }
            return new TRow(this,vs);
        }
         internal class EvalBookmark : Cursor
        {
            readonly EvalRowSet _ers;
            internal EvalBookmark(Context _cx, EvalRowSet ers) 
                : base(_cx, ers, 0, 0, (TRow)_cx.values[ers.defpos])
            {
                _ers = ers;
            }
            EvalBookmark(EvalBookmark cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v) 
            {
                _ers = cu._ers;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new EvalBookmark(this,cx,p,v);
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
        internal TableRowSet(Context cx, long t,BTree<long,Finder>fi)
            : base(t, cx, cx.Dom(t), cx.Signature(t),fi)
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
        internal override bool TableColsOk => true;
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
            protected TableCursor(Context _cx, TableRowSet trs, Table tb, int pos, 
                ABookmark<long, TableRow> bmk) 
                : base(_cx,trs.defpos, pos, bmk.key(), _Row(trs,bmk.value()))
            {
                _bmk = bmk; _table = tb; _trs = trs;
            }
            TableCursor(TableCursor cu,Context cx, long p,TypedValue v) :base(cu,cx,p,v)
            {
                _bmk = cu._bmk; _table = cu._table; _trs = cu._trs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TableCursor(this, cx, p, v);
            }
            static TRow _Row(TableRowSet trs,TableRow rw)
            {
                var vs = BTree<long, TypedValue>.Empty;
                for (var b=trs.dataType.representation.First();b!=null;b=b.Next())
                {
                    var p = b.key();
                    vs += (p, rw.vals[p]);
                }
                return new TRow(trs.rt, trs.dataType, vs);
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
                    return new TableCursor(_cx, trs, table, 0, b);
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
                    return new TableCursor(_cx,_trs,_table, _pos + 1, bmk);
                }
            }
            internal override TableRow Rec()
            {
                return _bmk.value();
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
        /// Unusally, the rowSet defpos is in the index's defining position,
        /// as the INRS is independent of role, user, command.
        /// Context must have a suitable tr field.
        /// </summary>
        /// <param name="f">the from part</param>
        /// <param name="x">the index</param>
        internal IndexRowSet(Context cx, Table tb, Index x, PRow filt, BTree<long,Finder>fi) 
            : base(x.defpos,cx,tb.domain,cx.Signature(tb.defpos),fi)
        {
            table = tb;
            index = x;
            filter = filt;
        }
        IndexRowSet(Context cx,IndexRowSet irs) :base(irs.defpos,cx,irs)
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
        internal override bool TableColsOk => true;
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
            internal readonly IndexRowSet _irs;
            internal readonly MTreeBookmark _bmk;
            internal readonly TableRow _rec;
            internal readonly PRow _key;
            IndexCursor(Context _cx, IndexRowSet irs, int pos, MTreeBookmark bmk, TableRow trw,
                PRow key=null)
                : base(_cx, irs.defpos, pos, trw.defpos, irs.rt, irs.dataType,
                      new TRow(irs, trw.vals))
            {
                _bmk = bmk; _irs = irs; _rec = trw; _key = key;
            }
            IndexCursor(IndexCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _bmk = cu._bmk; _irs = cu._irs; _rec = cu._rec; _key = cu._key;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new IndexCursor(this, cx, p, v);
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
                return new IndexCursor(_cx,_irs, _pos + 1, mb, _irs.table.tableRows[mb.Value().Value]);
            }
            internal static IndexCursor New(Context _cx,IndexRowSet irs, PRow key = null)
            {
                var _irs = irs;
                var table = _irs.table;
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
                    return new IndexCursor(_cx,_irs, 0, bmk, rec, key??irs.filter);
                }
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                var bmk = _bmk;
                var _table = _irs.table;
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
                    return new IndexCursor(_cx, _irs, _pos + 1, bmk, rec);
                }
            }
        }
    }

    /// <summary>
    /// A rowset for distinct values
    /// </summary>
    internal class DistinctRowSet : RowSet
    {
        readonly MTree mtree;
        internal readonly RowSet source;
        internal readonly BTree<int, TRow> rows = BTree<int,TRow>.Empty;
        /// <summary>
        /// constructor: a distinct rowset
        /// </summary>
        /// <param name="r">a source rowset</param>
        internal DistinctRowSet(Context _cx,RowSet r) 
            : base(_cx.GetUid(),_cx,r.dataType,r.rt,r.finder,r.keys,r.where)
        {
            source = r;
            mtree = (MTree)Build(_cx);
        }
        protected DistinctRowSet(DistinctRowSet rs,long a,long b):base(rs,a,b)
        {
            source = rs.source;
            mtree = rs.mtree;
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
            var ds = BTree<long,DBObject>.Empty;
            for (var b = source.keys.First(); b != null; b = b.Next())
            {
                var sv = (SqlValue)cx.obs[b.value().Item1];
                ds += (sv.defpos,sv.domain);
            }
            var mt = new MTree(new TreeInfo(source.keys, ds, TreeBehaviour.Allow, TreeBehaviour.Allow));
            var vs = BList<TypedValue>.Empty;
            var rs = BTree<int, TRow>.Empty;
            for (var a = source.First(cx); a != null; a = a.Next(cx))
            {
                for (var ti = mt.info; ti != null; ti = ti.tail)
                    vs+= a[ti.head];
                MTree.Add(ref mt, new PRow(vs), 0);
                rs += ((int)rs.Count, a);
            }
            return mt;
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
                :base(cx,drs,pos,0,drs.rows[(int)bmk.Value().Value])
            {
                _bmk = bmk;
                _drs = drs;
            }
            internal static DistinctCursor New(Context _cx,DistinctRowSet drs)
            {
                var ox = _cx.from;
                _cx.from += drs.source.finder;
                for (var bmk = drs.mtree.First(); bmk != null; bmk = bmk.Next() as MTreeBookmark)
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
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new NotImplementedException();
            }
            public override Cursor Next(Context _cx)
            {
                var ox = _cx.from;
                _cx.from += _drs.source.finder;
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
            :base(_cx.nextHeap++, _cx, r.dataType,r.rt,r.finder,
                 _cx._Pick(r.rt,os.items),r.where,os,r.matches)
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
            sb.Append(keys);
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        internal override int? Count => (int)tree.rows.Count;
        protected override object Build(Context cx)
        {
            var _cx = new Context(cx);
            _cx.from += source.finder; 
            var tree = new RTree(defpos,cx, keys, ordSpec.domain, 
                distinct ? TreeBehaviour.Ignore : TreeBehaviour.Allow, TreeBehaviour.Allow);
            for (var e = source.First(_cx); e != null; e = e.Next(_cx))
            {
                var vs = BTree<long,TypedValue>.Empty;
                for (var b = rowOrder.items.First(); b != null; b = b.Next())
                {
                    var s = cx.obs[b.value()];
                    vs += (s.defpos,s.Eval(_cx));
                }
                RTree.Add(ref tree, new TRow(cx._Pick(rt,rowOrder.items), rowOrder.domain, vs), e);
            }
            return tree;
        }
        public override Cursor First(Context _cx)
        {
            return OrderedCursor.New(_cx, this, RTreeBookmark.New(_cx, tree));
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
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                throw new NotImplementedException();
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
        internal readonly BList<long> rows;
        internal SqlRowSet(long dp,Context cx,long xp, BList<long> rs) 
            : base(dp, cx, cx.Dom(xp),cx.Signature(xp))
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
            readonly ABookmark<int, long> _bmk;
            SqlCursor(Context cx,SqlRowSet rs,int pos,ABookmark<int,long> bmk)
                : base(cx,rs,pos,0,(TRow)cx.obs[rs.rows[bmk.key()]].Eval(cx))
            {
                _srs = rs;
                _bmk = bmk;
            }
            SqlCursor(SqlCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _srs = cu._srs;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SqlCursor(this, cx, p, v);
            }
            internal static SqlCursor New(Context _cx,SqlRowSet rs)
            {
                for (var b=rs.rows.First();b!=null;b=b.Next())
                {
                    var rb = new SqlCursor(_cx,rs, 0, b);
                    if (rb.Matches(_cx) && Query.Eval(rs.where, _cx))
                        return rb;
                }
                return null;
            }
            public override Cursor Next(Context _cx)
            {
                for (var b=_bmk.Next();b!=null;b=b.Next())
                {
                    var rb = new SqlCursor(_cx,_srs, _pos+1,b);
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
        internal readonly BTree<long,bool> needed;
        internal TableExpRowSet(long dp, Context cx, RowType cs, 
            RowType ks, BTree<long,bool> ne,
            RowSet sc,BTree<long, bool> wh, BTree<long, TypedValue> ma,BTree<long,Finder> fi)
            : base(dp, cx, sc.dataType, cs, fi, ks, wh, sc.rowOrder, ma) 
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
                :base(cx,trs,pos,bmk._defpos,new TRow(trs,bmk))
            {
                _trs = trs;
                _bmk = bmk;
            }
            TableExpCursor(TableExpCursor cu,Context cx, long p,TypedValue v):base(cu,cx,p,v)
            {
                _trs = cu._trs;
                _bmk = cu._bmk;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new TableExpCursor(this, cx, p, v);
            }
            public static TableExpCursor New(Context cx,TableExpRowSet trs)
            {
                var ox = cx.from;
                cx.from += trs.source.finder;
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
                cx.from += _trs.source.finder;
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
        internal ExplicitRowSet(long dp,Context cx,RowType cols,
            Domain dt,BList<(long,TRow)>r) : base(dp,cx,dt,cols)
        {
            rows = r;
        }
        internal ExplicitRowSet(long dp,Context cx,RowSet sce)
            : base(dp, cx, sce.dataType,sce.rt, sce.finder)
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
            return new ExplicitRowSet(dp,cx,rt,dataType,rows);
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
        public override string ToString()
        {
            var r = base.ToString();
            if (rows.Count<10)
            {
                var sb = new StringBuilder(r);
                var cm = "";
                sb.Append("[");
                for (var b=rows.First();b!=null;b=b.Next())
                {
                    var (p, rw) = b.value();
                    sb.Append(cm); cm = ",";
                    sb.Append(DBObject.Uid(p));
                    sb.Append(": ");sb.Append(rw);
                }
                sb.Append("]");
                r = sb.ToString();
            }
            return r;
        }
        internal class ExplicitCursor : Cursor
        {
            readonly ExplicitRowSet _ers;
            readonly ABookmark<int,(long,TRow)> _prb;
            ExplicitCursor(Context _cx, ExplicitRowSet ers,ABookmark<int,(long,TRow)>prb,int pos) 
                :base(_cx,ers,pos,prb.value().Item1,prb.value().Item2.ToArray())
            {
                _ers = ers;
                _prb = prb;
            }
            ExplicitCursor(ExplicitCursor cu,Context cx, long p,TypedValue v):base(cu,cx,p,v)
            {
                _ers = cu._ers;
                _prb = cu._prb;
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new ExplicitCursor(this, cx, p, v);
            }
            internal static ExplicitCursor New(Context _cx,ExplicitRowSet ers,int i)
            {
                var ox = _cx.from;
                _cx.from += ers.finder;
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
                _cx.from += _ers.finder;
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
    /// Deal with execution of Triggers. The main complication here is that triggers execute 
    /// in their definer's role (possibly many roles involved in defining table, columns, etc)
    /// so the column names and data types will be different for different triggers, and
    /// trigger actions may affect different tables.
    /// We need to ensure that the TransitionRowSet rowType contains all the columns for the target,
    /// even if they are not in the data rowSet.
    /// As with TableRowSet and IndexRowSet, _finder refers to physical uids.
    /// Another nuisance is that we need to manage our own copies of autokeys, as we may be preparing
    /// many new rows for a table.
    /// </summary>
    internal class TransitionRowSet : RowSet
    {
        internal readonly BTree<long, TypedValue> defaults = BTree<long, TypedValue>.Empty; 
        internal readonly From from; // will be a SqlInsert, QuerySearch or UpdateSearch
        internal readonly ObInfo targetInfo;
        internal readonly Activation targetAc;
        internal readonly BTree<long, Finder> targetTrans,transTarget;
        internal readonly long indexdefpos = -1L;
        internal readonly PTrigger.TrigType _tgt;
        /// <summary>
        /// There may be several triggers of any type, so we manage a set of transition activations for each.
        /// These are for table before, table instead, table after, row before, row instead, row after.
        /// </summary>
        internal readonly BTree<long, TriggerActivation> rb, ri, ra;
        internal readonly BTree<long, TriggerActivation> tb, ti, ta, td;
        internal readonly Adapters _eqs;
        internal TransitionRowSet(Context cx, From q, PTrigger.TrigType tg, Adapters eqs)
            : base(cx.nextHeap++, cx, q.domain, q.rowType,
                  cx.data[q.defpos]?.finder??BTree<long,Finder>.Empty, null, q.where)
        {
            from = q;
            _eqs = eqs;
            var tr = cx.db;
            var t = tr.objects[from.target] as Table;
            indexdefpos = t.FindPrimaryIndex(tr)?.defpos ?? -1L;
            // check now about conflict with generated columns
            if (t.Denied(cx, Grant.Privilege.Insert))
                throw new DBException("42105", q);
            var rt = q.rowType; // data rowType
            targetInfo = tr.schemaRole.infos[t.defpos] as ObInfo;
            var tgTn = BTree<long, Finder>.Empty;
            var tnTg = BTree<long, Finder>.Empty;
            for (var b = rt.First(); b != null; b = b.Next()) // at this point q is the insert statement, simpleQuery is the base table
            {
                var p = b.value().Item1;
                var s = cx.obs[p];
                var c = (s is SqlCopy sc) ? sc.copyFrom : s.defpos;
                tgTn += (c,new Finder(p,defpos));
                tnTg += (p,new Finder(c,q.defpos));
            }
            targetTrans = tgTn;
            transTarget = tnTg;
            targetAc = new Activation(cx, q.name);
            if (tg != PTrigger.TrigType.Delete)
            {
                for (var b = targetInfo.rowType?.First(); b != null; b = b.Next())
                {
                    var p = b.value().Item1;
                    cx.from += (p, new Finder(p,defpos));
                    var tc = (TableColumn)tr.objects[p];
                    var tv = tc.defaultValue ?? tc.domain.defaultValue;
                    if (tv != TNull.Value)
                    {
                        defaults += (tc.defpos, tv);
                        cx.values += (tc.defpos, tv);
                    }
                    for (var c = tc.constraints.First(); c != null; c = c.Next())
                    {
                        cx.Frame(c.key());
                        for (var d = tc.domain.constraints.First(); d != null; d = d.Next())
                            cx.Frame(d.key());
                    }
                    cx.Frame(tc.generated.exp,tc.defpos);
                }
            }
            _tgt = tg;
            // Get the trigger sets and set up the activations
            tb = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.Before]);
            ti = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.Instead]);
            ta = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachStatement | PTrigger.TrigType.After]);
            td = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.Deferred]);
            rb = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachRow | PTrigger.TrigType.Before]);
            ri = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachRow | PTrigger.TrigType.Instead]);
            ra = Setup(cx, t, t.triggers[_tgt | PTrigger.TrigType.EachRow | PTrigger.TrigType.After]);
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
        internal override bool TableColsOk => true;
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
        TargetCursor CheckPrimaryKey(Context cx,TargetCursor tgc)
        {
            var ix = (Index)cx.db.objects[indexdefpos];
            if (ix == null)
                return tgc;
            var k = BList<TypedValue>.Empty;
            for (var b=ix.keys.First(); b!=null; b=b.Next())
            {
                var tc = (TableColumn)cx.obs[b.value().Item1];
                var v = tgc[tc.defpos];
                if (v == null || v == TNull.Value)
                {
                    if (tc.domain.prim != Sqlx.INTEGER)
                        throw new DBException("22004");
                    v = ix.rows.NextKey(k, 0, b.key());
                    if (v == TNull.Value)
                        v = new TInt(0);
                    tgc += (cx, tc.defpos, v);
                    cx.values += (tc.defpos, v);
                }
                k += v;
            }
            return tgc;
        }
        /// <summary>
        /// Set up activations for executing a set of triggers
        /// </summary>
        /// <param name="q"></param>
        /// <param name="tgs"></param>
        /// <returns></returns>
        BTree<long, TriggerActivation> Setup(Context _cx,Table tb,BTree<long, bool> tgs)
        {
            var r = BTree<long, TriggerActivation>.Empty;
            var cx = new Context(_cx.tr);
            cx.nextHeap = _cx.nextHeap;
            cx.obs = _cx.obs;
            cx.data = _cx.data;
            cx.obs += (tb.defpos,tb);
            for (var b = tb.tblCols.First(); b != null; b = b.Next())
            {
                var tc = (DBObject)_cx.tr.objects[b.key()];
                cx.obs += (tc.defpos,tc);
            }
            if (tgs != null)
                for (var tg = tgs.First(); tg != null; tg = tg.Next())
                {
                    var t = tg.key();
                    cx.Frame(t);
                    // NB at the cx.obs[t] version of the trigger has the wrong action field
                    r += (t, new TriggerActivation(cx, this, (Trigger)cx.db.objects[t]));
                }
            return r;
        }
        /// <summary>
        /// Perform the triggers in a set. 
        /// </summary>
        /// <param name="acts"></param>
        (TransitionCursor,bool) Exec(Context _cx, BTree<long, TriggerActivation> acts)
        {
            var r = false;
            if (acts == null)
                return (_cx.cursors[defpos] as TransitionCursor, r);
            targetAc.db = _cx.db;
            var c = _cx.cursors[defpos];
            TargetCursor row = (c as TargetCursor)
                ??((c is TransitionCursor tc)? new TargetCursor(targetAc,tc):null);
            bool skip;
            for (var a = acts.First(); a != null; a = a.Next())
            {
                var ta = a.value();
                ta.db = _cx.db;
                (row, skip) = ta.Exec(targetAc, row);
                r = r || skip;
                targetAc.db = ta.db;
            }
            _cx = targetAc.SlideDown();
            _cx.val = TBool.For(r);
            var cu = row?._trsCu;
            _cx.cursors += (defpos, cu); // restore the TransitionCursor
            return (cu,r);
        }
        internal (TransitionCursor,bool) InsertSA(Context _cx)
        { return Exec(_cx,ta); }
        internal (TransitionCursor, bool) InsertSB(Context _cx)
        { return Exec(_cx, tb); }
        internal (TransitionCursor, bool) UpdateSA(Context _cx)
        { return Exec(_cx,ta); }
        internal (TransitionCursor, bool) UpdateSB(Context _cx)
        { Exec(_cx, tb);  return Exec(_cx, ti); }
        internal (TransitionCursor, bool) DeleteSB(Context _cx)
        { Exec(_cx,tb); return Exec(_cx,ti); }
        internal (TransitionCursor, bool) DeleteSA(Context _cx)
        { return Exec(_cx, td); }
        internal class TransitionCursor : Cursor
        {
            internal readonly TransitionRowSet _trs;
            internal readonly Cursor _fbm; // transition source cursor
            internal readonly TRow targetRow; // for physical record construction, triggers, constraints
            internal readonly BTree<long,TypedValue> _oldVals;
            internal TransitionCursor(Context cx, TransitionRowSet trs, Cursor fbm, int pos)
                : base(cx, trs, pos, fbm._defpos, new TRow(trs,fbm))
            {
                _trs = trs;
                _fbm = fbm;
                _oldVals = values;
                targetRow = new TRow(_trs.targetInfo,_trs.targetTrans,values);
                cx.values += (targetRow.values,false);
            }
            TransitionCursor(TransitionCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _trs = cu._trs;
                _fbm = cu._fbm;
                _oldVals = cu._oldVals;
                targetRow = new TRow(_trs.targetInfo,_trs.targetTrans,values);
                cx.values += (targetRow.values,false);
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                cx.values += (p, v);
                return new TransitionCursor(this, cx, p, v);
            }
            public static TransitionCursor operator+(TransitionCursor cu,
                (Context,long,TypedValue)x)
            {
                var (cx, p, tv) = x;
                cx.values += (p, tv);
                return new TransitionCursor(cu, cx, p, tv);
            }
            static TransitionCursor New(Context cx,TransitionRowSet trs, int pos,
                 Cursor fbm)
            { 
                var trc = new TransitionCursor(cx, trs, fbm, pos);
                var tb = trs.rt.First();
                for (var b=fbm.columns.First(); b!=null&&tb!=null;b=b.Next(),tb=tb.Next())
                {
                    var cp = b.value().Item1;
                    var sl = cx.obs[cp];
                    TypedValue tv = fbm[cp];
                    if (sl is SqlProcedureCall sv)
                    {
                        var fp = ((CallStatement)cx.obs[sv.call]).procdefpos;
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
                                tv = cx.obs[pr.body].Eval(ac);
                            }
                        }
                    }
                    trc += (cx, tb.value().Item1, tv);
                }
                trc = TargetCursor.New(cx, trc, pos)._trsCu;
                return trc;
            }
            internal static TransitionCursor New(Context _cx,TransitionRowSet trs)
            {
                var ox = _cx.from;
                var sce = _cx.data[trs.from.defpos];
                _cx.from += sce?.finder;
                for (var fbm = sce?.First(_cx); fbm != null;
                    fbm = fbm.Next(_cx))
                    if (fbm.Matches(_cx) && Query.Eval(trs.from.where, _cx))
                    {
                        var r = New(_cx, trs, 0, fbm);
                        _cx.from = ox;
                        return r;
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
            BTree<long, TriggerActivation> Setup(Context cx, BTree<long, bool> tgs)
            {
                var r = BTree<long, TriggerActivation>.Empty;
                cx = new Context(cx); // for this trs
                if (tgs != null)
                    for (var tg = tgs.First(); tg != null; tg = tg.Next())
                    {
                        var trg = tg.key();
                        var ta = new TriggerActivation(cx,_trs, (Trigger)cx.obs[trg]);
                        r +=(trg, ta);
                    }
                return r;
            }
            public override Cursor Next(Context cx)
            {
                var ox = cx.from;
                cx.from += cx.data[_fbm._rowsetpos].finder;
                var from = _trs.from;
                if(cx.obs[from.where.First()?.key()??-1L] is SqlValue sv && sv.domain.prim == Sqlx.CURRENT)
                        return null;
                var t = cx.db.objects[_trs.from.target] as Table;
                for (var fbm = _fbm.Next(cx); fbm != null; fbm = fbm.Next(cx))
                {
                    var ret = New(cx,_trs, 0, fbm);
                    for (var b = from.where.First(); b != null; b = b.Next())
                        if (cx.obs[b.key()].Eval(cx) != TBool.True)
                            goto skip;
                    cx.from = ox;
                    return ret;
                    skip:;
                }
                cx.from = ox;
                return null;
            }
            internal override TableRow Rec()
            {
                return _fbm.Rec();
            }
            /// <summary>
            /// Some convenience functions for calling from Transaction.Execute(..)
            /// </summary>
            internal (TransitionCursor,bool) InsertRA(Context _cx)
            { return _trs.Exec(_cx,_trs.ra); }
            internal (TransitionCursor,bool) InsertRB(Context _cx)
            { _trs.Exec(_cx, _trs.rb); return _trs.Exec(_cx,_trs.ri); }
            internal (TransitionCursor,bool) UpdateRA(Context _cx)
            { return _trs.Exec(_cx,_trs.ra); }
            internal (TransitionCursor,bool) UpdateRB(Context _cx)
            { _trs.Exec(_cx,_trs.rb); return _trs.Exec(_cx,_trs.ri); }
            internal (TransitionCursor,bool) DeleteRB(Context _cx)
            { _trs.Exec(_cx, _trs.rb); return _trs.Exec(_cx,_trs.ri); }

        }
        internal class TargetCursor : Cursor
        {
            internal readonly BTree<long, Finder> _map;
            internal readonly TransitionCursor _trsCu;
            internal readonly BTree<long, TypedValue> _oldVals;
            internal TargetCursor(Context cx, TransitionCursor cu)
                : base(cx, cu._trs.defpos, cu._pos, cu._defpos, cu._trs.targetInfo.rowType,
                     cu._trs.targetInfo.domain, cu.targetRow)
            {
                _trsCu = cu;
                var ov = BTree<long, TypedValue>.Empty;
                var ti = cu._trs.targetInfo;
                for (var b = ti.rowType?.First(); b != null; b = b.Next())
                {
                    var p = b.value().Item1;
                    ov += (p, cu._oldVals[cu._trs.targetTrans[p].col]);
                }
                _oldVals = ov;
                _map = cu._trs.targetTrans;
            }
            internal static TargetCursor New(Context cx, TransitionCursor trc,int pos)
            {
                var tgc = new TargetCursor(cx, trc);
                var trs = trc._trs;
                var ti = trs.targetInfo;
                for (var b=ti.rowType.First(); b!=null;b=b.Next())
                    if (cx.db.objects[b.value().Item1] is TableColumn tc)
                    {
                        switch (tc.generated.gen)
                        {
                            case Generation.Expression:
                                if (cx.values[tc.defpos] != TNull.Value)
                                    throw new DBException("0U000", cx.NameFor(tc.defpos));
                               tgc += (cx, tc.defpos, cx.obs[tc.generated.exp].Eval(cx));
                                break;
                        }
                        if (tc.defaultValue != TNull.Value && tgc[tc.defpos] == TNull.Value)
                            tgc += (cx, tc.defpos, tc.defaultValue);
                        if (tc.notNull && cx.values[tc.defpos] == TNull.Value)
                            throw new DBException("22206", cx.NameFor(tc.defpos));
                        for (var cb = tc.constraints?.First(); cb != null; cb = cb.Next())
                        {
                            var cp = cb.key();
                            var ck = (Check)cx.db.objects[cp];
                            cx.obs += (ck.defpos, ck);
                            cx.Frame(cp);
                            var se = (SqlValue)cx.obs[ck.search];
                            if (se.Eval(cx) != TBool.True)
                                throw new DBException("22212", cx.NameFor(tc.defpos));
                        }
                    }
                if (trs.indexdefpos > 0)
                    tgc = trs.CheckPrimaryKey(cx, tgc);
                return tgc;
            }
            TargetCursor(TargetCursor cu, Context cx, long p, TypedValue v) : base(cu, cx, p, v)
            {
                _map = cu._map;
                _trsCu = cu._trsCu+(cx,_map[p].col,v);
                cx.values += (p,v);
                cx.cursors += (cu._trsCu._trs.defpos, this);
            }
            public static TargetCursor operator +(TargetCursor cu,(Context, long, TypedValue) x)
            {
                var (cx, p, tv) = x;
                return new TargetCursor(cu, cx, p, tv);
            }
            public override Cursor Next(Context _cx)
            {
                var cu = (TransitionCursor)_trsCu.Next(_cx);
                if (cu == null)
                    return null;
                return new TargetCursor(_cx, cu);
            }

            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                return new TargetCursor(cx,_trsCu+(cx,_map[p].col,v));
            }

            internal override TableRow Rec()
            {
                return _trsCu.Rec();
            }
        }
    }
    /// <summary>
    /// Used for oldTable/newTable in trigger execution
    /// </summary>
    internal class TransitionTableRowSet : RowSet
    {
        internal readonly BTree<long,TableRow> data;
        internal readonly BTree<long, Finder> map,rmap;
        /// <summary>
        /// Old table: compute the rows that will be in the update/delete
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="cx"></param>
        /// <param name="trs"></param>
        internal TransitionTableRowSet(long dp,Context cx,TransitionRowSet trs)
            :base(dp,cx,trs.dataType,trs.rt,trs.finder)
        {
            var dat = BTree<long, TableRow>.Empty;
            for (var b = trs.First(cx); b != null; b = b.Next(cx))
                dat += (b._defpos, b.Rec());
            data = dat;
            map = trs.targetTrans;
            rmap = trs.transTarget;
        }
        /// <summary>
        /// New table: Get the new rows from the context
        /// </summary>
        /// <param name="dp"></param>
        /// <param name="cx"></param>
        /// <param name="trs"></param>
        internal TransitionTableRowSet(long dp,Context cx,long trs)
            :base(dp,cx,cx.data[trs].dataType,cx.data[trs].rt)
        {
            data = cx.newTables[trs];
        }
        public override Cursor First(Context _cx)
        {
            return TransitionTableCursor.New(_cx,this);
        }

        internal override RowSet New(long dp, Context cx)
        {
            throw new NotImplementedException();
        }

        internal override RowSet New(long a, long b)
        {
            throw new NotImplementedException();
        }
        internal class TransitionTableCursor : Cursor
        {
            internal readonly ABookmark<long, TableRow> _bmk;
            internal readonly TransitionTableRowSet _tt;
            TransitionTableCursor(Context cx,TransitionTableRowSet tt,ABookmark<long,TableRow> bmk,int pos)
                :base(cx,tt,pos,bmk.key(),new TRow(tt,tt.rmap,bmk.value().vals))
            {
                _bmk = bmk; _tt = tt;
            }
            internal static TransitionTableCursor New(Context cx,TransitionTableRowSet tt)
            {
                var bmk = tt.data.First();
                return (bmk!=null)? new TransitionTableCursor(cx, tt, bmk, 0): null;
            }
            public override Cursor Next(Context _cx)
            {
                var bmk = _bmk.Next();
                return (bmk != null) ? new TransitionTableCursor(_cx, _tt, bmk, _pos + 1):null;
            }

            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new NotImplementedException();
            }

            internal override TableRow Rec()
            {
                return _bmk.value();
            }
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
        internal SortedRowSet(long dp, Context cx, RowSet s, RowType kl)
            : this(dp,kl, cx, s, 
                new TreeInfo(kl, s.dataType, TreeBehaviour.Allow, TreeBehaviour.Allow))
        { }
        internal SortedRowSet(long dp,RowType ks, Context _cx,RowSet s, TreeInfo ti)
            : base(dp,_cx, s.dataType, s.rt, s.finder, ks, s.where)
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
            sb.Append(keys.ToString());
            base._Strategy(sb, indent);
            source.Strategy(indent);
        }
        internal override int? Count => rows.Count;
        protected override object Build(Context cx)
        {
            var ox = cx.from;
            cx.from += source.finder;
            var tree = new MTree(treeInfo);
            var si = source.rt;
            for (var a = source.First(cx);a!= null;a=a.Next(cx))
            {
                MTree.Add(ref tree, cx.MakeKey(keys), rows.Count);
                var vs = BTree<long, TypedValue>.Empty;
                for (var b=source.dataType.representation.First();b!=null;b=b.Next())
                {
                    var p = b.key();
                    var s = (SqlValue)cx.obs[p];
                    vs += (p, s.Eval(cx));
                }
                rows.Add(new TRow(source,vs));
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
            SortedCursor(SortedCursor cu,Context cx,long p,TypedValue v):base(cu,cx,p,v)
            {
                _srs = cu._srs;
                _mbm = cu._mbm;
            }
            protected override Cursor New(Context cx,long p, TypedValue v)
            {
                return new SortedCursor(this, cx, p, v);
            }
            public override MTreeBookmark Mb()
            {
                return _mbm;
            }
            internal static SortedCursor New(Context _cx,SortedRowSet srs)
            {
                var ox = _cx.from;
                _cx.from += srs.source.finder;
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
        readonly RowType actuals;
        readonly RowSet rowSet;
        internal RoutineCallRowSet(Context cx,From f,Procedure p, RowType r) 
            :base(cx.GetUid(),cx,f.domain,f.rowType,null,null,f.where)
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
            : base(_cx.GetUid(),_cx,s.dataType,s.rt,null,s.keys,s.where)
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
            : base(cx.GetUid(),cx,q.domain,q.rowType,null,null,q.where)
        {
            var vs = BList<SqlValue>.Empty;
            if (d != null)
                for(int i=0;i<d.rows.Count;i++)
                    vs += (SqlValue)cx.obs[d.rows[i]];
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
                return new TRow(sv,new BTree<long,TypedValue>(sv.defpos,(TDocument)sv.Eval(cx)));
            }
            internal static DocArrayBookmark New(Context _cx,DocArrayRowSet drs)
            {
                var bmk = drs.vals.First();
                if (bmk == null)
                    return null;
                return new DocArrayBookmark(_cx,drs, bmk);
            }
            protected override Cursor New(Context cx, long p, TypedValue v)
            {
                throw new NotImplementedException();
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
            :base(cx.GetUid(),cx,f.domain,f.rowType,null,null,f.where,null)
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