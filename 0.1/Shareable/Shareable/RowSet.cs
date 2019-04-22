using System;
using System.Text;
#nullable enable
namespace Shareable
{
    /// <summary>
    /// In the server, a rowSet is a Collection(Serialisable),
    /// and the rows are generally fully-evaluated SRows.
    /// The SRows are sometimes instantiated on traversal instead of on definition.
    /// At the client, a rowset is a DocArray and the rows are Documents.
    /// </summary>
    public abstract class RowSet : Collection<Serialisable>
    {
        public readonly SQuery _qry;
        public readonly STransaction _tr;
        public readonly Context _cx;
        public RowSet(STransaction tr, SQuery q, Context cx, int? n):base(n)
        {
            _tr = tr; _qry = q; _cx = cx;
        }
    }
    /// <summary>
    /// A RowBookmark evaluates its Serialisable _ob (usually an SRow).
    /// This matters especially for SSelectStatements
    /// </summary>
    public abstract class RowBookmark : Bookmark<Serialisable>, ILookup<long, Serialisable>
    {
        public readonly RowSet _rs;
        public readonly Context _cx; // first entry will be an SRow
        protected RowBookmark(RowSet rs, Context cx, int p) : base(p)
        {
            _rs = rs; _cx = cx;
        }
        public SRow _ob => _cx.Row();
        public override Serialisable Value => (SRow)_cx.refs; // should always be an SRow
        public bool Matches(SList<Serialisable> wh)
        {
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.Value.Lookup(_rs._tr,_cx) != SBoolean.True)
                    return false;
            return true;
        }
        protected static Context _Cx(RowSet rs, SRow r, Context? n = null)
        {
            if (n == null)
                n = rs._cx;
            if (rs is TableRowSet trs)
                n = new Context(new SDict<long, Serialisable>(trs._tb.uid, r), n);
            return new Context(r, n);
        }
        public virtual MTreeBookmark<Serialisable>? Mb()
        {
            return null;
        }
        public virtual RowBookmark? ResetToTiesStart(MTreeBookmark<Serialisable> mb)
        {
            return null;
        }
        public virtual STransaction Update(STransaction tr, SDict<long, Serialisable> assigs)
        {
            return tr; // no changes here
        }
        public virtual STransaction Delete(STransaction tr)
        {
            return tr; // no changes here
        }

        public bool defines(long s)
        {
            return s == _rs._qry.Alias || _ob.vals.Contains(s);
        }
        public Serialisable this[long s] => s == _rs._qry.Alias ? _ob : _ob[s];
    }
    public class DistinctRowSet : RowSet
    {
        public readonly RowSet _sce;
        public readonly SDict<SRow, bool> rows;
        public DistinctRowSet(RowSet sce) : base(sce._tr, sce._qry, sce._cx, null)
        {
            _sce = sce;
            var r = SDict<SRow, bool>.Empty;
            for (var b = sce.First(); b != null; b = b.Next())
                r += (((RowBookmark)b)._ob, true);
            rows = r;
        }
        public override Bookmark<Serialisable>? First()
        {
            return DistinctRowBookmark.New(this);
        }
        internal class DistinctRowBookmark : RowBookmark
        {
            public readonly DistinctRowSet _drs;
            public readonly Bookmark<(SRow,bool)> _bmk;
            DistinctRowBookmark(DistinctRowSet drs,Bookmark<(SRow,bool)> bmk,int pos) 
                : base(drs,_Cx(drs,bmk.Value.Item1),pos)
            { _drs = drs; _bmk = bmk; }
            internal static DistinctRowBookmark? New(DistinctRowSet drs)
            {
                return (drs.rows.First() is Bookmark<(SRow, bool)> rb) ?
                    new DistinctRowBookmark(drs, rb, 0) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return (_bmk.Next() is Bookmark<(SRow,bool)> rb)?
                    new DistinctRowBookmark(_drs,rb,Position+1):null;
            }
            public override STransaction Update(STransaction tr, SDict<long, Serialisable> assigs)
            {
                throw new System.NotImplementedException();
            }
            public override STransaction Delete(STransaction tr)
            {
                throw new System.NotImplementedException();
            }
        }
    }
    public class OrderedRowSet : RowSet
    {
        public readonly RowSet _sce;
        public readonly SMTree<Serialisable> _tree;
        public readonly SDict<int, SRow> _rows;
        public OrderedRowSet(RowSet sce,SSelectStatement sel) :base(sce._tr,sel,sce._cx,sce.Length)
        {
            _sce = sce;
            var ti = SList<TreeInfo<Serialisable>>.Empty;
            int n = 0;
            for (var b = sel.order.First(); b != null; b = b.Next())
                ti = ti+(new TreeInfo<Serialisable>(b.Value, 'A', 'D',!b.Value.desc), n++);
            var t = new SMTree<Serialisable>(ti);
            var r = SDict<int, SRow>.Empty;
            int m = 0;
            for (var b = sce.First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
            {
                var k = new Variant[n];
                var i = 0;
                for (var c = sel.order.First(); c != null; c = c.Next())
                    k[i] = new Variant(c.Value.col.Lookup(sce._tr,b._cx),!c.Value.desc);
                t = t.Add(m,k);
                r += (m++, b._ob);
            }
            _tree = t;
            _rows = r;
        }
        public OrderedRowSet(RowSet sce,SList<TreeInfo<Serialisable>>ti)
            : base(sce._tr,sce._qry,sce._cx,null)
        {
            _sce = sce;
            var t = new SMTree<Serialisable>(ti);
            var r = SDict<int, SRow>.Empty;
            int m = 0;
            for (var b = sce.First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
            {
                var k = new Variant[ti.Length??0];
                var i = 0;
                for (var c = ti.First(); c != null; c = c.Next())
                    k[i] = new Variant(c.Value.headName.Lookup(sce._tr,b._cx));
                t = t.Add(m, k);
                r += (m++, b._ob);
            }
            _tree = t;
            _rows = r;
        }
        public override Bookmark<Serialisable>? First()
        {
            return OrderedBookmark.New(this);
        }
        internal class OrderedBookmark : RowBookmark
        {
            public readonly OrderedRowSet _ors;
            public readonly MTreeBookmark<Serialisable> _bmk;
            OrderedBookmark(OrderedRowSet ors,MTreeBookmark<Serialisable> bmk,int pos)
                :base(ors,_Cx(ors,ors._rows[(int)bmk.Value.Item2]),pos)
            {
                _ors = ors; _bmk = bmk;
            }
            internal static OrderedBookmark? New(OrderedRowSet ors)
            {
                return (ors._tree.First() is MTreeBookmark<Serialisable> rb) ? 
                    new OrderedBookmark(ors, rb, 0) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return (_bmk.Next() is MTreeBookmark<Serialisable> rb) ?
                    new OrderedBookmark(_ors, rb, Position+1) : null;
            }
            public override MTreeBookmark<Serialisable>? Mb()
            {
                return _bmk;
            }
            public override RowBookmark? ResetToTiesStart(MTreeBookmark<Serialisable> mb)
            {
                if (mb != null)
                    return new OrderedBookmark(_ors, mb, Position + 1);
                return null;
            }
            public override STransaction Update(STransaction tr, SDict<long, Serialisable> assigs)
            {
                throw new System.NotImplementedException();
            }
            public override STransaction Delete(STransaction tr)
            {
                throw new System.NotImplementedException();
            }
        }
    }
    public class TableRowSet : RowSet
    {
        public readonly STable _tb;
        /// <summary>
        /// Add in read constraint
        /// </summary>
        /// <param name="db"></param>
        /// <param name="t"></param>
        public TableRowSet(STransaction db,STable t,Context cx) 
            : base(db+t.uid /*read constraint*/,t,
                  cx,t.rows.Length)
        {
            _tb = t;
        }
        public override Bookmark<Serialisable>? First()
        {
            return TableRowBookmark.New(this);
        }
        internal class TableRowBookmark : RowBookmark
        {
            public readonly TableRowSet _trs;
            public Bookmark<(long, long)> _bmk;
            protected TableRowBookmark(TableRowSet trs,Bookmark<(long,long)>bm,int p) 
                :base(trs,_Cx(trs,new SRow(trs._tr,trs._tr.Get(bm.Value.Item2))),p)
            {
                _trs = trs; _bmk = bm;
            }
            internal static TableRowBookmark? New(TableRowSet trs)
            {
                return (trs._tb.rows.First() is Bookmark<(long, long)> b) ?
                    new TableRowBookmark(trs, b, 0) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return (_bmk.Next() is Bookmark<(long,long)> b)?
                    new TableRowBookmark(_trs,b,Position+1):null;
            }
            public override STransaction Update(STransaction tr, SDict<long, Serialisable> assigs)
            {
                var rc = _ob.rec ?? throw new Exception("PE01");
                return (STransaction)tr.Install(new SUpdate(tr,rc, assigs),rc,tr.curpos); 
            }
            public override STransaction Delete(STransaction tr)
            {
                var rc = _ob.rec ?? throw new Exception("PE02");
                return (STransaction)tr.Install(new SDelete(tr,rc.table, rc.Defpos),rc,tr.curpos); // ok
            }
        }
    }
    public class IndexRowSet : RowSet
    {
        public readonly SIndex _ix;
        public readonly SList<Serialisable> _wh;
        public readonly SCList<Variant> _key;
        public readonly SExpression.Op _op;
        public readonly bool _unique;
        public IndexRowSet(STransaction tr,STable t,SIndex ix,SCList<Variant> key, 
            SExpression.Op op,SList<Serialisable> wh,Context cx) 
            :base(Rdc(tr,ix,key),t, cx, t.rows.Length)
        {
            _ix = ix; _key = key; _wh = wh; _op = op; 
            _unique = key.Length == _ix.cols.Length && _ix.references==-1;
        }
        /// <summary>
        /// Add in read constraints: a key specifies just one row as the read
        /// Constraint. Otherwise lock the entire table
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="ix"></param>
        /// <param name="_key"></param>
        /// <returns></returns>
        static STransaction Rdc(STransaction tr,SIndex ix,SCList<Variant> _key)
        {
            if (_key.Length ==0)
                return tr + ix.table;
            var mb = ix.rows.PositionAt(_key);
            if (mb == null)
                return tr;
            if (mb.hasMore(tr, ix.cols.Length??0))
                return tr + ix.table;
            return tr + mb.Value.Item2;
        }
        public override Bookmark<Serialisable>? First()
        {
            return IndexRowBookmark.New(this);
        }
        internal class IndexRowBookmark : RowBookmark
        {
            public readonly IndexRowSet _irs;
            public readonly MTreeBookmark<Serialisable> _mbm;
            protected IndexRowBookmark(IndexRowSet irs,SRow ob,MTreeBookmark<Serialisable> mbm,int p) 
                :base(irs,_Cx(irs,ob),p)
            {
                _irs = irs; _mbm = mbm;
            }
            internal static IndexRowBookmark? New(IndexRowSet irs)
            {
                var k = irs._key;
                var b = (MTreeBookmark<Serialisable>?)((k.Length!=0) ? irs._ix.rows.PositionAt(k) 
                    : irs._ix.rows.First());
                for (;b != null; b=NextOrPrev(irs._op,b))
                {
                    var rc = irs._tr.Get(b.Value.Item2);
                    var rb = new IndexRowBookmark(irs, new SRow(irs._tr, rc), b, 0);
                    if (!rc.EqualMatches(rb, irs._wh))
                        return null;
                    if (rc.Matches(rb, irs._wh))
                        return rb;
                }
                return null;
            }
            static MTreeBookmark<Serialisable>? NextOrPrev(SExpression.Op op,MTreeBookmark<Serialisable> b)
            {
                return ((op == SExpression.Op.Lss || op == SExpression.Op.Leq) ?
                    b.Previous() : b.Next()) as MTreeBookmark<Serialisable>;
            }
            public override Bookmark<Serialisable>? Next()
            {
                if (_irs._unique && _irs._op==SExpression.Op.Eql)
                    return null;
                for (var b = NextOrPrev(_irs._op,_mbm); b != null; b = NextOrPrev(_irs._op,b))
                {
                    var rc = _irs._tr.Get(b.Value.Item2);
                    var rb = new IndexRowBookmark(_irs, new SRow(_irs._tr,rc), b, Position + 1);
                    if (rc.Matches(rb, _irs._wh))
                        return rb;
                }
                return null;
            }
            public override MTreeBookmark<Serialisable>? Mb()
            {
                return _mbm;
            }
            public override RowBookmark? ResetToTiesStart(MTreeBookmark<Serialisable> mb)
            {
                if (mb == null)
                    return null;
                var rc = _irs._tr.Get(mb.Value.Item2);
                return new IndexRowBookmark(_irs,new SRow(_irs._tr,rc),mb,Position+1);
            }
            public override STransaction Update(STransaction tr, SDict<long, Serialisable> assigs)
            {
                var rc = _ob.rec ?? throw new System.Exception("PE41");
                return (STransaction)tr.Install(new SUpdate(tr, rc, assigs),rc,tr.curpos); // ok
            }
            public override STransaction Delete(STransaction tr)
            {
                var rc = _ob.rec ?? throw new System.Exception("PE42");
                return (STransaction)tr.Install(new SDelete(tr, rc.table, rc.Defpos),rc,tr.curpos); // ok
            }
        }
    }
    public class AliasRowSet : RowSet
    {
        public readonly SAlias _alias;
        public readonly RowSet _sce;
        public AliasRowSet(SAlias a,RowSet sce) :
            base(sce._tr,a,new Context(new SDict<long,Serialisable>(a.alias,a.qry),sce._cx),sce.Length)
        {
            _alias = a;
            _sce = sce;
        }

        public override Bookmark<Serialisable>? First()
        {
            return AliasRowBookmark.New(this);
        }
        internal class AliasRowBookmark : RowBookmark
        {
            public readonly AliasRowSet _ars;
            public readonly RowBookmark _bmk;
            AliasRowBookmark(AliasRowSet ars,RowBookmark bmk,int p)
                :base(ars,_Context(ars,bmk._cx),p)
            {
                _ars = ars;
                _bmk = bmk;
            }
            static Context _Context(AliasRowSet ars,Context cx)
            {
                var a = ars._alias;
                return new Context(cx.refs,
                    new Context(new SDict<long, Serialisable>(a.alias, a.qry),cx.next));
            }
            internal static AliasRowBookmark? New(AliasRowSet ars)
            {
                if (ars._sce.First() is RowBookmark b)
                    return new AliasRowBookmark(ars,b,0);
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                if (_bmk.Next() is RowBookmark b)
                    return new AliasRowBookmark(_ars, b, Position+1);
                return null;
            }
        }
    }
    public class SearchRowSet : RowSet
    {
        public readonly SSearch _sch;
        public readonly RowSet _sce;

        public SearchRowSet(STransaction tr, SQuery top, SSearch sc, Context cx)
            : this(Source(tr, top, sc, cx), sc, cx)
        { }
        SearchRowSet(RowSet sce, SSearch sc, Context cx) : base(sce._tr, sc, cx, null)
        {
            _sch = sc;
            _sce = sce;
        }
        static RowSet Source(STransaction tr,SQuery top, SSearch sc,Context cx)
        { 
            RowSet? s = null;
            var matches = SDict<long,(Serialisable,SExpression.Op)>.Empty;
            if (sc.sce is STable tb)
            {
                for (var wb = sc.where.First(); wb != null; wb = wb.Next())
                    if (wb.Value is SExpression x)
                    {
                        if (x.left is SColumn c && tb.refs.Contains(c.uid) &&
                            x.right != null && x.right.isValue)
                            matches = matches+ (c.uid, (x.right,x.op));
                        else if (x.right is SColumn cr && tb.refs.Contains(cr.uid) &&
                                x.left != null && x.left.isValue)
                            matches = matches+(cr.uid, (x.left,Reverse(x.op)));
                    }
                var best = SCList<Variant>.Empty;
                if (matches.Length != null)
                    for (var b = tb.indexes.First(); best.Length != null && matches.Length.Value > best.Length.Value && b != null;
                        b = b.Next())
                    {
                        var ma = SCList<Variant>.Empty;
                        var op = SExpression.Op.Eql;
                        var ix = (SIndex)tr.objects[b.Value.Item1];
                        for (var wb = ix.cols.First(); wb != null; wb = wb.Next())
                        {
                            if (!matches.Contains(wb.Value))
                                break;
                            op = Compat(op, matches[wb.Value].Item2);
                            if (op == SExpression.Op.NotEql)
                                break;
                            ma = ma.InsertAt(new Variant(Variants.Ascending, matches[wb.Value].Item1),
                                ma.Length??0);
                        }
                        if (ma.Length != null && ma.Length.Value > best.Length.Value)
                        {
                            best = ma;
                            s = new IndexRowSet(tr, tb, ix, ma, op, sc.where,cx);
                            tr = s._tr;
                        }
                    }
            }
            return s?? sc.sce?.RowSet(tr,top,cx) ?? throw new System.Exception("PE03");
        }
        static SExpression.Op Reverse(SExpression.Op op)
        {
            switch (op)
            {
                case SExpression.Op.Gtr: return SExpression.Op.Lss;
                case SExpression.Op.Geq: return SExpression.Op.Leq;
                case SExpression.Op.Lss: return SExpression.Op.Gtr;
                case SExpression.Op.Leq: return SExpression.Op.Geq;
            }
            return op;
        }
        static SExpression.Op Compat(SExpression.Op was, SExpression.Op now)
        {
            if (was == now || was == SExpression.Op.Eql)
                return now;
            return SExpression.Op.NotEql;
        }
        public override Bookmark<Serialisable>? First()
        {
            return SearchRowBookmark.New(this);
        }
        internal class SearchRowBookmark : RowBookmark
        {
            public readonly SearchRowSet _sch;
            public RowBookmark _bmk;
            protected SearchRowBookmark(SearchRowSet sr,RowBookmark bm,int p):
                base(sr, bm._cx, p)
            {
                _sch = sr; _bmk = bm;
            }
            internal static SearchRowBookmark? New(SearchRowSet rs)
            {
                for (var b = rs._sce.First(); b != null; b = b.Next())
                {
                    var rb = new SearchRowBookmark(rs, (RowBookmark)b, 0);
                    if (rb.Matches(rs._sch.where)==true)
                        return rb;
                }
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                for (var b = _bmk.Next(); b != null; b = b.Next())
                {
                    var rb = new SearchRowBookmark(_sch, (RowBookmark)b, Position + 1);
                    if (rb.Matches(_sch._sch.where)==true)
                        return rb;
                }
                return null;
            }
            public override STransaction Update(STransaction tr, SDict<long, Serialisable> assigs)
            {
                return _bmk.Update(tr, assigs);
            }
            public override STransaction Delete(STransaction tr)
            {
                return _bmk.Delete(tr);
            }
        }
    }
    public class EvalRowSet : RowSet
    {
        public readonly SDict<long, Serialisable> _vals;
        public EvalRowSet(RowSet r, SQuery q, Context cx)
            : base(r._tr, q, cx, null)
        {
            var vs = SDict<long, Serialisable>.Empty;
            var ags = cx.Ags();
            var b = r.First() as RowBookmark;
            if (b==null)
                for (var ab = ags.First(); ab != null; ab = ab.Next())
                    if (ab.Value.Item2 is SFunction f)
                        vs += (f.fid, (f.func == SFunction.Func.Count) ? SInteger.Zero : Serialisable.Null);
            for (; b != null; b = b.Next() as RowBookmark)
                for (var ab = ags.First(); ab != null; ab = ab.Next())
                    if (ab.Value.Item2 is SFunction f)
                    {
                        var v = f.arg.Lookup(r._tr,b._cx);
                        if (v.isValue && v != Serialisable.Null)
                            vs += (f.fid, vs.Contains(f.fid) ? f.AddIn(vs[f.fid], v)
                                : f.StartCounter(v));
                    }
            _vals = vs;
        }
        public override Bookmark<Serialisable>? First()
        {
            var r = new SRow();
            var ab = _qry.display.First();
            for (var b = _qry.cpos.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                r += (ab.Value.Item2, b.Value.Item2.Lookup(_tr,new Context(_vals, null)));
            return new EvalRowBookmark(this,r, _vals);
        }
        public class EvalRowBookmark : RowBookmark
        {
            internal EvalRowBookmark(EvalRowSet ers, SRow r,SDict<long,Serialisable> a) 
                : base(ers, _Cx(ers,r, new Context(a,null)), 0) { }
            public override Bookmark<Serialisable>? Next()
            {
                return null;
            }
        }
    }
    public class GroupRowSet : RowSet
    {
        public readonly SGroupQuery _gqry;
        public readonly SList<TreeInfo<long>> _info; // computed from the grouped columns
        public readonly SMTree<long> _tree; // for the treeinfo in the GroupRowSet
        public readonly SDict<long, SDict<long,Serialisable>> _grouprows; // accumulators for the aggregates
        public readonly SQuery _top;
        public readonly RowSet _sce;
        public GroupRowSet(STransaction tr, SQuery top, SGroupQuery gqry,
            Context cx) 
            : this(gqry.source.RowSet(tr, top, cx), top, gqry, cx)
        {
        }
        GroupRowSet(RowSet sce,SQuery top,SGroupQuery gqry,Context cx)
            :base(sce._tr,gqry,cx,null)
        {
            _gqry = gqry;
            _sce = sce;
            var inf = SList<TreeInfo<long>>.Empty;
            for (var b = gqry.groupby.First(); b != null; b = b.Next())
                inf += (new TreeInfo<long>(b.Value.Item2, 'd', 'i'), b.Value.Item1);
            _info = inf;
            var t = new SMTree<long>(inf);
            var r = SDict<long, SDict<long, Serialisable>>.Empty;
            var n = 0;
            for (var b = sce.First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
            {
                var k = Key(b);
                if (!t.Contains(k))
                {
                    t += (k, n);
                    r += (n, SDict<long, Serialisable>.Empty);
                    n++;
                }
                var m = t.PositionAt(k)?.Value.Item2 ?? 0;
                r += (m, AddIn(sce._tr,r[m], b._cx));
            }
            _tree = t;
            _grouprows = r;
            _top = top;
        }
        protected SCList<Variant> Key(RowBookmark b)
        {
            var k = SCList<Variant>.Empty;
            for (var g = _gqry.groupby.First(); g != null; g = g.Next())
                k += new Variant(b._ob[g.Value.Item2]);
            return k;
        }
        protected SRow _Row(MTreeBookmark<long> b)
        {
            var r = new SRow();
            var kc = SDict<long, Serialisable>.Empty;
            var gb = b.Value.Item1.First();
            for (var kb = _info.First(); gb != null && kb != null; gb = gb.Next(), kb = kb.Next())
                kc += (kb.Value.headName, (Serialisable)gb.Value.ob);
            var cx = new Context(kc,new Context( _grouprows[b.Value.Item2], _cx));
            var ab = _top.Display.First();
            for (var cb = _top.cpos.First(); ab != null && cb != null; ab = ab.Next(), cb = cb.Next())
                r += (ab.Value.Item2,cb.Value.Item2.Lookup(_tr,cx));
            return r;
        }
        static SDict<long,Serialisable> AddIn(STransaction tr,SDict<long,Serialisable> cur, Context cx)
        {
            var ags = cx.Ags();
            for (var b = ags.First(); b != null; b = b.Next())
                if (b.Value.Item2 is SFunction f)
                {
                    var v = f.arg.Lookup(tr,new Context(cur, cx));
                    if (v != Serialisable.Null)
                        cur += (f.fid, cur.Contains(f.fid) ? f.AddIn(cur[f.fid], v)
                            : f.StartCounter(v));
                }
            return cur;
        }
        public override Bookmark<Serialisable>? First()
        {
            return GroupRowBookmark.New(this);
        }
        /// <summary>
        /// The GroupRowBookmarks all contain references to the index groups->rows
        /// During the first traversal this is built up.
        /// </summary>
        internal class GroupRowBookmark : RowBookmark
        {
            public readonly GroupRowSet _grs;
            public readonly MTreeBookmark<long> _bmk;
            protected GroupRowBookmark(GroupRowSet grs, MTreeBookmark<long> b,
                SRow r, SDict<long,Serialisable> a, int p)
                : base(grs,_Cx(grs,r,new Context(a,null)),p)
            {
                _grs = grs; _bmk = b;
            }
            internal static GroupRowBookmark? New(GroupRowSet rs)
            {
                var b = rs._tree.First() as MTreeBookmark<long>;
                if (b == null)
                    return null;
                return new GroupRowBookmark(rs, b, rs._Row(b), rs._grouprows[0], 0);
            }
            public override Bookmark<Serialisable>? Next()
            {
                var b = _bmk.Next() as MTreeBookmark<long>;
                if (b == null)
                    return null;
                return new GroupRowBookmark(_grs, b, _grs._Row(b), _grs._grouprows[b.Value.Item2],0);
            }
        }
    }
    public class SelectRowSet : RowSet
    {
        public readonly SSelectStatement _sel;
        public readonly RowSet _source;
        public SelectRowSet(RowSet sce,SSelectStatement sel,Context cx)
            :base(sce._tr,sel,cx,null)
        {
            _sel = sel;
            _source = sce;
        }

        public override Bookmark<Serialisable>? First()
        {
            return SelectRowBookmark.New(this);
        }
        internal class SelectRowBookmark : RowBookmark
        {
            public readonly SelectRowSet _srs;
            public readonly RowBookmark _bmk;
            SelectRowBookmark(SelectRowSet rs,RowBookmark bmk,SRow rw,int p)
                :base(rs,_Cx(rs,rw,bmk._cx),p)
            {
                _srs = rs; _bmk = bmk;
            }
            internal static SelectRowBookmark? New(SelectRowSet rs)
            {
                for (var b = rs._source.First() as RowBookmark;b!=null;b=b.Next() as RowBookmark )
                {
                    var rw = (SRow)rs._qry.Lookup(rs._tr,b._cx);
                    if (rw.isNull)
                        continue;
                    var rb = new SelectRowBookmark(rs, b, rw, 0);
                    if (rb._ob.cols.Length!=0)
                        return rb;
                }
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                for (var b = _bmk.Next() as RowBookmark; b != null; b = b.Next() as RowBookmark)
                {
                    var rw = (SRow)_srs._qry.Lookup(_rs._tr,b._cx);
                    if (rw.isNull)
                        continue;
                    var rb = new SelectRowBookmark(_srs, b, rw, Position + 1);
                    if (rb._ob.cols.Length != 0)
                        return rb;
                }
                return null;
            }
            public override STransaction Update(STransaction tr, SDict<long, Serialisable> assigs)
            {
                return _bmk.Update(tr, assigs);
            }
            public override STransaction Delete(STransaction tr)
            {
                return _bmk.Delete(tr);
            }
        }
    }
    public class JoinRowSet : RowSet
    {
        public readonly SJoin _join;
        public readonly RowSet _left, _right;
        public readonly int _klen;
        internal JoinRowSet(STransaction tr,SQuery top, SJoin j, RowSet lf, RowSet rg, 
            Context cx)
            : base(tr, j, cx, null)
        {
            _join = j;
            var lti = SList<TreeInfo<Serialisable>>.Empty;
            var rti = SList<TreeInfo<Serialisable>>.Empty;
            var n = 0;
            for (var b = j.ons.First();b!=null;b=b.Next())
            {
                var e = b.Value;
                if (e.op != SExpression.Op.Eql)
                    continue;
                lti += (new TreeInfo<Serialisable>((SColumn)e.left, 'A', 'D'),n);
                rti += (new TreeInfo<Serialisable>((SColumn)e.right, 'A', 'D'),n);
                n++;
            }
            for (var b = j.uses.First(); b != null; b = b.Next())
            {
                var e = b.Value;
                lti += (new TreeInfo<Serialisable>(j.left.refs[e.Item2], 'A', 'D'),n); //NB Item2
                rti += (new TreeInfo<Serialisable>(j.right.refs[e.Item1], 'A', 'D'),n);// NB Item1
                n++;
            }
            _klen = lti.Length??0;
            if (lti.Length!=0)
            {
                lf = new OrderedRowSet(lf, lti);
                rg = new OrderedRowSet(rg, rti);
            }
            _left = lf;
            _right = rg;
        }
        public override Bookmark<Serialisable>? First()
        {
            return JoinRowBookmark.New(this);
        }
        public class JoinRowBookmark : RowBookmark
        {
            public readonly JoinRowSet _jrs;
            public readonly RowBookmark? _lbm, _rbm;
            public readonly bool _useL, _useR;
            internal JoinRowBookmark(JoinRowSet jrs,RowBookmark? left,bool ul,RowBookmark? right, bool ur,int pos)
                :base(jrs,_Context(jrs,left,ul,right,ur),pos)
            {
                _jrs = jrs; _lbm = left; _useL = ul; _rbm = right; _useR = ur;
            }
            static Context _Context(JoinRowSet jrs, RowBookmark? lbm, bool ul, RowBookmark? rbm, bool ur)
            {
                var cx = rbm?._cx;
                if (lbm != null)
                    cx = Context.Append(lbm._cx, cx);
                return _Cx(jrs,_Row(jrs, lbm, ul, rbm, ur), cx);
            }
            static SRow _Row(JoinRowSet jrs,RowBookmark? lbm,bool ul,RowBookmark? rbm,bool ur)
            {
                var r = new SRow();
                Bookmark<(int, (long,string))>? ab;
                var ds = SDict<long, (long, string)>.Empty;
                for (var b = jrs._qry.display.First(); b != null; b = b.Next())
                {
                    var id = b.Value.Item2;
                    ds += (id.Item1, id);
                }
                switch (jrs._join.joinType)
                {
                    default:
                        {
                            if (lbm != null && ul)
                            {
                                ab = lbm?._ob.names.First();
                                if (ul)
                                    for (var b = lbm?._ob.cols.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                                    {
                                        var n = ab.Value.Item2;
                                        var k = ds[n.Item1];
                                        r += (k, b.Value.Item2);
                                    }
                            }
                            if (rbm != null && ur)
                            {
                                ab = rbm?._ob.names.First();
                                for (var b = rbm?._ob.cols.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                                {
                                    var n = ab.Value.Item2;
                                    var k = ds[n.Item1];
                                    r += (k, b.Value.Item2);
                                }
                            }
                            break;
                        }
                    case SJoin.JoinType.Natural:
                        {
                            if (lbm != null && ul)
                            {
                                ab = lbm._ob.names.First();
                                for (var b = lbm._ob.cols.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                                    r += (ab.Value.Item2, b.Value.Item2);
                            }
                            if (rbm != null && ur)
                            {
                                ab = rbm._ob.names.First();
                                for (var b = rbm._ob.cols.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                                    if (!((SJoin)jrs._qry).uses.Contains(ab.Value.Item2.Item1))
                                        r += (ab.Value.Item2, b.Value.Item2);
                            }
                            break;
                        }
                }
                return r;
            }
            public static RowBookmark? New(JoinRowSet jrs)
            {
                RowBookmark? lf, rg;
                for (lf= jrs._left.First() as RowBookmark,rg = jrs._right.First() as RowBookmark;
                    lf!=null && rg!=null; )
                {
                    if (jrs._join.joinType == SJoin.JoinType.Cross)
                        return new JoinRowBookmark(jrs, lf, true, rg, true, 0);
                    var c = jrs._join.Compare(lf, rg);
                    if (c==0)
                        return new JoinRowBookmark(jrs, lf, true, rg, true, 0);
                    if (c < 0)
                    {
                        if (jrs._join.joinType.HasFlag(SJoin.JoinType.Left))
                            return new JoinRowBookmark(jrs, lf, true, rg, false, 0);
                        lf = lf.Next() as RowBookmark;
                    }
                    else
                    {
                        if (jrs._join.joinType.HasFlag(SJoin.JoinType.Right))
                            return new JoinRowBookmark(jrs, lf, false, rg, true, 0);
                        rg = rg.Next() as RowBookmark;
                    }
                }
                if (lf!=null && jrs._join.joinType.HasFlag(SJoin.JoinType.Left))
                    return new JoinRowBookmark(jrs, lf, true, null, false, 0);
                if (rg!=null && jrs._join.joinType.HasFlag(SJoin.JoinType.Right))
                    return new JoinRowBookmark(jrs, null, false, rg, true, 0);
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                var lbm = _lbm;
                var rbm = _rbm;
                var depth = (_jrs._join.ons.Length + _jrs._join.uses.Length)??0;
                if (rbm==null && lbm != null && _jrs._join.joinType.HasFlag(SJoin.JoinType.Left))
                {
                    lbm = lbm.Next() as RowBookmark;
                    if (lbm == null)
                        return null;
                    return new JoinRowBookmark(_jrs, lbm, true, null, false, Position + 1);
                }
                if (lbm==null && rbm != null && _jrs._join.joinType.HasFlag(SJoin.JoinType.Right))
                {
                    rbm = rbm.Next() as RowBookmark;
                    if (rbm == null)
                        return null;
                    return new JoinRowBookmark(_jrs, null, false, rbm, true, Position + 1);
                }
                while (lbm != null && rbm != null)
                {
                    if (_jrs._join.joinType==SJoin.JoinType.Cross)
                    {
                        rbm = rbm.Next() as RowBookmark;
                        if (rbm != null)
                            return new JoinRowBookmark(_jrs, lbm, true, rbm, true, Position + 1);
                        lbm = lbm.Next() as RowBookmark;
                        rbm = _jrs._right.First() as RowBookmark;
                        if (lbm!=null && rbm!=null)
                            return new JoinRowBookmark(_jrs, lbm, true, rbm, true, Position + 1);
                        return null;
                    }
                    if (rbm.Mb() is MTreeBookmark<Serialisable> mb0 &&
                        mb0.hasMore(_jrs._tr, depth))
                    {
                        rbm = rbm.Next() as RowBookmark;
                        return new JoinRowBookmark(_jrs, lbm, true, rbm, true, Position + 1);
                    }
                    lbm = lbm.Next() as RowBookmark;
                    if (lbm == null)
                        break;
                    var mb = (lbm.Mb() is MTreeBookmark<Serialisable> ml && ml.changed(depth)) ? null :
                        rbm.Mb()?.ResetToTiesStart(_jrs._tr, depth);
                    rbm = (mb != null) ? rbm = rbm.ResetToTiesStart(mb) : rbm.Next() as RowBookmark;
                    if (rbm == null)
                        break;
                    var c = _jrs._join.Compare(lbm, rbm);
                    if (c == 0)
                        return new JoinRowBookmark(_jrs, lbm, true, rbm, true, Position + 1);
                    if (c < 0)
                    {
                        if (_jrs._join.joinType.HasFlag(SJoin.JoinType.Left))
                            return new JoinRowBookmark(_jrs, lbm, true, rbm, false, Position + 1);
                        lbm = lbm.Next() as RowBookmark;
                    }
                    else
                    {
                        if (_jrs._join.joinType.HasFlag(SJoin.JoinType.Right))
                            return new JoinRowBookmark(_jrs, lbm, false, rbm, true, Position + 1);
                        rbm = rbm.Next() as RowBookmark;
                    }
                }
                if (lbm != null && _jrs._join.joinType.HasFlag(SJoin.JoinType.Left))
                    return new JoinRowBookmark(_jrs, lbm, true, null, false, Position + 1);
                if (rbm != null && _jrs._join.joinType.HasFlag(SJoin.JoinType.Right))
                    return new JoinRowBookmark(_jrs, null, false, rbm, true, Position + 1);
                return null;
            }
        }
    }
    public class SysRows : RowSet
    {
        public readonly SysTable tb;
        internal SysRows(STransaction tr, SysTable t) 
            : base(tr, t, Context.Empty, null)
        {
            tb = t; 
        }
        public override Bookmark<Serialisable>? First()
        {
            switch (_tr.Name(tb.uid))
            {
                case "_Log": return LogBookmark.New(this,0,0);
                case "_Columns": return ColumnsBookmark.New(this);
                case "_Constraints": return ConstraintsBookmark.New(this);
                case "_Indexes": return IndexesBookmark.New(this);
                case "_Tables": return TablesBookmark.New(this);
            }
            return null;
        }
        public SRow _Row(params Serialisable[] vals)
        {
            var r = new SRow();
            int j = 0;
            for (var b = tb.cpos.First(); b != null; b = b.Next())
                if (b.Value.Item2 is SColumn s)
                    r += ((s.uid,SDatabase._system.Name(s.uid)), vals[j++]);
                        // Serialisable.New(((SColumn)b.Value.val).dataType, vals[j++]));
            return r;
        }
        internal class LogBookmark : RowBookmark
        {
            public readonly SysRows _srs;
            public readonly long _log;
            public readonly long _next;
            internal LogBookmark(SysRows rs, long lg, SDbObject ob, long nx, int p) 
                : base(rs, _Cx(rs,rs._Row(new SString(ob.Uid()), // Uid
                    new SString(ob.type.ToString()), //Type
                    new SString(ob.ToString()),
                    new SString(rs._tr.uids.Contains(ob.uid)?rs._tr.uids[ob.uid]:""),
                    new SString(SDbObject._Uid(ob.Affects)))), p)  // Desc
            {
                _srs = rs;  _log = lg; _next = nx; 
            }
            internal static LogBookmark? New(SysRows rs,long lg,int p)
            {
                var rdr = new Reader(rs._tr, lg);
                return (rdr._Get() is SDbObject ob) ?
                    new LogBookmark(rs, 0, ob, rdr.Position, p) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return New((SysRows)_rs, _next, Position + 1);
            }
        }
        internal class ColumnsBookmark : RowBookmark
        {
            public readonly SysRows _srs;
            public readonly TablesBookmark _tbm;
            public readonly SColumn _sc;
            public readonly int _seq;
            internal ColumnsBookmark(SysRows rs, TablesBookmark tbm, SColumn sc, int seq, int p)
                : base(rs, _Cx(rs,rs._Row(new SString(rs._tr.Name(tbm._tb.uid)), // Table
                    new SString(rs._tr.Name(sc.uid)), // Name
                    new SString(sc.dataType.ToString()), //Type
                    new SInteger(sc.constraints.Length ?? 0),
                    new SString(sc.Uid())),tbm._cx), p)  
            {
                _srs = rs; _sc = sc; _seq = seq; _tbm = tbm;
            }
            internal static ColumnsBookmark? New(SysRows rs)
            {
                for (var tbm = TablesBookmark.New(rs); tbm != null; tbm = tbm.Next() as TablesBookmark)
                {
                    var b = tbm?._tb.cpos?.First();
                    if (b != null)
                        return new ColumnsBookmark(rs, tbm, (SColumn)b.Value.Item2, b.Value.Item1, 0);
                }
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                var n = _tbm._tb.cpos.PositionAt(_seq)?.Next();
                if (n!=null)
                    return new ColumnsBookmark(_srs, _tbm, (SColumn)n.Value.Item2, n.Value.Item1, 
                        Position+1);
                for (var tbm = _tbm.Next() as TablesBookmark; tbm != null; 
                    tbm = tbm.Next() as TablesBookmark)
                {
                    var b = tbm?._tb.cpos?.First();
                    if (b != null)
                        return new ColumnsBookmark(_srs, tbm, (SColumn)b.Value.Item2, b.Value.Item1, 
                            Position+1);
                }
                return null;
            }
        }
        internal class ConstraintsBookmark : RowBookmark
        {
            public readonly SysRows _srs;
            public readonly ColumnsBookmark _cbm;
            public readonly SFunction _cf;
            public readonly string _id;
            internal ConstraintsBookmark(SysRows rs, ColumnsBookmark cbm, SFunction cf, 
                string id, int p)
                : base(rs, _Cx(rs,rs._Row(new SString(rs._tr.Name(cbm._tbm._tb.uid)), // Table
                    new SString(rs._tr.Name(cbm._sc.uid)), // Column
                    new SString(Check(rs._tr,cf,id)), // Check
                    new SString(cf.arg.ToString())),cbm._cx), p)  //Expression
            {
                _srs = rs; _cbm = cbm; _cf = cf; _id = id;
            }
            static string Check(STransaction tr,SFunction cf,string id)
            {
                return (cf.func == SFunction.Func.Constraint) ? id : 
                    cf.func.ToString().ToUpper();
            }
            internal static ConstraintsBookmark? New(SysRows rs)
            {
                for (var cbm = ColumnsBookmark.New(rs); cbm != null; 
                    cbm = cbm.Next() as ColumnsBookmark)
                    {
                        var b = cbm._sc.constraints.First();
                        if (b != null)
                            return new ConstraintsBookmark(rs, cbm, 
                                b.Value.Item2, b.Value.Item1, 0);
                    }
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                var n = _cbm._sc.constraints.PositionAt(_id)?.Next();
                if (n != null)
                    return new ConstraintsBookmark(_srs, _cbm, n.Value.Item2, n.Value.Item1,
                        Position + 1);
                for (var cbm = _cbm.Next() as ColumnsBookmark; cbm != null;
                    cbm = cbm.Next() as ColumnsBookmark)
                {
                    var b = cbm._sc.constraints.First();
                    if (b != null)
                        return new ConstraintsBookmark(_srs, cbm,
                            b.Value.Item2, b.Value.Item1, Position+1);
                }
                return null;
            }
        }
        internal class IndexesBookmark : RowBookmark
        {
            public readonly SysRows _srs;
            public readonly TablesBookmark _tbm;
            public readonly SIndex _ix;
            internal IndexesBookmark(SysRows rs, TablesBookmark tbm, SIndex ix, int p)
                : base(rs, _Cx(rs,rs._Row(new SString(rs._tr.Name(tbm._tb.uid)), // TableName
                    new SString(Type(ix)), // Cols
                    new SString(Cols(rs,ix)),
                    new SString(References(rs._tr,ix))),tbm._cx), p)  //Rows
            {
                _srs = rs; _tbm = tbm; _ix = ix;
            }
            static string Type(SIndex x)
            {
                if (x.primary)
                    return "PRIMARY KEY";
                if (x.references >= 0)
                    return "FOREIGN KEY";
                return "UNIQUE";
            }
            static string Cols(SysRows rs,SIndex ix)
            {
                var sb = new StringBuilder("(");
                var cm = "";
                for (var b=ix.cols.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(rs._tr.Name(((SColumn)rs._tr.objects[b.Value]).uid));
                }
                sb.Append(")");
                return sb.ToString();
            }
            static string References(STransaction tr,SIndex ix)
            {
                return (ix.references < 0) ? "" : ("" + tr.Name(ix.references));
            }
            internal static IndexesBookmark? New(SysRows rs)
            {
                for (var tbm = TablesBookmark.New(rs); tbm != null; tbm = tbm.Next() as TablesBookmark)
                {
                    var b = tbm?._tb.indexes?.First();
                    if (b != null)
                        return new IndexesBookmark(rs, tbm, (SIndex)rs._tr.objects[b.Value.Item1], 0);
                }
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                var n = _tbm._tb.indexes.PositionAt(_ix.uid)?.Next();
                if (n != null)
                    return new IndexesBookmark(_srs, _tbm,
                        (SIndex)_srs._tr.objects[n.Value.Item1], Position + 1);
                for (var tbm = _tbm.Next() as TablesBookmark; 
                    tbm != null; tbm = tbm.Next() as TablesBookmark)
                {
                    var b = tbm?._tb.indexes?.First();
                    if (b != null)
                        return new IndexesBookmark(_srs, tbm, 
                            (SIndex)_srs._tr.objects[b.Value.Item1], 0);
                }
                return null;
            }
        }
        internal class TablesBookmark : RowBookmark
        {
            public readonly SysRows _srs;
            public readonly STable _tb;
            public readonly long _oid;
            internal TablesBookmark(SysRows rs, long oid, STable tb, int p)
                : base(rs, _Cx(rs,rs._Row(new SString(rs._tr.Name(tb.uid)), // Name
                    new SInteger(tb.cpos.Length??0), // Cols
                    new SInteger(tb.rows.Length??0),
                    new SInteger(tb.indexes.Length??0),new SString(tb.Uid())),null), p)  //Rows
            {
                _srs = rs; _oid = oid; _tb = tb;
            }
            internal static TablesBookmark? New(SysRows rs)
            {
                for (var b = rs._tr.objects.First();b!=null; b=b.Next())
                    if (b.Value.Item2 is STable tb && tb.type==Types.STable)
                        return new TablesBookmark(rs, b.Value.Item1, tb, 0);
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                for (var b = _srs._tr.objects.PositionAt(_oid)?.Next(); b != null; b = b.Next())
                    if (b.Value.Item2 is STable tb && tb.type==Types.STable)
                        return new TablesBookmark(_srs, b.Value.Item1, tb, Position+1);
                return null;
            }
        }
    }
}
