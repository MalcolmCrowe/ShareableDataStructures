using System;
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
        public readonly SDict<long, SFunction> _aggregates;
        public RowSet(STransaction tr, SQuery q, SDict<long, SFunction>ags, int? n):base(n)
        {
            _tr = tr; _qry = q; _aggregates = ags;
        }
    }
    /// <summary>
    /// A RowBookmark evaluates its Serialisable _ob (usually an SRow).
    /// This matters especially for SSelectStatements
    /// </summary>
    public abstract class RowBookmark : Bookmark<Serialisable>,ILookup<string,Serialisable>
    {
        public readonly RowSet _rs;
        public readonly SRow _ob;
        public readonly SDict<long, Serialisable> _ags;
        protected RowBookmark(RowSet rs, SRow ob, int p) : base(p)
        {
            _rs = rs; _ob = ob; _ags = SDict<long, Serialisable>.Empty;
        }
        protected RowBookmark(RowSet rs, SRow ob, SDict<long,Serialisable> a,int p) : base(p)
        {
            _rs = rs; _ob = ob; _ags = a;
        }
        public override Serialisable Value => _ob; // should always be an SRow
        public Serialisable this[string s] => (s.CompareTo(_rs._qry.Alias) == 0)?_ob:_ob.vals[s];
        public bool Matches(SList<Serialisable> wh,Context cx)
        {
            cx = new Context(this, cx);
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.Value.Lookup(cx) != SBoolean.True)
                    return false;
            return true;
        }
        public bool Matches(SList<SExpression> wh, Context cx)
        {
            cx = new Context(this, cx);
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.Value.Lookup(cx) != SBoolean.True)
                    return false;
            return true;
        }
        public virtual MTreeBookmark<Serialisable>? Mb()
        {
            return null;
        }
        public virtual RowBookmark? ResetToTiesStart(MTreeBookmark<Serialisable> mb)
        {
            return null;
        }
        public virtual STransaction Update(STransaction tr,SDict<string,Serialisable> assigs)
        {
            return tr; // no changes here
        }
        public virtual STransaction Delete(STransaction tr)
        {
            return tr; // no changes here
        }

        public bool defines(string s)
        {
            return s.CompareTo(_rs._qry.Alias)==0 || _ob.vals.Contains(s);
        }
    }
    public class DistinctRowSet : RowSet
    {
        public readonly RowSet _sce;
        public readonly SDict<SRow, bool> rows;
        public DistinctRowSet(RowSet sce) : base(sce._tr, sce._qry, sce._aggregates, null)
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
            public readonly Bookmark<ValueTuple<SRow,bool>> _bmk;
            DistinctRowBookmark(DistinctRowSet drs,Bookmark<ValueTuple<SRow,bool>> bmk,int pos) 
                : base(drs,bmk.Value.Item1,pos)
            { _drs = drs; _bmk = bmk; }
            internal static DistinctRowBookmark? New(DistinctRowSet drs)
            {
                return (drs.rows.First() is Bookmark<ValueTuple<SRow, bool>> rb) ?
                    new DistinctRowBookmark(drs, rb, 0) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return (_bmk.Next() is Bookmark<ValueTuple<SRow,bool>> rb)?
                    new DistinctRowBookmark(_drs,rb,Position+1):null;
            }
            public override STransaction Update(STransaction tr, SDict<string, Serialisable> assigs)
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
        public OrderedRowSet(RowSet sce,SSelectStatement sel,Context cx) :base(sce._tr,sel,sce._aggregates,sce.Length)
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
                    k[i] = new Variant(c.Value.col.Lookup(new Context(b,cx)),!c.Value.desc);
                t = t.Add(m,k);
                r += (m++, b._ob);
            }
            _tree = t;
            _rows = r;
        }
        public OrderedRowSet(RowSet sce,SList<TreeInfo<Serialisable>>ti,Context cx)
            : base(sce._tr,sce._qry,sce._aggregates,null)
        {
            _sce = sce;
            var t = new SMTree<Serialisable>(ti);
            var r = SDict<int, SRow>.Empty;
            int m = 0;
            for (var b = sce.First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
            {
                var k = new Variant[ti.Length.Value];
                var i = 0;
                for (var c = ti.First(); c != null; c = c.Next())
                    k[i] = new Variant(c.Value.headName.Lookup(new Context(b, cx)));
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
                :base(ors,ors._rows[(int)bmk.Value.Item2],pos)
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
            public override STransaction Update(STransaction tr, SDict<string, Serialisable> assigs)
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
        public TableRowSet(STransaction db,STable t) 
            : base(db+t.uid /*read constraint*/,t,
                  SDict<long,SFunction>.Empty,t.rows.Length)
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
            public Bookmark<ValueTuple<long, long>> _bmk;
            protected TableRowBookmark(TableRowSet trs,Bookmark<ValueTuple<long,long>>bm,int p) 
                :base(trs,new SRow(trs._tr,trs._tr.Get(bm.Value.Item2)),p)
            {
                _trs = trs; _bmk = bm;
            }
            internal static TableRowBookmark? New(TableRowSet trs)
            {
                return (trs._tb.rows.First() is Bookmark<ValueTuple<long, long>> b) ?
                    new TableRowBookmark(trs, b, 0) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return (_bmk.Next() is Bookmark<ValueTuple<long,long>> b)?
                    new TableRowBookmark(_trs,b,Position+1):null;
            }
            public override STransaction Update(STransaction tr, SDict<string, Serialisable> assigs)
            {
                return (STransaction)tr.Install(new SUpdate(tr, 
                    _ob.rec??throw new Exception("??"), assigs),tr.curpos); 
            }
            public override STransaction Delete(STransaction tr)
            {
                var rc = _ob.rec ?? throw new Exception("??");
                return (STransaction)tr.Install(new SDelete(tr,rc.table, rc.Defpos),tr.curpos); // ok
            }
        }
    }
    public class IndexRowSet : RowSet
    {
        public readonly SIndex _ix;
        public readonly SList<Serialisable> _wh;
        public readonly SCList<Variant> _key;
        public readonly bool _unique;
        public IndexRowSet(STransaction tr,STable t,SIndex ix,SCList<Variant> key, SList<Serialisable> wh) 
            :base(Rdc(tr,ix,key),t, SDict<long,SFunction>.Empty, t.rows.Length)
        {
            _ix = ix; _key = key; _wh = wh;
            _unique = key.Length == _ix.cols.Length;
        }
        /// <summary>
        /// Add in read constraints: if the key means just one row that is the read
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
            protected IndexRowBookmark(IndexRowSet irs,SRow ob,MTreeBookmark<Serialisable> mbm,int p) :base(irs,ob,p)
            {
                _irs = irs; _mbm = mbm;
            }
            internal static IndexRowBookmark? New(IndexRowSet irs)
            {
                var k = irs._key;
                var b = (MTreeBookmark<Serialisable>?)((k.Length!=0) ? irs._ix.rows.PositionAt(k) 
                    : irs._ix.rows.First());
                for (;b != null; b = b.Next() as MTreeBookmark<Serialisable>)
                {
                    var rc = irs._tr.Get(b.Value.Item2);
                    var rb = new IndexRowBookmark(irs, new SRow(irs._tr, rc), b, 0);
                    if (rc.Matches(rb, irs._wh))
                        return rb;
                }
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                if (_irs._unique)
                    return null;
                for (var b = _mbm.Next(); b != null; b = b.Next())
                {
                    var rc = _irs._tr.Get(b.Value.Item2);
                    var rb = new IndexRowBookmark(_irs, new SRow(_irs._tr,rc), 
                        (MTreeBookmark<Serialisable>)b, Position + 1);
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
            public override STransaction Update(STransaction tr, SDict<string, Serialisable> assigs)
            {
                return (STransaction)tr.Install(new SUpdate(tr, _ob.rec??throw new System.Exception("No record"), assigs),tr.curpos); // ok
            }
            public override STransaction Delete(STransaction tr)
            {
                var rc = _ob.rec ?? throw new System.Exception("No record");
                return (STransaction)tr.Install(new SDelete(tr, rc.table, rc.Defpos),tr.curpos); // ok
            }
        }
    }
    public class SearchRowSet : RowSet
    {
        public readonly SSearch _sch;
        public readonly RowSet _sce;

        public SearchRowSet(STransaction tr, SQuery top, SSearch sc, SDict<long, SFunction> ags, Context cx)
            : this(Source(tr, top, sc, ags, cx), sc, ags)
        { }
        SearchRowSet(RowSet sce, SSearch sc, SDict<long, SFunction> ags) : base(sce._tr, sc, ags, null)
        {
            _sch = sc;
            _sce = sce;
        }
        static RowSet Source(STransaction tr,SQuery top, SSearch sc,SDict<long,SFunction>ags,Context cx)
        { 
            RowSet? s = null;
            var matches = SDict<long,Serialisable>.Empty;
            if (sc.sce is STable tb)
            {
                for (var wb = sc.where.First(); wb != null; wb = wb.Next())
                    if (wb.Value.Lookup(cx) is SExpression x && x.op == SExpression.Op.Eql)
                    {
                        if (x.left is SColumn c && tb.names.Contains(c.name) &&
                            x.right != null && x.right.isValue)
                            matches = matches+ (c.uid, x.right);
                        else if (x.right is SColumn cr && tb.names.Contains(cr.name) &&
                                x.left != null && x.left.isValue)
                            matches = matches+(cr.uid,x.left);
                    }
                var best = SCList<Variant>.Empty;
                if (matches.Length != null)
                    for (var b = tb.indexes.First(); best.Length != null && matches.Length.Value > best.Length.Value && b != null;
                        b = b.Next())
                    {
                        var ma = SCList<Variant>.Empty;
                        var ix = (SIndex)tr.objects[b.Value.Item1];
                        for (var wb = ix.cols.First(); ma.Length != null && wb != null; wb = wb.Next())
                        {
                            if (!matches.Contains(wb.Value))
                                break;
                            ma = ma.InsertAt(new Variant(Variants.Ascending, matches[wb.Value]),
                                ma.Length.Value);
                        }
                        if (ma.Length != null && ma.Length.Value > best.Length.Value)
                        {
                            best = ma;
                            s = new IndexRowSet(tr, tb, ix, ma, sc.where);
                            tr = s._tr;
                        }
                    }
            }
            return s?? sc.sce?.RowSet(tr,top,ags,cx) ?? throw new System.Exception("??");
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
                base(sr, bm._ob, p)
            {
                _sch = sr; _bmk = bm;
            }
            internal static SearchRowBookmark? New(SearchRowSet rs)
            {
                for (var b = rs._sce.First(); b != null; b = b.Next())
                {
                    var rb = new SearchRowBookmark(rs, (RowBookmark)b, 0);
                    if (rb.Matches(rs._sch.where,Context.Empty)==true)
                        return rb;
                }
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                for (var b = _bmk.Next(); b != null; b = b.Next())
                {
                    var rb = new SearchRowBookmark(_sch, (RowBookmark)b, Position + 1);
                    if (rb.Matches(_sch._sch.where,Context.Empty)==true)
                        return rb;
                }
                return null;
            }
            public override STransaction Update(STransaction tr, SDict<string, Serialisable> assigs)
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
        public EvalRowSet(RowSet r,SQuery q,SDict<long,SFunction>ags,Context cx)
            : base(r._tr,q,ags,null)
        {
            var vs = SDict<long, Serialisable>.Empty;
            for (var b = r.First() as RowBookmark;b!=null;
                b=b.Next() as RowBookmark)
                for (var ab = ags.First();ab!=null;ab=ab.Next())
                {
                    var f = ab.Value.Item2;
                    var v = f.arg.Lookup(new Context(b,cx));
                    if (v!=Serialisable.Null)
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
                r += (ab.Value.Item2, b.Value.Item2.Lookup(new Context(_vals, null)));
            return new EvalRowBookmark(this,r, _vals);
        }
        public class EvalRowBookmark : RowBookmark
        {
            internal EvalRowBookmark(EvalRowSet ers, SRow r,SDict<long,Serialisable> a) 
                : base(ers, r, a, 0) { }
            public override Bookmark<Serialisable>? Next()
            {
                return null;
            }
        }
    }
    public class GroupRowSet : RowSet
    {
        public readonly SGroupQuery _gqry;
        public readonly SList<TreeInfo<string>> _info; // computed from the grouped columns
        public readonly SMTree<string> _tree; // for the treeinfo in the GroupRowSet
        public readonly SDict<long, SDict<long,Serialisable>> _grouprows; // accumulators for the aggregates
        public readonly SQuery _top;
        public readonly RowSet _sce;
        public GroupRowSet(STransaction tr, SQuery top, SGroupQuery gqry,
            SDict<long, SFunction> ags, Context cx) 
            : this(gqry.source.RowSet(tr, top, ags, cx), top, gqry, ags, cx)
        {
        }
        GroupRowSet(RowSet sce,SQuery top,SGroupQuery gqry,SDict<long,SFunction>ags,Context cx)
            :base(sce._tr,gqry,ags,null)
        {
            _gqry = gqry;
            _sce = sce;
            var inf = SList<TreeInfo<string>>.Empty;
            for (var b = gqry.groupby.First(); b != null; b = b.Next())
                inf += (new TreeInfo<string>(b.Value.Item2, 'd', 'i'), b.Value.Item1);
            _info = inf;
            var t = new SMTree<string>(inf);
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
                r += (m, AddIn(ags, r[m], new Context(b, cx)));
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
        protected SRow _Row(MTreeBookmark<string> b)
        {
            var r = new SRow();
            var kc = SDict<string, Serialisable>.Empty;
            var gb = b.Value.Item1.First();
            for (var kb = _info.First(); gb != null && kb != null; gb = gb.Next(), kb = kb.Next())
                kc += (kb.Value.headName, (Serialisable)gb.Value.ob);
            var cx = new Context(kc, _grouprows[b.Value.Item2], null);
            var ab = _top.Display.First();
            for (var cb = _top.cpos.First(); ab != null && cb != null; ab = ab.Next(), cb = cb.Next())
                r += (ab.Value.Item2,cb.Value.Item2.Lookup(cx));
            return r;
        }
        static SDict<long,Serialisable> AddIn(SDict<long,SFunction> ags, SDict<long,Serialisable> cur, Context cx)
        {
            for (var b=ags.First(); b!=null;b=b.Next())
            {
                var f = b.Value.Item2;
                var v = f.arg.Lookup(new Context(cur,cx));
                if (v != Serialisable.Null)
                    cur += (f.fid,cur.Contains(f.fid)?f.AddIn(cur[f.fid],v)
                        :f.StartCounter(v));
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
            public readonly MTreeBookmark<string> _bmk;
            protected GroupRowBookmark(GroupRowSet grs, MTreeBookmark<string> b,
                SRow r, SDict<long,Serialisable> a, int p)
                : base(grs,r,a,p)
            {
                _grs = grs; _bmk = b;
            }
            internal static GroupRowBookmark? New(GroupRowSet rs)
            {
                var b = rs._tree.First() as MTreeBookmark<string>;
                if (b == null)
                    return null;
                return new GroupRowBookmark(rs, b, rs._Row(b), rs._grouprows[0], 0);
            }
            public override Bookmark<Serialisable>? Next()
            {
                var b = _bmk.Next() as MTreeBookmark<string>;
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
        public SelectRowSet(RowSet sce,SSelectStatement sel,SDict<long,SFunction>ags,Context cx)
            :base(sce._tr,sel,ags,null)
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
                :base(rs,rw,p)
            {
                _srs = rs; _bmk = bmk;
            }
            internal static SelectRowBookmark? New(SelectRowSet rs)
            {
                for (var b = rs._source.First() as RowBookmark;b!=null;b=b.Next() as RowBookmark )
                {
                    var rw = (SRow)rs._qry.Lookup(new Context(b,null));
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
                    var rw = (SRow)_srs._qry.Lookup(new Context(b,null));
                    if (rw.isNull)
                        continue;
                    var rb = new SelectRowBookmark(_srs, b, rw, Position + 1);
                    if (rb._ob.cols.Length != 0)
                        return rb;
                }
                return null;
            }
            public override STransaction Update(STransaction tr, SDict<string, Serialisable> assigs)
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
        internal JoinRowSet(SQuery top, SJoin j, RowSet lf, RowSet rg, SDict<long, SFunction> a, Context cx)
            : base(rg._tr, j, a, null)
        {
            _join = j;
            var lti = SList<TreeInfo<Serialisable>>.Empty;
            var rti = SList<TreeInfo<Serialisable>>.Empty;
            for (var b = j.ons.First();b!=null;b=b.Next())
            {
                var e = b.Value;
                if (e.op != SExpression.Op.Eql)
                    continue;
                lti += new TreeInfo<Serialisable>((SColumn)e.left, 'A', 'D');
                rti += new TreeInfo<Serialisable>((SColumn)e.right, 'A', 'D');
            }
            for (var b = j.uses.First(); b != null; b = b.Next())
            {
                var e = b.Value;
                lti += new TreeInfo<Serialisable>(j.left.names[e], 'A', 'D');
                rti += new TreeInfo<Serialisable>(j.right.names[e], 'A', 'D');
            }
            _klen = lti.Length??0;
            if (lti.Length!=0)
            {
                lf = new OrderedRowSet(lf, lti, cx);
                rg = new OrderedRowSet(rg, rti, cx);
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
                :base(jrs,_Row(jrs,left,ul,right,ur),pos)
            {
                _jrs = jrs; _lbm = left; _useL = ul; _rbm = right; _useR = ur;
            }
            static SRow _Row(JoinRowSet jrs,RowBookmark? lbm,bool ul,RowBookmark? rbm,bool ur)
            {
                var r = new SRow();
                Bookmark<(int, string)>? ab;
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
                                        if (rbm?._ob.vals.Contains(n) == true)
                                            n = jrs._left._qry.Alias + "." + n;
                                        r += (n, b.Value.Item2);
                                    }
                            }
                            if (rbm != null && ur)
                            {
                                ab = rbm?._ob.names.First();
                                for (var b = rbm?._ob.cols.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                                {
                                    var n = ab.Value.Item2;
                                    if (lbm?._ob.vals.Contains(n) == true)
                                        n = jrs._right._qry.Alias + "." + n;
                                    r += (n, b.Value.Item2);
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
                                    if (lbm==null || !ul || !lbm._ob.vals.Contains(ab.Value.Item2))
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
                    return new JoinRowBookmark(_jrs, lbm, true, null, false, Position+1);
                if (rbm != null && _jrs._join.joinType.HasFlag(SJoin.JoinType.Right))
                    return new JoinRowBookmark(_jrs, null, false, rbm, true, Position+1);
                return null;
            }
        }
    }
    public class SysRows : RowSet
    {
        public readonly SysTable tb;
        public readonly AStream fs;
        internal SysRows(STransaction tr, SysTable t) 
            : base(tr, t, SDict<long,SFunction>.Empty, null)
        {
            tb = t; fs = tr.File();
        }
        public override Bookmark<Serialisable>? First()
        {
            switch (tb.name)
            {
                case "_Log": return LogBookmark.New(this, 0, 0);
                case "_Tables": return TablesBookmark.New(this, 0, 0);
            }
            return null;
        }
        public SRow _Row(params Serialisable[] vals)
        {
            var r = new SRow();
            int j = 0;
            for (var b = tb.cpos.First(); b != null; b = b.Next())
                if (b.Value.Item2 is SSelector s)
                    r += (s.name, vals[j++]);
                        // Serialisable.New(((SColumn)b.Value.val).dataType, vals[j++]));
            return r;
        }
        internal class LogBookmark : RowBookmark
        {
            public readonly SysRows _srs;
            public readonly long _log;
            public readonly long _next;
            internal LogBookmark(SysRows rs, long lg, SDbObject ob, long nx, int p) 
                : base(rs, rs._Row(new SString(ob.Uid()), // Uid
                    new SInteger((int)ob.type), //Type
                    new SString(ob.ToString())), p)  // Desc
            {
                _srs = rs;  _log = lg; _next = nx;
            }
            internal static LogBookmark? New(SysRows rs, long lg, int pos)
            {
                var rdr = new Reader(rs.fs, lg);
                return (rdr._Get(rs._tr) is SDbObject ob) ?
                    new LogBookmark(rs, lg, ob, rdr.Position, pos) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return New((SysRows)_rs, _next, Position + 1);
            }
        }
        internal class TablesBookmark : RowBookmark
        {
            public readonly SysRows _srs;
            public readonly long _log;
            public readonly long _next;
            internal TablesBookmark(SysRows rs, long lg, STable tb, long nx, int p)
                : base(rs, rs._Row(new SString(tb.name), // Name
                    new SInteger(tb.cpos.Length??0), // Cols
                    new SInteger(tb.rows.Length??0)), p)  //Rows
            {
                _srs = rs; _log = lg; _next = nx;
            }
            internal static TablesBookmark? New(SysRows rs, long lg, int pos)
            {
                var rdr = new Reader(rs.fs, lg);
                for (var ob = rdr._Get(rs._tr);ob!=null;ob = rdr._Get(rs._tr))
                {
                    if (ob is STable tb)
                        return new TablesBookmark(rs, lg, tb, rdr.Position, pos);
                }
                return null;
            }

            public override Bookmark<Serialisable>? Next()
            {
                return New((SysRows)_rs, _next, Position + 1);
            }
        }
    }
}
