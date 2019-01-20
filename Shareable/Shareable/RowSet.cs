#nullable enable
namespace Collection
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
        public RowSet(STransaction tr, SQuery q, int? n):base(n)
        {
            _tr = tr; _qry = q;
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
        protected RowBookmark(RowSet rs, SRow ob, int p) : base(p)
        {
            _rs = rs; _ob = ob;
        }
        public override Serialisable Value => _ob; // should always be an SRow

        public Serialisable this[string s] => (s.CompareTo(_rs._qry.Alias) == 0)?_ob:_ob.vals[s];

        public virtual bool SameGroupAs(RowBookmark r)
        {
            return true;
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
        public DistinctRowSet(RowSet sce) : base(sce._tr, sce._qry, null)
        {
            _sce = sce;
            var r = SDict<SRow, bool>.Empty;
            for (var b = sce.First(); b != null; b = b.Next())
                r = r + (((RowBookmark)b)._ob, true);
            rows = r;
        }
        public override Bookmark<Serialisable>? First()
        {
            return DistinctRowBookmark.New(this);
        }
        internal class DistinctRowBookmark : RowBookmark
        {
            public readonly DistinctRowSet _drs;
            public readonly Bookmark<SSlot<SRow,bool>> _bmk;
            DistinctRowBookmark(DistinctRowSet drs,Bookmark<SSlot<SRow,bool>> bmk,int pos) 
                : base(drs,bmk.Value.key,pos)
            { _drs = drs; _bmk = bmk; }
            internal static DistinctRowBookmark? New(DistinctRowSet drs)
            {
                return (drs.rows.First() is Bookmark<SSlot<SRow, bool>> rb) ?
                    new DistinctRowBookmark(drs, rb, 0) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return (_bmk.Next() is Bookmark<SSlot<SRow,bool>> rb)?
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
        internal class Play
        {
            public static void X()
            {
                var x = (5, 6);

            }
        }
    }
    public class OrderedRowSet : RowSet
    {
        public readonly RowSet _sce;
        public readonly SMTree<Serialisable> _tree;
        public readonly SDict<int, SRow> _rows;
        public OrderedRowSet(RowSet sce,SSelectStatement sel) :base(sce._tr,sel,sce.Length)
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
                    k[i] = new Variant(c.Value.col.Lookup(b),!c.Value.desc);
                t = t.Add(m,k);
                r = r+(m++, b._ob);
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
                :base(ors,ors._rows[(int)bmk.Value.val],pos)
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
        public TableRowSet(STransaction db,STable t) : base(db,t, t.rows.Length)
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
            public Bookmark<SSlot<long, long>> _bmk;
            protected TableRowBookmark(TableRowSet trs,Bookmark<SSlot<long,long>>bm,int p) 
                :base(trs,new SRow(trs._tr,trs._tr.Get(bm.Value.val)),p)
            {
                _trs = trs; _bmk = bm;
            }
            internal static TableRowBookmark? New(TableRowSet trs)
            {
                return (trs._tb.rows.First() is Bookmark<SSlot<long, long>> b) ?
                    new TableRowBookmark(trs, b, 0) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return (_bmk.Next() is Bookmark<SSlot<long,long>> b)?
                    new TableRowBookmark(_trs,b,Position+1):null;
            }
            public override STransaction Update(STransaction tr, SDict<string, Serialisable> assigs)
            {
                return (STransaction)tr.Install(new SUpdate(tr, _ob.rec, assigs),tr.curpos); // ok
            }
            public override STransaction Delete(STransaction tr)
            {
                return (STransaction)tr.Install(new SDelete(tr, _ob.rec.table, _ob.rec.Defpos),tr.curpos); // ok
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
            :base(tr,t, t.rows.Length)
        {
            _ix = ix; _key = key; _wh = wh;
            _unique = key.Length == _ix.cols.Length;
        }
        public override Bookmark<Serialisable>? First()
        {
            return IndexRowBookmark.New(this);
        }
        internal class IndexRowBookmark : RowBookmark
        {
            public readonly IndexRowSet _irs;
            public readonly MTreeBookmark<long> _mbm;
            protected IndexRowBookmark(IndexRowSet irs,SRow ob,MTreeBookmark<long> mbm,int p) :base(irs,ob,p)
            {
                _irs = irs; _mbm = mbm;
            }
            internal static IndexRowBookmark? New(IndexRowSet irs)
            {
                var k = irs._key;
                var b = (MTreeBookmark<long>?)((k.Length.Value > 0) ? irs._ix.rows.PositionAt(k) 
                    : irs._ix.rows.First());
                for (;b != null; b = b.Next() as MTreeBookmark<long>)
                {
                    var rc = irs._tr.Get(b.Value.val);
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
                    var rc = _irs._tr.Get(b.Value.val);
                    var rb = new IndexRowBookmark(_irs, new SRow(_irs._tr,rc), 
                        (MTreeBookmark<long>)b, Position + 1);
                    if (rc.Matches(rb, _irs._wh))
                        return rb;
                }
                return null;
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
        public SearchRowSet(STransaction tr,SSearch sc,ILookup<string,Serialisable> nms) :base (tr,sc, null)
        {
            _sch = sc;
            RowSet? s = null;
            var matches = SDict<long,Serialisable>.Empty;
            if (_sch.sce is STable tb)
            {
                for (var wb = _sch.where.First(); wb != null; wb = wb.Next())
                    if (wb.Value.Lookup(nms) is SExpression x && x.op == SExpression.Op.Eql)
                    {
                        if (x.left is SColumn c && tb.names.Contains(c.name) &&
                            x.right != null && x.right.isValue)
                            matches = matches+ (c.uid, x.right);
                        else if (x.right is SColumn cr && tb.names.Contains(cr.name) &&
                                x.left != null && x.left.isValue)
                            matches = matches+(cr.uid,x.left);
                    }
                var best = SCList<Variant>.Empty;
                for (var b = tb.indexes.First(); matches.Length.Value > best.Length.Value && b != null; 
                    b = b.Next())
                {
                    var ma = SCList<Variant>.Empty;
                    var ix = (SIndex)tr.objects[b.Value.key];
                    for (var wb = ix.cols.First(); wb != null; wb = wb.Next())
                    {
                        if (!matches.Contains(wb.Value))
                            break;
                        ma = ma.InsertAt(new Variant(Variants.Ascending,matches[wb.Value]),
                            ma.Length.Value);
                    }
                    if (ma.Length.Value > best.Length.Value)
                    {
                        best = ma;
                        s = new IndexRowSet(tr, tb, ix, ma, sc.where);
                    }
                }
            }
            _sce = s?? _sch.sce?.RowSet(tr,nms) ?? throw new System.Exception("??");
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
                    if (((RowBookmark)b)._ob.rec?.Matches(rb, rs._sch.where)==true)
                        return rb;
                }
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                for (var b = _bmk.Next(); b != null; b = b.Next())
                {
                    var rb = new SearchRowBookmark(_sch, (RowBookmark)b, Position + 1);
                    if (((RowBookmark)b)._ob.rec?.Matches(rb, _sch._sch.where)==true)
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
    public class GroupRowSet : RowSet
    {
        public readonly SGroupQuery _gqry;
        public readonly RowSet _sce;
        public GroupRowSet(STransaction tr,SGroupQuery gqry,ILookup<string,Serialisable> nms) :base(tr,gqry,null)
        {
            _gqry = gqry;
            _sce = gqry.source.RowSet(tr, nms);
        }
        public override Bookmark<Serialisable>? First()
        {
            return GroupRowBookmark.New(this);
        }
        internal class GroupRowBookmark : RowBookmark
        {
            public readonly GroupRowSet _grs;
            public RowBookmark _bmk;
            protected GroupRowBookmark(GroupRowSet grs,RowBookmark bm,int p)
                : base(grs,_Row(grs,bm),p)
            {
                _grs = grs; _bmk = bm;
            }
            static SRow _Row(GroupRowSet grs,RowBookmark bm)
            {
                throw new System.NotImplementedException();
            }
            internal static GroupRowBookmark? New(GroupRowSet rs)
            {
                return (rs._sce.First() is RowBookmark b) ? new GroupRowBookmark(rs, b,0) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                return (_bmk.Next() is RowBookmark b) ? new GroupRowBookmark(_grs, b, 0) : null;
            }
        }
    }
    public class SelectRowSet : RowSet
    {
        public readonly SSelectStatement _sel;
        public readonly RowSet _source;
        public SelectRowSet(STransaction tr,SSelectStatement sel,ILookup<string,Serialisable> nms)
            :base(tr,sel,null)
        {
            _sel = sel;
            _source = sel.qry.RowSet(tr,nms);
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
                    var rw = (SRow)rs._qry.Lookup(b);
                    if (rw.isNull)
                        continue;
                    var rb = new SelectRowBookmark(rs, b, rw, 0);
                    if (((SRow)rb._ob).cols.Length.Value!=0)
                        return rb;
                    if (rs._sel.aggregates) // aggregates collapse the rowset
                        break;
                }
                return null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                if (!_srs._sel.aggregates) // aggregates collapse the rowset
                    for (var b = _bmk.Next() as RowBookmark; b != null; b = b.Next() as RowBookmark)
                    {
                        var rw = (SRow)_srs._qry.Lookup(b);
                        if (rw.isNull)
                            continue;
                        var rb = new SelectRowBookmark(_srs, b, rw, Position + 1);
                        if (((SRow)rb._ob).cols.Length.Value != 0)
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
        internal JoinRowSet(STransaction tr,SJoin j,ILookup<string,Serialisable> nms)
            : base(tr,j,null)
        {
            _join = j;
            _left = j.left.RowSet(tr, nms);
            _right = j.right.RowSet(tr, nms);
        }
        public override Bookmark<Serialisable>? First()
        {
            return JoinRowBookmark.New(this);
        }
        public class JoinRowBookmark : RowBookmark
        {
            public readonly JoinRowSet _jrs;
            public readonly RowBookmark _lbm, _rbm;
            protected JoinRowBookmark(JoinRowSet jrs,RowBookmark lbm,RowBookmark rbm,int pos)
                : base(jrs,_Row(jrs,lbm,rbm),pos)
            {
                _jrs = jrs; _lbm = lbm; _rbm = rbm;
            }
            static SRow _Row(JoinRowSet jrs,RowBookmark lbm,RowBookmark rbm)
            {
                throw new System.NotImplementedException();
            }
            public static RowBookmark? New(JoinRowSet jrs)
            {
                return (jrs._left.First() is RowBookmark lb && jrs._right.First() is RowBookmark rb) ?
                    new JoinRowBookmark(jrs, lb, rb, 0) : null;
            }
            public override Bookmark<Serialisable>? Next()
            {
                throw new System.NotImplementedException();
            }
        }
    }
    public class SysRows : RowSet
    {
        public readonly SysTable tb;
        public readonly AStream fs;
        internal SysRows(STransaction tr, SysTable t) : base(tr, t, null)
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
                if (b.Value.val is SSelector s)
                    r = r.Add(s.name, vals[j++]);
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
                    new SInteger(tb.cpos.Length.Value), // Cols
                    new SInteger(tb.rows.Length.Value)), p)  //Rows
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
