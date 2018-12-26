namespace Shareable
{
    /// <summary>
    /// In the server, a rowSet is a Shareable(Serialisable),
    /// and the rows are generally fully-evaluated SRows.
    /// The SRows are sometimes instantiated on traversal instead of on definition.
    /// At the client, a rowset is a DocArray and the rows are Documents.
    /// </summary>
    public abstract class RowSet : Shareable<Serialisable>
    {
        public readonly SQuery _qry;
        public readonly SDatabase _db;
        public RowSet(SDatabase d, SQuery q, int? n):base(n)
        {
            _db = d; _qry = q;
        }
    }
    /// <summary>
    /// A RowBookmark evaluates its Serialisable _ob (usually an SRow).
    /// This matters especially for SSelectStatements
    /// </summary>
    public abstract class RowBookmark : Bookmark<Serialisable>
    {
        public readonly RowSet _rs;
        public readonly Serialisable _ob;
        protected RowBookmark(RowSet rs, Serialisable ob, int p) : base(p)
        {
            _rs = rs; _ob = ob;
        }
        public override Serialisable Value => _ob; // should always be an SRow
        public virtual bool SameGroupAs(RowBookmark r)
        {
            return true;
        }
    }
    public class DistinctRowSet : RowSet
    {
        public readonly RowSet _sce;
        public readonly SDict<SRow, bool> rows;
        public DistinctRowSet(RowSet sce) : base(sce._db, sce._qry, null)
        {
            _sce = sce;
            var r = SDict<SRow, bool>.Empty;
            for (var b = sce.First(); b != null; b = b.Next())
                r = r.Add((SRow)((RowBookmark)b)._ob, true);
            rows = r;
        }
        public override Bookmark<Serialisable> First()
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
            internal static DistinctRowBookmark New(DistinctRowSet drs)
            {
                return (drs.rows.First() is Bookmark<SSlot<SRow, bool>> rb) ?
                    new DistinctRowBookmark(drs, rb, 0) : null;
            }
            public override Bookmark<Serialisable> Next()
            {
                return (_bmk.Next() is Bookmark<SSlot<SRow,bool>> rb)?
                    new DistinctRowBookmark(_drs,rb,Position+1):null;
            }
        }
    }
    public class OrderedRowSet : RowSet
    {
        public readonly RowSet _sce;
        public readonly SMTree<Serialisable> _tree;
        public readonly SDict<int, Serialisable> _rows;
        public OrderedRowSet(RowSet sce,SSelectStatement sel) :base(sce._db,sel,sce.Length)
        {
            var ti = SList<TreeInfo<Serialisable>>.Empty;
            int n = 0;
            for (var b = sel.order.First(); b != null; b = b.Next())
                ti = ti.InsertAt(new TreeInfo<Serialisable>(b.Value, 'A', 'D',!b.Value.desc), n++);
            var t = new SMTree<Serialisable>(ti);
            var r = SDict<int, Serialisable>.Empty;
            int m = 0;
            for (var b = sce.First() as RowBookmark; b != null; b = b.Next() as RowBookmark)
            {
                var k = new Variant[n];
                var i = 0;
                for (var c = sel.order.First(); c != null; c = c.Next())
                    k[i] = new Variant(b._ob[c.Value.col.name],!c.Value.desc);
                t = t.Add(m,k);
                r = r.Add(m++, b._ob);
            }
            _tree = t;
            _rows = r;
        }
        public override Bookmark<Serialisable> First()
        {
            return OrderedBookmark.New(this);
        }
        internal class OrderedBookmark : RowBookmark
        {
            public readonly OrderedRowSet _ors;
            public readonly MTreeBookmark<Serialisable> _bmk;
            OrderedBookmark(OrderedRowSet ors,MTreeBookmark<Serialisable> bmk,int pos)
                :base(ors,ors._rows.Lookup((int)bmk.Value.val),pos)
            {
                _ors = ors; _bmk = bmk;
            }
            internal static OrderedBookmark New(OrderedRowSet ors)
            {
                return (ors._tree.First() is MTreeBookmark<Serialisable> rb) ? 
                    new OrderedBookmark(ors, rb, 0) : null;
            }
            public override Bookmark<Serialisable> Next()
            {
                return (_bmk.Next() is MTreeBookmark<Serialisable> rb) ?
                    new OrderedBookmark(_ors, rb, Position+1) : null;
            }
        }
    }
    public class TableRowSet : RowSet
    {
        public readonly STable _tb;
        public TableRowSet(SDatabase db,STable t) : base(db,t, t.rows.Length)
        {
            _tb = t;
        }

        public override Bookmark<Serialisable> First()
        {
            return TableRowBookmark.New(this);
        }
        internal class TableRowBookmark : RowBookmark
        {
            public readonly TableRowSet _trs;
            public Bookmark<SSlot<long, long>> _bmk;
            protected TableRowBookmark(TableRowSet trs,Bookmark<SSlot<long,long>>bm,int p) 
                :base(trs,trs._db.Get(bm.Value.val),p)
            {
                _trs = trs; _bmk = bm;
            }
            internal static TableRowBookmark New(TableRowSet trs)
            {
                return (trs._tb.rows.First() is Bookmark<SSlot<long, long>> b) ?
                    new TableRowBookmark(trs, b, 0) : null;
            }
            public override Bookmark<Serialisable> Next()
            {
                return (_bmk.Next() is Bookmark<SSlot<long,long>> b)?
                    new TableRowBookmark(_trs,b,Position+1):null;
            }
        }
    }
    public class IndexRowSet : RowSet
    {
        public readonly SIndex _ix;
        public readonly SList<Serialisable> _wh;
        public readonly SCList<Variant> _key;
        public readonly bool _unique;
        public IndexRowSet(SDatabase db,STable t,SIndex ix,SList<Serialisable> wh) :base(db,t, t.rows.Length)
        {
            _ix = ix; _wh = wh;
            var key = SCList<Variant>.Empty;
            int n = 0;
            for (var c = _ix.cols; c != null && c.Length != 0; c = c.next)
            {
                for (var b = _wh.First(); b != null; b = b.Next())
                    if (b.Value is SExpression x && x.op==SExpression.Op.Eql &&
                        x.left is SColumn sc && sc.uid == c.element)
                    {
                        key = (SCList<Variant>)key.InsertAt(new Variant(Variants.Ascending,x.right), n++);
                        goto okay;
                    }
                break;
            okay:;
            }
            _key = key;
            _unique = key.Length == _ix.cols.Length;
        }
        public override Bookmark<Serialisable> First()
        {
            return IndexRowBookmark.New(this);
        }
        internal class IndexRowBookmark : RowBookmark
        {
            public readonly IndexRowSet _irs;
            public readonly MTreeBookmark<long> _mbm;
            protected IndexRowBookmark(IndexRowSet irs,Serialisable ob,MTreeBookmark<long> mbm,int p) :base(irs,ob,p)
            {
                _irs = irs; _mbm = mbm;
            }
            internal static IndexRowBookmark New(IndexRowSet irs)
            {
                for (var b = irs._ix.rows.PositionAt(irs._key); b != null; b = b.Next() as MTreeBookmark<long>)
                {
                    var r = irs._db.Get(b.Value.val);
                    var rb = new IndexRowBookmark(irs, r, b, 0);
                    if (r.Matches(rb, irs._wh))
                        return rb;
                }
                return null;
            }
            public override Bookmark<Serialisable> Next()
            {
                if (_irs._unique)
                    return null;
                for (var b = _mbm.Next(); b != null; b = b.Next())
                {
                    var r = _irs._db.Get(b.Value.val);
                    var rb = new IndexRowBookmark(_irs, r, b as MTreeBookmark<long>, Position + 1);
                    if (r.Matches(rb, _irs._wh))
                        return rb;
                }
                return null;
            }
        }
    }
    public class SearchRowSet : RowSet
    {
        public readonly SSearch _sch;
        public readonly RowSet _sce;
        public SearchRowSet(SDatabase db,SSearch sc) :base (db,sc, null)
        {
            _sch = sc;
            _sce = (_sch.sce is STable tb && db.GetPrimaryIndex(tb.uid) is SIndex ix) ?
                new IndexRowSet(db, tb, ix, _sch.where) :
                _sch.sce.RowSet(db);
        }
        public override Bookmark<Serialisable> First()
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
            internal static SearchRowBookmark New(SearchRowSet rs)
            {
                for (var b = rs.First(); b != null; b = b.Next())
                {
                    var rb = new SearchRowBookmark(rs, (RowBookmark)b, 0);
                    if (((SRecord)((RowBookmark)b)._ob).Matches(rb, rs._sch.where))
                        return rb;
                }
                return null;
            }
            public override Bookmark<Serialisable> Next()
            {
                for (var b = _bmk.Next(); b != null; b = b.Next())
                {
                    var rb = new SearchRowBookmark(_sch, (RowBookmark)b, Position + 1);
                    if (((SRecord)((RowBookmark)b)._ob).Matches(rb, _sch._sch.where))
                        return rb;
                }
                return null;
            }
        }
    }
    public class SelectRowSet : RowSet
    {
        public readonly RowSet _source;
        public SelectRowSet(SDatabase db,SSelectStatement sel):base(db,sel,null)
        {
            _source = sel.qry.RowSet(db);
        }

        public override Bookmark<Serialisable> First()
        {
            return SelectRowBookmark.New(this);
        }
        internal class SelectRowBookmark : RowBookmark
        {
            public readonly SelectRowSet _srs;
            public readonly RowBookmark _bmk;
            SelectRowBookmark(SelectRowSet rs,RowBookmark bmk,int p)
                :base(rs,new SRow(rs._qry,bmk._ob as SRow),p)
            {
                _srs = rs; _bmk = bmk;
            }
            internal static SelectRowBookmark New(SelectRowSet rs)
            {
                return new SelectRowBookmark(rs, rs._source.First() as RowBookmark, 0);
            }
            public override Bookmark<Serialisable> Next()
            {
                return (_bmk.Next() is RowBookmark bmk) ?
                    new SelectRowBookmark(_srs, bmk, Position + 1) : null;
            }
        }
    }
    public class SysRows : RowSet
    {
        public readonly SysTable tb;
        public readonly AStream fs;
        internal SysRows(SDatabase d, SysTable t) : base(d, t, null)
        {
            tb = t; fs = d.File();
        }
        public override Bookmark<Serialisable> First()
        {
            switch (tb.name)
            {
                case "_Log": return LogBookmark.New(this, 0, 0);
                case "_Tables": return TablesBookmark.New(this, 0, 0);
            }
            return null;
        }
        public SRow _Row(params object[] vals)
        {
            var r = new SRow();
            int j = 0;
            for (var b = tb.cpos.First(); b != null; b = b.Next())
                if (b.Value is SSelector s)
                    r = r.Add(s.name, Serialisable.New(((SColumn)b.Value).dataType, vals[j++]));
            return r;
        }
        internal class LogBookmark : RowBookmark
        {
            public readonly SysRows _srs;
            public readonly long _log;
            public readonly long _next;
            internal LogBookmark(SysRows rs, long lg, SDbObject ob, long nx, int p) 
                : base(rs, rs._Row(ob.Uid(), // Uid
                    (int)ob.type, //Type
                    ob.ToString()), p)  // Desc
            {
                _srs = rs;  _log = lg; _next = nx;
            }
            internal static LogBookmark New(SysRows rs, long lg, int pos)
            {
                var rdr = new Reader(rs.fs, lg);
                return (rdr._Get(rs._db) is SDbObject ob) ?
                    new LogBookmark(rs, lg, ob, rdr.Position, pos) : null;
            }
            public override Bookmark<Serialisable> Next()
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
                : base(rs, rs._Row(tb.name, // Name
                    tb.cols.Length, // Cols
                    tb.rows.Length), p)  //Rows
            {
                _srs = rs; _log = lg; _next = nx;
            }
            internal static TablesBookmark New(SysRows rs, long lg, int pos)
            {
                var rdr = new Reader(rs.fs, lg);
                for (var ob = rdr._Get(rs._db);ob!=null;ob = rdr._Get(rs._db))
                {
                    if (ob is STable tb)
                        return new TablesBookmark(rs, lg, tb, rdr.Position, pos);
                }
                return null;
            }

            public override Bookmark<Serialisable> Next()
            {
                return New((SysRows)_rs, _next, Position + 1);
            }
        }
    }
}
