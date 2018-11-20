namespace Shareable
{
    public abstract class RowSet : Shareable<Serialisable>
    {
        public readonly SQuery _qry;
        public readonly SDatabase _db;
        public RowSet(SDatabase d, SQuery q)
        {
            _db = d; _qry = q;
        }
    }
    public abstract class RowBookmark : Bookmark<Serialisable>
    {
        public readonly RowSet _rs;
        public readonly Serialisable _ob;
        protected RowBookmark(RowSet rs, Serialisable ob, int p) : base(p)
        {
            _rs = rs; _ob = ob;
        }
        public override Serialisable Value => _ob;
    }
    public class TableRowSet : RowSet
    {
        public readonly STable _tb;
        public TableRowSet(SDatabase db,STable t) : base(db,t)
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
        public readonly SDict<SSelector, Serialisable> _wh;
        public readonly SCList<Variant> _key;
        public readonly bool _unique;
        public IndexRowSet(SDatabase db,STable t,SIndex ix,SDict<SSelector,Serialisable> wh) :base(db,t)
        {
            _ix = ix; _wh = wh;
            var key = SCList<Variant>.Empty;
            for (var c = _ix.cols; c != null && c.Length != 0; c = c.next)
            {
                for (var b = _wh.First(); b != null; b = b.Next())
                    if (b.Value.key.uid == c.element)
                    {
                        key = key.InsertAt(new Variant(Variants.Single,b.Value.val), key.Length);
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
                for (var b = irs._ix.rows.PositionAt(irs._key);b!=null;b=b.Next() as MTreeBookmark<long>)
                {
                    var r = irs._db.Get(b.Value.val);
                    if (r.Matches(irs._wh))
                        return new IndexRowBookmark(irs, r, b, 0);
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
                    if (r.Matches(_irs._wh))
                        return new IndexRowBookmark(_irs, r, b as MTreeBookmark<long>, Position+1);
                }
                return null;
            }
        }
    }
    public class SearchRowSet : RowSet
    {
        public readonly SSearch _sch;
        public readonly RowSet _sce;
        public SearchRowSet(SDatabase db,SSearch sc) :base (db,sc)
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
                    if (((SRecord)((RowBookmark)b)._ob).Matches(rs._sch.where))
                        return new SearchRowBookmark(rs, (RowBookmark)b, 0);
                return null;
            }
            public override Bookmark<Serialisable> Next()
            {
                for (var b = _bmk.Next(); b != null; b = b.Next())
                    if (((SRecord)((RowBookmark)b)._ob).Matches(_sch._sch.where))
                        return new SearchRowBookmark(_sch, (RowBookmark)b, Position+1);
                return null;
            }
        }
    }
    public class SysRows : RowSet
    {
        public readonly SysTable tb;
        internal SysRows(SDatabase d, SysTable t) : base(d, t)
        {
            tb = t;
        }
        public override Bookmark<Serialisable> First()
        {
            switch (tb.name)
            {
                case "_Log": return LogBookmark.New(this, 0, 0);
            }
            return null;
        }
        internal class LogBookmark : RowBookmark
        {
            public readonly long _log;
            public readonly long _next;
            internal LogBookmark(RowSet rs, long lg, Serialisable ob, long nx, int p) : base(rs, ob, p)
            {
                _log = lg; _next = nx;
            }
            internal static LogBookmark New(RowSet rs, long lg, int pos)
            {
                return (rs._db._Get(lg) is AStream.SysItem si) ?
                    new LogBookmark(rs, lg, si.item, si.next, pos) : null;
            }

            public override Bookmark<Serialisable> Next()
            {
                return New(_rs, _next, Position + 1);
            }
        }
    }
}
