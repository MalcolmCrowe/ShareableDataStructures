/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author Malcolm
 */
public class SearchRowSet extends RowSet {

    public final SSearch _sch;
    public final RowSet _sce;
    public SearchRowSet(STransaction tr, SQuery top, SSearch sc,
            SDict<Long,Serialisable> ags) throws Exception 
    {
        this(Source(tr,top,sc,ags),sc,ags);
    }
    SearchRowSet(RowSet sce,SSearch sc,SDict<Long,Serialisable>ags)
    {
        super(sce._tr,sc,ags);
        _sch = sc;
        _sce = sce;
    }
    static RowSet Source(STransaction tr,SQuery top,SSearch sc,SDict<Long,Serialisable> ags)
            throws Exception
    {
            RowSet s = null;
            SDict<Long,Serialisable> matches = null;
            if (sc.sce instanceof STable)
            {
                var tb = (STable)sc.sce;
                for (var wb = sc.where.First(); wb != null; wb = wb.Next())
                    if (wb.getValue() instanceof SExpression)
                    { 
                        var x = (SExpression)wb.getValue(); 
                        if(x.op == SExpression.Op.Eql)
                        {
                            if (x.left instanceof SColumn)
                            {
                                var c = (SColumn)x.left;
                                if (tb.refs.Contains(c.uid) &&
                                    x.right != null && x.right.isValue())
                                matches = (matches==null)?new SDict(c.uid,x.right):
                                matches.Add(c.uid, x.right);
                            }
                            else if (x.right instanceof SColumn)
                            {
                                var c = (SColumn)x.right;
                                if (tb.refs.Contains(c.uid) &&
                                    x.left != null && x.left.isValue())
                                 matches = (matches==null)?new SDict(c.uid,x.left):
                                matches.Add(c.uid, x.left);
                            }
                        }
                    }
                SCList<Variant> best = null;
                if (matches != null && tb.indexes!=null)
                    for (var b = tb.indexes.First();(best==null || 
                            matches.Length> best.Length) && b != null;
                        b = b.Next())
                    {
                        SCList<Variant> ma = null;
                        var n = 0;
                        var ix = (SIndex)tr.objects.get(b.getValue().key);
                        for (var wb = ix.cols.First(); wb != null; wb = wb.Next())
                        {
                            if (!matches.Contains(wb.getValue()))
                                break;
                            var v = new Variant(Variants.Ascending, matches.get(wb.getValue()));
                            ma = (ma==null)?new SCList(v):ma.InsertAt(v,n);
                            n++;
                        }
                        if (ma!= null && (best==null || ma.Length > best.Length))
                        {
                            best = ma;
                            s = new IndexRowSet(tr, tb, ix, ma, sc.where);
                            tr = s._tr;
                        }
                    }
            }
            if (s!=null)
                return s;
            if (sc.sce!=null)
                return sc.sce.RowSet(tr,top,ags);
            throw new Exception("PE03");
    }
    @Override
    public Bookmark<Serialisable> First() {
        for (var b = (RowBookmark)_sce.First(); b != null; b = (RowBookmark)b.Next()) {
            var rb = new SearchRowBookmark(this,(RowBookmark)b,0);
            if (rb.Matches(_sch.where)==true) 
                return rb;
        }
        return null;
    }

    class SearchRowBookmark extends RowBookmark {

        public final SearchRowSet _sch;
        public final RowBookmark _bmk;

        protected SearchRowBookmark(SearchRowSet sr, RowBookmark bm, int p)
        {
            super(sr, bm._cx, p);
            _sch = sr;
            _bmk = bm;
        }
        @Override
        public Bookmark<Serialisable> Next() {
            for (var b = (RowBookmark)_bmk.Next(); b != null; b = (RowBookmark)b.Next()) {
                var rb = new SearchRowBookmark(_sch,(RowBookmark)b,Position+1);
                if (rb.Matches(_sch._sch.where))
                    return rb;
            }
            return null;
        }
        @Override
        public STransaction Update(STransaction tr, 
                SDict<Long, Serialisable> assigs) throws Exception
        {
            return _bmk.Update(tr, assigs);
        }
        @Override
        public STransaction Delete(STransaction tr) throws Exception
        {
            return _bmk.Delete(tr);
        }
    }

}
