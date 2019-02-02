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
            SDict<Long,SFunction> ags, Context cx) throws Exception 
    {
        super(tr,sc,ags);
        _sch = sc;
        RowSet s = null;
        SDict<Long,Serialisable> matches = null;
        if (_sch.sce instanceof STable)
        {
            var tb = (STable)_sch.sce;
            for (var wb = _sch.where.First(); wb != null; wb = wb.Next())
                try {
                    var v = wb.getValue().Lookup(cx);
                    if (v instanceof SExpression)
                    {
                        var x = (SExpression)v;
                        if (x.op == SExpression.Op.Eql)
                        {
                            matches=Build(matches,tb,x.left,x.right);
                            matches = Build(matches,tb,x.right,x.left);
                        }
                    }
                } catch(Exception e)
                {
                    System.out.println("Evaluation error: "+e.getMessage());
                }
            SCList<Variant> best = null;
            if (matches!=null && tb.indexes!=null)
            for (var b = tb.indexes.First(); 
                    (best==null || matches.Length > best.Length) && b != null; 
                    b = b.Next())
            {
                SCList<Variant> ma = null;
                var ix = (SIndex)tr.objects.Lookup(b.getValue().key);
                for (var wb = ix.cols.First(); wb != null; wb = wb.Next())
                {
                    if (!matches.Contains(wb.getValue()))
                        break;
                    var va = new Variant(Variants.Ascending,
                            matches.Lookup(wb.getValue()));
                    ma = (ma==null)?new SCList(va):ma.InsertAt(va,ma.Length);
                }
                if (ma!=null && (best==null || ma.Length > best.Length))
                {
                    best = ma;
                    s = new IndexRowSet(tr, tb, ix, ma, sc.where);
                }
            }
        }
        if (s==null && _sch.sce!=null)
            s = _sch.sce.RowSet(tr,top,ags,cx);
        if (s==null)
            throw new Exception("??");
        _sce = s;
    }
    private SDict<Long,Serialisable> Build(SDict<Long,Serialisable> m,
            STable tb,Serialisable a,Serialisable v)
    {
        if (!(a instanceof SColumn))
            return m;
        var c = (SColumn)a;
        if (!tb.names.Contains(c.name))
            return m;
        return (m==null)?new SDict(c.uid,v):m.Add(c.uid,v);
    }

    @Override
    public Bookmark<Serialisable> First() {
        for (var b = (RowBookmark)_sce.First(); b != null; b = (RowBookmark)b.Next()) {
            var rb = new SearchRowBookmark(this,(RowBookmark)b,0);
            if (b._ob.rec!=null && b._ob.rec.Matches(rb,_sch.where)) 
                return rb;
        }
        return null;
    }

    class SearchRowBookmark extends RowBookmark {

        public final SearchRowSet _sch;
        public final RowBookmark _bmk;

        protected SearchRowBookmark(SearchRowSet sr, RowBookmark bm, int p) {
            super(sr, bm._ob, p);
            _sch = sr;
            _bmk = bm;
        }
        @Override
        public Bookmark<Serialisable> Next() {
            for (var b = (RowBookmark)_bmk.Next(); b != null; b = (RowBookmark)b.Next()) {
                var rb = new SearchRowBookmark(_sch,(RowBookmark)b,Position+1);
                if (b._ob.rec!=null && b._ob.rec.Matches(rb,_sch._sch.where))
                    return rb;
            }
            return null;
        }
        @Override
        public STransaction Update(STransaction tr, 
                SDict<String, Serialisable> assigs) throws Exception
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
