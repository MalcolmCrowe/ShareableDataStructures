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
    public SearchRowSet(SDatabase tr, SQuery top, SSearch sc,
            Context cx) throws Exception 
    {
        this(Source(tr,top,sc,cx),sc,cx);
    }
    SearchRowSet(RowSet sce,SSearch sc,Context cx)
    {
        super(sce._tr,sc,cx);
        _sch = sc;
        _sce = sce;
    }
    static RowSet Source(SDatabase tr,SQuery top,SSearch sc,Context cx)
            throws Exception
    {
            RowSet s = null;
            SDict<Long,SSlot<Serialisable,Integer>> matches = null;
            if (sc.sce instanceof STable)
            {
                var tb = (STable)sc.sce;
                for (var wb = sc.where.First(); wb != null; wb = wb.Next())
                    if (wb.getValue() instanceof SExpression)
                    { 
                        var x = (SExpression)wb.getValue(); 
                        if (x.left instanceof SColumn)
                        {
                            var c = (SColumn)x.left;
                            if (tb.refs.Contains(c.uid) &&
                                x.right != null && x.right.isValue())
                            {
                                var sl = new SSlot(x.right,x.op);
                                matches = (matches==null)?new SDict(c.uid,sl):
                                    matches.Add(c.uid, sl);
                            }
                        }
                        else if (x.right instanceof SColumn)
                        {
                            var c = (SColumn)x.right;
                            if (tb.refs.Contains(c.uid) &&
                                x.left != null && x.left.isValue())
                            {
                                var sl = new SSlot(x.left,Reverse(x.op));
                                matches = (matches==null)?new SDict(c.uid,sl):
                                    matches.Add(c.uid, sl);
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
                        int op = SExpression.Op.Eql;
                        var n = 0;
                        var ix = (SIndex)tr.objects.get(b.getValue().key);
                        for (var wb = ix.cols.First(); wb != null; wb = wb.Next())
                        {
                            if (!matches.Contains(wb.getValue()))
                                break;
                            op = Compat(op, (int)((SSlot)matches.get(wb.getValue())).val);
                            if (op == SExpression.Op.NotEql)
                                break;
                            var v = new Variant(Variants.Ascending, matches.get(wb.getValue()).key);
                            ma = (ma==null)?new SCList(v):ma.InsertAt(v,n);
                            n++;
                        }
                        if (ma!= null && (best==null || ma.Length > best.Length))
                        {
                            best = ma;
                            s = new IndexRowSet(tr, tb, ix, ma, op, sc.where, cx);
                            tr = s._tr;
                        }
                    }
            }
            if (s!=null)
                return s;
            if (sc.sce!=null)
                return sc.sce.RowSet(tr,top,cx);
            throw new Exception("PE03");
    }
    static int Reverse(int op)
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
    static int Compat(int was, int now)
    {
        if (was == now || was == SExpression.Op.Eql)
            return now;
        return SExpression.Op.NotEql;
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
