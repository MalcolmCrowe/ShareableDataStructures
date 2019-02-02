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
public class SelectRowSet extends RowSet {
        public final SSelectStatement _sel;
        public final RowSet _source;
        public SelectRowSet(STransaction tr,SSelectStatement sel,
                SDict<Long,SFunction> ags,Context cx) throws Exception
        {   super(tr,sel,ags);
            _sel = sel;
            if (sel.cpos!=null)
            for (var b = sel.cpos.First(); b != null; b = b.Next())
                ags = b.getValue().val.Aggregates(ags, cx);
            _source = sel.qry.RowSet(tr,sel,ags,cx);
        }

        public Bookmark<Serialisable> First()
        {
            try {
                for (var b = (RowBookmark)_source.First();b!=null;
                        b=(RowBookmark)b.Next())
                {
                    var rw = (SRow)_qry.Lookup(new Context(b,null));
                    if (rw.isNull)
                        continue;
                    var rb = new SelectRowBookmark(this,b, rw, 0);
                    if (rb._ob.cols!=null)
                        return rb;
                    if (_sel.aggregates) // aggregates collapse the rowset
                        break;
                }
            } catch (Exception e)
            {
                System.out.println("Evaluation failure");
            }
            return null;  
        }
        class SelectRowBookmark extends RowBookmark
        {
            public final SelectRowSet _srs;
            public final RowBookmark _bmk;
            SelectRowBookmark(SelectRowSet rs,RowBookmark bmk,SRow rw,int p)  
            {
                super(rs,rw,p);
                _srs = rs; _bmk = bmk;
            }
            @Override
            public Bookmark<Serialisable> Next()
            {
                try {
                    for (var b = (RowBookmark)_bmk.Next(); b != null; b = (RowBookmark)b.Next())
                    {
                        var rw = (SRow)_srs._qry.Lookup(new Context(b,null));
                        if (rw.isNull)
                            continue;
                        var rb = new SelectRowBookmark(_srs, b, rw, Position + 1);
                        if (rb._ob.cols!=null)
                            return rb;
                    }
                } catch(Exception e)
                {
                    System.out.println("Evaluation failure");
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
