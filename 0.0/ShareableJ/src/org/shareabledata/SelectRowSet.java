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
        public SelectRowSet(RowSet sce,SSelectStatement sel,
                SDict<Long,SFunction> ags,Context cx) throws Exception
        {   super(sce._tr,sel,ags);
            _sel = sel;
            _source = sce;
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
