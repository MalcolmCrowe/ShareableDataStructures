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
public class DistinctRowSet extends RowSet {
        public final RowSet _sce;
        public final SDict<SRow, Boolean> rows;
        public DistinctRowSet(RowSet sce) throws Exception
        {
            super(sce._tr, sce._qry, sce._cx);
            _sce = sce;
            SDict<SRow, Boolean> r = null;
            for (var b = (RowBookmark)sce.First(); b != null; b = (RowBookmark)b.Next())
                r=(r==null)?new SDict(b.Ob(), true):r.Add(b.Ob(),true);
            rows = r;
        }
        public Bookmark<Serialisable> First()
        {
            var rb = rows.First();
            return (rb==null) ?null:
                    new DistinctRowBookmark(this, rb, 0);
        }
        class DistinctRowBookmark extends RowBookmark
        {
            public final DistinctRowSet _drs;
            public final Bookmark<SSlot<SRow,Boolean>> _bmk;
            DistinctRowBookmark(DistinctRowSet drs,
                    Bookmark<SSlot<SRow,Boolean>> bmk,int pos) 
            { 
                super (drs,_Cx(drs,bmk.getValue().key,null),pos);
                _drs = drs; _bmk = bmk; 
            }
            public Bookmark<Serialisable> Next()
            {
                var rb = _bmk.Next();
                return (rb!=null)?
                    new DistinctRowBookmark(_drs,rb,Position+1):null;
            }
        }
    
}
