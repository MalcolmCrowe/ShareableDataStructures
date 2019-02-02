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
public class TableRowSet extends RowSet {

    public final STable _tb;

    public TableRowSet(SDatabase db, STable t) {
        super(db, t,null);
        _tb = t;
    }

    @Override
    public Bookmark<Serialisable> First() {
            var b = _tb.rows.First();
            return (b != null)
                    ? new TableRowBookmark(this, b, 0) : null;
    }

    class TableRowBookmark extends RowBookmark {

        public final TableRowSet _trs;
        public Bookmark<SSlot<Long, Long>> _bmk;

        protected TableRowBookmark(TableRowSet trs, Bookmark<SSlot<Long, Long>> bm, int p)
        {
            super(trs, new SRow(trs._tr,trs._tr.Get(bm.getValue().val)), p);
            _trs = trs;
            _bmk = bm;
        }

        @Override
        public Bookmark<Serialisable> Next() {
            try {
                var b = _bmk.Next();
                return (b != null)
                        ? new TableRowBookmark(_trs, b, Position + 1) : null;
            } catch (Exception e) {
                return null;
            }
        }
        @Override
        public STransaction Update(STransaction tr, SDict<String, Serialisable> assigs)
                throws Exception
        {
            return (STransaction)tr.Install(new SUpdate(tr, _ob.rec, assigs),tr.curpos); // ok
        }
        @Override
        public STransaction Delete(STransaction tr) throws Exception
        {
            return (STransaction)tr.Install(new SDelete(tr, _ob.rec.table, 
                    _ob.rec.Defpos()),tr.curpos); // ok
        }
    }
}
