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
        super(db, t);
        _tb = t;
    }

    @Override
    public Bookmark<Serialisable> First() {
        try {
            var b = _tb.rows.First();
            return (b != null)
                    ? new TableRowBookmark(this, b, 0) : null;
        } catch (Exception e) {
            return null;
        }
    }

    class TableRowBookmark extends RowBookmark {

        public final TableRowSet _trs;
        public Bookmark<SSlot<Long, Long>> _bmk;

        protected TableRowBookmark(TableRowSet trs, Bookmark<SSlot<Long, Long>> bm, int p)
                throws Exception {
            super(trs, trs._db.Get(bm.getValue().val), p);
            _trs = trs;
            _bmk = bm;
        }

        public Bookmark<Serialisable> Next() {
            try {
                var b = _bmk.Next();
                return (b != null)
                        ? new TableRowBookmark(_trs, b, Position + 1) : null;
            } catch (Exception e) {
                return null;
            }
        }
    }
}
