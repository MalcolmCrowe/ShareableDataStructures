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

    public SearchRowSet(SDatabase db, SSearch sc) throws Exception {
        super(db, sc);
        _sch = sc;
        if (_sch.sce instanceof STable) {
            var tb = (STable) _sch.sce;
            var ix = db.GetPrimaryIndex(tb.uid);
            if (ix != null) {
                _sce = new IndexRowSet(db, tb, ix, _sch.where);
            } else {
                _sce = _sch.sce.RowSet(db);
            }
        } else {
            _sce = _sch.sce.RowSet(db);
        }
    }

    @Override
    public Bookmark<Serialisable> First() {
        for (var b = _sce.First(); b != null; b = b.Next()) {
            if (((SRecord) ((RowBookmark) b)._ob).Matches(_sch.where)) {
                return new SearchRowBookmark(this, (RowBookmark) b, 0);
            }
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
        public Bookmark<Serialisable> Next() {
            for (var b = _bmk.Next(); b != null; b = b.Next()) {
                if (((SRecord) ((RowBookmark) b)._ob).Matches(_sch._sch.where)) {
                    return new SearchRowBookmark(_sch, (RowBookmark) b, Position + 1);
                }
            }
            return null;
        }
    }

}
