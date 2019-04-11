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

    public TableRowSet(STransaction db, STable t, Context cx) {
        super(db.Add(t.uid), t,cx);
        _tb = t;
    }

    @Override
    public Bookmark<Serialisable> First() {
            var b = _tb.rows.First();
            try {
            return (b != null)
                    ? new TableRowBookmark(this, b, 0) : null;
            } catch(Exception e)
            {
                return null;
            }
    }

    class TableRowBookmark extends RowBookmark {

        public final TableRowSet _trs;
        public Bookmark<SSlot<Long, Long>> _bmk;

        protected TableRowBookmark(TableRowSet trs, Bookmark<SSlot<Long, Long>> bm, int p)
                throws Exception
        {
            super(trs,_Cx(trs,new SRow(trs._tr,
                    trs._tr.Get(bm.getValue().val)),null), p);
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
        public STransaction Update(STransaction tr, SDict<Long, Serialisable> assigs)
                throws Exception
        {
            return (STransaction)tr.Install(new SUpdate(tr, Ob().rec, assigs),tr.curpos); // ok
        }
        @Override
        public STransaction Delete(STransaction tr) throws Exception
        {
            var rc = Ob().rec;
            return (STransaction)tr.Install(new SDelete(tr, Ob().rec.table, 
                    rc.Defpos()),rc,tr.curpos); // ok
        }
    }
}
