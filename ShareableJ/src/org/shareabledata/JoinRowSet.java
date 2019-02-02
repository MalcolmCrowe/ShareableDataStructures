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
public class JoinRowSet extends RowSet {
    public final SJoin _join;
    public final RowSet _left, _right;
    JoinRowSet(STransaction tr,SQuery top, SJoin j,
            SDict<Long,SFunction> ags, Context cx) throws Exception
    {
        super(tr,j,ags);
        _join = j;
        _left = j.left.RowSet(tr, top, ags, cx);
        _right = j.right.RowSet(tr, top, ags, cx);
    }
    SRow _Row(RowBookmark lbm,RowBookmark rbm)
    {
        throw new Error("Not implemented");
    }
    public Bookmark<Serialisable> First()
    {
        var lb = (RowBookmark)_left.First();
        var rb = (RowBookmark)_right.First();
        return (lb==null||rb==null)?null: new JoinRowBookmark(this, lb, rb, 0);
    }
    public class JoinRowBookmark extends RowBookmark
    {
        public final JoinRowSet _jrs;
        public final RowBookmark _lbm, _rbm;
        protected JoinRowBookmark(JoinRowSet jrs,RowBookmark lbm,RowBookmark rbm,int pos)
        {
            super(jrs,jrs._Row(lbm,rbm),pos);
            _jrs = jrs; _lbm = lbm; _rbm = rbm;
        }
        public Bookmark<Serialisable> Next()
        {
            throw new Error("Not yet");
        }
    }

}
