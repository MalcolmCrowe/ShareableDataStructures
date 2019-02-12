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
    public final int _klen;
    JoinRowSet(STransaction tr,SQuery top, SJoin j,
            SDict<Long,SFunction> ags, Context cx) throws Exception
    {
        super(tr,j,ags);
        _join = j;
        SList<TreeInfo<Serialisable>> lti = null;
        SList<TreeInfo<Serialisable>> rti = null;
        int n = 0;
        if (j.ons!=null)
        for (var b = j.ons.First();b!=null;b=b.Next())
        {
            var e = b.getValue();
            if (e.op != SExpression.Op.Eql)
                continue;
            var al = new TreeInfo<Serialisable>((SColumn)e.left, 'A', 'D',true);
            var ar = new TreeInfo<Serialisable>((SColumn)e.right, 'A', 'D',true);
            lti=(lti==null)?new SList<>(al):lti.InsertAt(al,n);
            rti=(rti==null)?new SList<>(ar):rti.InsertAt(ar,n);
            n++;
        }
        if (j.uses!=null)
        for (var b = j.uses.First(); b != null; b = b.Next())
        {
            var e = b.getValue();
            var al = new TreeInfo<Serialisable>(j.left.names.Lookup(e), 'A', 'D',true);
            var ar = new TreeInfo<Serialisable>(j.right.names.Lookup(e), 'A', 'D',true);
            lti=(lti==null)?new SList<>(al):lti.InsertAt(al,n);
            rti=(rti==null)?new SList<>(ar):rti.InsertAt(ar,n);
            n++;
        }
        var lf = j.left.RowSet(tr, j.left, ags, cx);
        var rg = j.right.RowSet(tr, j.right, ags, cx);
        _klen = (lti==null)?0:lti.Length;
        if (lti!=null)
        {
            lf = new OrderedRowSet(lf, lti, cx);
            rg = new OrderedRowSet(rg, rti, cx);
        }
        _left = lf;
        _right = rg;
    }
    static SRow _Row(JoinRowSet jrs,RowBookmark lbm,boolean ul,
            RowBookmark rbm,boolean ur)
    {
        var r = new SRow();
        Bookmark<SSlot<Integer, String>> ab;
        switch (jrs._join.joinType)
        {
            default:
                {
                    if (lbm != null && ul)
                    {
                        ab = lbm._ob.names.First();
                        if (ul)
                            for (var b = lbm._ob.cols.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                            {
                                var n = ab.getValue().val;
                                if (jrs._join.right.names.Contains(n))
                                    n = jrs._left._qry.getAlias() + "." + n;
                                r = r.Add(n, b.getValue().val);
                            }
                    }
                    if (rbm != null && ur)
                    {
                        ab = rbm._ob.names.First();
                        for (var b = rbm._ob.cols.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                        {
                            var n = ab.getValue().val;
                            if (jrs._join.left.names.Contains(n))
                                n = jrs._right._qry.getAlias() + "." + n;
                            r =r.Add(n, b.getValue().val);
                        }
                    }
                    break;
                }
            case SJoin.JoinType.Natural:
                {
                    if (lbm != null && ul)
                    {
                        ab = lbm._ob.names.First();
                        for (var b = lbm._ob.cols.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                            r =r.Add(ab.getValue().val, b.getValue().val);
                    }
                    if (rbm != null && ur)
                    {
                        ab = rbm._ob.names.First();
                        for (var b = rbm._ob.cols.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                            if (lbm==null || !ul || !lbm._ob.vals.Contains(ab.getValue().val))
                                r =r.Add(ab.getValue().val, b.getValue().val);
                    }
                    break;
                }
        }
        return r;
    }

    public Bookmark<Serialisable> First()
    {
        RowBookmark lf, rg;
        for (lf= (RowBookmark)_left.First(),rg = (RowBookmark)_right.First();
            lf!=null && rg!=null; )
        {
            if (_join.joinType == SJoin.JoinType.Cross)
                return new JoinRowBookmark(this, lf, true, rg, true, 0);
            var c = _join.Compare(lf, rg);
            if (c==0)
                return new JoinRowBookmark(this, lf, true, rg, true, 0);
            if (c < 0)
            {
                if ((_join.joinType&SJoin.JoinType.Left)!=0)
                    return new JoinRowBookmark(this, lf, true, rg, false, 0);
                lf = (RowBookmark)lf.Next();
            }
            else
            {
                if ((_join.joinType&SJoin.JoinType.Right)!=0)
                    return new JoinRowBookmark(this, lf, false, rg, true, 0);
                rg = (RowBookmark)rg.Next();
            }
        }
        if (lf!=null && (_join.joinType&SJoin.JoinType.Left)!=0)
            return new JoinRowBookmark(this, lf, true, null, false, 0);
        if (rg!=null && (_join.joinType&SJoin.JoinType.Right)!=0)
            return new JoinRowBookmark(this, null, false, rg, true, 0);
        return null;
    }
    public class JoinRowBookmark extends RowBookmark
    {
        public final JoinRowSet _jrs;
        public final RowBookmark _lbm, _rbm;
        public final boolean _useL,_useR;
        protected JoinRowBookmark(JoinRowSet jrs,RowBookmark lbm,boolean ul,
                RowBookmark rbm,boolean ur,int pos)
        {
            super(jrs,jrs._Row(jrs,lbm,ul,rbm,ur),pos);
            _jrs = jrs; _lbm = lbm; _rbm = rbm; _useL=ul; _useR=ur;
        }
        public Bookmark<Serialisable> Next()
        {
                var lbm = _lbm;
                var rbm = _rbm;
                var depth = 0;
                if (_jrs._join.ons!=null)
                    depth =_jrs._join.ons.Length;
                if (_jrs._join.uses!=null)
                    depth = _jrs._join.uses.Length;
                while (lbm != null && rbm != null)
                {
                    if (_jrs._join.joinType == SJoin.JoinType.Cross)
                    {
                        rbm = (RowBookmark)rbm.Next();
                        if (rbm != null)
                            return new JoinRowBookmark(_jrs, lbm, true, rbm, true, Position + 1);
                        lbm = (RowBookmark)lbm.Next();
                        rbm = (RowBookmark)_jrs._right.First();
                        if (lbm != null && rbm != null)
                            return new JoinRowBookmark(_jrs, lbm, true, rbm, true, Position + 1);
                        return null;
                    }
                    if (_useR && _useL)
                    {
                        MTreeBookmark<Serialisable> mb0 = rbm.Mb();
                        if (mb0!=null && mb0.hasMore(_jrs._tr, depth))
                        {
                            rbm = (RowBookmark)rbm.Next();
                            return new JoinRowBookmark(_jrs, lbm, true, rbm, true, Position + 1);
                        }
                        lbm = (RowBookmark)lbm.Next();
                        if (lbm == null)
                            break;
                        MTreeBookmark<Serialisable> ml = lbm.Mb();
                        MTreeBookmark<Serialisable> mr = rbm.Mb();
                        var mb = (ml!=null && ml.changed(depth)) ? null :
                            (mr!=null)?mr.ResetToTiesStart(_jrs._tr, depth):null;
                        rbm = (mb != null) ? rbm = rbm.ResetToTiesStart(mb) : 
                                (RowBookmark)rbm.Next();
                        if (rbm == null)
                            break;
                    }
                    else if (_useL)
                    {
                        lbm = (RowBookmark)lbm.Next();
                        if (lbm == null)
                            break;
                    }
                    else
                    {
                        rbm = (RowBookmark)lbm.Next();
                        if (rbm == null)
                        {
                            lbm = (RowBookmark)lbm.Next();
                            if (lbm == null)
                                break;
                        }
                    }
                    if (lbm == null || rbm == null)
                        break;
                    var c = _jrs._join.Compare(lbm, rbm);
                    if (c == 0)
                        return new JoinRowBookmark(_jrs, lbm, true, rbm, true, Position + 1);
                    if (c < 0)
                    {
                        if ((_jrs._join.joinType&SJoin.JoinType.Left)!=0)
                            return new JoinRowBookmark(_jrs, lbm, true, rbm, false, Position + 1);
                        lbm = (RowBookmark)lbm.Next();
                    }
                    else
                    {
                        if ((_jrs._join.joinType&SJoin.JoinType.Right)!=0)
                            return new JoinRowBookmark(_jrs, lbm, false, rbm, true, Position + 1);
                        rbm = (RowBookmark)rbm.Next();
                    }
                }
                if (lbm != null && (_jrs._join.joinType&SJoin.JoinType.Left)!=0)
                {
                    if (lbm==_lbm && _useL)
                        lbm = (RowBookmark)lbm.Next();
                    if (lbm != null)
                        return new JoinRowBookmark(_jrs, lbm, true, null, false, Position + 1);
                }
                if (rbm != null && (_jrs._join.joinType&SJoin.JoinType.Right)!=0)
                {
                    if (rbm==_rbm && _useR)
                        rbm = (RowBookmark)rbm.Next();
                    if (rbm != null)
                        return new JoinRowBookmark(_jrs, null, false, rbm, true, Position + 1);
                }
                return null;
        }
    }

}
