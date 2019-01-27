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
public class OrderedRowSet extends RowSet {
        public final RowSet _sce;
        public final SMTree<Serialisable> _tree;
        public final SDict<Integer, SRow> _rows;
        public OrderedRowSet(RowSet sce,SSelectStatement sel) throws Exception
        {
            super(sce._tr,sel);
            _sce = sce;
            SList<TreeInfo> ti = null;
            int n = 0;
            for (var b = sel.order.First(); b != null; b = b.Next(),n++) 
            {
                var inf = new TreeInfo(b.getValue(), 'A', 'D',
                        !b.getValue().desc);
                ti = (ti==null)?new SList(inf):ti.InsertAt(inf, n);
            }
            var t = new SMTree<Serialisable>(ti);
            SDict<Integer, SRow> r = null;
            int m = 0;
            for (var b = (RowBookmark)sce.First(); b != null; b = (RowBookmark)b.Next(),m++)
            {
                var k = new Variant[n];
                var i = 0;
                for (var c = sel.order.First(); c != null; c = c.Next())
                    k[i] = new Variant(c.getValue().col.Lookup(b),!c.getValue().desc);
                t = t.Add(m,k);
                r=(r==null)?new SDict(0,b._ob):r.Add(m, b._ob);
            }
            _tree = t;
            _rows = r;
        }
        @Override
        public Bookmark<Serialisable> First()
        {
            var rb = (MTreeBookmark<Serialisable>)_tree.First();
            return (rb!=null) ? 
                    new OrderedBookmark(this, rb, 0) : null;
        }
        class OrderedBookmark extends RowBookmark
        {
            public final OrderedRowSet _ors;
            public final MTreeBookmark<Serialisable> _bmk;
            OrderedBookmark(OrderedRowSet ors,MTreeBookmark<Serialisable> bmk,int pos)
            {
                super(ors,ors._rows.Lookup((int)(long)bmk.getValue().val),pos);
                _ors = ors; _bmk = bmk;
            }
            public Bookmark<Serialisable> Next()
            {
                var rb = (MTreeBookmark<Serialisable>)_bmk.Next();
                return (rb!=null) ?
                    new OrderedBookmark(_ors, rb, Position+1) : null;
            }
        }

}
