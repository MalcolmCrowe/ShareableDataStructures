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
public class IndexRowSet extends RowSet {
       public final SIndex _ix;
        public final SDict<SSelector, Serialisable> _wh;
        public final SCList<Variant> _key;
        public final boolean _unique;
        public IndexRowSet(SDatabase db,STable t,SIndex ix,SDict<SSelector,Serialisable> wh) throws Exception
        {
            super(db,t);
            _ix = ix; _wh = wh;
            SCList<Variant> key = null;
            for (var c = _ix.cols; c != null && c.Length != 0; c = c.next)
            {
                var found=false;
                for (var b = _wh.First(); b != null && !found; b = b.Next())
                    if (b.getValue().key.uid == c.element)
                    {
                        var v =new Variant(Variants.Single,b.getValue().val);
                        key = (key==null)? new SCList<>(v):
                                (SCList<Variant>)key.InsertAt(v, key.Length);
                        found = true;
                    }
                 if (!found)
                     break;
            }
            _key = key;
            _unique = key.Length == _ix.cols.Length;
        }
        public Bookmark<Serialisable> First()
        {
            try {
                for (var b = _ix.rows.PositionAt(_key);b!=null;b=(MTreeBookmark<Long>)b.Next())
                {
                    var r = _db.Get((long)b.getValue().val);
                    if (r.Matches(_wh))
                        return new IndexRowBookmark(this, r, b, 0);
                }
            } catch(Exception e)
            { }
                return null;
        }
        class IndexRowBookmark extends RowBookmark
        {
            public final IndexRowSet _irs;
            public final MTreeBookmark<Long> _mbm;
            protected IndexRowBookmark(IndexRowSet irs,Serialisable ob,MTreeBookmark<Long> mbm,int p)
            {
                super(irs,ob,p);
                _irs = irs; _mbm = mbm;
            }
            public Bookmark<Serialisable> Next()
            {
                try{
                if (_irs._unique)
                    return null;
                for (var b = _mbm.Next(); b != null; b = b.Next())
                {
                    var r = _irs._db.Get(b.getValue().val);
                    if (r.Matches(_irs._wh))
                        return new IndexRowBookmark(_irs, r, (MTreeBookmark<Long>)b , Position+1);
                }
                } catch(Exception e){}
                return null;
            }
        }
    }


