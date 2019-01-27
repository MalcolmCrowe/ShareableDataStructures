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
        public final SList<Serialisable> _wh;
        public final SCList<Variant> _key;
        public final boolean _unique;
        public IndexRowSet(SDatabase db,STable t,SIndex ix,SCList<Variant> key,
                SList<Serialisable> wh)
        {
            super(db,t);
            _ix = ix; _wh = wh;
            _key = key;
            _unique = key!=null && key.Length == _ix.cols.Length;
        }
        public Bookmark<Serialisable> First()
        {
            try {
                var b = (MTreeBookmark<Long>)((_key==null)?_ix.rows.First()
                        :_ix.rows.PositionAt(_key));
                for (;b!=null;b=(MTreeBookmark<Long>)b.Next())
                {
                    var r = _tr.Get(b.getValue().val);
                    var rb = new IndexRowBookmark(this, new SRow(_tr,r), b, 0);
                    if (r.Matches(rb,_wh))
                        return rb;
                }
            } catch(Exception e)
            { 
                throw new Error("MTree");
            }
            return null;
        }
        class IndexRowBookmark extends RowBookmark
        {
            public final IndexRowSet _irs;
            public final MTreeBookmark<Long> _mbm;
            protected IndexRowBookmark(IndexRowSet irs,SRow ob,MTreeBookmark<Long> mbm,int p)
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
                    var r = _irs._tr.Get(b.getValue().val);
                    var rb = new IndexRowBookmark(_irs,
                                new SRow(_irs._tr, r), (MTreeBookmark<Long>)b , Position+1);
                    if (r.Matches(rb,_irs._wh))
                        return rb;
                }
                } catch(Exception e){}
                return null;
            }
            @Override
            public STransaction Update(STransaction tr, 
                    SDict<String, Serialisable> assigs) throws Exception
            {
                return (STransaction)tr.Install(new SUpdate(tr, _ob.rec, assigs),
                    tr.curpos); // ok
            }
            public STransaction Delete(STransaction tr) throws Exception
            {
                var rc = _ob.rec;
                return (STransaction)tr.Install(new SDelete(tr, rc.table, 
                        rc.Defpos()), tr.curpos); // ok
            }
        }
    }


