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
        public final int _op;
        public final boolean _unique;
        public IndexRowSet(STransaction db,STable t,SIndex ix,SCList<Variant> key,
                int op,SList<Serialisable> wh,Context cx)
        {
            super(Rdc(db,ix,key),t,null);
            _ix = ix; _wh = wh;
            _key = key; _op = op;
            _unique = key!=null && key.Length == _ix.cols.Length;
        }
        static STransaction Rdc(STransaction tr,SIndex ix,SCList<Variant> _key)
        {
            if (_key==null)
                return tr.Add(ix.table);
            var mb = ix.rows.PositionAt(_key);
            if (mb == null)
                return tr;
            if (mb.hasMore(tr, ix.cols.Length))
                return tr.Add(ix.table);
            return tr.Add((long)mb.getValue().val);            
        }
       @Override
        public Bookmark<Serialisable> First()
        {
            try {
                var b = (MTreeBookmark<Long>)((_key==null)?_ix.rows.First()
                        :_ix.rows.PositionAt(_key));
                for (;b!=null;b=NextOrPrev(_op,b))
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
        static MTreeBookmark NextOrPrev(int op,MTreeBookmark<Long> b)
        {
            return (MTreeBookmark)((op == SExpression.Op.Lss || op == SExpression.Op.Leq) ?
                b.Previous() : b.Next());
        }
        class IndexRowBookmark extends RowBookmark
        {
            public final IndexRowSet _irs;
            public final MTreeBookmark<Long> _mbm;
            protected IndexRowBookmark(IndexRowSet irs,SRow ob,MTreeBookmark<Long> mbm,int p)
            {
                super(irs,_Cx(irs,ob,null),p);
                _irs = irs; _mbm = mbm;
            }
            public Bookmark<Serialisable> Next()
            {
                try{
                if (_irs._unique)
                    return null;
                for (var b = NextOrPrev(_irs._op,_mbm); b != null; 
                        b = NextOrPrev(_irs._op,b))
                {
                    var r = _irs._tr.Get((long)b.getValue().val);
                    var rb = new IndexRowBookmark(_irs,
                                new SRow(_irs._tr, r), b , Position+1);
                    if (r.Matches(rb,_irs._wh))
                        return rb;
                }
                } catch(Exception e){}
                return null;
            }
            @Override
            public STransaction Update(STransaction tr, 
                    SDict<Long, Serialisable> assigs) throws Exception
            {
                return (STransaction)tr.Install(new SUpdate(tr, Ob().rec, assigs),
                    tr.curpos); // ok
            }
            public STransaction Delete(STransaction tr) throws Exception
            {
                var rc = Ob().rec;
                return (STransaction)tr.Install(new SDelete(tr, rc.table, 
                        rc.Defpos()), tr.curpos); // ok
            }
        }
    }


