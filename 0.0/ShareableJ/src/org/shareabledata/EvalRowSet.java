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
public class EvalRowSet extends RowSet {
        public final SDict<Long, Serialisable> _vals;
        public EvalRowSet(RowSet r,SQuery q,SDict<Long,SFunction>ags,Context cx)
        {
            super(r._tr,q,ags);
            SDict<Long, Serialisable> vs = null;
            for (var b = (RowBookmark)r.First();b!=null;
                b=(RowBookmark)b.Next())
                for (var ab = ags.First();ab!=null;ab=ab.Next())
                {
                    var f = ab.getValue().val;
                    var v = f.arg.Lookup(new Context(b,cx));
                    if (v!=Serialisable.Null)
                    {
                        var w = (vs!=null && vs.Contains(f.fid)) ? 
                                f.AddIn(vs.get(f.fid), v)
                            : f.StartCounter(v);
                        vs = (vs==null)?new SDict(f.fid, w):vs.Add(f.fid,w);
                    }
                }
            _vals = vs;
        }
        public Bookmark<Serialisable> First()
        {
            var r = new SRow();
            var ab = _qry.display.First();
            for (var b = _qry.cpos.First(); ab != null && b != null; ab = ab.Next(), b = b.Next())
                r=r.Add(ab.getValue().val, b.getValue().val.Lookup(new Context(null,_vals)));
            return new EvalRowBookmark(this,r, _vals);
        }
        public class EvalRowBookmark extends RowBookmark
        {
            EvalRowBookmark(EvalRowSet ers, SRow r,SDict<Long,Serialisable> a) 
            {
                super(ers, r, a, 0); 
            }
            public Bookmark<Serialisable> Next()
            {
                return null;
            }
        }
    }
