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
public class SInsertStatement extends Serialisable  {
        public final String tb;
        public final SList<SSelector> cols;
        public final Serialisable vals;
        public SInsertStatement(String t,SList<SSelector> c,Serialisable v)
            
        {
            super(Types.SInsert);
            tb = t; cols = c; vals = v;
        }
        public STransaction Obey(STransaction tr) throws Exception
        {
            var tb = (STable)tr.names.Lookup(this.tb); 
            var n = (cols==null)?0:cols.Length; // # named cols
            SList<Long> cs = null;
            Exception ex = null;
            var i = 1; // ha!
            if (n>0)
            for (var b=cols.First();b!=null;b=b.Next())
            {
                var ob = tb.names.Lookup(b.getValue().name);
                if (ob instanceof SColumn)
                {
                    var sc = (SColumn)ob;
                    cs = (cs==null)?new SList<Long>(sc.uid):cs.InsertAt(sc.uid, i++);
                }
                else
                    ex = new Exception("Column " + b.getValue().name + " not found");
            }
            if (vals instanceof SValues)
            {
                var svs = (SValues)vals;
                var nc = svs.vals.Length;
                if ((n == 0 && nc != tb.cpos.Length) || (n != 0 && n != nc))
                    ex = new Exception("Wrong number of columns");
                SDict<Long, Serialisable> f = null;
                var c = svs.vals;
                if (n == 0)
                    for (var b = tb.cpos.First(); c!=null && b!=null; b = b.Next(), c = c.next) // not null
                    {
                        var ob = (SSelector)b.getValue().val;
                        var v = c.element.Lookup(null);
                        f = (f==null)?new SDict(ob.uid,v):f.Add(ob.uid, v);
                    }
                else
                    for (var b = cs; c!=null && b!=null; b = b.next, c = c.next) // not null
                        f =(f==null)?new SDict(b.element, c.element)
                                :f.Add(b.element,c.element);
                tr = (STransaction)tr.Install(new SRecord(tr, tb.uid, f),tr.curpos);
            }
            if (ex != null)
                throw ex;
            return tr;
        }
        public static SInsertStatement Get(SDatabase db,Reader f) throws Exception
        {
            var t = f.GetString();
            var n = f.GetInt();
            SList<SSelector> c = null;
            for (var i = 0; i < n; i++)
                c = (c==null)?new SList((SSelector)f._Get(db)):
                        c.InsertAt((SSelector)f._Get(db), i);
            return new SInsertStatement(t, c, f._Get(db));
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.PutString(tb);
            f.PutInt((cols==null)?0:cols.Length);
            if (cols!=null)
            for (var b = cols.First(); b != null; b = b.Next())
                b.getValue().Put(f);
            vals.Put(f);
        }

}
