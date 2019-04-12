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
public class SValues extends Serialisable {
        public final SList<Serialisable> vals;
        public SValues(SList<Serialisable> c)
        {
            super(Types.SValues);
            vals = c;
        }
        public SValues(Reader f) throws Exception
        {
            super(Types.SValues);
            var n = f.GetInt();
            var nr = f.GetInt();
            SList<Serialisable> v = null;
            for (var i = 0; i < n; i++)
                v = (v==null)?new SList<>(f._Get()):
                        v.InsertAt(f._Get(), i);
            vals = v;
        }
        @Override
        public boolean isValue() { return true; }
        public static SValues Get(Reader f) throws Exception
        {
            var n = f.GetInt();
            var nr = f.GetInt();
            SList<Serialisable> v = null;
            for (var i = 0; i < n; i++)
                v =(v==null)?new SList(f._Get()):v.InsertAt(f._Get(), i);
            return new SValues(v);
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.PutInt(vals.Length);
            f.PutInt(1);
            for (var b = vals.First(); b != null; b = b.Next())
                b.getValue().Put(f);
        }

}
