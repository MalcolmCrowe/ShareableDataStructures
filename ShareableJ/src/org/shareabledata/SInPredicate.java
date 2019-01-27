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
public class SInPredicate extends Serialisable {
        public final Serialisable arg;
        public final Serialisable list;
        public SInPredicate(Serialisable a,Serialisable r)
        {
            super(Types.SInPredicate);
            arg = a; list = r;
        }
        @Override
        public boolean isValue() { return false;}
        public static SInPredicate Get(SDatabase db,Reader f) throws Exception
        {
            var a = f._Get(db);
            return new SInPredicate(a, f._Get(db));
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            arg.Put(f);
            list.Put(f);
        }
        @Override
        public Serialisable Lookup(ILookup<String, Serialisable> nms)
        {
            return new SInPredicate(arg.Lookup(nms), list.Lookup(nms));
        }
}
