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
public class SOrder extends Serialisable {
        public final Serialisable col;
        public final boolean desc;
        public SOrder(Serialisable c,boolean d)
        { 
            super(Types.SOrder);
            col = c; desc = d;
        }
        protected SOrder(ReaderBase f) throws Exception
        {
            super(Types.SOrder);
            col = f._Get();
            desc = f.ReadByte() == 1;
        }
        @Override
        public boolean isValue() {return false;}
        public static SOrder Get(ReaderBase f) throws Exception
        {
            return new SOrder(f);
        }
        @Override
        public Serialisable UseAliases(SDatabase db, SDict<Long, Long> ta)
        {
            return new SOrder(col.UseAliases(db, ta),desc);
        }
        @Override
        public Serialisable UpdateAliases(SDict<Long, String> uids)
        {
            var c = col.UpdateAliases(uids);
            return (c == col) ? this : new SOrder(c, desc);
        }
        @Override
        public Serialisable Prepare(STransaction db, SDict<Long, Long> pt)
                throws Exception
        {
            return new SOrder(col.Prepare(db, pt),desc);
        }
        @Override
        public void Put(WriterBase f) throws Exception
        {
            super.Put(f);
            col.Put(f);
            f.WriteByte((byte)(desc ? 1 : 0));
        }    
}
