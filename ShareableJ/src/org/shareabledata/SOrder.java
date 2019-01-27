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
        protected SOrder(SDatabase db,Reader f) throws Exception
        {
            super(Types.SOrder);
            col = f._Get(db);
            desc = f.ReadByte() == 1;
        }
        @Override
        public boolean isValue() {return false;}
        public static SOrder Get(SDatabase db,Reader f) throws Exception
        {
            return new SOrder(db, f);
        }
        @Override
        public void Put(StreamBase f) 
        {
            super.Put(f);
            col.Put(f);
            f.WriteByte((byte)(desc ? 1 : 0));
        }    
}
