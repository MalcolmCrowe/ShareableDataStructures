/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.io.*;
/**
 *
 * @author Malcolm
 */
public class SBoolean extends Serialisable implements Comparable {
        public final boolean sbool;
        public static final SBoolean True = new SBoolean(true);
        public static final SBoolean False = new SBoolean(false);        
        private SBoolean(boolean n)
        {
            super(Types.SBoolean);
            sbool = n;
        }
        static SBoolean For(boolean r)
        {
            return r? True:False;
        }
        public Serialisable Commit(STransaction tr,Writer f)throws Exception
        {
            f.PutInt(sbool?1:0);
            return this;
        }
        public static Serialisable Get(ReaderBase f) throws Exception
        {
            return For(f.ReadByte()==1);
        }
        @Override
        public void Put(WriterBase f)throws Exception
        {
            super.Put(f);
            f.WriteByte((byte)(sbool?1:0));
        }
        @Override
        public int compareTo(Object o) {
            if (o==Null)
                return 1;
            if (o instanceof SRow)
            {
                var sr = (SRow)o;
                if (sr.cols.Length==1)
                    return compareTo(sr.vals.First().getValue().val);
            }
            var that = (SBoolean)o;
            return (sbool==that.sbool)?0:sbool?1:-1;
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            sb.append('"');sb.append(sbool);sb.append('"');
        }
        @Override
        public String toString()
        {
            return "Boolean "+sbool;
        }
}
