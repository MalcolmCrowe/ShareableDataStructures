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
public class SInteger extends Serialisable implements Comparable {
        public final int value;
        public final Bigint big;
        public static final SInteger Zero = new SInteger(0);
        public static final SInteger One = new SInteger(1);
        public SInteger(int v)
        {
            super(Types.SInteger);
            value = v;
            big = null;
        }
        public SInteger(Bigint b)
        {
            super((b.compareTo(Bigint.intMax)<0 && b.compareTo(Bigint.intMin)>0)?
                    Types.SInteger:Types.SBigInt);
            if (b.compareTo(Bigint.intMax)<0 && b.compareTo(Bigint.intMin)>0)
            {
                value = b.toInt(); big = null;
            } else
            {
                value = 0; big = b;
            }
        }
        SInteger(Reader f)
        {
            this(f.GetInt());
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.PutInt(value);
        }
        public static Serialisable Get(Reader f)
        {
            return new SInteger(f);
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            if (big!=null)
                sb.append(big);
            else
                sb.append(value);
        }
        @Override
        public int compareTo(Object o) {
            SInteger that = (SInteger)o;
            if (big==null)
            {
                if (that.big==null)
                    return (value==that.value)?0:(value<that.value)?-1:1;
                return new Bigint(value).compareTo(that.big);
            }
            if (that.big==null)
                return big.compareTo(new Bigint(that.value));
            return big.compareTo(that.big);
        }
        @Override
        public String toString()
        {
            return "Integer " + value;
        }
}
