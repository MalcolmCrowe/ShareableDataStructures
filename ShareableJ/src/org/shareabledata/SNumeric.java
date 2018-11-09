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
public class SNumeric extends Serialisable {
        public final long mantissa;
        public final int precision;
        public final int scale;
        public SNumeric(long m,int p,int s)
        {
            super(Types.SNumeric);
            mantissa = m;
            precision = p;
            scale = s;
        }
        SNumeric(AStream f) throws IOException
        {
            super(Types.SNumeric, f);
            mantissa = f.GetLong();
            precision = f.GetInt();
            scale = f.GetInt();
        }
        public void Put(AStream f) throws Exception
        {
            super.Put(f);
            f.PutLong(mantissa);
            f.PutInt(precision);
            f.PutInt(scale);
        }
        public static Serialisable Get(AStream f) throws IOException
        {
            return new SNumeric(f);
        }
        public String toString()
        {
            return "Numeric " + ((mantissa * Math.pow(10.0,-scale)));
        }
}
