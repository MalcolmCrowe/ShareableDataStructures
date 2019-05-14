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
public class SString extends Serialisable implements Comparable {
        public final String str;
        public SString(String s)
        {
            super(Types.SString);
            str = s;
        }
        SString(ReaderBase f) throws Exception
        {
            super(Types.SString, f);
            str = f.GetString();
        }
        @Override
        public void Put(WriterBase f) throws Exception
        {
            super.Put(f);
            f.PutString(str);
        }
        public static Serialisable Get(ReaderBase f) throws Exception
        {
            return new SString(f);
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
            var that = (SString)o;
            return str.compareTo(that.str);
        }
        @Override
        public void Append(StringBuilder sb)
        {
            sb.append('"');sb.append(str);sb.append('"');
        }
        @Override
        public String toString()
        {
            return "String '"+str+"'";
        }
}
