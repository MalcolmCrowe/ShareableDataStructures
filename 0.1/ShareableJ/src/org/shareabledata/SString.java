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
        SString(Reader f) 
        {
            super(Types.SString, f);
            str = f.GetString();
        }
        @Override
        public void Put(StreamBase f) 
        {
            super.Put(f);
            f.PutString(str);
        }
        public static Serialisable Get(Reader f)
        {
            return new SString(f);
        }
        @Override
        public int compareTo(Object o) {
            var that = (SString)o;
            return str.compareTo(that.str);
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            sb.append('"');sb.append(str);sb.append('"');
        }
        @Override
        public String toString()
        {
            return "String '"+str+"'";
        }
}
