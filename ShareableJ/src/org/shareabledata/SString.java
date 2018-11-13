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
public class SString extends Serialisable {
        public final String str;
        public SString(String s)
        {
            super(Types.SString);
            str = s;
        }
        SString(AStream f) throws Exception
        {
            super(Types.SString, f);
            str = f.GetString();
        }
        public void Put(AStream f) throws Exception
        {
            super.Put(f);
            f.PutString(str);
        }
        public static Serialisable Get(AStream f) throws Exception
        {
            return new SString(f);
        }
        public String toString()
        {
            return "String '"+str+"'";
        }
}
