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
public class SBoolean extends Serialisable {
        public final int sbool;
        public SBoolean(int n)
        {
            super(Types.SBoolean);
            sbool = n;
        }
        SBoolean(AStream f)throws IOException
        {
            super(Types.SBoolean, f);
            sbool = f.GetInt();
        }
        public Serialisable Commit(STransaction tr,AStream f) throws IOException
        {
            f.PutInt(sbool);
            return this;
        }
        public static Serialisable Get(AStream f) throws IOException
        {
            return new SBoolean(f);
        }
        public String ToString()
        {
            return "Boolean "+sbool;
        }
}
