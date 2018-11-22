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
        SBoolean(Reader f)throws Exception
        {
            super(Types.SBoolean, f);
            sbool = f.GetInt();
        }
        public Serialisable Commit(STransaction tr,AStream f) throws Exception
        {
            f.PutInt(sbool);
            return this;
        }
        public static Serialisable Get(Reader f) throws Exception
        {
            return new SBoolean(f);
        }
        @Override
        public void Put(StreamBase f) throws Exception
        {
            super.Put(f);
            f.PutInt(sbool);
        }
        @Override
        public String ToString()
        {
            return "Boolean "+sbool;
        }
}
