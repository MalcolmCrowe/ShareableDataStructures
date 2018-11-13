/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.io.*;
import java.sql.Timestamp;
import java.time.Instant;
/**
 *
 * @author Malcolm
 */
public class STimestamp extends Serialisable {
        public final long ticks;
        public STimestamp(Timestamp t)
        {
            super(Types.STimestamp);
            ticks = t.getTime();
        }
        STimestamp(AStream f)throws Exception
        {
            super(Types.STimestamp,f);
            ticks = f.GetLong();
        }
        public void Put(AStream f) throws Exception
        {
            super.Put(f);
            f.PutLong(ticks);
        }
        public static STimestamp Get(AStream f) throws Exception
        {
            return new STimestamp(f);
        }
        public String ToString()
        {
            return "Timestamp " + new Timestamp(ticks).toString();
        }
}
