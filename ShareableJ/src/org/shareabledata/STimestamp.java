/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.util.Date;
import java.util.GregorianCalendar;
import java.text.SimpleDateFormat;
import java.time.Instant;
/**
 *
 * @author Malcolm
 */
public class STimestamp extends Serialisable implements Comparable {
        public final long ticks;
        public STimestamp(int y,int mo, int d,int h, int mi, int s)
        {
            super(Types.STimestamp);
            var dt = new GregorianCalendar();
            dt.set(y,mo-1,d,h,mi,s);
            ticks = dt.getTimeInMillis();
        }
        STimestamp(Reader f)
        {
            super(Types.STimestamp,f);
            ticks = f.GetLong();
        }
        @Override
        public void Put(StreamBase f) 
        {
            super.Put(f);
            f.PutLong(ticks);
        }
        public static STimestamp Get(Reader f)
        {
            return new STimestamp(f);
        }
        @Override
        public int compareTo(Object o) {
            var that = (STimestamp)o;
            return (ticks==that.ticks)?0:(ticks<that.ticks)?-1:1;
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            sb.append('"');sb.append(ticks);sb.append('"');
        }
        @Override
        public String toString()
        {
            var ins = Instant.ofEpochMilli(ticks);
            var dt = Date.from(ins);
            var f = new SimpleDateFormat("dd:HH:mm:ss");
            return "Timestamp " + f.format(dt);
        }
}
