/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.text.SimpleDateFormat;
/**
 *
 * @author Malcolm
 */
public class STimeSpan extends Serialisable implements Comparable {
        public final long day;
        public final long hour;
        public final long min;
        public final long sec;
        public final long milli;
        public final Bigint ticks;
        public STimeSpan(Bigint t)
        {
            super(Types.STimeSpan);
            var thou = new Bigint(1000);
            var sixty = new Bigint(60);
            var twentyfour = new Bigint(24);
            var m = t.Divide(new Bigint(10000));
            var s = m.Divide(thou);
            milli = m.Remainder(thou).toInt();
            var mi = s.Divide(sixty);
            sec = s.Remainder(sixty).toInt();
            var h = mi.Divide(sixty);
            min = mi.Remainder(sixty).toInt();
            day = h.Divide(twentyfour).toInt();
            hour = h.Remainder(twentyfour).toInt();
            ticks = t;
        }
        public STimeSpan(int h,int m,int s)
        {
            super(Types.STimeSpan);
            day = 0;
            hour = h;
            min = m;
            sec = s;
            milli = 0;
            ticks = new Bigint((h*60+m)*60+s).Times(new Bigint(10000000));
        }
        STimeSpan(Reader f) 
        {
            super(Types.STimeSpan, f);
            Bigint t = f.GetInteger();
            var thou = new Bigint(1000);
            var sixty = new Bigint(60);
            var twentyfour = new Bigint(24);
            var m = t.Divide(new Bigint(10000));
            var s = m.Divide(thou);
            milli = m.Remainder(thou).toInt();
            var mi = s.Divide(sixty);
            sec = s.Remainder(sixty).toInt();
            var h = mi.Divide(sixty);
            min = mi.Remainder(sixty).toInt();
            day = h.Divide(twentyfour).toInt();
            hour = h.Remainder(twentyfour).toInt();
            ticks = t;
        }
        @Override
        public void Put(StreamBase f) 
        {
            super.Put(f);
            f.PutInt(ticks);
        }
        public static Serialisable Get(Reader f)
        {
            return new STimeSpan(f);
        }
        @Override
        public int compareTo(Object o) {
            var that = (STimeSpan)o;
            return ticks.compareTo(that.ticks);
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            sb.append('"');
            sb.append(String.format("%02d:%02d:%02d:%02d.%03d", 
                    day,hour,min,sec,milli));
            sb.append('"');
        }
        @Override
        public String toString()
        {
            return "Date "+String.format("%02d:%02d:%02d:%02d.%03d", 
                    day,hour,min,sec,milli);
        }
}
