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
        public final boolean sign;
        public final long day;
        public final long hour;
        public final long min;
        public final long sec;
        public final long frac;
        public final Bigint ticks;
        public STimeSpan(Bigint t)
        {
            super(Types.STimeSpan);
            var ot = t;
            sign = t.compareTo(Bigint.Zero)<0;
            if (sign)
                t = t.Negate();
            var sixty = new Bigint(60);
            var twentyfour = new Bigint(24);
            var s = t.Divide(new Bigint(10000000));
            frac = t.Remainder(new Bigint(10000000)).toInt();
            var mi = s.Divide(sixty);
            sec = s.Remainder(sixty).toInt();
            var h = mi.Divide(sixty);
            min = mi.Remainder(sixty).toInt();
            day = h.Divide(twentyfour).toInt();
            hour = h.Remainder(twentyfour).toInt();
            ticks = ot;
        }
        public STimeSpan(boolean sg,int d,int h,int m,int s,int f)
        {
            super(Types.STimeSpan);
            var t = new Bigint(((d*24+h)*60+m)*60+s).Times(new Bigint(10000000))
                    .Plus(new Bigint(f));
            if (sg)
                t = t.Negate();
            ticks = t;
            day = d;
            hour = h;
            min = m;
            sec = s;
            frac = f;
            sign = sg;
        }
        STimeSpan(Reader f) 
        {
            super(Types.STimeSpan, f);
            Bigint t = f.GetInteger();
            var ot = t;
            var sg = t.compareTo(Bigint.Zero)<0;
            if (sg)
                t = t.Negate();
            var sixty = new Bigint(60);
            var twentyfour = new Bigint(24);
            var s = t.Divide(new Bigint(10000000));
            frac = s.Remainder(new Bigint(10000000)).toInt();
            var mi = s.Divide(sixty);
            sec = s.Remainder(sixty).toInt();
            var h = mi.Divide(sixty);
            min = mi.Remainder(sixty).toInt();
            day = h.Divide(twentyfour).toInt();
            hour = h.Remainder(twentyfour).toInt();
            sign = sg;
            ticks = ot;
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
        void Str(StringBuilder sb)
        {
            if (sign)
                sb.append('-');
            if (day==0)
                sb.append(String.format("%02d:%02d:%02d.%07d", 
                    hour,min,sec,frac));
            sb.append(String.format("d.%02d:%02d:%02d.%07d", 
                    day,hour,min,sec,frac));            
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            sb.append('"');
            Str(sb);
            sb.append('"');
        }
        @Override
        public String toString()
        {
            var sb = new StringBuilder();
            Str(sb);
            return sb.toString();
        }
}
