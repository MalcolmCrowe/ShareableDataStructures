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
            var si = s.toInt();
            var mi = si/60;
            sec = si%60;
            var h = mi/60;
            min = mi%60;
            day = h/24;
            hour = h%24;
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
        STimeSpan(Reader f) throws Exception
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
            frac = t.Remainder(new Bigint(10000000)).toInt();
            var si = s.toInt();
            var mi = si/60;
            sec = si%60;
            var h = mi/60;
            min = mi%60;
            day = h/24;
            hour = h%24;
            sign = sg;
            ticks = ot;
        }
        @Override
        public void Put(StreamBase f) 
        {
            super.Put(f);
            f.PutInteger(ticks);
        }
        public static Serialisable Get(Reader f) throws Exception
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
            {
                if (frac==0)
                   sb.append(String.format("%02d:%02d:%02d", 
                        hour,min,sec));
                else
                sb.append(String.format("%02d:%02d:%02d.%07d", 
                    hour,min,sec,frac));
            } 
            else if (frac==0)
                 sb.append(String.format("d.%02d:%02d:%02d", 
                    day,hour,min,sec));
            else
                sb.append(String.format("d.%02d:%02d:%02d.%07d", 
                    day,hour,min,sec,frac));            
        }
        @Override
        public void Append(StringBuilder sb)
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
