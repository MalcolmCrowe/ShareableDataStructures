/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.util.Date;
import java.util.Calendar;
/**
 *
 * @author Malcolm
 */
public class SDate extends Serialisable implements Comparable {
        public final int year;
        public final int month;
        public final Bigint rest;
        public SDate(Date d)
        {
            super(Types.SDate);
            var c = new Calendar.Builder().setInstant(d).build();
            year = c.get(Calendar.YEAR);
            month = c.get(Calendar.MONTH)+1;
            var day = c.get(Calendar.DAY_OF_MONTH);
            var hr = c.get(Calendar.HOUR);
            var min = c.get(Calendar.MINUTE);
            var sec = c.get(Calendar.SECOND);
            rest = new Bigint(((day*24+hr)*60+min)*60+sec)
                    .Times(new Bigint(10000000));
        }
        public SDate(int y,int m,long d)
        {
            super(Types.SDate);
            year = y; month = m; 
            rest = new Bigint((d-1)*24*60*60).Times(new Bigint(10000000));
        }
        SDate(Reader f)
        {
            super(Types.SDate, f);
            year = f.GetInt();
            month = f.GetInt();
            rest = f.GetInteger();
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.PutInt(year);
            f.PutInt(month);
            f.PutInt(rest);
        }
        public static Serialisable Get(Reader f) 
        {
            return new SDate(f);
        }
        @Override
        public int compareTo(Object o) {
            var that = (SDate)o;
            var c = (year==that.year)?0:(year<that.year)?-1:1;
            if (c!=0)
                return c;
            c = (month==that.month)?0:(month<that.month)?-1:1;
            if (c!=0)
                return c;
            return rest.compareTo(that.rest);
        }
        String str()
        {
            var thou = new Bigint(1000);
            var sixty = new Bigint(60);
            var twentyfour = new Bigint(24);
            var m = rest.Divide(new Bigint(10000));
            var s = m.Divide(thou);
            var mil = m.Remainder(thou).toInt();
            var mi = s.Divide(sixty);
            var sec = s.Remainder(sixty).toInt();
            var h = mi.Divide(sixty);
            var min = mi.Remainder(sixty).toInt();
            var day = h.Divide(twentyfour).toInt();
            var hour = h.Remainder(twentyfour).toInt();
            return String.format("%01d:%02d:%02d:%02d.%03d",
                    day,hour,min,sec,mil);
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            sb.append('"');sb.append(str());sb.append('"');
        }
        @Override
        public String toString()
        {
            return "Date "+str();
        }
}
