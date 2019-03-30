/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
/**
 *
 * @author Malcolm
 */
public class SDate extends Serialisable implements Comparable {
        public final int year;
        public final int month;
        public final Bigint rest;
        public SDate(int y,int mo,int d,int h,int mi,int s,int frac)
        {
            super(Types.SDate);
            var r = new Bigint((((d-1)*24+h)*60+mi)*60+s);
            r = r.Times(new Bigint(10000000)).Plus(new Bigint(frac));
            year = y; month = mo; 
            rest = r;            
        }
        public SDate(int y,int m,Bigint r)
        {
            super(Types.SDate);
            year = y; month = m; 
            rest = r;
        }
        SDate(Reader f) throws Exception
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
            f.PutInteger(rest);
        }
        public static Serialisable Get(Reader f) throws Exception
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
            var sixty = new Bigint(60);
            var twentyfour = new Bigint(24);
            var s = rest.Divide(new Bigint(10000000));
            var f = rest.Remainder(new Bigint(10000000)).toInt();
            var mi = s.Divide(sixty);
            var sec = s.Remainder(sixty).toInt();
            var h = mi.Divide(sixty);
            var min = mi.Remainder(sixty).toInt();
            var day = h.Divide(twentyfour).toInt()+1;
            var hour = h.Remainder(twentyfour).toInt();
            if (hour==0&&min==0&&sec==0&&f==0)
                return String.format("%d-%02d-%02d",year,month,day);
            if (f==0)
                return String.format("%d-%02d-%02dT%02d:%02d:%02d",
                    year,month,day,hour,min,sec);
            return String.format("%d-%02d-%02dT%02d:%02d:%02d.%07d",
                    year,month,day,hour,min,sec,f);
        }
        @Override
        public void Append(StringBuilder sb)
        {
            
            sb.append('"');sb.append(str());sb.append('"');
        }
        @Override
        public String toString()
        {
            return "Date "+str();
        }
}
