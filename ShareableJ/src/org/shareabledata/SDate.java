/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.io.*;
import java.util.*;
import java.time.*;
import java.sql.Timestamp;
/**
 *
 * @author Malcolm
 */
public class SDate extends Serialisable {
        public final int year;
        public final int month;
        public final long rest;
        public SDate(Date s) 
        {
            super(Types.SDate);
            LocalDateTime t = new Timestamp(s.getTime()).toLocalDateTime();
            year = t.getYear();
            month = t.getMonthValue();
            LocalDateTime ts = LocalDateTime.of(year, month, 0,0,0);
            rest = Duration.between(t,ts).toMillis();
        }
        SDate(AStream f) throws Exception
        {
            super(Types.SDate, f);
            year = f.GetInt();
            month = f.GetInt();
            rest = f.GetLong();
        }
        public void Put(AStream f) throws Exception
        {
            super.Put(f);
            f.PutInt(year);
            f.PutInt(month);
            f.PutLong(rest);
        }
        public static Serialisable Get(AStream f) throws Exception
        {
            return new SDate(f);
        }
        public String toString()
        {
            return "Date "+LocalDateTime.of(year,month,1,0,0).plusNanos(rest*1000000).toString();
        }
}
