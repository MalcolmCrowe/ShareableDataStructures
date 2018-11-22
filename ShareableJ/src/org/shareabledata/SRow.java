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
public class SRow extends Serialisable {
        public final SDict<String, Serialisable> cols;
        public SRow()
        {
            super(Types.SRow);
            cols = null;
        }
        public SRow Add(String n, Serialisable v)
        {
            return new SRow((cols==null)?
                    new SDict<String,Serialisable>(n,v):cols.Add(n, v));
        }
        public SRow Remove(String n)
        {
            return new SRow(cols.Remove(n));
        }
        SRow(SDict<String,Serialisable> c)
        {
            super(Types.SRow);
            cols = c;
        }
        SRow(SDatabase d,Reader f) throws Exception
        {
            super(Types.SRow);
            int n = f.GetInt();
            SDict<String, Serialisable> r = null;
            for(int i=0;i<n;i++)
            {
                String k = f.GetString();
                Serialisable v = f._Get(d);
                r = r.Add(k, v);
            }
            cols = r;
        }
        public static SRow Get(SDatabase d,Reader f) throws Exception
        {
            return new SRow(d,f);
        }
        @Override
        public void Put(StreamBase f) throws Exception
        {
            super.Put(f);
            f.PutInt(cols.Length);
            for (var b = cols.First(); b != null; b = b.Next())
            {
                f.PutString(b.getValue().key);
                var s = b.getValue().val;
                if (s!=null)
                    s.Put(f);
                else
                    Null.Put(f);
            }
        }
        public String ToString()
        {
            StringBuilder sb = new StringBuilder("SRow (");
            String cm = "";
            for (Bookmark<SSlot<String,Serialisable>> b=cols.First();b!=null;b=b.Next())
            {
                sb.append(cm); cm = ",";
                sb.append(b.getValue().key);
                sb.append(":");
                sb.append(b.getValue().val.ToString());
            }
            sb.append(")");
            return sb.toString();
        }
}
