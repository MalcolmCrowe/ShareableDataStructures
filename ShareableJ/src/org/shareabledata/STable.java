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
public class STable extends SDbObject {
        public final String name;
        public final SList<SColumn> cpos;
        public final SDict<String, SColumn> names;
        public final SDict<Long,SColumn> cols;
        public final SDict<Long, Long> rows; // defpos->uid of latest update
        public STable Add(SColumn c) throws Exception
        {
            return new STable(this,
                    (cols==null)?new SDict<Long,SColumn>(c.uid,c):cols.Add(c.uid,c),
                    (cpos==null)?new SList<SColumn>(c):cpos.InsertAt(c, cpos.Length),
                    (names==null)?new SDict<String,SColumn>(c.name,c):names.Add(c.name, c)
           );
        }
        public STable Add(SRecord r)
        {
            return new STable(this,rows.Add(r.Defpos(), r.uid));
        }
        public STable Remove(long n) throws Exception
        {
            if (cols!=null && cols.Contains(n))
            {
                var k = 0;
                var cp = cpos;
                var sc = cols.Lookup(n);
                for(var b=cpos.First();b!=null;b=b.Next(),k++)
                    if (b.getValue().uid==n)
                    {
                        cp = cp.RemoveAt(k);
                        break;
                    }
                return new STable(this, cols.Remove(n),cp,names.Remove(sc.name));
            }
            else
                return new STable(this,rows.Remove(n));
        }
        public STable(STransaction tr,String n)
        {
            super(Types.STable,tr);
            name = n;
            cols = null;
            cpos = null;
            rows = null;
            names = null;
        }
        public STable(STable t,String n)
        {
            super(t);
            name = n;
            cols = t.cols;
            cpos = t.cpos;
            rows = t.rows;
            names = t.names;
        }
        STable(STable t,SDict<Long,SColumn> c,SList<SColumn> p,SDict<String,SColumn> n) 
        {
            super(t);
            name = t.name;
            cols = c;
            cpos = p;
            names = n;
            rows = t.rows;
        }
        STable(STable t,SDict<Long,Long> r)
        {
            super(t);
            name = t.name;
            cpos = t.cpos;
            cols = t.cols;
            names = t.names;
            rows = r;
        }
        STable(StreamBase f) throws Exception
        {
            super(Types.STable,f);
            name = f.GetString();
            cols = null;
            cpos = null;
            names = null;
            rows = null;
        }
        public STable(STable t,AStream f) throws Exception
        {
            super(t,f);
            name = t.name;
            f.PutString(name);
            cols = t.cols;
            cpos = t.cpos;
            names = t.names;
            rows = t.rows;
        }
        public static STable Get(SDatabase d,AStream f) throws Exception
        {
            return new STable(f);
        }
        public boolean Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.STable:
                    return ((STable)that).name.compareTo(name) == 0;
            }
            return false;
        }
        public String toString()
        {
            return "Table "+name+"["+Uid()+"]";
        }
}
