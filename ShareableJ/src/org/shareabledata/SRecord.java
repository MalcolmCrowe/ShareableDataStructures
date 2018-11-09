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
public class SRecord extends SDbObject {
        public final SDict<Long, Serialisable> fields;
        public final long table;
        public SRecord(STransaction tr,long t,SDict<Long,Serialisable> f)
        {
            super(Types.SRecord,tr);
            fields = f;
            table = t;
        }
        public long Defpos()
        {
            return uid;
        }
        public SRecord(SDatabase db,SRecord r,AStream f) throws Exception
        {
            super(r,f); 
            table = f.Fix(r.table);
            fields = r.fields;
            f.PutLong(table);
            var tb = (STable)db.Lookup(table);
            f.PutInt(r.fields.Count());
            for (var b=r.fields.First();b!=null;b=b.Next())
            {
                f.PutLong(b.getValue().key);
                b.getValue().val.Put(f);
            }
        }
        protected SRecord(SDatabase d,StreamBase f) throws Exception
        {
            super(Types.SRecord,f);
            table = f.GetLong();
            int n = f.GetInt();
            var tb = (STable)d.Lookup(table);
            SDict<Long,Serialisable> a = null;
            for(int i = 0;i< n;i++)
            {
                var k = f.GetLong();
                if (a==null)
                    a = new SDict<Long,Serialisable>(k,f._Get(d));
                else
                    a = a.Add(k, f._Get(d));
            }
            fields = a;
        }
        public static SRecord Get(SDatabase d,StreamBase f) throws Exception
        {
            return new SRecord(d,f);
        }
        @Override
        public void Append(StringBuilder sb)
        {
            sb.append(" for "); sb.append(Uid());
            String cm = "(";
            for (var b = fields.First(); b != null; b = b.Next())
            {
                sb.append(cm); cm = ",";
                sb.append(b.getValue().key); sb.append(":");
                sb.append(b.getValue().val.toString());
            }
            sb.append(")");
        }
        public boolean Conflicts(Serialisable that)
        {
            switch(that.type)
            {
                case Types.SDelete:
                    return ((SDelete)that).delpos == Defpos();
            }
            return false;
        }
        public String toString()
        {
            StringBuilder sb = new StringBuilder("Record ");
            sb.append(Uid());
            Append(sb);
            return sb.toString();
        }
}
