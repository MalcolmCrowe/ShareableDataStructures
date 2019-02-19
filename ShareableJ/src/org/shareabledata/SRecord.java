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
        public SRecord(int ty,STransaction tr,long t,SDict<Long,Serialisable> f)
        {
            super(ty,tr);
            fields = f;
            table = t;
        }
        public long Defpos()
        {
            return uid;
        }
        public SRecord(SDatabase db,SRecord r,AStream f)
        {
            super(r,f); 
            table = f.Fix(r.table);
            fields = r.fields;
            f.PutLong(table);
            var tb = (STable)db.objects.Lookup(table);
            f.PutInt(r.fields.Length);
            for (var b=r.fields.First();b!=null;b=b.Next())
            {
                f.PutLong(b.getValue().key);
                b.getValue().val.Put(f);
            }
        }
        protected SRecord(int t,SDatabase d,Reader f) throws Exception
        {
            super(t,f);
            table = f.GetLong();
            int n = f.GetInt();
            var tb = (STable)d.objects.Lookup(table);
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
        public static SRecord Get(SDatabase d,Reader f) throws Exception
        {
            return new SRecord(Types.SRecord,d,f);
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            sb.append(" for "); sb.append(Uid());
            String cm = "(";
            for (var b = fields.First(); b != null; b = b.Next())
            {
                sb.append(cm); cm = ",";
                sb.append(SDbObject._Uid(b.getValue().key)); sb.append(":");
                sb.append(b.getValue().val.toString());
            }
            sb.append(")");
        }
        public boolean Matches(RowBookmark rb,SList<Serialisable> wh)
        {
            if (wh!=null)
                for (var b = wh.First(); b != null; b = b.Next())
                {
                    var v = b.getValue();
                    if (v instanceof SExpression)
                     try {
                        var e = ((SExpression)v).Lookup(new Context(rb,null));
                        if (e!=SBoolean.True)
                            return false;
                        } catch(Exception e)
                        {
                            System.out.println("Evaluation error: "+e.getMessage());
                            return false;
                        }
                }
            return true;
        }
        @Override
        public boolean Check(SDict<Long, Boolean> rdC)
        {
            return (rdC!=null) && (rdC.Contains(Defpos()) || rdC.Contains(table));
        }
        @Override
        public boolean Conflicts(Serialisable that)
        {
            switch(that.type)
            {
                case Types.SDelete:
                    return ((SDelete)that).delpos == Defpos();
            }
            return false;
        }
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder(super.toString());
            Append(null,sb);
            return sb.toString();
        }
}
