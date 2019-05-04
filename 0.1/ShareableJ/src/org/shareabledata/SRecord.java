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
        public SRecord(STransaction tr,long t,SDict<Long,Serialisable> f) throws Exception
        {
            this(Types.SRecord,tr,t,f);
        }
        public SRecord(int ty,STransaction tr,long t,SDict<Long,Serialisable> f)
                throws Exception
        {
            super(ty,tr);
            var tb = (STable)tr.objects.get(t);
            var a = tb.Aggregates(null);
            var cx = Context.New(a, null);
            var rb = tb.getDisplay().First();
            for (var b = tb.cols.First(); b != null && rb!=null; b = b.Next(), rb=rb.Next())
            {
                var sc = b.getValue().val;
                var cn = rb.getValue().val.uid;
                if (sc.constraints!=null)
                for (var c = sc.constraints.First(); c != null; c = c.Next())
                {
                    var fn = c.getValue().val;
                    switch (fn.func)
                    {
                        case SFunction.Func.Default:
                            if ((!f.Contains(cn)) || f.get(cn) == Null)
                                f=(f==null)?new SDict(cn, fn.arg):f.Add(cn,fn.arg);
                            break;
                        case SFunction.Func.NotNull:
                            if ((!f.Contains(cn)) || f.get(cn) == Null)
                                throw new Exception("Value of "+tr.Name(cn)+" cannot be null");
                            break;
                        case SFunction.Func.Constraint:
                            {
                                var cf = Context.New(f, cx);
                                if (fn.arg.Lookup(tr,cf) == SBoolean.False)
                                    throw new Exception("Constraint violation");
                                break;
                            }
                        case SFunction.Func.Generated:
                            {
                                var cf = Context.New(f, cx);
                                if (f.Contains(cn) && f.get(cn) != Null)
                                    throw new Exception("Value cannot be supplied for column " + tr.Name(cn));
                                var v = fn.arg.Lookup(tr,cf);
                                f=(f==null)?new SDict(cn, v):f.Add(cn,v);
                            }
                            break;
                    }
                }
            }
            fields = f;
            table = t;
        }
        public long Defpos()
        {
            return uid;
        }
        public SRecord(SDatabase db,SRecord r,Writer f) throws Exception
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
        protected SRecord(int t,ReaderBase f) throws Exception
        {
            super(t,f);
            table = f.GetLong();
            int n = f.GetInt();
            var tb = (STable)f.db.objects.Lookup(table);
            SDict<Long,Serialisable> a = null;
            for(int i = 0;i< n;i++)
            {
                var k = f.GetLong();
                if (a==null)
                    a = new SDict<Long,Serialisable>(k,f._Get());
                else
                    a = a.Add(k, f._Get());
            }
            fields = a;
        }
        public static SRecord Get(ReaderBase f) throws Exception
        {
            return new SRecord(Types.SRecord,f);
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
                        var e = ((SExpression)v).Lookup(rb._rs._tr,rb._cx);
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
        public boolean Conflicts(SDatabase db,STransaction tr,Serialisable that)
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
            StringBuilder sb = new StringBuilder();
            Append(null,sb);
            return sb.toString();
        }
}
