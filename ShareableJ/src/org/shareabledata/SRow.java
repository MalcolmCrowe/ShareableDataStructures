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
public class SRow extends Serialisable implements ILookup<String,Serialisable>
{
    public final SDict<Integer,String> names;
    public final SDict<Integer,Serialisable> cols;
    public final SDict<String, Serialisable> vals;
    public final boolean isNull;
    public final SRecord rec;
    public SRow()
    {
        super(Types.SRow);
        names = null;
        cols = null;
        vals = null;
        isNull = true;
        rec = null;
    }
    public SRow Add(String n, Serialisable v)
    {
        return new SRow(
            (names==null)?new SDict(0,n):names.Add(names.Length, n),
            (cols==null)?new SDict(0,v):cols.Add(cols.Length, v),
            (vals==null)?new SDict(n,v):vals.Add(n,v),
            rec
        );
    }
    SRow(SDict<Integer,String>n,SDict<Integer,Serialisable> c,
            SDict<String,Serialisable>v,SRecord r)
    {
        super(Types.SRow);
        names = n;
        cols = c;
        vals = v;
        rec = r;
        isNull = false;
    }
    public SRow(SList<String>a,SList<Serialisable> s)
    {
        super(Types.SRow);
        var cn = new SDict(0,a.element);
        var r = new SDict(0,s.element);
        var vs = new SDict(a.element,s.element);
        var k = 1;
        var isn = true;
        for (a=a.next,s=s.next;s!=null;s=s.next,a=a.next,k++) 
        {
            var n = a.element;
            cn = cn.Add(k, n);
            r = r.Add(k, s.element);
            vs = vs.Add(n, s.element);
            if (s.element != Null)
                isn = false;
        }
        names = cn;
        cols = r;
        vals = vs;
        rec = null;
        isNull = isn;        
    }
    SRow(SDatabase d,Reader f) throws Exception
    {
        super(Types.SRow);
        var n = f.GetInt();
        SDict<Integer,String> cn = null;
        SDict<Integer, Serialisable> r = null;
        SDict<String,Serialisable> vs = null;
        for(int i=0;i<n;i++)
        {
            var k = f.GetString();
            cn = (cn==null)?new SDict<>(0,k):cn.Add(i,k);
            var v = f._Get(d);
            r = (r==null)?new SDict<>(0,v):r.Add(i, v);
            vs = (vs==null)?new SDict<>(k,v):vs.Add(k,v);
        }
        names = cn;
        cols = r;
        vals = vs;
        isNull = false;
        rec = null;
    }
    public SRow(SDatabase db,SRecord r)
    {
        super(Types.SRow);
        var tb = (STable)db.objects.Lookup(r.table);
        SDict<Integer, String> cn = null;
        SDict<Integer, Serialisable> co = null;
        SDict<String, Serialisable> vs = null;
        var k = 0;
        for (var b = tb.cpos.First(); b != null; b = b.Next(),k++)
        {
            var sc = (SColumn)b.getValue().val;
            var v = r.fields.Lookup(sc.uid);
            if (v==null)
                v = Null;
            co = (co==null)?new SDict(k, v):co.Add(k,v);
            cn = (cn==null)?new SDict(k, sc.name):cn.Add(k,sc.name);
            vs = (vs==null)? new SDict(sc.name, v):vs.Add(sc.name, v);
        }
        names = cn;
        cols = co;
        vals = vs;
        rec = r;
        isNull = false;
    }
    public SRow(SSelectStatement ss, Context cx)
    { 
        super(Types.SRow);
        SDict<Integer, Serialisable> r = null;
        SDict<String, Serialisable> vs = null;
        var isn = true;
        var cb = ss.cpos.First();
        for (var b = ss.display.First(); cb != null && b != null; b = b.Next(), cb = cb.Next())
        {
            try {
            var v = cb.getValue().val.Lookup(cx);
            if (v instanceof SRow && ((SRow)v).cols.Length == 1)
                v = ((SRow)v).cols.Lookup(0);
            if (v==null)
                v = Null;
            r=(r==null)?new SDict(b.getValue().key,v):r.Add(b.getValue().key, v);
            vs=(vs==null)?new SDict(b.getValue().val, v):
                    vs.Add(b.getValue().val, v);
            if (v != Null)
                isn = false;
            } catch(Exception e)
            {
                System.out.println("Evaluation failure: "+e.getMessage());
                r = null;
                vs = null;
                break;
            }
        }
        names = ss.display;
        cols = r;
        vals = vs;
        rec = ((RowBookmark)cx.head)._ob.rec;
        isNull = isn;
    }
    public static SRow Get(SDatabase d,Reader f) throws Exception
    {
        return new SRow(d,f);
    }
    @Override
    public void Put(StreamBase f)
    {
        super.Put(f);
        f.PutInt(cols.Length);
        var cb = cols.First();
        for (var b = names.First(); cb!=null && b != null; cb=cb.Next(), b = b.Next())
        {
            f.PutString(b.getValue().val);
            var s = cb.getValue().val;
            if (s!=null)
                s.Put(f);
            else
                Null.Put(f);
        }
    }
    @Override
    public Serialisable Lookup(Context cx)
    {
        SDict<Integer, Serialisable> v = null;
        SDict<String, Serialisable> r = null;
        var nb = names.First();
        for (var b = cols.First(); nb != null && b != null; 
                nb = nb.Next(), b = b.Next())
        {
            var e = b.getValue().val.Lookup(cx);
            v =(v==null)?new SDict(b.getValue().key,e):v.Add(b.getValue().key, e);
            r=(r==null)?new SDict(nb.getValue().val,e):r.Add(nb.getValue().val, e);
        }
        return new SRow(names, v, r, 
                (cx.head instanceof RowBookmark)?((RowBookmark)cx.head)._ob.rec:null);
    }
    @Override
    public void Append(SDatabase db,StringBuilder sb)
    {
        sb.append('{');
        String cm = "";
        var nb = names.First();
        for (var b=cols.First();nb!=null && b!=null;
                nb=nb.Next(),b=b.Next())
        {
            var v = b.getValue();
            if (v.val!=Null)
            {
                sb.append(cm); cm = ",";
                sb.append(nb.getValue().val);
                sb.append(":");
                v.val.Append(db,sb);
            }
        }
        sb.append("}");        
    }
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Append(null,sb);
        return sb.toString();
    }
    @Override
    public boolean defines(String s)
    {
        return vals.Contains(s);
    }
    @Override
    public Serialisable get(String s)
    {
        return vals.Lookup(s);
    }
}
