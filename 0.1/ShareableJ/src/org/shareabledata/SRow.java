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
public class SRow extends Serialisable implements ILookup<Long,Serialisable>,
        Comparable
{
    public final SDict<Integer,Ident> names;
    public final SDict<Integer,Serialisable> cols;
    public final SDict<Long, Serialisable> vals;
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
    public SRow Add(Ident n, Serialisable v)
    {
        return new SRow(
            (names==null)?new SDict(0,n):names.Add(names.Length, n),
            (cols==null)?new SDict(0,v):cols.Add(cols.Length, v),
            (vals==null)?new SDict(n.uid,v):vals.Add(n.uid,v),
            rec
        );
    }
    SRow(SDict<Integer,Ident>n,SDict<Integer,Serialisable> c,
            SDict<Long,Serialisable>v,SRecord r)
    {
        super(Types.SRow);
        names = n;
        cols = c;
        vals = v;
        rec = r;
        isNull = false;
    }
    public SRow(SList<Ident>a,SList<Serialisable> s)
    {
        super(Types.SRow);
        var cn = new SDict(0,a.element);
        var r = new SDict(0,s.element);
        var vs = new SDict(a.element.uid,s.element);
        var k = 1;
        var isn = true;
        for (a=a.next,s=s.next;s!=null;s=s.next,a=a.next,k++) 
        {
            var n = a.element;
            cn = cn.Add(k, n);
            r = r.Add(k, s.element);
            vs = vs.Add(n.uid, s.element);
            if (s.element != Null)
                isn = false;
        }
        names = cn;
        cols = r;
        vals = vs;
        rec = null;
        isNull = isn;        
    }
    SRow(Reader f) throws Exception
    {
        super(Types.SRow);
        var n = f.GetInt();
        SDict<Integer,Ident> cn = null;
        SDict<Integer, Serialisable> r = null;
        SDict<Long,Serialisable> vs = null;
        for(int i=0;i<n;i++)
        {
            var k = f.GetLong();
            var id = new Ident(k,f.db.Name(k));
            cn = (cn==null)?new SDict<>(0,id):cn.Add(i,id);
            var v = f._Get();
            r = (r==null)?new SDict<>(0,v):r.Add(i, v);
            vs = (vs==null)?new SDict<>(k,v):vs.Add(k,v);
        }
        names = cn;
        cols = r;
        vals = vs;
        isNull = false;
        rec = null;
    }
    public SRow(SDatabase db,SRecord r) throws Exception
    {
        super(Types.SRow);
        var tb = (STable)db.objects.Lookup(r.table);
        SDict<Integer, Ident> cn = null;
        SDict<Integer, Serialisable> co = null;
        SDict<Long, Serialisable> vs = null;
        var k = 0;
        if (tb.cpos!=null)
        for (var b = tb.cpos.First(); b != null; b = b.Next(),k++)
        {
            var sc = (SColumn)b.getValue().val;
            var id = new Ident(sc.uid,db.Name(sc.uid));
            var v = r.fields.Lookup(sc.uid);
            if (v==null)
                v = Null;
            co = (co==null)?new SDict(k, v):co.Add(k,v);
            cn = (cn==null)?new SDict(k, id):cn.Add(k,id);
            vs = (vs==null)? new SDict(sc.uid, v):vs.Add(sc.uid, v);
        }
        names = cn;
        cols = co;
        vals = vs;
        rec = r;
        isNull = false;
    }
    public SRow(STransaction tr,SSelectStatement ss, Context cx)
    { 
        super(Types.SRow);
        SDict<Integer, Serialisable> r = null;
        SDict<Long, Serialisable> vs = null;
        var isn = true;
        if (ss.cpos!=null && ss.display!=null)
        {
            var cb = ss.cpos.First();
            for (var b = ss.display.First(); cb != null && b != null; b = b.Next(), cb = cb.Next())
            {
                try {
                var v = cb.getValue().val.Lookup(tr,cx);
                if (v instanceof SRow && ((SRow)v).cols.Length == 1)
                    v = ((SRow)v).cols.Lookup(0);
                if (v==null)
                    v = Null;
                r=(r==null)?new SDict(b.getValue().key,v):r.Add(b.getValue().key, v);
                vs=(vs==null)?new SDict(b.getValue().val.uid, v):
                        vs.Add(b.getValue().val.uid, v);
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
        }
        names = ss.display;
        cols = r;
        vals = vs;
        rec = ((SRow)cx.refs).rec;
        isNull = isn;
    }
    public static SRow Get(Reader f) throws Exception
    {
        return new SRow(f);
    }
    public Serialisable UseAliases(SDatabase db, SDict<Long, Long> ta)
    {
        SList<Ident> ns = null;
        SList<Serialisable> vs = null;
        for (var i=0;i<names.Length;i++)
        {
            var nm = names.get(i);
            if (ta.Contains(nm.uid))
                nm = new Ident(ta.get(names.get(i).uid),nm.id);
            var v = cols.get(i).UseAliases(db, ta);
            ns =(ns==null)?new SList(nm):ns.InsertAt(nm,i);
            vs =(vs==null)?new SList(v):vs.InsertAt(v,i);
        }
        return new SRow(ns, vs);
    }
    @Override
    public Serialisable Prepare(STransaction db, SDict<Long,Long> pt) throws Exception
    {
        SDict<Integer, Ident> nms = null;
        SDict<Integer, Serialisable> cls = null;
        SDict<Long, Serialisable> vls = null;
        var nb = names.First();
        for (var b=cols.First();nb!=null&&b!=null;b=b.Next(),nb=nb.Next())
        {
            var n = nb.getValue();
            var u = SDbObject.Prepare(n.val.uid, pt);
            var id = new Ident(u,n.val.id);
            var v = b.getValue().val.Prepare(db, pt);
            nms = (nms==null)?new SDict(n.key,id):
                    nms.Add(n.key,id);
            cls = (cls==null)?new SDict(b.getValue().key,v):
                    cls.Add(b.getValue().key,v);
            vls = (vls==null)?new SDict(u, v):vls.Add(u,v);
        }
        if (nms==null|cls==null||vls==null)
            throw new Exception("PE07");
        return new SRow(nms,cls,vls,rec);
    }
    @Override
    public void Put(StreamBase f)
    {
        super.Put(f);
        f.PutInt(cols.Length);
        var cb = cols.First();
        if (names!=null)
        for (var b = names.First(); cb!=null && b != null; cb=cb.Next(), b = b.Next())
        {
            f.PutLong(b.getValue().val.uid);
            var s = cb.getValue().val;
            if (s!=null)
                s.Put(f);
            else
                Null.Put(f);
        }
    }
    @Override
    public Serialisable Lookup(STransaction tr,Context cx)
    {
        SDict<Integer, Serialisable> v = null;
        SDict<Long, Serialisable> r = null;
        var nb = names.First();
        for (var b = cols.First(); nb != null && b != null; 
                nb = nb.Next(), b = b.Next())
        {
            var e = b.getValue().val.Lookup(tr,cx);
            v =(v==null)?new SDict(b.getValue().key,e):v.Add(b.getValue().key, e);
            r=(r==null)?new SDict(nb.getValue().val.uid,e):
                    r.Add(nb.getValue().val.uid, e);
        }
        return new SRow(names, v, r, 
                (cx.refs instanceof SRow)?((SRow)cx.refs).rec:null);
    }
    public int compareTo(Object ob)
    {
        if (ob instanceof SRow)
        {
            SRow sr = (SRow)ob;
            var c = cols.Length - sr.cols.Length;
            if (c!=0)
                return c;
            var ab = sr.vals.First();
            for (var b = vals.First(); ab != null && b != null; ab = ab.Next(), b=b.Next())
            {
                c = b.getValue().val.compareTo(ab.getValue().val);
                if (c != 0)
                    return c;
            }
            return 0;
        }
        if (cols.Length == 1)
            return (vals.First().getValue().val).compareTo(ob);
        return 1;
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
                sb.append(nb.getValue().val.id);
                sb.append(":");
                v.val.Append(db,sb);
            }
        }
        sb.append("}");        
    }
    @Override
    public void Append(StringBuilder sb)
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
                sb.append(nb.getValue().val.id);
                sb.append(":");
                v.val.Append(sb);
            }
        }
        sb.append("}");        
    }
    @Override
    public boolean isValue()
    {
        for (var b = cols.First(); b != null; b = b.Next())
            if (!b.getValue().val.isValue())
                return false;
        return true;        
    }
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Append(sb);
        return sb.toString();
    }
    @Override
    public boolean defines(Long u)
    {
        return vals.Contains(u);
    }
    @Override
    public Serialisable get(Long s)
    {
        return vals.Lookup(s);
    }
}
