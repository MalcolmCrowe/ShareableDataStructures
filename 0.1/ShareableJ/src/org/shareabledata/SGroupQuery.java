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
public class SGroupQuery extends SQuery {
    public final SQuery source;
    public final SDict<Integer, Long> groupby;
    public final SList<Serialisable> having;
    public SGroupQuery(SQuery sc,Reader f,long u) throws Exception
    {
        super(Types.SGroupQuery,u);
        source = sc;
        SDict<Integer, Long> g = null;
        SList<Serialisable> h = null;
        var n = f.GetInt();
        var tr = (STransaction)f.db;
        for (var i = 0; i < n; i++)
        {
            var nm = f.GetLong();
            g=(g==null)?new SDict(0,nm):g.Add(i,nm);
        }
        n = f.GetInt();
        for (var i = 0; i < n; i++)
        {
            var hh = f._Get().Lookup(tr,Context.New(source.refs,null));
            h=(h==null)?new SList(hh):h.InsertAt(hh, i);
        }
        groupby = g;
        having = h;
    }
    public SGroupQuery(SQuery s,SDict<Integer,Ident> d,SDict<Integer,Serialisable> c,
        SDict<Integer,Long> g,SList<Serialisable> h) 
    {
        super(Types.SGroupQuery, d,c); 
        source = s;
        groupby = g;
        having = h;
    }
    @Override
    public SDict<Long, Long> Names(SDatabase tr, SDict<Long, Long> pt)
            throws Exception
    {
        return source.Names(tr, pt);
    }
    @Override
    public void Put(StreamBase f)
    {
        super.Put(f);
        source.Put(f);
        f.PutInt(groupby.Length);
        for (var b = groupby.First(); b != null; b = b.Next())
            f.PutLong(b.getValue().val);
        f.PutInt((having==null)?0:having.Length);
        if (having!=null)
        for (var b = having.First(); b != null; b = b.Next())
            b.getValue().Put(f);
    }
    @Override
    public Serialisable Prepare(STransaction db, SDict<Long,Long> pt)
            throws Exception
    {
        SDict<Integer, Ident> ds = null;
        SDict<Integer, Serialisable> cs = null;
        SDict<Integer, Long> g = null;
        SList<Serialisable> h = null;
        var n = 0;
        if (display!=null)
        for (var b = display.First(); b != null; b = b.Next())
        {
            var k = b.getValue().key;
            var v = Prepare(b.getValue().val, pt);
            ds =(ds==null)?new SDict(k,v):ds.Add(k,v);
        }
        if (cpos!=null)
        for (var b = cpos.First(); b != null; b = b.Next())
        {
            var k = b.getValue().key;
            var v = b.getValue().val.Prepare(db, pt);
            cs =(cs==null)?new SDict(k,v):cs.Add(k,v);
        }
        for (var b = groupby.First(); b != null; b = b.Next())
        {
            var k = b.getValue().key;
            var v = Prepare(b.getValue().val, pt);
            g =(g==null)?new SDict(k,v):g.Add(k,v);
        }
        if (having!=null)
        for (var b = having.First(); b != null; b = b.Next())
        {
            var v = b.getValue().Prepare(db, pt);
            h =(h==null)?new SList(v):h.InsertAt(v, n++);
        }
        return new SGroupQuery((SQuery)source.Prepare(db,pt),ds,cs,g,h);
    }
    @Override
    public Serialisable UseAliases(SDatabase db, SDict<Long, Long> ta)
    {
        SDict<Integer, Ident> ds = null;
        SDict<Integer, Serialisable> cs = null;
        SDict<Integer, Long> g = null;
        SList<Serialisable> h = null;
        var n = 0;
        if (display!=null)
        for (var b = display.First(); b != null; b = b.Next())
        {
            var k = b.getValue().key;
            var u = Use(b.getValue().val,ta);
            ds =(ds==null)?new SDict(k,u):ds.Add(k,u);
        }
        if (cpos!=null)
        for (var b = cpos.First(); b != null; b = b.Next())
        {
            var k = b.getValue().key;
            var v = b.getValue().val.UseAliases(db,ta);
            cs = (cs==null)?new SDict(k,v):cs.Add(k,v);
        }
        for (var b = groupby.First(); b != null; b = b.Next())
        {
            var k = b.getValue().key;
            var u = Use(b.getValue().val,ta);
            g =(g==null)?new SDict(k,u):g.Add(k,u);
        }
        if (having!=null)
        for (var b = having.First(); b != null; b = b.Next())
        {
            var v = b.getValue().UseAliases(db,ta);
            h =(h==null)?new SList(v):h.InsertAt(v, n);
            n++;
        }
        return new SGroupQuery((SQuery)source.UseAliases(db,ta), ds, cs, g, h);
    }
    @Override
    public Serialisable UpdateAliases(SDict<Long, String> uids)
    {
        var w = uids.First();
        if (w == null || w.getValue().key > -1000000)
            return this;
        SDict<Integer, Ident> ds = null;
        SDict<Integer, Serialisable> cs = null;
        SDict<Integer, Long> g = null;
        SList<Serialisable> h = null;
        var n = 0;
        if (display!=null)
        for (var b = display.First(); b != null; b = b.Next())
        {
            var k = b.getValue().key;
            var u = b.getValue().val.uid;
            if (uids.Contains(u - 1000000))
                u -= 1000000;
            var id = new Ident(u,b.getValue().val.id);
            ds =(ds==null)?new SDict(k,id):ds.Add(k,id);
        }
        if (cpos!=null)
        for (var b = cpos.First(); b != null; b = b.Next())
        {
            var k = b.getValue().key;
            var v = b.getValue().val.UpdateAliases(uids);
            cs = (cs==null)?new SDict(k,v):cs.Add(k,v);
        }
        for (var b = groupby.First(); b != null; b = b.Next())
        {
            var k = b.getValue().key;
            var u = b.getValue().val;
            if (uids.Contains(u - 1000000))
                u -= 1000000;
            g =(g==null)?new SDict(k,u):g.Add(k,u);
        }
        if(having!=null)
        for (var b = having.First(); b != null; b = b.Next())
        {
            var v = b.getValue().UpdateAliases(uids);
            h =(h==null)?new SList(v):h.InsertAt(v, n);
            n++;
        }
        return new SGroupQuery((SQuery)source.UpdateAliases(uids), ds, cs,
            g, h);
    }
    public static SGroupQuery Get(Reader f) throws Exception
    {
        var u = f.GetLong();
        var source = f._Get();
        if (!(source instanceof SQuery))
            throw new Exception("Query expected");
        return new SGroupQuery((SQuery)source, f,u);
    }
    @Override
    public RowSet RowSet(STransaction tr, SQuery top, Context cx)
            throws Exception
    {
        return new GroupRowSet(tr,top, this, cx);
    }
    @Override
    public Serialisable Lookup(STransaction tr,Context cx)
    {
        if (!(cx.refs instanceof SearchRowSet.SearchRowBookmark))
            return this;
        return source.Lookup(tr,cx);
    }
    @Override
    public void Append(SDatabase db, StringBuilder sb)
    {
        source.Append(db, sb);
        sb.append(" groupby ");
        var cm = "";
        for (var b =groupby.First();b!=null;b=b.Next())
        {
            sb.append(cm); cm = ",";
            sb.append(b.getValue().val);
        }
        if (having.Length>0)
        {
            sb.append(" having ");
            cm = "";
            for (var b=having.First();b!=null;b=b.Next())
            {
                sb.append(cm); cm = " and ";
                b.getValue().Append(db,sb);
            }
        }
    }
    public long getAlias() { return source.getAlias(); }
}
