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
public class SSearch extends SQuery {

    public final SQuery sce;
    public final SList<Serialisable> where;

    public SSearch(SQuery sc,Reader f,long u) throws Exception {
        super(Types.SSearch, u);
        SList<Serialisable> w = null;
        var n = f.GetInt();
        for (var i = 0; i < n; i++) {
            var x = f._Get();
            w = (w == null) ? new SList(x): w.InsertAt(x,i);
        }
        sce = sc;
        where = w;
        f.context = this;
    }

    public SSearch(SQuery s, SList<Serialisable> w) {
        super(Types.SSearch, -1);
        sce = s;
        where = w;
    }
    @Override
    public SDict<Long, Long> Names(SDatabase tr, SDict<Long, Long> pt)
            throws Exception
    {
        return sce.Names(tr, pt);
    }
    @Override
    public void Put(StreamBase f) {
        super.Put(f);
        sce.Put(f);
        f.PutInt(where.Length);
        for (var b = where.First(); b != null; b = b.Next()) {
            b.getValue().Put(f);
        }
    }
    @Override
    public Serialisable Prepare(STransaction db, SDict<Long, Long> pt)
            throws Exception
    {
        SList<Serialisable> w = null;
        var n = 0;
        for (var b = where.First(); b != null; b = b.Next(),n++)
        {
            var v = b.getValue().Prepare(db, pt);
            w =(w==null)?new SList(v):w.InsertAt(v, n);
        }
        return new SSearch((SQuery)sce.Prepare(db, pt),w);
    }
    @Override
    public Serialisable UseAliases(SDatabase db, SDict<Long, Long> ta)
    {
        SList<Serialisable> w = null;
        var n = 0;
        for (var b = where.First(); b != null; b = b.Next(),n++)
        {
            var v = b.getValue().UseAliases(db, ta);
            w =(w==null)?new SList(v):w.InsertAt(v,n);
        }
        return new SSearch((SQuery)sce.UseAliases(db, ta), w);
    }
    public Serialisable UpdateAliases(SDict<Long, String> uids)
    {
        var uu = uids.First();
        if (uu == null || uu.getValue().key > -1000000)
            return this;
        SList<Serialisable> w = null;
        var n = 0;
        for (var b = where.First(); b != null; b = b.Next(),n++)
        {
            var v = b.getValue().UpdateAliases(uids);
            w =(w==null)?new SList(v):w.InsertAt(v,n);
        }
        return new SSearch((SQuery)sce.UpdateAliases(uids), w);
    }
    public static SSearch Get(Reader f) throws Exception {
        var u = f.GetLong();
        var sc = f._Get();
        if (sc==null || !(sc instanceof SQuery))
            throw new Exception("Query expected");
        return new SSearch((SQuery)sc,f,u);
    }
    
    @Override
    public Serialisable Lookup(STransaction tr,Context cx) 
    {
        return(cx.refs instanceof SearchRowSet.SearchRowBookmark)? 
                sce.Lookup(tr,cx):this;
    }

    @Override
    public RowSet RowSet(STransaction db, SQuery top, 
            Context cx) throws Exception {
        return new SearchRowSet(db, top, this, cx);
    }
    
    @Override
    public long getAlias() {return sce.getAlias(); }
    
    @Override
    public SDict<Integer,Ident> getDisplay() 
    {
        return (display!=null)? display:sce.getDisplay();
    }
}
