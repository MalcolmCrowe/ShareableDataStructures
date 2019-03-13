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

    public SSearch(SDatabase db,Reader f) throws Exception {
        super(Types.SSearch, f);
        sce = (SQuery) f._Get(db);
        if (sce == null) {
            throw new Exception("Query expected");
        }
        SList<Serialisable> w = null;
        var n = f.GetInt();
        for (var i = 0; i < n; i++) {
            var x = f._Get(db).Lookup(new Context(sce.names,null));
            w = (w == null) ? new SList(x): w.InsertAt(x,i);
        }
        where = w;
    }

    public SSearch(SQuery s, SList<Serialisable> w) {
        super(Types.SSearch, -1);
        sce = s;
        where = w;
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

    public static SSearch Get(SDatabase db, Reader f) throws Exception {
        return new SSearch(db,f);
    }
    
    @Override
    public Serialisable Lookup(String a)
    {
        return sce.Lookup(a);
    }

    @Override
    public Serialisable Lookup(Context nms) 
    {
        return(nms.head instanceof SearchRowSet.SearchRowBookmark)? 
                sce.Lookup(nms):this;
    }

    @Override
    public RowSet RowSet(STransaction db, SQuery top, 
            SDict<Long,SFunction> ags,Context cx) throws Exception {
        return new SearchRowSet(db, top, this, ags, cx);
    }
    
    @Override
    public String getAlias() {return sce.getAlias(); }
    
    @Override
    public SDict<Integer,String> getDisplay() 
    {
        return (display!=null)? display:sce.getDisplay();
    }
}
