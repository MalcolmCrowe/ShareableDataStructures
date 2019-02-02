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
    public final SDict<Integer, String> groupby;
    public final SList<Serialisable> having;
    public SGroupQuery(SDatabase db,Reader f) throws Exception
    {
        super(Types.SGroupQuery,f);
        source = (SQuery)f._Get(db);
        if (source==null)
            throw new Exception("Query expected");
        SDict<Integer, String> g = null;
        SList<Serialisable> h = null;
        var n = f.GetInt();
        for (var i = 0; i < n; i++)
            g=(g==null)?new SDict(0,f.GetString()):g.Add(i, f.GetString());
        n = f.GetInt();
        for (var i = 0; i < n; i++)
        {
            var hh = f._Get(db).Lookup(new Context(source.names,null));
            h=(h==null)?new SList(hh):h.InsertAt(hh, i);
        }
        groupby = g;
        having = h;
    }
    public SGroupQuery(SQuery s,SDict<Integer,String> d,SDict<Integer,Serialisable> c,
        Context cx,SDict<Integer,String> g,SList<Serialisable> h) 
    {
        super(Types.SGroupQuery, d,c,cx); 
        source = s;
        groupby = g;
        having = h;
    }
    public void Put(StreamBase f)
    {
        super.Put(f);
        source.Put(f);
        f.PutInt(groupby.Length);
        for (var b = groupby.First(); b != null; b = b.Next())
            f.PutString(b.getValue().val);
        f.PutInt(having.Length);
        for (var b = having.First(); b != null; b = b.Next())
            b.getValue().Put(f);
    }
    public static SGroupQuery Get(SDatabase d,Reader f) throws Exception
    {
        return new SGroupQuery(d, f);
    }
    public RowSet RowSet(STransaction tr, SQuery top, SDict<Long,SFunction> ags,
            Context cx) throws Exception
    {
        return new GroupRowSet(tr,top, this, ags, cx);
    }
    public Serialisable Lookup(Context nms)
    {
        if (!(nms.head instanceof SearchRowSet.SearchRowBookmark))
            return this;
        return source.Lookup(nms);
    }
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
    public String getAlias() { return source.getAlias(); }
}
