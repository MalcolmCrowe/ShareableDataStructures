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
public class GroupRowSet extends RowSet {
    public final SGroupQuery _gqry;
    public final SList<TreeInfo<Long>> _info; // computed from the grouped columns
    public final SMTree<Long> _tree; // for the treeinfo in the GroupRowSet
    public final SDict<Long, SDict<Long,Serialisable>> _grouprows; // accumulators for the aggregates
    public final SQuery _top;
    public final RowSet _sce;
    public GroupRowSet(SDatabase tr,SQuery top,SGroupQuery gqry,
            Context cx) throws Exception
    {
        this(gqry.source.RowSet(tr,top,cx), top, gqry, cx);
    }
    GroupRowSet(RowSet sce,SQuery top,SGroupQuery gqry,Context cx)
            throws Exception
    {
        super(sce._tr,gqry,cx);
        _gqry = gqry;
        _sce = sce;
        SList<TreeInfo<Long>> inf = null;
        for (var b=gqry.groupby.First();b!=null;b=b.Next())
        {
            var t = new TreeInfo(b.getValue().val,'d','i',false);
            inf =(inf==null)?new SList(t):inf.InsertAt(t,b.getValue().key);
        }
        _info = inf;
        var t = new SMTree<Long>(inf);
        SDict<Long, SDict<Long,Serialisable>> r = null;
        long n = 0;
        for (var b=(RowBookmark)_sce.First();b!=null;b=(RowBookmark)b.Next())
        {
            var k = Key(b);
            if (!t.Contains(k))
            {
                t = t.Add(k, n).t;
                r =(r==null)?new SDict(n,null):r.Add(n,null);
                n++;
            }
            var mb = t.PositionAt(k);
            var m = (long)((mb!=null)?mb.getValue().val:0);
            var ag = AddIn(sce._tr, r.get(m), b._cx);
            r =(r==null)?new SDict(m, ag):r.Add(m,ag);
        }
        _tree = t;
        _grouprows = r;
        _top = top;
    }
    protected SCList<Variant> Key(RowBookmark b) throws Exception
        {
        SCList<Variant> k = null;
        var n = 0;
        for (var g = _gqry.groupby.First(); g != null; g = g.Next(),n++)
        {
            var v= new Variant(b.Ob().get(g.getValue().val),true);
            k =(k==null)?new SCList(v):k.InsertAt(v,n);
        }
        return k;
    }
    protected SRow _Row(MTreeBookmark<Long> b)
    {
        var r = new SRow();
        SDict<Long, Serialisable> kc = null;
        var gb = b.getValue().key.First();
        for (var kb = _info.First(); gb != null && kb != null; 
                gb = gb.Next(), kb = kb.Next())
            kc =(kc==null)?
                new SDict(kb.getValue().headName,(Serialisable)gb.getValue().ob):
                kc.Add(kb.getValue().headName,(Serialisable)gb.getValue().ob);
        var cx = Context.New(kc,Context.New(_grouprows.get(b.getValue().val),_cx));
        var ab = _top.getDisplay().First();
        for (var cb = _top.cpos.First(); ab != null && cb != null; 
                ab = ab.Next(), cb = cb.Next())
            r = r.Add(ab.getValue().val,cb.getValue().val.Lookup(_tr,cx));
        return r;
    }
    static SDict<Long,Serialisable> AddIn(SDatabase tr, 
            SDict<Long,Serialisable> cur, Context cx) throws Exception
    {
        var ags = (cx==null)?null:cx.Ags();
        for (var b=ags.First(); b!=null;b=b.Next())
        {
            var f = (SFunction)b.getValue().val;
            var v = f.arg.Lookup(tr,Context.New(cur,cx));
            if (v != Serialisable.Null)
            {
                var w = (cur!=null && cur.Contains(f.fid))?f.AddIn(cur.get(f.fid),v)
                    :f.StartCounter(v);
                cur =(cur==null)?new SDict(f.fid,w):cur.Add(f.fid,w);
            }
        }
        return cur;
    }
    public Bookmark<Serialisable> First()
    {
        var b = (MTreeBookmark<Long>)_tree.First();
        return (b==null)? null: 
                new GroupRowBookmark(this, b,_Row(b),_grouprows.Lookup(0L),0);
    }
    class GroupRowBookmark extends RowBookmark
    {
        public final GroupRowSet _grs;
        public MTreeBookmark<Long> _bmk;
        protected GroupRowBookmark(GroupRowSet grs,MTreeBookmark<Long> bm,
                SRow r,SDict<Long,Serialisable> a,int p)
        { 
            super(grs,_Cx(grs,r,Context.New(a,null)),p);
            _grs = grs; _bmk = bm;
        }

        @Override
        public Bookmark<Serialisable> Next()
        {
            var b = (MTreeBookmark<Long>)_bmk.Next();
            return (b==null)?null:
                    new GroupRowBookmark(_grs, b, _grs._Row(b),
                            _grs._grouprows.Lookup(b.getValue().val),0);
        }
    }
    
}
