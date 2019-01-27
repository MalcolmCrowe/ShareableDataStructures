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
    public final RowSet _sce;
    public GroupRowSet(STransaction tr,SGroupQuery gqry,Context cx) throws Exception
    {
        super(tr,gqry);
        _gqry = gqry;
        _sce = gqry.source.RowSet(tr, cx);
    }
    SRow _Row(RowBookmark bm)
    {
        throw new Error("Not here yet");
    }
    public Bookmark<Serialisable> First()
    {
        var b = _sce.First();
        return (b==null)? null: new GroupRowBookmark(this, (RowBookmark)b,0);
    }
    class GroupRowBookmark extends RowBookmark
    {
        public final GroupRowSet _grs;
        public RowBookmark _bmk;
        protected GroupRowBookmark(GroupRowSet grs,RowBookmark bm,int p)
        { 
            super(grs,grs._Row(bm),p);
            _grs = grs; _bmk = bm;
        }

        public Bookmark<Serialisable> Next()
        {
            var b = _bmk.Next();
            return (b==null)?null:new GroupRowBookmark(_grs, (RowBookmark)b, 0);
        }
    }
    
}
