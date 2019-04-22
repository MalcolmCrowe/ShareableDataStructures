/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *  /// When view processing is added, we will often find multiple table occurrences
    /// in the resulting query. These will need to be fully aliased 
    /// with their columns as the view is encountered during the construction
    /// of the top-level query (the view aliases will need to be added to it).
    /// As a precaution let us do this for all its tables and columns as routine.

 * @author Malcolm
 */
public class SView extends SDbObject {
        public final SDict<Long,Boolean> viewobs;
        public final SQuery viewdef;
        public SView(SQuery d) 
        {
            super(Types.SView);
            viewdef = d;
            viewobs = null;
        }
        public SView(STransaction tr,SQuery d) throws Exception
        {
            super(Types.SView,tr);
            viewdef = d;
            viewobs = ViewObs(tr,d);
        }
        public SView(STransaction tr,SView v,Writer f) throws Exception
        {
            super(v,f);
            super.Put(f);
            var vd = (SQuery)v.viewdef.Fix(f);
            vd.Put(f);
            viewdef = vd;
            viewobs = ViewObs(tr,vd);            
        }
        static SDict<Long, Boolean> ViewObs(SDatabase db, SQuery d)
                throws Exception
        {
            var pt = d.Names(db, null);
            SDict<Long, Boolean> qt = null;
            for (var b = pt.First(); b != null; b = b.Next())
                qt =(qt==null)?new SDict(b.getValue().val, true):qt.Add(b.getValue().val,true);
            return qt;
        }
        @Override
        public Serialisable Prepare(STransaction db, SDict<Long,Long> pt)
                throws Exception
        {
            return new SView((SQuery)viewdef.Prepare(db,pt));
        }
        public void Put(WriterBase f) throws Exception
        {
            viewdef.Put(f);
        }
        public static SView Get(ReaderBase f) throws Exception
        {
            var vw = (SView)f.db.objects.get(f.GetLong());
            // Construct new aliases in tr for all objects used in the view definition including aliases.
            SDict<Long, Long> ta = null;
            for (var b = vw.viewobs.First(); b != null; b = b.Next())
                ta = Aliases(f, b.getValue().key, ta);
            // Transform vw to use these new aliases for all its objects and return it.
            vw = (SView)vw.UseAliases(f.db,ta);
            return vw;
        }
        static SDict<Long,Long> Aliases(ReaderBase f,long u,SDict<Long,Long> ta)
        {
            var a = --f.lastAlias;
            var n = "$" + (maxAlias - a);
            f.db.Add(a, n);
            ta = (ta==null)?new SDict(u, a):ta.Add(u,a);
            if (f.db.role.props.Contains(u))
                for (var b = f.db.role.props.get(u).First(); b != null; b = b.Next())
                    ta = Aliases(f, b.getValue().key, ta);
            return ta;
        }        
        public boolean Conflicts(SDatabase db,STransaction tr,Serialisable that)
        {
            switch (that.type)
            {
                case Types.SView:
                {
                    var v = (SView)that;
                    return db.role.uids.get(uid).compareTo(tr.Name(v.uid)) == 0;
                }
            }
            return false;
        }
}
