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
    public abstract class SSelector extends SDbObject
    {
        public SSelector(SSelector s)
        {
            super(s);
        }
        public SSelector(int t,long u)
        {
            super(t,u);
        }
        public SSelector(STransaction tr,int t)
        {
            super(t,tr);
        }
        protected SSelector(int t, ReaderBase f) throws Exception
        {
            super(t,f);
        }
        protected SSelector(SSelector s,Writer f) throws Exception
        {
            super(s,f);
        }
        public static Serialisable Get(ReaderBase f) throws Exception
        {
            var x = f.GetLong(); // a client-side uid
            var ro = f.db.role;
            var n = ro.uids.get(x);// client-side name
            if (f.context instanceof SQuery)
            {
                if (ro.defs.get(f.context.uid).defines(n)) //it's a ColumnDef
                {
                    var sc = ((STable)f.context).cols.get(ro.defs.get(f.context.uid).get(n));
                    f.db = f.db.Add(sc, sc.uid);
                    return sc;
                }
            }
            else if (ro.globalNames.defines(n)) // it's a table or stored query
                return f.db.objects.get(ro.globalNames.get(n));
            throw new Exception("Unknown " + n);
        }
    }
