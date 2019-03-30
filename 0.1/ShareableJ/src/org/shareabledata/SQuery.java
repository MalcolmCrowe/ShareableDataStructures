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
public class SQuery extends SDbObject {
        public final SDict<Integer,Serialisable> cpos;
        public final SDict<Long, Serialisable> refs;
        public final SDict<Integer, Ident> display;
        public static final SQuery Static = new SQuery(Types.SQuery,SysTable._uid--);
        public SQuery(int t, long u)
        {
            super(t,u);
            display = null;
            cpos = null;
            refs = null;
        }
        public SQuery(int t, STransaction tr)
        {
            super(t,tr);
            display = null;
            cpos = null;
            refs = null;
        }
        public SQuery(SQuery q)
        {
            super(q);
            display = q.display;
            cpos = q.cpos;
            refs = q.refs;
        }
        public SQuery(int t,SQuery q)
        {
            super(t);
            display = q.display;
            cpos = q.cpos;
            refs = q.refs;
        }
        protected SQuery(SQuery q, SDict<Integer, Ident> c, SDict<Integer,Serialisable> p, 
                SDict<Long, Serialisable> n)
        {
            super(q);
            cpos = p;
            display = c;
            refs = n;
        }
        public SQuery(int t,SDict<Integer,Ident>a,SDict<Integer,Serialisable> c)
        {
            super(t);
            SDict<Long,Serialisable> cn = null;
            if (a!=null && c!=null)
            {
                var ab = a.First();
                for (var cb=c.First();ab!=null&&cb!=null;ab=ab.Next(),cb=cb.Next())
                    cn = (cn==null)?new SDict(ab.getValue().val.uid,cb.getValue().val):
                            cn.Add(ab.getValue().val.uid, cb.getValue().val);
            }
            display = a;
            cpos = c;
            refs = cn;
        }
        protected SQuery(int t,Reader f) throws Exception
        {
            super(t,f);
            display = null;
            cpos = null;
            refs = null;
        }
        /// This constructor is only called when committing am STable.
        /// Ignore the columns defined in the transaction.
        protected SQuery(SQuery q, AStream f) throws Exception
        {
            super(q,f);
            display = null;
            cpos = null;
            refs = null;
        }
        public static SQuery Get(Reader f) throws Exception
        {
            return Static;
        }
        public SDict<Long, Long> Names(SDatabase db, SDict<Long, Long> pt)
                throws Exception
        {
            // prepare a list of names this query defines
            SDict<String, Long> ns = null;
            if (uid!=-1)
            {
                var nm = db.role.uids.get(uid);
                ns = (ns==null)?new SDict(nm,uid):ns.Add(nm,uid);
            }
            if (display!=null)
            for (var b = display.First(); b != null; b = b.Next())
            {
                var v = b.getValue().val;
                ns = (ns==null)?new SDict(v.id,v.uid):ns.Add(v.id,v.uid);
            }
            // scan the list of client-side uids if any to add entries to the parsing table
            if (db.role.uids!=null && ns!=null)
            for (var b = db.role.uids.PositionAt(maxAlias); 
                    b != null && b.getValue().key<0; b = b.Next())
                if (ns.Contains(b.getValue().val))
                {
                    var k = b.getValue().key;
                    var v = ns.get(b.getValue().val);
                    pt =(pt==null)?new SDict(k,v):pt.Add(k,v);
                }
            return pt;
        }
        protected long Use(long u, SDict<Long, Long> ta)
        {
            return (ta!=null && ta.Contains(u)) ? ta.get(u) : u;
        }
        protected Ident Use(Ident n,SDict<Long,Long> ta)
        {
            return (ta!=null && ta.Contains(n.uid))?
                    new Ident(ta.get(n.uid),n.id): n;
        }
        protected SSlot<Long,Long> Use(SSlot<Long,Long> u, SDict<Long, Long> ta)
        {
            return new SSlot(Use(u.key,ta),Use(u.val,ta));
        }
        @Override
        public Serialisable Prepare(STransaction db, SDict<Long,Long> pt) throws Exception
        {
            SDict<Integer, Ident> ds=null;
            SDict<Integer, Serialisable> cp=null;
            if (display!=null)
            for (var b = display.First(); b != null; b = b.Next())
            {
                var v = b.getValue();
                ds = (ds==null)?new SDict(v.key,Prepare(v.val.uid, pt)):
                        ds.Add(b.getValue().key, 
                           new Ident(Prepare(b.getValue().val.uid, pt),v.val.id));
            }
            if (cpos!=null)
            for (var b = cpos.First(); b != null; b = b.Next())
                cp = (cp==null)?new SDict(b.getValue().key, 
                        b.getValue().val.Prepare(db,pt)):
                        cp.Add(b.getValue().key, b.getValue().val.Prepare(db, pt));
            return new SQuery(type,ds,cp);
        }
        /// <summary>
        /// Construct the Rowset for the given SDatabase (may have changed since SQuery was built)
        /// </summary>
        /// <param name="db">The current state of the database or transaction</param>
        /// <returns></returns>
        public RowSet RowSet(STransaction db,SQuery top,
                SDict<Long,Serialisable> ags) throws Exception{
            throw new Exception("Not implemented");
        }
        
        SDict<Integer,Ident> getDisplay()
        {
            return display; 
        }
        public long getAlias()
        {
            return -1;
        }
        static long CheckAlias(SDict<Long,String>uids,long u)
        {
            var r=u-1000000;
            return uids.Contains(r)?r:u;
        }
         static Ident CheckAlias(SDict<Long,String>uids,Ident n)
        {
            var r=n.uid-1000000;
            return uids.Contains(r)?new Ident(r,n.id):n;
        }
        static SSlot<Long,Long> CheckAlias(SDict<Long,String> uids,SSlot<Long,Long> p)
        {
            return new SSlot(CheckAlias(uids,p.key),CheckAlias(uids,p.val));
        }
        @Override
        public boolean isValue()
        {
            return false;
        }
}
