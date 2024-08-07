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
        public final SDict<String, Serialisable> names;
        public final SDict<Integer, String> display;
        public SQuery(int t, long u)
        {
            super(t,u);
            display = null;
            cpos = null;
            names = null;
        }
        public SQuery(int t, STransaction tr)
        {
            super(t,tr);
            display = null;
            cpos = null;
            names = null;
        }
        public SQuery(SQuery q)
        {
            super(q);
            display = q.display;
            cpos = q.cpos;
            names = q.names;
        }
        public SQuery(int t,SQuery q)
        {
            super(t);
            display = q.display;
            cpos = q.cpos;
            names = q.names;
        }
        protected SQuery(int t, SDict<Integer, String> a, SDict<Integer,Serialisable> c, 
                Context cx)
        {
            super(t,-1);
            SDict<Integer, Serialisable> cp = null;
            SDict<String, Serialisable> cn = null;
            if (a!=null && c!=null)
            {
                var ab = a.First();
                for (var cb = c.First();ab!=null && cb!=null;ab=ab.Next(),cb=cb.Next())
                {
                    var s = cb.getValue().val;
                    if (cx!=Context.Empty)
                        s = cb.getValue().val.Lookup(cx);
                    cp=(cp==null)?new SDict(cb.getValue().key, s)
                            :cp.Add(cb.getValue().key, s);
                    cn=(cn==null)?new SDict(ab.getValue().val, s)
                            :cn.Add(ab.getValue().val, s);
                }
            }
            display = a;
            cpos = cp;
            names = cn;
        }
        protected SQuery(SQuery q, SDict<Integer, String> c, SDict<Integer,Serialisable> p, 
                SDict<String, Serialisable> n)
        {
            super(q);
            cpos = p;
            display = c;
            names = n;
        }
        protected SQuery(int t, Reader f)
        {
            super(t,f);
            display = null;
            cpos = null;
            names = null;
        }
        /// This constructor is only called when committing am STable.
        /// Ignore the columns defined in the transaction.
        protected SQuery(SQuery q, AStream f) throws Exception
        {
            super(q,f);
            display = null;
            cpos = null;
            names = null;
        }
        public Serialisable Lookup(String a)
        {
            var r = names.Lookup(a);
            return (r==null)?Null:r;
        }
        /// <summary>
        /// Construct the Rowset for the given SDatabase (may have changed since SQuery was built)
        /// </summary>
        /// <param name="db">The current state of the database or transaction</param>
        /// <returns></returns>
        public RowSet RowSet(STransaction db,SQuery top,
                SDict<Long,SFunction> ags, Context cx) throws Exception{
            throw new Exception("Not implemented");
        }
        
        SDict<Integer,String> getDisplay()
        {
            return display; 
        }
        public String getAlias()
        {
            return "";
        }
}
