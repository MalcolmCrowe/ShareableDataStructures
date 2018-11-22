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
public abstract class SQuery extends SDbObject {
        public final SList<SSelector> cpos;
        public final SDict<String, SSelector> names;
        public final SDict<Long, SSelector> cols;
        public SQuery(int t, long u)
        {
            super(t,u);
            cols = null;
            cpos = null;
            names = null;
        }
        public SQuery(int t, STransaction tr)
        {
            super(t,tr);
            cols = null;
            cpos = null;
            names = null;
        }
        public SQuery(SQuery q)
        {
            super(q);
            cols = q.cols;
            cpos = q.cpos;
            names = q.names;
        }
        protected SQuery(SQuery q, SDict<Long, SSelector> c, SList<SSelector> p, 
                SDict<String, SSelector> n)
        {
            super(q);
            cpos = p;
            cols = c;
            names = n;
        }
        protected SQuery(int t, Reader f)
        {
            super(t,f);
            cols = null;
            cpos = null;
            names = null;
        }
        protected SQuery(SQuery q, AStream f) throws Exception
        {
            super(q,f);
            cols = q.cols;
            cpos = q.cpos;
            names = q.names;
        }
        /// <summary>
        /// Queries come to us with client-local SDbObjects instead of STransaction SDbObjects. 
        /// We need to look them up
        /// </summary>
        /// <param name="db">A database or transaction</param>
        /// <returns>A version of this with correct references for db</returns>
        public abstract SQuery Lookup(SDatabase db) throws Exception;
        /// <summary>
        /// Construct the Rowset for the given SDatabase (may have changed since SQuery was built)
        /// </summary>
        /// <param name="db">The current state of the database or transaction</param>
        /// <returns></returns>
        public abstract RowSet RowSet(SDatabase db);
}
