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
public class SAlias extends SQuery {
        public final SQuery qry;
        public final long alias;
        public SAlias(SQuery q,long a,Reader f, long u) throws Exception
        {
            super(Types.SAlias, u);
            var tr = (STransaction)f.db;
            qry = (SQuery)q.Prepare(tr,null);
            alias = a;
            f.db = tr.Add(a,qry);
        }
        public SAlias(SQuery q,long a,long u)
        {
            super(Types.SAlias,u);
            qry = q;
            alias = a;
        }
        @Override
        public SDict<Long, Long> Names(SDatabase tr, SDict<Long, Long> pt)
                throws Exception
        {
            return qry.Names(tr, pt);
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            qry.Put(f);
            f.PutLong(alias);
        }
        @Override
        public Serialisable UseAliases(SDatabase db, SDict<Long, Long> ta)
        {
            return new SAlias((SQuery)qry.UseAliases(db,ta),alias,uid);
        }
        @Override
        public Serialisable UpdateAliases(SDict<Long, String> uids)
        {
            var q = (SQuery)qry.UpdateAliases(uids);
            return (q==qry)?this:new SAlias(q,alias,uid);
        }
        @Override
        public Serialisable Prepare(STransaction db, SDict<Long,Long> pt)
                throws Exception
        {
            return new SAlias((SQuery)qry.Prepare(db,pt),alias,uid);
        }
        public static SAlias Get(Reader f) throws Exception
        {
            var u = f.GetLong();
            var q = (SQuery)f._Get();
            return new SAlias(q,f.GetLong(),f,u);
        }
        @Override
        public RowSet RowSet(STransaction tr, SQuery top,
                Context cx) throws Exception
        {
            return new AliasRowSet(this,qry.RowSet(tr, top, cx));
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            qry.Append(db,sb);
            sb.append(" "); sb.append(alias);
        }
        @Override
        public long getAlias(){ return alias; } 
        @Override
        public SDict<Integer,Ident> getDisplay() { return qry.getDisplay(); }
}
