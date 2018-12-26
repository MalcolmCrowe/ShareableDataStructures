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
        public final SDict<SSelector,Serialisable> where;
        public SSearch(Reader f) throws Exception
        {
            super(Types.SSearch,f);
            sce = (SQuery)f._Get(null);
            if (sce==null)
                    throw new Exception("Query expected");
            SDict<SSelector, Serialisable> w = null;
            var n = f.GetInt();
            for (var i=0;i<n;i++)
            {
                var k = (SSelector)f._Get(null);
                if (k==null)
                        throw new Exception("Selector expected");
                w = (w==null)?new SDict<>(k,f._Get(null))
                        :w.Add(k, f._Get(null));
            }
            where = w;
        }
        public SSearch(SQuery s,SDict<SSelector,Serialisable> w)
        {
            super(Types.SSearch,-1);
            sce = s;
            where = w;
        }
        public void Put(StreamBase f) throws Exception
        {
            super.Put(f);
            sce.Put(f);
            f.PutInt(where.Length);
            for (var b=where.First();b!=null;b=b.Next())
            {
                b.getValue().key.Put(f);
                b.getValue().val.Put(f);
            }
        }
        public static SSearch Get(Reader f) throws Exception
        {
            return new SSearch(f);
        }
        @Override
        public SQuery Lookup(SDatabase db) throws Exception
        {
            var s = sce.Lookup(db);
            SDict<SSelector, Serialisable> w = null;
            for (var b = where.First(); b != null; b = b.Next())
            {
                var x = b.getValue().key.Lookup(s);
                var v = b.getValue().val;
                w = (w==null)?new SDict<SSelector,Serialisable>(x,v):
                        w.Add(x, v);
            }
            return new SSearch(s,w);
        }
        @Override
        public RowSet RowSet(SDatabase db) throws Exception
        {
            return new SearchRowSet(db, this);
        }
    }
