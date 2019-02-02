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
    public abstract class RowBookmark extends Bookmark<Serialisable>
            implements ILookup<String,Serialisable>
    {
        public final RowSet _rs;
        public final SRow _ob;
        public final SDict<Long,Serialisable> _ags;
        protected RowBookmark(RowSet rs, SRow ob, int p)
        {
            super(p);
            _rs = rs; _ob = ob; _ags = null;
        }
        protected RowBookmark(RowSet rs, SRow ob, 
                SDict<Long,Serialisable> a,int p)
        {
            super(p);
            _rs = rs; _ob = ob; _ags = a;
        }
        @Override
        public Serialisable getValue() { return  _ob; }
        public void Append(SDatabase db,StringBuilder sb) throws Exception
        {
            _ob.Append(db,sb);
        }
        @Override
        public boolean defines(String s)
        {
            return s.compareTo(_rs._qry.getAlias())==0||_ob.vals.Contains(s);
        }
        public Serialisable get(String s)
        {
            return s.compareTo(_rs._qry.getAlias())==0?_ob:_ob.get(s);
        }
        public boolean SameGroupAs(RowBookmark r)
        {
            return true;
        }
        public STransaction Update(STransaction tr,
                SDict<String,Serialisable> assigs) throws Exception
        {
            return tr; // no changes here
        }
        public STransaction Delete(STransaction tr) throws Exception
        {
            return tr; // no changes here
        }
    }
