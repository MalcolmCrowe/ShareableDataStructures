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
            implements ILookup<Long,Serialisable>
    {
        public final RowSet _rs;
        public final Context _cx; // first entry is an SRow
        protected RowBookmark(RowSet rs, Context cx, int p)
        {
            super(p);
            _rs = rs; _cx=cx;
        }
        public SRow Ob() throws Exception
        { return _cx.Row(); }
        public static Context _Cx(RowSet rs, SRow r, Context n)
        {
            if (rs instanceof TableRowSet)
            {
                var trs = (TableRowSet)rs;
                n = Context.New(new SDict(trs._tb.uid, r), n,rs._tr);
            }
            return Context.New(r, n,rs._tr);
        }
        @Override
        public Serialisable getValue() { return  (SRow)_cx.refs; }
        @Override
        public boolean defines(Long s) 
        {
            return s==_rs._qry.getAlias()||((SRow)_cx.refs).vals.Contains(s);
        }
        public Serialisable get(Long s)
        {
            if (s==_rs._qry.getAlias())
                return (SRow)_cx.refs;
            return ((SRow)_cx.refs).vals.get(s);
        }
        public boolean SameGroupAs(RowBookmark r)
        {
            return true;
        }
        public boolean Matches(SList<Serialisable> wh)
        {
            if (wh!=null)
            for (var b = wh.First(); b != null; b = b.Next())
                if (b.getValue().Lookup(_cx) != SBoolean.True)
                    return false;
            return true;
        }
        public MTreeBookmark<Serialisable> Mb()
        {
            return null;
        }
        public RowBookmark ResetToTiesStart(MTreeBookmark<Serialisable> mb)
        {
            return null;
        }
        public STransaction Update(STransaction tr,
                SDict<Long,Serialisable> assigs) throws Exception
        {
            return tr; // no changes here
        }
        public STransaction Delete(STransaction tr) throws Exception
        {
            return tr; // no changes here
        }
    }
