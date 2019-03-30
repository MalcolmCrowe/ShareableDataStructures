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
public class STable extends SQuery {
        public final SDict<Long,SColumn> cols;
        public final SDict<Long, Long> rows; // defpos->uid of latest update
        public final SDict<Long,Boolean> indexes;
        public STable Add(SColumn c,String s) 
        {
            return new STable(this,
                    (cols==null)?new SDict<>(c.uid,c):cols.Add(c.uid,c),
                    (display==null)?new SDict<>(0,new Ident(c.uid,s)):
                            display.Add(display.Length,new Ident(c.uid,s)),
                    (cpos==null)?new SDict<>(0,c):cpos.Add(cpos.Length,c),
                    (refs==null)?new SDict<>(c.uid,c):refs.Add(c.uid, c)
           );
        }
        public STable Add(SRecord r)
        {
            var k = r.Defpos();
            var v = r.uid;
            var rws = (rows==null)?new SDict<Long,Long>(k,v):rows.Add(k,v);
            return new STable(this,rws);
        }
        public STable Remove(long n)
        {
            if (cols!=null && cols.Contains(n))
            {
                var k = 0;
                var sc = cols.Lookup(n);
                SDict<Integer,Serialisable> cp = null;
                SDict<Integer,Ident> di = null;
                var db = display.First();
                for(var b=cpos.First();db!=null && b!=null;b=b.Next(),
                        db=db.Next(),k++)
                {
                    var v = b.getValue().val;
                    if (v instanceof SColumn && ((SColumn)v).uid!=n)
                    {
                        var c = (SColumn)v;
                        var id = db.getValue().val;
                        di = (di==null)?new SDict<>(k,id):di.Add(k,id);
                        cp = (cp==null)?new SDict<>(k,c):cp.Add(k,c);   
                    }
                }
                return new STable(this, cols.Remove(n),di,cp,refs.Remove(sc.uid));
            }
            else
                return new STable(this,rows.Remove(n));
        }
        STable(int ty,STable tb)
        {
            super(ty,tb);
            cols = tb.cols;
            rows = tb.rows;
            indexes = tb.indexes;
        }
        STable(long u)
        {
            super(Types.STable,u);
            cols = null;
            rows = null;
            indexes = null;
        }
        STable(int t,long u)
        {
            super(t,u);
            cols = null;
            rows = null;
            indexes = null;
        }
        public STable(STransaction tr)
        {
            super(Types.STable,tr);
            cols = null;
            rows = null;
            indexes = null;
        }
        public STable(STable t,String n)
        {
            super(t);
            cols = t.cols;
            rows = t.rows;
            indexes = t.indexes;
        }
        STable(STable t,SDict<Long,SColumn> c,SDict<Integer,Ident>a,
                SDict<Integer,Serialisable> p,SDict<Long,Serialisable> n) 
        {
            super(t,a,p,n);
            cols = c;
            rows = t.rows;
            indexes = t.indexes;
        }
        STable(STable t,SDict<Long,Long> r)
        {
            super(t);
            cols = t.cols;
            rows = r;
            indexes = t.indexes;
        }
        STable(SDict<Long,Boolean> x,STable t)
        {
            super(t);
            cols = t.cols;
            rows = t.rows;
            indexes = x;
        }
        // When an STable is committed is should be empty.
        // If the transactions has cols/rows for it they will committed later.
        public STable(STable t,String nm,AStream f) throws Exception
        {
            super(t,f);
            f.PutString(nm);
            cols = null;
            rows = null;
            indexes = null;
        }
        public static STable Get(Reader f)throws Exception
        {
            var db = f.db;
            if (f instanceof SocketReader)
            {
                var u = f.GetLong();
                var nm = db.role.uids.get(u);
                if (!db.role.globalNames.Contains(nm))
                    throw new Exception("No table " + nm);
                return (STable)db.objects.get(db.role.globalNames.get(nm));
            }
            var c = f.getPosition() - 1;
            var tb = new STable(c);
            var tn = f.GetString();
            f.db = db.Install(tb,tn,c);
            return tb;
        }
        @Override
        public Serialisable Prepare(STransaction db, SDict<Long,Long> pt)
        {
            return this;
        }
        @Override
        public Serialisable UseAliases(SDatabase db,SDict<Long, Long> ta)
        {
            if (ta.Contains(uid))
                return new SAlias(this, ta.get(uid), uid);
            return super.UseAliases(db,ta);
        }
        @Override
        public Serialisable Lookup(Context nms)
        {
             return (nms.refs instanceof RowBookmark)?
                     (SRow)((RowBookmark)nms.refs)._cx.refs: this;
        }
        @Override
        public RowSet RowSet(STransaction tr,SQuery top, 
                SDict<Long,Serialisable> ags)
        {
            if (indexes!=null)
                for (var b = indexes.First(); b != null; b = b.Next())
                {
                    var x = (SIndex)tr.objects.Lookup(b.getValue().key);
                    if (x.references < 0)
                        return new IndexRowSet(tr, this, x, null, null);
                }
            return new TableRowSet(tr, this);
        }
        @Override
        public long getAlias()
        {
            return uid;
        }
        public String toString()
        {
            return "Table "+Uid();
        }
}
