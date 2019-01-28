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
        public final String name;
        public final SDict<Long,SSelector> cols;
        public final SDict<Long, Long> rows; // defpos->uid of latest update
        public final SDict<Long,Boolean> indexes;
        public STable Add(SColumn c) 
        {
            return new STable(this,
                    (cols==null)?new SDict<>(c.uid,c):cols.Add(c.uid,c),
                    (display==null)?new SDict<>(0,c.name):display.Add(display.Length,c.name),
                    (cpos==null)?new SDict<>(0,c):cpos.Add(cpos.Length,c),
                    (names==null)?new SDict<>(c.name,c):names.Add(c.name, c)
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
                SDict<Integer,String> di = null;
                var db = display.First();
                for(var b=cpos.First();db!=null && b!=null;b=b.Next(),
                        db=db.Next(),k++)
                {
                    var v = b.getValue().val;
                    if (v instanceof SColumn && ((SColumn)v).uid!=n)
                    {
                        var c = (SColumn)v;
                        di = (di==null)?new SDict<>(k,c.name):di.Add(k,c.name);
                        cp = (cp==null)?new SDict<>(k,c):cp.Add(k,c);   
                    }
                }
                return new STable(this, cols.Remove(n),di,cp,names.Remove(sc.name));
            }
            else
                return new STable(this,rows.Remove(n));
        }
        STable(int ty,STable tb)
        {
            super(ty,tb);
            name = tb.name;
            cols = tb.cols;
            rows = tb.rows;
            indexes = tb.indexes;
        }
        STable(String n, long u)
        {
            super(Types.STable,u);
            name = n;
            cols = null;
            rows = null;
            indexes = null;
        }
        public STable(String n)
        {
            super(Types.STable,-1);
            name = n;
            cols = null;
            rows = null;
            indexes = null;
        }
        public STable(STransaction tr,String n)
        {
            super(Types.STable,tr);
            name = n;
            cols = null;
            rows = null;
            indexes = null;
        }
        public STable(STable t,String n)
        {
            super(t);
            name = n;
            cols = t.cols;
            rows = t.rows;
            indexes = t.indexes;
        }
        STable(STable t,SDict<Long,SSelector> c,SDict<Integer,String>a,
                SDict<Integer,Serialisable> p,SDict<String,Serialisable> n) 
        {
            super(t,a,p,n);
            name = t.name;
            cols = c;
            rows = t.rows;
            indexes = t.indexes;
        }
        STable(STable t,SDict<Long,Long> r)
        {
            super(t);
            name = t.name;
            cols = t.cols;
            rows = r;
            indexes = t.indexes;
        }
        STable(SDict<Long,Boolean> x,STable t)
        {
            super(t);
            name = t.name;
            cols = t.cols;
            rows = t.rows;
            indexes = x;
        }
        STable(Reader f)
        {
            super(Types.STable,f);
            name = f.GetString();
            cols = null;
            rows = null;
            indexes = null;
        }
        // When an STable is committed is should be empty.
        // If the transactions has cols/rows for it they will committed later.
        public STable(STable t,AStream f) throws Exception
        {
            super(t,f);
            name = t.name;
            f.PutString(name);
            cols = null;
            rows = null;
            indexes = null;
        }
        public static STable Get(SDatabase db,Reader f)throws Exception
        {
            var tb = new STable(f);
            var n = tb.name;
            if (tb.uid<0)
            {
                if (n.charAt(0) == '_')
                    tb = (STable)SysTable.system.Lookup(n);
                else
                    tb = db.GetTable(n);
            }
            if (tb==null)
                    throw new Exception("No such table "+tb.name);
            return tb;
        }
        @Override
        public void Put(StreamBase f) 
        {
            super.Put(f);
            f.PutString(name);
        }
        @Override
        public Serialisable Lookup(ILookup<String,Serialisable> nms) 
        {
             return (nms instanceof RowBookmark)?((RowBookmark)nms)._ob:this;
        }
        @Override
        public boolean Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.STable:
                    return ((STable)that).name.compareTo(name) == 0;
            }
            return false;
        }
        @Override
        public RowSet RowSet(STransaction tr,Context cx)
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
        public String getAlias()
        {
            return name;
        }
        public String toString()
        {
            return super.toString()+name;
        }
}
