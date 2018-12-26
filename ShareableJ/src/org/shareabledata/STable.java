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
        public final SDict<Long, Long> rows; // defpos->uid of latest update
        public STable Add(SColumn c) throws Exception
        {
            return new STable(this,
                    (cols==null)?new SDict<>(c.uid,c):cols.Add(c.uid,c),
                    (cpos==null)?new SList<>(c):cpos.InsertAt(c, cpos.Length),
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
        public STable Remove(long n) throws Exception
        {
            if (cols!=null && cols.Contains(n))
            {
                var k = 0;
                var cp = cpos;
                var sc = cols.Lookup(n);
                for(var b=cpos.First();b!=null;b=b.Next(),k++)
                    if (b.getValue().uid==n)
                    {
                        cp = cp.RemoveAt(k);
                        break;
                    }
                return new STable(this, cols.Remove(n),cp,names.Remove(sc.name));
            }
            else
                return new STable(this,rows.Remove(n));
        }
        STable(String n, long u)
        {
            super(Types.STable,u);
            name = n;
            rows = null;          
        }
        public STable(String n)
        {
            super(Types.STable,-1);
            name = n;
            rows = null;          
        }
        public STable(STransaction tr,String n)
        {
            super(Types.STable,tr);
            name = n;
            rows = null;
        }
        public STable(STable t,String n)
        {
            super(t);
            name = n;
            rows = t.rows;
        }
        STable(STable t,SDict<Long,SSelector> c,SList<SSelector> p,SDict<String,SSelector> n) 
        {
            super(t,c,p,n);
            name = t.name;
            rows = t.rows;
        }
        STable(STable t,SDict<Long,Long> r)
        {
            super(t);
            name = t.name;
            rows = r;
        }
        STable(Reader f) throws Exception
        {
            super(Types.STable,f);
            name = f.GetString();
            rows = null;
        }
        public STable(STable t,AStream f) throws Exception
        {
            super(t,f);
            name = t.name;
            f.PutString(name);
            rows = t.rows;
        }
        public static STable Get(Reader f) throws Exception
        {
            return new STable(f);
        }
        @Override
        public void Put(StreamBase f) throws Exception
        {
            super.Put(f);
            f.PutString(name);
        }
        @Override
        public SQuery Lookup(SDatabase db) throws Exception
        {
            STable tb = null;
            if (name.charAt(0) == '_')
            {
                var st = SysTable.system.Lookup(name);
                if (st!=null)
                    tb = st;
            } else
                tb = db.GetTable(name);
            if (tb==null)
                throw new Exception("No such table " + name);
            if (cols==null || cols.Length == 0)
                return tb;
            SDict<Long, SSelector> co = null;
            SList<SSelector> cp = null;
            SDict<String, SSelector> cn = null;
            for (var c = cpos;c!=null && c.Length!=0;c=c.next)
            {
                var tc = tb.names.Lookup(((SColumn)c.element).name);
                co = (co==null)?new SDict<Long,SSelector>(tc.uid,tc)
                        :co.Add(tc.uid, tc);
                cp = (cp==null)?new SList<SSelector>(tc)
                        :cp.InsertAt(tc, cp.Length);
                cn = (cn==null)?new SDict<String,SSelector>(tc.name,tc)
                        :cn.Add(tc.name, tc);
            }
            return new STable(tb, co, cp, cn);
        }
        public boolean Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.STable:
                    return ((STable)that).name.compareTo(name) == 0;
            }
            return false;
        }
        public RowSet RowSet(SDatabase db)
        {
            return new TableRowSet(db, this);
        }
        public String toString()
        {
            return super.toString()+name;
        }
}
