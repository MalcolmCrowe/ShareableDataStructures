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
        public STable Add(int sq,SColumn c,String s) 
        {
            var sd = (sq>=0)?sq:(display==null)?0:display.Length;
            var sp = (sq>=0)?sq:(cpos==null)?0:cpos.Length;
            var cs = (cols==null)?new SDict(c.uid,c):cols.Add(c.uid,c);
            var ds = (display==null)?new SDict(sd,new Ident(c.uid,s)):
                            display.Add(sd,new Ident(c.uid,s));
            var cp = (cpos==null)?new SDict(sp,c):cpos.Add(sp,c);
            var rf = (refs==null)?new SDict(c.uid,c):refs.Add(c.uid, c);
            return new STable(this,cs,ds,cp,rf);
        }
        public STable Add(SRecord r)
        {
            var k = r.Defpos();
            var v = r.uid;
            var rws = (rows==null)?new SDict<Long,Long>(k,v):rows.Add(k,v);
            return new STable(this,rws);
        }
        public SColumn FindForRole(SDatabase db,String nm)
        {
            var ss = db.role.subs.get(uid);
            return (SColumn)db.objects.get(ss.obs.get(ss.defs.get(nm)).key);
        }
        public SIndex FindIndex(SDatabase db,SList<Long> key)
        {
            var ss = db.role.subs.get(uid);
            if (indexes!=null)
                for (var b = indexes.First(); b != null; b = b.Next())
                {
                    var x = (SIndex)db.objects.get(b.getValue().key);
                    if (x.cols.Length != key.Length)
                        continue;
                    var kb = key.First();
                    var ma = true;
                    for (var xb = x.cols.First(); ma && xb != null && kb != null;
                        xb = xb.Next(), kb = kb.Next())
                    {
                        var u = kb.getValue();
                        if (u < 0)
                        {
                            var kn = db.role.uids.get(kb.getValue());
                            u = ss.obs.get(ss.defs.get(kn)).key;
                        }
                        ma = xb.getValue() == u;
                    }
                    if (ma)
                        return x;
                }
            return null;
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
                        db=db.Next())
                {
                    var v = b.getValue().val;
                    if (v instanceof SColumn && ((SColumn)v).uid!=n)
                    {
                        var c = (SColumn)v;
                        var id = db.getValue().val;
                        di = (di==null)?new SDict<>(k,id):di.Add(k,id);
                        cp = (cp==null)?new SDict<>(k,c):cp.Add(k,c);  
                        k++;
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
        public STable(STable t,String nm,Writer f) throws Exception
        {
            super(t,f);
            f.PutString(nm);
            cols = null;
            rows = null;
            indexes = null;
        }
        public static STable Get(ReaderBase f)throws Exception
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
            var c = f.Position() - 1;
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
        public Serialisable Lookup(SDatabase tr,Context nms)
        {
             return (nms.refs instanceof RowBookmark)?
                     (SRow)((RowBookmark)nms.refs)._cx.refs: this;
        }
        @Override
        public RowSet RowSet(SDatabase tr,SQuery top, 
                Context cx)
        {
            if (indexes!=null)
                for (var b = indexes.First(); b != null; b = b.Next())
                {
                    var x = (SIndex)tr.objects.Lookup(b.getValue().key);
                    if (x.references < 0)
                        return new IndexRowSet(tr, this, x, null, 
                                SExpression.Op.NotEql, null, cx);
                }
            return new TableRowSet(tr, this, cx);
        }
        @Override
        public boolean Conflicts(SDatabase db, STransaction tr, Serialisable that)
        {
            try {
            return that.type == Types.STable &&
                db.Name(uid).compareTo(tr.Name(((STable)that).uid)) == 0;
            } catch (Exception e) {
                return false;
            }
        }
        public SRecord Check(STransaction tr,SRecord rc) throws Exception
        {
            // check primary/unique key for nulls
            if (indexes!=null)
            for (var b=indexes.First();b!=null;b=b.Next())
            {
                var x = (SIndex)tr.objects.get(b.getValue().key);
                var k = x.Key(rc, x.cols);
                var i = 0;
                for (var kb=k.First();kb!=null;kb=kb.Next(),i++)
                    if (kb.getValue().ob==null)
                    {
                        if (x.primary && i == x.cols.Length - 1)
                        { 
                            long cu=0;
                            var j = 0;
                            var mb = x.rows.PositionAt(k);
                            for (var cb = x.cols.First(); j <= i && cb != null; cb = cb.Next(), j++)
                            {
                                cu = cb.getValue();
                                if (j<i)
                                    mb = mb._inner;
                            }
                            var sc = (SColumn)tr.objects.get(cu);
                            if (sc.dataType == Types.SInteger)
                            {
                                var bu = mb._outer._bmk;
                                while (bu._parent != null)
                                    bu = bu._parent;
                                var ov = (Variant)bu._bucket.Last();
                                var v = new SInteger(((SInteger)ov.ob).value + 1);
                                var f = rc.fields;
                                f = f.Add(cu,v);
                                return new SRecord(tr, rc.table, f);
                            }
                        }
                        throw new Exception("Illegal null value in primary key");
                    }
            }
            return rc;
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            try{
            sb.append("Table ");
            if (db != null)
                sb.append(db.Name(uid));
            else
                sb.append(_Uid(uid));
            } catch(Exception e){}
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
