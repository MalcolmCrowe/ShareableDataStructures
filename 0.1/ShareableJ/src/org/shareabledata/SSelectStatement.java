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
public class SSelectStatement extends SQuery {
        public final boolean distinct;
        public final SList<SOrder> order;
        public final SQuery qry;
        /// <summary>
        /// The select statement has a source query, 
        /// complex expressions and aliases for its columns,
        /// and an ordering
        /// </summary>
        /// <param name="d">Whrther distinct has been specified</param>
        /// <param name="a">The aliases (display) or null</param>
        /// <param name="c">The column expressions or null</param>
        /// <param name="q">The source query, assumed analysed</param>
        /// <param name="or">The ordering</param>
        public SSelectStatement(boolean d, SDict<Integer,Ident> a, 
                SDict<Integer,Serialisable> c, SQuery q, SList<SOrder> or) 
        {
            super(Types.SSelect,a,c);
            distinct = d;  qry = q; order = or;
            var ag = false;
            if (cpos!=null)
                for (var b = cpos.First(); b != null; b = b.Next())
                    if (b.getValue().val.type == Types.SFunction)
                        ag = true;
        }
        @Override
        public SDict<Long, Long> Names(SDatabase tr, SDict<Long, Long> pt)
                throws Exception
        {
            return qry.Names(tr, pt);
        }
        public static SSelectStatement Get(ReaderBase f) throws Exception
        {
            f.GetInt(); // uid for the SSelectStatement probably -1
            var d = f.ReadByte() == 1;
            var n = f.GetInt();
            SDict<Integer,Ident> a = null;
            SDict<Integer,Serialisable> c = null;
            for (var i = 0; i < n; i++)
            {
                var al = f.GetLong();
                var id = new Ident(al,f.db.Name(al));
                a=(a==null)?new SDict(0,id):a.Add(i,id);
                var av = f._Get();
                c=(c==null)?new SDict(0,av):c.Add(i,av);
            }
            var q = (SQuery)f._Get();
            SList<SOrder> o = null;
            n = f.GetInt();
            for (var i = 0; i < n; i++)
            {
                var v = (SOrder)f._Get();
                o =(o==null)?new SList(v):o.InsertAt(v, i);
            }
            return new SSelectStatement(d,a,c,q,o);
        }
        @Override
        public Serialisable UseAliases(SDatabase db, SDict<Long,Long> ta)
        {
            SList<SOrder>os = null;
            var n = 0;
            if (order!=null)
            for (var b = order.First(); b != null; b = b.Next())
            {
                var o = (SOrder)b.getValue().UseAliases(db, ta);
                os = (os==null)?new SList (o, n):os.InsertAt(o,n);
                n++;
            }
            SDict<Integer, Ident> ds = null;
            SDict<Integer, Serialisable> cs = null;
            for (var b = display.First(); b != null; b = b.Next())
            {
                var d = Use(b.getValue().val, ta);
                ds = (ds==null)?new SDict(b.getValue().key,d):
                        ds.Add(b.getValue().key, d);
            }
            for (var b = cpos.First(); b != null; b = b.Next())
            {
                var c =  b.getValue().val.UseAliases(db, ta);
                cs = (cs==null)?new SDict(b.getValue().key,c):
                        cs.Add(b.getValue().key, c);
            }
            var qy = (SQuery)qry.UseAliases(db, ta);
            return new SSelectStatement(distinct, ds, cs, qy, os);
        }
        @Override
        public Serialisable Prepare(STransaction db, SDict<Long,Long> pt) throws Exception
        {
            SList<SOrder>os = null;
            var n = 0;
            if (order!=null)
            for (var b = order.First(); b != null; b = b.Next(),n++)
            {
                var o = (SOrder)b.getValue().Prepare(db, pt);
                os = (os==null)?new SList(o):os.InsertAt(o,n);
            }
            SDict<Integer, Ident> ds = null;
            SDict<Integer, Serialisable> cs = null;
            if (display!=null)
            for (var b = display.First(); b != null; b = b.Next())
            {
                var s = b.getValue();
                var d = Prepare(s.val, pt);
                var k = s.key;
                ds = (ds==null)?new SDict(k,d):
                        ds.Add(k, d);
            }
            if (cpos!=null)
            for (var b = cpos.First(); b != null; b = b.Next())
            {
                var c =  b.getValue().val.Prepare(db, pt);
                cs = (cs==null)?new SDict(b.getValue().key,c):
                        cs.Add(b.getValue().key, c);
            }
            var qy = (SQuery)qry.Prepare(db, pt);
            return new SSelectStatement(distinct, ds, cs, qy, os);
        }
        @Override
        public Serialisable UpdateAliases(SDict<Long,String> uids)
        {
            var w = uids.First();
            if (w == null || w.getValue().key > -1000000)
                return this;
            SList<SOrder>os = null;
            var n = 0;
            if (order!=null)
            for (var b = order.First(); b != null; b = b.Next())
            {
                var o = (SOrder)b.getValue().UpdateAliases(uids);
                os = (os==null)?new SList (o, n):os.InsertAt(o,n);
                n++;
            }
            SDict<Integer, Ident> ds = null;
            SDict<Integer, Serialisable> cs = null;
            for (var b = display.First(); b != null; b = b.Next())
            {
                var id = b.getValue().val;
                var u = id.uid;
                if (uids.Contains(u - 1000000))
                {
                    u -= 1000000;
                    id = new Ident(u,id.id);
                }
                ds = (ds==null)?new SDict(b.getValue().key,id):
                        ds.Add(b.getValue().key, id);
            }
            for (var b = cpos.First(); b != null; b = b.Next())
            {
                var c =  b.getValue().val.UpdateAliases(uids);
                cs = (cs==null)?new SDict(b.getValue().key,c):
                        cs.Add(b.getValue().key, c);
            }
            var qy = (SQuery)qry.UpdateAliases(uids);
            return new SSelectStatement(distinct, ds, cs, qy, os);
        }
        @Override
        public void Put(WriterBase f)throws Exception
        {
            super.Put(f);
            f.WriteByte((byte)(distinct ? 1 : 0));
            f.PutInt((display==null)?0:display.Length);
            if (display!=null)
            {
                var ab = display.First();
                for (var b = cpos.First(); ab!=null && b != null; b = b.Next(), 
                        ab=ab.Next())
                {
                    f.PutLong(ab.getValue().val.uid);
                    b.getValue().val.Put(f);
                }
            }
            qry.Put(f);
            f.PutInt((order==null)?0:order.Length);
            if (order!=null)
                for (var b=order.First();b!=null;b=b.Next())
                    b.getValue().Put(f);
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            if (distinct)
                sb.append("distinct ");
            super.Append(db,sb);
        }
        @Override
        public String toString() 
        {
            var sb = new StringBuilder("Select ");
            var cm = "";
            if (display!=null)
            {
                var ab = display.First();
                for (var b = cpos.First(); ab!=null && b != null; b = b.Next(),ab=ab.Next())
                {
                    sb.append(cm); cm = ",";
                    var ob = b.getValue().val;
                    sb.append(ob);
                    if (ob instanceof SSelector && 
                            ab.getValue().val.uid == ((SSelector)ob).uid)
                        continue;
                    sb.append(" as "); sb.append(ab.getValue().val);
                }
            }
            sb.append(' ');
            sb.append(qry);
            return sb.toString();
        }

        @Override
        public RowSet RowSet(STransaction tr,SQuery top,
                Context cx) throws Exception
        {
            var ags = (cx==null)?null:cx.Ags();
            if (order!=null)
                for (var b = order.First(); b != null; b = b.Next())
                    ags = b.getValue().col.Aggregates(ags);
            var ags1 = ags;
            if (cpos!=null)
                for (var b = cpos.First(); b != null; b = b.Next())
                    ags1 = b.getValue().val.Aggregates(ags1);
            cx = Context.Replace(ags,cx);
            var cx1 = Context.Replace(ags1,cx);
            RowSet r = new SelectRowSet(qry.RowSet(tr,this,cx1), this, cx);
            if (cpos!=null && !(qry instanceof SGroupQuery))
            {
                for (var b = cpos.First(); b != null; b = b.Next())
                    ags = b.getValue().val.Aggregates(ags);
                cx = Context.Replace(ags, cx);
                if (ags!=null && ags.Length != 0)
                    r = new EvalRowSet(((SelectRowSet)r)._source, this, cx);
            }
            if (distinct)
                r = new DistinctRowSet(r);
            if (order!=null)
                r = new OrderedRowSet(r, this);
            return r;
        }
        @Override
        public Serialisable Lookup(STransaction tr,Context cx) 
        {
            if (display==null)
                return (SRow)cx.refs;
            return new SRow(tr,this,cx);
        }
        @Override
        public long getAlias() { return qry.getAlias(); }
        @Override
        public SDict<Integer, Ident> getDisplay() 
        { 
            return (display == null) ? qry.getDisplay() : display; 
        }
    
}
