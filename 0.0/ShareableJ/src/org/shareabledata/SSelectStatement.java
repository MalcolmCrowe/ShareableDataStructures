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
        public final boolean distinct,aggregates;
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
        public SSelectStatement(boolean d, SDict<Integer,String> a, 
                SDict<Integer,Serialisable> c, SQuery q, SList<SOrder> or,
                Context cx) 
        {
            super(Types.SSelect,a,c,new Context(q.names,cx));
            distinct = d;  qry = q; order = or;
            var ag = false;
            if (cpos!=null)
                for (var b = cpos.First(); b != null; b = b.Next())
                    if (b.getValue().val.type == Types.SFunction)
                        ag = true;
            aggregates = ag;
        }
        public static SSelectStatement Get(SDatabase db,Reader f) throws Exception
        {
            f.GetInt(); // uid for the SSelectStatement probably -1
            var d = f.ReadByte() == 1;
            var n = f.GetInt();
            SDict<Integer,String> a = null;
            SDict<Integer,Serialisable> c = null;
            for (var i = 0; i < n; i++)
            {
                a=(a==null)?new SDict(0,f.GetString()):a.Add(i, f.GetString());
                c=(c==null)?new SDict(0,f._Get(db)):c.Add(i,f._Get(db));
            }
            var q = (SQuery)f._Get(db);
            SList<SOrder> o = null;
            n = f.GetInt();
            for (var i = 0; i < n; i++)
                o =(o==null)?new SList((SOrder)f._Get(db)):o.InsertAt((SOrder)f._Get(db), i);
            return new SSelectStatement(d,a,c,q,o,Context.Empty);
        }
        @Override
        public void Put(StreamBase f)
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
                    f.PutString(ab.getValue().val);
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
                            ab.getValue().val.compareTo(((SSelector)ob).name) == 0)
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
                SDict<Long,SFunction> ags, Context cx) throws Exception
        {
            if (order!=null)
                for (var b = order.First(); b != null; b = b.Next())
                    ags = b.getValue().col.Aggregates(ags, cx);
            var ags1 = ags;
            if (cpos!=null)
                for (var b = cpos.First(); b != null; b = b.Next())
                    ags1 = b.getValue().val.Aggregates(ags1, cx);
            var a = this;
            RowSet r = new SelectRowSet(qry.RowSet(tr,a,ags1,cx), a, ags, cx);
            if (cpos!=null && !(qry instanceof SGroupQuery))
            {
                for (var b = cpos.First(); b != null; b = b.Next())
                    ags = b.getValue().val.Aggregates(ags, cx);
                if (ags!=null && ags.Length != 0)
                    r = new EvalRowSet(((SelectRowSet)r)._source, this, ags, cx);
            }
            if (distinct)
                r = new DistinctRowSet(r);
            if (order!=null)
                r = new OrderedRowSet(r, this, cx);
            return r;
        }
        @Override
        public Serialisable Lookup(Context nms) 
        {
            var r = (RowBookmark)nms.head;
            if (display==null)
                return r._ob;
            return new SRow(this,nms);
        }
        @Override
        public String getAlias() { return qry.getAlias(); }
        @Override
        public SDict<Integer, String> getDisplay() 
        { 
            return (display == null) ? qry.getDisplay() : display; 
        }
    
}
