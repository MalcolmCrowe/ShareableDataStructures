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
public class SJoin extends SQuery {
        class JoinType { static final int None=0, Inner=1, Natural=2, Cross=4, 
                Left=8, Right=16, Named=32; }
        public final int joinType;
        public final boolean outer;
        public final SQuery left,right;
        public final SList<SExpression> ons;
        public final SList<String> uses;
        public SJoin(SDatabase db,Reader f) throws Exception
        {
            super(Types.STableExp,_Join(db,f));
            left = (SQuery)f._Get(db);
            if (left==null)
                 throw new Exception("Query expected");
            outer = f.GetInt() == 1;
            joinType = f.GetInt();
            right = (SQuery)f._Get(db);
            if (right==null)
                throw new Exception("Query expected");
            var n = f.GetInt();
            SList<SExpression> on = null;
            SList<String> us = null;
            int m=0;
            if ((joinType&JoinType.Natural)!=0)
                for (var lb=left.names.First();lb!=null;lb=lb.Next())
                {
                    var k = lb.getValue().key;
                    if (right.names.Contains(k))
                    {
                        us = (us==null)?new SList(k):us.InsertAt(k,m);
                        m++;
                    }
                }
            else if ((joinType&JoinType.Named)!=0)
                for (var i=0;i<n;i++)
                {
                    var k = f.GetString();
                    us = (us==null)?new SList(k):us.InsertAt(k,i);
                }
            else if (!((joinType&JoinType.Cross)!=0))
                for (var i = 0; i < n; i++)
                {
                    var e =(SExpression)f._Get(db);
                    if (e==null)
                        throw new Exception("ON exp expected");
                    on =(on==null)?new SList(e):on.InsertAt(e, i);
                }
            ons = on;
            uses = us;
        }
        public SJoin(SQuery lf,boolean ou,int jt,SQuery rg,SList<SExpression> on,
            SList<String>us,SDict<Integer,String> d,SDict<Integer,Serialisable> c,
            Context cx) 
        {
            super(Types.STableExp,d,c,cx);
            left = lf; right = rg; outer = ou; joinType = jt; ons = on;uses=us;
        }
        static SQuery _Join(SDatabase db,Reader f) throws Exception
        {
            f.GetInt();
            var st = f.pos;
            SDict<Integer, String> d = null;
            SDict<Integer, Serialisable> c = null;
            var x = f._Get(db);
            if (!(x instanceof SQuery))
                throw new Exception("Query expected");
            var left = (SQuery)x; 
            var outer = f.GetInt() == 1;
            var joinType = f.GetInt();
            x = f._Get(db);
            if (!(x instanceof SQuery))
                throw new Exception("Query expected");
            var right = (SQuery)x;
            var ab = left.getDisplay().First();
            SDict<String,Boolean> uses = null;
            if ((joinType&JoinType.Named)!=0)
            {
                var n = f.GetInt();
                for (var i = 0; i < n; i++)
                {
                    var k = f.GetString();
                    uses =(uses==null)?new SDict<String,Boolean>(k,true):
                            uses.Add(k,true);
                }
            }
            var k = 0;
            for (var lb = left.cpos.First(); ab!=null && lb != null; ab=ab.Next(), lb = lb.Next())
            {
                var col = lb.getValue();
                var n = ab.getValue().val;
                if (right.names.Contains(n) 
                    && ((!((joinType&JoinType.Natural)!=0))|| 
                        (uses!=null&&uses.Contains(n))))
                    n = left.getAlias() + "." + n;
                d=(d==null)?new SDict(k, n):d.Add(k,n);
                c=(c==null)?new SDict(k, col.val):c.Add(k,col.val);
                k++;
            }
            ab = right.getDisplay().First();
            for (var rb = right.cpos.First(); ab != null && rb != null; ab = ab.Next(), rb = rb.Next())
            {
                if (joinType == JoinType.Natural && left.names.Contains(ab.getValue().val))
                    continue;
                if (uses!=null && uses.Contains(ab.getValue().val))
                    continue;
                var col = rb.getValue();
                var n = ab.getValue().val;
                if (left.names.Contains(n)
                     && ((!((joinType&JoinType.Natural)!=0)) || 
                        (uses!=null && uses.Contains(n))))
                    n = right.getAlias() + "." + n;
                d=(d==null)?new SDict(k, n):d.Add(k,n);
                c=(c==null)?new SDict(k, col.val):c.Add(k,col.val);
                k++;
            }
            f.pos = st;
            return new SQuery(Types.STableExp, d, c,Context.Empty);            
        }
        public void Put(StreamBase f)
        {
            super.Put(f);
            left.Put(f);
            f.PutInt(outer ? 1 : 0);
            f.PutInt((int)joinType);
            right.Put(f);
            int n = 0;
            if (ons!=null)
                n = ons.Length;
            if (uses!=null)
                n = uses.Length;
            f.PutInt(n);
            if (ons!=null)
            for (var b = ons.First(); b != null; b = b.Next())
                b.getValue().Put(f);
            if (uses!=null)
            for (var b = uses.First(); b != null; b = b.Next())
                f.PutString(b.getValue());
        }
        public static SJoin Get(SDatabase d,Reader f) throws Exception
        {
            return new SJoin(d, f);
        }
        public int Compare(RowBookmark lb,RowBookmark rb)
        {
            var lc = new Context(lb,Context.Empty);
            var rc = new Context(rb, Context.Empty);
            if (ons!=null)
                for (var b = ons.First(); b != null; b = b.Next())
                {
                    var ex = (SExpression)b.getValue();
                    var c = ex.left.Lookup(lc).compareTo(ex.right.Lookup(rc));
                    if (c!=0)
                        return c;
                }
            if (uses!=null)
                for (var b = uses.First(); b != null; b = b.Next())
                {
                    var c = lc.get(b.getValue()).compareTo(rc.get(b.getValue()));
                    if (c != 0)
                        return c;
                }
            return 0;
        }
        @Override
        public RowSet RowSet(STransaction tr,SQuery top,
                SDict<Long,SFunction> ags,Context cx) throws Exception
        {
            return new JoinRowSet(tr, top, this, ags, cx);
        }
        @Override
        public void Append(SDatabase db, StringBuilder sb)
        {
            left.Append(db, sb);
            if (outer)
                sb.append(" outer ");
            if (joinType != JoinType.None)
            { sb.append(" "); sb.append(joinType); sb.append(" "); }
            right.Append(db, sb);
            if (ons!=null)
            {
                sb.append(" on ");
                var cm = "";
                for (var b=ons.First();b!=null;b=b.Next())
                {
                    sb.append(cm); cm = ",";
                    b.getValue().Append(db, sb);
                }
            }
        }
    
}
