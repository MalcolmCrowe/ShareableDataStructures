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
        class JoinType { static final int None=0, Natural=1, Cross=2, Left=3, Right=4, Full=5; }
        public final int joinType;
        public final boolean outer;
        public final SQuery left,right;
        public final SList<SExpression> ons;
        public SJoin(SDatabase db,Reader f) throws Exception
        {
            super(Types.STableExp,f);
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
            for (var i = 0; i < n; i++)
            {
                var e =(SExpression)f._Get(db);
                if (e==null)
                    throw new Exception("ON exp expected");
                on =(on==null)?new SList(e):on.InsertAt(e, i);
            }
            ons = on;
        }
        public SJoin(SQuery lf,boolean ou,int jt,SQuery rg,SList<SExpression> on,
            SDict<Integer,String> d,SDict<Integer,Serialisable> c,SDict<String,Serialisable> n) 
            
        {
            super(Types.STableExp,d,c,n);
            left = lf; right = rg; outer = ou; joinType = jt; ons = on;
        }
        public void Put(StreamBase f)
        {
            super.Put(f);
            left.Put(f);
            f.PutInt(outer ? 1 : 0);
            f.PutInt((int)joinType);
            right.Put(f);
            f.PutInt(ons.Length);
            for (var b = ons.First(); b != null; b = b.Next())
                b.getValue().Put(f);
        }
        public static SJoin Get(SDatabase d,Reader f) throws Exception
        {
            return new SJoin(d, f);
        }
        public RowSet RowSet(STransaction tr, Context cx) throws Exception
        {
            return new JoinRowSet(tr, this, cx);
        }
        public void Append(SDatabase db, StringBuilder sb)
        {
            left.Append(db, sb);
            if (outer)
                sb.append(" outer ");
            if (joinType != JoinType.None)
            { sb.append(" "); sb.append(joinType); sb.append(" "); }
            right.Append(db, sb);
            if (ons.Length>0)
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
