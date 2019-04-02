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
        public final SList<SExpression> ons; // equality expressions lcol=rcol
        public final SDict<Long,Long> uses; // key is for RIGHT, val for LEFT
        public SJoin(Reader f) throws Exception
        {
            super(Types.STableExp,_Join(f));
            left = (SQuery)f._Get();
            if (left==null)
                 throw new Exception("Query expected");
            outer = f.GetInt() == 1;
            joinType = f.GetInt();
            right = (SQuery)f._Get();
            if (right==null)
                throw new Exception("Query expected");
            var n = f.GetInt();
            SList<SExpression> on = null;
            SDict<Long,Long> us = null;
            var tr = (STransaction)f.db;
            SDict<String,Long> lns = null;
            var ld = left.getDisplay();
            if (ld!=null)
            for (var b=ld.First();b!=null;b=b.Next())
            {
                var v = b.getValue().val;
                lns = (lns==null)?new SDict(v.id,v.uid):lns.Add(v.id, v.uid);
            }
            SDict<String,Long> rns = null;
            var rd = right.getDisplay();
            if (rd!=null)
            for (var b=rd.First();b!=null;b=b.Next())
            {
                var v = b.getValue().val;
                if (joinType==JoinType.Natural && lns!=null && lns.Contains(v.id))
                    us = (us==null)?new SDict(v.uid,lns.get(v.id)):
                            us.Add(v.uid, lns.get(v.id));
                rns = (rns==null)?new SDict(v.id,v.uid):rns.Add(v.id, v.uid);
            }
            if((joinType & JoinType.Named) !=0 && lns!=null && rns!=null)
                for (var i=0;i<n;i++)
                {
                    var nm = tr.role.uids.get(f.GetLong());
                    if (!(lns.Contains(nm) && rns.Contains(nm)))
                       throw new Exception("name "+nm+" not present in Join");
                    us = (us==null)?new SDict(rns.get(nm),lns.get(nm)):
                            us.Add(rns.get(nm),lns.get(nm));
                }
            else if (!((joinType&JoinType.Cross)!=0))
                for (var i = 0; i < n; i++)
                {
                    var e =(SExpression)f._Get();
                    if (e==null)
                        throw new Exception("ON exp expected");
                    on =(on==null)?new SList(e):on.InsertAt(e, 0);
                }
            ons = on;
            uses = us;
            f.context = this;
        }
        public SJoin(SQuery lf,boolean ou,int jt,SQuery rg,SList<SExpression> on,
            SDict<Long,Long>us,SDict<Integer,Ident> d,SDict<Integer,Serialisable> c) 
        {
            super(Types.STableExp,d,c);
            left = lf; right = rg; outer = ou; joinType = jt; ons = on;uses=us;
        }
        static SQuery _Join(Reader f) throws Exception
        {
            f.GetInt();
            var st = f.pos;
            SDict<Integer, Ident> d = null;
            SDict<Integer, Serialisable> c = null;
            SDict<String,Long> nms = null;
            var x = f._Get();
            if (!(x instanceof SQuery))
                throw new Exception("Query expected");
            var left = (SQuery)x; 
            var outer = f.GetInt() == 1;
            var joinType = f.GetInt();
            x = f._Get();
            if (!(x instanceof SQuery))
                throw new Exception("Query expected");
            var right = (SQuery)x;
            SDict<Long,Long> uses = null;
            if ((joinType&JoinType.Named)!=0)
            {
                var n = f.GetInt();
                for (var i = 0; i < n; i++)
                {
                    var k = f.GetLong();
                    var m = f.GetLong();
                    uses =(uses==null)?new SDict<Long,Long>(k,m):
                            uses.Add(k,m);
                }
            }
            var k = 0;
            var ld = left.getDisplay();
            if (ld!=null && left.cpos!=null)
            {
                var ab = ld.First();
                for (var lb = left.cpos.First(); ab != null && lb != null; 
                        ab = ab.Next(),lb = lb.Next())
                {
                    var col = lb.getValue();
                    var ai = ab.getValue().val;
                    var u = ai.uid;
                    d=(d==null)?new SDict(k, ai):d.Add(k,ai);
                    c=(c==null)?new SDict(k, col.val):c.Add(k,col.val);
                    var n = f.db.role.uids.get(u);
                    nms = (nms==null)?new SDict(n,u):nms.Add(n,u);
                    k++;
                }
            }
            var rd = right.getDisplay();
            if (rd!=null && right.cpos!=null)
            {
                var ab = rd.First();
                for (var rb = right.cpos.First(); ab!=null && rb != null; 
                        ab=ab.Next(),rb = rb.Next())
                {
                    var col = rb.getValue();
                    var ai = ab.getValue().val;
                    var u = ai.uid;
                    var n = f.db.role.uids.get(u);
                    if (joinType==JoinType.Natural && nms!=null && nms.Contains(n))
                        continue;
                    if (uses!=null && uses.Contains(u))
                        continue;
                    d=(d==null)?new SDict(k, ai):d.Add(k,ai);
                    c=(c==null)?new SDict(k, col.val):c.Add(k,col.val);
                    k++;
                }
            }
            f.pos = st;
            return new SQuery(Types.STableExp, d, c);            
        }
        @Override
        public SDict<Long, Long> Names(SDatabase tr, SDict<Long, Long> pt)
                throws Exception
        {
            return right.Names(tr, left.Names(tr,pt));
        }        
        @Override
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
                    f.PutLong(b.getValue().key);
        }
        @Override
        public Serialisable Prepare(STransaction db, SDict<Long,Long> pt) throws Exception
        {
            SList<SExpression> os = null;
            SDict<Long,Long> us = null;
            SDict<Integer, Ident> ds = null;
            SDict<Integer, Serialisable> cs = null;
            var n = 0;
            if (ons!=null)
            for (var b = ons.First(); b != null; b = b.Next())
            {
                var v = (SExpression)b.getValue().Prepare(db, pt);
                os =(os==null)?new SList(v):os.InsertAt(v, n);
                n++;
            }
            n = 0;
            if (uses!=null)
            for (var b = uses.First(); b != null; b = b.Next())
            {
                var v = Prepare(b.getValue(),db,pt);
                us =(us==null)?new SDict(v.key,v.val):us.Add(v.key,v.val);
            }
            var lf = (SQuery)left.Prepare(db, pt);
            var rg = (SQuery)right.Prepare(db, pt);
            SDict<String, Long> lns = null;
            for (var b = lf.getDisplay().First(); b != null; b = b.Next())
            {
                var u = b.getValue().val;
                lns= (lns==null)?new SDict(u.id,u.uid):lns.Add(u.id,u.uid);
            }
            SDict<String, Long> rns = null;
            for (var b = rg.getDisplay().First(); b != null; b = b.Next())
            {
                var u = b.getValue().val;
                rns= (rns==null)?new SDict(u.id,u.uid):rns.Add(u.id,u.uid);
            }
            for (var b = lf.getDisplay().First(); b != null; b = b.Next())
            {
                var ou = b.getValue().val.uid;
                var dn = b.getValue().val.id;
                if (rns.Contains(dn) && joinType!=JoinType.Natural)
                    dn = db.Name(lf.getAlias()) + "." + dn;
                ds = (ds==null)?new SDict(n,new Ident(ou,dn)):
                        ds.Add(n,new Ident(ou,dn));
                n++;
            }
            for (var b = rg.getDisplay().First(); b != null; b = b.Next())
            {
                var ou = b.getValue().val.uid;
                var dn = b.getValue().val.id;
                if (lns.Contains(dn) && joinType!=JoinType.Natural)
                    dn = db.Name(rg.getAlias()) + "." + dn;
                ds = (ds==null)?new SDict(n,new Ident(ou,dn)):
                        ds.Add(n,new Ident(ou,dn));
                n++;
            }
            if (cpos!=null)
            for (var b = cpos.First(); b != null; b = b.Next())
            {
                var k = b.getValue().key;
                var v = b.getValue().val.Prepare(db, pt);
                cs = (cs==null)?new SDict(k,v):cs.Add(k,v);
            }
            return new SJoin((SQuery)left.Prepare(db, pt), outer, joinType,
                (SQuery)right.Prepare(db, pt), os, us, ds, cs);
        }
                @Override
        public Serialisable UpdateAliases(SDict<Long, String> uids)
        {
            var w = uids.First();
            if (w == null || w.getValue().key > -1000000)
                return this;
            SList<SExpression> os = null;
            SDict<Long,Long> us = null;
            SDict<Integer, Ident> ds = null;
            SDict<Integer, Serialisable> cs = null;
            var n = 0;
            if (ons!=null)
            for (var b = ons.First(); b != null; b = b.Next())
            {
                var v = (SExpression)b.getValue().UpdateAliases(uids);
                os =(os==null)?new SList(v):os.InsertAt(v, n);
                n++;
            }
            n = 0;
            if (uses!=null)
            for (var b = uses.First(); b != null; b = b.Next())
            {
                var v = CheckAlias(uids,b.getValue());
                us =(us==null)?new SDict(v.key,v.val):us.Add(v.key,v.val);
            }
            if(display!=null)
            for (var b=display.First();b!=null;b=b.Next())
            {
                var k = b.getValue().key;
                var v = CheckAlias(uids,b.getValue().val);
                ds = (ds==null)?new SDict(k,v):ds.Add(k, v);
            }
            if (cpos!=null)
            for (var b = cpos.First(); b != null; b = b.Next())
            {
                var k = b.getValue().key;
                var v = b.getValue().val.UpdateAliases(uids);
                cs = (cs==null)?new SDict(k,v):cs.Add(k,v);
            }
            return new SJoin((SQuery)left.UpdateAliases(uids), outer, joinType,
                (SQuery)right.UpdateAliases(uids), os, us, ds, cs);
        }

        @Override
        public Serialisable UseAliases(SDatabase db, SDict<Long, Long> ta)
        {
            SList<SExpression> os = null;
            SDict<Long,Long> us = null;
            SDict<Integer, Ident> ds = null;
            SDict<Integer, Serialisable> cs = null;
            var n = 0;
            if (ons!=null)
            for (var b = ons.First(); b != null; b = b.Next())
            {
                var v = (SExpression)b.getValue().UseAliases(db,ta);
                os =(os==null)?new SList(v):os.InsertAt(v, n);
                n++;
            }
            n = 0;
            if (uses!=null)
            for (var b = uses.First(); b != null; b = b.Next())
            {
                var v = Use(b.getValue(),ta);
                us =(us==null)?new SDict(v.key,v.val):us.Add(v.key,v.val);
            }
            if (display!=null)
            for (var b=display.First();b!=null;b=b.Next())
            {
                var k = b.getValue().key;
                var v = Use(b.getValue().val,ta);
                ds = (ds==null)?new SDict(k,v):ds.Add(k, v);
            }
            if (cpos!=null)
            for (var b = cpos.First(); b != null; b = b.Next())
            {
                var k = b.getValue().key;
                var v = b.getValue().val.UseAliases(db,ta);
                cs = (cs==null)?new SDict(k,v):cs.Add(k,v);
            }
            return new SJoin((SQuery)left.UseAliases(db,ta), outer, joinType,
                (SQuery)right.UseAliases(db,ta), os, us, ds, cs);
        }

        public static SJoin Get(Reader f) throws Exception
        {
            return new SJoin(f);
        }
        public int Compare(RowBookmark lb,RowBookmark rb)
        {
            if (ons!=null)
                for (var b = ons.First(); b != null; b = b.Next())
                {
                    var ex = (SExpression)b.getValue();
                    var c = ex.left.Lookup(lb._cx).compareTo(ex.right.Lookup(rb._cx));
                    if (c!=0)
                        return c;
                }
            if (uses!=null)
                for (var b = uses.First(); b != null; b = b.Next())
                {
                    var c = lb._cx.get(b.getValue().key).compareTo(rb._cx.get(b.getValue().val));
                    if (c != 0)
                        return c;
                }
            return 0;
        }
        @Override
        public RowSet RowSet(STransaction tr,SQuery top,
                SDict<Long,Serialisable> ags) throws Exception
        {
            var lf = left.RowSet(tr, left, ags);
            var rg = right.RowSet(lf._tr, right, ags);
            return new JoinRowSet(rg._tr, top, this, lf, rg, ags);
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
