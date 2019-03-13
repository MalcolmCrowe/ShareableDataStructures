/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author 66668214
 */
public class SUpdateSearch extends Serialisable 
{
        public final SQuery qry;
        public final SDict<String, Serialisable> assigs;
        public SUpdateSearch(SQuery q,SDict<String,Serialisable> a)
        {
            super(Types.SUpdateSearch);
            qry = q; assigs = a;
        }
        public STransaction Obey(STransaction tr,Context cx) throws Exception
        {
            for (var b = (RowBookmark)qry.RowSet(tr,qry,null,cx).First(); 
                    b != null; b = (RowBookmark)b.Next())
            {
                SDict<String, Serialisable> u = null;
                for (var c = assigs.First(); c != null; c = c.Next())
                {
                    var v = c.getValue();
                    var vl = v.val.Lookup(new Context(b,null));
                    u=(u==null)?new SDict(v.key,vl):u.Add(v.key, vl);
                }
                tr = b.Update(tr,u);
            }
            return tr;
        }
        public static SUpdateSearch Get(SDatabase db,Reader f) throws Exception
        {
            var q = (SQuery)f._Get(db);
            var n = f.GetInt();
            SDict<String, Serialisable> a = null;
            for (var i=0;i<n;i++)
            {
                var s = f.GetString();
                var qv = q.Lookup(s);
                if (qv ==null || !(qv instanceof SColumn))
                    throw new Exception("Column " + s + " not found");
                a =(a==null)?new SDict(((SColumn)qv).name,f._Get(db)):
                        a.Add(((SColumn)qv).name, f._Get(db));    
            }
            return new SUpdateSearch(q, a);
        }
        public void Put(StreamBase f) 
        {
            super.Put(f);
            qry.Put(f);
            f.PutInt(assigs.Length);
            for (var b = assigs.First(); b != null; b = b.Next())
            {
                f.PutString(b.getValue().key); b.getValue().val.Put(f);
            }
        }
        public String toString()
        {
            var sb = new StringBuilder("Update ");
            qry.Append(null,sb);
            sb.append(" set ");
            var cm = "";
            for (var b = assigs.First();b!=null;b=b.Next())
            {
                sb.append(cm); cm = ",";
                sb.append(b.getValue().key);
                sb.append('=');
                sb.append(b.getValue().val);
            }
            return sb.toString();
        }

}
