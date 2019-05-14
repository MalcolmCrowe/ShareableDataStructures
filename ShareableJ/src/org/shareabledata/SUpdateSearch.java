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
        public final SDict<Long, Serialisable> assigs;
        public SUpdateSearch(SQuery q,SDict<Long,Serialisable> a)
        {
            super(Types.SUpdateSearch);
            qry = q; assigs = a;
        }
        @Override
        public STransaction Obey(STransaction tr,Context cx) throws Exception
        {
            for (var b = (RowBookmark)qry.RowSet(tr,qry,null).First(); 
                    b != null; b = (RowBookmark)b.Next())
            {
                SDict<Long, Serialisable> u = null;
                for (var c = assigs.First(); c != null; c = c.Next())
                {
                    var v = c.getValue();
                    var vl = v.val.Lookup(tr,b._cx);
                    u=(u==null)?new SDict(v.key,vl):u.Add(v.key, vl);
                }
                tr = b.Update(tr,u);
            }
            return tr;
        }
        public static SUpdateSearch Get(ReaderBase f) throws Exception
        {
            var q = (SQuery)f._Get();
            var n = f.GetInt();
            SDict<Long, Serialisable> a = null;
            for (var i=0;i<n;i++)
            {
                var s = f._Get();
                if (s ==null || !(s instanceof SDbObject))
                    throw new Exception("Column " + s + " not found");
                var ob = (SDbObject)s;
                var v = f._Get();
                a =(a==null)?new SDict(ob.uid,v):
                        a.Add(ob.uid, v);    
            }
            return new SUpdateSearch(q, a);
        }
        @Override
        public Serialisable Prepare(STransaction db, SDict<Long, Long> pt)
                throws Exception
        {
            SDict<Long, Serialisable> a = null;
            for (var b = assigs.First(); b != null; b = b.Next())
            {
                var k = SDbObject.Prepare(b.getValue().key, pt);
                var v = b.getValue().val.Prepare(db, pt);
                a = (a==null)?new SDict(k,v):a.Add(k,v);
            }
            var q = (SQuery)qry.Prepare(db,pt);
            return new SUpdateSearch(q,a);
        }
        @Override
        public void Put(WriterBase f) throws Exception
        {
            super.Put(f);
            qry.Put(f);
            f.PutInt(assigs.Length);
            for (var b = assigs.First(); b != null; b = b.Next())
            {
                f.WriteByte((byte)Types.SName); 
                f.PutLong(b.getValue().key); 
                b.getValue().val.Put(f);
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
