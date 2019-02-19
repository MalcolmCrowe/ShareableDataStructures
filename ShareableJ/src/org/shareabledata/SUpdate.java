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
public class SUpdate extends SRecord {
        public final long defpos;
        @Override
        public long Defpos() { return defpos; }
        public SUpdate(STransaction tr,SRecord r,SDict<String,Serialisable> u)
        {
            super(Types.SUpdate,tr,r.table,_Merge(tr,r,u));
            defpos = r.Defpos();
        }
        public SUpdate(SDatabase db,SUpdate r, AStream f)
        {
            super(db,r,f);
            defpos = f.Fix(r.Defpos());
            f.PutLong(defpos);
        }
        SUpdate(SDatabase d,Reader f) throws Exception
        {
            super(Types.SUpdate,d,f);
            defpos = f.GetLong();
        }
        static SDict<Long,Serialisable> _Merge(STransaction tr,SRecord r,
            SDict<String,Serialisable> us)
        {
            var tb = (STable)tr.objects.Lookup(r.table);
            SDict<Long, Serialisable> u = null;
            if (us!=null)
            for (var b=us.First();b!=null;b=b.Next())
            {
                var k = ((SColumn)(tb.names.get(b.getValue().key))).uid;
                var v = b.getValue().val;
                u =(u==null)?new SDict(k,v):u.Add(k,v);
            }
            return r.fields.Merge(u);
        }
        public Serialisable Commit(STransaction tr,AStream f) 
        {
            return new SUpdate(tr,this, f);
        }
        public static SRecord Get(SDatabase d,Reader f) throws Exception
        {
            return new SUpdate(d,f);
        }
        public boolean Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.SUpdate:
                    return ((SUpdate)that).Defpos() == Defpos();
            }
            return super.Conflicts(that);
        }
        public String toString()
        {
            StringBuilder sb = new StringBuilder("Update ");
            sb.append(Uid());
            sb.append(" of "); sb.append(SDbObject._Uid(defpos));
            Append(null,sb);
            return sb.toString();
        }
}
