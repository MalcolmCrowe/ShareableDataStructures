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
        public final SRecord oldrec;
        @Override
        public long Defpos() { return defpos; }
        public SUpdate(STransaction tr,SRecord r,SDict<Long,Serialisable> u)
                throws Exception
        {
            super(Types.SUpdate,tr,r.table,_Merge(tr,r,u));
            defpos = r.Defpos();
            oldrec = r;
        }
        public SUpdate(SDatabase db,SUpdate r, Writer f)throws Exception
        {
            super(db,r,f);
            defpos = r.defpos;
            oldrec = r.oldrec;
            f.PutLong(defpos);
        }
        SUpdate(ReaderBase f) throws Exception
        {
            super(Types.SUpdate,f);
            defpos = f.GetLong();
            oldrec = (f instanceof Reader)?f.db.Get(defpos):null;
        }
        static SDict<Long,Serialisable> _Merge(STransaction tr,SRecord r,
            SDict<Long,Serialisable> us)
        {
            var tb = (STable)tr.objects.Lookup(r.table);
            SDict<Long, Serialisable> u = null;
            if (us!=null)
            for (var b=us.First();b!=null;b=b.Next())
            {
                var k = b.getValue().key;
                var v = b.getValue().val;
                u =(u==null)?new SDict(k,v):u.Add(k,v);
            }
            return r.fields.Merge(u);
        }
        public Serialisable Commit(STransaction tr,Writer f) throws Exception
        {
            return new SUpdate(tr,this, f);
        }
        public static SRecord Get(ReaderBase f) throws Exception
        {
            return new SUpdate(f);
        }
        @Override
        public void CheckConstraints(SDatabase db, STable st) throws Exception
        {
            var cx = Context.New(fields,Context.Empty);
            for (var b = st.cols.First(); b != null; b = b.Next())
                for (var c = b.getValue().val.constraints.First(); c != null; c = c.Next())
                    switch (c.getValue().key)
                    {
                        case "CHECK":
                            if (c.getValue().val.Lookup(db, cx) != SBoolean.True)
                                throw new Exception("Check condition fails");
                            break;
                    }
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var x = (SIndex)db.objects.get(b.getValue().key);
                var ok = x.Key(oldrec, x.cols);
                var uk = x.Key(this, x.cols);
                x.Check(db, this, ok.compareTo(uk) == 0);
            }
        }
        @Override
        public boolean Conflicts(SDatabase db,STransaction tr,Serialisable that)
        {
            switch (that.type)
            {
                case Types.SUpdate:
                    return ((SUpdate)that).Defpos() == Defpos();
            }
            return super.Conflicts(db,tr,that);
        }
        public String toString()
        {
            StringBuilder sb = new StringBuilder("Update ");
            sb.append(Uid());
            sb.append(" of "); sb.append(SDbObject._Uid(defpos));
            Append(null,sb);
            return sb.toString();
        }
        @Override
        public long getAffects() { return defpos; }
}
