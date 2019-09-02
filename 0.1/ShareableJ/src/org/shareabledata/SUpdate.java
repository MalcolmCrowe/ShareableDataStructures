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
        public final SDict<Long,Serialisable> oldfields; // old key fields if different
        @Override
        public long Defpos() { return defpos; }
        public SUpdate(STransaction tr,SRecord r,SDict<Long,Serialisable> u)
                throws Exception
        {
            super(Types.SUpdate,tr,r.table,_Merge(tr,r,u));
            defpos = r.Defpos();
            var tb = (STable)tr.objects.get(r.table);
            SDict<Long,Serialisable> ofs = null;
            if (tb.indexes!=null)
              for (var b = tb.indexes.First(); b != null; b = b.Next())
                for (var c = ((SIndex)tr.objects.get(b.getValue().key)).cols.First(); 
                        c != null; c = c.Next())
                {
                    var ov = r.fields.get(c.getValue());
                    var nv = fields.get(c.getValue());
                    if (ov.compareTo(nv)!=0)
                        ofs = (ofs==null)?new SDict(c.getValue(), ov):
                                ofs.Add(c.getValue(), ov);
                }          
            oldfields = ofs;
        }
        public SUpdate(SDatabase db,SUpdate r, Writer f)throws Exception
        {
            super(db,r,f);
            defpos = f.Fix(r.defpos);
            oldfields = r.oldfields;
            f.PutLong(defpos);
            f.PutInt(oldfields.Length);
            for (var b=oldfields.First();b!=null;b=b.Next())
            {
                f.PutLong(b.getValue().key);
                b.getValue().val.Put(f);
            }
        }
        SUpdate(ReaderBase f) throws Exception
        {
            super(Types.SUpdate,f);
            defpos = f.GetLong();
            SDict<Long, Serialisable> ofs = null;
            if (!(f instanceof SocketReader))
            {
                var n = f.GetInt();
                for (var i = 0; i < n; i++)
                {
                    var u = f.GetLong();
                    var s = f._Get();
                    ofs = (ofs==null)?new SDict(u,s):ofs.Add(u,s);
                }
            }
            oldfields = ofs;
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
                for (var c = b.getValue().val.constraints.First(); c != null; 
                        c = c.Next())
                    switch (c.getValue().key)
                    {
                        case "CHECK":
                            if (c.getValue().val.Lookup(db, cx) != SBoolean.True)
                                throw new Exception("Check condition fails");
                            break;
                    }
            if (oldfields != null)
            {
                // Make a full list of all old key fields
                var ofs = fields; // start with the new ones
                // replace the old fields that were different
                for (var b = oldfields.First(); b != null; b = b.Next())
                    ofs = ofs.Add(b.getValue().key,b.getValue().val);
                // Now use ofs to compute the old keys
                for (var b = st.indexes.First(); b != null; b = b.Next())
                {
                    var x = (SIndex)db.objects.get(b.getValue().key);
                    var ok = x.Key(ofs, x.cols);
                    var uk = x.Key(this, x.cols);
                    x.Check(db, this, ok.compareTo(uk) == 0);
                }
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
