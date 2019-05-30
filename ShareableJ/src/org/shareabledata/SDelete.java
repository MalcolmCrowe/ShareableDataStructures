/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.io.*;
/**
 *
 * @author Malcolm
 */
public class SDelete extends SDbObject {
        public final long table;
        public final long delpos;
        public final SDict<Long,Serialisable> oldfields;
        public SDelete(STransaction tr, SRecord or) 
        {
            super(Types.SDelete,tr);
            table = or.table;
            delpos = or.Defpos();
            oldfields = or.fields;
        }
        public SDelete(SDelete r, Writer f) throws Exception
        {
            super(r,f);
            table = f.Fix(r.table);
            delpos = f.Fix(r.delpos);
            oldfields = r.oldfields;
            f.PutLong(table);
            f.PutLong(delpos);
            f.PutInt(oldfields.Length);
            for (var b = oldfields.First(); b != null; b = b.Next())
            {
                f.PutLong(b.getValue().key);
                b.getValue().val.Put(f);
            }
        }
        SDelete(ReaderBase f) throws Exception
        {
            super(Types.SDelete,f);
            table = f.GetLong();
            delpos = f.GetLong();
            SDict<Long,Serialisable> ofs = null;
             if (!(f instanceof SocketReader))
            {
                var n = f.GetInt();
                for (var i = 0; i < n; i++)
                {
                    var u = f.GetLong();
                    var s = f._Get();
                    ofs = (ofs==null)?new SDict(u, s):ofs.Add(u,s);
                }
            }
            oldfields = ofs;           
        }
        public static SDelete Get(SDatabase d, ReaderBase f) throws Exception
        {
            return new SDelete(f);
        }
        public void CheckConstraints(SDatabase db,STable st) throws Exception
        {
            for (var b = st.indexes.First(); b != null; b = b.Next())
            {
                var px = (SIndex)db.objects.get(b.getValue().key);
                if (!px.primary)
                    continue;
                var k = px.Key(oldfields, px.cols);
                for (var ob = db.objects.PositionAt(0L); ob != null; ob = ob.Next()) // don't bother with system tables
                {    
                    var ov = ob.getValue().val;
                    if(ov.type==Types.STable)
                    {
                        var ot = (STable)ov;
                        for (var ox = ot.indexes.First(); ox != null; ox = ox.Next())
                        {
                            var x = (SIndex)db.objects.get(ox.getValue().key);
                            if (x.references == table && x.rows.Contains(k))
                                throw new Exception("Referential constraint: illegal delete");
                        }
                    }
                }
            }            
        }
        @Override
        public boolean Check(SDict<Long, Boolean> rdC)
        {
            return (rdC!=null) && (rdC.Contains(delpos) || rdC.Contains(table));
        }
        @Override
        public boolean Conflicts(SDatabase db,STransaction tr,Serialisable that)
        { 
            switch(that.type)
            {
                case Types.SUpdate:
                    return ((SUpdate)that).Defpos() == delpos;
                case Types.SRecord:
                    return ((SRecord)that).Defpos() == delpos;
            }
            return false;
        }
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("Delete ");
            sb.append(Uid());
            sb.append(" of "); sb.append(SDbObject._Uid(delpos));
            sb.append("["); sb.append(SDbObject._Uid(table)); sb.append("]");
            return sb.toString();
        }
}
