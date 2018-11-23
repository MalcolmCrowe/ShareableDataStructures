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
public class SUpdate extends SRecord {
        public final long defpos;
        public long Defpos() { return defpos; }
        public SUpdate(STransaction tr,SRecord r,SDict<Long,Serialisable> u)
        {
            super(tr,r.table,r.fields.Merge(u));
            defpos = r.Defpos();
        }
        public SUpdate(SDatabase db,SUpdate r, AStream f) throws Exception
        {
            super(db,r,f);
            defpos = f.Fix(r.Defpos());
            f.PutLong(defpos);
        }
        SUpdate(SDatabase d,Reader f) throws Exception
        {
            super(d,f);
            defpos = f.GetLong();
        }
        public Serialisable Commit(STransaction tr,AStream f) throws Exception
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
        public String ToString()
        {
            StringBuilder sb = new StringBuilder("Update ");
            sb.append(Uid());
            sb.append(" of "); sb.append(STransaction.Uid(defpos));
            Append(sb);
            return sb.toString();
        }
}
