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
        public SUpdate(STransaction tr,SRecord r)
        {
            super(tr,r.table,r.fields);
            defpos = r.Defpos();
        }
        public long Defpos() { return defpos; }
        public SUpdate(SDatabase db,SUpdate r, AStream f) throws Exception
        {
            super(db,r,f);
            defpos = f.Fix(r.defpos);
            f.PutLong(defpos);
        }
        SUpdate(SDatabase d,AStream f) throws Exception
        {
            super(d,f);
            defpos = f.GetLong();
        }
        public Serialisable Commit(STransaction tr,AStream f) throws Exception
        {
            return new SUpdate(tr,this, f);
        }
        public static SRecord Get(SDatabase d,AStream f) throws Exception
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
