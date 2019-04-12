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
        public SDelete(STransaction tr, long t, long p) 
        {
            super(Types.SDelete,tr);
            table = t;
            delpos = p;
        }
        public SDelete(SDelete r, AStream f) throws Exception
        {
            super(r,f);
            table = f.Fix(r.table);
            delpos = f.Fix(r.delpos);
            f.PutLong(table);
            f.PutLong(delpos);
        }
        SDelete(Reader f) throws Exception
        {
            super(Types.SDelete,f);
            table = f.GetLong();
            delpos = f.GetLong();
        }
        public static SDelete Get(SDatabase d, Reader f) throws Exception
        {
            return new SDelete(f);
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
