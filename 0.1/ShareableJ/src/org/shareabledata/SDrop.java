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
public class SDrop extends SDbObject {
        public final long drpos;
        public final long parent;
        public final String detail;
        public SDrop(STransaction tr,long d,long p,String s)
        {
            super(Types.SDrop,tr);
            drpos = d; parent = p;
            detail = s;
        }
        public SDrop(long d,long p,String s)
        {
            super(Types.SDrop);
            drpos = d; parent = p;
            detail = s;
        }
        SDrop(ReaderBase f) throws Exception
        {
            super(Types.SDrop,f);
            drpos = f.GetLong();
            parent = f.GetLong();
            detail = f.GetString();
        }
        public SDrop(SDrop d,Writer f) throws Exception
        {
            super(d,f);
            drpos = f.Fix(d.drpos);
            parent = f.Fix(d.parent);
            detail = d.detail;
            f.PutLong(drpos);
            f.PutLong(parent);
            f.PutString(detail);
        }
        public static SDrop Get(ReaderBase f) throws Exception
        {
            return new SDrop(f);
        }
        public boolean Conflicts(Serialisable that)
        {
            switch(that.type)
            {
                case Types.SDrop:
                    {
                        var d = (SDrop)that;
                        return (d.drpos == drpos && d.parent==parent) || d.drpos==parent || d.parent==drpos;
                    }
                case Types.SColumn:
                    {
                        var c = (SColumn)that;
                        return c.table == drpos || c.uid == drpos;
                    }
                case Types.SAlter:
                    {
                        var a = (SAlter)that;
                        return a.defpos == drpos || a.col == drpos;
                    }
            }
            return false;
        }
        @Override
        public String toString()
        {
            var sb = new StringBuilder();
            sb.append("Drop ");
            sb.append("" + drpos);
            sb.append((parent!=0)?"":(" of "+parent));
            sb.append(detail);
            return sb.toString();
        }
        @Override
        public long getAffects() { return drpos; }
}
