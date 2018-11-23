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
        public SDrop(STransaction tr,long d,long p)
        {
            super(Types.SDrop,tr);
            drpos = d; parent = p;
        }
        SDrop(Reader f) throws Exception
        {
            super(Types.SDrop,f);
            drpos = f.GetLong();
            parent = f.GetLong();
        }
        public SDrop(SDrop d,AStream f) throws Exception
        {
            super(d,f);
            drpos = f.Fix(d.drpos);
            parent = f.Fix(d.parent);
            f.PutLong(drpos);
            f.PutLong(parent);
        }
        public static SDrop Get(Reader f) throws Exception
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
                        return a.defpos == drpos || a.parent == drpos;
                    }
            }
            return false;
        }
        public String toString()
        {
            return "Drop " + drpos + ((parent!=0)?"":(" of "+parent));
        }
}
