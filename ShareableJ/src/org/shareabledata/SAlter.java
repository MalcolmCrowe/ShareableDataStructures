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
public class SAlter extends SDbObject {
        public final long defpos;
        public final long parent;
        public final String name;
        public final int dataType;
        public SAlter(STransaction tr,String n,int d,long o,long p)
        {
            super(Types.SAlter,tr);
            defpos = o;  
            name = n; 
            dataType = d; 
            parent = p;
        }
        SAlter(StreamBase f) throws Exception
        {
            super(Types.SAlter,f);
            defpos = f.GetLong();
            parent = f.GetLong(); //may be -1
            name = f.GetString();
            dataType = f.ReadByte();
        }
        public SAlter(SAlter a,AStream f) throws Exception
        {
            super(a,f);
            name = a.name;
            dataType = a.dataType;
            defpos = f.Fix(a.defpos);
            parent = f.Fix(a.parent);
            f.PutLong(defpos);
            f.PutLong(parent);
            f.PutString(name);
            f.WriteByte((byte)dataType);
        }
        public static SAlter Get(StreamBase f) throws Exception
        {
            return new SAlter(f);
        }
        public boolean Conflicts(Serialisable that)
        {
            switch(that.type)
            {
                case Types.SAlter:
                    var a = (SAlter)that;
                    return a.defpos == defpos;
                case Types.SDrop:
                    var d = (SDrop)that;
                    return d.drpos == defpos || d.drpos == parent;
            }
            return false;
        }
        public String ToString()
        {
            return "Alter " + defpos + ((parent!=0)?"":(" of "+parent)) 
                + name + " " + dataType;
        }
}
