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
        public final long col;
        public final String name;
        public final int dataType;
        public final int seq;
        public final SDict<String,SFunction> constraints;
        public SAlter(STransaction tr,String n,int d,long o,long p,
                int sq,SDict<String,SFunction> cs)
        {
            super(Types.SAlter,tr);
            defpos = o;  
            name = n; 
            dataType = d; 
            col = p;
            seq = sq;
            constraints = cs;
        }
        public SAlter(String n,int d,long o,long p,
            int sq,SDict<String,SFunction> cs)
        {
            super(Types.SAlter);
            defpos = o;  
            name = n; 
            dataType = d; 
            col = p;
            seq = sq;
            constraints = cs;
        }
        SAlter(ReaderBase f) throws Exception
        {
            super(Types.SAlter,f);
            defpos = f.GetLong();
            col = f.GetLong(); //may be -1
            name = f.GetString();
            dataType = f.ReadByte();
            seq = f.GetInt();
            SDict<String,SFunction> cs = null;
            var n = f.GetInt();
            for (var i=0;i<n;i++)
            {
                var id = f.GetString();
                var c = f._Get();
                if (c==null || c.type!=Types.SFunction)
                    throw new Exception("Constraint expected");
                var fn = (SFunction)c;
                cs =(cs==null)?new SDict(id,fn):cs.Add(id,fn);
            }
            constraints = cs;
        }
        public SAlter(SAlter a,Writer f) throws Exception
        {
            super(a,f);
            name = a.name;
            dataType = a.dataType;
            seq = a.seq;
            defpos = f.Fix(a.defpos);
            col = f.Fix(a.col);
            f.PutLong(defpos);
            f.PutLong(col);
            f.PutString(name);
            f.WriteByte((byte)dataType);
            f.PutInt(seq);
            SDict<String,SFunction> cs = null;
            f.PutInt((a.constraints==null)?0:a.constraints.Length);
            if (a.constraints!=null)
                for (var b=a.constraints.First();b!=null;b=b.Next())
                {
                    var k = b.getValue().key;
                    var cf = (SFunction)b.getValue().val.Fix(f);
                    f.PutString(k);
                    cs = (cs==null)?new SDict(k,cf):cs.Add(k, cf);
                }
            constraints = cs;
        }
        public static SAlter Get(ReaderBase f) throws Exception
        {
            return new SAlter(f);
        }
        public STransaction Obey(STransaction tr, Context cx) throws Exception
        {
             return (STransaction)tr.Install(this, tr.curpos);
        }
 
        public boolean Conflicts(SDatabase db,STransaction tr,Serialisable that)
        {
            switch(that.type)
            {
                case Types.SAlter:
                    var a = (SAlter)that;
                    return a.defpos == defpos;
                case Types.SDrop:
                    var d = (SDrop)that;
                    return d.drpos == defpos || d.drpos == col;
            }
            return false;
        }
        @Override
        public String toString()
        {
            var sb = new StringBuilder();
            sb.append("Alter ");
            sb.append(_Uid(defpos));
            sb.append((col == -1) ? "" : (" column " + _Uid(col)));
            if (name.length() != 0)
            {
                sb.append(" TO ");
                sb.append(name);
            }
            if (seq!= -1)
                sb.append(" TO "+seq);
            sb.append((dataType!=Types.Serialisable)?(" " + DataTypeName(dataType)):"");
            for(var b=constraints.First();b!=null;b=b.Next())
            {
                sb.append(" ");sb.append(b.getValue().key);
                b.getValue().val.Append(sb);
            }
            return sb.toString();
        }
        @Override
        public long getAffects() { return defpos; }
}
