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
public class SCreateIndex extends Serialisable {
        public final SString index;
        public final SString table;
        public final SBoolean primary;
        public final Serialisable references; // SString or Null
        public final SList<SSelector> cols;
        public SCreateIndex(SString i,SString t,SBoolean b,Serialisable r,SList<SSelector>c)
        { 
            super(Types.SCreateIndex);
            index = i; table = t; primary = b; references = r; cols = c; 
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.PutString(table.str);
            var refer = (references instanceof SString)?(SString)references:null;
            f.WriteByte((byte)((refer!=null&&refer.str.length()>0)?2 : 
                    primary.sbool ? 0 : 1));
            f.PutString((refer==null)?"":refer.str);
            f.PutInt(cols.Length);
            for (var b = cols.First(); b != null; b = b.Next())
                f.PutString(b.getValue().name);
        }
        public String toString()
        {
            var sb = new StringBuilder("Create ");
            if (primary.sbool)
                sb.append("primary ");
            sb.append("index ");
            sb.append(index.str); sb.append(" for ");
            sb.append(table.str);
            var refer = (SString)references;
            if (refer!=null && refer.str.length()>0)
            {
                sb.append("references ");sb.append(refer.str); 
            }
            sb.append('(');
            var cm = "";
            for (var b=cols.First();b!=null;b=b.Next())
            {
                sb.append(cm); cm = ",";
                sb.append(b.getValue().name);
            }
            sb.append(')');
            return sb.toString();
        }
    
}
