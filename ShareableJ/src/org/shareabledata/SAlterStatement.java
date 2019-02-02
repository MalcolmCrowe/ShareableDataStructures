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
public class SAlterStatement extends Serialisable {
        public final String id;
        public final String col;
        public final String name;
        public final int dataType;
        public SAlterStatement (String i, String c, String n, int d)   
        {
            super(Types.SAlterStatement);
            id = i; col = c; name = n; dataType = d;
        }
        public static STransaction Obey(STransaction tr,Reader rdr)
                throws Exception
        {
            var tn = rdr.GetString(); // table name
            var tb = (STable)tr.names.Lookup(tn);
            if (tb==null)
                throw new Exception("Table " + tn + " not found");
            var cn = rdr.GetString(); // column name or ""
            var nm = rdr.GetString(); // new name
            var dt = rdr.ReadByte();
            if (cn.length() == 0)
                return (STransaction)tr.Install(new SAlter(tr, nm, Types.STable, tb.uid, 0), tr.curpos);
            else if (dt == Types.Serialisable)
            {
                var s = (SSelector)tb.names.Lookup(cn);
                if (s==null)
                    throw new Exception("Column " + cn + " not found");
                return(STransaction)tr.Install(
                        new SAlter(tr, nm, Types.SColumn, tb.uid,s.uid), tr.curpos);
            }
            else 
                return (STransaction)tr.Install(new SColumn(tr, nm, dt, tb.uid),tr.curpos);
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.PutString(id);
            f.PutString(col);
            f.PutString(name);
            f.WriteByte((byte)dataType);
        }
        public static SAlterStatement Get(Reader f)
        {
            var id = f.GetString();
            var col = f.GetString();
            var name = f.GetString();
            return new SAlterStatement(id, col, name, f.ReadByte());
        }
        @Override
        public String toString()
        {
            var sb = new StringBuilder("Alter table ");
            sb.append(id);
            if (col.length() > 0)
            {
                sb.append(" alter "); sb.append(col);
            }
            else
                sb.append(" add ");
            sb.append(name);
            sb.append(' ');
            sb.append(DataTypeName(dataType));
            return sb.toString();
        }    
}
