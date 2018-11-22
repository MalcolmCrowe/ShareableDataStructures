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
public class SColumn extends SSelector {
        public final int dataType;
        public final long table;
        public SColumn(String n,int d)
        {
            super(Types.SColumn,n,-1);
            dataType = d; table = -1;
        }
                /// <summary>
        /// For system column
        /// </summary>
        /// <param name="n"></param>
        /// <param name="t"></param>
        /// <param name="u"> will be negative</param>
        public SColumn(String n,int t,long u)
        {
            super(Types.SColumn,n,u);
            dataType = t; table = -1;
        }
        public SColumn(STransaction tr,String n, int t, long tbl)
        {
            super(Types.SColumn,n,tr);
            dataType = t; table = tbl;
        }
        public SColumn(SColumn c,String n,int d)
        {
            super(c,n);
            dataType = d; table = c.table;
        }
        SColumn(Reader f) throws Exception
        {
            super(Types.SColumn,f);
            dataType = f.ReadByte();
            table = f.GetLong();
        }
        public SColumn(SColumn c,AStream f) throws Exception
        {
            super(c,f);
            dataType = c.dataType;
            table = f.Fix(c.table);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
        }
        public static SColumn Get(Reader f) throws Exception
        {
            return new SColumn(f);
        }
        public boolean Conflicts(Serialisable that)
        {
            switch (that.type)
            {
                case Types.SColumn:
                    {
                        SColumn c = (SColumn)that;
                        return c.table == table && c.name.compareTo(name) == 0;
                    }
                    case Types.SDrop:
                    {
                        var d = (SDrop)that;
                        return d.drpos == table;
                    }
            }
            return false;
        }
        public SSelector Lookup(SQuery qry)
        {
            return qry.names.Lookup(name);
        }
        public String toString()
        {
            return super.toString() +"(" +STransaction.Uid(table)+ ")"+ name + " (" + Types.ToString(dataType)+")";
        }
}
