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
public class SColumn extends SDbObject {
        public final String name;
        public final int dataType;
        public final long table;
        public SColumn(String n,int d)
        {
            super(Types.SColumn);
            name = n; dataType = d; table = -1;
        }
                /// <summary>
        /// For system column
        /// </summary>
        /// <param name="n"></param>
        /// <param name="t"></param>
        /// <param name="u"> will be negative</param>
        public SColumn(String n,int t,long u)
        {
            super(Types.SColumn,u);
            name = n; dataType = t; table = -1;
        }
        public SColumn(STransaction tr,String n, int t, long tbl)
        {
            super(Types.SColumn,tr);
            name = n; dataType = t; table = tbl;
        }
        public SColumn(SColumn c,String n,int d)
        {
            super(c);
            name = n; dataType = d; table = c.table;
        }
        SColumn(StreamBase f) throws Exception
        {
            super(Types.SColumn,f);
            name = f.GetString();
            dataType = f.ReadByte();
            table = f.GetLong();
        }
        public SColumn(SColumn c,AStream f) throws Exception
        {
            super(c,f);
            name = c.name;
            dataType = c.dataType;
            table = f.Fix(c.table);
            f.PutString(name);
            f.WriteByte((byte)dataType);
            f.PutLong(table);
        }
        public static SColumn Get(AStream f) throws Exception
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
        public String toString()
        {
            return super.toString() +"(" +STransaction.Uid(table)+ ")"+ name + " (" + Types.ToString(dataType)+")";
        }
}
