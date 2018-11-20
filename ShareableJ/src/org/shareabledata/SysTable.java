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
public class SysTable extends STable {
        public static long _uid = 0;
        public static SDict<String, SysTable> system = null;
        /// <summary>
        /// System tables are like templates: need to be virtually specialised for a db
        /// </summary>
        /// <param name="n"></param>
        SysTable(String  n)
        {
            super(n,--_uid);
        }
        SysTable(SysTable t, SDict<Long, SColumn> c, SList<SColumn> p, 
                SDict<String, SColumn> n)
        {
            super(t, c, p, n);
        }
        /// <summary>
        /// Set the database for the system table
        /// </summary>
        /// <param name="d"></param>
        /// <param name="t"></param>
        public SysTable(SDatabase d, SysTable t)
        {
            super(t,new SysRows(d,t));
        }
        static
        {
            try {
            var t = new SysTable("_Log");
            t = t.Add("Uid", Types.SString);
            t = t.Add("Type", Types.SInteger);
            t = t.Add("Desc", Types.SString);
            system = system.Add(t.name, t);
            } catch(Exception e) {}
        }
        public STable Add(SColumn c) throws Exception
        {
            return new SysTable(this, cols.Add(c.uid, c), cpos.InsertAt(c, cpos.Length),
                names.Add(c.name, c));
        }
        SysTable Add(String n,int t) throws Exception
        {
            return (SysTable)Add(new SysColumn(n, t));
        }
}
