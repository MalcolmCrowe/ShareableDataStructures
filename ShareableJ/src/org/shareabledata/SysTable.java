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
        static void Init()
        {
            try {
            SDict<String,SysTable> s = null;
            var t = new SysTable("_Log");
            t = t.Add("Uid", Types.SString);
            t = t.Add("Type", Types.SInteger);
            t = t.Add("Desc", Types.SString);
            s = (s==null)?new SDict<>(t.name,t):s.Add(t.name, t);
            system = s;
            } catch(Exception e) 
            {
                System.out.println(e.getMessage());
            }
        }
        public static long _uid = 0;
        public static SDict<String, SysTable> system;
        /// <summary>
        /// System tables are like templates: need to be virtually specialised for a db
        /// </summary>
        /// <param name="n"></param>
        SysTable(String  n)
        {
            super(n,--_uid);
        }
        SysTable(SysTable t, SDict<Long, SSelector> c, SList<SSelector> p, 
                SDict<String, SSelector> n)
        {
            super(t, c, p, n);
        }
        @Override
        public STable Add(SColumn c) throws Exception
        {
            return new SysTable(this,
                    (cols==null)?new SDict<>(c.uid,c):cols.Add(c.uid,c),
                    (cpos==null)?new SList<>(c):cpos.InsertAt(c, cpos.Length),
                    (names==null)?new SDict<>(c.name,c):names.Add(c.name, c));
        }
        SysTable Add(String n,int t) throws Exception
        {
            return (SysTable)Add(new SysColumn(n, t));
        }
        @Override
        public RowSet RowSet(SDatabase db)
        {
            return new SysRows(db,this);
        }
}
