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
        static SDatabase SysTables(SDatabase d)
        {
            try{
            d=Add(d,"_Log",new SSlot("Uid", Types.SString),
                    new SSlot("Type", Types.SString),
                    new SSlot("Desc", Types.SString),
                    new SSlot("Id",Types.SString),
                    new SSlot("Affects",Types.SString));
            d=Add(d,"_Columns",new SSlot("Table",Types.SString),
                    new SSlot("Name",Types.SString),
                new SSlot("Type",Types.SString),
                new SSlot("Constraints",Types.SInteger), 
                new SSlot("Uid", Types.SString));
            d=Add(d,"_Constraints", 
                    new SSlot("Table", Types.SString), 
                    new SSlot("Column", Types.SString),
                new SSlot("Check", Types.SString), 
                new SSlot("Expression", Types.SString));
            d=Add(d,"_Indexes", new SSlot("Table", Types.SString), 
                    new SSlot("Type", Types.SString),
                new SSlot("Cols", Types.SString), 
                new SSlot("References", Types.SString));
            d=Add(d,"_Tables",new SSlot("Name", Types.SString),
                    new SSlot("Cols", Types.SInteger),
                new SSlot("Rows", Types.SInteger),
                new SSlot("Indexes",Types.SInteger), 
                new SSlot("Uid", Types.SString));
            }
            catch(Exception e){}
            return d;

        }
        public static final long _SysUid = -0x70000000;
        public static long _uid = _SysUid;
        /// <summary>
        /// System tables are like templates: need to be virtually specialised for a db
        /// </summary>
        /// <param name="n"></param>
        SysTable()
        {
            super(Types.SSysTable,--_uid);
        }
        SysTable(SysTable t, SDict<Long, SColumn> c, SDict<Integer,Ident>d,
                SDict<Integer,Serialisable> p,SDict<Long, Serialisable> n)
        {
            super(t, c, d, p, n);
        }
        @Override
        public STable Add(SColumn c,String s) 
        {
            var id = new Ident(c.uid,s);
            return new SysTable(this,
                    (cols==null)?new SDict(c.uid,c):cols.Add(c.uid,c),
                    (display==null)?new SDict(0,id):
                            display.Add((int)display.Length, id),
                    (cpos==null)?new SDict(0,c):cpos.Add(cpos.Length,c),
                    (refs==null)?new SDict(c.uid,c):refs.Add(c.uid, c));
        }
        static SDatabase Add(SDatabase d,String n,SSlot<String,Integer> ... cs) 
                throws Exception
        {
            var st = new SysTable();
            d = d.Install(st,n,0);
            for (var i=0;i<cs.length;i++)
                d = d.Install(new SColumn(--_uid,st.uid,cs[i].val),
                        cs[i].key,0);
            return d;
        }
        @Override
        public RowSet RowSet(STransaction db,SQuery top, 
                Context cx)
        {
            return new SysRows(db,this);
        }
}
