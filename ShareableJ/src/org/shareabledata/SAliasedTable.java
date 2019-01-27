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
public class SAliasedTable extends STable {
        public final STable table;
        public final String alias;
        public SAliasedTable(STable tb,String a) 
        {
            super(Types.SAliasedTable, tb);
            table = tb;
            alias = a;
        }
        @Override
        public void Put(StreamBase f)
        {
            super.Put(f);
            f.PutString(alias);
        }
        @Override
        public Serialisable Lookup(String a)
        {
            return (a.compareTo(alias)==0)?table:super.Lookup(a);
        }
        public static SAliasedTable Get(SDatabase d,Reader f) throws Exception
        {
            return new SAliasedTable(STable.Get(d, f),f.GetString());
        }
        @Override
        public RowSet RowSet(STransaction tr, Context cx)
        {
            return new TableRowSet(tr, this);
        }
        @Override
        public void Append(SDatabase db,StringBuilder sb)
        {
            table.Append(db,sb);
            sb.append(" "); sb.append(alias);
        }
        @Override
        public String getAlias(){ return alias; }
        @Override
        public SDict<Integer, String> getDisplay() 
        {
            return (display==null)? table.getDisplay():display;
        }    
}
