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
public class SCreateTable extends Serialisable {
    public final String tdef;
    public final SList<SColumn> coldefs;
    public SCreateTable(String tn,SList<SColumn> c)
    { 
        super(Types.SCreateTable);
        tdef = tn; coldefs = c; 
    }
    protected SCreateTable(Reader f)
    {
        super(Types.SCreateTable,f);
        tdef = f.GetString();
        var n = f.GetInt();
        SList<SColumn> c = null;
        for (var i = 0; i < n; i++)
        {
            var co = SColumn.Get(f);
            var col = new SColumn(co,co.name,f.ReadByte());
            c=(c==null)?new SList(col):c.InsertAt(col, i);
        }
        coldefs = c;
    }
    public static SCreateTable Get(Reader f)
    {
        return new SCreateTable(f);
    }
    @Override
    public void Put(StreamBase f)
    {
        super.Put(f);
        f.PutString(tdef);
        f.PutInt(coldefs.Length);
        for (var b = coldefs.First(); b != null; b = b.Next())
        {
            f.PutString(b.getValue().name);
            f.WriteByte((byte)b.getValue().dataType);
        }
    }
    @Override
    public String toString()
    {
        return "CreateTable "+tdef+" "+coldefs.toString();
    }
    
}
