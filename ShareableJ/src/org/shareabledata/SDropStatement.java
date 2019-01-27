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
public class SDropStatement extends Serialisable {
    public final String drop;
    public final String table;
    public SDropStatement(String d,String t)
    {  super(Types.SDropStatement);
        drop = d; table = t; }
    public static STransaction Obey(STransaction tr, Reader rdr) throws Exception
    {
        var nm = rdr.GetString(); // object name
        var pt = tr.names.Lookup(nm);
        if (pt==null)
            throw new Exception("Object " + nm + " not found");
        var cn = rdr.GetString();
        SDrop d;
        if (cn.length()==0)
            d = new SDrop(tr, pt.uid, -1);
        else {
            var s = (SSelector)((STable)pt).names.Lookup(cn);
            if (s==null)
                throw new Exception("Column " + cn + " not found");
            d= new SDrop(tr,s.uid,pt.uid);
        }
        return (STransaction)tr.Install(d, tr.curpos);
    }
    @Override
    public void Put(StreamBase f)
    {
        super.Put(f);
        f.PutString(drop);
        f.PutString(table);
    }
    @Override
    public String toString()
    {
        return "Drop " + drop + ((table.length()>0)?(" from "+table):"");
    }
   
}
