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
    public final long tdef;
    public final SList<SColumn> coldefs;
    public final SList<SIndex> constraints;
    public SCreateTable(long tn,SList<SColumn> c,SList<SIndex> cs)
    { 
        super(Types.SCreateTable);
        tdef = tn; coldefs = c; constraints = cs;
    }
    public static Serialisable Get(ReaderBase f) throws Exception
    {
        var db = f.db;
        var tn = db.role.uids.get(f.GetLong());
        if (db.role.globalNames.Contains(tn))
            throw new Exception("Table " + tn + " already exists");
        db = db.Install(new STable((STransaction)db), tn, db.curpos);
        var n = f.GetInt();
        for (var i = 0; i < n; i++)
        {
            var sc = (SColumn)f._Get();
            db = db.Install(sc, db.role.uids.get(sc.uid), db.curpos);
        }
        n = f.GetInt();
        for (var i = 0; i < n; i++)
            db = db.Install((SIndex)f._Get(), db.curpos);
        f.db = db;
        return Null;
    }
    @Override
    public void Put(WriterBase f) throws Exception
    {
        super.Put(f);
        f.PutLong(tdef);
        f.PutInt(coldefs.Length);
        for (var b = coldefs.First(); b != null; b = b.Next())
            b.getValue().PutColDef(f);
        f.PutInt((constraints==null)?0:constraints.Length);
        if (constraints!=null)
        for (var b = constraints.First(); b != null; b = b.Next())
            b.getValue().Put(f);
    }
    @Override
    public String toString()
    {
        return "CreateTable "+SDbObject._Uid(tdef)+" "+coldefs.toString();
    }
    
}
