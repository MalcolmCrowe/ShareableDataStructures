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
public class SDropIndex extends SDbObject {
    public final long table;
     public final SList<Long> key;
     public SDropIndex(long tb,SList<Long> k)
     {
         super(Types.SDropIndex);
         table = tb; key = k;
     }
     public SDropIndex(STransaction tr,long tb,SList<Long> k)
     {
         super(Types.SDropIndex,tr);
         table = tb; key = k;
     }
     public SDropIndex(ReaderBase f) throws Exception
     {
         super(Types.SDropIndex, f);
         table = f.GetLong();
         var n = f.GetInt();
         SList<Long> k = null;
         for (var i = 0; i < n; i++)
         {
             var u = f.GetLong();
             k=(k==null)?new SList(u):k.InsertAt(u, i);
         }
         key = k;
     }
    public SDropIndex(SDatabase db,SDropIndex di,Writer f)
            throws Exception
    {
        super(di,f);
        table = f.Fix(di.table);
        f.PutLong(table);
        f.PutInt(di.key.Length);
        SList<Long> k = null;
        var i = 0;
        for (var b = di.key.First(); b != null; b = b.Next(),i++)
        {
            var u = f.Fix(b.getValue());
            k = (k==null)?new SList(u):k.InsertAt(u,i);
            f.PutLong(u);
        }
        key = k;
    }
    @Override
     public Serialisable Prepare(STransaction db, SDict<Long, Long> pt)
             throws Exception
     {
        var tn = db.role.uids.get(table);
        if (!db.role.globalNames.defines(tn))
            throw new Exception("Table " + tn + " not found");
        var tb = (STable)db.objects.get(db.role.globalNames.get(tn));
        var x = tb.FindIndex(db, key);
        if (x!=null)
        {
            for (var xb = db.objects.First(); xb != null; xb = xb.Next())
                if (xb.getValue().val instanceof SIndex)
                {
                    var rx = (SIndex)xb.getValue().val;
                    if (rx.refindex == x.uid)   
                        throw new Exception("Restricted by reference");
                }
            return new SDropIndex(db,tb.uid,x.cols);
        }
        throw new Exception("No such table constraint");
     }
    @Override
     public STransaction Obey(STransaction tr,Context cx) throws Exception
     {
         return (STransaction)tr.Install(this, tr.curpos);
     }
    @Override
    public void Put(WriterBase f) throws Exception
   {
       super.Put(f);
       f.PutLong(table);
       f.PutInt(key.Length);
       for (var b = key.First(); b != null; b = b.Next())
           f.PutLong(b.getValue());
   }
    @Override
    public String toString()
    {
        var sb = new StringBuilder("DropIndex for ");
        sb.append(_Uid(table));
        sb.append(" (");
        var cm = "";
        for (var b = key.First(); b != null; b = b.Next())
        {
            sb.append(cm); cm = ",";
            sb.append(_Uid(b.getValue()));
        }
        sb.append(")");
        return sb.toString();
    }
}
