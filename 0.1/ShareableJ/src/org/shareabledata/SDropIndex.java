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
public class SDropIndex extends Serialisable {
    public final long table;
     public final SList<Long> key;
     public SDropIndex(long tb,SList<Long> k)
     {
         super(Types.SDropIndex);
         table = tb; key = k;
     }
     public SDropIndex(ReaderBase f) throws Exception
     {
         super(Types.SDrop, f);
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
    @Override
     public Serialisable Prepare(STransaction db, SDict<Long, Long> pt)
             throws Exception
     {
         var tb = (STable)db.objects.get(table);
         for (var b = tb.indexes.First(); b != null; b = b.Next())
         {
             var x = (SIndex)db.objects.get(b.getValue().key);
             if (x.cols.Length != key.Length)
                 continue;
             var kb = key.First();
             var ma = true;
             for (var xb = x.cols.First(); ma && xb != null && kb != null;
                 xb = xb.Next(), kb = kb.Next())
                 ma = xb.getValue() == kb.getValue();
             if (ma)
                 return new SDrop(x.uid,-1,"");
         }
         throw new Exception("No such table constraint");
     }    
}
