/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author 66668214
 */
public class SDeleteSearch extends Serialisable {
        public final SQuery qry;
        public SDeleteSearch(SQuery q)
        { 
            super(Types.SDeleteSearch);
            qry = q; 
        }
        public STransaction Obey(STransaction tr,Context cx) throws Exception
        {
            for (var b = (RowBookmark)qry.RowSet(tr,cx).First(); b != null; 
                    b = (RowBookmark)b.Next())
            {
                var rc = b._ob.rec; // not null
                tr = (STransaction)tr.Install(new SDelete(tr, rc.table, rc.uid),tr.curpos); 
            }
            return tr;
        }
        public static SDeleteSearch Get(SDatabase db,Reader f) throws Exception
        {
            return new SDeleteSearch((SQuery)f._Get(db));
        }
        public void Put(StreamBase f)
        {
            super.Put(f);
            qry.Put(f);
        }
        public String toString()
        {
            var sb = new StringBuilder("Delete ");
            qry.Append(null,sb);
            return sb.toString();
        }
}
