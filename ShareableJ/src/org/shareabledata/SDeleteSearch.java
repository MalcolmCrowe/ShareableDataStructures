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
        @Override
        public Serialisable Prepare(STransaction db, SDict<Long,Long> pt) throws Exception
        {
            return new SDeleteSearch((SQuery)qry.Prepare(db, pt));
        }
        @Override
        public STransaction Obey(STransaction tr,Context cx) throws Exception
        {
            for (var b = (RowBookmark)qry.RowSet(tr,qry,null).First(); 
                b != null;b = (RowBookmark)b.Next())
                tr = b.Delete(tr);
            return tr;
        }
        public static SDeleteSearch Get(ReaderBase f) throws Exception
        {
            return new SDeleteSearch((SQuery)f._Get());
        }

        @Override
        public void Put(WriterBase f) throws Exception
        {
            super.Put(f);
            qry.Put(f);
        }
        @Override
        public String toString()
        {
            var sb = new StringBuilder("Delete ");
            qry.Append(null,sb);
            return sb.toString();
        }
}
