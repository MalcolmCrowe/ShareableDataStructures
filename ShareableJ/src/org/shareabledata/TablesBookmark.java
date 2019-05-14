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
public class TablesBookmark extends RowBookmark {
        public final SysRows _srs;
        public final STable _tb;
        public final Bookmark<SSlot<Long,SDbObject>> _bmk;
        TablesBookmark(SysRows rs, Bookmark<SSlot<Long,SDbObject>> bmk, 
                STable tb,int p) throws Exception
        {
            super(rs,_Cx(rs,rs._Row(new SString(rs._tr.Name(tb.uid)), // Name
                    new SInteger(tb.cpos.Length), // Cols
                    new SInteger((tb.rows==null)?0:tb.rows.Length),
                    new SInteger((tb.indexes==null)?0:tb.indexes.Length),
                    new SString(tb.Uid())),null),p);
            _srs = rs; _bmk = bmk; _tb=tb;
        }
        public static TablesBookmark New(SysRows rs) throws Exception
        {
            if (rs._tr.objects!=null)
             for (var b = rs._tr.objects.First(); b != null; b = b.Next())
             {
                 var tb = b.getValue().val;
                 if (tb instanceof STable)
                     return new TablesBookmark(rs, b, (STable)tb, 0);
             }
             return null;  
        }
        @Override
        public Bookmark<Serialisable> Next()
        {
            try {
            for (var b = _bmk.Next(); b != null; b = b.Next())
            {
                var tb = b.getValue().val;
                if (tb instanceof STable)
                    return new TablesBookmark(_srs, b, (STable)tb, 0);
            }
            } catch(Exception e) {}
            return null;
        }  
}
