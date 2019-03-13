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
        public final Bookmark<SSlot<Long,SDbObject>> _bmk;
        TablesBookmark(SysRows rs, Bookmark<SSlot<Long,SDbObject>> bmk, 
                STable tb,int p)
        {
            super(rs,rs._Row(new SString(tb.name),
                    new SInteger(tb.cpos.Length),
                    new SInteger(tb.rows.Length)),p);
            _srs = rs; _bmk = bmk;
        }
        @Override
        public Bookmark<Serialisable> Next()
        {
            for (var b = _bmk.Next(); b != null; b = b.Next())
            {
                var tb = b.getValue().val;
                if (tb instanceof STable)
                    return new TablesBookmark(_srs, b, (STable)tb, 0);
            }
            return null;
        }
        @Override
        public void Append(StringBuilder sb)
        {
            var t = (STable)_bmk.getValue().val;
            sb.append("{ Name: '");sb.append(t.name);
            sb.append("', Cols: ");sb.append(t.cols.Length);
            sb.append(", Rows: ");sb.append(t.rows.Length); sb.append("}");
        }    
}
