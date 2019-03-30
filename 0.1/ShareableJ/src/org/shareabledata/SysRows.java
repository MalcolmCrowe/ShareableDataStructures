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
public class SysRows extends RowSet{
        public final SysTable tb;
        SysRows(STransaction tr, SysTable t) 
        {
            super(tr, t, null);
            tb = t; 
        }
        public SRow _Row(Serialisable... vals) throws Exception
        {
            var r = new SRow();
            int j = 0;
            for (var b = tb.cpos.First(); b != null; b = b.Next())
            {
                var k = (SColumn)b.getValue().val;
                r = r.Add(new Ident(k.uid,SDatabase._system.Name(k.uid)),vals[j++]);
            }
                        // Serialisable.New(((SColumn)b.Value.val).dataType, vals[j++]));
            return r;
        }
    @Override
    public Bookmark<Serialisable> First() {
        try {
            var rdr = new Reader(_tr, 0);
            switch (_tr.Name(tb.uid)) {
                case "_Log": return LogBookmark.New(this);
                case "_Tables": return TablesBookmark.New(this);
                case "_Columns": return ColumnsBookmark.New(this);
                case "_Constraints": return ConstraintsBookmark.New(this);
                case "_Indexes": return IndexesBookmark.New(this);
            }
        } catch (Exception e) {
        }
        return null;
    }
}
