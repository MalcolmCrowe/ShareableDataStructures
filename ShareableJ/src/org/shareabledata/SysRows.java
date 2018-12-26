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
public class SysRows extends RowSet {

    public final SysTable tb;
    public final AStream fs;

    SysRows(SDatabase d, SysTable t) {
        super(d, t);
        fs = d.File();
        tb = t;
    }

    @Override
    public Bookmark<Serialisable> First() {
        try {
            var rdr = new Reader(fs, 0);
            switch (tb.name) {
                case "_Log": {
                    var s = rdr._Get(_db);
                    return (s == null) ? null
                            : new LogBookmark(this, 0, s, rdr.getPosition(), 0);
                }
                case "_Table": {
                for (var b = _db.objects.First(); b != null; b = b.Next())
                {
                    var tb = b.getValue().val;
                    if (tb instanceof STable)
                        return new TablesBookmark(this, b, 0);
                }
                return null;                    
                }
            }
        } catch (Exception e) {
        }
        return null;
    }
}
