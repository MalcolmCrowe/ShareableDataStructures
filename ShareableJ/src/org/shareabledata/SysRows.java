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
        super(d, t,null);
        fs = d.File();
        tb = t;
    }

    @Override
    public Bookmark<Serialisable> First() {
        try {
            var rdr = new Reader(fs, 0);
            switch (tb.name) {
                case "_Log": {
                    SDbObject s = (SDbObject)rdr._Get(_tr);
                    return (s == null) ? null
                            : new LogBookmark(this, 0, 
                                    _Row(new SString(SDbObject._Uid(s.uid)),
                    new SInteger((int)s.type), 
                    new SString(s.toString())), rdr.getPosition(), 0);
                }
                case "_Tables": {
                for (var b = _tr.objects.First(); b != null; b = b.Next())
                {
                    var tb = b.getValue().val;
                    if (tb instanceof STable)
                        return new TablesBookmark(this, b, (STable)tb, 0);
                }
                return null;                    
                }
            }
        } catch (Exception e) {
        }
        return null;
    }
    public SRow _Row(Serialisable... vals)
    {
        var r = new SRow();
        int j = 0;
        for (var b = tb.cpos.First(); b != null; b = b.Next())
        {
            var s = (SSelector)b.getValue().val;
            r = r.Add(s.name, vals[j++]);
        }
        return r;
    }
}
