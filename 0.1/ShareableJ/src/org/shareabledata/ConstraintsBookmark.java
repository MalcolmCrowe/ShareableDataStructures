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
public class ConstraintsBookmark extends RowBookmark {
    public final SysRows _srs;
    public final ColumnsBookmark _cbm;
    public final SFunction _cf;
    public final String _id;
    ConstraintsBookmark(SysRows rs, ColumnsBookmark cbm, SFunction cf, 
        String id, int p) throws Exception
    {
        super(rs, _Cx(rs,rs._Row(new SString(rs._tr.Name(cbm._tbm._tb.uid)), // Table
            new SString(rs._tr.Name(cbm._sc.uid)), // Column
            new SString(Check(rs._tr,cf,id)), // Check
            new SString(cf.arg.toString())),cbm._cx), p);  //Expression
        _srs = rs; _cbm = cbm; _cf = cf; _id = id;
    }
    static String Check(STransaction tr,SFunction cf,String id)
    {
        return (cf.func == SFunction.Func.Constraint) ? id : 
            SFunction.Func.names[cf.func];
    }
    static ConstraintsBookmark New(SysRows rs) throws Exception
    {
        for (var cbm = ColumnsBookmark.New(rs); cbm != null; 
            cbm = (ColumnsBookmark)cbm.Next())
            {
                var b = cbm._sc.constraints.First();
                if (b != null)
                    return new ConstraintsBookmark(rs, cbm, 
                        b.getValue().val, b.getValue().key, 0);
            }
        return null;
    }
    @Override
    public Bookmark<Serialisable> Next()
    {
        try {
        var p = _cbm._sc.constraints.PositionAt(_id);
        if (p==null)
            return null;
        var n = p.Next();
        if (n != null)
            return new ConstraintsBookmark(_srs, _cbm, n.getValue().val, 
                    n.getValue().key,Position + 1);
        for (var cbm = (ColumnsBookmark)_cbm.Next(); cbm != null;
            cbm = (ColumnsBookmark)cbm.Next())
        {
            var b = cbm._sc.constraints.First();
            if (b != null)
                return new ConstraintsBookmark(_srs, cbm,
                    b.getValue().val, b.getValue().key, Position+1);
        }
        } catch(Exception e) {}
        return null;
    }

}
