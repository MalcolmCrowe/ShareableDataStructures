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
public class ColumnsBookmark extends RowBookmark {
            public final SysRows _srs;
            public final TablesBookmark _tbm;
            public final SColumn _sc;
            public final int _seq;
            ColumnsBookmark(SysRows rs, TablesBookmark tbm, SColumn sc, int seq, int p)
                    throws Exception
            {
                super(rs, _Cx(rs,rs._Row(new SString(rs._tr.Name(tbm._tb.uid)), // Table
                    new SString(rs._tr.Name(sc.uid)), // Name
                    new SString(Types.types[sc.dataType]), //Type
                    new SInteger((sc.constraints==null)?0:sc.constraints.Length),
                    new SString(sc.Uid())),tbm._cx), p);
                _srs = rs; _sc = sc; _seq = seq; _tbm = tbm;
            }
            static ColumnsBookmark New(SysRows rs) throws Exception
            {
                for (var tbm = TablesBookmark.New(rs); tbm != null; tbm = (TablesBookmark)tbm.Next())
                {
                    if (tbm._tb.cpos==null)
                        return null;
                    var b = tbm._tb.cpos.First();
                    if (b != null)
                        return new ColumnsBookmark(rs, tbm, 
                                (SColumn)b.getValue().val, b.getValue().key, 0);
                }
                return null;
            }
            public Bookmark<Serialisable> Next()
            {
                try{
                var n = _tbm._tb.cpos.PositionAt(_seq).Next();
                if (n!=null)
                    return new ColumnsBookmark(_srs, _tbm, 
                            (SColumn)n.getValue().val, n.getValue().key, 
                        Position+1);
                for (var tbm = (TablesBookmark)_tbm.Next(); tbm != null; 
                    tbm = (TablesBookmark)tbm.Next())
                {
                    if (tbm._tb.cpos==null)
                        return null;
                    var b = tbm._tb.cpos.First();
                    if (b != null)
                        return new ColumnsBookmark(_srs, tbm, 
                                (SColumn)b.getValue().val, b.getValue().key, 
                            Position+1);
                }
                } catch (Exception e) {}
                return null;
            }

}
