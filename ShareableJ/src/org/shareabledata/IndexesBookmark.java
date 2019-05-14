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
public class IndexesBookmark extends RowBookmark {
              public final SysRows _srs;
            public final TablesBookmark _tbm;
            public final SIndex _ix;
            IndexesBookmark(SysRows rs, TablesBookmark tbm, SIndex ix, int p)
                    throws Exception
            {
                super (rs,_Cx(rs,rs._Row(new SString(rs._tr.Name(tbm._tb.uid)), // TableName
                    new SString(Type(ix)), // Cols
                    new SString(Cols(rs,ix)),
                    new SString(References(rs._tr,ix))),tbm._cx), p); // Rows
                _srs = rs; _tbm = tbm; _ix = ix;
            }
            static String Type(SIndex x)
            {
                if (x.primary)
                    return "PRIMARY KEY";
                if (x.references >= 0)
                    return "FOREIGN KEY";
                return "UNIQUE";
            }
            static String Cols(SysRows rs,SIndex ix) throws Exception
            {
                var sb = new StringBuilder("(");
                var cm = "";
                for (var b=ix.cols.First();b!=null;b=b.Next())
                {
                    sb.append(cm); cm = ",";
                    sb.append(rs._tr.Name(((SColumn)rs._tr.objects.get(b.getValue())).uid));
                }
                sb.append(")");
                return sb.toString();
            }
            static String References(SDatabase tr,SIndex ix) throws Exception
            {
                return (ix.references < 0) ? "" : ("" + tr.Name(ix.references));
            }
            static IndexesBookmark New(SysRows rs) throws Exception
            {
                for (var tbm = TablesBookmark.New(rs); tbm != null; 
                        tbm = (TablesBookmark)tbm.Next())
                {
                    if (tbm._tb.indexes==null)
                        continue;
                    var b = tbm._tb.indexes.First();
                    if (b != null)
                        return new IndexesBookmark(rs, tbm, 
                                (SIndex)rs._tr.objects.get(b.getValue().key), 0);
                }
                return null;
            }
            public Bookmark<Serialisable> Next()
            {
                try {
                var p =_tbm._tb.indexes.PositionAt(_ix.uid);
                if (p==null)
                    return null;
                var n = p.Next();
                if (n != null)
                    return new IndexesBookmark(_srs, _tbm,
                        (SIndex)_srs._tr.objects.get(n.getValue().key), Position + 1);
                for (var tbm = (TablesBookmark)_tbm.Next(); 
                    tbm != null; tbm = (TablesBookmark)tbm.Next())
                {
                    if (tbm._tb.indexes==null)
                        return null;
                    var b = tbm._tb.indexes.First();
                    if (b != null)
                        return new IndexesBookmark(_srs, tbm, 
                            (SIndex)_srs._tr.objects.get(b.getValue().key), 0);
                }
                } catch(Exception e) {}
                return null;
            }  
}
