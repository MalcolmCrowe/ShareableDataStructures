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
public class SInsert extends Serialisable  {
    public final long table;
    public final SList<Long> cols;
    public final Serialisable vals;
    public SInsert(long t,SList<Long> c,Serialisable v)
    {
        super(Types.SInsert);
        table = t; cols = c; vals = v;
    }
    @Override
    public Serialisable Prepare(STransaction tr,SDict<Long,Long> pt)
            throws Exception
    {
        var t = table;
        if (t < 0)
        {
            var tn = tr.role.uids.get(t);
            if (!tr.role.globalNames.Contains(tn))
                throw new Exception("Table " + tn + " not found");
            t = tr.role.globalNames.get(tn);
        }
        SList<Long> cs = null;
        var rt = tr.role.defs.get(t);
        var i = 0;
        if (cols!=null)
        for (var b = cols.First(); b != null; b = b.Next())
        {
            var u = b.getValue();
            if (u < 0) // it is a client-side uid to be looked up in the SNames contribution to tr
            {
                var cn = tr.role.uids.get(u);
                if (!rt.Contains(cn))
                    throw new Exception("Column " + cn + " not found");
                u = rt.get(cn);
            }
            cs =(cs==null)?new SList(u):cs.InsertAt(u,i);
            i++;
        }
        switch(vals.type)
        {
            case Types.SValues:
            {
                var svs = (SValues)vals;
                var tb = (STable)tr.objects.get(t);
                var nc = svs.vals.Length;
                if (tb.cpos!=null && 
                        ((i == 0 && nc != tb.cpos.Length) || 
                        (i != 0 && i != nc)))
                    throw new Exception("Wrong number of columns");
                SList<Serialisable> vs = null;
                i = 0;
                for (var b = svs.vals.First(); b != null; b = b.Next(),i++)
                {
                    var v = b.getValue().Prepare(tr, tb.Names(tr,pt));
                    vs =(vs==null)?new SList(v):vs.InsertAt(v,i);
                }
                return new SInsert(t, cs, new SValues(vs));
            }
            case Types.SSelect:
                {
                    var ss = (SSelectStatement)vals;
                    var tb = (STable)tr.objects.get(t);
                    ss = (SSelectStatement)ss.Prepare(tr, ss.Names(tr,pt));
                    var nc = ss.getDisplay().Length;
                    if ((i == 0 && nc != tb.cpos.Length) || (i != 0 && i != nc))
                        throw new Exception("Wrong number of columns");
                    return new SInsert(t, cs, ss);
                }
        }
        throw new Exception("Unknown insert syntax "+Types.types[vals.type]);
    }
    @Override
    public STransaction Obey(STransaction tr,Context cx) throws Exception
    {
        var tb = (STable)tr.objects.get(table); 
        switch(vals.type)
        {
            case Types.SValues:
            {
                var svs = (SValues)vals;
                SDict<Long, Serialisable> f = null;
                var c = svs.vals;
                if (cols==null && tb.cpos!=null)
                    for (var b = tb.cpos.First(); c!=null && b != null; b = b.Next(), c = c.next)
                    {
                        var sc = (SColumn)b.getValue().val;
                        var v = sc.Check(tr,c.element.Lookup(tr,cx),cx);
                        f =(f==null)?new SDict(sc.uid, v):f.Add(sc.uid, v);
                    }
                else if (cols!=null)
                    for (var b = cols; c!=null && b!=null; b = b.next, c = c.next)
                    {
                        var sc = (SColumn)tr.objects.get(b.element);
                        var v = sc.Check(tr,c.element.Lookup(tr,cx),cx);
                        f =(f==null)?new SDict(sc.uid, v):f.Add(sc.uid, v);
                    }
                else
                    throw new Exception("PE05");
                tr = (STransaction)tr.Install(tb.Check(tr,new SRecord(tr, table, f)), tr.curpos);
                break;
            }
            case Types.SSelect:
            {
                var ss = (SSelectStatement)vals;
                var rs = ss.RowSet(tr, ss, null);
                for (var rb = (RowBookmark)rs.First();rb!=null;rb=(RowBookmark)rb.Next())
                {
                    SDict<Long, Serialisable> f = null;
                    var c = rb.Ob().vals.First();
                    if (cols==null)
                        for (var b = tb.cpos.First(); c!= null && b != null; b = b.Next(), c = c.Next())
                        {
                            var sc = (SColumn)b.getValue().val;
                            var v = sc.Check(tr,c.getValue().val.Lookup(tr,cx), cx);
                            f=(f==null)?new SDict(sc.uid, v):f.Add(sc.uid, v);
                        }
                    else
                        for (var b = cols; c != null && b.Length != 0; b = b.next, c = c.Next())
                        {
                            var sc = (SColumn)tr.objects.get(b.element);
                            var v = sc.Check(tr,c.getValue().val.Lookup(tr,cx), cx);
                            f =(f==null)?new SDict(b.element, v):f.Add(b.element,v);
                        }
                    tr = (STransaction)tr.Install(new SRecord(tr, table, f), tr.curpos);
                }
                break;
            }
        }
        return tr;
    }
    @Override
    public void Put(WriterBase f) throws Exception
    {
        super.Put(f);
        f.PutLong(table);
        f.PutInt((cols==null)?0:cols.Length);
        if (cols!=null)
        for (var b = cols.First(); b != null; b = b.Next())
            f.PutLong(b.getValue());
        vals.Put(f);
    }

}
