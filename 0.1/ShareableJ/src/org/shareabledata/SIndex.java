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
public class SIndex extends SDbObject {

    public final long table;
    public final boolean primary;
    public final long references;
    public final long refindex;
    public final SList<Long> cols;
    public final SMTree<Serialisable> rows;
    /// <summary>
    /// A primary or unique index
    /// </summary>
    /// <param name="t"></param>
    /// <param name="c"></param>

    public SIndex(long t, boolean p, long r, SList<Long> c)
            throws Exception {
        super(Types.SIndex);
        table = t;
        primary = p;
        cols = c;
        references = r;
        rows = new SMTree<Serialisable>(null);
        refindex = -1L;
    }

    SIndex(ReaderBase f) throws Exception 
    {
        super(Types.SIndex,f);
        var ro = f.db.role;
        var tn = ro.uids.get(f.GetLong());
        if (!ro.globalNames.Contains(tn))
            throw new Exception("Table " + tn + " not found");
        table = ro.globalNames.get(tn);
        primary = f.ReadByte()!=0;
        var n = f.GetInt();
        var rt = ro.defs.get(table);
        var c = new Long[n];
        for (var i = 0; i < n; i++)
        {
            var cn = ro.uids.get(f.GetLong());
            if (!rt.Contains(cn))
                throw new Exception("Column " + cn + " not found");
            c[i] = rt.get(cn);
        }
        var ru = f.GetLong();
        var rn = (ru == -1L) ? "" : ro.uids.get(ru);
        var rx = -1L;
        if (ru != -1)
        {
            if (!ro.globalNames.Contains(rn))
                throw new Exception("Ref table " + rn + " not found");
            ru = ro.globalNames.get(rn);
            var qx = f.db.GetPrimaryIndex(ru);
            if (qx!=null)
                rx = ((SIndex)qx).uid;
            else
                throw new Exception("Ref table " + rn + " has no primary index");
        }
        references = ru;
        refindex = rx;
        cols = new SList<Long>(c);
        rows = new SMTree<Serialisable>(Info((STable)f.db.objects.get(table), cols,references>=0));

    }

    public SIndex(SIndex x, Writer f) throws Exception {
        super(x, f);
        table = f.Fix(x.table);
        f.PutLong(table);
        primary = x.primary;
        f.WriteByte((byte) (primary ? 1 : 0));
        Long[] c = new Long[x.cols.Length];
        f.PutInt(x.cols.Length);
        var i = 0;
        for (var b = x.cols.First(); b != null; b = b.Next()) {
            c[i] = f.Fix(b.getValue());
            f.PutLong(c[i++]);
        }
        references = f.Fix(x.references);
        refindex = f.Fix(x.refindex);
        f.PutLong(references);
        cols = new SList<Long>(c);
        rows = x.rows;
    }

    public SIndex(SIndex x, SMTree<Serialisable>.MTResult mt) throws Exception 
    {
        super(x);
        if (mt.tb != TreeBehaviour.Allow) {
            throw new Exception("Index constraint violation");
        }
        table = x.table;
        primary = x.primary;
        references = x.references;
        refindex = x.refindex;
        cols = x.cols;
        rows = mt.t;
    }
    
    public SIndex(SIndex x, SMTree<Serialisable> mt) throws Exception 
    {
        super(x);
        table = x.table;
        primary = x.primary;
        references = x.references;
        refindex = x.refindex;
        cols = x.cols;
        rows = mt;
    }
    @Override
    public void Put(WriterBase f) throws Exception
    {
        super.Put(f);
        f.PutLong(table);
        f.WriteByte((byte)(primary ? 1 : 0));
        f.PutInt(cols.Length);
        for (var b = cols.First(); b != null; b = b.Next())
            f.PutLong(b.getValue());
        f.PutLong(references);
    }
    public static SIndex Get(ReaderBase f) throws Exception {
        return new SIndex(f);
    }
    public void Check(SDatabase db,SRecord r,boolean updating)
            throws Exception
    {
        var k = Key(r, cols);
        if ((!updating) && refindex == -1 && rows.Contains(k))
            throw new Exception("Duplicate Key constraint violation");
        if (refindex != -1)
        {
            var rx = (SIndex)db.objects.get(refindex);
            if (!rx.rows.Contains(k))
                throw new Exception("Referential constraint violation");
        }
    }
    public boolean Contains(SRecord sr) throws Exception {
        return rows.Contains(Key(sr, cols));
    }

    public SIndex Add(SRecord r, long c) throws Exception {
        return new SIndex(this, rows.Add(Key(r, cols), c));
    }

    public SIndex Update(SRecord o, SCList<Variant> ok, SUpdate u, 
            SCList<Variant> uk, long c) throws Exception {
        return new SIndex(this, 
                rows.Remove(ok, o.uid).Add(uk, u.uid));
    }

    public SIndex Remove(SRecord sr, long c) throws Exception {
        return new SIndex(this, rows.Remove(Key(sr, cols), c));
    }

    SList<TreeInfo<Serialisable>> Info(STable tb, SList<Long> cols, boolean fkey) 
            throws Exception 
    {
        if (cols==null) {
            return null;
        }
        var n = Info(tb, cols.next,fkey);
        var ti = new TreeInfo<Serialisable>(tb.cols.Lookup(cols.element), 
                (cols.Length!=1 || fkey)?'A':'D', 'D', true);
        if (n == null) {
            return new SList<TreeInfo<Serialisable>>(ti);
        }
        return n.InsertAt(ti, 0);
    }

    SCList<Variant> Key(SRecord sr, SList<Long> cols) throws Exception {
        if (cols==null) {
            return null;
        }
        return new SCList<Variant>(new Variant(sr.fields.Lookup(cols.element),true), Key(sr, cols.next));
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(super.toString() + " [" + SDbObject._Uid(table) + "] (");
        var cm = "";
        if (cols!=null)
        for (var b = cols.First(); b != null; b = b.Next()) {
            sb.append(cm);
            cm = ",";
            sb.append(SDbObject._Uid(b.getValue()));
        }
        sb.append(")");
        return sb.toString();
    }
}
