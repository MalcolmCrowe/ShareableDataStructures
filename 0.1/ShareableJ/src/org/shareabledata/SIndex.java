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
        rows = new SMTree(null);
        refindex = -1L;
    }
    public SIndex(STransaction tr,long t, boolean p, long r, SList<Long> c)
            throws Exception {
        super(Types.SIndex,tr);
        table = t;
        primary = p;
        cols = c;
        references = r;
        if (r >= 0)
        {
            var rx = tr.GetPrimaryIndex(r);
            if (rx==null)
                throw new Exception("referenced table has no primary index");
            refindex = rx.uid;
        }
        else
            refindex = -1;
            rows = new SMTree(Info((STable)tr.objects.get(table), cols,refindex>=0));
    }

    SIndex(ReaderBase f) throws Exception 
    {
        super(Types.SIndex,f);
        table = f.GetLong();
        primary = f.ReadByte()!=0;
        var n = f.GetInt();
        var c = new Long[n];
        for (var i = 0; i < n; i++)
            c[i] = f.GetLong();
        references = f.GetLong();
        refindex = -1;
        cols = new SList(c);
        if (f instanceof Reader)
        {
            var rdr = (Reader) f;
            rows = new SMTree<Serialisable>(Info((STable)rdr.db.objects.get(table), cols, refindex >= 0));
        }
        else
            rows = new SMTree(null);

    }
    @Override
    public Serialisable Prepare(STransaction tr,SDict<Long,Long>pt)
            throws Exception
    {
        var ro = tr.role;
        var tn = ro.uids.get(table);
        if (!ro.globalNames.Contains(tn))
            throw new Exception("Table " + tn + " not found");
        var tb = ro.globalNames.get(tn);
        var pr = primary;
        var rt = ro.subs.get(tb);
        var c = new Long[cols.Length];
        var i = 0;
        for (var b=cols.First();b!=null;b=b.Next(),i++)
        {
            var cn = ro.uids.get(b.getValue());
            if (rt==null || !rt.defs.Contains(cn))
                throw new Exception("Column " + cn + " not found");
            c[i] = rt.obs.get(rt.defs.get(cn)).key;
        }
        var ru = references;
        var rn = (ru == -1L) ? "" : ro.uids.get(ru);
        var rx = -1L;
        if (ru != -1)
        {
            if (!ro.globalNames.Contains(rn))
                throw new Exception("Ref table " + rn + " not found");
            ru = ro.globalNames.get(rn);
         }
        return new SIndex(tr,tb,pr,ru,new SList(c));
    }
    public SIndex(SDatabase db,SIndex x, Writer f) throws Exception {
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
        cols = new SList(c);
        rows = new SMTree(Info((STable)db.objects.get(table), cols, references >= 0));
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
        var sb = new StringBuilder( "Index "+
                _Uid(uid) + "[" + SDbObject._Uid(table) + "] (");
        var cm = "";
        if (cols!=null)
        for (var b = cols.First(); b != null; b = b.Next()) {
            sb.append(cm);
            cm = ",";
            sb.append(SDbObject._Uid(b.getValue()));
        }
        sb.append(")");
        if (primary)
            sb.append(" primary ");
        if (refindex >= 0)
            sb.append(" ref index " + refindex);
        return sb.toString();
    }
}
