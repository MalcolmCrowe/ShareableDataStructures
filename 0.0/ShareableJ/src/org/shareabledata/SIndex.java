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
    public final SMTree<Long> rows;
    /// <summary>
    /// A primary or unique index
    /// </summary>
    /// <param name="t"></param>
    /// <param name="c"></param>

    public SIndex(STransaction tr, long t, boolean p, long r, SList<Long> c)
            throws Exception {
        super(Types.SIndex, tr);
        table = t;
        primary = p;
        cols = c;
        references = r;
        if (r>=0)
        {
            var rx = tr.GetPrimaryIndex(r);
            if (rx==null)
                throw new Exception("referenced table has no primary index");
            refindex = rx.uid;
        } else
            refindex = -1;
        rows = new SMTree<Long>(Info((STable) tr.objects.Lookup(table), cols,
                refindex>=0));
    }

    SIndex(SDatabase d, Reader f) throws Exception {
        super(Types.SIndex, f);
        table = f.GetLong();
        primary = f.ReadByte() != 0;
        var n = f.GetInt();
        var c = new Long[n];
        for (var i = 0; i < n; i++) {
            c[i] = f.GetLong();
        }
        references = f.GetLong();
        if (references>=0)
        {
            var rx = d.GetPrimaryIndex(references);
            if (rx==null)
                throw new Exception("internal error");
            refindex = rx.uid;
        }
        else
            refindex = -1;
        cols = new SList<Long>(c);
        rows = new SMTree<Long>(Info((STable) d.objects.Lookup(table), cols,
        references>=0));
    }

    public SIndex(SIndex x, AStream f) throws Exception {
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

    public SIndex(SIndex x, SMTree<Long>.MTResult mt) throws Exception 
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
    
    public SIndex(SIndex x, SMTree<Long> mt) throws Exception 
    {
        super(x);
        table = x.table;
        primary = x.primary;
        references = x.references;
        refindex = x.refindex;
        cols = x.cols;
        rows = mt;
    }

    public static SIndex Get(SDatabase d, Reader f) throws Exception {
        return new SIndex(d, f);
    }

    public boolean Contains(SRecord sr) throws Exception {
        return rows.Contains(Key(sr, cols));
    }

    public SIndex Add(SRecord r, long c) throws Exception {
        return new SIndex(this, rows.Add(Key(r, cols), c));
    }

    public SIndex Update(SRecord o, SUpdate u, long c) throws Exception {
        return new SIndex(this, 
                rows.Remove(Key(o, cols), o.uid).Add(Key(u, cols), u.uid));
    }

    public SIndex Remove(SRecord sr, long c) throws Exception {
        return new SIndex(this, rows.Remove(Key(sr, cols), c));
    }

    SList<TreeInfo<Long>> Info(STable tb, SList<Long> cols, boolean fkey) 
            throws Exception 
    {
        if (cols==null || cols.Length == 0) {
            return null;
        }
        var n = Info(tb, cols.next,fkey);
        var ti = new TreeInfo<Long>(cols.element, 
                (cols.Length!=1 || fkey)?'A':'D', 'D', true);
        if (n == null) {
            return new SList<TreeInfo<Long>>(ti);
        }
        return n.InsertAt(ti, 0);
    }

    SCList<Variant> Key(SRecord sr, SList<Long> cols) throws Exception {
        if (cols==null || cols.Length == 0) {
            return null;
        }
        return new SCList<Variant>(new Variant(sr.fields.Lookup(cols.element),true), Key(sr, cols.next));
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(super.toString() + " [" + SDbObject._Uid(table) + "] (");
        var cm = "";
        for (var b = cols.First(); b != null; b = b.Next()) {
            sb.append(cm);
            cm = ",";
            sb.append(SDbObject._Uid(b.getValue()));
        }
        sb.append(")");
        return sb.toString();
    }
}
