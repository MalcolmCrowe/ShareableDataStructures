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
    public final SList<Long> cols;
    public final SMTree<Long> rows;
    /// <summary>
    /// A primary or unique index
    /// </summary>
    /// <param name="t"></param>
    /// <param name="c"></param>

    public SIndex(STransaction tr, long t, boolean p, SList<Long> c)
            throws Exception {
        super(Types.SIndex, tr);
        table = t;
        primary = p;
        cols = c;
        references = -1;
        rows = new SMTree<Long>(Info((STable) tr.Lookup(table), cols));
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
        cols = new SList<Long>(c);
        rows = new SMTree<Long>(Info((STable) d.Lookup(table), cols));
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
        f.PutLong(references);
        cols = new SList<Long>(c);
        rows = x.rows;
    }

    public SIndex(SIndex x, SMTree<Long>.MTResult mt) throws Exception {
        super(x);
        if (mt.tb != TreeBehaviour.Allow) {
            throw new Exception("Index constraint violation");
        }
        table = x.table;
        primary = x.primary;
        references = x.references;
        cols = x.cols;
        rows = mt.t;
    }
    
        public SIndex(SIndex x, SMTree<Long> mt) throws Exception {
        super(x);
        table = x.table;
        primary = x.primary;
        references = x.references;
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
        return new SIndex(this, rows.Remove(Key(o, cols), c).Add(Key(u, cols), c));
    }

    public SIndex Remove(SRecord sr, long c) throws Exception {
        return new SIndex(this, rows.Remove(Key(sr, cols), c));
    }

    SList<TreeInfo> Info(STable tb, SList<Long> cols) throws Exception {
        if (cols==null || cols.Length == 0) {
            return null;
        }
        var n = Info(tb, cols.next);
        var ti = new TreeInfo<Long>(cols.element, 'D', 'D');
        if (n == null) {
            return new SList<TreeInfo>(ti);
        }
        return n.InsertAt(ti, 0);
    }

    SCList<Variant> Key(SRecord sr, SList<Long> cols) throws Exception {
        if (cols==null || cols.Length == 0) {
            return null;
        }
        return new SCList<Variant>(new Variant(sr.fields.Lookup(cols.element)), Key(sr, cols.next));
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(super.toString() + " [" + STransaction.Uid(table) + "] (");
        var cm = "";
        for (var b = cols.First(); b != null; b = b.Next()) {
            sb.append(cm);
            cm = ",";
            sb.append(STransaction.Uid(b.getValue()));
        }
        sb.append(")");
        return sb.toString();
    }
}
