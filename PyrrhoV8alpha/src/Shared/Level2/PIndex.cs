using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Reflection.Emit;
using System.Text;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2026
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
    /// <summary>
    /// PIndex is a way of associating a BTree with a PTable
    /// A PIndex is made whenever there is a primary, references or unique constraint
    /// </summary>
    internal class PIndex : Physical
    {
        /// <summary>
        /// The defining position of the index
        /// </summary>
        public virtual long defpos { get { return ppos; } }
        /// <summary>
        /// The name of the index: generated as U(nnn)
        /// </summary>
        public string name = "";
        /// <summary>
        /// The hosting table
        /// </summary>
        public long tabledefpos = -1L;
        /// <summary>
        /// The key TableColumns for the index 
        /// </summary>
        public Domain columns = Domain.Row;
        /// <summary>
        /// The constraint type
        /// </summary>
        public ConstraintType flags = 0;
        public TMetadata metadata = TMetadata.Empty;
        /// <summary>
        /// Constraint Type for the Index.
        /// Desc does not seem to be used. 
        /// These values are written to the database and should not be changed
        /// </summary>
        [Flags]
        public enum ConstraintType
        {
            NoType = 0, PrimaryKey = 1, ForeignKey = 2, Unique = 4, Desc = 8,
            RestrictUpdate = 16, CascadeUpdate = 32, SetDefaultUpdate = 64,
            SetNullUpdate = 128, RestrictDelete = 256,
            CascadeDelete = 512, SetDefaultDelete = 1024, SetNullDelete = 2048,
            SystemTimeIndex = 4096, ApplicationTimeIndex = 8192, NoBuild = 16384
        }
        internal const ConstraintType Deletes =
            ConstraintType.RestrictDelete | ConstraintType.CascadeDelete |
            ConstraintType.SetDefaultDelete | ConstraintType.SetNullDelete;
        internal const ConstraintType Updates =
            ConstraintType.RestrictUpdate | ConstraintType.CascadeUpdate
            | ConstraintType.SetDefaultUpdate | ConstraintType.SetNullUpdate;
        public const ConstraintType Cascade = Deletes | Updates;
        public const ConstraintType Reference = ConstraintType.ForeignKey
            | ConstraintType.RestrictUpdate | ConstraintType.RestrictDelete;
        /// <summary>
        /// The referenced Index for a foreign key
        /// </summary>
        public long reference;
        /// <summary>
        /// The adapter function (PIndex1)
        /// </summary>
        public string adapter = "";
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr, defpos)) return defpos;
            if (!Committed(wr, tabledefpos)) return tabledefpos;
            for (var b = columns.First(); b is not null; b = b.Next())
                if (b.value() is long p && !Committed(wr, p)) return p;
            if (reference >= 0 && wr.cx.db.objects[reference] is Level3.Index xr)
            {
                var reftable = xr.tabledefpos;
                if (!Committed(wr, reftable)) return reftable;
                if (!Committed(wr, reference)) return reference;
            }
            return -1;
        }
        internal override bool NeededFor(BTree<long, Physical> physicals)
        {
            if (!ifNeeded)
                return true;
            for (var b = physicals.First(); b != null; b = b.Next())
                if (b.value() is Record r && r.tabledefpos == tabledefpos)
                    return true;
            return false;
        }
        /// <summary>
        /// Constructor: A new PIndex request from the Parser
        /// </summary>
        /// <param name="nm">The name of the index</param>
        /// <param name="tb">The table being indexed</param>
        /// <param name="cl">The defining positions of key TableColumns</param>
        /// <param name="fl">The constraint flags</param>
        /// <param name="rx">The defining position of the referenced index (or -1)</param>
        /// <param name="tb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PIndex(string nm, Table tb, Domain cl,
            ConstraintType fl, long rx, long pp, Database d, bool ifN = false) :
            this(Type.PIndex, nm, tb, cl, fl, rx, pp, d)
        {
            ifNeeded = ifN;
        }
        /// <summary>
        /// Constructor: A new PIndex request from the Parser
        /// </summary>
        /// <param name="t">The PIndex type</param>
        /// <param name="nm">The name of the index</param>
        /// <param name="tb">The table being indexed</param>
        /// <param name="cl">The defining positions of key TableColumns</param>
        /// <param name="fl">The constraint flags</param>
        /// <param name="rx">The defining position of the referenced index (or -1)</param>
        /// <param name="tb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PIndex(Type t, string nm, Table tb, Domain cl,
            ConstraintType fl, long rx, long pp, Database d) :
            base(t, pp, d)
        {
            if (fl == ConstraintType.ForeignKey)
                fl = Reference;
            name = nm ?? throw new DBException("42102").Mix();
            tabledefpos = tb.defpos;
            columns = cl;
            flags = fl;
            reference = rx;
        }
        /// <summary>
        /// Constructor: A new PIndex request from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">Position in the buffer</param>
        public PIndex(Reader rdr) : base(Type.PIndex, rdr) { }
        /// <summary>
        /// Constructor: A new PIndex request from the buffer
        /// </summary>
        /// <param name="t">The PIndex type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">Position in the buffer</param>
        public PIndex(Type t, Reader rdr) : base(t, rdr) { }
        protected PIndex(PIndex x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.cx.Fix(x.tabledefpos);
            var bs = BList<DBObject>.Empty;
            for (var b = x.columns.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var nc = wr.cx._Ob(wr.cx.Fix(p)) ?? throw new PEException("PE0098");
                    bs += nc;
                }
            columns = (Domain)wr.cx.Add(new Domain(-1L, wr.cx, Qlx.ROW, bs, bs.Length));
            flags = x.flags;
            name = x.name;
            reference = wr.cx.Fix(x.reference);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PIndex(this, wr);
        }
        /// <summary>
        /// The Affedcted Physical is this
        /// </summary>
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocatioon information for positions</param>
        public override void Serialise(Writer wr) //LOCKED
        {
            tabledefpos = wr.cx.Fix(tabledefpos);
            wr.PutString(name.ToString());
            wr.PutLong(tabledefpos);
            wr.PutInt(columns.Length);
            for (int j = 0; j < columns.Length; j++)
                wr.PutLong(columns[j] ?? -1L);
            wr.PutInt((int)flags);
            reference = wr.cx.Fix(reference);
            wr.PutLong(reference);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            name = rdr.GetString();
            tabledefpos = rdr.GetLong();
            int n = rdr.GetInt();
            if (n > 0)
            {
                var rt = CTree<int,long>.Empty;
                var rs = CTree<long, Domain>.Empty;
                for (int j = 0; j < n; j++)
                {
                    var cp = rdr.GetLong();
                    rt += (j,cp);
                    rs += (cp, rdr.context._Dom(cp) ?? Domain.Null); // for Log$ may be historical
                }
                columns = (rt == CTree<int,long>.Empty) ? Domain.Row : 
                    new Domain(-1L, rdr.context, Qlx.ROW, rs, rt);
            }
            flags = (ConstraintType)rdr.GetInt();
            if ((int)flags>=16)
                metadata += (Qlx.ACTION, new TInt((int)flags));
            reference = rdr.GetLong();
            if (rdr.context.db.objects[reference] is Table tc)
            {
                if (rdr.context.db.objects[reference] is Level3.Index rx)
                {
                    var sb = new StringBuilder(tc.NameFor(rdr.context));
                    var cm = '(';
                    for (var b = rx.keys.First(); b != null; b = b.Next())
                        if (rdr.context.db.objects[b.value()] is TableColumn xc)
                        {
                            sb.Append(cm); cm = ',';
                            sb.Append(xc.NameFor(rdr.context));
                        }
                    if (cm != '(') sb.Append(')');
 //                   metadata += (Qlx.REFERENCES, 
  //                      new TConnector(Qlx.WITH, sb.ToString(), tc, ppos, metadata.ToString(), metadata));
                }
                rdr.context.db += tc + (Table.RefCols, ppos);
            }
            base.Deserialise(rdr);
        }
        public override bool Committed(Writer wr, long pos)
        {
            if (pos >= Transaction.Executables && pos < Transaction.HeapStart)
                return true;
            return base.Committed(wr, pos);
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.Alter3:
                    if (((Alter3)that).table?.defpos == tabledefpos)
                        return new DBException("40077", tabledefpos, that, ct);
                    break;
                case Type.Alter2:
                    if (((Alter2)that).table?.defpos == tabledefpos)
                        return new DBException("40077", tabledefpos, that, ct);
                    break;
                case Type.Alter:
                    if (((Alter)that).table?.defpos == tabledefpos)
                        return new DBException("40077", tabledefpos, that, ct);
                    break;
                case Type.PIndex2:
                case Type.PIndex1:
                case Type.PIndex:
                    if (((PIndex)that).tabledefpos == tabledefpos)
                        return new DBException("40042", tabledefpos, that, ct);
                    break;
                case Type.Drop:
                    {
                        if (that is Drop d)
                        {
                            if (d.delpos == tabledefpos || d.delpos == reference)
                                return new DBException("40012", d.delpos, that, ct);
                            for (int j = 0; j < columns.Length; j++)
                                if (d.delpos == columns[j] || d.delpos == -columns[j])
                                    return new DBException("40013", d.delpos, that, ct);
                        }
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            var (nt, ph) = base.Commit(wr, tr);
            if (tr is not null && wr.cx.db.objects[((PIndex)ph).tabledefpos] is Table tb
                && wr.cx.db.objects[ph.ppos] is Level3.Index x)
            {
                if (tb.indexes[x.keys] is CTree<long, bool> ct)
                    wr.cx.db += (tb.defpos, tb + (Table.Indexes, tb.indexes + (x.keys, ct - ppos)));
                if (reference > 0 && wr.cx.db.objects[x.reftabledefpos] is Table rt)
                    wr.cx.db += (rt.defpos, rt //+ (Table.RefIndexes, rt.rindexes - ppos)
                                 + (Table.SysRefIndexes, rt.sindexes - x.reftabledefpos));
                if (tr?.objects[tabledefpos] is Table tt && tt.infos[tt.definer]?.metadata is TMetadata md)
                {
                    (wr.cx,var o) = wr.cx.Add(tb, md);
                    tb = (Table)o;
                    var ch = false;
                    var mx = CTree<long, long>.Empty;
                    for (var b = tt.mindexes.First(); b != null; b = b.Next())
                    {
                        var k = b.key();
                        var nk = wr.cx.Fix(k);
                        var v = b.value();
                        var nv = wr.cx.Fix(v);
                        if (k != nk || v != nv)
                            ch = true;
                        mx += (nk, nv);
                    }
                    if (ch)
                        tb += (Table.MultiplicityIndexes, mx);
                    wr.cx.db += tb;
                }
            }
            else
                ((PIndex)ph).metadata = (TMetadata)metadata.Fix(wr.cx);
            return (nt, ph);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>The string representation</returns>
        public override string ToString()
        {
            string r = GetType().Name + " " + name;
            r = r + " on " + Pos(tabledefpos) + "(";
            for (int j = 0; j < columns.Length; j++)
                r += ((j > 0) ? "," : "") + DBObject.Uid(columns[j] ?? -1L);
            r += ") " + flags.ToString();
            if (reference >= 0)
                r += " refers to [" + Pos(reference) + "]";
            return r;
        }
        internal override DBObject? Install(Context cx)
        {
            if (cx.db.objects[tabledefpos] is not Table tb)
                return null;
            var x = new Level3.Index(this, cx);
            x += (DBObject.Infos, x.infos + (cx.role.defpos, new ObInfo("", Grant.Privilege.Execute)));
            cx.db += x; // even for the foreign key references case we save x in cx.db
            if (reference>0)
            {
                var rb = cx.db.objects[reference]; // backward compatibility: may be table or index
                var rt = (rb as Table) ?? (Table?)(cx.db.objects[(rb as Level3.Index)?.tabledefpos??-1L])
                    ?? throw new PEException("PE14052");
                if (rt.defpos < 0L)
                    throw new PEException("PE47872");
                var d = new CTree<Domain,bool>(rt,true);
                var rc = CList<TypedValue>.Empty;
                var tt = tb.rowType;
                var ts = tb.representation;
                var lk = -1L; // will become the defpos of the last referencing column
                var cm = "(";
                var kn = tb.NameFor(cx);
                var k1 = "";
                TableColumn tc = new TableColumn(ppos+1, BTree<long, object>.Empty);
                cx.db += (Database.NextPos, ppos + 1);
                Level3.Index? rx = null;
                if (flags.HasFlag(ConstraintType.ForeignKey))
                {
                    rx = rt.FindPrimaryIndex(cx);
                    var pb = rx?.keys.First();
                    for (var b = columns.First(); b != null; b = b.Next())
                    {
                        lk = b.value();  // prepare a composite name for a new reference column
                        k1 = cx.NameFor(lk) ?? lk.ToString();
                        kn += (cm + k1);
                        if (pb != null)
                            pb = pb.Next();
                        cm = ",";
                        if (b.Next() == null)
                            break;
                    }
                    tc += (TableColumn.Hide, true);
                    if (rx!=null)
                        tc += (TableColumn.KeyMap, rx.defpos);
                } else // use the last column instead of making a new one
                    tc = (cx.db.objects[tb.rowType.Last()?.value() ?? -1L] as TableColumn) ?? throw new PEException("71711");
                var nst = cx.db.nextStmt;
                var dm = Domain.Ref; //new Domain(nst, Qlx.REF, d);
                cx.Add(dm);
                var oi = tc.infos[cx.role.defpos]??new ObInfo((cm == ",") ? kn + ")" : k1, Grant.AllPrivileges);
                // Add a reference column with a name from that column or the table and all foreign keys
                // with the defpos of the index we are not creating
                metadata -= Qlx.REFERENCES;// we don't want this in a TableColumn 
                metadata += (Qlx.ACTION,new TInt(((int)flags)&0xfffffff0));
                if (!metadata.Contains(Qlx.OPTIONAL))
                    metadata += (Qlx.OPTIONAL, (tb is UDType)?TBool.False:TBool.True);
                var c = new TConnector(Qlx.WITH, oi.name ?? "", rt, tc.defpos, true, metadata.ToString(), metadata);
                if (c.cm is TMetadata mc && mc != metadata) // e.g. OPTIONAL may have been changed
                    metadata = c.cm;
                oi += (ObInfo._Metadata,metadata);
                cx.db += rt + (Table.RefCols, rt.refCols + (tc.defpos, true));
                tc = tc + (TableColumn._Table,tb.defpos) + (DBObject._Domain, dm) - Domain.Optional
                    + (TableColumn.KeyMap,ppos)
                    + (DBObject.Definer, cx.db.role.defpos) + (Level3.Index.RefTable, rt.defpos)
                    + (DBObject.LastChange, ppos) + (DBObject.Infos,new BTree<long,ObInfo>(cx.db.role.defpos,oi));
                cx.Install(tc);
                cx.db += tc;
                cx._Add(tc);
                tb += (cx, tc);
                var u = tb.colRefs[c.rd.defpos] ?? CTree<long, bool>.Empty;
                tb += (Domain.ColRefs, tb.colRefs + (c.rd.defpos, u + (tc.defpos, true)));
                cx.AddObs(tb);
                cx.db += tb;
                if (cx.db is Transaction tr && tb.infos[tb.definer] is ObInfo ti)
                    metadata = ti.metadata - Qlx.REFERENCES;
                return tb;
            }
   //         if (!x.flags.HasFlag(ConstraintType.NoBuild))
                x = x.Build(cx);
            var t = tb.indexes[x.keys] ?? CTree<long, bool>.Empty;
            tb += (Table.Indexes, tb.indexes + (x.keys, t + (x.defpos, true)));
            cx.db += tb;
            x = x.AddRows(tb, cx); // ??
            cx.Install(x);
            var cs = CList<long>.Empty;
    //        var fl = false;
            for (var b = x.keys.First(); b != null; b = b.Next())
                if (b.value() is long tc && cx.db.objects[tc] is TableColumn c)
                {
                    cs += tc - Domain.Optional;
                    cx.Add(c);
     //               fl = c.cs is TConnector cc && cc.q == Qlx.ID;
                } 
            var dp = defpos;
            tb = tb + (DBObject.LastChange, defpos);
            cx.Install(tb);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.db += tb;
            cx.db += x;
            return tb;
        }
    }

    /// <summary>
    /// PIndex1 is used for conditional or adapted referential constraints
    /// </summary>
    internal class PIndex1 : PIndex
    {
        /// <summary>
        /// Constructor: A new PIndex request from the Parser
        /// </summary>
        /// <param name="nm">The name of the index</param>
        /// <param name="tb">The table being indexed</param>
        /// <param name="cl">The defining positions of key TableColumns</param>
        /// <param name="fl">The constraint flags</param>
        /// <param name="rx">The defining position of the referenced index (or -1)</param>
        /// <param name="af">The adapter function</param>
        /// <param name="tb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PIndex1(string nm, Table tb, Domain cl,
            ConstraintType fl, long rx, string af, long pp, Database d) :
            this(Type.PIndex1, nm, tb, cl, fl, rx, af, pp, d)
        { }
        /// <summary>
        /// Constructor: A new PIndex request from the Parser
        /// </summary>
        /// <param name="t">The PIndex type</param>
        /// <param name="nm">The name of the index</param>
        /// <param name="tb">The table being indexed</param>
        /// <param name="cl">The defining positions of key TableColumns</param>
        /// <param name="fl">The constraint flags</param>
        /// <param name="rx">The defining position of the referenced index (or -1)</param>
        /// <param name="af">The adapter function</param>
        /// <param name="tb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PIndex1(Type t, string nm, Table tb, Domain cl,
            ConstraintType fl, long rx, string af, long pp, Database d) :
            base(t, nm, tb, cl, fl, rx, pp, d)
        {
            adapter = af;
        }
        /// <summary>
        /// Constructor: A new PIndex request from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">Position in the buffer</param>
        public PIndex1(Reader rdr) : base(Type.PIndex1, rdr) { }
        protected PIndex1(Type t, Reader rdr) : base(t, rdr) { }
        protected PIndex1(PIndex1 x, Writer wr) : base(x, wr)
        {
            adapter = x.adapter;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PIndex1(this, wr);
        }
        public override void Serialise(Writer wr) //LOCKED
        {
            wr.PutString(adapter);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            adapter = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return base.ToString() + ((adapter != "") ? ("USING: " + adapter) : "");
        }
    }
    /// <summary>
    /// PIndex2 is used for adding metadata flags to a referencing column or foreign key
    /// (not used as of v7)
    /// </summary>
    internal class PIndex2 : PIndex1
    {
        /// <summary>
        /// Constructor: A new PIndex request from the Parser
        /// </summary>
        /// <param name="nm">The name of the index</param>
        /// <param name="tb">The table being indexed</param>
        /// <param name="cl">The defining positions of key TableColumns</param>
        /// <param name="fl">The constraint flags</param>
        /// <param name="rx">The defining position of the referenced index or referencing column</param>
        /// <param name="af">The adapter function</param>
        /// <param name="md">The metadata flags</param>
        /// <param name="db">The database</param>
        public PIndex2(string nm, Table tb, Domain cl,
            ConstraintType fl, long rx, string af, TMetadata md, long pp, Database d) :
            this(Type.PIndex2, nm, tb, cl, fl, rx, af, md, pp, d)
        {
        }
        protected PIndex2(Type t, string nm, Table tb, Domain cl,
            ConstraintType fl, long rx, string af, TMetadata md, long pp, Database d)
            : base(t, nm, tb, cl, fl, rx, af, pp, d)
        {
            metadata = md;
        }
        /// <summary>
        /// Constructor: A new PIndex request from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">Position in the buffer</param>
        public PIndex2(Reader rdr) : base(Type.PIndex2, rdr) { }
        protected PIndex2(Type t, Reader rdr) : base(t, rdr) { }
        protected PIndex2(PIndex2 x, Writer wr) : base(x, wr)
        {
            metadata = x.metadata;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PIndex2(this, wr);
        }
        public override void Serialise(Writer wr) //LOCKED
        {
            wr.PutLong(metadata[Qlx.ACTION].ToInt()??0);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            metadata = TMetadata.Empty+(Qlx.ACTION,new TInt(rdr.GetLong()));
            base.Deserialise(rdr);
        }
    }
    internal class RefAction : Physical
    {
        public PIndex.ConstraintType ctype;
        public long index;
        public RefAction(long ix, PIndex.ConstraintType ct, long pp, Database d)
            : base(Type.RefAction, pp, d)
        {
            index = ix;
            ctype = ct;
        }
        public RefAction(Reader rdr) : base(Type.RefAction, rdr) { }
        protected RefAction(RefAction r, Writer wr) : base(r, wr)
        {
            index = r.index;
            ctype = r.ctype;
        }
        public override void Serialise(Writer wr)
        {
            wr.PutLong(wr.cx.Fix(index));
            wr.PutInt((int)ctype);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            index = rdr.GetLong();
            ctype = (PIndex.ConstraintType)rdr.GetInt();
            base.Deserialise(rdr);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new RefAction(this, wr);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr, index)) return index;
            return -1;
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.RefAction:
                    var ra = (RefAction)that; if (ra.index == index)
                        return new DBException("40077", index, that, ct);
                    break;
                case Type.PIndex:
                case Type.PIndex1:
                case Type.PIndex2:
                    var pi = (PIndex)that; if (pi.defpos == index)
                        return new DBException("40077", index, that, ct);
                    break;
                case Type.Drop:
                    var dp = (Drop)that; if (dp.delpos == index)
                        return new DBException("40025", index, that, ct);
                    break;
                case Type.Delete:
                case Type.Delete1:
                case Type.Delete2:
                    {
                        var x = (Level3.Index?)db.objects[index] ?? throw new PEException("PE1412");
                        var dl = (Delete)that; if (dl.tabledefpos == x.tabledefpos)
                            return new DBException("40077", index, that, ct);
                        break;
                    }
                case Type.Update:
                case Type.Update1:
                case Type.Update2:
                    {
                        var x = (Level3.Index?)db.objects[index] ?? throw new PEException("PE1412");
                        var up = (Update)that; if (up.tabledefpos == x.tabledefpos)
                            return new DBException("40077", index, that, ct);
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override DBObject? Install(Context cx)
        {
            var x = (Level3.Index?)cx.db.objects[index] ?? throw new DBException("42000", "RefAction");
        /*    var od = x.flags & PIndex.Deletes;
            var ou = x.flags & PIndex.Updates;
            var oc = x.flags & ~PIndex.Cascade; 
            var nd = ctype & PIndex.Deletes;
            var nu = ctype & PIndex.Updates;
            var nc = ctype & ~PIndex.Cascade;
            var nt = (oc | nc) | ((nd == PIndex.ConstraintType.NoType) ? od : nd)
                | ((nu == PIndex.ConstraintType.NoType) ? ou : nu);
            x += (Level3.Index.IndexConstraint, nt); */
            cx.db += x;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return x;
        }
        public override string ToString()
        {
            return "RefAction " + Pos(index) + " " + ctype.ToString();
        }
    }

}