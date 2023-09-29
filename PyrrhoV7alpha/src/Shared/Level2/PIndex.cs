using Pyrrho.Common;
using Pyrrho.Level4;
using Pyrrho.Level3;
using Pyrrho.Level5;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
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
        public ulong metadata = 0UL;
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
            SystemTimeIndex = 4096, ApplicationTimeIndex = 8192
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
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,tabledefpos)) return tabledefpos;
            for (var b=columns.First();b is not null;b=b.Next())
                if (b.value() is long p && !Committed(wr,p)) return p;
            if (reference >= 0 && wr.cx.db.objects[reference] is Level3.Index xr)
            {
                var reftable = xr.tabledefpos;
                if (!Committed(wr, reftable)) return reftable;
                if (!Committed(wr, reference)) return reference;
            }
            return -1;
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
            ConstraintType fl, long rx, long pp) :
            this(Type.PIndex, nm, tb, cl, fl, rx, pp)
        {  }
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
            ConstraintType fl, long rx, long pp) :
            base(t, pp)
        {
            if (fl == ConstraintType.ForeignKey || fl == ConstraintType.NoType)
                fl = Reference;
            name = nm?? throw new DBException("42102").Mix();
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
            name = wr.cx.NewNode(wr.Length, x.name.Trim(':'));
            if (x.name.EndsWith(':'))
                name += ':';
            tabledefpos = wr.cx.Fix(x.tabledefpos);
            var bs = BList<DBObject>.Empty;
            for (var b = x.columns.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var nc = wr.cx._Ob(wr.cx.Fix(p))?? throw new PEException("PE0098");
                    bs += nc;
                }
            columns = (Domain)wr.cx.Add(new Domain(-1L, wr.cx, Sqlx.ROW, bs, bs.Length));
            flags = x.flags;
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
                wr.PutLong(columns[j]??-1L);
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
                var rt = BList<long?>.Empty;
                var rs = CTree<long, Domain>.Empty;
                for (int j = 0; j < n; j++)
                {
                    var cp = rdr.GetLong();
                    rt += cp;
                    rs += (cp, rdr.context._Dom(cp) ?? Domain.Null); // for Log$ may be historical
                }
                columns = new Domain(-1L, rdr.context, Sqlx.ROW, rs, rt);
            }
            flags = (ConstraintType)rdr.GetInt();
            reference = rdr.GetLong();
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
            switch(that.type)
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
            var (nt,ph) = base.Commit(wr, tr);
            if (tr is not null && wr.cx.db.objects[((PIndex)ph).tabledefpos] is Table tb
                && wr.cx.db.objects[ph.ppos] is Level3.Index x)
            {
                if (tb.indexes[x.keys] is CTree<long, bool> ct)
                    wr.cx.db += (tb.defpos, tb + (Table.Indexes,tb.indexes + (x.keys, ct - ppos)));
                if (reference > 0 && wr.cx.db.objects[x.reftabledefpos] is Table rt)
                    wr.cx.db += (rt.defpos, rt + (Table.RefIndexes,rt.rindexes - ppos));
            }
            return (nt,ph);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>The string representation</returns>
        public override string ToString()
        {
            string r = GetType().Name + " "+ name;
            r = r + " on " + Pos(tabledefpos) + "(";
            for (int j = 0; j < columns.Length; j++)
                r += ((j > 0) ? "," : "") + DBObject.Uid(columns[j]??-1L);
            r += ") " + flags.ToString();
            if (reference >= 0)
                r += " refers to [" + Pos(reference) + "]";
            return r;
        }
        internal override DBObject? Install(Context cx, long p)
        {
            if (cx.db.objects[tabledefpos] is not Table tb)
                return null;
            var x = new Level3.Index(this, cx).Build(cx);
            var t = tb.indexes[x.keys] ?? CTree<long, bool>.Empty;
            tb += (Table.Indexes, tb.indexes + (x.keys, t + (x.defpos, true)));
            x += (DBObject.Infos, x.infos + (cx.role.defpos, new ObInfo("", Grant.Privilege.Execute)));
            for (var st = tb.super as Table; st != null; st = st.super as Table)
                for (var b = st.indexes[x.keys]?.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is Level3.Index sx && sx.rows is not null
                        && sx.flags.HasFlag(ConstraintType.PrimaryKey))
                        x += (Level3.Index.Tree, sx.rows);
            cx.Install(x, p);
            if (reference >= 0 && cx.db.objects[x.refindexdefpos] is Level3.Index rx)
            {
                rx += (DBObject.Dependents, rx.dependents + (x.defpos, true));
                var rt = (Table?)cx.db.objects[x.reftabledefpos] ?? throw new PEException("PE1435");
                var at = rt.rindexes[tb.defpos] ?? CTree<Domain, Domain>.Empty;
                rt += (Table.RefIndexes, rt.rindexes + (tb.defpos, at + (x.keys, rx.keys)));
                cx.Install(rt, p);
                cx.Install(rx, p);
            }
            var cs = BList<long?>.Empty;
            var kc = tb.keyCols;
            var fl = PColumn.GraphFlags.None;
            for (var b = x.keys.First(); b != null; b = b.Next())
                if (b.value() is long tc)
                {
                    cs += tc;
                    var c = (cx.db.objects[tc] as TableColumn) ?? throw new PEException("PE1437");
                    cx.Add(c);
                    fl |= c.flags;
                    kc += (tc, true);
                }
            if (fl.HasFlag(PColumn.GraphFlags.IdCol))
                tb += (NodeType.IdIx, x.defpos);
            if (fl.HasFlag(PColumn.GraphFlags.LeaveCol))
                tb = tb + (EdgeType.LeaveIx, x.defpos) + (EdgeType.LeavingType, x.reftabledefpos);
            if (fl.HasFlag(PColumn.GraphFlags.ArriveCol))
                tb = tb + (EdgeType.ArriveIx, x.defpos) + (EdgeType.ArrivingType, x.reftabledefpos);
            tb += (Table.KeyCols, kc);
            tb += (DBObject.LastChange, defpos);
            cx.Install(tb, p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.db += (tb.defpos, tb);
            cx.db += (x.defpos,x);
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
            ConstraintType fl, long rx, string af, long pp) :
            this(Type.PIndex1, nm, tb, cl, fl, rx, af, pp)
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
            ConstraintType fl, long rx, string af, long pp) :
            base(t, nm, tb, cl, fl, rx, pp)
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
    /// PIndex2 is used for adding metadata flags to an integrity or referential constraint
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
        /// <param name="rx">The defining position of the referenced index (or -1)</param>
        /// <param name="af">The adapter function</param>
        /// <param name="md">The metadata flags</param>
        /// <param name="db">The database</param>
        public PIndex2(string nm, Table tb, Domain cl,
            ConstraintType fl, long rx, string af, ulong md, long pp) :
            this(Type.PIndex2, nm, tb, cl, fl, rx, af, md, pp)
        {
        }
        protected PIndex2(Type t, string nm, Table tb, Domain cl,
            ConstraintType fl, long rx, string af, ulong md, long pp)
            : base(t, nm, tb, cl, fl, rx, af, pp)
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
            wr.PutLong((long)metadata);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            metadata = (ulong)rdr.GetLong();
            base.Deserialise(rdr);
        }
    }
    internal class RefAction : Physical
    {
        public PIndex.ConstraintType ctype;
        public long index;
        public RefAction(long ix, PIndex.ConstraintType ct, long pp)
            : base(Type.RefAction, pp) 
        {
            index = ix;
            ctype = ct;
        }
        public RefAction(Reader rdr) : base(Type.RefAction,rdr) { }
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
            if (!Committed(wr,index)) return index;
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
                    var dp = (Drop)that; if(dp.delpos == index)
                        return new DBException("40025", index, that, ct);
                    break;
                case Type.Delete:
                case Type.Delete1:
                    {
                        var x = (Level3.Index?)db.objects[index]??throw new PEException("PE1412");
                        var dl = (Delete)that; if (dl.tabledefpos == x.tabledefpos)
                            return new DBException("40077", index, that, ct);
                        break;
                    }
                case Type.Update:
                case Type.Update1:
                    {
                        var x = (Level3.Index?)db.objects[index] ?? throw new PEException("PE1412");
                        var up = (Update)that; if (up.tabledefpos == x.tabledefpos)
                            return new DBException("40077", index, that, ct);
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override DBObject? Install(Context cx, long p)
        {
            var x = (Level3.Index?)cx.db.objects[index]??throw new DBException("42000");
            var od = x.flags & PIndex.Deletes;
            var ou = x.flags & PIndex.Updates;
            var oc = x.flags & ~PIndex.Cascade;
            var nd = ctype & PIndex.Deletes;
            var nu = ctype & PIndex.Updates;
            var nc = ctype & ~PIndex.Cascade;
            var nt = (oc | nc) | ((nd == PIndex.ConstraintType.NoType) ? od : nd) 
                | ((nu == PIndex.ConstraintType.NoType) ? ou : nu);
            x += (Level3.Index.IndexConstraint, nt);
            cx.db += (x, p);
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