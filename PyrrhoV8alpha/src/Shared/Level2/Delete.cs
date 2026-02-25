using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
using Pyrrho.Common;

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
	/// A Delete entry notes a base table record to delete.
    /// The process is complicated because of relationships between
    /// records in a transaction (indexing, supertypes etc)
    /// Deprecated: Delete1 is much faster since v7
	/// </summary>
	internal class Delete : Physical
	{
        public long delpos; // the record being deleted
        public long tabledefpos; // its base table
        internal override long TableDefPos => tabledefpos;
        public TableRow? delrec; // the TableRow if provided
        public override long _table => tabledefpos;
        protected CTree<long, bool> suT = CTree<long, bool>.Empty;
        protected CTree<long, bool> sbT = CTree<long, bool>.Empty;
        public CTree<long, CTree<long, CTree<long, bool>>> siC
            = CTree<long, CTree<long, CTree<long, bool>>>.Empty;
        public override long Dependent(Writer wr, Transaction tr)
        {
            var dp = wr.cx.Fix(delpos);
            if (!Committed(wr, dp)) return dp;
            for (var b = suT.First(); b != null; b = b.Next())
                if (!Committed(wr, b.key())) return b.key();
            return -1;
        }
        /// <summary>
        /// Constructor: a new Delete request from the engine
        /// </summary>
        /// <param name="cx">The Context</param>
        /// <param name="rw">The table row</param>
        /// <param name="pp">The transaction position of the Delete Physical</param>
        public Delete(Context cx, TableRow rw, long pp)
            : this(Type.Delete, rw, pp, cx) { }
        protected Delete(Type t, TableRow rw, long pp, Context cx)
            : base(t, pp, cx.db)
        {
            tabledefpos = rw.tabledefpos;
            delrec = rw;
            delpos = rw.defpos;
            if (cx._Ob(tabledefpos) is Table tb)
            {
                for (var b = tb.super.First(); b != null; b = b.Next())
                    suT += (b.key().defpos, true);
                siC += tb.sindexes;
                siC += tb.sindexes;
                for (var b = tb.subtypes.First(); b != null; b = b.Next())
                    sbT += (b.key(), true);
            }
        }
        /// <summary>
        /// Constructor: a new Delete request from the buffer
        /// </summary>
        /// <param name="rdr">The Reader for the file</param>
		public Delete(Reader rdr) : base(Type.Delete, rdr) { }
        protected Delete(Type t,Reader rdr) : base(t, rdr) { }
        protected Delete(Delete x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.cx.Fix(x.tabledefpos);
            delpos = wr.cx.Fix(x.delpos);
            suT = wr.cx.FixTlb(x.suT);
        }
        /// <summary>
        /// Prepare the Delete Physical for Commit to the disk file
        /// </summary>
        /// <param name="wr"></param>
        /// <returns></returns>
        protected override Physical Relocate(Writer wr)
        {
            return new Delete(this, wr);
        }
        /// <summary>
        /// The affected record
        /// </summary>
		public override long Affects
		{
			get
			{
				return delpos;
			}
		}
        /// <summary>
        /// Serialise the Delete to the transaction log
        /// </summary>
        /// <param name="wr">The Writer for the file</param>
        public override void Serialise(Writer wr)
		{
            var dp = wr.cx.Fix(delpos);
            wr.PutLong(dp);
            wr.cx.affected ??= Rvv.Empty;
            wr.cx.affected += (tabledefpos, (dp, ppos));
            for (var b=suT.First();b!=null;b=b.Next())
               wr.cx.affected += (b.key(), (dp, ppos));
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise the Delete from the buffer
        /// </summary>
        /// <param name="rdr">The Reader for the file</param>
        public override void Deserialise(Reader rdr)
        {
            var dp = rdr.GetLong();
            base.Deserialise(rdr);
            delpos= dp;
        }
        /// <summary>
        /// A readable version of the Delete
        /// </summary>
        /// <returns>The string representation</returns>
		public override string ToString()
        {
            return "Delete Record "+Pos(delpos);
        }
        /// <summary>
        /// During the validation step for the transaction, the engine
        /// fetches all Physicals committed by other threads since the transcation start.
        /// These are examined for conflict with this Physical.
        /// </summary>
        /// <param name="db">The Database</param>
        /// <param name="cx">The Context</param>
        /// <param name="that">A possibly conflicting Physical</param>
        /// <param name="ct">The enclosinh transacton</param>
        /// <returns>An exception to raise (if any)</returns>
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.Delete:
                case Type.Delete1:
                case Type.Delete2:
                    if (((Delete)that).delpos == delpos)
                        return new DBException("40014", delpos, that, ct);
                    break;
                case Type.Update:
                case Type.Update1:
                case Type.Update2:
                    {
                        // conflict if the update targets our delpos
                        var u = (Update)that;
                        if (u._defpos == delpos)
                            return new DBException("40029", delpos, that, ct);
                        if (cx.db.objects[tabledefpos] is Table nt)
                        {
                            if (nt.sindexes.Contains(delpos))
                                return new DBException("40074", delpos, that, ct);
                            var ti = nt.infos[nt.definer] ?? throw new PEException("PE69101");
                            for (var a = (ti.metadata[Qlx.REFERENCES] as TSet)?.First(); a != null; a = a.Next())
                                if (a.Value() is TConnector tc && u.fields[tc.cp]?.ToLong() == delpos)
                                    return new DBException("40074", delpos, that, ct);
                        }
                        break;
                    }
                case Type.Record: 
                case Type.Record2:
                case Type.Record3:
           //     case Type.Record4:
                    {
                        // conflict if we refer to the deleted row
                        var r = (Record)that;
                        if (r.defpos == delpos)
                            return new DBException("40029", delpos, that, ct);
                        if (cx.db.objects[tabledefpos] is Table nt && nt.infos[nt.definer] is ObInfo ni)
                            for (var a = (ni.metadata[Qlx.REFERENCES] as TSet)?.First(); a != null; a = a.Next())
                                if (a.Value() is TConnector tc && r.fields[tc.cp]?.ToLong() == delpos)
                                    return new DBException("40027", delpos, that, ct);
                        break;
                    } 
                case Type.EditType:
                    {
                        if (((EditType)that).defpos == tabledefpos)
                            return new DBException("40025", tabledefpos, that, ct);
                        break;
                    }
            }
            return null;
        }
        /// <summary>
        /// Provided for legacy database files.
        /// Delete1 knows the right table to find the record in
        /// </summary>
        /// <param name="db"></param>
        /// <param name="ro"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.db.role;
            Table? rt = null;
            if (ro == null || cx.db == null) throw new DBException("42105").Add(Qlx.DELETE_STATEMENT);
            for (var ob = ro.dbobjects.First(); ob != null; ob = ob.Next())
                if (ob.value() is long op && cx.db.objects[op] is Table tb
                    && tb.tableRows[delpos] is TableRow delRow && tb.infos[tb.definer] is ObInfo ti)
                {
                    rt = tb;
                    for (var b = tb.indexes.First(); b != null; b = b.Next())
                        for (var c = b.value().First(); c != null; c = c.Next())
                            if (cx.db.objects[c.key()] is Level3.Index ix &&
                                ix.rows is MTree mt && mt.info is Domain inf &&
                                delRow.MakeKey(ix) is CList<TypedValue> key)
                            {
                                ix -= key;
                                if (ix.rows == null)
                                    ix += (Level3.Index.Tree,
                                        new MTree(inf, mt.nullsAndDuplicates, 0));
                                cx.Install(ix);
                            }
                    for (var b=(ti.metadata[Qlx.REFERENCES] as TSet)?.First();b!=null;b=b.Next())
                        if (b.Value() is TConnector tc
                    && cx.db.objects[tc.cp] is TableColumn co
                    && cx.db.objects[co.domain.alts.First()?.key().defpos ?? -1L] is Table rt1)
                        {
                            rt = rt1; // returning rt is silly: there may be several
                            var os = rt.sindexes;
                            var ol = delRow.vals[tc.cp]?.ToLong() ?? -1L;
                            var om = (os[ol] ?? CTree<long, CTree<long, bool>>.Empty) - tc.cp;
                            if (om == CTree<long, CTree<long, bool>>.Empty)
                                os -= ol;
                            else
                                os += (ol, om);
                            rt += (Table.SysRefIndexes, os);
                            cx.Add(rt);
                            cx.db += rt;
                        }
                    tb -= delpos;
                    tb += (Table.LastData, ppos);
                    cx.Install(tb);
                }
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return rt;
        }
    }
    /// <summary>
    /// An improved version of Delete: to delete a record in a base table
    /// </summary>
    internal class Delete1 : Delete
    {
        /// <summary>
        /// Constructor: a new Delete request from the engine
        /// </summary>
        /// <param name="rw">The TableRow to delete</param>
        /// <param name="pp">The transaction position of this Physical</param>
        /// <param name="cx">The Context</param>
        public Delete1(TableRow rw, long pp, Context cx)
            :this(Type.Delete1, rw, pp, cx)
        { }
        protected Delete1(Type t,TableRow rw, long pp, Context cx)
            : base(t, rw, pp, cx)
        {
            tabledefpos = rw.tabledefpos;
            delpos = rw.defpos;
        }
        /// <summary>
        /// Constructor: a new Delete request from the buffer
        /// </summary>
        /// <param name="rdr">The Reader for the file</param>
		public Delete1(Reader rdr) : base(Type.Delete1, rdr) { }
        protected Delete1(Type t,Reader rdr) : base(t, rdr) { }
        protected Delete1(Delete1 x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.cx.Fix(x.tabledefpos);
            delpos = wr.cx.Fix(x.delpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Delete1(this, wr);
        }
        /// <summary>
        /// Serialise the Delete to the PhysBase
        /// </summary>
        /// <param name="wr">The Writer for the file</param>
        public override void Serialise(Writer wr)
        {
            wr.PutLong(tabledefpos);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise the Delete from the buffer
        /// </summary>
        /// <param name="rdr">The Reader for the file</param>
        public override void Deserialise(Reader rdr)
        {
            tabledefpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.db.role;
            if (ro == null || cx.db == null) throw new DBException("42105").Add(Qlx.DELETE_STATEMENT);
            if (cx.db.objects[tabledefpos] is not Table tb
                || tb.tableRows[delpos] is not TableRow delRow)
                throw new PEException("PE10171");
            if (tb.sindexes[delpos]?.First()?.key() is long rp)
            {
                var xt = (cx.db.objects[rp] as TableColumn)?.tabledefpos ?? -1L;
                if (cx.db is Transaction)
                    throw new DBException("23001", delpos, cx.NameFor(xt) ?? "");
                else
                    throw new DBException("40075");
            }
            for (var b = tb.indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Level3.Index ix &&
                        ix.rows is MTree mt && mt.info is Domain inf &&
                        delRow.MakeKey(ix) is CList<TypedValue> key)
                    {
                        ix -= key;
                        if (ix.rows == null)
                            ix += (Level3.Index.Tree,
                                new MTree(inf, mt.nullsAndDuplicates, 0));
                        cx.Install(ix);
                    }
            for (var b = tb.rowType.First(); b != null; b = b.Next())
                if (cx._Ob(b.value()) is TableColumn co 
                && cx._Ob(co.toType) is Table rt)
                {
                    var os = rt.sindexes;
                    var cp = co.defpos;
                    var ol = delRow.vals[cp]?.ToLong() ?? -1L;
                    var om = os[ol] ?? CTree<long, CTree<long, bool>>.Empty;
                    var on = om[cp] ?? CTree<long, bool>.Empty;
                    on -= delrec?.defpos ?? delpos;
                    if (on == CTree<long, bool>.Empty)
                        om -= cp;
                    else
                        om += (cp, on);
                    if (om == CTree<long, CTree<long, bool>>.Empty)
                        os -= ol;
                    else
                        os += (ol, om);
                    rt += (Table.SysRefIndexes, os);
                    cx.Add(rt);
                    cx.db += rt;
                }
            tb -= delpos;
            tb += (Table.LastData, ppos);
            cx.Install(tb);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return tb;
        }
        public override string ToString()
        {
            return base.ToString()+"["+tabledefpos+"]";
        }
    }
}
