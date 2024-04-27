using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
	/// <summary>
	/// A Delete entry notes a base table record to delete.
    /// Deprecated: Delete1 is much faster since v7
	/// </summary>
	internal class Delete : Physical
	{
        public long delpos;
        public CTree<long,bool> tabledefpos = CTree<long,bool>.Empty;
        public TableRow? delrec;
        public CTree<long,CTree<Domain,Domain>> deC 
            = CTree<long,CTree<Domain,Domain>>.Empty;
        public override long Dependent(Writer wr, Transaction tr)
        {
            var dp = wr.cx.Fix(delpos);
            if (!Committed(wr, dp)) return dp;
            for (var b = tabledefpos.First(); b != null; b = b.Next())
                if (!Committed(wr, b.key())) return b.key();
            return -1;
        }
        /// <summary>
        /// Constructor: a new Delete request from the engine
        /// </summary>
        /// <param name="rc">The defining position of the record</param>
        /// <param name="tb">The local database</param>
        public Delete(TableRow rw, long pp)
            : base(Type.Delete, pp)
		{
            tabledefpos = rw.tabledefpos;
            delrec = rw;
            delpos = rw.defpos;
		}
        protected Delete(Type t,TableRow rw, long pp, Context cx)
    : base(t, pp)
        {
            tabledefpos = rw.tabledefpos;
            delrec = rw;
            delpos = rw.defpos;
            for (var b = tabledefpos.First();b!=null;b=b.Next())
                if (cx.db.objects[b.key()] is Table tb)
                    deC += tb.rindexes;
        }
        /// <summary>
        /// Constructor: a new Delete request from the buffer
        /// </summary>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">a defining position</param>
		public Delete(Reader rdr) : base(Type.Delete, rdr) { }
        protected Delete(Type t,Reader rdr) : base(t, rdr) { }
        protected Delete(Delete x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.cx.Fix(x.tabledefpos);
            delpos = wr.cx.Fix(x.delpos);
        }
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
        /// Serialise the Delete to the PhysBase
        /// </summary>
        /// <param name="r">Reclocation of position information</param>
        public override void Serialise(Writer wr)
		{
            var dp = wr.cx.Fix(delpos);
            wr.PutLong(dp);
            wr.cx.affected ??= Rvv.Empty;
            var ts = wr.cx.Fix(tabledefpos);
            for (var b=ts.First();b!=null;b=b.Next())
               wr.cx.affected += (b.key(), (dp, ppos));
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise the Delete from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(Reader rdr)
        {
            var dp = rdr.GetLong();
            base.Deserialise(rdr);
            delpos= dp;
        }
        internal override void Affected(ref BTree<long, BTree<long, long?>> aff)
        {
            for (var b = tabledefpos.First(); b != null; b = b.Next())
                if (aff[b.key()] is BTree<long, long?> ta)
                {
                    ta += (delpos, ppos);
                    aff += (b.key(), ta);
                }
        }
        /// <summary>
        /// A readable version of the Delete
        /// </summary>
        /// <returns>The string representation</returns>
		public override string ToString()
        {
            return "Delete Record "+Pos(delpos);
        }
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
                        // conflict if we refer to the deleted row
                        var u = (Update)that;
                        if (u._defpos == delpos)
                            return new DBException("40029", delpos, that, ct);
                        for (var t = tabledefpos.First(); t != null; t = t.Next())
                            for (var b = u.inC.First(); b != null; b = b.Next())
                                for (var c = b.value().First(); c != null; c = c.Next())
                                    if (db.objects[c.key()] is Level3.Index x &&
                                           x.reftabledefpos == t.key() &&
                                           db.objects[x.refindexdefpos] is Level3.Index rx &&
                                    u.MakeKey(rx.keys.rowType) is CList<TypedValue> k &&
                                    rx.rows is MTree mt && mt.Contains(k) != true)
                                        throw new DBException("40074", delpos, that, ct);
                        break;
                    }
                case Type.Record:
                case Type.Record2:
                case Type.Record3:
                case Type.Record4:
                    {
                        // conflict if we refer to the deleted row
                        var r = (Record)that;
                        if (r.defpos == delpos)
                            return new DBException("40029", delpos, that, ct);
                        for (var t = tabledefpos.First(); t != null; t = t.Next())
                            for (var b = r.inC.First(); b != null; b = b.Next())
                                for (var c = b.value().First(); c != null; c = c.Next())
                                    if (db.objects[c.key()] is Level3.Index x &&
                                    x.reftabledefpos == t.key() &&
                                    db.objects[x.refindexdefpos] is Level3.Index rx &&
                                    r.MakeKey(rx.keys.rowType) is CList<TypedValue> k &&
                                    rx.rows is MTree mt && mt.Contains(k) != true)
                                        throw new DBException("40027", delpos, that, ct);
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
        internal override DBObject? Install(Context cx, long p)
        {
            var ro = cx.db.role;
            Table? rt = null;
            if (ro == null || cx.db == null) throw new DBException("42105").Add(Sqlx.DELETE_STATEMENT);
            for (var ob = ro.dbobjects.First(); ob != null; ob = ob.Next())
                if (ob.value() is long op && cx.db.objects[op] is Table tb 
                    && tb.tableRows[delpos] is TableRow delRow)
                    {
                        rt = tb;
  /*                  if (tb.defpos == tabledefpos && tb is NodeType tn
                            && tn.rowType[0] is long q && delRow.vals[q] is TChar id
                            && cx.db.nodeIds[id.value] is TNode nn)
                        cx.db -= nn; */
                    for (var b = tb.indexes.First(); b != null; b = b.Next())
                            for (var c = b.value().First(); c != null; c = c.Next())
                                if (cx.db.objects[c.key()] is Level3.Index ix && 
                                    ix.rows is MTree mt && mt.info is Domain inf &&
                                    delRow.MakeKey(ix) is CList<TypedValue> key)
                                {
                                    ix -= key;
                                    if (ix.rows == null)
                                        ix += (Level3.Index.Tree, 
                                            new MTree(inf,mt.nullsAndDuplicates,0));
                                    cx.Install(ix, p);
                                }
                        tb -= delpos;
                        tb += (Table.LastData, ppos);
                        cx.Install(tb, p);
                    }

            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return rt;
        }
    }
    internal class Delete1 : Delete
    {
        /// <summary>
        /// Constructor: a new Delete request from the engine
        /// </summary>
        /// <param name="rc">The defining position of the record</param>
        public Delete1(TableRow rw, long pp, Context cx)
            : base(Type.Delete1, rw, pp, cx)
        {
            tabledefpos = rw.tabledefpos;
            delpos = rw.defpos;
        }
        public Delete1(Type t, TableRow rw, long pp, Context cx)
           : base(t, rw, pp, cx)
        {
            tabledefpos = rw.tabledefpos;
            delpos = rw.defpos;
        }
        /// <summary>
        /// Constructor: a new Delete request from the buffer
        /// </summary>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">a defining position</param>
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
        /// <param name="r">Reclocation of position information</param>
        public override void Serialise(Writer wr)
        {
            var tp = tabledefpos.Last()?.key()??throw new PEException("PE00805");
            wr.PutLong(tp);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise the Delete from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(Reader rdr)
        {
            var tb = rdr.GetLong();
            base.Deserialise(rdr);
            tabledefpos += (tb,true);
        }
        internal override DBObject? Install(Context cx, long p)
        {
            Table? r = null;
            for (var b = tabledefpos.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Table t && t.DoDel(cx, this, p) is Table u && t != u)
                    r = u;
            return r ?? throw new PEException("PE00806");
        }
        public override string ToString()
        {
            return base.ToString()+"["+tabledefpos+"]";
        }
    }
    internal class Delete2 : Delete1
    {
        public Delete2(Reader rdr) : base(Type.Delete2,rdr)
        { }

        public Delete2(TableRow old, long pp, Context cx)
            : base(Type.Delete2, old, pp, cx)
        { }

        protected Delete2(Delete1 x, Writer wr) : base(x, wr)
        { }

        protected override Physical Relocate(Writer wr)
        {
            return new Delete2(this, wr);
        }

        public override void Deserialise(Reader rdr)
        {
            var n = rdr.GetInt();
            for (var i = 0; i < n; i++)
                tabledefpos += (rdr.GetLong(), true);
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            var n = (int)tabledefpos.Count - 1;
            wr.PutInt(n);
            for (var b = tabledefpos.First(); b != null && n-- > 0; b = b.Next())
                wr.PutLong(b.key());
            base.Serialise(wr);
        }
    }
}
