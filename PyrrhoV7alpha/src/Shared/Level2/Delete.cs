using System;
using Pyrrho.Level1;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
	/// A Delete entry notes a base table record to delete.
    /// Deprecated: Delete1 is much faster since v7
	/// </summary>
	internal class Delete : Physical
	{
        public long delpos;
        public long tabledefpos;
        public TableRow delrec;
        public CTree<long,CTree<CList<long>,CList<long>>> deC 
            = CTree<long,CTree<CList<long>,CList<long>>>.Empty;
        public override long Dependent(Writer wr, Transaction tr)
        {
            var dp = wr.cx.Fix(delpos);
            if (!Committed(wr,dp)) return dp;
            if (!Committed(wr,tabledefpos)) return tabledefpos;
            return -1;
        }
        /// <summary>
        /// Constructor: a new Delete request from the engine
        /// </summary>
        /// <param name="rc">The defining position of the record</param>
        /// <param name="tb">The local database</param>
        public Delete(TableRow rw, long pp, Context cx)
            : base(Type.Delete, pp, cx)
		{
            tabledefpos = rw.tabledefpos;
            delrec = rw;
            delpos = rw.defpos;
		}
        protected Delete(Type t,TableRow rw, Table tb, long pp, Context cx)
    : base(t, pp, cx)
        {
            tabledefpos = rw.tabledefpos;
            delrec = rw;
            delpos = rw.defpos;
            deC = tb.rindexes;
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
            wr.cx.affected = (wr.cx.affected ?? Rvv.Empty) 
                + (wr.cx.Fix(tabledefpos), (dp, ppos));
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
        internal override void Affected(ref BTree<long, BTree<long, long>> aff)
        {
            var ta = aff[tabledefpos] ?? BTree<long, long>.Empty;
            ta += (delpos, ppos);
            aff += (tabledefpos, ta);
        }
        /// <summary>
        /// A readable version of the Delete
        /// </summary>
        /// <returns>The string representation</returns>
		public override string ToString()
        {
            return "Delete Record "+Pos(delpos);
        }
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.Delete:
                case Type.Delete1:
                    if (((Delete)that).delpos == delpos)
                        return new DBException("40014", delpos, that, ct);
                    break;
                case Type.Update:
                case Type.Update1:
                    {
                        var u = (Update)that;
                        if (u._defpos == delpos)
                            return new DBException("40029", delpos, that, ct);
                        // conflict if we refer to the deleted row
                        for (var b = u.inC.First(); b != null; b = b.Next())
                            {
                                var x = (Index)db.objects[b.value()];
                                if (x.reftabledefpos==tabledefpos)
                                {
                                    var rx = (Index)db.objects[x.refindexdefpos];
                                    if (!rx.rows.Contains(u.MakeKey(rx.keys)))
                                        throw new DBException("40074", delpos, that, ct);
                                }
                            }
                        break;
                    }
                case Type.Record:
                case Type.Record1:
                case Type.Record2:
                case Type.Record3:
                    {
                        // conflict if we refer to the deleted row
                        var dr = (TableRow)db.objects[delpos];
                        var r = (Record)that;
                        for (var b = r.inC.First(); b != null; b = b.Next())
                        {
                            var x = (Index)db.objects[b.value()];
                            if (x.reftabledefpos == tabledefpos)
                            {
                                var rx = (Index)db.objects[x.refindexdefpos];
                                if (!rx.rows.Contains(r.MakeKey(rx.keys)))
                                    throw new DBException("40027", delpos, that, ct);
                            }
                        }
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
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            for (var ob = ro.dbobjects.First(); ob != null; ob = ob.Next())
                if (cx.db.objects[ob.value()] is Table tb && tb.tableRows.Contains(delpos))
                {
                    var delRow = tb.tableRows[delpos];
                    for (var b = tb.indexes.First(); b != null; b = b.Next())
                    {
                        var ix = (Index)cx.db.objects[b.value()];
                        var inf = ix.rows.info;
                        var key = delRow.MakeKey(ix);
                        ix -= key;
                        if (ix.rows == null)
                            ix += (Index.Tree, new MTree(inf));
                        cx.Install(ix,p);
                    }
                    tb -= delpos;
                    tb += (Table.LastData, ppos);
                    cx.Install(tb,p);
                }
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
    }
    internal class Delete1 : Delete
    {
        /// <summary>
        /// Constructor: a new Delete request from the engine
        /// </summary>
        /// <param name="rc">The defining position of the record</param>
        /// <param name="tb">The local database</param>
        public Delete1(TableRow rw, Table tb, long pp, Context cx)
            : base(Type.Delete1, rw, tb, pp, cx)
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
            wr.PutLong(wr.cx.Fix(tabledefpos));
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
            tabledefpos = tb;
        }
        internal override void Install(Context cx, long p)
        {
            var tb = cx.db.objects[tabledefpos] as Table;
            var delRow = tb.tableRows[delpos];
    //        delRow.Cascade(cx.db, cx, cx.role, p); moved to TransitionRowSet
            for (var b = tb.indexes.First(); b != null; b = b.Next())
            {
                var ix = (Index)cx.db.objects[b.value()];
                var inf = ix.rows.info;
                var key = delRow.MakeKey(ix);
                ix -= (key,delpos);
                if (ix.rows == null)
                    ix += (Index.Tree, new MTree(inf));
                cx.Install(ix,p);
            }
            tb -= delpos;
            tb += (Table.LastData, ppos);
            cx.Install(tb,p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
        public override string ToString()
        {
            return base.ToString()+"["+tabledefpos+"]";
        }
    }
}
