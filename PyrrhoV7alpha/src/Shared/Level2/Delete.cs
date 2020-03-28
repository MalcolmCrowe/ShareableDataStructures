using System;
using Pyrrho.Level1;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
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
        public override long Dependent(Writer wr, Transaction tr)
        {
            var dp = wr.Fix(delpos);
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
            delpos = rw.defpos;
		}
        protected Delete(Type t,TableRow rw, long pp, Context cx)
    : base(t, pp, cx)
        {
            tabledefpos = rw.tabledefpos;
            delpos = rw.defpos;
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
            tabledefpos = wr.Fix(x.tabledefpos);
            delpos = wr.Fix(x.delpos);
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
  //          wr.PutLong(wr.Fix(tabledefpos));
            wr.PutLong(wr.Fix(delpos));
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise the Delete from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(Reader rdr)
        {
  //          var tb = rdr.GetLong();
            var dp = rdr.GetLong();
            base.Deserialise(rdr);
  //          tabledefpos = tb;
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
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch (that.type)
            {
                case Type.Delete:
                    return (((Delete)that).delpos == delpos) ? ppos : -1;
                case Type.Update:
                    return (((Update)that)._defpos == delpos) ? ppos : -1;
            }
            return -1;
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
                    var delRow = tb.tableRows[delpos] as TableRow;
                    ro = delRow.Cascade(cx.db, cx, ro, p);
                    for (var b = tb.indexes.First(); b != null; b = b.Next())
                    {
                        var ix = (Index)cx.db.objects[b.value()];
                        var inf = ix.rows.info;
                        var key = delRow.MakeKey(ix);
                        ix -= key;
                        if (ix.rows == null)
                            ix += (Index.Tree, new MTree(inf));
                        cx.db += (ix,p);
                    }
                    tb -= delpos;
                    cx.db += (tb, p);
                }
        }
    }
    internal class Delete1 : Delete
    {
        /// <summary>
        /// Constructor: a new Delete request from the engine
        /// </summary>
        /// <param name="rc">The defining position of the record</param>
        /// <param name="tb">The local database</param>
        public Delete1(TableRow rw, long pp, Context cx)
            : base(Type.Delete1, rw, pp, cx)
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
            tabledefpos = wr.Fix(x.tabledefpos);
            delpos = wr.Fix(x.delpos);
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
            wr.PutLong(wr.Fix(tabledefpos));
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
            var delRow = tb.tableRows[delpos] as TableRow;
            var ro = delRow.Cascade(cx.db, cx, cx.role, p);
            for (var b = tb.indexes.First(); b != null; b = b.Next())
            {
                var ix = (Index)cx.db.objects[b.value()];
                var inf = ix.rows.info;
                var key = delRow.MakeKey(ix);
                ix -= key;
                if (ix.rows == null)
                    ix += (Index.Tree, new MTree(inf));
                cx.db += (ix, p);
            }
            tb -= delpos;
            cx.db = cx.db + (tb, p) + (ro,p);
        }
        public override string ToString()
        {
            return base.ToString()+"["+tabledefpos+"]";
        }
    }
}
