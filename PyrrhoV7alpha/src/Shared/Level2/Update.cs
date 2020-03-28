using System;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Text;

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
	/// A Level 2 Update record for the physical database
    /// Only changed values need be in the Update as serialised on disk. Fields not in this update are searched for in oldRec
    /// On install, we store the updated TableRow in schemaRole->TableRows
    /// but we do not update our copy of oldRow
	/// </summary>
	internal class Update : Record
	{
        public long _defpos;
        public long prev;
        /// <summary>
        /// Constructor: an UPDATE from the Parser
        /// </summary>
        /// <param name="old">The current TableRow</param>
        /// <param name="tb">The defining position of the table</param>
        /// <param name="fl">The changed fields and values</param>
        /// <param name="u">The new record position</param>
        /// <param name="db">The transaction</param>
        public Update(TableRow old, Table tb, BTree<long, TypedValue> fl, long pp, 
            Context cx)
            : this(Type.Update, old, tb, fl, pp, cx)
        { }
        protected Update(Type t, TableRow old, Table tb,BTree<long,TypedValue> fl, 
            long pp, Context cx)
            : base(t,tb,fl,pp,cx)
        {
            _defpos = old.defpos;
            prev = old.prev;
        }
        public Update(Reader rdr) : base(Type.Update, rdr) { }
        protected Update(Type t, Reader rdr) : base(t, rdr) { }
        protected Update(Update x, Writer wr) : base(x, wr)
        {
            _defpos = wr.Fix(x._defpos);
            prev = wr.Fix(x.prev);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Update(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
		public override void Serialise(Writer wr)
		{
            wr.PutLong(prev);
            wr.PutLong(_defpos);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            prev = rdr.GetLong();
            _defpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.Drop:
                    {
                        var d = (Drop)that;
                        if (d.delpos == tabledefpos)
                            return ppos;
                        for (var b = fields.PositionAt(0); b != null; b = b.Next())
                            if (b.key() == d.delpos)
                                return ppos;
                        break;
                    }
                case Type.Delete:
                    return (((Delete)that).delpos == defpos) ? ppos : -1;
                case Type.Update1:
                case Type.Update:
                    return (((Update)that).defpos == defpos) ? ppos : -1;
                case Type.Alter3:
                case Type.Alter2:
                case Type.Alter:
                    return (((Alter)that).tabledefpos == tabledefpos) ? ppos : -1;
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn:
                    return (((PColumn)that).tabledefpos == tabledefpos) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }
        internal override TableRow AddRow(Context cx)
        {
            var tb = (Table)cx.db.objects[tabledefpos];
            var was = tb.tableRows[defpos];
            var now = new TableRow(this, cx.db, was);
            var same = true;
            for (var b = fields.First(); same && b != null; b = b.Next())
                if (tb.keyCols.Contains(b.key()))
                    same = b.value().CompareTo(was.vals[b.key()]) == 0;
            if (same)
                return now;
            var ro = cx.db.role;
            was.Cascade(cx.db, cx, ro, 0, Drop.DropAction.Restrict, fields);
            for (var xb = tb.indexes.First(); xb != null; xb = xb.Next())
            {
                var x = (Index)cx.db.objects[xb.value()];
                var ok = x.MakeKey(was);
                var nk = x.MakeKey(now);
                if (((x.flags & (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique)) != 0)
                    && x.rows.Contains(nk))
                    throw new DBException("2300", "duplicate key", nk);
                if (x.reftabledefpos >= 0)
                {
                    var rx = (Index)cx.db.objects[x.refindexdefpos];
                    if (!rx.rows.Contains(nk))
                        throw new DBException("23000", "missing foreign key ", nk);
                }
                if (ok._CompareTo(nk) != 0)
                {
                    x -= (ok, defpos);
                    x += (nk, defpos);
                    cx.db += (x, cx.db.loadpos);
                }
            }
            return now;
        }
        internal override void Install(Context cx, long p)
        {
            var fl = AddRow(cx);
            cx.db+=((Table)cx.db.objects[tabledefpos]+new TableRow(this, cx.db, fl),p);
        }
        public override long Affects => _defpos;
        public override long defpos => _defpos;
        public override string ToString()
        {
            return base.ToString() + " Prev:" + Pos(prev);
        }
    }
    internal class Update1 : Update
    {

        public Update1(TableRow old, Table tb, BTree<long, TypedValue> fl, Level lv, 
            long pp, Context cx) 
            : base(Type.Update1,old, tb, fl, pp, cx)
        {
            _classification = lv;
        }
        public Update1(Reader rdr) : base(Type.Update1, rdr)
        {
        }

        public Update1(TableRow old, BTree<long, TypedValue> fl, Table tb, Level lv, 
            long pp, Context cx) 
            : base(Type.Update1, old,tb,  fl, pp, cx)
        {
            _classification = lv;
        }

        protected Update1(Type t, Reader rdr) : base(t, rdr)
        {
        }

        protected Update1(Type t, TableRow old, Table tb, BTree<long, TypedValue> fl, 
            long pp, Context cx) 
            : base(t, old, tb, fl, pp, cx)
        {
        }
        protected Update1(Update1 x, Writer wr) : base(x, wr)
        {
            _classification = x._classification;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Update1(this, wr);
        }
        public override void Deserialise(Reader rdr)
        {
            _classification = Level.DeserialiseLevel(rdr);
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            Level.SerialiseLevel(wr,_classification);
            base.Serialise(wr);
        }
    }
}
