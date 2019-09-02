using System;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Text;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
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
        /// <summary>
        /// The old TableRow (immutable)
        /// </summary>
        public TableRow oldRow;
        /// <summary>
        /// Constructor: an UPDATE from the Parser
        /// </summary>
        /// <param name="old">The current TableRow</param>
        /// <param name="tb">The defining position of the table</param>
        /// <param name="fl">The changed fields and values</param>
        /// <param name="u">The new record position</param>
        /// <param name="db">The transaction</param>
        public Update(TableRow old, Table tb, BTree<long, TypedValue> fl, long u,Transaction db)
            : this(Type.Update, old, tb, fl, u,db)
        { }
        protected Update(Type t, TableRow old, Table tb,BTree<long,  TypedValue> fl, 
            long u,Transaction db)
            : base(t,tb,fl,u,db)
        {
            oldRow = old;
        }
        public Update(Reader rdr) : base(Type.Update, rdr) { }
        protected Update(Type t, Reader rdr) : base(t, rdr) { }
        protected Update(Update x, Writer wr) : base(x, wr)
        {
            oldRow = x.oldRow;
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
            wr.PutLong(wr.Fix(oldRow.ppos));
            wr.PutLong(wr.Fix(oldRow.defpos));
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            rdr.GetLong();
            var dp = rdr.GetLong();
            base.Deserialise(rdr);
            var tb = (Table)rdr.role.objects[tabledefpos];
            oldRow = tb.tableRows[dp];
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
                        for (var b = fields.First(); b != null; b = b.Next())
                            if (b.key() == d.delpos)
                                return ppos;
                        break;
                    }
                case Type.Delete:
                    return (((Delete)that).delRow.defpos == defpos) ? ppos : -1;
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
        internal override Database Install(Database db, Role ro, long p)
        {
            var tb = (Table)db.role.objects[tabledefpos];
            tb += new TableRow(this, db);
            return db + (db.schemaRole, tb);
        }
    }
    internal class Update1 : Update
    {

        public Update1(TableRow old, Table tb, BTree<long, TypedValue> fl, Level lv, 
            long u, Transaction db) 
            : base(Type.Update1,old, tb, fl, u,db)
        {
            _classification = lv;
        }
        public Update1(Reader rdr) : base(Type.Update1, rdr)
        {
        }

        public Update1(TableRow old, BTree<long, TypedValue> fl, Table tb, Level lv, 
            long u,Transaction db) 
            : base(Type.Update1, old,tb,  fl, u, db)
        {
            _classification = lv;
        }

        protected Update1(Type t, Reader rdr) : base(t, rdr)
        {
        }

        protected Update1(Type t, TableRow old, Table tb, BTree<long, TypedValue> fl, 
            long u, Transaction db) 
            : base(t, old, tb, fl, u, db)
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
