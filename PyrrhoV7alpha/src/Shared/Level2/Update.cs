using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System;

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
	/// A Level 2 Update record for the physical database
    /// Only changed values need be in the Update as serialised on disk. Fields not in this update are searched for in oldRec
    /// On install, we store the updated TableRow in table->tableRows
	/// </summary>
	internal class Update : Record
	{
        public long _defpos;
        public long prev;
        public TableRow prevrec;
        /// <summary>
        /// Constructor: an UPDATE from the Parser
        /// </summary>
        /// <param name="old">The current TableRow</param>
        /// <param name="tb">The defining position of the table</param>
        /// <param name="fl">The changed fields and values</param>
        /// <param name="u">The new record position</param>
        /// <param name="db">The transaction</param>
        public Update(TableRow old, Table tb, CTree<long, TypedValue> fl, long pp, 
            Context cx)
            : this(Type.Update, old, tb, fl, pp, cx)
        { }
        protected Update(Type t, TableRow old, Table tb,CTree<long,TypedValue> fl, 
            long pp, Context cx)
            : base(t,tb,fl,pp,cx)
        {
            _defpos = old.defpos;
            prevrec = old;
            if (t!=Type.Update1)
                _classification = old.classification;
            prev = old.ppos;
        }
        public Update(Reader rdr) : base(Type.Update, rdr) { }
        protected Update(Type t, Reader rdr) : base(t, rdr) 
        {  }
        protected Update(Update x, Writer wr) : base(x, wr)
        {
            _defpos = wr.cx.Fix(x._defpos);
            prev = wr.cx.Fix(x.prev);
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
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Drop:
                    {
                        var d = (Drop)that;
                        if (d.delpos == tabledefpos)
                            return new DBException("40010", tabledefpos, that, ct);
                        for (var b = fields.PositionAt(0); b != null; b = b.Next())
                            if (b.key() == d.delpos)
                                return new DBException("40010", d.delpos, that, ct);
                        break;
                    }
                case Type.Delete:
                case Type.Delete1:
                    {
                        var de = (Delete)that;
                        if (de.delpos == defpos)
                            return new DBException("40029", defpos, that, ct);
                        for (var b = de.deC[tabledefpos]?.First();b!=null;b=b.Next())
                        {
                            var tb = (Table)db.objects[tabledefpos];
                            var x = tb.FindIndex(db, b.key())?[0];
                            if (x!=null && x.rows.Contains(x.MakeKey(fields)))
                                return new DBException("40085", de.delpos);
                        }
                        break;
                    }
                case Type.Update1:
                case Type.Update:
                    {
                        var u = (Update)that;
                        for (var b = u.riC[tabledefpos]?.First(); b != null; b = b.Next())
                        {
                            if (u.prevrec.MakeKey(b.value()).CompareTo(MakeKey(b.key())) == 0 )
                                // conflict if our old values are referenced by a new foreign key
                                    throw new DBException("40014", u.prevrec.ToString());
                        }
                        var tb = (Table)cx.db.objects[u.tabledefpos];
                        for (var b = tb.indexes.First(); b != null; b = b.Next())
                        {
                            // conflict if this updated one of our foreign keys
                            var x = tb.FindIndex(db, b.key())?[0];
                            if (x != null)
                                for (var xb = ((Index)cx.db.objects[x.refindexdefpos])?.keys.First();
                                        xb != null; xb = xb.Next())
                                    if (fields.Contains(xb.value()))
                                        throw new DBException("40086", u.ToString());
                        }
                        // conflict on columns in matching rows
                        if (defpos != u.defpos)
                            return null;
                        for (var b = fields.First(); b != null; b = b.Next())
                            if (u.fields.Contains(b.key()))
                                return new DBException("40029", defpos, that, ct);
                        return null; // do not call the base
                    }
                case Type.Alter3:
                    if (((Alter3)that).table.defpos == tabledefpos)
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.Alter2:
                    if (((Alter2)that).table.defpos == tabledefpos)
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.Alter:
                    if (((Alter)that).table.defpos == tabledefpos)
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn:
                    if (((PColumn)that).table.defpos == tabledefpos)
                        return new DBException("40045", defpos, that, ct);
                    break;

            }
            return base.Conflicts(db, cx, that, ct);
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
            for (var xb = tb.indexes.First(); xb != null; xb = xb.Next())
                for (var c=xb.value().First();c!=null;c=c.Next())
            {
                var x = (Index)cx.db.objects[c.key()];
                var ok = x.MakeKey(was.vals);
                var nk = x.MakeKey(now.vals);
                if (ok._CompareTo(nk) != 0)
                {
                    x -= (ok, defpos);
                    x += (nk, defpos);
                    cx.db += (x, cx.db.loadpos);
                }
            }
            return now;
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
        public Update1(TableRow old, Table tb, CTree<long, TypedValue> fl, Level lv, 
            long pp, Context cx) 
            : base(Type.Update1,old, tb, fl, pp, cx)
        {
            if (cx.db._user != cx.db.owner)
                throw new DBException("42105");
            _classification = lv;
        }
        public Update1(Reader rdr) : base(Type.Update1, rdr)
        {  }
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
        internal override void Install(Context cx, long p)
        {
            var fl = AddRow(cx);
            cx.Install((Table)cx.db.objects[tabledefpos] 
                + new TableRow(this, cx.db, fl, _classification), p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
        public override void Serialise(Writer wr)
        {
            Level.SerialiseLevel(wr,_classification);
            base.Serialise(wr);
        }
    }
}
