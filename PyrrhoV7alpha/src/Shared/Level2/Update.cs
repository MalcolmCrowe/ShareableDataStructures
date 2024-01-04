using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System;
using System.Diagnostics.CodeAnalysis;

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
	/// A Level 2 Update record for the physical database
    /// Only changed values need be in the Update as serialised on disk. Fields not in this update are searched for in oldRec
    /// On install, we store the updated TableRow in table->tableRows
	/// </summary>
	internal class Update : Record
	{
        public long _defpos;
        public long prev;
        public TableRow? prevrec;
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
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
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
                        for (var b = de.deC[tabledefpos]?.First();b is not null;b=b.Next())
                        if (db.objects[tabledefpos] is Table tb && tb.FindIndex(db, b.key())?[0] is Level3.Index x
                            && x.MakeKey(fields) is CList<TypedValue> pk && x.rows?.Contains(pk)==true)
                                return new DBException("40085", de.delpos);
                        break;
                    }
                case Type.Update1:
                case Type.Update:
                    {
                        var u = (Update)that;
                        for (var b = u.riC[tabledefpos]?.First(); b != null; b = b.Next())
                            if (u.prevrec is TableRow pr && 
                                pr.MakeKey(b.value().rowType) is CList<TypedValue> pk &&
                                pk.CompareTo(MakeKey(b.key().rowType)) == 0 )
                                // conflict if our old values are referenced by a new foreign key
                                    throw new DBException("40014", u.prevrec.ToString());
                        if (db.objects[u.tabledefpos] is Table tb)
                        for (var b = tb.indexes.First(); b != null; b = b.Next())
                         if (tb.FindIndex(db, b.key())?[0] is Level3.Index x && 
                                    db.objects[x.refindexdefpos] is Level3.Index rx)
                            // conflict if this updated one of our foreign keys
                                for (var xb = rx?.keys.First();
                                        xb != null; xb = xb.Next())
                                    if (xb.value() is long p && fields.Contains(p))
                                        throw new DBException("40086", u.ToString());
                        // conflict on columns in matching rows
                        if (defpos != u.defpos)
                            return null;
                        for (var b = fields.First(); b != null; b = b.Next())
                            if (u.fields.Contains(b.key()))
                                return new DBException("40029", defpos, that, ct);
                        return null; // do not call the base
                    }
                case Type.Alter3:
                    if (((Alter3)that).table is Table t3 && t3.defpos == tabledefpos)
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.Alter2:
                    if (((Alter2)that).table is Table t2 && t2.defpos == tabledefpos)
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.Alter:
                    if (((Alter)that).table is Table t && t.defpos == tabledefpos)
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn:
                    if (((PColumn)that).table is Table tc && tc.defpos == tabledefpos)
                        return new DBException("40045", defpos, that, ct);
                    break;

            }
            return base.Conflicts(db, cx, that, ct);
        }
        public override DBException? Conflicts(CTree<long, bool> t,PTransaction ct)
        {
            for (var b = t.First(); b != null; b = b.Next())
                if (fields.Contains(b.key()))
                    return new DBException("40006",b.key(),this,ct).Mix();
            return null;
        }
        protected override TableRow Now(Context cx)
        {
            var tb = cx._Ob(tabledefpos) as Table ?? throw new PEException("PE40407");
            prevrec = tb.tableRows[defpos] ?? throw new DBException("42105");
            Check(cx);
            return new TableRow(this,cx,prevrec);
        }
        internal override void Check(Context cx)
        {
            if (cx._Ob(tabledefpos) is not Table tb) throw new DBException("42105");
            var dm = tb._PathDomain(cx);
            for (var b = dm.First(); b != null; b = b.Next())
            {
                if (b.value() is not long p || dm.representation[p] is not Domain dv)
                    throw new PEException("PE10701");
                if (fields[p] is TypedValue v
                    && v != TNull.Value && !v.dataType.EqualOrStrongSubtypeOf(dv))
                {
                    var nv = dm.Coerce(cx, v);
                    fields += (p, nv);
                }
            }
        }
        internal override void AddRow(Table tt, TableRow now, Context cx)
        {
            if (prevrec is null) throw new PEException("PE40408");
  /*          if (tt.defpos == tabledefpos && tt is NodeType tn
                    && tn.rowType[0] is long p && now.vals[p] is TChar id
                    && prevrec.vals[p] is TChar od && id.value != od.value
                    && cx.db.nodeIds[id.value] is TypedValue tv)
            {
                if (tn is EdgeType te)
                    cx.db = cx.db - (TEdge)tv + new TEdge(defpos, te, now.vals);
                else
                    cx.db = cx.db - (TNode)tv + new TNode(defpos, tn, now.vals);
            } */
            for (var xb = tt.indexes.First(); xb != null; xb = xb.Next())
                for (var c = xb.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Level3.Index x
                        && x.MakeKey(prevrec.vals) is CList<TypedValue> ok
                        && x.MakeKey(now.vals) is CList<TypedValue> nk
                        && ok.CompareTo(nk) != 0)
                    {
                        x -= (ok, defpos);
                        x += (nk, defpos);
                        cx.db += (x, cx.db.loadpos);
                    }
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
            if (cx.db==null || cx.db.user?.defpos != cx.db.owner)
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
        internal override DBObject? Install(Context cx, long p)
        {
            if (cx.db != null)
            {
                if (cx.db.objects[tabledefpos] is Table tb)
                {
                    var fl = tb.tableRows[defpos] ?? throw new PEException("PE40406");
                    Check(cx);
                    cx.Install(tb + new TableRow(this, cx, fl, _classification).Check(tb,cx), p);
                }
                if (cx.db.mem.Contains(Database.Log))
                    cx.db += (Database.Log, cx.db.log + (ppos, type));
            }
            return null;
        }
        public override void Serialise(Writer wr)
        {
            Level.SerialiseLevel(wr,_classification);
            base.Serialise(wr);
        }
    }
}
