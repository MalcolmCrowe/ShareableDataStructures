using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
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
        public Update(TableRow old, long tb, CTree<long, TypedValue> fl, long pp, 
            Context cx)
            : this(Type.Update, old, tb, fl, pp, cx)
        { }
        protected Update(Type t, TableRow old, long tb, CTree<long, TypedValue> fl,
            long pp, Context cx)
            : base(t, tb, fl, pp, cx)
        {
            _defpos = old.defpos;
            prevrec = old;
            if (t != Type.Update1)
                _classification = old.classification;
            prev = old.ppos;
            if (cx.db.objects[tb] is Table ta)
                sbT = ta.subtypes;
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
                        if (tabledefpos == d.delpos)
                            return new DBException("40010", tabledefpos, that, ct);
                        for (var b = fields.PositionAt(0); b != null; b = b.Next())
                            if (b.key() == d.delpos)
                                return new DBException("40010", d.delpos, that, ct);
                        break;
                    }
                case Type.Delete:
                case Type.Delete1:
                case Type.Delete2:
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
                case Type.Update2:
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
                    if (((Alter3)that).table is Table t3 && tabledefpos==t3.defpos)
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.Alter2:
                    if (((Alter2)that).table is Table t2 && tabledefpos == t2.defpos)
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.Alter:
                    if (((Alter)that).table is Table t && tabledefpos == t.defpos)
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.EditType:
                        if (((EditType)that).defpos == tabledefpos)
                            return new DBException("40025", tabledefpos, that, ct);
                        break;
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn:
                    if (((PColumn)that).table is Table tc && tabledefpos == tc.defpos)
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
            if (cx._Ob(tabledefpos) is Table tb && tb.tableRows[defpos] is TableRow tr)
            {
                prevrec = tr;
                tb.Update(cx, prevrec, fields);
            }
            if (prevrec is null)
                throw new PEException("PE00809");
            Check(cx);
            return new TableRow(this, cx, prevrec);
        }
        internal override void Check(Context cx)
        {
            if (cx._Ob(tabledefpos) is Table tb)
            {
                for (var b = tb.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    if (tb.representation[p] is not Domain dv)
                        throw new PEException("PE10701");
                    if (fields[p] is TypedValue v
                        && v != TNull.Value && !v.dataType.EqualOrStrongSubtypeOf(dv))
                    {
                        var nv = dv.Coerce(cx, v);
                        fields += (p, nv);
                    }
                }
            }
        }
        internal override Context Add(Context cx, Table tt, TableRow now)
        {
            if (tt.defpos < 0)
                return cx;
            cx = _Add(cx, tt, now);
            for (var b = subTables.First(); b != null; b = b.Next())   // update subtypes
                if (cx.db.objects[b.key()] is Table ta)
                    cx = _Add(cx, ta, now);
            return cx;
        }
        internal override Table AddRow(Table tt, TableRow now, Context cx)
        {
            tt += now;
            if (prevrec is null) throw new PEException("PE40408");
            if (tt is EdgeType et)
            {
                if (now.vals[et.leaveCol] == TNull.Value || now.vals[et.arriveCol] == TNull.Value)
                    throw new PEException("PE6901");
                if (et.FindPrimaryIndex(cx) is null)
                {
                    if (cx._Od(et.leavingType) is NodeType lt
                        && prevrec.vals[et.leaveCol] is TInt ol && ol.ToLong() is long lo
                       && now.vals[et.leaveCol] is TInt tl && tl.ToLong() is long li
                       && lo!=li)
                    {
                        var cn = (lt.sindexes-lo)[li] ?? CTree<long, CTree<long, bool>>.Empty;
                        var cc = cn[et.leaveCol] ?? CTree<long, bool>.Empty;
                        cc += (now.defpos, true);
                        cn += (et.leaveCol, cc);
                        lt += (Table.SysRefIndexes, lt.sindexes + (li, cn));
                        cx.Add(lt);
                        cx.db += lt;
                    }
                    if (cx._Od(et.arrivingType) is NodeType at
                        && prevrec.vals[et.leaveCol] is TInt oa && oa.ToLong() is long ao
                        && now.vals[et.arriveCol] is TInt ta && ta.ToLong() is long ai
                        && ao!=ai)
                    {
                        var cn = (at.sindexes-ao)[ai] ?? CTree<long, CTree<long, bool>>.Empty;
                        var cc = cn[et.arriveCol] ?? CTree<long, bool>.Empty;
                        cc += (now.defpos, true);
                        cn += (et.arriveCol, cc);
                        at += (Table.SysRefIndexes, at.sindexes + (ai, cn));
                        cx.Add(at);
                        cx.db += at;
                    }
                }
            }
            for (var xb = tt.indexes.First(); xb != null; xb = xb.Next())
                for (var c = xb.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Level3.Index x
                        && x.MakeKey(prevrec.vals) is CList<TypedValue> ok
                        && x.MakeKey(now.vals) is CList<TypedValue> nk
                        && ok.CompareTo(nk) != 0)
                    {
                        x -= (ok, defpos);
                        x += (nk, defpos);
                        cx.db += x;
                    }
            for (var b = subTables.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is Table tb && tb.tableRows.Contains(prev))
                {
                    if (prev != defpos)
                        tb -= prev;
                    tb += now;
                    cx.Add(tb);
                    cx.db += tb;
                }
            return (Table)cx.Add(tt);
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
        public Update1(TableRow old, long tb, CTree<long, TypedValue> fl, Level lv, 
            long pp, Context cx) 
            : base(Type.Update1,old, tb, fl, pp, cx)
        {
            if (cx.db==null || cx.db.user?.defpos != cx.db.owner)
                throw new DBException("42105").Add(Qlx.USER);
            _classification = lv;
        }
        protected Update1(Type t, TableRow old, long tb, CTree<long, TypedValue> fl, Level lv,
            long pp, Context cx)
            :base(t,old,tb,fl,pp,cx) 
        {
            if (cx.db == null || cx.db.user?.defpos != cx.db.owner)
                throw new DBException("42105").Add(Qlx.USER);
            _classification = lv;
        }
        public Update1(Reader rdr) : base(Type.Update1, rdr)
        {  }
        protected Update1(Type t,Reader rdr): base(t,rdr) { }
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
