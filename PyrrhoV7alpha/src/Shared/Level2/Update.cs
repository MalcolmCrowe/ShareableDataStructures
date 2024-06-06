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
        public Update(TableRow old, CTree<long,bool> tb, CTree<long, TypedValue> fl, long pp, 
            Context cx)
            : this(Type.Update, old, tb, fl, pp, cx)
        { }
        protected Update(Type t, TableRow old, CTree<long,bool> tb,CTree<long,TypedValue> fl, 
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
                        if (tabledefpos.Contains(d.delpos))
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
                        for (var c = tabledefpos.First();c!=null;c=c.Next())
                        for (var b = de.deC[c.key()]?.First();b is not null;b=b.Next())
                        if (db.objects[c.key()] is Table tb && tb.FindIndex(db, b.key())?[0] is Level3.Index x
                            && x.MakeKey(fields) is CList<TypedValue> pk && x.rows?.Contains(pk)==true)
                                return new DBException("40085", de.delpos);
                        break;
                    }
                case Type.Update2:
                case Type.Update1:
                case Type.Update:
                    {
                        var u = (Update)that;
                        for (var c = tabledefpos.First();c!=null;c=c.Next())
                        for (var b = u.riC[c.key()]?.First(); b != null; b = b.Next())
                            if (u.prevrec is TableRow pr && 
                                pr.MakeKey(b.value().rowType) is CList<TypedValue> pk &&
                                pk.CompareTo(MakeKey(b.key().rowType)) == 0 )
                                // conflict if our old values are referenced by a new foreign key
                                    throw new DBException("40014", u.prevrec.ToString());
                        for (var c=u.tabledefpos.First();c!=null; c=c.Next())
                        if (db.objects[c.key()] is Table tb)
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
                    if (((Alter3)that).table is Table t3 && tabledefpos.Contains(t3.defpos))
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.Alter2:
                    if (((Alter2)that).table is Table t2 && tabledefpos.Contains(t2.defpos))
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.Alter:
                    if (((Alter)that).table is Table t && tabledefpos.Contains(t.defpos))
                        return new DBException("40080", defpos, that, ct);
                    break;
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn:
                    if (((PColumn)that).table is Table tc && tabledefpos.Contains(tc.defpos))
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
            for (var b = tabledefpos.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Table tb && tb.tableRows[defpos] is TableRow tr)
                {
                    prevrec = tr;
                    break;
                }
            if (prevrec is null)
                throw new PEException("PE00809");
            Check(cx);
            return new TableRow(this,cx,prevrec);
        }
        internal override void Check(Context cx)
        {
            for (var c = tabledefpos.First(); c != null; c = c.Next())
                if (cx._Ob(c.key()) is Table tb)
                {
        //            var dm = tb._PathDomain(cx);
                    for (var b = tb.First(); b != null; b = b.Next())
                    {
                        if (b.value() is not long p || tb.representation[p] is not Domain dv)
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
                        var cl = lt.sindexes[et.defpos] ?? CTree<long, CTree<long, bool>>.Empty;
                        cl -= lo;
                        var cc = cl[li] ?? CTree<long, bool>.Empty;
                        cc += (now.defpos, true);
                        cl += (li, cc);
                        lt += (Table.SysRefIndexes, lt.sindexes + (et.defpos, cl));
                        cx.Add(lt);
                        cx.db += lt;
                    }
                    if (cx._Od(et.arrivingType) is NodeType at
                        && prevrec.vals[et.leaveCol] is TInt oa && oa.ToLong() is long ao
                        && now.vals[et.arriveCol] is TInt ta && ta.ToLong() is long ai
                        && ao!=ai)
                    {
                        var ca = at.sindexes[et.defpos] ?? CTree<long, CTree<long, bool>>.Empty;
                        ca -= ao;
                        var cc = ca[ai] ?? CTree<long, bool>.Empty;
                        cc += (now.defpos, true);
                        ca += (ai, cc);
                        at += (Table.SysRefIndexes, at.sindexes + (et.defpos, ca));
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
        public Update1(TableRow old, CTree<long,bool> tb, CTree<long, TypedValue> fl, Level lv, 
            long pp, Context cx) 
            : base(Type.Update1,old, tb, fl, pp, cx)
        {
            if (cx.db==null || cx.db.user?.defpos != cx.db.owner)
                throw new DBException("42105").Add(Qlx.USER);
            _classification = lv;
        }
        protected Update1(Type t, TableRow old, CTree<long, bool> tb, CTree<long, TypedValue> fl, Level lv,
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
        internal override DBObject? Install(Context cx)
        {
            if (cx.db != null)
                for (var b = tabledefpos.First(); b != null; b = b.Next())
                {
                    if (cx.db.objects[b.key()] is Table tb)
                    {
                        var fl = tb.tableRows[defpos] ?? throw new PEException("PE40406");
                        Check(cx);
                        cx.Install(tb + new TableRow(this, cx, fl, _classification)); //.Check(tb, cx));
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
    internal class Update2 : Update1
    {
        public Update2(Reader rdr) : base(Type.Update2,rdr)
        { }

        public Update2(TableRow old, CTree<long, bool> tb, CTree<long, TypedValue> fl, Level lv, long pp, Context cx) 
            : base(Type.Update2, old, tb, fl, lv, pp, cx)
        { }

        protected Update2(Update1 x, Writer wr) : base(x, wr)
        { }

        protected override Physical Relocate(Writer wr)
        {
            return new Update2(this, wr);
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
