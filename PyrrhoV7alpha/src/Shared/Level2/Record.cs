
using System.Text;
using Pyrrho.Level3;
using Pyrrho.Level4; 
using Pyrrho.Common;
using Pyrrho.Level5;
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
    /// A Level 2 Record record.
    /// Records are constructed within the transaction thread
    /// but can be shared at Level 4, so are immutable shareable
    /// </summary>
    internal class Record : Physical
    {
        /// <summary>
        /// The defining position of the record
        /// </summary>
        public virtual long defpos { get { return ppos; } }
        /// <summary>
        /// The defining positions of the tables
        /// </summary>
        public CTree<long, bool> tabledefpos = CTree<long, bool>.Empty;
        public override CTree<long, bool> _Table => tabledefpos;
        public bool nodeOrEdge = false; // set in constructor
        /// <summary>
        /// The tree of field ids and values for this record
        /// PColumn.defpos->val: but use Field method instead of fields[] to access
        /// </summary>
		public CTree<long, TypedValue> fields = CTree<long, TypedValue>.Empty;
        /// <summary>
        /// Referential Integrity checking
        /// serialised only for transaction master: to check against conflicting delete/keyUpdate
        /// </summary>
        protected Level _classification = Level.D;
        public virtual Level classification => _classification;
        public long subType = -1;
        // Insert and ReferenceInsert constraints: {keys} , tabledefpos->{(keys,fkeys)}
        public CTree<Domain, CTree<long, bool>> inC = CTree<Domain, CTree<long, bool>>.Empty;
        public CTree<long, bool> refs = CTree<long, bool>.Empty;
        public CTree<long, CTree<Domain, Domain>> riC
            = CTree<long, CTree<Domain, Domain>>.Empty;
        public long triggeredAction;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr, defpos)) return defpos;
            for (var b = tabledefpos.First(); b != null; b = b.Next())
                if (!Committed(wr, b.key())) return b.key();
            for (var b = fields.PositionAt(0); b != null; b = b.Next())
                if (!Committed(wr, b.key())) return b.key();
            for (var b = tabledefpos.First(); b != null; b = b.Next())
                if (tr.objects[b.key()] is EdgeType et)
                {
                    if (fields[et.leavingType] is TInt lt &&
                        !Committed(wr, lt.value)) return lt.value;
                    if (fields[et.arrivingType] is TInt at &&
                        !Committed(wr, at.value)) return at.value;
                }
            if (!Committed(wr, subType)) return subType;
            return -1;
        }
        public Record(CTree<long, bool> tb, CTree<long, TypedValue> fl, long pp, Context cx)
            : this(Type.Record, tb, fl, pp, cx)
        { }
        /// <summary>
        /// Constructor: a new Record (INSERT) from the Parser
        /// </summary>
        /// <param name="t">The Record or UpdatePost type</param>
        /// <param name="tb">The defining position of the table</param>
        /// <param name="fl">The field values</param>
        /// <param name="tb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected Record(Type t, CTree<long, bool> ts, CTree<long, TypedValue> fl, long pp,
            Context cx) : base(t, pp)
        {
            if (cx.tr == null || cx.db.user == null)
                throw new DBException("42105").Add(Qlx.USER);
            tabledefpos = ts;
            fields = fl;
            if (t != Type.Record3)
                _classification = cx.db.user.classification;
            if (cx.tr.triggeredAction > 0)
                triggeredAction = cx.tr.triggeredAction;
            for (var b = ts.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is Table tb)
                {
                    nodeOrEdge = tb is NodeType;
                    inC += tb.indexes;
                    riC += tb.rindexes;
                }
        }
        /// <summary>
        /// Constructor: a new Record (INSERT) from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public Record(Reader rdr) : base(Type.Record, rdr)
        { }
        /// <summary>
        /// Constructor: a new Record (INSERT) from the buffer
        /// </summary>
        /// <param name="t">The Record or Update type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected Record(Type t, Reader rdr) : base(t, rdr) { }
        protected Record(Record x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.cx.Fix(x.tabledefpos);
            fields = CTree<long, TypedValue>.Empty;
            for (var b = x.fields.PositionAt(0); b != null; b = b.Next())
            {
                var v = b.value();
                if (x.nodeOrEdge)
                {
                    if (v is TChar t && wr.cx.NewNode(ppos, t.value) is string w && t.value != w)
                        v = new TChar(w);
                    if (v is TInt p && wr.cx.Fix(p.value) is long nv && nv != p.value)
                        v = new TInt(nv);
                }
                fields += (wr.cx.Fix(b.key()), v);
            }
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Record(this, wr);
        }
        /// <summary>
        /// Serialise this Record to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
		public override void Serialise(Writer wr) //LOCKED
        {
            var tp = tabledefpos.Last()?.key() ?? throw new PEException("PE00802");
            wr.PutLong(tp);
            PutFields(wr);
            wr.cx.affected = (wr.cx.affected ?? Rvv.Empty) + (tp, (defpos, ppos));
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Record from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
		public override void Deserialise(Reader rdr)
        {
            tabledefpos += (rdr.GetLong(), true);
            GetFields(rdr);
            base.Deserialise(rdr);
        }
        /// <summary>
        /// Deserialise a tree of field values
        /// </summary>
        /// <param name="buf">The buffer</param>
        internal void GetFields(Reader rdr)
        {
            fields = CTree<long, TypedValue>.Empty;
            long n = rdr.GetLong();
            for (long j = 0; j < n; j++)
            {
                long c = rdr.GetLong();
                var (_, cdt) = rdr.GetColumnDomain(c); // nominal ob type from log
                cdt = cdt.GetDataType(rdr); // actual ob type from buffer
                if (cdt != null)
                {
                    var tv = cdt.Get(rdr.log, rdr, ppos);
                    fields += (c, tv);
                }
            }
        }
        /// <summary>
        /// Serialise a set of field values
        /// </summary>
        /// <param name="r">Relocation information</param>
        internal void PutFields(Writer wr)  //LOCKED
        {
            var cs = ColsFrom(wr.cx, tabledefpos, CTree<long, Domain>.Empty);
            wr.PutLong(fields.Count);
            for (var d = fields.PositionAt(0); d != null; d = d.Next())
                if (cs[d.key()] is Domain ndt && d.value() is TypedValue o)
                {
                    var k = d.key();
                    wr.PutLong(k); // coldefpos
                    var dt = o.dataType ?? Domain.Null;
                    dt.PutDataType(ndt, wr);
                    dt.Put(o, wr);
                }
        }
        static CTree<long, Domain> ColsFrom(Context cx, CTree<long, bool> tbs, CTree<long, Domain> cdt)
        {
            for (var c = tbs.First(); c != null; c = c.Next())
            {
                var tb = cx.db.objects[c.key()] as Table ?? throw new PEException("PE00803");
                cdt += ColsFrom(cx, tb.super, cdt);
                cdt += tb.tableCols;
            }
            return cdt;
        }
        static CTree<long, Domain> ColsFrom(Context cx, CTree<Domain, bool> tbs, CTree<long, Domain> cdt)
        {
            for (var c = tbs.First(); c != null; c = c.Next())
            {
                var tb = cx.db.objects[c.key().defpos] as Table ?? throw new PEException("PE00803");
                cdt += ColsFrom(cx, tb.super, cdt);
                cdt += tb.tableCols;
            }
            return cdt;
        }
        public CList<TypedValue> MakeKey(BList<long?> cols)
        {
            var r = CList<TypedValue>.Empty;
            for (var b = cols.First(); b is not null; b = b.Next())
                if (b.value() is long p)
                    r += fields[p] ?? TNull.Value;
            return r;
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch (that.type)
            {
                case Type.Drop:
                    {
                        var d = (Drop)that;
                        for (var b = tabledefpos.First(); b != null; b = b.Next())
                            if (db.objects[b.key()] is Table table && d.delpos == table.defpos)
                                return new DBException("40012", d.delpos, that, ct);
                        for (var s = fields.PositionAt(0); s != null; s = s.Next())
                            if (s.key() == d.delpos)
                                return new DBException("40013", d.delpos, that, ct);
                        return null;
                    }
                case Type.Alter3:
                    {
                        if (((Alter3)that).table is Table at && tabledefpos.Contains(at.defpos))
                            return new DBException("40079", tabledefpos, that, ct);
                        break;
                    }
                case Type.Alter2:
                    {
                        if (((Alter2)that).table is Table at && tabledefpos.Contains(at.defpos))
                            return new DBException("40079", tabledefpos, that, ct);
                        break;
                    }
                case Type.Alter:
                    {
                        if (((Alter)that).table is Table at && tabledefpos.Contains(at.defpos))
                            return new DBException("40079", tabledefpos, that, ct);
                        break;
                    }
                case Type.Update:
                case Type.Update1:
                case Type.Update2:
                case Type.Record:
                case Type.Record2:
                case Type.Record3:
                case Type.Record4:
                    {
                        var rec = (Record)that;
                        if (DifferentTables(this, rec))
                            break;
                        for (var b = rec.inC.First(); b != null; b = b.Next())
                            if (MakeKey(b.key().rowType).CompareTo(rec.MakeKey(b.key().rowType)) == 0)
                                return new DBException("40026", that);
                        break;
                    }
                case Type.Delete2:
                case Type.Delete1:
                case Type.Delete:
                    {
                        var del = (Delete)that;
                        for (var c = tabledefpos.First(); c != null; c = c.Next())
                            for (var b = del.deC[c.key()]?.First(); b != null; b = b.Next())
                                if (del.delrec is not null &&
                                    del.delrec.MakeKey(b.key().rowType).CompareTo(MakeKey(b.value().rowType)) == 0)
                                    return new DBException("40075", that);
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
        public static bool DifferentTables(Record a, Record b)
        {
            var ab = a.tabledefpos.First();
            var bb = b.tabledefpos.First();
            for (; ab is not null && bb is not null; ab = ab.Next(), bb = bb.Next())
                if (ab.key().CompareTo(bb.key()) == 0) return false;
            return ab is null && bb is null;
        }
        /// <summary>
        /// Fix indexes for a new Record
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        internal virtual Table AddRow(Table tt, TableRow now, Context cx)
        {
            if (cx.db == null)
                throw new PEException("PE6900");
            tt += now;
            if (tt is EdgeType et)
            {
                if (now.vals[et.leaveCol] == TNull.Value || now.vals[et.arriveCol] == TNull.Value)
                    throw new PEException("PE6901");
                if (et.FindPrimaryIndex(cx) is null)
                {
                    if (cx._Od(et.leavingType) is NodeType lt
                       && now.vals[et.leaveCol] is TInt tl && tl.ToLong() is long li)
                    {
                        var cl = lt.sindexes[et.defpos] ?? CTree<long, CTree<long, bool>>.Empty;
                        var cc = cl[li] ?? CTree<long, bool>.Empty;
                        cc += (now.defpos, true);
                        cl += (li, cc);
                        lt += (Table.SysRefIndexes, lt.sindexes + (et.defpos, cl));
                        cx.Add(lt);
                        cx.db += lt;
                    }
                    if (cx._Od(et.arrivingType) is NodeType at
                        && now.vals[et.arriveCol] is TInt ta && ta.ToLong() is long ai)
                    {
                        var ca = at.sindexes[et.arriveCol] ?? CTree<long, CTree<long, bool>>.Empty;
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
                        && x.MakeKey(now.vals) is CList<TypedValue> k && k[0] != TNull.Value)
                    {
                        if ((x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) ||
                                x.flags.HasFlag(PIndex.ConstraintType.Unique))
                            && (x.rows?.Contains(k) == true) && cx.db is Transaction
                            && x.rows?.Get(k, 0) is long q && q != now.defpos)
                            throw new DBException("23000", "duplicate key ", k);
                        if (cx.db.objects[x.refindexdefpos] is Level3.Index rx &&
                        //    rx.rows?.Contains(k)!=true
                             cx.db.objects[x.reftabledefpos] is Table tb
                            && tb.Top().FindPrimaryIndex(cx) is Level3.Index px
                            && px.rows?.Contains(k) != true
                            && cx.db is Transaction)
                            throw new DBException("23000", "missing foreign key ", k);
                        x += (k, defpos);
                        cx.db += x;
                    }
            // If this is a record for an unlabelled node or edge type, check we have a note of the type
            if (tt is NodeType nt)
            {
                if (nt.label.kind == Qlx.Null && nt.name == "")
                {
                    var ps = CTree<long, bool>.Empty;
                    for (var b = nt.representation.First(); b != null; b = b.Next())
                        ps += (b.key(), true);
                    if (nt is EdgeType en)
                    {
                        var e = cx.db.unlabelledEdgeTypes[en.leavingType] ?? BTree<long, BTree<CTree<long, bool>, long?>>.Empty;
                        var l = e[en.arrivingType] ?? BTree<CTree<long, bool>, long?>.Empty;
                        cx.db += (Database.UnlabelledEdgeTypes,
                            cx.db.unlabelledEdgeTypes + (en.leavingType,
                                e + (en.arrivingType, l + (ps, tt.defpos))));
                    }
                    else if (cx.db.unlabelledNodeTypes[ps] is null)
                        cx.db += (Database.UnlabelledNodeTypes, cx.db.unlabelledNodeTypes + (ps, tt.defpos));
                }
                else if (now.vals.Count == 0)
                    tt = nt + (TrivialRowSet.Singleton, new TRow(nt,now.vals));
            }
            return (Table)cx.Add(tt);
        }
        protected virtual TableRow Now(Context cx)
        {
            Check(cx);
            return new TableRow(this, cx);
        }
        internal virtual void Check(Context cx)
        {
            for (var b = tabledefpos.First(); b != null; b = b.Next())
            {
                if (cx._Ob(b.key()) is not Table tb) throw new DBException("42105").Add(Qlx.CONSTRAINT);
                //       var dm = tb._PathDomain(cx);
                for (var c = tb.First(); c != null; c = c.Next())
                {
                    if (c.value() is not long p || tb.representation[p] is not Domain dv)
                        throw new PEException("PE10701");
                    if (fields[p] is TypedValue v && v != TNull.Value && !v.dataType.EqualOrStrongSubtypeOf(dv))
                    {
                        var nv = dv.Coerce(cx, v);
                        fields += (p, nv);
                    }
                }
            }
        }
        /// <summary>
        /// The advantage of this override is that we have to hand uncommitted and committed versions
        /// of this record and its host tables/nodetypes/edgetypes. 
        /// </summary>
        /// <param name="wr"></param>
        /// <param name="tr"></param>
        /// <returns></returns>
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            for (var b = tabledefpos.First(); b != null; b = b.Next())
                if (tr?.objects[b.key()] is Table tb)
                    tb.Forget(tr, this);
            return base.Commit(wr, tr);
        }
        internal override DBObject? Install(Context cx)
        {
            Table? rt = null;
            for (var b = tabledefpos.First(); b != null; b = b.Next())
            {
                if (cx._Ob(b.key()) is not Table tb || tb.infos[tb.definer] is not ObInfo oi)
                    throw new PEException("PE0301");
                var ost = subType;
                var tp = tabledefpos;
                try
                {
                    var now = Now(cx);
                    cx = Add(cx, tb, now);
                    tb = (Table?)cx.obs[tb.defpos]??throw new DBException("42105").Add(Qlx.TABLE);
                }
                catch (DBException e)
                {
                    if (e.signal == "23000")
                        throw new DBException(e.signal, e.objects[0].ToString() + oi.name
                            + e.objects[1].ToString());
                    throw;
                }
                if (cx.db.mem.Contains(Database.Log))
                    cx.db += (Database.Log, cx.db.log + (ppos, type));
                subType = ost;
                tabledefpos = tp;
                rt = tb;
            }
            return rt??throw new PEException("PE00804");
        }
        Context Add(Context cx,Table tt, TableRow now)
        {
            if (tt.defpos < 0)
                return cx;
            for (var b = tt.super.First(); b != null; b = b.Next())   // update supertypes: extra values are harmless
                if (cx.db.objects[b.key()?.defpos ?? -1L] is Table st && st.defpos>0)
                {
                    NodeType? ot = null;
                    if (tt is NodeType yt && st is NodeType sn && (yt.idCol != sn.idCol || yt.idCol<0))
                    { //??
                        ot = sn;
                        if (now.vals[st.idCol] is TypedValue tv)
                            now += (yt.idCol, tv);
                    }
                    cx = Add(cx, st, now);
                    subType = st.defpos;
                    if (ot is not null && ot.defpos != st.defpos) //??
                    {
                        tabledefpos -= ot.defpos;
                        tabledefpos += (tt.defpos, true);
                    }
                }
            tt = AddRow(tt, now, cx);
            tt += (Table.LastData, ppos);
            cx.db += tt;
            cx.Add(tt);
            cx.Install(tt);
            return cx;
        }
        internal override void Affected(ref BTree<long, BTree<long, long?>> aff)
        {
            for (var b = tabledefpos.First(); b != null; b = b.Next())
            {
                var tp = b.key();
                var ta = aff[tp] ?? BTree<long, long?>.Empty;
                ta += (defpos, ppos);
                aff += (tp, ta);
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(' ');
            sb.Append(DBObject.Uid(defpos));
            sb.Append('[');
            var cm = "";
            for (var b = tabledefpos.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.key()));
            }
            sb.Append(']');
            cm = ": ";
            for (var b=fields.PositionAt(0);b is not null;b=b.Next())
            {
                var k = b.key();
                var v = b.value();
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(k));sb.Append('=');sb.Append(v.ToString());
                sb.Append("[");
                if (v.dataType.kind != Qlx.TYPE)
                    sb.Append(v.dataType.kind);
                else
                    sb.Append(v.dataType.name);
                sb.Append("]");
            }
            if (_classification != Level.D || type == Type.Update1)
            { sb.Append(" Classification: "); sb.Append(_classification); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// For Record entries where values are subtypes of the expected types we can add type info
    /// </summary>
    internal class Record2 : Record
    {
        /// <summary>
        /// Constructor: a new Record (INSERT) from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public Record2(Reader rdr) : base(Type.Record2, rdr) { }
        protected Record2(Type t, Reader rdr) : base(t, rdr) { }
        /// <summary>
        /// A new Record2 from the parser
        /// </summary>
        /// <param name="tb">The table the record is for</param>
        /// <param name="fl">The field values</param>
        /// <param name="st">The subtype defpos</param>
        /// <param name="bp">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected Record2(Type t, CTree<long,bool>tb, CTree<long,  TypedValue> fl, long st, 
            long pp, Context cx) : base(t, tb, fl, pp, cx)
        {
            subType = st;
        }
        protected Record2(Record2 x, Writer wr) : base(x, wr)
        {
            subType = wr.cx.Fix(x.subType);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Record2(this, wr);
        }
        /// <summary>
        /// Serialise this record to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr) //LOCKED
        {
            wr.PutLong(subType);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this record from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            subType = rdr.GetLong();
            base.Deserialise(rdr);
        }
    }
    /// <summary>
    /// For Record3 entries we also add classification info
    /// </summary>
    internal class Record3 : Record2
    {
        /// <summary>
        /// Constructor: a new Record (INSERT) from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public Record3(Reader rdr) : base(Type.Record3, rdr) 
        { }
        protected Record3(Type t, Reader rdr) : base(t, rdr) { }
        /// <summary>
        /// A new Record1 from the parser
        /// </summary>
        /// <param name="tb">The table the record is for</param>
        /// <param name="fl">The field values</param>
        /// <param name="bp">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public Record3(CTree<long,bool>tb, CTree<long, TypedValue> fl, long st, Level lv, long pp, Context cx)
            : base(Type.Record3, tb, fl, st, pp, cx)
        {
            _classification = lv;
        }
        /// <summary>
        /// A new Record1 from the parser
        /// </summary>
        /// <param name="tb">The table the record is for</param>
        /// <param name="fl">The field values</param>
        /// <param name="bp">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public Record3(Type t, CTree<long, bool> tb, CTree<long, TypedValue> fl, long st, Level lv, long pp, Context cx)
            : base(t, tb, fl, st, pp, cx)
        {
            _classification = lv;
        }
        protected Record3(Record3 x, Writer wr) : base(x, wr)
        {
            _classification = x._classification;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Record3(this, wr);
        }
        /// <summary>
        /// Serialise this record to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr) //LOCKED
        {
            Level.SerialiseLevel(wr,_classification);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this record from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            _classification = Level.DeserialiseLevel(rdr);
            base.Deserialise(rdr);
        }
    }
    internal class Record4 : Record3
    {
        public Record4(CTree<long,bool> tbs, CTree<long, TypedValue> fl, long st, Level lv, long pp, Context cx) 
            : base(Type.Record4, tbs, fl, st, lv, pp, cx)
        { }
        public Record4(Reader rdr) : base(Type.Record4,rdr)
        { }
        protected Record4(Record3 x, Writer wr) : base(x, wr)
        {
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Record4(this,wr);
        }

        public override void Deserialise(Reader rdr)
        {
            var n = rdr.GetInt();
            for (var i = 0; i < n; i++)
                tabledefpos += (rdr.GetLong(),true);
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
