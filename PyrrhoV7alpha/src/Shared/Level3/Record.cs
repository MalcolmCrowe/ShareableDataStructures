
using System;
using System.Text;
using Pyrrho.Level3;
using Pyrrho.Level4; 
using Pyrrho.Common;
using System.Xml;
using Pyrrho.Level5;
using System.Runtime.Intrinsics.Arm;
using System.Security.AccessControl;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
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
        /// The defining position of the table
        /// </summary>
        public long tabledefpos;
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
        public override long _Table => tabledefpos;
        public long subType = -1;
        // Insert and ReferenceInsert constraints: {keys} , tabledefpos->{(keys,fkeys)}
        public CTree<Domain,CTree<long,bool>> inC = CTree<Domain,CTree<long,bool>>.Empty;
        public CTree<long, bool> refs = CTree<long, bool>.Empty;
        public CTree<long,CTree<Domain,Domain>> riC 
            = CTree<long,CTree<Domain,Domain>>.Empty;
        public long triggeredAction;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,tabledefpos)) return tabledefpos;
            for (var b = fields.PositionAt(0); b != null; b = b.Next())
                if (!Committed(wr,b.key())) return b.key();
            if (tr.objects[tabledefpos] is EdgeType et)
            {
                if (fields[et.leavingType] is TInt lt &&
                    !Committed(wr,lt.value)) return lt.value;
                if (fields[et.arrivingType] is TInt at &&
                    !Committed(wr, at.value)) return at.value;
            }
            if (!Committed(wr,subType)) return subType;
            return -1;
        }
        public Record(Table tb, CTree<long, TypedValue> fl, long pp, Context cx)
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
        protected Record(Type t, Table tb, CTree<long, TypedValue> fl, long pp, 
            Context cx)  : base(t, pp)
        {
            if (cx.tr==null || cx.db.user == null)
                throw new DBException("42105");
            tabledefpos = tb.defpos;
            nodeOrEdge = tb is NodeType;
            if (fl.Count == 0)
                throw new DBException("2201C");
            fields = fl;
            if (t!=Type.Record3)
                _classification = cx.db.user.classification;
            if (cx.tr.triggeredAction > 0)
                triggeredAction = cx.tr.triggeredAction;
            inC = tb.indexes;
            riC = tb.rindexes;
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
                if (x.nodeOrEdge && v is TChar t && wr.cx.NewNode(ppos, t.value) is string w
                    && t.value != w)
                    v = new TChar(w);
                fields += (wr.cx.Fix(b.key()), v);
            }
        }
 /*       public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            if (tr != null)
                for (var t = tr.objects[tabledefpos] as Table; t != null;
                    t = tr.objects[(t as UDType)?.super?.defpos ?? -1L] as Table)
                    if (t.defpos<Transaction.TransPos)
                        tr += (t.defpos, t + (Table.TableRows,t.tableRows - defpos));
            return base.Commit(wr, tr);
        } */
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
            wr.PutLong(tabledefpos);
            PutFields(wr);
            wr.cx.affected = (wr.cx.affected ?? Rvv.Empty) + (tabledefpos, (defpos, ppos));
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Record from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
		public override void Deserialise(Reader rdr)
        {
            tabledefpos = rdr.GetLong();
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
            if (wr.cx.db.objects[tabledefpos] is not Table tb)
                throw new PEException("PE0300");
            var cs = ColsFrom(wr.cx, tb, CTree<long, Domain>.Empty);
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
        static CTree<long, Domain> ColsFrom(Context cx, Table tb, CTree<long, Domain> cdt)
        {
            if (tb is NodeType nt && nt.super is Table st)
                cdt = ColsFrom(cx, st, cdt);
            return cdt + tb.tableCols;
        }
        public CList<TypedValue> MakeKey(BList<long?> cols)
        {
            var r = CList<TypedValue>.Empty;
            for (var b = cols.First();b is not null;b=b.Next())
                if (b.value() is long p)
                    r += fields[p]??TNull.Value;
            return r;
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Drop:
                    {
                        var d = (Drop)that;
                        if (db.objects[tabledefpos] is Table table && d.delpos == table.defpos)
                            return new DBException("40012",d.delpos,that,ct);
                        for (var s = fields.PositionAt(0); s != null; s = s.Next())
                            if (s.key() == d.delpos)
                                return new DBException("40013",d.delpos,that,ct);
                        return null;
                    }
                case Type.Alter3:
                    {
                        if (((Alter3)that).table is Table at && at.defpos == tabledefpos)
                            return new DBException("40079", tabledefpos, that, ct);
                        break;
                    }
                case Type.Alter2:
                    {
                        if (((Alter2)that).table is Table at && at.defpos == tabledefpos)
                            return new DBException("40079", tabledefpos, that, ct);
                        break;
                    }
                case Type.Alter:
                    {
                        if (((Alter)that).table is Table at && at.defpos == tabledefpos)
                            return new DBException("40079", tabledefpos, that, ct);
                        break;
                    }
                case Type.Update:
                case Type.Update1:
                case Type.Record:
                case Type.Record2:
                case Type.Record3:
                    {
                        var rec = (Record)that;
                        if (rec.tabledefpos != tabledefpos)
                            break;
                        for (var b = rec.inC.First(); b != null; b = b.Next())
                            if (MakeKey(b.key().rowType).CompareTo(rec.MakeKey(b.key().rowType))==0)
                                return new DBException("40026", that);
                        break;
                    }
                case Type.Delete1:
                case Type.Delete:
                    {
                        var del = (Delete)that;
                        for (var b = del.deC[tabledefpos]?.First(); b != null; b = b.Next())
                            if (del.delrec is not null && 
                                del.delrec.MakeKey(b.key().rowType).CompareTo(MakeKey(b.value().rowType)) == 0)
                                return new DBException("40075", that);
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
        /// <summary>
        /// Fix indexes for a new Record
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        internal virtual void AddRow(Table tt, TableRow now, Context cx)
        {
            if (cx.db==null)
                throw new PEException("PE6900");
 /*           if (tt.defpos == tabledefpos && tt is NodeType nt)
                cx.db += (nt is EdgeType et) ? new TEdge(defpos, et, fields)
                    : new TNode(defpos, nt, fields); */
            for (var xb = tt.indexes.First(); xb != null; xb = xb.Next())
                for (var c = xb.value().First(); c != null; c = c.Next())
                    if (cx.db.objects[c.key()] is Level3.Index x 
                        && x.MakeKey(now.vals) is CList<TypedValue> k)
                    {
                        if ((x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) ||
                                x.flags.HasFlag(PIndex.ConstraintType.Unique))
                            && (x.rows?.Contains(k) == true) && cx.db is Transaction
                            && k[0] is TypedValue tk
                            && x.rows?.impl?[tk]?.ToLong() is long q && q!=now.defpos)
                            throw new DBException("23000", "duplicate key ", k);
                        if (cx.db.objects[x.refindexdefpos] is Level3.Index rx &&
                        //    rx.rows?.Contains(k)!=true
                             cx.db.objects[x.reftabledefpos] is Table tb
                            && tb.Base(cx).FindPrimaryIndex(cx) is Level3.Index px 
                            && px.rows?.Contains(k) != true
                            && cx.db is Transaction)
                            throw new DBException("23000", "missing foreign key ", k);
                        x += (k, defpos);
                        cx.db += (x, cx.db.loadpos);
                    }
        }
        protected virtual TableRow Now(Context cx)
        {
            return new TableRow(this, cx);
        }
        internal override DBObject? Install(Context cx, long p)
        {
            if (cx._Ob(tabledefpos) is not Table tb || tb.infos[tb.definer] is not ObInfo oi)
                throw new PEException("PE0301");
            var ost = subType;
            var tp = tabledefpos;
            try
            {
                var now = Now(cx);
                for (var tt = tb; tt != null; tt=cx.db.objects[tt.super?.defpos??-1L] as Table) 
                {
                    if (tt != tb)   // update supertypes: extra values are harmless
                    {
                        subType = tb.defpos;
                        tabledefpos = tt.defpos;
                    }
                    AddRow(tt, now,cx);
                    tt += now;
                    tt += (Table.LastData, ppos);
                    cx.Install(tt, p);
                }
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
            return tb;
        }
        internal override void Affected(ref BTree<long, BTree<long, long?>> aff)
        {
            var ta = aff[tabledefpos]??BTree<long,long?>.Empty;
            ta += (defpos, ppos);
            aff += (tabledefpos, ta);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(' ');
            sb.Append(DBObject.Uid(defpos));
            sb.Append('[');
            sb.Append(DBObject.Uid(tabledefpos));
            sb.Append(']');
            var cm = ": ";
            for (var b=fields.PositionAt(0);b is not null;b=b.Next())
            {
                var k = b.key();
                var v = b.value();
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(k));sb.Append('=');sb.Append(v.ToString());
                sb.Append("[");
                if (v.dataType.kind != Sqlx.TYPE)
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
    /// Show Record entries where values are subtypes of the expected types we can add type info
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
        protected Record2(Type t, Table tb, CTree<long,  TypedValue> fl, long st, 
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
    /// Show Record3 entries we also add classification info
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
        /// <summary>
        /// A new Record1 from the parser
        /// </summary>
        /// <param name="tb">The table the record is for</param>
        /// <param name="fl">The field values</param>
        /// <param name="bp">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public Record3(Table tb, CTree<long, TypedValue> fl, long st, Level lv, long pp, Context cx)
            : base(Type.Record3, tb, fl, st, pp, cx)
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
}
