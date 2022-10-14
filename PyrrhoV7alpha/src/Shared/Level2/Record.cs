
using System;
using System.Text;
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
        /// <summary>
        /// A relative URI (base is given in PImportTransaction)
        /// </summary>
        public string provenance;
        // Insert and ReferenceInsert constraints: {keys} , tabledefpos->{(keys,fkeys)}
        public CTree<CList<long>,CTree<long,bool>> inC = CTree<CList<long>,CTree<long,bool>>.Empty;
        public CTree<long, bool> refs = CTree<long, bool>.Empty;
        public CTree<long,CTree<CList<long>,CList<long>>> riC 
            = CTree<long,CTree<CList<long>,CList<long>>>.Empty;
        public long triggeredAction;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,tabledefpos)) return tabledefpos;
            for (var b = fields.PositionAt(0); b != null; b = b.Next())
                if (!Committed(wr,b.key())) return b.key();
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
            Context cx)  : base(t, pp, cx)
        {
            tabledefpos = tb.defpos;
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
        /// <param name="t">The Record or UpdatePost type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected Record(Type t, Reader rdr) : base(t, rdr) { }
        protected Record(Record x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.cx.Fix(x.tabledefpos);
            fields = CTree<long, TypedValue>.Empty;
            for (var b = x.fields.PositionAt(0); b != null; b = b.Next())
                fields += (wr.cx.Fix(b.key()), b.value());
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
        /// Deserialise a list of field values
        /// </summary>
        /// <param name="buf">The buffer</param>
        internal virtual void GetFields(Reader rdr)
        {
            fields = CTree<long, TypedValue>.Empty;
            long n = rdr.GetLong();
            for (long j = 0; j < n; j++)
            {
                long c = rdr.GetLong();
                var (_, cdt) = rdr.GetColumnDomain(c,ppos); // nominal obs type from log
                cdt = cdt.GetDataType(rdr); // actual obs type from buffer
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
        internal virtual void PutFields(Writer wr)  //LOCKED
        {
            wr.PutLong(fields.Count);
            var cs = ((Table) wr.cx.db.objects[tabledefpos]).tableCols;
            for (var d = fields.PositionAt(0); d != null; d = d.Next())
            {
                var k = d.key();
                var o = d.value();
                wr.PutLong(k); // coldefpos
                var ndt = cs[k];
                var dt = o?.dataType??Domain.Null;
                dt.PutDataType(ndt, wr);
                dt.Put(o,wr);
            }
        }
        public PRow MakeKey(CList<long> cols)
        {
            PRow r = null;
            for (var i = (int)cols.Count - 1; i >= 0; i--)
                r = new PRow(fields[cols[i]], r);
            return r;
        }
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Drop:
                    {
                        var d = (Drop)that;
                        var table = (Table)db.objects[tabledefpos];
                        if (d.delpos == table.defpos)
                            return new DBException("40012",d.delpos,that,ct);
                        for (var s = fields.PositionAt(0); s != null; s = s.Next())
                            if (s.key() == d.delpos)
                                return new DBException("40013",d.delpos,that,ct);
                        return null;
                    }
                case Type.Alter3:
                    if (((Alter3)that).table.defpos == tabledefpos)
                        return new DBException("40079", tabledefpos, that, ct);
                    break;
                case Type.Alter2:
                    if (((Alter2)that).table.defpos == tabledefpos)
                        return new DBException("40079", tabledefpos, that, ct);
                    break;
                case Type.Alter:
                    if (((Alter)that).table.defpos == tabledefpos)
                        return new DBException("40079", tabledefpos, that, ct);
                    break;
                case Type.Update:
                case Type.Update1:
                case Type.Record:
                case Type.Record1:
                case Type.Record2:
                case Type.Record3:
                    {
                        var rec = (Record)that;
                        if (rec.tabledefpos != tabledefpos)
                            break;
                        for (var b = rec.inC.First(); b != null; b = b.Next())
                            if (MakeKey(b.key()).CompareTo(rec.MakeKey(b.key())) == 0)
                                return new DBException("40026", that);
                        break;
                    }
                case Type.Delete1:
                case Type.Delete:
                    {
                        var del = (Delete)that;
                        for (var b = del.deC[tabledefpos]?.First(); b != null; b = b.Next())
                        {
                            if (del.delrec.MakeKey(b.key()).CompareTo(MakeKey(b.value())) == 0)
                                return new DBException("40075", that);
                        }
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
        internal virtual TableRow AddRow(Context cx)
        {
            var tb = (Table)cx.db.objects[tabledefpos];
            var now = new TableRow(this,cx.db);
            for (var xb=tb.indexes.First();xb!=null;xb=xb.Next())
                for (var c=xb.value().First();c!=null;c=c.Next())
            {
                var x = (Index)cx.db.objects[c.key()];
                var k = x.MakeKey(now.vals);
                if ((x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey) ||
                    x.flags.HasFlag(PIndex.ConstraintType.Unique))
                    && (x.rows?.Contains(k)==true))
                    throw new DBException("23000", "duplicate key ", k);
                if (x.reftabledefpos>=0)
                {
                    var rx = (Index)cx.db.objects[x.refindexdefpos];
        /*            if (!(rx is VirtualIndex) && */
                    if (!rx.rows.Contains(k)) 
                        throw new DBException("23000", "missing foreign key ", k);
                }
                x += (k, defpos);
                cx.db += (x, cx.db.loadpos);
            }
            return now;
        }
        internal override void Install(Context cx, long p)
        {
            var tb = cx.db.objects[tabledefpos] as Table;
            try
            {
                tb +=  AddRow(cx);
                tb += (Table.LastData, ppos);
            }
            catch (DBException e)
            {
                var oi = tb.infos[cx.role.defpos];
                if (e.signal == "23000")
                    throw new DBException(e.signal, e.objects[0].ToString() + oi.name 
                        + e.objects[1].ToString());
                throw e;
            }
            cx.Install(tb, p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
        internal override void Affected(ref BTree<long, BTree<long, long>> aff)
        {
            var ta = aff[tabledefpos]??BTree<long,long>.Empty;
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
            for (var b=fields.PositionAt(0);b!=null;b=b.Next())
            {
                var k = b.key();
                var v = b.value();
                sb.Append(cm); cm = ",";
                sb.Append(DBObject.Uid(k));sb.Append('=');sb.Append(v.Val());
            }
            if (_classification != Level.D || type == Type.Update1)
            { sb.Append(" Classification: "); sb.Append(_classification); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// For Record entries made in an Import operation, we can add some provenance information
    /// </summary>
    internal class Record1 : Record
    {
        /// <summary>
        /// Constructor: a new Record (INSERT) from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public Record1(Reader rdr) : base(Type.Record1, rdr) { }
        protected Record1(Type t,Reader rdr) : base(t, rdr) { }
        /// <summary>
        /// A new Record1 from the parser
        /// </summary>
        /// <param name="tb">The table the record is for</param>
        /// <param name="dm">The domain with the provenance</param>
        /// <param name="fl">The field values</param>
        /// <param name="bp">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected Record1(Type t, Table tb, CTree<long,  TypedValue> fl, string p,
            long pp, Context cx)
            : base(t, tb, fl, pp, cx)
        {
            provenance = p;
        }
        /// <summary>
        /// A new Record1 from the parser
        /// </summary>
        /// <param name="tb">The table the record is for</param>
        /// <param name="fl">The field values</param>
        /// <param name="prov">The provenance string</param>
        /// <param name="bp">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public Record1(Table tb, CTree<long, TypedValue> fl, string prov, long pp, Context cx)
            : this(Type.Record1, tb, fl, prov, pp, cx)
        {
        }
        protected Record1(Record1 x, Writer wr) : base(x, wr)
        {
            provenance = x.provenance;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Record1(this, wr);
        }

        /// <summary>
        /// Serialise this record to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr) //LOCKED
        {
            wr.PutString(provenance);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this record from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            provenance = rdr.GetString();
            base.Deserialise(rdr);
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
        /// <summary>
        /// A new Record1 from the parser
        /// </summary>
        /// <param name="tb">The table the record is for</param>
        /// <param name="fl">The field values</param>
        /// <param name="prov">The provenance string</param>
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
