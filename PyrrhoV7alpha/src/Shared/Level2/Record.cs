
using System;
using System.Collections;
using System.Text;
using Pyrrho.Level3;
using Pyrrho.Level4; 
using Pyrrho.Common;
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
		public BTree<long, TypedValue> fields = BTree<long, TypedValue>.Empty;
        /// <summary>
        /// Referential Integrity checking
        /// serialised only for transaction master: to check against conflicting delete/keyUpdate
        /// </summary>
        protected Level _classification = Level.D;
        public virtual Level classification { get { return _classification; } }
        public long subType = -1;
        /// <summary>
        /// A relative URI (base is given in PImportTransaction)
        /// </summary>
        public string provenance;
        public override long Dependent(Writer wr)
        {
            if (defpos != ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,tabledefpos)) return tabledefpos;
            for (var b = fields.First(); b != null; b = b.Next())
                if (!Committed(wr,b.key())) return b.key();
            if (!Committed(wr,subType)) return subType;
            return -1;
        }
        public Record(Table tb, BTree<long, TypedValue> fl, long u, Transaction db)
            : this(Type.Record, tb, fl, u, db) { }
        /// <summary>
        /// Constructor: a new Record (INSERT) from the Parser
        /// </summary>
        /// <param name="t">The Record or UpdatePost type</param>
        /// <param name="tb">The defining position of the table</param>
        /// <param name="fl">The field values</param>
        /// <param name="tb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected Record(Type t, Table tb, BTree<long, TypedValue> fl, long u, Transaction db)
            : base(t, u, db)
        {
            tabledefpos = tb.defpos;
            if (fl.Count == 0)
                throw new DBException("2201C");
            fields = fl;
            for (var b = fl.First(); b != null; b = b.Next())
                if (db.role.objects[b.key()] is TableColumn tc)
                {
                    if (tc.Denied(db, Grant.Privilege.Insert))
                        throw new DBException("42105", tc);
                    for (var c = tc.constraints?.First(); c != null; c = c.Next())
                        if (c.value().Eval(db,new Context(db)) != TBool.True)
                            throw new DBException("22212", tc.name);
                }
        }
        /// <summary>
        /// Constructor: a new Record (INSERT) from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public Record(Reader rdr) : base(Type.Record, rdr) { }
        /// <summary>
        /// Constructor: a new Record (INSERT) from the buffer
        /// </summary>
        /// <param name="t">The Record or UpdatePost type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected Record(Type t, Reader rdr) : base(t, rdr) { }
        protected Record(Record x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.Fix(x.tabledefpos);
            fields = BTree<long, TypedValue>.Empty;
            for (var b = x.fields.First(); b != null; b = b.Next())
                fields += (wr.Fix(b.key()), b.value());
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
            //         database.SerialiseIndexConstraint(indC,refC,ppos,r);
            base.Serialise(wr);
            wr.rvv += new Rvv(tabledefpos, defpos, ppos);
        }
        /// <summary>
        /// Deserialise this Record from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
		public override void Deserialise(Reader rdr)
        {
            tabledefpos = rdr.GetLong();
            GetFields(rdr);
            //         buf.DeserialiseIndexConstraint(ref indC, ref refC);
            base.Deserialise(rdr);
        }
        /// <summary>
        /// Deserialise a list of field values
        /// </summary>
        /// <param name="buf">The buffer</param>
        internal virtual void GetFields(Reader rdr)
        {
            fields = BTree<long, TypedValue>.Empty;
            long n = rdr.GetLong();
            for (long j = 0; j < n; j++)
            {
                long c = rdr.GetLong();
                var tc = (TableColumn)rdr.role.objects[c];
                var cdt = tc.domain;
                cdt = cdt.GetDataType(rdr);
                if (cdt != null)
                {
                    var tv = cdt.Get(rdr);
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
            var f = BTree<long, TypedValue>.Empty;
            for (var d = fields.First(); d != null; d = d.Next())
            {
                long k = d.key();
                TypedValue o = d.value();
                wr.PutLong(k); // coldefpos
                var ndt = ((TableColumn)wr.db.role.objects[k]).domain;
                var dt = o?.dataType??Domain.Null;
                dt.PutDataType(ndt, wr);
                dt.Put(o,wr);
            }
        }
        public PRow MakeKey(long[] cols)
        {
            PRow r = null;
            for (var i = cols.Length - 1; i >= 0; i--)
                r = new PRow(fields[cols[i]], r);
            return r;
        }
        public PRow MakeKey(BList<long> cols)
        {
            PRow r = null;
            for (var i = (int)cols.Count - 1; i >= 0; i--)
                r = new PRow(fields[cols[i]], r);
            return r;
        }
        public PRow MakeKey(BList<Selector> cols)
        {
            PRow r = null;
            for (var i = (int)cols.Count - 1; i >= 0; i--)
                r = new PRow(fields[cols[i].defpos], r);
            return r;
        }
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.Drop:
                    {
                        var d = (Drop)that;
                        var table = (Table)db.role.objects[tabledefpos];
                        if (d.delpos == table.defpos)
                            return ppos;
                        for (var s = fields.First(); s != null; s = s.Next())
                            if (s.key() == d.delpos)
                                return ppos;
                        return -1;
                    }
                case Type.Alter:
                    return (((Alter)that).tabledefpos == tabledefpos) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }

        internal override Database Install(Database db, Role ro, long p)
        {
            var tb = db.schemaRole.objects[tabledefpos] as Table;
            try
            {
                tb += new TableRow(this, db);
            }
            catch (DBException e)
            {
                if (e.signal == "23000")
                    throw new DBException(e.signal, e.objects[0].ToString() + tb.name + e.objects[1].ToString());
                throw e;
            }
            if (db.schemaRole != db.role)
                db += (db.schemaRole, tb,p);
            return db += (db.role, tb,p);
        }

        internal class ColumnValue
        {
            public PColumn col;
            public TypedValue val;
            public ColumnValue(PColumn tc,  TypedValue tv) { col = tc; val = tv; }
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Record "+defpos+"["+tabledefpos+"]");
            var cm = ": ";
            for (var b=fields.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.key());sb.Append('=');sb.Append(b.value().Val());
                sb.Append('[');sb.Append(b.value().dataType.defpos);sb.Append(']');
            }
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
        protected Record1(Type t, Table tb, BTree<long,  TypedValue> fl, string p,
            long u, Transaction tr)
            : base(t, tb, fl, u, tr)
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
        public Record1(Table tb, BTree<long, TypedValue> fl, string prov, long u, Transaction db)
            : this(Type.Record1, tb, fl, prov, u, db)
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
        protected Record2(Type t, Table tb, BTree<long,  TypedValue> fl, long st, 
            long u, Transaction tr)
            : base(t, tb, fl, u, tr)
        {
            subType = st;
        }
        protected Record2(Record2 x, Writer wr) : base(x, wr)
        {
            subType = wr.Fix(x.subType);
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
        public Record3(Reader rdr) : base(Type.Record3, rdr) { }
        /// <summary>
        /// A new Record1 from the parser
        /// </summary>
        /// <param name="tb">The table the record is for</param>
        /// <param name="fl">The field values</param>
        /// <param name="prov">The provenance string</param>
        /// <param name="bp">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public Record3(Table tb, BTree<long, TypedValue> fl, long st, Level lv, long u, Transaction tr)
            : base(Type.Record3, tb, fl, st, u, tr)
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
