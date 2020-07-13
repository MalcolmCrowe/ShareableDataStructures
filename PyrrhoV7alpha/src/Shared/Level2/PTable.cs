using System;
using System.Text;
using Pyrrho.Level4;
using Pyrrho.Level3;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
	/// A PTable has a name, a set of TableColumns, a set of records, a primary key, a set of indices
    /// Only the name is defined in the PTable: the rest comes in other data
	/// </summary>
	internal class PTable : Physical
	{
        /// <summary>
        /// The defining position of this table
        /// </summary>
        public long defpos { get { return ppos; } }
        /// <summary>
        /// The name of the table
        /// </summary>
		public string name;
        public string rowiri = "";
        public Grant.Privilege enforcement = (Grant.Privilege)15; // read,insert,udate,delete
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            return -1;
        }
        /// <summary>
        /// Constructor: a Table definition from the Parser
        /// </summary>
        /// <param name="nm">The name of the table</param>
        /// <param name="wh">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PTable(string nm, long pp, Context cx)
            : this(Type.PTable, nm, pp, cx)
		{}
        public PTable(long np, Context cx) : base(Type.PTable,np,cx) { }
        /// <summary>
        /// Constructor: a Table definition from the Parser
        /// </summary>
        /// <param name="t">The Ptable type</param>
        /// <param name="nm">The name of the table</param>
        /// <param name="wh">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected PTable(Type t, string nm, long pp, Context cx)
            : base(t, pp, cx)
		{
			name = nm;
		}
        /// <summary>
        /// Constructor: a Table definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PTable(Reader rdr) : base (Type.PTable,rdr)
		{}
        /// <summary>
        /// Constructor: a Table definition from the buffer
        /// </summary>
        /// <param name="t">The PTable type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected PTable(Type t, Reader rdr) : base(t,rdr) {}
        protected PTable(PTable x, Writer wr) : base(x, wr)
        {
            name = x.name;
            rowiri = x.rowiri;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PTable(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
		public override void Serialise(Writer wr)
		{
            wr.PutString(name??"");
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            name = rdr.GetString();
            rdr.names += (ppos, name);
			base.Deserialise(rdr);
		}
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
		{
			return "PTable "+name;
		}
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.PTable1:
                case Type.PTable:
                    return (name == ((PTable)that).name) ? ppos : -1;
                case Type.PView1:
                case Type.PView:
                    return (name == ((PView)that).name) ? ppos : -1;
                case Type.Change:
                    return (name == ((Change)that).name) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            // The definer is the given role
            var priv = Grant.Privilege.Owner | Grant.Privilege.Insert | Grant.Privilege.Select |
                Grant.Privilege.Delete | Grant.Privilege.References | Grant.Privilege.GrantDelete |
                Grant.Privilege.GrantSelect | Grant.Privilege.GrantInsert | Grant.Privilege.GrantReferences |
                Grant.Privilege.References | Grant.Privilege.GrantReferences |
                Grant.Privilege.Usage | Grant.Privilege.GrantUsage |
                Grant.Privilege.Trigger | Grant.Privilege.GrantTrigger;
            var tb = new Table(this);
            var ti = new ObInfo(ppos, name, Sqlx.TABLE, Domain.TableType)
                +(ObInfo.Privilege, priv);
            ro = ro + ti + (Role.DBObjects, ro.dbobjects + (name, ppos));
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db += (ro, p);
            cx.Install(tb, p);
        }
    }
    internal class PTable1 : PTable
    {
        public PTable1(string ir, string nm, long pp, Context cx)
            : base(Type.PTable1, nm, pp, cx)
        {
            rowiri = ir;
        }
        protected PTable1(Type typ, string ir, string nm, long pp, Context cx)
            : base(typ, nm, pp, cx)
        {
            rowiri = ir;
        }
        public PTable1(Reader rdr) : base(Type.PTable1, rdr) { }
        protected PTable1(Type tp, Reader rdr) : base(tp, rdr) { }
        protected PTable1(PTable1 x, Writer wr) : base(x, wr)
        {
            rowiri = x.rowiri;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PTable1(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutString(rowiri);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            rowiri = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "PTable1 " + name + " rowiri=" + rowiri;
        }
    }
    /// <summary>
    /// Modification to RowIri
    /// </summary>
    internal class AlterRowIri : PTable1
    {
        public long rowpos;
        public AlterRowIri(long pr, string ir, long pp, Context cx)
            : base(Type.AlterRowIri, ir, null, pp, cx)
        {
            rowpos = pr;
        }
        public AlterRowIri(Reader rdr) : base(Type.AlterRowIri, rdr) { }
        protected AlterRowIri(AlterRowIri x, Writer wr) : base(x, wr)
        {
            rowpos = wr.Fix(rowpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new AlterRowIri(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutLong(rowpos);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            var prev = rdr.GetLong(); // defpos is clobbered by base.Deserialise
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "AlterRowIri ["+rowpos+"] iri=" + rowiri;
        }
    }
    /// <summary>
    /// handle enforcement specification for Clearance
    /// </summary>
    internal class Enforcement :Physical
    {
        public long tabledefpos;
        public Grant.Privilege enforcement;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,tabledefpos)) return tabledefpos;
            return -1;
        }
        public Enforcement(Reader rdr) : base(Type.Enforcement, rdr)
        {
        }

        public Enforcement(long pt, Grant.Privilege en, long pp, Context cx)
            : base(Type.Enforcement, pp, cx)
        {
            tabledefpos = pt;
            enforcement = en;
        }

        protected Enforcement(Type tp, Reader rdr) : base(tp, rdr)
        {
        }

        protected Enforcement(Type typ, long pt, Grant.Privilege en, long pp, Context cx)
            : base(typ, pp, cx)
        {
            tabledefpos = pt;
            enforcement = en;
        }
        protected Enforcement(Enforcement x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.Fix(x.tabledefpos);
            enforcement = x.enforcement;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Enforcement(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutLong(tabledefpos);
            wr.PutLong((long)enforcement);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            tabledefpos = rdr.GetLong();
            enforcement = (Grant.Privilege)rdr.GetLong();
            base.Deserialise(rdr);
        }
        public static void Append(StringBuilder sb,Grant.Privilege enforcement)
        {
            if ((int)enforcement != 15)
            {
                if (enforcement.HasFlag(Grant.Privilege.Select))
                    sb.Append(" read");
                if (enforcement.HasFlag(Grant.Privilege.Insert))
                    sb.Append(" insert");
                if (enforcement.HasFlag(Grant.Privilege.Update))
                    sb.Append(" update");
                if (enforcement.HasFlag(Grant.Privilege.Delete))
                    sb.Append(" delete");
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Enforcement [" + tabledefpos + "] SCOPE");
            Append(sb, enforcement);
            return sb.ToString();
        }

        internal override void Install(Context cx, long p)
        {
            throw new NotImplementedException();
        }
    }
}
