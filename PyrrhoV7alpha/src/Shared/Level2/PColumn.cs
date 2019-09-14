using System;
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
	/// A PColumn belongs to a PTable and has a name, a sequence no, and a domain
	/// Both domains and TableColumns have check constraints, defaults and collates
	/// Though this seems redundant it is asking for trouble not to respect this SQL convention
	/// in the database structs. (Actually domain defaults are more restrictive.)
	/// Columns may have a notNull constraint and integrity, uniqueness and referential constraints.
    /// Obsolete: see PColumn2
	/// </summary>
	internal class PColumn : Physical
	{
        internal enum GenerationRule { No, Expression, RowStart, RowEnd, Position };
        /// <summary>
        /// The defining position of the Table
        /// </summary>
		public long tabledefpos;
        /// <summary>
        /// The name of the TableColumn
        /// </summary>
		public string name;
        /// <summary>
        /// The position in the table (this matters for select * from ..)
        /// </summary>
		public int seq;
        /// <summary>
        /// The defining position of the domain
        /// </summary>
		public long domdefpos;
		public string dfs = ""; // see PColumn2
        public string upd = ""; // see PColumn3
		public bool notNull = false;    // ditto
		public GenerationRule generated = GenerationRule.No; // ditto
        public override long Dependent(Writer wr)
        {
            if (!Committed(wr,tabledefpos)) return tabledefpos;
            if (!Committed(wr,domdefpos)) return domdefpos;
            return -1;
        }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">The PColumn type</param>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the columns</param>
        /// <param name="sq">The 0-based position in the table</param>
        /// <param name="dm">The defining position of the domain</param>
        /// <param name="tb">The local database</param>
        public PColumn(Type t, long pr, string nm, int sq, long dm, long u,Database db)
			:base(t,u,db)
		{
			tabledefpos = pr;
			name = nm;
			seq = sq;
			domdefpos = dm;
		}
        /// <summary>
        /// Constructor: a new Column definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PColumn(Reader rdr) : base (Type.PColumn,rdr){}
        /// <summary>
        /// Constructor: a new Column definition from the buffer
        /// </summary>
        /// <param name="t">The PColumn type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected PColumn(Type t,Reader rdr) : base(t,rdr) {}
        protected PColumn(PColumn x, Writer wr) : base(x, wr)
        {
            tabledefpos = wr.Fix(x.tabledefpos);
            name = x.name;
            seq = x.seq;
            domdefpos = wr.Fix(x.domdefpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PColumn(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
			tabledefpos = wr.Fix(tabledefpos);
			domdefpos = wr.Fix(domdefpos);
            wr.PutLong(tabledefpos);
            wr.PutString(name.ToString());
            wr.PutInt(seq);
            wr.PutLong(domdefpos);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            tabledefpos = rdr.GetLong();
            name = rdr.GetString();
            seq = rdr.GetInt();
            domdefpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn:
                    return tabledefpos == ((PColumn)that).tabledefpos ? ppos : -1;
                case Type.Alter3:
                    {
                        var a = (Alter3)that;
                        return (tabledefpos == a.tabledefpos && name == a.name) ? ppos : -1;
                    }
                case Type.Alter2:
                    {
                        var a = (Alter2)that;
                        return (tabledefpos == a.tabledefpos && name == a.name) ? ppos : -1;
                    }
                case Type.Alter:
                    {
                        var a = (Alter)that;
                        return (tabledefpos == a.tabledefpos && name == a.name) ? ppos : -1;
                    }
                case Type.Drop:
                    {
                        var d = (Drop)that;
                        return (tabledefpos == d.delpos || domdefpos == d.delpos) ? ppos : -1;
                    }
            }
            return base.Conflicts(db, tr, that);
        }
        internal int flags
        {
            get
            {
                return (notNull ? 0x100 : 0) + ((generated == 0) ? 0 : 0x200);
            }
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
        {
            return "PColumn " + name + " for " + Pos(tabledefpos) + "(" + seq + ")[" + Pos(domdefpos) + "]";
        }

        internal override Database Install(Database db, Role ro, long p)
        {
            var t = (Table)db.role.objects[tabledefpos];
            var dt = db.GetDomain(domdefpos);
            var tc = new TableColumn(t, this, dt);
            t += tc;
            db += (tc,p);
            return db+(db.role,t,p);
        }
    }
    /// <summary>
    /// PColumn2: this is an extension of PColumn to add some column constraints
    /// For a general description see PColumn
    /// </summary>
	internal class PColumn2 : PColumn
	{
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the column (may be null)</param>
        /// <param name="sq">The position of the column in the table</param>
        /// <param name="dm">The defining position of the domain</param>
        /// <param name="ds">The default string</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">True if GENERATED ALWAYS</param>
        /// <param name="tb">The local database</param>
        public PColumn2(long pr, string nm, int sq, long dm, string ds, bool nn, 
            GenerationRule ge, long u,Database db)
			:this(Type.PColumn2,pr,nm,sq,dm,ds,nn,ge,u,db)
		{
        }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the ident</param>
        /// <param name="sq">The position of the ident in the table</param>
        /// <param name="dm">The defining position of the domain</param>
        /// <param name="ds">The default string</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">the type of Generation Rule</param>
        /// <param name="tb">The local database</param>
        protected PColumn2(Type t, long pr, string nm, int sq, long dm, string ds, 
            bool nn, GenerationRule ge, long u,Database db)
			:base(t,pr,nm,sq,dm,u,db)
		{
			dfs = ds;
			notNull = nn || ds!="" || (ge!=GenerationRule.No);
			generated = ge;
		}
        /// <summary>
        /// Constructor: A new Column definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PColumn2(Reader rdr) : this(Type.PColumn2,rdr){}
        /// <summary>
        /// Constructor: A new Column definition from the buffer
        /// </summary>
        /// <param name="t">The PColumn2 type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        protected PColumn2(Type t, Reader rdr) : base(t, rdr) { }
        protected PColumn2(PColumn2 x, Writer wr) : base(x, wr)
        {
            dfs = x.dfs;
            notNull = x.notNull;
            generated = x.generated;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PColumn2(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutString(dfs);
            wr.PutInt(notNull ? 1 : 0);
            wr.PutInt((int)generated);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr) 
		{ 
			dfs = rdr.GetString();
			notNull = (rdr.GetInt()!=0) || dfs!="";
			generated = (GenerationRule)rdr.GetInt();
            notNull = notNull || generated!=GenerationRule.No;
			base.Deserialise(rdr);
		}
        public override string ToString()
        {
            return base.ToString() + ((dfs != "") ? "default=" : "") + dfs + (notNull ? " NOT NULL" : "") + ((generated==GenerationRule.No) ? "" : generated.ToString());
        }
	}
    /// <summary>
    /// PColumn3: this is an extension of PColumn to add some column constraints.
    /// Specifically we add the readonly constraint
    /// For a general description see PColumn
    /// </summary>
    internal class PColumn3 : PColumn2
    {
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the table column</param>
        /// <param name="sq">The position of the table column in the table</param>
        /// <param name="dm">The defining position of the domain</param>
        /// <param name="ds">The default string</param>
        /// <param name="ua">The udate assignment rule</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">True if GENERATED ALWAYS</param>
        /// <param name="db">The local database</param>
        public PColumn3(long pr, string nm, int sq, long dm, string ds, 
            string ua, bool nn, GenerationRule ge, long u,Database db)
            : this(Type.PColumn3, pr, nm, sq, dm, ds, ua, nn, ge, u,db)
        { }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the ident</param>
        /// <param name="sq">The position of the ident in the table</param>
        /// <param name="dm">The defining position of the domain</param>
        /// <param name="ds">The default string</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">True if GENERATED ALWAYS</param>
        /// <param name="db">The local database</param>
        protected PColumn3(Type t, long pr, string nm, int sq, long dm, string ds,
            string ua, bool nn, GenerationRule ge, long u,Database db)
            : base(t, pr, nm, sq, dm, ds, nn, ge, u, db)
        {
            upd = ua;
        }
        /// <summary>
        /// Constructor: A new Column definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public PColumn3(Reader rdr) : this(Type.PColumn3, rdr) { }
        /// <summary>
        /// Constructor: A new Column definition from the buffer
        /// </summary>
        /// <param name="t">The PColumn2 type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        protected PColumn3(Type t, Reader rdr) : base(t, rdr) { }
        protected PColumn3(PColumn3 x, Writer wr) : base(x, wr)
        {
            upd = x.upd;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PColumn3(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
        {
            wr.PutString(upd); 
            wr.PutLong(-1);// backwards compatibility
            wr.PutLong(-1);
            wr.PutLong(-1);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            upd = rdr.GetString();
            rdr.GetLong();
            rdr.GetLong();
            rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return base.ToString() + ((upd!="")?( "Update="+upd):"");
        }
    }
    /// <summary>
    /// Pyrrho 5.1. To allow constraints (even Primary Key) to refer to deep structure.
    /// This feature is introduced for Documents but will be used for row type columns, UDTs etc.
    /// </summary>
    internal class PColumnPath : Physical
    {
        /// <summary>
        /// The defining position of the Column
        /// </summary>
        public virtual long defpos { get { return ppos; } }
        /// <summary>
        /// The selector to which this path is appended
        /// </summary>
        public long coldefpos;
        /// <summary>
        /// a single component of the ColumnPath string
        /// </summary>
        public string path = null;
        /// <summary>
        /// The domain if known
        /// </summary>
        public long domdefpos;
        public override long Dependent(Writer wr)
        {
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,coldefpos)) return coldefpos;
            if (!Committed(wr,domdefpos)) return domdefpos;
            return -1;
        }
        /// <summary>
        /// Constructor: A ColumnmPath definition from the Parser
        /// </summary>
        /// <param name="co">The Column</param>
        /// <param name="pa">The path string</param>
        /// <param name="dm">The domain defining position</param>
        /// <param name="db">The local database</param>
        public PColumnPath(long co, string pa, long dm, long u, Transaction tr) 
            : base(Type.ColumnPath, u, tr)
        { 
            coldefpos = co;
            path = pa;
            domdefpos = dm;
        }
        /// <summary>
        /// Constructor: from the file buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public PColumnPath(Reader rdr) : base(Type.ColumnPath, rdr) { }
        public override void Serialise(Writer wr)
        {
            coldefpos = wr.Fix(coldefpos);
            domdefpos = wr.Fix(domdefpos);
            wr.PutLong(coldefpos);
            wr.PutString(path);
            wr.PutLong(domdefpos);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            coldefpos = rdr.GetLong();
            path = rdr.GetString();
            domdefpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "ColumnPath [" + coldefpos + "]" + path + "(" + domdefpos + ")";
        }

        internal override Database Install(Database db, Role ro, long p)
        {
            throw new NotImplementedException();
        }

        protected override Physical Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
    }
}
