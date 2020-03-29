using System;
using System.Text;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
        public virtual long defpos => ppos;
        /// <summary>
        /// The defining position of the domain
        /// </summary>
		public long domdefpos;
		public TypedValue dv = null; // see PColumn2
        public string dfs,ups;
        public BList<UpdateAssignment> upd = BList<UpdateAssignment>.Empty; // see PColumn3
		public bool notNull = false;    // ditto
		public GenerationRule generated = GenerationRule.None; // ditto
        public override long Dependent(Writer wr, Transaction tr)
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
        public PColumn(Type t, long pr, string nm, int sq, long dm, long pp, 
            Context cx) : base(t,pp,cx)
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
                return (notNull ? 0x100 : 0) + ((generated.gen == 0) ? 0 : 0x200);
            }
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(" "); sb.Append(name); sb.Append(" for ");
            sb.Append(Pos(tabledefpos));
            sb.Append("("); sb.Append(seq); sb.Append(")[");
            sb.Append(Pos(domdefpos)); sb.Append("]");
            return sb.ToString();
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var tb = (Table)cx.db.objects[tabledefpos];
            var ti = (ObInfo)ro.obinfos[tb.defpos];
            var dt = (Domain)cx.db.objects[domdefpos];
            var tc = new TableColumn(tb, this, dt);
            // the given role is the definer
            var priv = ti.priv & ~(Grant.Privilege.Delete | Grant.Privilege.GrantDelete);
            var oc = new ObInfo(ppos, name, (Domain)cx.db.objects[domdefpos], priv);
            if (cx.db.format < 51)
                ti += (ObInfo.Map, ti.map + (ppos.ToString(), ti.Length));
            ro = ro + (oc.defpos,oc) + (ti + oc);
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            tb += tc;
            cx.db = cx.db + (ro, p) + (tb, p) + (tc, p);
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
        /// <param name="dv">The default value</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">The generation rule</param>
        /// <param name="db">The local database</param>
        public PColumn2(long pr, string nm, int sq, long dm, string ds, TypedValue dv, 
            bool nn, GenerationRule ge, long pp, Context cx)
            : this(Type.PColumn2,pr,nm,sq,dm,ds,dv,nn,ge,pp,cx)
		{ }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the ident</param>
        /// <param name="sq">The position of the ident in the table</param>
        /// <param name="dm">The defining position of the domain</param>
        /// <param name="ds">The default value</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">the Generation Rule</param>
        /// <param name="db">The database</param>
        protected PColumn2(Type t, long pr, string nm, int sq, long dm, string ds,
            TypedValue v, bool nn, GenerationRule ge, long pp, Context cx)
            : base(t,pr,nm,sq,dm,pp,cx)
		{
			dfs = ds;
            dv = v;
			notNull = nn;
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
            dv = x.dv;
            notNull = x.notNull;
            generated = (GenerationRule)x.generated.Relocate(wr);
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
            wr.PutString(dfs.ToString());
            wr.PutInt(notNull ? 1 : 0);
            wr.PutInt((int)generated.gen);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr) 
		{ 
			dfs = rdr.GetString();
            notNull = (rdr.GetInt() != 0);
			var gn = (Generation)rdr.GetInt();
			base.Deserialise(rdr);
            if (dfs != "")
            {
                var dt = (Domain)rdr.context.db.objects[domdefpos];
                if (gn != Generation.Expression)
                    dv = dt.Parse(rdr.Position,dfs);
                else
                {
                    var oi = (ObInfo)rdr.role.obinfos[domdefpos];
                    generated = new GenerationRule(Generation.Expression,
                        dfs, new Parser(rdr.context)
                        .ParseSqlValue(dfs,oi).Reify(rdr.context,oi));
                }
            }
		}
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (dfs != "") { sb.Append(" default="); sb.Append(dfs); }
            if (notNull) sb.Append(" NOT NULL");
            if (generated.gen != Generation.No) { sb.Append(" Generated="); sb.Append(generated.gen); }
            return sb.ToString();
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
        /// <param name="dv">The default value</param>
        /// <param name="ua">The update assignments</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">The generation rule</param>
        /// <param name="db">The local database</param>
        public PColumn3(long pr, string nm, int sq, long dm, string ds, TypedValue dv, 
            string us, BList<UpdateAssignment> ua, bool nn, GenerationRule ge, long pp,
            Context cx)
            : this(Type.PColumn3, pr, nm, sq, dm, ds, dv, us, ua, nn, ge, pp, cx)
        { }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the ident</param>
        /// <param name="sq">The position of the ident in the table</param>
        /// <param name="dm">The defining position of the domain</param>
        /// <param name="dv">The default value</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">The generation rule</param>
        /// <param name="db">The local database</param>
        protected PColumn3(Type t, long pr, string nm, int sq, long dm, string ds, 
            TypedValue dv, string us, BList<UpdateAssignment> ua, bool nn, 
            GenerationRule ge, long pp, Context cx)
            : base(t, pr, nm, sq, dm, ds, dv, nn, ge, pp, cx)
        {
            upd = ua;
            ups = us;
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
            ups = x.ups;
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
            wr.PutString(ups??""); 
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
            ups = rdr.GetString();
            if (ups != "")
                try
                {
                    upd = new Parser(rdr.context).ParseAssignments(ups,
                        (ObInfo)rdr.role.obinfos[tabledefpos]);
                } catch(Exception)
                {
                    upd = BList<UpdateAssignment>.Empty;
                }
            rdr.GetLong();
            rdr.GetLong();
            rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (upd != BList<UpdateAssignment>.Empty) { sb.Append(" UpdateRule="); sb.Append(upd); }
            return sb.ToString();
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
        public override long Dependent(Writer wr, Transaction tr)
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
        public PColumnPath(long co, string pa, long dm, long pp, Context cx)
            : base(Type.ColumnPath, pp, cx)
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

        internal override void Install(Context cx, long p)
        {
            throw new NotImplementedException();
        }

        protected override Physical Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
    }
}
