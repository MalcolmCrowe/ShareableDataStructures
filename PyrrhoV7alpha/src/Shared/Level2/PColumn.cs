using System.Text;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Common;
using Pyrrho.Level5;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
 
namespace Pyrrho.Level2
{
	/// <summary>
	/// A PColumn belongs to a PTable and has a name, a sequence no, and a domain
	/// Both domains and TableColumns have check constraints, defaults and collates
	/// Though this seems redundant it is asking for trouble not to respect this SQL convention
	/// in the database structures. 
    /// (Domain defaults are widely used, but Domain programmable constraints are not currenmtly needed.)
	/// Columns may have a notNull constraint and integrity, uniqueness and referential constraints, 
    /// and their Framing contains code to implement generation rules and check constraints.
    /// Obsolete: see PColumn2, PColumn3
	/// </summary>
	internal class PColumn : Compiled
	{
        /// <summary>
        /// The Table
        /// </summary>
		public Table? table; // may be a shadow: watch for NodeType/EdgeType
        public long tabledefpos;
        /// <summary>
        /// The position in the table (-1 means don't care: replace by rowType.Length)
        /// </summary>
		public int seq;
        public virtual long defpos => ppos;
        /// <summary>
        /// The defining position of the domain
        /// </summary>
        public long domdefpos = -1L;
		public TypedValue dv => dataType?.defaultValue??TNull.Value; 
        public string dfs="",ups="";
        public BTree<UpdateAssignment,bool> upd = CTree<UpdateAssignment,bool>.Empty; // see PColumn3
		public bool notNull = false;    // see PColumn2
		public GenerationRule generated = GenerationRule.None; // ditto
        [Flags] // for Typed Graph Model June2023
        internal enum GraphFlags { None = 0, IdCol = 1, LeaveCol = 2, ArriveCol = 4, SetValue = 8 }
        public GraphFlags flags;
        public long index = -1L;  // IdIx // Leave or Arrive Ix
        public long toType = -1L; // Leave or Arrive Type
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (table != null && dataType != null)
            {
                if (!Committed(wr, table.defpos)) return table.defpos;
                domdefpos = dataType.Create(wr, tr);
            }
            return -1;
        }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">The PColumn type</param>
        /// <param name="pr">The table</param>
        /// <param name="nm">The name of the columns</param>
        /// <param name="sq">The 0-based position in the table</param>
        /// <param name="dm">The domain</param>
        /// <param name="tb">The local database</param>
        public PColumn(Type t, Table pr, string nm, int sq, Domain dm, long pp, 
            Context cx) : base(t,pp,cx,nm,dm,-1L)
		{
			table = pr;
			seq = sq;
            tabledefpos = pr.defpos;
            dataType = dm;
            domdefpos = dm.defpos;
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
            if (x.table != null && x.dataType is not null)
            {
                table = (Table)x.table.Fix(wr.cx);
                tabledefpos = table.defpos;
                seq = x.seq;
                domdefpos = x.domdefpos;
            }
        }
        protected override Physical Relocate(Writer wr)
        {
            index = wr.cx.Fix(index);
            toType = wr.cx.Fix(toType);
            return new PColumn(this, wr);
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            if (tr is not null && wr.cx.uids[ppos] is long q
                && wr.cx.db.objects[q] is Domain ndt)
                dataType = ndt;
            if (flags.HasFlag(GraphFlags.ArriveCol))
                index = (dataType as EdgeType)?.arriveIx ?? -1L; 
            else
               if (flags.HasFlag(GraphFlags.IdCol))
                index = (dataType as NodeType)?.idIx??-1L;
            else if (flags.HasFlag(GraphFlags.LeaveCol))
                index = (dataType as EdgeType)?.leaveIx ?? -1L;
            if (wr.cx.uids[tabledefpos] is long tp)
                for (var ta =wr.cx.db.objects[tp] as Table; ta != null; 
                    ta = wr.cx.db.objects[ta.super?.defpos??-1L] as Table)
            {
                var rt = BList<long?>.Empty;
                for (var b = ta.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && p != ppos && p<Transaction.TransPos)
                        rt += p;
                ta += (Domain.RowType, rt);
                ta += (Domain.Representation, ta.representation - ppos);
                ta += (Table.PathDomain, ta._PathDomain(wr.cx));
                wr.cx.db += (ta.defpos, ta);
            }
            var (nt,ph) = base.Commit(wr, tr);
            if (wr.cx.uids[tabledefpos] is long t && wr.cx.db.objects[t] is Table tb)
            {
                var ot = tb;
                var xs = tb.indexes;
                for (var b = xs.First(); b != null; b = b.Next())
                    if (b.key() is Domain d && d.Fix(wr.cx) is Domain nd && nd != d && nd.Length!=0)
                    {
                        xs -= d;
                        xs += (nd, b.value());
                    }
                if (xs != tb.indexes)
                    tb += (Table.Indexes, xs);
                var rx = tb.rindexes;
                for (var b = rx.First(); b != null; b = b.Next())
                    if (b.value() is CTree<Domain, Domain> td)
                    {
                        for (var c = td.First(); c != null; c = c.Next())
                            if (c.value() is Domain d && d.Fix(wr.cx) is Domain nd && nd != d)
                                td += (c.key(), nd);
                        if (td != b.value())
                            rx += (b.key(), td);
                    }
                if (rx != tb.rindexes)
                    tb += (Table.RefIndexes, rx);
                if (tb.keyCols.Contains(ppos))
                    tb += (Table.KeyCols, tb.keyCols - ppos + (ph.ppos,true));
                if (ot != tb)
                    wr.cx.db += (tb.defpos, tb);
            }
            return (nt,ph);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
            if (table == null || dataType == null)
                return;
			table = (Table)table.Fix(wr.cx);
            tabledefpos = table.defpos;
            wr.PutLong(table.defpos);
            wr.PutString(name.ToString());
            wr.PutInt(seq);
            domdefpos = wr.cx.Fix(domdefpos);
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
            table = (Table)(rdr.GetObject(tabledefpos)
                ??new Table(defpos,BTree<long,object>.Empty));
            name = rdr.GetString();
            seq = rdr.GetInt();
            domdefpos = rdr.GetLong();
            dataType = (Domain)(rdr.GetObject(domdefpos) ?? throw new PEException("PE0301"));
            base.Deserialise(rdr);
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            if (table == null || that == null || dataType==null)
                return new DBException("42105");
            switch(that.type)
            {
                case Type.PColumn3:
                case Type.PColumn2:
                case Type.PColumn:
                    if (that is PColumn pc && table.defpos == pc.table?.defpos)
                        return new DBException("40025", defpos, that, ct);
                    break;
                case Type.Alter3:
                    {
                        var a = (Alter3)that;
                        if (table.defpos == a.table?.defpos && name == a.name)
                            return new DBException("40025", table.defpos, that, ct);
                        break;
                    }
                case Type.Alter2:
                    {
                        var a = (Alter2)that;
                        if (table.defpos == a.table?.defpos && name == a.name)
                            return new DBException("40025", defpos, that, ct);
                        break;
                    }
                case Type.Alter:
                    {
                        var a = (Alter)that;
                        if (table.defpos == a.table?.defpos && name == a.name)
                            return new DBException("40025", defpos, that, ct);
                        break;
                    }
                case Type.Drop:
                    {
                        var d = (Drop)that;
                        if (table.defpos == d.delpos)
                            return new DBException("40012", table.defpos, that, ct);
                        if (cx.db.Find(dataType)?.defpos == d.delpos)
                            return new DBException("40016", defpos, that, ct);  
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            sb.Append(' '); sb.Append(name); sb.Append(" for ");
            sb.Append(Pos(tabledefpos));
            sb.Append('('); sb.Append(seq); sb.Append(")[");
            if (domdefpos >= 0)
                sb.Append(DBObject.Uid(domdefpos));
            else
                sb.Append(dataType); 
            sb.Append(']');
            if (flags != GraphFlags.None)
                sb.Append(" " + flags);
            if (index > 0)
                sb.Append(" " + DBObject.Uid(index));
            if (toType> 0)
                sb.Append(" "+DBObject.Uid(toType));
            return sb.ToString();
        }
        internal override DBObject? Install(Context cx, long p)
        {
            var ro = /* (table is VirtualTable) ? Database._system?.role : */ cx.role;
            if (table == null || dataType == null || ro == null)
                return null;
            // we allow a UDType to be converted to NodeType using metadata, so take care here
            if (cx.db.objects[table.defpos] is Table t && t.dbg>table.dbg)
                table = t;
            if (dataType.defpos > 0)
                cx.Install(dataType, p);
            var tc = new TableColumn(table, this, dataType, ro, cx.user);
            tc += (DBObject._Framing, framing);
            var rp = ro.defpos;
            var oi = table.infos[rp];
            if (oi == null)
                return null;
            var priv = oi.priv & ~(Grant.Privilege.Delete | Grant.Privilege.GrantDelete);
            var oc = new ObInfo(name, priv);
            tc += (DBObject.Infos, tc.infos + (rp, oc)); // table name will already be known
            cx.Add(tc);
            if (table.defpos < 0)
                throw new DBException("42105");
            seq = (tc.flags==GraphFlags.None) ? -1:tc.seq;
            table += (cx, seq, tc); // this is where the NodeType stuff happens
            tc = (TableColumn)(cx.obs[tc.defpos] ?? throw new DBException("42105"));
            tc += (TableColumn.Seq, seq);
            cx.db += (tc.defpos, tc);
            table += (DBObject.LastChange, ppos);
            for (var b = table.subtypes.First(); b != null; b = b.Next())
                if (cx._Ob(b.key()) is NodeType st)
                {
                    st += (Domain.Under, table);
                    cx.db += (st, cx.db.loadpos);
                    cx.db += st;
                }
            cx.Install(table, p);
            cx.db += (table.defpos, table);
            if (table is UDType ut)
                for (var b = ut.methods.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is Method me && me.udType.defpos == ut.defpos)
                        cx.db += (me.defpos, me + (Method.TypeDef, ut));
            if (cx.db.format < 51)
            {
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
                cx.db += (ro, p);
            }
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(tc, p);
            cx.obs += (table.defpos, table);
            cx.db += table;
            return table;
        }
    }
    /// <summary>
    /// PColumn2: this is an extension of PColumn to add some column constraints
    /// Show a general description see PColumn
    /// </summary>
	internal class PColumn2 : PColumn
	{
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="pr">The table</param>
        /// <param name="nm">The name of the column (may be null)</param>
        /// <param name="sq">The position of the column in the table</param>
        /// <param name="dm">The domain</param>
        /// <param name="dv">The default value</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">The generation rule</param>
        /// <param name="db">The local database</param>
        public PColumn2(Table pr, string nm, int sq, Domain dm, string ds, TypedValue dv, 
            bool nn, GenerationRule ge, long pp, Context cx)
            : this(Type.PColumn2,pr,nm,sq,dm,ds,dv,nn,ge,pp,cx)
		{ }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The table</param>
        /// <param name="nm">The name of the ident</param>
        /// <param name="sq">The position of the ident in the table</param>
        /// <param name="dm">The domain</param>
        /// <param name="ds">The default value</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">the Generation Rule</param>
        /// <param name="db">The database</param>
        protected PColumn2(Type t, Table pr, string nm, int sq, Domain dm, string ds,
            TypedValue v, bool nn, GenerationRule ge, long pp, Context cx)
            : base(t,pr,nm,sq,dm+(Domain.Default,v),pp,cx)
		{
			dfs = ds;
			notNull = nn;
			generated = ge;
            if (ge.gen == Generation.Expression)
            {
                generated += (RowSet.Target, ppos);
                framing = new Framing(cx, ge.nextStmt);
            }
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
            generated = (GenerationRule)x.generated.Fix(wr.cx);
            framing = (Framing)x.framing.Fix(wr.cx);
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
            var dfsrc = new Ident(rdr.GetString(), rdr.context.Ix(ppos + 1));
            dfs = dfsrc.ident;
            notNull = (rdr.GetInt() != 0);
			var gn = (Generation)rdr.GetInt();
            base.Deserialise(rdr);
            if (dfs != "")
            {
                if (gn != Generation.Expression)
                {
                    var dm = (Domain?)rdr.context.db.objects[domdefpos];
                    if (dm != null)
                        dataType = dm + (Domain.Default, dm.Parse(rdr.Position, dfs, rdr.context))
                            + (Domain.DefaultString, dfs);
                }
                else
                    generated = new GenerationRule(Generation.Expression,
                        dfs, SqlNull.Value, defpos, rdr.context.db.nextStmt);
            }
        }
        internal override void OnLoad(Reader rdr)
        {
            if (table == null || dataType == null)
                return;
            if (generated.gen == Generation.Expression 
                && rdr.context.db.objects[table?.defpos ?? -1L] is Table tb)
            {
                table = tb;
                var psr = new Parser(rdr, new Ident(dfs, rdr.context.Ix(ppos + 1))); // calls ForConstraintParse
                var op = rdr.context.parse;
                rdr.context.parse = ExecuteStatus.Compile;
                var nst = psr.cx.db.nextStmt;
                var sv = psr.ParseSqlValue(dataType);
                psr.cx.Add(sv);
                generated += (GenerationRule.GenExp, sv.defpos);
                framing = new Framing(psr.cx, nst);
                rdr.context.parse = op;
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
    /// Changed for the Typed Graph Model
    /// For a general description see PColumn
    /// </summary>
    internal class PColumn3 : PColumn2
    {
        public PColumn3(UDType ut, string nm, int sq, Domain dm,
            GraphFlags gf, long ix, long tp, long pp, Context cx)
            : this(ut, nm, sq, dm, "", TNull.Value, "", CTree<UpdateAssignment, bool>.Empty,
                    dm.notNull, GenerationRule.None, gf, ix, tp, pp, cx)
        { }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the table column</param>
        /// <param name="sq">The position of the table column in the table</param>
        /// <param name="dm">The domain</param>
        /// <param name="dv">The default value</param>
        /// <param name="ua">The update assignments</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">The generation rule</param>
        /// <param name="db">The local database</param>
        public PColumn3(Table pr, string nm, int sq, Domain dm, string ds, TypedValue dv, 
            string us, CTree<UpdateAssignment,bool> ua, bool nn, GenerationRule ge,
                        GraphFlags gf, long ix, long tp, long pp, Context cx)
            : this(Type.PColumn3, pr, nm, sq, dm, ds, dv, us, ua, nn, ge, gf, ix, tp, pp, cx)
        { }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the ident</param>
        /// <param name="sq">The position of the ident in the table</param>
        /// <param name="dm">The domain</param>
        /// <param name="dv">The default value</param>
        /// <param name="nn">True if the NOT NULL constraint is to apply</param>
        /// <param name="ge">The generation rule</param>
        /// <param name="db">The local database</param>
        protected PColumn3(Type t, Table pr, string nm, int sq, Domain dm, string ds, 
            TypedValue dv, string us, CTree<UpdateAssignment,bool> ua, bool nn, 
            GenerationRule ge, GraphFlags gf, long ix, long tp, long pp, Context cx)
            : base(t, pr, nm, sq, dm, ds, dv, nn, ge, pp, cx)
        {
            upd = ua;
            ups = us;
            flags = gf;
            index = ix;
            toType = tp;
            if ((flags.HasFlag(GraphFlags.LeaveCol) || flags.HasFlag(GraphFlags.ArriveCol)) && toType < 0)
                ;
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
            flags = x.flags;
            index = wr.cx.Fix(x.index);
            toType = wr.cx.Fix(x.toType);
        }
        protected override Physical Relocate(Writer wr)
        {
            index = wr.cx.Fix(index);
            toType = wr.cx.Fix(toType);
            return new PColumn3(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
        {
            wr.PutString(ups??""); 
            wr.PutLong((long)flags);
            wr.PutLong(index);
            wr.PutLong(toType);
            if ((flags.HasFlag(GraphFlags.LeaveCol) || flags.HasFlag(GraphFlags.ArriveCol)) && toType < 0)
                ;
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            ups = rdr.GetString();
            rdr.Upd(this);
            flags = (GraphFlags)rdr.GetLong();
            index = rdr.GetLong();
            toType = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (upd != CTree<UpdateAssignment,bool>.Empty) { sb.Append(" UpdateRule="); sb.Append(upd); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Pyrrho 5.1. To allow constraints (even Primary Key) to refer to deep structure.
    /// This feature is introduced for Documents but will be used for row type columns, UDTs etc.
    /// </summary>
    internal class PColumnPath : Defined
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
        public string? path = null;
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
            : base(Type.ColumnPath, pp, cx, "", Grant.AllPrivileges)
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
            coldefpos = wr.cx.Fix(coldefpos);
            domdefpos = wr.cx.Fix(domdefpos);
            wr.PutLong(coldefpos);
            wr.PutString(path ?? throw new PEException("PE0303"));
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

        internal override DBObject? Install(Context cx, long p)
        {
            throw new NotImplementedException();
        }

        protected override Physical Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
    }
}
