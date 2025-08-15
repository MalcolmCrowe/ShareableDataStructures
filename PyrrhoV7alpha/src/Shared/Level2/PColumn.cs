using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Text;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
	/// <summary>
	/// A PColumn belongs to a PTable and has a name, a sequence no, and a domain
	/// Both domains and TableColumns have check constraints, defaults, orderings and collates
	/// Though this seems redundant it is asking for trouble not to respect this SQL convention
	/// in the database structures. 
    /// (Domain defaults are widely used, but Domain programmable constraints are not currently needed.)
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
        public string dfs="",ups=""; // ups is used for cn if PColumn3 
        public BTree<UpdateAssignment,bool> upd = CTree<UpdateAssignment,bool>.Empty; // see PColumn3
		public bool optional = true;    // see PColumn2
		public GenerationRule generated = GenerationRule.None; // ditto
        public long flags = 0L; // see PColumn3
        public long toType = -1L; // ditto
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (table != null && dataType != null)
            {
                if (!Committed(wr, table.defpos)) return table.defpos;
                if (!Committed(wr, dataType.defpos)) return dataType.defpos;
                domdefpos = dataType.Create(wr, tr);
            }
            return -1;
        }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">The PColumn type</param>
        /// <param name="pr">The table</param>
        /// <param name="nm">The name of the column</param>
        /// <param name="sq">The 0-based position in the table (-1 if append)</param>
        /// <param name="dm">The domain</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">The Context</param>
        public PColumn(Type t, Table pr, string nm, int sq, Domain dm, long pp, 
            Context cx) : base(t,pp,cx,nm,dm,-1L)
		{
			table = cx._Ob(pr.defpos) as Table??throw new DBException("42107",pr.name);
			seq = (sq<0)?table.Length:sq;
            tabledefpos = pr.defpos;
            dataType = dm;
            domdefpos = dm.defpos;
        }
        /// <summary>
        /// Constructor: a new Column definition from the buffer
        /// </summary>
        /// <param name="rdr">The Reader for the file</param>
		public PColumn(Reader rdr) : base (Type.PColumn,rdr){}
        /// <summary>
        /// Constructor: a new Column definition from the buffer
        /// </summary>
        /// <param name="t">The PColumn type</param>
        /// <param name="rdr">The Reader for the file</param>
		protected PColumn(Type t,Reader rdr) : base(t,rdr) {}
        protected PColumn(PColumn x, Writer wr) : base(x, wr)
        {
            if (x.table != null && x.dataType is not null)
            {
                table = (Table)x.table.Fix(wr.cx);
                tabledefpos = table.defpos;
                seq = x.seq;
                domdefpos = x.domdefpos;
                toType = wr.cx.Fix(x.toType);
            }
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PColumn(this, wr);
        }
        /// <summary>
        /// Update the ongoing Transaction on Commit
        /// </summary>
        /// <param name="wr">The Writer for the file</param>
        /// <param name="tr">The Transaction</param>
        /// <returns>The updated Transaction and updated PColumn</returns>
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            if (tr is not null && wr.cx.uids[ppos] is long q
                && wr.cx.db.objects[q] is Domain ndt)
                dataType = ndt;
            if (wr.cx.uids[tabledefpos] is long tp && wr.cx.db.objects[tp] is Table ta)
                Commit(wr.cx, ta);
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
                var md = tb.metadata.Fix(wr.cx);
                if (md != tb.metadata)
                    tb += (ObInfo._Metadata, md);
                if (tb.infos[tb.definer] is ObInfo ti && ti._Fix(wr.cx) is ObInfo ni && ni.dbg != ti.dbg)
                {
                    tb += (DBObject.Infos, tb.infos + (tb.definer, ni));
                    tb += (ObInfo._Names, ni.names);
                }
                var sx = wr.cx.FixTlTlTlb(tb.sindexes);
                if (sx != tb.sindexes)
                    tb += (Table.SysRefIndexes, sx);
                if (tb.keyCols.Contains(ppos))
                    tb += (Table.KeyCols, tb.keyCols - ppos + (ph.ppos,true));
                if (ot != tb)
                    wr.cx.db += (tb.defpos, tb);
            } 
            return (nt,ph);
        }
        /// <summary>
        /// Handle supertype recursion
        /// </summary>
        /// <param name="cx">The Context</param>
        /// <param name="ta">A Table</param>
        void Commit(Context cx,Table ta)
        {
            var rt = CList<long>.Empty;
            for (var b = ta.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && p != ppos && p < Transaction.TransPos)
                    rt += p;
            ta += (Domain.RowType, rt);
            ta += (Domain.Representation, ta.representation - ppos);
            cx.db += (ta.defpos, ta);
            for (var b = ta.super.First(); b != null; b = b.Next())
                if (b.key() is Table t)
                    Commit(cx, t);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="wr">The Writer for the file</param>
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
        /// For EdgeType information
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        public TConnector? TCon(Context cx)
        {
            if (flags != 0L && cx.db.objects[toType] is Domain ct)
            {
                var q = (flags & 0xfL) switch
                {
                    1L => Qlx.ID,
                    2L => Qlx.FROM,
                    4L => Qlx.TO,
                    8L => Qlx.WITH,
                    _ => Qlx.NO
                };
                var dm = ((flags & 0x10L) == 0x10L) ? Domain.EdgeEnds : Domain.Position;
                var s = ups;
                if (ups == "")
                    s = name;
                TMetadata? tm = optional?(TMetadata.Empty+(Qlx.OPTIONAL,TBool.True)):null;
                return new TConnector(q, ct.defpos, s, dm, defpos,"",tm);
            }
            return null;
        }
        public void FromTCon(TConnector tc)
        {
            flags = tc.q switch
            {
                Qlx.ID => 1L,
                Qlx.TO => 4L,
                Qlx.FROM => 2L,
                Qlx.WITH => 8L,
                _ => 0L
            };
            if (tc.cd.kind == Qlx.SET)
            {
                flags += 16L;
                domdefpos = Domain.EdgeEnds.defpos;
            }
            toType = tc.ct;
        }
        public string TCon()
        {
            var s = ((flags & 0x10L) == 0L) ? "" : " SET";
            switch (flags&0xfL)
            {
                case 4: return " TO" + s;
                case 2: return " FROM" + s;
                case 8: return " WITH" + s;
            }
            return s;
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="rdr">the Reader for the file</param>
        public override void Deserialise(Reader rdr)
        {
            tabledefpos = rdr.GetLong();
            table = (Table)(rdr.GetObject(tabledefpos)
                ??new Table(defpos,BTree<long,object>.Empty));
            name = rdr.GetString();
            if (table is EdgeType et && TCon(rdr.context) is TConnector tc)
            {
                var ts = et.metadata[Qlx.EDGETYPE] as TSet ?? new TSet(Domain.Connector);
                rdr.context.db += et + (ObInfo._Metadata, et.metadata + (Qlx.EDGETYPE, ts + tc));
            }
            seq = rdr.GetInt();
            domdefpos = rdr.GetLong();
            dataType = (Domain)(rdr.GetObject(domdefpos) ?? throw new PEException("PE0301"));
            base.Deserialise(rdr);
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            if (table == null || that == null || dataType==null)
                return new DBException("42105").Add(Qlx.COLUMN_NAME);
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
            return sb.ToString();
        }
        /// <summary>
        /// Update the Database/Transaction on creation of this Column
        /// </summary>
        /// <param name="cx">The Context</param>
        /// <returns>The updated Domain (/Table/Type/View etc)</returns>
        internal override DBObject? Install(Context cx)
        {
            var ro = /* (table is VirtualTable) ? Database._system?.role : */ cx.role;
            if (table == null || dataType == null || ro == null)
                return null;
            // we allow a UDType to be converted to NodeType using metadata, so take care here
            if (cx.db.objects[table.defpos] is Table t && t.dbg>table.dbg)
                table = t;
            if (dataType.defpos > 0)
                cx.Install(dataType);
            var tc = new TableColumn(table, this, dataType, cx);
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
                throw new DBException("42105").Add(Qlx.COLUMN_NAME);
      //      seq = (tc.flags==GraphFlags.None) ? -1:tc.seq;
            if (name == "ID" && table is NodeType)
                table += (NodeType.IdCol, defpos);
            var ti = table.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.COLUMN_NAME);
            ti += (ObInfo._Names, ti.names + (name, (0L,ppos)));
            if (dataType.infos[cx.role.defpos]?.names is Names ss && ss!=Names.Empty)
                ti += (ObInfo.Defs, ti.defs + (tc.defpos, ss));
            table += (DBObject.Infos, table.infos+(cx.role.defpos,ti));
            table += (ObInfo._Names, ti.names);
            table += (Domain.Display, (int)ti.names.Count);
            table += (cx, tc); 
            tc = (TableColumn)(cx.obs[tc.defpos] ?? throw new DBException("42105").Add(Qlx.COLUMN_NAME));
            tc += (TableColumn.Seq, seq);
            tc = (TableColumn)tc.Apply(cx, table);
            var ms = "";
            var md = TMetadata.Empty;
            tc = (TableColumn)tc.Add(cx, ms, md);
            cx.db += (tc.defpos, tc);
            table += (DBObject.LastChange, ppos);
            table += (ObInfo._Names, ti.names);
            cx.db += (table.defpos, table);
            if (table is UDType ut)
                for (var b = ut.methods.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is Method me && me.udType.defpos == ut.defpos)
                        cx.db += (me.defpos, me + (Method.TypeDef, ut));
            if (table is NodeType nt && table.name.Length==0)
            {
                var ps = CTree<long, bool>.Empty;
                var ns = CTree<string, bool>.Empty;
                for (var b = nt.representation.First(); b != null; b = b.Next())
                {
                    ps += (b.key(), true);
                    ns += (name, true);
                }
                nt.AddNodeOrEdgeType(cx);
            }
            if (cx.db.format < 51)
            {
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
                cx.db += ro;
            }
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(tc);
            cx.obs += (table.defpos, table);
            cx.db += table;
            cx.AddDefs(0L,table);
            return table;
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
        /// <param name="pr">The table</param>
        /// <param name="nm">The name of the column (may be null)</param>
        /// <param name="sq">The position of the column in the table</param>
        /// <param name="dm">The domain</param>
        /// <param name="ds">The default value as a string</param>
        /// <param name="opt">Whether the column is optional</param>
        /// <param name="ge">The generation rule if any</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">GThe Context</param>
        public PColumn2(Table pr, string nm, int sq, Domain dm, 
            string ds, bool opt, GenerationRule ge, long pp, Context cx)
            : this(Type.PColumn2,pr,nm,sq,dm,ds,opt,ge,pp,cx)
		{ }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The table</param>
        /// <param name="nm">The name of the ident</param>
        /// <param name="sq">The position of the ident in the table</param>
        /// <param name="dm">The domain</param>
        /// <param name="ds">The default value as a string</param>
        /// <param name="opt">Whether the column is optional</param>
        /// <param name="ge">The generation rule if any</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">GThe Context</param>
        protected PColumn2(Type t, Table pr, string nm, int sq, Domain dm, 
            string ds, bool opt, GenerationRule ge, long pp, Context cx)
            : base(t, pr, nm, sq, dm, pp, cx)
        {
            dfs = ds;
            optional = opt;
            generated = ge;
            if (generated.gen == Generation.Expression)
            {
                generated += (RowSet.Target, ppos);
                framing = new Framing(cx, generated.nextStmt);
            }
		}
        /// <summary>
        /// Constructor: A new Column definition from the buffer
        /// </summary>
        /// <param name="rdr">The Reader for thefile</param>
		public PColumn2(Reader rdr) : this(Type.PColumn2,rdr){}
        /// <summary>
        /// Constructor: A new Column definition from the buffer
        /// </summary>
        /// <param name="t">The PColumn2 type</param>
        ///<param name="rdr">The Reader for thefile</param>
        protected PColumn2(Type t, Reader rdr) : base(t, rdr) { }
        protected PColumn2(PColumn2 x, Writer wr) : base(x, wr)
        {
            dfs = x.dfs;
            optional = x.optional;
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
        /// <param name="wr">The Writer for the file</param>
        public override void Serialise(Writer wr)
		{
            wr.PutString(dfs.ToString());
            wr.PutInt(optional ? 0 : 1);
            wr.PutInt((int)generated.gen);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="rdr">the Reader for the file</param>
        public override void Deserialise(Reader rdr) 
		{
            var dfsrc = new Ident(rdr.GetString(),ppos + 1);
            dfs = dfsrc.ident;
            optional = rdr.GetInt() <= 0;
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
        /// <summary>
        /// We are a subclass of Compiled: compile/recompile the source on creation and load
        /// </summary>
        /// <param name="rdr">The Reader for the file</param>
        internal override void OnLoad(Reader rdr)
        {
            if (table == null || dataType == null)
                return;
            if (generated.gen == Generation.Expression 
                && rdr.context.db.objects[table?.defpos ?? -1L] is Table tb)
            {
                table = tb;
                var psr = new Parser(rdr, new Ident(dfs, ppos + 1)); // calls ForConstraintParse
                var cx = psr.cx;
                cx.names = cx.anames;
                var op = rdr.context.parse;
                psr.cx.parse = ExecuteStatus.Compile;
                var nst = psr.cx.db.nextStmt;
                for (var b = tb.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.db.objects[p] is TableColumn tc)
                    {
                        var nm = tc.NameFor(cx)??"";
                        var rv = new QlInstance(0L,cx.GetUid(), cx, nm, -1L, tc.defpos);
                        cx.Add(rv);
                        cx.Add(nm, 0L, rv);
                    }
                var sv = psr.ParseSqlValue(new BTree<long,object>(DBObject._Domain,dataType));
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
            if (!optional) sb.Append(" NOT NULL");
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
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the table column</param>
        /// <param name="sq">The position of the table column in the table</param>
        /// <param name="dm">The declared domain of the table column</param>
        /// <param name="ms">The string version of the Metadata</param>
        /// <param name="md">Column Metadata</param>
        /// <param name="pp">The ransaction's position for this Physical</param>
        /// <param name="cx">The context</param>
        /// <param name="ifN">Only commit if referenced</param>
        public PColumn3(Table pr, string nm, int sq, Domain dm, string ms, 
            TMetadata md, long pp, Context cx, bool ifN = false)
            : this(pr, nm, sq, dm, _Meta(cx,ms,md,pp), pp, cx, ifN)
        {  }
        PColumn3(Table pr, string nm, int sq, Domain dm, 
            (string,bool,GenerationRule,TypedValue,Framing) xx, long pp, Context cx, bool ifN = false)
            : this(pr, nm, sq, dm, xx.Item1, xx.Item2, xx.Item3, xx.Item4, pp, cx, ifN)
        {
            framing = xx.Item5;
        }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the table column</param>
        /// <param name="sq">The position of the table column in the table</param>
        /// <param name="dm">The declared domain of the table column</param>
        /// <param name="ds">The default value as a string</param>
        /// <param name="opt">Whether the column is optional</param>
        /// <param name="ge">The generation rule if any</param>
        /// <param name="tm">The connector or TNull</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">The Context</param>
        ///<param name="ifN">Only commit if referenced</param>
        public PColumn3(Table pr, string nm, int sq, Domain dm, 
            string ds, bool opt, GenerationRule ge, TypedValue tm, long pp,
            Context cx, bool ifN = false)
            : this(Type.PColumn3, pr, nm, sq, dm, ds,opt,ge, tm, pp, cx)
        {
            ifNeeded = ifN;
        }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the table column</param>
        /// <param name="sq">The position of the table column in the table</param>
        /// <param name="dm">The declared domain of the table column</param>
        /// <param name="ds">The default value as a string</param>
        /// <param name="opt">Whether the column is optional</param>
        /// <param name="ge">The generation rule if any</param>
        /// <param name="tt">The connector or TNull</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">GThe Context</param>
        protected PColumn3(Type t, Table pr, string nm, int sq, Domain dm, 
            string ds, bool opt, GenerationRule ge, TypedValue tt, long pp, Context cx)
            : base(t, pr, nm, (sq<0)?pr.Length:sq, dm, ds, opt, ge, pp, cx)
        {
            if (((tt as TConnector)??((tt as TMetadata)?[Qlx.CONNECTING] as TConnector)) is TConnector tc)
            {
                var ts = pr.metadata[Qlx.EDGETYPE] as TSet ?? new TSet(Domain.Connector);
                ts += tc;
                if (table?.defpos>0)
                    cx.db += table = table + (ObInfo._Metadata, table.metadata + (Qlx.EDGETYPE, ts));
                else
                    cx.MetaPend(pr.defpos, defpos, nm, TMetadata.Empty + (Qlx.EDGETYPE, new TSet(Domain.Connector) + tc));
                FromTCon(tc);
            }
        }
        /// <summary>
        /// Constructor: A new Column definition from the buffer
        /// </summary>
        /// <param name="rdr">The Reader for the file</param>
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
            table = (Table?)wr.cx.db.objects[wr.cx.Fix(x.table?.defpos??-1L)]??table;
            toType = wr.cx.Fix(x.toType);
        }
        /// <summary>
        /// Break out metdata pieces
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="ms">The metadata string</param>
        /// <param name="md">The Metadata</param>
        /// <param name="pp">The transaction's position for the Physical</param>
        /// <returns>Default string,whether optional,Generation rule,The Metadata,Framing</returns>
        static (string, bool, GenerationRule, TypedValue, Framing) _Meta(Context cx,
            string ms, TMetadata md, long pp)
        {
            GenerationRule ge = new GenerationRule(md);
            TypedValue tv = md;
            var framing = Framing.Empty;
            string ds = md.Contains(Qlx.DEFAULT) ? md[Qlx.DEFAULT].ToString() : "";
            bool opt = md[Qlx.OPTIONAL].ToBool() ?? true;
            if (ge.gen == Generation.Expression)
            {
                ge += (RowSet.Target, pp);
                framing = new Framing(cx, ge.nextStmt);
            }
            return (ds, opt, ge, tv, framing);
        }
        internal override bool NeededFor(BTree<long, Physical> physicals)
        {
            if (!ifNeeded)
                return true;
            for (var b = physicals.First(); b != null; b = b.Next())
                if (b.value() is Record r && r.tabledefpos == tabledefpos)
                    return true;
            return false;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PColumn3(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="wr">The Writer for the file</param>
        public override void Serialise(Writer wr)
        {
            wr.PutString(ups??"");
            wr.PutLong(flags);
            wr.PutLong(-1L); // refIndex will not be defined on Deserialize
            wr.PutLong(toType);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="rdr">the Reader for the file</param>
        public override void Deserialise(Reader rdr)
        {
            ups = rdr.GetString();
            flags = rdr.GetLong();
            rdr.GetLong();
            toType = rdr.GetLong();
            base.Deserialise(rdr);
            if (toType>0)
                rdr.Upd(this);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (upd != CTree<UpdateAssignment, bool>.Empty) { sb.Append(" UpdateRule="); sb.Append(upd); }
            sb.Append(TCon());
            if (toType>0) { sb.Append(' '); sb.Append(DBObject.Uid(toType)); }
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
        /// <param name="pp">The trasaction's position for this Physical</param>
        /// <param name="cx">The Context</param>
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
        /// <param name="rdr">The Reader for the file</param>
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

        internal override DBObject? Install(Context cx)
        {
            throw new NotImplementedException();
        }
        protected override Physical Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
    }
}
