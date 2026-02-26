using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.CodeDom.Compiler;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.Arm;
using System.Text;
using System.Xml;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2026
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
        /// The position in the table (-1 means don't care)
        /// </summary>
		public int seq;
        public virtual long defpos => ppos;
        /// <summary>
        /// The defining position of the domain
        /// </summary>
        public long domdefpos = -1L;
        public string dfs="",cn=""; 
		public bool optional = true;    // see PColumn2
        public long coldefault = -1L;
		public Generation generation = Generation.No; // ditto
        public long flags = 0L; // see PColumn3
        public long reftype = -1L;// ..
        public long keymap = -1L; // ..
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (table != null && dataType != null)
            {
                if (!Committed(wr, table.defpos)) return table.defpos;
                if (!Committed(wr, dataType.defpos)) return dataType.defpos;
                domdefpos = ((Domain)dataType.Fix(wr.cx)).Create(wr, tr);
            }
            return -1;
        }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">The PColumn type</param>
        /// <param name="pr">The table</param>
        /// <param name="nm">The name of the column</param>
        /// <param name="dm">The domain</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">The Context</param>
        public PColumn(Type t, Table pr, string nm, Domain dm, long pp, 
            Context cx) : base(t,pp,cx,nm,dm,-1L)
		{
			table = cx._Ob(pr.defpos) as Table??throw new DBException("42107",pr.name);
            seq = (int)table.representation.Count;
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
                coldefault = wr.cx.Fix(x.coldefault);
                tabledefpos = table.defpos;
                seq = x.seq;
                domdefpos = x.domdefpos;
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
            if (tr is not null && wr.cx.uids[ppos] is long q && q!=0L
                && wr.cx.db.objects[q] is Domain ndt)
                dataType = ndt;
            if (wr.cx.uids[tabledefpos] is long tp && tp!=0L && wr.cx.db.objects[tp] is Table ta)
                Commit(wr.cx, ta);
            var (nt,ph) = base.Commit(wr, tr);
            if (wr.cx.uids[tabledefpos] is long t && t!=0L && wr.cx.db.objects[t] is Table tb)
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
                 if (tb.infos[tb.definer] is ObInfo ti && ti._Fix(wr.cx) is ObInfo ni && ni.dbg != ti.dbg)
                {
                    tb += (DBObject.Infos, tb.infos + (tb.definer, ni));
                    tb += (ObInfo._Names, ni.names);
                }
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
            var rt = CTree<int,long>.Empty;
            for (var b = ta.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && p != ppos && p < Transaction.TransPos)
                    rt += ((int)rt.Count,p);
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
        /// <returns></returns>
        public TConnector? TCon(Domain dt)
        {
            if (flags != 0L)
            {
                var q = (flags & 0xfL) switch
                {
                    2L => Qlx.FROM,
                    4L => Qlx.TO,
                    8L => Qlx.WITH,
                    _ => Qlx.NO
                };
                var s = cn;
                if (cn == "")
                    s = name;
                TMetadata? tm = optional?(TMetadata.Empty+(Qlx.OPTIONAL,TBool.True)):null;
                return new TConnector(q, s, dt, ppos, false, "",tm);
            }
            return null;
        }
        public void FromTCon(TConnector tc)
        {
            flags = tc.q switch
            {
                Qlx.TO => 4L,
                Qlx.FROM => 2L,
                Qlx.WITH => 8L,
                _ => 0L
            };
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
        Qlx Con(long f)
        {
            switch (flags&0xfL)
            {
                case 4: return Qlx.TO;
                case 2: return Qlx.FROM;
                case 8: return Qlx.WITH;
            }
            return Qlx.NO;
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
            var ro = cx.role;
            if (table == null || dataType == null || ro == null)
                return null;
            if (cx.db.objects[table.defpos] is Table t && t.dbg>table.dbg)
                table = t;
            if (dataType.defpos > 0)
                cx.Install(dataType);
            var tc = new TableColumn(table, this, dataType, cx);
            tc += (DBObject._Framing, tc.framing + framing);
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
            var cd = tc.domain;
            tc += (TableColumn._Generation, generation);
            tc += (TableColumn.ColumnDefault, coldefault);
            tc += (DBObject._Framing, framing);
            if (cx._Ob(reftype) is Table rt && rt.defpos>0 && TCon(rt) is TConnector nc)
            {
    /*            tc = tc + (TableColumn.Connectors, new CTree<Domain,TConnector>(nc.rd,nc))
                    + (Level3.Index.RefTable, nc.rd.defpos); */
                if (keymap>0 && rt.FindPrimaryIndex(cx) is Level3.Index rx)
                    tc += (TableColumn.KeyMap, rx.defpos);
                if (flags != 0)
                    table += (ObInfo.Model, table.model + (Con(flags), defpos));
                var u = table.colRefs[reftype] ?? CTree<long, bool>.Empty;
                table += (Domain.ColRefs, table.colRefs + (reftype,u+ (tc.defpos,true)));
                rt += (Table.RefCols, rt.refCols + (tc.defpos, true));
                cx.toFix += (rt.defpos, true);
                cx.Add(rt);
                cx.db += rt;
            }
            table += (cx, tc);
            var rw = table.rowType;
            var ti = table.infos[cx.role.defpos] ?? throw new DBException("42105").Add(Qlx.COLUMN_NAME);
            ti += (ObInfo._Names, ti.names + (name, (0L,ppos)));
            if (dataType.names is Names ss && ss!=Names.Empty)
                ti += (ObInfo.Defs, ti.defs + (tc.defpos, ss));
            table += (DBObject.Infos, table.infos+(cx.role.defpos,ti));
            table += (ObInfo._Names, ti.names);
            table += (Domain.Display, (int)ti.names.Count);
            tc = (TableColumn)(cx.obs[tc.defpos] ?? throw new DBException("42105").Add(Qlx.COLUMN_NAME));
            tc = (TableColumn)tc.Apply(cx, table);
            cx.db += (tc.defpos, tc);
            table += (DBObject.LastChange, ppos);
            table += (ObInfo._Names, ti.names);
            cx.db += (table.defpos, table);
            if (table is UDType ut)
                for (var b = ut.methods.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is Method me && me.udType.defpos == ut.defpos)
                        cx.db += (me.defpos, me + (Method.TypeDef, ut));
            if (cx.db.objects[table.defpos] is Table nt && table.name.Length==0)
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
            cx.db += table;
            cx._Add(table);
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
        /// <param name="dm">The domain</param>
        /// <param name="dv">The column default value expression string </param>
        /// <param name="opt">Whether the column is optional</param>
        /// <param name="ge">The generation rule if any</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">GThe Context</param>
        public PColumn2(Table pr, string nm, Domain dm, 
            string ds, bool opt, Generation g, long nst, long pp, Context cx)
            : this(Type.PColumn2,pr,nm,dm,ds,opt,g,nst,pp,cx)
		{ }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The table</param>
        /// <param name="nm">The name of the ident</param>
        /// <param name="dm">The domain</param>
        /// <param name="dv">The column default value expression string</param>
        /// <param name="opt">Whether the column is optional</param>
        /// <param name="ge">The generation rule if any</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">GThe Context</param>
        protected PColumn2(Type t, Table pr, string nm, Domain dm, 
            string ds, bool opt, Generation g, long nst, long pp, Context cx)
            : base(t, pr, nm, dm, pp, cx)
        {
            dfs = ds;
            optional = opt;
            generation = g;
            if (generation == Generation.Expression || generation==Generation.Default)
                framing = new Framing(cx, nst);
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
            generation = x.generation;
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
            wr.PutInt((int)generation);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="rdr">the Reader for the file</param>
        public override void Deserialise(Reader rdr)
        {
            var dfsrc = new Ident(rdr.GetString(), ppos + 1);
            dfs = dfsrc.ident;
            optional = rdr.GetInt() <= 0;
            generation = (Generation)rdr.GetInt();
            base.Deserialise(rdr);
        }
        /// <summary>
        /// We are a subclass of Compiled: compile/recompile the source on creation and load
        /// </summary>
        /// <param name="rdr">The Reader for the file</param>
        internal override void OnLoad(Reader rdr)
        {
            if (table == null || dataType == null)
                return;
            if (dfs!="" && rdr.context.db.objects[table?.defpos ?? -1L] is Table tb)
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
                framing = new Framing(psr.cx, nst);
                coldefault = sv.defpos;
                rdr.context.parse = op;
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (dfs != "") { sb.Append(" default="); sb.Append(dfs); }
            if (!optional) sb.Append(" NOT NULL");
            if (generation != Generation.No) { sb.Append(" Generated="); sb.Append(generation); }
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
        /// <param name="dm">The declared domain of the table column</param>
        /// <param name="ms">The string version of the Metadata</param>
        /// <param name="md">Column Metadata </param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">The context</param>
        /// <param name="ifN">Only commit if referenced</param>
        public PColumn3(Table pr, string nm, Domain dm, string ms, 
            TMetadata md, long nst, long pp, Context cx, bool ifN = false)
            : this(pr, nm, dm, _Meta(cx,ms,md), nst, pp, cx, ifN)
        {  }
        PColumn3(Table pr, string nm, Domain dm, 
            (string,bool,Generation,long,long,long,long) xx, long nst, long pp, Context cx, bool ifN = false)
            : this(pr, nm, dm, xx.Item1, xx.Item2, xx.Item3, nst, pp, cx, ifN)
        {
            coldefault = xx.Item4;
            flags = xx.Item5;
            reftype = xx.Item6;
            keymap = xx.Item7;
        }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the table column</param>
        /// <param name="dm">The declared domain of the table column</param>
        /// <param name="ds">The column default value</param>
        /// <param name="opt">Whether the column is optional</param>
        /// <param name="ge">The generation rule if any</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">The Context</param>
        ///<param name="ifN">Only commit if referenced</param>
        public PColumn3(Table pr, string nm, Domain dm, 
            string ds, bool opt, Generation g, long nst, long pp,
            Context cx, bool ifN = false)
            : this(Type.PColumn3, pr, nm, dm, ds,opt,g, nst, pp, cx)
        {
            ifNeeded = ifN;
        }
        /// <summary>
        /// Constructor: A new Column definition from the Parser
        /// </summary>
        /// <param name="t">the PColumn2 type</param>
        /// <param name="pr">The defining position of the table</param>
        /// <param name="nm">The name of the table column</param>
        /// <param name="dm">The declared domain of the table column</param>
        /// <param name="ds">The column default value</param>
        /// <param name="opt">Whether the column is optional</param>
        /// <param name="ge">The generation rule if any</param>
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">GThe Context</param>
        protected PColumn3(Type t, Table pr, string nm, Domain dm, 
            string ds, bool opt, Generation g, long nst, long pp, Context cx)
            : base(t, pr, nm, dm, ds, opt, g, nst, pp, cx)
        {
            pr = (Table)(cx.obs[pr.defpos]??throw new PEException("PE30341"));
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
            cn = x.cn;
            flags = x.flags;
            reftype = wr.cx.Fix(x.reftype);
            table = (Table?)wr.cx.db.objects[wr.cx.Fix(x.table?.defpos??-1L)]??table;
        }
        /// <summary>
        /// Break out metdata pieces
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="ms">The metadata string</param>
        /// <param name="md">The Metadata</param>
        /// <param name="pp">The generation rule expression/param>
        /// <returns>Default string,whether optional,Generation rule,The Metadata,Framing</returns>
        static (string, bool, Generation, long, long, long, long) _Meta(Context cx,
            string ms, TMetadata md)
        {
            Generation g = Generation.No;
            string ds = "";
            if (md[Qlx.DEFAULT] is TChar s)
            {
                g = Generation.Default;
                ds = s.ToString();
            }
            if (md[Qlx.GENERATED] is TInt ev)
                g = (Generation)ev.value;
            if (md.Contains(Qlx.START))
                g = Generation.RowStart;
            if (md.Contains(Qlx.END))
                g = Generation.RowEnd;
            bool opt = md[Qlx.OPTIONAL].ToBool() ?? true;
            long dv = md[Qlx.VALUE].ToLong() ?? -1L;
            long flags = md[Qlx.ACTION].ToLong() ?? 0L;
            long refType = md[Qlx.REFERENCING].ToLong() ?? -1L;
            long keymap = md[Qlx.FOREIGN].ToLong() ?? -1L;
            if (md[Qlx.CONNECTING] is TConnector tc)
                refType = tc.rd.defpos;
            return (ds, opt, g, dv, flags, refType, keymap);
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
            wr.PutString(cn??"");
            wr.PutLong(flags);
            wr.PutLong(reftype); 
            wr.PutLong(keymap);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="rdr">the Reader for the file</param>
        public override void Deserialise(Reader rdr)
        {
            cn = rdr.GetString();
            flags = rdr.GetLong();
            reftype = rdr.GetLong();
            keymap = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(TCon());
            if (reftype>0)
            { sb.Append(" refType "); sb.Append(DBObject.Uid(reftype)); }
            if (keymap>0)
            { sb.Append(" keyMap "); sb.Append(DBObject.Uid(keymap)); }
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
