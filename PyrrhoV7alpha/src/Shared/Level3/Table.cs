using System;
using System.Text;
using System.Collections.Generic;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using System.Runtime.CompilerServices;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
namespace Pyrrho.Level3
{
    /// <summary>
    /// When a Table is accessed
    /// any role with select access to the table will be able to retrieve rows subject 
    /// to security clearance and classification. Which columns are accessible also depends
    /// on privileges (but columns are not subject to classification).
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class Table : DBObject
    {
        internal const long
            ApplicationPS = -262, // long PeriodSpecification
            Enforcement = -263, // Grant.Privilege (T)
            Indexes = -264, // CTree<CList<long>,long> TableColumn,Index
            KeyCols = -320, // CTree<long,bool> TableColumn (over all indexes)
            LastData = -258, // long
            RefIndexes = -250, // CTree<long,CTree<CList<long>,CList<long>>> referencing Table,referencing TableColumns,referenced TableColumns
            SystemPS = -265, //long (system-period specification)
            TableChecks = -266, // BTree<long,bool> Check
            TableCols = -332, // BTree<long,Domain> TableColumn
            TableRows = -181, // BTree<long,TableRow>
            Triggers = -267; // BTree<PTrigger.TrigType,BTree<long,bool>> (T) 
        /// <summary>
        /// The rows of the table with the latest version for each
        /// </summary>
		public BTree<long, TableRow> tableRows => 
            (BTree<long,TableRow>)mem[TableRows]??BTree<long,TableRow>.Empty;
        public CTree<CList<long>, long> indexes => 
            (CTree<CList<long>,long>)mem[Indexes]??CTree<CList<long>,long>.Empty;
        public CTree<long, bool> keyCols =>
            (CTree<long, bool>)mem[KeyCols] ?? CTree<long, bool>.Empty;
        internal CTree<long, Domain> tblCols =>
            (CTree<long, Domain>)mem[TableCols] ?? CTree<long, Domain>.Empty;
        /// <summary>
        /// Enforcement of clearance rules
        /// </summary>
        internal Grant.Privilege enforcement => (Grant.Privilege)(mem[Enforcement]??0);
        internal long applicationPS => (long)(mem[ApplicationPS] ?? -1L);
        internal string iri => (string)mem[Domain.Iri];
        internal long systemPS => (long)(mem[SystemPS] ?? -1L);
        internal CTree<long,CTree<CList<long>,CList<long>>> rindexes =>
            (CTree<long,CTree<CList<long>,CList<long>>>)mem[RefIndexes] 
            ?? CTree<long,CTree<CList<long>,CList<long>>>.Empty;
        internal CTree<long, bool> tableChecks => 
            (CTree<long, bool>)mem[TableChecks]??CTree<long,bool>.Empty;
        internal CTree<PTrigger.TrigType, CTree<long,bool>> triggers =>
            (CTree<PTrigger.TrigType, CTree<long, bool>>)mem[Triggers]
            ??CTree<PTrigger.TrigType, CTree<long, bool>>.Empty;
        internal virtual long lastData => (long)(mem[LastData] ?? 0L);
        /// <summary>
        /// Constructor: a new empty table
        /// </summary>
        internal Table(PTable pt,Role ro) :base(pt.ppos, BTree<long,object>.Empty
            +(Name,pt.name)+(Definer,ro.defpos)
            +(Indexes,CTree<CList<long>,long>.Empty) + (LastChange, pt.ppos)
            + (_Domain,Domain.TableType.defpos)+(LastChange,pt.ppos)
            +(Triggers, CTree<PTrigger.TrigType, CTree<long, bool>>.Empty)
            +(Enforcement,(Grant.Privilege)15)) //read|insert|update|delete
        { }
        protected Table(long dp, Context cx, RowSet rs) : base(dp, BTree<long, object>.Empty
            + (Name, rs.name) + (Definer, cx.role)
                        + (Indexes, CTree<CList<long>, long>.Empty) 
            + (_Domain, rs.domain) + (LastChange, cx.db.loadpos)
            + (Triggers, CTree<PTrigger.TrigType, CTree<long, bool>>.Empty)
            + (Enforcement, Grant.Privilege.NoPrivilege))
        { }
        protected Table(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Table operator+(Table tb,(Context,DBObject)x) // tc can be SqlValue for Type def
        {
            var (cx, tc) = x;
            var ds = tb.dependents + (tc.defpos,true);
            var dp = _Max(tb.depth, 1 + tc.depth);
            var ci = (ObInfo)cx.role.infos[tc.defpos];
            var ts = tb.tblCols + (tc.defpos, ci.dataType);
            var m = tb.mem + (Dependents, ds) + (_Depth, dp) + (TableCols, ts);
            if (tc.sensitive)
                m += (Sensitive, true);
            return (Table)tb.New(m);
        }
        public static Table operator-(Table tb,long p)
        {
            return (Table)tb.New(tb.mem + (TableRows,tb.tableRows-p));
        }
        /// <summary>
        /// Add a new or updated row, indexes already fixed.
        /// </summary>
        /// <param name="t"></param>
        /// <param name="rw"></param>
        /// <returns></returns>
        public static Table operator +(Table t, TableRow rw)
        {
            var se = t.sensitive || rw.classification!=Level.D;
            return (Table)t.New(t.mem + (TableRows,t.tableRows+(rw.defpos,rw)) 
                + (Sensitive,se));
        }
        public static Table operator+(Table tb,(long,object)v)
        {
            return (Table)tb.New(tb.mem + v);
        }

        internal virtual ObInfo _ObInfo(long ppos, string name, Grant.Privilege priv)
        {
            return new ObInfo(ppos,name,Domain.TableType,priv);
        }

        /// <summary>
        /// Build an ObInfo for this user
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override ObInfo Inf(Context cx)
        {
            if ((cx.db.role.infos[defpos] is ObInfo oi))
                return oi;
            var ti = cx.Inf(defpos);
            var rt = CList<long>.Empty;
            for (var b=tblCols.First();b!=null;b=b.Next())
            {
                var ci = cx.Inf(b.key());
                if (cx.db._user==cx.db.owner 
                    || cx.db.user.clearance.ClearanceAllows(ci.classification))
                    rt += b.key();
            }
            if (rt.Count == 0)
                    throw new DBException("2E111", ti.name);
            if (rt.CompareTo(ti.dataType.rowType) == 0)
                return ti;
            ti = (ObInfo)ti.Relocate(cx.GetUid())
                 + (ObInfo._DataType, ti.dataType + (Domain.RowType, rt));
            cx.Add(ti);
            return ti;
        }
        internal override DBObject Instance(long lp,Context cx, Domain q, BList<Ident>cs=null)
        {
            var r = base.Instance(lp,cx, q);
            for (var b = tblCols.First(); b != null; b = b.Next())
            {
                ((TableColumn)cx.db.objects[b.key()]).Instance(lp,cx, q);
                var dm = b.value();
                if (dm!=Domain.Content)
                    dm.Instance(lp,cx, q);
            }
            return r;
        }
        internal long ColFor(Context context, string c)
        {
            for (var b = tblCols.First(); b != null; b = b.Next())
            {
                var p = b.key();
                if ((p >= Transaction.TransPos && context.obs[p] is SqlValue ob && ob.name == c)
                    || (context.db.role.infos[p] is ObInfo oi && oi.name == c))
                    return p;
            }
            return -1L;
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new Table(defpos,mem+(TableChecks,tableChecks+(ck.defpos,true)));
        }
        internal override void _Add(Context cx)
        {
            base._Add(cx);
            for (var b = tblCols.First(); b != null; b = b.Next())
            {
         /*       var p = b.value();
                if (p!=Domain.Content)
                    p.Instance(cx); */
                cx.Add((DBObject)cx.db.objects[b.key()]);
            }
        }
        internal Table AddTrigger(Trigger tg, Database db)
        {
            var tb = this;
            var ts = triggers[tg.tgType] ?? CTree<long, bool>.Empty;
            return tb + (Triggers, triggers+(tg.tgType, ts + (tg.defpos, true)));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Table(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Table(dp, mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (Table)base._Relocate(cx);
            if (applicationPS>=0)
                r += (ApplicationPS, cx.Fix(applicationPS));
            r += (Indexes, cx.Fix(indexes));
            r += (TableCols, cx.Fix(tblCols));
            if (systemPS >= 0)
                r += (SystemPS, cx.Fix(systemPS));
            r += (TableChecks, cx.Fix(tableChecks));
            r += (Triggers, cx.Fix(triggers));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (Table) base._Fix(cx);
            var na = cx.Fix(applicationPS);
            if (na!=applicationPS)
                r += (ApplicationPS, na);
            var ni = cx.Fix(indexes);
            if (ni!=indexes)
                r += (Indexes, ni);
            var tc = cx.Fix(tblCols);
            if (tc!=tblCols)
                r += (TableCols, tc);
            var ns = cx.Fix(systemPS);
            if (ns!=systemPS)
                r += (SystemPS, ns);
            var nc = cx.Fix(tableChecks);
            if (nc!=tableChecks)
                r += (TableChecks, nc);
            var nt = cx.Fix(triggers);
            if (nt!=triggers)
                r += (Triggers, nt);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = base._Replace(cx,so,sv);
            var dm = cx.ObReplace(domain, so, sv);
            if (dm != domain)
                r += (_Domain, dm);
            if (r!=this)
                r = (Table)New(cx,r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Cascade(Context cx,
            Drop.DropAction a = 0, BTree<long, TypedValue> u = null)
        {
            base.Cascade(cx, a, u);
            for (var b = indexes.First(); b != null; b = b.Next())
                ((Index)cx.db.objects[b.value()])?.Cascade(cx,a,u);
            for (var b = cx.role.dbobjects.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value()] is Table tb)
                    for (var c = tb.indexes.First(); c != null; c = c.Next())
                        if (((Index)cx.db.objects[c.value()])?.reftabledefpos == defpos)
                            tb.Cascade(cx,a,u);
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            for (var b = d.roles.First(); b != null; b = b.Next())
            {
                var ro = (Role)d.objects[b.value()];
                if (ro.infos[defpos] is ObInfo oi)
                {
                    ro -= oi;
                    ro += (Role.DBObjects, ro.dbobjects - oi.name);
                    nd += (ro,p);
                }
            }
            return base.Drop(d, nd, p);
        }
        internal override Database DropCheck(long ck, Database nd, long p)
        {
            return nd + (this + (TableChecks, tableChecks - ck),p);
        }
        internal Index FindPrimaryIndex(Database db)
        {
            for (var b=indexes.First();b!=null;b=b.Next())
            {
                var ix = (Index)db.objects[b.value()];
                if (ix.flags.HasFlag(PIndex.ConstraintType.PrimaryKey))
                    return ix;
            }
            return null;
        }
        internal Index FindIndex(Database db,BList<DBObject> key)
        {
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var ix = (Index)db.objects[b.value()];
                if (ix.keys.Count != key.Count)
                    continue;
                var c = ix.keys.First();
                for (var d = key.First(); d != null && c != null; d = d.Next(), c = c.Next())
                    if (d.value().defpos != c.value())
                        goto skip;
                return ix;
                    skip:;
            }
            return null;
        }
        internal Index FindIndex(Database db, CList<long> key)
        {
            return (Index)db.objects[indexes[key]];
        }
        internal override Domain Domains(Context cx, Grant.Privilege pr=Grant.Privilege.NoPrivilege)
        {
            return Inf(cx).dataType;
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm, Domain fd)
        {
            cx.Add(this);
            var rc = CTree<long,bool>.Empty;
            var d = fd.display;
            if (d == 0)
                d = int.MaxValue;
            for (var b = fd.rowType.First(); b != null && d-- > 0; b = b.Next())
            {
                rc += (b.value(), true);
                cx.instances += (b.value(), id.iix.dp);
            }
            var rowSet = (RowSet)cx._Add(new TableRowSet(id.iix.dp, cx, defpos, q.defpos)+(_From,fm)
                +(InstanceRowSet.RdCols,rc));
#if MANDATORYACCESSCONTROL
            Audit(cx, rowSet);
#endif
            return rowSet;
        }
        public override bool Denied(Context cx, Grant.Privilege priv)
        { 
            if (cx.db.user != null && enforcement.HasFlag(priv) &&
                !(cx.db.user.defpos == cx.db.owner
                    || cx.db.user.clearance.ClearanceAllows(classification)))
                return true;
            return base.Denied(cx, priv);
        }
        internal override void _ReadConstraint(Context cx,TableRowSet.TableCursor cu)
        {
            if (cx.db.autoCommit)
                return;
            ReadConstraint r = cx.rdC[defpos];
            var (dp, _) = cu._ds[defpos];
            if (r == null)
                r = new ReadConstraint(defpos,
                    new CheckSpecific(
                        new CTree<long, bool>(dp, true), cu._trs.rdCols));
            else
                r = r + cu;
            cx.rdC += (defpos, r);
        }
        internal CTree<CList<long>, long> IIndexes(CTree<long, long> ism)
        {
            var xs = CTree<CList<long>, long>.Empty;
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var k = CList<long>.Empty;
                for (var c = b.key().First(); c != null; c = c.Next())
                    k += ism[c.value()];
                xs += (k, b.value());
            }
            return xs;
        }
        /// <summary>
        /// A readable version of the Table
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(":"); sb.Append(Uid(domain));
            if (PyrrhoStart.VerboseMode && mem.Contains(Enforcement)) 
            { sb.Append(" Enforcement="); sb.Append(enforcement); }
            if (indexes.Count!=0) 
            { 
                sb.Append(" Indexes:(");
                var cm = "";
                for (var b=indexes.First();b!=null;b=b.Next())
                {
                    sb.Append(cm);cm = ",";
                    var cn = "(";
                    for (var c=b.key().First();c!=null;c=c.Next())
                    {
                        sb.Append(cn);cn = ",";
                        sb.Append(Uid(c.value()));
                    }
                    sb.Append(")"); sb.Append(Uid(b.value()));
                }
                sb.Append(")");
                sb.Append(" KeyCols: "); sb.Append(keyCols);
            }
            if (triggers.Count!=0) { sb.Append(" Triggers:"); sb.Append(triggers); }
            return sb.ToString();
        }
        string ToCamel(string s)
        {
            var sb = new StringBuilder();
            sb.Append(char.ToLower(s[0]));
            sb.Append(s.Substring(1));
            return sb.ToString();
        }
        /// <summary>
        /// Generate a row for the Role$Class table: includes a C# class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleClassValue(Context cx, DBObject from, 
            ABookmark<long, object> _enu)
        {
            var ro = cx.db.role;
            var md = (ObInfo)ro.infos[defpos];
            var versioned = md.metadata.Contains(Sqlx.ENTITY);
            var key = BuildKey(cx.db, out Index ix);
            var sb = new StringBuilder("\r\nusing System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + md.name + " from Database " + cx.db.name 
                + ", Role " + ro.name + "\r\n");
            if (md.description != "")
                sb.Append("/// " + md.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("[Table("); sb.Append(defpos);  sb.Append(","); sb.Append(md.schemaKey); sb.Append(")]\r\n");
            sb.Append("public class " + md.name + (versioned ? " : Versioned" : "") + " {\r\n");
            for (var b = md.dataType.representation.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var dt = b.value();
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j;
                    for (j = 0; j < ix.keys.Count; j++)
                        if (ix.keys[j] == p)
                            break;
                    if (j < ix.keys.Count)
                    {
                        sb.Append("  [Key(" + j + ")]\r\n");
                        if (tn == "Int64")
                            tn = "Int64?"; // unless it is also a reference, see below
                    }
                }
                for (var d = indexes.First(); d != null; d = d.Next())
                    if (cx.db.objects[d.value()] is Index x)
                    {
                        if (x.flags.HasFlag(PIndex.ConstraintType.Unique))
                            for (var c = d.key().First(); c != null; c = c.Next())
                                if (c.value() == p)
                                    sb.Append("  [Unique(" + d.value() + "," + c.key() + ")]\r\n");
                        if (x.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                            for (var c = d.key().First(); c != null; c = c.Next())
                                if (c.value() == p && tn == "Int64?")
                                    tn = "Int64";
                    }
                FieldType(cx,sb, dt);
                var ci = (ObInfo)cx.db.role.infos[p];
                for (var d=ci.metadata.First();d!=null;d=d.Next())
                    switch (d.key())
                    {
                        case Sqlx.X:
                        case Sqlx.Y:
                            sb.Append(" [" + d.key().ToString() + "]\r\n");
                            break;
                    }
                if (ci.description?.Length > 1)
                    sb.Append("  // " + ci.description + "\r\n");
                if (tn == "Int64?")
                    sb.Append("  // autoKey enabled\r\n");
                sb.Append("  public " + tn + " " + ci.name + ";\r\n");
            }
            for (var b=indexes.First();b!=null;b=b.Next())
            {
                var x = (Index)cx.db.objects[b.value()];
                if (x.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                {
                    var sa = new StringBuilder();
                    var cm = "";
                    for (var c=b.key().First();c!=null;c=c.Next())
                    {
                        sa.Append(cm); cm = ",";
                        var ci = (ObInfo)ro.infos[c.value()];
                        sa.Append(ci.name);
                    }
                    var rx = (Index)cx.db.objects[x.refindexdefpos];
                    var rt = (ObInfo)cx.db.role.infos[rx.tabledefpos];
                    sb.Append("  public " + rt.name + " " + ToCamel(rt.name) 
                        + "=> conn.FindOne<"+rt.name+">("+sa.ToString()+");\r\n");
                }
            }
            for (var b = rindexes.First(); b != null; b = b.Next())
            {
                var rt = (ObInfo)cx.db.role.infos[b.key()];
                var sa = new StringBuilder();
                var cm = "(\"";
                for (var c=b.value().First();c!=null;c=c.Next())
                {
                    var rb = c.value().First();
                    for (var xb = c.key().First(); xb != null && rb != null; xb = xb.Next(), rb = rb.Next())
                    {
                        sa.Append(cm);cm = "),(\"";
                        var ci = (ObInfo)cx.db.role.infos[rb.value()];
                        var bi = (ObInfo)cx.db.role.infos[xb.value()];
                        sa.Append(bi.name); sa.Append("\",");
                        sa.Append(ci.name);
                    }
                }
                sa.Append(")");
                sb.Append("  public " + rt.name + "[] " + ToCamel(rt.name) 
                    + "s => conn.FindWith<"+rt.name+">("+sa.ToString()+");\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(cx,cx._Dom(from),new TChar(md.name),new TChar(key),
                new TChar(sb.ToString()));
        } 
        /// <summary>
        /// Generate a row for the Role$Java table: includes a Java class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleJavaValue(Context cx, DBObject from, 
            ABookmark<long, object> _enu)
        {
            var md = (ObInfo)cx.db.role.infos[defpos];
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("/*\r\n * "); sb.Append(md.name); sb.Append(".java\r\n *\r\n * Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n * from Database " + cx.db.name + ", Role " 
                + cx.db.role.name + "r\n */");
            sb.Append("import org.pyrrhodb.*;\r\n");
            var key = BuildKey(cx.db,out Index ix);
            sb.Append("\r\n@Schema("); sb.Append(from.lastChange); sb.Append(")");
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(cx.db.user.name); sb.Append("\r\n */");
            if (md.description != "")
                sb.Append("/* " + md.description + "*/\r\n");
            sb.Append("public class " + md.name + ((versioned) ? " extends Versioned" : "") + " {\r\n");
            for(var b = md.dataType.rowType.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var cd = tblCols[b.value()];
                var dt = cd;
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j = (int)ix.keys.Count;
                    for (j = 0; j < ix.keys.Count; j++)
                        if (ix.keys[j] == p)
                            break;
                    if (j < ix.keys.Count)
                        sb.Append("  @Key(" + j + ")\r\n");
                }
                FieldJava(cx, sb, dt);
                var ci = (ObInfo)cx.db.role.infos[p];
                sb.Append("  public " + tn + " " + ci.name + ";");
                sb.Append("\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(cx,cx._Dom(from),new TChar(md.name),new TChar(key),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// Generate a row for the Role$Python table: includes a Python class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RolePythonValue(Context cx, DBObject from, ABookmark<long, object> _enu)
        {
            var md = (ObInfo)cx.db.role.infos[defpos];
            var ro = cx.db.role;
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(md.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + cx.db.name + ", Role " + cx.db.role.name + "\r\n");
            var key = BuildKey(cx.db, out Index ix);
            if (md.description != "")
                sb.Append("# " + md.description + "\r\n");
            sb.Append("class " + md.name + (versioned ? "(Versioned)" : "") + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            if (versioned)
                sb.Append("  super().__init__('','')\r\n");
            for(var b = md.dataType.representation.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var dt = b.value();
                var di = (ObInfo)cx.db.role.infos[p];
                var ci = (ObInfo)cx.db.role.infos[p];
                sb.Append("  self." + ci.name + " = " + dt.defaultValue);
                sb.Append("\r\n");
            }
            sb.Append("  self._schemakey = "); sb.Append(from.lastChange); sb.Append("\r\n");
            if (ix!=null)
            {
                var comma = "";
                sb.Append("  self._key = ["); 
                for (var i=0;i<ix.keys.Count;i++)
                {
                    var se = ix.keys[i];
                    sb.Append(comma); comma = ",";
                    sb.Append("'");  sb.Append(ix.keys[i]); sb.Append("'");
                }
                sb.Append("]\r\n");
            }
            return new TRow(cx,cx._Dom(from), new TChar(md.name),new TChar(key),
                new TChar(sb.ToString()));
        }
        string BuildKey(Database db,out Index ix)
        {
            ix = null;
            for (var xk = indexes.First(); xk != null; xk = xk.Next())
            {
                var x = db.objects[xk.value()] as Index;
                if (x.tabledefpos != defpos)
                    continue;
                if ((x.flags & PIndex.ConstraintType.PrimaryKey) == PIndex.ConstraintType.PrimaryKey)
                    ix = x;
            }
            var comma = "";
            var sk = new StringBuilder();
            if (ix != null)
            {
                for (var i = 0; i < (int)ix.keys.Count; i++)
                {
                    var se = ix.keys[i];
                    var ci = (ObInfo)db.role.infos[se];
                    var cd = db.objects[se] as TableColumn;
                    if (cd != null)
                    {
                        sk.Append(comma);
                        comma = ",";
                        sk.Append(ci.name);
                    }
                }
            }
            return sk.ToString();
        }
    }
    internal class VirtualTable : Table
    {
        internal const long
            _RestView = -372; // long RestView
        public long restView => (long)(mem[_RestView] ?? -1L);
        internal VirtualTable(PTable pt, Role ro,Context cx) : base(pt, ro) 
        {
            cx.Add(this);
        }
        internal VirtualTable(Ident tn,Context cx)
            : this(new PTable(tn.ident,Sqlx.VIEW,cx.db.nextPos,cx),cx)
        { }
        internal VirtualTable(PTable pt,Context cx)
            :this(pt,cx.db.role,cx)
        {
            cx.Add(pt);
        }
        protected VirtualTable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static VirtualTable operator+(VirtualTable v,(long,object)x)
        {
            return (VirtualTable)v.New(v.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new VirtualTable(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new VirtualTable(dp, mem);
        }
        internal override ObInfo _ObInfo(long ppos, string name, Grant.Privilege priv)
        {
            var ti = base._ObInfo(ppos, name, priv);
            return ti;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" RestView="); sb.Append(Uid(restView));
            return sb.ToString();
        }
    }
}
