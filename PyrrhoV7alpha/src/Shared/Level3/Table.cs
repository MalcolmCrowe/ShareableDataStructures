using System;
using System.Text;
using System.Collections.Generic;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using System.Runtime.CompilerServices;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
            Indexes = -264, // CTree<CList<long>,long> cols,Index
            KeyCols = -320, // CTree<long,bool> TableColumn (over all indexes)
            LastData = -258, // long
            RefIndexes = -250, // BTree<long,(CList<long>,CList<long>)> rTable,cols,rcols
            SystemPS = -265, //long (system-period specification)
            TableChecks = -266, // BTree<long,bool> Check
            TableCols = -332, // BTree<long,bool> TableColumn
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
        internal CTree<long, bool> tblCols =>
            (CTree<long, bool>)mem[TableCols] ?? CTree<long, bool>.Empty;
        /// <summary>
        /// Enforcement of clearance rules
        /// </summary>
        internal Grant.Privilege enforcement => (Grant.Privilege)(mem[Enforcement]??0);
        internal long applicationPS => (long)(mem[ApplicationPS] ?? -1L);
        internal long systemPS => (long)(mem[SystemPS] ?? -1L);
        internal BTree<long,(CList<long>,CList<long>)> rindexes =>
            (BTree<long, (CList<long>,CList<long>)>)mem[RefIndexes] 
            ?? BTree<long, (CList<long>,CList<long>)>.Empty;
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
            + (_Domain,Domain.TableType)+(LastChange,pt.ppos)
            +(Triggers, CTree<PTrigger.TrigType, CTree<long, bool>>.Empty)
            +(Enforcement,(Grant.Privilege)15)) //read|insert|update|delete
        { }
        protected Table(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Table operator+(Table tb,DBObject tc) // tc can be SqlValue for Type def
        {
            var ds = tb.dependents + (tc.defpos,true);
            var dp = _Max(tb.depth, 1 + tc.depth);
            var ts = tb.tblCols + (tc.defpos, true);
            var m = tb.mem + (Dependents, ds) + (Depth, dp) + (TableCols, ts);
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
        internal override ObInfo Inf(Context cx)
        {
            var ti = cx.Inf(defpos);
            var rt = domain.rowType;
            for (var b=rt.First();b!=null;b=b.Next())
            {
                var ci = cx.Inf(b.value());
                if (cx.db._user!=cx.db.owner 
                    && !cx.db.user.clearance.ClearanceAllows(ci.classification))
                    rt = rt.Without(b.value());
            }
            if (rt.CompareTo(domain.rowType)!=0)
            {
                if (rt.Count == 0)
                    throw new DBException("2E111", ti.name);
                ti += (_Domain, ti.domain + (Domain.RowType, rt));
            }
            return ti;
        }
        internal override CList<long> _Cols(Context cx)
        {
            return cx.Inf(defpos).domain.rowType;
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new Table(defpos,mem+(TableChecks,tableChecks+(ck.defpos,true)));
        }
        internal override void _Add(Context cx)
        {
            base._Add(cx);
            for (var b = domain.representation.First(); b != null; b = b.Next())
                cx.Add((DBObject)cx.db.objects[b.key()]);
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
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (Table)base._Relocate(wr);
            r += (_Domain, domain._Relocate(wr));
            if (applicationPS>=0)
                r += (ApplicationPS, wr.Fix(applicationPS));
            r += (Indexes, wr.Fix(indexes));
            r += (TableCols, wr.Fix(tblCols));
            if (systemPS >= 0)
                r += (SystemPS, wr.Fix(systemPS));
            r += (TableChecks, wr.Fix(tableChecks));
            r += (Triggers, wr.Fix(triggers));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (Table) base.Fix(cx);
            var nd = (Domain)domain.Fix(cx);
            if (nd.CompareTo(domain)!=0)
                r += (_Domain, nd);
            var na = cx.obuids[applicationPS]??applicationPS;
            if (na!=applicationPS)
                r += (ApplicationPS, na);
            var ni = cx.Fix(indexes);
            if (ni!=indexes)
                r += (Indexes, ni);
            var tc = cx.Fix(tblCols);
            if (tc!=tblCols)
                r += (TableCols, tc);
            var ns = cx.obuids[systemPS] ?? systemPS;
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
            var dm = (Domain)domain._Replace(cx, so, sv);
            if (dm != domain)
                r += (_Domain, dm);
            r = (Table)New(cx,r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override void Cascade(Context cx,
            Drop.DropAction a = 0, BTree<long, TypedValue> u = null)
        {
            base.Cascade(cx, a, u);
            for (var b = indexes.First(); b != null; b = b.Next())
                ((Index)cx.db.objects[b.value()]).Cascade(cx,a,u);
            for (var b = cx.role.dbobjects.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value()] is Table tb)
                    for (var c = tb.indexes.First(); c != null; c = c.Next())
                        if (((Index)cx.db.objects[c.value()]).reftabledefpos == defpos)
                            tb.Cascade(cx,a,u);
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
        internal (Index,int,PRow) BestForMatch(Context cx,BTree<long,TypedValue>filter)
        {
            int matches = 0;
            PRow match = null;
            Index index = null;
            int bs = 0;      // score for best index
            for (var p = indexes.First(); p != null; p = p.Next())
            {
                var x = (Index)cx.db.objects[p.value()];
                if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey
                    || x.tabledefpos != defpos)
                    continue;
                var dt = (ObInfo)cx.db.role.infos[x.defpos];
                int sc = 0;
                int nm = 0;
                PRow pr = null;
                var havematch = false;
                int sb = 1;
                var j = dt.domain.Length - 1;
                for (var b = dt.domain.rowType.Last(); b != null; b = b.Previous(), j--)
                {
                    var c = b.value();
                    for (var fd = filter.First(); fd != null; fd = fd.Next())
                    {
                        if (cx.obs[fd.key()] is SqlCopy co
                            && co.copyFrom == c)
                        {
                            sc += 9 - j;
                            nm++;
                            pr = new PRow(fd.value(), pr);
                            havematch = true;
                            goto nextj;
                        }
                    }
                    pr = new PRow(TNull.Value, pr);
                nextj:;
                }
                if (!havematch)
                    pr = null;
                sc += sb;
                if (sc > bs)
                {
                    index = x;
                    matches = nm;
                    match = pr;
                    bs = sc;
                }
            }
            return (index,matches,match);
        }
        internal Index BestForOrdSpec(Context cx,CList<long> ordSpec)
        {
            Index index = null;
            int bs = 0;      // score for best index
            for (var p = indexes.First(); p != null; p = p.Next())
            {
                var x = (Index)cx.db.objects[p.value()];
                if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey
                    || x.tabledefpos != defpos)
                    continue;
                var dt = (ObInfo)cx.db.role.infos[x.defpos];
                int sc = 0;
                int n = 0;
                int sb = 1;
                var j = dt.domain.Length - 1;
                for (var b = dt.domain.rowType.Last(); b != null; b = b.Previous(), j--)
                    if (n < ordSpec.Length)
                    {
                        var ok = ordSpec[n];
                        if (ok != -1L)
                        {
                            n++;
                            sb *= 10;
                        }
                    }
                sc += sb;
                if (sc > bs)
                {
                    index = x;
                    bs = sc;
                }
            }
            return index;
        }
        internal override void RowSets(Context cx, From f,CTree<long,RowSet.Finder> fi)
        {
            var (index, matches, match) = BestForMatch(cx, f.filter);
            if (index == null)
                index = BestForOrdSpec(cx, f.ordSpec);
            RowSet rowSet;
            if (index != null && index.rows != null)
            {
                var sce = (match == null) ? new IndexRowSet(cx, this, index, f.filter)
                            : new FilterRowSet(cx, this, index, match);
                rowSet = (f.rowType!=sce.rt)?(RowSet)new SelectedRowSet(cx, f, sce, fi):sce;
            }
            else
            {
                index = FindPrimaryIndex(cx.db);
                RowSet sa;
                if (index != null && index.rows != null)
                    sa = new IndexRowSet(cx, this, index, 
                        cx.Filter(this, f.where));
                else
                    sa = new TableRowSet(cx, defpos, fi, f.where);
                Audit(cx, sa, f);
                rowSet = (f.rowType == sa.rt && f.display == sa.display) ? sa
                    : new SelectedRowSet(cx, f, sa, fi);
                rowSet += (RowSet.RSTargets,new CTree<long,long>(f.target,f.defpos));
            }
            if (f.assig != CTree<UpdateAssignment, bool>.Empty)
                rowSet += (Query.Assig, f.assig);
            cx.data += (f.defpos, rowSet);
            cx.results += (f.defpos, rowSet.defpos);
        }
        public override bool Denied(Context cx, Grant.Privilege priv)
        { 
            if (cx.db.user != null && enforcement.HasFlag(priv) &&
                !(cx.db.user.defpos == cx.db.owner
                    || cx.db.user.clearance.ClearanceAllows(classification)))
                return true;
            return base.Denied(cx, priv);
        }
        /// <summary>
        /// Prepare an Insert on a single table including trigger operation.
        /// </summary>
        internal override Context Insert(Context cx, RowSet fm, bool iter, string prov, Level cl)
        {
            return new TableActivation(cx, fm, PTrigger.TrigType.Insert, prov, cl);
        }
        /// <summary>
        /// Execute an Update operation on the Table, including triggers
        /// </summary>
        /// <param name="f">The Update statement</param>
        /// <param name="ur">The update row identifiers may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">The target rowset may be explicit</param>
        internal override Context Update(Context cx, RowSet fm, bool iter)
        {
            return new TableActivation(cx, fm, PTrigger.TrigType.Update, null, null);
        }
        /// <summary>
        /// Prepare a Delete on a Table, including triggers
        /// </summary>
        /// <param name="f">The Delete operation</param>
        /// <param name="ds">A set of delete strings may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx, RowSet fm, bool iter)
        {
            return new TableActivation(cx, fm, PTrigger.TrigType.Delete, null, null);
        }
        internal override void _ReadConstraint(Context cx,SelectedRowSet.SelectedCursor cu)
        {
            ReadConstraint r = cx.rdC[defpos];
            if (r == null)
  
                r = new ReadConstraint(defpos,
                    new CheckSpecific(defpos,
                        new BTree<long, bool>(cu._defpos, true), cu._srs.rdCols));
            else
                r = r + cu;
            cx.rdC += (defpos, r);
        }
        /// <summary>
        /// A readable version of the Table
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" "); sb.Append(domain);
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
            }
            if (triggers.Count!=0) { sb.Append(" Triggers:"); sb.Append(triggers); }
            sb.Append(" KeyCols: "); sb.Append(keyCols);
            return sb.ToString();
        }
        /// <summary>
        /// Generate a row for the Role$Class table: includes a C# class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleClassValue(Transaction tr, DBObject from, 
            ABookmark<long, object> _enu)
        {
            var ob = (DBObject)_enu.value();
            var md = (ObInfo)tr.role.infos[ob.defpos];
            var ro = tr.role;
            var versioned = true;
            var key = BuildKey(tr, out Index ix);
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n[Schema("); sb.Append(from.lastChange); sb.Append(")]");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + md.name + " from Database " + tr.name + ", Role " + ro.name + "\r\n");
            if (md.description != "")
                sb.Append("/// " + md.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + md.name + ((versioned) ? " : Versioned" : "") + " {\r\n");
            var rt = tr.role.infos[from.defpos] as ObInfo;
            for (var b = rt.domain.representation.First();b!=null;b=b.Next())
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
                        sb.Append("  [Key(" + j + ")]\r\n");
                }
                FieldType(tr,sb, dt);
                var ci = (ObInfo)tr.role.infos[p];
                sb.Append("  public " + tn + " " + ci.name + ";");
                sb.Append("\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(rt,new TChar(md.name),new TChar(key),
                new TChar(sb.ToString()));
        } 
        /// <summary>
        /// Generate a row for the Role$Java table: includes a Java class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleJavaValue(Transaction tr, DBObject from, 
            ABookmark<long, object> _enu)
        {
            var ob = (DBObject)_enu.value();
            var md = (ObInfo)tr.role.infos[ob.defpos];
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("/*\r\n * "); sb.Append(md.name); sb.Append(".java\r\n *\r\n * Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n * from Database " + tr.name + ", Role " + tr.role.name + "r\n */");
            sb.Append("import org.pyrrhodb.*;\r\n");
            var key = BuildKey(tr,out Index ix);
            sb.Append("\r\n@Schema("); sb.Append(from.lastChange); sb.Append(")");
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(tr.user.name); sb.Append("\r\n */");
            if (md.description != "")
                sb.Append("/* " + md.description + "*/\r\n");
            sb.Append("public class " + md.name + ((versioned) ? " extends Versioned" : "") + " {\r\n");
            var rt = tr.role.infos[from.defpos] as ObInfo;
            for(var b = rt.domain.rowType.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var cd = rt.domain.representation[b.value()];
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
                FieldJava(tr, sb, dt);
                var ci = (ObInfo)tr.role.infos[p];
                sb.Append("  public " + tn + " " + ci.name + ";");
                sb.Append("\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(rt,new TChar(md.name),new TChar(key),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// Generate a row for the Role$Python table: includes a Python class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RolePythonValue(Transaction tr, DBObject from, ABookmark<long, object> _enu)
        {
            var tb = (Table)_enu.value();
            var md = (ObInfo)tr.role.infos[tb.defpos];
            var ro = tr.role;
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(md.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + tr.name + ", Role " + tr.role.name + "\r\n");
            var key = BuildKey(tr, out Index ix);
            if (md.description != "")
                sb.Append("# " + md.description + "\r\n");
            sb.Append("class " + md.name + (versioned ? "(Versioned)" : "") + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            if (versioned)
                sb.Append("  super().__init__('','')\r\n");
            var rt = tr.role.infos[from.defpos] as ObInfo;
            for(var b = rt.domain.representation.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var dt = b.value();
                var di = (ObInfo)tr.role.infos[p];
                var ci = (ObInfo)tr.role.infos[p];
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
            return new TRow(rt, new TChar(md.name),new TChar(key),
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
        internal VirtualTable(PTable pt, Role ro) : base(pt, ro) { }
        protected VirtualTable(long dp, BTree<long, object> m) : base(dp, m) { }

        internal override Basis New(BTree<long, object> m)
        {
            return new VirtualTable(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new VirtualTable(dp, mem);
        }
    }
}
