using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
using static Pyrrho.Level4.RowSet;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
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
    /// A database object for a view.
    /// The domain is adopted from the pv.viewdef immediately.
    /// Immutable
    /// However, we want to optimise queries deribed from views, so
    /// we use the second constructor to make a private immutable copy
    /// of the committed version.
    /// 
    /// </summary>
    internal class View : Domain
	{
        internal const long
            ViewDef = -379, // string
            ViewResult = -380; // long RowSet
        public string viewDef => (string)(mem[ViewDef] ?? "");
        public long result => (long)(mem[ViewResult] ?? -1L);
        public View(PView pv,Context cx,BTree<long,object>?m=null) 
            : base(pv.ppos, pv.dataType.mem + (m??BTree<long,object>.Empty)
                  + (ObInfo.Name,pv.name) + (Definer,pv.definer)
                  +(Owner,pv.owner)+(Infos,pv.infos)+(_Framing,pv.framing)
                  + (ViewDef,pv.viewdef) + (ViewResult,pv.framing.result)
                  + (LastChange, pv.ppos) + (Owner, cx.user?.defpos??-501L))
        { }
        protected View(long dp, BTree<long, object> m) : base(dp, m) { }
        public static View operator+(View v,(long,object)x)
        {
            var (dp, ob) = x;
            if (v.mem[dp] == ob)
                return v;
            return (View)v.New(v.mem + x);
        }
        /// <summary>
        /// Views are compiled objects, but queries containing them
        /// provide filters, build them into joins, use them in aggregations
        /// and subqueries etc, and in order to optimise the resulting
        /// rowsets we need to ensure that we have fresh uids for exposed
        /// obs (because we may have several independent references to the view).
        /// referenced objects in method OnInstance. 
        /// Instancing works differently during Load and during Parsing.
        /// 
        /// DURING PARSING (cx.parse=ExecuteStatus.Obey or Prepare):
        /// This routine prepares a fresh copy of a View reference, 
        /// moving all precompiled objects with Executable-range uids
        /// to the heap, including the View object itself.
        /// 
        /// DURING LOAD (cx.parse=ExecuteStatus.Load)
        /// We parse the view definition, but ensure that uids are allocated
        /// in an empty part of the Executable range instead of the lexical range.
        /// Then in Compiled.Frame() cx.parse=Execute.Frame briefly 
        /// while any heap uids in the framing are moved into the
        /// executable range.
        /// 
        /// On REFERENCE the framing objects for a table, view or restView are 
        /// instanced to have a new set of heap uids. Hence tablerowsets and
        /// restrowsets and restrowsetsusing are all instanced rowsets 
        /// (we don't need a separate rowset class for views as we work through to the
        /// target tablerowsets).
        /// 
        /// The EFFECT OF THESE CONTORTIONS ARE
        /// (1) uids of framing objects of any sort of committed Database.objects
        /// are never lexical or heap uids.
        /// (2) Show prepared statements the framing objects always have heap uids
        /// (prepared statements are never committed)
        /// (3) compiled objects referenced in Views in use (in Contexts) are always 
        /// on the heap, except for the domain uid for the View
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="f"></param>
        /// <returns></returns>
        internal override DBObject Instance(long lp, Context cx, BList<Ident>? cs = null)
        {
            var od = cx.done;
            cx.done = ObTree.Empty;
            var st = framing.result;
            cx.instDFirst = (cx.parse == ExecuteStatus.Obey) ? cx.nextHeap : cx.db.nextStmt;
            cx.instSFirst = (framing.obs.PositionAt(Transaction.Executables)?.key() ?? 0L) - 1;
            cx.instSLast = framing.obs.Last()?.key() ?? -1L;
            var ni = cx.GetUid();
            cx.uids += (defpos, ni);
            cx.Add((Framing)framing.Fix(cx)); // need virtual columns
            var ns = cx.Fix(st);
            var dt = (Domain)Fix(cx);
            var vi = (View)Relocate(ni) + (ViewResult, ns) + (_From,lp);
            vi = (View)vi.Fix(cx);
            cx.Add(vi);
            var vn = new Ident(vi.NameFor(cx), cx.Ix(vi.defpos));
            var ods = cx.defs;
            cx.AddDefs(vn, vi,alias);
            var ids = cx.defs[vn.ident]?[cx.sD].Item2;
            cx.done = ObTree.Empty;
            for (var b = ids?.First(); b != null; b = b.Next())
            {
                var c = b.key(); // a view column id
                var vx = b.value()[cx.sD].Item1;
                var qx = ods[(c, cx.sD)].Item1; // a reference in q to this
                if (vx == qx || qx==Iix.None)
                    continue;
                if (qx.dp >= 0 && 
                    (qx.sd == vx.sd || qx.sd==vx.sd-1) &&  // substitute the references for the instance columns
                        cx.obs[qx.dp] is SqlValue ov &&
                        cx.obs[vx.dp] is SqlValue tv)
                {
                    var nv = (SqlValue)tv.Relocate(qx.dp);
                    cx.Replace(ov, nv);
                    cx.Replace(tv, nv);
                    cx.undefined -= qx.dp;
                    cx.NowTry();
                }
                if (!cx.obs.Contains(qx.dp))
                    cx.uids += (qx.dp, vx.dp);
            }
            for (var b = cs?.First(); b != null; b = b.Next())
            {
                var iv = b.value();
                if (cx.defs.Contains(iv.ident) && cx.defs[iv.ident]?[cx.sD].Item1.dp is long sp &&
                    cx.obs[cx.uids[sp] ?? sp] is SqlValue so)
                {
                    cx.Replace(so, (SqlCopy)so.Relocate(iv.iix.dp));
                    if (!cx.obs.Contains(sp))
                        cx.uids += (sp, iv.iix.dp);
                }
            }
            cx.instDFirst = -1L;
            if (cx.db != null && framing.obs.Last()?.key() is long t)
            {
                if (cx.parse == ExecuteStatus.Obey)
                {
                    if (t >= cx.nextHeap)
                        cx.nextHeap = t + 1;
                }
                else if (t >= cx.db.nextStmt)
                    cx.db += (Database.NextStmt, t + 1);
            }
            vi = (View)(cx.obs[vi.defpos]??throw new DBException("PE030601"));
    //        cx.defs = on;
            cx.done = od;
            return vi.RowSets(vn, cx, dt,lp);
        }
        /// <summary>
        /// Triggered on the complete set if a view is referenced.
        /// The following tasks should get carried out by Apply
        /// 3.	A view column can be dropped from the request if nobody references it.
        /// 4.	If a view column is used as a simple filter, 
        /// we can pass the filter to the target, 
        /// and simplify everything by using the constant value.
        /// 5.	If a view column is aggregated, 
        /// we can perform some or all of the aggregation on the target, 
        /// but we may need to group by the other visible remote columns.
        /// 6.	With joins we need to preserve columns referenced in the join condition, 
        /// and keep track of keys. Then perform the join with the target instead.
        /// </summary>
        /// <param name="cx"></param>
        internal override RowSet RowSets(Ident vn,Context cx,Domain q,long fm,
            Grant.Privilege pr=Grant.Privilege.Select, string? a=null)
        {
            var ts = (RowSet?)cx.obs[result] ?? throw new DBException("42105");
            var m = new BTree<long, object>(ObInfo.Name, vn.ident);
            if (a != null)
                m += (_Alias, a);
            ts = ts.Apply(m, cx);
            return ts;
        }
        /// <summary>
        /// API development support: generate the C# information for a Role$Class description
        /// </summary>
        /// <param name="from">the From</param>
        /// <param name="_enu">the bookmark in the RoleObjects enumeration</param>
        /// <returns></returns>
        internal override TRow RoleClassValue(Context cx,RowSet from, ABookmark<long, object> _enu)
        {
            if (cx.db.role is not Role ro || _enu.value() is not View md ||
                    md.infos[ro.defpos] is not ObInfo mi || mi.name==null)
                throw new DBException("42105");
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n[Schema("); sb.Append(from.lastChange); sb.Append(")]");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + mi.name + " from Database " + cx.db.name + ", Role " + cx.db.role.name + "\r\n");
            if (mi.description != "")
                sb.Append("/// " + mi.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + mi.name + " : Versioned {\r\n");
            md.DisplayType(cx,sb);
            sb.Append("}\r\n");
            return new TRow(md,
                new TChar(mi.name),
                new TChar(""),
                new TChar(sb.ToString()));
        } 
        /// <summary>
        /// API development support: generate the Java information for a Role$Java description
        /// </summary>
        /// <param name="from">the From</param>
        /// <param name="_enu">the bookmark in the RoleObjects enumeration</param>
        /// <returns></returns>
        internal override TRow RoleJavaValue(Context cx, RowSet from, ABookmark<long, object> _enu)
        {
            if (cx.db.role is not Role ro || _enu.value() is not View md ||
                    md.infos[ro.defpos] is not ObInfo mi
                    || mi.name == null || cx.db.user is not User ud)
                throw new DBException("42105");
            var sb = new StringBuilder();
            sb.Append("\r\n/* \r\n * Class "); sb.Append(mi.name); sb.Append(".java\r\n");
            sb.Append("import org.pyrrhodb.*;\r\n");
            sb.Append("\r\n@Schema("); sb.Append(from.lastChange); sb.Append(')');
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(ud.name); sb.Append("\r\n */");
            sb.Append("\r\n * from Database " + cx.db.name + ", Role " + ro.name + "\r\n");
            if (mi.description != "")
                sb.Append(" * " + mi.description + "\r\n");
            sb.Append(" */\r\n");
            sb.Append("public class " + mi.name + " extends Versioned {\r\n");
            DisplayJType(cx,md, sb);
            sb.Append("}\r\n");
            return new TRow(md,
                new TChar(mi.name),
                new TChar(""),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// API development support: generate the Python information for a Role$Python description
        /// </summary>
        /// <param name="from">the From</param>
        /// <param name="_enu">the bookmark in the RoleObjects enumeration</param>
        /// <returns></returns>
        internal override TRow RolePythonValue(Context cx, RowSet from, ABookmark<long, object> _enu)
        {
            if (cx.db.role is not Role ro || _enu.value() is not View md ||
                    md.infos[ro.defpos] is not ObInfo mi || mi.name == null)
                throw new DBException("42105");
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(mi.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + cx.db.name + ", Role " + ro.name + "\r\n");
            if (mi.description != "")
                sb.Append("# " + mi.description + "\r\n");
            sb.Append("class " + mi.name + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            DisplayPType(cx,md, sb);
            return new TRow(md,
                new TChar(mi.name),
                new TChar(""),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// API development support: generate the Java type information for a field 
        /// </summary>
        /// <param name="dt">the obs type</param>
        /// <param name="sb">a string builder</param>
        /// <param name="kc">key information</param>
        static void DisplayJType(Context cx,Domain dt, StringBuilder sb)
        {
            var i = 0;
            for (var b = dt.rowType.First();b is not null;b=b.Next(),i++)
             if (b.value() is long p && cx.role is Role ro && dt.representation[p] is Domain ob && 
                    ob.infos[ro.defpos] is ObInfo c && c.name is not null){
                var n = c.name.Replace('.', '_');
                var tn = c.name;
                    var k = ob.kind;
                if (k != Sqlx.TYPE && k != Sqlx.ARRAY && k != Sqlx.MULTISET && k!=Sqlx.SET)
                    tn = ob.SystemType.Name;
                if (k == Sqlx.ARRAY || k == Sqlx.MULTISET || k==Sqlx.SET)
                {
                    if (tn == "[]")
                        tn = "_T" + i + "[]";
                    if (n.EndsWith("("))
                        n = "_F" + i;
                }
                ob.FieldType(cx,sb);
                sb.Append("  public " + tn + " " + n + ";\r\n");
            }
            i = 0;
            for (var b = dt.rowType.First(); b != null; b = b.Next(), i++)
                if (cx.role is  Role ro && b.value() is long p && dt.representation[p] is Domain ob && 
                    ob.infos[ro.defpos] is ObInfo ci && 
                    (ob.kind==Sqlx.ARRAY || ob.kind==Sqlx.MULTISET || ob.kind==Sqlx.SET)
                    && ob.elType is Domain ce)
                {
                    var tn = ci.name;
                    if (tn != null)
                        sb.Append("/* Delete this declaration of class " + tn + " if your app declares it somewhere else */\r\n");
                    else
                        tn += "_T" + i;
                    sb.Append("  public class " + tn + " extends Versioned {\r\n");
                    DisplayJType(cx, ce, sb);
                    sb.Append("  }\r\n");
                }
        }
        /// <summary>
        /// API development support: generate the Python type information for a field 
        /// </summary>
        /// <param name="dt">the obs type</param>
        /// <param name="sb">a string builder</param>
        /// <param name="kc">key information</param>
        static void DisplayPType(Context cx,Domain dt, StringBuilder sb)
        {
            var i = 0;
            for (var b = dt.rowType.First(); b != null; b = b.Next(), i++)
                if (cx.role != null && b.value() is long p && dt.representation[p] is DBObject oc && 
                    oc.infos[cx.role.defpos] is ObInfo c && c.name is not null &&
                    dt.representation[p] is Domain cd)
                {
                    var n = c.name.Replace('.', '_');
          //          var tn = c.name;
          //          if (cd.kind != Sqlx.TYPE && cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
          //              tn = cd.SystemType.Name;
                    if (cd.kind == Sqlx.ARRAY || cd.kind == Sqlx.MULTISET) // ??
                    {
           //             if (tn == "[]")
           //                 tn = "_T" + i + "[]";
                        if (n.EndsWith("("))
                            n = "_F" + i;
                    }
                    sb.Append("  self." + n + " = " + cd.defaultValue + "\r\n");
                }
        }
        internal override void Modify(Context cx, Modify m, long p)
        {
            if (cx.db == null)
                throw new PEException("PE48181");
            if (m.source == null)
                throw new DBException("42000",m.name);
            cx.db += (this + (ViewDef, m.source.ident), p);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new View(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new View(dp, m);
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            for (var b = d.roles.First(); b != null; b = b.Next())
                if (b.value() is long bp && d.objects[bp] is Role ro 
                    && infos[ro.defpos] is ObInfo oi && oi.name is not null)
                {
                    ro += (Role.DBObjects, ro.dbobjects - oi.name);
                    nd += (ro, p);
                }
            return base.Drop(d, nd, p);
        }
        /// <summary>
        /// a readable version of the View
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (viewDef !="")
            {
                sb.Append(" ViewDef "); sb.Append(viewDef);
            }
            if (result >= 0)
            {
                sb.Append(" Result "); sb.Append(Uid(result));
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// RestViews get their rows from a REST service, by arrangement with the provider.
    /// The idea is that the client can send a general query based on the view,
    /// in the hope that any additional processing on the remote system is
    /// compensated by having fewer requests and reduced network traffic.
    /// If usage becomes costly or is abused, the provider can obviously
    /// withdraw the facility or provide another view for simpler access.
    /// Exports are built from the RestView's remote columns together with 
    /// UsingOperands supplied as literals for the RestRowSet Build request;
    /// exports can include grouping ids, subquery alias etc.
    /// </summary>
    internal class RestView : View
    {
        internal const long
            Mime = -255, // string
            NamesMap = -399, // CTree<long,string> SqlValue (including exports)
            UsingTable = -372; // long Table
        internal string? mime => (string?)mem[Mime];
        internal long usingTable => (long)(mem[UsingTable]??-1L);
        internal BTree<string,(int,long?)> names =>
            (BTree<string, (int, long?)>)(mem[ObInfo.Names] ?? BTree<string, (int, long?)>.Empty);
        internal CTree<long, string> namesMap =>
            (CTree<long, string>?)mem[NamesMap] ?? CTree<long, string>.Empty;
        /// <summary>
        /// Constructor: a RestView from level 2
        /// </summary>
        /// <param name="pv">The PRestView</param>
        /// <param name="ro">the current (definer's) role</param>
        /// <param name="ow">the owner</param>
        /// <param name="rs">the tree of grantees</param>
        public RestView(PRestView pv, Context cx) : base(pv, cx,
            pv.dataType.mem + (UsingTable, pv.usingTable) 
            + (ViewDef, pv.viewdef) + (_Depth,pv.dataType.depth+1)
            + (NamesMap, (CTree<long, string>)(pv.dataType.mem[NamesMap]??CTree<long,string>.Empty))
            + (ObInfo.Names, (BTree<string, (int,long?)>)(pv.dataType.mem[ObInfo.Names]??BTree<string,(int, long?)>.Empty))
            +(Infos,new BTree<long,ObInfo>(cx.role.defpos,new ObInfo(pv.name,Grant.AllPrivileges))))
        { }
        internal RestView(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new RestView(defpos,m);
        }
        internal override void _Add(Context cx)
        {
            base._Add(cx);
            cx.obs += (defpos,this);
            var ds = cx.depths[depth] ?? ObTree.Empty;
            ds += (defpos, this);
            cx.depths += (depth, ds);
            cx.Add(this);
        }
        internal override DBObject Add(Context cx, PMetadata pm, long p)
        {
            var md = pm.Metadata();
            var oi = (infos[cx.role.defpos]??new ObInfo(name))
                +(ObInfo._Metadata,md)
                +(ObInfo.SchemaKey, p);
            var r = cx.Add(this + (Infos, infos + (cx.role.defpos, oi)));
            cx.db += (r,p);
            return r;
        }
        public static RestView operator +(RestView r, (long, object) x)
        {
            var (dp, ob) = x;
            if (r.mem[dp] == ob)
                return r;
            return new RestView(r.defpos, r.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new RestView(dp,m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nt = cx.Fix(usingTable);
            if (nt != usingTable)
                r += (UsingTable, nt);
            var nm = cx.Fix(namesMap);
            if (nm != namesMap)
                r += (NamesMap, nm);
            return r;
        }
        /// <summary>
        /// An abstract RestView is created by PRestView.Install that has 
        /// a domain with column uids in the executable range. 
        /// The first step here is to relocate this domain to use new heap uids, 
        /// SqlValues for its virtual columns.
        /// Then most of the work is for RestViews that have a usingTable.
        /// In that case we set up a template RestRowSet that is instanced
        /// at Build time for each row found in the usingTable.
        /// We don't want to change the processing used for Views but
        /// for RowSetUsing we need to provide for a Build step and
        /// distinguish the columns coming from the using table from the
        /// remote columns that will come from the instanced RestRowSets.
        /// To do this, RestRowSetUsing overrides ComputeNeeds to add the
        /// columns coming from the usingTable, so that the restview's uids 
        /// are used for these.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="cs">The insert columns if provided</param>
        /// <returns>A RestRowSet or RestRowSetUsing</returns>
        internal override DBObject Instance(long lp, Context cx, BList<Ident>? cs = null)
        {
            // set up instancing parameters
            var vn = new Ident(name, new Iix(lp,cx.sD,cx.GetUid()));
            cx.instDFirst = (cx.parse == ExecuteStatus.Obey) ? cx.nextHeap : cx.db.nextStmt;
            cx.instSFirst = (representation.First()?.key() ?? 0L) - 1;
            cx.instSLast = representation.Last()?.key() ?? -1L;
            // construct our instanced virtual columns, and the instanced domain
            var rt = BList<long?>.Empty;
            var rs = CTree<long, Domain>.Empty;
            var ns = BTree<string, (int,long?)>.Empty;
            var nm = CTree<long, string>.Empty;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long k)
                {
                    var nk = cx.Fix(k);
                    var n = namesMap[k] ?? "";
                    var dm = representation[k] ?? Null;
                    rs += (nk, dm);
                    rt += nk;
                    nm += (nk, n);
                    ns += (n, (b.key(),nk));
                    var id = new Ident(n, new Iix(nk,cx.sD,nk));
                    var sv = new SqlValue(id,BList<Ident>.Empty, cx, dm);
                    cx.Add(sv);
                    cx.defs += (new Ident(vn, id), vn.iix);
                }
            var nd = new Domain(cx.GetUid(), cx, Sqlx.TABLE, rs, rt, rt.Length);
            var oi = infos[cx.role.defpos] ?? throw new DBException("42105");
            var rv = new RestView(cx.GetUid(), nd.mem + (UsingTable, usingTable)
                + (NamesMap, nm) + (ObInfo.Names, ns) +(ViewDef,viewDef)
                + (_Depth,nd.depth+1)
                + (Infos, new BTree<long, ObInfo>(cx.role.defpos,oi
                    + (ObInfo._Metadata,oi.metadata)+ (ObInfo.Names, ns))));
            rv = (RestView)cx.Add(rv);
            cx.AddDefs(vn, nd);
            var r = rv.RowSets(vn, cx, nd, lp);
            if (r is RestRowSetUsing rsu && cx.obs[rsu.usingTableRowSet] is TableRowSet utr)
                for (var b = utr.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue sv && sv.name is string sn
                        && cx.defs[sn]?[cx.sD].Item1?.dp is long q && cx.obs[q] is SqlValue sq
                        && sv.dbg!=sq.dbg)
                        cx.Replace(sv, sq);
            cx.result = r.defpos;
            return cx.Add(r);
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain d, long fm,
            Grant.Privilege pr=Grant.Privilege.Select,string? a=null) 
        {
            var ix = id.iix; // new Iix(id.iix, cx.GetUid());
            var ods = cx.defs;
            var rrs = new RestRowSet(ix, cx, this, d);
            InstanceRowSet irs = rrs;
            if (usingTable>=0)
            {
                var ur = new TableRowSet(cx.GetUid(), cx, usingTable);
                var ids = ods[id.ident]?[cx.sD].Item2;
                cx.done = ObTree.Empty;
                for (var b = ids?.First(); b != null; b = b.Next())
                {
                    var c = b.key(); // a view column id
                    var qx = b.value()[cx.sD].Item1;
                    var vx = cx.defs[(c, cx.sD)].Item1; // a reference in q to this
                    if (vx == qx || qx == Iix.None)
                        continue;
                    if (qx.dp >= 0 && qx.dp!=vx.dp && qx.sd >= vx.sd &&  // substitute the references for the instance columns
                            cx.obs[qx.dp] is SqlValue ov &&
                            cx.obs[vx.dp] is SqlValue tv)
                    {
                        var nv = tv.Relocate(qx.dp);
                        cx.Replace(ov, nv);
                        cx.Replace(tv, nv);
                        cx.undefined -= qx.dp;
                        cx.NowTry();
                    }
                    if (!cx.obs.Contains(qx.dp))
                        cx.uids += (qx.dp, vx.dp);
                }
                ur = (TableRowSet)(cx.obs[ur.defpos] ?? throw new DBException("42000"));
                irs = new RestRowSetUsing(cx.GetIid(), cx, this, rrs, ur);
            }
            var m = irs.mem;
            var rt = irs.rsTargets;
            var mg = CTree<long, CTree<long, bool>>.Empty; // matching columns
            var tn = id.ident; // the object name
            var fa = (pr == Grant.Privilege.Select) ? Assertions.None : Assertions.AssignTarget;
            fa |= irs.asserts & Assertions.SpecificRows;
            m = m + (ObInfo.Name, tn) /*+ (Target, irs.target)*/ + (Matching, mg)
                   + (RSTargets, rt) + (Asserts, fa);
            if (a != null)
                m += (_Alias, a);
            if (irs.keys != Row)
                m += (Index.Keys, irs.keys);
            irs = (InstanceRowSet)irs.Apply(m, cx); 
            cx.UpdateDefs(id, irs, a);
            return (RowSet)(cx.obs[irs.defpos]??throw new PEException("PE70303"));
        } 
        internal override void _ReadConstraint(Context cx, TableRowSet.TableCursor cu)
        {
            base._ReadConstraint(cx, cu);
        }
        /// <summary>
        /// Generate a row for the Role$Class table: includes a C# class definition,
        /// and computes navigation properties
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleClassValue(Context cx, RowSet from,
            ABookmark<long, object> _enu)
        {
            if (cx.role == null) 
                throw new DBException("42105");
            var tb = (Table)(super as Table ?? throw new DBException("42105"));
            var ro = cx.role;
            var md = infos[ro.defpos] ?? throw new DBException("42105");
            cx.Add(framing);
            var versioned = md.metadata.Contains(Sqlx.ENTITY);
            var key = tb.BuildKey(cx, out Domain keys);
            var fields = CTree<string, bool>.Empty;
            var sb = new StringBuilder("\r\nusing System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + md.name + " from Database " + cx.db.name
                + ", Role " + ro.name + "\r\n");
            if (md.description != "")
                sb.Append("/// " + md.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("[Table("); sb.Append(defpos); sb.Append(','); sb.Append(md.schemaKey); sb.Append(")]\r\n");
            sb.Append("public class " + md.name + (versioned ? " : Versioned" : "") + " {\r\n");
            for (var b = tb.representation.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var dt = b.value();
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                if (keys != null)
                {
                    int j;
                    for (j = 0; j < keys.Length; j++)
                        if (keys[j] == p)
                            break;
                    if (j < keys.Length)
                        sb.Append("  [Key(" + j + ")]\r\n");
                }
                for (var d = tb.indexes.First(); d != null; d = d.Next())
                    for (var e = d.value().First(); e != null; e = e.Next())
                        if ((cx.obs[e.key()] ?? cx.db.objects[e.key()]) is Index x)
                        {
                            if (x.flags.HasFlag(PIndex.ConstraintType.Unique))
                                for (var c = d.key().First(); c != null; c = c.Next())
                                    if (c.value() == p)
                                        sb.Append("  [Unique(" + e.key() + "," + c.key() + ")]\r\n");
                            if (x.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                                for (var c = d.key().First(); c != null; c = c.Next())
                                    if (c.value() == p && tn == "Int64?")
                                        tn = "Int64";
                        }
                dt.FieldType(cx, sb);
                if (cx._Ob(p) is DBObject oc && oc.infos[cx.role.defpos] is ObInfo ci && ci.name is not null)
                {
                    fields += (ci.name, true);
                    for (var d = ci.metadata.First(); d != null; d = d.Next())
                        switch (d.key())
                        {
                            case Sqlx.X:
                            case Sqlx.Y:
                                sb.Append(" [" + d.key().ToString() + "]\r\n");
                                break;
                        }
                    if (ci.description?.Length > 1)
                        sb.Append("  // " + ci.description + "\r\n");
                }
                else
                    fields += (cx.NameFor(p), true);
                sb.Append("  public " + tn + " " + tb.NameFor(cx) + ";\r\n");
            }
            for (var b = tb.indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (cx._Ob(c.key()) is Index x && x.flags.HasFlag(PIndex.ConstraintType.ForeignKey)
                           && cx.db.objects[x.refindexdefpos] is Index rx
                           &&  cx._Ob(rx.tabledefpos) is DBObject ot && ot.infos[cx.role.defpos] is ObInfo rt
                           && rt.metadata.Contains(Sqlx.ENTITY) && rt.name is not null)
                    {
                        // many-one relationship
                        var sa = new StringBuilder();
                        var cm = "";
                        for (var d = b.key().First(); d != null; d = d.Next())
                            if (d.value() is long p)
                            {
                                sa.Append(cm); cm = ",";
                                sa.Append(cx.NameFor(p));
                            }
                        var rn = Table.ToCamel(rt.name);
                        for (var i = 0; fields.Contains(rn); i++)
                            rn = Table.ToCamel(rt.name) + i;
                        fields += (rn, true);
                        sb.Append("  public " + rt.name + " " + rn
                            + "=> conn.FindOne<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                    }
            for (var b = tb.rindexes.First(); b != null; b = b.Next())
                if (cx.role is not null && cx._Ob(b.key()) is DBObject ot && 
                    ot.infos[cx.role.defpos] is ObInfo rt && rt.name is not null)
                {
                    if (rt.metadata.Contains(Sqlx.ENTITY) &&
                        cx.db.objects[b.key()] is Table tt)
                        for (var c = b.value().First(); c != null; c = c.Next())
                        {
                            var sa = new StringBuilder();
                            var cm = "(\"";
                            var rn = Table.ToCamel(rt.name);
                            for (var i = 0; fields.Contains(rn); i++)
                                rn = Table.ToCamel(rt.name) + i;
                            fields += (rn, true);
                            var x = tt.FindIndex(cx.db, c.key())?[0];
                            if (x != null)
                            // one-one relationship
                            {
                                cm = "";
                                for (var bb = c.value().First(); bb != null; bb = bb.Next())
                                    if (bb.value() is long bp && c.value().representation[bp] is DBObject vo 
                                        && vo.infos[cx.role.defpos] is ObInfo vi)
                                    {
                                        sa.Append(cm); cm = ",";
                                        sa.Append(vi.name);
                                    }
                                sb.Append("  public " + rt.name + " " + rn
                                    + "s => conn.FindOne<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                                continue;
                            }
                            // one-many relationship
                            var rb = c.value().First();
                            for (var xb = c.key().First(); xb != null && rb != null; xb = xb.Next(), rb = rb.Next())
                                if (xb.value() is long xp && rb.value() is long rp)
                                {
                                    sa.Append(cm); cm = "),(\"";
                                    sa.Append(cx.NameFor(xp)); sa.Append("\",");
                                    sa.Append(cx.NameFor(rp));
                                }
                            sa.Append(')');
                            sb.Append("  public " + rt.name + "[] " + rn
                                + "s => conn.FindWith<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                        }
                    else //  e.g. this is Brand
                    if (cx.db.objects[b.key()] is Table pt) // auxiliary table e.g. BrandSupplier
                        for (var d = pt.indexes.First(); d != null; d = d.Next())
                            for (var e = d.value().First(); e != null; e = e.Next())
                                if (cx.db.objects[e.key()] is Index px && px.tabledefpos != defpos &&
                                    // many-many relationship 
                                    cx.db.objects[px.reftabledefpos] is Table tu && // e.g. Supplier
                                    cx.role is not null && tb.infos[cx.role.defpos] is ObInfo ti &&
                                    ti.metadata.Contains(Sqlx.ENTITY) && tu.FindPrimaryIndex(cx) is Level3.Index tx)
                                {
                                    var sk = new StringBuilder(); // e.g. Supplier primary key
                                    var cm = "\\\"";
                                    for (var c = tx.keys.First(); c != null; c = c.Next())
                                        if (c.value() is long p && cx.role != null 
                                            && representation[p] is DBObject oc &&
                                            oc.infos[cx.role.defpos] is ObInfo ci)
                                        {
                                            sk.Append(cm); cm = "\\\",\\\"";
                                            sk.Append(ci.name);
                                        }
                                    sk.Append("\\\"");
                                    var sa = new StringBuilder(); // e.g. BrandSupplier.Brand = Brand
                                    cm = "\\\"";
                                    var rb = px.keys.First();
                                    for (var xb = keys?.First(); xb != null && rb != null;
                                        xb = xb.Next(), rb = rb.Next())
                                        if (xb.value() is long xp && rb.value() is long rp)
                                        {
                                            sa.Append(cm); cm = "\\\" and \\\"";
                                            sa.Append(cx.NameFor(xp)); sa.Append("\\\"=\\\"");
                                            sa.Append(cx.NameFor(rp));
                                        }
                                    sa.Append("\\\"");
                                    var rn = Table.ToCamel(rt.name);
                                    for (var i = 0; fields.Contains(rn); i++)
                                        rn = Table.ToCamel(rt.name) + i;
                                    fields += (rn, true);
                                    sb.Append("  public " + ti.name + "[] " + rn
                                        + "s => conn.FindIn<" + ti.name + ">(\"select "
                                        + sk.ToString() + " from \\\"" + rt.name + "\\\" where "
                                        + sa.ToString() + "\");\r\n");
                                }
                }
            sb.Append("}\r\n");
            return new TRow(from, new TChar(md.name??""), new TChar(key),
                new TChar(sb.ToString()));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(Mime))
            {
                sb.Append(" Mime: "); sb.Append(mime);
            }
            if (usingTable>=0)
            {
                sb.Append(" UsingTable: ");sb.Append(Uid(usingTable));
            }
            return sb.ToString();
        }
    }
}
