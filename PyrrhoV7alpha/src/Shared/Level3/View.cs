using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Collections.Generic;
using System;
using System.Text;
using System.Runtime.InteropServices;
using System.Configuration;
using System.Diagnostics.Eventing.Reader;
using System.Net.NetworkInformation;
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
    /// The domain is computed from the pv.viewdef immediately.
    /// Immutable
    /// However, we want to optimise queries deribed from views, so
    /// we use the second constructor to make a private immutable copy
    /// of the committed version.
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class View : DBObject
	{
        internal const long
            ViewDef = -379, // string
            ViewPpos = -377,// long View
            ViewResult = -380, // long RowSet
            ViewTable = -371; // long   Table
        public string? viewDef => (string?)mem[ViewDef];
        public long viewPpos => (long)(mem[ViewPpos] ?? -1L);
        public long result => (long)(mem[ViewResult] ?? -1L);
        internal long viewTable => (long)(mem[ViewTable] ?? -1L);
        public View(PView pv,Context cx,BTree<long,object>?m=null) 
            : base(pv.ppos, pv._Dom(cx,m)
                  + (ObInfo.Name,pv.name) + (Definer,pv.definer)
                  +(Owner,pv.owner)+(Infos,pv.infos)
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
        /// (2) For prepared statements the framing objects always have heap uids
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
            var dt = cx.Fix(domain);
            if (cx._Dom(dt) is not Domain dv)
                throw new DBException("42105");
            var vi = (View)Relocate(ni) + (ViewResult, ns) + (_From,lp);
            vi = (View)vi.Fix(cx);
            cx.Add(vi);
            if (cx._Dom(vi) is not Domain vd)
                throw new PEException("PE5090");
            var vn = new Ident(vi.NameFor(cx), cx.Ix(vi.defpos));
            var ods = cx.defs;
            cx.AddDefs(vn, vd,alias);
            var ids = cx.defs[vn.ident]?[cx.sD].Item2;
            cx.done = ObTree.Empty;
            for (var b = ids?.First(); b != null; b = b.Next())
            {
                var c = b.key(); // a view column id
                var vx = b.value()[cx.sD].Item1;
                var qx = ods[(c, cx.sD)].Item1; // a reference in q to this
                if (vx == qx || qx==Iix.None)
                    continue;
                if (qx.dp >= 0 && qx.sd >= vx.sd &&  // substitute the references for the instance columns
                        cx.obs[qx.dp] is SqlValue ov &&
                        cx.obs[vx.dp] is SqlValue tv)
                {
                    var nv = tv.Relocate(qx.dp);
                    cx.Replace(ov, nv);
                    cx.Replace(tv, nv);
                    cx.undefined -= qx.dp;
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
                    cx.Replace(so, so.Relocate(iv.iix.dp));
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
            vi += (_Domain,dt);
    //        cx.defs = on;
            cx.done = od;
            return vi.RowSets(vn, cx, dv, vi.viewTable);
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
        internal override TRow RoleClassValue(Context cx,DBObject from, ABookmark<long, object> _enu)
        {
            if (cx.db.role is not Role ro || _enu.value() is not View md ||
                    cx._Dom(md) is not Domain dm || md.infos[ro.defpos] is not ObInfo mi
                    || mi.name==null)
                throw new DBException("42105");
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n[Schema("); sb.Append(from.lastChange); sb.Append(")]");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + mi.name + " from Database " + cx.db.name + ", Role " + cx.db.role.name + "\r\n");
            if (mi.description != "")
                sb.Append("/// " + mi.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + mi.name + " : Versioned {\r\n");
            dm.DisplayType(cx,sb);
            sb.Append("}\r\n");
            return new TRow(dm,
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
        internal override TRow RoleJavaValue(Context cx, DBObject from, ABookmark<long, object> _enu)
        {
            if (cx.db.role is not Role ro || _enu.value() is not View md ||
                    cx._Dom(md) is not Domain dm || md.infos[ro.defpos] is not ObInfo mi
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
            DisplayJType(cx,dm, sb);
            sb.Append("}\r\n");
            return new TRow(dm,
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
        internal override TRow RolePythonValue(Context cx, DBObject from, ABookmark<long, object> _enu)
        {
            if (cx.db.role is not Role ro || _enu.value() is not View md ||
                    cx._Dom(md) is not Domain dm || md.infos[ro.defpos] is not ObInfo mi
                    || mi.name == null)
                throw new DBException("42105");
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(mi.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + cx.db.name + ", Role " + ro.name + "\r\n");
            if (mi.description != "")
                sb.Append("# " + mi.description + "\r\n");
            sb.Append("class " + mi.name + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            DisplayPType(cx,dm, sb);
            return new TRow(dm,
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
            for (var b = dt.rowType.First();b!=null;b=b.Next(),i++)
             if (b.value() is long p && cx.role is Role ro && cx._Ob(p) is DBObject ob && 
                    ob.infos[ro.defpos] is ObInfo c && c.name!=null){
                var cd = cx._Dom(p)??throw new DBException("42105");
                var n = c.name.Replace('.', '_');
                var tn = c.name;
                if (cd.kind != Sqlx.TYPE && cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    tn = cd.SystemType.Name;
                if (cd.kind == Sqlx.ARRAY || cd.kind == Sqlx.MULTISET)
                {
                    if (tn == "[]")
                        tn = "_T" + i + "[]";
                    if (n.EndsWith("("))
                        n = "_F" + i;
                }
                cd.FieldType(cx,sb);
                sb.Append("  public " + tn + " " + n + ";\r\n");
            }
            i = 0;
            for (var b = dt.rowType.First(); b != null; b = b.Next(), i++)
                if (cx.role is  Role ro && b.value() is long p && cx._Ob(p) is DBObject ob && 
                    ob.infos[ro.defpos] is ObInfo ci && cx._Dom(ob) is Domain cd &&
                    (cd.kind==Sqlx.ARRAY || cd.kind==Sqlx.MULTISET))
                {
                    var ce = cd.elType;
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
                if (cx.role != null && b.value() is long p && cx._Ob(p) is DBObject oc && 
                    oc.infos[cx.role.defpos] is ObInfo c && c.name!=null &&
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
                throw new DBException("42000");
            cx.db = cx.db + (this + (ViewDef, m.source.ident), p);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new View(defpos, m);
        }
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (defpos >= Transaction.Analysing || cx.parse == ExecuteStatus.Parse)
                return (m == mem) ? this : (View)New(m);
            return cx.Add(new View(cx.GetUid(), m));
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new View(dp, m);
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            for (var b = d.roles.First(); b != null; b = b.Next())
                if (b.value() is long bp && d.objects[bp] is Role ro 
                    && infos[ro.defpos] is ObInfo oi && oi.name!=null)
                {
                    ro += (Role.DBObjects, ro.dbobjects - oi.name);
                    nd += (ro, p);
                }
            return base.Drop(d, nd, p);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object> m)
        {
            var r = base._Fix(cx, m);
            var vt = cx.Fix(viewTable);
            if (vt != viewTable)
                r += (ViewTable, vt);
            return r;
        }
        /// <summary>
        /// a readable version of the View
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" ViewDef "); sb.Append(viewDef);
            sb.Append(" Ppos: "); sb.Append(Uid(viewPpos));
            sb.Append(" Result "); sb.Append(Uid(result));
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
            ClientName = -381, // user name, deprecated
            ClientPassword = -382, // string, deprecated
            Mime = -255, // string
            NamesMap = -399, // CTree<long,string> SqlValue (including exports)
            UsingTableRowSet = -460, // long TableRowSet
            ViewStruct = -386;// long Domain
        internal string? nm => (string?)mem[ClientName];
        internal string? pw => (string?)mem[ClientPassword]; // deprecated
        internal long viewStruct => (long)(mem[ViewStruct]??-1L);
        internal string? mime => (string?)mem[Mime];
        internal string? clientName => (string?)mem[ClientName];
        internal string? clientPassword => (string?)mem[ClientPassword];
        internal long usingTableRowSet => (long)(mem[UsingTableRowSet]??-1L);
        internal BTree<string,long?> names =>
            (BTree<string,long?>?)mem[ObInfo.Names] ?? BTree<string,long?>.Empty;
        internal CTree<long, string> namesMap =>
            (CTree<long, string>?)mem[NamesMap] ?? CTree<long, string>.Empty;
        /// <summary>
        /// Constructor: a RestView from level 2
        /// </summary>
        /// <param name="pv">The PRestView</param>
        /// <param name="ro">the current (definer's) role</param>
        /// <param name="ow">the owner</param>
        /// <param name="rs">the list of grantees</param>
        public RestView(PRestView pv,Context cx) : base(pv,cx,_Mem(pv,cx,
            BTree<long, object>.Empty
            +(ViewStruct,pv.structpos)
            +(UsingTableRowSet,pv.usingTableRowSet)+(ViewTable,pv.structpos)
            +(ViewPpos,pv.ppos)+(Infos,new BTree<long,ObInfo>(cx.role.defpos,new ObInfo(pv.name)))
            +(ObInfo.Names, pv.names)+(NamesMap,pv.namesMap)
            +(_Framing,pv.framing)))
        { }
        protected RestView(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long, object> _Mem(PRestView pv, Context cx, BTree<long, object> m)
        {
            var vc = BTree<string, long?>.Empty;
            var d = 2;
            Domain? dm = null;
            for (var fb = pv.framing.obs.First(); fb != null; fb = fb.Next())
                if (fb.value() is Domain dd && dd.kind == Sqlx.VIEW)
                    dm = dd;
            if (dm != null)
                m += (_Domain, dm.defpos);
            var tb = (Table)(cx._Ob(pv.structpos) ?? throw new DBException("42105"));
            d = Math.Max(d, tb.depth);
            for (var b = cx._Dom(tb)?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue c && c.name != null)
                    vc += (c.name, b.value());
            m += (_Depth, d);
            if (pv.rname != null)
                m += (ClientName, pv.rname);
            if (pv.rpass != null)
                m += (ClientPassword, pv.rpass);
            return m;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RestView(defpos,m);
        }
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (defpos >= Transaction.Analysing || cx.parse == ExecuteStatus.Parse)
                return (m == mem) ? this : (RestView)New(m);
            return cx.Add(new RestView(cx.GetUid(), m));
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
            var nv = cx.Fix(viewStruct);
            if (nv != viewStruct)
                r += (ViewStruct, nv);
            var nt = cx.Fix(usingTableRowSet);
            if (nt != usingTableRowSet)
                r += (UsingTableRowSet, nt);
            var nm = cx.Fix(namesMap);
            if (nm != namesMap)
                r += (NamesMap, nm);
            return r;
        }
        /// <summary>
        /// Most of the work here is for RestViews that have a usingTable.
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
        /// <param name="q">The global select list if provided</param>
        /// <param name="cs">The insert columns if provided</param>
        /// <returns>A RestRowSet or RestRowSetUsing</returns>
        internal override DBObject Instance(long lp, Context cx, BList<Ident>? cs = null)
        {
            var r = (RowSet)base.Instance(lp, cx, cs);
            var rr = (r is RestRowSetUsing ru) ? (RestRowSet)(cx.obs[ru.template] ?? throw new DBException("42105")) : (RestRowSet)r;
            var mf = CTree<string, SqlValue>.Empty;
            var ns =BTree<string, long?>.Empty;
            for (var b = rr.namesMap.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue s)
                {
                    mf += (b.value(), s);
                    ns += (b.value(), b.key());
                }
            r += (ObInfo.Names, ns);
            for (var b = cx._Dom(r)?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue s && s.name!=null)
                {
                    if (mf[s.name] is SqlValue so
                      && so.defpos != s.defpos)
                    {
                        cx.Add(so);
                        cx.Add(new SqlCopy(s.defpos, cx, s.name, s.from, so));
                        cx.Replace(s, so);
                    }
                    else
                        for (var c = s.Needs(cx).First(); c != null; c = c.Next())
                            if (cx.obs[c.key()] is SqlValue sp && sp.name!=null && mf[sp.name] is SqlValue sn
                                  && sp.defpos != sn.defpos && sn.name!=null)
                            {
                                cx.Add(sn);
                                cx.Add(new SqlCopy(sp.defpos, cx, sn.name, sn.from, sn));
                                cx.Replace(sp, sn);
                            }
                }
            var vt = (Table)(cx.db.objects[viewTable] ?? throw new DBException("42105"));
            cx._Add(vt);
            cx.Add(vt.framing);
            return cx.Add(r);
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain d, long fm,
            Grant.Privilege pr=Grant.Privilege.Select,string? a=null) 
        {
            var ix = new Iix(id.iix, cx.GetUid());
            var rrs = new RestRowSet(ix, cx, this, d);
            InstanceRowSet irs = rrs;
            if (usingTableRowSet >= 0)
                irs = new RestRowSetUsing(cx.GetIid(), cx, this, rrs.defpos,
                    (TableRowSet)(cx.obs[usingTableRowSet]??throw new DBException("42105")),d);
            var m = irs.mem;
            var rt = irs.rsTargets;
            var mg = CTree<long, CTree<long, bool>>.Empty; // matching columns
            var tn = id.ident; // the object name
            var dm = (Domain)(cx.obs[(long)(m[_Domain]??-1L)] ?? throw new DBException("42105"));
            var fa = (pr == Grant.Privilege.Select) ? Assertions.None : Assertions.AssignTarget;
            fa |= irs.asserts & Assertions.SpecificRows;
            m = m + (ObInfo.Name, tn)
                   + (Target, irs.target) + (_Domain, dm.defpos)
                   + (Matching, mg)
                   + (RSTargets, rt) + (Asserts, fa)
                   + (Domain.Representation, dm.representation)
                   + (_Depth, Depth(cx,new BList<DBObject?>(dm)+irs));
            if (a != null)
                m += (_Alias, a);
            if (irs.keys != Domain.Row)
                m += (Index.Keys, irs.keys);
            irs = (InstanceRowSet)irs.Apply(m, cx); 
            cx.UpdateDefs(id, irs, a);
            return irs;
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
        internal override TRow RoleClassValue(Context cx, DBObject from,
            ABookmark<long, object> _enu)
        {
            if (cx._Dom(this) is not Domain dm || cx._Dom(from) is not Domain df || cx.role == null) 
                throw new DBException("42105");
            var tb = (Table)(cx.db.objects[dm.structure] ?? throw new DBException("42105"));
            var ro = cx.role;
            var md = infos[ro.defpos] ?? throw new DBException("42105");
            cx.Add(framing);
            dm = cx._Dom(tb) ?? throw new DBException("42105");
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
            for (var b = dm.representation.First(); b != null; b = b.Next())
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
                if (cx._Ob(p) is DBObject oc && oc.infos[cx.role.defpos] is ObInfo ci && ci.name!=null)
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
                           && rt.metadata.Contains(Sqlx.ENTITY) && rt.name!=null)
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
                if (cx.role!=null && cx._Ob(b.key()) is DBObject ot && 
                    ot.infos[cx.role.defpos] is ObInfo rt && rt.name!=null)
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
                                    if (bb.value() is long bp && cx._Ob(bp) is DBObject vo 
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
                                    cx.role!=null && tb.infos[cx.role.defpos] is ObInfo ti &&
                                    ti.metadata.Contains(Sqlx.ENTITY) && tu.FindPrimaryIndex(cx) is Level3.Index tx)
                                {
                                    var sk = new StringBuilder(); // e.g. Supplier primary key
                                    var cm = "\\\"";
                                    for (var c = tx.keys.First(); c != null; c = c.Next())
                                        if (c.value() is long p && cx.role != null 
                                            && cx._Ob(p) is DBObject oc &&
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
            return new TRow(df, new TChar(md.name??""), new TChar(key),
                new TChar(sb.ToString()));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (clientName!=null)
            {
                sb.Append(" Client: "); sb.Append(clientName);
            }
            if (clientPassword!=null)
            {
                sb.Append(" Password: "); sb.Append(clientPassword);
            }
            if (mem.Contains(Mime))
            {
                sb.Append(" Mime: "); sb.Append(mime);
            }
            if (mem.Contains(UsingTableRowSet))
            {
                sb.Append(" UsingTableRowSet: ");sb.Append(Uid(usingTableRowSet));
            }
            if (mem.Contains(ViewTable))
            {
                sb.Append(" ViewTable:");sb.Append(Uid(viewTable));
            }
            return sb.ToString();
        }
    }
}
