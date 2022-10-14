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
        public string viewDef => (string)mem[ViewDef];
        public long viewPpos => (long)(mem[ViewPpos] ?? -1L);
        public long result => (long)(mem[ViewResult] ?? -1L);
        internal long viewTable => (long)(mem[ViewTable] ?? -1L);
        public View(PView pv,Context cx,BTree<long,object>m=null) 
            : base(pv.ppos, pv._Dom(cx,m)
                  + (ObInfo.Name,pv.name) + (Definer,cx.db.role.defpos)
                  + (ViewDef,pv.viewdef) + (ViewResult,pv.framing.result)
                  + (LastChange, pv.ppos))
        { }
        protected View(long dp, BTree<long, object> m) : base(dp, m) { }
        public static View operator+(View v,(long,object)x)
        {
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
        internal override DBObject Instance(long lp, Context cx, BList<Ident> cs = null)
        {
            var od = cx.done;
            cx.done = ObTree.Empty;
            var st = framing.result;
            cx.instDFirst = (cx.parse == ExecuteStatus.Obey) ? cx.nextHeap : cx.db.nextStmt;
            cx.instSFirst = (framing.obs.PositionAt(Transaction.Executables)?.key() ?? 0L) - 1;
            cx.instSLast = framing.obs.Last()?.key() ?? -1L;
            var ni = cx.GetUid();
            cx.uids += (defpos, ni);
            cx.obs += framing.obs; // need virtual columns
            var fo = (Framing)framing.Fix(cx);
            var ns = cx.Fix(st);
            var dt = cx.Fix(domain);
            var vi = (View)Relocate(ni) + (ViewResult, ns) + (_From,lp);
            vi = (View)vi.Fix(cx);
            cx.Add(vi);
            var vn = new Ident(vi.infos[cx.role.defpos].name, cx.Ix(vi.defpos));
            cx.AddDefs(vn, cx._Dom(vi),alias);
            var ids = cx.defs[vn.ident][cx.sD].Item2;
            cx.done = ObTree.Empty;
            for (var b = ids.First(); b != null; b = b.Next())
            {
                var c = b.key(); // a view column id
                var vx = b.value()[cx.sD].Item1;
                var qx = cx.defs[(c, cx.sD)].Item1; // a reference in q to this
                if (vx == qx)
                    continue;
                if (qx.dp >= 0)
                {
                    if (qx.sd >= vx.sd)  // substitute the references for the instance columns
                    {
                        var ov = (SqlValue)cx.obs[qx.dp];
                        var tv = (SqlValue)cx.obs[vx.dp];
                        var nv = tv.Relocate(qx.dp);
                        cx.Replace(ov, nv);
                        cx.Replace(tv, nv);
                        cx.undefined -= qx.dp;
                    }
                }
                if (!cx.obs.Contains(qx.dp))
                    cx.uids += (qx.dp, vx.dp);
            } 
            for (var b = cs?.First(); b != null; b = b.Next())
            {
                var iv= b.value();
                if (cx.defs.Contains(iv.ident))
                {
                    var sp = cx.defs[iv.ident][cx.sD].Item1.dp;
                    var so = cx.obs[cx.uids[sp]??sp];
                    cx.Replace(so, so.Relocate(iv.iix.dp));
                    if (!cx.obs.Contains(sp))
                        cx.uids += (sp, iv.iix.dp);
                } 
            }
            cx.instDFirst = -1L;
            var t = framing.obs.Last().key();
            if (cx.parse == ExecuteStatus.Obey)
            {
                if (t >= cx.nextHeap)
                    cx.nextHeap = t + 1;
            }
            else if (t >= cx.db.nextStmt)
                cx.db += (Database.NextStmt,t + 1);
            vi += (_Domain,dt);
    //        cx.defs = on;
            cx.done = od;
            return vi.RowSets(vn, cx, (Domain)cx._Ob(dt), vi.viewTable);
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
            Grant.Privilege pr=Grant.Privilege.Select, string a=null)
        {
            var ts = (RowSet)cx.obs[result];
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
            var md = _enu.value() as View;
            var mi = md.infos[cx.role.defpos];
            var dm = cx._Dom(md);
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
            return new TRow(cx,dm,
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
            var md = _enu.value() as View;
            var dm = cx._Dom(md);
            var mi = md.infos[cx.role.defpos];
            var sb = new StringBuilder();
            sb.Append("\r\n/* \r\n * Class "); sb.Append(mi.name); sb.Append(".java\r\n");
            sb.Append("import org.pyrrhodb.*;\r\n");
            sb.Append("\r\n@Schema("); sb.Append(from.lastChange); sb.Append(")");
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(cx.db.user.name); sb.Append("\r\n */");
            sb.Append("\r\n * from Database " + cx.db.name + ", Role " + cx.db.role.name + "\r\n");
            if (mi.description != "")
                sb.Append(" * " + mi.description + "\r\n");
            sb.Append(" */\r\n");
            sb.Append("public class " + mi.name + " extends Versioned {\r\n");
            DisplayJType(cx,dm, sb);
            sb.Append("}\r\n");
            return new TRow(cx,dm,
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
            var md = _enu.value() as View;
            var dm = cx._Dom(md);
            var mi = md.infos[cx.role.defpos];
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(mi.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + cx.db.name + ", Role " + cx.db.role.name + "\r\n");
            if (mi.description != "")
                sb.Append("# " + mi.description + "\r\n");
            sb.Append("class " + mi.name + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            DisplayPType(cx,dm, sb);
            return new TRow(cx,dm,
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
            {
                var c = cx._Ob(b.value()).infos[cx.role.defpos];
                var cd = cx._Dom(b.value());
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
                FieldType(cx,sb,cd);
                sb.Append("  public " + tn + " " + n + ";\r\n");
            }
            i = 0;
            for (var b=dt.rowType.First();b!=null;b=b.Next(),i++)
            {
                var c = cx._Ob(b.value()).infos[cx.role.defpos];
                var cd = cx._Dom(b.value());
                if (cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    continue;
                cd = cd.elType;
                var tn = c.name;
                if (tn != null)
                    sb.Append("/* Delete this declaration of class " + tn + " if your app declares it somewhere else */\r\n");
                else
                    tn += "_T" + i;
                sb.Append("  public class " + tn + " extends Versioned {\r\n");
                DisplayJType(cx, cd, sb);
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
            for (var b=dt.rowType.First();b!=null;b=b.Next(),i++)
            {
                var p = b.value();
                var c = cx._Ob(p).infos[cx.role.defpos];
                var cd = dt.representation[p];
                var n = c.name.Replace('.', '_');
                var tn = c.name;
                if (cd.kind != Sqlx.TYPE && cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    tn = cd.SystemType.Name;
                if (cd.kind == Sqlx.ARRAY || cd.kind == Sqlx.MULTISET) // ??
                {
                    if (tn == "[]")
                        tn = "_T" + i + "[]";
                    if (n.EndsWith("("))
                        n = "_F" + i;
                }
                sb.Append("  self." + n + " = " + cd.defaultValue+ "\r\n");
            }
        }
        internal override void Modify(Context cx, Modify m, long p)
        {
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
        internal override DBObject Relocate(long dp)
        {
            return new View(dp, mem);
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            for (var b = d.roles.First(); b != null; b = b.Next())
            {
                var ro = (Role)d.objects[b.value()];
                if (infos[ro.defpos] is ObInfo oi)
                {
                    ro += (Role.DBObjects, ro.dbobjects - oi.name);
                    nd += (ro, p);
                }
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
        internal string nm => (string)mem[ClientName];
        internal string pw => (string)mem[ClientPassword]; // deprecated
        internal long viewStruct => (long)(mem[ViewStruct]??-1L);
        internal string mime => (string)mem[Mime];
        internal string clientName => (string)mem[ClientName];
        internal string clientPassword => (string)mem[ClientPassword];
        internal long usingTableRowSet => (long)(mem[UsingTableRowSet]??-1L);
        internal string name => (string)mem[ObInfo.Name];
        internal CTree<string,long> names =>
            (CTree<string,long>)mem[ObInfo.Names] ?? CTree<string,long>.Empty;
        internal CTree<long, string> namesMap =>
            (CTree<long, string>)mem[NamesMap] ?? CTree<long, string>.Empty;
        /// <summary>
        /// Constructor: a RestView from level 2
        /// </summary>
        /// <param name="pv">The PRestView</param>
        /// <param name="ro">the current (definer's) role</param>
        /// <param name="ow">the owner</param>
        /// <param name="rs">the list of grantees</param>
        public RestView(PRestView pv,Context cx) : base(pv,cx,_Mem(pv,cx,
            BTree<long,object>.Empty
            +(ViewStruct,pv.structpos)
            +(UsingTableRowSet,pv.usingTableRowSet)+(ViewTable,pv.structpos)
            +(ClientName,pv.rname)+(ClientPassword,pv.rpass)
            +(ViewPpos,pv.ppos)+(Infos,new BTree<long,ObInfo>(cx.role.defpos,new ObInfo(pv.name)))
            +(ObInfo.Names, pv.names)+(NamesMap,pv.namesMap)
            +(_Framing,pv.framing)))
        { }
        protected RestView(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(PRestView pv,Context cx,BTree<long,object> m)
        {
            var vc = BTree<string, long>.Empty;
            var d = 2;
            Domain dm = null;
            for (var fb = pv.framing.obs.First(); dm == null; fb = fb.Next())
                if (fb.value() is Domain dd && dd.kind == Sqlx.VIEW)
                    dm = dd;
            m += (_Domain, dm.defpos);
            var tb = (Table)cx._Ob(pv.structpos);
            d = Math.Max(d, tb.depth);
            for (var b = cx._Dom(tb).rowType.First(); b != null; 
                b = b.Next())
            {
                var c = (SqlValue)cx.obs[b.value()];
                vc += (c.name, b.value());
            }
            m += (_Depth, d);
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
        internal override void Add(Context cx, PMetadata pm, long p)
        {
            var md = pm.Metadata();
            var oi = (infos[cx.role.defpos]??new ObInfo(name))
                +(ObInfo._Metadata,md)
                +(ObInfo.SchemaKey, p);
            cx.db += (this + (Infos, infos+(cx.role.defpos,oi)),p);
        }
        public static RestView operator +(RestView r, (long, object) x)
        {
            return new RestView(r.defpos, r.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new RestView(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            r += (ViewStruct, cx.Fix(domain));
            r += (UsingTableRowSet, cx.Fix(usingTableRowSet));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (RestView)base._Fix(cx);
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
        internal override DBObject Instance(long lp, Context cx, BList<Ident> cs = null)
        {
            var r = (RowSet)base.Instance(lp,cx, cs);
            var rr = (r is RestRowSetUsing ru)?(RestRowSet)cx.obs[ru.template]:(RestRowSet)r;
            var mf = CTree<string, SqlValue>.Empty;
            for (var b = rr.namesMap.First(); b != null; b = b.Next())
                mf += (b.value(), (SqlValue)cx.obs[b.key()]);
            for (var b = cx._Dom(r).rowType.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is SqlValue s)
                {
                    if (mf[s.name] is SqlValue so
                      && so.defpos != s.defpos)
                    {
                        cx.Add(so);
                        cx.Add(new SqlCopy(s.defpos, cx, s.name, s.from, so));
                        cx.Replace(s, so);
                    }
                    else
                    {
                        for (var c = s.Needs(cx).First(); c != null; c = c.Next())
                        {
                            var sp = (SqlValue)cx.obs[c.key()];
                            if (mf[sp.name] is SqlValue sn
                              && sp.defpos != sn.defpos)
                            {
                                cx.Add(sn);
                                cx.Add(new SqlCopy(sp.defpos, cx, sn.name, sn.from, sn));
                                cx.Replace(sp, sn);
                            }
                        }
                    }
                }
            cx._Add((Table)cx.db.objects[viewTable]);
            return cx.Add(r);
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain d, long fm,
            Grant.Privilege pr=Grant.Privilege.Select,string a=null) 
        {
            var ix = new Iix(id.iix, cx.GetUid());
            var rrs = new RestRowSet(ix, cx, this, d);
            InstanceRowSet irs = rrs;
            if (usingTableRowSet >= 0)
                irs = new RestRowSetUsing(cx.GetIid(), cx, this, rrs.defpos,
                    (TableRowSet)cx.obs[usingTableRowSet],d);
            var m = irs.mem;
            var rt = irs.rsTargets;
            var mg = CTree<long, CTree<long, bool>>.Empty; // matching columns
            var tn = id.ident; // the object name
            var dm = (Domain)cx.obs[(long)m[_Domain]];
            var fa = (pr == Grant.Privilege.Select) ? Assertions.None : Assertions.AssignTarget;
            fa |= irs.asserts & Assertions.SpecificRows;
            m = m + (ObInfo.Name, tn)
                   + (Target, irs.target) + (_Domain, dm.defpos)
                   + (Matching, mg)
                   + (RSTargets, rt) + (Asserts, fa)
                   + (Domain.Representation, dm.representation)
                   + (_Depth, Depth(dm, irs));
            if (a != null)
                m += (_Alias, a);
            if (irs.keys != CList<long>.Empty)
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
            var dm = cx._Dom(this);
            var tb = (Table)cx.db.objects[dm.structure];
            var ro = cx.db.role;
            var md = infos[ro.defpos];
            cx.obs += framing.obs;
            dm = cx._Dom(tb);
            var versioned = md.metadata.Contains(Sqlx.ENTITY);
            var key = tb.BuildKey(cx, out CList<long> keys);
            var fields = CTree<string, bool>.Empty;
            var sb = new StringBuilder("\r\nusing System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + md.name + " from Database " + cx.db.name
                + ", Role " + ro.name + "\r\n");
            if (md.description != "")
                sb.Append("/// " + md.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("[Table("); sb.Append(defpos); sb.Append(","); sb.Append(md.schemaKey); sb.Append(")]\r\n");
            sb.Append("public class " + md.name + (versioned ? " : Versioned" : "") + " {\r\n");
            for (var b = dm.representation.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var dt = b.value();
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                if (keys != null)
                {
                    int j;
                    for (j = 0; j < keys.Count; j++)
                        if (keys[j] == p)
                            break;
                    if (j < keys.Count)
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
                FieldType(cx, sb, dt);
                var ci = cx._Ob(p).infos[cx.role.defpos];
                if (ci != null)
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
                    fields += (cx.obs[p].infos[cx.role.defpos].name, true);
                sb.Append("  public " + tn + " " + tb.NameFor(cx) + ";\r\n");
            }
            for (var b = tb.indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                {
                    var x = (Index)(cx.obs[c.key()] ?? cx.db.objects[c.key()]);
                    if (x.flags.HasFlag(PIndex.ConstraintType.ForeignKey))
                    {
                        // many-one relationship
                        var sa = new StringBuilder();
                        var cm = "";
                        for (var d = b.key().First(); d != null; d = d.Next())
                        {
                            sa.Append(cm); cm = ",";
                            sa.Append(cx.NameFor(d.value()));
                        }
                        var rx = (Index)cx.db.objects[x.refindexdefpos];
                        var rt = cx._Ob(rx.tabledefpos).infos[cx.role.defpos];
                        if (!rt.metadata.Contains(Sqlx.ENTITY))
                            continue;
                        var rn = tb.ToCamel(rt.name);
                        for (var i = 0; fields.Contains(rn); i++)
                            rn = tb.ToCamel(rt.name) + i;
                        fields += (rn, true);
                        sb.Append("  public " + rt.name + " " + rn
                            + "=> conn.FindOne<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                    }
                }
            for (var b = tb.rindexes.First(); b != null; b = b.Next())
            {
                var rt = cx._Ob(b.key()).infos[cx.role.defpos];
                if (rt.metadata.Contains(Sqlx.ENTITY))
                {
                    var tt = (Table)cx.db.objects[b.key()];
                    for (var c = b.value().First(); c != null; c = c.Next())
                    {
                        var sa = new StringBuilder();
                        var cm = "(\"";
                        var rn = tb.ToCamel(rt.name);
                        for (var i = 0; fields.Contains(rn); i++)
                            rn = tb.ToCamel(rt.name) + i;
                        fields += (rn, true);
                        var x = tt.FindIndex(cx.db, c.key())?[0];
                        if (x != null)
                        // one-one relationship
                        {
                            cm = "";
                            for (var bb = c.value().First(); bb != null; bb = bb.Next())
                            {
                                sa.Append(cm); cm = ",";
                                var vi = cx._Ob(bb.value()).infos[cx.role.defpos];
                                sa.Append(vi.name);
                            }
                            sb.Append("  public " + rt.name + " " + rn
                                + "s => conn.FindOne<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                            continue;
                        }
                        // one-many relationship
                        var rb = c.value().First();
                        for (var xb = c.key().First(); xb != null && rb != null; xb = xb.Next(), rb = rb.Next())
                        {
                            sa.Append(cm); cm = "),(\"";
                            sa.Append(cx.NameFor(xb.value())); sa.Append("\",");
                            sa.Append(cx.NameFor(rb.value()));
                        }
                        sa.Append(")");
                        sb.Append("  public " + rt.name + "[] " + rn
                            + "s => conn.FindWith<" + rt.name + ">(" + sa.ToString() + ");\r\n");
                    }
                }
                else //  e.g. this is Brand
                {
                    var pt = (Table)cx.db.objects[b.key()]; // auxiliary table e.g. BrandSupplier
                    for (var d = pt.indexes.First(); d != null; d = d.Next())
                        for (var e = d.value().First(); e != null; e = e.Next())
                        {
                            var px = (Index)cx.db.objects[e.key()];
                            if (px.reftabledefpos == defpos)
                                continue;
                            // many-many relationship 
                            var tt = (Table)cx.db.objects[px.reftabledefpos]; // e.g. Supplier
                            var ti = tb.infos[cx.role.defpos];
                            if (!ti.metadata.Contains(Sqlx.ENTITY))
                                continue;
                            var tx = tt.FindPrimaryIndex(cx);
                            var sk = new StringBuilder(); // e.g. Supplier primary key
                            var cm = "\\\"";
                            for (var c = tx.keys.First(); c != null; c = c.Next())
                            {
                                sk.Append(cm); cm = "\\\",\\\"";
                                var ci = cx._Ob(c.value()).infos[cx.role.defpos];
                                sk.Append(ci.name);
                            }
                            sk.Append("\\\"");
                            var sa = new StringBuilder(); // e.g. BrandSupplier.Brand = Brand
                            cm = "\\\"";
                            var rb = px.keys.First();
                            for (var xb = keys.First(); xb != null && rb != null;
                                xb = xb.Next(), rb = rb.Next())
                            {
                                sa.Append(cm); cm = "\\\" and \\\"";
                                sa.Append(cx.NameFor(xb.value())); sa.Append("\\\"=\\\"");
                                sa.Append(cx.NameFor(rb.value()));
                            }
                            sa.Append("\\\"");
                            var rn = tt.ToCamel(rt.name);
                            for (var i = 0; fields.Contains(rn); i++)
                                rn = tt.ToCamel(rt.name) + i;
                            fields += (rn, true);
                            sb.Append("  public " + ti.name + "[] " + rn
                                + "s => conn.FindIn<" + ti.name + ">(\"select "
                                + sk.ToString() + " from \\\"" + rt.name + "\\\" where "
                                + sa.ToString() + "\");\r\n");
                        }
                }
            }
            sb.Append("}\r\n");
            return new TRow(cx, cx._Dom(from), new TChar(md.name), new TChar(key),
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
