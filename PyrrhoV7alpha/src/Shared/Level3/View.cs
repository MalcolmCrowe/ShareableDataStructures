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
                  + (Name,pv.name) + (Definer,cx.db.role.defpos)
                  + (ViewDef,pv.viewdef) + (ViewResult,pv.framing.result)
                  + (LastChange, pv.ppos))
        { }
        protected View(long dp, BTree<long, object> m) : base(dp, m) { }
        public static View operator+(View v,(long,object)x)
        {
            return (View)v.New(v.mem + x);
        }
        internal override ObInfo Inf(Context cx)
        {
            throw new NotImplementedException();
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
        internal override DBObject Instance(long lp, Context cx, Domain q, BList<Ident> cs = null)
        {
            var od = cx.done;
            cx.done = ObTree.Empty;
            var st = framing.result;
            cx.instDFirst = (cx.parse == ExecuteStatus.Obey) ? cx.nextHeap : cx.nextStmt;
            cx.instSFirst = (framing.obs.PositionAt(Transaction.Executables)?.key() ?? 0L) - 1;
            cx.instSLast = framing.obs.Last()?.key() ?? -1L;
            var ni = cx.GetUid();
            cx.uids += (defpos, ni);
            cx.obs += framing.obs; // need virtual columns
            var fo = (Framing)framing.Fix(cx);
            var ns = cx.Fix(st);
            var dt = cx.Fix(domain);
            var vi = (View)Relocate(ni) + (ViewResult, ns) + (InstanceOf,ni) + (_From,lp);
            vi = (View)vi.Fix(cx);
            cx.Add(vi);
            var vn = new Ident(vi.name, cx.Ix(vi.defpos));
            cx.AddDefs(vn, cx._Dom(vi),alias);
            var ids = cx.defs[vi.name][cx.sD].Item2;
            for (var b = ids.First(); b != null; b = b.Next())
            {
                var c = b.key(); // a view column id
                var vx = b.value()[cx.sD].Item1;
                var qx = cx.defs[(c, cx.sD)].Item1; // a reference in q to this
                if (qx.dp >= 0)
                {
                    if (qx.sd >= vx.sd)  // substitute the references with the instance columns
                        cx.Replace((SqlValue)cx.obs[qx.dp], (SqlValue)cx.obs[vx.dp]);
                    else
                        cx.iim +=(vx.dp,new Iix(vx.dp,cx.sD,vx.dp));
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
                    cx.Replace(so, so.Relocate(iv.iix.dp)+(InstanceOf,so.defpos));
                    if (!cx.obs.Contains(sp))
                        cx.uids += (sp, iv.iix.dp);
                } 
            }
            if (q != null)
            {
                var rs = CTree<long,Domain>.Empty;
                var rt = CList<long>.Empty;
                for (var b = q.rowType.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    p = cx.uids[p] ?? p;
                    var sv = (SqlValue)cx.obs[p];
                    rt += p;
                    rs += (p, cx._Dom(sv));
                }
                q += (Domain.Representation, rs);
                q += (Domain.RowType, rt);
                q = (Domain)cx.Add(q);
            }
            cx.instDFirst = -1L;
            var t = framing.obs.Last().key();
            if (cx.parse == ExecuteStatus.Obey)
            {
                if (t >= cx.nextHeap)
                    cx.nextHeap = t + 1;
            }
            else
                if (t >= cx.nextStmt)
                cx.nextStmt = t + 1;
            vi += (_Domain,dt);
    //        cx.defs = on;
            cx.done = od;
            return vi.RowSets(vn, cx, q, vi.viewTable, q);
        }
        /// <summary>
        /// Triggered on the complete set if a view is referenced in a From.
        /// The following tasks should get carried out by Instance in Apply/Review
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
        internal override RowSet RowSets(Ident vn,Context cx,Domain q,long fm, Domain fd)
        {
           var r = (RowSet)cx.obs[result];
     //       cx.AddDefs(vn, cx._Dom(r),alias);
            return r;
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
            var mi = cx.db.role.infos[md.defpos] as ObInfo;
            var ro = cx.db.role;
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n[Schema("); sb.Append(from.lastChange); sb.Append(")]");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + mi.name + " from Database " + cx.db.name + ", Role " + cx.db.role.name + "\r\n");
            if (mi.description != "")
                sb.Append("/// " + mi.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + mi.name + " : Versioned {\r\n");
            mi.DisplayType(cx,sb);
            sb.Append("}\r\n");
            return new TRow(cx,mi,
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
            var mi = cx.db.role.infos[md.defpos] as ObInfo;
            var ro = cx.db.role;
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
            DisplayJType(cx,mi, sb);
            sb.Append("}\r\n");
            return new TRow(cx,mi,
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
            var mi = cx.db.role.infos[md.defpos] as ObInfo;
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(mi.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + cx.db.name + ", Role " + cx.db.role.name + "\r\n");
            if (mi.description != "")
                sb.Append("# " + mi.description + "\r\n");
            sb.Append("class " + mi.name + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            DisplayPType(cx,mi, sb);
            return new TRow(cx,mi,
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
        static void DisplayJType(Context cx,ObInfo dt, StringBuilder sb)
        {
            var i = 0;
            for (var b = dt.dataType.rowType.First();b!=null;b=b.Next(),i++)
            {
                var p = b.value();
                var c = (ObInfo)cx.db.role.infos[b.value()];
                var cd = c.dataType;
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
            for (var b=dt.dataType.rowType.First();b!=null;b=b.Next(),i++)
            {
                var c = (ObInfo)cx.db.role.infos[b.value()];
                var cd = c.dataType;
                if (cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    continue;
                cd = cd.elType;
                var tn = c.name;
                if (tn != null)
                    sb.Append("/* Delete this declaration of class " + tn + " if your app declares it somewhere else */\r\n");
                else
                    tn += "_T" + i;
                sb.Append("  public class " + tn + " extends Versioned {\r\n");
                DisplayJType(cx, cx.db.role.infos[c.defpos] as ObInfo, sb);
                sb.Append("  }\r\n");
            }
        }
        /// <summary>
        /// API development support: generate the Python type information for a field 
        /// </summary>
        /// <param name="dt">the obs type</param>
        /// <param name="sb">a string builder</param>
        /// <param name="kc">key information</param>
        static void DisplayPType(Context cx,ObInfo dt, StringBuilder sb)
        {
            var i = 0;
            for (var b=dt.dataType.rowType.First();b!=null;b=b.Next(),i++)
            {
                var c = (ObInfo)cx.db.role.infos[b.value()];
                var cd = (Domain)cx.obs[c.domain];
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
            cx.db = cx.db + (this + (ViewDef, m.source.ident), p) + (Database.SchemaKey,p);
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
            NamesMap = -287, // CTree<long,string> SqlValue (including exports)
            RemoteCols = -373, // CList<long> SqlValue
            SqlAgent = -256, // string
            UsingTableRowSet = -460, // long TableRowSet
            ViewStruct = -386;// long Domain
        internal string nm => (string)mem[ClientName];
        internal string pw => (string)mem[ClientPassword]; // deprecated
        internal long viewStruct => (long)(mem[ViewStruct]??-1L);
        internal string mime => (string)mem[Mime];
        internal string sqlAgent => (string)mem[SqlAgent];
        internal string clientName => (string)mem[ClientName];
        internal string clientPassword => (string)mem[ClientPassword];
        internal long usingTableRowSet => (long)(mem[UsingTableRowSet]??-1L);
        internal CTree<string,long> names =>
            (CTree<string,long>)mem[ObInfo.Names] ?? CTree<string,long>.Empty;
        internal CTree<long, string> namesMap =>
            (CTree<long, string>)mem[NamesMap] ?? CTree<long, string>.Empty;
        internal CList<long> remoteCols =>
            (CList<long>)mem[RemoteCols] ?? CList<long>.Empty;
        /// <summary>
        /// Constructor: a RestView from level 2
        /// </summary>
        /// <param name="pv">The PRestView</param>
        /// <param name="ro">the current (definer's) role</param>
        /// <param name="ow">the owner</param>
        /// <param name="rs">the list of grantees</param>
        public RestView(PRestView pv,Context cx) : base(pv,cx,_Mem(pv,cx)
            +(ViewStruct,((ObInfo)cx.db.role.infos[pv.structpos]).dataType.defpos)
            +(UsingTableRowSet,pv.usingTableRowSet)+(ViewTable,pv.structpos)
            +(ClientName,pv.rname)+(ClientPassword,pv.rpass)
            +(ObInfo.Names, pv.names)+(NamesMap,pv.namesMap)
            +(_Framing,pv.framing) + (_Domain,pv.framing.obs.First().key()))
        { }
        protected RestView(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(PRestView pv,Context cx)
        {
            var r = BTree<long, object>.Empty;
            var vc = BTree<string, long>.Empty;
            var d = 2;
            var tb = (Table)cx.db.objects[pv.structpos];
            d = Math.Max(d, tb.depth);
            for (var b = tb.Domains(cx).rowType.First(); b != null; 
                b = b.Next())
            {
                var ci = (ObInfo)cx.db.role.infos[b.value()];
                vc += (ci.name, b.value());
            }
            r += (_Depth, d);
            return r;
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
        internal override Database Add(Database d, PMetadata pm, long p)
        {
            var md = pm.Metadata();
            var oi = ((ObInfo)d.role.infos[defpos])+(ObInfo._Metadata,md);
            var ro = d.role + (defpos, oi);
            d += (ro,p);
            return base.Add(d, pm, p);
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
        internal override DBObject Instance(long lp, Context cx, Domain q, BList<Ident> cs = null)
        {
            var r = (RowSet)base.Instance(lp,cx, q, cs);
            if (q == null)
                q = cx._Dom(r);
            var rr = (r is RestRowSetUsing ru)?(RestRowSet)cx.obs[ru.template]:(RestRowSet)r;
            var mf = CTree<string, SqlValue>.Empty;
            for (var b = rr.namesMap.First(); b != null; b = b.Next())
                mf += (b.value(), (SqlValue)cx.obs[b.key()]);
            for (var b = q.rowType.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is SqlValue s)
                {
                    if (mf[s.name] is SqlValue so
                      && so.defpos != s.defpos)
                    {
                        cx.Add(so);
                        cx.Add(new SqlCopy(s.defpos, cx, s.name, s.from, so)
                            + (InstanceOf, s.defpos));
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
                                cx.Add(new SqlCopy(sp.defpos, cx, sn.name, sn.from, sn)
                                    + (InstanceOf, s.defpos));
                                cx.Replace(sp, sn);
                            }
                        }
                    }
                }
            cx._Add((Table)cx.db.objects[viewTable]);
            return cx.Add(r);
        }
        internal override RowSet RowSets(Ident id,Context cx, Domain d, long fm, Domain fd) // ?? d and fd are the same
        {
            var rs = CTree<long, Domain>.Empty;
            var ix = new Iix(id.iix, cx.GetUid());
            var rrs = new RestRowSet(ix, cx, this, d);
            var dm = cx._Dom(rrs);
            InstanceRowSet irs = rrs;
            if (usingTableRowSet >= 0)
                irs = new RestRowSetUsing(cx.GetIid(), cx, this, rrs.defpos,
                    (TableRowSet)cx.obs[usingTableRowSet],d);
            return irs;
        } 
        internal override void _ReadConstraint(Context cx, TableRowSet.TableCursor cu)
        {
            base._ReadConstraint(cx, cu);
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
            if (mem.Contains(SqlAgent))
            {
                sb.Append(" SqlAgent: "); sb.Append(sqlAgent);
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
