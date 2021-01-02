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
	/// A database object for a view.
    /// The domain is computed from the pv.viewdef immediately.
    /// Immutable
    /// However, we want to optimise queries deribed from views, so
    /// we use the second constructor to make a private immutable copy
    /// of the committed version.
	/// </summary>
	internal class View : DBObject
	{
        internal const long
            CallerQS = -375, // long QuerySpecification
            ViewCols = -378, // BTree<string,long> SqlValue
            ViewDef = -379, // string
            ViewPpos = -377,// long
            ViewQry = -380; // long CursorSpecification
        public string name => (string)mem[Name];
        public string viewDef => (string)mem[ViewDef];
        public BTree<string, long> viewCols =>
            (BTree<string, long>)mem[ViewCols] ?? BTree<string, long>.Empty;
        public long viewPpos => (long)(mem[ViewPpos] ?? -1L);
        public long viewQry => (long)(mem[ViewQry]??-1L);
        public long callerQS => (long)(mem[CallerQS] ?? -1L);
        public View(PView pv,Database db,BTree<long,object>m=null) 
            : base(pv.ppos, pv._Dom(db,m)
                  + (Name,pv.name) + (Definer,db.role.defpos)
                  + (ViewDef,pv.viewdef) 
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
        internal override void _Add(Context cx)
        {
            cx.Install1(framing);
            base._Add(cx);
        }
        internal override CList<long> _Cols(Context cx)
        {
            return domain.rowType;
        }
        /// <summary>
        /// This routine prepares a fresh copy of a View reference and optimises the RowSets
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="qs"></param>
        /// <returns></returns>
        internal virtual View Instance(Context cx,QuerySpecification qs)
        {
            var fx = cx.nextHeap++;
            cx.obuids += (defpos, fx);
            var st = viewPpos;
            for (var b = framing.obs.PositionAt(st); b != null; b = b.Next())
            {
                var ob = b.value();
                var np = cx.nextHeap++;
                cx._Add(ob.Relocate(np));
                cx.obuids += (ob.defpos, np); 
            }
            var ro = cx.role;
            for (var p = fx; p<cx.nextHeap; p++)
                if (ro.infos[p] is ObInfo oi)
                    ro += ((ObInfo)oi.Relocate(cx.obuids[p]??p), false);
            cx.db += (ro, cx.db.loadpos);
            cx.Install1(framing);
            var nf = (Framing)framing.Fix(cx);
            cx.Install1(nf);
            cx.Install2(nf);
            var r = (View)Fix(cx)+ (_Framing, nf) + (CallerQS, qs.defpos);
            cx._Add(r);
            return r;
        }
        /// <summary>
        /// Triggered on the complete set if a view is referenced in a From.
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
        protected virtual void ReviewRowSets(Context cx)
        { 
            // TBD
        }
        internal override void RowSets(Context cx, From f, BTree<long, RowSet.Finder> fi)
        {
            cx.Install2(framing);
            if (!cx.data.Contains(defpos))
            {
                var vq = (Query)cx.obs[viewQry];
                var rs = vq.RowSets(cx, fi);
                var sc = (long)(rs.mem[From.Source]??-1L);
                var fb = f.domain.rowType.First();
                for (var b=domain.rowType.First();b!=null&&fb!=null;
                    b=b.Next(),fb=fb.Next())
                    fi += (b.value(), new RowSet.Finder(fb.value(), sc));
                rs += (RowSet._Finder, fi);
                cx.data += (rs.defpos, rs);
                cx.data += (f.defpos, rs);
            }
        }
        /// <summary>
        /// Execute an Insert (for an updatable View)
        /// </summary>
        /// <param name="f">the From</param>
        /// <param name="prov">the provenance for the insert</param>
        /// <param name="data">the data to insert</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">the rowsets affected</param>
        internal override Context Insert(Context cx, From f, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
                Level cl)
        {
            var vrs = new ValueRowSet(cx.nextHeap++, cx, domain, f, data);
            var fi = data.finder;
            for (var b = data.rt.First(); b != null; b = b.Next())
            {
                var sc = (SqlCopy)cx.obs[b.value()];
                fi += (sc.copyFrom, new RowSet.Finder(b.value(), data.defpos));
            }
            vrs = vrs + (RowSet._Finder, fi);
            cx.data += (vrs.defpos, vrs);
            var vq = (Query)cx.obs[viewQry];
            return vq.Insert(cx, prov, vrs, eqs, rs, cl);
        }
        /// <summary>
        /// Execute a Delete (for an updatable View)
        /// </summary>
        /// <param name="f">the From</param>
        /// <param name="dr">the items to delete</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx,From f, BTree<string, bool> dr, Adapters eqs)
        {
            var vq = (Query)cx.obs[viewQry];
            return vq.Delete(cx, dr, eqs);
        }
        /// <summary>
        /// Execute an Update (for an updatabale View)
        /// </summary>
        /// <param name="f">the From</param>
        /// <param name="ur">the items to Update</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">the affected rowsets</param>
        internal override Context Update(Context cx,From f, BTree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            var vq = (Query)cx.obs[viewQry];
            var wh = f.where;
            var ua = f.assig;
            vq = vq.AddConditions(cx, ref wh, ref ua, null);
            cx.Add(vq);
            return vq.Update(cx, ur, eqs, rs);
        }
        /// <summary>
        /// API development support: generate the C# information for a Role$Class description
        /// </summary>
        /// <param name="from">the From</param>
        /// <param name="_enu">the bookmark in the RoleObjects enumeration</param>
        /// <returns></returns>
        internal override TRow RoleClassValue(Transaction tr,DBObject from, ABookmark<long, object> _enu)
        {
            var md = _enu.value() as View;
            var mi = tr.role.infos[md.defpos] as ObInfo;
            var ro = tr.role;
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n[Schema("); sb.Append(from.lastChange); sb.Append(")]");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + mi.name + " from Database " + tr.name + ", Role " + tr.role.name + "\r\n");
            if (mi.description != "")
                sb.Append("/// " + mi.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + mi.name + " : Versioned {\r\n");
            mi.DisplayType(tr,sb);
            sb.Append("}\r\n");
            return new TRow(mi,
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
        internal override TRow RoleJavaValue(Transaction tr, DBObject from, ABookmark<long, object> _enu)
        {
            var md = _enu.value() as View;
            var mi = tr.role.infos[md.defpos] as ObInfo;
            var ro = tr.role;
            var sb = new StringBuilder();
            sb.Append("\r\n/* \r\n * Class "); sb.Append(mi.name); sb.Append(".java\r\n");
            sb.Append("import org.pyrrhodb.*;\r\n");
            sb.Append("\r\n@Schema("); sb.Append(from.lastChange); sb.Append(")");
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(tr.user.name); sb.Append("\r\n */");
            sb.Append("\r\n * from Database " + tr.name + ", Role " + tr.role.name + "\r\n");
            if (mi.description != "")
                sb.Append(" * " + mi.description + "\r\n");
            sb.Append(" */\r\n");
            sb.Append("public class " + mi.name + " extends Versioned {\r\n");
            DisplayJType(tr,mi, sb);
            sb.Append("}\r\n");
            return new TRow(mi,
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
        internal override TRow RolePythonValue(Transaction tr, DBObject from, ABookmark<long, object> _enu)
        {
            var md = _enu.value() as View;
            var mi = tr.role.infos[md.defpos] as ObInfo;
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(mi.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + tr.name + ", Role " + tr.role.name + "\r\n");
            if (mi.description != "")
                sb.Append("# " + mi.description + "\r\n");
            sb.Append("class " + mi.name + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            DisplayPType(tr,mi, sb);
            return new TRow(mi,
                new TChar(mi.name),
                new TChar(""),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// API development support: generate the Java type information for a field 
        /// </summary>
        /// <param name="dt">the data type</param>
        /// <param name="sb">a string builder</param>
        /// <param name="kc">key information</param>
        static void DisplayJType(Transaction tr,ObInfo dt, StringBuilder sb)
        {
            var i = 0;
            for (var b = dt.domain.rowType.First();b!=null;b=b.Next(),i++)
            {
                var p = b.value();
                var c = (ObInfo)tr.role.infos[b.value()];
                var cd = c.domain;
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
                FieldType(tr,sb,cd);
                sb.Append("  public " + tn + " " + n + ";\r\n");
            }
            i = 0;
            for (var b=dt.domain.rowType.First();b!=null;b=b.Next(),i++)
            {
                var c = (ObInfo)tr.role.infos[b.value()];
                var cd = c.domain;
                if (cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    continue;
                cd = cd.elType;
                var tn = c.name;
                if (tn != null)
                    sb.Append("/* Delete this declaration of class " + tn + " if your app declares it somewhere else */\r\n");
                else
                    tn += "_T" + i;
                sb.Append("  public class " + tn + " extends Versioned {\r\n");
                DisplayJType(tr, tr.role.infos[c.defpos] as ObInfo, sb);
                sb.Append("  }\r\n");
            }
        }
        /// <summary>
        /// API development support: generate the Python type information for a field 
        /// </summary>
        /// <param name="dt">the data type</param>
        /// <param name="sb">a string builder</param>
        /// <param name="kc">key information</param>
        static void DisplayPType(Transaction tr,ObInfo dt, StringBuilder sb)
        {
            var i = 0;
            for (var b=dt.domain.rowType.First();b!=null;b=b.Next(),i++)
            {
                var c = (ObInfo)tr.role.infos[b.value()];
                var cd = c.domain;
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
            cx.db = cx.db + (this + (ViewDef, m.body), p) + (Database.SchemaKey,p);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new View(defpos, m);
        }
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            if (defpos >= Transaction.Analysing || cx.db.parse == ExecuteStatus.Parse)
                return (m == mem) ? this : (View)New(m);
            return cx.Add(new View(cx.nextHeap++, m));
        }
        internal override DBObject Relocate(long dp)
        {
            return new View(dp, mem);
        }
        internal override Basis Fix(Context cx)
        {
            var r = base.Fix(cx);
            var dm = domain.Fix(cx);
            if (domain != dm)
                r += (_Domain, dm);
            var nv = cx.Fix(viewCols);
            if (nv != viewCols)
                r += (ViewCols, nv);
            if (cx.obs[viewQry] is CursorSpecification q) // i.e. not for RestView
            {
                var nq = (CursorSpecification)q.Fix(cx);
                if (nq.defpos != viewQry)
                    r += (ViewQry, nq.defpos);
            }
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (View)base._Relocate(wr);
            var cs = wr.Fix(viewCols);
            if (cs != viewCols)
                r += (ViewCols, cs);
            var vq = wr.Fix(viewQry);
            if (vq != viewQry)
                r += (ViewQry, vq);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (View)base._Replace(cx, so, sv);
            var ch = false;
            var cs = BTree<string, long>.Empty;
            for (var b=viewCols?.First();b!=null;b=b.Next())
            {
                var p = cx.Replace(b.value(),so,sv);
                cs += (b.key(), p);
                if (p != b.value())
                    ch = true;
            }
            if (ch)
                r += (ViewCols, cs);
            var vq = cx.Replace(viewQry,so,sv);
            if (vq != viewQry)
                r = r + (ViewQry, vq);
            if (domain.representation.Contains(so.defpos))
                r += (_Domain, domain._Replace(cx,so, sv));
            cx.done+=(defpos,r);
            return r;
        }
        /// <summary>
        /// a readable version of the View
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Query "); sb.Append(viewDef);
            sb.Append(" Ppos: "); sb.Append(Uid(viewPpos));
            sb.Append(" Cols (");
            var cm = "";
            for (var b=viewCols.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.key()); sb.Append("=");
                sb.Append(Uid(b.value()));
            }
            sb.Append(") "); sb.Append(domain); 
            if (viewQry>=0)
            {
                sb.Append(" ViewQry: ");sb.Append(Uid(viewQry));
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// RestViews get their data from a REST service.
    /// The parser sets up the restview target in the global From.
    /// During Selects, based on the enclosing QuerySpecification,
    /// we work out a CursorSpecification for the From.source
    /// and a From for the usingTable if any, in the usingFrom field of the From.source CS.
    /// Thus there are four Queries involved in RestView evaluation, here referred to
    /// as QS,GF,CS and UF. Where example comlumns such as K.F occur below we understand
    /// there there may in general be more than one of each sort.
    /// All columns coming from QS maintain their positions in the final result rowSet.
    /// But their evaluation rules and names change: the alias field will hold the original
    /// name if there is no alias. 
    /// The rules are quite complicated:
    /// If QS contains no aggregation columns, GF and FS have the same columns and evaluation rules:
    /// and current values of usingTable columns K are supplied as literals in FS column exps.
    /// If there is no alias for a column expr, an alias is constructed naming K.
    /// Additional (non-grouped) columns will be added to CS,GF for AVG etc.
    /// GS will always have the same grouping columns as QS. For example
    /// QS (AVG(E+K),F) group by F -> 
    ///     CS (SUM(E+[K]) as C_2,F COUNT(E+[K]) as D_2), [K] is the current value of K from UF
    ///     GF (SUM(C_2) as C2,F,COUNT(D_2) as D2) 
    ///         -> QS(C2/D2 as "AVG(E+K)", F)
    /// </summary>
    internal class RestView : View
    {
        internal const long
            ClientName = -381, // string, deprecated
            ClientPassword = -382, // string, deprecated
            Mime = -255, // string
            SqlAgent = -256, // string
            UsingTablePos = -385, // long
            ViewStruct = -386,// Domain
            ViewTable = -371; // Table
        internal string nm => (string)mem[ClientName];
        internal string pw => (string)mem[ClientPassword]; // deprecated
        internal Domain viewStruct => (Domain)mem[ViewStruct];
        internal long viewTable => (long)(mem[ViewTable] ?? -1L);
        internal string mime => (string)mem[Mime];
        internal string sqlAgent => (string)mem[SqlAgent];
        internal string clientName => (string)mem[ClientName];
        internal string clientPassword => (string)mem[ClientPassword];
        internal long usingTable => (long)(mem[UsingTablePos]??-1L);
        /// <summary>
        /// Constructor: a RestView from level 2
        /// </summary>
        /// <param name="pv">The PRestView</param>
        /// <param name="ro">the current (definer's) role</param>
        /// <param name="ow">the owner</param>
        /// <param name="rs">the list of grantees</param>
        public RestView(PRestView pv,Database db) : base(pv,db,_Mem(pv,db)
            +(ViewStruct,((ObInfo)db.role.infos[pv.structpos]).domain)
            +(UsingTablePos,pv.usingtbpos)+(ViewTable,pv.structpos)
            +(ClientName,pv.rname)+(ClientPassword,pv.rpass))
        { }
        protected RestView(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        static BTree<long,object> _Mem(PRestView pv,Database db)
        {
            var r = BTree<long, object>.Empty;
            var vc = BTree<string, long>.Empty;
            var d = 2;
            var tb = (Table)db.objects[pv.structpos];
            d = Math.Max(d, tb.depth);
            for (var b = tb.domain.rowType.First(); b != null; b = b.Next())
            {
                var ci = (ObInfo)db.role.infos[b.value()];
                vc += (ci.name, b.value());
            }
            r += (ViewCols, vc);
            r += (Depth, d);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new RestView(defpos,m);
        }
        internal override Database Add(Database d, PMetadata pm, long p)
        {
            var oi = ((ObInfo)d.role.infos[defpos])+(ObInfo._Metadata,pm.Metadata());
            var ro = d.role + (defpos, oi);
            d += (ro,p); 
            return base.Add(d, pm, p);
        }
        internal override View Instance(Context cx,QuerySpecification q)
        {
            var r = (RestView)base.Instance(cx,q);
            var fm = new From(cx.nextHeap++, cx, r, name);
            var dm = domain;
            var vs = BList<SqlValue>.Empty;
            var ro = cx.db.role;
            var qn = BTree<string, long>.Empty;
            for (var b = q.rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                var sv = (SqlValue)cx.obs[p];
                qn += (sv.name, p);
            }
            for (var b = viewCols.First();b!=null;b=b.Next())
            {
                var cn = b.key();
                var cp = b.value();
                var cd = dm.representation[b.value()];
                var sc = new SqlCopy(cx.nextHeap++, cx, cn, fm.defpos, cp,
                    new BTree<long,object>(_Domain,cd));
                var ci = new ObInfo(sc.defpos, cn, cd, Grant.Privilege.Select);
                ro += (ci,false);
                cx.Add(sc);
                q = (QuerySpecification)cx.obs[q.defpos]; // may have changed again
                if (qn.Contains(cn))
                {
                    var od = cx.done;
                    cx.done = BTree<long, DBObject>.Empty;
                    var qp = qn[cn];
                    var qv = (SqlValue)cx.obs[qp];
                    cx.Replace(qv, sc);
                    cx.done = od;
                }
                vs += sc;
            }
            cx.db += (ro,cx.db.loadpos);
            dm = new Domain(Sqlx.TABLE, vs);
            r += (_Domain, dm);
            fm += (_Domain, dm);
            cx.Add(fm);
            cx.AddDefs(new Ident(name, fm.defpos), fm.rowType);
            var te = new TableExpression(cx.nextHeap++, 
                new BTree<long, object>(TableExpression.Target, fm.defpos)
                +(_Domain,dm));
            cx.Add(te);
            var qs = new QuerySpecification(cx.nextHeap++,cx,dm)+
                (QuerySpecification.TableExp, te)+(_Domain,dm);
            cx.Add(qs);
            var qe = new QueryExpression(cx.nextHeap++, true,
                new BTree<long,object>(QueryExpression._Left,qs.defpos))
                +(_Domain,dm);
            cx.Add(qe);
            var cs = new CursorSpecification(cx.nextHeap++)
                + (CursorSpecification.Union, qe.defpos)+(_Domain,dm)
                + (CursorSpecification._Source,"select * from "+name);
            cx.Add(cs);
            r += (ViewQry, cs.defpos);
            cx.Add(r);
            return r;
        }
        /// <summary>
        /// 1.	The RestRowSet works out the remoteCols from the view.Domain
        /// 2.	This analysis is triggered on the complete set if a restview is referenced.
        /// 3.	A remote column can be dropped from the request if nobody references it.
        /// 4.	If a remote column is used as a simple filter, 
        ///     we can pass the filter to the remote contributor, and 
        ///     simplify everything by using its constant value.
        /// 5.	If a remote column is aggregated, 
        ///     we can perform some or all of the aggregation in the remote, 
        ///     but we may need to group by the other visible remote columns.
        /// 6.	With joins we need to preserve columns referenced in the join condition, 
        ///     and keep track of keys.
        ///     But we do not attempt to construct remote joins 
        ///     (a different restview should be created for this).
        /// </summary>
        /// <param name="cx"></param>
        protected override void ReviewRowSets(Context cx)
        {
            base.ReviewRowSets(cx);
        }
        public static RestView operator +(RestView r, (long, object) x)
        {
            return new RestView(r.defpos, r.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new RestView(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = base._Relocate(wr);
            r += (ViewStruct, domain._Relocate(wr));
            r += (UsingTablePos, wr.Fix(usingTable));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (RestView)base.Fix(cx);
            var nv = viewStruct.Fix(cx);
            if (nv != viewStruct)
                r += (ViewStruct, nv);
            var nt = cx.obuids[usingTable] ?? usingTable;
            if (nt != usingTable)
                r += (UsingTablePos, nt);
            return r;
        }
        internal override void RowSets(Context cx, From gf, BTree<long, RowSet.Finder> fi)
        {
            RowSet r = new RestRowSet(cx, gf, this);
            if (cx.obs[usingTable] is Table ut)
            {
                var vs = CList<long>.Empty;
                var ps = BTree<long, bool>.Empty;
                for (var b=ut.domain.rowType.First();name!=null;b=b.Next())
                {
                    vs += b.value();
                    ps += (b.value(), true);
                }
                for (var b = r.rt.First(); b != null; b = b.Next())
                    if (!ps.Contains(b.value()))
                        vs += b.value();
                var dm = new Domain(Sqlx.TABLE, cx, vs);
                r = new JoinRowSet(cx,
                    new JoinPart(cx.nextHeap++) + (JoinPart.Natural, Sqlx.USING)
                        +(_Domain,dm)+(JoinPart.NamedCols,vs),
                    new TableRowSet(cx, ut.defpos,
                        BTree<long, RowSet.Finder>.Empty, BTree<long, bool>.Empty),
                    r);
            }
            cx.data += (r.defpos, r);
        }
    }
}
