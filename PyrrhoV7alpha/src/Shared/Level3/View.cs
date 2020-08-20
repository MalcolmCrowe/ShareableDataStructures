using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Collections.Generic;
using System;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
	/// A database object for a view
    /// Immutable
	/// </summary>
	internal class View : DBObject
	{
        internal const long
            RemoteGroups = -378, // GroupSpecification
            ViewDef = -379, // string
            ViewQuery = -380; // QueryExpression
        public string viewDef => (string)mem[ViewDef];
        /// <summary>
        /// The definition of the view
        /// </summary>
        public QueryExpression viewQry => (QueryExpression)mem[ViewQuery];
        internal GroupSpecification remoteGroups => (GroupSpecification)mem[RemoteGroups];
        public View(PView pv,BTree<long,object>m=null) 
            : base(pv.ppos, (m??BTree<long, object>.Empty)
            + (Name,pv.name)+(ViewQuery,pv.view))
        { }
        protected View(long dp, BTree<long, object> m) : base(dp, m) { }
        public static View operator+(View v,(long,object)x)
        {
            return new View(v.defpos, v.mem + x);
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
            var sce = (Query)cx.obs[f.source];
            sce.AddCondition(cx,f.where, null, data);
            return sce.Insert( cx,prov, data, eqs, rs, cl);
        }
        /// <summary>
        /// Execute a Delete (for an updatable View)
        /// </summary>
        /// <param name="f">the From</param>
        /// <param name="dr">the items to delete</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx,From f, BTree<string, bool> dr, Adapters eqs)
        {
            var sce = (Query)cx.obs[f.source];
            sce.AddCondition(cx, f.where, f.assigns, null);
            return sce.Delete(cx,dr,eqs);
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
            var sce = (Query)cx.obs[f.source];
            sce.AddCondition(cx,f.where, f.assigns, null);
            return sce.Update(cx,ur, eqs, rs);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var vw = (View)base._Replace(cx, so, sv);
            var df = (QueryExpression)viewQry._Replace(cx, so, sv);
            if (df != viewQry)
                vw += (ViewQuery, df);
            if (vw == this)
                return this;
            return cx.Add(vw);
        }
        /// <summary>
        /// a readable version of the View
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "View "+defpos;
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
            if (md.description != "")
                sb.Append("/// " + md.description + "\r\n");
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
            if (md.description != "")
                sb.Append(" * " + md.description + "\r\n");
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
            if (md.description != "")
                sb.Append("# " + md.description + "\r\n");
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
        internal override void Modify(Context cx, DBObject now, long p)
        {
            cx.db = cx.db + (this + (ViewQuery, now), p) + (Database.SchemaKey,p);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new View(defpos, mem);
        }
        internal override DBObject Relocate(long dp)
        {
            return new View(dp, mem);
        }
        internal override void Scan(Context cx)
        {
            cx.ObUnheap(defpos);
            viewQry.Scan(cx);
            remoteGroups.Scan(cx);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (View)base._Relocate(wr);
            r += (RemoteGroups,remoteGroups.Relocate(wr));
            r += (ViewQuery, viewQry.Relocate(wr));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (View)base.Fix(cx);
            r += (RemoteGroups, remoteGroups?.Fix(cx));
            r += (ViewQuery, viewQry?.Fix(cx));
            return r;
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
            RemoteCols = -383, // BTree<string,int>
            RemoteAggregates = -384, // bool
            UsingTablePos = -385, // long
            ViewStructPos = -386; // long
        internal string nm => (string)mem[ClientName];
        internal string pw => (string)mem[ClientPassword]; // deprecated
        internal long viewStruct => (long)(mem[ViewStructPos]??-1L);
        internal long usingTable => (long)(mem[UsingTablePos]??-1L);
        internal BTree<string,long> joinCols => 
            (BTree<string,long>)mem[RemoteCols]??BTree<string,long>.Empty;
        internal bool remoteAggregates => (bool)(mem[RemoteAggregates]??false);
        /// <summary>
        /// Constructor: a RestView from level 2
        /// </summary>
        /// <param name="pv">The PRestView</param>
        /// <param name="ro">the current (definer's) role</param>
        /// <param name="ow">the owner</param>
        /// <param name="rs">the list of grantees</param>
        public RestView(PRestView pv) : base(pv,BTree<long,object>.Empty
            +(ViewStructPos,pv.structpos)+(UsingTablePos,pv.usingtbpos)
            +(ClientName,pv.rname)+(ClientPassword,pv.rpass))
        { }
        protected RestView(long dp, BTree<long, object> m) : base(dp, m) { }
        public static RestView operator +(RestView r, (long, object) x)
        {
            return new RestView(r.defpos, r.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new RestView(dp,mem);
        }
        internal override void Scan(Context cx)
        {
            base.Scan(cx);
            cx.ObScanned(viewStruct);
            cx.ObScanned(usingTable);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = base._Relocate(wr);
            r += (ViewStructPos, wr.Fix(viewStruct));
            r += (UsingTablePos, wr.Fix(usingTable));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (RestView)base.Fix(cx);
            r += (ViewStructPos, cx.obuids[viewStruct]);
            r += (UsingTablePos, cx.obuids[usingTable]);
            return r;
        }
    }
}
