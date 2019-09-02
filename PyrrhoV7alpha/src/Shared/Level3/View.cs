using System.Net;
using System.IO;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Collections.Generic;
using System;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level3
{
	/// <summary>
	/// A database object for a view
    /// Immutable
	/// </summary>
	internal class View : DBObject
	{
        internal const long
            RemoteGroups = -372, // GroupSpecification
            ViewDef = -373, // string
            ViewQuery = -374; // CursorSpecification
        /// <summary>
        /// The definition of the view
        /// </summary>
		public string viewdef => (string)mem[ViewDef];
        public CursorSpecification viewQry => (CursorSpecification)mem[ViewQuery];
        internal GroupSpecification remoteGroups => (GroupSpecification)mem[RemoteGroups];
        public View(PView pv,BTree<long,object>m=null) 
            : base(pv.ppos, (m??BTree<long, object>.Empty)
            + (Name,pv.name)+(ViewDef,pv.view))
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
        internal override Transaction Insert(Transaction tr,Context _cx, Table f, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl,bool autokey = false)
        {
            f.source.AddCondition(data._tr,_cx,f.where, null, data);
            return f.source.Insert(data._tr,_cx,prov, data, eqs, rs, cl);
        }
        /// <summary>
        /// Execute a Delete (for an updatable View)
        /// </summary>
        /// <param name="f">the From</param>
        /// <param name="dr">the items to delete</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Transaction Delete(Transaction tr,Context cx,Table f, BTree<string, bool> dr, Adapters eqs)
        {
            f.source.AddCondition(tr,cx, f.where, f.assigns, null);
            return f.source.Delete(tr,cx,dr,eqs);
        }
        /// <summary>
        /// Execute an Update (for an updatabale View)
        /// </summary>
        /// <param name="f">the From</param>
        /// <param name="ur">the items to Update</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">the affected rowsets</param>
        internal override Transaction Update(Transaction tr,Context cx,Table f, BTree<string, bool> ur, Adapters eqs, List<RowSet> rs)
        {
            f.source.AddCondition(tr,cx,f.where, f.assigns, null);
            return f.source.Update(tr,cx,ur, eqs, rs);
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
        internal override TRow RoleClassValue(Transaction tr,Table from, ABookmark<long, object> _enu)
        {
            var md = _enu.value() as View;
            var ro = tr.role;
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n[Schema("); sb.Append(from.lastChange); sb.Append(")]");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + md.name + " from Database " + tr.name + ", Role " + tr.role.name + "\r\n");
            if (md.description != "")
                sb.Append("/// " + md.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + md.name + " : Versioned {\r\n");
            DisplayType(viewQry.rowType, sb);
            sb.Append("}\r\n");
            return new TRow(from.rowType,
                new TChar(md.name),
                new TChar(""),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// API development support: generate the C# type information for a field 
        /// </summary>
        /// <param name="dt">The type to use</param>
        /// <param name="db">The database</param>
        /// <param name="sb">a string builder</param>
        static void DisplayType(Domain dt,StringBuilder sb)
        {
            for (var i = 0; i < dt.names.Count; i++)
            {
                var c = dt.columns[i];
                var cd = c.domain;
                var n = c.name.Replace('.', '_');
                var tn = c.domain.name;
                if (cd.kind != Sqlx.TYPE && cd.kind!=Sqlx.ARRAY && cd.kind!=Sqlx.MULTISET)
                    tn = c.domain.SystemType.Name;
                if (cd.kind == Sqlx.ARRAY || cd.kind == Sqlx.MULTISET)
                {
                    if (tn == "[]")
                        tn = "_T" + i + "[]";
                    if (n.EndsWith("("))
                        n = "_F" + i;
                }
                FieldType(sb, cd);
                sb.Append("  public " + tn + " "+n+ ";\r\n");
            }
            for (var i = 0; i < dt.Length; i++)
            {
                var cd = dt.columns[i].domain;
                if (cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    continue;
                cd = cd.elType;
                var tn = cd.name.ToString();
                if (tn!=null)
                    sb.Append("// Delete this declaration of class "+tn+" if your app declares it somewhere else\r\n");
                else
                    tn += "_T" + i;
                sb.Append("  public class " + tn + " {\r\n");
                DisplayType(cd,sb);
                sb.Append("  }\r\n");
            } 
        }
        /// <summary>
        /// API development support: generate the Java information for a Role$Java description
        /// </summary>
        /// <param name="from">the From</param>
        /// <param name="_enu">the bookmark in the RoleObjects enumeration</param>
        /// <returns></returns>
        internal override TRow RoleJavaValue(Transaction tr, Table from, ABookmark<long, object> _enu)
        {
            var md = _enu.value() as View;
            var ro = tr.role;
            var sb = new StringBuilder();
            sb.Append("\r\n/* \r\n * Class "); sb.Append(md.name); sb.Append(".java\r\n");
            sb.Append("import org.pyrrhodb.*;\r\n");
            sb.Append("\r\n@Schema("); sb.Append(from.lastChange); sb.Append(")");
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(tr.user.name); sb.Append("\r\n */");
            sb.Append("\r\n * from Database " + tr.name + ", Role " + tr.role.name + "\r\n");
            if (md.description != "")
                sb.Append(" * " + md.description + "\r\n");
            sb.Append(" */\r\n");
            sb.Append("public class " + md.name + " extends Versioned {\r\n");
            DisplayJType(viewQry.rowType, sb);
            sb.Append("}\r\n");
            return new TRow(from.rowType,
                new TChar(md.name),
                new TChar(""),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// API development support: generate the Python information for a Role$Python description
        /// </summary>
        /// <param name="from">the From</param>
        /// <param name="_enu">the bookmark in the RoleObjects enumeration</param>
        /// <returns></returns>
        internal override TRow RolePythonValue(Transaction tr, Table from, ABookmark<long, object> _enu)
        {
            var md = _enu.value() as View;
            var ro = tr.role;
            var dt = from.rowType;
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(md.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + tr.name + ", Role " + tr.role.name + "\r\n");
            if (md.description != "")
                sb.Append("# " + md.description + "\r\n");
            sb.Append("class " + md.name + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            DisplayPType(dt, sb);
            return new TRow(from.rowType,
                new TChar(md.name),
                new TChar(""),
                new TChar(sb.ToString()));
        }
        /// <summary>
        /// API development support: generate the Java type information for a field 
        /// </summary>
        /// <param name="dt">the data type</param>
        /// <param name="sb">a string builder</param>
        /// <param name="kc">key information</param>
        static void DisplayJType(Domain dt, StringBuilder sb)
        {
            for (var i = 0; i < dt.names.Count; i++)
            {
                var c = dt.columns[i];
                var cd = c.domain;
                var n = c.name.Replace('.', '_');
                var tn = cd.name;
                if (cd.kind != Sqlx.TYPE && cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    tn = cd.SystemType.Name;
                if (cd.kind == Sqlx.ARRAY || cd.kind == Sqlx.MULTISET)
                {
                    if (tn == "[]")
                        tn = "_T" + i + "[]";
                    if (n.EndsWith("("))
                        n = "_F" + i;
                }
                FieldType(sb,cd);
                sb.Append("  public " + tn + " " + n + ";\r\n");
            }
            for (var i = 0; i < dt.Length; i++)
            {
                var c = dt.columns[i];
                var cd = c.domain;
                if (cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    continue;
                cd = cd.elType;
                var tn = cd.name.ToString();
                if (tn != null)
                    sb.Append("/* Delete this declaration of class " + tn + " if your app declares it somewhere else */\r\n");
                else
                    tn += "_T" + i;
                sb.Append("  public class " + tn + " extends Versioned {\r\n");
                DisplayJType(cd, sb);
                sb.Append("  }\r\n");
            }
        }
        /// <summary>
        /// API development support: generate the Python type information for a field 
        /// </summary>
        /// <param name="dt">the data type</param>
        /// <param name="sb">a string builder</param>
        /// <param name="kc">key information</param>
        static void DisplayPType(Domain dt, StringBuilder sb)
        {
            for (var i = 0; i < dt.names.Count; i++)
            {
                var c = dt.columns[i];
                var cd = c.domain;
                var n = c.name.Replace('.', '_');
                var tn = cd.name;
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

        internal override Basis New(BTree<long, object> m)
        {
            throw new NotImplementedException();
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
            ClientName = -375, // string, deprecated
            ClientPassword = -376, // string, deprecated
            RemoteCols = -377, // BTree<string,int>
            RemoteAggregates = -378, // bool
            UsingTablePos = -379, // long
            ViewStructPos = -380; // long
        internal string nm => (string)mem[ClientName];
        internal string pw => (string)mem[ClientPassword]; // deprecated
        internal long viewStruct => (long)(mem[ViewStructPos]??-1);
        internal long usingTable => (long)(mem[UsingTablePos]??-1);
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
     }
}
