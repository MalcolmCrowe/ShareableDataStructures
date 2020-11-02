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
            ViewCols = -378, // BTree<string,long> SqlValue
            ViewDef = -379, // string
            ViewPpos = -377,// long
            ViewQry = -380; // long QueryExpression
        public string name => (string)mem[Name];
        public string viewDef => (string)mem[ViewDef];
        public BTree<string, long> viewCols =>
            (BTree<string, long>)mem[ViewCols] ?? BTree<string, long>.Empty;
        public long viewPpos => (long)(mem[ViewPpos] ?? -1L);
        public long viewQry => (long)(mem[ViewQry]??-1L);
        public View(PView pv,Database db,BTree<long,object>m=null) 
            : base(pv.ppos, _Dom(pv)
                  + (Name,pv.name) + (Definer,db.role.defpos)
                  + (ViewDef,pv.viewdef)
                  + (LastChange, pv.ppos))
        { }
        internal View(View vw, Context cx)
            : base(cx.nextHeap++,_Dom(cx,vw))
        { }
        protected View(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Dom(PView pv)
        {
            var vd = pv.framing.obs[pv.query];
            var ns = BTree<string, long>.Empty;
            var d = 1+vd.depth;
            for (var b=vd.domain.rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                var c = (SqlValue)pv.framing.obs[p];
                d = _Max(d, 1 + c.depth);
                ns += (c.name, p);
            }
            return BTree<long,object>.Empty + (_Domain,vd.domain) + (ViewPpos,pv.ppos)
                +(ViewCols,ns) + (_Framing,pv.framing) + (ViewQry,pv.query)
                +(Depth,d);
        }
        /// <summary>
        /// This routine prepares a fresh copy of the View for use in query optimisation
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="vw"></param>
        /// <returns></returns>
        static BTree<long, object> _Dom(Context cx,View vw)
        {
            var fx = BTree<long, long?>.Empty;
            for (var b = vw.framing.obs.PositionAt(vw.viewPpos); b != null; b = b.Next())
                fx += (b.key(), cx.nextHeap++);
            for (var b = vw.framing.data.PositionAt(vw.viewPpos); b != null; b = b.Next())
                if (!fx.Contains(b.key()))
                    fx += (b.key(), cx.nextHeap++);
            var nf = new Framing(vw, fx);
            cx.Install1(nf);
            cx.Install2(nf);
            var ns = BTree<string, long>.Empty;
            for (var b = vw.viewCols.First(); b != null; b = b.Next())
                ns += (b.key(), fx[b.value()] ?? b.value());
            return BTree<long, object>.Empty + (_Domain, vw.domain.Fix(fx))
                + (ViewDef, vw.viewDef) + (ViewPpos, vw.viewPpos)
                + (ViewCols, ns) + (ViewQry,fx[vw.viewQry]) 
                + (_Framing,nf) + (Depth,vw.depth);
        }
        public static View operator+(View v,(long,object)x)
        {
            return new View(v.defpos, v.mem + x);
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
        internal override void Select(Context cx, From f, BTree<long, RowSet.Finder> fi)
        {
            cx.Install2(framing);
            if (!cx.data.Contains(defpos))
            {
                var vq = (Query)cx.obs[viewQry];
                var rs = vq.RowSets(cx, fi);
                var sc = (long)rs.mem[From.Source];
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
        internal override void Scan(Context cx)
        {
            cx.Scan(viewCols);
            cx.ObUnheap(viewQry);
            cx.ObUnheap(defpos);
        }
        internal override Basis Fix(Context cx)
        {
            return base.Fix(cx)+(ViewCols,cx.Fix(viewCols))+(ViewQry,cx.Fix(viewQry));
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
        internal override Basis Fix(BTree<long, long?> fx)
        {
            var r = (View)base.Fix(fx);
            r += (ViewCols, Fix(viewCols,fx));
            r += (ViewQry, fx[viewQry]??viewQry);
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
            sb.Append(" Ppos: "); sb.Append(viewPpos);
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
        public RestView(PRestView pv,Database db) : base(pv,db,BTree<long,object>.Empty
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
        internal override Basis Fix(BTree<long, long?> fx)
        {
            var r = base.Fix(fx);
            r += (ViewStructPos, fx[viewStruct]??viewStruct);
            r += (UsingTablePos, fx[usingTable]??usingTable);
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
