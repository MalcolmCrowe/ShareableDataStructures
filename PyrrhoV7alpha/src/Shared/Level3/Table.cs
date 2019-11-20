using System;
using System.Text;
using System.Collections.Generic;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
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
    /// When a Table is accessed, the Rows information comes from the schemaRole, 
    /// and any role with select access to the table will be able to retrieve rows subject 
    /// to security clearance and classification. Which columns are accessible also depends
    /// on privileges (but columns are not subject to classification).
    /// </summary>
    internal class Table : DBObject
    {
        internal const long
            ApplicationPS = -262, // long
            Enforcement = -263, // Grant.Privilege (T)
            Indexes = -264, // BTree<CList<TableColumn>,long> (T) 
            SystemPS = -265, //long
            TableChecks = -266, // BTree<long,Check> (T)
            Triggers = -267; // BTree<PTrigger.TrigType,ATree<long,Trigger>> (T) 
        /// <summary>
        /// The rows of the table with the latest version for each
        /// </summary>
		public BTree<long, object> tableRows => mem;
        public BTree<CList<TableColumn>, long> indexes => 
            (BTree<CList<TableColumn>,long>)mem[Indexes]??BTree<CList<TableColumn>,long>.Empty;
        /// <summary>
        /// Enforcement of clearance rules
        /// </summary>
        internal Grant.Privilege enforcement => (Grant.Privilege)(mem[Enforcement]??0);
        internal long applicationPS => (long)(mem[ApplicationPS] ?? -1L);
        internal long systemPS => (long)(mem[SystemPS] ?? -1L);
        internal BTree<long, Check> tableChecks => 
            (BTree<long, Check>)mem[TableChecks]??BTree<long,Check>.Empty;
        internal BTree<PTrigger.TrigType, BTree<long, Trigger>> triggers =>
            (BTree<PTrigger.TrigType, BTree<long, Trigger>>)mem[Triggers]
            ??BTree<PTrigger.TrigType, BTree<long, Trigger>>.Empty;
        /// <summary>
        /// Constructor: a new empty table
        /// </summary>
        internal Table(PTable pt) :base(pt.ppos, BTree<long,object>.Empty
            +(Name,pt.name)+(Definer,pt.db.role.defpos)
            //GetDomain((Database)null,BList<Selector>.Empty).Item2)
            +(Indexes,BTree<CList<TableColumn>,long>.Empty)
            +(Triggers, BTree<PTrigger.TrigType, BTree<long, Trigger>>.Empty)
            +(Enforcement,(Grant.Privilege)15)) //read|insert|update|delete
        {}
        protected Table(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Table operator+(Table tb,TableColumn tc)
        {
            var ds = tb.dependents + (tc.defpos,true);
            var dp = _Max(tb.depth, 1 + tc.depth);
            return (Table)tb.New(tb.mem+(Dependents,ds)+(Depth,dp));
        }
        public static Table operator+(Table tb,Metadata md)
        {
            var m = tb.mem;
            if (md.description != "") m += (Description, md.description); 
            return new Table(tb.defpos, m);
        }
        public static Table operator-(Table tb,long p)
        {
            return new Table(tb.defpos, tb.mem - p);
        }
        /// <summary>
        /// Add a new or updated row, indexes already fixed.
        /// </summary>
        /// <param name="t"></param>
        /// <param name="rw"></param>
        /// <returns></returns>
        public static Table operator +(Table t, (Database,TableRow) x)
        {
            var (db, rw) = x;
            var se = t.sensitive || rw.classification!=Level.D;
            return new Table(t.defpos, t.mem + (rw.defpos,rw) 
                + (Sensitive,se));
        }
        public static Table operator+(Table tb,(long,object)v)
        {
            return new Table(tb.defpos, tb.mem + v);
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new Table(defpos,mem+(TableChecks,tableChecks+(ck.defpos,ck)));
        }
        internal Table AddTrigger(Trigger tg, Database db)
        {
            var tb = this;
            var ts = triggers[tg.tgType] ?? BTree<long, Trigger>.Empty;
            return tb + (Triggers, triggers+(tg.tgType, ts + (tg.tabledefpos, tg)));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Table(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Table(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
        internal override (Database,Role) Cascade(Database d, Database nd,Role ro, 
            Drop.DropAction a = 0, BTree<long, TypedValue> u = null)
        {
            if (a != 0)
                nd += (Database.Cascade, true);
            for (var b = indexes.First(); b != null; b = b.Next())
                (nd,ro) = ((Index)d.objects[b.value()]).Cascade(d, nd,ro,a,u);
            for (var b = d.role.dbobjects.First(); b != null; b = b.Next())
                if (d.objects[b.value()] is Table tb)
                    for (var c = tb.indexes.First(); c != null; c = c.Next())
                        if (((Index)d.objects[c.value()]).reftabledefpos == defpos)
                            (nd,ro) = tb.Cascade(d,nd,ro,a,u);
            return base.Cascade(d, nd,ro,a,u);
        }
        /// <summary>
        /// Execute an Insert on the table including trigger operation.
        /// </summary>
        /// <param name="f">The Insert</param>
        /// <param name="prov">The provenance</param>
        /// <param name="data">The insert data may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">The target rowset may be explicit</param>
        internal override Transaction Insert(Transaction tr, Context _cx, From f,string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            int count = 0;
            if (Denied(tr, Grant.Privilege.Insert))
                throw new DBException("42105", ((ObInfo)tr.role.obinfos[defpos]).name);
            long st = 0;
            var ot = data.rowType;
            var ft = f.rowType;
            if (ft.Length == ot.Length && (!ft.Equals(ot))
                && ((ObInfo)tr.role.obinfos[defpos]).EqualOrStrongSubtypeOf(ot))
                st = ft.defpos;
            // parameter cl is only supplied when d_User.defpos==d.owner
            // otherwise check if we should compute it
            if (tr.user.defpos != tr.owner && enforcement.HasFlag(Grant.Privilege.Insert))
            {
                var uc = tr.user.clearance;
                if (!uc.ClearanceAllows(classification))
                    throw new DBException("42105", ((ObInfo)tr.role.obinfos[defpos]).name);
                // The new record’s classification will have the user’s minimum clearance level:
                // if this is above D, the groups will be the subset of the user’s groups 
                // that are in the table classification, 
                // and the references will be the same as the table 
                // (a subset of the user’s references)
                cl = uc.ForInsert(classification);
            }
            var trs = new TransitionRowSet(tr, _cx, f, PTrigger.TrigType.Insert, eqs);
            //       var ckc = new ConstraintChecking(tr, trs, this);
            // Do statement-level triggers
            tr = trs.InsertSA(tr, _cx);
            if (_cx.ret != TBool.True)
            {
                var nr = tr.nextTid;
                for (var trb = trs.First(_cx); trb != null; trb = trb.Next(_cx)) // trb constructor checks for autokey
                {
                    var _trb = trb as TransitionRowSet.TransitionRowBookmark;
                    var rt = (ObInfo)tr.schemaRole.obinfos[defpos];
                    for (var b = rt.columns.First(); b != null; b = b.Next())
                    {
                        var sc = b.value();
                        if (sc is SqlCol cm)
                        {
                            var tc = cm.tableCol;
                            var tv = _cx.row.values[tc.defpos];
                            if (tv == null || tv == TNull.Value)
                            {
                                if (tc.generated is GenerationRule gr && gr.gen!=Generation.No)
                                    _cx.row.values += (tc.defpos,gr.Eval(tr,_cx));
                                else
                                if ((cm.tableCol.defaultValue ?? cm.tableCol.domain.defaultValue)
                                    is TypedValue dv && dv != null && dv != TNull.Value)
                                    _cx.row.values += (cm.tableCol.defpos, dv);
                                else if (cm.tableCol.notNull)
                                    throw new DBException("22206", sc.name);
                            }
                            for (var cb = cm.tableCol.constraints?.First(); cb != null; cb = cb.Next())
                                if (cb.value().search.Eval(_cx, trb) != TBool.True)
                                    throw new DBException("22212", sc.name);
                        }
                    }
                    // Do row-level triggers
                    tr = _trb.InsertRB(tr,_cx);
                    Record r = null;
                    if (cl != Level.D)
                        r = new Record3(this,_cx.row.values, st, cl, nr++, tr);
                    else if (prov != null)
                        r = new Record1(this,_cx.row.values, prov, nr++,tr);
                    else
                        r = new Record(this, _cx.row.values, nr++, tr);
                    count++;
                    // install the record in the database
                    tr+=r;
                    _cx.affected+=new Rvv(defpos, trb._defpos, r.ppos);
                   // Row-level after triggers
                    tr = _trb.InsertRA(tr,_cx);
                }
                tr += (Database.NextTid, nr);
            }
            // Statement-level after triggers
            tr = trs.InsertSA(tr,_cx);
            return tr;
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
                    if (d.value().defpos != c.value().defpos)
                        goto skip;
                return ix;
                    skip:;
            }
            return null;
        }
        internal Index FindIndex(Database db, CList<TableColumn> key)
        {
            return (Index)db.objects[indexes[key]];
        }
        /// <summary>
        /// Execute a Delete on a Table, including triggers
        /// </summary>
        /// <param name="f">The Delete operation</param>
        /// <param name="ds">A set of delete strings may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Transaction Delete(Transaction tr, Context cx, From f,BTree<string, bool> ds, Adapters eqs)
        {
            var count = 0;
            if (Denied(tr, Grant.Privilege.Delete))
                throw new DBException("42105", ((ObInfo)tr.role.obinfos[defpos]).name);
            var trs = new TransitionRowSet(tr, cx, f, PTrigger.TrigType.Delete, eqs);
            var cl = tr.user.clearance;
            tr = trs.DeleteSB(tr, cx);
            var nr = tr.nextTid;
            cx.rb = f.RowSets(tr, cx).First(cx);
            for (var trb = trs.First(cx) as TransitionRowSet.TransitionRowBookmark; trb != null;
                trb = trb.Next(cx) as TransitionRowSet.TransitionRowBookmark)
            {
      //          if (ds.Count > 0 && !ds.Contains(trb.Rvv()))
        //            continue;
                tr = trb.DeleteRB(tr, cx);
                var rec = cx.rb.Rec();
                if (tr.user.defpos != tr.owner && enforcement.HasFlag(Grant.Privilege.Delete)?
                    // If Delete is enforced by the table and the user has delete privilege for the table, 
                    // but the record to be deleted has a classification level different from the user 
                    // or the clearance does not allow access to the record, throw an Access Denied exception.
                    ((!cl.ClearanceAllows(rec.classification)) || cl.minLevel > rec.classification.minLevel)
                    : cl.minLevel > 0)
                    throw new DBException("42105");
                tr += new Delete1(rec, nr++, tr);
                count++;
                cx.affected+=new Rvv(defpos, rec.defpos, tr.loadpos);
            }
            return tr+(Database.NextTid,nr);
        }
        /// <summary>
        /// Execute an Update operation on the Table, including triggers
        /// </summary>
        /// <param name="f">The Update statement</param>
        /// <param name="ur">The update row identifiers may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">The target rowset may be explicit</param>
        internal override Transaction Update(Transaction tr,Context cx,From f,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet>rs)
        {
            if (f.assigns == null)
                return tr;
            if (Denied(tr, Grant.Privilege.Insert))
                throw new DBException("42105", ((ObInfo)tr.role.obinfos[defpos]).name);
            var trs = new TransitionRowSet(tr, cx, f, PTrigger.TrigType.Update, eqs);
            var updates = BTree<long, UpdateAssignment>.Empty;
            SqlValue level = null;
            for (var ass=f.assigns.First();ass!=null;ass=ass.Next())
            {
                var c = ass.value().vbl;
                var tc = tr.objects[c.defpos] as TableColumn ??
                    throw new DBException("42112", ass.value().vbl.name);
                if (!cx.obs.Contains(c.defpos)) // can happen with updatable joins
                    continue;
                if (tc.generated != GenerationRule.None)
                    throw tr.Exception("0U000", c.name).Mix();
                if (c.Denied(tr, Grant.Privilege.Insert))
                    throw new DBException("42105", c.name);
                updates += (c.defpos, ass.value());
            }
      //      bool nodata = true;
            var cl = tr.user.clearance;
            cx.rb = f.RowSets(tr, cx).First(cx);
            if ((level != null || updates.Count > 0))
            {
                tr = trs.UpdateSB(tr, cx);
                var nr = tr.nextTid;
                for (var trb = trs.First(cx) as TransitionRowSet.TransitionRowBookmark;
                    trb != null; trb = trb.Next(cx) as TransitionRowSet.TransitionRowBookmark)
                {
                    //       if (ur.Count > 0 && !ur.Contains(trb._Rvv().ToString()))
                    //           continue;
                    var vals = BTree<long,TypedValue>.Empty;
                    for (var b=updates.First();b!=null;b=b.Next())
                    {
                        var ua = b.value();
                        var av = ua.val.Eval(tr, cx)?.NotNull();
                        var dt = ua.vbl.domain;
                        if (av != null && !av.dataType.EqualOrStrongSubtypeOf(dt))
                            av = dt.Coerce(av);
                        vals += (ua.vbl.defpos, av);
                    }
                    tr = trb.UpdateRB(tr, cx);
                    TableRow rc = trb.Rec();
                    // If Update is enforced by the table, and a record selected for update 
                    // is not one to which the user has clearance 
                    // or does not match the user’s clearance level, 
                    // throw an Access Denied exception.
                    if (enforcement.HasFlag(Grant.Privilege.Update)
                        && tr.user.defpos != tr.owner && ((rc != null) ?
                             ((!cl.ClearanceAllows(rc.classification))
                             || cl.minLevel != rc.classification.minLevel)
                             : cl.minLevel > 0))
                        throw new DBException("42105");
                    var u = (level == null) ?
                        new Update(rc, this, vals, nr++, tr) :
                        new Update1(rc, this, vals, (Level)level.Eval(tr, cx).Val(), nr++, tr);
                    tr += u;
                    trb.UpdateRA(tr,cx);
                    cx.affected+=new Rvv(defpos, u.defpos, tr.loadpos);
                }
                tr += (Database.NextTid, nr);
            }
            trs.UpdateSA(tr,cx);
            rs.Add(trs); // just for PUT
            return tr;
        }
        /// <summary>
        /// See if we already have an audit covering an access in the current transaction
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        internal override bool DoAudit(Transaction tr, long[] cols, string[] key)
        {
            // something clever here would be nice
            return true;
        }
        /// <summary>
        /// A readable version of the Table
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(Enforcement)) { sb.Append(" Enforcement="); sb.Append(enforcement); }
            if (mem.Contains(Indexes)) { sb.Append(" Indexes:"); sb.Append(indexes); }
            if (mem.Contains(Triggers)) { sb.Append(" Triggers:"); sb.Append(triggers); }
            return sb.ToString();
        }
        /// <summary>
        /// Generate a row for the Role$Class table: includes a C# class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal TRow RoleClassValue(Transaction tr, Table from, ABookmark<long, DBObject> _enu)
        {
            var ob = _enu.value();
            var md = (ObInfo)tr.role.obinfos[ob.defpos];
            var ro = tr.role;
            var versioned = true;
            var key = BuildKey(tr, out Index ix);
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n[Schema("); sb.Append(from.lastChange); sb.Append(")]");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + md.name + " from Database " + tr.name + ", Role " + ro.name + "\r\n");
            if (md.desc != "")
                sb.Append("/// " + md.desc + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + md.name + ((versioned) ? " : Versioned" : "") + " {\r\n");
            var rt = tr.role.obinfos[from.defpos] as ObInfo;
            for (var b = rt.columns.First();b!=null;b=b.Next())
            {
                var cv = b.value();
                var dt = cv.domain;
                var di = (ObInfo)tr.role.obinfos[dt.defpos];
                var tn = (dt.kind == Sqlx.TYPE) ? di.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j = (int)ix.keys.Count;
                    for (j = 0; j < ix.keys.Count; j++)
                        if (ix.keys[j].defpos == cv.defpos)
                            break;
                    if (j < ix.keys.Count)
                        sb.Append("  [Key(" + j + ")]\r\n");
                }
                FieldType(tr,sb, dt);
                sb.Append("  public " + tn + " " + rt.columns[b.key()].name + ";");
                sb.Append("\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(tr.nextTid,("Name",new TChar(md.name)),
                ("Key",new TChar(key)),
                ("Def",new TChar(sb.ToString())));
        }
        /// <summary>
        /// Generate a row for the Role$Java table: includes a Java class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleJavaValue(Transaction tr, From from, 
            ABookmark<long, object> _enu)
        {
            var ob = (DBObject)_enu.value();
            var md = (ObInfo)tr.role.obinfos[ob.defpos];
            var ro = tr.role;
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("/*\r\n * "); sb.Append(md.name); sb.Append(".java\r\n *\r\n * Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n * from Database " + tr.name + ", Role " + tr.role.name + "r\n */");
            sb.Append("import org.pyrrhodb.*;\r\n");
            var key = BuildKey(tr,out Index ix);
            sb.Append("\r\n@Schema("); sb.Append(from.lastChange); sb.Append(")");
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(tr.user.name); sb.Append("\r\n */");
            if (md.desc != "")
                sb.Append("/* " + md.desc + "*/\r\n");
            sb.Append("public class " + md.name + ((versioned) ? " extends Versioned" : "") + " {\r\n");
            var rt = tr.role.obinfos[from.defpos] as ObInfo;
            for(var b = rt.columns.First();b!=null;b=b.Next())
            {
                var cd = b.value();
                var dt = cd.domain;
                var di = tr.role.obinfos[dt.defpos] as ObInfo;
                var tn = (dt.kind == Sqlx.TYPE) ? di.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j = (int)ix.keys.Count;
                    for (j = 0; j < ix.keys.Count; j++)
                        if (ix.keys[j].defpos == cd.defpos)
                            break;
                    if (j < ix.keys.Count)
                        sb.Append("  @Key(" + j + ")\r\n");
                }
                FieldJava(tr, sb, dt);
                sb.Append("  public " + tn + " " + rt.columns[b.key()].name + ";");
                sb.Append("\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(tr.nextTid,("Name",new TChar(md.name)),
                ("Key",new TChar(key)),
                ("Def",new TChar(sb.ToString())));
        }
        /// <summary>
        /// Generate a row for the Role$Python table: includes a Python class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RolePythonValue(Transaction tr, From from, ABookmark<long, object> _enu)
        {
            var tb = (Table)_enu.value();
            var md = (ObInfo)tr.role.obinfos[tb.defpos];
            var ro = tr.role;
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("# "); sb.Append(md.name); sb.Append(" Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n# from Database " + tr.name + ", Role " + tr.role.name + "\r\n");
            var key = BuildKey(tr, out Index ix);
            if (md.desc != "")
                sb.Append("# " + md.desc + "\r\n");
            sb.Append("class " + md.name + (versioned ? "(Versioned)" : "") + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            if (versioned)
                sb.Append("  super().__init__('','')\r\n");
            var rt = tr.role.obinfos[from.defpos] as ObInfo;
            for(var b = rt.columns.First();b!=null;b=b.Next())
            {
                var cd = b.value();
                var dt = cd.domain;
                var di = (ObInfo)tr.role.obinfos[dt.defpos];
                var tn = (dt.kind == Sqlx.TYPE) ? di.name : dt.SystemType.Name;
                sb.Append("  self." + rt.columns[b.key()].name + " = " + dt.defaultValue);
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
                    var tc = tr.objects[se.defpos]as TableColumn;
                    sb.Append(comma); comma = ",";
                    sb.Append("'");  sb.Append(ix.keys[i]); sb.Append("'");
                }
                sb.Append("]\r\n");
            }
            return new TRow(tr.nextTid,("Name", new TChar(md.name)),
                ("Key",new TChar(key)),
                ("Def",new TChar(sb.ToString())));
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
                var oi = (ObInfo)db.role.obinfos[ix.defpos];
                for (var i = 0; i < (int)ix.keys.Count; i++)
                {
                    var se = ix.keys[i];
                    var cd = db.objects[se.defpos] as TableColumn;
                    if (cd != null)
                    {
                        sk.Append(comma);
                        comma = ",";
                        sk.Append(oi.columns[i].name);
                    }
                }
            }
            return sk.ToString();
        }
    }
}
