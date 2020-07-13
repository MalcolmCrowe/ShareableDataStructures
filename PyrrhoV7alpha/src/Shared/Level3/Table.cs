using System;
using System.Text;
using System.Collections.Generic;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
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
    /// When a Table is accessed, the Rows information comes from the schemaRole, 
    /// and any role with select access to the table will be able to retrieve rows subject 
    /// to security clearance and classification. Which columns are accessible also depends
    /// on privileges (but columns are not subject to classification).
    /// </summary>
    internal class Table : DBObject
    {
        internal const long
            ApplicationPS = -262, // long PeriodSpecification
            Enforcement = -263, // Grant.Privilege (T)
            Indexes = -264, // BTree<RowType,long> Index
            TableCols = -332, // BTree<long,bool> TableColumn
            SystemPS = -265, //long (system-period specification)
            TableChecks = -266, // BTree<long,bool> Check
            TableRows = -181, // BTree<long,TableRow>
            Triggers = -267; // BTree<PTrigger.TrigType,BTree<long,bool>> (T) 
        /// <summary>
        /// The rows of the table with the latest version for each
        /// </summary>
		public BTree<long, TableRow> tableRows => 
            (BTree<long,TableRow>)mem[TableRows]??BTree<long,TableRow>.Empty;
        public BTree<RowType, long> indexes => 
            (BTree<RowType,long>)mem[Indexes]
            ??BTree<RowType,long>.Empty;
        internal BTree<long, bool> tblCols =>
            (BTree<long, bool>)mem[TableCols] ?? BTree<long, bool>.Empty;
        /// <summary>
        /// Enforcement of clearance rules
        /// </summary>
        internal Grant.Privilege enforcement => (Grant.Privilege)(mem[Enforcement]??0);
        internal long applicationPS => (long)(mem[ApplicationPS] ?? -1L);
        internal long systemPS => (long)(mem[SystemPS] ?? -1L);
        internal override Sqlx kind => Sqlx.TABLE;
        internal BTree<long, bool> tableChecks => 
            (BTree<long, bool>)mem[TableChecks]??BTree<long,bool>.Empty;
        internal BTree<PTrigger.TrigType, BTree<long,bool>> triggers =>
            (BTree<PTrigger.TrigType, BTree<long, bool>>)mem[Triggers]
            ??BTree<PTrigger.TrigType, BTree<long, bool>>.Empty;
        /// <summary>
        /// Constructor: a new empty table
        /// </summary>
        internal Table(PTable pt) :base(pt.ppos, BTree<long,object>.Empty
            +(Name,pt.name)+(Definer,pt.database.role.defpos)
            +(Indexes,BTree<RowType,long>.Empty)
            +(_Domain,Domain.TableType)
            +(Triggers, BTree<PTrigger.TrigType, BTree<long, bool>>.Empty)
            +(Enforcement,(Grant.Privilege)15)) //read|insert|update|delete
        { }
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
            return new Table(tb.defpos, tb.mem + (TableRows,tb.tableRows-p));
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
            return new Table(t.defpos, t.mem + (TableRows,t.tableRows+(rw.defpos,rw)) 
                + (Sensitive,se));
        }
        public static Table operator+(Table tb,(long,object)v)
        {
            return (Table)tb.New(tb.mem + v);
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
            var ts = triggers[tg.tgType] ?? BTree<long, bool>.Empty;
            return tb + (Triggers, triggers+(tg.tgType, ts + (tg.defpos, true)));
        }
        internal override void AddCols(Context cx, Ident id, RowType s, bool force = false)
        {
            if ((!force) && (!cx.constraintDefs) && cx.obs[id.iix] is Table)
                return;
            for (var b = s?.First(); b != null; b = b.Next())
            {
                var p = b.value().Item1;
                var ob = cx.obs[p] ?? (DBObject)cx.db.objects[p];
                if ((!force) && (!cx.constraintDefs) && (ob is Table || ob is TableColumn))
                    continue;
                var n = (ob is SqlValue v) ? v.name : cx.NameFor(p);
                if (n == null)
                    continue;
                var ic = new Ident(n, p, ob.kind);
                cx.defs += (new Ident(id, ic), ob);
                cx.defs += (ic, ob);
            }
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
            var r = base._Relocate(wr);
            var dt = (Domain)domain._Relocate(wr);
            if (dt != domain)
                r += (_Domain, dt);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = base._Relocate(cx);
            var dt = (Domain)domain._Relocate(cx);
            if (dt != domain)
                r += (_Domain, dt);
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
        /// <summary>
        /// Execute an Insert on the table including trigger operation.
        /// </summary>
        /// <param name="f">The Insert</param>
        /// <param name="prov">The provenance</param>
        /// <param name="data">The insert data may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">The target rowset may be explicit</param>
        internal override Context Insert(Context cx, From f,string prov, 
            RowSet data, Adapters eqs, List<RowSet> rs,Level cl)
        {
            int count = 0;
            if (Denied(cx, Grant.Privilege.Insert))
                throw new DBException("42105", ((ObInfo)cx.db.role.infos[defpos]).name);
            var dt = data.dataType;
            var st = (dt != f.domain) ? dt.defpos : -1L; // subtype
            // parameter cl is only supplied when d_User.defpos==d.owner
            // otherwise check if we should compute it
            if (cx.db.user!=null &&
                cx.db.user.defpos != cx.db.owner && enforcement.HasFlag(Grant.Privilege.Insert))
            {
                var uc = cx.db.user.clearance;
                if (!uc.ClearanceAllows(classification))
                    throw new DBException("42105", ((ObInfo)cx.db.role.infos[defpos]).name);
                // The new record’s classification will have the user’s minimum clearance level:
                // if this is above D, the groups will be the subset of the user’s groups 
                // that are in the table classification, 
                // and the references will be the same as the table 
                // (a subset of the user’s references)
                cl = uc.ForInsert(classification);
            }
            var trs = new TransitionRowSet(cx, f, PTrigger.TrigType.Insert, eqs);
            //       var ckc = new ConstraintChecking(tr, trs, this);
            // Do statement-level triggers
            bool fi;
            (_,fi) = trs.InsertSB(cx);
            if (!fi) // no insteadof has fired
            {
                for (var trb = trs.First(cx) as TransitionRowSet.TransitionCursor; 
                    trb != null; trb = trb.Next(cx) as TransitionRowSet.TransitionCursor) // trb constructor checks for autokey
                {
                    // Do row-level triggers
                    (trb,fi) = trb.InsertRB(cx);
                    if (fi) // an insteadof trigger has fired
                        continue;
                    Record r = null;
                    var np = cx.db.nextPos;
                    if (cl != Level.D)
                        r = new Record3(this,trb.targetRow.values, st, cl, np, cx);
                    else if (prov != null)
                        r = new Record1(this,trb.targetRow.values, prov, np, cx);
                    else
                        r = new Record(this,trb.targetRow.values, np, cx);
                    var nr = new TableRow(r, cx.db);
                    var ns = cx.newTables[trs.defpos] ?? BTree<long, TableRow>.Empty;
                    cx.newTables += (trs.defpos, ns + (nr.defpos, nr));
                    cx.Add(r);
                    count++;
                    // install the record in the database
                    cx.tr.FixTriggeredActions(triggers,trs._tgt,r.ppos);
          //          _cx.affected+=new Rvv(defpos, trb._defpos, r.ppos);
                   // Row-level after triggers
                    (trb,fi) = trb.InsertRA(cx);
                }
            }
            // Statement-level after triggers
            trs.InsertSA(cx);
            return cx;
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
                    if (d.value().defpos != c.value().Item1)
                        goto skip;
                return ix;
                    skip:;
            }
            return null;
        }
        internal Index FindIndex(Database db, CList<long> key)
        {
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var kb = key.First();
                for (var c = b.key().First(); kb != null && c != null;
                    c = c.Next(), kb = kb.Next())
                    if (c.value().Item1 != kb.value())
                        goto skip;
                return (Index)db.objects[b.value()];
            skip:;
            }
            return null;
        }
        internal Index FindIndex(Database db, RowType key)
        {
            return (Index)db.objects[indexes[key]];
        }
        /// <summary>
        /// Execute a Delete on a Table, including triggers
        /// </summary>
        /// <param name="f">The Delete operation</param>
        /// <param name="ds">A set of delete strings may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Context Delete(Context cx, From f,BTree<string, bool> ds, Adapters eqs)
        {
            var count = 0;
            if (Denied(cx, Grant.Privilege.Delete))
                throw new DBException("42105", ((ObInfo)cx.db.role.infos[defpos]).name);
            f.RowSets(cx, cx.data[f.from]?.finder??BTree<long, RowSet.Finder>.Empty); 
            var trs = new TransitionRowSet(cx, f, PTrigger.TrigType.Delete, eqs);
            var cl = cx.db.user.clearance;
            cx.from += trs.finder;
            var fi = false;
            (_,fi) = trs.DeleteSB(cx);
            if (!fi)
                for (var trb = trs.First(cx) as TransitionRowSet.TransitionCursor; trb != null;
                    trb = trb.Next(cx) as TransitionRowSet.TransitionCursor)
                {
                    //          if (ds.Count > 0 && !ds.Contains(trb.Rvv()))
                    //            continue;
                    (trb,fi) = trb.DeleteRB(cx);
                    if (fi)
                        continue;
                    var rec = trb.Rec();
                    if (cx.db.user.defpos != cx.db.owner && enforcement.HasFlag(Grant.Privilege.Delete) ?
                        // If Delete is enforced by the table and the user has delete privilege for the table, 
                        // but the record to be deleted has a classification level different from the user 
                        // or the clearance does not allow access to the record, throw an Access Denied exception.
                        ((!cl.ClearanceAllows(rec.classification)) || cl.minLevel > rec.classification.minLevel)
                        : cl.minLevel > 0)
                        throw new DBException("42105");
                    cx.tr.FixTriggeredActions(triggers, trs._tgt, cx.db.nextPos);
                    var np = cx.db.nextPos;
                    cx.Add(new Delete1(rec, np, cx));
                    count++;
          //          cx.affected += new Rvv(defpos, rec.defpos, tr.loadpos);
                }
            trs.DeleteSA(cx);
            return cx;
        }
        /// <summary>
        /// Execute an Update operation on the Table, including triggers
        /// </summary>
        /// <param name="f">The Update statement</param>
        /// <param name="ur">The update row identifiers may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">The target rowset may be explicit</param>
        internal override Context Update(Context cx,From f,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet>rs)
        {
            if (f.assigns.Count==0)
                return cx;
            if (Denied(cx, Grant.Privilege.Insert))
                throw new DBException("42105", ((ObInfo)cx.db.role.infos[defpos]).name);
            var trs = new TransitionRowSet(cx, f, PTrigger.TrigType.Update, eqs);
            var updates = BTree<long, UpdateAssignment>.Empty;
            SqlValue level = null;
            for (var ass=f.assigns.First();ass!=null;ass=ass.Next())
            {
                var c = cx.obs[ass.value().vbl] as SqlCopy
                    ?? throw new DBException("0U000");
                var tc = cx.db.objects[c.copyFrom] as TableColumn ??
                    throw new DBException("42112", c.name);
                if (tc.generated != GenerationRule.None)
                    throw cx.db.Exception("0U000", c.name).Mix();
                if (c.Denied(cx, Grant.Privilege.Insert))
                    throw new DBException("42105", c.name);
                updates += (tc.defpos, ass.value());
            }
      //      bool nodata = true;
            var cl = cx.db.user?.clearance??Level.D;
            trs.from.RowSets(cx, trs.finder);
            cx.from += trs.finder;
            if ((level != null || updates.Count > 0))
            {
                var (_,fi) = trs.UpdateSB(cx);
                if (!fi)
                    for (var trb = trs.First(cx) as TransitionRowSet.TransitionCursor;
                        trb != null; trb = trb.Next(cx) as TransitionRowSet.TransitionCursor)
                    {
                        for (var b=updates.First();b!=null;b=b.Next())
                        {
                            var ua = b.value();
                            var tv = cx.obs[ua.val].Eval(cx).NotNull();
                            if (tv == TNull.Value && cx.obs[ua.vbl] is TableColumn tc
                                && tc.notNull)
                                throw new DBException("0U000", cx.NameFor(ua.vbl));
                            trb += (cx, ua.vbl, tv);
                        }
                        (trb,fi) = trb.UpdateRB(cx);
                        if (fi) // an insteadof trigger has fired
                            continue;
                        TableRow rc = trb.Rec();
                        // If Update is enforced by the table, and a record selected for update 
                        // is not one to which the user has clearance 
                        // or does not match the user’s clearance level, 
                        // throw an Access Denied exception.
                        if (enforcement.HasFlag(Grant.Privilege.Update)
                            && cx.db.user!=null
                            && cx.db.user.defpos != cx.db.owner && ((rc != null) ?
                                 ((!cl.ClearanceAllows(rc.classification))
                                 || cl.minLevel != rc.classification.minLevel)
                                 : cl.minLevel > 0))
                            throw new DBException("42105");
                        var np = cx.db.nextPos;
                        var u = (level == null) ?
                            new Update(rc, this, trb.targetRow.values, np, cx) :
                            new Update1(rc, this, trb.targetRow.values, (Level)level.Eval(cx).Val(), np, cx);
                        cx.Add(u);
                        var nr = new TableRow(u, cx.db);
                        var ns = cx.newTables[trs.defpos] ?? BTree<long, TableRow>.Empty;
                        cx.newTables += (trs.defpos,  ns + (nr.defpos, nr));
                        cx.tr.FixTriggeredActions(triggers, trs._tgt, u.ppos);
                        (trb,_) = trb.UpdateRA(cx);
              //          cx.affected += new Rvv(defpos, u.defpos, tr.loadpos);
                    }
            }
            trs.UpdateSA(cx);
            rs.Add(trs); // just for PUT
            return cx;
        }
        internal override RowType Struct(Context cx)
        {
            return ((ObInfo)cx.db.role.infos[defpos]).rowType;
        }
        /// <summary>
        /// See if we already have an audit covering an access in the current transaction
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        internal override bool DoAudit(long pp, Context cx, long[] cols, string[] key)
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
            sb.Append(domain);
            if (mem.Contains(Enforcement)) { sb.Append(" Enforcement="); sb.Append(enforcement); }
            if (indexes.Count!=0) { sb.Append(" Indexes:"); sb.Append(indexes); }
            if (triggers.Count!=0) { sb.Append(" Triggers:"); sb.Append(triggers); }
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
            if (md.desc != "")
                sb.Append("/// " + md.desc + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + md.name + ((versioned) ? " : Versioned" : "") + " {\r\n");
            var rt = tr.role.infos[from.defpos] as ObInfo;
            for (var b = rt.domain.representation.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var dt = b.value();
                var di = (ObInfo)tr.role.infos[dt.defpos];
                var tn = (dt.prim == Sqlx.TYPE) ? di.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j = (int)ix.keys.Count;
                    for (j = 0; j < ix.keys.Count; j++)
                        if (ix.keys[j].Item1 == p)
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
            var rt = tr.role.infos[from.defpos] as ObInfo;
            for(var b = rt.domain.representation.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var cd = b.value();
                var dt = cd;
                var di = tr.role.infos[dt.defpos] as ObInfo;
                var tn = (dt.prim == Sqlx.TYPE) ? di.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j = (int)ix.keys.Count;
                    for (j = 0; j < ix.keys.Count; j++)
                        if (ix.keys[j].Item1 == cd.defpos)
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
            if (md.desc != "")
                sb.Append("# " + md.desc + "\r\n");
            sb.Append("class " + md.name + (versioned ? "(Versioned)" : "") + ":\r\n");
            sb.Append(" def __init__(self):\r\n");
            if (versioned)
                sb.Append("  super().__init__('','')\r\n");
            var rt = tr.role.infos[from.defpos] as ObInfo;
            for(var b = rt.domain.representation.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var dt = b.value();
                var di = (ObInfo)tr.role.infos[dt.defpos];
                var tn = (dt.prim == Sqlx.TYPE) ? di.name : dt.SystemType.Name;
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
                var oi = (ObInfo)db.role.infos[ix.defpos];
                for (var i = 0; i < (int)ix.keys.Count; i++)
                {
                    var se = ix.keys[i].Item1;
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
}
