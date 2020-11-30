using System;
using System.Text;
using System.Collections.Generic;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using System.Runtime.CompilerServices;
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
    /// When a Table is accessed
    /// any role with select access to the table will be able to retrieve rows subject 
    /// to security clearance and classification. Which columns are accessible also depends
    /// on privileges (but columns are not subject to classification).
    /// </summary>
    internal class Table : DBObject
    {
        internal const long
            ApplicationPS = -262, // long PeriodSpecification
            Enforcement = -263, // Grant.Privilege (T)
            Indexes = -264, // BTree<CList<long>,long> Index
            KeyCols = -320, // BTree<long,bool> TableColumn (over all indexes)
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
        public BTree<CList<long>, long> indexes => 
            (BTree<CList<long>,long>)mem[Indexes]??BTree<CList<long>,long>.Empty;
        public BTree<long, bool> keyCols =>
            (BTree<long, bool>)mem[KeyCols] ?? BTree<long, bool>.Empty;
        internal BTree<long, bool> tblCols =>
            (BTree<long, bool>)mem[TableCols] ?? BTree<long, bool>.Empty;
        /// <summary>
        /// Enforcement of clearance rules
        /// </summary>
        internal Grant.Privilege enforcement => (Grant.Privilege)(mem[Enforcement]??0);
        internal long applicationPS => (long)(mem[ApplicationPS] ?? -1L);
        internal long systemPS => (long)(mem[SystemPS] ?? -1L);
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
            +(Indexes,BTree<CList<long>,long>.Empty) + (LastChange, pt.ppos)
            + (_Domain,Domain.TableType)+(LastChange,pt.ppos)
            +(Triggers, BTree<PTrigger.TrigType, BTree<long, bool>>.Empty)
            +(Enforcement,(Grant.Privilege)15)) //read|insert|update|delete
        { }
        /// <summary>
        /// Ad hoc table for LogRows, LogRowCol
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="nm"></param>
        internal Table(Context cx,string nm)
            :base(cx.nextHeap++,BTree<long,object>.Empty+(Name,nm)+(_Domain,Domain.TableType))
        {
            cx.Add(this);
        }
        protected Table(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Table operator+(Table tb,TableColumn tc)
        {
            var ds = tb.dependents + (tc.defpos,true);
            var dp = _Max(tb.depth, 1 + tc.depth);
            var ts = tb.tblCols + (tc.defpos, true);
            var m = tb.mem + (Dependents, ds) + (Depth, dp) + (TableCols, ts);
            if (tc.sensitive)
                m += (Sensitive, true);
            return (Table)tb.New(m);
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
        internal override ObInfo Inf(Context cx)
        {
            var ti = cx.Inf(defpos);
            var rt = domain.rowType;
            for (var b=rt.First();b!=null;b=b.Next())
            {
                var ci = cx.Inf(b.value());
                if (cx.db._user!=cx.db.owner 
                    && !cx.db.user.clearance.ClearanceAllows(ci.classification))
                    rt = rt.Without(b.value());
            }
            if (rt != domain.rowType)
            {
                if (rt.Count == 0)
                    throw new DBException("2E111", ti.name);
                ti += (_Domain, ti.domain + (Domain.RowType, rt));
            }
            return ti;
        }
        internal override CList<long> _Cols(Context cx)
        {
            return cx.Inf(defpos).domain.rowType;
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
            if (defpos < wr.Length)
                return this;
            var r = (Table)base._Relocate(wr);
            r += (_Domain, domain._Relocate(wr));
            if (applicationPS>=0)
                r += (ApplicationPS, wr.Fix(applicationPS));
            r += (Indexes, wr.Fix(indexes));
            r += (TableCols, wr.Fix(tblCols));
            if (systemPS >= 0)
                r += (SystemPS, wr.Fix(systemPS));
            r += (TableChecks, wr.Fix(tableChecks));
            r += (Triggers, wr.Fix(triggers));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r =(Table) base.Fix(cx);
            r += (_Domain, domain.Fix(cx));
            if (applicationPS >= 0)
                r += (ApplicationPS, cx.obuids[applicationPS]??applicationPS);
            r += (Indexes, cx.Fix(indexes));
            r += (TableCols, cx.Fix(tblCols));
            if (systemPS >= 0)
                r += (SystemPS, cx.obuids[systemPS]??systemPS);
            r += (TableChecks, cx.Fix(tableChecks));
            r += (Triggers, cx.Fix(triggers));
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
            r = (Table)New(cx,r.mem);
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
            var dt = data.domain;
            var st = (dt != f.domain) ? dt : null; // subtype
            var sp = cx.db.types[st];
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
            var trs = new TransitionRowSet(cx, f, data, PTrigger.TrigType.Insert, eqs);
            //       var ckc = new ConstraintChecking(tr, trs, this);
            // Do statement-level triggers
            bool? fi = trs.InsertSB(cx);
            if (fi!=true) // no insteadof has fired
            {
                for (var trb = trs.First(cx) as TransitionRowSet.TransitionCursor; 
                    trb != null; trb = trb.Next(cx) as TransitionRowSet.TransitionCursor) // trb constructor checks for autokey
                {
                    // Do row-level triggers
                    fi = trb.InsertRB(cx);
                    if (fi==true) // an insteadof trigger has fired
                        continue;
                    trb = (TransitionRowSet.TransitionCursor) cx.cursors[trs.defpos];
                    Record r;
                    var np = cx.db.nextPos;
                    if (cl != Level.D)
                        r = new Record3(this,trb.targetRow.values, sp, cl, np, cx);
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
                    fi = trb.InsertRA(cx);
                    trb = (TransitionRowSet.TransitionCursor)cx.cursors[trs.defpos];
                }
            }
            // Statement-level after triggers
            trs.InsertSA(cx);
            cx.result = null;
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
                    if (d.value().defpos != c.value())
                        goto skip;
                return ix;
                    skip:;
            }
            return null;
        }
        internal Index FindIndex(Database db, CList<long> key)
        {
            return (Index)db.objects[indexes[key]];
        }
        internal override void RowSets(Context cx, From f,BTree<long,RowSet.Finder> fi)
        {
            // ReadConstraints only apply in explicit transactions 
            ReadConstraint readC = cx.db.autoCommit ? null
                : cx.db._ReadConstraint(cx, this);
            // At this point we have a table/alias, 
            // we want to find the Index best meeting our needs
            // Score: Add 10^n for n ordType cols occurring in order, 
            // add n+1 for each filter column occurring in position k-n 
            // in an index with k cols.
            // Find the index with highest score, then set up
            // the match Link for it
            /*     if (periods.Contains(target.defpos))
                 {
                     // a periodspecification has been supplied for this table.
                     var ps = periods[target.defpos];
                     if (ps.kind == Sqlx.NO)
                     {
                         // simply use the appropriate time versioning index for this table
                         var tb = (Table)target;
                         var pp = (ps.periodname == "SYSTEM_TIME") ? tb.systemPS : tb.applicationPS;
                         var pd = (PeriodDef)tr.objects[pp];
                         if (pd != null)
                             index = tb.indexes[pp]; // I doubt this
                     }
                 } 
                 else */

            int matches = 0;
            PRow match = null;
            Index index = null;
            int bs = 0;      // score for best index
            for (var p = indexes.First(); p != null; p = p.Next())
            {
                var x = (Index)cx.db.objects[p.value()];
                if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey
                    || x.tabledefpos != defpos)
                    continue;
                var dt = (ObInfo)cx.db.role.infos[x.defpos];
                int sc = 0;
                int nm = 0;
                int n = 0;
                PRow pr = null;
                var havematch = false;
                int sb = 1;
                var j = dt.domain.Length - 1;
                for (var b = dt.domain.rowType.Last(); b != null; b = b.Previous(), j--)
                {
                    var c = b.value();
                    for (var fd = f.filter.First(); fd != null; fd = fd.Next())
                    {
                        if (cx.obs[fd.key()] is SqlCopy co
                            && co.copyFrom == c)
                        {
                            sc += 9 - j;
                            nm++;
                            pr = new PRow(fd.value(), pr);
                            havematch = true;
                            goto nextj;
                        }
                    }
                    if (n < f.ordSpec.Length)
                    {
                        var ok = f.ordSpec[n];
                        if (ok != -1L)
                        {
                            n++;
                            sb *= 10;
                        }
                    }
                    pr = new PRow(TNull.Value, pr);
                nextj:;
                }
                if (!havematch)
                    pr = null;
                sc += sb;
                if (sc > bs)
                {
                    index = x;
                    matches = nm;
                    match = pr;
                    bs = sc;
                }
            }
            RowSet rowSet;
            if (index != null && index.rows != null)
            {
                var sce = (match == null) ? new IndexRowSet(cx, this, index, fi, f.filter)
                            : new FilterRowSet(cx, this, index, match, fi);
                rowSet = new SelectedRowSet(cx, f, sce, fi);
                if (readC != null)
                {
                    if (matches == index.keys.Length &&
                        (index.flags & (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique))
                            != PIndex.ConstraintType.NoType)
                        readC.Singleton(index, match);
                    else
                        readC.Block();
                }
            }
            else
            {
                index = FindPrimaryIndex(cx.db);
                RowSet sa;
                if (index != null && index.rows != null)
                    sa = new IndexRowSet(cx, this, index, fi,
                        cx.Filter(this, f.where));
                else
                    sa = new TableRowSet(cx, defpos, fi, f.where);
                Audit(cx, sa, f);
                rowSet = new SelectedRowSet(cx, f, sa, fi);
                if (readC != null)
                    readC.Block();
            }
            cx.data += (f.defpos, rowSet);
            cx.results += (f.defpos, rowSet.defpos);
            if (readC != null)
                cx.rdC += (defpos, readC);
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
            if (Denied(cx, Grant.Privilege.Delete) ||
                (enforcement.HasFlag(Grant.Privilege.Insert) &&
                cx.db.user.clearance.minLevel > 0 &&
                (cx.db.user.clearance.minLevel != f.classification.minLevel ||
                cx.db.user.clearance.maxLevel != f.classification.maxLevel)))
                throw new DBException("42105", ((ObInfo)cx.db.role.infos[defpos]).name);
            var data = f.RowSets(cx, cx.data[f.from]?.finder??BTree<long, RowSet.Finder>.Empty); 
            var trs = new TransitionRowSet(cx, f, data, PTrigger.TrigType.Delete, eqs);
            var cl = cx.db.user.clearance;
            cx.from += trs.finder;
            bool? fi = trs.DeleteSB(cx);
            if (fi!=true)
                for (var trb = trs.First(cx) as TransitionRowSet.TransitionCursor; trb != null;
                    trb = trb.Next(cx) as TransitionRowSet.TransitionCursor)
                {
                    //          if (ds.Count > 0 && !ds.Contains(trb.Rvv()))
                    //            continue;
                    fi = trb.DeleteRB(cx);
                    if (fi==true)
                        continue;
                    trb = (TransitionRowSet.TransitionCursor)cx.cursors[trs.defpos];
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
            cx.result = null;
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
            if (f.assig.Count==0)
                return cx;
            if (Denied(cx, Grant.Privilege.Update))
                throw new DBException("42105", ((ObInfo)cx.db.role.infos[defpos]).name);
            var trs = new TransitionRowSet(cx, f, cx.data[f.defpos], PTrigger.TrigType.Update, eqs);
            var updates = BTree<long, UpdateAssignment>.Empty;
            SqlValue level = null; // Only the SA can modify the classification
            for (var ass = f.assig.First(); ass != null; ass = ass.Next())
                if (cx.obs[ass.key().vbl] is SqlSecurity)
                    level = cx.obs[ass.key().val] as SqlValue;
                else
                {
                    var c = cx.obs[ass.key().vbl] as SqlCopy
                        ?? throw new DBException("0U000");
                    var tc = cx.db.objects[c.copyFrom] as TableColumn ??
                        throw new DBException("42112", c.name);
                    if (tc.generated != GenerationRule.None)
                        throw cx.db.Exception("0U000", c.name).Mix();
                    if (c.Denied(cx, Grant.Privilege.Update))
                        throw new DBException("42105", c.name);
                    updates += (tc.defpos, ass.key());
                }
      //      bool nodata = true;
            var cl = cx.db.user?.clearance??Level.D;
            cx.from += trs.finder;
            if ((level != null || updates.Count > 0))
            {
                var fi = trs.UpdateSB(cx);
                if (fi!=true)
                    for (var trb = trs.First(cx) as TransitionRowSet.TransitionCursor;
                        trb != null; trb = trb.Next(cx) as TransitionRowSet.TransitionCursor)
                    {
                        for (var b=updates.First();b!=null;b=b.Next())
                        {
                            var ua = b.value();
                            var tv = cx.obs[ua.val].Eval(cx).NotNull();
                            if (tv == TNull.Value && cx.obs[ua.vbl] is TableColumn tc
                                && tc.notNull)
                                throw new DBException("0U000", cx.Inf(ua.vbl).name);
                            trb += (cx, ua.vbl, tv);
                        }
                        fi = trb.UpdateRB(cx);
                        if (fi==true) // an insteadof trigger has fired
                            continue;
                        trb = (TransitionRowSet.TransitionCursor)cx.cursors[trs.defpos];
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
                            new Update1(rc, this, trb.targetRow.values, 
                                (Level)level.Eval(cx).Val(), np, cx);
                        cx.Add(u);
                        var nr = new TableRow(u, cx.db);
                        var ns = cx.newTables[trs.defpos] ?? BTree<long, TableRow>.Empty;
                        cx.newTables += (trs.defpos,  ns + (nr.defpos, nr));
                        cx.tr.FixTriggeredActions(triggers, trs._tgt, u.ppos);
                        trb.UpdateRA(cx);
                        trb = (TransitionRowSet.TransitionCursor)cx.cursors[trs.defpos];
                        //          cx.affected += new Rvv(defpos, u.defpos, tr.loadpos);
                    }
            }
            trs.UpdateSA(cx);
            rs.Add(trs); // just for PUT
            cx.result = null; //??
            return cx;
        }
        public override bool Denied(Context cx, Grant.Privilege priv)
        { 
            if (cx.db.user != null && enforcement.HasFlag(priv) &&
                !(cx.db.user.defpos == cx.db.owner
                    || cx.db.user.clearance.ClearanceAllows(classification)))
                return true;
            return base.Denied(cx, priv);
        }
        /// <summary>
        /// A readable version of the Table
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" "); sb.Append(domain);
            if (mem.Contains(Enforcement)) { sb.Append(" Enforcement="); sb.Append(enforcement); }
            if (indexes.Count!=0) 
            { 
                sb.Append(" Indexes:(");
                var cm = "";
                for (var b=indexes.First();b!=null;b=b.Next())
                {
                    sb.Append(cm);cm = ",";
                    var cn = "(";
                    for (var c=b.key().First();c!=null;c=c.Next())
                    {
                        sb.Append(cn);cn = ",";
                        sb.Append(Uid(c.value()));
                    }
                    sb.Append(")"); sb.Append(Uid(b.value()));
                }
                sb.Append(")");
            }
            if (triggers.Count!=0) { sb.Append(" Triggers:"); sb.Append(triggers); }
            sb.Append(" KeyCols: "); sb.Append(keyCols);
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
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j = (int)ix.keys.Count;
                    for (j = 0; j < ix.keys.Count; j++)
                        if (ix.keys[j] == p)
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
            for(var b = rt.domain.rowType.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var cd = rt.domain.representation[b.value()];
                var dt = cd;
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j = (int)ix.keys.Count;
                    for (j = 0; j < ix.keys.Count; j++)
                        if (ix.keys[j] == p)
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
                var di = (ObInfo)tr.role.infos[p];
                var tn = (dt.kind == Sqlx.TYPE) ? di.name : dt.SystemType.Name;
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
                    var se = ix.keys[i];
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
