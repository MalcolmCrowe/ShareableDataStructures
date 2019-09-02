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
    internal class Table : Query
    {
        internal const long
            Assigns = -265, // BList<UpdateAssignment>
            ChartFormat = -266, // Sqlx (T)
            ClientFormat = -267, // Sqlx (T)
            Enforcement = -268, // Grant.Privilege (T)
            Entity = -269, // true (T)
            Indexes = -270, // BTree<long,Index> (T) supplied every time by schemaRole
            Legend = -271, // string (T)
            PeriodDef = -272, // PeriodDef
            Rows = -273, // BTree<long,TableRow> (T) supplied every time by schemaRole
            SystemTime = -274, // PeriodDef (T)
            TableChecks = -275, // BTree<long,Check> (T)
            TableProperties = -276, //BTree<string,DBObject> (T)
            Triggers = -277,// BTree<PTrigger.TrigType,ATree<long,Trigger>> (T) 
            Versions = -278; // BTree<long,Period> (T) supplied by schemaRole: ppos->Period
        internal BList<UpdateAssignment> assigns =>
             (BList<UpdateAssignment>)mem[Assigns] ?? BList<UpdateAssignment>.Empty;
        /// <summary>
        /// The rows of the table with the latest version for each
        /// </summary>
		public BTree<long, TableRow> tableRows => 
            (BTree<long,TableRow>)mem[Rows]??BTree<long,TableRow>.Empty;
        public BTree<long, Index> indexes => 
            (BTree<long,Index>)mem[Indexes]??BTree<long,Index>.Empty;
        /// <summary>
        /// If versioning, then this will be the SYSTEM_TIME periodDef
        /// </summary>
        internal PeriodDef systemTime => (PeriodDef)mem[SystemTime];
        internal Sqlx clientFormat => (Sqlx)(mem[ClientFormat] ?? Sqlx.NO);
        internal Sqlx chartFormat => (Sqlx)(mem[ChartFormat] ?? Sqlx.NO);
        internal bool entity => (bool)(mem[Entity] ?? false);
        /// <summary>
        /// Enforcement of clearance rules
        /// </summary>
        internal Grant.Privilege enforcement => (Grant.Privilege)mem[Enforcement];
        internal BTree<long, Period> versionedRows => 
            (BTree<long, Period>)mem[Versions]??BTree<long,Period>.Empty;
        internal BTree<long, Check> tableChecks => 
            (BTree<long, Check>)mem[TableChecks]??BTree<long,Check>.Empty;
        internal BTree<string, DBObject> tableProperties => 
            (BTree<string, DBObject>)mem[TableProperties]??BTree<string,DBObject>.Empty;
        internal PeriodDef periodDef => (PeriodDef)mem[PeriodDef];
        internal BTree<PTrigger.TrigType, BTree<long, Trigger>> triggers =>
            (BTree<PTrigger.TrigType, BTree<long, Trigger>>)mem[Triggers]
            ??BTree<PTrigger.TrigType, BTree<long, Trigger>>.Empty;
        internal readonly static Table _static = new Table();
        Table() : base(Static, Domain.Content) { }
        /// <summary>
        /// Constructor: a new empty table
        /// </summary>
        internal Table(PTable pt) :base(pt.ppos, BTree<long,object>.Empty
            +(Name,pt.name)+(Definer,pt.role.defpos)
            +(SqlValue.NominalType, new Domain(BList<Selector>.Empty))
            +(Rows,BTree<long,TableRow>.Empty)+(Indexes,BTree<long,Index>.Empty)
            +(Triggers, BTree<PTrigger.TrigType, BTree<long, Trigger>>.Empty)
            +(Enforcement,(Grant.Privilege)15) //read|insert|update|delete
            +(Versions,BTree<long,Period>.Empty)+(TableProperties,BTree<string,DBObject>.Empty)) 
        {}
        protected Table(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Table operator+(Table tb,TableColumn tc)
        {
            var rt = tb.rowType + tc;
            var ds = tb.dependents + tc.defpos;
            var dp = _Max(tb.depth, 1 + tc.depth);
            return (Table)tb.New(tb.mem+(SqlValue.NominalType, rt)
                +(Display,(int)rt.columns.Count)
                +(Dependents,ds)+(Depth,dp));
        }
        public static Table operator+(Table tb,Metadata md)
        {
            var m = tb.mem;
            if (md.Has(Sqlx.ENTITY)) m += (Entity, true); 
            if (md.Has(Sqlx.HISTOGRAM)) m += (ChartFormat, Sqlx.HISTOGRAM);
            else if (md.Has(Sqlx.PIE)) m += (ChartFormat, Sqlx.PIE);
            else if (md.Has(Sqlx.POINTS)) m += (ChartFormat, Sqlx.POINTS);
            else if (md.Has(Sqlx.LINE)) m += (ChartFormat, Sqlx.LINE);
            if (md.Has(Sqlx.JSON)) m += (ClientFormat, Sqlx.JSON);
            else if (md.Has(Sqlx.CSV)) m += (ClientFormat, Sqlx.CSV);
            else if (md.Has(Sqlx.XML)) m += (ClientFormat, Sqlx.XML);
            else if (md.Has(Sqlx.SQL)) m += (ClientFormat, Sqlx.SQL);
            if (md.description != "") m += (Description, md.description); 
            return new Table(tb.defpos, m);
        }
        internal override Metadata Meta()
        {
            var md = new Metadata();
            if (entity)
                md.Add(Sqlx.ENTITY);
            md.Add(chartFormat);
            md.Add(clientFormat);
            return md;
        }
        public static Table operator +(Table schema, TableRow row)
        {
            var rs = schema.tableRows + (row.defpos, row);
            var xs = schema.indexes;
            for (var b = xs.First(); b != null; b = b.Next())
            {
                var x = b.value();
                if (row.prevKeys?[x.defpos] is PRow key)
                    x -= key;
                xs += (x.defpos,x + row);
            }
            var se = schema.sensitive || row.classification!=Level.D;
            var vs = schema.versionedRows;
            if (schema.systemTime!=null)
            {
                var tm = schema.systemTime.domain.Coerce(new TDateTime(Domain.Timestamp, new DateTime(row.time)));
                vs += (row.ppos, new Period(tm,Domain.MaxDate));
                if (row.prev!=-1)
                    vs += (row.prev, new Period(vs[row.prev].start, tm));
            }
            return new Table(schema.defpos, schema.mem + (Indexes, xs) + (Rows,rs) 
                + (Sensitive,se) + (Versions,vs));
        }
        public static Table operator+(Table tb,(long,object)v)
        {
            return new Table(tb.defpos, tb.mem + v);
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new Table(defpos,mem+(TableChecks,tableChecks+(ck.defpos,ck)));
        }
        internal override DBObject AddProperty(Check ck, Database db)
        {
            return new Table(defpos,mem+(TableProperties,tableProperties+(ck.name,ck)));
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
        internal override BTree<long,DBObject> Add(PMetadata p,Database db)
        {
            var m = mem;
            if (p.Has(Sqlx.CSV)) m += (ClientFormat, Sqlx.CSV);
            if (p.Has(Sqlx.ENTITY)) m += (Entity, true);
            if (p.Has(Sqlx.HISTOGRAM)) m += (ClientFormat, Sqlx.HISTOGRAM);
            if (p.Has(Sqlx.JSON)) m += (ClientFormat, Sqlx.JSON);
            if (p.Has(Sqlx.LINE)) m += (ChartFormat, Sqlx.LINE);
            if (p.Has(Sqlx.PIE)) m += (ChartFormat, Sqlx.PIE);
            if (p.Has(Sqlx.POINTS)) m += (ChartFormat, Sqlx.POINTS);
            var t = new Table(defpos, m);
            return new BTree<long, DBObject>(t.defpos, t);
        }

        internal override void Resolve(Context cx)
        {
            var ti = new Ident(name, defpos);
            var ai = new Ident(rowType.name, defpos);
            for (var b=rowType.columns.First();b!=null;b=b.Next())
            {
                var sc = b.value();
                var ci = new Ident(sc.name, 0);
                if (cx.defs[ci] is SqlValue t && t.nominalDataType == Domain.Null)
                    cx.Replace(t, sc);
                var cj = new Ident(ti, ci);
                if (cx.defs[cj] is SqlValue u && u.nominalDataType == Domain.Null)
                    cx.Replace(u, sc);
                var ck = new Ident(ai, ci);
                if (cx.defs[ck] is SqlValue v && v.nominalDataType == Domain.Null)
                    cx.Replace(v, sc);
            }
        }
        /// <summary>
        /// Execute an Insert on the table including trigger operation.
        /// </summary>
        /// <param name="f">The Insert</param>
        /// <param name="prov">The provenance</param>
        /// <param name="data">The insert data may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">The target rowset may be explicit</param>
        internal override Transaction Insert(Transaction tr, Context _cx, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl, bool autokey = false)
        {
            int count = 0;
            if (Denied(tr, Grant.Privilege.Insert))
                throw new DBException("42105", name);
            long st = 0;
            var ot = data.rowType;
            if (rowType.Length == ot.Length && (!rowType.Equals(ot))
                && rowType.EqualOrStrongSubtypeOf(ot))
                st = rowType.defpos;
            // parameter cl is only supplied when d_User.defpos==d.owner
            // otherwise check if we should compute it
            if (tr.user.defpos != tr.owner && enforcement.HasFlag(Grant.Privilege.Insert))
            {
                var uc = tr.user.clearance;
                if (!uc.ClearanceAllows(classification))
                    throw new DBException("42105",name);
                // The new record’s classification will have the user’s minimum clearance level:
                // if this is above D, the groups will be the subset of the user’s groups 
                // that are in the table classification, 
                // and the references will be the same as the table 
                // (a subset of the user’s references)
                cl = uc.ForInsert(classification);
            }
            var trs = new TransitionRowSet(tr, _cx, this, PTrigger.TrigType.Insert, eqs, autokey);
            //       var ckc = new ConstraintChecking(tr, trs, this);
            // Do statement-level triggers
            tr = trs.InsertSA(tr, _cx);
            if (_cx.ret!=TBool.True)
            {
                var nr = tr.nextTid;
                for (var trb = trs.First(_cx); trb != null; trb = trb.Next(_cx)) // trb constructor checks for autokey
                {
                    var _trb = trb as TransitionRowSet.TransitionRowBookmark;
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

        internal Index FindPrimaryIndex()
        {
            for (var b=indexes.First();b!=null;b=b.Next())
            {
                var ix = b.value();
                if (ix.flags.HasFlag(PIndex.ConstraintType.PrimaryKey))
                    return ix;
            }
            return null;
        }
        internal Index FindIndex(BList<Selector> key)
        {
            for (var b = indexes.First(); b != null; b = b.Next())
            {
                var ix = b.value();
                if (ix.cols.Count != key.Count)
                    continue;
                var c = ix.cols.First();
                for (var d = key.First(); d != null && c != null; d = d.Next(), c = c.Next())
                    if (d.value().defpos != c.value().defpos)
                        goto skip;
                return ix;
                    skip:;
            }
            return null;
        }
        /// <summary>
        /// Execute a Delete on a Table, including triggers
        /// </summary>
        /// <param name="f">The Delete operation</param>
        /// <param name="ds">A set of delete strings may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override Transaction Delete(Transaction tr, Context cx, BTree<string, bool> ds, Adapters eqs)
        {
            var count = 0;
            if (Denied(tr, Grant.Privilege.Delete))
                throw new DBException("42105", name);
            var trs = new TransitionRowSet(tr, cx, this, PTrigger.TrigType.Delete, eqs, false);
            var cl = tr.user.clearance;
            tr = trs.DeleteSB(tr, cx);
            var nr = tr.nextTid;
            cx.rb = RowSets(tr, cx).First(cx);
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
                tr += new Delete(rec, nr++, tr);
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
        internal override Transaction Update(Transaction tr,Context cx,BTree<string, bool> ur, 
            Adapters eqs,List<RowSet>rs)
        {
            if (assigns == null)
                return tr;
            if (Denied(tr, Grant.Privilege.Insert))
                throw new DBException("42105", name);
            var trs = new TransitionRowSet(tr, cx, this, PTrigger.TrigType.Update, eqs, false);
            var updates = BTree<long, UpdateAssignment>.Empty;
            SqlValue level = null;
            for (var ass=assigns.First();ass!=null;ass=ass.Next())
            {
                var c = ass.value().vbl as TableColumn ??
                    throw new DBException("42112", ass.value().vbl.name);
                if (!cx.obs.Contains(c.defpos)) // can happen with updatable joins
                    continue;
                if (c.generated != PColumn.GenerationRule.No)
                    throw tr.Exception("0U000", c.name).Mix();
                if (c.Denied(tr, Grant.Privilege.Insert))
                    throw new DBException("42105", name);
                updates += (c.defpos, ass.value());
            }
      //      bool nodata = true;
            var cl = tr.user.clearance;
            cx.rb = RowSets(tr, cx).First(cx);
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
                        var dt = ua.vbl.nominalDataType;
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
        /// Optimise retrievals
        /// </summary>
        /// <param name="rec"></param>
        /// <returns></returns>
        internal bool CheckMatch(Transaction tr, Context cx, TableRow rec)
        {
            if (rec != null)
                for (var e = matches?.First(); e != null; e = e.Next())
                {
                    var v = rec.fields[e.key().defpos];
                    var m = e.value();
                    if (v != null && m != null && m.dataType.Compare(m, v) != 0)
                        return false;
                }
            return true;
        }
        internal override RowSet RowSets(Transaction tr, Context cx)
        {
            if (defpos == Query.Static)
                return new TrivialRowSet(tr, cx, this, new TRow(rowType,cx.values));
            RowSet rowSet = null;
 //           if (target == null)
 //               return new TrivialRowSet(tr, cx, this, Eval(tr, cx) as TRow ?? TRow.Empty);
            //         if (target is View vw)
            //             return vw.RowSets(tr, cx, this);
            ReadConstraint readC = tr._ReadConstraint(cx, target);
            int matches = 0;
            PRow match = null;
            Index index = null;
            // At this point we have a table/alias, 
            // we want to find the Index best meeting our needs
            // Score: Add 10^n for n ordType cols occurring in order, 
            // add n+1 for each filter column occurring in position k-n 
            // in an index with k cols.
            // Find the index with highest score, then set up
            // the match Link for it
            if (periods.Contains(target.defpos))
            {
                // a periodspecification has been supplied for this table.
                var ps = periods[target.defpos];
                if (ps.kind == Sqlx.NO)
                {
                    // simply use the appropriate time versioning index for this table
                    var tb = (Table)target;
                    var ix = (ps.periodname == "SYSTEM_TIME") ? Sqlx.SYSTEM_TIME : Sqlx.APPLICATION;
                    var pd = tb.FindPeriodDef(ix);
                    if (pd != null)
                        index = tb.indexes[pd.indexdefpos];
                }
            }
            else
            {
                int bs = 0;      // score for best index
                for (var p = indexes.First(); p != null; p = p.Next())
                {
                    var x = p.value();
                    if (x == null || x.flags != PIndex.ConstraintType.PrimaryKey || x.tabledefpos != target.defpos)
                        continue;
                    var dt = x.keyType;
                    int sc = 0;
                    int nm = 0;
                    int n = 0;
                    PRow pr = null;
                    int sb = 1;
                    for (int j = (int)x.cols.Count - 1; j >= 0; j--)
                    {
                        /*                  for (var fd = filter.First(); fd != null; fd = fd.Next())
                                          {
                                              if (x.cols[j] == fd.key())
                                              {
                                                  sc += 9 - j;
                                                  nm++;
                                                  pr = new PRow(fd.value(), pr);
                                                  goto nextj;
                                              }
                                          } */
                        var ob = x.cols[j];
                        if (ordSpec != null && n < ordSpec.items.Count)
                        {
                            var ok = ordSpec.items[n];
                            var sr = ValFor(ok.what);
                            if (ok != null && ok.what.MatchExpr(this, sr))
                            {
                                n++;
                                sb *= 10;
                            }
                        }
                        pr = new PRow(TNull.Value, pr);
                        //           nextj:;
                    }
                    sc += sb;
                    if (sc > bs)
                    {
                        index = x;
                        matches = nm;
                        match = pr;
                        bs = sc;
                    }
                }
            }
            var svo = ordSpec;
            if (index != null && index.rows != null)
            {
                rowSet = new IndexRowSet(tr, cx, this, index, match);
                if (readC != null)
                {
                    if (matches == (int)index.cols.Count &&
                        (index.flags & (PIndex.ConstraintType.PrimaryKey | PIndex.ConstraintType.Unique)) != PIndex.ConstraintType.NoType)
                        readC.Singleton(index, match);
                    else
                        readC.Block();
                }
            }
            else
            {
                if (target is Table tb)
                {
                    if (tb.tableRows != null)
                        rowSet = new TableRowSet(tr, cx, this);
                    else
                    {
                        index = tb.FindPrimaryIndex();
                        if (index != null && index.rows != null)
                            rowSet = new IndexRowSet(tr, cx, this, index, null);
                    }
                }
                if (readC != null)
                    readC.Block();
            }
            return rowSet;
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
        /// Build a period window
        /// </summary>
        /// <param name="fm">The query</param>
        /// <param name="ps">The period spec</param>
        /// <returns>The ATree of row positions</returns>
        public BTree<long, bool> Build(Transaction tr, Context cx, Table fm, PeriodSpec ps)
        {
            var r = BTree<long, bool>.Empty;
            for (var e = versionedRows.First(); e != null; e = e.Next())
            {
                var ts = e.value();
                var dt = ts.start.dataType;
                var time1 = ps.time1.Eval(tr, cx);
                var time2 = ps.time2.Eval(tr, cx);
                switch (ps.kind)
                {
                    case Sqlx.AS: if (!(dt.Compare(ts.start,time1) <= 0 
                            && dt.Compare(ts.end, time1) > 0)) continue; break;
                    case Sqlx.BETWEEN: if (!(dt.Compare(ts.start, time2) <= 0 
                            && dt.Compare(ts.end, time1) > 0)) continue; break;
                    case Sqlx.FROM: if (!(dt.Compare(ts.start, time2) < 0 
                            && dt.Compare(ts.end, time1) > 0)) continue; break;
                }
                r +=(e.key(), true);
            }
            return r;
        }
        /// <summary>
        /// Accessor: get a period definition for this table
        /// </summary>
        /// <param name="db">The database</param>
        /// <param name="ix">APPLICATION or SYSTEM</param>
        /// <returns>the period definition or null</returns>
        internal PeriodDef FindPeriodDef(Sqlx ix)
        {
            for (var c = rowType.columns.First(); c != null; c = c.Next())
                    if (c.value() is PeriodDef pd && pd.versionKind == ix)
                        return pd;
            return null;
        }
        /// <summary>
        /// Accessor: Check a new table check constraint
        /// </summary>
        /// <param name="tr">Transaction</param>
        /// <param name="c">The new Check constraint</param>
        internal void TableCheck(Transaction tr,PCheck c)
        {
            var cx = new Context(tr);
            var f = this;
            SqlValue bt = new Parser(tr).ParseSqlValue(c.check, Domain.Bool);
            bt = SqlValue.Setup(tr, cx, f, bt, Domain.Bool);
            f += (Where,bt);
            if (f.RowSets(tr,cx).First(cx) != null)
                throw new DBException("44000", c.check).ISO();
        }
        internal override SqlValue ValFor(SqlValue sv)
        {
            return sv;
        }
        /// <summary>
        /// A readable version of the Table
        /// </summary>
        /// <returns>the string representation</returns>
        public string ToString1()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(ClientFormat)) { sb.Append(" ClientFormat:"); sb.Append(clientFormat); }
            if (mem.Contains(Enforcement)) { sb.Append(" Enforcement="); sb.Append(enforcement); }
            if (mem.Contains(Entity)) sb.Append(" Entity");
            if (mem.Contains(ChartFormat)) { sb.Append(" "); sb.Append(chartFormat); }
            if (mem.Contains(Legend)) sb.Append(" Legend");
            if (mem.Contains(Rows)) { sb.Append(" Rows:"); sb.Append(tableRows); }
            if (mem.Contains(Indexes)) { sb.Append(" Indexes:"); sb.Append(indexes); }
            if (mem.Contains(SystemTime)) { sb.Append(" SystemTime="); sb.Append(systemTime); }
            if (mem.Contains(Triggers)) { sb.Append(" Triggers:"); sb.Append(triggers); }
            if (mem.Contains(Versions)) { sb.Append(" Versioned:"); sb.Append(versionedRows); }
            return sb.ToString();
        }
        public override string ToString()
        {
            return "Table " +name+" "+ base.ToString();
        }
        /// <summary>
        /// Generate a row for the Role$Class table: includes a C# class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal TRow RoleClassValue(Transaction tr, Table from, ABookmark<long, DBObject> _enu)
        {
            var md = _enu.value();
            var ro = tr.role;
            var versioned = true;
            var key = BuildKey(tr, out Index ix);
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n[Schema("); sb.Append(from.ppos); sb.Append(")]");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + md.name + " from Database " + tr.name + ", Role " + ro.name + "\r\n");
            if (md.desc != "")
                sb.Append("/// " + md.desc + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + md.name + ((versioned) ? " : Versioned" : "") + " {\r\n");
            for (var b = rowType.columns.First();b!=null;b=b.Next())
            {
                var cv = b.value();
                var dt = cv.domain;
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j = (int)ix.cols.Count;
                    for (j = 0; j < ix.cols.Count; j++)
                        if (ix.cols[j].defpos == cv.defpos)
                            break;
                    if (j < ix.cols.Count)
                        sb.Append("  [Key(" + j + ")]\r\n");
                }
                FieldType(sb, dt);
                sb.Append("  public " + tn + " " + cv.name + ";");
                sb.Append("\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(new TColumn("Name",new TChar(md.name)),
                new TColumn("Key",new TChar(key)),
                new TColumn("Def",new TChar(sb.ToString())));
        }
        /// <summary>
        /// Generate a row for the Role$Java table: includes a Java class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RoleJavaValue(Transaction tr, Table from, 
            ABookmark<long, object> _enu)
        {
            var md = (DBObject)_enu.value();
            var ro = tr.role;
            var versioned = true;
            var sb = new StringBuilder();
            sb.Append("/*\r\n * "); sb.Append(md.name); sb.Append(".java\r\n *\r\n * Created on ");
            sb.Append(DateTime.Now);
            sb.Append("\r\n * from Database " + tr.name + ", Role " + tr.role.name + "r\n */");
            sb.Append("import org.pyrrhodb.*;\r\n");
            var key = BuildKey(tr,out Index ix);
            sb.Append("\r\n@Schema("); sb.Append(from.ppos); sb.Append(")");
            sb.Append("\r\n/**\r\n *\r\n * @author "); sb.Append(tr.user.name); sb.Append("\r\n */");
            if (md.desc != "")
                sb.Append("/* " + md.desc + "*/\r\n");
            sb.Append("public class " + md.name + ((versioned) ? " extends Versioned" : "") + " {\r\n");
            for(var b = rowType.columns.First();b!=null;b=b.Next())
            {
                var cd = b.value();
                var dt = cd.domain;
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                if (ix != null)
                {
                    int j = (int)ix.cols.Count;
                    for (j = 0; j < ix.cols.Count; j++)
                        if (ix.cols[j].defpos == cd.defpos)
                            break;
                    if (j < ix.cols.Count)
                        sb.Append("  @Key(" + j + ")\r\n");
                }
                FieldJava(tr, sb, dt);
                sb.Append("  public " + tn + " " + cd.name + ";");
                sb.Append("\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(new TColumn("Name",new TChar(md.name)),
                new TColumn("Key",new TChar(key)),
                new TColumn("Def",new TChar(sb.ToString())));
        }
        /// <summary>
        /// Generate a row for the Role$Python table: includes a Python class definition
        /// </summary>
        /// <param name="from">The query</param>
        /// <param name="_enu">The object enumerator</param>
        /// <returns></returns>
        internal override TRow RolePythonValue(Transaction tr, Table from, ABookmark<long, object> _enu)
        {
            var md = (Table)_enu.value();
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
            for(var b = rowType.columns.First();b!=null;b=b.Next())
            {
                var cd = b.value();
                var dt = cd.domain;
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                sb.Append("  self." + cd.name + " = " + dt.defaultValue);
                sb.Append("\r\n");
            }
            sb.Append("  self._schemakey = "); sb.Append(from.ppos); sb.Append("\r\n");
            if (ix!=null)
            {
                var comma = "";
                sb.Append("  self._key = ["); 
                for (var i=0;i<ix.cols.Count;i++)
                {
                    var tc = tr.role.objects[ix.cols[i].defpos]as TableColumn;
                    sb.Append(comma); comma = ",";
                    sb.Append("'");  sb.Append(tc.name); sb.Append("'");
                }
                sb.Append("]\r\n");
            }
            return new TRow(new TColumn("Name", new TChar(md.name)),
                new TColumn("Key",new TChar(key)),
                new TColumn("Def",new TChar(sb.ToString())));
        }
        string BuildKey(Database db,out Index ix)
        {
            ix = null;
            for (var xk = indexes.First(); xk != null; xk = xk.Next())
            {
                var x = db.role.objects[xk.key()] as Index;
                if (x.tabledefpos != defpos)
                    continue;
                if ((x.flags & PIndex.ConstraintType.PrimaryKey) == PIndex.ConstraintType.PrimaryKey)
                    ix = x;
            }
            var comma = "";
            var sk = new StringBuilder();
            if (ix != null)
                for(var i =0;i<(int)ix.cols.Count;i++)
                {
                    var cp = ix.cols[i].defpos;
                    var cd = db.role.objects[cp] as TableColumn;
                    if (cd != null)
                    {
                        sk.Append(comma);
                        comma = ",";
                        sk.Append(cd.name);
                    }
                }
            return sk.ToString();
        }
    }
    /// <summary>
    /// A cursor for a non-indexed table.
    /// Immutable
    /// </summary>
    internal class TableCursor
    {
        internal Context cnx;
        internal readonly Database _db;
        internal readonly Table _table;
        internal readonly BTree<long, TypedValue> _match;
        internal readonly Record _rec;
        /// <summary>
        /// Constructor: a cursor at a given position
        /// </summary>
        /// <param name="db">The database</param>
        /// <param name="table">The table</param>
        /// <param name="match">A low-level selection</param>
        /// <param name="rec">a record</param>
        TableCursor(Context cx, Database db, Table table, BTree<long, TypedValue> match, Record rec = null)
        {
            cnx = cx; _db = db; _table = table; _match = match; _rec = rec;
        }
        /// <summary>
        /// Find a match for a given set of criteria
        /// </summary>
        /// <param name="_db">The database</param>
        /// <param name="_match">The match criteria</param>
        /// <param name="bmk">A bookmark to adjust</param>
        /// <returns>The record found or null</returns>
        static Record MoveToMatch(Context cnx, Database _db, BTree<long, TypedValue> _match, 
            ref ABookmark<long, TableRow> bmk)
        {
            for (; bmk != null; bmk = bmk.Next())
            {
                var r = _db.GetD(bmk.key()) as Record;
                for (var m = _match.First(); m != null; m = m.Next())
                    if (m.value().CompareTo(r.fields[m.key()]) != 0)
                        goto next;
                return r;
            next:;
            }
            return null;
        }
        /// <summary>
        /// Get the next cursor
        /// </summary>
        /// <returns>the new tablecursor</returns>
        public TableCursor Next()
        {
            var bmk = _table.tableRows.PositionAt(_rec.defpos)?.Next();
            var r = MoveToMatch(cnx, _db, _match, ref bmk);
            if (r == null)
                return null;
            return new TableCursor(cnx, _db, _table, _match, r);
        }
        /// <summary>
        /// Get the first tablecursor
        /// </summary>
        /// <param name="db">the database</param>
        /// <param name="tb">the table</param>
        /// <param name="match">the low-level match criteria</param>
        /// <returns></returns>
        internal static TableCursor New(Context cnx, Database db, Table tb, BTree<long, TypedValue> match)
        {
            var bmk = tb.tableRows.First();
            var r = MoveToMatch(cnx, db, match, ref bmk);
            if (r == null)
                return null;
            return new TableCursor(cnx, db, tb, match, r);
        }
    }
}
