using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level2;
using System;
using System.Text;
using System.Net;
using System.IO;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level4
{
    /// <summary>
    /// Activations provide a context for execution of stored procedure code and triggers
    /// and nested declaration contexts (such as For Statements etc). Activations always run
    /// with definer's privileges.
    /// </summary>
    internal class Activation : Context
    {
        internal readonly string label;
        /// <summary>
        /// Exception handlers defined for this block
        /// </summary>
        public BTree<string, Handler> exceptions =BTree<string, Handler>.Empty;
        public BTree<long, bool> locals = BTree<long, bool>.Empty;
        /// <summary>
        /// The current signal if any
        /// </summary>
        public Signal signal;
        /// <summary>
        /// This is for implementation of the UNDO handler
        /// </summary>
        public ExecState saved;
        /// <summary>
        /// Support for loops
        /// </summary>
        public Activation cont;
        public Activation brk;
        // for method calls
        public SqlValue var = null; 
        /// <summary>
        /// Constructor: a new activation for a given named block
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="n">The block name</param>
		public Activation(Context cx, string n) 
            : base(cx)
        {
            label = n;
            next = cx;
            data = cx.data;
            cursors = cx.cursors;
            nextHeap = cx.nextHeap;
        }
        /// <summary>
        /// Constructor: a new activation for a Procedure. See CalledActivation constructor.
        /// </summary>
        /// <param name="cx">The current context</param>
        /// <param name="pr">The procedure</param>
        /// <param name="n">The headlabel</param>
        protected Activation(Context cx,DBObject pr)
            : base(cx,cx.db.objects[pr.definer] as Role,cx.user)
        {
            label = ((ObInfo)db.role.infos[pr.defpos]).name;
            next = cx;
        }
        internal override Context SlideDown()
        {
            for (var b = values.PositionAt(0); b != null; b = b.Next())
            {
                var k = b.key();
                if (!locals.Contains(k))
                    next.values += (k, values[k]);
            }
            next.val = val;
            next.nextHeap = nextHeap;
            next.nextStmt = nextStmt;
            next.db = db; // adopt the transaction changes done by this
            return next;
        }
        protected virtual void Debug(System.Text.StringBuilder sb)
        {
            sb.Append(" " + cxid);
        }
        /// <summary>
        /// flag NOT_FOUND if there is a handler for it
        /// </summary>
        internal override void NoData()
        {
            if (exceptions.Contains("02000"))
                new Signal(tr.uid,"02000",cxid).Obey(this);
            else if (exceptions.Contains("NOT_FOUND"))
                new Signal(tr.uid,"NOT_FOUND", cxid).Obey(this);
            else if (next != null)
                next.NoData();
        }
        internal virtual TypedValue Ret()
        {
            return val;
        }
        internal override Context FindCx(long c)
        {
            if (locals.Contains(c))
                return this;
            return next?.FindCx(c) ?? throw new PEException("PE556");
        }
    }
    internal class CalledActivation : Activation
    {
        internal Procedure proc = null;
        internal Domain udt = null;
        internal Method cmt = null;
        internal ObInfo udi = null;
        public CalledActivation(Context cx, Procedure p,Domain ot)
            : base(cx, p)
        { 
            proc = p; udt = ot;
            for (var b = p.ins.First(); b != null; b = b.Next())
                locals += (b.value(),true);
            if (p is Method mt)
            {
                cmt = mt;
                for (var b = ot.rowType.First(); b != null; b = b.Next())
                {
                    var iv = cx.Inf(b.value());
                    locals += (iv.defpos, true);
                    cx.Add(iv);
                }
            }
        }
        internal override TypedValue Ret()
        {
            if (udi!=null)
                return new TRow(udi,values);
            return base.Ret();
        }
        protected override void Debug(System.Text.StringBuilder sb)
        {
            sb.Append(" " + proc.name);
        }
    }
    internal class TargetActivation : Activation
    {
        internal Context _cx;
        internal readonly RowSet _trs;
        internal readonly RowSet _fm;
        internal readonly ObInfo _ti;
        internal readonly long _tgt;
        internal readonly CTree<long, RowSet.Finder> _finder;
        internal PTrigger.TrigType _tty; // may be Insert
        internal int count = 0;
        internal TargetActivation(Context cx, RowSet fm, PTrigger.TrigType tt)
            : base(cx,"")
        {
            _cx = cx;
            _tty = tt; // guaranteed to be Insert, Update or Delete
            _trs = new TransitionRowSet(this,fm);
            _fm = fm;
            _tgt = fm.target;
            var ob = (DBObject)_cx.db.objects[_tgt];
            var ro = (Role)_cx.db.objects[ob.definer];
            _ti = (ObInfo)ro.infos[_tgt];
            var fi = CTree<long, RowSet.Finder>.Empty;
            var fb = fm.rt.First();
            for (var b = _ti.domain.rowType.First(); b != null&&fb!=null; b = b.Next(),fb=fb.Next())
            {
                var fp = fb.value();
                var f = new RowSet.Finder(fp, _trs.defpos);
                fi += (fp, f);
                fi += (b.value(), f);
            }
            _finder = fi;
        }
        protected override void Debug(System.Text.StringBuilder sb)
        {
            sb.Append(" " + _ti.name);
        }
        internal virtual void EachRow()
        { }
        internal virtual Context Finish()
        {
            return _cx;
        }
        internal static void RoundTrip(Context cx, long vp, PTrigger.TrigType tp, WebRequest rq, string url, StringBuilder sql)
        {
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("RoundTrip " + rq.Method + " " + url + " " + sql?.ToString());
            if (sql != null)
            {
                var bs = Encoding.UTF8.GetBytes(sql.ToString());
                rq.ContentLength = bs.Length;
                try
                {
                    var rqs = rq.GetRequestStream();
                    rqs.Write(bs, 0, bs.Length);
                    rqs.Close();
                }
                catch (WebException)
                {
                    throw new DBException("3D002", url);
                }
            }
            HttpWebResponse rp = null;
            rp = RestRowSet.GetResponse(rq);
            var vi = (ObInfo)cx.db.role.infos[vp];
            var post = rq.Method == "POST";
            if (vi.metadata.Contains(Sqlx.ETAG)) // see Pyrrho manual sec 3.8.1
            {
                var et = rp.Headers["ETag"];
                if (et!=null)
                switch (tp & (PTrigger.TrigType.Insert | PTrigger.TrigType.Update | PTrigger.TrigType.Delete))
                {
                    case PTrigger.TrigType.Insert:
                        if (post)
                        {
                            cx.etags.cons[url].rvv += Rvv.Parse(et);
                            ShowETag(cx,vi);
                        }
                        break;
                    case PTrigger.TrigType.Update:
                        cx.etags.cons[url].rvv += Rvv.Parse(et);
                        ShowETag(cx,vi);
                        break;
                    case PTrigger.TrigType.Delete:
                        if (!post)
                        {
                            cx.etags.cons[url].rvv += Rvv.Parse(et);
                            ShowETag(cx,vi);
                        }
                        break;
                }
            }
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("--> " + rp.StatusCode);
            if (rp == null || rp.StatusCode != HttpStatusCode.OK)
            {
                var rs = rp.GetResponseStream();
                var s = new StreamReader(rs).ReadToEnd();
                if (s.StartsWith("SQL Error: "))
                {
                    var sig = s.Substring(11, 5);
                    var ix = s.IndexOf('\n');
                    throw new DBException(sig, s.Substring(16));
                }
                throw new DBException("23000");
            }
            rp?.Close();
        }
        static void ShowETag(Context cx,ObInfo vi)
        {
            var url = (string)vi.metadata[Sqlx.URL] ?? vi.description;
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("Recording ETag "+ cx.etags.cons[url].rvv);
        }
    }
    internal class TableActivation : TargetActivation
    {
        internal Table table;
        /// <summary>
        /// There may be several triggers of any type, so we manage a set of transition activations for each.
        /// These are for table before, table instead, table after, row before, row instead, row after.
        /// </summary>
        internal BTree<long, TriggerActivation> acts = null;
        internal BTree<long, TableActivation> casc = BTree<long,TableActivation>.Empty;
        internal readonly CTree<PTrigger.TrigType,CTree<long, bool>> _tgs;
        internal Index index = null; // for autokey
        internal bool? trigFired = null;
        internal string prov = null;
        internal Level level = Level.D;
        internal SqlValue security = null;
        internal BTree<long, UpdateAssignment> updates = BTree<long, UpdateAssignment>.Empty;
        internal TableActivation(Context cx,RowSet fm,PTrigger.TrigType tt,string pr=null,Level cl=null)
            : base(cx,fm,tt)
        {
            _cx = cx; 
            table = (Table)obs[_tgt];
            _tgs = table.triggers;
            var trs = (TransitionRowSet)_trs;
            acts = BTree<long, TriggerActivation>.Empty;
            for (var b = _tgs.First(); b != null; b = b.Next())
                if (b.key().HasFlag(tt))
                    for (var tg = b.value().First(); tg != null; tg = tg.Next())
                    {
                        var t = tg.key();
                        // NB at this point obs[t] version of the trigger has the wrong action field
                        var td = (Trigger)db.objects[t];
                        var ta = new TriggerActivation(this, trs, td);
                        acts += (t, ta);
                        ta.Frame(t);  
                        ta.obs += (t, td);
                    }
            for (var b = table.rindexes.First(); b != null; b = b.Next())
            {
                var rt = (Table)cx.db.objects[b.key()];
                var (cs, rs) = b.value();
                var x = (Index)cx.db.objects[table.indexes[cs]];
                var rx = (Index)cx.db.objects[rt.indexes[rs]];
                var fl = CTree<long, TypedValue>.Empty;
                var xm = BTree<long, long>.Empty;
                var pb = rx.keys.First();
                for (var rb = x.keys.First(); pb != null && rb != null;
                    pb = pb.Next(), rb = rb.Next())
                    xm += (rb.value(), pb.value());
                for (var c = fm.matches.First(); c != null; c = c.Next())
                {
                    var sc = (SqlCopy)cx.obs[c.key()];
                    fl += (xm[sc.copyFrom], c.value());
                }
                var rf = new IndexRowSet(cx, rt, rx, fl)
                    + (RowSet.RSTargets, new CTree<long, long>(rt.defpos, trs.defpos));
                data += (rf.defpos, rf);
                obs += (rx.defpos, rx);
                var tb = (Table)db.objects[rt.defpos];
                obs += (tb.defpos, tb);
                for (var xb = rf.rt.First(); xb != null; xb = xb.Next())
                {
                    var p = xb.value();
                    obs += (p, (TableColumn)db.objects[p]);
                }
                nextHeap++;
                var ra = new TableActivation(this, rf, tt);
                ra._tty = PTrigger.TrigType.Update;
                if (tt == PTrigger.TrigType.Update)
                    switch (rx.flags & PIndex.Updates)
                    {
                       case PIndex.ConstraintType.CascadeUpdate:
                            break;
                        case PIndex.ConstraintType.SetDefaultUpdate:
                            for (var kb = rx.keys.Last(); kb != null; kb = kb.Previous())
                            {
                                var p = kb.value();
                                var sc = (SqlValue)ra.obs[p];
                                ra.updates += (p, new UpdateAssignment(p, sc.domain.defaultValue));
                            }
                            break;
                        case PIndex.ConstraintType.SetNullUpdate:
                            for (var kb = rx.keys.Last(); kb != null; kb = kb.Previous())
                            {
                                var p = kb.value();
                                ra.updates += (p, new UpdateAssignment(p, TNull.Value));
                            }
                            break;
                    }
                else if (tt==PTrigger.TrigType.Delete)
                    switch (rx.flags & PIndex.Deletes)
                    {
                        case PIndex.ConstraintType.RestrictDelete:
                        case PIndex.ConstraintType.CascadeDelete:
                            ra._tty = PTrigger.TrigType.Delete;
                            break;
                        case PIndex.ConstraintType.SetDefaultDelete:
                            for (var kb = rx.keys.Last(); kb != null; kb = kb.Previous())
                            {
                                var p = kb.value();
                                var sc = (SqlValue)ra.obs[p];
                                ra.updates += (p, new UpdateAssignment(p, sc.domain.defaultValue));
                            }
                            break;
                        case PIndex.ConstraintType.SetNullDelete:
                            for (var kb = rx.keys.Last(); kb != null; kb = kb.Previous())
                            {
                                var p = kb.value();
                                ra.updates += (p, new UpdateAssignment(p, TNull.Value));
                            }
                            break;

                    }
                casc += (rx.defpos, ra);
            }
            if (trs.indexdefpos >= 0)
                index =  (Index)cx.db.objects[trs.indexdefpos];
            switch (tt & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    if (table.Denied(cx, Grant.Privilege.Insert))
                        throw new DBException("42105", ((ObInfo)cx.db.role.infos[table.defpos]).name);
                    // parameter cl is only supplied when d_User.defpos==d.owner
                    // otherwise check if we should compute it
                    if (cx.db.user != null &&
                        cx.db.user.defpos != cx.db.owner && table.enforcement.HasFlag(Grant.Privilege.Insert))
                    {
                        var uc = cx.db.user.clearance;
                        if (!uc.ClearanceAllows(table.classification))
                            throw new DBException("42105", ((ObInfo)cx.db.role.infos[table.defpos]).name);
                        // The new record’s classification will have the user’s minimum clearance level:
                        // if this is above D, the groups will be the subset of the user’s groups 
                        // that are in the table classification, 
                        // and the references will be the same as the table 
                        // (a subset of the user’s references)
                        level = uc.ForInsert(table.classification);
                    }
                    //       var ckc = new ConstraintChecking(tr, trs, this);
                    finder = cx.finder + _finder;
                    break;
                case PTrigger.TrigType.Update:
                    if (table.Denied(cx, Grant.Privilege.Update))
                        throw new DBException("42105", ((ObInfo)cx.db.role.infos[table.defpos]).name);
                    for (var ass = fm.assig.First(); ass != null; ass = ass.Next())
                        if (cx.obs[ass.key().vbl] is SqlSecurity)
                            security = cx.obs[ass.key().val] as SqlValue;
                        else
                        {
                            var c = cx.obs[ass.key().vbl] as SqlCopy
                                ?? throw new DBException("0U000");
                            DBObject oc = c;
                            while (oc is SqlCopy sc) // Views have indirection here
                                oc = cx.obs[sc.copyFrom];
                            if (oc is TableColumn tc && tc.generated != GenerationRule.None)
                                throw cx.db.Exception("0U000", c.name).Mix();
                            if (c.Denied(cx, Grant.Privilege.Update))
                                throw new DBException("42105", c.name);
                            updates += (oc.defpos, ass.key());
                        }
                    //  Values in a row can be modified in 3 ways:
                    //  1. by one of the UpdateAssignments (i.e. this.updates, will affect vs)
                    //  2. in a row-level trigger by assignment to newrow (will affect ta.NewRow)
                    //  3. in a row-level before trigger by assignment to a column (will affect ta.values)
                    // A. Before before triggers, set ta.newRow to vs
                    // B. Allow before triggers to modify ta.newRow and ta.values progressively.
                    // C. After before triggers, set vs to final value of ta.newRow
                    // D. Compare tgc.rec to ta.values, destructively apply *changes* to vs 
                    // (By Note 116 of ISO 9075 (2016) changes of type 3 override those of type 1,
                    // and are made at this point.)
                    // E. Update tgc from vs.
                    level = cx.db.user?.clearance ?? Level.D;
                    cx.finder += _finder;
                    if ((level != null || updates.Count > 0))
                    {
                        var fi = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachStatement);
                        if (fi != true)
                            Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachStatement);
                    }
                    // Do statement-level triggers
                    trigFired = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachStatement);
                    break;
                case PTrigger.TrigType.Delete:
                    var targetInfo = (ObInfo)cx.db.role.infos[fm.target];
                    if (table.Denied(cx, Grant.Privilege.Delete) ||
                        (table.enforcement.HasFlag(Grant.Privilege.Delete) &&
                        cx.db.user.clearance.minLevel > 0 &&
                        (cx.db.user.clearance.minLevel != targetInfo.classification.minLevel ||
                        cx.db.user.clearance.maxLevel != targetInfo.classification.maxLevel)))
                        throw new DBException("42105", ((ObInfo)cx.db.role.infos[table.defpos]).name);
                    level = cx.db.user.clearance;
                    cx.finder += _finder;
                    trigFired = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachStatement);
                    if (trigFired != true)
                        trigFired = Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachStatement);
                    break;
            }
        }
        internal override void EachRow()
        {
            var tgc = (TransitionRowSet.TargetCursor)cursors[_trs.defpos];
            var rc = tgc._rec;
            var np = db.nextPos;
            var trs = (TransitionRowSet)_trs;
            if (trigFired == true)
                return;
            switch (_tty & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    {
                        // Do row-level triggers
                        trigFired = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachRow);
                        if (trigFired != true)
                            trigFired = Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachRow);
                        if (trigFired == true) // an insteadof trigger has fired
                            return;
                        tgc = (TransitionRowSet.TargetCursor)cursors[_trs.defpos];
                        var st = rc.subType;
                        Record r;
                        if (level != Level.D)
                            r = new Record3(table, tgc.values, st, level, db.nextPos, _cx);
                        else if (prov != null)
                            r = new Record1(table, tgc.values, prov, db.nextPos, _cx);
                        else
                            r = new Record(table, tgc.values, db.nextPos, _cx);
                        Add(r);
                        var ns = newTables[_trs.defpos] ?? BTree<long, TableRow>.Empty;
                        newTables += (_trs.defpos, ns + (r.defpos, new TableRow(r,_cx.db)));
                        count++;
                        // install the record in the transaction
                        //      cx.tr.FixTriggeredActions(triggers, ta._tty, r);
                        _cx.db = db;
                        // Row-level after triggers
                        Triggers(PTrigger.TrigType.After | PTrigger.TrigType.EachRow);
                        break;
                    }
                case PTrigger.TrigType.Update:
                    {
                        var vs = rc.vals;
                        var was = tgc._rec;
                        for (var b = updates.First(); b != null; b = b.Next())
                        {
                            var ua = b.value();
                            var tv = ua.Eval(this);
                            var tp = trs.transTarget[ua.vbl].col;
                            vs += (tp, tv);
                        }
                        // Step A
                        values += (Trigger.OldRow, tgc);
                        values += (Trigger.NewRow, new TRow(tgc.dataType, vs));
                        values += rc.vals;
                        // Step B
                        var fi = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachRow);
                        if (fi != true)
                            fi = Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachRow);
                        if (fi == true) // an insteadof trigger has fired
                            return;
                        // Step C
                        vs = ((TRow)values[Trigger.NewRow]).values;
                        // If Update is enforced by the table, and a record selected for update 
                        // is not one to which the user has clearance 
                        // or does not match the user’s clearance level, 
                        // throw an Access Denied exception.
                        if (table.enforcement.HasFlag(Grant.Privilege.Update)
                            && _cx.db.user != null
                            && _cx.db.user.defpos != _cx.db.owner && ((rc != null) ?
                                 ((!level.ClearanceAllows(rc.classification))
                                 || level.minLevel != rc.classification.minLevel)
                                 : level.minLevel > 0))
                            throw new DBException("42105");
                        // Step D
                        np = db.nextPos;
                        tgc = (TransitionRowSet.TargetCursor)cursors[_trs.defpos];
                        var old = was.vals;
                        for (var b = tgc.dataType.rowType.First(); b != null; b = b.Next())
                        {
                            var p = b.value();
                            var tv = values[p];
                            var ov = old[p];
                            var v = vs[p];
                            if (tv.CompareTo(ov)!=0 && v.CompareTo(tv) != 0)// ISO9075 Note 116
                                vs += (p, tv);
                        }
                        for (var b = vs.First(); b != null; b = b.Next())
                        {
                            var k = b.key();
                            if (rc.vals[k] is TypedValue v && v.CompareTo(b.value()) == 0)
                                vs -= k;
                        }
                        if (vs.Count == 0L)
                            return;
                        for (var b=casc.First();b!=null;b=b.Next())
                            was.Cascade(b.value(), vs);
                        var nu = tgc._rec;
                        var u = (security == null) ?
                                new Update(nu, table, vs, db.nextPos, _cx) :
                                new Update1(nu, table, vs,(Level)security.Eval(_cx).Val(), db.nextPos, _cx);
                        Add(u);
                        var ns = newTables[_trs.defpos] ?? BTree<long, TableRow>.Empty;
                        newTables += (_trs.defpos, ns + (nu.defpos, new TableRow(u,_cx.db)));
                        _cx.db = db;
                        Triggers(PTrigger.TrigType.After | PTrigger.TrigType.EachRow);
                        break;
                    }
                case PTrigger.TrigType.Delete:
                    {
                        var fi = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachRow);
                        if (fi == true)
                            return;
                        fi = Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachRow);
                        if (fi == true)
                            return;
                        tgc = (TransitionRowSet.TargetCursor)cursors[_trs.defpos];
                        rc = tgc._rec;
                        if (_cx.db.user.defpos != _cx.db.owner && table.enforcement.HasFlag(Grant.Privilege.Delete) ?
                            // If Delete is enforced by the table and the user has delete privilege for the table, 
                            // but the record to be deleted has a classification level different from the user 
                            // or the clearance does not allow access to the record, throw an Access Denied exception.
                            ((!level.ClearanceAllows(rc.classification)) || level.minLevel > rc.classification.minLevel)
                            : level.minLevel > 0)
                            throw new DBException("42105");
                        for (var b = casc.First(); b != null; b = b.Next())
                            rc.Cascade(b.value());
                        //      cx.tr.FixTriggeredActions(triggers, ta._tty, cx.db.nextPos);
                        var ns = newTables[_trs.defpos] ?? BTree<long, TableRow>.Empty;
                        newTables += (_trs.defpos, ns - rc.defpos);
                        Add(new Delete1(rc, table, db.nextPos, _cx));
                        _cx.db = db;
                        count++;
                        break;
                    }
            }
        }
        internal override Context Finish()
        {
            if (trigFired == true)
                return _cx;
            // Statement-level after triggers
            Triggers(PTrigger.TrigType.After | PTrigger.TrigType.EachStatement);
            _cx.result = -1L;
            return _cx;
        }
        /// <summary>
        /// Perform triggers
        /// </summary>
        /// <param name="fg"></param>
        /// <returns></returns>
        internal bool? Triggers(PTrigger.TrigType fg) // flags to add
        {
            bool? r = null;
            fg |= _tty;
            for (var a=acts.First();r!=true && a!=null;a=a.Next())
            {
                var ta = a.value();
                ta.finder = finder + ta._finder;
                ta.cursors += cursors;
                ta.db = db;
                if (ta._trig.tgType.HasFlag(fg))
                    r = ta.Exec();
            }
            SlideDown(); // get next to adopt our changes
            return r;
        }
        protected override void Debug(System.Text.StringBuilder sb)
        {
            sb.Append(" " + _ti.name);
        }
    }
    internal class RESTActivation : TargetActivation
    {
        internal RestRowSet _rr;
        internal RestView _vw;
        internal WebRequest _rq;
        internal string _url;
        internal string _targetName;
        internal StringBuilder _sql = new StringBuilder();
        internal string inscm = "";
        internal BTree<long, UpdateAssignment> updates = BTree<long, UpdateAssignment>.Empty;
        internal RESTActivation(Context cx, RestRowSet rr, RowSet fm, PTrigger.TrigType tgt,
            string prov = null, Level cl = null)
            : base(cx, fm, tgt)
        {
            _rr = rr;
            _vw = (RestView)cx.obs[rr.restView];
            var vi = (ObInfo)db.role.infos[_vw.viewPpos];
            (_url, _targetName, _sql) = _rr.GetUrl(this, vi);
            _url += "/" + _targetName;
            var url = new StringBuilder(_url);
            for (var b = _rr.matches.First(); b != null; b = b.Next())
            {
                var kn = ((SqlValue)cx.obs[b.key()]).name;
                url.Append("/"); url.Append(kn);
                url.Append("="); url.Append(b.value());
            }
            _rq = _rr.GetRequest(this, url.ToString(), vi);
            _rq.Method = "HEAD";
            rr.SetupETags(cx);
            RoundTrip(cx, rr.restView, tgt, _rq, _url, null); 
            switch (tgt & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    if (_vw.Denied(cx, Grant.Privilege.Insert))
                        throw new DBException("42105", ((ObInfo)cx.db.role.infos[rr.defpos]).name);
                    _rq.ContentType = "text/plain";
                    _sql.Append("insert into "); _sql.Append(_targetName);
                    _sql.Append(" values");
                    break;
                case PTrigger.TrigType.Update:
                    if (_vw.Denied(cx, Grant.Privilege.Update))
                        throw new DBException("42105", ((ObInfo)cx.db.role.infos[rr.defpos]).name);
                    for (var ass = rr.assig.First(); ass != null; ass = ass.Next())
                    {
                        var c = cx.obs[ass.key().vbl] as SqlCopy
                            ?? throw new DBException("0U000");
                        DBObject oc = c;
                        while (oc is SqlCopy sc) // Views have indirection here
                            oc = cx.obs[sc.copyFrom];
                        if (c.Denied(cx, Grant.Privilege.Update))
                            throw new DBException("42105", c.name);
                        updates += (oc.defpos, ass.key());
                    }
                    break;
                case PTrigger.TrigType.Delete:
                    if (_vw.Denied(cx, Grant.Privilege.Delete))
                        throw new DBException("42105", ((ObInfo)cx.db.role.infos[rr.defpos]).name);
                    break;
            }
        }
        internal override void EachRow()
        {
            var cu = _cx.cursors[_rr.defpos];
            switch (_tty & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    var vs = cu.values;
                    _sql.Append(inscm); inscm = ",";
                    var cm = "(";
                    for (var b = _rr.remoteCols.First(); b != null; b = b.Next())
                        if (vs[b.value()] is TypedValue tv)
                        {
                            _sql.Append(cm); cm = ",";
                            if (tv.dataType.kind == Sqlx.CHAR)
                            {
                                _sql.Append("'");
                                _sql.Append(tv.ToString().Replace("'", "'''"));
                                _sql.Append("'");
                            }
                            else
                                _sql.Append(tv);
                        }
                    _sql.Append(")");
                    break;
            }
        } 
        internal override Context Finish()
        {
            switch (_tty & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    _cx.AddPost(_url, _targetName, _sql.ToString(), db.user.name, _vw.defpos, _tty);
                    break;
                case PTrigger.TrigType.Update:
                    {
                        var cm = " set ";
                        if (_rr.assig.Count == 0)
                            return _cx;
                        _sql.Append("update "); _sql.Append(_targetName);
                        for (var b = _rr.assig.First(); b != null; b = b.Next())
                        {
                            var ua = b.key();
                            _sql.Append(cm); cm = ",";
                            _sql.Append(((SqlValue)obs[ua.vbl]).name); _sql.Append("=");
                            var tv = ua.Eval(this);
                            if (tv.dataType.kind == Sqlx.CHAR)
                            {
                                _sql.Append("'");
                                _sql.Append(tv.ToString().Replace("'", "'''"));
                                _sql.Append("'");
                            }
                            else
                                _sql.Append(tv);
                        }
                        if (_rr.where.Count > 0 || _rr.matches.Count > 0)
                        {
                            var sw = _rr.WhereString(_cx);
                            if (sw.Length > 0)
                            {
                                _sql.Append(" where ");
                                _sql.Append(sw);
                            }
                        }
                        _cx.AddPost(_url, _targetName, _sql.ToString(), db.user.name, _vw.defpos, _tty);
                    }
                    break;
                case PTrigger.TrigType.Delete:
                    {
                        _rq.ContentType = "text/plain";
                        _sql.Append("delete from "); _sql.Append(_targetName);
                        if (_rr.where.Count > 0 || _rr.matches.Count > 0)
                        {
                            var sw = _rr.WhereString(_cx);
                            if (sw.Length > 0)
                            {
                                _sql.Append(" where ");
                                _sql.Append(sw);
                            }
                        }
                        _cx.AddPost(_url, _targetName, _sql.ToString(), db.user.name, _vw.defpos, _tty);
                    }
                    break;
            }
            return _cx;
        }
    }
    internal class HTTPActivation: TargetActivation
    {
        internal RestRowSet _rr;
        internal RestView _vw;
        internal BTree<long, UpdateAssignment> updates = BTree<long, UpdateAssignment>.Empty;
        internal HTTPActivation(Context cx, RestRowSet rr, RowSet fm, PTrigger.TrigType tgt,
            string prov = null, Level cl=null)
            : base(cx, fm, tgt)
        {
            _rr = rr;
            _vw = (RestView)cx.obs[rr.restView];
            switch (tgt & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    if (_vw.Denied(cx, Grant.Privilege.Insert))
                        throw new DBException("42105", ((ObInfo)cx.db.role.infos[rr.defpos]).name);
                    break;
                case PTrigger.TrigType.Update:
                    if (_vw.Denied(cx, Grant.Privilege.Update))
                        throw new DBException("42105", ((ObInfo)cx.db.role.infos[rr.defpos]).name);
                    for (var ass = rr.assig.First(); ass != null; ass = ass.Next())
                    {
                        var c = cx.obs[ass.key().vbl] as SqlCopy
                            ?? throw new DBException("0U000");
                        DBObject oc = c;
                        while (oc is SqlCopy sc) // Views have indirection here
                            oc = cx.obs[sc.copyFrom];
                        if (oc is TableColumn tc && tc.generated != GenerationRule.None)
                            throw cx.db.Exception("0U000", c.name).Mix();
                        if (c.Denied(cx, Grant.Privilege.Update))
                            throw new DBException("42105", c.name);
                        updates += (oc.defpos, ass.key());
                    }
                    break;
                case PTrigger.TrigType.Delete:
                    if (_vw.Denied(cx, Grant.Privilege.Delete))
                        throw new DBException("42105", ((ObInfo)cx.db.role.infos[rr.defpos]).name);
                    break;
            }
        }
        internal override void EachRow()
        {
            var cu = (TransitionRowSet.TargetCursor)cursors[_trs.defpos];
            var vi = (ObInfo)db.role.infos[_vw.viewPpos];
            var (url, targetName, sql) = _rr.GetUrl(this, vi);
            var rq = _rr.GetRequest(this, url, vi);
            switch (_tty & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    rq.Method = "POST";
                    rq.Accept = _vw.mime ?? "application/json";
                    var cm = "[{";
                    var vs = cu.values;
                    for (var rb = cu.Rec().First(); rb != null; rb = rb.Next())
                    {
                        var rc = rb.value(); // probably a RemoteTableRow
                        var st = rc.subType;
                        var np = _cx.db.nextPos;
                        vs += rc.vals;
                        for (var b = _rr.remoteCols.First(); b != null; b = b.Next())
                        {
                            sql.Append(cm); cm = ",";
                            sql.Append('"'); sql.Append(obs[b.value()].name);
                            sql.Append("\":"); 
                            var v =vs[b.value()];
                            sql.Append((v is TChar) ? ("'" + v.ToString() + "'")
                                : v.ToString());
                        }
                        sql.Append("}]");
                        RoundTrip(this,_rr.restView, _tty, rq, url, sql);
                        var nr = new RemoteTableRow(np, vs, url, _rr);
                        var ns = _cx.newTables[_vw.defpos] ?? BTree<long, TableRow>.Empty;
                        _cx.newTables += (_vw.defpos, ns + (nr.defpos, nr));
                        count++;
                    }
                    break;
                case PTrigger.TrigType.Update:
                        if (_rr.assig.Count == 0)
                        return;
                    for (var rb = cu.Rec().First(); rb != null; rb = rb.Next())
                    {
                        var rc = rb.value(); // probably a RemoteTableRow
                        vs = rc.vals;
                        for (var b = updates.First(); b != null; b = b.Next())
                        {
                            var ua = b.value();
                            var tv = ua.Eval(_cx);
                            vs += (ua.vbl, tv);
                        }
                        var np = _cx.db.nextPos;
                        rq.Accept = _vw.mime ?? "application/json";
                        rq.Method = "PUT";
                        cm = "[{";
                        vs = cursors[_rr.defpos].values;
                        for (var b = _rr.assig.First(); b != null; b = b.Next())
                        {
                            var ua = b.key();
                            vs += (ua.vbl, ua.Eval(_cx));
                        }
                        var tb = _rr.rt.First();
                        for (var b = _rr.remoteCols.First(); b != null; b = b.Next(), tb = tb.Next())
                        {
                            sql.Append(cm); cm = ",";
                            sql.Append('"'); sql.Append(obs[b.value()].name);
                            var tv = vs[tb.value()];
                            sql.Append("\":");
                            if (tv.dataType.kind == Sqlx.CHAR)
                            {
                                sql.Append("'");
                                sql.Append(tv.ToString().Replace("'", "'''"));
                                sql.Append("'");
                            }
                            else
                                sql.Append(tv);
                        }
                        sql.Append("}]");
                        RoundTrip(this,_rr.restView, _tty, rq, url, sql);
                        var nr = new RemoteTableRow(np,vs,url,_rr);
                        var ns = _cx.newTables[_vw.defpos] ?? BTree<long, TableRow>.Empty;
                        _cx.newTables += (_vw.defpos, ns + (nr.defpos, nr));
                    }
                    break;
                case PTrigger.TrigType.Delete:
                    for (var rb = cu.Rec().First(); rb != null; rb = rb.Next())
                    {
                        rq.Method = "DELETE";
                        RoundTrip(this,_rr.restView, _tty, rq, url, sql);
                        count++;
                    }
                    break;
            }
        }
    }
    /// <summary>
    /// This Activation context is for executing a single trigger on a row of a TransitionRowSet. 
    /// Triggers run with definer's privileges. 
    /// There are two phases: setup, when the trigger is referenced,
    /// and execution, which may be entered for each row of a rowSet.
    /// A transition row set allows access to old and new states, which evolve if there are many triggers
    /// </summary>
    internal class TriggerActivation : Activation
    {
        internal readonly TransitionRowSet _trs;
        internal bool defer;
        /// <summary>
        /// The trigger definition
        /// </summary>
        internal readonly Trigger _trig;
        internal readonly CTree<long, RowSet.Finder> _finder;
        internal readonly BTree<long, long> trigTarget, targetTrig; // trigger->target, target->trigger
        /// <summary>
        /// Prepare for multiple executions of this trigger
        /// </summary>
        /// <param name="trs">The transition row set</param>
        /// <param name="tg">The trigger</param>
        internal TriggerActivation(Context cx, TransitionRowSet trs, Trigger tg)
            : base(cx, tg)
        {
            _trs = trs;
            parent = cx.next;
            nextHeap = cx.nextHeap;
            obs = cx.obs;
            Install1(tg.framing);
            Install2(tg.framing);
            var tb = (Table)cx.db.objects[trs.target];
            var ro = (Role)cx.db.objects[tg.definer];
            var ti = (ObInfo)ro.infos[tb.defpos];
            cx.obs += (tb.defpos, tb);
            _trig = tg;
            (trigTarget,targetTrig) = _Map(ti, tg);
            defer = _trig.tgType.HasFlag(Level2.PTrigger.TrigType.Deferred);
            if (cx.obs[tg.oldTable] is TransitionTable tt)
                new TransitionTableRowSet(tt.defpos,cx,trs,
                    tg.framing.obs[tt.defpos].domain,true);
            if (defer)
                cx.deferred += this;
            var fi = CTree<long, RowSet.Finder>.Empty;
            for (var b = trigTarget.First(); b != null; b = b.Next())
            {
                var f = new RowSet.Finder(b.key(), _trs.defpos);
                fi += (b.value(), f);
                fi += (b.key(), f);
            }
            var o = _trig.oldRow;
            var n = _trig.newRow;
            if (o != -1L)
                fi += (o, new RowSet.Finder(o, _trs.defpos));
            if (n != -1L)
                fi += (n, new RowSet.Finder(n, _trs.defpos));
            _finder = fi;
        }
        static (BTree<long,long>,BTree<long,long>) _Map(ObInfo oi,Trigger tg)
        {
            var ma = BTree<long, long>.Empty;
            var rm = BTree<long, long>.Empty;
            var sb = tg.domain.rowType.First();
            for (var b = oi.domain.rowType.First(); b != null && sb != null; b = b.Next(),
                sb = sb.Next())
            {
                var tp = sb.value();
                var p = b.value();
                ma += (tp, p);
                rm += (p, tp);
            }
            return (ma,rm);
        }
        /// <summary>
        /// Execute the trigger for the current row or table, using the definer's role.
        /// </summary>
        /// <returns>whether the trigger was fired (i.e. WHEN condition if any matched)</returns>
        internal bool Exec()
        {
            var rp = _trs.defpos;
            var trc = (TransitionRowSet.TransitionCursor)next.next.cursors[rp];
            if (trc!=null) // row=level trigger
                new TransitionRowSet.TriggerCursor(this, trc._tgc);
            if (defer)
                return false;
            if (_trig.oldRow != -1L)
                values += (_trig.oldRow, new TRow(_trig.domain, trigTarget,
                    ((TRow)next.values[Trigger.OldRow]).values));
            if (_trig.newRow != -1L)
                values += (_trig.newRow, new TRow(_trig.domain, trigTarget,
                    ((TRow)next.values[Trigger.NewRow]).values));
            if (_trig.oldTable != -1L)
            {
                var ot = (TransitionTable)obs[_trig.oldTable];
                data += (ot.defpos, new TransitionTableRowSet(ot.defpos, next, _trs,
                    ot.domain, true));
            }
            if (_trig.newTable != -1L)
            {
                var nt = (TransitionTable)obs[_trig.newTable];
                data += (nt.defpos, new TransitionTableRowSet(nt.defpos, next, _trs,
                    nt.domain, false));
            }
            var ta = (WhenPart)obs[_trig.action];
            var tc = obs[ta.cond]?.Eval(this);
            if (tc != TBool.False)
            {
                db += (Transaction.TriggeredAction, db.nextPos);
                Add(new Level2.TriggeredAction(_trig.defpos, db.nextPos, this));
                var nx = ((Executable)obs[ta.stms.First().value()]).Obey(this);
                if (nx != this)
                    throw new PEException("PE677");
                if (trc != null) // row-level trigger
                {
                    for (var b = _trig.domain.rowType.First(); b != null; b = b.Next())
                    {
                        var cp = b.value();
                        var np = trigTarget[cp];
                        trc += ((TargetActivation)next, np, values[cp]);
                    }
                    next.next.cursors += (_trs.defpos, trc);
                }
            }
            SlideDown();
            if (tc != TBool.False && _trig.tgType.HasFlag(Level2.PTrigger.TrigType.Instead))
                return true;
            return false;
        }
        TRow _Row(SqlRow sr,BTree<long,TypedValue>vals)
        {
            var vs = CTree<long, TypedValue>.Empty;
            for (var b=sr.columns.First();b!=null;b=b.Next())
            {
                var p = b.value();
                vs += (p, vals[((SqlCopy)obs[p]).copyFrom]);                
            }
            return new TRow(sr, vs);
        }
        internal override TriggerActivation FindTriggerActivation(long tabledefpos)
        {
            var fm = (From)obs[data[_trs.defpos].from];
            var t = tr.objects[fm.target] as Table;
            return (t.defpos == tabledefpos) ? this : base.FindTriggerActivation(tabledefpos);
        }
        protected override void Debug(System.Text.StringBuilder sb)
        { }
        internal override Context SlideDown()
        {
            var ta = (TableActivation)next;
            var tac = ta.cursors[ta._trs.defpos]; //TargetCursor 
            for (var b =_trs.domain.rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                if (values[p] is TypedValue v)
                {
                    var tp = trigTarget[p];
                    ta.values += (tp,v);
                    tac += (ta, tp, v);
                }
            }
            return base.SlideDown();
        }
    }
}
