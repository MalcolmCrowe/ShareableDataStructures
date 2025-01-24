using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level2;
using Pyrrho.Level5;
using System.Text;
using System.Net;
using static Pyrrho.Level3.Basis;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
    /// <summary>
    /// Activations provide a context for execution of stored procedure code and triggers
    /// and nested declaration contexts (such as Show Statements etc). Activations always run
    /// with definer's privileges.
    /// </summary>
    internal class Activation : Context
    {
        internal readonly string? label;
        /// <summary>
        /// Exception handlers defined for this block
        /// </summary>
        public BTree<string, Handler> exceptions =BTree<string, Handler>.Empty;
        /// <summary>
        /// The current signal if any
        /// </summary>
        public Signal? signal;
        /// <summary>
        /// This is for implementation of the UNDO handler
        /// </summary>
        public ExecState? saved;
        /// <summary>
        /// Support for loops
        /// </summary>
        public Activation? cont;
        public Activation? brk;
        // for method calls
        public QlValue? var = null; 
        /// <summary>
        /// Constructor: a new activation for a given named block
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="n">The block name</param>
		public Activation(Context cx, string n) 
            : base(cx)
        {
            label = n;
        }
        /// <summary>
        /// Constructor: a new activation for a Procedure. See CalledActivation constructor.
        /// </summary>
        /// <param name="cx">The current context</param>
        /// <param name="pr">The procedure</param>
        /// <param name="n">The headlabel</param>
        protected Activation(Context cx,DBObject pr)
            : base(cx,(Role)(cx.db.objects[pr.definer]??throw new PEException("PE639")),
                  cx.user??throw new PEException("PE638"))
        {
            label = (pr.infos[pr.definer]??throw new PEException("PE637")).name;
            next = cx;
        }
        internal override Context SlideDown()
        {
            if (next == null)
                throw new PEException("PE636");
            for (var b = values.PositionAt(0); b != null; b = b.Next())
            {
                var k = b.key();
                if (!bindings.Contains(k) && values[k] is TypedValue v)
                    next.values += (k, v);
            }
            next.val = val;
            next.nextHeap = Math.Max(next.nextHeap,nextHeap);
            next.result = result;
            if (next.next is Context nx)
                nx.lastret = next.lastret;
            if (db != next.db)
            {
                var nd = db;
                if (db.role != next.db.role)
                    nd += (Database.Role, next.db.role);
                next.db = nd; // adopt the transaction changes done by this
            }
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
            if (tr == null)
                throw new PEException("PE635");
            if (exceptions.Contains("02000"))
                new Signal(tr.uid,"02000",cxid).Obey(this);
            else if (exceptions.Contains("NOT_FOUND"))
                new Signal(tr.uid,"NOT_FOUND", cxid).Obey(this);
            else next?.NoData();
        }
        internal virtual TypedValue Ret()
        {
            return val;
        }
        internal override Context FindCx(long c)
        {
            if (bindings.Contains(c))
                return this;
            return next?.FindCx(c) ?? throw new PEException("PE556");
        }
    }
    internal class CalledActivation : Activation
    {
        internal Procedure? proc = null;
        internal Method? cmt = null;
        internal Domain? rdt = null;
        public CalledActivation(Context cx, Procedure p)
            : base(cx, p)
        {
            proc = p;
            for (var b = p.ins.First(); b != null; b = b.Next())
                if (b.value() is long c)
                    bindings += (c, p.domain);
            if (p is Method mt)
            {
                cmt = mt;
                for (var b = mt.udType.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long c)
                        bindings += (c, cx.db.objects[c] as Domain??Domain.Null);
            }
        }
        internal override TypedValue Ret()
        {
            if (rdt is not null)
                return new TRow(rdt,values);
            return base.Ret();
        }
        internal override Context SlideDown()
        {
            if (next is Context nx && proc?.framing?.obs is BTree<long, DBObject> os)
                for (var b = os.First(); b != null; b = b.Next())
                    if (b.value() is DBObject ob)
                        nx.obs += (b.key(), ob);
            if (next is not null && result is RowSet ra) // for GQL
            {
                next.obs += obs;
                next.nextHeap = nextHeap;
                next.result = result;
            }
            return base.SlideDown();
        }
    }
    internal class TargetActivation : Activation
    {
        internal Context _cx;
        internal readonly RowSet _fm;
        internal Domain _tgt;
        internal PTrigger.TrigType _tty; // may be Insert
        internal int count = 0;
        internal TargetActivation(Context cx, RowSet fm, PTrigger.TrigType tt)
            : base(cx,"")
        {
            _cx = cx;
            _tty = tt; // guaranteed to be Insert, Update or Delete
            _fm = fm;
            _tgt = cx._Ob(fm.target) as Domain??throw new DBException("42105");
        }
        internal virtual Context EachRow(Context cx,int pos)
        {
            return cx;
        }
        internal virtual Context Finish(Context cx)
        {
            return cx;
        }
        internal static HttpResponseMessage RoundTrip(Context cx, long vp, PTrigger.TrigType tp, HttpRequestMessage rq, string url, StringBuilder? sql)
        {
            if (cx.db as Transaction==null)
                throw new PEException("PE1982");
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("RoundTrip " + rq.Method + " " + url + " " + sql?.ToString());
            if (sql != null)
            {
                var sc = new StringContent(sql.ToString(), Encoding.UTF8);
                rq.Content = sc;
            }
            var rp = PyrrhoStart.htc.Send(rq);
            if (cx._Ob(vp) is not DBObject vo || vo.infos[cx.role.defpos] is not ObInfo vi)
                throw new PEException("PE633");
            var post = rq.Method == HttpMethod.Post;
            if (vi.metadata.Contains(Qlx.ETAG)) // see Pyrrho manual sec 3.8.1
            {
                var et = rp.Headers.ETag?.ToString()??"";
                var u = (vi.metadata.Contains(Qlx.URL) ?
                    vi.metadata[Qlx.URL]?.ToString() :
                    vi.metadata.Contains(Qlx.DESC) ?
                    vi.metadata[Qlx.DESC]?.ToString() : vi.description)
                    ?? throw new DBException("2700", "URL");
                var tr = (Transaction)cx.db;
                if (et != null)
                    switch (tp & (PTrigger.TrigType.Insert | PTrigger.TrigType.Update | PTrigger.TrigType.Delete))
                    {
                        case PTrigger.TrigType.Insert:
                            if (post)
                            {
                                tr += (Transaction._ETags,tr.etags+(u,et));
                                if (PyrrhoStart.HTTPFeedbackMode)
                                        Console.WriteLine("Recording ETag " + et);
                            }
                            break;
                        case PTrigger.TrigType.Update:
                            tr += (Transaction._ETags, tr.etags + (u, et));
                            if (PyrrhoStart.HTTPFeedbackMode)
                                Console.WriteLine("Recording ETag " + et);
                            break;
                        case PTrigger.TrigType.Delete:
                            if (!post)
                            {
                                tr += (Transaction._ETags, tr.etags + (u, et));
                                if (PyrrhoStart.HTTPFeedbackMode)
                                    Console.WriteLine("Recording ETag " + et);
                            }
                            break;
                    }
                cx.db = tr;
            }
            if (PyrrhoStart.HTTPFeedbackMode)
                Console.WriteLine("--> " + rp.StatusCode);
            if (rp == null)
                throw new Exception("23000");
            if (rp.StatusCode != HttpStatusCode.OK)
            {
                var rs = new StreamReader(rp.Content.ReadAsStream());
                var s = rs.ReadToEnd();
                if (s.StartsWith("SQL Error: "))
                {
                    var sig = s.Substring(11, 5);
                    throw new DBException(sig, s[16..]);
                }
                throw new DBException("23000");
            }
            return rp;
        }
    }
    internal class TableActivation : TargetActivation
    {
        internal Table table;
        /// <summary>
        /// There may be several triggers of any type, so we manage a set of transition activations for each.
        /// These are for table before, table instead, table after, row before, row instead, row after.
        /// </summary>
        internal BTree<long, TriggerActivation>? acts = null;
        internal BTree<long, TargetActivation> casc = BTree<long, TargetActivation>.Empty;
        internal readonly CTree<PTrigger.TrigType,CTree<long, bool>> _tgs;
        internal Level3.Index? index = null; // for autokey
        internal readonly TransitionRowSet _trs;
        internal bool? trigFired = null;
        internal Level level = Level.D;
        internal QlValue? security = null;
        internal readonly Domain insertCols;
        internal BTree<long, UpdateAssignment> updates = BTree<long, UpdateAssignment>.Empty;
        internal CTree<long, CTree<long, TypedValue>> pending =
            CTree<long, CTree<long, TypedValue>>.Empty;
        internal CTree<long, TypedValue>? newRow;  // mutable
        internal TableActivation(Context cx,TableRowSet ts, RowSet data, PTrigger.TrigType tt,
           Domain? iC=null) : base(cx,ts,tt)
        {
            _cx = cx;
            insertCols = iC??Domain.Row;
            if (db == null || role==null)
                throw new PEException("PE632");
            table = (Table)(db.objects[ts.target] ?? throw new PEException("PE632"));
            table.Instance(cx.GetUid(),this); // adds table's framing to cx.obs
            _tgs = table.triggers;
            ts += (cx, RowSet._Data, data.defpos);
            _trs = new TransitionRowSet(this, ts, data);
            acts = BTree<long, TriggerActivation>.Empty;
            for (var b = _tgs.First(); b != null; b = b.Next())
                if (b.key().HasFlag(tt))
                    for (var tg = b.value()?.First(); tg != null; tg = tg.Next())
                    {
                        var t = tg.key();
                        // NB at this point obs[t] version of the trigger has the wrong action field
                        if (db.objects[t] is Trigger td)
                        {
                            Add(td);
                            var ta = new TriggerActivation(this, _trs, td);
                            acts += (t, ta);
                            Add(td); // again!
                        }
                    }
            for (var b = table.rindexes.First(); b != null; b = b.Next())
                if (db.objects[b.key()] is Table rt)
                {
                    var cp = GetUid();
                    var vs = BList<DBObject>.Empty;
                    var tr = BTree<long, long?>.Empty;
                    for (var c = rt.rowType.First(); c != null; c = c.Next())
                        if (c.value() is long p && cx.obs[p] is QlValue sc 
                            && sc.infos[role.defpos] is ObInfo ci)
                        {
                            sc = new QlInstance(GetUid(), cx, ci?.name ?? sc?.alias ?? sc?.name ?? "", cp, p);
                            Add(sc);
                            vs += sc;
                            tr += (p, sc.defpos);
                        }
                    for (var c = b.value()?.First(); c != null; c = c.Next())
                        if (c.value() is Domain xk && table.FindIndex(db, xk)?[0] is Level3.Index x)
                        {
                            var rx = rt.FindIndex(db, c.key(), PIndex.ConstraintType.ForeignKey)?[0];
                            if (x == null || rx == null)
                                continue;
                            var fl = CTree<long, TypedValue>.Empty;
                            var xm = BTree<long, long?>.Empty;
                            var pb = rx.keys.First();
                            for (var rb = x.keys.First(); pb != null && rb != null;
                                pb = pb.Next(), rb = rb.Next())
                                if (rb.value() is long rp && pb.value() is long pp)
                                xm += (rp, pp);
                            for (var d = ts.matches.First(); d != null; d = d.Next())
                                if (obs[d.key()] is QlInstance sc && xm[sc.sPos] is long xp)
                                    fl += (xp, d.value());
                            var rf = new TableRowSet(GetUid(), this, b.key(), 0L);
                            if (fl != CTree<long, TypedValue>.Empty)
                                rf += (cx, RowSet.Filter, fl);
                            Add(rx);
                            Add(rt);
                            for (var xb = rf.rowType.First(); xb != null; xb = xb.Next())
                                if (xb.value() is long xp && obs[xp] is QlInstance xc 
                                    && db.objects[xc.sPos] is TableColumn tc)
                                    Add(tc);
                            if ((obs[rf.data] ?? obs[rf.defpos]) is RowSet da && !casc.Contains(rt.defpos))
                            {
                                var ra = new TableActivation(this, rf, da, tt);
                                if (tt != PTrigger.TrigType.Insert)
                                    ra._tty = PTrigger.TrigType.Update;
                                if (tt == PTrigger.TrigType.Update)
                                    switch (rx.flags & PIndex.Updates)
                                    {
                                        case PIndex.ConstraintType.CascadeUpdate:
                                            break;
                                        case PIndex.ConstraintType.SetDefaultUpdate:
                                            for (var kb = rx.keys.rowType.Last(); kb != null; kb = kb.Previous())
                                                if (kb.value() is long p &&
                                                    ra.obs[p] is QlValue sc)
                                                    ra.updates += (p, new UpdateAssignment(p, sc.domain.defaultValue));
                                            break;
                                        case PIndex.ConstraintType.SetNullUpdate:
                                            for (var kb = rx.keys.rowType.Last(); kb != null; kb = kb.Previous())
                                                if (kb.value() is long p)
                                                    ra.updates += (p, new UpdateAssignment(p, TNull.Value));
                                            break;
                                    }
                                else if (tt == PTrigger.TrigType.Delete)
                                    switch (rx.flags & PIndex.Deletes)
                                    {
                                        case PIndex.ConstraintType.RestrictDelete:
                                        case PIndex.ConstraintType.CascadeDelete:
                                            ra._tty = PTrigger.TrigType.Delete;
                                            break;
                                        case PIndex.ConstraintType.SetDefaultDelete:
                                            for (var kb = rx.keys.rowType.Last(); kb != null; kb = kb.Previous())
                                            if (kb.value() is long p &&
                                                ra.obs[p] is QlValue sc)
                                                    ra.updates += (p, new UpdateAssignment(p, sc.domain.defaultValue));
                                            break;
                                        case PIndex.ConstraintType.SetNullDelete:
                                            for (var kb = rx.keys.rowType.Last(); kb != null; kb = kb.Previous())
                                            if (kb.value() is long p)
                                                    ra.updates += (p, new UpdateAssignment(p, TNull.Value));
                                            break;

                                    }
                                cx.nextHeap = nextHeap;
                                casc += (rt.defpos, ra);
                            }
                        }
                }
            for (var b = table.indexes.First(); index == null && b != null; b = b.Next())
                for (var c=b.value()?.First();c is not null;c=c.Next())
            if (db.objects[c.key()] is Level3.Index x && x.flags.HasFlag(PIndex.ConstraintType.PrimaryKey))
                    index = x;
            switch (tt & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    if (table.Denied(cx, Grant.Privilege.Insert))
                        throw new DBException("42105").Add(Qlx.INSERT, new TChar(table.NameFor(cx)));
//#if MANDATORYACCESSCONTROL
                    // parameter cl is only supplied when d_User.defpos==d.owner
                    // otherwise check if we should compute it
                    if (cx.db.user != null &&
                        cx.db.user.defpos != cx.db.owner && table.enforcement.HasFlag(Grant.Privilege.Insert))
                    {
                        var uc = cx.db.user.clearance;
                        if (!uc.ClearanceAllows(table.classification))
                            throw new DBException("42105").Add(Qlx.SECURITY,new TChar(table.NameFor(cx)));
                        // The new record’s classification will have the user’s minimum clearance level:
                        // if this is above D, the groups will be the subset of the user’s groups 
                        // that are in the table classification, 
                        // and the references will be the same as the table 
                        // (a subset of the user’s references)
                        level = uc.ForInsert(table.classification);
                    }
//#endif
                    break;
                case PTrigger.TrigType.Update:
                    if (table.Denied(cx, Grant.Privilege.Update))
                        throw new DBException("42105").Add(Qlx.UPDATE, new TChar(table.NameFor(cx)));
                    for (var ass = ts.assig.First(); ass != null; ass = ass.Next())
                    {
                        var ua = ass.key();
                        var vb = cx.obs[ua.vbl];
                        if (vb is SqlSecurity)
                            security = cx.obs[ua.val] as QlValue;
                        else if (ts.iSMap[ua.vbl] is long vp)
                        {
                            if (vb as QlValue == null)
                                throw new DBException("22G0X");
                            while (vb is QlInstance sc && db.objects[sc.sPos] is DBObject oc) // Views have indirection here
                                vb = oc;
                            if (vb is TableColumn tc && tc.generated.gen != Generation.No)
                                throw new DBException("22G0X", vb.NameFor(this)).Mix();
                            if (vb.Denied(cx, Grant.Privilege.Update))
                                throw new DBException("42105").Add(Qlx.UPDATE, new TChar(vb.NameFor(this)));
                            updates += (vp, ua);
                        }
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
                    if (level != Level.D || updates.Count > 0)
                    {
                        var fi = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachStatement);
                        if (fi != true)
                            Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachStatement);
                    }
                    // Do statement-level triggers
                    trigFired = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachStatement);
                    break;
                case PTrigger.TrigType.Delete:
                    if (table.Denied(cx, Grant.Privilege.Delete)
//#if MANDATORYACCESSCONTROL
                        ||
                        (table.enforcement.HasFlag(Grant.Privilege.Delete) &&
                        cx.db.user is not null && cx.db.user.clearance.minLevel > 0 &&
                        (cx.db.user.clearance.minLevel != table.classification.minLevel ||
                        cx.db.user.clearance.maxLevel != table.classification.maxLevel))
//#endif
                        )
                        throw new DBException("42105").Add(Qlx.DELETE, new TChar(table.NameFor(cx)));
                    level = user?.clearance??Level.D;
                    trigFired = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachStatement);
                    if (trigFired != true)
                        trigFired = Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachStatement);
                    break;
            }
        }
        internal override Context EachRow(Context _cx,int pos)
        {
            var cu = next?.cursors[(_trs.data>=0)?_trs.data : _trs.from];
            var trc = (cu is not null)?new TransitionRowSet.TransitionCursor(this, _trs, (Cursor)cu, pos, insertCols)
                : (TransitionRowSet.TransitionCursor?)cursors[_trs.defpos];
            if (trc == null || db==null || _cx.db==null || trc._tgc==null)
                return _cx;
            var tgc = trc._tgc;
            var rc = tgc._rec;
            var trs = _trs;
            newRow = rc?.vals;
            if (newRow==null || rc==null || trigFired == true)
                return _cx;
            switch (_tty & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    {
                        // Do row-level triggers
                        trigFired = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachRow);
                        if (trigFired != true)
                            trigFired = Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachRow);
                        if (trigFired == true) // an insteadof trigger has fired
                            return _cx;
                        var st = rc.subType;
                        Record r;
                        if (level != Level.D)
                            r = new Record3(table.defpos, newRow, st, level, _cx.db.nextPos, _cx);
                        else
                            r = new Record(table.defpos, newRow, _cx.db.nextPos, _cx);
                        Add(r);
                        _cx.Add(r);
                        var ns = newTables[_trs.defpos] ?? BTree<long, TableRow>.Empty;
                        newTables += (_trs.defpos, ns + (r.defpos, new TableRow(r,_cx)));
                        count++;
                        if (table is NodeType nt)
                            values += (r.defpos, new TNode(this,rc));
                        // install the record in the transaction
                        //      cx.tr.FixTriggeredActions(triggers, ta._tty, r);
                        // Row-level after triggers
                        values += (Trigger.NewRow, new TRow(tgc.dataType, newRow));
                        Triggers(PTrigger.TrigType.After | PTrigger.TrigType.EachRow);
                        break;
                    }
                case PTrigger.TrigType.Update:
                    {
                        var was = tgc._rec; // target uids
                        for (var b = updates.First(); b != null; b = b.Next())
                            if (b.value() is UpdateAssignment ua)
                            {
                                var tv = ua.Eval(this);
                                if (trs.transTarget[ua.vbl] is long tp)
                                {
                                    CheckMetadata(tp, tv);
                                    newRow += (tp, tv);
                                }
                                else if (trs.targetTrans.Contains(ua.vbl)) // this is a surprise
                                    newRow += (ua.vbl, tv);
                            }
                        // Step A
                        values += (Trigger.OldRow, tgc);
                        values += (Trigger.NewRow, new TRow(tgc.dataType, newRow));
                        values += newRow;
                        // Step B
                        var fi = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachRow);
                        if (fi != true)
                            fi = Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachRow);
                        if (fi == true) // an insteadof trigger has fired
                            return _cx;
                        // Step C
                        //#if MANDATORYACCESSCONTROL
                        // If Update is enforced by the table, and a record selected for update 
                        // is not one to which the user has clearance 
                        // or does not match the user’s clearance level, 
                        // throw an Access Denied exception.
                        if (table.enforcement.HasFlag(Grant.Privilege.Update)
                            && (_cx.db.user == null ||
                            (_cx.db.user.defpos != _cx.db.owner && ((rc != null) ?
                                 ((!level.ClearanceAllows(rc.classification))
                                 || level.minLevel != rc.classification.minLevel)
                                 : level.minLevel > 0))))
                            throw new DBException("42105").Add(Qlx.SECURITY, new TChar(NameFor(table.defpos)??""));
//#endif
                        // Step D
                        tgc = (TransitionRowSet.TargetCursor)(cursors[_trs.defpos] ??
                            throw new PEException("PE631"));
                        for (var b = newRow.First(); b != null; b = b.Next())
                            if (b.value() is TypedValue nv)
                            {
                                var k = b.key();
                                if (rc?.vals[k] is TypedValue v && v.CompareTo(nv) == 0)
                                    newRow -= k;
                                else
                                    trc += (this, k, nv);
                            }
                        if (next == null || trc._tgc==null)
                            throw new PEException("PE631");
                        next.cursors += (_trs.defpos, trc._tgc);
                        if (newRow.Count == 0L)
                            return _cx;
                        for (var b = casc.First(); b != null; b = b.Next())
                            if (b.value() is TableActivation ct && was!=null)
                                _cx = was.Cascade(_cx, ct, newRow);
                        var nu = tgc._rec ?? throw new PEException("PE1907");
                        var u = (security == null) ?
                                new Update(nu, table.defpos, newRow, _cx.db.nextPos, _cx) :
                                new Update1(nu, table.defpos, newRow, ((TLevel)security.Eval(_cx)).val, _cx.db.nextPos, _cx);
                        Add(u);
                        _cx.Add(u);
                        var ns = newTables[_trs.defpos] ?? BTree<long, TableRow>.Empty;
                        newTables += (_trs.defpos, ns + (nu.defpos, new TableRow(u, _cx)));
                        Triggers(PTrigger.TrigType.After | PTrigger.TrigType.EachRow);
                        break;
                    }
                case PTrigger.TrigType.Delete:
                    {
                        var fi = Triggers(PTrigger.TrigType.Before | PTrigger.TrigType.EachRow);
                        if (fi == true)
                            return _cx;
                        fi = Triggers(PTrigger.TrigType.Instead | PTrigger.TrigType.EachRow);
                        if (fi == true)
                            return _cx;
                        tgc = (TransitionRowSet.TargetCursor)(cursors[_trs.defpos] ??
                            throw new PEException("PE631"));
                        rc = tgc._rec ?? throw new PEException("PE631");
//#if MANDATORYACCESSCONTROL
                        if (_cx.db.user==null || 
                            _cx.db.user.defpos != _cx.db.owner && table.enforcement.HasFlag(Grant.Privilege.Delete) ?
                            // If Delete is enforced by the table and the user has delete privilege for the table, 
                            // but the record to be deleted has a classification level different from the user 
                            // or the clearance does not allow access to the record, throw an Access Denied exception.
                            ((!level.ClearanceAllows(rc.classification)) || level.minLevel > rc.classification.minLevel)
                            : level.minLevel > 0)
                            throw new DBException("42105").Add(Qlx.DELETE);
//#endif
                        for (var b = casc.First(); b != null; b = b.Next())
                            if (b.value() is TableActivation ct)
                                _cx = rc.Cascade(_cx,ct);
                        if (parse.HasFlag(ExecuteStatus.Detach))
                            _cx = rc.Cascade(_cx,this);
                        //      cx.tr.FixTriggeredActions(triggers, ta._tty, cx.db.nextPos);
                        var ns = newTables[_trs.defpos] ?? BTree<long, TableRow>.Empty;
                        newTables += (_trs.defpos, ns - rc.defpos);
                        var de = new Delete1(rc, _cx.db.nextPos, _cx);
                        Add(de);
                        _cx.Add(de);
                        count++;
                        break;
                    }
            }
            return _cx;
        }
        internal override Context Finish(Context cx)
        {
            _cx = cx;
            if (trigFired == true)
                return _cx;
            // Statement-level after triggers
            Triggers(PTrigger.TrigType.After | PTrigger.TrigType.EachStatement);
            _cx.result = null;
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
            for (var a = acts?.First(); r != true && a != null; a = a.Next())
                if (a.value() is TriggerActivation ta && ta._trig is not null)
                {
                    ta.cursors = cursors;
                    ta.values = values;
                    ta.db = db;
                    if (ta._trig.tgType.HasFlag(fg))
                        r = ta.Exec();
                }
            SlideDown(); // get next to adopt our changes
            return r;
        }
    }
    internal class RESTActivation : TargetActivation
    {
        internal RestRowSet _rr;
        internal RestView _vw;
        internal HttpRequestMessage? _rq;
        internal string? _targetName;
        internal BTree<long, UpdateAssignment> updates = BTree<long, UpdateAssignment>.Empty;
        internal BList<(string, string, string)> actions = BList<(string, string, string)>.Empty;
        internal RESTActivation(Context cx, RestRowSet rr, RowSet ts, PTrigger.TrigType tgt)
            : base(cx, ts, tgt)
        {
            _rr = rr; 
            _vw = (RestView)(cx.obs[_rr.restView] ?? throw new DBException("42105").Add(Qlx.VIEW));
            _tgt = _rr;
            if (_vw.infos[role.defpos] is ObInfo vi)
                (url, _targetName) = _rr.GetUrl(this, vi);
            else
                throw new DBException("42105").Add(Qlx.VIEW);
            if (_targetName != null)
            { 
                url += "/" + _targetName;
                var ub = new StringBuilder(url);
                for (var b = _rr.matches.First(); b != null; b = b.Next())
                    if (obs[b.key()] is QlValue sk)
                    {
                        var kn = sk.name;
                        ub.Append('/'); ub.Append(kn);
                        ub.Append('='); ub.Append(b.value());
                    }
                _rq = _rr.GetRequest(cx, ub.ToString(), vi);
                _rq.Method = HttpMethod.Head;
                RoundTrip(cx, _vw.defpos, tgt, _rq, url, null);
            } 
            switch (tgt & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    if (_vw.Denied(cx, Grant.Privilege.Insert))
                        throw new DBException("42105").Add(Qlx.INSERT,new TChar(NameFor(ts.target)??""));
                    break;
                case PTrigger.TrigType.Update:
                    if (_vw.Denied(cx, Grant.Privilege.Update))
                        throw new DBException("42105").Add(Qlx.UPDATE, new TChar(NameFor(ts.target)??""));
                    for (var ass = _rr.assig.First(); ass != null; ass = ass.Next())
                    if (cx.obs[ass.key().vbl] is QlValue c){
                        if (c is not QlInstance && c.GetType().Name!="QlValue")
                            throw new DBException("22G0X");
                        DBObject oc = c;
                        while (oc is QlInstance sc && cx.obs[sc.sPos] is DBObject co) // Views have indirection here
                            oc = co;
                        if (c.Denied(cx, Grant.Privilege.Update))
                            throw new DBException("42105").Add(Qlx.COLUMN, new TChar(c.name??""));
                        updates += (oc.defpos, ass.key());
                    }
                    break;
                case PTrigger.TrigType.Delete:
                    if (_vw.Denied(cx, Grant.Privilege.Delete))
                        throw new DBException("42105").Add(Qlx.DELETE, new TChar(_vw.NameFor(cx)??""));
                    break;
            }
        }
        internal override Context EachRow(Context cx, int pos)
        {
            _cx = cx;
            var cu = _cx.cursors[_fm.defpos];
            var _sql = new StringBuilder(); 
            var assig = _rr.assig;
            if (_cx.obs[_fm.rsTargets.First()?.value()??-1L] is RestRowSetUsing ru &&
                obs[ru.usingTableRowSet] is TableRowSet ut && db.objects[ut.target] is Table tb)
            {
                assig = ru.assig;
                if (_Ob(ru.urlCol) is QlValue uv)
                   Add(uv);
                var u = obs[ru.urlCol]?.Eval(this);
                if (u == null || u == TNull.Value) // can happen for Insert with a named column tree, and for Delete
                {
                    // the values tree supplied must identify the url
                    for (var b = ut.indexes.First(); b != null; b = b.Next())
                        for (var c = b.value()?.First(); c != null; c = c.Next())
                            if (db.objects[c.key()] is Level3.Index nx && cu != null
                                    && nx.flags.HasFlag(PIndex.ConstraintType.PrimaryKey)
                                    && nx.MakeKey(cu.values,ru.sIMap) is CList<TypedValue> pk
                                    && nx.rows?.PositionAt(pk, 0) is MTreeBookmark mb
                                    && mb.Value() is long pv)
                            {
                                var rc = tb.tableRows[pv];
                                var sc = (QlInstance?)obs[ru.urlCol];
                                u = rc?.vals[sc?.sPos ?? -1L];
                            }
                    if (u == null || u == TNull.Value)
                        return _cx;
                }
                url = u.ToString();
                var ix = url.LastIndexOf('/');
                _targetName = url[(ix + 1)..];
                var vi = _vw.infos[role.defpos];
                var ub = new StringBuilder(url);
                for (var b = _rr.matches.First(); b != null; b = b.Next())
                    if (_cx.obs[b.key()] is QlValue sv)
                    {
                        var kn = sv.name;
                        ub.Append('/'); ub.Append(kn);
                        ub.Append('='); ub.Append(b.value());
                    }
                _rq = _rr.GetRequest(_cx, url.ToString(), vi);
                _rq.Method = HttpMethod.Head;
                RoundTrip(_cx, _vw.defpos, _tty, _rq, ub.ToString(), null);
            }
            switch (_tty & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    {
                        if (cu == null)
                            break;
                        var vs = cu.values;
                        _sql.Append("insert into "); _sql.Append(_targetName);
                        _sql.Append(" values");
                        var cm = "(";
                        for (var b = _rr.remoteCols.First(); b != null; b = b.Next())
                            if (b.value() is long p && vs[p] is TypedValue tv)
                            {
                                _sql.Append(cm); cm = ",";
                                if (tv.dataType.kind == Qlx.CHAR)
                                {
                                    _sql.Append('\'');
                                    _sql.Append(tv.ToString().Replace("'", "'''"));
                                    _sql.Append('\'');
                                }
                                else
                                    _sql.Append(tv);
                            }
                        _sql.Append(");");
                        break;
                    }
                case PTrigger.TrigType.Update:
                    {

                        var cm = " set ";
                        if (assig.Count == 0)
                            break;
                        _sql.Append("update "); _sql.Append(_targetName);
                        for (var b = assig.First(); b != null; b = b.Next())
                            if (b.key() is UpdateAssignment ua && obs[ua.vbl] is QlValue vb
                                    && obs[ua.val] is QlValue va)
                            {
                                _sql.Append(cm); cm = ",";
                                _sql.Append(vb.name); _sql.Append('=');
                                _sql.Append(va.ToString(_rr.sqlAgent, Remotes.Operands,
                                    _rr.remoteCols, _rr.namesMap, this));
                            }
                        if (_rr.where.Count > 0 || _rr.matches.Count > 0)
                        {
                            var sw = _rr.WhereString(_cx, _rr.namesMap);
                            if (sw.Length > 0)
                            {
                                _sql.Append(" where ");
                                _sql.Append(sw);
                            }
                        }
                     }
                    break;
                case PTrigger.TrigType.Delete:
                    {
                        if (_rq == null)
                            break;
                        _sql.Append("delete from "); _sql.Append(_targetName);
                        if (_rr.where.Count > 0 || _rr.matches.Count > 0)
                        {
                            var sw = _rr.WhereString(_cx, _rr.namesMap);
                            if (sw.Length > 0)
                            {
                                _sql.Append(" where ");
                                _sql.Append(sw);
                            }
                        }
                    }
                    break;
            }
            if (url is not null && _targetName is not null)
                actions += (url, _targetName, _sql.ToString());
            return _cx;
        }
        internal override Context Finish(Context cx)
        {
            _cx = cx;
            string str;
            for (var ab = actions.First(); ab != null; ab = ab.Next())
            {
                (url, _targetName, str) = ab.value();
                _cx.AddPost(url, _targetName, str, db.user?.name??"", _vw.defpos, _tty);
            }
            return _cx;
        }
    }
    internal class HTTPActivation: TargetActivation
    {
        internal RestRowSet _rr;
        internal RestView _vw;
        internal BTree<long, UpdateAssignment> updates = BTree<long, UpdateAssignment>.Empty;
        internal HTTPActivation(Context cx, RestRowSet rr, RowSet ts, PTrigger.TrigType tgt)
            : base(cx, ts, tgt)
        {
            _rr = rr;
            _vw = (RestView)(cx.obs[_rr.restView]??throw new DBException("42105").Add(Qlx.VIEW));
            _tgt = _rr;
            switch (tgt & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    if (_vw.Denied(cx, Grant.Privilege.Insert))
                        throw new DBException("42105").Add(Qlx.INSERT, new TChar(_vw.NameFor(cx)));
                    break;
                case PTrigger.TrigType.Update:
                    if (_vw.Denied(cx, Grant.Privilege.Update))
                        throw new DBException("42105").Add(Qlx.VIEW, new TChar(_vw.NameFor(cx)));
                    for (var ass = _rr.assig.First(); ass != null; ass = ass.Next())
                    if (cx.obs[ass.key().vbl] is QlValue c){
                        if (c is not QlInstance && c.GetType().Name!="QlValue")
                            throw new DBException("22G0X");
                        DBObject oc = c;
                        while (oc is QlInstance sc && cx.obs[sc.sPos] is DBObject oo) // Views have indirection here
                            oc = oo;
                        if (oc is TableColumn tc && tc.generated != GenerationRule.None)
                            throw cx.db.Exception("22G0X", c.NameFor(cx)).Mix();
                        if (c.Denied(cx, Grant.Privilege.Update))
                            throw new DBException("42105").Add(Qlx.UPDATE, new TChar(_vw.NameFor(cx)));
                        updates += (oc.defpos, ass.key());
                    }
                    break;
                case PTrigger.TrigType.Delete:
                    if (_vw.Denied(cx, Grant.Privilege.Delete))
                        throw new DBException("42105").Add(Qlx.DELETE, new TChar(_vw.NameFor(cx)));
                    break;
            }
        }
        // perform the round trips one row at a time for now
        internal override Context EachRow(Context cx, int pos)
        {
            _cx = cx;
            var cu = cursors[_fm.defpos];
            if (cu == null)
                return _cx;
            var vs = cu.values;
            if (role is not Role ro || _vw.infos[ro.defpos] is not ObInfo vi)
                throw new DBException("42105").Add(Qlx.ROLE);
            var (url, _) = _rr.GetUrl(this, vi);
            if (url is null)
                throw new DBException("42105").Add(Qlx.URL);
            var sql = new StringBuilder();
            var rq = _rr.GetRequest(this, url, vi);
            var np = _cx.db.nextPos;
            switch (_tty & (PTrigger.TrigType)7)
            {
                case PTrigger.TrigType.Insert:
                    rq.Method = HttpMethod.Post;
                    rq.Headers.Add("Accept",_vw.mime ?? "application/json");
                    var cm = "[{";
                    var nb = _rr.namesMap.First();
                    for (var b = _rr.remoteCols.First(); b != null && nb != null;
                        b = b.Next(), nb = nb.Next())
                    {
                        sql.Append(cm); cm = ",";
                        sql.Append('"'); sql.Append(nb.value());
                        sql.Append("\":");
                        if (b.value() is long p && vs[p] is TypedValue v && v.ToString() is string s)
                            sql.Append((v is TChar) ? ("'" + s + "'") : s);
                    }
                    sql.Append("}]");
                    RoundTrip(this, _vw.defpos, _tty, rq, url, sql);
                    var nr = new RemoteTableRow(np, vs, url, _rr);
                    var ns = _cx.newTables[_vw.defpos] ?? BTree<long, TableRow>.Empty;
                    _cx.newTables += (_vw.defpos, ns + (nr.defpos, nr));
                    count++;
                    break;
                case PTrigger.TrigType.Update:
                    if (_rr.assig.Count == 0)
                        return _cx;
                    for (var b = updates.First(); b != null; b = b.Next())
                        if (b.value() is UpdateAssignment ua)
                        {
                            var tv = ua.Eval(_cx);
                            vs += (ua.vbl, tv);
                        }
                    rq.Headers.Add("Accept",_vw.mime ?? "application/json");
                    rq.Method = HttpMethod.Put;
                    cm = "[{";
                    vs = cursors[_rr.defpos]?.values??CTree<long,TypedValue>.Empty;
                    for (var b = _rr.assig.First(); b != null; b = b.Next())
                    {
                        var ua = b.key();
                        vs += (ua.vbl, ua.Eval(_cx));
                    }
                    var tb = _rr.rowType.First();
                    for (var b = _rr.remoteCols.First(); b != null && tb != null;
                        b = b.Next(), tb = tb.Next())
                        if (b.value() is long p)
                        {
                            sql.Append(cm); cm = ",";
                            sql.Append('"');
                            sql.Append(obs[p]?.NameFor(this) ?? "");
                            var tv = vs[p];
                            sql.Append("\":");
                            if (tv != null && tv.dataType.kind == Qlx.CHAR)
                            {
                                sql.Append('\'');
                                sql.Append(tv.ToString().Replace("'", "'''"));
                                sql.Append('\'');
                            }
                            else
                                sql.Append(tv);
                        }
                    sql.Append("}]");
                    RoundTrip(this, _vw.defpos, _tty, rq, url, sql);
                    var ur = new RemoteTableRow(np, vs, url, _rr);
                    var us = _cx.newTables[_vw.defpos] ?? BTree<long, TableRow>.Empty;
                    _cx.newTables += (_vw.defpos, us + (ur.defpos, ur));
                    break;
                case PTrigger.TrigType.Delete:
                    for (var rb = ((Cursor)cu).Rec()?.First(); rb != null; rb = rb.Next())
                    {
                        rq.Method = HttpMethod.Delete;
                        RoundTrip(this, _vw.defpos, _tty, rq, url, sql);
                        count++;
                    }
                    break;
            }
            return _cx;
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
        internal readonly TransitionRowSet? _trs;
        internal bool defer;
        /// <summary>
        /// The trigger definition
        /// </summary>
        internal readonly Trigger _trig;
        internal readonly BTree<long, long?>? trigTarget, targetTrig; // trigger->target, target->trigger
        /// <summary>
        /// Prepare for multiple executions of this trigger
        /// </summary>
        /// <param name="trs">The transition row set</param>
        /// <param name="tg">The trigger</param>
        internal TriggerActivation(Context cx, TransitionRowSet trs, Trigger ot)
            : base(cx, ot)
        {
            _trs = trs;
            _trig = ot; 
            parent = cx.next;
            nextHeap = cx.nextHeap;
            obs = cx.obs;
            if (cx.db.objects[ot.defpos] is Trigger t0 && t0.Instance(trs.defpos, this) is Trigger tg
                && cx.db.objects[trs.target] is Table tb)
            {
                Add(tb);
                _trig = tg;
                (trigTarget, targetTrig) = _Map(tb, tg);
                defer = _trig.tgType.HasFlag(PTrigger.TrigType.Deferred);
                if (cx.obs[tg.oldTable] is TransitionTable tt)
                    new TransitionTableRowSet(tt.defpos, cx, trs, tt, true);
                if (defer)
                    cx.deferred += this;
            }
        }
        static (BTree<long,long?>,BTree<long,long?>) _Map(Domain dm,Trigger tg)
        {
            var ma = BTree<long, long?>.Empty;
            var rm = BTree<long, long?>.Empty;
            var sb = tg.domain.First();
            for (var b = dm.rowType.First(); b != null && sb != null; b = b.Next(),
                sb = sb.Next())
                if (sb.value() is long tp && b.value() is long p)
                {
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
            if (_trs == null || next == null || _trig == null || 
                trigTarget == null || targetTrig == null || db==null)
                return false;
            var rp = _trs.defpos;
            var trc = (TransitionRowSet.TransitionCursor?)next.next?.cursors[rp];
            Cursor? cu = null;
            if (trc is not null && trc._tgc is not null) // row-level trigger
                cu = new TransitionRowSet.TriggerCursor(this, trc._tgc);
            if (defer)
                return false;
            if (_trig.oldRow != -1L && next.values[Trigger.OldRow] is TypedValue ot)
                values += (_trig.oldRow, ot);
            if (_trig.newRow != -1L && next.values[Trigger.NewRow] is TypedValue nt)
                values += (_trig.newRow, nt);
            if (obs[_trig.oldTable] is TransitionTable ott){
                next.Add(ott);
                Add(new TransitionTableRowSet(ott.defpos, next, _trs,
                    ott, true));
            }
            if (obs[_trig.newTable] is TransitionTable ntt)
            {
                next.Add(ntt);
                Add(new TransitionTableRowSet(ntt.defpos, next, _trs, ntt, false));
            }
            if (obs[_trig.action] is WhenPart wp)
            {
                var tc = obs[wp.cond]?.Eval(this);
                if (tc != TBool.False)
                {
                    db += (Transaction.TriggeredAction, db.nextPos);
                    Add(new TriggeredAction(_trig.ppos, db.nextPos));
                    if (next.values[_trs.target] is TypedValue v) // the TriggerCursor (if any)
                    {
                        values += (_trs.target, v);
                        values += _trig.Frame(next.values);
                        var nx = Executable.ObeyList(wp.stms,"",null,this);
                        if (nx != this)
                            throw new PEException("PE677");
                        if (trc != null) // row-level trigger 
                        {
                            var ta = (TableActivation)next;
                            for (var b = cu?.dataType.rowType.First(); b != null; b = b.Next())
                                if (b.value() is long p)
                                {
                                    var tv = values[p];
                                    if (tv is not null && tv != cu?[p] && ta.newRow != null && trigTarget.Contains(p)
                                            && trigTarget[p] is long tp) // notify the TableActivation
                                        ta.newRow += (tp, tv);
                                }
                        }
                    }
                }
                SlideDown();
                if (tc != TBool.False && _trig.tgType.HasFlag(PTrigger.TrigType.Instead))
                    return true;
            }
            return false;
        }
        internal override TriggerActivation FindTriggerActivation(long tabledefpos)
        {
            if (obs[obs[_trs?.defpos ?? -1L]?.from ?? -1L] is RowSet fm
                && tr?.objects[fm.target] is Table t && t?.defpos == tabledefpos)
                return this;
            return base.FindTriggerActivation(tabledefpos);
        }
        protected override void Debug(StringBuilder sb)
        { }
        internal override Context SlideDown()
        {
            if (next == null)
                throw new DBException("42105").Add(Qlx.EXECUTE);
            next.values = values;
            next.warnings += warnings;
            next.deferred += deferred;
            next.nextHeap = Math.Max(next.nextHeap,nextHeap);
            next.lastret = lastret;
            if (db != next.db)
            {
                var nd = db;
                if (db.role != next.db.role)
                    nd += (Database.Role, next.db.role);
                next.db = nd; // adopt the transaction changes done by this
            }
            return next;
        }
    }
}
