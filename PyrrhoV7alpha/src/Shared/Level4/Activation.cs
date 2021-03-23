using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level2;
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
            if (PyrrhoStart.DebugMode && next.db!=db)
            {
                var ps = ((Transaction)db).physicals;
                var ns = ((Transaction)next.db).physicals;
                var sb = new System.Text.StringBuilder("SD: "+GetType().Name+" "+cxid);
                Debug(sb);
                var nb = ns.First();
                for (var b = ps.First(); b != null; b = b.Next(), nb = nb?.Next())
                {
                    var p = b.key();
                    for (; nb != null && nb.key() < p; nb = nb.Next())
                        nb = nb.Next();
                    if (nb != null && nb.key() == p)
                    {
                        nb = nb.Next();
                        continue;
                    }
                    sb.Append(" " + b.value().ToString());
                }
                System.Console.WriteLine(sb.ToString());
            }
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
        internal readonly TransitionRowSet _trs;
        internal readonly RowSet _fm;
        internal readonly ObInfo _ti;
        internal readonly long _tgt;
        internal readonly CTree<long, RowSet.Finder> _finder;
        internal PTrigger.TrigType _tty; // may be Insert
        internal TargetActivation(Context _cx, RowSet fm, PTrigger.TrigType tt)
            : base(_cx, "")
        {
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
    }
    internal class TableActivation : TargetActivation
    {
        /// <summary>
        /// There may be several triggers of any type, so we manage a set of transition activations for each.
        /// These are for table before, table instead, table after, row before, row instead, row after.
        /// </summary>
        internal BTree<long, TriggerActivation> acts = null;
        internal readonly CTree<PTrigger.TrigType,CTree<long, bool>> _tgs;
        internal Index index = null; // for autokey
        internal TableActivation(Context _cx,RowSet fm,PTrigger.TrigType tt)
            : base(_cx,fm,tt)
        {
            var tb = (Table)_cx.obs[_tgt];
            _tgs = tb.triggers;
            acts = BTree<long, TriggerActivation>.Empty;
            for (var b = _tgs.First(); b != null; b = b.Next())
                if (b.key().HasFlag(tt))
                    for (var tg = b.value().First(); tg != null; tg = tg.Next())
                    {
                        var t = tg.key();
                        // NB at this point obs[t] version of the trigger has the wrong action field
                        var td = (Trigger)db.objects[t];
                        var ta = new TriggerActivation(this, _trs, td);
                        acts += (t, ta);
                        ta.Frame(t);  
                        ta.obs += (t, td);
                    }
            if (_trs.indexdefpos >= 0)
                index =  (Index)_cx.db.objects[_trs.indexdefpos];
        }
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
        /// <summary>
        /// Perform the triggers in a set. 
        /// </summary>
        /// <param name="acts"></param>
        internal bool Exec()
        {
            var r = false;
            db = next.db;
            nextHeap = next.nextHeap;
            bool skip;
            for (var a = acts?.First(); a != null; a = a.Next())
            {
                var ta = a.value();
                ta.db = db;
                ta.nextHeap = nextHeap;
                skip = ta.Exec();
                r = r || skip;
            }
            return r;
        }
        protected override void Debug(System.Text.StringBuilder sb)
        {
            sb.Append(" " + _ti.name);
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
        internal bool deferred;
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
            deferred = _trig.tgType.HasFlag(Level2.PTrigger.TrigType.Deferred);
            if (cx.obs[tg.oldTable] is TransitionTable tt)
                new TransitionTableRowSet(tt.defpos,cx,trs,
                    tg.framing.obs[tt.defpos].domain,true);
            if (deferred)
                cx.db += (Transaction.Deferred, cx.tr.deferred + this);
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
        static (SqlRow,BTree<long,long>) _Map(ObInfo oi,SqlValue sv)
        {
            var ma = BTree<long, long>.Empty;
            var sb = sv.columns.First();
            for (var b = oi.domain.rowType.First(); b != null && sb != null; b = b.Next(), 
                sb = sb.Next())
                ma += (sb.value(), b.value());
            return ((SqlRow)sv, ma);
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
            if (deferred)
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
