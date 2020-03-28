using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level4;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
// 
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    /// <summary>
    /// Activations provide a context for execution of stored procedure code and triggers
    /// and nested declaration contexts (such as For Statements etc). Activations always run
    /// with definer's privileges.
    /// </summary>
    internal class Activation : Context
    {
        internal string label;
        internal Domain domain;
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
        }
        /// <summary>
        /// Constructor: a new activation for a Procedure. See CalledActivation constructor.
        /// The blockid for a CalledActivation is always the nameAndArity of the routine (e.g. MyFunc$3).
        /// </summary>
        /// <param name="cx">The current context</param>
        /// <param name="pr">The procedure</param>
        /// <param name="n">The headlabel</param>
        protected Activation(Context cx,Procedure pr, string n)
            : base(cx,cx.db.objects[pr.definer] as Role,cx.user)
        {
            next = cx;
            nextHeap = cx.nextHeap;
            label = n;
            domain = pr.domain;
        }
        protected Activation(Context cx,long definer,string n)
            :base(cx,cx.db.objects[definer] as Role,cx.user)
        {
            next = cx;
            nextHeap = cx.nextHeap;
            label = n;
        }
        internal override void SlideDown(Context was)
        {
            for (var b = locals.First(); b != null; b = b.Next())
                if (was.values.Contains(b.key()))
                    values += (b.key(), was.values[b.key()]);
            val = was.val;
            base.SlideDown(was);
        }
        internal override TypedValue AddValue(DBObject s, TypedValue tv)
        {
            for (var ac = this; ac != null; ac = ac.next as Activation)
                if (ac is TriggerActivation ta && ac.from.Contains(s.defpos))
                {
                    var p = ac.from[s.defpos];
                    if (ac.cursors[p] is TransitionRowSet.TransitionCursor cu)
                        ac.cursors += (p, cu + (this, ta._trig.from.rowType, s.defpos, tv));
                }
            return base.AddValue(s,tv);
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
        public override string ToString()
        {
            return "Activation " + cxid;
        }
    }
    internal class CalledActivation : Activation
    {
        internal Procedure proc = null;
        internal Domain udt = null;
        internal Method cmt = null;
        internal ObInfo udi = null;
        public CalledActivation(Context cx, Procedure p,Domain ot)
            : base(cx, p, ((ObInfo)cx.db.role.obinfos[p.defpos]).name)
        { 
            proc = p; udt = ot;
            if (p is Method mt)
            {
                cmt = mt;
                udi = (ObInfo)tr.role.obinfos[cmt.udType.defpos];
                for (var b = udi.columns.First(); b != null; b = b.Next())
                {
                    var iv = b.value();
                    locals += (iv.defpos, true);
                    cx.Add(iv);
                }
            }
        }
        internal override TypedValue Ret()
        {
            if (udi!=null)
                return new TRow(udi.domain,values);
            return base.Ret();
        }
        public override string ToString()
        {
            return "CalledActivation " + proc.defpos;
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
        internal BTree<long, Index> oldIndexes = BTree<long, Index>.Empty;
        internal BTree<long, TableRow> oldRows = BTree<long, TableRow>.Empty;
        internal bool deferred;
        /// <summary>
        /// The trigger definition
        /// </summary>
        internal readonly Trigger _trig;
        /// <summary>
        /// Prepare for multiple executions of this trigger
        /// </summary>
        /// <param name="trs">The transition row set</param>
        /// <param name="tg">The trigger</param>
        internal TriggerActivation(Context _cx, TransitionRowSet trs, Trigger tg)
            : base(_cx, tg.definer, tg.name)
        {
            _trs = trs;
            parent = _cx.next;
            var fm = trs.from;
            var t = _cx.db.objects[fm.target] as Table;
            oldRows = t.tableRows;
            for (var b = t.indexes.First(); b != null; b = b.Next())
                oldIndexes += (b.value(), (Index)_cx.db.objects[b.value()]);
            _trig = (Trigger)tg.Frame(this);
            deferred = _trig.tgType.HasFlag(Level2.PTrigger.TrigType.Deferred);
            if (deferred)
                _cx.db += (Transaction.Deferred, _cx.tr.deferred + this); 
            domain = _cx.db.role.obinfos[t.defpos] as Domain;
        }
        /// <summary>
        /// Execute the trigger for the current row or table, using the definer's role
        /// </summary>
        /// <returns>whether the trigger was fired (i.e. WHEN condition if any matched)</returns>
        internal (Context,bool) Exec(Context cx,TransitionRowSet.TransitionCursor row)
        {
            if (deferred)
                return (cx, false);
            values += cx.values;
            data += cx.data;
            if (_trig.oldTable != null)
                data += (_trig.oldTable.defpos,_trs);
            if (_trig.oldRow != null)
                data += (_trig.oldRow.defpos,_trs);
            if (row!=null)
                values += (row.dataType.defpos,cx.values[_trs.from.defpos]);
            var rs = data[_trig.from.defpos];
            from = rs.finder;
            if (row!=null) // row triggers have cursors 
                cursors += (_trig.from.defpos, // trigger-side version of transition cursor
                    new TransitionRowSet.TransitionCursor(this, _trig.from.rowType, row));
            var ta = _trig.action;
            var tc = ta.cond?.Eval(cx);
            if (tc != TBool.False)
            {
                var oa = cx.tr.triggeredAction;
                cx.db += (Transaction.TriggeredAction, cx.db.nextPos);
                cx.Add(new Level2.TriggeredAction(_trig.defpos, cx.db.nextPos, cx));
                var nx = ta.stms.First().value().Obey(this);
                if (nx != this)
                    throw new PEException("PE677");
                var vs = BTree<long, TypedValue>.Empty;
                Cursor nc = (row==null)?null:cursors[_trig.from.defpos];
                for (var b=_trig.from.rowType.First();b!=null;b=b.Next())
                { // updated values for transition-side transition cursor
                    var s = b.value();
                    var p = (s is SqlCopy sc) ? sc.copyFrom : s.defpos;
                    if (nc!=null)
                     vs += (p, nc[s.defpos]);
                }
                cx.values += vs; 
                cx.SlideDown(this);
                if (row!=null)
                    cx.cursors += (row._trs.defpos,
                        new TransitionRowSet.TransitionCursor(this,row,vs));
                cx.db += (Transaction.TriggeredAction, oa);
            }
            return (cx,tc!=TBool.False && _trig.tgType.HasFlag(Level2.PTrigger.TrigType.Instead));
        }
        internal override TriggerActivation FindTriggerActivation(long tabledefpos)
        {
            var fm = _trs.from;
            var t = tr.objects[fm.target] as Table;
            return (t.defpos == tabledefpos) ? this : base.FindTriggerActivation(tabledefpos);
        }
    }
}
