using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level4;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
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
        protected Activation(Transaction tr,Context cx,Procedure pr, string n)
            : base(cx,tr.objects[pr.definer] as Role,cx.user)
        {
            next = cx;
            top = cx.top;
            label = n;
            domain = pr.retType;
        }
        protected Activation(Transaction tr,Context cx,long definer,string n)
            :base(cx,tr.objects[definer] as Role,cx.user)
        {
            next = cx;
            top = cx.top;
            label = n;
        }
        internal override void SlideDown(Context was)
        {
            for (var b = locals.First(); b != null; b = b.Next())
                if (was.values.Contains(b.key()))
                    values += (b.key(), was.values[b.key()]);
            if (was is Activation a && a.breakto != null && a.breakto!=this)
                    breakto = a.breakto;
            base.SlideDown(was);
        }
        /// <summary>
        /// flag NOT_FOUND if there is a handler for it
        /// </summary>
        internal override void NoData(Transaction tr)
        {
            if (exceptions.Contains("02000"))
                new Signal(tr.uid,"02000",cxid).Obey(tr,this);
            else if (exceptions.Contains("NOT_FOUND"))
                new Signal(tr.uid,"NOT_FOUND", cxid).Obey(tr,this);
            else if (next != null)
                next.NoData(tr);
        }
        public override string ToString()
        {
            return "Activation " + cxid;
        }
    }
    internal class CalledActivation : Activation
    {
        internal Procedure proc = null;
        internal Domain owningType = null;
        public CalledActivation(Transaction tr, Context cx, Procedure p,Domain ot)
            : base(tr, cx, p, ((ObInfo)tr.role.obinfos[p.defpos]).name)
        { proc = p; owningType = ot; }
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
        internal BTree<long, object> oldRows = BTree<long, object>.Empty;
        internal BTree<long, TypedValue> newRow;
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
            : base(trs._tr as Transaction,_cx, tg.definer, tg.name)
        {
            _trs = trs;
            parent = _cx.next;
            newRow = parent?.row.values;
            var fm = trs.qry as From;
            var t = trs._tr.objects[fm.target] as Table;
            oldRows = t.tableRows;
            for (var b = t.indexes.First(); b != null; b = b.Next())
                oldIndexes += (b.value(), (Index)trs._tr.objects[b.value()]);
            _trig = (Trigger)tg.Frame(this);
            domain = trs._tr.role.obinfos[t.defpos] as Domain;
            if (tg.oldRow != null)
                defs += (tg.oldRow, new ObInfoOldRow(fm.rowType,trs.qry.defpos));
            if (tg.newRow != null)
                defs += (tg.newRow, fm.rowType);
            if (tg.oldTable != null)
                defs += (tg.oldTable, new FromOldTable(tg.oldTable,fm));
            if (tg.newTable != null)
                defs += (tg.newTable, fm);
        }
        public static TriggerActivation operator+(TriggerActivation a,BTree<long,TypedValue>nv)
        {
            for (var b = nv?.First(); b != null; b = b.Next())
                a.newRow += (b.key(), b.value());
            return a;
        }
        /// <summary>
        /// Execute the trigger for the current row or table, using the definer's context
        /// </summary>
        /// <returns>whether the trigger was fired (i.e. WHEN condition if any matched)</returns>
        internal (Transaction,bool) Exec(Transaction tr, Context cx)
        {
            row = null;
            values = cx.values;
            data = cx.data;
            if (cx.row!=null)
                values += (cx.row.info.defpos,cx.values[_trs.from.defpos]);
            var ta = _trig.action;
            var tc = ta.cond?.Eval(tr, cx);
            if (tc != TBool.False)
            {
                var oa = tr.triggeredAction;
                tr += (Transaction.TriggeredAction, tr.nextPos);
                tr += new Level2.TriggeredAction(_trig.defpos, tr);
                tr = ta.stms.First().value().Obey(tr, this);
        //        if (parent != null)
       //            for (var b = affected.First(); b != null; b = b.Next())
       //                 parent.affected += b.value();
                tr += (Transaction.TriggeredAction, oa);
            }
            return (tr,tc!=TBool.False && _trig.tgType.HasFlag(Level2.PTrigger.TrigType.Instead));
        }
        internal override TriggerActivation FindTriggerActivation(long tabledefpos)
        {
            var fm = _trs.qry as From;
            var t = _trs._tr.objects[fm.target] as Table;
            return (t.defpos == tabledefpos) ? this : base.FindTriggerActivation(tabledefpos);
        }
    }
}
