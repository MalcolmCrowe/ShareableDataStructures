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
        internal Domain nominalDataType;
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
            : base(cx,tr.roles[pr.definer],cx.user)
        {
            label = n;
            nominalDataType = pr.retType;
        }
        protected Activation(Transaction tr,Context cx,long definer,string n)
            :base(cx,tr.roles[definer],cx.user)
        {
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
            : base(tr, cx, p, p.name)
        { proc = p; owningType = ot; }
        public override string ToString()
        {
            return "CalledActivation " + proc.name + " "+cxid;
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
        internal BTree<long, TypedValue> oldRow=BTree<long,TypedValue>.Empty;
        internal BTree<long, TypedValue> newRow=BTree<long,TypedValue>.Empty; 
        /// <summary>
        /// The trigger definition
        /// </summary>
        internal readonly Trigger _trig;
        /// <summary>
        /// The names of properties etc as seen by the definer of this trigger
        /// </summary>
        internal readonly BTree<Ident,long?> _props;
        /// <summary>
        /// Prepare for multiple executions of this trigger
        /// </summary>
        /// <param name="trs">The transition row set</param>
        /// <param name="tg">The trigger</param>
        internal TriggerActivation(Context _cx, TransitionRowSet trs, Trigger tg)
            : base(trs._tr,_cx, tg.definer, tg.name)
        {
            _trs = trs;
            _trig = tg;
            var fm = trs.qry as From;
            var tr = trs._tr;
            var t = fm.target as Table;
            nominalDataType = t;
        }
        /// <summary>
        /// Execute the trigger for the current row or table, using the definer's context
        /// </summary>
        /// <returns>whether the trigger was fired (i.e. WHEN condition if any matched)</returns>
        internal Transaction Exec(Transaction tr, Context ox)
        {
            bool r = false;
            var cx = new Context(ox, tr.role, tr.user);
            var trig = _trig;
            tr+=new Level2.TriggeredAction(_trig.defpos,tr.uid,tr);
            var ta = _trig.action;
            tr = ta.First().value().Obey(tr,cx);
            return tr;
        }

    }
}
