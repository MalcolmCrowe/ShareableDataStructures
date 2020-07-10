using Pyrrho.Common;
using Pyrrho.Level3;
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
            for (var b = values.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (!locals.Contains(k))
                    next.values += (k, values[k]);
   /*             if (next.from.Contains(k))
                {
                    var rs = from[k];
                    next.cursors += (rs,next.cursors[rs]+(next,k,b.value()));
                } */
            }
            next.val = val;
            next.nextHeap = nextHeap;
            next.nextStmt = nextStmt;
            next.cursors = cursors;
            next.db = db; // adopt the transaction changes done by this
            return next;
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
                locals += (b.value().Item1, true);
            if (p is Method mt)
            {
                cmt = mt;
                udi = (ObInfo)tr.role.infos[cmt.udType.defpos];
                for (var b = udi.rowType?.First(); b != null; b = b.Next())
                {
                    var iv = b.value().Item1;
                    locals += (iv, true);
                    cx.Add((DBObject)tr.objects[iv]);
                }
            }
        }
        internal override TypedValue Ret()
        {
            if (udi!=null)
                return new TRow(udi,values);
            return base.Ret();
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
        internal readonly SqlRow oldRow, newRow;
        internal readonly BTree<long, long> oldMap, newMap;
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
            obs += (tg.framing,true);
            var tb = (Table)cx.db.objects[trs.from.target];
            var oi = cx._RowType(tb.defpos);
            cx.obs += (tb.defpos, tb);
            _trig = tg;
            deferred = _trig.tgType.HasFlag(Level2.PTrigger.TrigType.Deferred);
            if (cx.obs[tg.oldRow] is SqlRow so)
                (oldRow,oldMap) = _Map(oi,so);
            if (cx.obs[tg.newRow] is SqlRow sn)
                (newRow,newMap) = _Map(oi,sn);
            if (cx.obs[tg.oldTable] is TransitionTable tt)
                new TransitionTableRowSet(tt.defpos,cx,trs);
            if (deferred)
                cx.db += (Transaction.Deferred, cx.tr.deferred + this);
        }
        static (SqlRow,BTree<long,long>) _Map(RowType oi,SqlValue sv)
        {
            var ma = BTree<long, long>.Empty;
            var sb = sv.rowType?.First();
            for (var b = oi?.First(); b != null && sb != null; b = b.Next(), sb = sb.Next())
                ma += (b.value().Item1, sb.value().Item1);
            return ((SqlRow)sv, ma);
        }
        /// <summary>
        /// Execute the trigger for the current row or table, using the definer's role.
        /// We pass in the current transition.targetRow as a TargetCursor just in case.
        /// </summary>
        /// <returns>whether the trigger was fired (i.e. WHEN condition if any matched)</returns>
        internal (TransitionRowSet.TargetCursor,bool) Exec(Context cx,TransitionRowSet.TargetCursor row)
        {
            if (deferred)
                return (row, false);
            values += (cx.values,false); 
            data += (cx.data,false);
            from = _trs.finder;
            cursors = cx.cursors;
            if (row != null)
            {
                cursors += (_trs.defpos, row);
                data -= _trs.defpos; // as it doesn't match the TargetCursor
                if (oldRow != null)
                    values += (_trig.oldRow, _Row(oldRow, row._oldVals));
                if (newRow != null)
                    values += (_trig.newRow, _Row(newRow, row.values));
            }
            if (_trig.oldTable!= -1L)
                data += (_trig.oldTable, new TransitionTableRowSet(_trig.oldTable,cx,_trs));
            if (_trig.newTable!= -1L)
                data += (_trig.newTable, new TransitionTableRowSet(_trig.newTable, cx, _trs.defpos));
            var ta = (WhenPart)obs[_trig.action];
            var tc = cx.obs[ta.cond]?.Eval(cx);
            if (tc != TBool.False)
            {
                var oa = cx.tr.triggeredAction;
                cx.db += (Transaction.TriggeredAction, cx.db.nextPos);
                cx.Add(new Level2.TriggeredAction(_trig.defpos, cx.db.nextPos, cx));
                var nx = ((Executable)obs[ta.stms.First().value()]).Obey(this);
                if (nx != this)
                    throw new PEException("PE677");
                if (row != null && _trig.newRow > 0)
                {
                    var v = (SqlNewRow)obs[_trig.newRow];
                    var vs = values[_trig.newRow];
                    for (var b = v.rowType?.First(); b != null; b = b.Next())
                    {
                        var c = obs[b.value().Item1];
                        var p = (c is SqlCopy sc) ? sc.copyFrom : c.defpos;
                        row += (cx, p, vs[c.defpos]);
                    }
                }
                cx = SlideDown();
                cx.db += (Transaction.TriggeredAction, oa);
            }
            if (tc != TBool.False && _trig.tgType.HasFlag(Level2.PTrigger.TrigType.Instead))
                return (row, true);
            return (row, false);
        }
        TRow _Row(SqlRow sr,BTree<long,TypedValue>vals)
        {
            var vs = BTree<long, TypedValue>.Empty;
            for (var b=sr.rowType?.First();b!=null;b=b.Next())
            {
                var p = b.value().Item1;
                vs += (p, vals[((SqlCopy)obs[p]).copyFrom]);                
            }
            return new TRow(sr, vs);
        }
        /// <summary>
        /// next is targetAc, with targetCursor. Update it using newRow if defined
        /// </summary>
        /// <returns></returns>
        internal override Context SlideDown()
        {
            if (cursors[_trs.defpos] is TransitionRowSet.TargetCursor cu)
            {
                if (values[_trig.newRow] is TRow nr)
                    for (var b = nr.values.First(); b != null; b = b.Next())
                    {
                        var k = b.key();
                        var v = b.value();
                        if (cu[k]!=v)
                            cu += (this, k, v);
                    }
                next.cursors += (_trs.defpos, cu);
            }
            return base.SlideDown();
        }
        internal override TriggerActivation FindTriggerActivation(long tabledefpos)
        {
            var fm = _trs.from;
            var t = tr.objects[fm.target] as Table;
            return (t.defpos == tabledefpos) ? this : base.FindTriggerActivation(tabledefpos);
        }
    }
}
