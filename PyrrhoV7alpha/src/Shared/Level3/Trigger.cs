using System;
using System.ComponentModel;
using System.Management.Instrumentation;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
    /// A Trigger database object
    /// // shareable as at 26 April 2021
    /// </summary>
    internal class Trigger : DBObject
	{
        internal const long
            Action = -290, // long Executable
            NewRow = -293, // long Cursor
            NewTable = -294, // long RowSet
            OldRow = -295, // long Cursor
            OldTable = -296, // long RowSet
            TrigPpos = -299, // long Trigger
            TrigType = -297, // PTrigger.TrigType
            UpdateCols = -298; // CList<long> SqlValue
        public long table => (long)mem[From.Target];
        /// <summary>
        /// The trigger type (flags)
        /// </summary>
		public PTrigger.TrigType tgType=> (PTrigger.TrigType)mem[TrigType];
        /// <summary>
        /// The list of update TableColumns
        /// </summary>
		public CList<long> cols => (CList<long>)mem[UpdateCols]??CList<long>.Empty;
        /// <summary>
        /// the name of the old row
        /// </summary>
        public long oldRow => (long)(mem[OldRow]??-1L);
        /// <summary>
        /// the name of the new row
        /// </summary>
		public long newRow =>(long)(mem[NewRow]??-1L);
        public long oldTable => (long)(mem[OldTable]??-1L);
        /// <summary>
        /// the name of the new table
        /// </summary>
		public long newTable => (long)(mem[NewTable]??-1L);
        public long action => (long)(mem[Action]??-1L);
        public long ppos => (long)(mem[TrigPpos] ?? -1L);
        /// <summary>
        /// A new Trigger from the PhysBase
        /// </summary>
		public Trigger(PTrigger p,Role ro)
            : base(p.ppos, 
                  _Mem(p) + (Action,p.def) + (Name,p.name)
                  + (Definer, ro.defpos) + (_From, p.from) + (TrigPpos, p.ppos)
                  + (_Framing, p.framing) + (From.Target, p.target) + (TrigType, p.tgtype)
                  + (UpdateCols, p.cols) + (LastChange, p.ppos))
		{ }
        public Trigger(long defpos, BTree<long, object> m) : base(defpos, m) 
        { }
        static BTree<long,object> _Mem(PTrigger p)
        {
            var r = new BTree<long, object>(_Domain, p.dataType.defpos);
            if (p.oldTable != null)
                r += (OldTable, p.oldTable.iix.dp);
            if (p.newTable != null)
                r += (NewTable, p.newTable.iix.dp);
            if (p.oldRow != null)
                r += (OldRow, p.oldRow.iix.dp);
            if (p.newRow != null)
                r += (NewRow, p.newRow.iix.dp);
            return r;
        }
        public static Trigger operator+(Trigger t,(long,object)x)
        {
            return (Trigger)t.New(t.mem + x);
        }
        /// <summary>
        /// a string representation of the trigger
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
		{
            var sb = new StringBuilder(base.ToString());
            sb.Append(" TrigType=");sb.Append(tgType);
            sb.Append(" On=");sb.Append(Uid(table));
            if (defpos >= Transaction.HeapStart)
            {
                sb.Append(" From: "); sb.Append(Uid(from));
                sb.Append(" Action:"); sb.Append(Uid(action));
                if (cols != null && cols!=CList<long>.Empty)
                {
                    sb.Append(" UpdateCols:");
                    var cm = '(';
                    for (var i = 0; i < cols.Count; i++)
                    { sb.Append(cm); cm = ','; sb.Append(cols[i]); }
                }
                if (oldRow != -1L) { sb.Append(" OldRow="); sb.Append(Uid(oldRow)); }
                if (newRow != -1L) { sb.Append(" NewRow="); sb.Append(Uid(newRow)); }
                if (oldTable != -1L) { sb.Append(" OldTable="); sb.Append(Uid(oldTable)); }
                if (newTable != -1L) { sb.Append(" NewTable="); sb.Append(Uid(newTable)); }
            }
            return sb.ToString();
		}

        internal override Basis New(BTree<long, object> m)
        {
            return new Trigger(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Trigger(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = base._Replace(cx, so, sv);
            if (table == so.defpos)
                r += (From.Target, sv.defpos);
            var a = cx.ObReplace(action, so, sv);
            if (a != action)
                r += (Action, a);
            var ch = false;
            var cs = CList<long>.Empty;
            for (var b = cols?.First(); b != null; b = b.Next())
            {
                var c = cx.ObReplace(b.value(), so, sv);
                cs += c;
                ch = ch || c != b.value();
            }
            if (ch)
                r += (UpdateCols, cs);
            var o = cx.ObReplace(oldRow, so, sv);
            if (o != oldRow)
                r += (OldRow, o);
            o = cx.ObReplace(newRow, so, sv);
            if (o != newRow)
                r += (NewRow, o);
            o = cx.ObReplace(oldTable, so, sv);
            if (o != oldTable)
                r += (OldTable, o);
            o = cx.ObReplace(newTable, so, sv);
            if (o != newTable)
                r += (NewTable, o);
            if (r!=this)
                r = New(cx, r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            var tb = (Table)nd.objects[table];
            var tgs = BTree<PTrigger.TrigType, BTree<long, bool>>.Empty;
            for (var b=tb.triggers.First();b!=null;b=b.Next())
            {
                var ts = BTree<long, bool>.Empty;
                var ch = false;
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (c.key() != defpos)
                        ts += (c.key(), true);
                    else
                        ch = true;
                if (ch)
                    tgs += (b.key(), ts);
            }
            tb += (Table.Triggers, tgs);
            nd += (tb, p);
            return base.Drop(d, nd, p);
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (Trigger)base._Fix(cx);
            var na = cx.Fix(action);
            if (na != action)
                r += (Action, na);
            var nr = cx.Fix(newRow);
            if (newRow != nr)
                r += (NewRow, nr);
            var nt = cx.Fix(newTable);
            if (newTable != nt)
                r += (NewTable, nt);
            var no = cx.Fix(oldRow);
            if (oldRow != no)
                r += (OldRow, no);
            var nu = cx.Fix(oldTable);
            if (oldTable != nu)
                r += (OldTable, nu);
            var nc = cx.Fix(cols);
            if (nc != cols)
                r += (UpdateCols, nc);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r= (Trigger)base._Relocate(cx);
            r += (Action, cx.Fix(action));
            r += (NewRow, cx.Fix(newRow));
            r += (NewTable, cx.Fix(newTable));
            r += (OldRow, cx.Fix(oldRow));
            r += (OldTable, cx.Fix(oldTable));
            r += (UpdateCols, cx.Fix(cols));
            return r;
        }
    }
    /// <summary>
    /// Transition tables are not listed in roles but referred to in triggers
    /// </summary>
    internal class TransitionTable : From
    {
        internal const long
            ColIds = -304, // CList<long> TableColumn
            Old = -327, // bool
            Trig = -326; // long
        internal CList<long> colIds => (CList<long>)mem[ColIds] ?? CList<long>.Empty;
        internal bool old => (bool)mem[Old];
 //       internal long trig => (long)mem[Trig];
        internal TransitionTable(Ident ic, bool old, Context cx, From fm, Trigger tg)
                : base(ic.iix.dp,_Mem(cx, ic, fm) + (Old, old) + (Trig, tg.defpos)
                      +(IIx,new Iix(fm.iix,ic.iix.dp)))
        { }
        protected TransitionTable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static TransitionTable operator+(TransitionTable t,(long,object)x)
        {
            return (TransitionTable)t.New(t.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionTable(defpos,m);
        }
        static BTree<long,object> _Mem(Context cx,Ident ic,From fm)
        {
            var cs = CList<long>.Empty;
            var vs = BList<SqlValue>.Empty;
            var ds = CTree<long, bool>.Empty;
            var d = 1+fm.depth;
      /*      for (var b = cx._Dom(fm).rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var c = (SqlValue)cx.obs[p];
                var u = cx.GetIid();
                var v = new SqlCopy(u, cx, c.name, ic.iix.dp, p);
                cx.Add(v);
                vs += v;
                cs += v.defpos;
                ds += (u.dp, true);
                d = _Max(d, 1 + v.depth);
            } 
            var nd = new Domain(cx.GetUid(),cx,Sqlx.ROW,vs);
            cx.Add(nd); */
            return BTree<long, object>.Empty + (_Domain, fm.domain) 
                    + (Name, ic.ident)
                  + (Dependents, ds) + (_Depth, d) + (ColIds, cs)
                  + (Target, fm.target);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TransitionTable(dp,mem);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r =  (TransitionTable)base._Relocate(cx);
     //       r += (Trig, cx.Fix(trig));
            r += (ColIds, cx.Fix(colIds));
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (TransitionTable)base._Fix(cx);
    //        var nt = cx.Fix(trig);
    //        if (nt != trig)
    //            r += (Trig, nt);
            var nc = cx.Fix(colIds);
            if (nc != colIds)
                r += (ColIds, nc);
            return r;
        }
   /*     internal override void _Add(Context cx)
        {
            // don't call the base
            var tg = (Trigger)(cx.obs[trig]);
            var tb = ((Table)cx.db.objects[tg.table]);
            cx.Add(tb);
            cx.obs += (defpos, this);
            var dp = cx.depths[depth] ?? ObTree.Empty;
            cx.depths += (depth, dp + (defpos, this));
        } */
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(old ? " old" : " new");
      //      sb.Append(" for "); sb.Append(Uid(trig));
            sb.Append(" from ");sb.Append(Uid(target));
            return sb.ToString();
        }
    }
}
