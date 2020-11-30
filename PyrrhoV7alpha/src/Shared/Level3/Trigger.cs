using System;
using System.ComponentModel;
using System.Management.Instrumentation;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
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
	/// A Trigger database object
	/// </summary>
	internal class Trigger : DBObject
	{
        internal const long
            Action = -290, // long Executable
            NewRow = -293, // long Cursor
            NewTable = -294, // long Query
            OldRow = -295, // long Cursor
            OldTable = -296, // long Query
            TrigType = -297, // PTrigger.TrigType
            UpdateCols = -298; // BList<long> SqlValue
        public string name => (string)mem[Name];
        public long table => (long)mem[From.Target];
        /// <summary>
        /// The trigger type (flags)
        /// </summary>
		public PTrigger.TrigType tgType=> (PTrigger.TrigType)mem[TrigType];
        /// <summary>
        /// The list of update TableColumns
        /// </summary>
		public BList<long> cols => (BList<long>)mem[UpdateCols]??BList<long>.Empty;
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
        /// <summary>
        /// A new Trigger from the PhysBase
        /// </summary>
		public Trigger(PTrigger p)
            : base(p.ppos, 
                  _Mem(p) + (Action,p.def) + (Name,p.name)
                  + (Definer, p.database.role.defpos)
                  + (_Framing, p.framing) + (From.Target, p.target) + (TrigType, p.tgtype)
                  + (UpdateCols, p.cols) + (LastChange, p.ppos))
		{ }
        public Trigger(long defpos, BTree<long, object> m) : base(defpos, m) 
        { }
        static BTree<long,object> _Mem(PTrigger p)
        {
            var r = BTree<long, object>.Empty;
            if (p.oldTable != null)
                r += (OldTable, p.oldTable.iix);
            if (p.newTable != null)
                r += (NewTable, p.newTable.iix);
            if (p.oldRow != null)
                r += (OldRow, p.oldRow.iix);
            if (p.newRow != null)
                r += (NewRow, p.newRow.iix);
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
            sb.Append(" Action:");sb.Append(Uid(action));
            if (cols != null)
            {
                sb.Append(" UpdateCols:");
                var cm = '(';
                for (var i = 0; i < cols.Count; i++)
                { sb.Append(cm); cm = ','; sb.Append(cols[i]); }
            }
            if (oldRow!= -1L) { sb.Append(" OldRow="); sb.Append(Uid(oldRow)); }
            if (newRow != -1L) { sb.Append(" NewRow="); sb.Append(Uid(newRow)); }
            if (oldTable != -1L) { sb.Append(" OldTable="); sb.Append(Uid(oldTable)); }
            if (newTable != -1L) { sb.Append(" NewTable="); sb.Append(Uid(newTable)); }
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
            var a = cx.Replace(action, so, sv);
            if (a != action)
                r += (Action, a);
            var ch = false;
            var cs = BList<long>.Empty;
            for (var b = cols?.First(); b != null; b = b.Next())
            {
                var c = cx.Replace(b.value(), so, sv);
                cs += c;
                ch = ch || c != b.value();
            }
            if (ch)
                r += (UpdateCols, cs);
            var o = cx.Replace(oldRow, so, sv);
            if (o != oldRow)
                r += (OldRow, o);
            o = cx.Replace(newRow, so, sv);
            if (o != newRow)
                r += (NewRow, o);
            o = cx.Replace(oldTable, so, sv);
            if (o != oldTable)
                r += (OldTable, o);
            o = cx.Replace(newTable, so, sv);
            if (o != newTable)
                r += (NewTable, o);
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
        internal override Basis Fix(Context cx)
        {
            var r = (Trigger)base.Fix(cx);
            r += (Action, cx.obuids[action]??action);
            if (newRow>=0)
                r += (NewRow, cx.obuids[newRow]??newRow);
            if (newTable >= 0)
                r += (NewTable, cx.obuids[newTable]??newTable);
            if (oldRow >= 0)
                r += (OldRow, cx.obuids[oldRow]??oldRow);
            if (oldTable >= 0)
                r += (OldTable, cx.obuids[oldTable]??oldTable);
            r += (UpdateCols, cx.Fix(cols));
            return r; 
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r= (Trigger)base._Relocate(wr);
            r += (Action, wr.Fixed(action)?.defpos??-1L);
            r += (NewRow, wr.Fixed(newRow)?.defpos ?? -1L);
            r += (NewTable, wr.Fixed(newTable)?.defpos ?? -1L);
            r += (OldRow, wr.Fixed(oldRow)?.defpos ?? -1L);
            r += (OldTable, wr.Fixed(oldTable)?.defpos ?? -1L);
            r += (UpdateCols, wr.Fix(cols));
            return r;
        }
    }
    /// <summary>
    /// Transition tables are not listed in roles but referred to in triggers
    /// </summary>
    internal class TransitionTable : From
    {
        internal const long
            Old = -327, // bool
            Trig = -326; // long
        internal bool old => (bool)mem[Old];
        internal long trig => (long)mem[Trig];
        internal CList<long> columns => (CList<long>)mem[SqlValue._Columns] ?? CList<long>.Empty;
        internal TransitionTable(Ident ic, bool old, Context cx, From fm, Trigger tg)
                : base(ic.iix, _Mem(cx, ic, fm) + (Old, old) + (Trig, tg.defpos))
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
            var ds = BTree<long, bool>.Empty;
            var d = 1+fm.depth;
            for (var b = fm.rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var c = (SqlValue)cx.obs[p];
                var u = cx.GetUid();
                var v = new SqlCopy(u, cx, c.name, ic.iix, p);
                cx.Add(v);
                vs += v;
                cs += v.defpos;
                ds += (u, true);
                d = _Max(d, 1 + v.depth);
            } 
            var nd = new Domain(Sqlx.ROW,vs);
            return BTree<long, object>.Empty + (_Domain, nd) + (Name, ic.ident)
                  + (SqlValue._Columns, cs) + (Dependents, ds) + (Depth, d)
                  + (Target, fm.target);
        }
        internal override DBObject Relocate(long dp)
        {
            return new TransitionTable(dp,mem);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r =  (TransitionTable)base._Relocate(wr);
            r += (Trig, wr.Fix(trig));
            r += (SqlValue._Columns, wr.Fix(columns));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (TransitionTable)base.Fix(cx);
            r += (Trig, cx.obuids[trig]??trig);
            r += (SqlValue._Columns, cx.Fix(columns));
            return r;
        }
        internal override void _Add(Context cx)
        {
            // don't call the base
            var tg = (Trigger)cx.obs[trig];
            var tb = ((Table)cx.db.objects[tg.table]);
            tb._Add(cx);
            cx.obs += (defpos, this);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(old ? " old" : " new");
            sb.Append(" for "); sb.Append(Uid(trig));
            sb.Append(" from ");sb.Append(Uid(target));
            return sb.ToString();
        }
    }
}
