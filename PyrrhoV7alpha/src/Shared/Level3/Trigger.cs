using System.Security.Cryptography;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

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
            UpdateCols = -298; // CList<long> QlValue
        public long table => (long)(mem[RowSet.Target]??-1L);
        /// <summary>
        /// The trigger type (flags)
        /// </summary>
		public PTrigger.TrigType tgType => (PTrigger.TrigType)(mem[TrigType] ?? PTrigger.TrigType.Deferred);
        /// <summary>
        /// The tree of update TableColumns
        /// </summary>
		public CList<long> cols => (CList<long>?)mem[UpdateCols]??CList<long>.Empty;
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
                  _Mem(p) + (Action,p.def) + (ObInfo.Name,p.name) +(Owner,p.owner)
                  +(Infos,p.infos) + (_Domain,p.dataType)
                  + (Definer, p.definer) + (_From, p.from) + (TrigPpos, p.ppos)
                  + (_Framing, p.framing) + (RowSet.Target, p.target) + (TrigType, p.tgtype)
                  + (LastChange, p.ppos))
		{ }
        public Trigger(long defpos, BTree<long, object> m) : base(defpos, m) 
        { }
        static BTree<long,object> _Mem(PTrigger p)
        {
            var r = new BTree<long, object>(_Domain, p.dataType);
            if (p.oldTable != null)
                r += (OldTable, p.oldTable.uid);
            if (p.newTable != null)
                r += (NewTable, p.newTable.uid);
            if (p.oldRow != null)
                r += (OldRow, p.oldRow.uid);
            if (p.newRow != null)
                r += (NewRow, p.newRow.uid);
            if (p.cols != null)
                r += (UpdateCols, p.cols);
            return r;
        }
        public static Trigger operator +(Trigger et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (Trigger)et.New(m + x);
        }

        public static Trigger operator +(Trigger e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (p == UpdateCols)
                m += (_Depth, cx._DepthLl(e.cols, d));
            else
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (Trigger)e.New(m + (p, o));
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
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return (dp == defpos) ? this : new Trigger(dp, mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (Trigger)base._Replace(cx, so, sv);
            if (table == so.defpos)
                r +=(cx, RowSet.Target, sv.defpos);
            var a = cx.ObReplace(action, so, sv);
            if (a != action)
                r +=(cx, Action, a);
            var ch = false;
            var cs = CList<long>.Empty;
            for (var b = cols?.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var c = cx.ObReplace(p, so, sv);
                    cs += c;
                    ch = ch || c != b.value();
                }
            if (ch)
                r += (cx, UpdateCols, cs);
            var o = cx.ObReplace(oldRow, so, sv);
            if (o != oldRow)
                r +=(cx, OldRow, o);
            o = cx.ObReplace(newRow, so, sv);
            if (o != newRow)
                r +=(cx, NewRow, o);
            o = cx.ObReplace(oldTable, so, sv);
            if (o != oldTable)
                r +=(cx, OldTable, o);
            o = cx.ObReplace(newTable, so, sv);
            if (o != newTable)
                r +=(cx, NewTable, o);
            return r;
        }
        internal override Database Drop(Database d, Database nd)
        {
            var tb = (Table)(nd.objects[table] ?? throw new DBException("42107", "??"));
            var tgs = CTree<PTrigger.TrigType, CTree<long, bool>>.Empty;
            for (var b=tb.triggers.First();b is not null;b=b.Next())
            {
                var ts = CTree<long, bool>.Empty;
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (c.key() != defpos)
                        ts += (c.key(), true);
                tgs += (b.key(), ts);
            }
            tb += (Table.Triggers, tgs);
            nd += tb;
            return base.Drop(d, nd);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
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
            var nc = cx.FixLl(cols);
            if (nc != cols)
                r += (UpdateCols, nc);
            return r;
        }
    }
    /// <summary>
    /// Transition tables are not listed in roles but referred to in triggers
    /// </summary>
    internal class TransitionTable : RowSet
    {
        internal const long
            ColIds = -304, // CList<long> TableColumn
            Old = -327, // bool
            Trig = -458; // long
        internal CList<long> colIds => (CList<long>)(mem[ColIds] ?? CList<long>.Empty);
        internal long trig => (long)(mem[Trig] ?? -1L);
        internal bool old => (bool)(mem[Old]??false);
 //       internal long trig => (long)mem[Trig];
        internal TransitionTable(Ident ic, bool old, Context cx, RowSet fm, Trigger tg)
                : base(ic.uid, BTree<long, object>.Empty + (RowType, fm.rowType) 
                      + (ObInfo.Name, ic.ident)
                  + (Target, fm.target) + (Old, old) + (Trig, tg.defpos))
        {
            var ns = fm.names;
            if (ns.Count==0L) // belt and braces
            {
                for (var b = fm.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.NameFor(p) is string nm)
                        ns += (nm, (ic.lp,p));
                cx.Add(fm + (ObInfo._Names, ns));
            }
            cx.defs += (ic.uid, ns);
            cx.Add(ic.ident,ic.lp,fm);
        }
        protected TransitionTable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static TransitionTable operator +(TransitionTable et, (long, object) x)
        {
            var d = et.depth;
            var m = et.mem;
            var (dp, ob) = x;
            if (et.mem[dp] == ob)
                return et;
            if (ob is DBObject bb && dp != _Depth)
            {
                d = Math.Max(bb.depth + 1, d);
                if (d > et.depth)
                    m += (_Depth, d);
            }
            return (TransitionTable)et.New(m + x);
        }

        public static TransitionTable operator +(TransitionTable e, (Context, long, object) x)
        {
            var d = e.depth;
            var m = e.mem;
            var (cx, p, o) = x;
            if (e.mem[p] == o)
                return e;
            if (o is long q && cx.obs[q] is DBObject ob)
            {
                d = Math.Max(ob.depth + 1, d);
                if (d > e.depth)
                    m += (_Depth, d);
            }
            return (TransitionTable)e.New(m + (p, o));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new TransitionTable(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return (dp == defpos && m==mem) ? this : new TransitionTable(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var nt = cx.Fix(trig);
            if (nt != trig)
                r += (Trig, nt);
            var nc = cx.FixLl(colIds);
            if (nc != colIds)
                r += (ColIds, nc);
            return r;
        }
        protected override Cursor _First(Context cx)
        {
            throw new NotImplementedException();
        }
        protected override Cursor _Last(Context cx)
        {
            throw new NotImplementedException();
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
