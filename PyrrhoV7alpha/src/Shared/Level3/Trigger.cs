using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level3
{
	/// <summary>
	/// A Trigger database object
	/// </summary>
	internal class Trigger : DBObject
	{
        internal const long
            Action = -290, // WhenPart
            Def = -291, // string
            _From = -396, // long
            NewRow = -292, // string
            NewTable = -293, // string
            OldRow = -294, // string
            OldTable = -295, // string
            TrigType = -297, // PTrigger.TrigType
            UpdateCols = -298; // BList<long>
        /// <summary>
        /// The defining position of the associated table
        /// </summary>
		public long from => (long)mem[_From];
        public long table => (long)mem[From.Target];
        public Ident def => (Ident)mem[Def];
        public string name => (string)mem[Name] ?? "";
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
		public Ident oldRow =>(Ident)mem[OldRow];
        /// <summary>
        /// the name of the new row
        /// </summary>
		public Ident newRow =>(Ident)mem[NewRow];
        /// <summary>
        /// the name of the old table
        /// </summary>
		public Ident oldTable =>(Ident)mem[OldTable];
        /// <summary>
        /// the name of the new table
        /// </summary>
		public Ident newTable => (Ident)mem[NewTable];
        public WhenPart action => (WhenPart)mem[Action];
        /// <summary>
        /// A new Trigger from the PhysBase
        /// </summary>
		public Trigger(PTrigger p,Database db)
            : base(p.name,p.ppos,p.defpos,db.role.defpos,BTree<long,object>.Empty
                  +(Action,(WhenPart)p.def)+(_From,p.from.defpos)
                  +(From.Target,p.from.target) + (TrigType,p.tgtype)
                  +(UpdateCols,p.cols)+(OldRow,p.oldRow)+(NewRow,p.newRow)
                  +(OldTable,p.oldTable)+(NewTable,p.newTable)
                  +(Def,p.src))
		{ }
        public Trigger(long defpos, BTree<long, object> m) : base(defpos, m) 
        { }
        public static Trigger operator+(Trigger tg,(long,object)x)
        {
            return new Trigger(tg.defpos, tg.mem + x);
        }
        /// <summary>
        /// a string representation of the trigger
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
		{
            var sb = new StringBuilder(base.ToString());
            sb.Append(" TrigType=");sb.Append(tgType);
            sb.Append(" From="); sb.Append(Uid(from));
            sb.Append(" On=");sb.Append(Uid(table));
            sb.Append(" Action:");sb.Append(action);
            if (cols != null)
            {
                sb.Append(" UpdateCols:");
                var cm = '(';
                for (var i = 0; i < cols.Count; i++)
                { sb.Append(cm); cm = ','; sb.Append(cols[i]); }
            }
            if (oldRow!=null) { sb.Append(" OldRow="); sb.Append(oldRow); }
            if (newRow != null) { sb.Append(" NewRow="); sb.Append(newRow); }
            if (oldTable != null) { sb.Append(" OldTable="); sb.Append(oldTable); }
            if (newTable != null) { sb.Append(" NewTable="); sb.Append(newTable); }
            return sb.ToString();
		}

        internal override Basis New(BTree<long, object> m)
        {
            return new Trigger(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Trigger(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = this;
            var d = wr.Fix(defpos);
            if (d != defpos)
                r = (Trigger)Relocate(wr);
            var ac = BList<Executable>.Empty;
            var ch = false;
            var cn = (SqlValue)action.cond?.Relocate(wr);
            for (var b=action.stms.First();b!=null;b=b.Next())
            {
                var a = (Executable)b.value().Relocate(wr);
                ch = ch || (a != b.value());
                ac += a;
            }
            if (ch || cn!=action.cond)
                r += (Action, new WhenPart(action.defpos,cn,ac));
            var ta = wr.Fix(from);
            if (ta != from)
                r += (_From, ta);
            var uc = BList<long>.Empty;
            ch = false;
            for (var b=cols.First();b!=null;b=b.Next())
            {
                var c = wr.Fix(b.value());
                ch = ch || (c != b.value());
                uc += c;
            }
            if (ch)
                r += (UpdateCols, uc);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var ac = BList<Executable>.Empty;
            var cn = (SqlValue)action.cond?.Frame(cx);
            for (var b = action.stms.First(); b != null; b = b.Next())
                ac += (Executable)b.value().Frame(cx);
            return cx.Add(this + (Action, new WhenPart(action.defpos,cn,ac)));
        }
    }
}
