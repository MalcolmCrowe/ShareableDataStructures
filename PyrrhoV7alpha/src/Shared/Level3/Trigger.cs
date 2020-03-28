using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
            _From = -292, // From
            NewRow = -293, // string
            NewTable = -294, // string
            OldRow = -295, // FromOldTable
            OldRowId = -366, // Ident
            OldTable = -296, // FromOldTable
            OldTableId = -367, // Ident
            TrigType = -297, // PTrigger.TrigType
            UpdateCols = -298; // BList<long>
        /// <summary>
        /// The defining position of the associated table
        /// </summary>
		public From from => (From)mem[_From];
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
		public FromOldTable oldRow =>(FromOldTable)mem[OldRow];
        public Ident oldRowId => (Ident)mem[OldRowId];
        /// <summary>
        /// the name of the new row
        /// </summary>
		public Ident newRow =>(Ident)mem[NewRow];
        /// <summary>
        /// the name of the old table
        /// </summary>
		public FromOldTable oldTable =>(FromOldTable)mem[OldTable];
        public Ident oldTableId => (Ident)mem[OldTableId];
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
                  +(Action,(WhenPart)p.def)+(_From,p.from)
                  +(From.Target,p.from.target) + (TrigType,p.tgtype)
                  +(UpdateCols,p.cols)+(OldRow,p.oldRow)+(OldRowId,p.oldRowId)
                  +(NewRow,p.newRow)+(OldTableId,p.oldTableId)
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
            sb.Append(" From:"); sb.Append(from);
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
            throw new NotImplementedException();
        }
        internal override DBObject Frame(Context cx)
        {
            var ac = BList<Executable>.Empty;
            var ta = (From)from.Frame(cx);
            var cn = (SqlValue)action.cond?.Frame(cx);
            oldRow?.Frame(cx);
            oldTable?.Frame(cx);
            for (var b = action.stms.First(); b != null; b = b.Next())
                ac += (Executable)b.value().Frame(cx);
            return cx.Add(this + (Action, new WhenPart(action.defpos,cn,ac))
                + (_From,ta),true);
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            var tb = (Table)nd.objects[table];
            var tgs = BTree<PTrigger.TrigType, BTree<long, Trigger>>.Empty;
            for (var b=tb.triggers.First();b!=null;b=b.Next())
            {
                var ts = BTree<long, Trigger>.Empty;
                var ch = false;
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (c.key() != defpos)
                        ts += (c.key(), c.value());
                    else
                        ch = true;
                if (ch)
                    tgs += (b.key(), ts);
            }
            tb += (Table.Triggers, tgs);
            nd += (tb, p);
            return base.Drop(d, nd, p);
        }
    }
}
