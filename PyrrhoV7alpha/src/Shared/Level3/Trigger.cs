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
            Action = -303, // BList<Executable>
            Def = -304, // string
            NewRow = -305, // string
            NewTable = -306, // string
            OldRow = -307, // string
            OldTable = -308, // string
            Table = -309, // long
            TrigType = -310, // PTrigger.TrigType
            UpdateCols = -311; // BList<long>
        /// <summary>
        /// The defining position of the associated table
        /// </summary>
		public long tabledefpos => (long)mem[Table];
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
		public string oldRow =>(string)mem[OldRow];
        /// <summary>
        /// the name of the new row
        /// </summary>
		public string newRow =>(string)mem[NewRow];
        /// <summary>
        /// the name of the old table
        /// </summary>
		public string oldTable =>(string)mem[OldTable];
        /// <summary>
        /// the name of the new table
        /// </summary>
		public string newTable => (string)mem[NewTable];
        public BList<Executable> action => (BList<Executable>)mem[Action];
        /// <summary>
        /// the SQL body of the trigger (including the when part) in case we need to reparse
        /// in a multi-database connection
        /// </summary>
		public string def=> (string)mem[Def];
        /// <summary>
        /// A new Trigger from the PhysBase
        /// </summary>
		public Trigger(PTrigger p,Database db)
            : base(p.name,p.ppos,p.defpos,db.role.defpos,BTree<long,object>.Empty
                  +(Action,new Parser(db).ParseTriggerDefinition(p.def))
                  +(Def,p.def)+(Table,p.tabledefpos)+(TrigType,p.tgtype)
                  +(UpdateCols,p.cols)+(OldRow,p.oldRow)+(NewRow,p.newRow)
                  +(OldTable,p.oldTable)+(NewTable,p.newTable))
		{ }
        public Trigger(Transaction tr,PTrigger.TrigType tgt,string or,string nr, string ot,
            string nt,BTree<long,object> m=null) 
            :base(tr.uid, (m??BTree<long, object>.Empty)
                  + (TrigType, tgt) + (OldRow, or) + (NewRow, nr)
                  + (OldTable, ot) + (NewTable, nt))
        { }
        public Trigger(long defpos, BTree<long, object> m) : base(defpos, m) { }
        /// <summary>
        /// a string representation of the trigger
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
		{
            var sb = new StringBuilder(base.ToString());
            sb.Append(" TrigType=");sb.Append(tgType);
            sb.Append(" On=");sb.Append(Uid(tabledefpos));
            sb.Append(" Def={");sb.Append(def);sb.Append('}');
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
    }
}
