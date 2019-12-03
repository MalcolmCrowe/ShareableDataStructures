using System;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
namespace Pyrrho.Level2
{
	/// <summary>
	/// A Level 2 Trigger definition
	/// </summary>
	internal class PTrigger : Physical
	{
        /// <summary>
        /// The possible trigger types (flag attribute)
        /// </summary>
		[Flags]
			public enum TrigType { Insert=1, Update=2, Delete=4, Before=8, After=16, EachRow=32, Instead=64, EachStatement=128 }
        /// <summary>
        /// The defining position for the trigger
        /// </summary>
		public virtual long defpos { get { return ppos; }}
        /// <summary>
        /// The name of the trigger
        /// </summary>
		public string name;
        /// <summary>
        /// The associated table
        /// </summary>
		public long tabledefpos;
        /// <summary>
        /// The trigger type
        /// </summary>
		public TrigType tgtype = (TrigType)0;
        /// <summary>
        /// The TableColumns for update
        /// </summary>
		public long[] cols = null;
        /// <summary>
        /// The alias for the old row
        /// </summary>
		public string oldRow = null;
        /// <summary>
        /// The alias for the new row
        /// </summary>
		public string newRow = null;
        /// <summary>
        /// The alias for the old table
        /// </summary>
		public string oldTable = null;
        /// <summary>
        /// The alias for the new table
        /// </summary>
		public string newTable = null;
        /// <summary>
        /// The definition of the trigger
        /// </summary>
		public Executable def;
        public string src;
        public override long Dependent(Writer wr)
        {
            if (defpos != ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,tabledefpos)) return tabledefpos;
            for (var i = 0; i < cols.Length; i++)
                if (!Committed(wr,cols[i])) return cols[i];
            return -1;
        }
        /// <summary>
        /// Constructor: A Trigger definition from the Parser
        /// </summary>
        /// <param name="tc">The trigger name</param>
        /// <param name="tb">The defining position for the table</param>
        /// <param name="ty">The trigger type</param>
        /// <param name="cs">A list of the defining positions of update TableColumns</param>
        /// <param name="or">The alias for the old row</param>
        /// <param name="nr">The alias for the new row</param>
        /// <param name="ot">The alias for the old table</param>
        /// <param name="nt">The alias for the new table</param>
        /// <param name="def">The definition of the trigger</param>
        /// <param name="sce">The source string for the trigger definition</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PTrigger(string tc, long tb, int ty, long[] cs, string or, 
            string nr, string ot, string nt, Executable def, string sce,long u, Transaction tr)
			:this(Type.PTrigger,tc,tb,ty,cs,or,nr,ot,nt,def,sce,u, tr)
		{
		}
        /// <summary>
        /// Constructor: A Trigger definition from the Parser
        /// </summary>
        /// <param name="tp">The PTrigger type</param>
        /// <param name="tc">The trigger name</param>
        /// <param name="tb">The defining position for the table</param>
        /// <param name="ty">The trigger type</param>
        /// <param name="cs">A TypedValue[] list of the defining positions of update TableColumns</param>
        /// <param name="or">The alias for the old row</param>
        /// <param name="nr">The alias for the new row</param>
        /// <param name="ot">The alias for the old table</param>
        /// <param name="nt">The alias for the new table</param>
        /// <param name="df">The definition of the trigger</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected PTrigger(Type tp, string tc, long tb, int ty, long[] cs, 
            string or, string nr, string ot, string nt, Executable df, 
            string sce, long u, Transaction tr)
			:base(tp,u, tr)
		{
            name = tc;
			tabledefpos = tb;
			tgtype = (TrigType)ty;
			cols = cs;
			oldRow = or;
			newRow = nr;
			oldTable = ot;
			newTable = nt;
			def = df;
            src = sce;
		}     
        /// <summary>
        /// Constructor: a Trigger definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PTrigger(Reader rdr) : base(Type.PTrigger,rdr) {}
        protected PTrigger(PTrigger x, Writer wr) : base(x, wr)
        {
            name = x.name;
            tabledefpos = wr.Fix(x.tabledefpos);
            tgtype = x.tgtype;
            if (x.cols != null)
                for (var i = 0; i<x.cols.Length; i++)
                    cols[i] = wr.Fix(x.cols[i]);
            oldRow = x.oldRow;
            newRow = x.newRow;
            oldTable = x.oldTable;
            newTable = x.newTable;
            def = (Executable)x.def.Relocate(wr);
            src = x.src;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PTrigger(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation ifnormation for positions</param>
		public override void Serialise(Writer wr) 
		{
            wr.PutString(name.ToString());
			tabledefpos = wr.Fix(tabledefpos);
            wr.PutLong(tabledefpos);
            wr.PutInt((int)tgtype);
			if (cols==null)
                wr.PutInt(0);
			else
			{
				int n = cols.Length;
                wr.PutInt(n);
				for(int i=0;i<n;i++)
                    wr.PutLong(cols[i]);
			}
            wr.PutString(oldRow?.ToString()??"");
            wr.PutString(newRow?.ToString()??"");
            wr.PutString(oldTable?.ToString()??"");
            wr.PutString(newTable?.ToString()??"");
            wr.PutString(src);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			name = rdr.GetString();
			tabledefpos = rdr.GetLong();
			tgtype = (TrigType)rdr.GetInt();
			int n = rdr.GetInt();
            var cols = new long[n];
			while (n-->0)
                cols[n] = rdr.GetLong();
            var or = rdr.Position;
			oldRow = rdr.GetString();
            var nr = rdr.Position;
			newRow = rdr.GetString();
            var ot = rdr.Position;
            oldTable = rdr.GetString();
            var nt = rdr.Position;
            newTable = rdr.GetString();
            var lp = rdr.Position;
            src = rdr.GetString();
            // prepare a context for parsing the trigger definition
            var tb = (Table)rdr.db.objects[tabledefpos];
            var ti = (ObInfo)rdr.role.obinfos[tabledefpos];
            var fm = new From(nt, tb, ti);
            var cx = new Context(rdr.db);
            var db = rdr.db + (Database._ExecuteStatus,ExecuteStatus.Parse);
            AddTable(cx, newTable, fm);
            AddTable(cx, oldTable, new FromOldTable(ot,fm));
            AddRow(cx, oldRow, ti);
            AddRow(cx, newRow, new ObInfoNewRow(tb,rdr.role));
            def = new Parser(db, cx).ParseTriggerDefinition(src,lp);
			base.Deserialise(rdr);
		}
        void AddTable(Context cx,string s,From t)
        {
            if (s != "")
                cx.defs += (new Ident(s, 0), t);
        }
        void AddRow(Context cx,string s,ObInfo oi)
        {
            if (s != "")
                cx.defs += (new Ident(s, 0), oi);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
            var sb = new System.Text.StringBuilder();
            sb.Append("Trigger ");
            sb.Append(name);
            sb.Append(" ");
            sb.Append(tgtype.ToString());
			if (cols!=null)
            {
                sb.Append(" of ");
                sb.Append(cols.ToString());
            }
            sb.Append(" on ");
            sb.Append(Pos(tabledefpos));
            Add("old row", oldRow, sb);
            Add("new row", newRow, sb);
            Add("old table", oldTable, sb);
            Add("new table", newTable, sb);
            sb.Append(": ");
            sb.Append(src);
            return sb.ToString();
		}
        void Add(string c,string v,System.Text.StringBuilder sb)
        {
            if (v == null || v=="")
                return;
            sb.Append(",");
            sb.Append(v);
            sb.Append("=");
            sb.Append(c);
        }
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.PTrigger:
                    return (tabledefpos == ((PTrigger)that).tabledefpos) ? ppos : -1;
                case Type.Drop:
                    return (tabledefpos == ((Drop)that).delpos) ? ppos : -1;
                case Type.Change:
                    return (tabledefpos == ((Change)that).affects) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }
        internal override (Database, Role) Install(Database db, Role ro, long p)
        {
            var tb = (Table)db.mem[tabledefpos];
            var tg = new Trigger(this, db);
            tb = tb.AddTrigger(tg, db);
            ro += new ObInfo(defpos, name);
            db = db + (ro, p) + (tb,p) + (tg,p);
            return (db,ro);
        }
    }
}
