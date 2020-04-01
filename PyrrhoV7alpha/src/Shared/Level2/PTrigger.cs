using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
			public enum TrigType { Insert=1, Update=2, Delete=4, Before=8, After=16, EachRow=32, Instead=64, 
            EachStatement=128, Deferred=256 }
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
		public From from;
        /// <summary>
        /// The trigger type
        /// </summary>
		public TrigType tgtype = 0;
        /// <summary>
        /// The TableColumns for update
        /// </summary>
		public long[] cols = null;
        /// <summary>
        /// The alias for the old row
        /// </summary>
		public Ident oldRowId = null;
        public FromOldTable oldRow = null;
        /// <summary>
        /// The alias for the new row
        /// </summary>
		public Ident newRow = null;
        /// <summary>
        /// The alias for the old table
        /// </summary>
        public Ident oldTableId = null; 
		public FromOldTable oldTable = null;
        /// <summary>
        /// The alias for the new table
        /// </summary>
		public Ident newTable = null;
        /// <summary>
        /// The definition of the trigger
        /// </summary>
		public Executable def;
        public Ident src; // the ident is the source code of the action!
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,from.target)) return from.target;
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
        public PTrigger(string tc, From fm, int ty, long[] cs, Ident or,
            Ident nr, Ident ot, Ident nt, Executable def, Ident sce, 
            Context cx, long pp)
            : this(Type.PTrigger, tc, fm, ty, cs, or, nr, ot, nt, def, sce, cx, pp)
        { }
        /// <summary>
        /// Constructor
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
        protected PTrigger(Type tp, string tc, From fm, int ty, long[] cs, 
            Ident or, Ident nr, Ident ot, Ident nt, Executable df, 
            Ident sce, Context cx, long pp)
            : base(tp,pp,cx)
		{
            name = tc;
			from = fm;
			tgtype = (TrigType)ty;
			cols = cs;
            oldRowId = or;
			oldRow = (or!=null)?(new FromOldTable(or,fm) + (DBObject._Domain, Domain.Row)) : null;
			newRow = nr;
            oldTableId = ot;
			oldTable = (ot!=null)?new FromOldTable(ot,fm):null;
			newTable = nt;
			def = df;
            src = sce;
            if (cx.db is Transaction tr && tr.format < 51)
                digested = cx.digest;
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
            tgtype = x.tgtype;
            wr.srcPos = wr.Length + 1 + StringLength(name) + IntLength(x.from.target)
                + IntLength((int)tgtype)
                + ((cols == null) ? 1 : ColsLength(cols))
                + StringLength(oldRowId) + StringLength(newRow)
                + StringLength(oldTableId) + StringLength(newTable);
            src = new Ident(x.src.ident, wr.Fix(x.src.iix));
            oldRowId = x.oldRowId?.Relocate(wr);
            newRow = x.newRow?.Relocate(wr);
            oldTableId = x.oldTableId?.Relocate(wr);
            newTable = x.newTable?.Relocate(wr);
            oldTable = (FromOldTable)x.oldTable?.Relocate(wr);
            oldRow = (FromOldTable)x.oldRow?.Relocate(wr);
            from = (From)x.from.Relocate(wr);
            def = (Executable)x.def.Relocate(wr);
            if (x.cols != null)
                for (var i = 0; i<x.cols.Length; i++)
                    cols[i] = wr.Fix(x.cols[i]);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PTrigger(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
		public override void Serialise(Writer wr) 
		{
            wr.PutString(name.ToString());
            wr.PutLong(wr.Fix(from.target));
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
            oldRowId = wr.PutIdent(oldRowId);
            newRow = wr.PutIdent(newRow);
            oldTableId = wr.PutIdent(oldTableId);
            newTable = wr.PutIdent(newTable);
            src = new Ident((wr.cx.db.format < 51)?DigestSql(wr,src.ident):src.ident,wr.Length);
            wr.PutString(src.ident);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			name = rdr.GetString();
            var fp = rdr.Position;
            var tp = rdr.GetLong();
            var tb = (Table)rdr.context.db.objects[tp];
			from = new From(new Ident(name,fp),rdr.context,tb,null,For(rdr.context,fp,tb));
			tgtype = (TrigType)rdr.GetInt();
			int n = rdr.GetInt();
            var cols = new long[n];
			while (n-->0)
                cols[n] = rdr.GetLong();
			oldRowId = rdr.GetIdent();
			newRow = rdr.GetIdent();
            oldTableId = rdr.GetIdent();
            newTable = rdr.GetIdent();
            src = rdr.GetIdent();
            // prepare a context for parsing the trigger definition
            var cx = new Context(rdr.context.db);
            cx.Add(from);
            var db = rdr.context.db + (Database._ExecuteStatus,ExecuteStatus.Parse);
            def = new Parser(db, src).ParseTriggerDefinition(this);
			base.Deserialise(rdr);
		}
        internal static Selection For(Context cx,long dp, Table tb)
        {
            var oi = (ObInfo)cx.db.role.obinfos[tb.defpos];
            var qn = new Selection(dp, oi.name);
            var off = 5; // we will find gaps in the database for trigger columns
            cx.db += (Database.NextStmt, cx.db.nextStmt + oi.Length);
            for (var b = oi.columns.First(); b != null; b = b.Next())
            {
                while (cx.db.objects.Contains(++off))
                    ;
                var c = b.value();
                qn += new SqlCopy(off, c.name, c, oi.defpos, c.defpos);
            }
            return qn;
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
            sb.Append(Pos(from.target));
            if (oldRow != null) Add("old row ", oldRow.ToString(), sb);
            if(newRow!=null) Add("new row ", newRow.ident, sb);
            if(oldTable!=null) Add("old table ", oldTable.ToString(), sb);
            if (newTable!=null) Add("new table ", newTable.ident, sb);
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
                    return (from.target == ((PTrigger)that).from.target) ? ppos : -1;
                case Type.Drop:
                    return (from.target == ((Drop)that).delpos) ? ppos : -1;
                case Type.Change:
                    return (from.target == ((Change)that).affects) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var tb = (Table)cx.db.objects[from.target];
            var tg = new Trigger(this, cx.db);
            tb = tb.AddTrigger(tg, cx.db);
            ro += new ObInfo(defpos, name);
            cx.db = cx.db + (ro, p) + (tb, p) + (tg, p);
        }
    }
}
