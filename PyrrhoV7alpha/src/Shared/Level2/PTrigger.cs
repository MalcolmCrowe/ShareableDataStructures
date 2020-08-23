using System;
using Pyrrho.Common;
using Pyrrho.Level3;
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
namespace Pyrrho.Level2
{
	/// <summary>
	/// A Level 2 Trigger definition
	/// </summary>
	internal class PTrigger : Compiled
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
		public long target;
        /// <summary>
        /// The trigger type
        /// </summary>
		public TrigType tgtype = 0;
        /// <summary>
        /// The TableColumns for update
        /// </summary>
		public BList<long> cols = null;
        /// <summary>
        /// The alias for the old row
        /// </summary>
		public Ident oldRow = null;
        /// <summary>
        /// The alias for the new row
        /// </summary>
		public Ident newRow = null;
        /// <summary>
        /// The alias for the old table: consists of rows that are being deleted or updated
        /// (so inherits where conditions for the outer statement)
        /// </summary>
        public Ident oldTable = null; 
        /// <summary>
        /// The alias for the new table: consists of the new rows  
        /// </summary>
		public Ident newTable = null;
        public Ident src; // the ident is the source code of the action!
        public long def; // the compiled version (in Compiled.framing)
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,target)) return target;
            for (var i = 0; i < cols?.Length; i++)
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
        /// <param name="sce">The source string for the trigger definition</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PTrigger(string tc, long tb, int ty, BList<long> cs, Ident or,
            Ident nr, Ident ot, Ident nt, Ident sce, Context cx, long pp)
            : this(Type.PTrigger, tc, tb, ty, cs, or, nr, ot, nt, sce, cx, pp)
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
        /// <param name="sce">The source code of the trigger</param>
        /// <param name="cx">The context</param>
        /// <param name="pp">The current position in the datafile</param>
        protected PTrigger(Type tp, string tc, long tb, int ty, BList<long> cs, 
            Ident or, Ident nr, Ident ot, Ident nt, Ident sce, Context cx, long pp)
            : base(tp,pp,cx,new Framing(cx))
		{
            name = tc;
            target = tb;
			tgtype = (TrigType)ty;
			cols = cs;
            oldRow = or;
			newRow = nr;
            oldTable = ot;
			newTable = nt;
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
            src = x.src;
            tgtype = x.tgtype;
            target = wr.Fix(x.target);
            def = wr.Fixed(x.def).defpos;
            var cs = BList<long>.Empty;
            if (x.cols != null)
                for (var b = x.cols.First(); b!=null; b=b.Next())
                    cs += wr.Fix(b.value());
            cols = cs;
            oldRow = wr.Fix(x.oldRow);
            newRow = wr.Fix(x.newRow);
            oldTable = wr.Fix(x.oldTable);
            newTable = wr.Fix(x.newTable);
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
            wr.srcPos = wr.Length+1; 
            wr.PutString(name.ToString());
            wr.PutLong(wr.Fix(target));
            wr.PutInt((int)tgtype);
			if (cols==null)
                wr.PutInt(0);
			else
			{
				int n = cols.Length;
                wr.PutInt(n);
				for(var b=cols.First();b!=null;b=b.Next())
                    wr.PutLong(wr.Fix(b.value()));
			}
            // DON'T update oldRow, newRow, oldTable, newTable
            wr.PutIdent(oldRow);
            wr.PutIdent(newRow);
            wr.PutIdent(oldTable);
            wr.PutIdent(newTable);
            src = new Ident((wr.cx.db.format < 51)?DigestSql(wr,src.ident):src.ident,wr.Length);
            src = wr.PutIdent(src);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			name = rdr.GetString();
            target = rdr.GetLong();
			tgtype = (TrigType)rdr.GetInt();
			int n = rdr.GetInt();
            var cols = BList<long>.Empty;
			while (n-->0)
                cols += rdr.GetLong();
			oldRow = rdr.GetIdent();
			newRow = rdr.GetIdent();
            oldTable = rdr.GetIdent();
            newTable = rdr.GetIdent();
            src = rdr.GetIdent();
			base.Deserialise(rdr);
		}
        internal override void OnLoad(Reader rdr)
        {
            var ob = ((DBObject)rdr.context.db.objects[target]);
            var psr = new Parser(rdr, new Ident(src.ident, ppos + 1), ob);
            psr.cx.srcFix = ppos+1;
            def = psr.ParseTriggerDefinition(this);
            Frame(psr.cx);
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
            if (cols != null)
            {
                var cm = " of (";
                for (var b = cols.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(DBObject.Uid(b.value()));
                }
                sb.Append(")");
            }
            sb.Append(" on ");
            sb.Append(Pos(target));
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
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PTrigger:
                    if (target == ((PTrigger)that).target) 
                        return new DBException("40032",target,that,ct);
                    break;
                case Type.Drop:
                    if (target == ((Drop)that).delpos)
                        return new DBException("40012", target, that, ct);
                    break;
                case Type.Change:
                    if (target == ((Change)that).affects)
                        return new DBException("40043", ppos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var tb = (Table)cx.db.objects[target];
            var tg = new Trigger(this); // complete version of trigger with def, but framing not quite right
            tb = tb.AddTrigger(tg, cx.db);
            ro = ro + new ObInfo(defpos, name, Domain.Null);
            cx.db += (ro, p);
            cx.Install(tb, p);
            cx.Install(tg, p);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
        public override (Transaction,Physical) Commit(Writer wr, Transaction t)
        {
            var (tr,ph) = base.Commit(wr, t);
            var pt = (PTrigger)ph;
            var tg = (DBObject)tr.objects[defpos] + (Check.Condition, pt.framing.obs[pt.def])
                + (DBObject._Framing, pt.framing) + (Trigger.OldRow,pt.oldRow?.iix??-1L)
                +(Trigger.NewRow,pt.newRow?.iix??-1) + (Trigger.OldTable,pt.oldTable?.iix??-1)
                +(Trigger.NewTable,pt.newTable?.iix??-1L);
            var co = ((Table)tr.objects[target]).AddTrigger((Trigger)tg, tr);
            return ((Transaction)(tr + (tg, tr.loadpos) + (co, tr.loadpos)),ph);
        }
    }
}
