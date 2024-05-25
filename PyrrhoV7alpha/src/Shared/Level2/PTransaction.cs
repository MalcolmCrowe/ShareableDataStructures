using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level4;
using Pyrrho.Level3;
using System.Xml;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
	/// <summary>
	/// A Transaction record in the Physical database
	/// </summary>
	internal class PTransaction : Physical
	{
        /// <summary>
        /// The number of Physicals in the Transaction
        /// </summary>
		public int nrecs = 0;
        /// <summary>
        /// The user for the transaction, checked in Commit
        /// </summary>
		public User? ptuser;
        public Role ptrole;
        /// <summary>
        /// The transaction time, updated in Commit
        /// </summary>
		public long pttime;
        public override long Dependent(Writer wr, Transaction tr)
        {
            return -1;
        }
        /// <summary>
        /// Constructor: a Transaction record for a Commit
        /// </summary>
        /// <param name="nr">The number of records in this transaction</param>
        /// <param name="usr">The user performing the commit</param>
        /// <param name="rl">The role for the commit</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PTransaction(int nr, User? usr, Role rl, long pp)
            : this (Type.PTransaction,nr,usr,rl,pp)
		{ }
        /// <summary>
        /// Constructor: a Transaction record for a Commit
        /// </summary>
        /// <param name="tp">The PTransaction or PTransaction2 type</param>
        /// <param name="nr">The number of records in this transaction</param>
        /// <param name="usr">The user performing the commit</param>
        /// <param name="rl">The role for the commit</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected PTransaction(Type tp, int nr, User? usr, Role rl, long pp)
            : base(tp, pp)
        {
            nrecs = nr;
            ptuser = usr;
			ptrole = rl;
			pttime = DateTime.Now.Ticks;
		}
        /// <summary>
        /// Constructor: a Transaction record from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PTransaction(Reader rdr) : this(Type.PTransaction,rdr)
		{}
        /// <summary>
        /// Constructor: a Transaction record from the buffer
        /// </summary>
        /// <param name="tp">The PTransaction or PTransaction2 type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        protected PTransaction(Type tp, Reader rdr)
            : base(tp, rdr)
        { ptuser = rdr.user; ptrole = rdr.role; }
        protected PTransaction(PTransaction x, Writer wr) : base(x, wr)
        {
            nrecs = x.nrecs;
            ptrole = x.ptrole;
            ptuser = x.ptuser;
            pttime = x.pttime;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PTransaction(this, wr);
        }
        /// <summary>
        /// Commit this transaction to the PhysBase
        /// </summary>
        /// <param name="t">The starting position of the Commit</param>
        /// <param name="reloc">Relocation information for positions</param>
        /// <returns>Dummy</returns>
		public override (Transaction?,Physical) Commit(Writer wr,Transaction? tr)
		{
			if (ptrole==Database.guestRole)
				throw new DBException("28000").ISO();
			pttime = DateTime.Now.Ticks;
			return base.Commit(wr,tr);
		}
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
		public override void Serialise(Writer wr)
		{
            wr.PutInt(nrecs);
            wr.PutLong(ptrole.defpos); 
            wr.PutLong(ptuser?.defpos??-1L);
            wr.PutLong(pttime);
			// no base.Serialise() for PTransaction
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			nrecs = rdr.GetInt();
            var db = rdr.context.db;
			ptrole = (Role)(db.objects[rdr.GetLong()]??db.schema);
			ptuser = (User?)db.objects[rdr.GetLong()];
			pttime = rdr.GetLong();
		}
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
            var cu = Thread.CurrentThread.CurrentUICulture;
			return "PTransaction for "+nrecs+" Role="+Pos(ptrole.defpos)
                +" User="+Pos(ptuser?.defpos ?? -1L)+" Time="+new DateTime(pttime).ToString(cu);
		}
        internal override DBObject? Install(Context cx, long p)
        {
            if (ptrole.defpos!=cx.db.role.defpos)
                cx.db += (Database.Role,cx.db.objects[ptrole.defpos] as Role??throw new DBException("42105"));
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return ptrole;
        }
    }
    /// <summary>
    /// Note in a Transaction record a boundary where the user and role change to the definer of a trigger
    /// </summary>
    internal class TriggeredAction : Physical
    {
        internal long trigger;
        internal long refPhys = -1;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,trigger)) return trigger;
            if (!Committed(wr,refPhys)) return refPhys;
            return -1;
        }
        internal TriggeredAction(Reader rdr) : base(Type.TriggeredAction,rdr)
        { }
        internal TriggeredAction(long tg,long pp)
            : base(Type.TriggeredAction,pp)
        {
            trigger = tg;
        }
        protected TriggeredAction(TriggeredAction x, Writer wr) : base(x, wr)
        {
            trigger = wr.cx.Fix(x.trigger);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new TriggeredAction(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutLong(trigger);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            trigger = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "TriggeredAction " + trigger;
        }

        internal override DBObject? Install(Context cx, long p)
        {
            var tg = (Trigger?)cx.db.objects[trigger] ?? throw new PEException("PE1415");
            var ro = (Role?)cx.db.objects[tg.definer] ?? throw new PEException("PE1416");
            cx.db += (ro, p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return null;
        }
    }
    /// <summary>
    /// If there is no primary index, an audit record applies to the whole table
    /// </summary>
    internal class Audit : Physical,IComparable
    {
        internal User? user;
        internal long _user = -1L;
        internal long table = -1L;
        internal CTree<long, string> match = CTree<long,string>.Empty; 
        internal long timestamp = -1L;
        public override long Dependent(Writer wr,Transaction tr)
        {
            if (!Committed(wr,table))
                throw new DBException("0000"); // audit of uncommitted object ???
            if (!Committed(wr,_user)) return _user; // ad-hoc user
            for (var b=match.First();b is not null;b=b.Next())
                if (!Committed(wr,b.key()))
                    throw new DBException("0000"); // audit of uncommitted object ???
            return -1;
        }
        internal Audit(User us,long ta, CTree<long,string> ma, long ts,long pp)
            : this(Type.Audit,us,ta,ma,ts,pp)
        {
        }
        protected Audit(Type t, User us, long ta, CTree<long,string> ma, long ts, long pp)
            : base(t,pp)
        {
            user = us; table = ta; 
            timestamp = ts; match = ma;
        }
        internal Audit(Reader rdr) : this(Type.Audit, rdr) { }
        protected Audit(Type t, Reader rdr) : base(t, rdr) { }
        public override void Deserialise(Reader rdr)
        {
            _user = rdr.GetLong();
                user = (User)(rdr.context.db.objects[rdr.GetLong()]
                ?? throw new PEException("PE6061"));
            table = rdr.GetLong();
            timestamp = rdr.GetLong();
            var n = rdr.GetInt();
            var cols = new long[n];
            for (var i = 0; i < n; i++)
                cols[i] = rdr.GetLong();
            var ma = CTree<long, string>.Empty;
            for (var i = 0; i < n; i++)
                ma += (cols[i],rdr.GetString());
            match = ma;
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            table = wr.cx.Fix(table);
            timestamp = wr.cx.Fix(timestamp);
            wr.PutLong((_user >=0)?wr.cx.Fix(_user):-1L);
            wr.PutLong(table);
            wr.PutLong(timestamp);
            wr.PutLong(match.Count);
            for (var b = match.First();b is not null;b=b.Next())
                wr.PutLong(wr.cx.Fix(b.key()));
            for (var b = match.First(); b != null; b = b.Next())
                wr.PutString(b.value());
            base.Serialise(wr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            if (user is not null && user.defpos>=0)
            { sb.Append("Audit: "); sb.Append(user.name); }
            sb.Append(" ["); sb.Append(table); sb.Append("] ");
            sb.Append(new DateTime(timestamp).ToString(Thread.CurrentThread.CurrentUICulture));
            if (match.Count > 0)
            {
                sb.Append(" {");
                var cm = "";
                for (var b=match.First(); b is not null; b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.key()); sb.Append("='");
                    sb.Append(b.value()); sb.Append("'");
                }
                sb.Append('}');
            }
            return sb.ToString();
        }

        internal override DBObject? Install(Context cx, long p)
        {
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return null;
        }

        protected override Physical Relocate(Writer wr)
        {
            if (_user >=0)
                user = (User?)wr.cx.db.objects[wr.cx.Fix(_user)]
                    ?? throw new DBException("42105").Add(Qlx.USER);
            table = wr.cx.Fix(table);
            match = wr.cx.Fix(match);
            return this;
        }

        public int CompareTo(object? obj)
        {
            var that = obj as Audit;
            if (that == null)
                return 1;
            var c = _user.CompareTo(that._user);
            if (c != 0)
                return c;
            var tb = that.match.First();
            var b = match.First();
            for (; b != null && tb != null; b = b.Next(), tb = tb.Next())
            {
                c = b.key().CompareTo(tb.key());
                if (c != 0)
                    return c;
                c = b.value().CompareTo(tb.value());
                if (c != 0)
                    return c;
            }
            return (b != null) ? 1 : (tb != null) ? -1 : 0;
        }
    }
}
