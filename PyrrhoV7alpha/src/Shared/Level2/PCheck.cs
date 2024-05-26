using System;
using System.Security.AccessControl;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

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
	/// A PCheck is for a check constraint for Table, Column, or Domain.
	/// </summary>
	internal class PCheck : Compiled
	{
		public long ckobjdefpos; // of object (e.g. Domain,Table) to which this check applies
        public long subobjdefpos = -1; // of Column if a columns check
        public long defpos;
		public string? check;
        public long test;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,ckobjdefpos)) return ckobjdefpos;
            if (!Committed(wr,subobjdefpos)) return subobjdefpos;
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            return -1;
        }
        /// <summary>
        /// Constructor: A new check constraint from the Parser
        /// </summary>
        /// <param name="ob">The object to which the check applies</param>
        /// <param name="nm">The name of the constraint</param>
        /// <param name="cs">The constraint as a string</param>
        /// <param name="nst">Start of framing executables</param>
        /// <param name="db">The local database</param>
        public PCheck(DBObject ob, string nm, QlValue se, string cs, long nst, long pp, Context cx)
            : this(Type.PCheck, ob, nm, se, cs, nst, pp, cx) { }
        protected PCheck(Type tp, DBObject ob, string nm, QlValue se, string cs, long nst,
            long pp, Context cx) : base(tp,pp,cx,nm,ob.defpos,Domain.Bool,nst)
		{
			ckobjdefpos = ob.defpos;
            defpos = ppos;
			check = cs;
            test = se.defpos;
        }
        /// <summary>
        /// Constructor: A new check constraint from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PCheck(Reader rdr) : base (Type.PCheck,rdr)
		{}
        protected PCheck(PCheck x, Writer wr) : base(x, wr)
        {
            ckobjdefpos = wr.cx.Fix(x.ckobjdefpos);
            defpos = wr.cx.Fix(x.defpos);
            name = x.name;
            check = x.check;
            test = x.test;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PCheck(this, wr);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Check " +name+" ["+Pos(ckobjdefpos)+"]: "+check;
		}
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutLong(ckobjdefpos);
            wr.PutString(name?.ToString()??"");
            wr.PutString(check??"");
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			ckobjdefpos = rdr.GetLong();
			name = rdr.GetString();
            defpos = ppos;
            check = rdr.GetString();
			base.Deserialise(rdr);
        }
        internal override void OnLoad(Reader rdr)
        {
            if (check != "" && check is not null)
            {
                var ob = (DBObject?)rdr.context.db.objects[ckobjdefpos]??throw new PEException("PE1437");
                var psr = new Parser(rdr, new Ident(check, rdr.context.Ix(ppos+1)));
                nst = psr.cx.db.nextStmt;
                var sv = psr.ParseSqlValue(Domain.Bool).Reify(rdr.context);
                test = sv.defpos;
                framing = new Framing(psr.cx,nst);
            }
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PCheck2:
                case Type.PCheck:
                    if (name!="" && name == ((PCheck)that).name)
                        return new DBException("40046", defpos, that, ct);
                    break;
                case Type.Drop:
                    if (ckobjdefpos == ((Drop)that).delpos)
                        return new DBException("40010", defpos, that, ct);
                    break;
                case Type.Change:
                    if (ckobjdefpos == ((Change)that).defpos)
                        return new DBException("40021", ckobjdefpos, that, ct);
                    break;
                case Type.Alter:
                    if (ckobjdefpos == ((Alter)that).defpos)
                        return new DBException("40025", defpos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.db.role;
            var ck = new Check(this, cx.db);
            ck += (DBObject.Infos, new BTree<long, ObInfo>(ro.defpos,
                new ObInfo(name ?? "", Grant.Privilege.Execute)));
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            var ob = (DBObject?)cx.db.objects[ck.checkobjpos] ?? throw new PEException("PE1438");
            ob = ob.Add(ck, cx.db);
            cx.Install(ob);
            cx.Install(ck);
            return ob;
        }
        public override (Transaction?,Physical) Commit(Writer wr, Transaction? t)
        {
            var (tr,ph) = base.Commit(wr, t);
            var pc = (PCheck)ph;
            if (tr?.objects[defpos] is not Check ob || pc.framing.obs[pc.test] is not DBObject se
                || tr?.objects[ckobjdefpos] is not DBObject co)
                throw new PEException("PE1350");
            var ck = ob + (Check.Condition, se.defpos) + (DBObject._Framing, pc.framing);
            co = co.Add(ck, tr);
            wr.cx.instDFirst = -1;
            return (tr + ck + co,ph);
        }
    }
    /// <summary>
    /// A version of PCheck that deals with deeply-structured objects.
    /// </summary>
    internal class PCheck2 : PCheck
    {
        /// <summary>
        /// Constructor: A new check constraint from the Parser
        /// </summary>
        /// <param name="dm">The object to which the check applies</param>
        /// <param name="so">The subobject to which the check applies</param>
        /// <param name="nm">The name of the constraint</param>
        /// <param name="cs">The constraint as a string</param>
        /// <param name="nst">The first possible framing object</param>
        /// <param name="pb">The local database</param>
        public PCheck2(DBObject ob, DBObject so, string nm, QlValue se, string cs, long nst, long pp, 
            Context cx)
            : base(Type.PCheck2,ob,nm,se,cs,nst,pp,cx)
		{
            subobjdefpos=so.defpos;
		}
        /// <summary>
        /// Constructor: A new check constraint from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PCheck2(Reader rdr) : base(rdr)
		{}
        protected PCheck2(PCheck2 p, Writer wr) : base(p, wr) 
        {
            subobjdefpos = wr.cx.Fix(p.subobjdefpos);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PCheck2(this,wr);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Check " +name+" ["+Pos(ckobjdefpos)+":"+Pos(subobjdefpos)+"]: "+check;
		}
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
			subobjdefpos = wr.cx.Fix(subobjdefpos);
            wr.PutLong(subobjdefpos);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			subobjdefpos = rdr.GetLong();
			base.Deserialise(rdr);
		}
        /// <summary>
        /// Looks the same as PCheck::Install but gets a different constructor for Check
        /// and calls a different Add !
        /// </summary>
        /// <param name="db"></param>
        /// <param name="ro"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.role;
            var ck = new Check(this, cx.db);
            ck += (DBObject.Infos, new BTree<long, ObInfo>(ro.defpos,
                new ObInfo(name??"", Grant.Privilege.Execute)));
            if (cx.db.objects[ck.checkobjpos] is not DBObject co)
                throw new PEException("PE1451");
            cx.Install(ck);
            var nc = co.Add(ck, cx.db);
            cx.Install(nc);
    //        cx.Add(ck.framing);
            cx.db += ck;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return nc;
        }
    }
}
