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
	/// A Grant request adds privileges on objects
	/// </summary>
	internal class Grant : Physical
	{
        /// <summary>
        /// The Privilege enumeration. Values of this type are placed in the database 
        /// so the following values cannot be changed.
        /// </summary>
		[Flags]
		public enum Privilege
		{
			NoPrivilege=0x000000, 
            Select=0x000001, Insert=0x000002, Delete=0x000004, Update=0x000008, 
			References=0x000010, Execute=0x000020, Owner=0x000040, UseRole=0x000080, 
            Usage=0x000100, Under=0x000200, Trigger=0x000400, NotUsed=0x000800,
            GrantSelect=0x001000, GrantInsert=0x002000, GrantDelete=0x004000, GrantUpdate=0x008000, 
            GrantReferences=0x010000, GrantExecute=0x020000, GrantOwner=0x040000, AdminRole=0x080000, 
            GrantUsage=0x100000, GrantUnder=0x200000,  GrantTrigger=0x400000
		};
		public static Privilege AllPrivileges = (Privilege)0x7fffff;
        /// <summary>
        /// The privilege being granted (or revoked)
        /// </summary>
		public Privilege priv;
        /// <summary>
        /// The object to which the privilege applies
        /// </summary>
		public long obj;
        /// <summary>
        /// The grantee object
        /// </summary>
		public long grantee;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,obj)) return obj;
            if (!Committed(wr,grantee)) return grantee;
            return -1;
        }
        /// <summary>
        /// Constructor: a Grant request from the Parser
        /// </summary>
        /// <param name="pr">The privilege</param>
        /// <param name="ob">The object</param>
        /// <param name="ge">The grantee</param>
        /// <param name="pb">The local base</param>
        public Grant(Privilege pr, long ob, long ge, long pp, Context cx)
            : this(Type.Grant, pr, ob, ge, pp, cx) { }
        protected Grant(Type t,Privilege pr, long ob, long ge, long pp, Context cx)
            : base(t, pp, cx)
		{
            priv = pr;
            obj = ob;
            grantee = ge;
        }
        public Grant(ReaderBase rdr) : base(Type.Grant, rdr) { }
        /// <summary>
        /// Constructor: a Grant request
        /// </summary>
        /// <param name="tp">The Grant type</param>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">the defining position</param>
		protected Grant(Type tp, ReaderBase rdr) : base(tp,rdr)
		{}
        protected Grant(Grant x, Writer wr) : base(x, wr)
        {
            priv = x.priv;
            obj = wr.Fix(x.obj);
            grantee = wr.Fix(x.grantee);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Grant(this, wr);
        }
        /// <summary>
        /// Serilaise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutInt((int)priv);
            wr.PutLong(obj);
            wr.PutLong(grantee);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise the Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(ReaderBase rdr)
		{
			priv = (Privilege)rdr.GetInt();
			obj = rdr.GetLong();
			grantee = rdr.GetLong();
			base.Deserialise(rdr);
		}
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Grant:
                    {
                        var g = (Grant)that;
                        if (obj == g.obj && grantee == g.grantee)
                            return new DBException("40051", ppos, that, ct);
                        break;
                    }
                case Type.Drop:
                    if (obj == ((Drop)that).delpos)
                        return new DBException("40010", ppos, that, ct);
                    break;
                case Type.Alter3:
                case Type.Alter2:
                case Type.Alter:
                    if (obj == ((Alter)that).defpos)
                        return new DBException("40051", ppos, that, ct);
                    break;
                case Type.Change:
                    if (obj == ((Change)that).affects)
                        return new DBException("40051", ppos, that, ct);
                    break;
                case Type.Modify:
                    {
                        var m = (Modify)that;
                        if (obj == m.modifydefpos || grantee == m.modifydefpos)
                            return new DBException("40051", ppos, that, ct);
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Grant "+priv.ToString()+" on "+Pos(obj)+" to "+((grantee>0)?Pos(grantee):"PUBLIC");
		}
        /// <summary>
        /// (Proc and View always use definer's role so no reparsing)
        /// </summary>
        /// <param name="db"></param>
        /// <param name="ro"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        internal override void Install(Context cx, long p)
        {
            var gee = (DBObject)cx.db.objects[grantee];
            var ro = cx.db.role;
            var oi = ro.infos[obj] as ObInfo;
            if (oi == null)
                throw new DBException("42105");
            // limit any grant to PUBLIC 
            if (gee is Role r)
            {
                ro = r;
                if (ro.defpos == Database.Public)
                    priv = (Privilege)((int)priv & 0xfff);
            }
    //        if (gee is User u && u.initialRole==Database.Public)
    //            ro += (User.InitialRole, obj);
            var ci = ro.infos[obj] as ObInfo;
            var cp = ci?.priv ?? Privilege.NoPrivilege;
            var rt = CList<long>.Empty;
            // limit grant to the request, then add the requested privilege (as limited)
            var pr = (oi.priv & cp) | priv;
            // if its a table, modify grantees privileges on columns too
            if (cx.db.objects[obj] is Table tb)
            {
                if (priv.HasFlag(Privilege.Select))
                {
                    rt = oi.domain.rowType;
                    for (var b = tb.domain.rowType.First(); b != null; b = b.Next())
                    {
                        var c = b.value();
                        var ic = (ObInfo)cx.db.role.infos[c];
                        ro += (new ObInfo(c, ic.name, ic.domain, pr), false);
                    }
                }
                else
                    for (var b = tb.domain.rowType.First(); b != null; b = b.Next())
                    {
                        var c = b.value();
                        if (ro.infos[c] is ObInfo ic &&
                                ic.priv.HasFlag(Privilege.Select))
                            rt += c;
                    }
            }
            // install the privilege on the target object
            ro += (new ObInfo(obj, oi.name, oi.domain + (Domain.RowType,rt),pr),true);
            cx.db += (ro, p);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
    }
    internal class Authenticate : Physical
    {
        internal long userpos;
        internal string pwd;
        internal long irolepos;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,userpos)) return userpos;
            if (!Committed(wr,irolepos)) return irolepos;
            return -1;
        }
        internal Authenticate(long us, string p, long r, long pp, Context cx)
            : base(Type.Authenticate, pp, cx)
        {
            userpos = us; pwd = p ?? ""; irolepos = r;
        }
        internal Authenticate(ReaderBase rdr) : base(Type.Authenticate, rdr) { }
        protected Authenticate(Authenticate x, Writer wr) : base(x, wr)
        {
            userpos = wr.Fix(x.userpos);
            irolepos = wr.Fix(x.irolepos);
            pwd = x.pwd;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Authenticate(this, wr);
        }

        public override void Serialise(Writer wr)
        {
            wr.PutLong(userpos);
            wr.PutString(pwd);
            wr.PutLong(irolepos);
            base.Serialise(wr);
        }

        public override void Deserialise(ReaderBase rdr)
        {
            userpos = rdr.GetLong();
            pwd = rdr.GetString();
            irolepos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "Authenticate [" +userpos+"] "+ pwd + " FOR [" + irolepos+"]";
        }

        internal override void Install(Context cx, long p)
        {
            throw new NotImplementedException();
        }
    }
}
