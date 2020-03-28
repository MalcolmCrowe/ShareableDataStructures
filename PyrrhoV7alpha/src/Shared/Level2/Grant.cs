using System;
using Pyrrho.Common;
using Pyrrho.Level1;
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
		public static Privilege AllPrivileges = (Privilege)0x4fffff;
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
        public Grant(Reader rdr) : base(Type.Grant, rdr) { }
        /// <summary>
        /// Constructor: a Grant request
        /// </summary>
        /// <param name="tp">The Grant type</param>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">the defining position</param>
		protected Grant(Type tp, Reader rdr) : base(tp,rdr)
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
        public override void Deserialise(Reader rdr)
		{
			priv = (Privilege)rdr.GetInt();
			obj = rdr.GetLong();
			grantee = rdr.GetLong();
			base.Deserialise(rdr);
		}
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.Grant:
                    {
                        var g = (Grant)that;
                        return (obj == g.obj && grantee == g.grantee) ? ppos : -1;
                    }
                case Type.Drop:
                    return (obj == ((Drop)that).delpos) ? ppos : -1;
                case Type.Alter3:
                case Type.Alter2:
                case Type.Alter:
                    return (obj == ((Alter)that).defpos) ? ppos : -1;
                case Type.Change:
                    return (obj == ((Change)that).affects) ? ppos : -1;
                case Type.Modify:
                    {
                        var m = (Modify)that;
                        return (obj == m.modifydefpos || grantee == m.modifydefpos) ? ppos : -1;
                    }
            }
            return base.Conflicts(db, tr, that);
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
        /// (Proc and View alwyaes use definer's role so no reparsing)
        /// </summary>
        /// <param name="db"></param>
        /// <param name="ro"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        internal override void Install(Context cx, long p)
        {
            var gee = cx.db.objects[grantee];
            var ro = cx.db.role;
            if (gee is Role r)
                ro = r;
            var oi = ro.obinfos[obj] as ObInfo;
            var pr = oi.priv | priv;
            ro += new ObInfo(obj, oi.name, oi.domain, pr, oi.mem);
            cx.db += (ro, p);
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
        internal Authenticate(Reader rdr) : base(Type.Authenticate, rdr) { }
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

        public override void Deserialise(Reader rdr)
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
