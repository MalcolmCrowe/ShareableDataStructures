using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level1;
using Pyrrho.Level4;
using Pyrrho.Level3;
using System.Security.Principal;

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
	/// A Role definition
	/// </summary>
	internal class PRole: Physical
	{
        /// <summary>
        /// The name of the Role
        /// </summary>
		public string name;
        /// <summary>
        /// The description of the role
        /// </summary>
        public string details = "";
        public override long Dependent(Writer wr, Transaction tr)
        {
            return -1;
        }
        /// <summary>
        /// Constructor: a Role definition from the Parser
        /// </summary>
        /// <param name="nm">The name of the role</param>
        /// <param name="dt">The description of the role</param>
        /// <param name="wh">The physical database</param>
        /// <param name="curpos">The position in the datafile</param>
		public PRole(string nm,string dt,long pp, Context cx) 
            : base(Type.PRole,pp,cx)
		{
            name = nm;
            details = dt;
        }
        public PRole(Reader rdr) : base(Type.PRole, rdr) { }
        protected PRole(PRole x, Writer wr) : base(x, wr)
        {
            name = x.name;
            details = x.details;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PRole(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutString(name.ToString());
            if (type==Type.PRole)
                wr.PutString(details);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr) 
		{
            name = rdr.GetString();
            if (type == Type.PRole)
                details = rdr.GetString();
			base.Deserialise(rdr);
		}
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PRole:
                    if (name==((PRole)that).name)
                        return new DBException("40032", ppos, that, ct);
                    break;
                case Type.Change:
                    if (name == ((Change)that).name)
                        return new DBException("40032", ppos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() { return "PRole "+name; }
        internal override void Install(Context cx, long p)
        {
            // If this is the first Role to be defined, 
            // it becomes the schema role and also the current role
            var first = cx.db.roles.Count == Database._system.roles.Count;
            var nr = new Role(this, cx.db, first);
            if (first) // give the new Role the Schema uid
                nr = (Role)nr.Relocate(cx.db._role);
            // give the current role privileges on the new Role
            var ri = new ObInfo(nr.defpos, name, Domain.Role,Role.use|Role.admin);
            nr += (ri, true);
            var ro = cx.db.role + (nr.defpos, ri) + (ri,true);
            cx.db = cx.db+(ro,p)+(nr,p)+(Database.Roles,cx.db.roles+(name,nr.defpos));
            if (first)
                cx.db = cx.db+(DBObject.Definer, nr.defpos)+(Database._Role,nr.defpos);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
    }
     internal class PMetadata : Physical
     {
         public string name = null;
         /// <summary>
         /// column sequence number for view column
         /// </summary>
        public long seq;
        public long defpos;
        public string detail = "";
        public string iri = "";
        public long refpos;
        public long flags;
        public static Sqlx[] keys = { Sqlx.NO, Sqlx.ENTITY, Sqlx.ATTRIBUTE, //0x0-0x2
            Sqlx.PIE, Sqlx.NONE, Sqlx.POINTS, Sqlx.X, Sqlx.Y, Sqlx.HISTOGRAM, //0x4-0x80
            Sqlx.LINE, Sqlx.CAPTION, Sqlx.NONE, Sqlx.NONE, Sqlx.NONE, Sqlx.NONE, //0x100-0x4000
            Sqlx.LEGEND, Sqlx.URL, Sqlx.MIME, Sqlx.SQLAGENT, Sqlx.USER, // 0x8000-0x80000
            Sqlx.PASSWORD, Sqlx.IRI}; // 0x100000-0x200000
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,refpos)) return refpos;
            return -1;
        }
        public PMetadata(string nm, long sq, long ob, BTree<Sqlx,object> md, long pp, Context cx)
            : this(Type.Metadata, nm, sq, ob, md, pp, cx) { }
        public PMetadata(Type t,string nm,long sq,long ob, BTree<Sqlx,object> md,long pp,Context cx)
            :base(t,pp,cx)
        { 
            name = nm;
            seq = sq;
            defpos = ob;
            detail = Detail(md);
            iri = Iri(md);
            refpos = Inv(md);
            flags = Flags(md);
        }
        protected PMetadata(Type t, string nm, long sq, long ob, string ds, 
            string ir,long rf,BTree<Sqlx,object> md,long pp, Context cx)
          : base(t, pp, cx)
        {
            name = nm;
            seq = sq;
            defpos = ob;
            detail = ds;
            iri = ir;
            refpos = rf;
            flags = Flags(md);
        }
        public PMetadata(Reader rdr) : this(Type.Metadata, rdr) { }
        protected PMetadata(Type t, Reader rdr) : base(t, rdr) { }
        protected PMetadata(PMetadata x, Writer wr) : base(x, wr)
        {
            name = x.name;
            seq = x.seq;
            defpos = wr.Fix(x.defpos);
            detail = x.detail;
            iri = x.iri;
            refpos = wr.Fix(x.refpos);
            flags = x.flags;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PMetadata(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutString(name.ToString());
            wr.PutString(detail);
            wr.PutString(iri);
            defpos = wr.Fix(defpos);
            wr.PutLong(seq+1); 
            wr.PutLong(wr.Fix(refpos));
            wr.PutLong(flags);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr) 
		{
			name =rdr.GetString();
            detail = rdr.GetString();
            iri = rdr.GetString();
            seq = rdr.GetLong()-1;
            defpos = rdr.GetLong();
            flags = rdr.GetLong();
            base.Deserialise(rdr);
		}
        internal static long Flags(BTree<Sqlx,object> md)
        {
            var r = 0L;
            for (var b=md.First();b!=null;b=b.Next())
            {
                var m = 1L;
                for (var i = 0; i < keys.Length; i++, m <<= 1)
                    if (b.key() == keys[i])
                        r += m;
            }
            return r;
        }
        internal string MetaFlags()
        {
            var sb = new StringBuilder();
            var cm = "";
            var m = 1L;
            for (var i=0;i<keys.Length;i++,m<<=1)
                if ((flags&m)!=0L)
                { sb.Append(cm); cm = " "; sb.Append(keys[i]); }
            return sb.ToString();
        }
        internal BTree<Sqlx,object> Metadata()
        {
            var r = BTree<Sqlx, object>.Empty;
            var m = 1L;
            for (var i = 1; i < keys.Length; i++, m <<= 1)
                if ((flags & m) != 0L)
                {
                    object v = "";
                    var k = keys[i];
                    switch (k)
                    {
                        case Sqlx.NO:
                        case Sqlx.URL:
                        case Sqlx.MIME:
                        case Sqlx.SQLAGENT:
                            r += (k, (detail == "") ? iri : detail);
                            break;
                        case Sqlx.INVERTS:
                            r += (k, refpos);
                            break;
                        default:
                            r += (k, v);
                            break;
                    }
                }
            return r;
        }
        long Inv(BTree<Sqlx,object> md)
        {
            return (long)(md[Sqlx.INVERTS] ?? -1L);
        }
        string Iri(BTree<Sqlx, object> md)
        {
            return (string)md[Sqlx.NO];
        }
        string Detail(BTree<Sqlx,object> md)
        {
            return (string)md[Sqlx.NO];
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() 
        {
            var sb = new StringBuilder();
            sb.Append("PMetadata "); sb.Append(name);
            sb.Append("["); sb.Append(DBObject.Uid(defpos));
            if (seq >= 0) { sb.Append("."); sb.Append(seq);  }
            sb.Append("]");
            var m = 1L;
            var cm = "";
            for (var i = 1; i < keys.Length; i++, m <<= 1)
                if ((flags&m)!=0L)
                {
                    sb.Append(cm); cm = ","; sb.Append(keys[i]);
                }
            if (detail != "")
            { sb.Append("("); sb.Append(detail); sb.Append(")"); }
            sb.Append(iri);
            return sb.ToString();
        }
        public override DBException Conflicts(Database db, Context cx, Physical that,PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Metadata3:
                case Type.Metadata2:
                case Type.Metadata:
                    {
                        var t = (PMetadata)that;
                        if (defpos == t.defpos || name == t.name)
                            return new DBException("40041", ppos, that, ct);
                        break;
                    }
                case Type.Drop:
                    if (((Drop)that).delpos == defpos)
                        return new DBException("40010", ppos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that,ct);
        }
        /// <summary>
        /// The default behaviour is to update the ObInfo for the current role.
        /// We allow for the possibility that an object itself may be affected by metadata.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="p"></param>
        internal override void Install(Context cx, long p)
        {
            var oi = (ObInfo)cx.db.role.infos[defpos];
            var ni = oi + this;
            if (oi != ni)
            {
                var ro = cx.db.role + (ni, false);
                cx.db = cx.db+(ro, p);
            }
            cx.db = ((DBObject)cx.db.objects[defpos]).Add(cx.db,this, p);
       }
    }
     internal class PMetadata2 : PMetadata
     {
       /// <summary>
        /// Constructor: role-based metadata from the parser
        /// </summary>
        /// <param name="nm">The name of the object</param>
        /// <param name="md">The new metadata</param>
        /// <param name="sq">The column seq no for a view column</param>
        /// <param name="ob">the DBObject ref</param>
        /// <param name="db">The physical database</param>
        protected PMetadata2(Type tp,string nm, long sq, long ob, BTree<Sqlx,object> md, long pp,Context cx)
         : base(tp, nm, sq, ob, md, pp, cx)
        {
        }
        public PMetadata2(Reader rdr) : base (Type.Metadata2,rdr){}
        public PMetadata2(Type pt,Reader rdr) : base(pt, rdr) {}
        protected PMetadata2(PMetadata2 x, Writer wr) : base(x, wr)
        {
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PMetadata2(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
		{
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr) 
		{
            rdr.GetInt();
            rdr.GetLong();
			base.Deserialise(rdr);
		}
       /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() 
        {
            return "PMetadata2 " + name + "[" + defpos + "." + seq + "]" + ((detail != "") ? "(" + detail + ")" : "") +
                iri + flags;
        }

     }
    internal class PMetadata3 : PMetadata2
    {
        /// <summary>
        /// Constructor: role-based metadata from the parser
        /// </summary>
        /// <param name="nm">The name of the object</param>
        /// <param name="md">The new metadata</param>
        /// <param name="sq">The column seq no for a view column</param>
        /// <param name="ob">the DBObject ref</param>
        /// <param name="wh">The physical database</param>
        /// <param name="curpos">The position in the datafile</param>
        public PMetadata3(string nm, long sq, long ob, BTree<Sqlx,object> md, long pp, Context cx)
            : base(Type.Metadata3, nm, sq, ob, md, pp, cx)
        {
        }
        public PMetadata3(Reader rdr) : base(Type.Metadata3, rdr) { }
        protected PMetadata3(PMetadata3 x, Writer wr) : base(x, wr)
        {
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PMetadata3(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
        {
            wr.PutLong(refpos);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            refpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            return "PMetadata3 " + name + "[" + defpos + "." + seq + "]" + ((detail != "") ? "(" + detail + ")" : "") +
                iri + flags + DBObject.Uid(refpos);
        }

    }
}
