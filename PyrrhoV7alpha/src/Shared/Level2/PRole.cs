using System.Text;
using Pyrrho.Common;
using Pyrrho.Level4;
using Pyrrho.Level3;
using System.Threading;
using System.Xml;
using System.Reflection.Metadata;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
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
	internal class PRole: Defined
	{
        /// <summary>
        /// The description of the role
        /// </summary>
        public string details = "";
        /// <summary>
        /// Constructor: a Role definition from the Parser
        /// </summary>
        /// <param name="nm">The name of the role</param>
        /// <param name="dt">The description of the role</param>
        /// <param name="wh">The physical database</param>
        /// <param name="curpos">The position in the datafile</param>
		public PRole(string nm,string dt,long pp, Context cx) 
            : base(Type.PRole,pp,cx,nm,Role.use|Role.admin) // the role's definer is its administrator by default
		{
            details = dt;
            infos += (ppos, new ObInfo(nm, Role.use));
        }
        public PRole(Reader rdr) : base(Type.PRole, rdr) { }
        protected PRole(PRole x, Writer wr) : base(x, wr)
        {
            details = x.details;
            if (wr.cx.role.name == x.name)
                wr.cx.db += (Database.Role,wr.cx.role+ (DBObject.Infos, infos));
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
            // name = rdr.GetString();  will not work here
            var nm = rdr.GetString();
            infos = new BTree<long,ObInfo>(rdr.context.role.defpos,new ObInfo(nm,Grant.AllPrivileges));
            if (type == Type.PRole)
                details = rdr.GetString();
			base.Deserialise(rdr);
		}
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PRole1:
                case Type.PRole:
                    if (name==((PRole)that).name)
                        return new DBException("40032", name, that, ct);
                    break;
                case Type.Change:
                    if (name == ((Change)that).name)
                        return new DBException("40032", name, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() { return "PRole "+name; }
        internal override DBObject? Install(Context cx, long p)
        {
            // If this is the first Role to be defined, 
            // it becomes the schema role and also the current role
            var first = cx.db.roles.Count == Database._system.roles.Count;
            var nr = new Role(this, cx.db, first);
            if (first) // make the new Role the Schema role, and the definer of all objects so far
            {
                cx.db += (Database._Schema, nr.defpos);
                for (var b = cx.db.objects.PositionAt(0); b != null && b.key()<Transaction.Analysing; 
                    b = b.Next())
                {
                    var k = b.key();
                    var ob = (DBObject)b.value();
                    if (ob.GetType().Name=="Domain" || ob.defpos<=0) // but Domains always belong to Database._system._role
                        continue;
                    var os = ob.infos;
                    var oi = os[-502]??throw new PEException("PE1410");
                    os -= -502;
                    os += (nr.defpos, oi);
                    cx.db += (k, ob.New(ob.mem+ (DBObject.Definer, nr.defpos)
                          +(DBObject.Infos,os)));
                }
            }
            cx.db = cx.db+(nr,p)+(Database.Roles,cx.db.roles+(name,nr.defpos));
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            if (first)
            {
                nr += (DBObject.Definer, nr.defpos);
                nr += (DBObject.Infos, infos + (nr.defpos, new ObInfo(name, Grant.AllPrivileges)));
                cx.db = cx.db + (DBObject.Definer, nr.defpos)
                    + (nr, p);
            }
            return nr;
        }
    }
    /// <summary>
    /// Not all metadata syntax gives a PMetadata: see PNodeType and PEdgeType
    /// </summary>
     internal class PMetadata : Physical
     {
         public string? name = null;
         /// <summary>
         /// column sequence number for view column
         /// </summary>
        public long seq = -1L; // backward compatibility
        public long defpos;
        public CTree<Sqlx,TypedValue> detail = CTree<Sqlx,TypedValue>.Empty;
        public string iri = "";
        public long refpos = -1L;
        public long flags = 0L;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,refpos)) return refpos;
            return -1;
        }
        public PMetadata(string nm, long sq, DBObject ob, CTree<Sqlx,TypedValue> md, long pp)
            : this(Type.Metadata, nm, sq, ob, md, pp) { }
        public PMetadata(Type t,string nm,long sq,DBObject ob, CTree<Sqlx,TypedValue> md,long pp)
            :base(t,pp)
        { 
            name = nm;
            seq = sq;
            defpos = ob.defpos;
            detail = md;
            iri = md[Sqlx.IRI]?.ToString()??"";
            refpos = md[Sqlx.INVERTS]?.ToLong()??-1L;
            flags = 0L;
        }
        public PMetadata(Reader rdr) : this(Type.Metadata, rdr) { }
        protected PMetadata(Type t, Reader rdr) : base(t, rdr) { }
        protected PMetadata(PMetadata x, Writer wr) : base(x, wr)
        {
            name = x.name;
            seq = x.seq;
            defpos = wr.cx.Fix(x.defpos);
            detail = x.detail;
            iri = x.iri;
            refpos = wr.cx.Fix(x.refpos);
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
            wr.PutString(name?.ToString()??"");
            wr.PutString(Detail(wr));
            wr.PutString(iri??"");
            wr.PutLong(seq+1); 
            defpos = wr.cx.Fix(defpos);
            wr.PutLong(defpos);
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
            detail = new Parser(rdr.context,rdr.GetString()).ParseMetadata(Sqlx.ANY);
            iri = rdr.GetString();
            seq = rdr.GetLong()-1;
            defpos = rdr.GetLong();
            flags = rdr.GetLong();
            base.Deserialise(rdr);
		}
        string Detail(Writer wr)
        {
            var sb = new StringBuilder();
            for (var b = detail.First(); b != null; b = b.Next())
                switch (b.key())
                {
                    case Sqlx.DESC:
                    case Sqlx.URL:
                        sb.Append(b.key());
                        sb.Append('\'');
                        sb.Append(b.value());
                        sb.Append("' ");
                        break;
                    case Sqlx.MIME:
                    case Sqlx.SQLAGENT:
                    case Sqlx.USER:
                    case Sqlx.PASSWORD:
                        sb.Append(b.key());
                        sb.Append(" \"");
                        sb.Append(b.value());
                        sb.Append("\" ");
                        break;
                    case Sqlx.IRI:
                        sb.Append(b.value().ToString());
                        break;
                    case Sqlx.INVERTS:
                        sb.Append(b.key());
                        sb.Append(' ');
                        if (b.value().ToLong() is long lp && wr.cx.db.objects[lp] is DBObject ob &&
                            ob.infos[wr.cx.role.defpos] is ObInfo oi && oi.name != null)
                            sb.Append(oi.name);
                        sb.Append(' ');
                        break;
                    case Sqlx.PREFIX:
                    case Sqlx.SUFFIX:
                        sb.Append(b.key());
                        sb.Append('"');
                        sb.Append(b.value());
                        sb.Append("\" ");
                        break;
                    case Sqlx.MAX:
                    case Sqlx.MIN:  
                        {
                            var lw = detail[Sqlx.MIN];
                            var hi = detail[Sqlx.MAX]??new TChar("*");
                            sb.Append("CARDINALITY("); sb.Append(lw);
                            { sb.Append(" TO "); sb.Append(hi); }
                            sb.Append(')');
                        }
                        break;
                    default:
                        sb.Append(b.key());
                        sb.Append(' ');
                        break;
                }
            return sb.ToString();
        }
        internal static long Flags(CTree<Sqlx,TypedValue> md)
        {
            return 0L;
        }
        internal string MetaFlags()
        {
            return detail.ToString();
        }
        internal CTree<Sqlx,TypedValue> Metadata()
        {
            return detail;
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() 
        {
            var sb = new StringBuilder();
            sb.Append("PMetadata "); sb.Append(name);
            sb.Append(detail);
            return sb.ToString();
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that,PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Metadata3:
                case Type.Metadata2:
                case Type.Metadata:
                    {
                        var t = (PMetadata)that;
                        if (defpos == t.defpos || name == t.name)
                            return new DBException("40041", defpos, that, ct);
                        break;
                    }
                case Type.Drop:
                    if (((Drop)that).delpos == defpos)
                        return new DBException("40010", defpos, that, ct);
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
        internal override DBObject? Install(Context cx, long p)
        {
            var ob = ((DBObject?)cx.db.objects[defpos]) ?? throw new DBException("42000");
            ob = ob.Add(cx,this, p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return ob;
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
        /// <param name="ob">the DBObject</param>
        /// <param name="db">The physical database</param>
        protected PMetadata2(Type tp,string nm, long sq, DBObject ob, CTree<Sqlx,TypedValue> md, long pp)
         : base(tp, nm, sq, ob, md, pp)
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
            return "PMetadata2 " + name + "[" + defpos + "." + seq + "]" + detail +
                iri;
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
        /// <param name="ob">the DBObject</param>
        /// <param name="wh">The physical database</param>
        /// <param name="curpos">The position in the datafile</param>
        public PMetadata3(string nm, long sq, DBObject ob, CTree<Sqlx,TypedValue> md, long pp)
            : base(Type.Metadata3, nm, sq, ob, md, pp)
        {  }
        public PMetadata3(Reader rdr) : base(Type.Metadata3, rdr) { }
        protected PMetadata3(PMetadata3 x, Writer wr) : base(x, wr)
        {  }
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
            return "PMetadata3 " + name + "[" + defpos + "." + seq + "]" + detail +
                iri;
        }

    }
}
