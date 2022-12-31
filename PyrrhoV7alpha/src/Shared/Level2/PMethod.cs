using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Data.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
	/// A PMethod gives details for a user defined type method
	/// </summary>
	internal class PMethod : PProcedure
	{
        /// <summary>
        /// The different sorts of Method: static, constructor etc
        /// </summary>
		public enum MethodType { Instance,Overriding, Static,Constructor };
		public UDType? udt;
        public long? _udt;
        /// <summary>
        /// The type of this method
        /// </summary>
		public MethodType? methodType;
        /// <summary>
        /// Constructor: a new Method definition from the Parser
        /// </summary>
        /// <param name="nm">The name $ arity of the method</param>
        /// <param name="rt">The return type</param>
        /// <param name="mt">The method type</param>
        /// <param name="td">The owning type</param>
        /// <param name="pc">The procedure clause</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PMethod(string nm, CList<long> ar, Domain rt, 
            MethodType mt, UDType td, Method? md, Ident sce,long pp, Context cx)
            : this(Type.PMethod2,nm,ar,rt,mt,td,md,sce,pp,cx)
		{ }
        /// <summary>
        /// Constructor: a new Method definition from the Parser
        /// </summary>
        /// <param name="tp">The PMethod type</param>
        /// <param name="nm">The name of the method</param>
        /// <param name="ar">The arity</param>
        /// <param name="rt">The return type</param>
        /// <param name="mt">The method type</param>
        /// <param name="td">The defining position of the type</param>
        /// <param name="pc">The procedure clause including body</param>
        /// <param name="u">The defining position for the method</param>
        /// /// <param name="db">The database</param>
        protected PMethod(Type tp, string nm, CList<long> ar, 
            Domain rt, MethodType mt, UDType td, Method? md, Ident sce,
            long pp, Context cx)
            : base(tp,nm,ar,rt,md,sce,pp,cx)
		{
			udt = td;
            _udt = td.defpos;
			methodType = mt;
            if (mt == MethodType.Constructor)
                dataType = td;
		}
        /// <summary>
        /// Constructor: a new Method definition from the ReadBuffer
        /// </summary>
        /// <param name="bp">the ReadBuffer</param>
        /// <param name="pos">The defining position</param>
		public PMethod(Type tp, Reader rdr) : base(tp,rdr){}
        protected PMethod(PMethod x, Writer wr) : base(x, wr)
        {
            if (x.udt is not UDType ut)
                throw new PEException("PE0610");
            var u = wr.cx.Fix(ut.defpos);
            udt = (UDType)(wr.cx.db.objects[u] ?? throw new PEException("PE42123"));
            _udt = udt.defpos;
            methodType = x.methodType;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PMethod(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr) 
		{
            if (udt is not UDType || methodType is null)
                throw new PEException("PE42125");
            wr.PutLong(udt.defpos);
            wr.PutInt((int)methodType);
			base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rd)
		{
            var r = rd.GetLong();
            _udt = r;
            udt = (UDType)(rd.context?._Dom(r) ?? throw new PEException("PE42126"));
			methodType = (MethodType)rd.GetInt();
            base.Deserialise(rd);
            if (methodType == MethodType.Constructor)
                dataType = udt;
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
            return "Method " + methodType.ToString()+" " 
                + DBObject.Uid(ppos) + "="+((_udt is long u)?DBObject.Uid(u):"_")
                + "." + name + source?.ident??""; 
		}
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            if (udt is not UDType)
                throw new PEException("PE42130");
            switch (that.type)
            {
                case Type.Drop:
                    if (udt.defpos == ((Drop)that).delpos)
                        return new DBException("40016", defpos, that, ct);
                    break;
                case Type.Change:
                    if (udt.defpos == ((Change)that).affects)
                        return new DBException("40021", defpos, that, ct);
                    break;
                case Type.PMethod2:
                case Type.PMethod:
                    {
                        var t = (PMethod)that;
                        if (udt.defpos == t.udt?.defpos
                            && name == t.name)
                            return new DBException("40039", defpos, that, ct);
                        break;
                    }
                case Type.Modify:
                    if (name == ((Modify)that).name)
                        return new DBException("40036", defpos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override DBObject? Install(Context cx, long p)
        {
            var rp = Database._system.role.defpos;
            var mt = new Method(this,cx);
            var priv = Grant.Privilege.Select | Grant.Privilege.GrantSelect |
                Grant.Privilege.Execute | Grant.Privilege.GrantExecute;
            udt = (UDType?)(cx.db.objects[udt?.defpos??-1L]??udt);
            if (cx.db==null || _udt == null || udt==null ||
                udt.infos[cx.role.defpos] is not ObInfo ui)
                throw new PEException("PE0611");
            var um = ui.methodInfos;
            var sig = cx.Signature(parameters);
            var om = um[name] ?? CTree<CList<Domain>, long>.Empty;
            um += (name, om+(sig, mt.defpos));
            ui += (ObInfo.MethodInfos, um);
            var mi = new ObInfo(name, priv);
            mt += (DBObject.Infos, new BTree<long, ObInfo>(rp, mi));
            udt += (DBObject.Infos, new BTree<long, ObInfo>(rp, ui));
            cx.db += (udt.defpos, udt);
            cx.db += (mt.defpos, mt);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(mt,p);
            return mt;
        }
    }
}
