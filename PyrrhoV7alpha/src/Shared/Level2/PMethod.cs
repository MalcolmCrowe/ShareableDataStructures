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
	/// A PMethod gives details for a user defined type method
	/// </summary>
	internal class PMethod : PProcedure
	{
        /// <summary>
        /// The different sorts of Method: static, constructor etc
        /// </summary>
		public enum MethodType { Instance,Overriding, Static,Constructor };
        /// <summary>
        /// The UDT
        /// </summary>
		public Domain domain;
        /// <summary>
        /// The type of this method
        /// </summary>
		public MethodType methodType;
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
        public PMethod(string nm, BList<long> ar, Domain rt, 
            MethodType mt, Domain td, Method md, Ident sce,long pp, Context cx)
            : this(Type.PMethod2,nm,ar,rt,mt,td,md,sce,pp,cx)
		{}
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
        protected PMethod(Type tp, string nm, BList<long> ar, 
            Domain rt, MethodType mt, Domain td, Method md, Ident sce,
            long pp, Context cx)
            : base(tp,nm,ar,rt,md,sce,pp,cx)
		{
			domain = td;
			methodType = mt;
            if (mt == MethodType.Constructor)
                retType = td;
		}
        /// <summary>
        /// Constructor: a new Method definition from the ReadBuffer
        /// </summary>
        /// <param name="bp">the ReadBuffer</param>
        /// <param name="pos">The defining position</param>
		public PMethod(Type tp, Reader rdr) : base(tp,rdr){}
        protected PMethod(PMethod x, Writer wr) : base(x, wr)
        {
            domain = (Domain)x.domain._Relocate(wr);
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
            wr.PutLong(wr.cx.db.types[domain].Value);
            wr.PutInt((int)methodType);
			base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			domain = (Domain)rdr.context.db.objects[rdr.GetLong()];
			methodType = (MethodType)rdr.GetInt();
 //           if (methodType == PMethod.MethodType.Constructor) will be done in base
 //               retdefpos = typedefpos; 
            base.Deserialise(rdr);
            Compile(name, rdr.context, rdr.Position);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
            return "Method " + methodType.ToString()+" " + domain 
                + "." + nameAndArity + "[" + retType + "] " + source.ident; 
		}
        public override long Conflicts(Database db, Context cx, Physical that)
        {
            switch(that.type)
            {
                case Type.Drop:
                    return (db.types[domain] == ((Drop)that).delpos) ? ppos : -1;
                case Type.Change:
                    return (db.types[domain] == ((Change)that).affects) ? ppos : -1;
                case Type.PMethod2:
                case Type.PMethod:
                    {
                        var t = (PMethod)that;
                        return (db.types[domain] == db.types[t.domain]
                            && nameAndArity == t.nameAndArity) ? ppos : -1;
                    }
                case Type.Modify:
                    return (nameAndArity == ((Modify)that).name) ? ppos : -1;
            }
            return base.Conflicts(db, cx, that);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var mt = new Method(this,cx);
            var priv = Grant.Privilege.Select | Grant.Privilege.GrantSelect |
                Grant.Privilege.Execute | Grant.Privilege.GrantExecute;
            var mi = new ObInfo(defpos, name, domain)+(ObInfo.Privilege, priv);
            ro = ro + mt + mi + (cx.db.types[domain].Value,domain+(mt,name));
            cx.db += (ro, p);
            cx.Install(mt,p);
        }
    }
}
