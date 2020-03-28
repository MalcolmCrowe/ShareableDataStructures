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
	/// A PMethod gives details for a user defined type method
	/// </summary>
	internal class PMethod : PProcedure
	{
        /// <summary>
        /// The different sorts of Method: static, constructor etc
        /// </summary>
		public enum MethodType { Instance,Overriding, Static,Constructor };
        /// <summary>
        /// The defining position of the UDT
        /// </summary>
		public long typedefpos;
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
        /// <param name="td">The defining position of the owning type</param>
        /// <param name="pc">The procedure clause</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PMethod(string nm, int ar, long rt, MethodType mt, long td, string pc, 
            long pp, Context cx)
            : this(Type.PMethod2,nm,ar,rt,mt,td,pc,pp,cx)
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
        protected PMethod(Type tp, string nm, int ar, long rt, MethodType mt, long td, string pc, 
            long pp, Context cx)
            : base(tp,nm,ar,rt,pc,pp,cx)
		{
			typedefpos = td;
			methodType = mt;
            if (mt == MethodType.Constructor)
                retdefpos = typedefpos;
		}
        /// <summary>
        /// Constructor: a new Method definition from the ReadBuffer
        /// </summary>
        /// <param name="bp">the ReadBuffer</param>
        /// <param name="pos">The defining position</param>
		public PMethod(Type tp, Reader rdr) : base(tp,rdr){}
        protected PMethod(PMethod x, Writer wr) : base(x, wr)
        {
            typedefpos = wr.Fix(x.typedefpos);
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
            wr.PutLong(typedefpos);
            wr.PutInt((int)methodType);
			base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			typedefpos = rdr.GetLong();
			methodType = (MethodType)rdr.GetInt();
 //           if (methodType == PMethod.MethodType.Constructor) will be done in base
 //               retdefpos = typedefpos; 
            base.Deserialise(rdr);
            Compile(rdr.context, rdr.Position);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
            return "Method " + methodType.ToString()+" " + Pos(typedefpos) + "." + nameAndArity + "[" + Pos(retdefpos) + "] " + proc_clause; 
		}
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.Drop:
                    return (typedefpos == ((Drop)that).delpos) ? ppos : -1;
                case Type.Change:
                    return (typedefpos == ((Change)that).affects) ? ppos : -1;
                case Type.PMethod2:
                case Type.PMethod:
                    {
                        var t = (PMethod)that;
                        return (typedefpos == t.typedefpos && nameAndArity == t.nameAndArity) ? ppos : -1;
                    }
                case Type.Modify:
                    return (nameAndArity == ((Modify)that).name) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var oi = (ObInfo)ro.obinfos[typedefpos];
            var mt = new Method(this,Sqlx.CREATE,cx.db);
            var priv = Grant.Privilege.Select | Grant.Privilege.GrantSelect |
                Grant.Privilege.Execute | Grant.Privilege.GrantExecute;
            var mi = new ObInfo(defpos, name, mt.domain, priv);
            ro = ro + mt + mi + (oi+(mt,name));
            cx.db = cx.db + (ro,p) + (mt,p);
        }
    }
}
