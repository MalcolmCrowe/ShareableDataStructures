using System;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
namespace Pyrrho.Level2
{
	/// <summary>
	/// A procedure or function definition. Method definitions use the PMethod subclass
	/// </summary>
	internal class PProcedure : Physical
	{
        /// <summary>
        /// The defining position for the Procedure
        /// </summary>
		public virtual long defpos { get { return ppos; }}
        /// <summary>
        /// The name of the procedure 
        /// </summary>
		public string name,nameAndArity;
        /// <summary>
        /// The number of parameters
        /// </summary>
		public int arity;
        /// <summary>
        /// The return type
        /// </summary>
        public long retdefpos;
        /// <summary>
        /// The definition of the procedure
        public string proc_clause;
        public override long Dependent(Writer wr)
        {
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,retdefpos)) return retdefpos;
            return -1;
        }
        /// </summary>
        /// <summary>
        /// Constructor: a procedure or function definition from the Parser
        /// </summary>
        /// <param name="nm">The name $ arity of the proc/func</param>
        /// <param name="pc">The procedure body including parameters</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PProcedure(string nm, long rt, string pc,long u, Transaction tr)
			:this(Type.PProcedure2,nm,rt,pc,u,tr)
		{
		}
        /// <summary>
        /// Constructor: a procedure or function definition from the Parser
        /// </summary>
        /// <param name="tp">The PProcedure or PMethod type</param>
        /// <param name="nm">The name $ arity of the proc/func</param>
        /// <param name="pc">The procedure body including parameters</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected PProcedure(Type tp, string nm, long rt, string pc,long u, Transaction tr)
			:base(tp,u,tr)
		{
            proc_clause = pc;
            retdefpos = rt;
            var ss = nm.Split('$');
            nameAndArity = nm;
            name = ss[0];
			arity = int.Parse(ss[1]);
		}
        /// <summary>
        /// Constructor: a procedure or function definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PProcedure(Type tp, Reader rdr) : base(tp,rdr) {}
        protected PProcedure(PProcedure x, Writer wr) : base(x, wr)
        {
            proc_clause = x.proc_clause;
            retdefpos = wr.Fix(x.retdefpos);
            nameAndArity = x.nameAndArity;
            name = x.name;
            arity = x.arity;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PProcedure(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr) 
		{
            wr.PutString(nameAndArity.ToString());
            wr.PutInt(arity);
            retdefpos = wr.Fix(retdefpos);
            if (type==Type.PMethod2 || type==Type.PProcedure2)
                wr.PutLong(retdefpos);
            wr.PutString(proc_clause);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			nameAndArity=rdr.GetString();
			arity=rdr.GetInt();
            if (type == Type.PMethod2 || type == Type.PProcedure2)
                retdefpos = rdr.GetLong();
            else
                retdefpos = -1;
            if (this is PMethod mt && mt.methodType == PMethod.MethodType.Constructor)
                retdefpos = mt.typedefpos;
			proc_clause=rdr.GetString();
			base.Deserialise(rdr);
		}
        /// <summary>
        /// A readble version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Procedure "+nameAndArity+"("+arity+")"+((retdefpos>0)?("["+Pos(retdefpos)+"] "):"") + proc_clause;
		}
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.PProcedure:
                    return (nameAndArity == ((PProcedure)that).nameAndArity) ? ppos : -1;
                case Type.Change:
                    return (nameAndArity == ((Change)that).name) ? ppos : -1;
                case Type.Ordering:
                    return (defpos == ((Ordering)that).funcdefpos) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }

        internal override Database Install(Database db, Role ro, long p)
        {
            var pr = new Procedure(this, db, false, Sqlx.CREATE, BTree<long, object>.Empty);
            return db + (ro,pr);
        }
    }
}
