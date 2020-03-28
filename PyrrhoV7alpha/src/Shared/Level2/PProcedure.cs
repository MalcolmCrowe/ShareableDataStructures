using System;
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
        public bool mth = false;
        public Procedure proc;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos!=ppos && !Committed(wr,defpos)) return defpos;
            if (!Committed(wr,retdefpos)) return retdefpos;
            return -1;
        }
        public PProcedure(string nm, int ar, long rt, string pc, long pp, Context cx) :
            this(Type.PProcedure2, nm, ar, rt, pc, pp, cx)
        { }
        /// <summary>
        /// Constructor: a procedure or function definition from the Parser.
        /// The procedure clause is optional in this constructor to enable parsing
        /// of recursive procedure declarations (the parser fills it in later).
        /// The parse step in this constructor is used for methods and constructors, and
        /// the procedure heading is included in the proc_clause for backward compatibility.
        /// </summary>
        /// <param name="tp">The PProcedure or PMethod type</param>
        /// <param name="nm">The name of the proc/func</param>
        /// <param name="ar">The arity</param>
        /// <param name="rt">The return type</param>
        /// <param name="pc">The procedure clause including parameters, or ""</param>
        /// <param name="db">The database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected PProcedure(Type tp, string nm, int ar, long rt, string pc,long pp, Context cx)
            : base(tp,pp,cx)
		{
            proc_clause = pc;
            retdefpos = rt;
            name = nm;
            nameAndArity = nm + "$" + ar;
            arity = ar;
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
            var s = proc_clause;
            if (wr.cx.db.format < 51)
                s = DigestSql(wr,s);
            wr.PutString(s);
			base.Serialise(wr);
            Compile(wr.cx, wr.Length);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			nameAndArity=rdr.GetString();
            var ss = nameAndArity.Split('$');
            name = ss[0];
			arity=rdr.GetInt();
            if (type == Type.PMethod2 || type == Type.PProcedure2)
                retdefpos = rdr.GetLong();
            else
                retdefpos = -1;
            if (this is PMethod mt && mt.methodType == PMethod.MethodType.Constructor)
                retdefpos = mt.typedefpos;
			proc_clause=rdr.GetString();
			base.Deserialise(rdr);
            Compile(rdr.context, rdr.Position);
        }
        protected void Compile(Context cx, long p)
        {
            var op = cx.db.parse;
            var ro = cx.db.role;
            cx.db += (Database._ExecuteStatus, ExecuteStatus.Parse);
            // preinstall the bodyless proc to allow recursive procs
            Install(cx, p);
            var pr = cx.db.objects[ppos] as Procedure;
            proc = new Parser(cx.db).ParseProcedureBody(pr, proc_clause);
            cx.db += (proc, p);
            Install(cx, p);
            cx.db += (Database._ExecuteStatus, op);
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

        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var priv = Grant.Privilege.Owner | Grant.Privilege.Execute
                | Grant.Privilege.GrantExecute;
            var rd = (Domain)cx.db.objects[retdefpos];
            if (proc!=null)
                proc += (Procedure.RetType, rd);
            var ri = (ObInfo)ro.obinfos[retdefpos];
            var oi = new ObInfo(defpos, name, rd, priv);
            if (ri != null)
                oi += (ObInfo.Columns, ri.columns);
            var pr = proc??new Procedure(this, cx.db, BTree<long, object>.Empty);
            ro = ro + oi + this;
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db = cx.db + (ro,p) +(pr,p);
        }
    }
}
