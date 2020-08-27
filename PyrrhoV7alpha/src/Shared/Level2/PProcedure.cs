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
	/// A procedure or function definition. Method definitions use the PMethod subclass
	/// </summary>
	internal class PProcedure : Compiled
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
        /// The return type for the definer's role
        /// </summary>
        public Domain retType;
        public Ident source;
        public bool mth = false;
        public BList<long> parameters;
        public long proc; // the procedure code is in Compiled.framing
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr, defpos)) return defpos;
            retType.Create(wr, tr);
            return -1;
        }
        internal int arity => parameters.Length;
        public PProcedure(string nm, BList<long> ar, Domain rt, Procedure pr,Ident sce, 
            long pp, Context cx) : this(Type.PProcedure2, nm, ar, rt, pr, sce, pp, cx)
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
        protected PProcedure(Type tp, string nm, BList<long> ps, Domain rt, Procedure pr,
            Ident sce, long pp, Context cx) : base(tp,pp,cx,new Framing(cx))
		{
            source = sce;
            parameters = ps;
            retType = rt;
            name = nm;
            nameAndArity = nm + "$" + arity;
            Frame(cx);
            proc = pr?.defpos??-1L;
        }
        /// <summary>
        /// Constructor: a procedure or function definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PProcedure(Type tp, Reader rdr) : base(tp,rdr) {}
        protected PProcedure(PProcedure x, Writer wr) : base(x, wr)
        {
            source = x.source;
            wr.srcPos = wr.Length + 1;
            retType = (Domain)x.retType._Relocate(wr);
            parameters = wr.Fix(x.parameters);
            nameAndArity = x.nameAndArity;
            name = x.name;
            proc = wr.Fix(x.defpos);
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
            retType = (Domain)retType?._Relocate(wr);
            if (type==Type.PMethod2 || type==Type.PProcedure2)
                wr.PutLong(wr.cx.db.types[retType].Value);
            var s = source;
            if (wr.cx.db.format < 51)
                s = new Ident(DigestSql(wr,s.ident),s.iix);
            wr.PutString(s.ident);
            proc = wr.Fix(proc);
			base.Serialise(wr);
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
			var n=rdr.GetInt();
            if (type == Type.PMethod2 || type == Type.PProcedure2)
                retType = (Domain)rdr.context.db.objects[rdr.GetLong()];
            else
                retType = Domain.Null;
            if (this is PMethod mt && mt.methodType == PMethod.MethodType.Constructor)
                retType = mt.domain;
            source = new Ident(rdr.GetString(), ppos);
            var (pps, _) = new Parser(rdr.context, source).
                ParseProcedureHeading(new Ident(name, ppos));
            parameters = pps;
			base.Deserialise(rdr);
            Compile(name, rdr.context, rdr.Position);
        }
        protected void Compile(string name, Context cx, long p)
        {
            var op = cx.db.parse;
            cx.db += (Database._ExecuteStatus, ExecuteStatus.Parse);
            // preinstall the bodyless proc to allow recursive procs
            Install(cx, p);
            proc = ppos;
            cx.db += (Database._ExecuteStatus, op);
        }
        /// <summary>
        /// A readble version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "PProcedure "+name+"("+arity+")"
                +((retType!=Domain.Null)?("["+retType+"] "):" ") + source.ident;
		}
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.PProcedure:
                    if (nameAndArity == ((PProcedure)that).nameAndArity)
                        return new DBException("40039", ppos, that, ct);
                    break;
                case Type.Change:
                    if (nameAndArity == ((Change)that).name)
                        return new DBException("40039", ppos, that, ct);
                    break;
                case Type.Ordering:
                    if (defpos == ((Ordering)that).funcdefpos)
                        return new DBException("40039", ppos, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }

        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            ro = ro + (new ObInfo(ppos,name,retType),true) + this;
            var pr = new Procedure(this, cx);
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db += (ro, p);
            cx.Install(pr, p);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
    }
}
