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
        /// The return type etc
        /// </summary>
        public Domain retType;
        public Ident source;
        public bool mth = false;
        public BList<(long,Domain)> parameters;
        public Procedure proc;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr, defpos)) return defpos;
            return -1;
        }
        internal int arity => parameters.Length;
        public PProcedure(string nm, BList<(long,Domain)> ar, Domain rt, Procedure pr,Ident sce, 
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
        protected PProcedure(Type tp, string nm, BList<(long,Domain)> ps, Domain rt, Procedure pr,
            Ident sce, long pp, Context cx) : base(tp,pp,cx)
		{
            source = sce;
            parameters = ps;
            retType = rt;
            name = nm;
            nameAndArity = nm + "$" + arity;
            proc = pr;
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
            retType = (Domain)wr.cx.db.objects[wr.Fix(x.retType.defpos)];
            parameters = wr.Relocate(x.parameters);
            nameAndArity = x.nameAndArity;
            name = x.name;
            proc = (Procedure)x.proc.Relocate(wr.Fix(defpos)).Relocate(wr);
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
            retType = (Domain)retType?.Relocate(wr);
            if (type==Type.PMethod2 || type==Type.PProcedure2)
                wr.PutLong(retType.defpos);
            var s = source;
            if (wr.cx.db.format < 51)
                s = new Ident(DigestSql(wr,s.ident),s.iix);
            wr.PutString(s.ident);
            proc.Relocate(wr);
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
                retType = (Domain)rdr.context.db.objects[mt.typedefpos];
            source = new Ident(rdr.GetString(), ppos);
            var (pps, _) = new Parser(rdr.context.db, source)
                .ParseProcedureHeading(new Ident(name, ppos));
            parameters = ProcParameter.Formals(pps);
			base.Deserialise(rdr);
            Compile(name, rdr.context, rdr.Position);
        }
        protected void Compile(string name, Context cx, long p)
        {
            var op = cx.db.parse;
            cx.db += (Database._ExecuteStatus, ExecuteStatus.Parse);
            // preinstall the bodyless proc to allow recursive procs
            Install(cx, p);
            proc = cx.db.objects[ppos] as Procedure;
            proc = new Parser(cx.db).ParseProcedureBody(name, proc, source);
            Install(cx, p);
            cx.db += (Database._ExecuteStatus, op);
        }
        internal BList<ProcParameter> Ins(Database db)
        {
            var (pps, _) = new Parser(db,source).ParseProcedureHeading(new Ident(name, source.iix));
            return pps;
        }
        /// <summary>
        /// A readble version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Procedure "+nameAndArity+"("+arity+")"
                +((retType.defpos>0)?("["+Pos(retType.defpos)+"] "):"") + source.ident;
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
            var oi = new ObInfo(ppos,nameAndArity,retType,priv);
            ro = ro + oi + this;
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db = cx.db + (ro, p) + (proc, p);
        }
    }
}
