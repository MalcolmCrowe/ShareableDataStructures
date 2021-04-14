using System;
using System.Configuration;
using System.Data.SqlTypes;
using System.Security.Principal;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
        public long retTypeDefpos;
        public Ident source;
        public bool mth = false;
        public CList<long> parameters;
        public long proc = -1; // the procedure code is in Compiled.framing
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr, defpos)) return defpos;
            retType = (Domain)retType._Relocate(wr);
            retTypeDefpos = retType.Create(wr, tr);
            wr.cx.db += (Database.Types,wr.cx.db.types+(retType,retTypeDefpos));
            return -1;
        }
        internal int arity;
        public PProcedure(string nm, CList<long> ar, Domain rt, Procedure pr,Ident sce, 
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
        protected PProcedure(Type tp, string nm, CList<long> ps, Domain rt, Procedure pr,
            Ident sce, long pp, Context cx) : base(tp,pp,cx,Framing.Empty,rt)
		{
            source = sce;
            parameters = ps;
            arity = parameters.Length;
            retType = rt;
            name = nm;
            nameAndArity = nm + "$" + arity;
            proc = pr?.body??-1L;
        }
        /// <summary>
        /// Constructor: a procedure or function definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PProcedure(Type tp, ReaderBase rdr) : base(tp,rdr) {}
        protected PProcedure(PProcedure x, Writer wr) : base(x, wr)
        {
            source = x.source;
            wr.srcPos = wr.Length + 1;
            retType = x.retType;
            retTypeDefpos = x.retTypeDefpos;
            parameters = wr.Fix(x.parameters);
            nameAndArity = x.nameAndArity;
            arity = x.arity;
            name = x.name;
            proc = wr.Fix(x.proc);
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
            if (type==Type.PMethod2 || type==Type.PProcedure2)
                wr.PutLong(retTypeDefpos);
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
        public override void Deserialise(ReaderBase rb)
		{
			nameAndArity=rb.GetString();
            var ss = nameAndArity.Split('$');
            name = ss[0];
			arity = rb.GetInt();
            retTypeDefpos = rb.GetLong();
            source = new Ident(rb.GetString(), ppos + 1);
			base.Deserialise(rb);
            if (rb is Reader rdr)
            {
                if (this is PMethod mt && mt.methodType == PMethod.MethodType.Constructor)
                    retType = mt.udt;
                if (type == Type.PMethod2 || type == Type.PProcedure2)
                    retType = (Domain)rdr.context.db.objects[retTypeDefpos];
                else
                    retType = Domain.Null;
                var psr = new Parser(rdr.context, source);
                var (pps, _) = psr.ParseProcedureHeading(new Ident(name, ppos));
                framing = new Framing(psr.cx);
                parameters = pps;
                Compile(rdr);
            }
            else
                retType = rb.GetDomain(retTypeDefpos,ppos);
        }
        protected void Compile(Reader rdr)
        {
            // preinstall the bodyless proc to allow recursive calls
            var psr = new Parser(rdr.context, source);
            Install(psr.cx, rdr.Position);
            var (_, xp) = psr.ParseProcedureHeading(new Ident(name, ppos));
            proc = psr.ParseProcedureStatement(xp)?.defpos??-1L;
            psr.cx.obs += (ppos, ((Procedure)psr.cx.obs[ppos]) + (Procedure.Body, proc));
            Frame(psr.cx);
            // final installation now that the body is defined
            Install(rdr.context, rdr.Position);
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
                case Type.PProcedure2:
                case Type.PProcedure:
                case Type.PMethod:
                case Type.PMethod2:
                    if (nameAndArity == ((PProcedure)that).nameAndArity)
                        return new DBException("40039", name, that, ct);
                    break;
                case Type.Change:
                    if (nameAndArity == ((Change)that).name)
                        return new DBException("40039", name, that, ct);
                    break;
                case Type.Ordering:
                    if (defpos == ((Ordering)that).funcdefpos)
                        return new DBException("40039",name, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override void OnLoad(Reader rdr)
        {
            var psr = new Parser(rdr.context);
            var pr = (Procedure)rdr.context.db.objects[ppos];
            psr.cx.srcFix = ppos + 1;
            rdr.context.obs += (pr.defpos, pr+(Procedure.Body,proc));
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            ro = ro + (new ObInfo(ppos, name, retType,
                Grant.Privilege.Execute|Grant.Privilege.GrantExecute), true) + this;
            var pr = new Procedure(this, cx) + (DBObject.Definer, ro.defpos)
                + (DBObject._Framing, framing);
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db = cx.db + (ro, p) + (pr, p);
            cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(pr, p);
        }
    }
}
