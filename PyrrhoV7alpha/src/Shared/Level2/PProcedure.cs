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
        public RowType ins = RowType.Empty;
        public Domain retType = Domain.Null;
        public Ident source;
        public bool mth = false;
        public long body; // the procedure code is in Compiled.framing
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr, defpos)) return defpos;
            return -1;
        }
        internal int arity => ins.Length;
        public PProcedure(string nm, RowType ps, Domain rt, Ident sce, 
            long pp, Context cx) : this(Type.PProcedure2, nm, ps, rt, sce, pp, cx)
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
        protected PProcedure(Type tp, string nm, RowType ps, Domain rt, 
            Ident sce, long pp, Context cx) : base(tp,pp,cx,cx.obs)
		{
            source = sce;
            ins = ps;
            retType = rt;
            name = nm;
            nameAndArity = nm + "$" + arity;
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
            nameAndArity = x.nameAndArity;
            name = x.name;
            body = wr.Fix(x.body);
            var ps = RowType.Empty;
            for (var b = x.ins.First(); b != null; b = b.Next())
                ps += wr.Fix(b.value());
            ins = ps;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PProcedure(this, wr);
        }
        internal override void Relocate(Context cx)
        {
            base.Relocate(cx);
            cx.db += ((DBObject)cx.db.objects[ppos] 
                + (DBObject.Framing, framing) + (Procedure.Body, body), cx.db.loadpos);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr) 
		{
            wr.PutString(nameAndArity.ToString());
            wr.PutInt(arity);
            retType = (Domain)retType._Relocate(wr);
            if (type==Type.PMethod2 || type==Type.PProcedure2)
                wr.PutLong(retType.defpos);
            var s = source;
            if (wr.cx.db.format < 51)
                s = new Ident(DigestSql(wr,s.ident),s.iix,Sqlx.PROCEDURE);
            wr.PutString(s.ident);
            body = wr.Fix(body);
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
			rdr.GetInt();
            if (type == Type.PMethod2 || type == Type.PProcedure2)
                retType = (Domain)rdr.context.db.objects[rdr.GetLong()]??Domain.Null;
            if (this is PMethod mt && mt.methodType == PMethod.MethodType.Constructor)
                retType = (Domain)rdr.context.db.objects[mt.typedefpos];
            source = new Ident(rdr.GetString(), ppos, Sqlx.PROCEDURE);
			base.Deserialise(rdr);
        }
        internal override void OnLoad(Reader rdr)
        {
            var psr = new Parser(rdr.context, new Ident(source.ident, ppos + 1,Sqlx.PROCEDURE));
            psr.cx.srcFix = ppos + 1;
            psr.ParseProcedureHeading(this);
            psr.ParseProcedure(this);
            psr.cx.obs+=(ppos,new Procedure(this,psr.cx));
            Frame(psr.cx);
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
        public override (Transaction, Physical) Commit(Writer wr, Transaction tr)
        {
            Physical ph;
            (tr,ph) = base.Commit(wr, tr);
            var r = (PProcedure)ph;
            var ro = tr.role;
            return (tr, r);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var pr = new Procedure(this, cx);
            ro = ro + pr;
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db += (ro, p);
            cx.Install(pr, p);
        }
    }
}
