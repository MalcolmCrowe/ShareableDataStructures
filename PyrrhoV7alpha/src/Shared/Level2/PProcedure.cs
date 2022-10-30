using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Configuration;

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
		public string name="",nameAndArity="";
        public Ident source;
        public bool mth = false;
        public CList<long> parameters;
        public long proc = -1; // the procedure code is in Compiled.framing
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr, defpos)) return defpos;
            return -1;
        }
        internal int arity;
        public PProcedure(string nm, CList<long> ar, Domain rt, Procedure pr,Ident sce, 
            long pp, Context cx) : this(Type.PProcedure,nm, ar, rt, pr, sce, pp, cx)
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
            Ident sce, long pp, Context cx) : base(tp,pp,cx,pr?.defpos??-1L,rt)
		{
            source = sce;
            parameters = ps;
            arity = parameters.Length;
            name = nm;
            dataType = rt;
            nameAndArity = nm + "$" + arity;
            proc = pr?.body??-1L;
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
            parameters = wr.cx.FixLl(x.parameters);
            nameAndArity = x.nameAndArity;
            arity = x.arity;
      //      returns = x.returns;
            name = x.name;
            proc = wr.cx.Fix(x.proc);
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
            var s = source;
            if (wr.cx.db.format < 51)
                s = new Ident(DigestSql(wr,s.ident),s.iix);
            wr.PutString(s.ident);
            proc = wr.cx.Fix(proc);
			base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rb)
		{
            nst = rb.context.db.nextStmt;
            nameAndArity = rb.GetString();
            var ss = nameAndArity.Split('$');
            name = ss[0];
			arity = rb.GetInt();
            if (type==Type.PProcedure2)
                rb.GetLong();
            source = new Ident(rb.GetString(), rb.context.Ix(ppos + 1));
			base.Deserialise(rb);
        }
        public override (Transaction, Physical) Commit(Writer wr, Transaction tr)
        {
            var r = base.Commit(wr, tr);
            wr.cx.instDFirst = -1L;
            return r;
        }
        /// <summary>
        /// A readble version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
            return GetType().Name + " "+name  + source.ident;
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
            var psr = new Parser(rdr.context,source);
            psr.cx.defs = Ident.Idents.Empty;
            var n = new Ident(name,new Iix(ppos));
            var (rt,dt) = psr.ParseProcedureHeading(n);
            psr.cx._Add(dt);
            parameters = rt;
            Install(psr.cx, rdr.Position);
            psr.LexPos(); //synchronise with CREATE
            var op = psr.cx.parse;
            psr.cx.parse = ExecuteStatus.Compile;
            if (psr.tok!=Sqlx.EOF)
                proc = psr.ParseProcedureStatement(dt).defpos;
            psr.cx.parse = op;
            framing = new Framing(psr.cx,nst);
            dataType = dt;
            rdr.context.db = psr.cx.db;
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var oi = new ObInfo(name,
                Grant.Privilege.Execute | Grant.Privilege.GrantExecute);
            var pr = new Procedure(this, cx,
                BTree<long,object>.Empty + (DBObject.Definer, ro.defpos)
                + (DBObject._Framing, framing) + (Procedure.Body, proc)
                + (DBObject.Infos,new BTree<long,ObInfo>(cx.role.defpos,oi)));
            var ps = ro.procedures??CTree<string,CTree<int,long>>.Empty;
            var pn = (ps[name]??CTree<int,long>.Empty) + (arity,defpos);
            ro += (Role.Procedures, ps + (name, pn));
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db = cx.db + (ro, p) + (pr, p) 
                + (Database.Procedures,cx.db.procedures+(defpos,nameAndArity));
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(pr, p);
            if (framing.obs.Count==0)
                cx.AddParams(pr);
            base.Install(cx, p);
        }
    }
}
