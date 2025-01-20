using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Configuration;
using System.Xml;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

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
		public string nameAndArity => name+"$"+parameters.Length;
        public Ident? source;
        public bool mth = false;
        public Domain parameters = Domain.Null;
        public long proc = -1; // the procedure code is in Compiled.framing
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (defpos != ppos && !Committed(wr, defpos)) return defpos;
            return -1;
        }
        internal int arity;
        public PProcedure(string nm, Domain ar, Domain rt, Procedure? pr,Ident sce, long nst,
            long pp, Context cx) : this(Type.PProcedure,nm, ar, rt, pr, sce, nst, pp, cx)
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
        /// <param name="ps">The parameters</param>
        /// <param name="rt">The return type</param>
        /// <param name="pc">The procedure clause including parameters, or ""</param>
        /// <param name="nst">The first possible framing object</param>
        /// <param name="db">The database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected PProcedure(Type tp, string nm, Domain ps, Domain rt, Procedure? pr,
            Ident sce, long nst, long pp, Context cx) : base(tp,pp,cx,nm, pr?.defpos??-1L,rt, nst)
		{
            source = sce;
            parameters = ps;
            arity = parameters.Length;
            name = nm;
            dataType = rt;
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
            if (x.parameters is not null)
             parameters = (Domain)x.parameters.Fix(wr.cx);
            arity = x.arity;
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
            var s = source??throw new PEException("PE3000");
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
            var ss = rb.GetString().Split('$');
            name = ss[0];
			arity = rb.GetInt();
            if (type==Type.PProcedure2)
                rb.GetLong();
            source = new Ident(rb.GetString(), ppos + 1);
			base.Deserialise(rb);
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
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
            return GetType().Name + " " + name + (source?.ident??"");
		}
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
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
            if (source == null)
                return;
            var psr = new Parser(rdr.context, source);
            psr.cx.defs = BTree<long,Names>.Empty;
            psr.cx.obs = ObTree.Empty;
            psr.cx.depths = BTree<int, ObTree>.Empty;
            var n = new Ident(name, ppos);
            var (rt, dt) = psr.ParseProcedureHeading(n);
            psr.cx._Add(dt);
            parameters = rt;
            dataType = dt;
            framing = new Framing(psr.cx,nst); // heading only
            var pr = (Procedure?)Install(psr.cx);
            psr.cx.anames = Names.Empty;
            psr.cx.names = Names.Empty;
            if (pr is not null)
            { 
                psr.cx.AddParams(pr);
                psr.cx.anames = psr.cx.names+(n.ident, ppos);
                rdr.context.defs += (ppos, pr.names);
                rdr.context.Add(pr);
            }
            psr.LexDp(); //synchronise with CREATE
            var op = psr.cx.parse;
            psr.cx.parse = ExecuteStatus.Compile;
            if (psr.tok != Qlx.EOF 
                && psr.ParseStatementList((Procedure.ProcBody,true),(DBObject._Domain,dt),(NestedStatement.WfOK,true)) is Executable bd)
                proc = bd.defpos;
            psr.cx.parse = op;
            framing = new Framing(psr.cx,nst);
            rdr.context.db = psr.cx.db;
        }
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.role;
            if (source == null || parameters==null)
                throw new DBException("42108", name);
            var oi = new ObInfo(name,
                Grant.Privilege.Execute | Grant.Privilege.GrantExecute);
            var ns = Names.Empty;
            for (var b = dataType.rowType.First(); b != null; b = b.Next())
                if (b.value() is long q && framing.obs[q] is QlValue v &&  v.name is string n)
                    ns += (n, q);
            oi += (ObInfo._Names, ns);
            var pr = new Procedure(this, 
                BTree<long, object>.Empty + (DBObject.Definer, ro.defpos)
                + (DBObject._Framing, framing) + (Procedure.Body, proc)
                + (DBObject.Owner, cx.user??User.None) +(DBObject._Domain,dataType)
                + (DBObject.Infos,new BTree<long,ObInfo>(cx.role.defpos,oi)));
            var ps = ro.procedures??BTree<string,BTree<CList<Domain>,long?>>.Empty;
            var pn = (ps[name]??BTree<CList<Domain>,long?>.Empty) + (cx.db.Signature(pr),defpos);
            ro += (Role.Procedures, ps + (name, pn));
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db = cx.db + ro + pr + (Database.Procedures,cx.db.procedures+(defpos,name));
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(pr);
            if (framing.obs.Count==0)
                cx.AddParams(pr);
            cx.db += (pr.defpos, pr);
            return pr;
        }
    }
}
