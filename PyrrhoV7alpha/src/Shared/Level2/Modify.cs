using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

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
	/// Modify is used for changes to procs, methods, functions, and views.
    /// Extend this if the syntax ever allows ALTER for triggers, views, checks, or indexes (!)
	/// </summary>
	internal class Modify : Compiled
	{
        /// <summary>
        /// The object being modified
        /// </summary>
		public long modifydefpos= -1L;
        /// <summary>
        /// The new parameters and body of the routine
        /// </summary>
		public Ident? source;
        public CList<long> parms = CList<long>.Empty;
        /// <summary>
        /// The Parsed version of the body for the definer's role
        /// </summary>
        public long proc = -1L;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,modifydefpos)) return modifydefpos;
            return -1;
        }
        /// <summary>
        /// Constructor: A Modify request from the parser
        /// </summary>
        /// <param name="nm">The (new) name of the routine</param>
        /// <param name="dp">The defining position of the routine</param>
        /// <param name="pc">The (new) parameters and body of the routine</param>
        /// <param name="pb">The local database</param>
        public Modify(long dp, Procedure me, Ident sce, long nst, long pp, Context cx)
            : base(Type.Modify, pp, _Pre(cx), me.NameFor(cx), ((Method)me).udType.defpos,
                  me.domain, nst)
		{
            modifydefpos = dp;
            source = sce;
            proc = me.body;
        }
        static Context _Pre(Context cx) // hack to keep our formalparameters in framing
        {
            cx.db += (Database.NextStmt, Transaction.Executables);
            return cx;
        }
        public Modify(string nm, long dp, QueryStatement rs, Ident sce, long pp, Context cx)
            : base(Type.Modify, pp, cx, nm, rs.domain, cx.db.nextStmt)
        {
            modifydefpos = dp;
            source = sce;
            proc = rs.defpos;
        }
        /// <summary>
        /// Constructor: A Modify request from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public Modify(Reader rdr) : base(Type.Modify,rdr) {}
        protected Modify(Modify x, Writer wr) : base(x, wr)
        {
            modifydefpos = wr.cx.Fix(x.modifydefpos);
            name = x.name;
            source = x.source;
            parms = wr.cx.FixLl(x.parms);
            proc = wr.cx.Fix(x.proc);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Modify(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhyBase
        /// </summary>
        /// <param name="r">Relocation information for the positions</param>
        public override void Serialise(Writer wr) 
		{
            if (source == null)
                throw new PEException("PE48174");
			modifydefpos = wr.cx.Fix(modifydefpos);
            wr.PutLong(modifydefpos);
            wr.PutString(name);
            wr.PutString(source.ident??"");
            proc = wr.cx.Fix(proc);
			base.Serialise(wr);
        }
        /// <summary>
        /// Desrialise this physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			modifydefpos = rdr.GetLong();
			name = rdr.GetString();
			source = new Ident(rdr.GetString(), ppos + 1);
			base.Deserialise(rdr);
		}
        internal override void OnLoad(Reader rdr)
        {
            if (rdr.context.db.objects[modifydefpos] is not Method pr || source==null)
                throw new DBException("3E006");
            var psr = new Parser(rdr.context, source);
            var cx = psr.cx;
            nst = cx.db.nextStmt;
            cx.obs = ObTree.Empty;
            cx.depths = BTree<int,ObTree>.Empty;
            // instantiate everything we may need
            var odt = pr.udType;
            cx.AddDefs(odt);
            cx.defs += (odt.defpos, odt.names);
            if (odt.infos[cx.role.defpos] is ObInfo ti)
                cx.Add(ti.names);
            pr.Instance(psr.LexDp(), psr.cx);
            odt.Instance(psr.LexDp(),psr.cx);
            for (var b = pr.ins.First(); b != null; b = b.Next())
                if (b.value() is long k)
                {
                    if (psr.cx.obs[k] is not FormalParameter p || p.name == null)
                        throw new DBException("3E006");
                    psr.cx.Add(p.name, p);
                }
            cx.Install(pr);
            // and parse the body
            if (psr.ParseStatement((DBObject._Domain,pr.domain),(NestedStatement.WfOK,true),(Procedure.ProcBody,true)) is not Executable bd)
                throw new PEException("PE1978");
            for (var b = cx.undefined.First(); b != null; b = b.Next())
                if (b.key() is long k && cx.obs[k] is SqlCall uo)
                    uo.Resolve(cx);
            if (cx.undefined.Count != 0L)
                throw new PEException("PE60901");
            proc = bd.defpos;
            framing = new Framing(cx,nst);
            framing += (Framing.Obs, pr.framing.obs + framing.obs);
            pr += (Procedure.Body, proc);
            pr += (DBObject._Framing,framing);
            rdr.context.Install(pr);
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Grant:
                    {
                        var g = (Grant)that;
                        if (modifydefpos == g.obj || modifydefpos == g.grantee)
                            return new DBException("40051", modifydefpos, that, ct);
                        break; 
                    }
                case Type.Drop:
                    if (modifydefpos == ((Drop)that).delpos)
                        return new DBException("40010", modifydefpos, that, ct);
                    break;
                case Type.Modify:
                    {
                        var m = (Modify)that;
                        if (name == m.name || modifydefpos == m.modifydefpos)
                            return new DBException("40052", modifydefpos, that, ct);
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
            return "Modify " + name + "["+ modifydefpos+"] to " + source?.ident;
		}
        internal override DBObject? Install(Context cx)
        {
            if (cx.db.role is not Role ro ||cx.db.objects[modifydefpos] is not Method pr)
                throw new PEException("PE48140");
            pr = pr + (DBObject.Definer, ro.defpos)
                + (DBObject._Framing, framing) + (Procedure.Body, proc);
            if (pr.methodType.HasFlag(PMethod.MethodType.Constructor))
                pr += (DBObject._Domain, pr.udType);
            pr += (DBObject.Infos, new BTree<long, ObInfo>(ro.defpos, new ObInfo(name,
                Grant.Privilege.Execute | Grant.Privilege.GrantExecute)));
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + modifydefpos, ppos));
            cx.db = cx.db + ro + pr;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(pr);
            return pr;
        }
    }
}
