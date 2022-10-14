using System;
using System.Configuration;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using Pyrrho.Common;
using Pyrrho.Level1;
using Pyrrho.Level3;
using Pyrrho.Level4;

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
	/// Modify is used for changes to procs, methods, functions, and views.
    /// Extend this if the syntax ever allows ALTER for triggers, views, checks, or indexes (!)
	/// </summary>
	internal class Modify : Compiled
	{
        /// <summary>
        /// The object being modified
        /// </summary>
		public long modifydefpos;
        /// <summary>
        /// The new name of the routine
        /// </summary>
		public string nameAndArity;
        /// <summary>
        /// The new parameters and body of the routine
        /// </summary>
		public Ident source;
        public CList<long> parms;
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
        public Modify(string nm, long dp, Procedure me, Ident sce, long pp, Context cx)
            : base(Type.Modify,pp,_Pre(cx),me.body,cx._Dom(me))
		{
            modifydefpos = dp;
            nameAndArity = me.infos[cx.role.defpos].name;
            source = sce;
            proc = me.body;
            nst = me.framing.obs.First()?.key() ?? nst;
        }
        static Context _Pre(Context cx) // hack to keep our formalparameters in framing
        {
            cx.db += (Database.NextStmt, Transaction.Executables);
            return cx;
        }
        public Modify(string nm, long dp, RowSet rs, Ident sce, long pp, Context cx)
    : base(Type.Modify, pp, cx, rs.defpos, cx._Dom(rs))
        {
            modifydefpos = dp;
            nameAndArity = nm;
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
            nameAndArity = x.nameAndArity;
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
			modifydefpos = wr.cx.Fix(modifydefpos);
            wr.PutLong(modifydefpos);
            wr.PutString(nameAndArity);
            if (wr.cx.db.format < 51)
                source = new Ident(DigestSql(wr, source.ident), source.iix);
            wr.PutString(source.ident);
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
			nameAndArity = rdr.GetString();
			source = new Ident(rdr.GetString(), rdr.context.Ix(ppos + 1));
			base.Deserialise(rdr);
		}
        internal override void OnLoad(Reader rdr)
        {
            var pr = (Method)rdr.context.db.objects[modifydefpos];
            var psr = new Parser(rdr.context, source);
            nst = psr.cx.db.nextStmt;
            psr.cx.obs = ObTree.Empty;
            // instantiate everything we may need
            var odt = pr.udType;
            pr.Instance(psr.LexPos().dp, psr.cx);
            odt.Instance(psr.LexPos().dp,psr.cx);
            for (var b = pr.ins.First(); b != null; b = b.Next())
            {
                var p = (FormalParameter)psr.cx.obs[b.value()];
                var ip = rdr.context.Ix(p.defpos);
                psr.cx.defs += (new Ident(p.name, ip), ip);
            }
            psr.cx.Install(pr, 0);
            // and parse the body
            var bd = psr.ParseProcedureStatement(rdr.context._Dom(pr));
            proc = bd.defpos;
            framing = new Framing(psr.cx,nst);
            framing += (Framing.Obs, pr.framing.obs + framing.obs);
            pr += (Procedure.Body, proc);
            pr += (DBObject._Framing,framing);
            rdr.context.Install(pr, rdr.Position);
        }
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
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
                        if (nameAndArity == m.nameAndArity || modifydefpos == m.modifydefpos)
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
            return "Modify " + nameAndArity + "["+ modifydefpos+"] to " + source.ident;
		}
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var pr = (Method)cx.db.objects[modifydefpos];
            pr = pr + (DBObject.Definer, ro.defpos)
                + (DBObject._Framing, framing) + (Procedure.Body, proc);
            pr += (DBObject.Infos, new BTree<long, ObInfo>(ro.defpos, new ObInfo(nameAndArity,
                Grant.Privilege.Execute | Grant.Privilege.GrantExecute)));
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + modifydefpos, ppos));
            cx.db = cx.db + (ro, p) + (pr, p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Install(pr, p);
            base.Install(cx, p);
        }
    }
}
