using System;
using Pyrrho.Common;
using Pyrrho.Level1;
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
	/// Modify is used for changes to procs, methods, functions, and views.
    /// Extend this if the syntax ever allows ALTER for triggers, views, checks, or indexes (!)
	/// </summary>
	internal class Modify : Physical
	{
        /// <summary>
        /// The object being modified
        /// </summary>
		public long modifydefpos;
        /// <summary>
        /// The new name of the routine
        /// </summary>
		public string name;
        /// <summary>
        /// The new parameters and body of the routine
        /// </summary>
		public string body;
        /// <summary>
        /// The Parsed version of the body for the definer's role
        /// </summary>
        public DBObject now;
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
        public Modify(string nm, long dp, string pc, DBObject nw, long pp, Context cx)
            : base(Type.Modify,pp,cx)
		{
            modifydefpos = dp;
            name = nm;
            body = pc;
            now = nw;
        }
        /// <summary>
        /// Constructor: A Modify request from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public Modify(Reader rdr) : base(Type.Modify,rdr) {}
        protected Modify(Modify x, Writer wr) : base(x, wr)
        {
            modifydefpos = wr.Fix(x.modifydefpos);
            name = x.name;
            body = x.body;
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
			modifydefpos = wr.Fix(modifydefpos);
            wr.PutLong(modifydefpos);
            wr.PutString(name);
            wr.PutString(body);
			base.Serialise(wr);
            var pp = wr.cx.db.objects[modifydefpos] as Procedure;
            pp += (Procedure.Clause, body);
            wr.cx.Install(pp,wr.cx.db.loadpos);
        }
        /// <summary>
        /// Desrialise this p[hysical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			modifydefpos = rdr.GetLong();
			name = rdr.GetString();
			body = rdr.GetString();
			base.Deserialise(rdr);
            switch (name)
            {
                default:
                    var pp = rdr.context.db.objects[modifydefpos] as Procedure;
                    pp += (Procedure.Clause, body);
                    pp = new Parser(rdr.context).ParseProcedureBody(pp.name, pp, new Ident(pp.clause,modifydefpos));
                    now = pp;
                    break;
                case "Source":
                    now = new Parser(rdr.context).ParseQueryExpression(-1,body,Domain.TableType.defpos);
                    break;
                case "Insert": // we ignore all of these (PView1)
                case "Update":
                case "Delete":
                    now = null;
                    break;
            }
		}
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.Grant:
                    {
                        var g = (Grant)that;
                        return (modifydefpos == g.obj || modifydefpos == g.grantee) ? ppos : -1;
                    }
                case Type.Drop:
                    return (modifydefpos == ((Drop)that).delpos) ? ppos : -1;
                case Type.Modify:
                    {
                        var m = (Modify)that;
                        return (name == m.name || modifydefpos == m.modifydefpos) ? ppos : -1;
                    }
            }
            return base.Conflicts(db, tr, that);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Modify "+Pos(modifydefpos)+": "+name+" to "+body;
		}

        internal override void Install(Context cx, long p)
        {
            ((DBObject)cx.db.objects[modifydefpos]).Modify(cx,now,p);
            cx.obs += (modifydefpos, (DBObject)cx.db.objects[modifydefpos]);
        }
    }
}
