using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level3
{
	/// <summary>
	/// A level 3 Procedure/Function object.
    /// Immutable
	/// </summary>
	internal class Procedure : DBObject
	{
        internal const long
            Arity = -176, // int
            Body = -177, // Executable
            Clause = -178,// string
            Inverse = -179, // long
            Monotonic = -180, // bool
            Params = -181, // BList<ProcParameter>
            RetType = -182; // Domain
        /// <summary>
        /// The arity (number of parameters) of the procedure
        /// </summary>
		public int arity => (int)mem[Arity];
        /// <summary>
        /// The body and ins stored in the database uses the definer's role. 
        /// These fields are filled in during Install.
        /// </summary>
        public Executable body => (Executable)mem[Body];
		public BList<ProcParameter> ins => 
            (BList<ProcParameter>)mem[Params]?? BList<ProcParameter>.Empty;
        public Domain retType => (Domain)mem[RetType];
        public string clause => (string)mem[Clause];
        public long inverse => (long)(mem[Inverse]??-1);
        public bool monotonic => (bool)(mem[Monotonic] ?? false);
        /// <summary>
        /// Constructor: Build a level 3 procedure from a level 2 procedure
        /// </summary>
        /// <param name="p">The level 2 procedure</param>
		public Procedure(PProcedure p, Database db,bool mth,Sqlx create,BTree<long,object> m)
            : base( p.ppos, p.defpos, db.role.defpos, m
                  + (Arity, p.arity) + (RetType, db.GetDomain(p.retdefpos))
                  + (Body,new Parser(db,new Context(db)).ParseProcedureClause(mth,create))
                  + (Clause, p.proc_clause))
        { }
        public Procedure(long defpos,BTree<long, object> m) : base(defpos, m) { }
        public static Procedure operator+(Procedure p,(long,object)v)
        {
            return new Procedure(p.defpos, p.mem + v);
        }
        public override string ToString()
		{
			var sb = new StringBuilder(base.ToString());
            sb.Append(" Arity=");sb.Append(arity);
            sb.Append(" RetType:"); sb.Append(retType);
            sb.Append(" Params");
            var cm = '(';
            for (var i=0;i<(int)ins.Count;i++)
            {
                sb.Append(cm); cm = ','; sb.Append(ins[i]);
            }
            sb.Append(") Body:");sb.Append(body);
            sb.Append(" Clause{");sb.Append(clause);sb.Append('}');
            if (mem.Contains(Inverse)) { sb.Append(" Inverse="); sb.Append(inverse); }
            if (mem.Contains(Monotonic)) { sb.Append(" Monotonic"); }
            return sb.ToString();
		}
        /// <summary>
        /// Execute a Procedure/function
        /// </summary>
        /// <param name="dbx">The participant dbix</param>
        /// <param name="n">The procedure name</param>
        /// <param name="actIns">The actual parameters</param>
        /// <returns>The return value</returns>
        public Transaction Exec(Transaction tr,Context cx, BList<SqlValue> actIns)
        {
            //         Permission(tr.user, tr.role, Grant.Privilege.Execute);
            var n = (int)ins.Count;
            var acts = new TypedValue[n];
            for (int i = 0; i < n; i++)
                acts[i] = actIns[i].Eval(tr,cx);
            var a = cx.GetActivation();
            var bd = body;
            var act = new CalledActivation(tr,cx, this,Domain.Null);
            for (int i = 0; i < acts.Length; i++)
                act.values +=(ins[i].defpos, acts[i]);
            tr = bd.Obey(tr, act);
            var r = act.ret;
            for (int i = 0; i < n; i++)
            {
                var p = ins[i];
                if (p.paramMode == Sqlx.INOUT || p.paramMode == Sqlx.OUT)
                    acts[i] = act.row.values[p.defpos];
                if (p.paramMode == Sqlx.RESULT)
                    r = act.row.values[p.defpos];
            }
            if (cx != null)
            {
                cx.ret = r;
                for (int i = 0; i < n; i++)
                {
                    var p = ins[i];
                    if (p.paramMode == Sqlx.INOUT || p.paramMode == Sqlx.OUT)
                        cx.values += (actIns[i].defpos, acts[i]);
                }
            }
            return tr;
        }
        /// <summary>
        /// Check dependents for a rename/drop transaction
        /// </summary>
        /// <param name="t">The rename/drop transaction</param>
        /// <returns>the sort of dependency concerned</returns>
        public override Sqlx Dependent(Transaction t,Context cx)
        {
            return body.Dependent(t,cx);
        }
        internal override BTree<long,DBObject> Add(PMetadata p,Database db)
        {
            var r = BTree<long, DBObject>.Empty;
            var idp = inverse;
            var me = this;
            if (p.refpos > 0 && p.Has(Sqlx.INVERTS))
            {
                // establish a pair of mutually inverse functions
                idp = p.refpos;
                if (db.role.objects[idp] is Procedure pi)
                {
                    pi += (Inverse, defpos);
                    me += (Inverse, pi.defpos);
                    r += (pi.defpos, pi);
                }
            }
            var mon = p.Has(Sqlx.MONOTONIC);
            me += (Monotonic, mon);
            if (p.seq >= 0)
                me += (Selector.Seq, p.seq);
            r += (me.defpos,me);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Procedure(defpos,m);
        }
    }
}
