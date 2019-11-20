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
            Arity = -167, // int
            Body = -168, // Executable
            Clause = -169,// string
            Inverse = -170, // long
            Monotonic = -171, // bool
            Params = -172, // BList<ProcParameter>
            RetType = -173; // Domain
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
        public long inverse => (long)(mem[Inverse]??-1L);
        public bool monotonic => (bool)(mem[Monotonic] ?? false);
        /// <summary>
        /// Constructor: Build a level 3 procedure from a level 2 procedure
        /// </summary>
        /// <param name="p">The level 2 procedure</param>
		public Procedure(PProcedure p, Database db,bool mth,Sqlx create,BTree<long,object> m)
            : base( p.ppos, p.defpos, db.role.defpos, m
                  + (Arity, p.arity) + (RetType, db.role.obinfos[p.retdefpos])
                  + (Body,new Parser(db,new Context(db)).ParseProcedureClause(mth,create))
                  + (Clause, p.proc_clause))
        { }
        public Procedure(long defpos,BTree<long, object> m) : base(defpos, m) { }
        public static Procedure operator+(Procedure p,(long,object)v)
        {
            return new Procedure(p.defpos, p.mem + v);
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
                var v = act.row.values[p.defpos];
                if (p.paramMode == Sqlx.INOUT || p.paramMode == Sqlx.OUT)
                    acts[i] = v;
                if (p.paramMode == Sqlx.RESULT)
                    r = v;
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
        internal virtual bool Uses(long t)
        {
            return false;
        }
        internal override Database Modify(Database db, DBObject now, long p)
        {
            return db + (this+(Body,now),p);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Procedure(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Procedure(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = this;
            var d = wr.Fix(defpos);
            if (d != defpos)
                r = (Procedure)Relocate(d);
            var rt = (Domain)retType?.Relocate(wr);
            if (rt != retType)
                r += (RetType, rt);
            return r;
        }
        internal override (Database,Role) Cascade(Database d, Database nd,Role ro, 
            Drop.DropAction a = 0, BTree<long, TypedValue> u = null)
        {
            if (a != 0)
                nd += (Database.Cascade, true);
            for (var b = d.role.dbobjects.First(); b != null; b = b.Next())
            {
                var ob = (DBObject)d.objects[b.value()];
                if (ob.Calls(defpos, d))
                    (nd,ro) = ob.Cascade(d,nd,ro,a,u);
            }
            return base.Cascade(d, nd,ro,a,u);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return body.Calls(defpos, db);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Arity="); sb.Append(arity);
            sb.Append(" RetType:"); sb.Append(retType);
            sb.Append(" Params");
            var cm = '(';
            for (var i = 0; i < (int)ins.Count; i++)
            {
                sb.Append(cm); cm = ','; sb.Append(ins[i]);
            }
            sb.Append(") Body:"); sb.Append(body);
            sb.Append(" Clause{"); sb.Append(clause); sb.Append('}');
            if (mem.Contains(Inverse)) { sb.Append(" Inverse="); sb.Append(inverse); }
            if (mem.Contains(Monotonic)) { sb.Append(" Monotonic"); }
            return sb.ToString();
        }
    }
}
