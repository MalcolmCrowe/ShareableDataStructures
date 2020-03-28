using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
            RetType = -173; // ObInfo
        /// <summary>
        /// The arity (number of parameters) of the procedure
        /// </summary>
		public int arity => (int)mem[Arity];
        public string name => (string)mem[Name];
        /// <summary>
        /// The body and ins stored in the database uses the definer's role. 
        /// These fields are filled in during Install.
        /// </summary>
        public Executable body => (Executable)mem[Body];
		public BList<ProcParameter> ins => 
            (BList<ProcParameter>)mem[Params]?? BList<ProcParameter>.Empty;
        public ObInfo retType => (ObInfo)mem[RetType]?? ObInfo.Any;
        public string clause => (string)mem[Clause];
        public long inverse => (long)(mem[Inverse]??-1L);
        public bool monotonic => (bool)(mem[Monotonic] ?? false);
        /// <summary>
        /// Constructor: Build a level 3 procedure from a level 2 procedure
        /// </summary>
        /// <param name="p">The level 2 procedure</param>
		public Procedure(PProcedure p, Database db,BTree<long,object> m)
            : base( p.ppos, p.defpos, db.role.defpos, m
                  + (Arity, p.arity) + (RetType, db.objects[p.retdefpos])
                  + (Name,p.name) + (Clause, p.proc_clause))
        { }
        public Procedure(long defpos,BTree<long, object> m) : base(defpos, m) { }
        public static Procedure operator+(Procedure p,(long,object)v)
        {
            return (Procedure)p.New(p.mem + v);
        }
        /// <summary>
        /// Execute a Procedure/function.
        /// </summary>
        /// <param name="actIns">The actual parameters</param>
        /// <returns>The possibily modified Transaction</returns>
        public Context Exec(Context cx, BList<SqlValue> actIns)
        {
            var oi = (ObInfo)cx.db.role.obinfos[defpos];
            if (!oi.priv.HasFlag(Grant.Privilege.Execute))
                throw new DBException("42105");
            var n = (int)ins.Count;
            var acts = new TypedValue[n];
            for (int i = 0; i < n; i++)
                acts[i] = actIns[i].Eval(cx);
            var act = new CalledActivation(cx, this,Domain.Null);
            var bd = (Executable)body.Frame(act);
            for (int i = 0; i < acts.Length; i++)
                act.values +=(ins[i].defpos, acts[i]);
            cx = bd.Obey(act);
            var r = act.Ret();
            if (r is RowSet ts)
            {
/*                // organise the import map
                var rt = ts.rowSet.rowType;
                var ri = (ObInfo)tr.role.obinfos[retType.structure];
                for (var b = ri.columns.First(); b != null; b = b.Next())
                {
                    var rc = b.value();
                    ts.rowSet.imp += (rc.defpos,rt.columns[rt.map[rc.name].Value].defpos);
                } */
                for (var b = act.values.First(); b != null; b = b.Next())
                    if (!cx.values.Contains(b.key()))
                        cx.values += (b.key(), b.value());
            }
            for (int i = 0; i < n; i++)
            {
                var p = ins[i];
                var v = act.values[p.defpos];
                if (p.paramMode == Sqlx.INOUT || p.paramMode == Sqlx.OUT)
                    acts[i] = v;
                if (p.paramMode == Sqlx.RESULT)
                    r = v;
            }
            if (cx != null)
            {
                cx.val = r;
                for (int i = 0; i < n; i++)
                {
                    var p = ins[i];
                    if (p.paramMode == Sqlx.INOUT || p.paramMode == Sqlx.OUT)
                        cx.AddValue(actIns[i], acts[i]);
                }
            }
            return cx;
        }
        internal virtual bool Uses(long t)
        {
            return false;
        }
        internal override void Modify(Context cx, DBObject now, long p)
        {
            cx.db = cx.db + (this+(Body,now),p) + (Database.SchemaKey,p);
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
            var r = (Procedure)base.Relocate(wr);
            var rt = retType?.Relocate(wr);
            if (rt != retType)
                r += (RetType, rt);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            body.Frame(cx);
            return base.Frame(cx);
        }
        internal override void Cascade(Context cx,
            Drop.DropAction a = 0, BTree<long, TypedValue> u = null)
        {
            base.Cascade(cx, a, u);
            for (var b = cx.role.dbobjects.First(); b != null; b = b.Next())
            {
                var ob = (DBObject)cx.db.objects[b.value()];
                if (ob.Calls(defpos, cx.db))
                    ob.Cascade(cx,a,u);
            }
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
