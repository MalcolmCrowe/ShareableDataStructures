using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
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

namespace Pyrrho.Level3
{
	/// <summary>
	/// A level 3 Procedure/Function object.
    /// The domain for the Procedure/Function gives the return type.
    /// The order of columns in the return type (if any) is fixed
    /// and matches the uid ordering. Similarly for the parameters.
    /// Execution always uses the definer's (PProcedure) versions, 
    /// fetched from the schema role.
    /// Immutable
	/// </summary>
	internal class Procedure : DBObject
	{
        internal const long
            Body = -168, // long Executable
            Clause = -169,// string
            Inverse = -170, // long
            Monotonic = -171, // bool
            Params = -172; // RowType  FormalParameter
        /// <summary>
        /// The arity (number of parameters) of the procedure
        /// </summary>
		public int arity => ins.Length;
        public string name => (string)mem[Name];
        /// <summary>
        /// The body and ins stored in the database uses the definer's role. 
        /// These fields are filled in during Install.
        /// </summary>
        public long body => (long)(mem[Body]??-1L);
		public RowType ins => 
            (RowType)mem[Params]?? RowType.Empty;
        public string clause => (string)mem[Clause];
        public long inverse => (long)(mem[Inverse]??-1L);
        public bool monotonic => (bool)(mem[Monotonic] ?? false);
        internal override RowType rowType => (domain is Structure st)?st.rowType:RowType.Empty;
        /// <summary>
        /// Constructor: Build a level 3 procedure from a level 2 procedure
        /// </summary>
        /// <param name="p">The level 2 procedure</param>
		public Procedure(PProcedure p, Context cx, BTree<long, object> m= null)
            : base( p.ppos, p.defpos, cx.role.defpos, (m ?? BTree<long, object>.Empty)
                  + (Params, p.ins) +(_Domain,Domain._Structure(p.defpos,p.retType))
                  +(Framing,p.framing)+(Body,p.body)
                  + (Name,p.name) + (Clause, p.source.ident))
        { }
        protected Procedure(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        public static Procedure operator+(Procedure p,(long,object)v)
        {
            return (Procedure)p.New(p.mem + v);
        }
        internal override Sqlx kind => Sqlx.PROCEDURE;
        /// <summary>
        /// Execute a Procedure/function.
        /// </summary>
        /// <param name="actIns">The actual parameters</param>
        /// <returns>The possibily modified Transaction</returns>
        public Context Exec(Context cx, RowType actIns)
        {
            var oi = (ObInfo)cx.db.role.infos[defpos];
            if (!oi.priv.HasFlag(Grant.Privilege.Execute))
                throw new DBException("42105");
            var n = (int)ins.Count;
            var acts = new TypedValue[n];
            var i = 0;
            for (var b=actIns.First();b!=null;b=b.Next(), i++)
                acts[i] = cx.obs[b.value().Item1].Eval(cx);
            var act = new CalledActivation(cx, this,Domain.Null);
            act.obs += (framing,true);
            var bd = (Executable)act.obs[body];
            i = 0;
            for (var b=ins.First(); b!=null;b=b.Next(), i++)
                act.values += (b.value().Item1, acts[i]);
            cx = bd.Obey(act);
            var r = act.Ret();
            if (r is RowSet ts)
            {
                for (var b = act.values.First(); b != null; b = b.Next())
                    if (!cx.values.Contains(b.key()))
                        cx.values += (b.key(), b.value());
                cx.data += act.data; // wow
            }
            i = 0;
            for (var b = ins.First(); b != null; b = b.Next(), i++)
            {
                var p = (FormalParameter)act.obs[b.value().Item1];
                var m = p.paramMode;
                var v = act.values[p.defpos];
                if (m == Sqlx.INOUT || m == Sqlx.OUT)
                    acts[i] = v;
                if (m == Sqlx.RESULT)
                    r = v;
            }
            if (cx != null)
            {
                cx.val = r;
                i = 0;
                for (var b = ins.First(); b != null; b = b.Next(), i++)
                {
                    var p = (FormalParameter)act.obs[b.value().Item1];
                    var m = p.paramMode;
                    if (m == Sqlx.INOUT || m == Sqlx.OUT)
                        cx.AddValue(cx.obs[actIns[i].Item1], acts[i]);
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
        internal override Basis _Relocate(Writer wr)
        {
            var r = (Procedure)base._Relocate(wr);
            var ps = RowType.Empty;
            var ch = false;
            for (var b=ins.First();b!=null;b=b.Next())
            {
                var p = b.value();
                var np = wr.Fix(p);
                ps += np;
                if (np != p)
                    ch = true;
            }
            if (ch)
                r += (Params, ps);
            if (wr.Fixed(body) is Executable bd  && bd.defpos != body)
                r += (Body, bd.defpos);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (Procedure)base._Relocate(cx);
            var ps = RowType.Empty;
            var ch = false;
            for (var b = ins.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var np = cx.Unheap(p);
                ps += np;
                if (np != p)
                    ch = true;
            }
            if (ch)
                r += (Params, ps);
            if (cx.Fixed(body) is Executable bd && bd.defpos != body)
                r += (Body, bd.defpos);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = base._Replace(cx, so, sv);
            var ps = RowType.Empty;
            var ch = false;
            for (var b = ins.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var np = cx.Replace(p, so, sv);
                ps += np;
                if (np != p)
                    ch = true;
            }
            if (ch)
                r += (Params, ps);
            if (body != -1L)
            {
                var bd = cx.Replace(body, so, sv);
                if (bd != body)
                    r += (Body, bd);
            }
            return r;
        }
        internal override void Cascade(Context cx,
            Drop.DropAction a = 0, BTree<long, TypedValue> u = null)
        {
            base.Cascade(cx, a, u);
            for (var b = cx.role.dbobjects.First(); b != null; b = b.Next())
            {
                var ob = (DBObject)cx.db.objects[b.value()];
                if (ob.Calls(defpos, cx))
                    ob.Cascade(cx,a,u);
            }
        }
        internal override bool Calls(long defpos, Context cx)
        {
            return cx.obs[body].Calls(defpos, cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(ins);
            sb.Append(" Body:"); sb.Append(Uid(body));
            sb.Append(" Clause{"); sb.Append(clause); sb.Append('}');
            if (mem.Contains(Inverse)) { sb.Append(" Inverse="); sb.Append(inverse); }
            if (mem.Contains(Monotonic)) { sb.Append(" Monotonic"); }
            return sb.ToString();
        }
    }
}
