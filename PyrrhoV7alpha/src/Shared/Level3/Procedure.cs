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
    /// The ObInfo is role-dependent and so is computed for the SqlCall.
    /// Similarly for the parameters.
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
            Params = -172; // BList<long>  FormalParameter
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
		public BList<long> ins => 
            (BList<long>)mem[Params]?? BList<long>.Empty;
        public string clause => (string)mem[Clause];
        public long inverse => (long)(mem[Inverse]??-1L);
        public bool monotonic => (bool)(mem[Monotonic] ?? false);
        /// <summary>
        /// Constructor: Build a level 3 procedure from a level 2 procedure
        /// </summary>
        /// <param name="p">The level 2 procedure</param>
		public Procedure(PProcedure p, Context cx,BTree<long,object> m=null)
            : base( p.ppos, p.defpos, cx.role.defpos, (m??BTree<long,object>.Empty)
                  + (Params, p.parameters) +(_Domain,p.retType) + (Body, p.proc)
                  + (Name,p.name) + (Clause, p.source.ident) + (LastChange, p.ppos))
        { }
        /// <summary>
        /// Constructor: a new Procedure/Function from the parser
        /// </summary>
        /// <param name="defpos"></param>
        /// <param name="ps"></param>
        /// <param name="rt"></param>
        /// <param name="m"></param>
        public Procedure(long defpos,BList<long> ps, Domain dt, 
            BTree<long, object> m=null) : base(defpos, (m??BTree<long,object>.Empty)
                +(Params,ps)+(_Domain,dt)) { }
        protected Procedure(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Procedure operator+(Procedure p,(long,object)v)
        {
            return (Procedure)p.New(p.mem + v);
        }
        /// <summary>
        /// Execute a Procedure/function.
        /// </summary>
        /// <param name="actIns">The actual parameters</param>
        /// <returns>The possibily modified Transaction</returns>
        public Context Exec(Context cx, BList<long> actIns)
        {
            var oi = (ObInfo)cx.db.role.infos[defpos];
            if (!oi.priv.HasFlag(Grant.Privilege.Execute))
                throw new DBException("42105");
            var n = (int)ins.Count;
            var acts = new TypedValue[n];
            var i = 0;
            for (var b=actIns.First();b!=null;b=b.Next(), i++)
                acts[i] = cx.obs[b.value()].Eval(cx);
            var act = new CalledActivation(cx, this,Domain.Null);
            act.Install1(framing);
            act.Install2(framing);
            var bd = (Executable)act.obs[body];
            i = 0;
            for (var b=ins.First(); b!=null;b=b.Next(), i++)
                act.values += (((FormalParameter)act.obs[b.value()]).val, acts[i]);
            cx = bd.Obey(act);
            var r = act.Ret();
            if (r is TArray ts)
            {
                for (var b = act.values.First(); b != null; b = b.Next())
                    if (!cx.values.Contains(b.key()))
                        cx.values += (b.key(), b.value());
            }
            i = 0;
            for (var b = ins.First(); b != null; b = b.Next(), i++)
            {
                var p = (FormalParameter)act.obs[b.value()];
                var m = p.paramMode;
                var v = act.values[p.val];
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
                    var p = (FormalParameter)act.obs[b.value()];
                    var m = p.paramMode;
                    if (m == Sqlx.INOUT || m == Sqlx.OUT)
                        cx.AddValue(cx.obs[actIns[i]], acts[i]);
                }
            }
            return cx;
        }
        internal override void Modify(Context cx, Modify m, long p)
        {
            cx.db = cx.db + (this+(Body,m.now.defpos),p) + (Database.SchemaKey,p);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Procedure(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Procedure(dp, mem);
        }
        internal override void Scan(Context cx)
        {
            cx.ObUnheap(defpos);
            cx.Scan(ins);
            cx.ObScanned(body);
        }
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = (Procedure)base._Relocate(wr);
            r += (Params, wr.Fix(ins));
            r += (Body, wr.Fixed(body)?.defpos??-1L);
            return r;
        }
        internal override Basis _Relocate(Context cx, Context nc)
        {
            var r = (Procedure)base._Relocate(cx,nc);
            r += (Params, cx.Fix(ins));
            r += (Body, cx.obuids[body]);
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = (Procedure)base.Fix(cx);
            r += (Params, cx.Fix(ins));
            if (body>=0)
                r += (Body, cx.obuids[body]);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (Procedure)base._Replace(cx, so, sv);
            var bd = cx.obs[body]._Replace(cx, so, sv);
            var ch = (bd?.defpos ?? -1L) != body;
            if (ch)
                r += (Body, bd.defpos);
            var fs = BList<long>.Empty;
            ch = false;
            for (var b=ins.First();b!=null;b=b.Next())
            {
                var fp = cx._Replace(b.value(), so, sv);
                ch = ch || (fp.defpos != b.value());
                fs += fp.defpos;
            }
            if (ch)
                r += (Params, fs);
            r = (Procedure)New(cx, r.mem);
            cx.done += (defpos, r);
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
            sb.Append(" "); sb.Append(name);
            sb.Append(" Arity="); sb.Append(arity);
            if (domain!=Domain.Null) sb.Append(domain);
            sb.Append(" Params");
            var cm = '(';
            for (var i = 0; i < (int)ins.Count; i++)
            {
                sb.Append(cm); cm = ','; sb.Append(Uid(ins[i]));
            }
            sb.Append(") Body:"); sb.Append(Uid(body));
            sb.Append(" Clause{"); sb.Append(clause); sb.Append('}');
            if (mem.Contains(Inverse)) { sb.Append(" Inverse="); sb.Append(Uid(inverse)); }
            if (mem.Contains(Monotonic)) { sb.Append(" Monotonic"); }
            return sb.ToString();
        }
    }
}
