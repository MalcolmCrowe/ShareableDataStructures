using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
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
    /// The Procedure/Function properties contains the properties of the return type.
    /// This makes for complications if the return type is a UDType or a Table, and
    /// in that case we need to use _Dm(cx) to reconstruct the target type.
    /// The ObInfo is role-dependent and so is computed for the SqlCall.
    /// Similarly for the parameters.
    /// Execution always uses the definer's (PProcedure) versions, 
    /// fetched from the schema role.
    /// Immutable
    /// 
    /// </summary>
    internal class Procedure : DBObject
	{
        internal const long
            Body = -168, // long Executable
            Clause = -169,// string
            Inverse = -170, // long
            Monotonic = -171, // bool
            Params = -172; // Domain  
        /// <summary>
        /// The arity (number of parameters) of the procedure
        /// </summary>
		public int arity => ins.Length;
        /// <summary>
        /// The body and ins stored in the database uses the definer's role. 
        /// These fields are filled in during Install.
        /// </summary>
        public long body => (long)(mem[Body]??-1L);
		public Domain ins => 
            (Domain)(mem[Params]?? Domain.Null);
        public string clause => (string?)mem[Clause]??"";
        public long inverse => (long)(mem[Inverse]??-1L);
        public bool monotonic => (bool)(mem[Monotonic] ?? false);
        /// <summary>
        /// Constructor: Build a level 3 procedure from a level 2 procedure
        /// </summary>
        /// <param name="p">The level 2 procedure</param>
		public Procedure(PProcedure p, BTree<long,object> m)
            : base( p.ppos, m + (ObInfo.Name,p.name)+(Definer,p.definer)+(Owner,p.owner)
                  + (Infos,p.infos)
                  + (Params, p.parameters) + (_Domain, p.dataType)
                  + (Body, p.proc) + (Clause, p.source?.ident??"") + (LastChange, p.ppos))
        { }
        /// <summary>
        /// Constructor: a new Procedure/Function from the parser
        /// </summary>
        /// <param name="defpos"></param>
        /// <param name="ps"></param>
        /// <param name="rt"></param>
        /// <param name="m"></param>
        public Procedure(long defpos,Context cx,Domain ps, Domain dt, 
            BTree<long, object> m) : base(defpos, m +(Params,ps) + (_Domain, dt)
                + (Definer, cx.role.defpos) + (Owner, cx.user?.defpos ?? -501L))
        { }
        protected Procedure(long dp, BTree<long, object> m) : base(dp, m) { }
        public static Procedure operator+(Procedure p,(long,object)v)
        {
            return (Procedure)p.New(p.mem + v);
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            nd += (Database.Procedures, d.procedures - defpos);
            return base.Drop(d, nd, p);
        }
        /// <summary>
        /// Execute a Procedure/function.
        /// </summary>
        /// <param name="actIns">The actual parameters</param>
        /// <returns>The possibily modified Transaction</returns>
        public Context Exec(Context cx, BList<long?> actIns)
        {
            if (infos[cx.role.defpos] is not ObInfo oi
                || !oi.priv.HasFlag(Grant.Privilege.Execute))
                throw new DBException("42105");
            cx.Add(framing);
            var n = ins.Length;
            var acts = new TypedValue[n];
            var i = 0;
            for (var b = actIns.First(); b != null; b = b.Next(), i++)
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                    acts[i] = v.Eval(cx);
            var act = new CalledActivation(cx, this);
            var bd = (Executable?)cx.obs[body] ?? throw new DBException("42108", oi.name ?? "??");
            for (var b = ins.First(); b != null; b = b.Next(), i++)
                if (b.value() is long p)
                    act.values += (p, acts[b.key()]);
            cx = bd.Obey(act);
            var r = act.Ret();
            if (r is TList ts)
                for (var b = act.values.First(); b != null; b = b.Next())
                    if (!cx.values.Contains(b.key()))
                        cx.values += (b.key(), b.value());
            i = 0;
            for (var b = ins.First(); b != null; b = b.Next(), i++)
                if (b.value() is long bp && act.obs[bp] is FormalParameter p)
                {
                    var m = p.paramMode;
                    var v = act.values[p.val] ?? TNull.Value;
                    if (m == Sqlx.INOUT || m == Sqlx.OUT)
                        acts[i] = v;
                    if (m == Sqlx.RESULT)
                        r = v;
                }
            if (this is Method mt && mt.methodType == PMethod.MethodType.Constructor)
            {
                if (mt.udType.superShape)
                    r = cx.val;
                else
                    r = new TRow(mt.udType, act.values);
            }
            cx.val = r ?? TNull.Value;
            i = 0;
            for (var b = ins.First(); b != null; b = b.Next(), i++)
                if (b.value() is long bp && act.obs[bp] is FormalParameter p &&
                    cx.obs[actIns[i] ?? -1L] is DBObject x)
                {
                    var m = p.paramMode;
                    if (m == Sqlx.INOUT || m == Sqlx.OUT)
                        cx.AddValue(x, acts[i]);
                }
            return cx;
        }
        internal override void Modify(Context cx, Modify m, long p)
        {
            cx.db = cx.db + (this+(Body,m.proc),p);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Procedure(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new Procedure(dp, m);
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var np = (Domain)ins.Fix(cx);
            if (np!=ins)
                r += (Params, np);
            var nb = cx.Fix(body);
            if (nb>=0)
                r += (Body, nb);
            return r;
        }
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            var r = (Procedure)base.ShallowReplace(cx, was, now);
            var ps = ins.ShallowReplace1(cx,was,now);
            if (ps != ins)
                r += (Params, ps);
            return r;
        }
        internal override void Cascade(Context cx,
            Drop.DropAction a = 0, BTree<long, TypedValue>? u = null)
        {
            base.Cascade(cx, a, u);
            for (var b = cx.role.dbobjects.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.db.objects[p] is DBObject ob)
                    if (ob.Calls(defpos, cx))
                        ob.Cascade(cx, a, u);
        }
        internal override bool Calls(long defpos, Context cx)
        {
            if (cx.obs[body] is not DBObject ob)
                throw new PEException("PE1480");
            return ob.Calls(defpos, cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(domain);
            sb.Append(" Arity="); sb.Append(arity); sb.Append(" ");
            sb.Append(" Params");
            var cm = '(';
            for (var i = 0; i < ins.Length; i++)
            {
                sb.Append(cm); cm = ','; sb.Append(Uid(ins[i]??-1L));
            }
            sb.Append(") Body:"); sb.Append(Uid(body));
            sb.Append(" Clause{"); sb.Append(clause); sb.Append('}');
            if (mem.Contains(Inverse)) { sb.Append(" Inverse="); sb.Append(Uid(inverse)); }
            if (mem.Contains(Monotonic)) { sb.Append(" Monotonic"); }
            return sb.ToString();
        }
    }
}
