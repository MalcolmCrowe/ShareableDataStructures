using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
    /// A Level 3 Method definition (a subclass of Procedure)
    /// Immutable
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class Method : Procedure
    {
        internal const long
            MethodType = -165, // PMethod.MethodType
            TypeDef = -166; // UDType
        /// <summary>
        /// The owning type definition (each role will have its own ObInfo)
        /// </summary>
		public UDType udType => (UDType)mem[TypeDef];
        /// <summary>
        /// The method type (constructor etc)
        /// </summary>
		public PMethod.MethodType methodType => (PMethod.MethodType)mem[MethodType];
        /// <summary>
        /// Constructor: A new level 3 method from a level 2 method
        /// </summary>
        /// <param name="m">The level 2 method</param>
        /// <param name="definer">the definer</param>
        /// <param name="owner">the owner</param>
        /// <param name="rs">the accessing roles</param>
        public Method(PMethod m, Context cx)
            : base(m, cx, BTree<long, object>.Empty + (_Framing,m.framing)
                  + (TypeDef, m.udt) + (MethodType, m.methodType))
        { }
        public Method(long defpos, BTree<long, object> m) : base(defpos, m) { }
        public static Method operator+(Method m,(long,object)x)
        {
            return (Method)m.New(m.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Method(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new Method(dp,mem);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" UDType="); sb.Append(udType);
            sb.Append(" MethodType="); sb.Append(methodType);
            return sb.ToString();
        }
        internal override void Modify(Context cx, Modify m, long p)
        {
            cx.db += (this + (Body, m.proc) + (Params, m.parms)
                    + (_Framing, m.framing), p);
        }
        internal override DBObject Instance(long lp,Context cx, BList<Ident> cs=null)
        {
            udType.Instance(lp,cx);
            return base.Instance(lp,cx);
        }
        /// <summary>
        /// Execute a Method
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="db">The database</param>
        /// <param name="dt">The return type</param>
        /// <param name="ut">The owning object type</param>
        /// <param name="sce">The source object instance (null for constructor)</param>
        /// <param name="n">The method name</param>
        /// <param name="actIns">The actual parameter list</param>
        /// <returns>The return value</returns>
        public Context Exec(Context cx, long var, CList<long> actIns)
        {
            var oi = infos[cx.role.defpos];
            if (!oi.priv.HasFlag(Grant.Privilege.Execute))
                throw new DBException("42105");
            var a = cx.GetActivation();
            a.var = (SqlValue)cx.obs[var];
            var ut = (UDType)cx.db.objects[udType.defpos];
            cx.Add(cx._Dom(ut).Instance(ut.defpos, cx));
            var targ = a.var?.Eval(cx) ?? ut.defaultValue;
            for (var b=ut.representation.First();b!=null;b=b.Next())
            {
                 var p= b.key();
                 cx.values+=(p,targ[p]);
            }
            var n = (int)ins.Count;
            var acts = new TypedValue[n];
            var i = 0;
            for (var b = actIns.First(); i < n && b != null; b = b.Next(), i++)
                acts[i] = cx.obs[b.value()].Eval(cx);
            var me = (Method)cx.db.objects[defpos];
            var act = new CalledActivation(cx, me, ut.defpos);
            me = (Method)me.Instance(act.GetUid(),act);
            var bd = (Executable)act.obs[me.body];
            if (targ is TRow rw)
                for (var b = rw.values.First(); b != null; b = b.Next())
                    act.values += (b.key(), b.value());
            act.values += (defpos, targ);
            act.val = targ;
            i = 0;
            for (var b = me.ins.First(); b != null; b = b.Next(), i++)
            {
                var pp = b.value();
                var pi = (FormalParameter)act.obs[pp];
                act.values += (pi.val, acts[i]);
            }
            cx = bd.Obey(act);
            var r = act.Ret();
            if (r is TArray)
            {
                for (var b = act.values.First(); b != null; b = b.Next())
                    if (!cx.values.Contains(b.key()))
                        cx.values += (b.key(), b.value());
            }
            i = 0;
            for (var b = me.ins.First(); b != null; b = b.Next(), i++)
            {
                var p = (FormalParameter)cx.obs[b.value()];
                var m = p.paramMode;
                var v = act.values[p.val];
                if (m == Sqlx.INOUT || m == Sqlx.OUT)
                    acts[i] = v;
                if (m == Sqlx.RESULT)
                    r = v;
            }
            if (methodType == PMethod.MethodType.Constructor)
            {
                var ks = CTree<long, TypedValue>.Empty;
                for (var b = ut.representation.First(); b != null; b = b.Next())
                {
                    var p = b.key();
                    ks += (p, act.values[p]);
                }
                r = new TRow(ut, ks);
            }
            if (cx != null)
            {
                cx.val = r;
                i = 0;
                for (var b = me.ins.First(); b != null; b = b.Next(), i++)
                {
                    var p = (FormalParameter)cx.obs[b.value()];
                    var m = p.paramMode;
                    if (m == Sqlx.INOUT || m == Sqlx.OUT)
                        cx.AddValue(cx.obs[actIns[i]], acts[i]);
                }
            }
            return cx;
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            var udt = (UDType)udType;
            var oi = BTree<long, ObInfo>.Empty;
            for (var u = udt.infos.First(); u != null; u = u.Next())
            {
                var ms = CTree<string, CTree<int, long>>.Empty;
                for (var b = u.value().methodInfos.First(); b != null; b = b.Next())
                {
                    var sm = CTree<int, long>.Empty;
                    var ch = false;
                    for (var c = b.value().First(); c != null; c = c.Next())
                    {
                        if (c.value() != defpos)
                            sm += (c.key(), c.value());
                        else
                            ch = true;
                    }
                    if (ch)
                        ms += (b.key(), sm);
                }
                oi += (u.key(), u.value() + (ObInfo.MethodInfos, ms));
            }
            udt += (Infos, oi);
            nd += (udt.defpos, udt);
            return base.Drop(d, nd, p);
        }
    }
}
