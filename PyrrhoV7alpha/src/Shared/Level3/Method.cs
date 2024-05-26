using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Text;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{
    /// <summary>
    /// A Level 3 Method definition (a subclass of Procedure)
    /// Immutable
    /// 
    /// </summary>
    internal class Method : Procedure
    {
        internal const long
            MethodType = -165, // PMethod.MethodType
            TypeDef = -166; // UDType
        /// <summary>
        /// The owning type definition (each role will have its own ObInfo)
        /// </summary>
		public UDType udType => (UDType)(mem[TypeDef]??throw new PEException("PE1968"));
        /// <summary>
        /// The method type (constructor etc)
        /// </summary>
		public PMethod.MethodType methodType => (PMethod.MethodType)(mem[MethodType]??PMethod.MethodType.Instance);
        /// <summary>
        /// Constructor: A new level 3 method from a level 2 method
        /// </summary>
        /// <param name="m">The level 2 method</param>
        /// <param name="definer">the definer</param>
        /// <param name="owner">the owner</param>
        /// <param name="rs">the accessing roles</param>
        public Method(PMethod m)
            : base(m, BTree<long, object>.Empty + (_Framing,m.framing)
                  + (TypeDef, m.udt??throw new PEException("PE48131"))
                  + (MethodType, m.methodType??throw new PEException("PE48132")))
        { }
        public Method(long defpos, BTree<long, object> m) : base(defpos, m) { }
        public static Method operator+(Method m,(long,object)x)
        {
            var (dp, ob) = x;
            if (m.mem[dp] == ob)
                return m;
            return (Method)m.New(m.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Method(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new Method(dp, m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" UDType="); sb.Append(udType);
            sb.Append(" MethodType="); sb.Append(methodType);
            return sb.ToString();
        }
        internal override void Modify(Context cx, Modify m)
        {
            cx.db += this + (Body, m.proc) + (Params, m.parms) + (_Framing, m.framing);
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
        /// <param name="actIns">The actual parameter tree</param>
        /// <returns>The return value</returns>
        public Context Exec(Context cx, long var, BList<long?> actIns)
        {
            if (cx.role==null || infos[cx.role.defpos] is not ObInfo oi
             || !oi.priv.HasFlag(Grant.Privilege.Execute))
                throw new DBException("42105").Add(Qlx.EXECUTE);
            var a = cx.GetActivation();
            var vr = (QlValue?)cx.obs[var]; // for a constructor, vr is null!
            if (cx.db.objects[udType.defpos] is not UDType ut)
                return cx;
            a.var = vr;
            cx.Add(ut.Instance(ut.defpos, cx));
            var targ = vr?.Eval(cx) ?? ut.defaultValue;
            for (var b = ut.representation.First(); b != null; b = b.Next())
                if (targ[b.key()] is TypedValue tv)
                    cx.values += (b.key(), tv);
            var n = ins.Length;
            var acts = new TypedValue[n];
            var i = 0;
            for (var b = actIns.First(); i < n && b != null; b = b.Next(), i++)
                if (b.value() is long p && cx.obs[p]?.Eval(cx) is TypedValue tv)
                    acts[i] = tv;
            if (cx.db.objects[defpos] is not Method me)
                throw new DBException("42108", NameFor(cx));
            var act = new CalledActivation(cx, me);
            me = (Method)me.Instance(act.GetUid(),act);
            if (act.obs[me.body] is not Executable bd)
                throw new DBException("42108", NameFor(cx));
            if (targ is TRow rw)
                for (var b = rw.values.First(); b != null; b = b.Next())
                    act.values += (b.key(), b.value());
            act.values += (defpos, targ);
            i = 0;
            for (var b = me.ins.First(); b != null; b = b.Next(), i++)
                if (b.value() is long p && act.obs[p] is FormalParameter pi) 
                act.values += (pi.val, acts[i]);
            cx = bd._Obey(act);
            var r = act.Ret();
            if (r is TList)
                for (var b = act.values.First(); b != null; b = b.Next())
                    if (!cx.values.Contains(b.key()))
                        cx.values += (b.key(), b.value());
            i = 0;
            for (var b = me.ins.First(); b != null; b = b.Next(), i++)
                if (b.value() is long bp && cx.obs[bp] is FormalParameter p)
                    if (act.values[p.val] is TypedValue v)
                    {
                        var m = p.paramMode;
                        if (m == Qlx.INOUT || m == Qlx.OUT)
                            acts[i] = v;
                        if (m == Qlx.RESULT)
                            r = v;
                    }
            if (this is Method mt && mt.methodType == PMethod.MethodType.Constructor)
                cx.val = new TRow(mt.udType, act.values);
            else if (r is not null)
                cx.val = r;
            return cx;
        }
        internal override Database Drop(Database d, Database nd)
        {
            var udt = udType;
            var oi = BTree<long, ObInfo>.Empty;
            for (var u = udt.infos.First(); u != null; u = u.Next())
            {
                var ms = BTree<string, BTree<CList<Domain>, long?>>.Empty;
                for (var b = u.value().methodInfos.First(); b != null; b = b.Next())
                {
                    var sm = BTree<CList<Domain>, long?>.Empty;
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
            return base.Drop(d, nd);
        }
    }
}
