using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Text;
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
    /// A Level 3 Method definition (a subclass of Procedure)
    /// Immutable
    /// </summary>
    internal class Method : Procedure
    {
        internal const long
            MethodType = -165, // PMethod.MethodType
            TypeDef = -166; // Domain
        /// <summary>
        /// The owning type definition (each role will have its own ObInfo)
        /// </summary>
		public Domain udType => (Domain)mem[TypeDef]??Domain.Null;
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
            : base(m, cx, BTree<long, object>.Empty
                  + (TypeDef, m.typedefpos) + (MethodType, m.methodType))
        { }
        public Method(long defpos, BTree<long, object> m) : base(defpos, m) { }
        public static Method operator+(Method m,(long,object)x)
        {
            return new Method(m.defpos, m.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Method(defpos,m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" UDType="); sb.Append(udType);
            sb.Append(" MethodType="); sb.Append(methodType);
            return sb.ToString();
        }
        internal override void Modify(Context cx, DBObject now, long p)
        {
            cx.db = cx.db + (this + (Body, now.defpos), p) + (Database.SchemaKey,p); // ensure call on the correct operator+
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
        public Context Exec(Context cx, long var, RowType actIns)
        {
            var oi = (ObInfo)cx.db.role.infos[defpos];
            if (!oi.priv.HasFlag(Grant.Privilege.Execute))
                throw new DBException("42105");
            var a = cx.GetActivation();
            a.var = (SqlValue)cx.obs[var];
            var ut = udType;
            var targ = a.var.Eval(cx);
            var n = (int)ins.Count;
            var acts = new TypedValue[n];
            var i = 0;
            for (var b = actIns.First(); b != null; b = b.Next(), i++)
                acts[i] = cx.obs[b.value().Item1].Eval(cx);
            var act = new CalledActivation(cx, this, ut);
            var bd = (Executable)act.obs[body];
            act.obs += (bd.framing,true);
            if (targ is TRow rw)
                for (var b = rw.values.First(); b != null; b = b.Next())
                    act.values += (b.key(), b.value());
            act.values += (defpos,targ);
            i = 0;
            for (var b = ins.First(); b != null; b = b.Next(), i++)
                act.values += (b.value().Item1, acts[i]);
            if (methodType != PMethod.MethodType.Constructor)
                for (var b=ut.representation.First();b!=null;b=b.Next())
                {
                    var p= b.key();
                    act.values+=(p,cx.values[p]);
                }
            cx = bd.Obey(act);
            var r = act.Ret();
            if (r is RowSet ts)
            {
                for (var b = act.values.First(); b != null; b = b.Next())
                    if (!cx.values.Contains(b.key()))
                        cx.values += (b.key(), b.value());
            }
            i = 0;
            for (var b = ins.First(); b != null; b = b.Next(), i++)
            {
                var p = (FormalParameter)cx.obs[b.value().Item1];
                var m = p.paramMode;
                var v = act.values[b.value().Item1];
                if (m == Sqlx.INOUT || m == Sqlx.OUT)
                    acts[i] = v;
                if (m == Sqlx.RESULT)
                    r = v;
            }
            if (methodType == PMethod.MethodType.Constructor)
            {
                var ks = BTree<long,TypedValue>.Empty;
                for (var b = ut.representation.First(); b != null; b = b.Next())
                {
                    var p = b.key();
                    ks+=(p,act.values[p]);
                }
                r = new TRow(cx.Signature(ut.defpos),ut, ks);
            }
            if (cx != null)
            {
                cx.val = r;
                i = 0;
                for (var b = ins.First(); b != null; b = b.Next(), i++)
                {
                    var p = (FormalParameter)cx.obs[b.value().Item1];
                    var m = p.paramMode;
                    if (m == Sqlx.INOUT || m == Sqlx.OUT)
                        cx.AddValue(cx.obs[actIns[i].Item1], acts[i]);
                }
            }
            return cx;
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            var ui = (ObInfo)d.role.infos[udType.defpos];
            var ms = BTree<string, BTree<int, long>>.Empty;
            for (var b=ui.methods.First();b!=null;b=b.Next())
            {
                var sm = BTree<int, long>.Empty;
                var ch = false;
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (c.value() != defpos)
                        sm += (c.key(), c.value());
                    else
                        ch = true;
                if (ch)
                    ms += (b.key(), sm);
            }
            nd += (nd.role+(ui+(ObInfo.Methods,ms)),p);
            return base.Drop(d, nd, p);
        }
    }
}
