using System;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Common;
using System.Text;
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
    /// A Level 3 Method definition (a subclass of Procedure)
    /// Immutable
    /// </summary>
    internal class Method : Procedure
    {
        internal const long
            MethodType = -165, // PMethod.MethodType
            TypeDef = -166; // Domain
        /// <summary>
        /// The owning type definition
        /// </summary>
		public Domain udType => (Domain)mem[TypeDef];
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
        public Method(PMethod m, Sqlx create, Database db)
            : base(m, db, BTree<long, object>.Empty
                  + (TypeDef, db.objects[m.typedefpos]) + (MethodType, m.methodType))
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
            cx.db = cx.db + (this + (Body, now), p) + (Database.SchemaKey,p); // ensure call on the correct operator+
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
        public TypedValue Exec(Context cx, SqlValue var, BList<SqlValue> actIns)
        {
            TypedValue r;
            var a = cx.GetActivation();
            a.var = var;
            var au = new Context(cx, cx.tr.role, cx.tr.user);
            var bd = body;
            var ut = udType;
            var ui = (ObInfo)cx.tr.role.obinfos[ut.defpos];
            var targ = var.Eval(au);
            var act = new CalledActivation(au, this, ut);
            if (targ is TRow rw)
                for (var b = rw.values.First(); b != null; b = b.Next())
                    act.values += (b.key(), b.value());
            act.values += (defpos,targ);
            var acts = new TypedValue[(int)actIns.Count];
            for (int i = 0; i < actIns.Count; i++)
                acts[i] = actIns[i].Eval(cx);
            for (int i = 0; i < actIns.Count; i++)
                act.values+=(ins[i].defpos, acts[i]);
            if (methodType != PMethod.MethodType.Constructor)
                for (int i = 0; i < ui.Length; i++)
                {
                    var se = ui.columns[i];
                    act.values+=(se.defpos,cx.values[se.defpos]);
                }
            cx = act.proc.body.Obey(cx);
            r = cx.val;
            for (int i = 0; i < ins.Count; i++)
            {
                var p = ins[i];
                if (cx is Activation ac && (p.paramMode == Sqlx.INOUT || p.paramMode == Sqlx.OUT))
                    acts[i] = act.values[p.defpos];
                if (p.paramMode == Sqlx.RESULT)
                    r = act.values[p.defpos];
            }
            if (methodType == PMethod.MethodType.Constructor)
            {
                var ks = new TypedValue[ui.Length];
                for (int i = 0; i < ui.Length; i++)
                    ks[i] = act.values[ui.columns[i].defpos];
                r = new TRow(ui, ks);
            }
            for (int i = 0; i < ins.Count; i++)
            {
                var p = ins[i];
                if (cx is Activation ac && (p.paramMode == Sqlx.INOUT || p.paramMode == Sqlx.OUT))
                    ac.values+=(actIns[i].defpos, acts[i]);
            } 
            return r;
        }
        internal override Database Drop(Database d, Database nd, long p)
        {
            var ui = (ObInfo)nd.role.obinfos[udType.defpos];
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
