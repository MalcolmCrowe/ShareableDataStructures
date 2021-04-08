using System.Collections.Generic;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using System;
using System.Configuration;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
    /// DBObject is the base class for Level 3 database objects (e.g. Table, Role, Procedure, Domain)
    /// Immutable
    /// </summary>
    internal abstract class DBObject : Basis
    {
        /// <summary>
        /// The uid of the abstract object this is or affects
        /// </summary>
        public readonly long defpos;
        internal const long
            _Alias = -62, // string        
            Classification = -63, // Level
            CompareContext = -250, // Context structured types
            Definer = -64, // long Role
            Defpos = -257, // long for Rest service
            Dependents = -65, // CTree<long,bool> Non-obvious objects that need this to exist
            Depth = -66, // int  (max depth of dependents)
            _Domain = -176, // Domain 
            _Framing = -167, // BTree<long,DBObject> compiled objects
            _From = -306, // long
            LastChange = -68, // long (formerly called Ppos)
            Sensitive = -69; // bool
        /// <summary>
        /// During transaction execution, many DBObjects have aliases.
        /// Aliases do not form part of renaming machinery
        /// </summary>
        internal string alias => (string)mem[_Alias];
        /// <summary>
        /// The definer of the object (a Role)
        /// </summary>
        public long definer => (long)(mem[Definer] ?? -1L);
        //        internal Context compareContext => 
        internal long lastChange => (long)(mem[LastChange] ?? 0L);// compareContext?.db.loadpos ?? 0L;
        /// <summary>
        /// Sensitive if it contains a sensitive type
        /// </summary>
        internal bool sensitive => (bool)(mem[Sensitive] ?? false);
        internal Level classification => (Level)mem[Classification] ?? Level.D;
        internal Domain domain => (Domain)mem[_Domain];
        internal long from => (long)(mem[_From] ?? -1L);
        /// <summary>
        /// For compiled code - triggers and Procedures
        /// </summary>
        internal Framing framing =>
            (Framing)mem[_Framing] ?? Framing.Empty;
        /// <summary>
        /// This list does not include indexes/columns/rows for tables
        /// or other obvious structural dependencies
        /// </summary>
        internal CTree<long, bool> dependents =>
            (CTree<long, bool>)mem[Dependents] ?? CTree<long, bool>.Empty;
        internal int depth => (int)(mem[Depth] ?? 1);
        /// <summary>
        /// Constructor
        /// </summary>
        protected DBObject(long dp, BTree<long, object> m) : base(m)
        {
            defpos = dp;
        }
        protected DBObject(long pp, long dp, long dr, BTree<long, object> m = null)
            : this(dp, (m ?? BTree<long, object>.Empty) + (LastChange, pp) + (Definer, dr))
        { }
        protected DBObject(string nm, long pp, long dp, long dr, BTree<long, object> m = null)
            : this(pp, dp, dr, (m ?? BTree<long, object>.Empty) + (LastChange, pp) + (Name, nm))
        { }
        public static DBObject operator +(DBObject ob, (long, object) x)
        {
            return (DBObject)ob.New(ob.mem + x);
        }
        /// <summary>
        /// Used for shared Query and RowSets to create new copies 
        /// when we want to modify a property (e.g. adding a filter).
        /// As far as I can see this is not required for SqlValues or Executables.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="m"></param>
        /// <returns></returns>
        internal virtual DBObject New(Context cx, BTree<long, object> m)
        {
            return (DBObject)New(m);
        }
        internal static int _Max(params int[] x)
        {
            var r = 0;
            for (var i = 0; i < x.Length; i++)
                if (x[i] > r)
                    r = x[i];
            return r;
        }
        internal static CTree<long, bool> _Deps(CList<long> vs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = vs?.First(); b != null; b = b.Next())
                r += (b.value(), true);
            return r;
        }
        internal static CTree<long, bool> _Deps(BList<SqlValue> vs)
        {
            var r = CTree<long, bool>.Empty;
            for (var b = vs?.First(); b != null; b = b.Next())
                r += (b.value().defpos, true);
            return r;
        }
        internal virtual CList<long> _Cols(Context cx)
        {
            return CList<long>.Empty;
        }
        internal static int _Depth(BList<SqlValue> vs)
        {
            var r = 0;
            for (var b = vs?.First(); b != null; b = b.Next())
                if (b.value().depth > r)
                    r = b.value().depth;
            return r;
        }
        /// <summary>
        /// Check to see if the current role has the given privilege on this (except Admin)
        /// For ADMIN and classified objects we check the current user has this privilege
        /// </summary>
        /// <param name="priv">The privilege in question</param>
        /// <returns>the current role if it has this privilege</returns>
        public virtual bool Denied(Context cx, Grant.Privilege priv)
        {
            var tr = cx.tr;
            if (tr == null)
                return false;
            if (defpos > Transaction.TransPos)
                return false;
            var oi = (ObInfo)tr.role.infos[defpos];
            return (oi != null) && (oi.priv & priv) == 0;
        }
        internal virtual CTree<long, bool> Needs(Context cx)
        {
            return CTree<long, bool>.Empty;
        }
        internal virtual ObInfo Inf(Context cx)
        {
            throw new NotImplementedException();
        }
        internal virtual CTree<long, RowSet.Finder> Needs(Context context, RowSet rs)
        {
            return CTree<long, RowSet.Finder>.Empty;
        }
        internal virtual bool LocallyConstant(Context cx,RowSet rs)
        {
            return false;
        }
        /// <summary>
        /// This one is used mainly in commit to transaction log,
        /// blindly changes the defpos
        /// </summary>
        /// <param name="dp"></param>
        /// <returns></returns>
        internal abstract DBObject Relocate(long dp);
        /// <summary>
        /// When a Physical commits to transaction log, this routine
        /// helps to relocate the associated compiled objects
        /// </summary>
        /// <param name="wr"></param>
        /// <returns></returns>
        internal override Basis _Relocate(Writer wr)
        {
            if (defpos < wr.Length)
                return this;
            var r = ((DBObject)base._Relocate(wr)).Relocate(wr.Fix(defpos));
            var dm = domain?._Relocate(wr);
            if (dm != domain)
                r += (_Domain, dm);
            var df = wr.Fix(definer);
            if (df != definer)
                r += (Definer, df);
            var ds = CTree<long, bool>.Empty;
            for (var b = dependents.First(); b != null; b = b.Next())
                ds += (wr.Fix(b.key()), true);
            if (ds != dependents)
                r += (Dependents, ds);
            if (mem.Contains(TableExpression.Nuid))
            {
                var nu = (long)mem[TableExpression.Nuid];
                var nn = wr.rss.Contains(nu)? wr.rss[nu].defpos
                    : wr.uids.Contains(nu)? wr.uids[nu]
                    : wr.Fixed(nu).defpos;
                if (nn != nu)
                    r += (TableExpression.Nuid, nn);
            }
            wr.cx.obs += (r.defpos, r);
            return r;
        }
        /// <summary>
        /// Adjust compiled DBObject (calls _Relocate)
        /// </summary>
        /// <param name="wr"></param>
        /// <returns></returns>
        internal DBObject Relocate(Writer wr)
        {
            if (this is RowSet)
            {
                if (wr.cx.rsuids.Contains(defpos))
                    return wr.cx.data[wr.cx.rsuids[defpos]??defpos];
            }
            else if (wr.cx.obuids.Contains(defpos))
                return wr.cx.obs[wr.cx.obuids[defpos]??defpos];
            var r = (DBObject)_Relocate(wr);
            if (r is RowSet rs)
                wr.cx.data += (r.defpos, rs);
            else
                wr.cx.obs += (r.defpos, r);
            return r;
        }
        /// <summary>
        /// Fix does the work of relocation for sharing - see Compiled.Relocate(cx)
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override Basis Fix(Context cx)
        {
            var r = this;
            var np = cx.obuids[defpos]??defpos;
            if (np != defpos)
            {
                r = cx.obs[np];
                if (r==null || r  is SqlNull)
                    r = cx._Add(Relocate(np));
            }
            var nd = cx.obuids[definer] ?? definer;
            if (definer !=nd)
                    r += (Definer, nd);
            var ds = cx.Fix(dependents);
            if (ds != dependents)
                r += (Dependents, ds);
            if (mem.Contains(TableExpression.Nuid))
            {
                var nu = (long)mem[TableExpression.Nuid];
                var nn = cx.rsuids[nu]??cx.obuids[nu]??nu;
                if (nn != nu)
                    r += (TableExpression.Nuid, nn);
            }
            return r;
        }
        /// <summary>
        /// Some DBObjects are modified when metadata is defined
        /// </summary>
        /// <param name="d"></param>
        /// <param name="pm"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        internal virtual Database Add(Database d,PMetadata pm, long p)
        {
            return d;
        }
        // Helper for format<51 compatibility
        internal virtual SqlValue ToSql(Ident id,Database db)
        {
            return null;
        }
        internal virtual BTree<long,SystemFilter> SysFilter(Context cx,BTree<long,SystemFilter> sf)
        {
            return sf;
        }
        internal virtual SqlValue Operand(Context cx)
        {
            return null;
        }
        /// <summary>
        /// Drop anything that needs this, directly or indirectly,
        /// and then drop this.
        /// Called by Drop for Database on Commit and Load
        /// </summary>
        /// <param name="d"></param>
        /// <param name="nd"></param>
        /// <returns></returns>
        internal virtual void Cascade(Context cx, Drop.DropAction a=0,
            BTree<long,TypedValue>u=null)
        {
            for (var b = cx.tr.physicals.First(); b != null; b = b.Next())
                if (b.value() is Drop dr && dr.delpos == defpos)
                    return;
            cx.Add(new Drop1(defpos, a, cx.tr.nextPos, cx));
            if (dependents.Count == 0)
                return;
            for (var b = dependents.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is DBObject ob)
                {
                    if (a == 0)
                    {
                        if (!(this is Table tb && cx.db.objects[b.key()] is TableColumn tc
                            && tb.defpos == tc.tabledefpos))
                        {
                            throw new DBException("23001",
                                GetType().Name + " " + Uid(defpos), ob.GetType().Name + " " + Uid(b.key()));
                        }
                    }
                    ob.Cascade(cx, a, u);
                }
        }
        /// <summary>
        /// Execute an Insert operation for a Table, View, RestView.
        /// The new or existing Rowsets may be explicit or in the physical database.
        /// Deal with triggers.
        /// </summary>
        /// <param name="f">A query</param>
        /// <param name="prov">The provenance string</param>
        /// <param name="cl">The classification sought</param>
        internal virtual Context Insert(Context _cx, RowSet fm, string prov, Level cl)
        {
            _cx.Install2(framing);
            return _cx;
        }
        internal virtual Context Delete(Context cx,RowSet fm)
        {
            throw new NotImplementedException();
        }
        internal virtual Context Update(Context cx, RowSet fm)
        {
            throw new NotImplementedException();
        }
        internal virtual Database Drop(Database d, Database nd,long p)
        {
            return nd - defpos;
        }
        internal virtual Database DropCheck(long ck,Database nd,long p)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// Discover if any call found on routine defpos
        /// </summary>
        /// <param name="defpos"></param>
        /// <param name="tr"></param>
        internal virtual bool Calls(long defpos,Context cx)
        {
            return false;
        }
        internal static bool Calls(BList<DBObject> vs, long defpos, Context cx)
        {
            for (var b = vs?.First(); b != null; b = b.Next())
                if (b.value().Calls(defpos, cx))
                    return true;
            return false;
        }
        internal virtual void Modify(Context cx, Modify m, long p)
        {
            cx.db += (m.now, p);
        }
        internal virtual DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = this;
            if (mem.Contains(TableExpression.Nuid))
            {
                var nu = (long)mem[TableExpression.Nuid];
                var fm = cx.Replace(nu, so, sv);
                if (fm != nu)
                    r += (TableExpression.Nuid, fm);
            }
            return r;
        }
        internal DBObject Replace(Context cx,DBObject was,DBObject now)
        {
            var ldpos = cx.db.loadpos;
            for (var cc = cx.next; cc != null; cc = cc.next)
                ldpos = cc.db.loadpos;
            if (defpos < ldpos)
                return this;
            var r = _Replace(cx, was, now);
            if (r != this && dependents.Contains(was.defpos) && (now.depth + 1) > depth)
            {
                r += (Depth, now.depth + 1);
                cx.done += (r.defpos, r);
            }
            for (var b = dependents.First(); b != null; b = b.Next())
                if (cx.done[b.key()] is DBObject d && d.depth >= r.depth)
                {
                    r += (Depth, d.depth + 1);
                    cx.done += (r.defpos, r);
                } 
            return r;
        }
        internal virtual object Build(Context _cx, RowSet rs)
        {
            return null;
        }
        internal virtual void _Add(Context cx)
        {
            cx.obs += (defpos, this);
        }
        /// <summary>
        /// Add a new column to the query, and update the row type
        /// (Needed for alter)
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        internal virtual DBObject Add(Context cx, SqlValue v)
        {
            if (v == null)
                return this;
            var deps = dependents + (v.defpos, true);
            var dpt = _Max(depth, 1 + v.depth);
            var r = this + (Dependents, deps) + (Depth, dpt)
                + (_Domain, domain + (v.defpos, v.domain));
            return r;
        }
        internal virtual DBObject Remove(Context cx, SqlValue v)
        {
            if (v == null)
                return this;
            var rt = CList<long>.Empty;
            var rp = BTree<long, Domain>.Empty;
            var ch = false;
            var rb = domain.representation.First();
            for (var b = domain.rowType?.First(); b != null && rb != null; b = b.Next(), rb = rb.Next())
                if (b.value() == v.defpos)
                    ch = true;
                else
                {
                    rp += (rb.key(), rb.value());
                    rt += b.value();
                }
            return ch ?
                New(cx, mem + (_Domain, new Domain(Sqlx.ROW, cx, rt)) + (Dependents, dependents - v.defpos))
                : this;
        }
        internal virtual DBObject Hide(Context cx, SqlValue v)
        {
            if (v == null)
                return this;
            var rt = CList<long>.Empty;
            var ch = false;
            for (var b = domain.rowType?.First(); b != null; b = b.Next())
                if (b.value() == v.defpos)
                    ch = true;
                else
                    rt += b.value();
            if (!ch)
                return this;
            var d = domain.display -1;
            rt += v.defpos;
            return New(cx, mem + (_Domain, new Domain(Sqlx.ROW, domain.representation, rt, d)));
        }
        /// <summary>
        /// Called after a condition is added to a join that could turn it into an FDJoin.
        /// We can therefore assume that any resulting relocation has already been done.
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal virtual DBObject ReviewJoins(Context cx)
        {
            return this;
        }
        internal virtual DBObject Conditions(Context cx)
        {
            return this;
        }
        /// <summary>
        /// </summary>
        /// <param name="svs">A list of where conditions</param>
        /// <param name="tr"></param>
        /// <param name="q">A source query</param>
        /// <returns></returns>
        internal virtual DBObject MoveConditions(Context cx, Query q)
        {
            return this;
        }
        internal CTree<long, bool> Needs(Context cx, CTree<long, bool> s)
        {
            for (var b = domain.rowType?.First(); b != null; b = b.Next())
                s = ((SqlValue)cx.obs[b.value()]).Needs(cx, s);
            return s;
        }
        internal virtual DBObject Orders(Context cx, CList<long> ord)
        {
            return this;
        }
        public static bool Eval(CTree<long, bool> svs, Context cx)
        {
            for (var b = svs?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Eval(cx) != TBool.True)
                    return false;
            return true;
        }
        /// <summary>
        /// Bottom up: Add q.matches to this.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="q"></param>
        /// <returns></returns>
        internal virtual DBObject AddMatches(Context cx, Query q)
        {
            return this;
        }
        /// <summary>
        /// The given Sqlvalue is guaranteed to be a constant at this level and context.
        /// We don't propagate to other levels.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="sv"></param>
        /// <param name="tv"></param>
        /// <returns></returns>
        internal virtual DBObject AddMatch(Context cx, SqlValue sv, TypedValue tv)
        {
            return this;
        }
        internal virtual DBObject AddCondition(Context cx, long prop, long cond)
        {
            return this;
        }
        internal virtual DBObject AddCondition(Context cx, CTree<long, bool> conds)
        {
            return this;
        }
        internal DBObject AddCondition(Context cx, long prop, CTree<long, bool> conds)
        {
            var q = this;
            for (var b = conds.First(); b != null; b = b.Next())
                q = q.AddCondition(cx, prop, (SqlValue)cx.obs[b.key()], false);
            return New(cx, q.mem);
        }
        internal virtual DBObject AddCondition(Context cx, long prop, SqlValue cond, bool onlyKnown)
        {
            return this;
        }
        internal virtual void Set(Context cx, TypedValue v)
        {
            cx.values += (defpos, v);
        }
        /// <summary>
        /// If the value contains aggregates we need to accumulate them
        /// </summary>
        internal virtual BTree<long,Register> StartCounter(Context _cx, RowSet rs,BTree<long,Register> tg)
        {
            return tg;
        }
        /// <summary>
        /// If the value contains aggregates we need to accumulate them. 
        /// Carefully watch out for common subexpressions, and only AddIn once!
        /// </summary>
        internal virtual BTree<long, Register> AddIn(Context _cx, Cursor rb, BTree<long, Register> tg) 
        {
            return tg;
        }
        internal virtual DBObject TypeOf(long lp,Context cx,TypedValue v)
        {
            throw new System.NotImplementedException();
        }
        internal virtual TypedValue Eval(Context cx)
        {
            return cx.values[defpos];
        }
        internal virtual bool aggregates(Context cx)
        {
            return false;
        }
        /// <summary>
        /// SqlValues are sticky if from is defined for the first RowSet that can access them
        /// </summary>
        /// <returns></returns>
        internal virtual bool sticky()
        {
            return false;
        }
        internal virtual CTree<long,TypedValue> AddMatch(Context cx,CTree<long,TypedValue> ma,
            Table tb=null)
        {
            return ma;
        }
        /// <summary>
        /// Check constraints can be added to Domains, TableColumns and Tables
        /// </summary>
        /// <param name="ck"></param>
        /// <param name="db"></param>
        /// <returns></returns>
        internal virtual DBObject Add(Check ck,Database db)
        {
            throw new PEException("PE481");
        }
        internal virtual DBObject AddProperty(Check ck, Database db)
        {
            throw new PEException("PE481");
        }
        internal virtual void RowSets(Context cx,From f,CTree<long,RowSet.Finder> fi)
        {
            if (!cx.data.Contains(f.defpos))
                cx.data+=(f.defpos, new TrivialRowSet(f.defpos, cx, new TRow(domain, cx.values), -1, fi));
        }
        /// <summary>
        /// Scan makes catalogue cx.obref of dependencies for instancing.
        /// </summary>
        internal virtual BTree<long,VIC?> Scan(BTree<long,VIC?> t)
        {
            if (mem.Contains(TableExpression.Nuid))
                t = Scan(t, (long)mem[TableExpression.Nuid], VIC.OK | VIC.OV | VIC.RV);
            return t;
        }
        internal BTree<long,VIC?> Scan(BTree<long,VIC?> t,long dp,VIC vc)
        {
            if (dp>=0)
                t += (dp, (t[dp] ?? VIC.None) | vc);
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, BList<long> ord, VIC vc)
        {
            for (var b = ord?.First(); b != null; b = b.Next())
                t += (b.value(), (t[b.value()] ?? VIC.None) | vc);
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, CList<UpdateAssignment> us)
        {
            for (var b = us.First(); b != null; b = b.Next())
            {
                var ua = b.value();
                t += (ua.val, (t[ua.val] ?? VIC.None) | VIC.OK | VIC.OV);
                t += (ua.vbl, (t[ua.vbl] ?? VIC.None) | VIC.OK | VIC.OV);
            }
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, CTree<UpdateAssignment, bool> us,
            VIC va)
        {
            for (var b = us.First(); b != null; b = b.Next())
            {
                var ua = b.key();
                t += (ua.val, (t[ua.val] ?? VIC.None) | va);
                t += (ua.vbl, (t[ua.vbl] ?? VIC.None) | va);
            }
            return t;
        }
        internal BTree<long, VIC?> Scan<V>(BTree<long, VIC?> t, BTree<long, V> ms, VIC va)
        {
            for (var b = ms.First(); b != null; b = b.Next())
                t += (b.key(), (t[b.key()] ?? VIC.None) | va);
            return t;
        }
        internal BTree<long, VIC?> Scan<K>(BTree<long, VIC?> t, BTree<K, long> ms, VIC va) where K : IComparable
        {
            for (var b = ms.First(); b != null; b = b.Next())
                t += (b.value(), (t[b.value()] ?? VIC.None) | va);
            return t;
        }
        internal BTree<long, VIC?> Scan<K>(BTree<long, VIC?> t, BTree<K, object> ms) where K : IComparable
        {
            for (var b = ms.First(); b != null; b = b.Next())
                if (b.value() is DBObject ob)
                    t += (ob.defpos, (t[ob.defpos] ?? VIC.None) | VIC.OK | VIC.OV);
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, CTree<long, long> ms, VIC va, VIC vb)
        {
            for (var b = ms.First(); b != null; b = b.Next())
            {
                t += (b.key(), (t[b.key()] ?? VIC.None) | va);
                t += (b.value(), (t[b.value()] ?? VIC.None) | vb);
            }
            return t;
        }
        internal BTree<long,VIC?> Scan(BTree<long,VIC?> t, CTree<string, CTree<long, long>> vc,
            VIC va, VIC vb)
        {
            for (var b = vc.First(); b != null; b = b.Next())
                t = Scan(t, b.value(), va, vb);
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, CTree<long, RowSet.Finder> fi)
        {
            for (var b = fi?.First(); b != null; b = b.Next())
            {
                t += (b.key(), (t[b.key()] ?? VIC.None) | VIC.RK | VIC.OV);
                var f = b.value();
                t += (f.col, (t[f.col] ?? VIC.None) | VIC.RK | VIC.OV);
                t += (f.rowSet, (t[f.rowSet] ?? VIC.None) | VIC.RK | VIC.RV);
            }
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, BList<(long, BTree<long,Cursor>)> rs, 
            VIC vc)
        {
            for (var b = rs.First(); b != null; b = b.Next())
                t += (b.key(), (t[b.key()] ?? VIC.None) | vc);
                // TDO
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, BList<(long, TRow)> rs,
    VIC vc)
        {
            for (var b = rs.First(); b != null; b = b.Next())
                t += (b.key(), (t[b.key()] ?? VIC.None) | vc);
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, 
            CTree<long, CTree<long, bool>> ma, VIC va, VIC vb)
        {
            for (var b = ma.First(); b != null; b = b.Next())
            {
                t += (b.key(), (t[b.key()] ?? VIC.None) | va);
                t = Scan(t, b.value(), vb);
            }
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, CTree<long, Domain> rs)
        {
            for (var b = rs.First(); b != null; b = b.Next())
                t += (b.key(), (t[b.key()] ?? VIC.None) | VIC.OK | VIC.OV);
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t, BTree<long, RowSet> rs, VIC va)
        {
            for (var b = rs.First(); b != null; b = b.Next())
                t += (b.key(), (t[b.key()] ?? VIC.None) | va);
            return t;
        }
        internal BTree<long, VIC?> Scan<K>(BTree<long, VIC?> t, CTree<K, CTree<long, bool>> ts, VIC va) where K : IComparable
        {
            for (var b = ts.First(); b != null; b = b.Next())
                t = Scan(t, b.value(), va);
            return t;
        }
        internal BTree<long, VIC?> Scan(BTree<long, VIC?> t,BList<(long,TypedValue)> ls,VIC va)
        {
            for (var b = ls.First(); b != null; b = b.Next())
                t = Scan(t, b.value().Item1, va);
            return t;
        }
        /// <summary>
        /// Creates new instances of objects in framing lists
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal virtual DBObject Instance(Context cx)
        {
            return this;
        }
        /// <summary>
        /// Implementation of the Role$Class table: Produce a C# class corresponding to a Table or View
        /// </summary>
        /// <param name="from">A query</param>
        /// <param name="_enu">An enumerator for the set of database objects</param>
        /// <returns>A row for the Role$Class table</returns>
        internal virtual TRow RoleClassValue(Transaction tr,DBObject from,
            ABookmark<long, object> _enu)
        {
            return null;
        } 
        /// <summary>
        /// Implementation of the Role$Java table: Produce a Java class corresponding to a Table or View
        /// </summary>
        /// <param name="from">A query</param>
        /// <param name="_enu">An enumerator for the set of database objects</param>
        /// <returns>A row for the Role$Class table</returns>
        internal virtual TRow RoleJavaValue(Transaction tr, DBObject from, ABookmark<long, object> _enu)
        {
            return null;
        }
        /// <summary>
        /// Implementation of the Role$Python table: Produce a Python class corresponding to a Table or View
        /// </summary>
        /// <param name="from">A query</param>
        /// <param name="_enu">An enumerator for the set of database objects</param>
        /// <returns>A row for the Role$Class table</returns>
        internal virtual TRow RolePythonValue(Transaction tr, DBObject from, ABookmark<long, object> _enu)
        {
            return null;
        }
        /// <summary>
        /// Implementation of the Role$Class table: Produce a type attribute for a field
        /// </summary>
        /// <param name="sb">A string builder to receive the attribute</param>
        /// <param name="dt">The Pyrrho datatype</param>
        protected static void FieldType(Database db,StringBuilder sb, Domain dt)
        {
            switch (Domain.Equivalent(dt.kind))
            {
                case Sqlx.ONLY: 
                    FieldType(db, sb, (dt as UDType)?.super); return;
                case Sqlx.INTEGER:
                    if (dt.prec!=0)
                        sb.Append("[Field(PyrrhoDbType.Integer," + 
                            (int)dt.prec + ")]\r\n");
                    return;
                case Sqlx.NUMERIC:
                    sb.Append("[Field(PyrrhoDbType.Decimal," + dt.prec + "," + dt.scale + ")]\r\n");
                    return;
                case Sqlx.NCHAR:
                case Sqlx.CHAR:
                    if (dt.prec != 0)
                        sb.Append("[Field(PyrrhoDbType.String," + dt.prec + ")]\r\n");
                    return;
                case Sqlx.REAL:
                    if (dt.scale != 0 || dt.prec!=0)
                        sb.Append("[Field(PyrrhoDBType.Real," + dt.prec + "," + dt.scale + ")]\r\n");
                    return;
                case Sqlx.DATE: sb.Append("[Field(PyrrhoDbType.Date)]\r\n"); return;
                case Sqlx.TIME: sb.Append("[Field(PyrrhoDbType.Time)]\r\n"); return;
                case Sqlx.INTERVAL: sb.Append("[Field(PyrrhoDbType.Interval)]\r\n"); return;
                case Sqlx.BOOLEAN: sb.Append("[Field(PyrrhoDbType.Bool)]\r\n"); return;
                case Sqlx.TIMESTAMP: sb.Append("[Field(PyrrhoDbType.Timestamp)]\r\n"); return;
                case Sqlx.ROW: sb.Append("[Field(PyrrhoDbType.Row," + dt.elType.name+ ")]\r\n"); 
                    return;
            }
        }
        /// <summary>
        /// Implementation of the Role$Java table: Produce a type annotation for a field
        /// </summary>
        /// <param name="sb">A string builder to receive the attribute</param>
        /// <param name="dt">The Pyrrho datatype</param>
        protected void FieldJava(Database db, StringBuilder sb, Domain dt)
        {
            switch (Domain.Equivalent(dt.kind))
            {
                case Sqlx.ONLY: FieldJava(db, sb, (dt as UDType)?.super); return;
                case Sqlx.INTEGER:
                    if (dt.prec != 0)
                        sb.Append("@FieldType(PyrrhoDbType.Integer," + dt.prec + ")\r\n");
                    return;
                case Sqlx.NUMERIC:
                    sb.Append("@FieldType(PyrrhoDbType.Decimal," + dt.prec + "," + dt.scale + ")\r\n");
                    return;
                case Sqlx.NCHAR:
                case Sqlx.CHAR:
                    if (dt.prec != 0)
                        sb.Append("@FieldType(PyrrhoDbType.String," + dt.prec + ")\r\n");
                    return;
                case Sqlx.REAL:
                    if (dt.scale != 0||dt.prec!=0)
                        sb.Append("@FieldType(PyrrhoDBType.Real," + dt.prec + "," + dt.scale + ")\r\n");
                    return;
                case Sqlx.DATE: sb.Append("@FieldType(PyrrhoDbType.Date)\r\n"); return;
                case Sqlx.TIME: sb.Append("@FieldType(PyrrhoDbType.Time)\r\n"); return;
                case Sqlx.INTERVAL: sb.Append("@FieldType(PyrrhoDbType.Interval)\r\n"); return;
                case Sqlx.BOOLEAN: sb.Append("@FieldType(PyrrhoDbType.Bool)\r\n"); return;
                case Sqlx.TIMESTAMP: sb.Append("@FieldType(PyrrhoDbType.Timestamp)\r\n"); return;
                case Sqlx.ROW: sb.Append("@FieldType(PyrrhoDbType.Row," + dt.elType.name + ")\r\n");
                    return;
            }
        }
        internal virtual BTree<Sqlx,object> Meta(Database db)
        {
            return ((ObInfo)db.role.infos[defpos]).metadata;
        }
        /// <summary>
        /// Issues here: This object may not have been committed yet
        /// We only want to record audits in the PhysBase for committed data
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="m"></param>
        internal void Audit(Context cx, RowSet rs, Query f)
        {
            if (cx.tr == null || cx.db.user.defpos == cx.db.owner)
                return;
            var tb = this as Table;
            if (defpos >= Transaction.TransPos)
                return;
            var mac = (tb?.classification.maxLevel ?? 0) > 0&&
                tb?.enforcement.HasFlag(Grant.Privilege.Select) ==true
                && cx.db._user!=cx.db.owner;
            if (!(mac || sensitive))
                return;
            var ckc = f.display < tb.domain.rowType.Length;
            if (ckc && !mac)
            {
                var j = 0;
                var cb = tb.domain.rowType.First();
                for (var b = f.rowType.First(); ckc && b != null;
                        b = b.Next(), cb = cb.Next(), j++)
                    if (j < f.display)
                    {
                        if (((TableColumn)cx.db.objects[cb.value()]).sensitive)
                            ckc = false;
                    }
                    else if (b.value() < Transaction.Executables)
                        ckc = false;
            }
            if (ckc && !mac)
                return;
            if (!sensitive)
            {
                var found = false;
                for (var b = rs.First(cx); (!found) && b != null; b = b.Next(cx))
                    if (b[Classification]is TLevel lv && lv.Val() is Level vl 
                        && vl.maxLevel > 0)
                        found = true;
                if (!found)
                    return;
            }
            var match = CTree<long, string>.Empty;
            for (var b = rs.matches?.First(); b != null; b = b.Next())
                match += (b.key(), b.value()?.ToString() ?? "null");
            var a = new Audit(cx.tr.user, defpos, match, DateTime.Now.Ticks, cx.db.nextPos, cx);
            if (cx.auds.Contains(a))
                return;
            cx.auds += (a, true);
            cx.tr.Audit(a, cx); // write it to the file immediately
        }
        internal static string Uid(long u)
        {
            if (u >= PyrrhoServer.Preparing)
                return "%" + (u - PyrrhoServer.Preparing);
            if (u >= Transaction.Executables)
                return "@" + (u - Transaction.Executables);
            if (u >= Transaction.Analysing)
                return "#" + (u - Transaction.Analysing);
            if (u >= Transaction.TransPos)
                return "!" + (u - Transaction.TransPos); 
            if (u == -1)
                return "_";
            return "" + u;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(Uid(defpos));
            if (domain is Domain dm && (dm.kind == Sqlx.CONTENT || dm.kind == Sqlx.UNION))
            { sb.Append(" "); sb.Append(dm.kind);  }
            if (mem.Contains(Definer)) { sb.Append(" Definer="); sb.Append(Uid(definer)); }
            if (mem.Contains(Classification)) { sb.Append(" Classification="); sb.Append(classification); }
            if (mem.Contains(LastChange)) { sb.Append(" Ppos="); sb.Append(Uid(lastChange)); }
            if (mem.Contains(TableExpression.Nuid))
            { sb.Append(" Nuid="); sb.Append(Uid((long)mem[TableExpression.Nuid])); }
            if (sensitive) sb.Append(" Sensitive"); 
            return sb.ToString();
        }
    }
}
