using System.Collections.Generic;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using System;
using System.Configuration;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.CompilerServices;
using System.Data.Common;
using System.Xml;
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
    /// DBObject is the base class for Level 3 database objects (e.g. Table, Role, Procedure, Domain)
    /// Immutable
    /// 
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
            Definer = -64, // long Role
            Defpos = -257, // long for Rest service
            Dependents = -65, // CTree<long,bool> (for Drop and Cascades)
            _Depth = -66,  // int (computed on cx.Add())
            _Domain = -192, // Domain 
            _Framing = -167, // Framing
            _From = -306, // long From
            _Ident = -409, // Ident (used in ForwardReference and RowSet)
            Infos = -126, // BTree<long,ObInfo> Role
            LastChange = -68, // long (formerly called Ppos)
            Owner = -59, // long
            Sensitive = -69; // bool
        /// <summary>
        /// During transaction execution, many DBObjects have aliases.
        /// Aliases do not form part of renaming machinery
        /// </summary>
        internal string? alias => (string?)mem[_Alias];
   //     internal Domain dataType => (Domain)mem[_DataType];
        /// <summary>
        /// The definer of the object (a Role)
        /// </summary>
        public long definer => (long)(mem[Definer] ?? -1L);
        public long owner => (long)(mem[Owner] ?? -1L);
        //        internal Context compareContext => 
        internal long lastChange => (long)(mem[LastChange] ?? 0L);// compareContext?.db.loadpos ?? 0L;
        /// <summary>
        /// Sensitive if it contains a sensitive type
        /// </summary>
        internal bool sensitive => (bool)(mem[Sensitive] ?? false);
        internal Level classification => (Level?)mem[Classification] ?? Level.D;
        internal BTree<long, ObInfo> infos =>
    (BTree<long, ObInfo>?)mem[Infos] ?? BTree<long, ObInfo>.Empty;
        internal long from => (long)(mem[_From] ?? -1L);
        internal string name => (string)(mem[ObInfo.Name] ?? "");
        public virtual Domain domain => (Domain)(mem[_Domain] ?? Domain.Null);
        /// <summary>
        /// This tree does not include indexes/columns/rows for tables
        /// or other obvious structural dependencies
        /// </summary>
        internal CTree<long, bool> dependents =>
            (CTree<long, bool>?)mem[Dependents] ?? CTree<long, bool>.Empty;
        internal int depth => (int)(mem[_Depth] ?? 1);
        /// <summary>
        /// Show compiled code - triggers and Procedures
        /// </summary>
        internal Framing framing =>
            (Framing?)mem[_Framing] ?? Framing.Empty;
        internal Ident? id => (Ident?)mem[_Ident];
        /// <summary>
        /// Constructor
        /// </summary>
        protected DBObject(long dp, BTree<long, object> m) : base(m)
        {
            defpos = dp;
        }
        protected DBObject(long pp, long dp, BTree<long, object>? m = null)
            : this(dp, (m ?? BTree<long, object>.Empty) + (LastChange, pp))
        { }
        internal abstract DBObject New(long dp, BTree<long, object> m);
        internal override Basis New(BTree<long, object> m)
        {
            return New(defpos,m);
        }
        public static DBObject operator+(DBObject ob,(long,object)x)
        {
            return ob.New(ob.defpos, ob.mem + x);
        }
        internal virtual DBObject Relocate(long dp,Context cx)
        {
            return New(dp, _Fix(cx,mem));
        }
        internal virtual DBObject Replace(Context cx,DBObject was,DBObject now)
        {
            if (cx.done[defpos] is DBObject rr)
                return rr;
            var ldpos = cx.db.loadpos;
            for (var cc = cx.next; cc != null && cc.db != null; cc = cc.next)
                ldpos = cc.db.loadpos;
            if (defpos < ldpos)
                return this;
            var r = _Replace(cx, was, now);
            cx.done += (defpos, r);
            return r;
        }
        protected virtual DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            return this;
        }
        internal virtual (DBObject?, Ident?) _Lookup(long lp, Context cx, string nm, Ident? n, DBObject? r)
        {
            return (this, n);
        }
        internal virtual long ColFor(Context cx, string c)
        {
            for (var b = domain.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject ob &&
                        ((p >= Transaction.TransPos && ob is SqlValue sv
                            && (sv.alias ?? sv.name) == c)
                        || ob.NameFor(cx) == c))
                    return p;
            return -1L;
        }
        /// <summary>
        /// Do not use this function in a constructor of a subclass of SqlValue or RowSet
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="vs"></param>
        /// <param name="ls"></param>
        /// <returns></returns>
        internal static BTree<long, object> _Deps(Context cx,BList<DBObject> vs,BList<long?> ls)
        {
            var d = 1;
            var r = BTree<long,object>.Empty;
            var os = CTree<long,bool>.Empty;
            for (var b=ls.First();b is not null;b=b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject ob)
                {
                    os += (ob.defpos, true);
                    d = Math.Max(ob.depth + 1, d);
                }
            for (var b = vs.First(); b != null; b = b.Next())
            {
                os += (b.value().defpos, true);
                d = Math.Max(b.value().depth + 1, d);
            }
            return r + (Dependents, os) + (_Depth, d);
        }
        /// <summary>
        /// Do not use this function in a constructor of a subclass of SqlValue or RowSet
        /// </summary>
        /// <param name="vs"></param>
        /// <returns></returns>
        internal static BTree<long,object> _Deps(Context cx,BList<long?> ls, params DBObject?[] vs)
        {
            var d = 1;
            var r = BTree<long, object>.Empty;
            var os = CTree<long, bool>.Empty;
            for (var b = ls.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is DBObject ob)
                {
                    os += (ob.defpos, true);
                    d = Math.Max(ob.depth + 1, d);
                }
            foreach (var o in vs)
                if (o != null)
                {
                    r += (o.defpos, true);
                    d = Math.Max(o.depth + 1, d);
                }
            return r + (Dependents, os)+(_Depth,d);
        }
        /// <summary>
        /// Check to see if the current role has the given privilege on this (except Admin).
        /// Object owners (users) implicitly have all privileges on their objects.
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
            if (tr.user?.defpos == owner)
                return false;
            var oi = infos[tr.role.defpos];
            return (oi != null) && (oi.priv & priv) == 0;
        }
        internal virtual CTree<long, bool> Needs(Context cx)
        {
            return CTree<long, bool>.Empty;
        }
        internal virtual CTree<long, bool> _Rdc(Context cx)
        {
            return CTree<long,bool>.Empty;
        }
        internal virtual CTree<long, bool> Needs(Context context, long rs)
        {
            return CTree<long, bool>.Empty;
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
        internal DBObject Relocate(long dp)
        {
            return (dp==defpos)?this:New(dp, mem);
        }
        /// <summary>
        /// Adjust compiled DBObject (calls _Relocate)
        /// </summary>
        /// <param name="wr"></param>
        /// <returns></returns>
        internal DBObject Relocate(Context cx)
        {
            if (defpos < 0)
            {
                var dm = this as Domain ?? throw new PEException("PE688");
                return (Domain)New(dm._Fix(cx,mem));
            }
            if (cx.done[defpos] is DBObject rr)
                return rr;
            var r = (DBObject)New(_Fix(cx,mem));
            cx.Add(r);
            cx.done += (defpos, r);
            return r;
        }
        internal virtual int Depth(Context cx)
        {
            return 1;
        }
        internal override Basis Fix(Context cx)
        {
            if (defpos < 0)
                return this;
            var r = New(cx.Fix(defpos),_Fix(cx,mem));
            if (defpos!=-1L)
                cx.Add(r);
            return r;
        }
        internal virtual DBObject AddFrom(Context cx, long q)
        {
            return cx.Add(this + (_From, q));
        }
        internal CTree<long, TypedValue> Frame(CTree<long, TypedValue> vs)
        {
            var map = BTree<long, long?>.Empty;
            for (var b = framing.obs.First(); b != null; b = b.Next())
                if (b.value() is SqlCopy sc)
                    map += (sc.copyFrom, sc.defpos);
            var r = CTree<long, TypedValue>.Empty;
            for (var b = vs.First(); b != null; b = b.Next())
                if (map[b.key()] is long s && b.value() is TypedValue v)
                    r += (s, v);
            return r;
        }
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            var r = this;
            var fs = ShallowReplace(cx, infos, was, defpos);
            if (fs != infos)
                r += (Infos, fs);
            return r;
        }
        BTree<long,ObInfo> ShallowReplace(Context cx,BTree<long,ObInfo> fs,long was,long now)
        {
            for (var b=fs.First();b!=null;b=b.Next())
                if (b.value() is ObInfo oi)
                {
                    var ni = (ObInfo)oi.ShallowReplace(cx, was, now);
                    if (ni != oi)
                        fs += (b.key(), ni);
                }
            return fs;
        }
        /// <summary>
        /// Fix does the work of relocation for sharing - see Compiled.Relocate(cx)
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object> m)
        {
            var r = mem;
            var fm = cx.Fix(from);
            if (fm != from)
                r += (_From, fm);
            var nd = cx.Fix(definer);
            if (definer != nd)
                r += (Definer, nd);
            var ds = cx.FixTlb(dependents);
            if (ds != dependents)
                r += (Dependents, ds);
            var ni = cx.Fix(infos);
            if (ni != infos)
                r += (Infos, ni);
            return r;
        }
        /// <summary>
        /// Some DBObjects are modified when metadata is defined
        /// </summary>
        /// <param name="d"></param>
        /// <param name="pm"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        internal virtual DBObject Add(Context cx,PMetadata pm, long p)
        {
            var m = mem;
            if (infos[cx.role.defpos] is ObInfo oi)
                m += (Infos, infos + (cx.role.defpos, oi+ (ObInfo._Metadata, pm.detail)));
            m += (LastChange, pm.ppos);
            var r = New(defpos, m);
            cx.db += (r,p);
            return cx.Add(r);
        }
        internal virtual BTree<long,SystemFilter> SysFilter(Context cx,BTree<long,SystemFilter> sf)
        {
            return sf;
        }
        internal virtual CTree<long,bool> Operands(Context cx)
        {
            return CTree<long, bool>.Empty;
        }
        internal virtual CTree<long, bool> ExposedOperands(Context cx,CTree<long,bool> ag,Domain? gc)
        {
            var os = Operands(cx) - ag;
            if (gc is not null)
                for (var b = os.First(); b != null; b = b.Next())
                    if (gc.representation.Contains(b.key()))
                        os -= b.key();
            return os;
        }
        internal virtual DBObject AddTrigger(Trigger tg)
        {
            return this;
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
            BTree<long,TypedValue>?u=null)
        {
            if (cx.db is not Transaction tr)
                return;
            for (var b = tr.physicals.First(); b != null; b = b.Next())
                if (b.value() is Drop dr && dr.delpos == defpos)
                    return;
            for (var b = dependents?.First(); b != null; b = b.Next())
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
            cx.Add(new Drop1(defpos, a, cx.tr?.nextPos??0));
        }
        /// <summary>
        /// Execute an Insert operation for a Table, View, RestView.
        /// The new or existing Rowsets may be explicit or in the physical database.
        /// Deal with triggers.
        /// </summary>
        /// <param name="f">A query</param>
        /// <param name="cl">The classification sought</param>
        internal virtual BTree<long, TargetActivation> Insert(Context cx, RowSet ts, Domain rt)
        {
            return BTree<long, TargetActivation>.Empty;
        }
        internal virtual BTree<long, TargetActivation> Delete(Context cx,RowSet fm)
        {
            return BTree<long, TargetActivation>.Empty;
        }
        internal virtual BTree<long, TargetActivation> Update(Context cx, RowSet fm)
        {
            return BTree<long, TargetActivation>.Empty;
        }
        internal virtual Database Drop(Database d, Database nd,long p)
        {
            return nd - defpos;
        }
        internal virtual Database DropCheck(long ck,Database nd,long p)
        {
            throw new NotImplementedException();
        }
        internal virtual TypedValue _Default()
        {
            return TNull.Value;
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
            var mt = (Method?)cx.obs[m.proc] ?? throw new PEException("PE1006");
            cx.db += (mt, p);
        }
        internal virtual (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            throw new NotImplementedException();
        }
        internal virtual void _Add(Context cx)
        {
            cx.obs += (defpos, this); // don't do this anywhere else except Fix/Relocate, use cx.Add(ob)
        }
        internal virtual void _ReadConstraint(Context cx,TableRowSet.TableCursor cu)
        { }
        internal virtual DBObject Orders(Context cx, Domain ord)
        {
            return this;
        }
        public static bool Eval(CTree<long, bool> svs, Context cx)
        {
            for (var b = svs?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()]?.Eval(cx) != TBool.True)
                    return false;
            return true;
        }
        internal virtual void Set(Context cx, TypedValue v)
        {
            cx.values += (defpos, v);
        }
        /// <summary>
        /// Replace TypedValues that are QParams with actuals
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal virtual DBObject QParams(Context cx)
        {
            return this;
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
            throw new NotImplementedException();
        }
        internal TypedValue Eval(Context cx)
        {
            if (cx.obs[from] is RowSet r && r.ambient.Contains(defpos) &&
                cx.values[defpos] is TypedValue tv && tv != TNull.Value)
                return tv;
            if (cx.binding[defpos] is TypedValue tb)
                return tb;
            return _Eval(cx);
        }
        internal virtual TypedValue _Eval(Context cx)
        {
            return cx.values[defpos]??TNull.Value;
        }
        internal virtual CTree<long,TypedValue> Add(Context cx,CTree<long,TypedValue> ma,
            Table? tb=null)
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
        internal virtual RowSet RowSets(Ident id,Context cx, Domain q, long fm, 
            Grant.Privilege pr=Grant.Privilege.Select,string? a=null)
        {
            return new TrivialRowSet(id.iix.dp, cx, new TRow(q, cx.values),a);
        }
        /// <summary>
        /// Creates new instances of objects in framing lists.
        /// Label instances of framing objects with instanceOf (see Context.Add) 
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal virtual DBObject Instance(long lp,Context cx,BList<Ident>?cs=null)
        {
            cx.Add(framing);
            return this;
        }
        /// <summary>
        /// Implementation of the Role$Class table: Produce a C# class corresponding to a Table or View
        /// </summary>
        /// <param name="from">A query</param>
        /// <param name="_enu">An enumerator for the set of database objects</param>
        /// <returns>A row for the Role$Class table</returns>
        internal virtual TRow RoleClassValue(Context cx,RowSet from,
            ABookmark<long, object> _enu)
        {
            throw new NotImplementedException();
        } 
        /// <summary>
        /// Implementation of the Role$Java table: Produce a Java class corresponding to a Table or View
        /// </summary>
        /// <param name="from">A query</param>
        /// <param name="_enu">An enumerator for the set of database objects</param>
        /// <returns>A row for the Role$Class table</returns>
        internal virtual TRow RoleJavaValue(Context cx, RowSet from, ABookmark<long, object> _enu)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// Implementation of the Role$Python table: Produce a Python class corresponding to a Table or View
        /// </summary>
        /// <param name="from">A query</param>
        /// <param name="_enu">An enumerator for the set of database objects</param>
        /// <returns>A row for the Role$Class table</returns>
        internal virtual TRow RolePythonValue(Context cx, RowSet from, ABookmark<long, object> _enu)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// Implementation of the Role$SQL table: Produce SQL for a RestView corresponding to a Table or similar
        /// </summary>
        /// <param name="from">A query</param>
        /// <param name="_enu">An enumerator for the set of database objects</param>
        /// <returns>A row for the Role$Class table</returns>
        internal virtual TRow RoleSQLValue(Context cx, RowSet from, ABookmark<long, object> _enu)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// Issues here: This object may not have been committed yet
        /// We only want to record audits in the PhysBase for committed obs
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="m"></param>
        internal void Audit(Context cx, RowSet rs)
        {
            if (cx.tr == null || cx.db.user==null || cx.db.user.defpos == cx.db.owner)
                return;
            var tb = this as Table;
            if (defpos >= Transaction.TransPos)
                return;
            var mac = (tb?.classification.maxLevel ?? 0) > 0&&
                tb?.enforcement.HasFlag(Grant.Privilege.Select) ==true
                && cx.user is not null && cx.user.defpos !=cx.db.owner;
            if (!(mac || sensitive))
                return;
            if (!sensitive)
            {
                var found = false;
                for (var b = rs.First(cx); (!found) && b != null; b = b.Next(cx))
                    if (b[Classification]is TLevel lv && lv.val is Level vl 
                        && vl.maxLevel > 0)
                        found = true;
                if (!found)
                    return;
            }
            var match = CTree<long, string>.Empty;
            for (var b = rs.matches?.First(); b != null; b = b.Next())
                match += (b.key(), b.value()?.ToString() ?? "null");
            var a = new Audit(cx.db.user, defpos, match, DateTime.Now.Ticks, cx.db.nextPos);
            if (cx.auds.Contains(a))
                return;
            cx.auds += (a, true);
            cx.tr.Audit(a, cx); // write it to the file immediately
        }
        internal static string Uid(long u)
        {
            if (u >= Transaction.HeapStart)
                return "%" + (u - Transaction.HeapStart);
            if (u >= Transaction.Executables)
                return "`" + (u - Transaction.Executables);
            if (u >= Transaction.Analysing)
                return "#" + (u - Transaction.Analysing);
            if (u >= Transaction.TransPos)
                return "!" + (u - Transaction.TransPos); 
            if (u == -1)
                return "_";
            return "" + u;
        }
        internal virtual string NameFor(Context cx)
        {
            if (cx.db.objects[defpos] is not DBObject ob || ob.dbg < dbg)
                ob = this;
            if (ob.alias is string s)
                return s;
            var ci = ob.infos[cx.role.defpos] ?? ob.infos[definer] ??
                ob.infos[Database.Guest] ?? throw new DBException("42105");
            return ci.name??"??";
        }
        internal virtual void Note(Context cx,StringBuilder sb)
        {  }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (id != null)
            {
                sb.Append(' '); sb.Append(id);
            }
            sb.Append(' '); sb.Append(Uid(defpos));
            if (mem.Contains(Definer)) { sb.Append(" Definer="); sb.Append(Uid(definer)); }
            if (mem.Contains(Classification)) { sb.Append(" Classification="); sb.Append(classification); }
            if (mem.Contains(LastChange)) { sb.Append(" LastChange="); sb.Append(Uid(lastChange)); }
            if (sensitive) sb.Append(" Sensitive"); 
            return sb.ToString();
        }
    }
    internal class ObTree: BTree<long, DBObject>
    {
        public new readonly static ObTree Empty = new ();
        internal ObTree() : base(null) { }
        internal ObTree(long k, DBObject v) : base(k, v) { }
        internal ObTree(Bucket<long, DBObject> b) : base(b) { }
        public static ObTree operator +(ObTree tree, (long, DBObject) v)
        {
            return (ObTree)tree.Add(v.Item1, v.Item2);
        }
        public static ObTree operator +(ObTree tree, BTree<long, DBObject> a)
        {
            return (ObTree) tree.Add(a);
        }
        public static ObTree operator -(ObTree tree, long k)
        {
            return (ObTree)tree.Remove(k);
        }
        protected override ATree<long, DBObject> Add(long k, DBObject v)
        {
            if (root is not null && Contains(k))
                return new ObTree(root.Update(this, k, v));
            return Insert(k, v);
        }
        public override ATree<long, DBObject> Add(ATree<long, DBObject> a)
        {
            var tree = this;
            for (var b = a?.First(); b != null; b = b.Next())
                tree = (ObTree)tree.Add(b.key(), b.value());
            return tree;
        }
        protected override ATree<long, DBObject> Insert(long k, DBObject v) // this does not contain k
        {
            if (root == null || root.total == 0)  // empty BTree
                return new ObTree(k, v);
            if (root.count == Size)
                return new ObTree(root.Split()).Add(k, v);
            return new ObTree(root.Add(this, k, v));
        }
        internal override ATree<long, DBObject> Update(long k, DBObject v) // this Contains k
        {
            if (root==null || !Contains(k))
                throw new Exception("PE01");
            return new ObTree(root.Update(this, k, v));
        }

        internal override ATree<long, DBObject> Remove(long k)
        {
            if (root==null || !Contains(k))
                return this;
            var b = root.Remove(this, k);
            return (b==null)?Empty:new ObTree(b);
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            var cm = "(";
            for (var b = First(); b != null; b = b.Next())
            {
                var k = b.key();
                var v = b.value();
                if (k>=0 )//&& (k < Transaction.Analysing) || v.GetType() != typeof(Domain))
                {
                    sb.Append(cm); cm = ",\n  ";
                    sb.Append(DBObject.Uid(k)); sb.Append('=');
                    sb.Append(v);
                }
            }
            if (cm != "(")
                sb.Append(')');
            return sb.ToString();
        }
    }
    internal class ForwardReference : Domain
    {
        internal const long
            Subs = -382; // CTree<long,bool>
        internal CTree<long,bool> subs => (CTree<long, bool>?)mem[Subs]??CTree<long, bool>.Empty;
        /// <summary>
        /// A forward reference non-terminal.
        /// Non-terminal, will always have suggested columns
        /// </summary>
        /// <param name="nm">Name</param>
        /// <param name="cx">The context</param>
        /// <param name="lp">Lexical position: stored in LastChange</param>
        /// <param name="dr">Definer</param>
        /// <param name="m">Other properties, e.g. domain, depth</param>
        public ForwardReference(Ident n, Context cx) 
            : base(n.iix.lp, n.iix.dp, cx.Name(n))
        {
            cx.Add(this);
            cx.undefined += (defpos,cx.sD);
        }
        protected ForwardReference(long dp, BTree<long, object> m)
            : base(dp, m)
        { }
        internal override Basis New(BTree<long, object> m)
        {
            return new ForwardReference(defpos,m);
        }
        public static ForwardReference operator+(ForwardReference fr,(long,object)x)
        {
            var (dp, ob) = x;
            if (fr.mem[dp] == ob)
                return fr;
            return (ForwardReference)fr.New(fr.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new ForwardReference(dp,m);
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var rs = cx.obs[f] as RowSet;
            for (var b = rs?.rowType.First(); b != null; b = b.Next())
                if (cx.obs[b.value() ?? -1L] is SqlValue sv && sv.name == name
                    && cx.defs[name] is BTree<int, (Iix, Ident.Idents)> t)
                {
                    var si = sv.domain.infos[cx.role.defpos];
                    var (ix, ds) = t[cx.sD - 1];
                    cx.undefined -= defpos;
                    cx.defs += (name, new Iix(ix, sv.defpos), ds);
                    for (var c = ds.First(); c != null; c = c.Next())
                        if (cx.obs[c.value()[cx.sD - 1].Item1.dp] is SqlValue sc && sc.name == c.key())
                        {
                            cx.undefined -= sc.defpos;
                            if (si is not null)
                                sc = new SqlCopy(sc.defpos, cx, sc.name, defpos,
                                    cx.db.objects[si.names[c.key()].Item2 ?? -1L] as DBObject);
                            else
                                sc = new SqlField(sc.defpos, sc.name, -1, defpos, Content, defpos);
                            sv = new SqlValueExpr(defpos, cx, Sqlx.DOT, sv, sc, Sqlx.NO);
                            cx.Add(sv);
                            cx.Replace(sc, sv);
                            cx.Add(sc);
                            break;
                        }
                }
            return (BList<DBObject>.Empty, m);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (subs!=CTree<long,bool>.Empty) 
            {
                sb.Append(" Subs: ");sb.Append(subs.ToString());
            }
            return sb.ToString();
        }
    }
}
