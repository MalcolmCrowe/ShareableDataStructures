using System.Collections.Generic;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using System;
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
            Definer = -64, // long
            Dependents = -65, // BTree<long,bool> Non-obvious objects that need this to exist
            Depth = -66, // int  (max depth of dependents)
            Description = -67, // string
            _Domain = -176, // Domain
            Kind = -80, // Sqlx (specified for SqlValue, Domain, otherwise delegated to Domain)
            LastChange = -68, // long (formerly called Ppos)
            Sensitive = -69; // bool
        /// <summary>
        /// During transaction execution, many DBObjects have aliases.
        /// Aliases do not form part of renaming machinery
        /// </summary>
        internal string alias => (string)mem[_Alias];
        /// <summary>
        /// The definer of the object.
        /// </summary>
        public long definer =>(long)(mem[Definer]??-1L);
        public string description => (string)mem[Description] ?? "";
        internal virtual Domain domain => (Domain)mem[_Domain] ?? Domain.Content;
        public virtual Sqlx kind => domain.kind;
        internal long lastChange => (long)(mem[LastChange] ?? defpos);
        /// <summary>
        /// Sensitive if it contains a sensitive type
        /// </summary>
        internal bool sensitive => (bool)(mem[Sensitive]??false);
        internal Level classification => (Level)mem[Classification]??Level.D;
        internal string desc => (string)mem[Description];
        /// <summary>
        /// This list does not include indexes/columns/rows for tables
        /// or other obvious structural dependencies
        /// </summary>
        internal BTree<long,bool> dependents => 
            (BTree<long,bool>)mem[Dependents] ?? BTree<long,bool>.Empty;
        internal int depth => (int)(mem[Depth] ?? 1);
        /// <summary>
        /// Constructor
        /// </summary>
        protected DBObject(long dp, BTree<long,object>m) : base(m)
        {
            defpos = dp;
        }
        protected DBObject(long pp,long dp,long dr,BTree<long,object> m=null) 
            :this(dp,(m??BTree<long,object>.Empty)+(LastChange,pp)+(Definer,dr))
        {}
        protected DBObject(string nm,long pp, long dp, long dr, BTree<long, object> m = null)
            : this(pp,dp,dr,(m??BTree<long,object>.Empty)+(Name,nm))
        {}
        public static DBObject operator+(DBObject ob,(long,object)x)
        {
            return (DBObject)ob.New(ob.mem + x);
        }
        internal static int _Max(params int[] x)
        {
            var r = 0;
            for (var i = 0; i < x.Length; i++)
                if (x[i] > r)
                    r = x[i];
            return r;
        }
        internal static int _Max(BList<SqlValue> x)
        {
            var r = 0;
            for (var b = x.First(); b!=null; b=b.Next())
                if (b.value().depth > r)
                    r = b.value().depth;
            return r;
        }
        internal static BTree<long,bool> _Deps(BList<SqlValue> vs)
        {
            var r = BTree<long,bool>.Empty;
            for (var b = vs?.First(); b != null; b = b.Next())
                r += (b.value().defpos,true);
            return r;
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
            if (tr.user!=null && !(classification == Level.D || tr.user.defpos==tr.owner
                || tr.user.clearance.ClearanceAllows(classification)))
                return true;
            if (defpos > Transaction.TransPos)
                return false;
            var oi = (ObInfo)tr.role.obinfos[defpos];
            return (oi!=null) && (oi.priv & priv)==0;
        }
        internal abstract DBObject Relocate(long dp);
        internal override Basis Relocate(Writer wr)
        {
            var r = (DBObject)base.Relocate(wr);
            var ds = BTree<long, bool>.Empty;
            for (var b = dependents.First(); b != null; b = b.Next())
                ds += (wr.Fix(b.key()), true);
            return (ds==dependents)?r:r + (Dependents, ds);
        }
        internal virtual DBObject Frame(Context cx)
        {
            return cx.Add(this,true);
        }
        internal virtual Database Add(Database d,PMetadata pm, long p)
        {
            return d;
        }
        // Helper for format<51 compatibility
        internal virtual SqlValue ToSql(Ident id,Database db)
        {
            return null;
        }
        // Overridden in Domain and ObInfo
        public virtual TypedValue Parse(Scanner lx,bool union=false)
        {
            return TNull.Value;
        }
        public virtual TypedValue Get(Reader rdr)
        {
            return TNull.Value;
        }
        public virtual void Put(TypedValue tv,Writer wr)
        { }
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
        internal virtual Database Drop(Database d, Database nd,long p)
        {
            var oi = nd.role.obinfos[defpos] as ObInfo;
            nd = nd + (nd.role - oi, p);
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
        internal virtual bool Calls(long defpos,Database db)
        {
            return false;
        }
        internal static bool Calls(BList<SqlValue> vs,long defpos,Database db)
        {
            for (var b = vs?.First(); b != null; b = b.Next())
                if (b.value().Calls(defpos, db))
                    return true;
            return false;
        }
        internal static bool Calls(BList<DBObject> vs, long defpos, Database db)
        {
            for (var b = vs?.First(); b != null; b = b.Next())
                if (b.value().Calls(defpos, db))
                    return true;
            return false;
        }
        internal virtual void Modify(Context cx, DBObject now, long p)
        {
            cx.db += (now, p);
        }
        internal virtual DBObject TableRef(Context cx,From f)
        {
            return this;
        }
        internal virtual DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = this;
            if (defpos<0 || this is Domain)
                return this;
            var dm = (Domain)domain._Replace(cx, so, sv);
            if (dm != domain)
                r += (_Domain, dm);
            return r;
        }
        internal DBObject Replace(Context cx,DBObject was,DBObject now)
        {
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
        internal virtual void Build(Context _cx, RowSet rs)
        {
        }
        /// <summary>
        /// If the value contains aggregates we need to accumulate them
        /// </summary>
        internal virtual void StartCounter(Context _cx, RowSet rs)
        {
        }
        internal void AddIn(Context _cx, Cursor rb,TRow key)
        {
            var aggsDone = BTree<long, bool?>.Empty;
            _AddIn(_cx, rb, key, ref aggsDone);
        }
        internal void AddIn(Context _cx, RTreeBookmark rb)
        {
            var aggsDone = BTree<long, bool?>.Empty;
            _AddIn(_cx, rb, ref aggsDone);
        }
        /// <summary>
        /// If the value contains aggregates we need to accumulate them. 
        /// Carefully watch out for common subexpressions, and only AddIn once!
        /// </summary>
        internal virtual void _AddIn(Context _cx, Cursor rb, TRow key, 
            ref BTree<long, bool?> aggsDone) { }
        internal virtual void _AddIn(Context _cx, RTreeBookmark rb, ref BTree<long, bool?> aggsDone)
        { }
        internal virtual TypedValue Coerce(TypedValue v)
        {
            return v;
        }
        internal virtual TypedValue Coerce(Cursor rb)
        {
            return rb;
        }
        internal virtual DBObject TypeOf(long lp,Context cx,TypedValue v)
        {
            throw new System.NotImplementedException();
        }
        internal virtual TypedValue Eval(Context cx)
        {
            return cx.values[defpos];
        }
        internal virtual bool aggregates()
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
        /// <summary>
        /// Execute an Insert operation for a Table, View, RestView.
        /// The new or existing Rowsets may be explicit or in the physical database.
        /// Deal with triggers.
        /// </summary>
        /// <param name="f">A query</param>
        /// <param name="prov">The provenance string</param>
        /// <param name="data">The data to be inserted may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">The existing RowSet may be explicit</param>
        /// <param name="cl">The classification sought</param>
        internal virtual Context Insert(Context _cx, From f, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            return _cx;
        }
        /// <summary>
        /// Execute a Delete operation for a Table, View, RestView.
        /// The set of rows to delete may be explicitly specified.
        /// Deal with triggers.
        /// </summary>
        /// <param name="f">A query</param>
        /// <param name="dr">A possible explicit set of row references</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual Context Delete(Context cx,From f, BTree<string,bool> dr, Adapters eqs)
        {
            return cx;
        }
        /// <summary>
        /// Execute an Update operation for a Table, View or RestView.
        /// The set of rows to update may be explicitly specified.
        /// The existing rows may be explicitly specified.
        /// </summary>
        /// <param name="f">A query</param>
        /// <param name="ur">The rows to update may be explicit</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        /// <param name="rs">The existing rowset may be explicit</param>
        internal virtual Context Update(Context cx,From f,BTree<string,bool> ur,Adapters eqs, List<RowSet> rs)
        {
            return cx;
        }
        /// <summary>
        /// Implementation of the Role$Class table: Produce a C# class corresponding to a Table or View
        /// </summary>
        /// <param name="from">A query</param>
        /// <param name="_enu">An enumerator for the set of database objects</param>
        /// <returns>A row for the Role$Class table</returns>
        internal virtual TRow RoleClassValue(Transaction tr,From from,
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
        internal virtual TRow RoleJavaValue(Transaction tr, From from, ABookmark<long, object> _enu)
        {
            return null;
        }
        /// <summary>
        /// Implementation of the Role$Python table: Produce a Python class corresponding to a Table or View
        /// </summary>
        /// <param name="from">A query</param>
        /// <param name="_enu">An enumerator for the set of database objects</param>
        /// <returns>A row for the Role$Class table</returns>
        internal virtual TRow RolePythonValue(Transaction tr, From from, ABookmark<long, object> _enu)
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
                    FieldType(db, sb, dt.super); return;
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
                case Sqlx.ROW: sb.Append("[Field(PyrrhoDbType.Row," + 
                    ((ObInfo)db.role.obinfos[dt.elType.defpos]).name+ ")]\r\n"); return;
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
                case Sqlx.ONLY: FieldJava(db, sb, dt.super); return;
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
                case Sqlx.ROW: sb.Append("@FieldType(PyrrhoDbType.Row," 
                    + ((ObInfo)db.role.obinfos[dt.elType.defpos]).name + ")\r\n"); return;
            }
        }
        internal virtual Metadata Meta()
        {
            return null;
        }
        /// <summary>
        /// On reflection: auditing is only relevant for data in base tables!
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        internal virtual bool DoAudit(long pp, Context cx,long[] cols, string[] key)
        {
            return false;
        }
        internal void Audit(long pp, Context cx, long[] cols, string[]key)
        {
            if (DoAudit(pp, cx, cols, key))
            {
                var a = new Audit(cx.tr.user, defpos,
                    cols, key, System.DateTime.Now.Ticks, pp, cx);
                cx.Add(a);
                cx.tr.Audit(a);
            }
        }
        /// <summary>
        /// Issues here: This object may not have been committed yet
        /// We only want to record audits in the PhysBase for committed data
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="m"></param>
        internal void Audit(long pp,Context cx,Index ix,PRow m)
        {
            var tb = this as Table;
            if (((!sensitive) && (tb?.classification.minLevel??0) == 0) 
                || defpos >= Transaction.TransPos)
                return;
            if ((!sensitive) && 
                tb?.enforcement.HasFlag(Grant.Privilege.Select)!=true)
                return;
            if (definer==cx.role.defpos || definer == cx.user.defpos)
                return;
            var key = new string[m?.Length ?? 0];
            var cols = new long[m?.Length ?? 0];
            for (var i = 0; m != null; m = m._tail, i++)
            {
                cols[i] = ix.keys[i].defpos;
                key[i] = m._head.ToString();
            }
            Audit(pp, cx, cols, key);
        }
        /// <summary>
        /// Issues here: This object may not have been committed yet
        /// We only want to record audits in the PhysBase for committed data
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="m"></param>
        internal void Audit(long pp, Context cx, Query f)
        {
            if (cx.tr == null)
                return;
            var tb = this as Table;
            if (((!sensitive) && (tb?.classification.minLevel??0)==0)
                || defpos >= Transaction.TransPos)
                return;
            if ((!sensitive) &&
                tb?.enforcement.HasFlag(Grant.Privilege.Select)!=true)
                return;
            if (definer == cx.user.defpos)
                return;
            var m = f.matches?.Count ?? 0;
            var cols = new long[m];
            var key = new string[m];
            var i = 0;
            for (var b = f.matches?.First(); b != null; b = b.Next(),i++)
            {
                cols[i] = b.key().defpos;
                key[i] = b.value()?.ToString()??"null";
            }
            Audit(pp, cx, cols, key);
        }
        internal virtual long Defpos()
        {
            return defpos;
        }
        internal static string Uid(long u)
        {
            if (u >= PyrrhoServer.Preparing)
                return "%" + (u - PyrrhoServer.Preparing);
            if (u >= Transaction.Compiling)
                return "@" + (u - Transaction.Compiling);
            if (u >= Transaction.Analysing)
                return "#" + (u - Transaction.Analysing);
            if (u >= Transaction.TransPos)
                return "'" + (u - Transaction.TransPos); 
            if (u == -1)
                return "_";
            return "" + u;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' '); sb.Append(Uid(defpos));
            if (mem.Contains(Definer)) { sb.Append(" Definer="); sb.Append(Uid(definer)); }
            if (mem.Contains(Classification)) { sb.Append(" Classification="); sb.Append(classification); }
            if (mem.Contains(LastChange)) { sb.Append(" Ppos="); sb.Append(Uid(lastChange)); }
            if (mem.Contains(Sensitive)) sb.Append(" Sensitive"); 
            return sb.ToString();
        }
    }
}
