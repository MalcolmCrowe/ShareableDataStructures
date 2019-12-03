using System.Collections.Generic;
using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
using Pyrrho.Level4;
using System;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
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
        internal Domain domain => (Domain)mem[_Domain] ?? Domain.Content;
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
        public virtual bool Denied(Transaction tr, Grant.Privilege priv)
        {
            if (tr.user!=null && !(classification == Level.D || tr.user.defpos==tr.owner
                || tr.user.clearance.ClearanceAllows(classification)))
                return true;
            if (defpos > Transaction.TransPos)
                return false;
            var oi = (ObInfo)tr.role.obinfos[defpos];
            return (oi.priv & priv)==0;
        }
        internal abstract DBObject Relocate(long dp);
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var ds = BTree<long, bool>.Empty;
            for (var b = dependents.First(); b != null; b = b.Next())
                ds += (wr.Fix(b.key()), true);
            return (ds==dependents)?r:r + (Dependents, ds);
        }
        internal virtual DBObject Frame(Context cx)
        {
            return cx.Add(this);
        }
        internal virtual Database Add(Database d,Role ro,PMetadata pm, long p)
        {
            return d;
        }
        /// <summary>
        /// Record a need (droppable objects only)
        /// </summary>
        /// <param name="lp"></param>
        /// <returns></returns>
        internal virtual Database Needs(Database db,long lp)
        {
            // only record for droppable object
            return db;
        }
        /// <summary>
        /// Drop anything that needs this, directly or indirectly,
        /// and then drop this.
        /// Called by Drop for Database on Commit and Load
        /// </summary>
        /// <param name="d"></param>
        /// <param name="nd"></param>
        /// <returns></returns>
        internal virtual (Database, Role) Cascade(Database d,Database nd,Role ro,Drop.DropAction a=0,
            BTree<long,TypedValue>u=null)
        {
            for (var b = dependents.First(); b != null; b = b.Next())
                (nd,ro) = ((DBObject)d.objects[b.key()]).Cascade(d, nd,ro,a,u);
            var oi = ro.obinfos[defpos] as ObInfo;
            nd = nd + (nd.role - oi, nd.loadpos);
            return (nd - defpos, ro); 
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
        internal virtual Database Modify(Database db, DBObject now, long p)
        {
            return db;
        }
        internal virtual Database DropConstraint(Check c,Database d,Database nd)
        {
            return nd;
        }
        internal virtual DBObject TableRef(Context cx,From f)
        {
            return this;
        }
        internal virtual DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = this;
            if (defpos<0)
                return this;
            var dm = (Domain)domain.Replace(cx, so, sv);
            if (dm != domain)
                r += (_Domain, dm);
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
        internal void AddIn(Context _cx, RowBookmark rb)
        {
            var aggsDone = BTree<long, bool?>.Empty;
            _AddIn(_cx, rb, ref aggsDone);
        }
        /// <summary>
        /// If the value contains aggregates we need to accumulate them. 
        /// Carefully watch out for common subexpressions, and only AddIn once!
        /// </summary>
        internal virtual void _AddIn(Context _cx, RowBookmark rb, ref BTree<long, bool?> aggsDone) { }

        internal virtual TypedValue Coerce(TypedValue v)
        {
            return v;
        }
        internal virtual TypedValue Coerce(RowBookmark rb)
        {
            return rb.row;
        }
        internal virtual DBObject TypeOf(long lp,Context cx,TypedValue v)
        {
            throw new System.NotImplementedException();
        }
        internal virtual DBObject Path(Ident field)
        {
            throw new System.NotImplementedException();
        }
        internal virtual TypedValue Eval(Transaction tr, Context cx)
        {
            return cx.values[defpos];
        }
        internal virtual TypedValue Eval(Context cx, RowBookmark rb)
        {
            return cx.values[defpos];
        }
        internal virtual bool aggregates()
        {
            return false;
        }
        /// <summary>
        /// If this value contains an aggregator, set the register for it.
        /// If not, return null and the caller will make a Literal.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal virtual SqlValue SetReg(Context _cx, TRow k)
        {
            return null;
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
        /// Check to see if this object references a database object (e.g. user/role) being renamed or dropped
        /// </summary>
        /// <param name="t">The rename or drop transaction</param>
        /// <returns>NO,RESTRICT,DROP</returns>
		public virtual Sqlx Dependent(Transaction t,Context cx)
		{
            return Sqlx.NO;
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
        internal virtual Transaction Insert(Transaction tr,Context _cx, From f, string prov, RowSet data, Adapters eqs, List<RowSet> rs,
            Level cl)
        {
            return tr;
        }
        /// <summary>
        /// Execute a Delete operation for a Table, View, RestView.
        /// The set of rows to delete may be explicitly specified.
        /// Deal with triggers.
        /// </summary>
        /// <param name="f">A query</param>
        /// <param name="dr">A possible explicit set of row references</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual Transaction Delete(Transaction tr,Context cx,From f, BTree<string,bool> dr, Adapters eqs)
        {
            return tr;
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
        internal virtual Transaction Update(Transaction tr,Context cx,From f,BTree<string,bool> ur,Adapters eqs, List<RowSet> rs)
        {
            return tr;
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
        internal virtual bool DoAudit(Transaction tr,long[] cols, string[] key)
        {
            return false;
        }
        internal void Audit(Transaction tr, long[] cols, string[]key)
        {
            if (DoAudit(tr, cols, key))
                tr.Audit(new Audit(tr.user, defpos,
                    cols, key,System.DateTime.Now.Ticks, tr.uid, tr));
        }
        /// <summary>
        /// Issues here: This object may not have been committed yet
        /// We only want to record audits in the PhysBase for committed data
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="m"></param>
        internal void Audit(Transaction tr,Index ix,PRow m)
        {
            var tb = this as Table;
            if (((!sensitive) && (tb?.classification.minLevel??0) == 0) 
                || defpos >= Transaction.TransPos)
                return;
            if ((!sensitive) && 
                tb?.enforcement.HasFlag(Grant.Privilege.Select)!=true)
                return;
            if (definer==tr.role.defpos || definer == tr.user.defpos)
                return;
            var key = new string[m?.Length ?? 0];
            var cols = new long[m?.Length ?? 0];
            for (var i = 0; m != null; m = m._tail, i++)
            {
                cols[i] = ix.keys[i].defpos;
                key[i] = m._head.ToString();
            }
            Audit(tr, cols, key);
        }
        /// <summary>
        /// Issues here: This object may not have been committed yet
        /// We only want to record audits in the PhysBase for committed data
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="m"></param>
        internal void Audit(Transaction tr, Query f)
        {
            var tb = this as Table;
            if (((!sensitive) && (tb?.classification.minLevel??0)==0)
                || defpos >= Transaction.TransPos)
                return;
            if ((!sensitive) &&
                tb?.enforcement.HasFlag(Grant.Privilege.Select)!=true)
                return;
            if (definer == tr.user.defpos)
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
            Audit(tr, cols, key);
        }
        internal static string Uid(long u)
        {
            if (u >= PyrrhoServer.ConnPos)
                return "^" + (u - PyrrhoServer.ConnPos);
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
