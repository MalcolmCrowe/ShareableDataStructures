using System;
using System.Text;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
    internal class SystemTable : Table
    {
        internal SystemTable(string n)
            : base(--_uid,new BTree<long,object>(Name,n)+(SqlValue.NominalType, Domain.TableType))
        {
            Database._system += (Database._system.schemaRole,this);
        }
        protected SystemTable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SystemTable operator+(SystemTable s,(long,object)x)
        {
            return new SystemTable(s.defpos, s.mem + x);
        }
        public static SystemTable operator +(SystemTable tb, TableColumn tc)
        {
            return (SystemTable)tb.New(tb.mem + (SqlValue.NominalType, tb.rowType + tc));
        }
        /// <summary>
        /// Accessor: Check object permissions
        /// </summary>
        /// <param name="db">The Database</param>
        /// <param name="priv">The privilege being checked for</param>
        /// <returns>the Role</returns>
        public override bool Denied(Transaction tr,Grant.Privilege priv)
        {
            if (priv != Grant.Privilege.Select)
                return true;
            // Sys$ tables are public
            if (name.StartsWith("Sys$") || name.StartsWith("Role$"))
                return false;
            // Log$ ROWS etc tables are private to the database owner
            if (tr.user.defpos == tr.owner)
                return false;
            return base.Denied(tr, priv);
        }
        public Database Add(Database d)
        {
            d += (d.schemaRole, this);
            for (var b = rowType.columns.First(); b != null; b = b.Next())
                d += (d.schemaRole, b.value());
            return d;
        }
        internal override RowSet RowSets(Transaction tr, Context cx)
        {
            return new SystemRowSet(tr,cx,this);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SystemTable(defpos,m);
        }
    }
    internal class SystemTableColumn : TableColumn
    {
        public bool DBNeeded = true;
        /// <summary>
        /// A table column for a System or Log table
        /// </summary>
        /// <param name="sd">the database</param>
        /// <param name="n"></param>
        /// <param name="sq">the ordinal position</param>
        /// <param name="t">the dataType</param>
        internal SystemTableColumn(Table t, string n, int sq, Domain dt)
            : base(--_uid, BTree<long, object>.Empty + (Name, n) + (Table, t) + (NominalType, dt)
                  + (Seq, sq))
        { }
        protected SystemTableColumn(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SystemTableColumn operator+(SystemTableColumn s,(long,object) x)
        {
            return new SystemTableColumn(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Accessor: Check object permissions
        /// </summary>
        /// <param name="db">The Database</param>
        /// <param name="priv">The privilege being checked for</param>
        /// <returns>the Role</returns>
        public override bool Denied(Transaction tr, Grant.Privilege priv)
        {
            if (!DBNeeded)
                return false;
            if (priv != Grant.Privilege.Select || tr==null)
                return true;
            // Sys$ tables are public
            if (name.StartsWith("Sys$") || name.StartsWith("Role$"))
                return false;
            // Log$ ROWS etc tables are private to the database owner
            if (tr.user.defpos == tr.owner)
                return false;
            return base.Denied(tr, priv);
        }
    }
	/// <summary>
	/// Perform selects from virtual 'system tables'
	/// </summary>
    internal class SystemRowSet : RowSet
    {
        public override string keywd()
        {
            return " System ";
        }
        protected Table from;
        /// <summary>
        /// Construct results for a system table
        /// </summary>
        /// <param name="f">the from part</param>
        internal SystemRowSet(Transaction tr,Context cx,Table f)
            : base(tr,new Context(cx),f,f.rowType)
        {
            from = f;
        }
        internal override void _Strategy(StringBuilder sb, int indent)
        {
            sb.Append(from.name);
            base._Strategy(sb, indent);
        }
        /// <summary>
        /// Kludge: force class initialisation
        /// </summary>
        /// <param name="o"></param>
        internal static void Kludge()
        {
        }
        /// <summary>
        /// Class initialisation: define the system tables
        /// </summary>
        static SystemRowSet()
        {
            var d = Database._system;
            // level 2 stuff
            d = LogResults(d);
            d = LogAlterResults(d);
            d = LogChangeResults(d);
            d = LogDeleteResults(d);
            d = LogDropResults(d);
            d = LogModifyResults(d);
            d = LogCheckResults(d);
            d = LogClassificationResults(d);
            d = LogClearanceResults(d);
            d = LogColumnResults(d);
            d = LogDateTypeResults(d);
            d = LogDomainResults(d);
            d = LogEditResults(d);
            d = LogEnforcementResults(d);
            d = LogGrantResults(d);
            d = LogIndexResults(d);
            d = LogIndexKeyResults(d);
            d = LogMetadataResults(d);
            d = LogOrderingResults(d);
            d = LogProcedureResults(d);
            d = LogRevokeResults(d);
            d = LogTableResults(d);
            d = LogTablePeriodResults(d);
            d = LogTriggerResults(d);
            d = LogTriggerUpdateColumnResults(d);
            d = LogTriggeredActionResults(d);
            d = LogTypeResults(d);
            d = LogTypeMethodResults(d);
            //          LogTypeUnionResults();
            d = LogViewResults(d);
            d = LogRecordResults(d);
            d = LogRecordFieldResults(d);
            d = LogTransactionResults(d);
            d = LogUpdateResults(d);
            d = LogUserResults(d);
            d = LogRoleResults(d);
            // level 3 stuff
            d = RoleClassResults(d);
            d = RoleColumnResults(d);
            d = RoleColumnCheckResults(d);
            d = RoleColumnPrivilegeResults(d);
            d = RoleDomainResults(d);
            d = RoleDomainCheckResults(d);
            d = RoleIndexResults(d);
            d = RoleIndexKeyResults(d);
            d = RoleJavaResults(d);
            d = RoleObjectResults(d);
            d = RoleParameterResults(d);
            d = RoleProcedureResults(d);
            d = RolePythonResults(d);
            d = RoleSubobjectResults(d);
            d = RoleTableResults(d);
            d = RolePrivilegeResults(d);
            d = RoleTableCheckResults(d);
            d = RoleTablePeriodResults(d);
            d = RoleTriggerResults(d);
            d = RoleTriggerUpdateColumnResults(d);
            d = RoleTypeResults(d);
            d = RoleMethodResults(d);
            d = SysRoleResults(d);
            d = SysUserResults(d);
            d = SysRoleUserResults(d);
            d = RoleViewResults(d);
            // level >=4 stuff
            d = SysClassificationResults(d);
            d = SysClassifiedColumnDataResults(d);
            d = SysEnforcementResults(d);
            d = SysAuditResults(d);
            d = SysAuditKeyResults(d);
            d = RolePrimaryKeyResults(d);
#if PROFILES
            // Profile tables
            ProfileResults();
            ProfileReadConstraintResults();
            ProfileRecordResults();
            ProfileRecordColumnResults();
            ProfileTableResults();
#endif
            Database._system = d;
        }
        /// <summary>
        /// Create the required bookmark
        /// </summary>
        /// <param name="r">the rowset to enumerate</param>
        /// <returns>the bookmark</returns>
        protected override RowBookmark _First(Context _cx)
        {
            var res = this;
            if (res.from == null) // for kludge
                return null;
            SystemTable st = res.from as SystemTable;
            switch (st.name)
            {
                // level 2 stuff
                case "Log$": return LogBookmark.New(_cx,res);
                case "Log$Alter": return LogAlterBookmark.New(_cx,res);
                case "Log$Change": return LogChangeBookmark.New(_cx, res);
                case "Log$DateType": return LogDateTypeBookmark.New(_cx, res);
                case "Log$Delete": return LogDeleteBookmark.New(_cx, res);
                case "Log$Drop": return LogDropBookmark.New(_cx, res);
                case "Log$Check": return LogCheckBookmark.New(_cx, res);
                case "Log$Classification": return LogClassificationBookmark.New(_cx, res);
                case "Log$Clearance": return LogClearanceBookmark.New(_cx, res);
                case "Log$Column": return LogColumnBookmark.New(_cx, res);
                case "Log$Domain": return LogDomainBookmark.New(_cx, res);
                case "Log$Edit": return LogEditBookmark.New(_cx, res);
                case "Log$Enforcement": return LogEnforcementBookmark.New(_cx, res);
                case "Log$Grant": return LogGrantBookmark.New(_cx, res);
                case "Log$Index": return LogIndexBookmark.New(_cx, res);
                case "Log$IndexKey": return LogIndexKeyBookmark.New(_cx, res);
                case "Log$Metadata": return LogMetadataBookmark.New(_cx, res);
                case "Log$Modify": return LogModifyBookmark.New(_cx, res);
                case "Log$Ordering": return LogOrderingBookmark.New(_cx, res);
                case "Log$Procedure": return LogProcedureBookmark.New(_cx, res);
                case "Log$Table": return LogTableBookmark.New(_cx, res);
                case "Log$TablePeriod": return LogTablePeriodBookmark.New(_cx, res);
                case "Log$Trigger": return LogTriggerBookmark.New(_cx, res);
                case "Log$TriggerUpdateColumn": return LogTriggerUpdateColumnBookmark.New(_cx, res);
                case "Log$TriggeredAction": return LogTriggeredActionBookmark.New(_cx, res);
                case "Log$Type": return LogTypeBookmark.New(_cx, res);
                case "Log$TypeMethod": return LogTypeMethodBookmark.New(_cx, res);
                //            case "Log$TypeUnion": return new LogTypeUnionEnumerator(r,matches);
                case "Log$View": return LogViewBookmark.New(_cx, res);
                case "Log$Insert": return LogRecordBookmark.New(_cx, res);
                case "Log$InsertField": return LogRecordFieldBookmark.New(_cx, res);
                case "Log$Revoke": return LogRevokeBookmark.New(_cx, res);
                case "Log$Transaction": return LogTransactionBookmark.New(_cx, res);
                case "Log$Update": return LogUpdateBookmark.New(_cx, res);
                case "Log$User": return LogUserBookmark.New(_cx, res);
                case "Log$Role": return LogRoleBookmark.New(_cx, res);
                // level 3 stuff
                case "Role$Class": return RoleClassBookmark.New(_cx, res);
                case "Role$Column": return RoleColumnBookmark.New(_cx, res);
                case "Role$ColumnCheck": return RoleColumnCheckBookmark.New(_cx, res);
                case "Role$ColumnPrivilege": return RoleColumnPrivilegeBookmark.New(_cx, res);
                case "Role$Domain": return RoleDomainBookmark.New(_cx, res);
                case "Role$DomainCheck": return RoleDomainCheckBookmark.New(_cx, res);
                case "Role$Index": return RoleIndexBookmark.New(_cx, res);
                case "Role$IndexKey": return RoleIndexKeyBookmark.New(_cx, res);
                case "Role$Java": return RoleJavaBookmark.New(_cx, res);
                case "Role$Method": return RoleMethodBookmark.New(_cx, res);
                case "Role$Object": return RoleObjectBookmark.New(_cx, res);
                case "Role$Parameter": return RoleParameterBookmark.New(_cx, res);
                case "Role$Privilege": return RolePrivilegeBookmark.New(_cx, res);
                case "Role$Procedure": return RoleProcedureBookmark.New(_cx, res);
                case "Role$Python": return RolePythonBookmark.New(_cx, res);
                case "Role$Subobject": return RoleSubobjectBookmark.New(_cx, res);
                case "Role$Table": return RoleTableBookmark.New(_cx, res);
                case "Role$TableCheck": return RoleTableCheckBookmark.New(_cx, res);
                case "Role$TablePeriod": return RoleTablePeriodBookmark.New(_cx, res);
                case "Role$Trigger": return RoleTriggerBookmark.New(_cx, res);
                case "Role$TriggerUpdateColumn": return RoleTriggerUpdateColumnBookmark.New(_cx, res);
                case "Role$Type": return RoleTypeBookmark.New(_cx, res);
                case "Role$View": return RoleViewBookmark.New(_cx, res);
                case "Sys$Role": return SysRoleBookmark.New(_cx, res);
                case "Sys$RoleUser": return SysRoleUserBookmark.New(_cx, res);
                case "Sys$User": return SysUserBookmark.New(_cx, res);
                // level >=4 stuff
                case "Sys$Classification": return SysClassificationBookmark.New(_cx, res);
                case "Sys$ClassifiedColumnData": return SysClassifiedColumnDataBookmark.New(_cx, res);
                case "Sys$Enforcement": return SysEnforcementBookmark.New(_cx, res);
                case "Sys$Audit": return SysAuditBookmark.New(_cx, res);
                case "Sys%AuditKey": return SysAuditKeyBookmark.New(_cx, res);
                case "Role$PrimaryKey": return RolePrimaryKeyBookmark.New(_cx, res);
#if PROFILES
                // profile stuff
                case "Profile$": return ProfileBookmark.New(_cx,res);
                case "Profile$ReadConstraint": return ProfileReadConstraintBookmark.New(_cx,res);
                case "Profile$Record": return ProfileRecordBookmark.New(_cx,res);
                case "Profile$RecordColumn": return ProfileRecordColumnBookmark.New(_cx,res);
                case "Profile$Table": return ProfileTableBookmark.New(_cx,res);
#endif
            }
            return null;
        }
        protected (Physical,long) _NextPhysical(long pp)
        {
            var rdr = new Reader(_tr, pp);
            var ph = rdr.Create();
            pp = (int)rdr.Position;
            if (ph == null)
                return (null,-1);
            return (ph,pp);
        }
        /// <summary>
        /// A bookmark for a system table
        /// </summary>
        internal abstract class SystemBookmark : RowBookmark
        {
            /// <summary>
            /// the system rowset
            /// </summary>
            public SystemRowSet res;
            public override TRow row => CurrentValue();
            public override TRow key => CurrentKey();
            /// <summary>
            /// base constructor for the system enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            protected SystemBookmark(Context _cx,SystemRowSet r,int pos,long dpos) :base(_cx,r,pos,dpos)
            {
                res = r;
            }
            protected TypedValue Pos(long p)
            {
                if (p == -1)
                    return TNull.Value;
                if (p < Transaction.TransPos)
                    return new TChar("" + p);
                return new TChar("'" + (p-Transaction.TransPos));
            }
            internal override TableRow Rec()
            {
                return null;
            }
            protected TypedValue Display(string d)
            {
                return new TChar(d);
            }
            public virtual TRow CurrentKey()
            {
                return row;
            }
            public abstract TRow CurrentValue();
        }
         /// <summary>
        /// A base class for enumerating log tables
        /// </summary>
        internal abstract class LogSystemBookmark : SystemBookmark
        {
            /// <summary>
            /// the database being used
            /// </summary>
            protected readonly Database db;
            internal readonly Physical ph;
            internal readonly long nextpos;
            public override TRow row => CurrentValue();
            public override TRow key => CurrentKey();
            /// <summary>
            /// Construct the LogSystemBookmark
            /// </summary>
            /// <param name="r">the rowset</param>
            protected LogSystemBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) p)
                : base(_cx,r,pos,p.Item1.ppos)
            {
                ph = p.Item1;
                nextpos = p.Item2;
            }
        }
        /// <summary>
        /// Set up the Log$ table
        /// </summary>
        static Database LogResults(Database d)
        {
            var t = new SystemTable("Log$");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Desc", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Type", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Affects", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// Define the Log$ enumerator
        /// </summary>
        internal class LogBookmark : LogSystemBookmark
        {
            /// <summary>
            /// Construct the LogEnumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            protected LogBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) p)
                : base(_cx,r, pos, p)
            {
                _cx.Add(r.qry,this);
            }
            internal static LogBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var rb = new LogBookmark(_cx,res, 0, x);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Desc,Type,Affects,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                return new TRow(res.from.rowType,
                    Pos(ph.ppos),
                    new TChar(ph.ToString()),
                    new TChar(ph.type.ToString()),
                    Pos(ph.Affects));
            }
            /// <summary>
            /// Move to the next log entry
            /// </summary>
            /// <returns>whether there is a next entry</returns>
            public override RowBookmark Next(Context _cx)
            {
                for (var x=res._NextPhysical(nextpos); x.Item1!=null; x=res._NextPhysical(x.Item2))
                {
                    var rb = new LogBookmark(_cx,res, _pos+1, x);
                    if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                        return rb;
                }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$Alter table
        /// </summary>
        static Database LogAlterResults(Database d)
        {
            var t = new SystemTable("Log$Alter");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "DefPos", 1, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// An enumerator for the Log$Alter table
        /// </summary>
        internal class LogAlterBookmark : LogSystemBookmark
        {
            /// <summary>
            /// construct the Log$Alter enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            LogAlterBookmark(Context _cx,SystemRowSet r,int pos,(Physical,long)p)
                : base(_cx,r,pos,p)
            {
                _cx.Add(r.qry,this);
            }
            internal static LogAlterBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Alter || ph.type == Physical.Type.Alter2
                        || ph.type == Physical.Type.Alter3)
                    {
                        var rb = new LogAlterBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current record (Pos,DefPos,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                var al = ph as PColumn;
                return new TRow(res.from.rowType,
                    Pos(al.ppos),
                    Pos(al.Affects));
            }
            /// <summary>
            /// move to the next Log$Alter entry
            /// </summary>
            /// <returns>whether there is a next entry</returns>
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Alter || ph.type == Physical.Type.Alter2 || ph.type == Physical.Type.Alter3)
                    {
                        var rb = new LogAlterBookmark(_cx,res, _pos+1,x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$Change table
        /// </summary>
        static Database LogChangeResults(Database d)
        {
            var t = new SystemTable("Log$Change");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Previous", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 2, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Change
        /// </summary>
        internal class LogChangeBookmark : LogSystemBookmark
        {
            LogChangeBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogChangeBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Change)
                    {
                        var rb = new LogChangeBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Previous,Name,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Change ch = (Change)ph;
                return new TRow(res.from.rowType,
                    Pos(ch.ppos),
                    Pos(ch.Previous),
                    new TChar(ch.name));
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Change)
                    {
                        var rb = new LogChangeBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
        }
        /// <summary>
        /// setup the Log$Delete table
        /// </summary>
        static Database LogDeleteResults(Database d)
        {
            var t = new SystemTable("Log$Delete");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "DelPos", 1, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Delete
        /// </summary>
        internal class LogDeleteBookmark : LogSystemBookmark
        {
            LogDeleteBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogDeleteBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Delete)
                    {
                        var rb = new LogDeleteBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,DelPos,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Delete d = (Delete)ph;
                return new TRow(res.from.rowType,
                    Pos(d.ppos),
                    Pos(d.delRow.defpos));
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Delete)
                    {
                        var rb = new LogDeleteBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$Drop table
        /// </summary>
        static Database LogDropResults(Database d)
        {
            var t = new SystemTable("Log$Drop");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "DelPos", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 2, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Drop
        /// </summary>
        internal class LogDropBookmark : LogSystemBookmark
        {
            LogDropBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogDropBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Drop)
                    {
                        var rb = new LogDropBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,DelPos,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Drop d = (Drop)ph;
                return new TRow(res.from.rowType,
                    Pos(d.ppos),
                    Pos(d.delpos));
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Drop)
                    {
                        var rb = new LogDropBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr,_cx))
                            return rb;
                    }
                }
                return null;
            }
        }
        /// <summary>
        /// Set up the Log$Metadata table
        /// </summary>
        static Database LogMetadataResults(Database d)
        {
            var t = new SystemTable("Log$Metadata");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "DefPos", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Description", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Output", 4, Domain.Char);
            t+=new SystemTableColumn(t, "RefPos", 5, Domain.Char);
            t+=new SystemTableColumn(t, "Iri", 6, Domain.Char);
            return t.Add(d);
        }
        internal class LogMetadataBookmark : LogSystemBookmark
        {
            LogMetadataBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogMetadataBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Metadata || ph.type == Physical.Type.Metadata2 
                        || ph.type == Physical.Type.Metadata3)
                    {
                        var rb = new LogMetadataBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Metadata || ph.type == Physical.Type.Metadata2
                        || ph.type == Physical.Type.Metadata3)
                    {
                        var rb = new LogMetadataBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,DefPos,Name,Proc,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PMetadata m = (PMetadata)ph;
                return new TRow(res.from.rowType,
                    Pos(m.ppos),
                    Pos(m.defpos),
                    new TChar(m.name),
                    new TChar(m.description),
                    new TChar(m.Flags()),
                    Pos(m.refpos),
                    new TChar(m.iri));
            }
         }
        /// <summary>
        /// Set up the Log$Modify table
        /// </summary>
        static Database LogModifyResults(Database d)
        {
            var t = new SystemTable("Log$Modify");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "DefPos", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Proc", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// An enumerator for Log$Modify
        /// </summary>
        internal class LogModifyBookmark : LogSystemBookmark
        {
            LogModifyBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogModifyBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Modify)
                    {
                        var rb = new LogModifyBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Modify)
                    {
                        var rb = new LogModifyBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,DefPos,Name,Proc,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Modify m = (Modify)ph;
                return new TRow(res.from.rowType,
                    Pos(m.ppos),
                    Pos(m.modifydefpos),
                    new TChar(m.name),
                    new TChar(m.body));
            }
         }
        /// <summary>
        /// set up the Log$User table
        /// </summary>
        static Database LogUserResults(Database d)
        {
            var t = new SystemTable("Log$User");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for the Log$User table
        /// </summary>
        internal class LogUserBookmark : LogSystemBookmark
        {
            LogUserBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogUserBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PUser)
                    {
                        var rb = new LogUserBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PUser)
                    {
                        var rb = new LogUserBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PUser a = (PUser)ph;
                return new TRow(res.from.rowType,
                    Pos(a.ppos),
                    new TChar(a.name));
            }
        }
        /// <summary>
        /// set up the Log$Role table
        /// </summary>
        static Database LogRoleResults(Database d)
        {
            var t = new SystemTable("Log$Role");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Details", 2, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Role
        /// </summary>
        internal class LogRoleBookmark : LogSystemBookmark
        {
            LogRoleBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogRoleBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PRole||ph.type==Physical.Type.PRole1)
                    {
                        var rb = new LogRoleBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PRole || ph.type == Physical.Type.PRole1)
                    {
                        var rb = new LogRoleBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr,_cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PRole a = (PRole)ph;
                return new TRow(res.from.rowType,
                    Pos(a.ppos),
                    new TChar(a.name),
                    new TChar(a.details));
            }
        }
        /// <summary>
        /// set up the Log$Check table
        /// </summary>
        static Database LogCheckResults(Database d)
        {
            var t = new SystemTable("Log$Check");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Ref", 1, Domain.Char);
            t+=new SystemTableColumn(t, "ColRef", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Check", 4, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Check
        /// </summary>
        internal class LogCheckBookmark : LogSystemBookmark
        {
            LogCheckBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogCheckBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PCheck||ph.type == Physical.Type.PCheck2)
                    {
                        var rb = new LogCheckBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PCheck||ph.type == Physical.Type.PCheck2)
                    {
                        var rb = new LogCheckBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Ref,Name,Check,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PCheck c = (PCheck)ph;
                return new TRow(res.from.rowType,
                    Pos(c.ppos),
                    Pos(c.ckobjdefpos),
                    Pos(c.subobjdefpos),
                    new TChar(c.name),
                    Display(c.check));
            }
        }
        static Database LogClassificationResults(Database d)
        {
            var t = new SystemTable("Log$Classification");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Obj", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Classification", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 3, Domain.Char);
            return t.Add(d);
        }
        internal class LogClassificationBookmark : LogSystemBookmark
        {
            LogClassificationBookmark(Context _cx,SystemRowSet rs, int pos, (Physical,long) ph) 
                : base(_cx,rs, pos, ph)
            {
                _cx.Add(rs.qry, this);
            }
            internal static LogClassificationBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Classify)
                    {
                        var rb = new LogClassificationBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Classify)
                    {
                        var rb = new LogClassificationBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }

            /// <summary>
            /// the current value: (Pos,User,Clearance,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Classify c = (Classify)ph;
                return new TRow(res.from.rowType,
                    Pos(c.ppos),
                    Pos(c.obj),
                    new TChar(c.classification.ToString()),
                    Pos(c.trans));
            }
        }
        static Database LogClearanceResults(Database d)
        {
            var t = new SystemTable("Log$Clearance");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "User", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Clearance", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 3, Domain.Char);
            return t.Add(d);
        }
        internal class LogClearanceBookmark : LogSystemBookmark
        {
            LogClearanceBookmark(Context _cx,SystemRowSet rs, int pos, (Physical,long) ph)
                : base(_cx,rs, pos, ph)
            {
                _cx.Add(rs.qry, this);
            }
            internal static LogClearanceBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Clearance)
                    {
                        var rb = new LogClearanceBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Clearance)
                    {
                        var rb = new LogClearanceBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr,_cx))
                            return rb;
                    }
                }
                return null;
            }

            /// <summary>
            /// the current value: (Pos,User,Clearance,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Clearance c = (Clearance)ph;
                return new TRow(res.from.rowType,
                    Pos(c.ppos),
                    Pos(c.user.defpos),
                    new TChar(c.clearance.ToString()),
                    Pos(c.trans));
            }
        }
        /// <summary>
        /// set up the Log$Column table
        /// </summary>
        static Database LogColumnResults(Database d)
        {
            var t = new SystemTable("Log$Column");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Seq", 3, Domain.Int);
            t+=new SystemTableColumn(t, "Domain", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Default", 5, Domain.Char);
            t+=new SystemTableColumn(t, "NotNull", 6, Domain.Bool);
            t+=new SystemTableColumn(t, "Generated", 7, Domain.Char);
            t+=new SystemTableColumn(t, "Update", 8, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 9, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Column
        /// </summary>
        internal class LogColumnBookmark: LogSystemBookmark
        {
            LogColumnBookmark(Context _cx,SystemRowSet rs,int pos,(Physical,long) ph)
                : base(_cx,rs,pos,ph)
            {
                _cx.Add(rs.qry, this);
            }
            internal static LogColumnBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PColumn||ph.type==Physical.Type.PColumn2
                        ||ph.type==Physical.Type.PColumn3)
                    {
                        var rb = new LogColumnBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PColumn||ph.type==Physical.Type.PColumn2
                        ||ph.type==Physical.Type.PColumn3)
                    {
                        var rb = new LogColumnBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }

            /// <summary>
            /// the current value: (Pos,Table,Name,Seq.Domain,Default,NotNull,Generate,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                    PColumn c = (PColumn)ph;
                    return new TRow(res.from.rowType,
                        Pos(c.ppos),
                        Pos(c.tabledefpos),
                        new TChar(c.name),
                        new TInt(c.seq),
                        Pos(c.domdefpos),
                        Display(c.dfs),
                        TBool.For(c.notNull),
                        new TChar(c.generated.ToString()),
                        Display(c.upd),
                        Pos(c.trans));
            }
         }
        /// <summary>
        /// set up the Log$TablePeriod table
        /// </summary>
        static Database LogTablePeriodResults(Database d)
        {
            var t = new SystemTable("Log$TablePeriod");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 1, Domain.Char);
            t+=new SystemTableColumn(t, "PeriodName", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Versioning", 3, Domain.Bool);
            t+=new SystemTableColumn(t, "StartColumn", 4, Domain.Char);
            t+=new SystemTableColumn(t, "EndColumn", 5, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 3, Domain.Char);
            return t.Add(d);

        }
        /// <summary>
        /// an enumerator for Log$Column
        /// </summary>
        internal class LogTablePeriodBookmark : LogSystemBookmark
        {
            LogTablePeriodBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogTablePeriodBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PeriodDef)
                    {
                        var rb = new LogTablePeriodBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PeriodDef)
                    {
                        var rb = new LogTablePeriodBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Table,Name,Seq.Domain,Default,NotNull,Generate,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PPeriodDef c = (PPeriodDef)ph;
                return new TRow(res.from.rowType,
                    Pos(c.ppos),
                    Pos(c.tabledefpos),
                    new TChar(c.periodname),
                    TBool.True,
                    Pos(c.startcol),
                    Pos(c.endcol),
                    Pos(c.trans));
            }
         }
        /// <summary>
        /// set up the Log$DateType table
        /// </summary>
        static Database LogDateTypeResults(Database d)
        {
            var t = new SystemTable("Log$DateType");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Kind", 2, Domain.Char);
            t+=new SystemTableColumn(t, "StartField", 3, Domain.Int);
            t+=new SystemTableColumn(t, "EndField",  4, Domain.Int);
            t+=new SystemTableColumn(t, "Transaction", 5, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$DateType
        /// </summary>
        internal class LogDateTypeBookmark : LogSystemBookmark
        {
            LogDateTypeBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogDateTypeBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PDateType)
                    {
                        var rb = new LogDateTypeBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PDateType)
                    {
                        var rb = new LogDateTypeBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,DateType,start,end,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                    PDateType d = (PDateType)ph;
                    return new TRow(res.from.rowType,
                        Pos(d.ppos),
                        new TChar(d.name),
                        new TChar(d.kind.ToString()),
                        new TChar(d.start.ToString()),
                        new TChar(d.end.ToString()),
                        Pos(d.trans));
            }
         }
        /// <summary>
        /// set up the Log$Domain table
        /// </summary>
        static Database LogDomainResults(Database d)
        {
            var t = new SystemTable("Log$Domain");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Kind", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 2, Domain.Char);
            t+=new SystemTableColumn(t, "DataType", 3, Domain.Char);
            t+=new SystemTableColumn(t, "DataLength", 4, Domain.Int);
            t+=new SystemTableColumn(t, "Scale", 5, Domain.Int);
            t+=new SystemTableColumn(t, "Charset", 6, Domain.Char);
            t+=new SystemTableColumn(t, "Collate", 7, Domain.Char);
            t+=new SystemTableColumn(t, "Default", 8, Domain.Char);
            t+=new SystemTableColumn(t, "StructDef", 9, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 10, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Domain
        /// </summary>
        internal class LogDomainBookmark : LogSystemBookmark
        {
            LogDomainBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogDomainBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PDomain||ph.type==Physical.Type.PDomain1)
                    {
                        var rb = new LogDomainBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr,_cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PDomain||ph.type==Physical.Type.PDomain1)
                    {
                        var rb = new LogDomainBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Kind,Name,DataType,DataLength,Scale,Charset,Collate,Default,StructDef,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PDomain d = (PDomain)ph;
                return new TRow(res.from.rowType,
                    Pos(d.ppos),
                    new TChar(d.type.ToString()),
                    new TChar(d.name),
                    new TChar(d.kind.ToString()),
                    new TInt(d.prec),
                    new TInt(d.scale),
                    new TChar(d.charSet.ToString()),
                    new TChar(d.culture.Name),
                    Display(d.defaultValue),
                    new TInt(d.elTypedefpos),
                    Pos(d.trans));
            }
         }
        /// <summary>
        /// set up the Log$Edit table
        /// </summary>
        static Database LogEditResults(Database d)
        {
            var t = new SystemTable("Log$Edit");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Prev", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 2, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Edit
        /// </summary>
        internal class LogEditBookmark : LogSystemBookmark
        {
            LogEditBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogEditBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Edit)
                    {
                        var rb = new LogEditBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Edit)
                    {
                        var rb = new LogEditBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Prev,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Edit d = (Edit)ph;
                return new TRow(res.from.rowType,
                    Pos(d.ppos),
                    Pos(d.prev),
                    Pos(d.trans));
            }
         }
        static Database LogEnforcementResults(Database d)
        {
            var t = new SystemTable("Log$Enforcement");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Flags", 2, Domain.Int);
            t+=new SystemTableColumn(t, "Transaction", 3, Domain.Char);
            return t.Add(d);
        }
        internal class LogEnforcementBookmark : LogSystemBookmark
        {
            LogEnforcementBookmark(Context _cx,SystemRowSet rs, int pos, (Physical,long) ph)
                :base(_cx,rs,pos,ph)
            {
                _cx.Add(rs.qry, this);
            }
            internal static LogEnforcementBookmark New(Context _cx,SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Enforcement)
                    {
                        var rb = new LogEnforcementBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override TRow CurrentValue()
            {
                var en = ph as Enforcement;
                return new TRow(res.rowType, Pos(en.ppos), Pos(en.tabledefpos),
                    new TInt((long)en.enforcement),Pos(en.trans));
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Enforcement)
                    {
                        var rb = new LogEnforcementBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$Grant table
        /// </summary>
        static Database LogGrantResults(Database d)
        {
            var t = new SystemTable("Log$Grant");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Privilege", 1, Domain.Int);
            t+=new SystemTableColumn(t, "Object", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Grantee", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 4, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for the Log$Grant table
        /// </summary>
        internal class LogGrantBookmark : LogSystemBookmark
        {
            LogGrantBookmark(Context _cx,SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(r.qry, this);
            }
            internal static LogGrantBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Grant)
                    {
                        var rb = new LogGrantBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Grant)
                    {
                        var rb = new LogGrantBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }

            /// <summary>
            /// the current value: (Pos,Privilege,Object,Grantee,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Grant g = (Grant)ph;
                return new TRow(res.from.rowType,
                    Pos(g.ppos),
                    new TInt((long)g.priv),
                    Pos(g.obj),
                    Pos(g.grantee),
                    Pos(g.trans));
            }
        }
        /// <summary>
        /// set up the Log$Index table
        /// </summary>
        static Database LogIndexResults(Database d)
        {
            var t = new SystemTable("Log$Index");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Flags", 3, Domain.Int);
            t+=new SystemTableColumn(t, "Reference", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Adapter", 5, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 6, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for the Log$Index table
        /// </summary>
        internal class LogIndexBookmark : LogSystemBookmark
        {
            LogIndexBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogIndexBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PIndex||ph.type==Physical.Type.PIndex1
                        ||ph.type==Physical.Type.PIndex2)
                    {
                        var rb = new LogIndexBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PIndex||ph.type==Physical.Type.PIndex1
                        ||ph.type==Physical.Type.PIndex2)
                    {
                        var rb = new LogIndexBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Table,Flags,Reference,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                    PIndex p = (PIndex)ph;
                    return new TRow(res.from.rowType,
                        Pos(p.ppos),
                        new TChar(p.name),
                        Pos(p.tabledefpos),
                        new TChar(p.flags.ToString()),
                        Pos(p.reference),
                        Display(p.adapter),
                        Pos(p.trans));
            }
         }
        /// <summary>
        /// set up the Log$IndexKey table
        /// </summary>
        static Database LogIndexKeyResults(Database d)
        {
            var t = new SystemTable("Log$IndexKey");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "ColNo", 1, Domain.Int);
            t+=new SystemTableColumn(t, "Column", 2, Domain.Int);
            t+=new SystemTableColumn(t, "Transaction", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for the Log$IndexKey table
        /// </summary>
        internal class LogIndexKeyBookmark : LogSystemBookmark
        {
            /// <summary>
            /// The key position
            /// </summary>
            readonly int _ix;
            /// <summary>
            /// construct a Log$IndexKey enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            LogIndexKeyBookmark(Context _cx, SystemRowSet rs, int pos, (Physical,long) ph, int i)
                 : base(_cx,rs, pos,ph)
            {
                _ix = i;
                _cx.Add(res.qry, this);
            }
            internal static LogIndexKeyBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var p = x.Item1;
                    if (p.type == Physical.Type.PIndex || p.type == Physical.Type.PIndex1 || p.type == Physical.Type.PIndex2)
                    {
                        var rb = new LogIndexKeyBookmark(_cx,res, 0, x, 0);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var ix = _ix + 1; ph is PIndex x && ix < x.columns.Count; ix = ix + 1)
                {
                    var rb = new LogIndexKeyBookmark(_cx,res, _pos, (ph, nextpos), ix);
                    if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                        return rb;
                }
                for (var pp = res._NextPhysical(nextpos); pp.Item1 != null; pp = res._NextPhysical(pp.Item2))
                {
                    var p = pp.Item1;
                    if (p.type == Physical.Type.PIndex || p.type == Physical.Type.PIndex1 || p.type == Physical.Type.PIndex2)
                    {
                        var x = (PIndex)p;
                        for (var ix = 0; ix < x.columns.Count; ix++)
                        {
                            var rb = new LogIndexKeyBookmark(_cx,res, _pos + 1, pp, ix);
                            if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                                return rb;
                        }
                    }
                }
                return null;
            }
            /// <summary>
            /// the current key: (Pos,ColNo)
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(res.keyType,
                    new TInt(ph.ppos),
                    new TInt(_ix));
            }
            /// <summary>
            /// the current value: (Pos,ColNo,Column,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PIndex x = (PIndex)ph;
                return new TRow(res.from.rowType,
                    Pos(x.ppos),
                    new TInt(_ix),
                    new TInt(((Selector)res._tr.role.objects[x.columns[_ix]]).seq),
                    Pos(x.trans));
            }
        }
        /// <summary>
        /// set up the Log$Ordering table
        /// </summary>
        static Database LogOrderingResults(Database d)
        {
            var t = new SystemTable("Log$Ordering");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "TypeDefPos", 1, Domain.Char);
            t+=new SystemTableColumn(t, "FuncDefPos", 2, Domain.Char);
            t+=new SystemTableColumn(t, "OrderFlags", 3, Domain.Int);
            t+=new SystemTableColumn(t, "Transaction", 4, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for the Log$Ordering table
        /// </summary>
        internal class LogOrderingBookmark : LogSystemBookmark
        {
            LogOrderingBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogOrderingBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Ordering)
                    {
                        var rb = new LogOrderingBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Ordering)
                    {
                        var rb = new LogOrderingBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,TypeDefPos,FuncDefPos,OrderFlags,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Ordering o = (Ordering)ph;
                return new TRow(res.from.rowType,
                    Pos(o.ppos),
                    Pos(o.typedefpos),
                    Pos(o.funcdefpos),
                    new TChar(o.flags.ToString()),
                    Pos(o.trans));
            }
        }
        /// <summary>
        /// set up the Log$Procedure table
        /// </summary>
        static Database LogProcedureResults(Database d)
        {
            var t = new SystemTable("Log$Procedure");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Arity", 2, Domain.Int);
            t+=new SystemTableColumn(t, "RetDefPos", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Proc", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 5, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for the Log$Procedure table
        /// </summary>
        internal class LogProcedureBookmark : LogSystemBookmark
        {
            LogProcedureBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogProcedureBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PProcedure||ph.type == Physical.Type.PProcedure2)
                    {
                        var rb = new LogProcedureBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PProcedure||ph.type == Physical.Type.PProcedure2)
                    {
                        var rb = new LogProcedureBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Arity,Proc,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PProcedure p = (PProcedure)ph;
                return new TRow(res.from.rowType,
                    Pos(p.ppos),
                    new TChar(p.nameAndArity),
                    new TInt(p.arity),
                    Pos(p.retdefpos),
                    Display(p.proc_clause),
                    Pos(p.trans));
            }
        }
        /// <summary>
        /// set up the Log$Revoke table
        /// </summary>
        static Database LogRevokeResults(Database d)
        {
            var t = new SystemTable("Log$Revoke");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Privilege", 1, Domain.Int);
            t+=new SystemTableColumn(t, "Object", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Grantee", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 4, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Revoke
        /// </summary>
        internal class LogRevokeBookmark : LogSystemBookmark
        {
            LogRevokeBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogRevokeBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Revoke)
                    {
                        var rb = new LogRevokeBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Revoke)
                    {
                        var rb = new LogRevokeBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Privilege,Object,Grantee,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Grant g = (Grant)ph;
                return new TRow(res.from.rowType,
                    Pos(g.ppos),
                    new TInt((long)g.priv),
                    Pos(g.obj),
                    Pos(g.grantee),
                    Pos(g.trans));
            }
         }
        /// <summary>
        /// set up the Log$Table table
        /// </summary>
        static Database LogTableResults(Database d)
        {
            var t = new SystemTable("Log$Table");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Defpos", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Table
        /// </summary>
        internal class LogTableBookmark : LogSystemBookmark
        {
            LogTableBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogTableBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PTable||ph.type == Physical.Type.PTable1)
                    {
                        var rb = new LogTableBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PTable||ph.type == Physical.Type.PTable1)
                    {
                        var rb = new LogTableBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,name,Iri,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PTable d = (PTable)ph;
                return new TRow(res.from.rowType,
                    Pos(d.ppos),
                    new TChar(d.name),
                    Pos(d.defpos),
                    Pos(d.trans));
            }
        }
        /// <summary>
        /// set up the Log$Trigger table
        /// </summary>
        static Database LogTriggerResults(Database d)
        {
            var t = new SystemTable("Log$Trigger");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Flags", 3, Domain.Char);
            t+=new SystemTableColumn(t, "OldTable", 4, Domain.Char);
            t+=new SystemTableColumn(t, "NewTable", 5, Domain.Char);
            t+=new SystemTableColumn(t, "OldRow", 6, Domain.Char);
            t+=new SystemTableColumn(t, "NewRow", 7, Domain.Char);
            t+=new SystemTableColumn(t, "Def", 8, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 9, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Trigger
        /// </summary>
        internal class LogTriggerBookmark : LogSystemBookmark
        {
            LogTriggerBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogTriggerBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PTrigger)
                    {
                        var rb = new LogTriggerBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PTrigger)
                    {
                        var rb = new LogTriggerBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// The current value: (Pos,name,Table,Flags,OldTable,NewTable,OldRow,NewRow,Def,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PTrigger d = (PTrigger)ph;
                return new TRow(res.from.rowType,
                    Pos(d.ppos),
                    new TChar(d.name),
                    Pos(d.tabledefpos),
                    new TChar(d.tgtype.ToString()),
                    new TChar(d.oldTable),
                    new TChar(d.newTable),
                    new TChar(d.oldRow),
                    new TChar(d.newRow),
                    Display(d.def),
                    Pos(d.trans));
            }
        }
        /// <summary>
        /// setup the Log$TriggerUpdate table
        /// </summary>
        static Database LogTriggerUpdateColumnResults(Database d)
        {
            var t = new SystemTable("Log$TriggerUpdateColumn");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Column", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 2, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$TriggerUpdate
        /// </summary>
        internal class LogTriggerUpdateColumnBookmark : LogSystemBookmark
        {
            /// <summary>
            /// the second part of the key
            /// </summary>
            readonly long[] _sub;
            readonly int _ix;
            /// <summary>
            /// construct a new Log$TriggerUpdate enumerator
            /// </summary>
            /// <param name="r"></param>
            LogTriggerUpdateColumnBookmark(Context _cx, SystemRowSet res,int pos,(Physical,long) ph,long[] s,int i)
                : base(_cx,res,pos,ph)
            {
                _sub = s;
                _ix = i;
                _cx.Add(res.qry, this);
            }
            internal static LogTriggerUpdateColumnBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PTrigger)
                    {
                        var pt = (PTrigger)ph;
                        if (pt.cols != null)
                            for (var ix = 0; ix < pt.cols.Length; ix++)
                            {
                                var rb = new LogTriggerUpdateColumnBookmark(_cx,res, 0, x, pt.cols, ix);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var ix = _ix + 1; ix < _sub.Length; ix = ix + 1)
                {
                    var rb = new LogTriggerUpdateColumnBookmark(_cx,res, _pos + 1, (ph, nextpos), _sub, ix);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (var pp = res._NextPhysical(nextpos); pp.Item1 is PTrigger pt && pt.cols != null;
                    pp = res._NextPhysical(pp.Item2))
                    for (var ix = 0; ix < pt.cols.Length; ix = ix + 1)
                    {
                        var rb = new LogTriggerUpdateColumnBookmark(_cx,res, _pos + 1, pp, pt.cols, ix);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current key: (Pos,Column)
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(res.keyType,
                    new TInt(ph.ppos),
                    new TInt(_sub[_ix]));
            }
            /// <summary>
            /// the current value: (Pos,Column)
            /// </summary>
            public override TRow CurrentValue()
            {
                PTrigger pt = (PTrigger)ph;
                return new TRow(res.from.rowType,
                    Pos(pt.ppos),
                    new TInt(_sub[_ix]),
                    Pos(pt.trans));
            }
        }
        static Database LogTriggeredActionResults(Database d)
        {
            var t = new SystemTable("Log$TriggeredAction");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Trigger", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 2, Domain.Char);
            return t.Add(d);
        }
        internal class LogTriggeredActionBookmark : LogSystemBookmark
        {
            LogTriggeredActionBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogTriggeredActionBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.TriggeredAction)
                    {
                        var rb = new LogTriggeredActionBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.TriggeredAction)
                    {
                        var rb = new LogTriggeredActionBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,SuperType)
            /// </summary>
            public override TRow CurrentValue()
            {
                TriggeredAction t = (TriggeredAction)ph;
                return new TRow(res.from.rowType,
                    Pos(t.ppos),
                    Pos(t.trigger),
                    Pos(t.trans));
            }
        }
        /// <summary>
        /// set up the Log$Type table
        /// </summary>
        static Database LogTypeResults(Database d)
        {
            var t = new SystemTable("Log$Type");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "SuperType", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 2, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for the Log$Type table
        /// </summary>
        internal class LogTypeBookmark : LogSystemBookmark
        {
            LogTypeBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogTypeBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PType||ph.type == Physical.Type.PType1)
                    {
                        var rb = new LogTypeBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PType||ph.type == Physical.Type.PType1)
                    {
                        var rb = new LogTypeBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,SuperType)
            /// </summary>
            public override TRow CurrentValue()
            {
                PType t = (PType)ph;
                return new TRow(res.from.rowType,
                    Pos(t.ppos),
                    Pos(t.underdefpos),
                    Pos(t.trans));
            }
         }
        /// <summary>
        /// set up the Log$TypeMethod table
        /// </summary>
        static Database LogTypeMethodResults(Database d)
        {
            var t = new SystemTable("Log$TypeMethod");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Type", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$TypeMethod
        /// </summary>
        internal class LogTypeMethodBookmark : LogSystemBookmark
        {
            LogTypeMethodBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogTypeMethodBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PMethod||ph.type == Physical.Type.PMethod2)
                    {
                        var rb = new LogTypeMethodBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PMethod||ph.type == Physical.Type.PMethod2)
                    {
                        var rb = new LogTypeMethodBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Type,Name,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                    PMethod t = (PMethod)ph;
                    return new TRow(res.from.rowType,
                        Pos(t.ppos),
                        Pos(t.typedefpos),
                        new TChar(t.nameAndArity),
                        Pos(t.trans));
            }
        }
        /// <summary>
        /// set up the Log$View table
        /// </summary>
        static Database LogViewResults(Database d)
        {
            var t = new SystemTable("Log$View");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Select", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Struct", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Using", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 5, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$View
        /// </summary>
        internal class LogViewBookmark : LogSystemBookmark
        {
            LogViewBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogViewBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PView||ph.type == Physical.Type.PView1)
                    {
                        var rb = new LogViewBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PView||ph.type == Physical.Type.PView1)
                    {
                        var rb = new LogViewBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Select,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                PView d = (PView)ph;
                TypedValue us = TNull.Value;
                TypedValue st = TNull.Value;
                if (d is PRestView2)
                    us = Pos(((PRestView2)d).usingtbpos);
                if (d is PRestView)
                    st = Pos(((PRestView)d).structpos);
                return new TRow(res.from.rowType,
                    Pos(d.ppos),
                    new TChar(d.name),
                    Display(d.view),
                    st,
                    us,
                    Pos(d.trans));
            }
        }
        /// <summary>
        /// set up the Log$Record table
        /// </summary>
        static Database LogRecordResults(Database d)
        {
            var t = new SystemTable("Log$Insert");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 1, Domain.Char);
            t+=new SystemTableColumn(t, "SubType", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Classification", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Record
        /// </summary>
        internal class LogRecordBookmark : LogSystemBookmark
        {
            LogRecordBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogRecordBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Record||ph.type == Physical.Type.Record1||ph.type == Physical.Type.Record2)
                    {
                        var rb = new LogRecordBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Record||ph.type == Physical.Type.Record1||ph.type == Physical.Type.Record2)
                    {
                        var rb = new LogRecordBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// current value: (Pos,Table,SubType,Classification,Transaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Record d = (Record)ph;
                var dp = (d.subType < 0) ? d.tabledefpos : d.subType;
                return new TRow(res.from.rowType,
                    Pos(d.ppos),
                    Pos(d.tabledefpos),
                    Pos(d.subType),
                    new TChar(d.classification.ToString()),
                    Pos(d.trans));
            }
        }
        /// <summary>
        /// set up the Log$RecordField table
        /// </summary>
        static Database LogRecordFieldResults(Database d)
        {
            var t = new SystemTable("Log$InsertField");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "ColRef", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Data", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$RecordField
        /// </summary>
        internal class LogRecordFieldBookmark : LogSystemBookmark
        {
            readonly LogRecordBookmark _rec;
            /// <summary>
            /// an enumerator for fields in this record
            /// </summary>
            readonly ABookmark<long, TypedValue> _fld;
            /// <summary>
            /// create the Log$RecordField enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            LogRecordFieldBookmark(Context _cx, LogRecordBookmark rec,int pos,ABookmark<long,TypedValue>fld)
                : base(_cx,rec.res,pos,(rec.ph,rec.nextpos))
            {
                _rec = rec;
                _fld = fld;
                _cx.Add(res.qry, this);
            }
            internal static LogRecordFieldBookmark New(Context _cx, SystemRowSet res)
            {
                for (var lb = LogRecordBookmark.New(_cx,res); lb != null; 
                    lb = (LogRecordBookmark)lb.Next(_cx))
                    for (var b = (lb.ph as Record).fields.First(); b != null; b = b.Next())
                    {
                        var rb = new LogRecordFieldBookmark(_cx,lb, 0, b);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// current key: (Pos,ColRef)
            /// </summary>
            public override TRow CurrentKey()
            {
                var r = ph as Record;
                long p = _fld.key();
                return new TRow(_rec.res.keyType, new TInt(r.defpos),
                    new TInt(p));
            }
            /// <summary>
            /// current value: (Pos,Colref,Data)
            /// </summary>
            public override TRow CurrentValue()
            {
                Record r = (Record)ph;
                long p = _fld.key();
                var v = r.fields[p] ?? TNull.Value;
                return new TRow(res.from.rowType,
                    Pos(r.ppos),
                    Pos(p),
                    new TChar(v.ToString()),
                    Pos(r.trans));
            }
            /// <summary>
            /// Move to next Field or Record
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                for (var fld = _fld.Next(); fld != null; fld = fld.Next())
                {
                    var rb = new LogRecordFieldBookmark(_cx,_rec, _pos + 1, fld);
                    if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                        return rb;
                }
                for (var rec = (LogRecordBookmark)_rec.Next(_cx); rec != null; 
                    rec = (LogRecordBookmark)Next(_cx))
                    for (var b = (rec.ph as Record).fields.First(); b != null; b = b.Next())
                    {
                        var rb = new LogRecordFieldBookmark(_cx,rec, _pos + 1, b);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$UpdatePost table
        /// </summary>
        static Database LogUpdateResults(Database d)
        {
            var t = new SystemTable("Log$Update");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "DefPos", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 2, Domain.Char);
            t+=new SystemTableColumn(t, "SubType", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Classification", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 5, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$UpdatePost
        /// </summary>
        internal class LogUpdateBookmark : LogSystemBookmark
        {
            LogUpdateBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogUpdateBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Update||ph.type == Physical.Type.Update1)
                    {
                        var rb = new LogUpdateBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.Update||ph.type == Physical.Type.Update1)
                    {
                        var rb = new LogUpdateBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// current value: (Pos,DefPos,Table,SubType,ClassificationTransaction)
            /// </summary>
            public override TRow CurrentValue()
            {
                Update u = (Update)ph;
                var dp = (u.subType < 0) ? u.tabledefpos : u.subType;
                return new TRow(res.from.rowType,
                    Pos(u.ppos),
                    Pos(u.defpos),
                    Pos(u.tabledefpos),
                    Pos(u.subType),
                    new TChar(u.classification.ToString()),
                    Pos(u.trans));
            }
        }
        /// <summary>
        /// set up the Log$Transaction table
        /// </summary>
        static Database LogTransactionResults(Database d)
        {
            var t = new SystemTable("Log$Transaction");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "NRecs", 1, Domain.Int);
            t+=new SystemTableColumn(t, "Time", 2, Domain.Int);
            t+=new SystemTableColumn(t, "User", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Role", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Source", 5, Domain.Char);
            t+=new SystemTableColumn(t, "Transaction", 6, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Log$Transaction
        /// </summary>
        internal class LogTransactionBookmark : LogSystemBookmark
        {
            LogTransactionBookmark(Context _cx, SystemRowSet r, int pos, (Physical,long) ph)
                : base(_cx,r, pos, ph)
            {
                _cx.Add(res.qry, this);
            }
            internal static LogTransactionBookmark New(Context _cx, SystemRowSet res)
            {
                for (var x = res._NextPhysical(5); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PTransaction||ph.type == Physical.Type.PTransaction2)
                    {
                        var rb = new LogTransactionBookmark(_cx,res, 0, x);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                for (var x = res._NextPhysical(nextpos); x.Item1 != null; x = res._NextPhysical(x.Item2))
                {
                    var ph = x.Item1;
                    if (ph.type == Physical.Type.PTransaction||ph.type == Physical.Type.PTransaction2)
                    {
                        var rb = new LogTransactionBookmark(_cx,res, _pos + 1, x);
                        if (Query.Eval(_rs.qry.where, _rs._tr, _cx))
                            return rb;
                    }
                }
                return null;
            }
            /// <summary>
            /// current value: (Pos,NRecs,Time,User,Authority)
            /// </summary>
            public override TRow CurrentValue()
            {
                    PTransaction t = (PTransaction)ph;
                    PImportTransaction it = t as PImportTransaction;
                    return new TRow(res.from.rowType,
                        Pos(t.ppos),
                        new TInt(t.nrecs),
                        new TDateTime(new DateTime(t.time)),
                        Pos(t.ptuser),
                        Pos(t.ptrole),
                        new TChar((it == null) ? "" : it.uri),
                        Pos(t.trans));
            }
        }
        /// <summary>
        /// set up the Sys$Role table
        /// </summary>
        static Database SysRoleResults(Database d)
        {
            var t = new SystemTable("Sys$Role");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// enumerate the Sys$Role table
        /// </summary>
        internal class SysRoleBookmark : SystemBookmark
        {
            /// <summary>
            /// an enumerator for the role's objects
            /// </summary>
            readonly ABookmark<long,object> _bmk;
            /// <summary>
            /// The current Role
            /// </summary>
            internal Role role { get { return (Role)_bmk.value(); } }
            /// <summary>
            /// create the Sys$Role enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            SysRoleBookmark(Context _cx, SystemRowSet res,int pos, ABookmark<long,object> bmk)
                : base(_cx,res,pos,bmk.key())
            {
                _bmk = bmk;
                _cx.Add(res.qry, this);
            }
            internal static SysRoleBookmark New(Context _cx, SystemRowSet res)
            {
                for (var en = res._tr.role.objects.PositionAt(0); en != null; en = en.Next())
                {
                    var rb = new SysRoleBookmark(_cx,res, 0, en);
                    if (en.value() is Role && Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                return null;
            }
            /// <summary>
            /// the current key: Pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(res.keyType, new TInt(role.defpos));
            }
            /// <summary>
            /// the current value (Pos,Name,Kind)
            /// </summary>
            public override TRow CurrentValue()
            {
                Role a = role;
                return new TRow(res.from.rowType,
                    Pos(a.defpos),
                    new TChar(a.name));
            }
            /// <summary>
            /// Move to the next Role
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                for (var bmk = _bmk.Next(); bmk != null; bmk = bmk.Next())
                {
                    var rb = new SysRoleBookmark(_cx,res, _pos + 1, bmk);
                    if (bmk.value() is Role && Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                return null;
            }

        }
        /// <summary>
        /// set up the Sys$User table
        /// </summary>
        static Database SysUserResults(Database d)
        {
            var t = new SystemTable("Sys$User");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "SetPassword", 2, Domain.Bool); // usually null
            t+=new SystemTableColumn(t, "InitialRole", 3, Domain.Char); // usually null
            t+=new SystemTableColumn(t, "Clearance", 4, Domain.Char); // usually null, otherwise D to A
            return t.Add(d);
        }
        /// <summary>
        /// enumerate the Sys$User table
        /// </summary>
        internal class SysUserBookmark : SystemBookmark
        {
            /// <summary>
            /// an enumerator for the roles tree
            /// </summary>
            readonly ABookmark<long,object> en;
            /// <summary>
            /// The current Role
            /// </summary>
            internal User user
            {
                get
                {
                    return (User)en.value();
                }
            }
            /// <summary>
            /// create the Sys$Role enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            SysUserBookmark(Context _cx, SystemRowSet res,int pos, ABookmark<long,object> bmk)
                : base(_cx,res,pos,bmk.key())
            {
                en = bmk;
                _cx.Add(res.qry, this);
            }
            internal static SysUserBookmark New(Context _cx, SystemRowSet res)
            {
                for (var en = res._tr.role.objects.PositionAt(0); en != null; en = en.Next())
                {
                    var rb = new SysUserBookmark(_cx,res, 0, en);
                    if (en.value() is User && Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                return null;
            }
            /// <summary>
            /// the current key: Pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(res.keyType, new TInt(user.defpos));
            }
            /// <summary>
            /// the current value (Pos,Name,Kind)
            /// </summary>
            public override TRow CurrentValue()
            {
                User a = user;
                return new TRow(res.from.rowType,
                    Pos(a.defpos),
                    new TChar(a.name),
                    (a.pwd==null)?TNull.Value:TBool.For(a.pwd.Length==0),
                    new TChar(((Role)res._tr.role.objects[a.initialRole]).name),
                    new TChar(a.clearance.ToString()));
            }
            /// <summary>
            /// Move to the next Role
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                for (var e = en.Next(); e != null; e = e.Next())
                {
                    var rb = new SysUserBookmark(_cx,res, _pos + 1, e);
                    if (e.value() is User 
                        && Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                return null;
            }
        }
        static Database SysRoleUserResults(Database d)
        {
            var t = new SystemTable("Sys$RoleUser");
            t+=new SystemTableColumn(t, "Role", 0, Domain.Char);
            t+=new SystemTableColumn(t, "User", 1, Domain.Char);
            return t.Add(d);
        }
        internal class SysRoleUserBookmark : SystemBookmark
        {
            readonly ABookmark<long,Role> _rbmk;
            readonly ABookmark<long, object> _inner;
            readonly User _user;
            SysRoleUserBookmark(Context _cx, SystemRowSet srs,ABookmark<long,Role> rbmk, 
                ABookmark<long,object> inner,int pos) : base(_cx,srs,pos,inner.key())
            {
                _rbmk = rbmk;
                _inner = inner;
                _user = (User)inner.value();
                _cx.Add(res.qry, this);
            }
            internal static SysRoleUserBookmark New(Context _cx, SystemRowSet res)
            {
                for (var rb = res._tr.roles.First(); rb != null; rb = rb.Next())
                    for (var inner = rb.value().objects.PositionAt(0); inner!=null;inner=inner.Next())
                        if (inner.value() is User us && us.priv.HasFlag(Grant.Privilege.Usage))
                        {
                            var sb = new SysRoleUserBookmark(_cx,res, rb, inner, 0);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return sb;
                        }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                var inner = _inner;
                var rbmk = _rbmk;
                for (; ; )
                {
                    inner = inner.Next();
                    if (inner?.value() is User us && us.priv.HasFlag(Grant.Privilege.Usage))
                    {
                        var sb = new SysRoleUserBookmark(_cx,res, rbmk, inner, _pos+1);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return sb;
                    }
                    if (inner != null)
                        continue;
                    rbmk = rbmk?.Next();
                    if (rbmk == null)
                        return null;
                    inner = rbmk.value().objects.PositionAt(0);
                    if (inner?.value() is User u && u.priv.HasFlag(Grant.Privilege.Usage))
                    {
                        var sb = new SysRoleUserBookmark(_cx,res, rbmk, inner, _pos + 1);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return sb;
                    }
                }
            }
            public override TRow CurrentKey()
            {
                return new TRow(res.keyType, new TInt(_inner.key()));
            }
            public override TRow CurrentValue()
            {
                return new TRow(res.from.rowType,
                    new TChar(_rbmk.value().name),
                    new TChar(_user.name));
            }
        }
        static Database SysAuditResults(Database d)
        {
            var t = new SystemTable("Sys$Audit");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "User", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Timestamp", 3, Domain.Timestamp);
            return t.Add(d);
        }
        internal class SysAuditBookmark : SystemBookmark
        {
            public readonly LogBookmark _bmk;
            public SysAuditBookmark(Context _cx, SystemRowSet res, LogBookmark b,int pos)
                :base(_cx,res,pos,b._defpos)
            {
                _bmk = b;
                _cx.Add(res.qry, this);
            }
            internal static SysAuditBookmark New(Context _cx, SystemRowSet res)
            {
                for (var bmk = LogBookmark.New(_cx,res);bmk!=null;bmk=bmk.Next(_cx) as LogBookmark)
                    switch(bmk.ph.type)
                    {
                        case Physical.Type.Audit:
                            var rb = new SysAuditBookmark(_cx,res, bmk, 0);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                            break;
                    }
                return null;
            }
            public override TRow CurrentValue()
            {
                switch (_bmk.ph.type)
                {
                    case Physical.Type.Audit:
                        {
                            var au = _bmk.ph as Audit;
                            return new TRow(res.rowType, Pos(au.ppos),
                                Pos(au.user.defpos),Pos(au.table),
                                new TDateTime(new DateTime(au.timestamp)));
                        }
                }
                throw new PEException("PE001");
            }

            public override RowBookmark Next(Context _cx)
            {
                for (var bmk = _bmk.Next(_cx) as LogBookmark; bmk != null; 
                    bmk = bmk.Next(_cx) as LogBookmark)
                    switch (bmk.ph.type)
                    {
                        case Physical.Type.Audit:
                            var rb = new SysAuditBookmark(_cx,res, bmk, _pos+1);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                            break;
                    }
                return null;
            }
        }
        static Database SysAuditKeyResults(Database d)
        {
            var t = new SystemTable("Sys$AuditKey");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Seq", 1, Domain.Int);
            t+=new SystemTableColumn(t, "Col", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Key", 3, Domain.Char);
            return t.Add(d);
        }
        internal class SysAuditKeyBookmark : SystemBookmark
        {
            public readonly LogBookmark _bmk;
            public readonly int _ix;
            public SysAuditKeyBookmark(Context _cx, SystemRowSet res, LogBookmark bmk, int ix, int pos) 
                : base(_cx,res, pos,bmk._defpos)
            {
                _bmk = bmk; _ix = ix; _cx.Add(res.qry, this);
            }
            internal static SysAuditKeyBookmark New(Context _cx, SystemRowSet res)
            {
                for (var bmk = LogBookmark.New(_cx,res); bmk != null; bmk = bmk.Next(_cx) as LogBookmark)
                    switch (bmk.ph.type)
                    {
                        case Physical.Type.Audit:
                            var rb = new SysAuditKeyBookmark(_cx,res, bmk, 0, 0);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                            break;
                    }
                return null;
            }
            public override TRow CurrentValue()
            {
                var au = _bmk.ph as Audit;
                return new TRow(res.rowType, Pos(au.ppos), new TInt(_ix),
                    Pos(au.cols[_ix]),new TChar(au.key[_ix]));
            }

            public override RowBookmark Next(Context _cx)
            {
                var ix = _ix;
                var bmk = _bmk;
                var au = bmk.ph as Audit;
                for (; ; )
                {
                    for (ix = ix + 1; ix < au.key.Length; ix++)
                    {
                        var sb = new SysAuditKeyBookmark(_cx,res, _bmk, ix, _pos + 1);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return sb;
                    }
                    for (bmk = bmk?.Next(_cx) as LogBookmark; bmk != null; 
                        bmk = bmk.Next(_cx) as LogBookmark)
                        if (bmk.ph.type == Physical.Type.Audit)
                            break;
                    if (bmk == null)
                        return null;
                    ix = 0;
                    au = bmk.ph as Audit;
                    var rb = new SysAuditKeyBookmark(_cx,res, bmk, 0, _pos + 1);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
            }
        }
        static Database SysClassificationResults(Database d)
        {
            var t = new SystemTable("Sys$Classification");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Type", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Classification", 2, Domain.Char);
            t+=new SystemTableColumn(t, "LastTransaction", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// Classification is useful for DBObjects, TableColumns, and records
        /// </summary>
        internal class SysClassificationBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _obm;
            readonly ABookmark<long, TableRow> _tbm;
            readonly long _ppos;
            readonly Level _classification;
            readonly long _trans;
            readonly string _type;
            SysClassificationBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long,object> obm,
                ABookmark<long, TableRow> tbm, 
                long ppos,string type,Level classification)
                : base(_cx,res, pos, tbm?.key() ?? obm.key())
            {
                _obm = obm;  _tbm = tbm; _type = type;
                _ppos = ppos; _classification = classification; _trans = Trans(res,ppos);
                _cx.Add(res.qry, this);
            }
            long Trans(SystemRowSet res,long pp)
            {
                var ph = res._tr.GetD(pp);
                return ph.trans;
            }
            internal static SysClassificationBookmark New(Context _cx, SystemRowSet res)
            {
                for (var obm = res._tr.schemaRole.objects.PositionAt(0); obm != null; 
                    obm = obm.Next())
                {
                    var ob = (DBObject)obm.value();
                    if (ob.classification != Level.D)
                    {
                        var b = new SysClassificationBookmark(_cx,res, 0, obm, null,
                            ob.ppos, ob.GetType().Name, ob.classification);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return b;
                    }
                    if (ob is Table tb)
                        for (var tbm = tb.tableRows?.First(); tbm != null; tbm = tbm.Next())
                            if (tbm.value().classification != Level.D)
                            {
                                var rb = new SysClassificationBookmark(_cx,res, 0, obm, tbm,
                                    tbm.key(), "Record", tbm.value().classification);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                }
                return null;
            }
            public override TRow CurrentValue()
            {
                return new TRow(res.rowType, Pos(_ppos), 
                    new TChar(_type.ToString()),
                    new TChar(_classification.ToString()),
                    Pos(_trans));
            }
            public override RowBookmark Next(Context _cx)
            {
                var obm = _obm;
                var tbm = _tbm;
                for (; ; )
                {
                    var ob = (DBObject)obm.value();
                    if (ob is Table tb)
                    {
                        if (tbm == null)
                            tbm = tb.tableRows.First();
                        else
                            tbm = tbm.Next();
                        for (; tbm != null; tbm = tbm.Next())
                            if (tbm.value().classification != Level.D)
                            {
                                var ta = tbm.value();
                                var b = new SysClassificationBookmark(_cx,res, _pos + 1, obm, tbm,
                                            tbm.key(), "Record", tbm.value().classification);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return b;
                            }
                    }
                    obm = obm.Next();
                    if (obm == null)
                        return null;
                    ob = (DBObject)obm.value();
                    if (ob.classification != Level.D)
                    {
                        var rb = new SysClassificationBookmark(_cx,res, _pos + 1, obm, null, 
                                                ob.ppos, ob.GetType().Name, ob.classification);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                } 
            }
        }
        static Database SysClassifiedColumnDataResults(Database d)
        {
            var t = new SystemTable("Sys$ClassifiedColumnData");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Col", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Classification", 2, Domain.Char);
            t+=new SystemTableColumn(t, "LastTransaction", 3, Domain.Char);
            return t.Add(d);
        }
        internal class SysClassifiedColumnDataBookmark : SystemBookmark
        {
            readonly ABookmark<long, TableColumn> _cbm;
            readonly ABookmark<long, TableRow> _tbm;
            readonly long _ppos;
            readonly Level _classification;
            readonly long _trans;
            SysClassifiedColumnDataBookmark(Context _cx, SystemRowSet res, int pos, 
                ABookmark<long, TableColumn> cbm, ABookmark<long,TableRow> tbm,
                long ppos,Level classification) 
                :base(_cx,res,pos,tbm?.key()??cbm.key())
            {
                _ppos = ppos; _classification = classification;
                _cbm = cbm; _tbm = tbm;_cx.Add(res.qry, this);
            }
            long Trans(SystemRowSet res, long pp)
            {
                var ph = res._tr.GetD(pp);
                return ph.trans;
            }
            internal static SysClassifiedColumnDataBookmark New(Context _cx, SystemRowSet res)
            {
                var cols = BTree<long, TableColumn>.Empty;
                for (var b = res._tr.schemaRole.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is TableColumn tc && tc.classification != Level.D)
                        cols+=(tc.defpos, tc);
                for (var cbm = cols.First();cbm!=null;cbm=cbm.Next())
                {
                    var tc = cbm.value() as TableColumn;
                    var tb = res._tr.schemaRole.objects[tc.tabledefpos] as Table;
                    for (var tbm = tb.tableRows?.First(); tbm != null; tbm = tbm.Next())
                    {
                        var rt = tbm.value();
                        if (rt.fields.Contains(tc.defpos))
                        {
                            var rb = new SysClassifiedColumnDataBookmark(_cx,res, 0, cbm,
                                tbm, rt.defpos, rt.classification);
                            if (Query.Eval(res.qry.where, res._tr,_cx))
                                return rb;
                        }
                    }
                }
                return null;
            }
            public override TRow CurrentValue()
            {
                return new TRow(res.rowType, Pos(_ppos),
                    Pos(_cbm.key()),
                    new TChar(_classification.ToString()),
                    Pos(_trans));
            }
            public override RowBookmark Next(Context _cx)
            {
                var cbm = _cbm;
                var tbm = _tbm;
                for (; ; )
                {
                    var tc = cbm.value();
                    var tb = res._tr.role.objects[tc.tabledefpos] as Table;
                    if (tbm == null)
                        tbm = tb.tableRows.First();
                    else
                        tbm = tbm.Next();
                    for (; tbm != null; tbm = tbm.Next())
                    {
                        var rt = tbm.value();
                        if (rt.fields.Contains(tc.defpos))
                        {
                            var rb = new SysClassifiedColumnDataBookmark(_cx,res, _pos + 1, cbm, tbm,
                                                                    rt.defpos, rt.classification);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                    }
                    cbm = cbm.Next();
                    if (cbm == null)
                        return null;
                }
            }
        }
        static Database SysEnforcementResults(Database d)
        {
            var t = new SystemTable("Sys$Enforcement");
            t+=new SystemTableColumn(t, "Name", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Scope", 1, Domain.Char);
            return t.Add(d);
        }
        internal class SysEnforcementBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _en;
            SysEnforcementBookmark(Context _cx, SystemRowSet res, int pos, 
                ABookmark<long,object> en) :base(_cx,res,pos,en.key())
            {
                _en = en;_cx.Add(res.qry, this);
            }
            internal static SysEnforcementBookmark New(Context _cx, SystemRowSet res)
            {
                for (var b = res._tr.role.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is Table t && (int)t.enforcement!=15)
                    {
                        var rb = new SysEnforcementBookmark(_cx,res, 0, b);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            public override TRow CurrentValue()
            {
                var sb = new StringBuilder();
                var t = (Table)_en.value();
                Enforcement.Append(sb, t.enforcement);
                return new TRow(res.rowType, new TChar(t.name),
                    new TChar(sb.ToString()));
            }

            public override RowBookmark Next(Context _cx)
            {
                for (var b = _en.Next(); b != null; b = b.Next())
                    if (b.value() is Table t && (int)t.enforcement != 15)
                    {
                        var rb = new SysEnforcementBookmark(_cx,res, _pos+1, b);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$View table
        /// </summary>
        static Database RoleViewResults(Database d)
        {
            var t = new SystemTable("Role$View");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "View", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Select", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Struct", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Using", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Definer", 5, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$View
        /// </summary>
        internal class RoleViewBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the tables tree
            /// </summary>
            readonly ABookmark<long,object> _bmk;
            /// <summary>
            /// create the Sys$View enumerator
            /// </summary>
            /// <param name="r"></param>
            RoleViewBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> bmk) 
                : base(_cx,res,pos,bmk.key())
            {
                _bmk = bmk;_cx.Add(res.qry, this);
            }
            internal static RoleViewBookmark New(Context _cx, SystemRowSet res)
            {
                for (var bmk = res._tr.role.objects.PositionAt(0);bmk!= null;bmk=bmk.Next())
                    if (bmk.value() is View vw)
                    {
                        var rb =new RoleViewBookmark(_cx,res, 0, bmk);
                        if (Query.Eval(res.qry.where, res._tr,_cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current key: Pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value: (Pos,Name,Select)
            /// </summary>
            public override TRow CurrentValue()
            {
                var us = "";
                var st = "";
                if (_bmk.value() is RestView rv)
                {
                    var tb = (Table)res._tr.role.objects[rv.usingTable];
                    if (tb != null)
                        us = tb.name;
                    var sp = res._tr.GetDomain(rv.viewStruct);
                    if (sp != null)
                        st = sp.name;
                }
                var vw = (View)_bmk.value();
                return new TRow(res.from.rowType,
                    Pos(_defpos),
                    new TChar(vw.name),
                    new TChar(vw.viewdef),
                    new TChar(st),
                    new TChar(us),
                    new TChar(((Role)res._tr.role.objects[vw.definer]).name));
            }
            /// <summary>
            /// Move to the next View
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                for (var bmk = _bmk.Next(); bmk != null; bmk = bmk.Next())
                    if (bmk.value() is View vw)
                    {
                        var rb = new RoleViewBookmark(_cx,res, _pos + 1, bmk);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
       }
        /// <summary>
        /// set up the Role$DomainCheck table
        /// </summary>
        static Database RoleDomainCheckResults(Database d)
        {
            var t = new SystemTable("Role$DomainCheck");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "DomainName", 1, Domain.Char);
            t+=new SystemTableColumn(t, "CheckName", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Select", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// and enumerator for Role$DomainCheck
        /// </summary>
        internal class RoleDomainCheckBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for the current key
            /// </summary>
            readonly ABookmark<long,Check> _inner;
            readonly ABookmark<long,object> _outer;
            /// <summary>
            /// create the Sys$DomainCheck enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleDomainCheckBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<long,Check> inner)
                : base(_cx,res,pos,inner.key())
            {
                _outer = outer;
                _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RoleDomainCheckBookmark New(Context _cx, SystemRowSet res)
            {
                for (var bmk = res._tr.role.objects.PositionAt(0); bmk != null; bmk = bmk.Next())
                {
                    var dm = bmk.value() as Domain;
                    for (var inner = dm?.constraints.First(); inner != null; inner = inner.Next())
                        if (inner.value() is Check ck)
                        {
                            var rb = new RoleDomainCheckBookmark(_cx,res, 0, bmk, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                }
                return null;
            }
            /// <summary>
            /// the current key: (checkpos)
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value: (DomainName,CheckName,Select,Pos)
            /// </summary>
            public override TRow CurrentValue()
            {
                var domain = _outer.value() as Domain;
                var check = _inner.value() as Check;
                return new TRow(res.from.rowType, Pos(_defpos),
                    new TChar(domain.name),
                    new TChar(check.name),
                    new TChar(check.source));
            }
            /// <summary>
            /// Move to the next Sys$DomainCheck
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                for (;;)
                {
                    if (outer == null)
                        return null;
                    if (inner != null && (inner = inner.Next()) != null)
                    {
                        var check = inner.value() as Check;
                        if (check != null)
                        {
                            var rb = new RoleDomainCheckBookmark(_cx,res, _pos + 1, outer, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                    }
                    inner = null;
                    if ((outer = outer.Next()) == null)
                        return null;
                    var d = outer.value() as Domain;
                    if (d == null)
                        continue;
                    inner = d.constraints.First();
                    if (inner != null)
                    {
                        var check = inner.value() as Check;
                        if (check != null)
                        {
                            var sb = new RoleDomainCheckBookmark(_cx,res, _pos + 1, outer, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return sb;
                        }
                    }
                }
            }
        }
        /// <summary>
        /// set up the Role$TableCheck table
        /// </summary>
        static Database RoleTableCheckResults(Database d)
        {
            var t = new SystemTable("Role$TableCheck");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "TableName", 1, Domain.Char);
            t+=new SystemTableColumn(t, "CheckName", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Select", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$TableCheck
        /// </summary>
        internal class RoleTableCheckBookmark : SystemBookmark
        {
            /// <summary>
            /// Enumerators for traversing trees
            /// </summary>
            readonly ABookmark<long, Check> _inner;
            readonly ABookmark<long,object> _outer;
            /// <summary>
            /// create the Sys$TableCheck enumerator
            /// </summary>
            /// <param name="r"></param>
            RoleTableCheckBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long,object>outer,
               ABookmark<long, Check> inner) : base(_cx,res,pos,inner.key())
            {
                _outer = outer;
                _inner = inner;
                _cx.Add(res.qry, this);
            }
            internal static RoleTableCheckBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table rt)
                        for (var inner = rt.tableChecks.First(); inner != null; inner = inner.Next())
                            if (inner.value() is Check ck)
                            {
                                var rb = new RoleTableCheckBookmark(_cx,res, 0, outer, inner);
                                if (Query.Eval(res.qry.where, res._tr,_cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current key (CheckPos)
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value: (TableName,CheckName,Select,Pos)
            /// </summary>
            public override TRow CurrentValue()
            {
                var ch = _inner.value() as Check;
                return new TRow(res.from.rowType, Pos(_defpos),
                    new TChar(((DBObject)res._tr.role.objects[ch.checkobjpos]).name),
                    new TChar(ch.name),
                    new TChar(ch.source));
            }
            /// <summary>
            /// Move to the next TableCheck
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                Check ch;
                if (outer == null)
                    return null;
                for (; ; )
                {
                    if ((inner = inner?.Next())!= null)
                    {
                        ch = inner.value() as Check;
                        if (ch != null)
                            goto test;
                    }
                    if ((outer = outer.Next()) == null)
                        return null;
                    var rt = outer.value() as Table;
                    if (rt == null)
                        continue;
                    inner = rt.tableChecks?.First();
                    if (inner == null)
                        continue;
                    ch = inner.value() as Check;
                    if (ch == null)
                        continue;
                    test:
                    var rb = new RoleTableCheckBookmark(_cx,res, _pos + 1, outer, inner);
                    if (Query.Eval(res.qry.where, res._tr,_cx))
                        return rb;
                }
            }
        }
        /// <summary>
        /// set up the Role$TablePeriod table
        /// </summary>
        static Database RoleTablePeriodResults(Database d)
        {
            var t = new SystemTable("Role$TablePaeriod");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "TableName", 1, Domain.Char);
            t+=new SystemTableColumn(t, "PeriodName", 2, Domain.Char);
            t+=new SystemTableColumn(t, "PeriodStartColumn", 3, Domain.Char);
            t+=new SystemTableColumn(t, "PeriodEndColumn", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Versioning", 5, Domain.Bool);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$TablePeriod
        /// </summary>
        internal class RoleTablePeriodBookmark : SystemBookmark
        {
            /// <summary>
            /// Enumerators for traversing trees
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly SystemRowSet _res;
            /// <summary>
            /// create the Sys$TableCheck enumerator
            /// </summary>
            /// <param name="r"></param>
            RoleTablePeriodBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object>outer)
                : base(_cx,res,pos,outer.key())
            {
                _outer = outer; _res = res;_cx.Add(res.qry, this);
            }
            internal static RoleTablePeriodBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null;
                    outer = outer.Next())
                    if (outer.value() is Table t && t.periodDef != null)
                    {
                        var rb = new RoleTablePeriodBookmark(_cx,res, 0, outer);
                        if (Query.Eval(res.qry.where, res._tr,_cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current key (CheckPos)
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value: (TableName,CheckName,Select,Pos)
            /// </summary>
            public override TRow CurrentValue()
            {
                var pdef = ((Table)_outer.value()).periodDef;
                var t = res._tr.role.objects[pdef.tabledefpos] as Table;
                var start = res._tr.role.objects[pdef.startColDefpos] as TableColumn;
                var end = res._tr.role.objects[pdef.endColDefpos] as TableColumn;
                return new TRow(res.from.rowType, Pos(_defpos),
                    new TChar(t.name),
                    new TChar(pdef.name),
                    new TChar(start.name),
                    new TChar(end.name),
                    TBool.For(pdef.versioning != null));
            }
            /// <summary>
            /// Move to the next PeriodDef
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                if (outer == null)
                    return null;
                for (; ; )
                {
                    if ((outer = outer.Next()) == null)
                        return null;
                    if ((outer.value() as Table)?.periodDef != null)
                        return new RoleTablePeriodBookmark(_cx,_res, _pos + 1, outer);
                }
            }
        }
        /// <summary>
        /// set up the Role$Column table
        /// </summary>
        static Database RoleColumnResults(Database d)
        {
            var t = new SystemTable("Role$Column");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Seq", 3, Domain.Int);
            t+=new SystemTableColumn(t, "Domain", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Default", 5, Domain.Char);
            t+=new SystemTableColumn(t, "NotNull", 6, Domain.Int);
            t+=new SystemTableColumn(t, "Generated", 7, Domain.Char);
            t+=new SystemTableColumn(t, "Update", 8, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Column
        /// </summary>
        internal class RoleColumnBookmark : SystemBookmark
        {
            /// <summary>
            /// Enumerators for implementation
            /// </summary>
            readonly ABookmark<long, object> _outer;
            readonly ABookmark<string, Selector> _inner;
            /// <summary>
            /// create the Sys$Column enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleColumnBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<string, Selector> inner)
                : base(_cx,res, pos, inner.value().defpos)
            {
                _outer = outer;
                _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RoleColumnBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null;
                    outer = outer.Next())
                    if (outer.value() is Table rt)
                        for (var inner = rt.rowType.names.First(); inner != null;
                                inner = inner.Next())
                            if (inner.value() is TableColumn)
                            {
                                var rb = new RoleColumnBookmark(_cx,res, 0, outer, inner);
                                if (Query.Eval(res.qry.where, res._tr,_cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current key: ColPos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value: (Pos,Table,Name,Seq,Unique,Domain,Default,NotNull,Generated)
            /// </summary>
            public override TRow CurrentValue()
            {
                var tc = _inner.value() as TableColumn;
                var tb = _outer.value() as Table;
                return new TRow(res.from.rowType,
                    Pos(_defpos),
                    new TChar(tb.name),
                    new TChar(tc.name),
                    new TInt(tc.seq),
                    new TChar(tc.domain.name),
                    new TChar(tc.domain.defaultValue.ToString()),
                    TBool.For(tc.notNull),
                    new TChar(tc.generated.ToString()),
                    new TChar(tc.updateSource));
            }
            /// <summary>
            /// Move to the next Column def
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                for (; outer != null; outer = outer.Next())
                    if (outer.value() is Table rt)
                    {
                        if (inner == null)
                            inner = ((outer.value() as Table).rowType.names.First());
                        else
                            inner = inner.Next();
                        for (; inner != null; inner = inner.Next())
                            if (inner.value() is TableColumn tc)
                            {
                                var rb = new RoleColumnBookmark(_cx,res, _pos + 1, outer, inner);
                                if (Query.Eval(res.qry.where, res._tr,_cx))
                                    return rb;
                            }
                        inner = null;
                    }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$Class table
        /// </summary>
        static Database RoleClassResults(Database d)
        {
            var t = new SystemTable("Role$Class");
            t+=new SystemTableColumn(t, "Name", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Key", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Definition", 2, Domain.Char);
            return t.Add(d);
        }
        internal class RoleClassBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _enu;
            RoleClassBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> e)
                    : base(_cx,res, pos, e.key())
            {
                _enu = e;_cx.Add(res.qry, this);
            }
            internal static RoleClassBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                {
                    var t = outer.value();
                    if (((t is Table) || (t is View)) && !(t is RestView))
                    {
                        var rb = new RoleClassBookmark(_cx,res, 0, outer);
                        if (Query.Eval(res.qry.where, res._tr,_cx))
                            return rb;
                    }
                }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                var enu = _enu;
                for (enu = enu.Next(); enu != null; enu = enu.Next())
                {
                    var tb = enu.value();
                    if ((tb is Table || tb is View) && !(tb is RestView))
                    {
                        var rb = new RoleClassBookmark(_cx,res, _pos + 1, enu);
                        if (Query.Eval(res.qry.where, res._tr,_cx))
                            return rb;
                    }
                }
                return null;
            }
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Name", new TInt(_defpos)));
            }
            public override TRow CurrentValue()
            {
                return ((DBObject)_enu.value()).RoleClassValue(res._tr,res.from, _enu);
            }
        }

        /// <summary>
        /// set up the Role$ColumnCheck table
        /// </summary>
        static Database RoleColumnCheckResults(Database d)
        {
            var t = new SystemTable("Role$ColumnCheck");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char); 
            t+=new SystemTableColumn(t, "TableName", 1, Domain.Char);
            t+=new SystemTableColumn(t, "ColumnName", 2, Domain.Char);
            t+=new SystemTableColumn(t, "CheckName", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Select", 4, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$ColumnCheck
        /// </summary>
        internal class RoleColumnCheckBookmark : SystemBookmark
        {
            /// <summary>
            /// 3 enumerators for implementation!
            /// </summary>
            readonly ABookmark<long,Check> _inner;
            readonly ABookmark<string,Selector> _middle;
            readonly ABookmark<long,object> _outer;
            /// <summary>
            /// create the Sys$ColumnCheck enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleColumnCheckBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object>outer,
                ABookmark<string, Selector> middle,ABookmark<long,Check> inner)
                : base(_cx,res,pos,inner.key())
            {
                _outer = outer;
                _middle = middle;
                _inner = inner;
                _cx.Add(res.qry, this);
            }
            internal static RoleColumnCheckBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null;
                    outer = outer.Next())
                    if (outer.value() is Table tb)
                        for (var middle = tb.rowType.names.First(); middle != null;
                                middle = middle.Next())
                            if (middle.value() is TableColumn tc)
                                for (var inner = tc.constraints.First(); inner != null;
                                        inner = inner.Next())
                                    if (inner.value() is Check ck)
                                    {
                                        var rb = new RoleColumnCheckBookmark(_cx,res, 0, outer,
                                            middle, inner);
                                        if (Query.Eval(res.qry.where, res._tr,_cx))
                                            return rb;
                                    }
                return null;
            }
            /// <summary>
            /// The current key: (CheckPos)
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value: Table,Column,Check,Select,Pos)
            /// </summary>
            public override TRow CurrentValue()
            {
                var tb = _outer.value() as Table;
                var tc = _middle.value() as TableColumn;
                var ck = _inner.value() as Check;
                return new TRow(res.from.rowType, Pos(_defpos),
                    new TChar(tb.name),
                    new TChar(tc.name),
                    new TChar(ck.name),
                    new TChar(ck.source));
            }
            /// <summary>
            /// Move to the next ColumnCheck
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                for (; ; )
                {
                    if (inner != null && (inner=inner.Next())!= null)
                    {
                        var rb = new RoleColumnCheckBookmark(_cx,res, _pos + 1, outer, middle, inner);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                        continue;
                    }
                    if (middle != null && (middle=middle.Next())!= null)
                    {
                        var rt = middle.value() as TableColumn;
                        if (rt == null)
                            continue;
                        inner = rt.constraints.First();
                        continue;
                    }
                    if ((outer = outer.Next()) == null)
                        return null;
                    var tb = outer.value() as Table;
                    if (tb == null)
                        continue;
                    middle = tb.rowType.names.First();
                    if (inner != null)
                    {
                        var rb = new RoleColumnCheckBookmark(_cx,res, _pos + 1, outer, middle, inner);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                        continue;
                    }
                }
            }
        }
        /// <summary>
        /// set up the Role$ColumnPrivileges table
        /// </summary>
        static Database RoleColumnPrivilegeResults(Database d)
        {
            var t = new SystemTable("Role$ColumnPrivilege");
            t+=new SystemTableColumn(t, "Table", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Column", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Grantee", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Privilege", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$columnPrivilege
        /// </summary>
        internal class RoleColumnPrivilegeBookmark : SystemBookmark
        {
            /// <summary>
            /// 2 Enumerators for implementation
            /// </summary>
            readonly ABookmark<long,object> _middle;
            readonly ABookmark<long, object> _inner;
            /// <summary>
            /// create the Sys$ColumnUser enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleColumnPrivilegeBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object> middle,ABookmark<long,object> inner)
                : base(_cx,res,pos,inner.key())
            {
                _middle = middle;
                _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RoleColumnPrivilegeBookmark New(Context _cx, SystemRowSet res)
            {
                for (var middle = res._tr.schemaRole.objects.PositionAt(0); middle != null; middle = middle.Next())
                    if (middle.value() is Selector sc)
                        for (var inner = res._tr.role.objects.PositionAt(0); inner != null; inner = inner.Next())
                            if (inner.value() is Role ro && ro.objects[sc.defpos] is Selector rc)
                            {
                                var rb = new RoleColumnPrivilegeBookmark(_cx,res, 0, middle, inner);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current key: ColPos,UserPos
            /// </summary>
            public override TRow CurrentKey()
            {
                var ro = _inner.value() as Role;
                var tc = _middle.value() as Selector;
                var tb = ro.objects[tc.tabledefpos] as Table;
                return new TRow(new TColumn("Table",new TChar(tb.name)),
                    new TColumn("Name", new TChar(tc.name)),
                    new TColumn("Grantee", new TChar(ro.name)));
            }
            /// <summary>
            /// the current value: (Table,Column,GranteeType,Grantee,Privilege)
            /// </summary>
            public override TRow CurrentValue()
            {
                var ro = _inner.value() as Role;
                var tc = _middle.value() as Selector;
                var tb = ro.objects[tc.tabledefpos] as Table;
                return new TRow(res.from.rowType,
                    new TChar(tb.name),
                    new TChar(tc.name),
                    new TChar(ro.name),
                    new TChar(tc.priv.ToString()));
            }
            /// <summary>
            /// Move to the next ColumnPrivilege data
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var middle = _middle;
                var inner = _inner;
                if (inner != null && (inner = inner.Next()) != null
                    && inner.value() is Selector sc && sc.defpos == middle.key())
                {
                    var rb = new RoleColumnPrivilegeBookmark(_cx,res, _pos + 1, middle, inner);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (middle = middle.Next(); middle != null; middle = middle.Next())
                    if (middle.value() is Selector mc)
                        for (inner = _rs._tr.role.objects.PositionAt(0); inner != null; inner = inner.Next())
                            if (inner.value() is Role ro && ro.objects[mc.defpos] is Selector rc && rc.defpos == mc.defpos)
                            {
                                var rb = new RoleColumnPrivilegeBookmark(_cx,res, _pos + 1, middle, inner);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$Domain table
        /// </summary>
        static Database RoleDomainResults(Database d)
        {
            var t = new SystemTable("Role$Domain");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "DataType", 2, Domain.Char);
            t+=new SystemTableColumn(t, "DataLength", 3, Domain.Int);
            t+=new SystemTableColumn(t, "Scale", 4, Domain.Int);
            t+=new SystemTableColumn(t, "StartField", 5, Domain.Char);
            t+=new SystemTableColumn(t, "EndField", 6, Domain.Char);
            t+=new SystemTableColumn(t, "DefaultValue", 7, Domain.Char);
            t+=new SystemTableColumn(t, "Struct", 8, Domain.Char);
            t+=new SystemTableColumn(t, "Definer", 9, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// and enumerator for Role$Domain
        /// </summary>
        internal class RoleDomainBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the domains tree
            /// </summary>
            readonly ABookmark<long,object> _en;
            /// <summary>
            /// create the Sys$Domain enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleDomainBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer)
                : base(_cx,res,pos,outer.key())
            {
                _en = outer;_cx.Add(res.qry, this);
            }
            internal static RoleDomainBookmark New(Context _cx, SystemRowSet res)
            {
                for (var b = res._tr.role.objects.PositionAt(0);b!= null;b=b.Next())
                    if (b.value() is Domain dm)
                    {
                        var rb =new RoleDomainBookmark(_cx,res, 0, b);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current key: Pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value: (Pos,Name,DataType,dataLength,Scale,DefaultValue,NotNull,Struct)
            /// </summary>
            public override TRow CurrentValue()
            {
                long prec = 0;
                long scale = 0;
                var dm = (Domain)_en.value();
                if (dm.kind == Sqlx.NUMERIC || dm.kind == Sqlx.REAL)
                {
                    prec = dm.prec;
                    scale = dm.scale;
                }
                if (dm.kind == Sqlx.CHAR || dm.kind == Sqlx.NCHAR)
                    prec = dm.prec;
                string start = "";
                string end = "";
                if (dm.kind == Sqlx.INTERVAL)
                {
                    start = dm.start.ToString();
                    end = dm.end.ToString();
                }
                string elname = "";
                if (dm.elType is Domain et)
                        elname = et.name;
                return new TRow(res.from.rowType,
                    Pos(_defpos),
                    new TChar(dm.name),
                    new TChar(dm.kind.ToString()),
                    new TInt(prec),
                    new TInt(scale),
                    new TChar(start),
                    new TChar(end),
                    new TChar(dm.defaultValue.ToString()),
                    new TChar(elname),
                    new TChar(_rs._tr.roles[dm.definer].name));
            }
            /// <summary>
            /// Move to next Domain
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _en;
                for (outer = outer.Next();outer!= null;outer=outer.Next())
                if (outer.value() is Domain)
                    {
                        var rb = new RoleDomainBookmark(_cx,res, _pos + 1, outer);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$Index table
        /// </summary>
        static Database RoleIndexResults(Database d)
        {
            var t = new SystemTable("Role$Index");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Table", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Flags", 3, Domain.Char);
            t+=new SystemTableColumn(t, "RefTable", 4, Domain.Char);
            t+=new SystemTableColumn(t, "RefIndex", 5, Domain.Char);
            t+=new SystemTableColumn(t, "Distinct", 6, Domain.Char); // probably ""
            t+=new SystemTableColumn(t, "Adapter", 7, Domain.Char);
            t+=new SystemTableColumn(t, "Rows", 8, Domain.Int);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Index
        /// </summary>
        internal class RoleIndexBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the indexes
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<long, Index> _inner;
            /// <summary>
            /// craete the Sys$Index enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleIndexBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object> outer,
                ABookmark<long,Index>inner)
                : base(_cx,res,pos,inner.key())
            {
                _outer = outer;
                _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RoleIndexBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table tb)
                        for (var inner = tb.indexes.First(); inner != null; inner = inner.Next())
                        {
                            var rb = new RoleIndexBookmark(_cx,res, 0, outer,inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                return null;
            }
            /// <summary>
            /// the current key: Pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value: (Pos,Table,Name,Flags,RefTable,RefIndex,Distinct)
            /// </summary>
            public override TRow CurrentValue()
            {
                Index x = _inner.value();
                Table t = (Table)res._tr.role.objects[x.tabledefpos];
                Index rx = (Index)res._tr.role.objects[x.refindexdefpos];
                Table rt = (Table)res._tr.role.objects[x.reftabledefpos];
                return new TRow(res.from.rowType,
                   Pos(x.defpos),
                   new TChar(t.name),
                   new TChar(x.name),
                   new TChar(x.flags.ToString()),
                   new TChar(rt?.name??""),
                   new TChar(rx?.name??""),
                   new TChar(rx?.rows.Count.ToString()??""),
                   new TChar(x.adapter?.name),
                   new TInt(x.rows.Count)
                   );
            }
            /// <summary>
            /// Move to next index
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                for (inner=inner.Next();inner!=null;inner=inner.Next())
                {
                    var rb = new RoleIndexBookmark(_cx,res, _pos + 1, outer, inner);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (outer=outer.Next();outer!=null;outer=outer.Next())
                    if (outer.value() is Table tb)
                        for (inner=tb.indexes.First();inner!=null;inner=inner.Next())
                        {
                            var rb = new RoleIndexBookmark(_cx,res, _pos + 1, outer, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$IndxeKey table
        /// </summary>
        static Database RoleIndexKeyResults(Database d)
        {
            var t = new SystemTable("Role$IndexKey");
            t+=new SystemTableColumn(t, "IndexName", 0, Domain.Char);
            t+=new SystemTableColumn(t, "TableColumn", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Position", 2, Domain.Int);
            t+=new SystemTableColumn(t, "Flags", 3, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$IndexKey
        /// </summary>
        internal class RoleIndexKeyBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _outer;
            readonly ABookmark<long, Index> _middle;
            readonly ABookmark<int, Selector> _inner;
            /// <summary>
            /// create the Role$IndexKey enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleIndexKeyBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object> outer,
                ABookmark<long,Index> middle, ABookmark<int,Selector> inner)
                : base(_cx,res,pos,inner.value().defpos)
            {
                _outer = outer; _middle = middle; _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RoleIndexKeyBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table tb)
                        for (var middle = tb.indexes.First(); middle != null; middle = middle.Next())
                            for (var inner = middle.value().cols.First(); inner != null; inner = inner.Next())
                            {
                                var rb = new RoleIndexKeyBookmark(_cx,res, 0, outer,middle,inner);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current key: (Pos,Ix)
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(
                    new TColumn("Pos", new TInt(_defpos)),
                    new TColumn("Ix", new TInt(_inner.value().seq)));
            }
            /// <summary>
            /// the current value: (Indexname,TableColumn,Position)
            /// </summary>
            public override TRow CurrentValue()
            {
                return new TRow(res.from.rowType,
                    new TChar(_middle.value().name),
                    new TChar(_inner.value().name),
                    new TInt(_inner.value().seq),
                    new TChar(""));
            }
            /// <summary>
            /// Move to next Indexkey data
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                for (inner = inner.Next();inner!=null;inner=inner.Next())
                {
                    var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, middle, inner);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (middle=middle.Next();middle!=null;middle=middle.Next())
                    for (inner=middle.value().cols.First();inner!=null;inner=inner.Next())
                    {
                        var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, middle, inner);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                for (outer = outer.Next(); outer!=null;outer=outer.Next())
                    if (outer.value() is Table tb)
                        for (middle = tb.indexes.First(); middle != null; middle = middle.Next())
                            for (inner = middle.value().cols.First(); inner != null; inner = inner.Next())
                            {
                                var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, middle, inner);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$Java table
        /// </summary>
        static Database RoleJavaResults(Database d)
        {
            var t = new SystemTable("Role$Java");
            t+=new SystemTableColumn(t, "Name", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Key", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Definition", 2, Domain.Char);
            return t.Add(d);
        }
        internal class RoleJavaBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _enu;
            RoleJavaBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> e) 
                : base(_cx,res, pos,e.key())
            {
                _enu = e;_cx.Add(res.qry, this);
            }
            internal static RoleJavaBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is DBObject tb && (tb is Table || tb is View)
                        && !(tb is RestView))
                    {
                        var rb = new RoleJavaBookmark(_cx,res, 0, outer);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                var enu = _enu;
                for (enu = enu.Next(); enu != null; enu = enu.Next())
                    if (enu.value() is DBObject tb && (tb is Table || tb is View) && !(tb is RestView))
                    {
                        var rb = new RoleJavaBookmark(_cx,res, _pos + 1, enu);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Name", new TInt(_enu.key())));
            }
            public override TRow CurrentValue()
            {
                return ((DBObject)_enu.value()).RoleJavaValue(res._tr, res.from, _enu);
            }
        }
        /// <summary>
        /// set up the Role$Python table
        /// </summary>
        static Database RolePythonResults(Database d)
        {
            var t = new SystemTable("Role$Python");
            t+=new SystemTableColumn(t, "Name", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Key", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Definition", 2, Domain.Char);
            return t.Add(d);
        }
        internal class RolePythonBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _enu;
            RolePythonBookmark(Context _cx, SystemRowSet res, int pos, 
                ABookmark<long, object> e) : base(_cx,res, pos,e.key())
            {
                _enu = e;_cx.Add(res.qry, this);
            }
            internal static RolePythonBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is DBObject tb && (tb is Table || tb is View) && !(tb is RestView))
                    {
                        var rb = new RolePythonBookmark(_cx,res, 0, outer);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            public override RowBookmark Next(Context _cx)
            {
                var enu = _enu;
                for (enu=enu.Next();enu!=null;enu=enu.Next())
                    if (enu.value() is DBObject tb && (tb is Table || tb is View) && !(tb is RestView))
                    {
                        var rb = new RolePythonBookmark(_cx,res, _pos+1, enu);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Name", new TInt(_enu.key())));
            }
            public override TRow CurrentValue()
            {
                return ((DBObject)_enu.value()).RolePythonValue(res._tr, res.from, _enu);
            }
        }
        /// <summary>
        /// set up the Role$Procedure table
        /// </summary>
        static Database RoleProcedureResults(Database d)
        {
            var t = new SystemTable("Role$Procedure");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Arity", 2, Domain.Int);
            t+=new SystemTableColumn(t, "Returns", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Definition", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Inverse", 5, Domain.Char);
            t+=new SystemTableColumn(t, "Monotonic",6, Domain.Bool);
            t+=new SystemTableColumn(t, "Definer", 7, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Procedure
        /// </summary>
        internal class RoleProcedureBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the procedures tree
            /// </summary>
            readonly ABookmark<long,object> _en;
            /// <summary>
            /// create the Sys$Procedure enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleProcedureBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> en)
                : base(_cx,res,pos,en.key())
            {
                _en = en;_cx.Add(res.qry, this);
            }
            internal static RoleProcedureBookmark New(Context _cx, SystemRowSet res)
            {
                for (var en = res._tr.role.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is Procedure && !(en.value() is Method))
                    {
                        var rb =new RoleProcedureBookmark(_cx,res, 0, en);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current key: Pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value: (Pos,Name,Definition)
            /// </summary>
            public override TRow CurrentValue()
            {
                Procedure p = (Procedure)_en.value();
                var s = p.name;
                string inv = "";
                if (p.inverse>0)
                    inv = ((Procedure)res._tr.role.objects[p.inverse]).name;
                return new TRow(res.from.rowType,
                    Pos(_defpos),
                    new TChar(s),
                    new TInt(p.arity),
                    new TChar(p.retType.ToString()),
                    new TChar(p.clause),
                    new TChar(inv),
                    p.monotonic ? TBool.True : TBool.False,
                    new TChar(res._tr.roles[p.definer].name));
            }
            /// <summary>
            /// Move to the next procedure
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var en = _en;
                for (en = en.Next(); en != null; en = en.Next())
                    if (en.value() is Procedure ob && !(ob is Method))
                    {
                        var rb = new RoleProcedureBookmark(_cx,res, _pos + 1, en);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
        }
        static Database RoleParameterResults(Database d)
        {
            var t = new SystemTable("Role$Parameter");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t,"Seq",1, Domain.Int);
            t+=new SystemTableColumn(t,"Name" ,2, Domain.Char);
            t+=new SystemTableColumn(t,"Type",3, Domain.Char);
            t+=new SystemTableColumn(t,"Mode",4, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Parameter
        /// </summary>
        internal class RoleParameterBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the procedures tree
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<int, ProcParameter> _inner;
            /// <summary>
            /// create the Rle$Parameter enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleParameterBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> en,
                ABookmark<int,ProcParameter> inner)
                : base(_cx,res,pos,en.key())
            {
                _outer = en;
                _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RoleParameterBookmark New(Context _cx, SystemRowSet res)
            {
                for (var en = res._tr.role.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is Procedure p && p.arity > 0)
                        for (var inner = p.ins.First(); inner != null; inner = inner.Next())
                        {
                            var rb = new RoleParameterBookmark(_cx,res, 0, en, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                return null;
            }
            /// <summary>
            /// the current key: Pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)),
                    new TColumn("Seq",new TInt(_inner.key())));
            }
            /// <summary>
            /// the current value: (Pos,Name,Definition)
            /// </summary>
            public override TRow CurrentValue()
            {
                ProcParameter pp = _inner.value();
                return new TRow(res.from.rowType,
                    Pos(_defpos),
                    new TInt(_inner.key()),
                    new TChar(pp.label),
                    new TChar(pp.dataType.ToString()),
                    new TChar(((pp.result==Sqlx.RESULT) ? Sqlx.RESULT : pp.paramMode).ToString()));
            }
            /// <summary>
            /// Move to the next parameter
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                {
                    var rb = new RoleParameterBookmark(_cx,res, _pos + 1, outer, inner);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    if (outer.value() is Procedure p)
                        for (inner = p.ins.First(); inner != null; inner = inner.Next())
                        {
                            var rb = new RoleParameterBookmark(_cx,res, _pos + 1, outer, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                return null;
            }
        }
        static Database RoleSubobjectResults(Database d)
        {
            var t = new SystemTable("Role$Subobject");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Type", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Seq", 3, Domain.Int);
            t+=new SystemTableColumn(t, "Column", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Subobject", 5, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Object
        /// </summary>
        internal class RoleSubobjectBookmark : SystemBookmark
        {
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<long,bool> _inner;
            /// <summary>
            /// create the Sys$Procedure enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleSubobjectBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> en,
                ABookmark<long,bool> ix)
                : base(_cx,res,pos,en.key())
            {
                _outer = en;
                _inner = ix;_cx.Add(res.qry, this);
            }
            internal static RoleSubobjectBookmark New(Context _cx, SystemRowSet res)
            {
                for (var en = res._tr.role.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is DBObject ob)
                        for (var ix = ob.dependents.First(); ix != null; ix = ix.Next())
                    {
                        var rb = new RoleSubobjectBookmark(_cx,res, 0, en, ix);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current key: Name,Type
            /// </summary>
            public override TRow CurrentKey()
            {
                var ob = (DBObject)_outer.value();
                return new TRow(
                    new TColumn("Type", new TChar(ob.GetType().Name)),
                    new TColumn("Seq",new TInt(_inner.key())));
            }
            /// <summary>
            /// the current value: (Name,Type,Owner,Source)
            /// </summary>
            public override TRow CurrentValue()
            {
                var ob = (DBObject)_outer.value();
                var sb = (DBObject)_rs._tr.role.objects[_inner.key()];
                return new TRow(res.from.rowType,
                    Pos(_defpos),
                    new TChar(ob.GetType().Name),
                    new TChar(ob.name),
                    new TInt(_inner.key()),
                    new TChar(sb.name),
                    Pos(_inner.key()));
            }
            /// <summary>
            /// Move to the next object
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                {
                    var rb = new RoleSubobjectBookmark(_cx,res, _pos + 1, outer, inner);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    if (outer.value() is DBObject ob)
                        for (inner = ob.dependents.First(); inner != null; inner = inner.Next())
                        {
                            var rb = new RoleSubobjectBookmark(_cx,res, _pos + 1, outer, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                return null;
            }
        }

        /// <summary>
        /// set up the Role$Table table
        /// </summary>
        static Database RoleTableResults(Database d)
        {
            var t = new SystemTable("Role$Table");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Columns", 2, Domain.Int);
            t+=new SystemTableColumn(t, "Rows", 3, Domain.Int);
            t+=new SystemTableColumn(t, "Triggers", 4, Domain.Int);
            t+=new SystemTableColumn(t, "CheckConstraints", 5, Domain.Int);
            t+=new SystemTableColumn(t, "References", 6, Domain.Int);
            t+=new SystemTableColumn(t, "RowIri", 7, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Table
        /// </summary>
        internal class RoleTableBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the tables tree
            /// </summary>
            readonly ABookmark<long,object> _en;
            /// <summary>
            /// create the Role$Table enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleTableBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> en)
                : base(_cx,res,pos,en.key())
            {
                _en = en;_cx.Add(res.qry, this);
            }
            internal static RoleTableBookmark New(Context _cx, SystemRowSet res)
            {
                for (var b = res._tr.role.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is Table t)
                    {
                        var rb =new RoleTableBookmark(_cx,res, 0, b);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current key: Pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)));
            }
            /// <summary>
            /// the current value (Pos,Name,Columns,Rows,Triggers,CheckConstraints,Changes,References,Owner)
            /// </summary>
            public override TRow CurrentValue()
            {
                Table t = (Table)_en.value();
                return new TRow(res.from.rowType,
                    Pos(_defpos),
                    new TChar(t.name),
                    new TInt(t.rowType.Length),
                    new TInt(t.tableRows.Count),
                    new TInt(t.triggers.Count),
                    new TInt(t.tableChecks.Count),
                    new TInt(t.properties.Count),
                    new TChar(t.rowType.iri));
            }
            /// <summary>
            /// Move to next Table
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var en = _en;
                for (en=en.Next();en!=null;en=en.Next())
                    if (en.value() is Table t)
                    {
                        var rb =new RoleTableBookmark(_cx,res, _pos + 1, en);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$Trigger table
        /// </summary>
        static Database RoleTriggerResults(Database d)
        {
            var t = new SystemTable("Role$Trigger");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Flags", 2, Domain.Char);
            t+=new SystemTableColumn(t, "TableName", 3, Domain.Char);
            t+=new SystemTableColumn(t, "OldRow", 4, Domain.Char);
            t+=new SystemTableColumn(t, "NewRow", 5, Domain.Char);
            t+=new SystemTableColumn(t, "OldTable", 6, Domain.Char);
            t+=new SystemTableColumn(t, "NewTable", 6, Domain.Char);
            t+=new SystemTableColumn(t, "Def", 7, Domain.Char);
            t+=new SystemTableColumn(t, "Definer", 8, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Trigger
        /// </summary>
        internal class RoleTriggerBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for implementation
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<PTrigger.TrigType,BTree<long, Trigger>> _middle;
            readonly ABookmark<long,Trigger> _inner;
            /// <summary>
            /// create the Sys$Trigger enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleTriggerBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object> outer,
                ABookmark<PTrigger.TrigType,BTree<long,Trigger>>middle,
                ABookmark<long,Trigger>inner)
                : base(_cx,res,pos,inner.key())
            {
                _outer = outer;
                _middle = middle;
                _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RoleTriggerBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table t)
                        for (var middle = t.triggers.First(); middle != null; middle = middle.Next())
                            for (var inner = middle.value().First(); inner != null; inner = inner.Next())
                            {
                                var rb = new RoleTriggerBookmark(_cx,res, 0, outer, middle, inner);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current key: Pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_inner.key())));
            }
            /// <summary>
            /// the current value: (Pos,name,Flags,TableName,OldRow,NewRow,OldTable,NewTable,Def)
            /// </summary>
            public override TRow CurrentValue()
            {
                Trigger tg = (Trigger)_inner.value();
                Table tb = (Table)_outer.value();
                return new TRow(res.from.rowType,
                    Pos(tg.defpos),
                    new TChar(tg.name),
                    new TChar(tg.tgType.ToString()),
                    new TChar(tb.name),
                    new TChar(tg.oldRow),
                    new TChar(tg.newRow),
                    new TChar(tg.oldTable),
                    new TChar(tg.newTable),
                    new TChar(tg.def),
                    new TChar(((DBObject)res._tr.role.objects[tg.definer]).name));
            }            /// <summary>
            /// Move to the next Trigger
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                {
                    var rb = new RoleTriggerBookmark(_cx,res, _pos + 1, outer, middle, inner);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (middle=middle.Next();middle!=null;middle=middle.Next())
                    for (inner=middle.value().First();inner!=null;inner=inner.Next())
                        {
                            var rb = new RoleTriggerBookmark(_cx,res, _pos + 1, outer, middle, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                for (outer=outer.Next();outer!=null;outer=outer.Next())
                    if (outer.value() is Table t)
                        for (middle = t.triggers.First(); middle != null; middle = middle.Next())
                            for (inner = middle.value().First(); inner != null; inner = inner.Next())
                            {
                                var rb = new RoleTriggerBookmark(_cx,res, _pos + 1, outer, middle, inner);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$TriggerUpdateColumn table
        /// </summary>
        static Database RoleTriggerUpdateColumnResults(Database d)
        {
            var t = new SystemTable("Role$TriggerUpdateColumn");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "ColumnName", 2, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$TriggerUpdateColumn
        /// </summary>
        internal class RoleTriggerUpdateColumnBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for implementation
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<PTrigger.TrigType, BTree<long, Trigger>> _middle;
            readonly ABookmark<long,Trigger> _inner;
            readonly ABookmark<int, long> _fourth;
            /// <summary>
            /// the database
            /// </summary>
            readonly Database database;
            RoleTriggerUpdateColumnBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<PTrigger.TrigType, BTree<long, Trigger>> middle, ABookmark<long, Trigger> inner,
                ABookmark<int,long> fourth)
                : base(_cx,res, pos,inner.key())
            {
                _outer = outer;
                _middle = middle;
                _inner = inner;
                _fourth = fourth;_cx.Add(res.qry, this);
            }
            internal static RoleTriggerUpdateColumnBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table t)
                        for (var middle = t.triggers.First(); middle != null; middle = middle.Next())
                            for (var inner = middle.value().First(); inner != null; inner = inner.Next())
                                for (var fourth = inner.value().cols.First(); fourth != null; fourth = fourth.Next())
                                {
                                    var rb = new RoleTriggerUpdateColumnBookmark(_cx,res, 0, outer, middle, 
                                        inner, fourth);
                                    if (Query.Eval(res.qry.where, res._tr, _cx))
                                        return rb;
                                }
                return null;
            }
            /// <summary>
            /// the current key (Pos,sub)
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_defpos)),
                    new TColumn("Sub", new TInt(_fourth.key())));
            }
            /// <summary>
            /// the current value:
            /// </summary>
            public override TRow CurrentValue()
            {
                Trigger tg = (Trigger)_inner.value();
                TableColumn tc = (TableColumn)res._tr.role.objects[_fourth.value()];
                Table tb = (Table)_outer.value();
                return new TRow(res.from.rowType,
                    Pos(tg.defpos),
                    new TChar(tb.name),
                    new TChar(tc.name));
            }
            /// <summary>
            /// Move to next TriggerColumnUpdate
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                var fourth = _fourth;
                for (fourth = fourth.Next(); fourth != null; fourth = fourth.Next())
                {
                    var rb = new RoleTriggerUpdateColumnBookmark(_cx,res, _pos + 1, outer, middle, inner, fourth);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (inner=inner.Next();inner!=null;inner=inner.Next())
                    for (fourth = inner.value().cols.First(); fourth != null; fourth = fourth.Next())
                    {
                        var rb = new RoleTriggerUpdateColumnBookmark(_cx,res, _pos + 1, outer, middle, inner, fourth);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                for (middle=middle.Next();middle!=null;middle=middle.Next())
                    for (inner = middle.value().First(); inner != null; inner = inner.Next())
                        for (fourth = inner.value().cols.First(); fourth != null; fourth = fourth.Next())
                        {
                            var rb = new RoleTriggerUpdateColumnBookmark(_cx,res, _pos + 1, outer, middle, inner, fourth);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                for (outer=outer.Next();outer!=null;outer=outer.Next())
                    if (outer.value() is Table t)
                    for (middle = t.triggers.First(); middle != null; middle = middle.Next())
                        for (inner = middle.value().First(); inner != null; inner = inner.Next())
                            for (fourth = inner.value().cols.First(); fourth != null; fourth = fourth.Next())
                            {
                                var rb = new RoleTriggerUpdateColumnBookmark(_cx,res, _pos + 1, outer, middle, inner, fourth);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$Type table
        /// </summary>
        static Database RoleTypeResults(Database d)
        {
            var t = new SystemTable("Role$Type");
            t+=new SystemTableColumn(t, "Pos", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "SuperType", 2, Domain.Char);
            t+=new SystemTableColumn(t, "OrderFunc", 3, Domain.Char);
            t+=new SystemTableColumn(t, "OrderCategory", 4, Domain.Char);
            t+=new SystemTableColumn(t, "WithUri", 5, Domain.Char);
            t+=new SystemTableColumn(t, "Definer", 6, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Type
        /// </summary>
        internal class RoleTypeBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the domains tree
            /// </summary>
            readonly ABookmark<long,object> _en;
            /// <summary>
            /// the database
            /// </summary>
            readonly Database database;
            /// <summary>
            /// create the Sys$Type enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleTypeBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object> en)
                : base(_cx,res,pos,en.key())
            {
                _en = en;_cx.Add(res.qry, this);
            }
            internal static RoleTypeBookmark New(Context _cx, SystemRowSet res)
            {
                for (var en = res._tr.role.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is UDType)
                    {
                        var rb =new RoleTypeBookmark(_cx,res,0, en);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current key: pos
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Pos", new TInt(_en.key())));
            }
            /// <summary>
            /// the current value: (Pos,Name,SuperType)
            /// </summary>
            public override TRow CurrentValue()
            {
                UDType t = (UDType)_en.value();
                return new TRow(res.from.rowType,
                    Pos(t.ppos),
                    new TChar(t.name),
                    new TChar(((DBObject)res._tr.role.objects[t.underdefpos])?.name),
                    new TChar(t.orderFunc?.name),
                    new TChar((t.orderflags != OrderCategory.None) ? t.orderflags.ToString() : ""),
                    new TChar(t.withuri ?? ""),
                    new TChar(((DBObject)res._tr.role.objects[t.definer]).name));
            }
            /// <summary>
            /// Move to next Type
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var en = _en;
                for (en = en.Next(); en != null; en = en.Next())
                    if (en.value() is UDType)
                    {
                        var rb = new RoleTypeBookmark(_cx,res, _pos + 1, en);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                return null;
            }

        }
        /// <summary>
        /// set up the Role$Method table
        /// </summary>
        static Database RoleMethodResults(Database d)
        {
            var t = new SystemTable("Role$Method");
            t+=new SystemTableColumn(t, "Type", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Method", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Arity", 2, Domain.Int);
            t+=new SystemTableColumn(t, "MethodType", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Definition", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Definer", 5, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Method
        /// </summary>
        internal class RoleMethodBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for implementation
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<string, BList<Method>> _middle;
            readonly ABookmark<int,Method> _inner;
            /// <summary>
            /// the database
            /// </summary>
            readonly Database database;
            /// <summary>
            /// create the Role$Method enumerator
            /// </summary>
            /// <param name="r"></param>
            RoleMethodBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<string, BList<Method>> middle, ABookmark<int,Method> inner)
                : base(_cx,res,pos,inner.value().defpos)
            {
                _outer = outer;
                _middle = middle;
                _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RoleMethodBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is UDType ut)
                        for (var middle = ut.methods.First(); middle != null; middle = middle.Next())
                            for (var inner = middle.value().First(); inner != null; inner = inner.Next())
                            {
                                var rb = new RoleMethodBookmark(_cx,res, 0, outer, middle, inner);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current key: Pos,Methpos
            /// </summary>
            public override TRow CurrentKey()
            {
                UDType t = (UDType)_outer.value();
                return new TRow(
                    new TColumn("Type", new TChar(t.name)),
                    new TColumn("Method", new TChar(_inner.value().name)),
                    new TColumn("Arity", new TInt(_inner.value().arity)));
            }
            /// <summary>
            /// the current value: (Type,Method,Arity,MethodType,Proc)
            /// </summary>
            public override TRow CurrentValue()
            {
                UDType t = (UDType)_outer.value();
                Method p = _inner.value();
                return new TRow(res.from.rowType,
                   new TChar(t.name),
                   new TChar(p.name),
                   new TInt(p.arity),
                   new TChar(p.methodType.ToString()),
                   new TChar(p.clause),
                   new TChar(((DBObject)res._tr.role.objects[p.definer]).name));
            }
            /// <summary>
            /// Move to the next method
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                {
                    var rb = new RoleMethodBookmark(_cx,res, _pos + 1, outer, middle, inner);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (middle = middle.Next(); middle != null; middle = middle.Next())
                    for (inner = middle.value().First(); inner != null; inner = inner.Next())
                    {
                        var rb = new RoleMethodBookmark(_cx,res, _pos + 1, outer, middle, inner);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    if (outer.value() is UDType ut)
                        for (middle = ut.methods.First(); middle != null; middle = middle.Next())
                            for (inner = middle.value().First(); inner != null; inner = inner.Next())
                            {
                                var rb = new RoleMethodBookmark(_cx,res, _pos + 1, outer, middle, inner);
                                if (Query.Eval(res.qry.where, res._tr, _cx))
                                    return rb;
                            }
                return null;
            }
        }
        /// <summary>
        /// set up the Role$Privilege table
        /// </summary>
        static Database RolePrivilegeResults(Database d)
        {
            var t = new SystemTable("Role$Privilege");
            t+=new SystemTableColumn(t, "ObjectType", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Grantee", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Privilege", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Definer", 4, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// enumerate Role$Privilege
        /// </summary>
        internal class RolePrivilegeBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for implementation
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<long,Role> _inner;
            /// <summary>
            /// create the Sys$Privilege enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RolePrivilegeBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object>outer,
                ABookmark<long,Role>inner)
                : base(_cx,res,pos,outer.key())
            {
                _outer = outer;
                _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RolePrivilegeBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    for (var inner = res._tr.roles.First(); inner != null; inner = inner.Next())
                        if (inner.value().objects.Contains(outer.key()))
                        {
                            var rb = new RolePrivilegeBookmark(_cx,res, 0, outer, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                return null;
            }
            /// <summary>
            /// the current key: (not very useful)
            /// </summary>
            public override TRow CurrentKey()
            {
                return new TRow(new TColumn("Key", new TInt(_outer.key())));
            }
            /// <summary>
            /// the current value: (ObjectType,Name,GranteeType,Grantee,Privilege)
            /// </summary>
            public override TRow CurrentValue()
            {
                var ro = _inner.value();
                var t = (DBObject)ro.objects[_defpos];
                return new TRow(res.from.rowType,
                    new TChar(t.GetType().Name),
                    new TChar(t.name),
                    new TChar(ro.name),
                    new TChar(t.priv.ToString()),
                    new TChar(((DBObject)res._tr.role.objects[t.definer]).name));
            }
            /// <summary>
            /// Move to the next Role$Privilege data
            /// </summary>
            /// <returns>whethere there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer= _outer;
                var inner = _inner;
                for (inner=inner.Next();inner!=null;inner=inner.Next())
                    if (inner.value().objects.Contains(outer.key()))
                    {
                        var rb =new RolePrivilegeBookmark(_cx,res, _pos + 1, outer, inner);
                        if (Query.Eval(res.qry.where, res._tr, _cx))
                            return rb;
                    }
                for (outer=outer.Next();outer!=null;outer=outer.Next())
                    for (inner = res._tr.roles.First(); inner != null; inner = inner.Next())
                        if (inner.value().objects.Contains(outer.key()))
                        {
                            var rb = new RolePrivilegeBookmark(_cx,res, _pos + 1, outer, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                return null;
            }
        }
         /// <summary>
        /// set up the Role$Object table
        /// </summary>
        static Database RoleObjectResults(Database d)
        {
            var t = new SystemTable("Role$Object");
            t+=new SystemTableColumn(t, "Type", 0, Domain.Char); 
            t+=new SystemTableColumn(t, "Name", 1, Domain.Char);
            t+=new SystemTableColumn(t, "Source", 2, Domain.Char);
            t+=new SystemTableColumn(t, "Output", 3, Domain.Char);
            t+=new SystemTableColumn(t, "Description", 4, Domain.Char);
            t+=new SystemTableColumn(t, "Iri", 6, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Object
        /// </summary>
        internal class RoleObjectBookmark : SystemBookmark
        {
            readonly Database db;
            /// <summary>
            /// enumerate the RoleObject tree
            /// </summary>
            readonly ABookmark<long,object> _en;
            /// <summary>
            /// create the Role$Obejct enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleObjectBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> en)
                : base(_cx,res,pos,en.key())
            {
                _en = en;_cx.Add(res.qry, this);
            }
            internal static RoleObjectBookmark New(Context _cx, SystemRowSet res)
            {
                for (var bm = res._tr.role.objects.PositionAt(0); bm != null; bm = bm.Next())
                {
                    var ob = (DBObject)bm.value();
                    if (!(ob.mem.Contains(DBObject.Description)
                        || ob.mem.Contains(Domain.Iri)
                        || ob.mem.Contains(Table.ClientFormat)))
                        continue;
                    var rb = new RoleObjectBookmark(_cx,res, 0, bm);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                return null;
            }
            /// <summary>
            /// the current key: Name,Type
            /// </summary>
            public override TRow CurrentKey()
            {
                var ob = (DBObject)_en.value();
                return new TRow(
                    new TColumn("Type", new TChar(ob.GetType().Name)),
                    new TColumn("Name", new TChar(ob.name)));
            }
            /// <summary>
            /// the current value: (Name,Type,Owner,Source)
            /// </summary>
            public override TRow CurrentValue()
            {
                var ob = (DBObject)_en.value();
                return new TRow(res.from.rowType,
                    new TChar(ob.GetType().Name),
                    new TChar(ob.name),
                    new TChar((ob as Domain)?.provenance ?? ""),
                    new TChar((ob as Table)?.chartFormat.ToString() ?? ""),
                    new TChar(ob.description),
                    new TChar((ob as Domain)?.iri));
            }
            /// <summary>
            /// Move to the next object
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var en = _en;
                for (en=en.Next();en!=null ;en=en.Next())
                {
                    var ob = (DBObject)en.value();
                    if (!(ob.mem.Contains(DBObject.Description)
                        || ob.mem.Contains(Domain.Iri)
                        || ob.mem.Contains(Table.ClientFormat)))
                        continue;
                    var rb = new RoleObjectBookmark(_cx,res, _pos+1, en);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                return null;
             }
        }
        /// <summary>
        /// set up the Role$PrimaryKey table
        /// </summary>
        static Database RolePrimaryKeyResults(Database d)
        {
            var t = new SystemTable("Role$PrimaryKey");
            t+=new SystemTableColumn(t, "Table", 0, Domain.Char);
            t+=new SystemTableColumn(t, "Ordinal", 1, Domain.Int);
            t+=new SystemTableColumn(t, "Column", 2, Domain.Char);
            return t.Add(d);
        }
        /// <summary>
        /// an enumerator for Role$Primarykey
        /// </summary>
        internal class RolePrimaryKeyBookmark : SystemBookmark
        {
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<int, Selector> _inner;
            /// <summary>
            /// create the Sys$PrimaryKey enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RolePrimaryKeyBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object> outer,
                ABookmark<int,Selector>inner)
                : base(_cx,res,pos,inner.value().defpos)
            {
                _outer = outer;
                _inner = inner;_cx.Add(res.qry, this);
            }
            internal static RolePrimaryKeyBookmark New(Context _cx, SystemRowSet res)
            {
                for (var outer = res._tr.role.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table t)
                    for (var inner = t.FindPrimaryIndex().cols.First(); inner != null; inner = inner.Next())
                    {
                            var rb = new RolePrimaryKeyBookmark(_cx,res, 0, outer, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current key: (Table,Ordinal)
            /// </summary>
            public override TRow CurrentKey()
            {
                var t = (Table)_outer.value();
                return new TRow(new TColumn("Table", new TChar(t.name)),
                    new TColumn("Ordinal", new TInt(_inner.key())));
            }
            /// <summary>
            /// the current value(table,ordinal,ident)
            /// </summary>
            public override TRow CurrentValue()
            {
                var tb = (Table)_outer.value();
                return new TRow(res.from.rowType,
                    new TChar(tb.name),
                    new TInt(_inner.key()),
                    new TChar(_inner.value().name));
            }
            /// <summary>
            /// Move to next primary key data
            /// </summary>
            /// <returns>whether there is one</returns>
            public override RowBookmark Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                for (inner=inner.Next();inner!=null;inner=inner.Next())
                {
                    var rb = new RolePrimaryKeyBookmark(_cx,res, _pos+1, outer, inner);
                    if (Query.Eval(res.qry.where, res._tr, _cx))
                        return rb;
                }
                for (outer=outer.Next();outer!=null;outer=outer.Next())
                    if (outer.value() is Table t)
                        for (inner = t.FindPrimaryIndex().cols.First(); inner != null; inner = inner.Next())
                        {
                            var rb = new RolePrimaryKeyBookmark(_cx,res, _pos + 1, outer, inner);
                            if (Query.Eval(res.qry.where, res._tr, _cx))
                                return rb;
                        }
                return null;
            }
        }

#if PROFILES
        /// <summary>
        /// Make Transaction Profile Information accessible
        /// </summary>
        static void ProfileResults()
        {
            var t = new SystemTable("Profile$");
            new SystemTableColumn(t, "Id", 0, Domain.Int);
            new SystemTableColumn(t, "Occurrences",1,Domain.Int);
            new SystemTableColumn(t, "Fails", 2, Domain.Int);
            new SystemTableColumn(t, "Schema", 3, Domain.Bool);
        }
        /// <summary>
        /// An enumerator for the Profile$ table
        /// </summary>
        internal class ProfileBookmark : SystemBookmark
        {
            /// <summary>
            /// a local enumerator
            /// </summary>
            readonly int _ix;
            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="r"></param>
            /// <param name="m"></param>
            ProfileBookmark(SystemRowSet res, int ix)
                : base(res,ix)
            {
                db = res.database;
                _ix = ix;
            }
            internal static ProfileBookmark New(SystemRowSet res)
            {
                var db = res.database;
                if (db?.profile?.transactions == null || db.profile.transactions.Count == 0)
                    return null;
                for (var ix = 0; ix < db.profile.transactions.Count; ix++)
                {
                    var rb = new ProfileBookmark(res, ix);
                    if (Query.Eval(res.qry.where, res.tr, res))
                        return rb;
                }
                return null;
            }
            public override TRow CurrentKey()
            {
                if (db?.profile == null || _ix < 0 || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                return new TRow(res, new TColumn("Id", new TInt(p.id)));
            }
            public override TRow CurrentValue()
            {
                if (db?.profile == null || _ix < 0 || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                return new TRow(res.from.nominalDataType,new TInt(p.id), 
                        new TInt(p.num), new TInt(p.fails), TBool.For(p.schema));
            }
            public override RowBookmark Next()
            {
                for (; ; )
                {
                    if (db?.profile == null || _ix + 1 >= db.profile.transactions.Count)
                        return Null();
                    var rb = new ProfileBookmark(res, _ix + 1);
                    if (Query.Eval(res.qry.where, res.tr, res))
                        return rb;
                }
            }
        }
        static void ProfileTableResults()
        {
            var t = new SystemTable("Profile$Table");
            new SystemTableColumn(t, "Id", Sqlx.INTEGER);
            new SystemTableColumn(t, "Table", 0, Domain.Char);
            new SystemTableColumn(t, "BlockAny", Sqlx.BOOLEAN); 
            new SystemTableColumn(t, "Dels", Sqlx.INTEGER);
            new SystemTableColumn(t, "Index", 0, Domain.Char);
            new SystemTableColumn(t, "Pos", 0, Domain.Char);
            new SystemTableColumn(t, "ReadRecs", Sqlx.INTEGER);
            new SystemTableColumn(t, "Schema", Sqlx.BOOLEAN);
        }
        internal class ProfileTableBookmark : SystemBookmark
        {
            /// <summary>
            /// The database (used for friendly names)
            /// </summary>
            readonly Database db;
            readonly int _ix;
            readonly ABookmark<long, TableProfile> _inner;
            ProfileTableBookmark(SystemRowSet res,long pos,int ix,
                ABookmark<long,TableProfile> inner)
                : base(res,pos)
            {
                db = res.database;
                _ix = ix;
                _inner = inner;
            }
            internal static ProfileTableBookmark New(SystemRowSet res)
            {
                var db = res.database;
                if (db?.profile?.transactions == null)
                    return null;
                for (int i = 0; i < db.profile.transactions.Count; i++)
                    for (var inner = db.profile.transactions[i].tables.First(); inner != null; inner = inner.Next())
                    {
                        var rb = new ProfileTableBookmark(res, 0, i, inner);
                        if (Query.Eval(res.qry.where, res.tr, res))
                            return rb;
                    }
                return null;
            }
            public override RowBookmark Next()
            {
                var ix = _ix;
                var inner = _inner;
                for (;;)
                {
                    if (inner != null && (inner = inner.Next()) != null)
                        return new ProfileTableBookmark(res, _pos + 1, ix, inner);
                    ix = -1;
                    if (db?.profile == null || ix + 1 >= db.profile.transactions.Count)
                        return Null();
                    var p = db.profile.transactions[ix + 1];
                    inner = p.tables.First();
                    if (inner != null)
                    {
                        var rb = new ProfileTableBookmark(res, _pos + 1, ix, inner);
                        if (Query.Eval(res.qry.where, res.tr, res))
                            return rb;
                    }
                }
            }
            public override TRow CurrentKey()
            {
                if (db?.profile == null || _ix<0 || _ix>= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var t = db.objects[_inner.key()];
                return new TRow(res, new TColumn("Id", new TInt(p.id)),
                    new TColumn("Table", new TChar(t.NameInSession(db))));
            }
            public override TRow CurrentValue()
            {
                if (db?.profile == null || _ix < 0 || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];

                var t = db.objects[_inner.key()];
                var c = _inner.value();
                return new TRow(res.from.nominalDataType, new TInt(p.id), new TChar(t.NameInSession(db)),
                    TBool.For(c.blocked), new TInt(c.dels),
                     Pos(c.ckix), Pos(c.tab), new TInt(c.specific), TBool.For(c.schema));
            }
        }
        static void ProfileReadConstraintResults()
        {
            var t = new SystemTable("Profile$ReadConstraint");
            new SystemTableColumn(t, "Id", Sqlx.INTEGER);
            new SystemTableColumn(t, "Table", 0, Domain.Char);
            new SystemTableColumn(t, "ColPos", 0, Domain.Char);
            new SystemTableColumn(t, "ReadCol", 0, Domain.Char);
        }
        internal class ProfileReadConstraintBookmark : SystemBookmark
        {
            readonly Database db;
            readonly int _ix;
            readonly ABookmark<long, TableProfile> _middle;
            readonly ABookmark<long,bool> _inner;
            ProfileReadConstraintBookmark(SystemRowSet res,long pos,int ix,
                ABookmark<long,TableProfile> middle,ABookmark<long,bool>inner)
                : base(res,pos)
            {
                db = res.database;
                _ix = ix;
                _middle = middle;
                _inner = inner;
            }
            internal static ProfileReadConstraintBookmark New(SystemRowSet res)
            {
                var db = res.database;
                if (db?.profile?.transactions == null)
                    return null;
                for (int i = 0; i < db.profile.transactions.Count; i++)
                    for (var middle = db.profile.transactions[i].tables.First(); middle != null;
                        middle = middle.Next())
                        for (var inner = middle.value().read.First(); inner != null; inner = inner.Next())
                        {
                            var rb = new ProfileReadConstraintBookmark(res, 0, i, middle, inner);
                            if (Query.Eval(res.qry.where, res.tr, res))
                                return rb;
                        }
                return null;
            }
            public override RowBookmark Next()
            {
                var ix = _ix;
                var middle = _middle;
                var inner = _inner;
                for (;;)
                {
                    if (inner != null && (inner = inner.Next()) != null)
                        return new ProfileReadConstraintBookmark(res, _pos + 1, ix, middle, inner);
                    if (middle != null && (middle=middle.Next())!= null)
                    {
                        var tp = middle.value();
                        inner = tp.read.First();
                        continue;
                    }
                    if (db?.profile == null || ix + 1 >= db.profile.transactions.Count)
                        return Null();
                    var p = db.profile.transactions[++ix];
                    middle = p.tables.First();
                    if (middle != null)
                    {
                        var tp = middle.value();
                        inner = tp.read.First();
                        if (inner != null)
                        {
                            var rb = new ProfileReadConstraintBookmark(res, _pos + 1, ix, middle, inner);
                            if (Query.Eval(res.qry.where, res.tr, res))
                                return rb;
                        }
                    }
                }
            }
            public override TRow CurrentKey()
            {
                if (db?.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var t = db.objects[_middle.key()] as Table;
                if (db.objects[_inner.key()] is TableColumn c)
                    return new TRow(res, new TColumn("Id", new TInt(p.id)),
                        new TColumn("Table", new TChar(t.CurrentName(db))),
                            new TColumn("ColPos", new TChar(c.CurrentName(db))));
                return null;
            }
            public override TRow CurrentValue()
            {
                if (db?.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var t = db.objects[_middle.key()] as Table;
                if (db.objects[_inner.key()] is TableColumn c)
                    return new TRow(res.from.nominalDataType, new TInt(p.id),
                        new TChar(t.CurrentName(db)), Pos(c.defpos), new TChar(c.CurrentName(db)));
                return null;
            }
        }
         static void ProfileRecordResults()
        {
            var t = new SystemTable("Profile$Record");
            new SystemTableColumn(t, "Id", Sqlx.INTEGER);
            new SystemTableColumn(t, "Table", 0, Domain.Char);
            new SystemTableColumn(t, "Rid", 0, Domain.Char);
            new SystemTableColumn(t, "Recs", Sqlx.INTEGER);
        }
        internal class ProfileRecordBookmark : SystemBookmark
        {
            readonly Database db;
            readonly int _ix;
            readonly ABookmark<long, TableProfile> _middle;
            readonly int _rx;
            ProfileRecordBookmark(SystemRowSet res,long pos,int ix,
                ABookmark<long,TableProfile> middle,int rx)
                : base(res,pos)
            {
                db = res.database;
                _ix = ix;
                _middle = middle;
                _rx = rx;
            }
            internal static ProfileRecordBookmark New(SystemRowSet res)
            {
                var db = res.database;
                if (db?.profile?.transactions == null)
                    return null;
                for (int i = 0; i < db.profile.transactions.Count; i++)
                    for (var middle = db.profile.transactions[i].tables.First();
                        middle != null; middle = middle.Next())
                        if (middle.value().recs is List<RecordProfile> ls)
                            for (var ix = 0; ix < ls.Count; ix++)
                            {
                                var rb = new ProfileRecordBookmark(res, 0, i, middle, ix);
                                if (Query.Eval(res.qry.where, res.tr, res))
                                    return rb;
                            }
                return null;
            }
            public override RowBookmark Next()
            {
                var ix = _ix;
                var middle = _middle;
                var rx = _rx;
                for (;;)
                {
                    if (middle!=null &&  rx+1 < middle.value().recs.Count)
                    {
                        var rb =new ProfileRecordBookmark(res, _pos + 1, ix, middle, rx+1);
                        if (Query.Eval(res.qry.where, res.tr, res))
                            return rb;
                    }
                    if (middle != null && (middle = middle.Next())!= null)
                    {
                        rx = 0;
                        continue;
                    }
                    if (ix + 1 >= db.profile.transactions.Count)
                        return Null();
                    var p = db.profile.transactions[++ix];
                    middle = p.tables.First();
                    if (middle != null)
                        rx = -1;
                }
            }
            public override TRow CurrentKey()
            {
                if (db?.profile==null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var t = db.objects[_middle.key()];
                var r = _middle.value().recs[_rx];
                    return new TRow(res, new TColumn("Id",new TInt(p.id)),
                        new TColumn("Table",new TChar(t.NameInSession(db))),
                            new TColumn("Rid",new TInt(r.id)));
            }
            public override TRow CurrentValue()
            {
                if (db?.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var t = db.objects[_middle.key()];
                var r = _middle.value().recs[_rx];
                return new TRow(res.from.nominalDataType, new TInt(p.id),
                        new TChar(t.NameInSession(db)), new TInt(r.id), new TInt(r.num));
            }
        }
        static void ProfileRecordColumnResults()
        {
            var t = new SystemTable("Profile$RecordColumn");
            new SystemTableColumn(t, "Id", Sqlx.INTEGER);
            new SystemTableColumn(t, "Table", 0, Domain.Char);
            new SystemTableColumn(t, "Rid", 0, Domain.Char);
            new SystemTableColumn(t, "ColPos", 0, Domain.Char);
            new SystemTableColumn(t, "RecCol", 0, Domain.Char);
        }

        internal class ProfileRecordColumnBookmark : SystemBookmark
        {
            readonly Database db = null;
            readonly int _ix;
            readonly ABookmark<long, TableProfile> _second;
            readonly int _rx;
            readonly ABookmark<long,  TypedValue> _fourth;
            ProfileRecordColumnBookmark(SystemRowSet res,long pos,int ix,
                ABookmark<long,TableProfile> second,int rx, ABookmark<long,TypedValue> fourth)
                : base(res,pos)
            {
                db = res.database;
                _ix = ix;
                _second = second;
                _rx = rx;
                _fourth = fourth;
            }
            internal static ProfileRecordColumnBookmark New(SystemRowSet res)
            {
                var db = res.database;
                if (db?.profile?.transactions == null)
                    return null;
                for (int i = 0; i < db.profile.transactions.Count; i++)
                    for (var second = db.profile.transactions[i].tables.First();
                        second != null; second = second.Next())
                    {
                        var rcs = second.value()?.recs;
                        if (rcs != null)
                            for (int j = 0; j < rcs.Count; j++)
                                for (var fourth = rcs[j].fields.First(); fourth != null; fourth = fourth.Next())
                                {
                                    var rb = new ProfileRecordColumnBookmark(res, 0, i, second, j, fourth);
                                    if (Query.Eval(res.qry.where, res.tr, res))
                                        return rb;
                                }
                    }
                return null;
            }
            public override RowBookmark Next()
            {
                var ix = _ix;
                var second = _second;
                var rx = _rx;
                var fourth = _fourth;
                for (; ; )
                {
                    if (fourth != null && (fourth = fourth.Next()) != null)
                        return new ProfileRecordColumnBookmark(res, _pos + 1, ix, second, rx, fourth);
                    fourth = null;
                    if (second != null && rx + 1 < second.value().recs.Count)
                    {
                        var r = second.value().recs[++rx];
                        fourth = r.fields.First();
                        if (fourth != null)
                        {
                            var rb = new ProfileRecordColumnBookmark(res, _pos + 1, ix, second, rx, fourth);
                            if (Query.Eval(res.qry.where, res.tr, res))
                                return rb;
                        }
                        continue;
                    }
                    if (second != null && (second=second.Next())!= null)
                    {
                        rx = -1;
                        continue;
                    }
                    if (ix + 1 >= db.profile.transactions.Count)
                        return Null();
                    var p = db.profile.transactions[++ix];
                    second = p.tables.First();
                    rx = -1;
                }
            }
            public override TRow CurrentKey()
            {
                if (db?.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var t = db.objects[_second.key()] as Table;
                var r = _second.value().recs[_rx];
                if (db.objects[_fourth.key()] is TableColumn c)
                    return new TRow(res.keyType, new TInt(p.id),
                        new TChar(t.CurrentName(db)),
                            new TInt(r.id),
                                new TChar(c.CurrentName(db)));
                return null;
            }
            public override TRow CurrentValue()
            {
                if (db?.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var t = db.objects[_second.key()] as Table;
                var r = _second.value().recs[_rx];
                if (db.objects[_fourth.key()] is TableColumn c)
                    return new TRow(res.from.nominalDataType, new TInt(p.id),
                        new TChar(t.CurrentName(db)), new TInt(r.id), Pos(c.defpos), new TChar(c.CurrentName(db)));
                return null;
            }
        }
#endif
    }

}


