using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level4
{
    // shareable as of 26 April 2021
    internal class SystemTable : Table
    {
        internal const long
            SysCols = -175; // BTree<long,TableColumn>
        public BTree<long, TableColumn> sysCols =>
            (BTree<long, TableColumn>?)mem[SysCols] ?? BTree<long, TableColumn>.Empty;
        public BList<long?> sRowType => (BList<long?>?)mem[InstanceRowSet.SRowType] ?? BList<long?>.Empty;
        internal SystemTable(string n)
            : base(--_uid, new BTree<long, object>(ObInfo.Name, n)
                  + (_Domain, --_uid)
                  + (Infos, new BTree<long, ObInfo>(
                      (n.StartsWith("Log$") ? Database.schemaRole?.defpos 
                      : Database.guestRole?.defpos)??throw new PEException("PE1104"),
                      new ObInfo(n, Grant.AllPrivileges))))
        {
            var sys = Database._system ?? throw new PEException("PE1013");
            Database._system = sys + (this, 0) + (Domain.TableType.Relocate(_uid), 0);
        }
        protected SystemTable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SystemTable operator+(SystemTable s,(long,object)x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SystemTable(s.defpos, s.mem + x);
        }
        /// <summary>
        /// Unlike ordinary tables, system table columns are defined before the table is 
        /// added to the database. Moreover, system tables and columns cannot be altered.
        /// </summary>
        /// <param name="s"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static SystemTable operator+(SystemTable s,SystemTableColumn c)
        {
            return s + (SysCols, s.sysCols + (c.defpos, c))
                + (InstanceRowSet.SRowType, s.sRowType + c.defpos)
                + (TableCols, s.tableCols 
                    + (c.defpos, (Domain)(Database._system.objects[c.domain] ??throw new PEException("PE49211"))));
        }
        /// <summary>
        /// Accessor: Check object permissions
        /// </summary>
        /// <param name="db">The Database</param>
        /// <param name="priv">The privilege being checked for</param>
        /// <returns>the Role</returns>
        public override bool Denied(Context cx,Grant.Privilege priv)
        {
            var tr = cx.tr;
            if (priv != Grant.Privilege.Select || tr==null || tr.user==null)
                return true;
            // Role$ tables are public
            if (name.StartsWith("Role$"))
                return false;
            // All other systems tables are private to the database owner
            if (tr.user?.defpos != tr.owner)
                return true;
            return base.Denied(cx, priv);
        }
        public void Add()
        {
            var d = Database._system??throw new PEException("PE1014");
            var dr = d.role ?? throw new PEException("PE1015");
            var ro = dr + (Role.DBObjects, dr.dbobjects + (name, defpos));
            d = d + (this, 0) + (ro,-1);
            Database._system = d;
        }
        public SystemTable AddIndex(params string[] k)
        {
            var rt = BList<long?>.Empty;
            var rs = CTree<long, Domain>.Empty;
            foreach (var c in k)
                for (var b = sysCols.First(); b != null; b = b.Next())
                    if (b.value() is SystemTableColumn sc && sc.name == c)
                    {
                        rt += sc.defpos;
                        rs += (sc.defpos, (Domain)(Database._system.objects[sc.domain] ?? throw new PEException("PE0099")));
                        break;
                    }
            var ks = new Domain(--_uid, BTree<long, object>.Empty + (Domain.Kind, Sqlx.ROW) + (Domain.RowType, rt)
                + (Domain.Representation, rs));
            var x = new SystemIndex(defpos, ks);
            var sys = Database._system ?? throw new PEException("PE1014");
            Database._system = sys+ (x.defpos, x);
            var t = indexes[ks] ?? CTree<long, bool>.Empty;
            return this + (Indexes, indexes + (ks, t+(x.defpos,true)));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SystemTable(defpos,m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new SystemTable(dp, m);
        }
        internal override string NameFor(Context cx)
        {
            if (Denied(cx, Grant.Privilege.Select))
                throw new DBException("42105");
            return name;
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm,
            Grant.Privilege pr = Grant.Privilege.Select, string? a=null)
        {
            var r = new SystemRowSet(cx, q, this, null)+(_From,fm)+(_Ident,id);
            if (a != null)
                r += (_Alias, a);
            return r;
        }
    }
    // shareable as of 26 April 2021
    internal class SystemTableColumn : TableColumn
    {
        internal const long
            Key = -389;
        public readonly bool DBNeeded = true;
        public int isKey => (int)(mem[Key]??-1);
        /// <summary>
        /// A table column for a System or Log table
        /// </summary>
        /// <param name="sd">the database</param>
        /// <param name="n"></param>
        /// <param name="t">the dataType</param>
        internal SystemTableColumn(SystemTable t, string n, Domain dt, int k)
            : base(--_uid, BTree<long, object>.Empty + (ObInfo.Name, n) + (_Table, t.defpos) 
                  + (_Domain, dt.defpos) + (Key,k)
                  + (Infos, new BTree<long, ObInfo>(Database.schemaRole?.defpos??throw new PEException("PE1105"),
                      new ObInfo(n, Grant.AllPrivileges))))
        {
            var td = (Domain)(Database._system?.objects[t.domain]?? throw new PEException("PE1106"));
            td += (Domain.RowType, td.rowType + defpos);
            td += (Domain.Representation, td.representation + (defpos, dt));
            t += (Table.TableCols, t.tableCols + (defpos, dt));
            t += (SystemTable.SysCols, t.sysCols + (defpos, this));
            t += (_Domain, td.defpos);
            Database._system = Database._system + (this, 0)+(t,0)+(td,0);
        }
        protected SystemTableColumn(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SystemTableColumn operator+(SystemTableColumn s,(long,object) x)
        {
            var (dp, ob) = x;
            if (s.mem[dp] == ob)
                return s;
            return new SystemTableColumn(s.defpos, s.mem + x);
        }
        internal override string NameFor(Context cx)
        {
            return name??"?";
        }
        /// <summary>
        /// Accessor: Check object permissions
        /// </summary>
        /// <param name="db">The Database</param>
        /// <param name="priv">The privilege being checked for</param>
        /// <returns>the Role</returns>
        public override bool Denied(Context cx, Grant.Privilege priv)
        {
            var tr = cx.tr;
            if (!DBNeeded)
                return false;
            if (priv != Grant.Privilege.Select || tr==null)
                return true;
            // Sys$ tables are public
            if (name==null || name.StartsWith("Sys$") || name.StartsWith("Role$"))
                return false;
            // Log$ ROWS etc tables are private to the database owner
            if (tr?.user?.defpos == tr?.owner)
                return false;
            return base.Denied(cx, priv);
        }
    }
    // shareable as of 26 April 2021
    internal class SystemIndex : Level3.Index
    {
        public SystemIndex(long tb,Domain ks) 
            : base(--_uid,BTree<long, object>.Empty+(IndexConstraint,PIndex.ConstraintType.Unique)
                  +(Keys,ks)+(TableDefPos,tb))
        { }
    }
    // shareable as of 26 April 2021
    internal class SystemFilter
    {
        public readonly long col;
        public readonly TypedValue val1,val2;
        public readonly Sqlx op1,op2; // EQL, GTR etc
        public SystemFilter(long c,Sqlx o,TypedValue v,Sqlx o2=Sqlx.NO,TypedValue? v2=null) 
        { col = c; op1 = o; op2 = o2;  val1 = v; val2 = v2??TNull.Value; }
        internal TypedValue? Start(Context cx,SystemRowSet rs,string s,int i,bool desc)
        {
            var sf = rs.sysFilt;
            if (sf != null && sf[i] is SystemFilter fi && rs.sysIx!=null 
                && cx.obs[rs.sysIx.keys[i]??-1L] is SystemTableColumn stc && stc.name == s)
            {
                switch (op1)
                {
                    case Sqlx.EQL:
                        return fi.val1;
                    case Sqlx.GTR:
                    case Sqlx.GEQ:
                        if (!desc)return fi.val1;
                        break;
                    case Sqlx.LSS:
                    case Sqlx.LEQ:
                        if (desc) return fi.val1;
                        break;
                }
                switch (op2)
                {
                    case Sqlx.EQL:
                        return fi.val2;
                    case Sqlx.GTR:
                    case Sqlx.GEQ:
                        return desc ? null : fi.val2;
                    case Sqlx.LSS:
                    case Sqlx.LEQ:
                        return desc ? fi.val2 : null;
                }
            }
            return null;
        }
        internal static BTree<long,SystemFilter> Add(BTree<long,SystemFilter> sf,
            long c,Sqlx o, TypedValue v)
        {
            if (sf[c] is SystemFilter f)
            {
                if ((f.op1 == Sqlx.LSS || f.op1 == Sqlx.LEQ)
                    && (o==Sqlx.GTR || o==Sqlx.GEQ))
                    return sf + (c, new SystemFilter(c, f.op1, f.val1, o, v));
                else if ((f.op1 == Sqlx.GTR || f.op1 == Sqlx.GEQ)
                    && (o == Sqlx.LSS || o == Sqlx.LEQ))
                    return sf + (c, new SystemFilter(c, o, v, f.op1, f.val1));
                else
                    throw new DBException("42000");
            }
            return sf + (c, new SystemFilter(c, o, v));
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(DBObject.Uid(col));
            sb.Append(SqlValue.For(op1));
            sb.Append(val1);
            return sb.ToString();
        }
    }
    /// <summary>
    /// Perform selects from virtual 'system tables'
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class SystemRowSet : TableRowSet
    {
        internal const long
            SysFilt = -455, // BList<SysFilter>
            SysIx = -454,   // SystemIndex
            SysTable = -445; // SystemTable
        internal SystemTable? sysFrom => (SystemTable?)mem[SysTable];
        internal SystemIndex? sysIx => (SystemIndex?)mem[SysIx];
        internal BList<SystemFilter> sysFilt => (BList<SystemFilter>?)mem[SysFilt]??BList<SystemFilter>.Empty;
        /// <summary>
        /// Construct results for a system table.
        /// Independent of database, role, and user.
        /// Context is provided to be informed about the rowset.
        /// </summary>
        /// <param name="f">the from part</param>
        internal SystemRowSet(Context cx, Domain dm,SystemTable f, CTree<long, bool>? w = null)
            : base(cx.GetUid(), cx, f.defpos, _Mem(cx, dm, f, w))
        { }
        protected SystemRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, Domain dm,SystemTable f, CTree<long, bool>? w)
        {
            var mf = BTree<long,SystemFilter>.Empty;
            for (var b = w?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is SqlValue s)
                mf = s.SysFilter(cx, mf);
            SystemIndex? sx = null;
            var sf = BList<SystemFilter>.Empty;
            for (var b = f.indexes.First(); b != null; b = b.Next())
                for (var c = b.value().First(); c != null; c = c.Next())
                    if (Database._system.objects[c.key()] is SystemIndex x)
                    {
                        var xf = BList<SystemFilter>.Empty;
                        for (var sb = x.keys.First(); sb != null; sb = sb.Next())
                        {
                            var sc = sb.value();
                            for (var d = mf.First(); d != null; d = d.Next())
                            {
                                var fn = d.value();
                                if (sc == fn.col)
                                    xf += fn;
                            }
                        }
                        if (xf.Length > sf.Length)
                        {
                            sf = xf;
                            sx = x;
                        }
                    }
           var r = BTree<long, object>.Empty
                + (_Domain,dm.defpos) +(SRowType,(cx._Dom(f)??Domain.Content).rowType)+(SysTable, f) 
                + (SysFilt, sf);
            if (sx != null)
                r += (SysIx, sx);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SystemRowSet(defpos, m);
        }
        public static SystemRowSet operator +(SystemRowSet rs, (long, object) x)
        {
            var (dp, ob) = x;
            if (rs.mem[dp] == ob)
                return rs;
            return (SystemRowSet)rs.New(rs.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new SystemRowSet(dp, m);
        }
        internal static void Kludge(int k) // k is there to provoke another kludge
        { }
        /// <summary>
        /// Class initialisation: define the system tables
        /// </summary>
        static SystemRowSet()
        {
            // level 2 stuff
            LogResults();
            LogAlterResults();
            LogChangeResults();
            LogDeleteResults();
            LogDropResults();
            LogModifyResults();
            LogCheckResults();
            LogClassificationResults();
            LogClearanceResults();
            LogColumnResults();
            LogDateTypeResults();
            LogDomainResults();
            LogEditResults();
            LogEnforcementResults();
            LogGrantResults();
            LogIndexResults();
            LogIndexKeyResults();
            LogMetadataResults();
            LogOrderingResults();
            LogProcedureResults();
            LogRevokeResults();
            LogTableResults();
            LogTablePeriodResults();
            LogTriggerResults();
            LogTriggerUpdateColumnResults();
            LogTriggeredActionResults();
            LogEdgeTypeResults();
            LogTypeResults();
            LogTypeMethodResults();
            //          LogTypeUnionResults();
            LogViewResults();
            LogRecordResults();
            LogRecordFieldResults();
            LogTransactionResults();
            LogUpdateResults();
            LogUserResults();
            LogRoleResults();
            // level 3 stuff
            RoleClassResults();
            RoleColumnResults();
            RoleColumnCheckResults();
            RoleColumnPrivilegeResults();
            RoleDomainResults();
            RoleDomainCheckResults();
            RoleIndexResults();
            RoleIndexKeyResults();
            RoleJavaResults();
            RoleObjectResults();
            RoleParameterResults();
            RoleProcedureResults();
            RolePythonResults();
            RoleSubobjectResults();
            RoleTableResults();
            RolePrivilegeResults();
            RoleTableCheckResults();
            RoleTablePeriodResults();
            RoleTriggerResults();
            RoleTriggerUpdateColumnResults();
            RoleTypeResults();
            RoleMethodResults();
            SysRoleResults();
            SysUserResults();
            SysRoleUserResults();
            RoleViewResults();
            // level >=4 stuff
            SysClassificationResults();
            SysClassifiedColumnDataResults();
            SysEnforcementResults();
            SysGraphResults();
            SysAuditResults();
            SysAuditKeyResults();
            RolePrimaryKeyResults();
#if PROFILES
            // Profile tables
            ProfileResults();
            ProfileReadConstraintResults();
            ProfileRecordResults();
            ProfileRecordColumnResults();
            ProfileTableResults();
#endif
        }
        public override Cursor? First(Context _cx)
        {
            return _First(_cx);
        }
        /// <summary>
        /// Create the required bookmark
        /// </summary>
        /// <param name="r">the rowset to enumerate</param>
        /// <returns>the bookmark</returns>
        protected override Cursor? _First(Context _cx)
        {
            var res = this;
            if (res.sysFrom == null) // for kludge
                return null;
            SystemTable st = res.sysFrom;
            if (st.name.StartsWith("Log$"))
                _cx.db = _cx.db.BuildLog();
            switch (st.name)
            {
                // level 2 stuff
                case "Log$": return LogBookmark.New(_cx, res);
                case "Log$Alter": return LogAlterBookmark.New(_cx, res);
                case "Log$Change": return LogChangeBookmark.New(_cx, res);
                case "Log$Check": return LogCheckBookmark.New(_cx, res);
                case "Log$Classification": return LogClassificationBookmark.New(_cx, res);
                case "Log$Clearance": return LogClearanceBookmark.New(_cx, res);
                case "Log$Column": return LogColumnBookmark.New(_cx, res);
                case "Log$DateType": return LogDateTypeBookmark.New(_cx, res);
                case "Log$Delete": return LogDeleteBookmark.New(_cx, res);
                case "Log$Domain": return LogDomainBookmark.New(_cx, res);
                case "Log$Drop": return LogDropBookmark.New(_cx, res);
                case "Log$Edit": return LogEditBookmark.New(_cx, res);
                case "Log$Enforcement": return LogEnforcementBookmark.New(_cx, res);
                case "Log$Grant": return LogGrantBookmark.New(_cx, res);
                case "Log$Index": return LogIndexBookmark.New(_cx, res);
                case "Log$IndexKey": return LogIndexKeyBookmark.New(_cx, res);
                case "Log$Metadata": return LogMetadataBookmark.New(_cx, res);
                case "Log$Modify": return LogModifyBookmark.New(_cx, res);
                case "Log$Ordering": return LogOrderingBookmark.New(_cx, res);
                case "Log$Procedure": return LogProcedureBookmark.New(_cx, res);
                case "Log$Record": return LogRecordBookmark.New(_cx, res);
                case "Log$RecordField": return LogRecordFieldBookmark.New(_cx, res);
                case "Log$Revoke": return LogRevokeBookmark.New(_cx, res);
                case "Log$Role": return LogRoleBookmark.New(_cx, res);
                case "Log$Table": return LogTableBookmark.New(_cx, res);
                case "Log$TablePeriod": return LogTablePeriodBookmark.New(_cx, res);
                case "Log$Transaction": return LogTransactionBookmark.New(_cx, res);
                case "Log$Trigger": return LogTriggerBookmark.New(_cx, res);
                case "Log$TriggerUpdateColumn": return LogTriggerUpdateColumnBookmark.New(_cx, res);
                case "Log$TriggeredAction": return LogTriggeredActionBookmark.New(_cx, res);
                case "Log$Type": return LogTypeBookmark.New(_cx, res);
                case "Log$TypeMethod": return LogTypeMethodBookmark.New(_cx, res);
                //            case "Log$TypeUnion": return new LogTypeUnionEnumerator(r,matches);
                case "Log$Update": return LogUpdateBookmark.New(_cx, res);
                case "Log$User": return LogUserBookmark.New(_cx, res);
                case "Log$View": return LogViewBookmark.New(_cx, res);
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
                case "Sys$Graph": return SysGraphBookmark.New(_cx,res);
                case "Sys$Audit": return SysAuditBookmark.New(_cx, res);
                case "Sys$AuditKey": return SysAuditKeyBookmark.New(_cx, res);
                case "Role$PrimaryKey": return RolePrimaryKeyBookmark.New(_cx, res);
                default:
                    break;
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
        protected override Cursor? _Last(Context _cx)
        {
            var res = this;
            if (res.sysFrom == null) // for kludge
                return null;
            SystemTable st = res.sysFrom;
            return st.name switch
            {
                // level 2 stuff
                "Log$" => LogBookmark.New(res, _cx),
                "Log$Alter" => LogAlterBookmark.New(res, _cx),
                "Log$Change" => LogChangeBookmark.New(res, _cx),
                "Log$Check" => LogCheckBookmark.New(res, _cx),
                "Log$Classification" => LogClassificationBookmark.New(res, _cx),
                "Log$Clearance" => LogClearanceBookmark.New(res, _cx),
                "Log$Column" => LogColumnBookmark.New(res, _cx),
                "Log$DateType" => LogDateTypeBookmark.New(res, _cx),
                "Log$Delete" => LogDeleteBookmark.New(res, _cx),
                "Log$Domain" => LogDomainBookmark.New(res, _cx),
                "Log$Drop" => LogDropBookmark.New(res, _cx),
                "Log$Edit" => LogEditBookmark.New(res, _cx),
                "Log$Enforcement" => LogEnforcementBookmark.New(res, _cx),
                "Log$Grant" => LogGrantBookmark.New(res, _cx),
                "Log$Index" => LogIndexBookmark.New(res, _cx),
                "Log$IndexKey" => LogIndexKeyBookmark.New(res, _cx),
                "Log$Metadata" => LogMetadataBookmark.New(res, _cx),
                "Log$Modify" => LogModifyBookmark.New(res, _cx),
                "Log$Ordering" => LogOrderingBookmark.New(res, _cx),
                "Log$Procedure" => LogProcedureBookmark.New(res, _cx),
                "Log$Record" => LogRecordBookmark.New(res, _cx),
                "Log$RecordField" => LogRecordFieldBookmark.New(res, _cx),
                "Log$Revoke" => LogRevokeBookmark.New(res, _cx),
                "Log$Role" => LogRoleBookmark.New(res, _cx),
                "Log$Table" => LogTableBookmark.New(res, _cx),
                "Log$TablePeriod" => LogTablePeriodBookmark.New(res, _cx),
                "Log$Transaction" => LogTransactionBookmark.New(res, _cx),
                "Log$Trigger" => LogTriggerBookmark.New(res, _cx),
                "Log$TriggerUpdateColumn" => LogTriggerUpdateColumnBookmark.New(res, _cx),
                "Log$TriggeredAction" => LogTriggeredActionBookmark.New(res, _cx),
                "Log$EdgeType" => LogEdgeTypeBookmark.New(res, _cx),
                "Log$Type" => LogTypeBookmark.New(res, _cx),
                "Log$TypeMethod" => LogTypeMethodBookmark.New(res, _cx),
                "Log$Update" => LogUpdateBookmark.New(res, _cx),
                "Log$User" => LogUserBookmark.New(res, _cx),
                "Log$View" => LogViewBookmark.New(res, _cx),
                // level >=4 stuff
                "Sys$Audit" => SysAuditBookmark.New(res, _cx),
                "Sys$AuditKey" => SysAuditKeyBookmark.New(res, _cx),
                _ => null,
            };
        }
        internal TypedValue Start(Context cx, string s, int i = 0, bool desc = false)
        {
            return (sysFilt is BList<SystemFilter> sf && sf.Length>i)?
                (sf[i]?.Start(cx, this, s, i, desc)??TNull.Value):TNull.Value;
        }
        /// <summary>
        /// A bookmark for a system table
        /// // shareable as of 26 April 2021
        /// </summary>
        internal abstract class SystemBookmark : Cursor
        {
            /// <summary>
            /// the system rowset
            /// </summary>
            public readonly SystemRowSet res;
            /// <summary>
            /// base constructor for the system enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            protected SystemBookmark(Context cx, SystemRowSet r, int pos, long dpos,long pp,
                TRow rw)
                : base(cx, r, pos, new BTree<long,(long,long)>(r.defpos,(dpos,pp)), rw)
            {
                res = r;
            }
            protected static TypedValue Pos(long? p)
            {
                return new TPosition(p??-1L);
            }
            internal override BList<TableRow> Rec()
            {
                return BList<TableRow>.Empty;
            }
            protected static TypedValue Display(string d)
            {
                return new TChar(d);
            }
            internal bool Match(SystemRowSet rs)
            {
                for (var b = rs.sysFilt?.First(); b != null; b = b.Next())
                {
                    var sf = b.value();
                    var v = this[sf.col];
                    if (!Test(v, sf.op1, sf.val1))
                        return false;
                    if (sf.op1 != Sqlx.NO && !Test(v, sf.op2, sf.val2))
                        return false;
                }
                return true;
            }
            static bool Test(TypedValue v, Sqlx op, TypedValue w)
            {
                switch (op)
                {
                    case Sqlx.EQL:
                        if (v.CompareTo(w) != 0)
                            return false;
                        break;
                    case Sqlx.GTR:
                        if (v.CompareTo(w) < 0)
                            return false;
                        break;
                    case Sqlx.LSS:
                        if (v.CompareTo(w) >= 0)
                            return false;
                        break;
                    case Sqlx.LEQ:
                        if (v.CompareTo(w) > 0)
                            return false;
                        break;
                    case Sqlx.GEQ:
                        if (v.CompareTo(w) < 0)
                            return false;
                        break;
                }
                return true;
            }
        }
         /// <summary>
        /// A base class for enumerating log tables
        /// </summary>
        internal abstract class LogSystemBookmark : SystemBookmark
        {
            internal readonly Physical ph;
            internal readonly long nextpos;
            /// <summary>
            /// Construct the LogSystemBookmark
            /// </summary>
            /// <param name="r">the rowset</param>
            protected LogSystemBookmark(Context _cx,SystemRowSet r, int pos, 
                Physical p,long pp,TRow rw) 
                : base(_cx,r,pos,p.ppos,p.ppos,rw)
            {
                ph = p;
                nextpos = pp;
            }
        }
        /// <summary>
        /// Set up the Log$ table
        /// </summary>
        static void LogResults()
        {
            var t = new SystemTable("Log$");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Desc", Domain.Char,0);
            t+=new SystemTableColumn(t, "Type", Domain.Char,0);
            t+=new SystemTableColumn(t, "Affects", Domain.Position,0);
            t = t.AddIndex("Pos");
            t.Add();
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
            protected LogBookmark(Context cx,SystemRowSet r, int pos, Physical ph,long pp)
                : base(cx,r, pos, ph, pp ,_Value(cx,r,ph))
            { }
            internal static LogBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                if (_cx.db!=null){
                    var (nph,np) = _cx.db._NextPhysical(lb.key());
                    if (nph == null)
                        return null;
                    var rb = new LogBookmark(_cx, res, 0, nph, np);
                    if (!rb.Match(res))
                        return null;
                    if (Eval(res.where, _cx))
                        return rb;
                }
                return null;
            }
            internal static LogBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (_cx.db != null)
                    {
                        var (nph, np) = _cx.db._NextPhysical(lb.key());
                        if (nph == null)
                            return null;
                        var rb = new LogBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Desc,Type,Affects,Transaction)
            /// </summary>
            static TRow _Value(Context cx,SystemRowSet res,Physical ph)
            {
                return new TRow(cx._Dom(res)??Domain.Null,
                    Pos(ph.ppos),
                    new TChar(ph.ToString()),
                    new TChar(ph.type.ToString()),
                    Pos(ph.Affects));
            }
            /// <summary>
            /// Move to the next log entry
            /// </summary>
            /// <returns>whether there is a next entry</returns>
            protected override Cursor? _Next(Context _cx)
            {
                if (_cx.db!=null)
                for (var (nph,np) = _cx.db._NextPhysical(nextpos); nph != null; 
                        (nph,np) = _cx.db._NextPhysical(np))
                {
                     var rb = new LogBookmark(_cx,res, _pos+1, nph, np);
                    if (!rb.Match(res))
                        return null;
                    if (Eval(res.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log?.PositionAt(ph.ppos)?.Previous(); lb != null; lb = lb.Previous())
                    if (_cx.db != null)
                    {
                        var (nph,np) = _cx.db._NextPhysical(lb.key());
                        if (nph == null)
                            return null;
                        var rb = new LogBookmark(_cx, res, _pos + 1, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$Alter table
        /// </summary>
        static void LogAlterResults()
        {
            var t = new SystemTable("Log$Alter");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "DefPos", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
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
            LogAlterBookmark(Context _cx,SystemRowSet r,int pos,Physical ph, long pp)
                : base(_cx,r,pos,ph,pp, _Value(_cx, r, ph))
            {
            }
            internal static LogAlterBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log?.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Alter || lb.value() == Physical.Type.Alter2
                        || lb.value() == Physical.Type.Alter3)
                    {
                        var (nph, pp) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogAlterBookmark(_cx, res, 0, nph,pp);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogAlterBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Alter || lb.value() == Physical.Type.Alter2
                        || lb.value() == Physical.Type.Alter3)
                    {
                        var (nph, pp) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogAlterBookmark(_cx, res, 0, nph, pp);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current record (Pos,DefPos,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                var al = ph as PColumn ?? throw new PEException("PE1610");
                return new TRow(cx._Dom(res)?? throw new PEException("PE6610"),
                    Pos(al.ppos),
                    Pos(al.Affects));
            }
            /// <summary>
            /// move to the next Log$Alter entry
            /// </summary>
            /// <returns>whether there is a next entry</returns>
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log?.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Alter || lb.value() == Physical.Type.Alter2
                        || lb.value() == Physical.Type.Alter3)
                    {
                        var (nph, pp) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogAlterBookmark(_cx, res, 0, nph, pp);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log?.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Alter || lb.value() == Physical.Type.Alter2
                        || lb.value() == Physical.Type.Alter3)
                    {
                        var (nph, pp) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogAlterBookmark(_cx, res, 0, nph, pp);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$Change table
        /// </summary>
        static void LogChangeResults()
        {
            var t = new SystemTable("Log$Change");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Previous", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Change
        /// </summary>
        internal class LogChangeBookmark : LogSystemBookmark
        {
            LogChangeBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogChangeBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Change)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogChangeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogChangeBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Change)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogChangeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Previous,Name,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Change ch = (Change)ph;
                return new TRow(cx._Dom(res)??throw new PEException("PE6611"),
                    Pos(ch.ppos),
                    Pos(ch.Previous),
                    new TChar(ch.name));
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Change)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogChangeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Change)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogChangeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// setup the Log$Delete table
        /// </summary>
        static void LogDeleteResults()
        {
            var t = new SystemTable("Log$Delete");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "DelPos", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Delete
        /// </summary>
        internal class LogDeleteBookmark : LogSystemBookmark
        {
            LogDeleteBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogDeleteBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Delete||lb.value()==Physical.Type.Delete1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDeleteBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogDeleteBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Delete || lb.value() == Physical.Type.Delete1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDeleteBookmark(_cx, res, 0, nph,np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,DelPos,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Delete d = (Delete)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE6613"),
                    Pos(d.ppos),
                    Pos(d.delpos));
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Delete || lb.value()==Physical.Type.Delete1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDeleteBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Delete || lb.value() == Physical.Type.Delete1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDeleteBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$Drop table
        /// </summary>
        static void LogDropResults()
        {
            var t = new SystemTable("Log$Drop");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "DelPos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Drop
        /// </summary>
        internal class LogDropBookmark : LogSystemBookmark
        {
            LogDropBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogDropBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Drop || lb.value()==Physical.Type.Drop1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDropBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogDropBookmark? New(SystemRowSet res,Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Drop || lb.value() == Physical.Type.Drop1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDropBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,DelPos,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Drop d = (Drop)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE6614"),
                    Pos(d.ppos),
                    Pos(d.delpos));
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Drop || lb.value()==Physical.Type.Drop1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDropBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Drop || lb.value() == Physical.Type.Drop1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDropBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// Set up the Log$Metadata table
        /// </summary>
        static void LogMetadataResults()
        {
            var t = new SystemTable("Log$Metadata");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "DefPos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Description", Domain.Char,0);
            t+=new SystemTableColumn(t, "Output", Domain.Char,0);
            t+=new SystemTableColumn(t, "RefPos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Detail", Domain.Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class LogMetadataBookmark : LogSystemBookmark
        {
            LogMetadataBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogMetadataBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Metadata || lb.value() == Physical.Type.Metadata2
                        || lb.value() == Physical.Type.Metadata3)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogMetadataBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogMetadataBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Metadata || lb.value() == Physical.Type.Metadata2
                        || lb.value() == Physical.Type.Metadata3)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogMetadataBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Metadata||lb.value()==Physical.Type.Metadata2
                        ||lb.value()==Physical.Type.Metadata3)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogMetadataBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Metadata || lb.value() == Physical.Type.Metadata2
                        || lb.value() == Physical.Type.Metadata3)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogMetadataBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,DefPos,Name,Proc,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PMetadata m = (PMetadata)ph;
                var md = m.Metadata();
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0310"),
                    Pos(m.ppos),
                    Pos(m.defpos),
                    new TChar(m.name ?? throw new PEException("PE0311")),
                    new TChar(md.Contains(Sqlx.PASSWORD)?"*******":m.detail.ToString()),
                    new TChar(m.MetaFlags()),
                    Pos(m.refpos),
                    // At present m.iri may contain a password (this should get fixed)
                    new TChar((m.iri!="" && md.Contains(Sqlx.PASSWORD))?"*******":m.iri));
            }
         }
        /// <summary>
        /// Set up the Log$Modify table
        /// </summary>
        static void LogModifyResults()
        {
            var t = new SystemTable("Log$Modify");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "DefPos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Proc", Domain.Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// An enumerator for Log$Modify
        /// </summary>
        internal class LogModifyBookmark : LogSystemBookmark
        {
            LogModifyBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogModifyBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Modify)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogModifyBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogModifyBookmark? New( SystemRowSet res,Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Modify)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogModifyBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Modify)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogModifyBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Modify)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogModifyBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,DefPos,Name,Proc,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Modify m = (Modify)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0400"),
                    Pos(m.ppos),
                    Pos(m.modifydefpos),
                    new TChar(m.name),
                    new TChar(m.source?.ident??""));
            }
         }
        /// <summary>
        /// set up the Log$User table
        /// </summary>
        static void LogUserResults()
        {
            var t = new SystemTable("Log$User");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$User table
        /// </summary>
        internal class LogUserBookmark : LogSystemBookmark
        {
            LogUserBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogUserBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PUser)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogUserBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogUserBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PUser)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogUserBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PUser)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogUserBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PUser)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogUserBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PUser a = (PUser)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0230"),
                    Pos(a.ppos),
                    new TChar(a.name));
            }
        }
        /// <summary>
        /// set up the Log$Role table
        /// </summary>
        static void LogRoleResults()
        {
            var t = new SystemTable("Log$Role");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Details", Domain.Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Role
        /// </summary>
        internal class LogRoleBookmark : LogSystemBookmark
        {
            LogRoleBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogRoleBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PRole || lb.value()==Physical.Type.PRole1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogRoleBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogRoleBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PRole || lb.value() == Physical.Type.PRole1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogRoleBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PRole||lb.value()==Physical.Type.PRole1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogRoleBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PRole || lb.value() == Physical.Type.PRole1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogRoleBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PRole a = (PRole)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0403"),
                    Pos(a.ppos),
                    new TChar(a.name),
                    new TChar(a.details));
            }
        }
        /// <summary>
        /// set up the Log$Check table
        /// </summary>
        static void LogCheckResults()
        {
            var t = new SystemTable("Log$Check");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Ref", Domain.Position,0);
            t+=new SystemTableColumn(t, "ColRef", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Check", Domain.Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Check
        /// </summary>
        internal class LogCheckBookmark : LogSystemBookmark
        {
            LogCheckBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogCheckBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PCheck||lb.value()==Physical.Type.PCheck2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogCheckBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogCheckBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PCheck || lb.value() == Physical.Type.PCheck2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogCheckBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PCheck||lb.value()==Physical.Type.PCheck2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogCheckBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PCheck || lb.value() == Physical.Type.PCheck2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogCheckBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Ref,Name,Check,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PCheck c = (PCheck)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0404"),
                    Pos(c.ppos),
                    Pos(c.ckobjdefpos),
                    Pos(c.subobjdefpos),
                    new TChar(c.name ?? throw new PEException("PE0405")),
                    Display(c.check ?? throw new PEException("PE0406")));
            }
        }
        static void LogClassificationResults()
        {
            var t = new SystemTable("Log$Classification");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Obj", Domain.Position,0);
            t+=new SystemTableColumn(t, "Classification", Domain.Char,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class LogClassificationBookmark : LogSystemBookmark
        {
            LogClassificationBookmark(Context _cx,SystemRowSet rs, int pos, Physical ph, long pp) 
                : base(_cx,rs, pos, ph, pp, _Value(_cx, rs, ph))
            {
            }
            internal static LogClassificationBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Classify)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogClassificationBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogClassificationBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Classify)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogClassificationBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Classify)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogClassificationBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Classify)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogClassificationBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }

            /// <summary>
            /// the current value: (Pos,User,Clearance,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Classify c = (Classify)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0407"),
                    Pos(c.ppos),
                    Pos(c.obj),
                    new TChar(c.classification.ToString()),
                    Pos(c.trans));
            }
        }
        static void LogClearanceResults()
        {
            var t = new SystemTable("Log$Clearance");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "User", Domain.Position,0);
            t+=new SystemTableColumn(t, "Clearance", Domain.Char,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class LogClearanceBookmark : LogSystemBookmark
        {
            LogClearanceBookmark(Context _cx,SystemRowSet rs, int pos, Physical ph, long pp)
                : base(_cx,rs, pos, ph, pp, _Value(_cx, rs, ph,
                    (_cx.user??throw new DBException("42105")).defpos))
            {
            }
            internal static LogClearanceBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Clearance)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogClearanceBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogClearanceBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Clearance)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogClearanceBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Clearance)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogClearanceBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Clearance)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogClearanceBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,User,Clearance,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph,long u)
            {
                Clearance c = (Clearance)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0408"),
                    Pos(c.ppos),
                    Pos(u),
                    new TChar(c.clearance.ToString()),
                    Pos(c.trans));
            }
        }
        /// <summary>
        /// set up the Log$Column table
        /// </summary>
        static void LogColumnResults()
        {
            var t = new SystemTable("Log$Column");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Table", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Seq", Domain.Int,0);
            t+=new SystemTableColumn(t, "Domain", Domain.Position,0);
            t+=new SystemTableColumn(t, "Default", Domain.Char,0);
            t+=new SystemTableColumn(t, "NotNull", Domain.Bool,0);
            t+=new SystemTableColumn(t, "Generated", Domain.Char,0);
            t+=new SystemTableColumn(t, "Update", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Column
        /// </summary>
        internal class LogColumnBookmark: LogSystemBookmark
        {
            LogColumnBookmark(Context _cx,SystemRowSet rs,int pos, Physical ph, long pp)
                : base(_cx,rs,pos,ph, pp, _Value(_cx, rs, ph))
            {
            }
            internal static LogColumnBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    switch (lb.value())
                    {
                        case Physical.Type.Alter:
                        case Physical.Type.Alter2:
                        case Physical.Type.Alter3:
                        case Physical.Type.PColumn:
                        case Physical.Type.PColumn2:
                        case Physical.Type.PColumn3:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogColumnBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            internal static LogColumnBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    switch (lb.value())
                    {
                        case Physical.Type.Alter:
                        case Physical.Type.Alter2:
                        case Physical.Type.Alter3:
                        case Physical.Type.PColumn:
                        case Physical.Type.PColumn2:
                        case Physical.Type.PColumn3:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogColumnBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    switch (lb.value())
                    {
                        case Physical.Type.Alter:
                        case Physical.Type.Alter2:
                        case Physical.Type.Alter3:
                        case Physical.Type.PColumn:
                        case Physical.Type.PColumn2:
                        case Physical.Type.PColumn3:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogColumnBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    switch (lb.value())
                    {
                        case Physical.Type.Alter:
                        case Physical.Type.Alter2:
                        case Physical.Type.Alter3:
                        case Physical.Type.PColumn:
                        case Physical.Type.PColumn2:
                        case Physical.Type.PColumn3:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogColumnBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Table,Name,Seq.Domain,Default,NotNull,Generate,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                if (ph is not PColumn c || cx._Dom(res) is not Domain dm ||
                    c.table is not Table tb)
                    throw new PEException("PE0410");
                    return new TRow(dm,
                        Pos(c.ppos),
                        Pos(tb.defpos),
                        new TChar(c.name),
                        new TInt(c.seq),
                        Pos(c.domdefpos),
                        Display(c.dfs.ToString()),
                        TBool.For(c.notNull),
                        new TChar(c.generated.ToString()),
                        Display(c.upd.ToString()),
                        Pos(c.trans));
            }
         }
        /// <summary>
        /// set up the Log$TablePeriod table
        /// </summary>
        static void LogTablePeriodResults()
        {
            var t = new SystemTable("Log$TablePeriod");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Table", Domain.Position,0);
            t+=new SystemTableColumn(t, "PeriodName", Domain.Char,0);
            t+=new SystemTableColumn(t, "Versioning", Domain.Bool,0);
            t+=new SystemTableColumn(t, "StartColumn", Domain.Position,0);
            t+=new SystemTableColumn(t, "EndColumn", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();

        }
        /// <summary>
        /// an enumerator for Log$Column
        /// </summary>
        internal class LogTablePeriodBookmark : LogSystemBookmark
        {
            LogTablePeriodBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogTablePeriodBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PeriodDef)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTablePeriodBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogTablePeriodBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PeriodDef)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTablePeriodBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PeriodDef)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTablePeriodBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PeriodDef)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTablePeriodBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Table,Name,Seq.Domain,Default,NotNull,Generate,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PPeriodDef c = (PPeriodDef)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0411"),
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
        static void LogDateTypeResults()
        {
            var t = new SystemTable("Log$DateType");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Kind", Domain.Char,0);
            t+=new SystemTableColumn(t, "StartField", Domain.Int,0);
            t+=new SystemTableColumn(t, "EndField",  Domain.Int,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$DateType
        /// </summary>
        internal class LogDateTypeBookmark : LogSystemBookmark
        {
            LogDateTypeBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogDateTypeBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PDateType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDateTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogDateTypeBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PDateType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDateTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PDateType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDateTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PDateType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogDateTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,DateType,start,end,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                    var d = ((PDateType)ph).domain;
                    return new TRow(cx._Dom(res) ?? throw new PEException("PE0412"),
                        Pos(ph.ppos),
                        new TChar(d.name),
                        new TChar(d.kind.ToString()),
                        new TChar(d.start.ToString()),
                        new TChar(d.end.ToString()),
                        Pos(ph.trans));
            }
         }
        /// <summary>
        /// set up the Log$Domain table
        /// </summary>
        static void LogDomainResults()
        {
            var t = new SystemTable("Log$Domain");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Kind", Domain.Char,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "DataType", Domain.Char,0);
            t+=new SystemTableColumn(t, "DataLength", Domain.Int,0);
            t+=new SystemTableColumn(t, "Scale", Domain.Int,0);
            t+=new SystemTableColumn(t, "Charset", Domain.Char,0);
            t+=new SystemTableColumn(t, "Collate", Domain.Char,0);
            t+=new SystemTableColumn(t, "Default", Domain.Char,0);
            t+=new SystemTableColumn(t, "StructDef", Domain.Char,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Domain
        /// </summary>
        internal class LogDomainBookmark : LogSystemBookmark
        {
            LogDomainBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogDomainBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    switch (lb.value())
                    {
                        case Physical.Type.PDomain:
                        case Physical.Type.PDomain1:
                        case Physical.Type.Edit:
                        case Physical.Type.PType:
                        case Physical.Type.PType1:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogDomainBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            internal static LogDomainBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    switch (lb.value())
                    {
                        case Physical.Type.PDomain:
                        case Physical.Type.PDomain1:
                        case Physical.Type.Edit:
                        case Physical.Type.PType:
                        case Physical.Type.PType1:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogDomainBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    switch (lb.value())
                    {
                        case Physical.Type.PDomain:
                        case Physical.Type.PDomain1:
                        case Physical.Type.Edit:
                        case Physical.Type.PType:
                        case Physical.Type.PType1:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogDomainBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    switch (lb.value())
                    {
                        case Physical.Type.PDomain:
                        case Physical.Type.PDomain1:
                        case Physical.Type.Edit:
                        case Physical.Type.PType:
                        case Physical.Type.PType1:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogDomainBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Kind,Name,DataType,DataLength,Scale,Charset,Collate,Default,StructDef,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Domain d = ((PDomain)ph).domain;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0413"),
                    Pos(ph.ppos),
                    new TChar(d.kind.ToString()),
                    new TChar(d.name),
                    new TChar(d.kind.ToString()),
                    new TInt(d.prec),
                    new TInt(d.scale),
                    new TChar(d.charSet.ToString()),
                    new TChar(d.culture.Name),
                    Display(d.defaultString),
                    new TChar(d.elType.ToString()),
                    Pos(ph.trans));
            }
         }
        /// <summary>
        /// set up the Log$Edit table
        /// </summary>
        static void LogEditResults()
        {
            var t = new SystemTable("Log$Edit");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Prev", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Edit
        /// </summary>
        internal class LogEditBookmark : LogSystemBookmark
        {
            LogEditBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {  }
            internal static LogEditBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Edit)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEditBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogEditBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Edit)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEditBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Edit)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEditBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Edit)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEditBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Prev,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Edit d = (Edit)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0415"),
                    Pos(d.ppos),
                    Pos(d._prev),
                    Pos(d.trans));
            }
         }
        static void LogEnforcementResults()
        {
            var t = new SystemTable("Log$Enforcement");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Table", Domain.Char,0);
            t+=new SystemTableColumn(t, "Flags", Domain.Int,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class LogEnforcementBookmark : LogSystemBookmark
        {
            LogEnforcementBookmark(Context _cx,SystemRowSet rs, int pos, Physical ph, long pp)
                :base(_cx,rs,pos,ph, pp, _Value(_cx, rs, ph))
            {
            }
            internal static LogEnforcementBookmark? New(Context _cx,SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Enforcement)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEnforcementBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogEnforcementBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Enforcement)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEnforcementBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                if (cx._Dom(res) is not Domain dm || ph is not Enforcement en) 
                    throw new PEException("PE42112");
                return new TRow(dm, Pos(en.ppos), Pos(en.tabledefpos),
                    new TInt((long)en.enforcement),Pos(en.trans));
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Enforcement)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEnforcementBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Enforcement)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEnforcementBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$Grant table
        /// </summary>
        static void LogGrantResults()
        {
            var t = new SystemTable("Log$Grant");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Privilege", Domain.Int,0);
            t+=new SystemTableColumn(t, "Object", Domain.Position,0);
            t+=new SystemTableColumn(t, "Grantee", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Grant table
        /// </summary>
        internal class LogGrantBookmark : LogSystemBookmark
        {
            LogGrantBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogGrantBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Grant)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogGrantBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogGrantBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Grant)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogGrantBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Grant)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogGrantBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null;
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Grant)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogGrantBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }

            /// <summary>
            /// the current value: (Pos,Privilege,Object,Grantee,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Grant g = (Grant)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0416"),
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
        static void LogIndexResults()
        {
            var t = new SystemTable("Log$Index");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Table", Domain.Position,0);
            t+=new SystemTableColumn(t, "Flags", Domain.Char,0);
            t+=new SystemTableColumn(t, "Reference", Domain.Position,0);
            t+=new SystemTableColumn(t, "Adapter", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Index table
        /// </summary>
        internal class LogIndexBookmark : LogSystemBookmark
        {
            LogIndexBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogIndexBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PIndex || lb.value() == Physical.Type.PIndex1
                        ||lb.value()==Physical.Type.PIndex2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogIndexBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogIndexBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PIndex || lb.value() == Physical.Type.PIndex1
                        || lb.value() == Physical.Type.PIndex2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogIndexBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PIndex || lb.value() == Physical.Type.PIndex1
                        ||lb.value()==Physical.Type.PIndex2) 
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogIndexBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PIndex || lb.value() == Physical.Type.PIndex1
                        || lb.value() == Physical.Type.PIndex2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogIndexBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Table,Flags,Reference,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                    PIndex p = (PIndex)ph;
                    return new TRow(cx._Dom(res) ?? throw new PEException("PE0418"),
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
        static void LogIndexKeyResults()
        {
            var t = new SystemTable("Log$IndexKey");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "ColNo", Domain.Int,1);
            t+=new SystemTableColumn(t, "Column", Domain.Int,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
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
            LogIndexKeyBookmark(Context _cx, SystemRowSet rs, int pos, Physical ph, long pp, int i)
                 : base(_cx,rs, pos,ph, pp, _Value(_cx, rs, ph,i))
            {
                _ix = i;
            }
            internal static LogIndexKeyBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PIndex || lb.value() == Physical.Type.PIndex1
                        ||lb.value()==Physical.Type.PIndex2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogIndexKeyBookmark(_cx, res, 0, nph, np, 0);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogIndexKeyBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PIndex || lb.value() == Physical.Type.PIndex1
                        || lb.value() == Physical.Type.PIndex2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogIndexKeyBookmark(_cx, res, 0, nph, np, 
                            ((PIndex)nph).columns.Length-1);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var ix = _ix + 1; ph is PIndex x && ix < x.columns.Length; ix++)
                {
                    var rb = new LogIndexKeyBookmark(_cx,res, _pos, ph, nextpos, ix);
                    if (Eval(res.where, _cx))
                        return rb;
                }
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PIndex || lb.value() == Physical.Type.PIndex1
                        ||lb.value()==Physical.Type.PIndex2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogIndexKeyBookmark(_cx, res, 0, nph, np, 0);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var ix = _ix - 1; ph is PIndex && ix>=0; ix--)
                {
                    var rb = new LogIndexKeyBookmark(_cx, res, _pos, ph, nextpos, ix);
                    if (Eval(res.where, _cx))
                        return rb;
                }
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PIndex || lb.value() == Physical.Type.PIndex1
                        || lb.value() == Physical.Type.PIndex2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogIndexKeyBookmark(_cx, res, 0, nph, np,
                                              ((PIndex)nph).columns.Length - 1);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,ColNo,Column,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph,int _ix)
            {
                PIndex x = (PIndex)ph;
                return new TRow(cx._Dom(res)??throw new PEException("PE42111"),
                    Pos(x.ppos),
                    new TInt(_ix),
                    Pos(x.columns[_ix]),
                    Pos(x.trans));
            }
        }
        /// <summary>
        /// set up the Log$Ordering table
        /// </summary>
        static void LogOrderingResults()
        {
            var t = new SystemTable("Log$Ordering");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "TypeDefPos", Domain.Position,0);
            t+=new SystemTableColumn(t, "FuncDefPos", Domain.Position,0);
            t+=new SystemTableColumn(t, "OrderFlags", Domain.Int,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Ordering table
        /// </summary>
        internal class LogOrderingBookmark : LogSystemBookmark
        {
            LogOrderingBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogOrderingBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Ordering)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogOrderingBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogOrderingBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Ordering)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogOrderingBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Ordering)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogOrderingBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Ordering)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogOrderingBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,TypeDefPos,FuncDefPos,OrderFlags,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Ordering o = (Ordering)ph;
                return new TRow(cx._Dom(res)??throw new PEException("PE42109"),
                    Pos(o.ppos),
                    new TChar(o.domain.ToString()),
                    Pos(o.funcdefpos),
                    new TChar(o.flags.ToString()),
                    Pos(o.trans));
            }
        }
        /// <summary>
        /// set up the Log$Procedure table
        /// </summary>
        static void LogProcedureResults()
        {
            var t = new SystemTable("Log$Procedure");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Arity", Domain.Int,0);
            t+=new SystemTableColumn(t, "RetDefPos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Proc", Domain.Char,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Procedure table
        /// </summary>
        internal class LogProcedureBookmark : LogSystemBookmark
        {
            LogProcedureBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogProcedureBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    switch (lb.value())
                    {
                        case Physical.Type.PProcedure:
                        case Physical.Type.PProcedure2:
                        case Physical.Type.PMethod:
                        case Physical.Type.PMethod2:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogProcedureBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            internal static LogProcedureBookmark? New(SystemRowSet res,Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    switch (lb.value())
                    {
                        case Physical.Type.PProcedure:
                        case Physical.Type.PProcedure2:
                        case Physical.Type.PMethod:
                        case Physical.Type.PMethod2:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogProcedureBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    switch (lb.value())
                    {
                        case Physical.Type.PProcedure:
                        case Physical.Type.PProcedure2:
                        case Physical.Type.PMethod:
                        case Physical.Type.PMethod2:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogProcedureBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    switch (lb.value())
                    {
                        case Physical.Type.PProcedure:
                        case Physical.Type.PProcedure2:
                        case Physical.Type.PMethod:
                        case Physical.Type.PMethod2:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogProcedureBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Arity,Proc,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PProcedure p = (PProcedure)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0419"),
                    Pos(p.ppos),
                    new TChar(p.nameAndArity),
                    new TInt(p.arity),
                    new TChar(p.dataType.ToString()),
                    Display(p.source?.ident??""),
                    Pos(p.trans));
            }
        }
        /// <summary>
        /// set up the Log$Revoke table
        /// </summary>
        static void LogRevokeResults()
        {
            var t = new SystemTable("Log$Revoke");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Privilege", Domain.Int,0);
            t+=new SystemTableColumn(t, "Object", Domain.Position,0);
            t+=new SystemTableColumn(t, "Grantee", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Revoke
        /// </summary>
        internal class LogRevokeBookmark : LogSystemBookmark
        {
            LogRevokeBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogRevokeBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Revoke)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogRevokeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogRevokeBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Revoke)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogRevokeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Revoke)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogRevokeBookmark(_cx, res, 0, nph, np);
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Revoke)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogRevokeBookmark(_cx, res, 0, nph, np);
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Privilege,Object,Grantee,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Grant g = (Grant)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0420"),
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
        static void LogTableResults()
        {
            var t = new SystemTable("Log$Table");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Defpos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Table
        /// </summary>
        internal class LogTableBookmark : LogSystemBookmark
        {
            LogTableBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogTableBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PTable||lb.value()==Physical.Type.PTable1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTableBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogTableBookmark? New(SystemRowSet res, Context _cx)
            {
                 for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PTable || lb.value() == Physical.Type.PTable1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTableBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PTable||lb.value()==Physical.Type.PTable1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTableBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PTable || lb.value() == Physical.Type.PTable1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTableBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,name,Iri,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PTable d = (PTable)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0421"),
                    Pos(d.ppos),
                    new TChar(d.name),
                    Pos(d.defpos),
                    Pos(d.trans));
            }
        }
        /// <summary>
        /// set up the Log$Trigger table
        /// </summary>
        static void LogTriggerResults()
        {
            var t = new SystemTable("Log$Trigger");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Table", Domain.Position,0);
            t+=new SystemTableColumn(t, "Flags", Domain.Char,0);
            t+=new SystemTableColumn(t, "OldTable", Domain.Char,0);
            t+=new SystemTableColumn(t, "NewTable", Domain.Char,0);
            t+=new SystemTableColumn(t, "OldRow", Domain.Char,0);
            t+=new SystemTableColumn(t, "NewRow", Domain.Char,0);
            t+=new SystemTableColumn(t, "Def", Domain.Char,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Trigger
        /// </summary>
        internal class LogTriggerBookmark : LogSystemBookmark
        {
            LogTriggerBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogTriggerBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PTrigger)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTriggerBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogTriggerBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PTrigger)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTriggerBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PTrigger)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTriggerBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PTrigger)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTriggerBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// The current value: (Pos,name,Table,Flags,OldTable,NewTable,OldRow,NewRow,Def,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PTrigger d = (PTrigger)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0422"),
                    Pos(d.ppos),
                    new TChar(d.name),
                    Pos(d.target),
                    new TChar(d.tgtype.ToString()),
                    new TChar(d.oldTable?.ident??""),
                    new TChar(d.newTable?.ident??""),
                    new TChar(d.oldRow?.ident??""),
                    new TChar(d.newRow?.ident??""),
                    Display(d.src?.ident??""),
                    Pos(d.trans));
            }
        }
        /// <summary>
        /// setup the Log$TriggerUpdate table
        /// </summary>
        static void LogTriggerUpdateColumnResults()
        {
            var t = new SystemTable("Log$TriggerUpdateColumn");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Column", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$TriggerUpdate
        /// </summary>
        internal class LogTriggerUpdateColumnBookmark : LogSystemBookmark
        {
            /// <summary>
            /// the second part of the key
            /// </summary>
            readonly BList<long?> _sub;
            readonly int _ix;
            /// <summary>
            /// construct a new Log$TriggerUpdate enumerator
            /// </summary>
            /// <param name="r"></param>
            LogTriggerUpdateColumnBookmark(Context _cx, SystemRowSet res,int pos,Physical ph, long pp,
                BList<long?> s,int i): base(_cx,res,pos,ph, pp,_Value(_cx, res, ph,s,i))
            {
                _sub = s;
                _ix = i;
            }
            internal static LogTriggerUpdateColumnBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PTrigger)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var pt = (PTrigger)nph;
                        if (pt.cols != null)
                            for (var ix = 0; ix < pt.cols.Length; ix++)
                            {
                                var rb = new LogTriggerUpdateColumnBookmark(_cx,res, 0, nph,np, pt.cols, ix);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                            }
                    }
                return null;
            }
            internal static LogTriggerUpdateColumnBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PTrigger)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var pt = (PTrigger)nph;
                        if (pt.cols != null)
                            for (var ix = pt.cols.Length-1; ix>=0; ix--)
                            {
                                var rb = new LogTriggerUpdateColumnBookmark(_cx, res, 0, nph, np, 
                                    pt.cols, ix);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                            }
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var ix = _ix + 1; ix < _sub.Length; ix++)
                {
                    var rb = new LogTriggerUpdateColumnBookmark(_cx,res, _pos + 1, ph, nextpos, _sub, ix);
                    if (Eval(res.where, _cx))
                        return rb;
                }
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PTrigger)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var pt = (PTrigger)nph;
                        if (pt.cols != null)
                            for (var ix = 0; ix < pt.cols.Length; ix++)
                            {
                                var rb = new LogTriggerUpdateColumnBookmark(_cx, res, 0, nph, np, pt.cols, ix);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                            }
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var ix = _ix - 1; ix >=0; ix--)
                {
                    var rb = new LogTriggerUpdateColumnBookmark(_cx, res, _pos + 1, ph, nextpos, _sub, ix);
                    if (Eval(res.where, _cx))
                        return rb;
                }
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PTrigger)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var pt = (PTrigger)nph;
                        if (pt.cols != null)
                            for (var ix = pt.cols.Length-1; ix>=0; ix--)
                            {
                                var rb = new LogTriggerUpdateColumnBookmark(_cx, res, 0, nph, np, pt.cols, ix);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                            }
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Column)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph,BList<long?> _sub,int _ix)
            {
                PTrigger pt = (PTrigger)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0423"),
                    Pos(pt.ppos),
                    new TInt(_sub[_ix]??-1L),
                    Pos(pt.trans));
            }
        }
        static void LogTriggeredActionResults()
        {
            var t = new SystemTable("Log$TriggeredAction");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Trigger", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class LogTriggeredActionBookmark : LogSystemBookmark
        {
            LogTriggeredActionBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogTriggeredActionBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.TriggeredAction)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTriggeredActionBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogTriggeredActionBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.TriggeredAction)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTriggeredActionBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Alter)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTriggeredActionBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Alter)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTriggeredActionBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,SuperType)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                TriggeredAction t = (TriggeredAction)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0424"),
                    Pos(t.ppos),
                    Pos(t.trigger),
                    Pos(t.trans));
            }
        }
        /// <summary>
        /// set up the Log$Type table
        /// </summary>
        static void LogTypeResults()
        {
            var t = new SystemTable("Log$Type");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t += new SystemTableColumn(t, "Name", Domain.Char, 0);
            t+=new SystemTableColumn(t, "SuperType", Domain.Position,0);
            t += new SystemTableColumn(t, "Graph", Domain.Char, 0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Type table
        /// </summary>
        internal class LogTypeBookmark : LogSystemBookmark
        {
            LogTypeBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogTypeBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PType||lb.value()==Physical.Type.PType1
                        || lb.value() == Physical.Type.PNodeType || lb.value() == Physical.Type.PEdgeType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogTypeBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PType || lb.value() == Physical.Type.PType1
                        || lb.value() == Physical.Type.PNodeType || lb.value() == Physical.Type.PEdgeType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PType || lb.value() == Physical.Type.PType1
                        || lb.value() == Physical.Type.PNodeType || lb.value() == Physical.Type.PEdgeType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PType || lb.value() == Physical.Type.PType1
                        || lb.value() == Physical.Type.PNodeType || lb.value() == Physical.Type.PEdgeType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,SuperType)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PType t = (PType)ph;
                var un = (t.under is null) ? TNull.Value : Pos(t.under.defpos);
                TypedValue gr = (t is PEdgeType) ? new TChar("EDGETYPE")
                    : (t is PNodeType) ? new TChar("NODETTYPE") : TNull.Value;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0425"),
                    Pos(t.ppos),
                    new TChar(t.name),
                    un,
                    gr,
                    Pos(t.trans));
            }
         }
        static void LogEdgeTypeResults()
        {
            var t = new SystemTable("Log$EdgeType");
            t += new SystemTableColumn(t, "Pos", Domain.Position, 1);
            t += new SystemTableColumn(t, "LeavingType", Domain.Position, 0);
            t += new SystemTableColumn(t, "ArrivingType", Domain.Position, 0);
            t += new SystemTableColumn(t, "Transaction", Domain.Position, 0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$EdgeType table
        /// </summary>
        internal class LogEdgeTypeBookmark : LogSystemBookmark
        {
            LogEdgeTypeBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx, r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogEdgeTypeBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PEdgeType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEdgeTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogEdgeTypeBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PEdgeType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEdgeTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PEdgeType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEdgeTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null;
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PEdgeType)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogEdgeTypeBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,SuperType)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PEdgeType t = (PEdgeType)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0425"),
                    Pos(t.ppos),
                    Pos(t.dataType.rowType[1]??-1L),
                    Pos(t.dataType.rowType[2]??-1L),
                    Pos(t.trans));
            }
        }
        /// <summary>
        /// set up the Log$TypeMethod table
        /// </summary>
        static void LogTypeMethodResults()
        {
            var t = new SystemTable("Log$TypeMethod");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Type", Domain.Char,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$TypeMethod
        /// </summary>
        internal class LogTypeMethodBookmark : LogSystemBookmark
        {
            LogTypeMethodBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogTypeMethodBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PMethod||lb.value()==Physical.Type.PMethod2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTypeMethodBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogTypeMethodBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PMethod || lb.value() == Physical.Type.PMethod2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTypeMethodBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PMethod||lb.value()==Physical.Type.PMethod2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTypeMethodBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PMethod || lb.value() == Physical.Type.PMethod2)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTypeMethodBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Type,Name,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PMethod t = (PMethod)ph;
                if (t.udt is null || cx._Dom(res) is not Domain dm)
                    throw new PEException("PE0426");
                    return new TRow(dm,
                        Pos(t.ppos),
                        new TChar(t.udt.ToString()),
                        new TChar(t.name),
                        Pos(t.trans));
            }
        }
        /// <summary>
        /// set up the Log$View table
        /// </summary>
        static void LogViewResults()
        {
            var t = new SystemTable("Log$View");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Select", Domain.Char,0);
            t+=new SystemTableColumn(t, "Struct", Domain.Position,0);
            t+=new SystemTableColumn(t, "Using", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$View
        /// </summary>
        internal class LogViewBookmark : LogSystemBookmark
        {
            LogViewBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogViewBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PView)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogViewBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogViewBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PView)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogViewBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PView)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogViewBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PView)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogViewBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Select,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                PView d = (PView)ph;
                TypedValue us = TNull.Value;
                TypedValue st = TNull.Value;
                if (d is PRestView2 view)
                    us = Pos(view.usingtbpos);
                if (d is PRestView vw)
                    st = Pos(vw.structpos);
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0426"),
                    Pos(d.ppos),
                    new TChar(d.name),
                    Display(d.viewdef),
                    st,
                    us,
                    Pos(d.trans));
            }
        }
        /// <summary>
        /// set up the Log$Record table
        /// </summary>
        static void LogRecordResults()
        {
            var t = new SystemTable("Log$Record");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Table", Domain.Position,0);
            t+=new SystemTableColumn(t, "SubType", Domain.Position,0);
            t+=new SystemTableColumn(t, "Classification", Domain.Char,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Record
        /// </summary>
        internal class LogRecordBookmark : LogSystemBookmark
        {
            LogRecordBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogRecordBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    switch (lb.value())
                    {
                        case Physical.Type.Record:
                        case Physical.Type.Record2:
                        case Physical.Type.Record3:
                        case Physical.Type.Update:
                        case Physical.Type.Update1:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogRecordBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            internal static LogRecordBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    switch (lb.value())
                    {
                        case Physical.Type.Record:
                        case Physical.Type.Record2:
                        case Physical.Type.Record3:
                        case Physical.Type.Update:
                        case Physical.Type.Update1:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogRecordBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    switch (lb.value())
                    {
                        case Physical.Type.Record:
                        case Physical.Type.Record2:
                        case Physical.Type.Record3:
                        case Physical.Type.Update:
                        case Physical.Type.Update1:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogRecordBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    switch (lb.value())
                    {
                        case Physical.Type.Record:
                        case Physical.Type.Record2:
                        case Physical.Type.Record3:
                        case Physical.Type.Update:
                        case Physical.Type.Update1:
                            {
                                var (nph, np) = _cx.db.GetPhysical(lb.key());
                                var rb = new LogRecordBookmark(_cx, res, 0, nph, np);
                                if (!rb.Match(res))
                                    return null;
                                if (Eval(res.where, _cx))
                                    return rb;
                                continue;
                            }
                    }
                return null;
            }
            /// <summary>
            /// current value: (Pos,Table,SubType,Classification,Transaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Record d = (Record)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0428"),
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
        static void LogRecordFieldResults()
        {
            var t = new SystemTable("Log$RecordField");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "ColRef", Domain.Position,1);
            t+=new SystemTableColumn(t, "Data", Domain.Char,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
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
            LogRecordFieldBookmark(Context _cx, LogRecordBookmark rec,int pos,
                ABookmark<long,TypedValue>fld)
                : base(_cx,rec.res,pos,rec.ph,rec.nextpos, _Value(_cx, rec.res,rec.ph,fld))
            {
                _rec = rec;
                _fld = fld;
            }
            internal static LogRecordFieldBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var lb = LogRecordBookmark.New(_cx, res); lb != null;
                    lb = (LogRecordBookmark?)lb.Next(_cx))
                    if (lb.ph is Record rc)
                        for (var b = rc.fields.PositionAt(0); b != null; b = b.Next())
                        {
                            var rb = new LogRecordFieldBookmark(_cx, lb, 0, b);
                            if (!rb.Match(res))
                                return null;
                            if (Eval(res.where, _cx))
                                return rb;
                        }
                return null;
            }
            internal static LogRecordFieldBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = LogRecordBookmark.New(res, _cx); lb != null;
                    lb = (LogRecordBookmark?)lb.Previous(_cx))
                    if (lb.ph is Record rc)
                        for (var b = rc.fields.Last(); b != null; b = b.Previous())
                        {
                            var rb = new LogRecordFieldBookmark(_cx, lb, 0, b);
                            if (!rb.Match(res))
                                return null;
                            if (Eval(res.where, _cx))
                                return rb;
                        }
                return null;
            }
            /// <summary>
            /// current value: (Pos,Colref,Data)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph, ABookmark<long,TypedValue>_fld)
            {
                Record r = (Record)ph;
                long p = _fld.key();
                var v = r.fields[p] ?? TNull.Value;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0430"),
                    Pos(r.ppos),
                    Pos(p),
                    new TChar(v.ToString()),
                    Pos(r.trans));
            }
            /// <summary>
            /// Move to next Field or Record
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                for (var fld = _fld.Next(); fld != null; fld = fld.Next())
                {
                    var rb = new LogRecordFieldBookmark(_cx,_rec, _pos + 1, fld);
                    if (Eval(res.where, _cx))
                        return rb;
                }
                for (var rec = (LogRecordBookmark?)_rec.Next(_cx); rec != null;
                    rec = (LogRecordBookmark?)_Next(_cx))
                    if (rec.ph is Record rc)
                        for (var b = rc.fields.PositionAt(0); b != null; b = b.Next())
                        {
                            var rb = new LogRecordFieldBookmark(_cx, rec, _pos + 1, b);
                            if (!rb.Match(res))
                                return null;
                            if (Eval(res.where, _cx))
                                return rb;
                        }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var fld = _fld.Previous(); fld != null; fld = fld.Previous())
                {
                    var rb = new LogRecordFieldBookmark(_cx, _rec, _pos + 1, fld);
                    if (Eval(res.where, _cx))
                        return rb;
                }
                for (var rec = (LogRecordBookmark?)_rec.Previous(_cx); rec != null;
                    rec = (LogRecordBookmark?)_Previous(_cx))
                    if (rec.ph is Record rc)
                        for (var b = rc.fields.Last(); b != null; b = b.Previous())
                        {
                            var rb = new LogRecordFieldBookmark(_cx, rec, _pos + 1, b);
                            if (!rb.Match(res))
                                return null;
                            if (Eval(res.where, _cx))
                                return rb;
                        }
                return null;
            }
        }
        /// <summary>
        /// set up the Log$UpdatePost table
        /// </summary>
        static void LogUpdateResults()
        {
            var t = new SystemTable("Log$Update");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "DefPos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Table", Domain.Position,0);
            t+=new SystemTableColumn(t, "SubType", Domain.Position,0);
            t+=new SystemTableColumn(t, "Classification", Domain.Char,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$UpdatePost
        /// </summary>
        internal class LogUpdateBookmark : LogSystemBookmark
        {
            LogUpdateBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogUpdateBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Update || lb.value()==Physical.Type.Update1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogUpdateBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogUpdateBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.Update || lb.value() == Physical.Type.Update1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogUpdateBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.Update || lb.value()==Physical.Type.Update1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogUpdateBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.Update || lb.value() == Physical.Type.Update1)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogUpdateBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// current value: (Pos,DefPos,Table,SubType,ClassificationTransaction)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                Update u = (Update)ph;
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0431"),
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
        static void LogTransactionResults()
        {
            var t = new SystemTable("Log$Transaction");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "NRecs", Domain.Int,0);
            t+=new SystemTableColumn(t, "Time", Domain.Int,0);
            t+=new SystemTableColumn(t, "User", Domain.Position,0);
            t+=new SystemTableColumn(t, "Role", Domain.Position,0);
            t+=new SystemTableColumn(t, "Transaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Transaction
        /// </summary>
        internal class LogTransactionBookmark : LogSystemBookmark
        {
            LogTransactionBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(_cx, r, ph))
            {
            }
            internal static LogTransactionBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PTransaction)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTransactionBookmark(_cx, res, 0, nph,np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static LogTransactionBookmark? New(SystemRowSet res,Context _cx)
            {
                for (var lb = _cx.db.log.Last(); lb != null; lb = lb.Previous())
                    if (lb.value() == Physical.Type.PTransaction)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTransactionBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(nextpos); lb != null; lb = lb.Next())
                    if (lb.value() == Physical.Type.PTransaction)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTransactionBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var lb = _cx.db.log.PositionAt(ph.ppos)?.Previous(); lb != null; 
                    lb = lb.Previous())
                    if (lb.value() == Physical.Type.PTransaction)
                    {
                        var (nph, np) = _cx.db.GetPhysical(lb.key());
                        var rb = new LogTransactionBookmark(_cx, res, 0, nph, np);
                        if (!rb.Match(res))
                            return null;
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// current value: (Pos,NRecs,Time,User,Authority)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, Physical ph)
            {
                    PTransaction t = (PTransaction)ph;
                    return new TRow(cx._Dom(res) ?? throw new PEException("PE0434"),
                        Pos(t.ppos),
                        new TInt(t.nrecs),
                        new TDateTime(new DateTime(t.time)),
                        Pos(t.ptuser?.defpos??-1L),
                        Pos(t.ptrole.defpos),
                        Pos(t.trans));
            }
        }
        /// <summary>
        /// set up the Sys$Role table
        /// </summary>
        static void SysRoleResults()
        {
            var t = new SystemTable("Sys$Role");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// enumerate the Sys$Role table
        /// shareable as of 26 April 2021
        /// </summary>
        internal class SysRoleBookmark : SystemBookmark
        {
            /// <summary>
            /// an enumerator for the role's objects (all of which will be shareable)
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
                : base(_cx,res,pos,bmk.key(),
                      ((Role)bmk.value()).lastChange,_Value(_cx, res, (Role)bmk.value()))
            {
                _bmk = bmk;
            }
            internal static SysRoleBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var en = _cx.db.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is Role)
                    {
                        var rb = new SysRoleBookmark(_cx, res, 0, en);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value (Pos,Name,Kind)
            /// </summary>
            static TRow _Value(Context cx,SystemRowSet res,Role a)
            {
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0435"),
                    Pos(a.defpos),
                    new TChar(a.name??""));
            }
            /// <summary>
            /// Move to the next Role
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                for (var bmk = _bmk.Next(); bmk != null; bmk = bmk.Next())
                {
                    var rb = new SysRoleBookmark(_cx,res, _pos + 1, bmk);
                    if (bmk.value() is Role && rb.Match(res) && RowSet.Eval(res.where, _cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Sys$User table
        /// </summary>
        static void SysUserResults()
        {
            var t = new SystemTable("Sys$User");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "SetPassword", Domain.Bool,0); // usually null
            t+=new SystemTableColumn(t, "Clearance", Domain.Char,0); // usually null, otherwise D to A
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// enumerate the Sys$User table
        /// // shareable as of 26 April 2021
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
            /// create the Sys$User enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            SysUserBookmark(Context _cx, SystemRowSet res,int pos, ABookmark<long,object> bmk)
                : base(_cx,res,pos,bmk.key(), ((User)bmk.value()).lastChange,
                      _Value(_cx, res, (User)bmk.value()))
            {
                en = bmk;
            }
            internal static SysUserBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var en = _cx.db.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is User)
                    {
                        var rb = new SysUserBookmark(_cx, res, 0, en);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value (Pos,Name,Kind)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet res, User a)
            {
                return new TRow(cx._Dom(res) ?? throw new PEException("PE0426"),
                    Pos(a.defpos),
                    new TChar(a.name??""),
                    (a.pwd==null)?TNull.Value:TBool.For(a.pwd.Length==0),
                    new TChar(a.clearance.ToString()));
            }
            /// <summary>
            /// Move to the next Role
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                for (var e = en.Next(); e != null; e = e.Next())
                    if (e.value() is User)
                    {
                        var rb = new SysUserBookmark(_cx, res, _pos + 1, e);
                        if (rb.Match(res) && RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void SysRoleUserResults()
        {
            var t = new SystemTable("Sys$RoleUser");
            t+=new SystemTableColumn(t, "Role", Domain.Char,1);
            t+=new SystemTableColumn(t, "User", Domain.Char,0);
            t .AddIndex("Role", "User");
            t.Add();
        }
        // shareable as of 26 April 2021
        internal class SysRoleUserBookmark : SystemBookmark
        {
            readonly ABookmark<string, long?> _rbmk;
            readonly ABookmark<string, long?> _inner;

            SysRoleUserBookmark(Context _cx, SystemRowSet srs,ABookmark<string,long?> rbmk, 
                ABookmark<string,long?> inner,int pos) : base(_cx,srs,pos,inner.value()??-1L, 
               ((User?)_cx.db.objects[inner.value()??-1L] ?? throw new PEException("PE0437")).defpos,  
               _Value(_cx, srs, rbmk.value()??-1L,inner.value()??-1L))
            {
                _rbmk = rbmk;
                _inner = inner;
            }
            internal static SysRoleUserBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var rb = _cx.db.roles.First(); rb != null; rb = rb.Next())
                    if (_cx.db.objects[rb.value()??-1L] is Role ro)
                        for (var inner = ro.dbobjects.First(); inner != null; inner = inner.Next())
                            if (_cx.db.objects[inner.value()??-1L] is User us &&
                                        us.infos[ro.defpos] is ObInfo ui &&
                                        ui.priv.HasFlag(Grant.Privilege.Usage))
                            {
                                var sb = new SysRoleUserBookmark(_cx, res, rb, inner, 0);
                                if (sb.Match(res) && Eval(res.where, _cx))
                                    return sb;
                            }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                if (_cx.role is not Role cr)
                    return null;
                var inner = _inner;
                var rbmk = _rbmk;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                {
                    if (inner.value() is long p && _cx.db.objects[p] is User us &&
                       us.infos[cr.defpos] is ObInfo ui &&
                       ui.priv.HasFlag(Grant.Privilege.Usage) && rbmk!=null)
                    {
                        var sb = new SysRoleUserBookmark(_cx, res, rbmk, inner, _pos + 1);
                        if (Eval(res.where, _cx))
                            return sb;
                    }
                    if (inner != null)
                        continue;
                    while (inner == null)
                    {
                        rbmk = rbmk?.Next();
                        if (rbmk == null || rbmk.value() is not long rp)
                            return null;
                        var ro = (Role)(_cx.db.objects[rp]?? throw new PEException("PE440"));
                        inner = ro.dbobjects.First();
                    }
                    if (_cx.db.objects[inner.value()??-1L] is User ns &&
                    ns.infos[cr.defpos] is ObInfo oi &&
                    oi.priv.HasFlag(Grant.Privilege.Usage) && rbmk!=null)
                    {
                        var sb = new SysRoleUserBookmark(_cx, res, rbmk, inner, _pos + 1);
                        if (sb.Match(res) && Eval(res.where, _cx))
                            return sb;
                    }
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
            static TRow _Value(Context _cx, SystemRowSet rs, long rpos, long upos)
            {
                if (_cx.db.objects[rpos] is not Role ro ||
                    _cx.db.objects[upos] is not User us || _cx._Dom(rs) is not Domain dm)
                    throw new PEException("PE0450");
                return new TRow(dm,
                    new TChar(ro.name ?? ""),
                    new TChar(us.name ?? ""));
            }
        }
        static void SysAuditResults()
        {
            var t = new SystemTable("Sys$Audit");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "User", Domain.Char,0);
            t+=new SystemTableColumn(t, "Table", Domain.Position,0);
            t+=new SystemTableColumn(t, "Timestamp", Domain.Timestamp,0);
            t.Add();
        }
        internal class SysAuditBookmark : SystemBookmark
        {
            public readonly LogBookmark _bmk;
            public SysAuditBookmark(Context _cx, SystemRowSet res, LogBookmark b,int pos)
                :base(_cx,res,pos,b.ph.ppos,0,_Value(_cx,res,b.ph))
            {
                _bmk = b;
            }
            internal static SysAuditBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var bmk = LogBookmark.New(_cx,res);bmk!=null;bmk=bmk.Next(_cx) as LogBookmark)
                    switch(bmk.ph.type)
                    {
                        case Physical.Type.Audit:
                            var rb = new SysAuditBookmark(_cx,res, bmk, 0);
                            if (Eval(res.where, _cx))
                                return rb;
                            break;
                    }
                return null;
            }
            internal static SysAuditBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var bmk = LogBookmark.New(res,_cx); bmk != null; bmk = bmk.Previous(_cx) as LogBookmark)
                    if (bmk.ph.type is Physical.Type.Audit &&  
                        new SysAuditBookmark(_cx, res, bmk, 0) is SysAuditBookmark rb &&
                            Eval(res.where, _cx))
                                return rb;
                return null;
            }
            static TRow _Value(Context _cx, SystemRowSet rs, Physical ph)
            {
                if (_cx._Dom(rs) is not Domain dm || ph is not Audit au ||
                    au.user is not User us)
                    throw new PEException("PE0451");
                return new TRow(dm,
                    Pos(au.ppos),
                    new TChar(us.name??""), Pos(au.table),
                    new TDateTime(new DateTime(au.timestamp)));
            }

            protected override Cursor? _Next(Context _cx)
            {
                for (var bmk = _bmk.Next(_cx) as LogBookmark; bmk != null; 
                    bmk = bmk.Next(_cx) as LogBookmark)
                    switch (bmk.ph.type)
                    {
                        case Physical.Type.Audit:
                            var rb = new SysAuditBookmark(_cx,res, bmk, _pos+1);
                            if (Eval(res.where, _cx))
                                return rb;
                            break;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                for (var bmk = _bmk.Previous(_cx) as LogBookmark; bmk != null;
                    bmk = bmk.Previous(_cx) as LogBookmark)
                    switch (bmk.ph.type)
                    {
                        case Physical.Type.Audit:
                            var rb = new SysAuditBookmark(_cx, res, bmk, _pos + 1);
                            if (Eval(res.where, _cx))
                                return rb;
                            break;
                    }
                return null;
            }
        }
        static void SysAuditKeyResults()
        {
            var t = new SystemTable("Sys$AuditKey");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Seq", Domain.Int,1);
            t+=new SystemTableColumn(t, "Col", Domain.Position,0);
            t+=new SystemTableColumn(t, "Key", Domain.Char,0);
            t.Add();
        }
        internal class SysAuditKeyBookmark : SystemBookmark
        {
            public readonly LogBookmark _bmk;
            public readonly ABookmark<long, string> _inner;
            public readonly int _ix;
            public SysAuditKeyBookmark(Context _cx, SystemRowSet res, LogBookmark bmk,
                ABookmark<long, string> _in, int ix, int pos)
                : base(_cx, res, pos, bmk.ph.ppos, 0, _Value(_cx, res, bmk.ph, _in, ix))
            {
                _bmk = bmk; _inner = _in; _ix = ix;
            }
            internal static SysAuditKeyBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var bmk = LogBookmark.New(_cx, res); bmk != null;
                    bmk = bmk.Next(_cx) as LogBookmark)
                    if (bmk.ph is Audit au && au.match.First() is ABookmark<long, string> inner)
                    {
                        var rb = new SysAuditKeyBookmark(_cx, res, bmk, inner, 0, 0);
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            internal static SysAuditKeyBookmark? New(SystemRowSet res, Context _cx)
            {
                for (var bmk = LogBookmark.New(res, _cx); bmk != null;
                    bmk = bmk.Previous(_cx) as LogBookmark)
                    if (bmk.ph is Audit au && au.match.Last() is ABookmark<long, string> inner)
                    {
                        var rb = new SysAuditKeyBookmark(_cx, res, bmk, inner, 0, 0);
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            static TRow _Value(Context _cx, SystemRowSet res, Physical ph,
                ABookmark<long, string> _in, int _ix)
            {
                return new TRow(_cx._Dom(res) ?? throw new PEException("PE0452"), 
                    Pos(ph.ppos), new TInt(_ix),
                    Pos(_in.key()), new TChar(_in.value()));
            }

            protected override Cursor? _Next(Context _cx)
            {
                var ix = _ix;
                var inner = _inner;
                for (var bmk = _bmk; bmk!=null; )
                {
                    for (inner = inner?.Next(), ix++; inner != null; inner = inner.Next(), ix++)
                    {
                        var sb = new SysAuditKeyBookmark(_cx, res, _bmk, inner, ix, _pos + 1);
                        if (Eval(res.where, _cx))
                            return sb;
                    }
                    ix = 0;
                    for (bmk = bmk?.Next(_cx) as LogBookmark; inner == null && bmk != null;
                        bmk = bmk.Next(_cx) as LogBookmark)
                        if (bmk.ph.type == Physical.Type.Audit &&
                            bmk.ph is Audit au)
                            inner = au.match.First();
                }
                return null;
            }
            protected override Cursor? _Previous(Context _cx)
            {
                var ix = _ix;
                var inner = _inner;
                for (var bmk = _bmk; bmk!=null;)
                {
                    for (inner = inner?.Previous(), ix++; inner != null; inner = inner.Previous(), ix++)
                    {
                        var sb = new SysAuditKeyBookmark(_cx, res, _bmk, inner, ix, _pos + 1);
                        if (Eval(res.where, _cx))
                            return sb;
                    }
                    ix = 0;
                    for (bmk = bmk?.Previous(_cx) as LogBookmark; inner==null && bmk != null;
                        bmk = bmk.Previous(_cx) as LogBookmark)
                        if (bmk.ph.type == Physical.Type.Audit)
                            inner = ((Audit)bmk.ph).match.Last();
                }
                return null;
            }
        }
        static void SysClassificationResults()
        {
            var t = new SystemTable("Sys$Classification");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Type", Domain.Char,0);
            t+=new SystemTableColumn(t, "Classification", Domain.Char,0);
            t+=new SystemTableColumn(t, "LastTransaction", Domain.Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// Classification is useful for DBObjects, TableColumns, and records
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class SysClassificationBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _obm;
            readonly ABookmark<long, TableRow>? _tbm;
            SysClassificationBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long,object> obm,
                ABookmark<long, TableRow>? tbm)
                : this(_cx,res, pos, obm, tbm, _Value(_cx, res, obm, tbm))
            {   }
            SysClassificationBookmark(Context _cx, SystemRowSet res, int pos,
                ABookmark<long, object> obm,
                ABookmark<long, TableRow>? tbm, (long, long, TRow) x)
                : base(_cx, res, pos, x.Item1, x.Item2, x.Item3)
            {   _obm = obm;  _tbm = tbm;  }
            internal static SysClassificationBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var obm = _cx.db.objects.PositionAt(0); obm != null;
                    obm = obm.Next())
                    if (obm.value() is DBObject ob && ob.classification == Level.D)
                    {
                        {
                            var b = new SysClassificationBookmark(_cx, res, 0, obm, null);
                            if (b.Match(res) && Eval(res.where, _cx))
                                return b;
                        }
                        if (ob is Table tb)
                            for (var tbm = tb.tableRows?.PositionAt(0); tbm != null; tbm = tbm.Next())
                            if (tbm.value() is TableRow rw && rw.classification != Level.D)
                                {
                                    var rb = new SysClassificationBookmark(_cx, res, 0, obm, tbm);
                                    if (rb.Match(res) && Eval(res.where, _cx))
                                        return rb;
                                }
                    }
                return null;
            }
            static (long, long,TRow) _Value(Context cx,SystemRowSet res, ABookmark<long, object> obm,
                ABookmark<long, TableRow>? tbm)
            {
                if (tbm is not null)
                {
                    if (tbm.value() is not TableRow rw
                        || cx.db.GetD(rw.ppos) is not Physical ph ||
                        cx._Dom(res) is not Domain dm)
                        throw new PEException("PE42120");
                    return (rw.defpos, rw.ppos, new TRow(dm, Pos(rw.ppos),
                        new TChar(rw.GetType().Name),
                        new TChar(rw.classification.ToString()),
                        Pos(ph.trans)));
                }
                else 
                {
                    var ppos = tbm?.key() ?? obm.key();
                    if (obm.value() is not DBObject ob || cx._Dom(res) is not Domain dm ||
                        cx.db is not Database db || db.GetD(ppos) is not Physical ph)
                        throw new PEException("PE0453");
                    return (ob.defpos, ob.lastChange, new TRow(dm, Pos(ppos),
                        new TChar(ob.GetType().Name),
                        new TChar(ob.classification.ToString()),
                        Pos(ph.trans)));
                }
            }
            protected override Cursor? _Next(Context cx)
            {
                if (_obm.value() is not DBObject ob)
                    throw new PEException("PE42120");
                var tbm = _tbm;
                for (var obm = _obm; obm != null;)
                {
                    if (ob is Table tb)
                    {
                        if (tbm == null)
                            tbm = tb.tableRows.First();
                        else
                            tbm = tbm.Next();
                    }
                    for (; tbm != null; tbm = tbm.Next())
                        if (tbm.value() is TableRow rw && rw.classification != Level.D)
                        {
                            var b = new SysClassificationBookmark(cx, res, _pos + 1, obm, tbm);
                            if (b.Match(res) && Eval(res.where, cx))
                                return b;
                        }
                    // tbm is null by now
                    obm = obm.Next();
                    if (obm == null)
                        return null;
                    if (obm.value() is not DBObject o)
                        throw new PEException("PE42121");
                    if (o.classification != Level.D)
                    {
                        var rb = new SysClassificationBookmark(cx, res, _pos + 1, obm, null);
                        if (rb.Match(res) && Eval(res.where, cx))
                            return rb;
                    }
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void SysClassifiedColumnDataResults()
        {
            var t = new SystemTable("Sys$ClassifiedColumnData");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Col", Domain.Position,1);
            t+=new SystemTableColumn(t, "Classification", Domain.Char,0);
            t+=new SystemTableColumn(t, "LastTransaction", Domain.Position,0);
            t.AddIndex("Pos", "Col");
            t.Add();
        }
        internal class SysClassifiedColumnDataBookmark : SystemBookmark
        {
            readonly ABookmark<long, TableColumn> _cbm;
            readonly ABookmark<long, TableRow> _tbm;
            SysClassifiedColumnDataBookmark(Context _cx, SystemRowSet res, int pos, 
                ABookmark<long, TableColumn> cbm, ABookmark<long, TableRow> tbm)
                : this(_cx, res, pos, cbm, tbm, _Value(_cx, res, tbm.value()))
            { }
            SysClassifiedColumnDataBookmark(Context _cx, SystemRowSet res, int pos,
                ABookmark<long, TableColumn> cbm, ABookmark<long, TableRow> tbm, 
                (long, long, TRow) x)
                : base(_cx, res, pos, x.Item1, x.Item2, x.Item3)
            { _cbm = cbm; _tbm = tbm; }
            internal static SysClassifiedColumnDataBookmark? New(Context _cx, SystemRowSet res)
            {
                var cols = BTree<long, TableColumn>.Empty;
                if (_cx.db is not Database db)
                    throw new PEException("PE42122");
                for (var b = _cx.db.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is TableColumn tc && tc.classification != Level.D)
                        cols+=(tc.defpos, tc);
                for (var cbm = cols.First();cbm!=null;cbm=cbm.Next())
                {
                    var tc = cbm.value();
                    if (db.objects[tc.tabledefpos] is not Table tb)
                        throw new PEException("PE42123");
                    for (var tbm = tb.tableRows?.PositionAt(0); tbm != null; tbm = tbm.Next())
                    {
                        var rt = tbm.value();
                        if (rt.vals[tc.defpos]!=null)
                        {
                            var rb = new SysClassifiedColumnDataBookmark(_cx,res, 0, cbm, tbm);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                    }
                }
                return null;
            }
            static (long, long, TRow) _Value(Context cx,SystemRowSet res,TableRow rw)
            {
                return (rw.defpos,rw.ppos, new TRow(cx._Dom(res) ?? throw new PEException("PE0457"),
                    Pos(rw.defpos),
                    new TChar(rw.classification.ToString()),
                    Pos(rw.ppos)));
            }
            protected override Cursor? _Next(Context _cx)
            {
                var cbm = _cbm;
                var tbm = _tbm.Next();
                for (;cbm!=null; )
                {
                    var tc = cbm.value();
                    for (; tbm != null; tbm = tbm.Next())
                    {
                        var rt = tbm.value();
                        if (rt.vals[tc.defpos]!=null)
                        {
                            var rb = new SysClassifiedColumnDataBookmark(_cx,res, _pos + 1, cbm, tbm);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                    }
                    cbm = cbm.Next();
                    if (cbm == null)
                        break;
                    if (_cx.db is not Database db || db.objects[tc.tabledefpos] is not Table tb)
                        throw new PEException("PE42124");
                    tbm = tb.tableRows.First();
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void SysEnforcementResults()
        {
            var t = new SystemTable("Sys$Enforcement");
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "Scope", Domain.Char,0);
            t.AddIndex("Name", "Scope");
            t.Add();
        }
        internal class SysEnforcementBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _en;
            SysEnforcementBookmark(Context _cx, SystemRowSet res, int pos, 
                ABookmark<long,object> en) :base(_cx,res,pos,en.key(),0,
                    _Value(_cx,res,en.value()))
            {
                _en = en;
            }
            internal static SysEnforcementBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var b = _cx.db.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is Table t && (int)t.enforcement!=15)
                    {
                        var rb = new SysEnforcementBookmark(_cx,res, 0, b);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            static TRow _Value(Context cx, SystemRowSet rs, object ob)
            {
                var ro = cx.role ?? throw new PEException("PE0459");
                var sb = new StringBuilder();
                var t = (Table)ob;
                var oi = t.infos[ro.defpos];
                Enforcement.Append(sb, t.enforcement);
                return new TRow(cx._Dom(rs) ?? throw new PEException("PE0460"), 
                    new TChar(oi?.name??""),
                    new TChar(sb.ToString()));
            }

            protected override Cursor? _Next(Context _cx)
            {
                for (var b = _en.Next(); b != null; b = b.Next())
                    if (b.value() is Table t && (int)t.enforcement != 15)
                    {
                        var rb = new SysEnforcementBookmark(_cx,res, _pos+1, b);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void SysGraphResults()
        {
            var t = new SystemTable("Sys$Graph");
            t += new SystemTableColumn(t, "Uid", Domain.Position, 0);
            t += new SystemTableColumn(t, "Id", Domain.Char, 1);
            t += new SystemTableColumn(t, "Type", Domain.Char, 0);
            t.AddIndex("Id");
            t.AddIndex("Pos");
            t.Add();
        }
        internal class SysGraphBookmark : SystemBookmark
        {
            readonly ABookmark<long, TGraph> _bmk;
            public SysGraphBookmark(Context cx, SystemRowSet r, int pos, 
                ABookmark<long,TGraph> bmk) 
                : base(cx, r, pos, bmk.key(), cx.db.loadpos, _Value(cx,r,bmk.value()))
            {
                _bmk = bmk;
            }
            internal static SysGraphBookmark? New(Context cx,SystemRowSet rs)
            {
                for (var bmk=cx.db.graphs.First();bmk!=null;bmk=bmk.Next())
                {
                    var rb = new SysGraphBookmark(cx, rs, 0, bmk);
                    if (rb.Match(rs) && Eval(rs.where, cx))
                        return rb;
                }
                return null;
            }
            static TRow _Value(Context cx,SystemRowSet rs,TGraph g)
            {
                if (g.nodes.First()?.value() is not TNode n ||
                    cx._Dom(rs) is not Domain dr)
                    throw new PEException("PE91046");
                return new TRow(dr, Pos(n.uid),
                    new TChar(n.id),
                    new TChar(n.dataType.name));
            }
            protected override Cursor? _Next(Context cx)
            {
                for (var bmk = _bmk.Next(); bmk != null; bmk = bmk.Next())
                {
                    var rb = new SysGraphBookmark(cx, res, 0, bmk);
                    if (rb.Match(res) && Eval(res.where, cx))
                        return rb;
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }

            internal override TypedValue New(Domain t)
            {
                return base.New(t);
            }
        }
        /// <summary>
        /// set up the Role$View table
        /// </summary>
        static void RoleViewResults()
        {
            var t = new SystemTable("Role$View");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "View", Domain.Char,1);
            t+=new SystemTableColumn(t, "Select", Domain.Char,0);
            t+=new SystemTableColumn(t, "Struct", Domain.Char,0);
            t+=new SystemTableColumn(t, "Using", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definer", Domain.Char,0);
            t.AddIndex("View");
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$View
        /// shareable as of 26 April 2021
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
                : base(_cx,res,pos,bmk.key(),((View)bmk.value()).lastChange,
                      _Value(_cx,res,bmk.key(),bmk.value()))
            {
                _bmk = bmk;
            }
            internal static RoleViewBookmark? New(Context _cx, SystemRowSet res)
            {
                if (_cx.role is not Role ro)
                    throw new PEException("PE49520");
                for (var bmk = _cx.db.objects.PositionAt(0);bmk!= null;bmk=bmk.Next())
                    if (bmk.value() is View v && v.infos[ro.defpos] is not null)
                    {
                        var rb =new RoleViewBookmark(_cx,res, 0, bmk);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Select)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, long pos, object ob)
            {
                if (_cx.role is not Role ro)
                    throw new DBException("42105");
                var us = "";
                if (ob is RestView rv && rv.usingTableRowSet >= 0 &&
                    rv.framing.obs[rv.usingTableRowSet] is TableRowSet ur &&
                    _cx.db.objects[ur.target] is Table tb && tb.infos[ro.defpos] is ObInfo oi)
                        us = oi.name;
                if (ob is not View vw || _cx._Ob(vw.viewTable) is not Table vt ||
                    vt.NameFor(_cx) is not string st || vw.infos[ro.defpos] is not ObInfo ov ||
                    _cx._Dom(rs) is not Domain dr)
                    throw new PEException("PE0460");
                return new TRow(dr,
                    Pos(pos),
                    new TChar(ov.name??""),
                    new TChar(vw.viewDef??""),
                    new TChar(st),
                    new TChar(us??""),
                    new TChar(((Role?)_cx.db.objects[vw.definer])?.name??""));
            }
            /// <summary>
            /// Move to the next View
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                if (_cx.role is not Role ro)
                    throw new PEException("PE49521");
                for (var bmk = _bmk.Next(); bmk != null; bmk = bmk.Next())
                    if (bmk.value() is View v && v.infos[ro.defpos] is not null)
                    {
                        var rb = new RoleViewBookmark(_cx,res, _pos + 1, bmk);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$DomainCheck table
        /// </summary>
        static void RoleDomainCheckResults()
        {
            var t = new SystemTable("Role$DomainCheck");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "DomainName", Domain.Char,1);
            t+=new SystemTableColumn(t, "CheckName", Domain.Char,1);
            t+=new SystemTableColumn(t, "Select", Domain.Char,0);
            t.AddIndex("Pos");
            t.AddIndex("DomainName","CheckName");
            t.Add();
        }
        /// <summary>
        /// and enumerator for Role$DomainCheck
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleDomainCheckBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for the current key
            /// </summary>
            readonly ABookmark<long,bool> _inner;
            readonly ABookmark<long,object> _outer;
            /// <summary>
            /// create the Sys$DomainCheck enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleDomainCheckBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<long, bool> inner)
                : this(_cx, res, pos, outer, inner, _Value(_cx, res, outer.value(), inner.key()))
            { }
            RoleDomainCheckBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<long,bool> inner, (long,TRow) x)
                : base(_cx,res,pos,x.Item1, x.Item1, x.Item2)
            {
                _outer = outer; _inner = inner;
            }
            internal static RoleDomainCheckBookmark? New(Context cx, SystemRowSet res)
            {
                if (cx.role is not Role ro)
                    throw new PEException("PE49522");
                for (var bmk = cx.db.objects.PositionAt(0); bmk != null; bmk = bmk.Next())
                if (bmk.value() is Domain dm){
                    for (var inner = dm?.constraints.First(); inner != null; inner = inner.Next())
                        if (cx.db.objects[inner.key()] is Check ck && ck.infos[ro.defpos] is not null)
                        {
                            var rb = new RoleDomainCheckBookmark(cx,res, 0, bmk, inner);
                            if (rb.Match(res) && Eval(res.where, cx))
                                return rb;
                        }
                }
                return null;
            }
            /// <summary>
            /// the current value: (DomainName,CheckName,Select,Pos)
            /// </summary>
            static (long,TRow) _Value(Context _cx, SystemRowSet rs, object ob, long ck)
            {
                if (_cx.db is not Database db || _cx._Dom(rs) is not Domain dm ||
                    ob is not Domain domain || db.objects[ck] is not Check check)
                    throw new PEException("PE0471");
                return (ck,new TRow(dm, 
                    Pos(ck),
                    new TChar(domain.name),
                    new TChar(check.name??""),
                    new TChar(check.source)));
            }
            /// <summary>
            /// Move to the next Sys$DomainCheck
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context cx)
            {
                var outer = _outer;
                var inner = _inner;
                if (cx.role is not Role ro)
                    throw new PEException("PE49523");
                for (; ; )
                {
                    if (outer == null)
                        return null;
                    if (inner != null && (inner = inner.Next()) != null && cx.obs[inner.key()] is Check)
                    {
                        var rb = new RoleDomainCheckBookmark(cx, res, _pos + 1, outer, inner);
                        if (rb.Match(res) && Eval(res.where, cx))
                            return rb;
                    }
                    inner = null;
                    if ((outer = outer.Next()) == null)
                        return null;
                    if (outer.value() is not Domain d)
                        continue;
                    inner = d.constraints.First();
                    if (inner != null && cx.obs[inner.key()] is Check ck && ck.infos[ro.defpos] is not null)
                    {
                        var sb = new RoleDomainCheckBookmark(cx, res, _pos + 1, outer, inner);
                        if (sb.Match(res) && Eval(res.where, cx))
                            return sb;
                    }
                }
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$TableCheck table
        /// </summary>
        static void RoleTableCheckResults()
        {
            var t = new SystemTable("Role$TableCheck");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "TableName", Domain.Char,1);
            t+=new SystemTableColumn(t, "CheckName", Domain.Char,1);
            t+=new SystemTableColumn(t, "Select", Domain.Char,0);
            t.AddIndex("Pos");
            t.AddIndex("TableName", "CheckName");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$TableCheck
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleTableCheckBookmark : SystemBookmark
        {
            /// <summary>
            /// Enumerators for traversing trees
            /// </summary>
            readonly ABookmark<long, bool> _inner;
            readonly ABookmark<long,object> _outer;
            /// <summary>
            /// create the Sys$TableCheck enumerator
            /// </summary>
            /// <param name="r"></param>
            RoleTableCheckBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long,object>outer,
               ABookmark<long, bool> inner) 
                : base(_cx,res,pos,inner.key(),((Check?)_cx.db.objects[inner.key()])?.lastChange??-1L,
                      _Value(_cx,res, _cx.db.objects[inner.key()] ?? throw new PEException("PE0473")))
            {
                _outer = outer;
                _inner = inner;
            }
            internal static RoleTableCheckBookmark? New(Context cx, SystemRowSet res)
            {
                if (cx.role is not Role ro)
                    throw new PEException("PE49525");
                for (var outer = cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table rt)
                        for (var inner = rt.tableChecks.First(); inner != null; inner = inner.Next())
                            if (cx.db.objects[inner.key()] is Check ck && ck.infos[ro.defpos] is not null)
                            {
                                var rb = new RoleTableCheckBookmark(cx,res, 0, outer, inner);
                                if (rb.Match(res) && Eval(res.where, cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current value: (TableName,CheckName,Select,Pos)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ck)
            {
                var ch = (Check)ck;
                if (_cx._Dom(rs) is not Domain dr || _cx._Ob(ch.checkobjpos) is not DBObject oc)
                    throw new PEException("PE0474");
                return new TRow(dr,
                    new TChar(oc.NameFor(_cx)),
                    new TChar(ch.name??""),
                    new TChar(ch.source));
            }
            /// <summary>
            /// Move to the next TableCheck
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context cx)
            {
                var outer = _outer;
                var inner = _inner;
                if (cx.role is not Role ro)
                    throw new PEException("PE49526");
                if (outer == null)
                    return null;
                for (; ; )
                {
                    if ((inner = inner?.Next())!= null &&
                        cx.obs[inner.key()] is Check)
                            goto test;
                    if ((outer = outer.Next()) == null)
                        return null;
                    if (outer.value() is not Table rt)
                        continue;
                    inner = rt.tableChecks?.First();
                    if (inner == null)
                        continue;
                    if (cx.obs[inner.key()] is not Check ck || ck.infos[ro.defpos] is null)
                        continue;
                    test:
                    var rb = new RoleTableCheckBookmark(cx,res, _pos + 1, outer, inner);
                    if (rb.Match(res) && Eval(res.where, cx))
                        return rb;
                }
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$TablePeriod table
        /// </summary>
        static void RoleTablePeriodResults()
        {
            var t = new SystemTable("Role$TablePaeriod");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "TableName", Domain.Char,1);
            t+=new SystemTableColumn(t, "PeriodName", Domain.Char,1);
            t+=new SystemTableColumn(t, "PeriodStartColumn", Domain.Char,0);
            t+=new SystemTableColumn(t, "PeriodEndColumn", Domain.Char,0);
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$TablePeriod
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleTablePeriodBookmark : SystemBookmark
        {
            /// <summary>
            /// Enumerators for traversing trees
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly bool _system;
            /// <summary>
            /// create the RoleTablePeriod enumerator
            /// </summary>
            /// <param name="r"></param>
            RoleTablePeriodBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object>outer,
                bool sys) : base(_cx,res,pos,outer.key(),((PeriodDef)outer.value()).lastChange,
                    _Value(_cx,res,outer.value(),sys))
            {
                _outer = outer; _system = sys;
            }
            internal static RoleTablePeriodBookmark? New(Context _cx, SystemRowSet res)
            {
                if (_cx.role is not Role ro)
                    throw new PEException("PE49527");
                for (var outer = _cx.db.objects.PositionAt(0); outer != null;
                    outer = outer.Next())
                    if (outer.value() is Table t && t.infos[ro.defpos] is not null)
                    {
                        if (t.systemPS > 0)
                        {
                            var rb = new RoleTablePeriodBookmark(_cx, res, 0, outer, true);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                        else if (t.applicationPS > 0)
                        {
                            var rb = new RoleTablePeriodBookmark(_cx, res, 0, outer, false);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                    }
                return null;
            }
            /// <summary>
            /// the current value: (TableName,CheckName,Select,Pos)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object tb, bool sys)
            {
                if (_cx.role is not Role ro)
                    throw new DBException("42105");
                var t = (Table)tb;
                var dm = _cx._Dom(t) ?? throw new PEException("PE0480");
                var oi = t.infos[_cx.role.defpos] ?? throw new PEException("PE0481");
                var pd = (PeriodDef)(_cx.db.objects[sys ? t.systemPS : t.applicationPS]
                    ?? throw new PEException("PE0482"));
                var op = pd.infos[_cx.role.defpos];
                string sn="", en="";
                for (var b = dm.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && _cx._Ob(p) is DBObject ob 
                        && ob.infos[ro.defpos] is ObInfo ci)
                    {
                        if (p == pd.startCol)
                            sn = ci.name ?? "";
                        if (p == pd.endCol)
                            en = ci.name ?? "";
                    }
                return new TRow(_cx._Dom(rs) ?? throw new PEException("PE0483"), 
                    Pos(t.defpos),
                    new TChar(oi.name??""),
                    new TChar(op?.name??""),
                    new TChar(sn),
                    new TChar(en));
            }
            /// <summary>
            /// Move to the next PeriodDef
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var system = _system;
                if (_cx.role is not Role ro)
                    throw new PEException("PE49528");
                if (outer == null)
                    return null;
                for (; ; )
                {
                    if (system && outer.value() is Table t && 
                        t.infos[ro.defpos] is not null && t.applicationPS>0)
                    {
                        var rb = new RoleTablePeriodBookmark(_cx, res, 0, outer, false);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                    if ((outer = outer.Next()) == null)
                        return null;
                    system = true;
                    if (outer.value() is Table tb && 
                        tb.infos[ro.defpos] is not null && tb.systemPS > 0)
                    {
                        var rb = new RoleTablePeriodBookmark(_cx, res, 0, outer, true);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                }
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$Column table
        /// </summary>
        static void RoleColumnResults()
        {
            var t = new SystemTable("Role$Column");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Table", Domain.Char,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Seq", Domain.Int,1);
            t+=new SystemTableColumn(t, "Domain", Domain.Position,0);
            t+=new SystemTableColumn(t, "Default", Domain.Char,0);
            t+=new SystemTableColumn(t, "NotNull", Domain.Bool,0);
            t+=new SystemTableColumn(t, "Generated", Domain.Char,0);
            t+=new SystemTableColumn(t, "Update", Domain.Char,0);
            t.AddIndex("Pos");
            t.AddIndex("Table", "Name");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Column:
        /// table and view column names as seen from the current role
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleColumnBookmark : SystemBookmark
        {
            /// <summary>
            /// Enumerators for implementation
            /// </summary>
            readonly ABookmark<long, object> _outer;
            readonly ABookmark<string, long?> _inner;
            /// <summary>
            /// create the Sys$Column enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleColumnBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<string,long?> inner)
                : base(_cx,res, pos, inner.value()??-1L,((DBObject)outer.value()).lastChange,
                      _Value(_cx,res,outer.value(),(int)inner.position(),inner.value()??-1L,
                          _cx._Dom(inner.value()??-1L) ?? throw new PEException("PE0485")))
            {
                _outer = outer;
                _inner = inner;
            }
            internal static RoleColumnBookmark? New(Context _cx, SystemRowSet res)
            {
                if (_cx.role is not Role ro)
                    throw new PEException("PE49529");
                for (var outer = _cx.db.objects.PositionAt(0); outer != null;
                    outer = outer.Next())
                    if (outer.value() is Table tb && tb.infos[ro.defpos] is ObInfo oi)
                        for (var inner = oi.names.First(); inner != null;
                                inner = inner.Next())
                        {
                            var rb = new RoleColumnBookmark(_cx, res, 0, outer, inner);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Table,Name,Seq,Unique,Domain,Default,NotNull,Generated)
            /// </summary>
            static TRow _Value(Context cx, SystemRowSet rs, object ta, int i,long p,Domain d)
            {
                if (cx.role is not Role ro)
                    throw new DBException("42105");
                if (ta is not Table tb || tb.infos[ro.defpos] is not ObInfo oi
                    || cx.db.objects[p] is not TableColumn tc
                    || tc.infos[ro.defpos] is not ObInfo si)
                    throw new PEException("PE0491");
                return new TRow(cx._Dom(rs) ?? throw new PEException("PE0492"),
                    Pos(p),
                    new TChar(oi.name??""),
                    new TChar(si.name??""),
                    new TInt(i),
                    Pos(cx.db.types[d]),
                    new TChar(d.defaultString??""),
                    TBool.For(d.notNull),
                    new TChar(tc.generated.gfs),
                    new TChar(tc.updateString??""));
            }
            /// <summary>
            /// Move to the next Column def
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                if (_cx.role is not Role ro)
                    throw new PEException("PE49530");
                for (; outer != null; outer = outer.Next())
                    if (outer.value() is Table tb && tb.infos[ro.defpos] is ObInfo oi)
                    {
                        if (inner == null)
                            inner = oi.names.First();
                        else
                            inner = inner.Next();
                        for (; inner != null; inner = inner.Next())
                        {
                            var rb = new RoleColumnBookmark(_cx, res, _pos + 1, outer, inner);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                        inner = null;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$Class table
        /// </summary>
        static void RoleClassResults()
        {
            var t = new SystemTable("Role$Class");
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "Key", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definition", Domain.Char,0);
            t.Add();
        }
        // shareable as of 26 April 2021
        internal class RoleClassBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _enu;
            RoleClassBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> e)
                    : base(_cx,res, pos, e.key(), ((DBObject)e.value()).lastChange,
                          _Value(_cx,res,e))
            {
                _enu = e;
            }
            internal static RoleClassBookmark? New(Context _cx, SystemRowSet res)
            {
                if (_cx.role is not Role ro)
                    throw new PEException("PE49500");
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                {
                    var t = (DBObject)outer.value();
                    if (t.infos[ro.defpos] is not null && (t is Table || t is View))
                    {
                        var rb = new RoleClassBookmark(_cx,res, 0, outer);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                var enu = _enu;
                if (_cx.role is not Role ro)
                    throw new PEException("PE49501");
                for (enu = enu.Next(); enu != null; enu = enu.Next())
                {
                    var tb = (DBObject)enu.value();
                    if (tb.infos[ro.defpos] is not null && (tb is Table || tb is View))
                    {
                        var rb = new RoleClassBookmark(_cx,res, _pos + 1, enu);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
            static TRow _Value(Context _cx,SystemRowSet res,ABookmark<long,object>e)
            {
                return ((DBObject)e.value())?.RoleClassValue(_cx,res, e)
                    ??throw new PEException("PE42108");
            }
        }

        /// <summary>
        /// set up the Role$ColumnCheck table
        /// </summary>
        static void RoleColumnCheckResults()
        {
            var t = new SystemTable("Role$ColumnCheck");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0); 
            t+=new SystemTableColumn(t, "TableName", Domain.Char,1);
            t+=new SystemTableColumn(t, "ColumnName", Domain.Char,1);
            t+=new SystemTableColumn(t, "CheckName", Domain.Char,1);
            t+=new SystemTableColumn(t, "Select", Domain.Char,0);
            t.AddIndex("Pos");
            t.AddIndex("TableName", "CheckName");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$ColumnCheck
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleColumnCheckBookmark : SystemBookmark
        {
            /// <summary>
            /// 3 enumerators for implementation!
            /// </summary>
            readonly ABookmark<long,bool> _inner;
            readonly ABookmark<string, long?> _middle;
            readonly ABookmark<long,object> _outer;
            /// <summary>
            /// create the Sys$ColumnCheck enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleColumnCheckBookmark(Context cx, SystemRowSet res,int pos,ABookmark<long,object>outer,
                ABookmark<string,long?> middle,ABookmark<long,bool> inner)
                : base(cx,res,pos,inner.key(),((DBObject?)cx.db.objects[inner.key()])?.lastChange??-1L,
                      _Value(cx,res,outer.value(),middle.value()??-1L,inner.key()))
            {
                _outer = outer;
                _middle = middle;
                _inner = inner;
            }
            internal static RoleColumnCheckBookmark? New(Context cx, SystemRowSet res)
            {
                if (cx.role is not Role ro)
                    throw new PEException("PE49502");
                for (var outer = cx.db.objects.PositionAt(0); outer != null;
                    outer = outer.Next())
                    if (outer.value() is Table tb && tb.infos[ro.defpos] is ObInfo oi)
                        for (var middle = oi.names.First(); middle != null;
                                middle = middle.Next())
                        if (middle.value() is long p && cx.db.objects[p] is TableColumn tc
                                && tc.infos[ro.defpos] is not null)
                                for (var inner = tc.constraints.First(); inner != null;
                                        inner = inner.Next())
                                    if (cx.db.objects[inner.key()] is Check)
                                    {
                                        var rb = new RoleColumnCheckBookmark(cx, res, 0, outer,
                                            middle, inner);
                                        if (rb.Match(res) && Eval(res.where, cx))
                                            return rb;
                                    }
                return null;
            }
            /// <summary>
            /// the current value: Table,Column,Check,Select,Pos)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ta,long tc,long ck)
            {
                if (_cx.role is not Role ro || ta is not Table tb ||
                    tb.infos[ro.defpos] is not ObInfo oi ||
                    _cx._Ob(tc) is not DBObject ot || ot.infos[ro.defpos] is not ObInfo ci ||
                    _cx.db.objects[ck] is not Check ch)
                    throw new PEException("PE0496");
                return new TRow(_cx._Dom(rs) ?? throw new PEException("PE0497"), 
                    Pos(ch.defpos),
                    new TChar(oi.name??""),
                    new TChar(ci.name??""),
                    new TChar(ch.name??""),
                    new TChar(ch.source??""));
            }
            /// <summary>
            /// Move to the next ColumnCheck
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                if (_cx.role is not Role ro)
                    throw new PEException("PE49504");
                for (; ; )
                {
                    if (inner != null && (inner = inner.Next()) != null && middle!=null)
                    {
                        var rb = new RoleColumnCheckBookmark(_cx, res, _pos + 1, outer, middle, inner);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                        continue;
                    }
                    if (middle != null && (middle = middle.Next()) != null)
                        if (middle.value() is long p && _cx.db.objects[p] is TableColumn tc &&
                            tc.infos[ro.defpos] is not null)
                        {
                            inner = tc.constraints.First();
                            continue;
                        }
                    if ((outer = outer.Next()) == null)
                        return null;
                    if (outer.value() is not Table tb || tb.infos[ro.defpos] is not ObInfo oi)
                        continue;
                    middle = oi.names.First();
                    if (inner != null && middle!=null)
                    {
                        var rb = new RoleColumnCheckBookmark(_cx, res, _pos + 1, outer, middle, inner);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                        continue;
                    }
                }
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$ColumnPrivileges table
        /// </summary>
        static void RoleColumnPrivilegeResults()
        {
            var t = new SystemTable("Role$ColumnPrivilege");
            t+=new SystemTableColumn(t, "Table", Domain.Char,1);
            t+=new SystemTableColumn(t, "Column", Domain.Char,1);
            t+=new SystemTableColumn(t, "Grantee", Domain.Char,1);
            t+=new SystemTableColumn(t, "Privilege", Domain.Char,0);
            t.AddIndex("Table", "Column", "Grantee");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$columnPrivilege
        /// // shareable as of 26 April 2021
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
                : base(_cx,res,pos,inner.key(),0,
                      _Value(_cx,res,middle.value(),inner.value()))
            {
                _middle = middle;
                _inner = inner;
            }
            internal static RoleColumnPrivilegeBookmark? New(Context _cx, SystemRowSet res)
            {
                if (_cx.role is not Role ro)
                    throw new PEException("PE49505");
                for (var middle = _cx.db.objects.PositionAt(0); middle != null; middle = middle.Next())
                    if (middle.value() is Table tb)
                        for (var inner = _cx.db.objects.PositionAt(0); inner != null; inner = inner.Next())
                            if (inner.value() is TableColumn tc && tc.tabledefpos==tb.defpos &&
                                tc.infos[ro.defpos] is not null)
                            {
                                var rb = new RoleColumnPrivilegeBookmark(_cx,res, 0, middle, inner);
                                if (rb.Match(res) && Eval(res.where, _cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current value: (Table,Column,GranteeType,Grantee,Privilege)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object cb,object rb)
            {
                if (rb is not Role ro || cb is not TableColumn tc ||
                    tc.infos[ro.defpos] is not ObInfo dc ||
                    _cx.db.objects[tc.tabledefpos] is not Table tb ||
                    tb.infos[ro.defpos] is not ObInfo dt ||
                    _cx._Dom(rs) is not Domain dm)
                        throw new PEException("PE42107");
                return new TRow(dm,
                    new TChar(dt.name??""),
                    new TChar(dc.name??""),
                    new TChar(ro.name??""),
                    new TChar(dc.priv.ToString()));
            }
            /// <summary>
            /// Move to the next ColumnPrivilege obs
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var middle = _middle;
                var inner = _inner;
                if (_cx.role is not Role ro)
                    throw new PEException("PE49506");
                if (inner != null && (inner = inner.Next()) != null
                    && inner.value() is TableColumn tc && tc.defpos == middle.key()
                    && tc.infos[ro.defpos] is not null)
                {
                    var rb = new RoleColumnPrivilegeBookmark(_cx,res, _pos + 1, middle, inner);
                    if (rb.Match(res) && Eval(res.where, _cx))
                        return rb;
                }
                for (middle = middle.Next(); middle != null; middle = middle.Next())
                    if (middle.value() is SqlValue mc)
                        for (inner = _cx.db.objects.PositionAt(0); inner != null; inner = inner.Next())
                            if (_cx.db.objects[mc.defpos] is SqlValue rc && rc.defpos == mc.defpos)
                            {
                                var rb = new RoleColumnPrivilegeBookmark(_cx,res, _pos + 1, middle, inner);
                                if (rb.Match(res) && Eval(res.where, _cx))
                                    return rb;
                            }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$Domain table
        /// </summary>
        static void RoleDomainResults()
        {
            var t = new SystemTable("Role$Domain");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "DataType", Domain.Char,0);
            t+=new SystemTableColumn(t, "DataLength", Domain.Int,0);
            t+=new SystemTableColumn(t, "Scale", Domain.Int,0);
            t+=new SystemTableColumn(t, "StartField", Domain.Char,0);
            t+=new SystemTableColumn(t, "EndField", Domain.Char,0);
            t+=new SystemTableColumn(t, "DefaultValue", Domain.Char,0);
            t+=new SystemTableColumn(t, "Struct", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definer", Domain.Char,0);
            t.AddIndex("Pos");
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// and enumerator for Role$Domain
        /// // shareable as of 26 April 2021
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
                : base(_cx,res,pos,outer.key(),((Domain)outer.value()).lastChange,
                      _Value(_cx,res,outer.value()))
            {
                _en = outer;
            }
            internal static RoleDomainBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var b = _cx.db.objects.PositionAt(0);b!= null;b=b.Next())
                    if (b.value() is Domain)
                    {
                        var rb =new RoleDomainBookmark(_cx,res, 0, b);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,DataType,dataLength,Scale,DefaultValue,NotNull,Struct)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ob)
            {
                long prec = 0;
                long scale = 0;
                var dm = (Domain)ob;
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
                if (dm.elType!=Domain.Null)
                    elname = dm.elType.name;
                if (_cx._Dom(rs) is not Domain dr)
                        throw new PEException("PE42107");
                return new TRow(dr,
                    Pos(_cx.db.types[dm]),
                    new TChar(dm.name),
                    new TChar(dm.kind.ToString()),
                    new TInt(prec),
                    new TInt(scale),
                    new TChar(start),
                    new TChar(end),
                    new TChar(dm.defaultValue.ToString()),
                    new TChar(elname),
                    TNull.Value);
            }
            /// <summary>
            /// Move to next Domain
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _en;
                for (outer = outer.Next();outer!= null;outer=outer.Next())
                if (outer.value() is Domain)
                    {
                        var rb = new RoleDomainBookmark(_cx,res, _pos + 1, outer);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$Index table
        /// </summary>
        static void RoleIndexResults()
        {
            var t = new SystemTable("Role$Index");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Table", Domain.Char,0);
            t+=new SystemTableColumn(t, "Flags", Domain.Char,0);
            t+=new SystemTableColumn(t, "RefTable", Domain.Char,0);
            t+=new SystemTableColumn(t, "Distinct", Domain.Char,0); // probably ""
            t+=new SystemTableColumn(t, "Adapter", Domain.Char,0);
            t+=new SystemTableColumn(t, "Rows", Domain.Int,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Index
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleIndexBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the indexes
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<Domain, CTree<long,bool>> _middle;
            readonly ABookmark<long, bool> _inner;
            /// <summary>
            /// craete the Sys$Index enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleIndexBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object> outer,
                ABookmark<Domain,CTree<long,bool>>middle, ABookmark<long,bool>inner, long lc)
                : base(_cx,res,pos,inner.key(),lc,
                      _Value(_cx,res,inner.key()))
            {
                _outer = outer;
                _middle = middle;
                _inner = inner;
            }
            internal static RoleIndexBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table tb)
                        for (var middle = tb.indexes.First(); middle != null; middle = middle.Next())
                            for (var inner = middle.value().First(); inner != null; inner = inner.Next())
                            {
                                var lc = _cx.obs[inner.key()]?.lastChange ?? -1L;
                                var rb = new RoleIndexBookmark(_cx, res, 0, outer, middle, inner, lc);
                                if (rb.Match(res) && Eval(res.where, _cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Table,Name,Flags,RefTable,RefIndex,Distinct)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, long xp)
            {
                if (_cx._Dom(rs) is not Domain dr || _cx.db.objects[xp] is not Level3.Index x ||
                    _cx.db.objects[x.tabledefpos] is not Table t || _cx.role is not Role ro ||
                    t.infos[ro.defpos] is not ObInfo oi || oi.name==null || x.rows is not MTree mt)
                    throw new PEException("PE48190");
                var rx = (Level3.Index?)_cx.db.objects[x.refindexdefpos];
                var rt = _cx._Ob(x.reftabledefpos);
                var ri = rt?.infos[_cx.role.defpos];
                var ad = _cx._Ob(x.adapter);
                var ai = ad?.infos[_cx.role.defpos];
                return new TRow(dr,
                   Pos(x.defpos),
                   new TChar(oi.name),
                   new TChar(x.flags.ToString()),
                   new TChar(ri?.name??""),
                   new TChar(rx?.rows?.Count.ToString()??""),
                   new TChar(ai?.name??""),
                   new TInt(mt.Count)
                   );
            }
            /// <summary>
            /// Move to next index
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                {
                    var lc = _cx.obs[inner.key()]?.lastChange ?? -1L;
                    var rb = new RoleIndexBookmark(_cx, res, _pos + 1, outer, middle, inner, lc);
                    if (rb.Match(res) && Eval(res.where, _cx))
                        return rb;
                }
                for (middle = middle.Next(); middle != null; middle = middle.Next())
                    for (inner = middle.value().First(); inner != null; inner = inner.Next())
                    {
                        var lc = _cx.obs[inner.key()]?.lastChange ?? -1L;
                        var rb = new RoleIndexBookmark(_cx, res, _pos + 1, outer, middle, inner, lc);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    if (outer.value() is Table tb)
                        for (middle = tb.indexes.First(); middle != null; middle = middle.Next())
                            for (inner = middle.value().First(); inner != null; inner = inner.Next())
                            {
                                var lc = _cx.obs[inner.key()]?.lastChange ?? -1L;
                                var rb = new RoleIndexBookmark(_cx, res, _pos + 1, outer, middle, inner, lc);
                                if (rb.Match(res) && Eval(res.where, _cx))
                                    return rb;
                            }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$IndxeKey table
        /// </summary>
        static void RoleIndexKeyResults()
        {
            var t = new SystemTable("Role$IndexKey");
            t+=new SystemTableColumn(t,"IndexName", Domain.Char,1);
            t+=new SystemTableColumn(t, "TableColumn", Domain.Char,0);
            t+=new SystemTableColumn(t, "Position", Domain.Int,1);
            t.AddIndex("IndexName", "TableColumn");
            t.AddIndex("IndexName", "Position");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$IndexKey
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleIndexKeyBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _outer;
            readonly ABookmark<Domain, CTree<long,bool>> _second;
            readonly ABookmark<long,bool> _third;
            readonly ABookmark<int, long?> _inner;
            /// <summary>
            /// create the Role$IndexKey enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleIndexKeyBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<Domain, CTree<long, bool>> second, ABookmark<long, bool> third,
                ABookmark<int, long?> inner)
                : base(_cx, res, pos, inner.value()??-1L, 0, _Value(_cx, res, inner.key(),
                      (TableColumn)(_cx.db.objects[inner.value()??-1L] ?? throw new PEException("PE49212"))))
            {
                _outer = outer; _second = second; _third = third; _inner = inner;
            }
            internal static RoleIndexKeyBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table tb)
                        for (var second = tb.indexes.First(); second != null; second = second.Next())
                            for (var third = second.value().First(); third != null; third = third.Next())
                                if (_cx.db.objects[third.key()] is Level3.Index x)
                                    for (var inner = x.keys.First();
                                        inner != null; inner = inner.Next())
                                    {
                                        var rb = new RoleIndexKeyBookmark(_cx, res, 0, outer, second, third, inner);
                                        if (rb.Match(res) && Eval(res.where, _cx))
                                            return rb;
                                    }
                return null;
            }
            /// <summary>
            /// the current value: (Indexname,TableColumn,Position)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, int i, TableColumn tc)
            {
                if (tc.infos[_cx.role.defpos] is not ObInfo oi ||
                    _cx._Dom(rs) is not Domain dm)
                    throw new DBException("42105");
                return new TRow(dm,
                    Pos(tc.defpos),
                    new TChar(oi?.name??""),
                    new TInt(i));
            }
            /// <summary>
            /// Move to next Indexkey obs
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var second = _second;
                var third = _third;
                var inner = _inner;
                for (inner = inner.Next();inner!=null;inner=inner.Next())
                {
                    var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, second, third, inner);
                    if (rb.Match(res) && Eval(res.where, _cx))
                        return rb;
                }
                for (third = third.Next();third!=null;third=third.Next())
                    for (inner=((Level3.Index?)_cx.db.objects[third.key()])?.keys.First();
                        inner!=null;inner=inner.Next())
                    {
                        var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, second, third, inner);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                for (second=second.Next();second!=null;second=second.Next())
                    for (third = second.value()?.First();third!=null;third=third.Next())
                    for (inner=((Level3.Index?)_cx.db.objects[third.key()])?.keys.First();inner!=null;inner=inner.Next())
                    {
                        var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, second, third, inner);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                for (outer = outer.Next(); outer!=null;outer=outer.Next())
                    if (outer.value() is Table tb)
                        for (second = tb.indexes.First(); second != null; second = second.Next())
                            for (third=second.value()?.First();third!=null;third=third.Next())
                            for (inner = ((Level3.Index?)_cx.db.objects[third.key()])?.keys.First(); inner != null;
                                inner = inner.Next())
                            {
                                var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, second, third, inner);
                                if (rb.Match(res) && RowSet.Eval(res.where, _cx))
                                    return rb;
                            }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$Java table
        /// </summary>
        static void RoleJavaResults()
        {
            var t = new SystemTable("Role$Java");
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "Key", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definition", Domain.Char,0);
            t.Add();
        }
        // shareable as of 26 April 2021
        internal class RoleJavaBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _enu;
            RoleJavaBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> e) 
                : base(_cx,res, pos,e.key(),((DBObject)e.value()).lastChange,_Value(_cx,res,e))
            {
                _enu = e;
            }
            internal static RoleJavaBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is DBObject tb && (tb is Table || tb is View)
                        && tb is not RestView)
                    {
                        var rb = new RoleJavaBookmark(_cx,res, 0, outer);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                var enu = _enu;
                for (enu = enu.Next(); enu != null; enu = enu.Next())
                    if (enu.value() is DBObject tb && (tb is Table || tb is View) && tb is not RestView)
                    {
                        var rb = new RoleJavaBookmark(_cx,res, _pos + 1, enu);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
            static TRow _Value(Context _cx, SystemRowSet rs, ABookmark<long,object>e)
            {
                return ((DBObject)e.value()).RoleJavaValue(_cx,rs, e);
            }
        }
        /// <summary>
        /// set up the Role$Python table
        /// </summary>
        static void RolePythonResults()
        {
            var t = new SystemTable("Role$Python");
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "Key", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definition", Domain.Char,0);
            t.Add();
        }
        // shareable as of 26 April 2021
        internal class RolePythonBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _enu;
            RolePythonBookmark(Context _cx, SystemRowSet res, int pos, 
                ABookmark<long, object> e) 
                : base(_cx,res, pos,e.key(), ((DBObject)e.value()).lastChange,
                      _Value(_cx, res, e))
            {
                _enu = e;
            }
            internal static RolePythonBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is DBObject tb && (tb is Table || tb is View) && tb is not RestView)
                    {
                        var rb = new RolePythonBookmark(_cx,res, 0, outer);
                        if (rb.Match(res) && RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                var enu = _enu;
                for (enu=enu.Next();enu!=null;enu=enu.Next())
                    if (enu.value() is DBObject tb && (tb is Table || tb is View) && tb is not RestView)
                    {
                        var rb = new RolePythonBookmark(_cx,res, _pos+1, enu);
                        if (rb.Match(res) && RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
            static TRow _Value(Context _cx, SystemRowSet rs, ABookmark<long, object> e)
            {
                return ((DBObject)e.value()).RolePythonValue(_cx,rs, e);
            }
        }
        /// <summary>
        /// set up the Role$Procedure table
        /// </summary>
        static void RoleProcedureResults()
        {
            var t = new SystemTable("Role$Procedure");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "Arity", Domain.Int,1);
            t+=new SystemTableColumn(t, "Returns", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definition", Domain.Char,0);
            t+=new SystemTableColumn(t, "Inverse", Domain.Char,0);
            t+=new SystemTableColumn(t, "Monotonic", Domain.Bool,0);
            t+=new SystemTableColumn(t, "Definer", Domain.Char,0);
            t.AddIndex("Pos");
            t.AddIndex("Name", "Arity");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Procedure
        /// // shareable as of 26 April 2021
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
                : base(_cx,res,pos,en.key(),((DBObject)en.value()).lastChange,
                      _Value(_cx,res,en.value()))
            {
                _en = en;
            }
            internal static RoleProcedureBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var en = _cx.db.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is Procedure && en.value() is not Method)
                    {
                        var rb =new RoleProcedureBookmark(_cx,res, 0, en);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Definition)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ob)
            {
                Procedure p = (Procedure)ob;
                if (_cx._Dom(rs) is not Domain dr 
                    || p.infos[_cx.db.role.defpos] is not ObInfo oi
                    || oi.name == null || _cx.db.objects[p.definer] is not Role de)
                    throw new PEException("PE23104");
                string inv = "";
                if (_cx._Ob(p.inverse) is DBObject io)
                    inv = io.NameFor(_cx);
                return new TRow(dr,
                    Pos(p.defpos),
                    new TChar(oi.name),
                    new TInt(p.arity),
                    new TChar(p.domain.ToString()),
                    new TChar(p.clause),
                    new TChar(inv),
                    p.monotonic ? TBool.True : TBool.False,
                    new TChar(de.name??""));
            }
            /// <summary>
            /// Move to the next procedure
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var en = _en;
                for (en = en.Next(); en != null; en = en.Next())
                    if (en.value() is Procedure ob && ob is not Method)
                    {
                        var rb = new RoleProcedureBookmark(_cx,res, _pos + 1, en);
                        if (rb.Match(res) && RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void RoleParameterResults()
        {
            var t = new SystemTable("Role$Parameter");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t,"Seq", Domain.Int,1);
            t+=new SystemTableColumn(t,"Name", Domain.Char,0);
            t+=new SystemTableColumn(t,"Type", Domain.Char,0);
            t+=new SystemTableColumn(t,"Mode", Domain.Char,0);
            t.AddIndex("Pos","Seq");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Parameter
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleParameterBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the procedures tree
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<int, long?> _inner;
            /// <summary>
            /// create the Rle$Parameter enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleParameterBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> en,
                ABookmark<int,long?> inner, FormalParameter fp)
                : base(_cx,res,pos,en.key(), fp.lastChange,
                      _Value(_cx,res,en.key(),inner.key(),fp))
            {
                _outer = en;
                _inner = inner;
            }
            internal static RoleParameterBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var en = _cx.db.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is Procedure p && p.arity > 0)
                        for (var inner = p.ins.First(); inner != null; inner = inner.Next())
                            if (inner.value() is long ip && _cx.obs[ip] is FormalParameter fp)
                            {
                                var rb = new RoleParameterBookmark(_cx, res, 0, en, inner, fp);
                                if (rb.Match(res) && Eval(res.where, _cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name,Definition)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, long dp, int i, FormalParameter pp)
            {
                if (_cx._Dom(rs) is not Domain dr || pp.name == null)
                    throw new PEException("PE23106");
                return new TRow(dr,
                    Pos(dp),
                    new TInt(i),
                    new TChar(pp.name),
                    new TChar(pp.domain.ToString()),
                    new TChar(((pp.result==Sqlx.RESULT) ? Sqlx.RESULT : pp.paramMode).ToString()));
            }
            /// <summary>
            /// Move to the next parameter
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                    if (inner.value() is long ip && _cx.obs[ip] is FormalParameter fp)
                    {
                        var rb = new RoleParameterBookmark(_cx, res, _pos + 1, outer, inner, fp);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    if (outer.value() is Procedure p)
                        for (inner = p.ins.First(); inner != null; inner = inner.Next())
                            if (inner.value() is long ip && _cx.obs[ip] is FormalParameter fp)
                            {
                                var rb = new RoleParameterBookmark(_cx, res, _pos + 1, outer, inner, fp);
                                if (rb.Match(res) && Eval(res.where, _cx))
                                    return rb;
                            }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void RoleSubobjectResults()
        {
            var t = new SystemTable("Role$Subobject");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Type", Domain.Char,1);
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "Seq", Domain.Int,1);
            t+=new SystemTableColumn(t, "Column", Domain.Char,0);
            t+=new SystemTableColumn(t, "Subobject", Domain.Char,0);
            t.AddIndex("Type", "Name", "Seq");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Object
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleSubobjectBookmark : SystemBookmark
        {
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<long,bool> _inner;
            readonly int _sq;
            /// <summary>
            /// create the Sys$Procedure enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleSubobjectBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> en,
                ABookmark<long,bool> ix, int i)
                : base(_cx,res,pos,en.key(),((DBObject)en.value()).lastChange,
                      _Value(_cx,res,en.value(),ix.key(),i))
            {
                _outer = en;
                _inner = ix;
                _sq = i;
            }
            internal static RoleSubobjectBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var en = _cx.db.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is DBObject ob)
                    {
                        var i = 0;
                        for (var ix = ob.dependents.First(); ix != null; ix = ix.Next(),i++)
                        {
                            var rb = new RoleSubobjectBookmark(_cx, res, 0, en, ix, i);
                            if (rb.Match(res) && RowSet.Eval(res.where, _cx))
                                return rb;
                        }
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Name,Type,Owner,Source)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object oo, long xp,int sq)
            {
                if (oo is not DBObject ob || _cx.role is not Role ro || _cx._Dom(rs) is not Domain dr
                    || ob.infos[ro.defpos] is not ObInfo oi || oi.name == null
                    || _cx.db.objects[xp] is not DBObject ox)
                    throw new DBException("42105");
                return new TRow(dr,
                    Pos(ob.defpos),
                    new TChar(ob.GetType().Name),
                    new TChar(oi.name),
                    new TInt(sq),
                    Pos(xp),
                    new TChar(ox.ToString()));
            }
            /// <summary>
            /// Move to the next object
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                var sq = _sq;
                for (inner = inner.Next(); inner != null; inner = inner.Next(),sq++)
                {
                    var rb = new RoleSubobjectBookmark(_cx,res, _pos + 1, outer, inner, sq);
                    if (rb.Match(res) && Eval(res.where, _cx))
                        return rb;
                }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    if (outer.value() is DBObject ob)
                    {
                        sq = 0;
                        for (inner = ob.dependents.First(); inner != null; inner = inner.Next(), sq++)
                        {
                            var rb = new RoleSubobjectBookmark(_cx, res, _pos + 1, outer, inner, sq);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// set up the Role$Table table
        /// </summary>
        static void RoleTableResults()
        {
            var t = new SystemTable("Role$Table");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "Columns", Domain.Int,0);
            t+=new SystemTableColumn(t, "Rows", Domain.Int,0);
            t+=new SystemTableColumn(t, "Triggers", Domain.Int,0);
            t+=new SystemTableColumn(t, "CheckConstraints", Domain.Int,0);
            t+=new SystemTableColumn(t, "RowIri", Domain.Char,0);
            t+=new SystemTableColumn(t, "LastData", Domain.Int, 0);
            t.AddIndex("Pos");
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Table
        /// // shareable as of 26 April 2021
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
                : base(_cx,res,pos,en.key(),((Table)en.value()).lastChange,
                      _Value(_cx,res,en.value()))
            {
                _en = en;
            }
            internal static RoleTableBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var b = _cx.db.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is Table t && _cx.role!=null &&
                        t.infos[_cx.role.defpos] is ObInfo oi && oi.name is string s
                        && s[0]!='(')
                    {
                        var rb =new RoleTableBookmark(_cx,res, 0, b);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value (Pos,Name,Columns,Rows,Triggers,CheckConstraints,Changes,References,Owner)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ob)
            {
                Table t = (Table)ob;
                if (_cx._Dom(rs) is not Domain dr || _cx._Dom(t) is not Domain rt)
                    throw new DBException("42105");
                var oi = t.infos[_cx.role.defpos] ?? t.infos[t.definer];
                return new TRow(dr,
                    Pos(t.defpos),
                    new TChar(oi?.name??rt.name),
                    new TInt(rt.Length),
                    new TInt(t.tableRows.Count),
                    new TInt(t.triggers.Count),
                    new TInt(t.tableChecks.Count),
                    new TChar(t.iri??""),
                    new TInt(t.lastData));
            }
            /// <summary>
            /// Move to next Table
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var en = _en;
                for (en=en.Next();en!=null;en=en.Next())
                    if (en.value() is Table t && _cx.role != null &&
                        t.infos[_cx.role.defpos] is ObInfo oi && oi.name is string s
                        && s[0] != '(')
                    {
                        var rb =new RoleTableBookmark(_cx,res, _pos + 1, en);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$Trigger table
        /// </summary>
        static void RoleTriggerResults()
        {
            var t = new SystemTable("Role$Trigger");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "Flags", Domain.Char,0);
            t+=new SystemTableColumn(t, "TableName", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definer", Domain.Char,0);
            t.AddIndex("Pos");
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Trigger
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleTriggerBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for implementation
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<PTrigger.TrigType,CTree<long, bool>> _middle;
            readonly ABookmark<long,bool> _inner;
            /// <summary>
            /// create the Sys$Trigger enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleTriggerBookmark(Context cx, SystemRowSet res,int pos,ABookmark<long,object> outer,
                ABookmark<PTrigger.TrigType,CTree<long,bool>>middle,ABookmark<long,bool>inner, 
                Trigger tg)
                : base(cx,res,pos,inner.key(), tg.lastChange, _Value(cx,res,outer.value(),tg))
            {
                _outer = outer;
                _middle = middle;
                _inner = inner;
            }
            internal static RoleTriggerBookmark? New(Context cx, SystemRowSet res)
            {
                for (var outer = cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table t)
                        for (var middle = t.triggers.First(); middle != null; middle = middle.Next())
                            for (var inner = middle.value().First(); inner != null; inner = inner.Next())
                                if (cx.db.objects[inner.key()] is Trigger tg)
                                {
                                    var rb = new RoleTriggerBookmark(cx, res, 0, outer, middle, inner,tg);
                                    if (rb.Match(res) && Eval(res.where, cx))
                                        return rb;
                                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,name,Flags,TableName,Def)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ob, Trigger tg)
            {
                Table tb = (Table)ob;
                if (_cx.role is not Role ro || _cx._Dom(rs) is not Domain dr ||
                    tb.infos[ro.defpos] is not ObInfo oi || oi.name == null ||
                    _cx.db.objects[tg.definer] is not Role de)
                    throw new DBException("42105");
                return new TRow(dr,
                    Pos(tg.defpos),
                    new TChar(tg.name),
                    new TChar(tg.tgType.ToString()),
                    new TChar(oi.name),
                    new TChar(de.name??""));
            }            /// <summary>
                         /// Move to the next Trigger
                         /// </summary>
                         /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                    if (_cx.db.objects[inner.key()] is Trigger tg)
                    {
                        var rb = new RoleTriggerBookmark(_cx, res, _pos + 1, outer, middle, inner, tg);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                for (middle = middle.Next(); middle != null; middle = middle.Next())
                    for (inner = middle.value().First(); inner != null; inner = inner.Next())
                        if (_cx.db.objects[inner.key()] is Trigger tg)
                        {
                            var rb = new RoleTriggerBookmark(_cx, res, _pos + 1, outer, middle, inner, tg);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    if (outer.value() is Table t)
                        for (middle = t.triggers.First(); middle != null; middle = middle.Next())
                            for (inner = middle.value().First(); inner != null; inner = inner.Next())
                                if (_cx.db.objects[inner.key()] is Trigger tg)
                                {
                                    var rb = new RoleTriggerBookmark(_cx, res, _pos + 1, outer, middle, inner, tg);
                                    if (rb.Match(res) && RowSet.Eval(res.where, _cx))
                                        return rb;
                                }
                return null;
            }
            protected override Cursor _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$TriggerUpdateColumn table
        /// </summary>
        static void RoleTriggerUpdateColumnResults()
        {
            var t = new SystemTable("Role$TriggerUpdateColumn");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "ColumnName", Domain.Char,1);
            t.AddIndex("Pos");
            t.AddIndex("Name", "ColumnName");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$TriggerUpdateColumn
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleTriggerUpdateColumnBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for implementation
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<PTrigger.TrigType, CTree<long, bool>> _middle;
            readonly ABookmark<long,bool> _inner;
            readonly ABookmark<int, long?> _fourth;
            RoleTriggerUpdateColumnBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<PTrigger.TrigType, CTree<long, bool>> middle, ABookmark<long, bool> inner,
                ABookmark<int,long?> fourth,Trigger tg)
                : base(_cx,res, pos,inner.key(),0,
                      _Value(_cx,res,outer.value(),tg,
                          fourth.value()??-1L))
            {
                _outer = outer;
                _middle = middle;
                _inner = inner;
                _fourth = fourth;
            }
            internal static RoleTriggerUpdateColumnBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table t)
                        for (var middle = t.triggers.First(); middle != null; middle = middle.Next())
                            for (var inner = middle.value().First(); inner != null; inner = inner.Next())
                                if (_cx.db.objects[inner.key()] is Trigger tg)
                                    for (var fourth = tg.cols.First();
                                        fourth != null; fourth = fourth.Next())
                                    {
                                        var rb = new RoleTriggerUpdateColumnBookmark(_cx, res, 0, outer,
                                            middle, inner, fourth, tg);
                                        if (rb.Match(res) && Eval(res.where, _cx))
                                            return rb;
                                    }
                return null;
            }
            /// <summary>
            /// the current value:
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ob,Trigger tg,long cp)
            {
                Table tb = (Table)ob;
                if (_cx.role is not Role ro || _cx._Dom(rs) is not Domain dr ||
                    tb.infos[ro.defpos] is not ObInfo ti ||
                    _cx.db.objects[cp] is not TableColumn tc ||
                    tc.infos[ro.defpos] is not ObInfo ci)
                    throw new DBException("42015"); 
                return new TRow(dr,
                    Pos(tg.defpos),
                    new TChar(ti.name??""),
                    new TChar(ci.name??""));
            }
            /// <summary>
            /// Move to next TriggerColumnUpdate
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                var fourth = _fourth;
                for (fourth = fourth.Next(); fourth != null; fourth = fourth.Next())
                    if (_cx.db.objects[inner.key()] is Trigger tg)
                    {
                        var rb = new RoleTriggerUpdateColumnBookmark(_cx, res, _pos + 1, outer, middle, inner, fourth, tg);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                    if (_cx.db.objects[inner.key()] is Trigger tg)
                        for (fourth = tg.cols.First(); fourth != null; fourth = fourth.Next())
                        {
                            var rb = new RoleTriggerUpdateColumnBookmark(_cx, res, _pos + 1, outer,
                                middle, inner, fourth, tg);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                for (middle = middle.Next(); middle != null; middle = middle.Next())
                    for (inner = middle.value().First(); inner != null; inner = inner.Next())
                        if (_cx.db.objects[inner.key()] is Trigger tg)
                            for (fourth = tg.cols.First(); fourth != null; fourth = fourth.Next())
                            {
                                var rb = new RoleTriggerUpdateColumnBookmark(_cx, res, _pos + 1, outer, middle, inner, fourth, tg);
                                if (rb.Match(res) && Eval(res.where, _cx))
                                    return rb;
                            }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    if (outer.value() is Table t)
                        for (middle = t.triggers.First(); middle != null; middle = middle.Next())
                            for (inner = middle.value().First(); inner != null; inner = inner.Next())
                                if (_cx.db.objects[inner.key()] is Trigger tg)
                                    for (fourth = tg.cols.First();fourth != null; fourth = fourth.Next())
                                    {
                                        var rb = new RoleTriggerUpdateColumnBookmark(_cx, res, _pos + 1, outer, middle, inner, fourth, tg);
                                        if (rb.Match(res) && Eval(res.where, _cx))
                                            return rb;
                                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$Type table
        /// </summary>
        static void RoleTypeResults()
        {
            var t = new SystemTable("Role$Type");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "SuperType", Domain.Char,0);
            t+=new SystemTableColumn(t, "OrderFunc", Domain.Char,0);
            t+=new SystemTableColumn(t, "OrderCategory", Domain.Char,0);
            t += new SystemTableColumn(t, "Suptypes", Domain.Char, 0);
            t+=new SystemTableColumn(t, "Definer", Domain.Char,0);
            t += new SystemTableColumn(t, "Graph", Domain.Char, 0);
            t.AddIndex("Pos");
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Type
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleTypeBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the domains tree
            /// </summary>
            readonly ABookmark<long,object> _en;
            /// <summary>
            /// create the Sys$Type enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleTypeBookmark(Context _cx, SystemRowSet res,int pos,ABookmark<long,object> en)
                : base(_cx,res,pos,en.key(),((Domain)en.value()).lastChange,
                      _Value(_cx,res,en.value()))
            {
                _en = en;
            }
            internal static RoleTypeBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var en = _cx.db.objects.PositionAt(0); en != null; en = en.Next())
                    if (en.value() is Domain)
                    {
                        var rb =new RoleTypeBookmark(_cx,res,0, en);
                        if (rb.Match(res) && RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Name)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ob)
            {
                var t = (UDType)ob;
                if (_cx.role is null || _cx._Dom(rs) is not Domain dr)
                    throw new DBException("42105");
                TypedValue gr = (t is EdgeType) ? new TChar("EDGETYPE")
                            : (t is NodeType) ? new TChar("NODETTYPE") : TNull.Value;
                return new TRow(dr,
                    Pos(t.defpos),
                    new TChar(t.name),
                    new TChar(t.super?.name??""),
                    (t.orderFunc is null)?TNull.Value:new TChar(t.orderFunc.NameFor(_cx)),
                    new TChar((t.orderflags == OrderCategory.None) ? "":
                        t.orderflags.ToString()),
                    new TChar(t.subtypes.Count.ToString()),
                    Pos(t.definer),
                    gr);
            }
            /// <summary>
            /// Move to next Type
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var en = _en;
                for (en = en.Next(); en != null; en = en.Next())
                    if (en.value() is Domain)
                    {
                        var rb = new RoleTypeBookmark(_cx,res, _pos + 1, en);
                        if (rb.Match(res) && RowSet.Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$Method table
        /// </summary>
        static void RoleMethodResults()
        {
            var t = new SystemTable("Role$Method");
            t+=new SystemTableColumn(t, "Type", Domain.Char,1);
            t+=new SystemTableColumn(t, "Method", Domain.Char,1);
            t+=new SystemTableColumn(t, "Arity", Domain.Int,1);
            t+=new SystemTableColumn(t, "MethodType", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definition", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definer", Domain.Char,0);
            t.AddIndex("Name", "Method", "Arity");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Method
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleMethodBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for implementation
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<string, BTree<CList<Domain>,long?>> _middle;
            readonly ABookmark<CList<Domain>,long?> _inner;
            /// <summary>
            /// create the Role$Method enumerator
            /// </summary>
            /// <param name="r"></param>
            RoleMethodBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> outer,
                ABookmark<string, BTree<CList<Domain>,long?>> middle, ABookmark<CList<Domain>, long?> inner)
                : base(_cx,res,pos,inner.value()??-1L,0,_Value(_cx,res,outer.value(),inner.value()??-1L))
            {
                _outer = outer;
                _middle = middle;
                _inner = inner;
            }
            internal static RoleMethodBookmark? New(Context _cx, SystemRowSet res)
            {
                var ro = _cx.role ?? throw new DBException("42105");
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is UDType ut && ut.infos[ro.defpos] is ObInfo oi)
                        for (var middle = oi.methodInfos.First(); middle != null;
                            middle = middle.Next())
                            for (var inner = middle.value().First(); inner != null; inner = inner.Next())
                            {
                                var rb = new RoleMethodBookmark(_cx, res, 0, outer, middle, inner);
                                if (rb.Match(res) && Eval(res.where, _cx))
                                    return rb;
                            }
                return null;
            }
            /// <summary>
            /// the current value: (Type,Method,Arity,MethodType,Proc)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ob,long mp)
            {
                var t = (Domain)ob;
                if (_cx.role is not Role ro || _cx.db.objects[mp] is not Method p
                    || p.infos[ro.defpos] is not ObInfo mi || _cx._Dom(rs) is not Domain dr
                    || _cx.db.objects[p.definer] is not Role de)
                    throw new DBException("42105");
                return new TRow(dr,
                   new TChar(t.name),
                   new TChar(mi.name??""),
                   new TInt(p.arity),
                   new TChar(p.methodType.ToString()),
                   new TChar(p.clause),
                   new TChar(de.name??""));
            }
            /// <summary>
            /// Move to the next method
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var middle = _middle;
                var inner = _inner;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                {
                    var rb = new RoleMethodBookmark(_cx, res, _pos + 1, outer, middle, inner);
                    if (rb.Match(res) && Eval(res.where, _cx))
                        return rb;
                }
                for (middle = middle.Next(); middle != null; middle = middle.Next())
                    for (inner = middle.value().First(); inner != null; inner = inner.Next())
                    {
                        var rb = new RoleMethodBookmark(_cx, res, _pos + 1, outer, middle, inner);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    if (_cx.role is Role ro && outer.value() is Domain ut && ut.kind == Sqlx.TYPE &&
                        ut.infos[ro.defpos] is ObInfo oi)
                        for (middle = oi.methodInfos.First(); middle != null; middle = middle.Next())
                            for (inner = middle.value().First(); inner != null; inner = inner.Next())
                            {
                                var rb = new RoleMethodBookmark(_cx, res, _pos + 1, outer, middle, inner);
                                if (rb.Match(res) && Eval(res.where, _cx))
                                    return rb;
                            }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$Privilege table
        /// </summary>
        static void RolePrivilegeResults()
        {
            var t = new SystemTable("Role$Privilege");
            t+=new SystemTableColumn(t, "ObjectType", Domain.Char,0);
            t+=new SystemTableColumn(t, "Name", Domain.Char,1);
            t+=new SystemTableColumn(t, "Grantee", Domain.Char,1);
            t+=new SystemTableColumn(t, "Privilege", Domain.Char,0);
            t+=new SystemTableColumn(t, "Definer", Domain.Char,0);
            t.AddIndex("ObjectType", "Name", "Grantee");
            t.Add();
        }
        /// <summary>
        /// enumerate Role$Privilege
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RolePrivilegeBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerators for implementation
            /// </summary>
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<string,long?> _inner;
            /// <summary>
            /// create the Sys$Privilege enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RolePrivilegeBookmark(Context _cx, SystemRowSet res,int pos,
                ABookmark<long,object>outer, ABookmark<string,long?>inner)
                : base(_cx,res,pos,outer.key(),0,_Value(_cx,res,outer.value(),inner))
            {
                _outer = outer;
                _inner = inner;
            }
            internal static RolePrivilegeBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    for (var inner = _cx.db.roles.First(); inner != null; inner = inner.Next())
                        if (outer.value() is DBObject ob && inner.value() is long p &&
                            _cx.db.objects[p] is Role ro && ob.infos.Contains(ro.defpos))
                        {
                            var rb = new RolePrivilegeBookmark(_cx, res, 0, outer, inner);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                return null;
            }
            /// <summary>
            /// the current value: (ObjectType,Name,GranteeType,Grantee,Privilege)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ob,ABookmark<string,long?> e)
            {
                var t = (DBObject)ob;
                if (_cx.role is not Role sr || _cx._Dom(rs) is not Domain dr || 
                    e.value() is not long p ||
                    _cx.db.objects[p] is not Role ri || t.infos[sr.defpos] is not ObInfo oi
                    || oi.name==null || _cx.db.objects[t.definer] is not Role de)
                    throw new PEException("PE49300");
                return new TRow(dr,
                    new TChar(t.GetType().Name),
                    new TChar(oi.name??""),
                    new TChar(ri.name??""),
                    new TChar(oi.priv.ToString()),
                    new TChar(de.name??""));
            }
            /// <summary>
            /// Move to the next Role$Privilege obs
            /// </summary>
            /// <returns>whethere there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                for (inner = inner.Next(); inner != null; inner = inner.Next())
                    if (inner.value() is long p &&
                        _cx.db.objects[p] is Role ri && ri.infos.Contains(outer.key()))
                    {
                        var rb = new RolePrivilegeBookmark(_cx, res, _pos + 1, outer, inner);
                        if (rb.Match(res) && Eval(res.where, _cx)
                            && inner.value() != _cx.role.defpos
                            && rb[1].ToString().Length != 0
                            && rb[2].CompareTo(rb[4]) != 0)
                            return rb;
                    }
                for (outer = outer.Next(); outer != null; outer = outer.Next())
                    for (inner = _cx.db.roles.First(); inner != null; inner = inner.Next())
                        if (inner.value() is long p && _cx.db.objects[p] is Role ri 
                            && ri.infos.Contains(outer.key()))
                        {
                            var rb = new RolePrivilegeBookmark(_cx, res, _pos + 1, outer, inner);
                            if (rb.Match(res) && Eval(res.where, _cx)
                               && inner.value() != _cx.role.defpos
                               && rb[1].ToString().Length != 0
                               && rb[2].CompareTo(rb[4]) != 0)
                                return rb;
                        }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
         /// <summary>
        /// set up the Role$Object table
        /// </summary>
        static void RoleObjectResults()
        {
            var t = new SystemTable("Role$Object");
            t+=new SystemTableColumn(t, "Type", Domain.Char,1); 
            t+=new SystemTableColumn(t, "Name", Domain.Char,0);
            t+=new SystemTableColumn(t, "Description", Domain.Char, 0);
            t+=new SystemTableColumn(t, "Iri", Domain.Char, 0);
            t+=new SystemTableColumn(t, "Metadata", Domain.Char,0);
            t.AddIndex("Type", "Name");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Object
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RoleObjectBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the RoleObject tree
            /// </summary>
            readonly ABookmark<long,object> _en;
            /// <summary>
            /// create the Role$Obejct enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RoleObjectBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> en)
                : base(_cx,res,pos,en.key(),((DBObject)en.value()).lastChange,
                      _Value(_cx,res,en.value()))
            {
                _en = en; 
            }
            internal static RoleObjectBookmark? New(Context _cx, SystemRowSet res)
            {
                if (_cx.role is not Role ro)
                    throw new PEException("PE49310");
                for (var bm = _cx.db.objects.PositionAt(0); bm != null; bm = bm.Next())
                    if (bm.value() is DBObject ob && ob.infos[ro.defpos] is ObInfo oi)
                    {
                        var ou = ObInfo.Metadata(oi.metadata, oi.description);
                        if (oi.name == "" && ou == "" && oi.description == "")
                            continue;
                        var rb = new RoleObjectBookmark(_cx, res, 0, bm);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Name,Type,Owner,Source)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object oo)
            {
                var ob = (DBObject)oo;
                if (_cx.role is not Role ro || _cx._Dom(rs) is not Domain dr)
                    throw new PEException("PE49310");
                if (ob.infos[ro.defpos] is not ObInfo oi || oi.name == null)
                    throw new DBException("42105");
                var dm = ob as Domain;
                return new TRow(dr,
                    new TChar(ob.GetType().Name),
                    new TChar(oi.name),
                    new TChar(oi.description ?? ""),
                    new TChar(dm?.iri ?? ""),
                    new TChar(ObInfo.Metadata(oi.metadata,oi.description??""))); ;
            }
            /// <summary>
            /// Move to the next object
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                if (_cx.role is not Role ro)
                    throw new DBException("42105");
                var en = _en;
                for (en = en.Next(); en != null; en = en.Next())
                    if (en.value() is DBObject ob && ob.infos[ro.defpos] is ObInfo oi)
                    {
                        var ou = ObInfo.Metadata(oi.metadata, oi.description);
                        if (oi.name == "" && ou == "" && oi.description == "")
                            continue;
                        var rb = new RoleObjectBookmark(_cx, res, _pos + 1, en);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        /// <summary>
        /// set up the Role$PrimaryKey table
        /// </summary>
        static void RolePrimaryKeyResults()
        {
            var t = new SystemTable("Role$PrimaryKey");
            t+=new SystemTableColumn(t, "Table", Domain.Char,1);
            t+=new SystemTableColumn(t, "Ordinal", Domain.Int,1);
            t+=new SystemTableColumn(t, "Column", Domain.Char,0);
            t.AddIndex("Table", "Ordinal");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Primarykey
        /// // shareable as of 26 April 2021
        /// </summary>
        internal class RolePrimaryKeyBookmark : SystemBookmark
        {
            readonly ABookmark<long,object> _outer;
            readonly ABookmark<int, long?> _inner;
            /// <summary>
            /// create the Sys$PrimaryKey enumerator
            /// </summary>
            /// <param name="r">the rowset</param>
            RolePrimaryKeyBookmark(Context _cx, SystemRowSet res,int pos,
                ABookmark<long,object> outer, ABookmark<int,long?>inner)
                : base(_cx,res,pos,inner.value()??-1L,((Table)outer.value()).lastChange,
                      _Value(_cx,res,outer.value(),inner))
            {
                _outer = outer;
                _inner = inner;
            }
            internal static RolePrimaryKeyBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is Table t)
                    for (var inner = t.FindPrimaryIndex(_cx)?.keys.First(); inner != null; inner = inner.Next())
                    {
                            var rb = new RolePrimaryKeyBookmark(_cx,res, 0, outer, inner);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value(table,ordinal,ident)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, object ob, ABookmark<int,long?> e)
            {
                var tb = (Table)ob;
                if (_cx._Dom(rs) is not Domain dr || tb.infos[_cx.role.defpos] is not ObInfo oi
                    || e.value() is not long p
                    || _cx.db.objects[p] is not ObInfo ci)
                    throw new DBException("42105");
                return new TRow(dr,
                    new TChar(oi.name??""),
                    new TInt(e.key()),
                    new TChar(ci.name??""));
            }
            /// <summary>
            /// Move to next primary key obs
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var outer = _outer;
                var inner = _inner;
                for (inner=inner.Next();inner!=null;inner=inner.Next())
                {
                    var rb = new RolePrimaryKeyBookmark(_cx,res, _pos+1, outer, inner);
                    if (rb.Match(res) && Eval(res.where, _cx))
                        return rb;
                }
                for (outer=outer.Next();outer!=null;outer=outer.Next())
                    if (outer.value() is Table t)
                        for (inner = t.FindPrimaryIndex(_cx)?.keys.First(); inner != null; inner = inner.Next())
                        {
                            var rb = new RolePrimaryKeyBookmark(_cx,res, _pos + 1, outer, inner);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                return null;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
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
            internal static ProfileBookmark? New(SystemRowSet res)
            {
                var db = res.database;
                if (db.profile?.transactions == null || db.profile.transactions.Count == 0)
                    return null;
                for (var ix = 0; ix < db.profile.transactions.Count; ix++)
                {
                    var rb = new ProfileBookmark(res, ix);
                    if (rb.Match(res) && RowSet.Eval(res.where, res.tr, res))
                        return rb;
                }
                return null;
            }
            public override TRow CurrentKey()
            {
                if (db.profile == null || _ix < 0 || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                return new TRow(res, new TColumn("Id", new TInt(p.id)));
            }
            public override TRow CurrentValue()
            {
                if (db.profile == null || _ix < 0 || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                return new TRow(res,new TInt(p.id), 
                        new TInt(p.num), new TInt(p.fails), TBool.For(p.schema));
            }
            public override Cursor Next()
            {
                for (; ; )
                {
                    if (db.profile == null || _ix + 1 >= db.profile.transactions.Count)
                        return Null();
                    var rb = new ProfileBookmark(res, _ix + 1);
                    if (rb.Match(res) && RowSet.Eval(res.where, res.tr, res))
                        return rb;
                }
            }
        }
        static void ProfileTableResults()
        {
            var t = new SystemTable("Profile$Table");
            new SystemTableColumn(t, "Id", Sqlx.INTEGER);
            new SystemTableColumn(t, "Table", 0, Domain.Position);
            new SystemTableColumn(t, "BlockAny", Sqlx.BOOLEAN); 
            new SystemTableColumn(t, "Dels", Sqlx.INTEGER);
            new SystemTableColumn(t, "Index", 0, Domain.Position);
            new SystemTableColumn(t, "Pos", 0, Domain.Position);
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
            internal static ProfileTableBookmark? New(SystemRowSet res)
            {
                var db = res.database;
                if (db.profile?.transactions == null)
                    return null;
                for (int i = 0; i < db.profile.transactions.Count; i++)
                    for (var inner = db.profile.transactions[i].tables.First(); inner != null; inner = inner.Next())
                    {
                        var rb = new ProfileTableBookmark(res, 0, i, inner);
                        if (rb.Match(res) && RowSet.Eval(res.where, res.tr, res))
                            return rb;
                    }
                return null;
            }
            public override Cursor Next()
            {
                var ix = _ix;
                var inner = _inner;
                for (;;)
                {
                    if (inner != null && (inner = inner.Next()) != null)
                        return new ProfileTableBookmark(res, _pos + 1, ix, inner);
                    ix = -1;
                    if (db.profile == null || ix + 1 >= db.profile.transactions.Count)
                        return Null();
                    var p = db.profile.transactions[ix + 1];
                    inner = p.tables.First();
                    if (inner != null)
                    {
                        var rb = new ProfileTableBookmark(res, _pos + 1, ix, inner);
                        if (rb.Match(res) && RowSet.Eval(res.where, res.tr, res))
                            return rb;
                    }
                }
            }
            public override TRow CurrentKey()
            {
                if (db.profile == null || _ix<0 || _ix>= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var t = db.objects[_inner.key()];
                return new TRow(res, new TColumn("Id", new TInt(p.id)),
                    new TColumn("Table", new TChar(t.NameInSession(db))));
            }
            public override TRow CurrentValue()
            {
                if (db.profile == null || _ix < 0 || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];

                var t = db.objects[_inner.key()];
                var c = _inner.value();
                return new TRow(res, new TInt(p.id), new TChar(t.NameInSession(db)),
                    TBool.For(c.blocked), new TInt(c.dels),
                     Pos(c.ckix), Pos(c.tab), new TInt(c.specific), TBool.For(c.schema));
            }
        }
        static void ProfileReadConstraintResults()
        {
            var t = new SystemTable("Profile$ReadConstraint");
            new SystemTableColumn(t, "Id", Sqlx.INTEGER);
            new SystemTableColumn(t, "Table", 0, Domain.Char);
            new SystemTableColumn(t, "ColPos", 0, Domain.Position);
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
            internal static ProfileReadConstraintBookmark? New(SystemRowSet res)
            {
                var db = res.database;
                if (db.profile?.transactions == null)
                    return null;
                for (int i = 0; i < db.profile.transactions.Count; i++)
                    for (var middle = db.profile.transactions[i].tables.First(); middle != null;
                        middle = middle.Next())
                        for (var inner = middle.value().read.First(); inner != null; inner = inner.Next())
                        {
                            var rb = new ProfileReadConstraintBookmark(res, 0, i, middle, inner);
                            if (rb.Match(res) && RowSet.Eval(res.where, res.tr, res))
                                return rb;
                        }
                return null;
            }
            public override Cursor Next()
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
                    if (db.profile == null || ix + 1 >= db.profile.transactions.Count)
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
                            if (rb.Match(res) && RowSet.Eval(res.where, res.tr, res))
                                return rb;
                        }
                    }
                }
            }
            public override TRow CurrentKey()
            {
                if (db.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var ta = db.objects[_middle.key()];
                var t = (ta is EdgeType et) ? (Table)et : (ta is NodeType nt) ? (Table)nt : ta as Table;
                if (db.objects[_inner.key()] is TableColumn c)
                    return new TRow(res, new TColumn("Id", new TInt(p.id)),
                        new TColumn("Table", new TChar(t.CurrentName(db))),
                            new TColumn("ColPos", new TChar(c.CurrentName(db))));
                return null;
            }
            public override TRow CurrentValue()
            {
                if (db.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var ta = db.objects[_middle.key()];
                var t = (ta is EdgeType et) ? (Table)et : (ta is NodeType nt) ? (Table)nt : ta as Table;
                if (db.objects[_inner.key()] is TableColumn c)
                    return new TRow(res, new TInt(p.id),
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
            internal static ProfileRecordBookmark? New(SystemRowSet res)
            {
                var db = res.database;
                if (db.profile?.transactions == null)
                    return null;
                for (int i = 0; i < db.profile.transactions.Count; i++)
                    for (var middle = db.profile.transactions[i].tables.First();
                        middle != null; middle = middle.Next())
                        if (middle.value().recs is List<RecordProfile> ls)
                            for (var ix = 0; ix < ls.Count; ix++)
                            {
                                var rb = new ProfileRecordBookmark(res, 0, i, middle, ix);
                                if (rb.Match(res) && RowSet.Eval(res.where, res.tr, res))
                                    return rb;
                            }
                return null;
            }
            public override Cursor Next()
            {
                var ix = _ix;
                var middle = _middle;
                var rx = _rx;
                for (;;)
                {
                    if (middle!=null &&  rx+1 < middle.value().recs.Count)
                    {
                        var rb =new ProfileRecordBookmark(res, _pos + 1, ix, middle, rx+1);
                        if (RowSet.Eval(res.where, res.tr, res))
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
                if (db.profile==null || _ix >= db.profile.transactions.Count)
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
                if (db.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var t = db.objects[_middle.key()];
                var r = _middle.value().recs[_rx];
                return new TRow(res, new TInt(p.id),
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
            internal static ProfileRecordColumnBookmark? New(SystemRowSet res)
            {
                var db = res.database;
                if (db.profile?.transactions == null)
                    return null;
                for (int i = 0; i < db.profile.transactions.Count; i++)
                    for (var second = db.profile.transactions[i].tables.First();
                        second != null; second = second.Next())
                    {
                        var rcs = second.value()?.recs;
                        if (rcs != null)
                            for (int j = 0; j < rcs.Count; j++)
                                for (var fourth = rcs[j].fields.PositionAt(0); fourth != null; fourth = fourth.Next())
                                {
                                    var rb = new ProfileRecordColumnBookmark(res, 0, i, second, j, fourth);
                                    if (rb.Match(res) && RowSet.Eval(res.where, res.tr, res))
                                        return rb;
                                }
                    }
                return null;
            }
            public override Cursor Next()
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
                        fourth = r.fields.PositionAt(0);
                        if (fourth != null)
                        {
                            var rb = new ProfileRecordColumnBookmark(res, _pos + 1, ix, second, rx, fourth);
                            if (rb.Match(res) && RowSet.Eval(res.where, res.tr, res))
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
                if (db.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var ta = db.objects[_second.key()];
                var t = (ta is EdgeType et) ? (Table)et : (ta is NodeType nt) ? (Table)nt : ta as Table;
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
                if (db.profile == null || _ix >= db.profile.transactions.Count)
                    return null;
                var p = db.profile.transactions[_ix];
                var ta = db.objects[_second.key()];
                var t = (ta is EdgeType et) ? (Table)et : (ta is NodeType nt) ? (Table)nt : ta as Table;
                var r = _second.value().recs[_rx];
                if (db.objects[_fourth.key()] is TableColumn c)
                    return new TRow(res, new TInt(p.id),
                        new TChar(t.CurrentName(db)), new TInt(r.id), Pos(c.defpos), new TChar(c.CurrentName(db)));
                return null;
            }
        }
#endif
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (sysIx != null) { sb.Append(" SysIx:"); sb.Append(Uid(sysIx.defpos)); }
            if (sysFilt !=null) { sb.Append(" SysFilt:"); sb.Append(sysFilt); }
            return sb.ToString();
        }
    }

}


