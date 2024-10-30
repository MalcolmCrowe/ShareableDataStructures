using System.Net.Http.Headers;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
    internal class SystemTable : Table
    {
        internal const long
            SysCols = -175; // BTree<long,TableColumn>
        public BTree<long, TableColumn> sysCols =>
            (BTree<long, TableColumn>?)mem[SysCols] ?? BTree<long, TableColumn>.Empty;
        public long role => (long)(mem[Database.Role] ?? -1L);
        public BList<long?> sRowType => (BList<long?>?)mem[InstanceRowSet.SRowType] ?? BList<long?>.Empty;
        internal SystemTable(string n) : base(--_uid, _Mem(n))
        {
            var sys = Database._system ?? throw new PEException("PE1013");
            Database._system = sys + this + TableType.Relocate(_uid);
        }
        protected SystemTable(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(string n)
        {
            var p = Database.schemaRole?.defpos?? throw new PEException("PE1104");
            return new BTree<long, object>(ObInfo.Name, n)
                  + (Database.Role, p)
                  + (Infos, new BTree<long, ObInfo>(p, new ObInfo(n, Grant.AllPrivileges)));
        }
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
            var oi = s.infos[s.role]??throw new PEException("PE010100");
            oi += (ObInfo._Names, oi.names+(c.name,c.defpos));
            return s + (SysCols, s.sysCols + (c.defpos, c)) + (Infos, s.infos+(s.role,oi))
                + (InstanceRowSet.SRowType, s.sRowType + c.defpos)
                + (RowType, s.rowType + c.defpos)
                + (Representation, s.representation+(c.defpos,c.domain));
             //   + (TableCols, s.tableCols + (c.defpos, c.domain));
        }
        internal override (BList<long?>, CTree<long, Domain>, BList<long?>, BTree<long, long?>, Names, BTree<long,Names>)
         ColsFrom(Context cx, long dp, BList<long?> rt, CTree<long, Domain> rs, BList<long?> sr, BTree<long, long?> tr, 
            Names ns, BTree<long,Names> ds, long ap)
        {
            return (rowType, representation, rowType, tr, ns, ds);
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
            d = d + this + ro;
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
                        rs += (sc.defpos, sc.domain);
                        break;
                    }
            var ks = new Domain(--_uid, BTree<long, object>.Empty + (Kind, Qlx.ROW) + (RowType, rt)
                + (Representation, rs));
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
                throw new DBException("42105").Add(Qlx.SYSTEM);
            return name;
        }
        internal override RowSet RowSets(Ident id, Context cx, Domain q, long fm, long ap,
            Grant.Privilege pr = Grant.Privilege.Select, string? a=null,TableRowSet? ur = null)
        {
            var r = new SystemRowSet(cx, this, ap, null)+(_From,fm)+(_Ident,id);
            if (a != null)
                r += (_Alias, a);
            return r;
        }
        internal RowSet RowSets(long dp,Context cx, RowSet r, CTree<long,bool>?w)
        {
            return new SystemRowSet(dp,cx, this, w, r.mem);
        }
    }
    
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
                  + (_Domain, dt) + (Key,k)
                  + (Infos, new BTree<long, ObInfo>(Database.schemaRole?.defpos??throw new PEException("PE1105"),
                      new ObInfo(n, Grant.AllPrivileges))))
        {
            t += (Domain.RowType, t.rowType + defpos);
            t += (Domain.Representation, t.representation + (defpos, dt));
            t += (SystemTable.SysCols, t.sysCols + (defpos, this));
            Database._system = Database._system + this + t;
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
            return name??"";
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
    
    internal class SystemIndex(long tb, Domain ks) : Level3.Index(--_uid,BTree<long, object>.Empty+(IndexConstraint,PIndex.ConstraintType.Unique)
                  +(Keys,ks)+(TableDefPos,tb))
    {
    }

    internal class SystemFilter(long c, Qlx o, TypedValue v, Qlx o2 = Qlx.NO, TypedValue? v2 = null)
    {
        public readonly long col = c;
        public readonly TypedValue val1 = v, val2 = v2 ?? TNull.Value;
        public readonly Qlx op1 = o, op2 = o2; // EQL, GTR etc

        internal TypedValue? Start(Context cx,SystemRowSet rs,string s,int i,bool desc)
        {
            var sf = rs.sysFilt;
            if (sf != null && sf[i] is SystemFilter fi && rs.sysIx is not null 
                && cx.obs[rs.sysIx.keys[i]??-1L] is SystemTableColumn stc && stc.name == s)
            {
                switch (op1)
                {
                    case Qlx.EQL:
                        return fi.val1;
                    case Qlx.GTR:
                    case Qlx.GEQ:
                        if (!desc)return fi.val1;
                        break;
                    case Qlx.LSS:
                    case Qlx.LEQ:
                        if (desc) return fi.val1;
                        break;
                }
                switch (op2)
                {
                    case Qlx.EQL:
                        return fi.val2;
                    case Qlx.GTR:
                    case Qlx.GEQ:
                        return desc ? null : fi.val2;
                    case Qlx.LSS:
                    case Qlx.LEQ:
                        return desc ? fi.val2 : null;
                }
            }
            return null;
        }
        internal static BTree<long,SystemFilter> Add(BTree<long,SystemFilter> sf,
            long c,Qlx o, TypedValue v)
        {
            if (sf[c] is SystemFilter f)
            {
                if ((f.op1 == Qlx.LSS || f.op1 == Qlx.LEQ)
                    && (o==Qlx.GTR || o==Qlx.GEQ))
                    return sf + (c, new SystemFilter(c, f.op1, f.val1, o, v));
                else if ((f.op1 == Qlx.GTR || f.op1 == Qlx.GEQ)
                    && (o == Qlx.LSS || o == Qlx.LEQ))
                    return sf + (c, new SystemFilter(c, o, v, f.op1, f.val1));
                else
                    throw new DBException("42000","Filter");
            }
            return sf + (c, new SystemFilter(c, o, v));
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(DBObject.Uid(col));
            sb.Append(QlValue.Show(op1));
            sb.Append(val1);
            return sb.ToString();
        }
    }
    /// <summary>
    /// Perform selects from virtual 'system tables'
    /// 
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
        internal new BList<long?> rowType => sRowType;
        /// <summary>
        /// Construct results for a system table.
        /// Independent of database, role, and user.
        /// Context is provided to be informed about the rowset.
        /// </summary>
        /// <param name="f">the from part</param>
        internal SystemRowSet(Context cx, SystemTable f, long ap, CTree<long, bool>? w = null)
            : base(cx.GetUid(), cx, f.defpos, ap, _Mem(cx, f, w, null))
        { }
        internal SystemRowSet(long dp,Context cx, SystemTable f, CTree<long, bool>? w, BTree<long, object>? m)
            : base(dp,_Mem(cx, f, w, m))
        {
            cx.Add(this);
        }
        protected SystemRowSet(long dp, BTree<long, object> m) : base(dp, m) { }
        static BTree<long,object> _Mem(Context cx, SystemTable f, CTree<long, bool>? w, BTree<long, object>? m)
        {
            var mf = BTree<long,SystemFilter>.Empty;
            for (var b = w?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is QlValue s)
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
            // NB: rowType stuff is done by TableRowSet
            if (f.name?.StartsWith("Log$")==true &&cx.user?.defpos!=cx.db.owner)
                throw new DBException("42105").Add(Qlx.OWNER);
            var r = (m ?? BTree<long, object>.Empty) + (SRowType, f.rowType) + (SysTable, f) + (SysFilt, sf);
            if (w is not null)
                r  += (_Where,w);
            if (f.infos[cx.role.defpos]?.names is Names ns)
                r += (ObInfo._Names, ns);
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
        internal new static void Kludge()
        { }
        /// <summary>
        /// Class initialisation: define the system tables
        /// </summary>
        static SystemRowSet()
        {
            // level 2 stuff
            LogResults();
            LogDeleteResults();
            LogDropResults();
            LogModifyResults();
            LogCheckResults();
            LogClassificationResults();
            LogClearanceResults();
            LogColumnResults();
            LogDateTypeResults();
            LogDomainResults();
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
            LogTypeResults();
            LogTypeMethodResults();
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
            RoleEdgeTypeResults();
            RoleGraphCatalogResults();
            RoleGraphEdgeTypeResults();
            RoleGraphInfoResults();
            RoleGraphLabelResults();
            RoleGraphNodeTypeResults();
            RoleGraphPropertyResults();
            RoleIndexResults();
            RoleIndexKeyResults();
            RoleJavaResults();
            RoleMethodResults();
            RoleNodeTypeResults();
            RoleObjectResults();
            RoleParameterResults();
            RolePrimaryKeyResults();
            RolePrivilegeResults();
            RoleProcedureResults();
            RolePythonResults();
            RoleSQLResults();
            RoleSubobjectResults();
            RoleTableResults();
            RoleTableCheckResults();
            RoleTablePeriodResults();
            RoleTriggerResults();
            RoleTriggerUpdateColumnResults();
            RoleTypeResults();
            SysRoleResults();
            SysUserResults();
            SysRoleUserResults();
            RoleViewResults();
            // level >=4 stuff
            SysClassificationResults();
            SysClassifiedColumnDataResults();
            SysEnforcementResults();
           // SysGraphResults();
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
                case "Log$Check": return LogCheckBookmark.New(_cx, res);
                case "Log$Classification": return LogClassificationBookmark.New(_cx, res);
                case "Log$Clearance": return LogClearanceBookmark.New(_cx, res);
                case "Log$Column": return LogColumnBookmark.New(_cx, res);
                case "Log$DateType": return LogDateTypeBookmark.New(_cx, res);
                case "Log$Delete": return LogDeleteBookmark.New(_cx, res);
                case "Log$Domain": return LogDomainBookmark.New(_cx, res);
                case "Log$Drop": return LogDropBookmark.New(_cx, res);
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
                case "Log$Update": return LogUpdateBookmark.New(_cx, res);
                case "Log$User": return LogUserBookmark.New(_cx, res);
                case "Log$View": return LogViewBookmark.New(_cx, res);
                // level 3 stuff
                case "Role$Class": return RoleClassBookmark.New(_cx, res);
                case "Role$Column": return RoleColumnBookmark.New(_cx, res);
                case "Role$ColumnCheck": return RoleColumnCheckBookmark.New(_cx, res);
                case "Role$ColumnPrivilege": return RoleColumnPrivilegeBookmark.New(_cx, res);
                case "Role$EdgeType": return RoleEdgeTypeBookmark.New(_cx, res);
                case "Role$Domain": return RoleDomainBookmark.New(_cx, res);
                case "Role$DomainCheck": return RoleDomainCheckBookmark.New(_cx, res);
                case "Role$GraphCatalog": return RoleGraphCatalogBookmark.New(_cx, res);
                case "Role$GraphEdgeType": return RoleGraphEdgeTypeBookmark.New(_cx, res);
                case "Role$GraphInfo": return RoleGraphInfoBookmark.New(_cx, res);
                case "Role$GraphLabel": return RoleGraphLabelBookmark.New(_cx, res);
                case "Role$GraphNodeType": return RoleGraphNodeTypeBookmark.New(_cx, res);
                case "Role$GraphProperty": return RoleGraphPropertyBookmark.New(_cx, res);
                case "Role$Index": return RoleIndexBookmark.New(_cx, res);
                case "Role$IndexKey": return RoleIndexKeyBookmark.New(_cx, res);
                case "Role$Java": return RoleJavaBookmark.New(_cx, res);
                case "Role$Method": return RoleMethodBookmark.New(_cx, res);
                case "Role$NodeType": return RoleNodeTypeBookmark.New(_cx, res);
                case "Role$Object": return RoleObjectBookmark.New(_cx, res);
                case "Role$Parameter": return RoleParameterBookmark.New(_cx, res);
                case "Role$Privilege": return RolePrivilegeBookmark.New(_cx, res);
                case "Role$Procedure": return RoleProcedureBookmark.New(_cx, res);
                case "Role$Python": return RolePythonBookmark.New(_cx, res);
                case "Role$SQL": return RoleSQLBookmark.New(_cx, res);
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
                //case "Sys$Graph": return SysGraphBookmark.New(cx,res);
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
                "Log$Check" => LogCheckBookmark.New(res, _cx),
                "Log$Classification" => LogClassificationBookmark.New(res, _cx),
                "Log$Clearance" => LogClearanceBookmark.New(res, _cx),
                "Log$Column" => LogColumnBookmark.New(res, _cx),
                "Log$DateType" => LogDateTypeBookmark.New(res, _cx),
                "Log$Delete" => LogDeleteBookmark.New(res, _cx),
                "Log$Domain" => LogDomainBookmark.New(res, _cx),
                "Log$Drop" => LogDropBookmark.New(res, _cx),
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
        internal new TypedValue Start(Context cx, string s, int i = 0, bool desc = false)
        {
            return (sysFilt is BList<SystemFilter> sf && sf.Length>i)?
                (sf[i]?.Start(cx, this, s, i, desc)??TNull.Value):TNull.Value;
        }
        internal override RowSet Apply(BTree<long, object> mm, Context cx, BTree<long, object>? m = null)
        {
            if (sysFrom is SystemTable f)
                return f.RowSets(defpos, cx, this, mm[_Where] as CTree<long, bool>);
            return this;
        }
        /// <summary>
        /// A bookmark for a system table
        /// 
        /// </summary>
        /// <remarks>
        /// base constructor for the system enumerator
        /// </remarks>
        /// <param name="r">the rowset</param>
        internal abstract class SystemBookmark(Context cx, SystemRowSet r, int pos, long dpos, long pp,
            TRow rw) : Cursor(cx, r, pos, new BTree<long,(long,long)>(r.defpos,(dpos,pp)), rw)
        {
            /// <summary>
            /// the system rowset
            /// </summary>
            public readonly SystemRowSet res = r;

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
                    if (sf.op1 != Qlx.NO && !Test(v, sf.op2, sf.val2))
                        return false;
                }
                return true;
            }
            static bool Test(TypedValue v, Qlx op, TypedValue w)
            {
                switch (op)
                {
                    case Qlx.EQL:
                        if (v.CompareTo(w) != 0)
                            return false;
                        break;
                    case Qlx.GTR:
                        if (v.CompareTo(w) < 0)
                            return false;
                        break;
                    case Qlx.LSS:
                        if (v.CompareTo(w) >= 0)
                            return false;
                        break;
                    case Qlx.LEQ:
                        if (v.CompareTo(w) > 0)
                            return false;
                        break;
                    case Qlx.GEQ:
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
        /// <remarks>
        /// Construct the LogSystemBookmark
        /// </remarks>
        /// <param name="r">the rowset</param>
        internal abstract class LogSystemBookmark(Context _cx, SystemRowSet r, int pos,
            Physical p, long pp, TRow rw) : SystemBookmark(_cx,r,pos,p.ppos,p.ppos,rw)
        {
            internal readonly Physical ph = p;
            internal readonly long nextpos = pp;
        }
        /// <summary>
        /// Set up the Log$ table
        /// </summary>
        static void LogResults()
        {
            var t = new SystemTable("Log$");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Desc", Char,0);
            t+=new SystemTableColumn(t, "Type", Char,0);
            t+=new SystemTableColumn(t, "Affects", Position,0);
            t += new SystemTableColumn(t, "Transaction", Domain.Position, 0);
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
                : base(cx,r, pos, ph, pp ,_Value(r,ph))
            { }
            internal static LogBookmark? New(Context _cx, SystemRowSet res)
            {
                var start = res.Start(_cx, "Pos")?.ToLong() ?? 5;
                for (var lb = _cx.db.log.PositionAt(start); lb != null; lb = lb.Next())
                if (_cx.db is not null){
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
            static TRow _Value(SystemRowSet res,Physical ph)
            {
                return new TRow(res,
                    Pos(ph.ppos),
                    new TChar(ph.ToString()),
                    new TChar(ph.type.ToString()),
                    Pos(ph.Affects),
                    Pos(ph.trans));
            }
            /// <summary>
            /// Move to the next log entry
            /// </summary>
            /// <returns>whether there is a next entry</returns>
            protected override Cursor? _Next(Context _cx)
            {
                if (_cx.db is not null)
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
        /// setup the Log$Delete table
        /// </summary>
        static void LogDeleteResults()
        {
            var t = new SystemTable("Log$Delete");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "DelPos", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Delete
        /// </summary>
        internal class LogDeleteBookmark : LogSystemBookmark
        {
            LogDeleteBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Delete d = (Delete)ph;
                return new TRow(res,
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "DelPos", Position,0);
            t+=new SystemTableColumn(t, "Transaction", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Drop
        /// </summary>
        internal class LogDropBookmark : LogSystemBookmark
        {
            LogDropBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Drop d = (Drop)ph;
                return new TRow(res,
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
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PMetadata m = (PMetadata)ph;
                var md = m.Metadata();
                return new TRow(res,
                    Pos(m.ppos),
                    Pos(m.defpos),
                    new TChar(m.name ?? throw new PEException("PE0311")),
                    new TChar(md.Contains(Qlx.PASSWORD)?"*******":m.detail.ToString()),
                    new TChar(m.MetaFlags()),
                    Pos(m.refpos),
                    // At present m.iri may contain a password (this should get fixed)
                    new TChar((m.iri!="" && md.Contains(Qlx.PASSWORD))?"*******":m.iri));
            }
         }
        /// <summary>
        /// Set up the Log$Modify table
        /// </summary>
        static void LogModifyResults()
        {
            var t = new SystemTable("Log$Modify");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "DefPos", Position,0);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Proc", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// An enumerator for Log$Modify
        /// </summary>
        internal class LogModifyBookmark : LogSystemBookmark
        {
            LogModifyBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
                        if (Eval(res.where, _cx))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Modify m = (Modify)ph;
                return new TRow(res,
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
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PUser a = (PUser)ph;
                return new TRow(res,
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Details", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Role
        /// </summary>
        internal class LogRoleBookmark : LogSystemBookmark
        {
            LogRoleBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PRole a = (PRole)ph;
                return new TRow(res,
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Ref", Position,0);
            t+=new SystemTableColumn(t, "ColRef", Position,0);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Check", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Check
        /// </summary>
        internal class LogCheckBookmark : LogSystemBookmark
        {
            LogCheckBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
                        if (Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Ref,Name,Check,Transaction)
            /// </summary>
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PCheck c = (PCheck)ph;
                return new TRow(res,
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Obj", Position,0);
            t+=new SystemTableColumn(t, "Classification", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class LogClassificationBookmark : LogSystemBookmark
        {
            LogClassificationBookmark(Context _cx,SystemRowSet rs, int pos, Physical ph, long pp) 
                : base(_cx,rs, pos, ph, pp, _Value(rs, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Classify c = (Classify)ph;
                return new TRow(res,
                    Pos(c.ppos),
                    Pos(c.obj),
                    new TChar(c.classification.ToString()));
            }
        }
        static void LogClearanceResults()
        {
            var t = new SystemTable("Log$Clearance");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "User", Position,0);
            t+=new SystemTableColumn(t, "Clearance", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class LogClearanceBookmark : LogSystemBookmark
        {
            LogClearanceBookmark(Context _cx,SystemRowSet rs, int pos, Physical ph, long pp)
                : base(_cx,rs, pos, ph, pp, _Value(rs, ph,
                    (_cx.user??throw new DBException("42105").Add(Qlx.SECURITY)).defpos))
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
            static TRow _Value(SystemRowSet res, Physical ph,long u)
            {
                Clearance c = (Clearance)ph;
                return new TRow(res,
                    Pos(c.ppos),
                    Pos(u),
                    new TChar(c.clearance.ToString()));
            }
        }
        /// <summary>
        /// set up the Log$Column table
        /// </summary>
        static void LogColumnResults()
        {
            var t = new SystemTable("Log$Column");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t += new SystemTableColumn(t, "Defpos", Position, 0);
            t+=new SystemTableColumn(t, "Table", Position,0);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Seq", Int,0);
            t+=new SystemTableColumn(t, "Domain", Position,0);
            t+=new SystemTableColumn(t, "Default", Char,0);
            t+=new SystemTableColumn(t, "NotNull", Bool,0);
            t+=new SystemTableColumn(t, "Generated", Char,0);
            t+=new SystemTableColumn(t, "Update", Position,0);
            t += new SystemTableColumn(t, "Flags", Char, 0);
            t += new SystemTableColumn(t, "RefIndex", Position, 0);
            t += new SystemTableColumn(t, "ToType", Position, 0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Column
        /// </summary>
        internal class LogColumnBookmark: LogSystemBookmark
        {
            LogColumnBookmark(Context _cx,SystemRowSet rs,int pos, Physical ph, long pp)
                : base(_cx,rs,pos,ph, pp, _Value(rs, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                if (ph is not PColumn c || 
                    c.table is not Table tb)
                    throw new PEException("PE0410");
                return new TRow(res,
                    Pos(c.ppos),
                    Pos(c.defpos),
                    Pos(tb.defpos),
                    new TChar(c.name),
                    new TInt(c.seq),
                    Pos(c.domdefpos),
                    Display(c.dfs.ToString()),
                    TBool.For(c.notNull),
                    new TChar(c.generated.ToString()),
                    Display(c.upd.ToString()),
                    new TChar(c.flags.ToString()),
                    Pos(c.index),
                    Pos(c.toType));
            }
         }
        /// <summary>
        /// set up the Log$TablePeriod table
        /// </summary>
        static void LogTablePeriodResults()
        {
            var t = new SystemTable("Log$TablePeriod");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Table", Position,0);
            t+=new SystemTableColumn(t, "PeriodName", Char,0);
            t+=new SystemTableColumn(t, "Versioning", Bool,0);
            t+=new SystemTableColumn(t, "StartColumn", Position,0);
            t+=new SystemTableColumn(t, "EndColumn", Position,0);
            t.AddIndex("Pos");
            t.Add();

        }
        /// <summary>
        /// an enumerator for Log$Column
        /// </summary>
        internal class LogTablePeriodBookmark : LogSystemBookmark
        {
            LogTablePeriodBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PPeriodDef c = (PPeriodDef)ph;
                return new TRow(res,
                    Pos(c.ppos),
                    Pos(c.tabledefpos),
                    new TChar(c.periodname),
                    TBool.True,
                    Pos(c.startcol),
                    Pos(c.endcol));
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
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$DateType
        /// </summary>
        internal class LogDateTypeBookmark : LogSystemBookmark
        {
            LogDateTypeBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                    var d = ((PDateType)ph).domain;
                    return new TRow(res,
                        Pos(ph.ppos),
                        new TChar(d.name),
                        new TChar(d.kind.ToString()),
                        new TChar(d.start.ToString()),
                        new TChar(d.end.ToString()));
            }
         }
        /// <summary>
        /// set up the Log$Domain table
        /// </summary>
        static void LogDomainResults()
        {
            var t = new SystemTable("Log$Domain");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Kind", Char,0);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "DataType", Char,0);
            t+=new SystemTableColumn(t, "DataLength", Int,0);
            t+=new SystemTableColumn(t, "Scale", Int,0);
            t+=new SystemTableColumn(t, "Charset", Char,0);
            t+=new SystemTableColumn(t, "Collate", Char,0);
            t+=new SystemTableColumn(t, "Default", Char,0);
            t+=new SystemTableColumn(t, "StructDef", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Domain
        /// </summary>
        internal class LogDomainBookmark : LogSystemBookmark
        {
            LogDomainBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
                        case Physical.Type.PTable1:
                        case Physical.Type.PNodeType:
                        case Physical.Type.PEdgeType:
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Domain d = ((PDomain)ph).domain;
                return new TRow(res,
                    Pos(ph.ppos),
                    new TChar(d.kind.ToString()),
                    new TChar(d.name),
                    new TChar(d.kind.ToString()),
                    new TInt(d.prec),
                    new TInt(d.scale),
                    new TChar(d.charSet.ToString()),
                    new TChar(d.culture.Name),
                    Display(d.defaultString),
                    new TChar(d.elType?.ToString()??""));
            }
         }
        static void LogEnforcementResults()
        {
            var t = new SystemTable("Log$Enforcement");
            t+=new SystemTableColumn(t, "Pos", Domain.Position,1);
            t+=new SystemTableColumn(t, "Table", Domain.Char,0);
            t+=new SystemTableColumn(t, "Flags", Domain.Int,0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class LogEnforcementBookmark : LogSystemBookmark
        {
            LogEnforcementBookmark(Context _cx,SystemRowSet rs, int pos, Physical ph, long pp)
                :base(_cx,rs,pos,ph, pp, _Value(rs, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                if (ph is not Enforcement en) 
                    throw new PEException("PE42112");
                return new TRow(res, Pos(en.ppos), Pos(en.tabledefpos),
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Privilege", Int,0);
            t+=new SystemTableColumn(t, "Object", Position,0);
            t+=new SystemTableColumn(t, "Grantee", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Grant table
        /// </summary>
        internal class LogGrantBookmark : LogSystemBookmark
        {
            LogGrantBookmark(Context _cx,SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Grant g = (Grant)ph;
                return new TRow(res,
                    Pos(g.ppos),
                    new TInt((long)g.priv),
                    Pos(g.obj),
                    Pos(g.grantee));
            }
        }
        /// <summary>
        /// set up the Log$Index table
        /// </summary>
        static void LogIndexResults()
        {
            var t = new SystemTable("Log$Index");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Table", Position,0);
            t+=new SystemTableColumn(t, "Flags", Char,0);
            t+=new SystemTableColumn(t, "Reference", Position,0);
            t+=new SystemTableColumn(t, "Adapter", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Index table
        /// </summary>
        internal class LogIndexBookmark : LogSystemBookmark
        {
            LogIndexBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                    PIndex p = (PIndex)ph;
                    return new TRow(res,
                        Pos(p.ppos),
                        new TChar(p.name),
                        Pos(p.tabledefpos),
                        new TChar(p.flags.ToString()),
                        Pos(p.reference),
                        Display(p.adapter));
            }
         }
        /// <summary>
        /// set up the Log$IndexKey table
        /// </summary>
        static void LogIndexKeyResults()
        {
            var t = new SystemTable("Log$IndexKey");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "ColNo", Int,1);
            t+=new SystemTableColumn(t, "Column", Position,0);
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
                 : base(_cx,rs, pos,ph, pp, _Value(rs, ph,i))
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
            static TRow _Value(SystemRowSet res, Physical ph,int _ix)
            {
                PIndex x = (PIndex)ph;
                return new TRow(res,
                    Pos(x.ppos),
                    new TInt(_ix),
                    Pos(x.columns[_ix]));
            }
        }
        /// <summary>
        /// set up the Log$Ordering table
        /// </summary>
        static void LogOrderingResults()
        {
            var t = new SystemTable("Log$Ordering");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "TypeDefPos", Position,0);
            t+=new SystemTableColumn(t, "FuncDefPos", Position,0);
            t+=new SystemTableColumn(t, "OrderFlags", Char,0);
            t+=new SystemTableColumn(t, "Transaction", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Ordering table
        /// </summary>
        internal class LogOrderingBookmark : LogSystemBookmark
        {
            LogOrderingBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Ordering o = (Ordering)ph;
                return new TRow(res,
                    Pos(o.ppos),
                    new TChar(o.ToString()),
                    Pos(o.funcdefpos),
                    new TChar(o.flags.ToString()));
            }
        }
        /// <summary>
        /// set up the Log$Procedure table
        /// </summary>
        static void LogProcedureResults()
        {
            var t = new SystemTable("Log$Procedure");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "RetDefPos", Position,0);
            t+=new SystemTableColumn(t, "Proc", Char,0);
            t+=new SystemTableColumn(t, "Transaction", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Procedure table
        /// </summary>
        internal class LogProcedureBookmark : LogSystemBookmark
        {
            LogProcedureBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PProcedure p = (PProcedure)ph;
                return new TRow(res,
                    Pos(p.ppos),
                    new TChar(p.name),
                    Pos(p.dataType.defpos),
                    Display(p.source?.ident??""));
            }
        }
        /// <summary>
        /// set up the Log$Revoke table
        /// </summary>
        static void LogRevokeResults()
        {
            var t = new SystemTable("Log$Revoke");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Privilege", Int,0);
            t+=new SystemTableColumn(t, "Object", Position,0);
            t+=new SystemTableColumn(t, "Grantee", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Revoke
        /// </summary>
        internal class LogRevokeBookmark : LogSystemBookmark
        {
            LogRevokeBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Grant g = (Grant)ph;
                return new TRow(res,
                    Pos(g.ppos),
                    new TInt((long)g.priv),
                    Pos(g.obj),
                    Pos(g.grantee));
            }
         }
        /// <summary>
        /// set up the Log$Table table
        /// </summary>
        static void LogTableResults()
        {
            var t = new SystemTable("Log$Table");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Defpos", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Table
        /// </summary>
        internal class LogTableBookmark : LogSystemBookmark
        {
            LogTableBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PTable d = (PTable)ph;
                return new TRow(res,
                    Pos(d.ppos),
                    new TChar(d.name),
                    Pos(d.defpos));
            }
        }
        /// <summary>
        /// set up the Log$Trigger table
        /// </summary>
        static void LogTriggerResults()
        {
            var t = new SystemTable("Log$Trigger");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Table", Position,0);
            t+=new SystemTableColumn(t, "Flags", Char,0);
            t+=new SystemTableColumn(t, "OldTable", Char,0);
            t+=new SystemTableColumn(t, "NewTable", Char,0);
            t+=new SystemTableColumn(t, "OldRow", Char,0);
            t+=new SystemTableColumn(t, "NewRow", Char,0);
            t+=new SystemTableColumn(t, "Def", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Trigger
        /// </summary>
        internal class LogTriggerBookmark : LogSystemBookmark
        {
            LogTriggerBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
                        if (Eval(res.where, _cx))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PTrigger d = (PTrigger)ph;
                return new TRow(res,
                    Pos(d.ppos),
                    new TChar(d.name),
                    Pos(d.target),
                    new TChar(d.tgtype.ToString()),
                    new TChar(d.oldTable?.ident??""),
                    new TChar(d.newTable?.ident??""),
                    new TChar(d.oldRow?.ident??""),
                    new TChar(d.newRow?.ident??""),
                    Display(d.src?.ident??""));
            }
        }
        /// <summary>
        /// setup the Log$TriggerUpdate table
        /// </summary>
        static void LogTriggerUpdateColumnResults()
        {
            var t = new SystemTable("Log$TriggerUpdateColumn");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Column", Position,0);
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
                BList<long?> s,int i): base(_cx,res,pos,ph, pp,_Value(res, ph,s,i))
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
            static TRow _Value(SystemRowSet res, Physical ph,BList<long?> _sub,int _ix)
            {
                PTrigger pt = (PTrigger)ph;
                return new TRow(res,
                    Pos(pt.ppos),
                    new TInt(_sub[_ix]??-1L));
            }
        }
        static void LogTriggeredActionResults()
        {
            var t = new SystemTable("Log$TriggeredAction");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Trigger", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class LogTriggeredActionBookmark : LogSystemBookmark
        {
            LogTriggeredActionBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                TriggeredAction t = (TriggeredAction)ph;
                return new TRow(res,
                    Pos(t.ppos),
                    Pos(t.trigger));
            }
        }
        /// <summary>
        /// set up the Log$Type table
        /// </summary>
        static void LogTypeResults()
        {
            var t = new SystemTable("Log$Type");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t += new SystemTableColumn(t, "Name", Char, 0);
            t+=new SystemTableColumn(t, "SuperType", Position,0);
            t += new SystemTableColumn(t, "Graph", Char, 0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for the Log$Type table
        /// </summary>
        internal class LogTypeBookmark : LogSystemBookmark
        {
            LogTypeBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PType t = (PType)ph;
                var sb = new StringBuilder();
                var cm = "";
                TypedValue un = TNull.Value;
                if (t.under.Count > 0)
                {
                    for (var b = t.under.First(); b != null; b = b.Next())
                    { sb.Append(cm); cm = ","; sb.Append(Pos(b.key().defpos)); }
                    un = new TChar(sb.ToString());
                }
                TypedValue gr = (t is PEdgeType) ? new TChar("EDGETYPE")
                    : (t is PNodeType) ? new TChar("NODETYPE") : TNull.Value;
                return new TRow(res,
                    Pos(t.ppos),
                    new TChar(t.name),
                    un,
                    gr);
            }
         }
        /// <summary>
        /// set up the Log$TypeMethod table
        /// </summary>
        static void LogTypeMethodResults()
        {
            var t = new SystemTable("Log$TypeMethod");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Type", Char,0);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$TypeMethod
        /// </summary>
        internal class LogTypeMethodBookmark : LogSystemBookmark
        {
            LogTypeMethodBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PMethod t = (PMethod)ph;
                if (t.udt is null)
                    throw new PEException("PE0426");
                    return new TRow(res,
                        Pos(t.ppos),
                        new TChar(t.udt.ToString()),
                        new TChar(t.name));
            }
        }
        /// <summary>
        /// set up the Log$View table
        /// </summary>
        static void LogViewResults()
        {
            var t = new SystemTable("Log$View");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Select", Char,0);
            t+=new SystemTableColumn(t, "Struct", Position,0);
            t+=new SystemTableColumn(t, "Using", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$View
        /// </summary>
        internal class LogViewBookmark : LogSystemBookmark
        {
            LogViewBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                PView d = (PView)ph;
                TypedValue us = TNull.Value;
                TypedValue st = TNull.Value;
                if (d is PRestView2 view)
                    us = Pos(view.usingTable);
                return new TRow(res,
                    Pos(d.ppos),
                    new TChar(d.name),
                    Display(d.viewdef),
                    st,
                    us);
            }
        }
        /// <summary>
        /// set up the Log$Record table
        /// </summary>
        static void LogRecordResults()
        {
            var t = new SystemTable("Log$Record");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Table", Position,0);
            t+=new SystemTableColumn(t, "SubType", Position,0);
            t+=new SystemTableColumn(t, "Classification", Char,0);
            t+=new SystemTableColumn(t, "Transaction", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Record
        /// </summary>
        internal class LogRecordBookmark : LogSystemBookmark
        {
            LogRecordBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Record d = (Record)ph;
                return new TRow(res,
                    Pos(d.ppos),
                    Pos(d.tabledefpos),
                    Pos(d.subType),
                    new TChar(d.classification.ToString()));
            }
        }
        /// <summary>
        /// set up the Log$RecordField table
        /// </summary>
        static void LogRecordFieldResults()
        {
            var t = new SystemTable("Log$RecordField");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "ColRef", Position,1);
            t+=new SystemTableColumn(t, "Data", Char,0);
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
                : base(_cx,rec.res,pos,rec.ph,rec.nextpos, _Value(rec.res,rec.ph,fld))
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
            static TRow _Value(SystemRowSet res, Physical ph, ABookmark<long,TypedValue>_fld)
            {
                Record r = (Record)ph;
                long p = _fld.key();
                var v = r.fields[p] ?? TNull.Value;
                return new TRow(res,
                    Pos(r.ppos),
                    Pos(p),
                    new TChar(v.ToString()));
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "DefPos", Position,0);
            t+=new SystemTableColumn(t, "Table", Position,0);
            t+=new SystemTableColumn(t, "SubType", Position,0);
            t+=new SystemTableColumn(t, "Classification", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$UpdatePost
        /// </summary>
        internal class LogUpdateBookmark : LogSystemBookmark
        {
            LogUpdateBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                Update u = (Update)ph;
                return new TRow(res,
                    Pos(u.ppos),
                    Pos(u.defpos),
                    Pos(u.tabledefpos),
                    Pos(u.subType),
                    new TChar(u.classification.ToString()));
            }
        }
        /// <summary>
        /// set up the Log$Transaction table
        /// </summary>
        static void LogTransactionResults()
        {
            var t = new SystemTable("Log$Transaction");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "NRecs", Int,0);
            t+=new SystemTableColumn(t, "Time", Int,0);
            t+=new SystemTableColumn(t, "User", Position,0);
            t+=new SystemTableColumn(t, "Role", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Log$Transaction
        /// </summary>
        internal class LogTransactionBookmark : LogSystemBookmark
        {
            LogTransactionBookmark(Context _cx, SystemRowSet r, int pos, Physical ph, long pp)
                : base(_cx,r, pos, ph, pp, _Value(r, ph))
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
            static TRow _Value(SystemRowSet res, Physical ph)
            {
                    PTransaction t = (PTransaction)ph;
                    return new TRow(res,
                        Pos(t.ppos),
                        new TInt(t.nrecs),
                        new TDateTime(new DateTime(t.time)),
                        Pos(t.ptuser?.defpos??-1L),
                        Pos(t.ptrole.defpos));
            }
        }
        /// <summary>
        /// set up the Sys$Role table
        /// </summary>
        static void SysRoleResults()
        {
            var t = new SystemTable("Sys$Role");
            t+=new SystemTableColumn(t, "Pos", Position,0);
            t+=new SystemTableColumn(t, "Name", Char,1);
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// enumerate the Sys$Role table
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
                      ((Role)bmk.value()).lastChange,_Value(res, (Role)bmk.value()))
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
            static TRow _Value(SystemRowSet res,Role a)
            {
                return new TRow(res,
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
            t+=new SystemTableColumn(t, "Pos", Position,0);
            t+=new SystemTableColumn(t, "Name", Char,1);
            t+=new SystemTableColumn(t, "SetPassword", Bool,0); // usually null
            t+=new SystemTableColumn(t, "Clearance", Char,0); // usually null, otherwise D to A
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// enumerate the Sys$User table
        /// 
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
                      _Value(res, (User)bmk.value()))
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
            static TRow _Value(SystemRowSet res, User a)
            {
                return new TRow(res,
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
            t+=new SystemTableColumn(t, "Role", Char,1);
            t+=new SystemTableColumn(t, "User", Char,1);
            t .AddIndex("Role", "User");
            t.Add();
        }
        
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
                       ui.priv.HasFlag(Grant.Privilege.Usage) && rbmk is not null)
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
                    oi.priv.HasFlag(Grant.Privilege.Usage) && rbmk is not null)
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
                    _cx.db.objects[upos] is not User us)
                    throw new PEException("PE0450");
                return new TRow(rs,
                    new TChar(ro.name ?? ""),
                    new TChar(us.name ?? ""));
            }
        }
        static void SysAuditResults()
        {
            var t = new SystemTable("Sys$Audit");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "User", Char,0);
            t+=new SystemTableColumn(t, "Table", Position,0);
            t+=new SystemTableColumn(t, "Timestamp", Timestamp,0);
            t.Add();
        }
        internal class SysAuditBookmark(Context _cx, SystemRowSet res, SystemRowSet.LogBookmark b, int pos) : SystemBookmark(_cx,res,pos,b.ph.ppos,0,_Value(res,b.ph))
        {
            public readonly LogBookmark _bmk = b;

            internal static SysAuditBookmark? New(Context _cx, SystemRowSet res)
            {
                for (var bmk = LogBookmark.New(_cx,res);bmk is not null;bmk=bmk.Next(_cx) as LogBookmark)
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
            static TRow _Value(SystemRowSet rs, Physical ph)
            {
                if (ph is not Audit au ||
                    au.user is not User us)
                    throw new PEException("PE0451");
                return new TRow(rs,
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Seq", Int,1);
            t+=new SystemTableColumn(t, "Col", Position,0);
            t+=new SystemTableColumn(t, "Key", Char,0);
            t.Add();
        }
        internal class SysAuditKeyBookmark(Context _cx, SystemRowSet res, SystemRowSet.LogBookmark bmk,
            ABookmark<long, string> _in, int ix, int pos) : SystemBookmark(_cx, res, pos, bmk.ph.ppos, 0, _Value(res, bmk.ph, _in, ix))
        {
            public readonly LogBookmark _bmk = bmk;
            public readonly ABookmark<long, string> _inner = _in;
            public readonly int _ix = ix;

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
            static TRow _Value(SystemRowSet res, Physical ph,
                ABookmark<long, string> _in, int _ix)
            {
                return new TRow(res, 
                    Pos(ph.ppos), new TInt(_ix),
                    Pos(_in.key()), new TChar(_in.value()));
            }

            protected override Cursor? _Next(Context _cx)
            {
                var ix = _ix;
                var inner = _inner;
                for (var bmk = _bmk; bmk is not null; )
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
                for (var bmk = _bmk; bmk is not null;)
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Type", Char,0);
            t+=new SystemTableColumn(t, "Classification", Char,0);
            t+=new SystemTableColumn(t, "LastTransaction", Position,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// Classification is useful for DBObjects, TableColumns, and records
        /// 
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
                        || cx.db.GetD(rw.ppos) is not Physical ph)
                        throw new PEException("PE42120");
                    return (rw.defpos, rw.ppos, new TRow(res, Pos(rw.ppos),
                        new TChar(rw.GetType().Name),
                        new TChar(rw.classification.ToString()),
                        Pos(ph.trans)));
                }
                else 
                {
                    var ppos = tbm?.key() ?? obm.key();
                    if (obm.value() is not DBObject ob ||
                        cx.db is not Database db || db.GetD(ppos) is not Physical ph)
                        throw new PEException("PE0453");
                    return (ob.defpos, ob.lastChange, new TRow(res, Pos(ppos),
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Col", Position,1);
            t+=new SystemTableColumn(t, "Classification", Char,0);
            t+=new SystemTableColumn(t, "LastTransaction", Position,0);
            t.AddIndex("Pos", "Col");
            t.Add();
        }
        internal class SysClassifiedColumnDataBookmark : SystemBookmark
        {
            readonly ABookmark<long, TableColumn> _cbm;
            readonly ABookmark<long, TableRow> _tbm;
            SysClassifiedColumnDataBookmark(Context _cx, SystemRowSet res, int pos, 
                ABookmark<long, TableColumn> cbm, ABookmark<long, TableRow> tbm)
                : this(_cx, res, pos, cbm, tbm, _Value(res, tbm.value()))
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
                for (var cbm = cols.First();cbm is not null;cbm=cbm.Next())
                {
                    var tc = cbm.value();
                    if (db.objects[tc.tabledefpos] is not Table tb)
                        throw new PEException("PE42123");
                    for (var tbm = tb.tableRows?.PositionAt(0); tbm != null; tbm = tbm.Next())
                    {
                        var rt = tbm.value();
                        if (rt.vals[tc.defpos] is not null)
                        {
                            var rb = new SysClassifiedColumnDataBookmark(_cx,res, 0, cbm, tbm);
                            if (rb.Match(res) && Eval(res.where, _cx))
                                return rb;
                        }
                    }
                }
                return null;
            }
            static (long, long, TRow) _Value(SystemRowSet res,TableRow rw)
            {
                return (rw.defpos,rw.ppos, new TRow(res,
                    Pos(rw.defpos),
                    new TChar(rw.classification.ToString()),
                    Pos(rw.ppos)));
            }
            protected override Cursor? _Next(Context _cx)
            {
                var cbm = _cbm;
                var tbm = _tbm.Next();
                for (;cbm is not null; )
                {
                    var tc = cbm.value();
                    for (; tbm != null; tbm = tbm.Next())
                    {
                        var rt = tbm.value();
                        if (rt.vals[tc.defpos] is not null)
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
            t+=new SystemTableColumn(t, "Name", Char,1);
            t+=new SystemTableColumn(t, "Scope", Char,1);
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
                return new TRow(rs, 
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
 /*       static void SysGraphResults()
        {
            var t = new SystemTable("Sys$Graph");
            t += new SystemTableColumn(t, "Uid", Position, 1);
            t += new SystemTableColumn(t, "Id", Char, 0);
            t += new SystemTableColumn(t, "Type", Char, 0);
            t.AddIndex("Pos");
            t.Add();
        }
        internal class SysGraphBookmark : SystemBookmark
        {
            readonly ABookmark<long, Graph> _bmk;
            public SysGraphBookmark(Context cx, SystemRowSet r, int pos, 
                ABookmark<long,Graph> bmk) 
                : base(cx, r, pos, bmk.key(), cx.db.loadpos, _Value(r,bmk.value()))
            {
                _bmk = bmk;
            }
            internal static SysGraphBookmark? New(Context cx,SystemRowSet rs)
            {
                for (var bmk=cx.db.graphs.First();bmk is not null;bmk=bmk.Next())
                {
                    var rb = new SysGraphBookmark(cx, rs, 0, bmk);
                    if (rb.Match(rs) && Eval(rs.where, cx))
                        return rb;
                }
                return null;
            }
            static TRow _Value(SystemRowSet rs,Graph g)
            {
                if (g.nodes.First()?.value() is not TNode n)
                    throw new PEException("PE91046");
                return new TRow(rs, n,
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
        } */
        /// <summary>
        /// set up the Role$View table
        /// </summary>
        static void RoleViewResults()
        {
            var t = new SystemTable("Role$View");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "View", Char,1);
            t+=new SystemTableColumn(t, "Select", Char,0);
            t+=new SystemTableColumn(t, "Struct", Char,0);
            t+=new SystemTableColumn(t, "Using", Char,0);
            t+=new SystemTableColumn(t, "Definer", Char,0);
            t.AddIndex("View");
            t.AddIndex("Pos");
            t.Add();
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
                    throw new DBException("42105").Add(Qlx.ROLE);
                var us = "";
                if (ob is RestView rv && _cx.db.objects[rv.usingTable] is Table tb && tb.infos[ro.defpos] is ObInfo oi)
                        us = oi.name;
                if (ob is not View vw || 
                    vw.NameFor(_cx) is not string st || vw.infos[ro.defpos] is not ObInfo ov)
                    throw new PEException("PE0460");
                return new TRow(rs,
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
        static void RoleSQLResults()
        {
            var t = new SystemTable("Role$SQL");
            t += new SystemTableColumn(t, "Name", Position, 1);
            t += new SystemTableColumn(t, "Key", Char, 0);
            t += new SystemTableColumn(t, "Definition", Char, 0);
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$View
        /// </summary>
        internal class RoleSQLBookmark : SystemBookmark
        {
            /// <summary>
            /// enumerate the tables tree
            /// </summary>
            readonly ABookmark<long, object> _bmk;
            /// <summary>
            /// create the Sys$View enumerator
            /// </summary>
            /// <param name="r"></param>
            RoleSQLBookmark(Context _cx, SystemRowSet res, int pos, ABookmark<long, object> bmk)
                : base(_cx, res, pos, bmk.key(), ((View)bmk.value()).lastChange,
                      _Value(_cx, res, bmk))
            {
                _bmk = bmk;
            }
            internal static RoleSQLBookmark? New(Context _cx, SystemRowSet res)
            {
                if (_cx.role is not Role ro)
                    throw new PEException("PE49520");
                for (var bmk = _cx.db.objects.PositionAt(0); bmk != null; bmk = bmk.Next())
                    if (bmk.value() is View v && v.infos[ro.defpos] is not null)
                    {
                        var rb = new RoleSQLBookmark(_cx, res, 0, bmk);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Name,Key,Definition)
            /// </summary>
            static TRow _Value(Context _cx, SystemRowSet rs, ABookmark<long,object>e)
            {
                return ((DBObject)e.value()).RoleSQLValue(_cx, rs, e);
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
                        var rb = new RoleSQLBookmark(_cx, res, _pos + 1, bmk);
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "DomainName", Char,0);
            t+=new SystemTableColumn(t, "CheckName", Char,0);
            t+=new SystemTableColumn(t, "Select", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// and enumerator for Role$DomainCheck
        /// 
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
                if (_cx.db is not Database db ||
                    ob is not Domain domain || db.objects[ck] is not Check check)
                    throw new PEException("PE0471");
                return (ck,new TRow(rs, 
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "TableName", Char,0);
            t+=new SystemTableColumn(t, "CheckName", Char,0);
            t+=new SystemTableColumn(t, "Select", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$TableCheck
        /// 
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
                if (_cx._Ob(ch.checkobjpos) is not DBObject oc)
                    throw new PEException("PE0474");
                return new TRow(rs,
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
            var t = new SystemTable("Role$TablePeriod");
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "TableName", Char,0);
            t+=new SystemTableColumn(t, "PeriodName", Char,0);
            t+=new SystemTableColumn(t, "PeriodStartColumn", Char,0);
            t+=new SystemTableColumn(t, "PeriodEndColumn", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$TablePeriod
        /// 
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
                    throw new DBException("42105").Add(Qlx.ROLE);
                var t = (Table)tb;
                var oi = t.infos[_cx.role.defpos] ?? throw new PEException("PE0481");
                var pd = (PeriodDef)(_cx.db.objects[sys ? t.systemPS : t.applicationPS]
                    ?? throw new PEException("PE0482"));
                var op = pd.infos[_cx.role.defpos];
                string sn="", en="";
                for (var b = t.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && _cx._Ob(p) is DBObject ob 
                        && ob.infos[ro.defpos] is ObInfo ci)
                    {
                        if (p == pd.startCol)
                            sn = ci.name ?? "";
                        if (p == pd.endCol)
                            en = ci.name ?? "";
                    }
                return new TRow(rs, 
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Table", Char,0);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Seq", Int,0);
            t+=new SystemTableColumn(t, "Domain", Position,0);
            t+=new SystemTableColumn(t, "Default", Char,0);
            t+=new SystemTableColumn(t, "NotNull", Bool,0);
            t+=new SystemTableColumn(t, "Generated", Char,0);
            t+=new SystemTableColumn(t, "Update", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Column:
        /// table and view column names as seen from the current role
        /// 
        /// </summary>
        internal class RoleColumnBookmark : SystemBookmark
        {
            /// <summary>
            /// Enumerators for implementation
            /// </summary>
            readonly ABookmark<long, object> _outer;
            readonly ABookmark<string,long?>? _inner;
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
                    throw new DBException("42105").Add(Qlx.ROLE);
                if (ta is not Table tb || tb.infos[ro.defpos] is not ObInfo oi
                    || cx.db.objects[p] is not TableColumn tc
                    || tc.infos[ro.defpos] is not ObInfo si)
                    throw new PEException("PE0491");
                return new TRow(rs,
                    Pos(p),
                    new TChar(oi.name??""),
                    new TChar(si.name??""),
                    new TInt(i),
                    Pos(cx.db.Find(d)?.defpos??-1L),
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
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Key", Char,0);
            t+=new SystemTableColumn(t, "Definition", Char,0);
            t.Add();
        }
        
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
            t+=new SystemTableColumn(t, "Pos", Position,1); 
            t+=new SystemTableColumn(t, "TableName", Char,0);
            t+=new SystemTableColumn(t, "ColumnName", Char,0);
            t+=new SystemTableColumn(t, "CheckName", Char,0);
            t+=new SystemTableColumn(t, "Select", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$ColumnCheck
        /// 
        /// </summary>
        internal class RoleColumnCheckBookmark : SystemBookmark
        {
            /// <summary>
            /// 3 enumerators for implementation!
            /// </summary>
            readonly ABookmark<long,bool> _inner;
            readonly ABookmark<string,long?> _middle;
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
                                for (var inner = tc.checks.First(); inner != null;
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
                return new TRow(rs, 
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
                    if (inner != null && (inner = inner.Next()) != null && middle is not null)
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
                            inner = tc.checks.First();
                            continue;
                        }
                    if ((outer = outer.Next()) == null)
                        return null;
                    if (outer.value() is not Table tb || tb.infos[ro.defpos] is not ObInfo oi)
                        continue;
                    middle = oi.names.First();
                    if (inner != null && middle is not null)
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
            t+=new SystemTableColumn(t, "Table", Char,0);
            t+=new SystemTableColumn(t, "Column", Char,0);
            t+=new SystemTableColumn(t, "Grantee", Char,0);
            t+=new SystemTableColumn(t, "Privilege", Char,0);
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$columnPrivilege
        /// 
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
                    tb.infos[ro.defpos] is not ObInfo dt)
                        throw new PEException("PE42107");
                return new TRow(rs,
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
                    if (middle.value() is QlValue mc)
                        for (inner = _cx.db.objects.PositionAt(0); inner != null; inner = inner.Next())
                            if (_cx.db.objects[mc.defpos] is QlValue rc && rc.defpos == mc.defpos)
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "DataType", Char,0);
            t+=new SystemTableColumn(t, "DataLength", Int,0);
            t+=new SystemTableColumn(t, "Scale", Int,0);
            t+=new SystemTableColumn(t, "StartField", Char,0);
            t+=new SystemTableColumn(t, "EndField", Char,0);
            t+=new SystemTableColumn(t, "DefaultValue", Char,0);
            t+=new SystemTableColumn(t, "Struct", Char,0);
            t+=new SystemTableColumn(t, "Definer", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// and enumerator for Role$Domain
        /// 
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
                      _Value(res,outer.value()))
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
            static TRow _Value(SystemRowSet rs, object ob)
            {
                long prec = 0;
                long scale = 0;
                var dm = (Domain)ob;
                if (dm.kind == Qlx.NUMERIC || dm.kind == Qlx.REAL)
                {
                    prec = dm.prec;
                    scale = dm.scale;
                }
                if (dm.kind == Qlx.CHAR || dm.kind == Qlx.NCHAR)
                    prec = dm.prec;
                string start = "";
                string end = "";
                if (dm.kind == Qlx.INTERVAL)
                {
                    start = dm.start.ToString();
                    end = dm.end.ToString();
                }
                string elname = "";
                if (dm.elType is not null)
                    elname = dm.elType.name;
                return new TRow(rs,
                    Pos(dm.defpos),
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Table", Char,0);
            t+=new SystemTableColumn(t, "Flags", Char,0);
            t+=new SystemTableColumn(t, "RefTable", Char,0);
            t+=new SystemTableColumn(t, "Distinct", Char,0); // probably ""
            t+=new SystemTableColumn(t, "Adapter", Char,0);
            t+=new SystemTableColumn(t, "Rows", Int,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Index
        /// 
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
                if (_cx.db.objects[xp] is not Level3.Index x ||
                    _cx.db.objects[x.tabledefpos] is not Table t || _cx.role is not Role ro ||
                    t.infos[ro.defpos] is not ObInfo oi || oi.name==null || x.rows is not MTree mt)
                    throw new PEException("PE48190");
                var rx = (Level3.Index?)_cx.db.objects[x.refindexdefpos];
                var rt = _cx._Ob(x.reftabledefpos);
                var ri = rt?.infos[_cx.role.defpos];
                var ad = _cx._Ob(x.adapter);
                var ai = ad?.infos[_cx.role.defpos];
                return new TRow(rs,
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
            t+=new SystemTableColumn(t,"IndexName", Char,1);
            t+=new SystemTableColumn(t, "TableColumn", Char,0);
            t+=new SystemTableColumn(t, "Position", Int,1);
            t.AddIndex("IndexName", "Position");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$IndexKey
        /// 
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
                if (tc.infos[_cx.role.defpos] is not ObInfo oi)
                    throw new DBException("42105").Add(Qlx.ROLE);
                return new TRow(rs,
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
                for (inner = inner.Next();inner is not null;inner=inner.Next())
                {
                    var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, second, third, inner);
                    if (rb.Match(res) && Eval(res.where, _cx))
                        return rb;
                }
                for (third = third.Next();third is not null;third=third.Next())
                    for (inner=((Level3.Index?)_cx.db.objects[third.key()])?.keys.First();
                        inner is not null;inner=inner.Next())
                    {
                        var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, second, third, inner);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                for (second=second.Next();second is not null;second=second.Next())
                    for (third = second.value()?.First();third is not null;third=third.Next())
                    for (inner=((Level3.Index?)_cx.db.objects[third.key()])?.keys.First();inner is not null;inner=inner.Next())
                    {
                        var rb = new RoleIndexKeyBookmark(_cx,res, _pos + 1, outer, second, third, inner);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                for (outer = outer.Next(); outer is not null;outer=outer.Next())
                    if (outer.value() is Table tb)
                        for (second = tb.indexes.First(); second != null; second = second.Next())
                            for (third=second.value()?.First();third is not null;third=third.Next())
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
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Key", Char,0);
            t+=new SystemTableColumn(t, "Definition", Char,0);
            t.Add();
        }
        
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
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Key", Char,0);
            t+=new SystemTableColumn(t, "Definition", Char,0);
            t.Add();
        }
        
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
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context _cx)
            {
                var enu = _enu;
                for (enu=enu.Next();enu is not null;enu=enu.Next())
                    if (enu.value() is DBObject tb && (tb is Table || tb is View) && tb is not RestView)
                    {
                        var rb = new RolePythonBookmark(_cx,res, _pos+1, enu);
                        if (rb.Match(res) && Eval(res.where, _cx))
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Returns", Char,0);
            t+=new SystemTableColumn(t, "Definition", Char,0);
            t+=new SystemTableColumn(t, "Inverse", Char,0);
            t+=new SystemTableColumn(t, "Monotonic", Bool,0);
            t+=new SystemTableColumn(t, "Definer", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Procedure
        /// 
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
                if (p.infos[_cx.db.role.defpos] is not ObInfo oi
                    || oi.name == null || _cx.db.objects[p.definer] is not Role de)
                    throw new PEException("PE23104");
                string inv = "";
                if (_cx._Ob(p.inverse) is DBObject io)
                    inv = io.NameFor(_cx);
                return new TRow(rs,
                    Pos(p.defpos),
                    new TChar(oi.name),
                    new TChar(p.ToString()),
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t,"Seq", Int,1);
            t+=new SystemTableColumn(t,"Name", Char,0);
            t+=new SystemTableColumn(t,"Type", Char,0);
            t+=new SystemTableColumn(t,"Mode", Char,0);
            t.AddIndex("Pos","Seq");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Parameter
        /// 
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
                      _Value(res,en.key(),inner.key(),fp))
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
            static TRow _Value(SystemRowSet rs, long dp, int i, FormalParameter pp)
            {
                if (pp.name == null)
                    throw new PEException("PE23106");
                return new TRow(rs,
                    Pos(dp),
                    new TInt(i),
                    new TChar(pp.name),
                    new TChar(pp.ToString()),
                    new TChar(((pp.result==Qlx.RESULT) ? Qlx.RESULT : pp.paramMode).ToString()));
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Type", Char,0);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Seq", Int,1);
            t+=new SystemTableColumn(t, "Column", Char,0);
            t+=new SystemTableColumn(t, "Subobject", Char,0);
            t.AddIndex("Pos","Seq");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Object
        /// 
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
                if (oo is not DBObject ob || _cx.role is not Role ro 
                    || ob.infos[ro.defpos] is not ObInfo oi || oi.name == null
                    || _cx.db.objects[xp] is not DBObject ox)
                    throw new DBException("42105").Add(Qlx.OBJECT);
                return new TRow(rs,
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Columns", Int,0);
            t+=new SystemTableColumn(t, "Rows", Int,0);
            t+=new SystemTableColumn(t, "Triggers", Int,0);
            t+=new SystemTableColumn(t, "CheckConstraints", Int,0);
            t+=new SystemTableColumn(t, "RowIri", Char,0);
            t+=new SystemTableColumn(t, "LastData", Int, 0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Table
        /// 
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
                    if (b.value() is Table t && _cx.role is not null &&
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
                var oi = t.infos[_cx.role.defpos] ?? t.infos[t.definer];
                return new TRow(rs,
                    Pos(t.defpos),
                    new TChar(oi?.name??t.name),
                    new TInt(t.Length),
                    new TInt(t.tableRows.Count),
                    new TInt(t.triggers.Count),
                    new TInt(t.tableChecks.Count),
                    new TChar(t.name??""),
                    new TInt(t.lastData));
            }
            /// <summary>
            /// Move to next Table
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                var en = _en;
                for (en=en.Next();en is not null;en=en.Next())
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Flags", Char,0);
            t+=new SystemTableColumn(t, "TableName", Char,0);
            t+=new SystemTableColumn(t, "Definer", Char,0);
            t.AddIndex("Pos");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Trigger
        /// 
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
                if (_cx.role is not Role ro ||
                    tb.infos[ro.defpos] is not ObInfo oi || oi.name == null ||
                    _cx.db.objects[tg.definer] is not Role de)
                    throw new DBException("42105").Add(Qlx.TRIGGER);
                return new TRow(rs,
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
            t+=new SystemTableColumn(t, "Pos", Position,1);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "ColumnName", Char,1);
            t.AddIndex("Pos","ColumnName");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$TriggerUpdateColumn
        /// 
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
                if (_cx.role is not Role ro || 
                    tb.infos[ro.defpos] is not ObInfo ti ||
                    _cx.db.objects[cp] is not TableColumn tc ||
                    tc.infos[ro.defpos] is not ObInfo ci)
                    throw new DBException("42015"); 
                return new TRow(rs,
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
            t+=new SystemTableColumn(t, "Pos", Position,0);
            t+=new SystemTableColumn(t, "Name", Char,1);
            t+=new SystemTableColumn(t, "SuperType", Char,0);
            t+=new SystemTableColumn(t, "OrderFunc", Char,0);
            t+=new SystemTableColumn(t, "OrderCategory", Char,0);
            t += new SystemTableColumn(t, "Subtypes", Int, 0);
            t+=new SystemTableColumn(t, "Definer", Char,0);
            t += new SystemTableColumn(t, "Graph", Char, 0);
            t.AddIndex("Pos");
            t.AddIndex("Name");
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Type
        /// 
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
                    if (en.value() is UDType)
                    {
                        var rb =new RoleTypeBookmark(_cx,res,0, en);
                        if (rb.Match(res) && Eval(res.where, _cx))
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
                if (_cx.role is null)
                    throw new DBException("42105").Add(Qlx.ROLE);
                TypedValue gr = (t is EdgeType) ? new TChar("EDGETYPE")
                            : (t is NodeType) ? new TChar("NODETYPE") : TNull.Value;
                var su = new StringBuilder();
                var cm = "";
                for (var b = t.super.First(); b != null; b = b.Next())
                    if (b.key().name != "")
                    {
                        su.Append(cm); cm = ","; su.Append(Uid(b.key().defpos));
                    }
                return new TRow(rs,
                    Pos(t.defpos),
                    new TChar(t.name),
                    new TChar(su.ToString()),
                    (t.orderFunc is null) ? TNull.Value : new TChar(t.orderFunc.NameFor(_cx)),
                    new TChar((t.orderflags == OrderCategory.None) ? "" :
                        t.orderflags.ToString()),
                    new TInt(t.subtypes.Count),
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
        /// set up the Role$Method table
        /// </summary>
        static void RoleMethodResults()
        {
            var t = new SystemTable("Role$Method");
            t+=new SystemTableColumn(t, "Type", Char,0);
            t+=new SystemTableColumn(t, "Method", Char,0);
            t+=new SystemTableColumn(t, "MethodType", Char,0);
            t+=new SystemTableColumn(t, "Definition", Char,0);
            t+=new SystemTableColumn(t, "Definer", Char,0);
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Method
        /// 
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
                var ro = _cx.role ?? throw new DBException("42105").Add(Qlx.ROLE);
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
                    || p.infos[ro.defpos] is not ObInfo mi 
                    || _cx.db.objects[p.definer] is not Role de)
                    throw new DBException("42105").Add(Qlx.METHOD);
                return new TRow(rs,
                   new TChar(t.name),
                   new TChar(mi.name??""),
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
                    if (_cx.role is Role ro && outer.value() is Domain ut && ut.kind == Qlx.TYPE &&
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
        static void RoleGraphCatalogResults()
        {
            var t = new SystemTable("Role$GraphCatalog");
            t += new SystemTableColumn(t, "Pos", Char, 1);
            t += new SystemTableColumn(t, "PathOrName", Char, 1);
            t += new SystemTableColumn(t, "Type", Char, 0);
            t += new SystemTableColumn(t, "Owner", Int, 0);
            t.Add();
        }
        internal class RoleGraphCatalogBookmark(Context cx, SystemRowSet r, int pos, ABookmark<string, long?> bmk, DBObject ob) : SystemBookmark(cx, r, pos, ob.defpos,ob.defpos,_Value(r,bmk,ob))
        {
            readonly ABookmark<string,long?> _bmk = bmk;

            internal static RoleGraphCatalogBookmark? New(Context cx, SystemRowSet res)
            {
                for (var b = cx.db.catalog.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.value()??-1L] is DBObject c)
                    {
                        var rb = new RoleGraphCatalogBookmark(cx, res, 0, b,c);
                        if (rb.Match(res) && Eval(res.where, cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                for (var b = _bmk.Next(); b != null; b = b.Next())
                    if (cx.db.objects[b.value() ?? -1L] is DBObject c)
                    {
                        var rb = new RoleGraphCatalogBookmark(cx, res, _pos+1, b, c);
                        if (rb.Match(res) && Eval(res.where, cx))
                            return rb;
                    }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,PathOrname,Type,Owner)
            /// </summary>
            static TRow _Value(SystemRowSet rs, ABookmark<string,long?>bmk, DBObject ob)
            {
                return new TRow(rs,
                    new TChar(Uid(ob.defpos)),
                    new TChar(bmk.key()),
                    new TChar(ob.GetType().Name),
                    new TChar(Uid(ob.definer)));
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void RoleGraphEdgeTypeResults()
        {
            var t = new SystemTable("Role$GraphEdgeType");
            t += new SystemTableColumn(t, "Pos", Char, 1);
            t += new SystemTableColumn(t, "GraphorGraphType", Char, 1);
            t += new SystemTableColumn(t, "Name", Char, 0);
            t += new SystemTableColumn(t, "Owner", Int, 0);
            t.Add();
        }
        internal class RoleGraphEdgeTypeBookmark(Context cx, SystemRowSet r, int pos,
            ABookmark<long, object> obmk, ABookmark<long, bool> bmk, DBObject g, EdgeType e) : SystemBookmark(cx, r, pos, e.defpos, e.defpos, _Value(r,g,e))
        {
            readonly ABookmark<long, object> _obmk = obmk;
            readonly DBObject _g = g;
            readonly ABookmark<long, bool> _bmk = bmk;

            internal static RoleGraphEdgeTypeBookmark? New(Context cx, SystemRowSet res)
            {
                for (var b = cx.db.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is Graph g)
                        for (var c = g.graphTypes.First(); c != null; c = c.Next())
                        {
                            if (cx._Ob(c.key()) is EdgeType e)
                            {
                                var rb = new RoleGraphEdgeTypeBookmark(cx, res, 0, b, c, g, e);
                                if (rb.Match(res) && Eval(res.where, cx))
                                    return rb;
                            }
                        }
                    else if (b.value() is GraphType gt)
                        for (var c = gt.constraints.First(); c != null; c = c.Next())
                            if (cx._Ob(c.key()) is EdgeType e)
                            {
                                var rb = new RoleGraphEdgeTypeBookmark(cx, res, 0, b, c, gt, e);
                                if (rb.Match(res) && Eval(res.where, cx))
                                    return rb;
                            }
                    return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                var obmk = _obmk;
                var bmk = _bmk;
                var g = _g;
                for (; obmk != null;)
                {
                    for (var c = bmk?.Next(); c != null; c = c.Next())
                        if (cx._Ob(c.key()) is EdgeType e)
                        {
                            var rb = new RoleGraphEdgeTypeBookmark(cx, res, _pos+1, obmk, c, g, e);
                            if (rb.Match(res) && Eval(res.where, cx))
                                return rb;
                        }
                    for (obmk = obmk.Next(); obmk != null; obmk = obmk.Next())
                    {
                        if (obmk.value() is Graph g1)
                        {
                            g = g1;
                            bmk = g1.graphTypes.First();
                            break;
                        }
                        else if (obmk.value() is GraphType gt)
                        {
                            g = gt;
                            bmk = gt.constraints.First();
                            break;
                        }
                    }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Graph,Name,Owner)
            /// </summary>
            static TRow _Value(SystemRowSet rs,DBObject g,EdgeType e)
            {
                return new TRow(rs,
                    new TChar(Uid(e.defpos)),
                    new TChar(Uid(g.defpos)),
                    new TChar(e.name ?? ""),
                    new TChar(Uid(e.definer)));
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void RoleGraphInfoResults()
        {
            var t = new SystemTable("Role$GraphInfo");
            t += new SystemTableColumn(t, "Name", Char, 1);
            t += new SystemTableColumn(t, "Count", Int, 0);
            t.Add();
        }
        internal class RoleGraphInfoBookmark : SystemBookmark
        {
            readonly BTree<string, int> _values;
            static readonly string[] effects = ["schemas","directories","graphs","graph-types","nodes","edges","properties","labels","label-sets"];
            RoleGraphInfoBookmark(Context cx,SystemRowSet r,int pos, BTree<string,int> v)
                :base(cx,r,pos,pos,pos,_Value(r,v,pos))
            {
                _values = v;
            }
            internal static RoleGraphInfoBookmark? New(Context cx, SystemRowSet res)
            {
                var ro = cx.role ?? throw new DBException("42105").Add(Qlx.ROLE);
                var bn = CTree<long, bool>.Empty; // base node types
                var be = CTree<long, bool>.Empty; // base edge types
                var st = CTree<long, bool>.Empty; // specific types
                var tn = 0; // node count
                var te = 0; // edge count
                var ps = CTree<long, Domain>.Empty; // properties
                var ls = CTree<Domain, bool>.Empty; // labels
                var ll = CTree<CTree<Domain, bool>,bool>.Empty; // label sets
                for (var b = ro.dbobjects.First(); b != null; b = b.Next())
                    if (b.value() is long p && p >= 0 && cx.db.objects[p] is NodeType t)
                    {
                        var ts = t.label.OnInsert(cx,0L);
                        ls += ts;
                        ll += (ts, true);
                        ps += t.representation;
                        if (t is EdgeType)
                        {
                            var bs = true;
                            for (var c = t.super.First(); c != null; c = c.Next())
                                if (c.key() is EdgeType)
                                    bs = false;
                            if (bs)
                                be += (p, true);
                            for (var c = t.tableRows.First(); c != null; c = c.Next())
                            {
                                var tr = c.value();
                                if (cx.db.objects[tr.tabledefpos] is Table tb)
                                {
                                    if (tb.nodeTypes.Count == 0 && tb.defpos == t.defpos)
                                    {
                                        st += (t.defpos, true);
                                        te++;
                                    }
                                    else for (var d = tb.nodeTypes.First(); d != null; d = d.Next())
                                            if (d.key().defpos == t.defpos)
                                            {
                                                st += (t.defpos, true);
                                                te++;
                                            }
                                }
                            }
                        }
                        else
                        {
                            var bs = true;
                            for (var c = t.super.First(); c != null; c = c.Next())
                                if (c.key() is EdgeType)
                                    bs = false;
                            if (bs)
                                bn += (p, true);
                            for (var c = t.tableRows.First(); c != null; c = c.Next())
                            {
                                var tr = c.value();
                                if (cx.db.objects[tr.tabledefpos] is Table tb)
                                {
                                    if (tb.defpos==t.defpos)
                                    {
                                        st += (t.defpos, true);
                                        tn++;
                                    }
                                    else for (var d = tb.nodeTypes.First();d!=null;d=d.Next())
                                    if (d.key().defpos==t.defpos)
                                    {
                                        st += (t.defpos, true);
                                        tn++;
                                    }
                                }
                            }
                        }
                    }
                var v = BTree<string,int>.Empty;
                v += ("schemas", 0);
                v += ("directories",0);
                v += ("graphs", 0);
                v += ("graph-types", 0);
                v += ("nodes", tn);
                v += ("edges", te);
                v += ("properties", (int)ps.Count);
                v += ("labels", (int)ls.Count);
                v += ("label-sets", (int)ll.Count);
                return new RoleGraphInfoBookmark(cx,res,0,v);
            }
            static TRow _Value(SystemRowSet r,BTree<string,int> vs,int pos)
            {
                var s = effects[pos];
                var n = vs[s];
                return new TRow(r, new TChar(s), new TInt(n));
            }

            protected override Cursor? _Next(Context cx)
            {
                if (_pos >= _values.Count - 1) return null;
                return new RoleGraphInfoBookmark(cx, res, _pos + 1, _values);
            }

            protected override Cursor? _Previous(Context cx)
            {
                if (_pos <= 0) return null;
                return new RoleGraphInfoBookmark(cx, res, _pos - 1, _values);
            }
        }
        static void RoleGraphLabelResults()
        {
            var t = new SystemTable("Role$GraphLabel");
            t += new SystemTableColumn(t, "Pos", Char, 1);
            t += new SystemTableColumn(t, "GraphorGraphType", Char, 0);
            t += new SystemTableColumn(t, "NodeorEdgeType", Char, 0);
            t += new SystemTableColumn(t, "Label", Char, 0);
            t.Add();
        }
        internal class RoleGraphLabelBookmark(Context cx, SystemRowSet r, int pos,
            ABookmark<long, object> obmk, ABookmark<long, bool> mbmk, ABookmark<Domain, bool> bmk,
            DBObject g, NodeType nt, string lb) : SystemBookmark(cx, r, pos, nt.defpos, nt.defpos, _Value(r, g, nt, lb))
        {
            readonly ABookmark<long, object> _obmk = obmk;
            readonly DBObject _g = g;
            readonly ABookmark<long,bool> _mbmk = mbmk;
            readonly NodeType _nt = nt;
            readonly ABookmark<Domain, bool> _bmk = bmk;

            internal static RoleGraphLabelBookmark? New(Context cx, SystemRowSet res)
            {
                for (var b = cx.db.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is Graph g)
                        for (var c = g.graphTypes.First(); c != null; c = c.Next())
                        {
                            if (cx._Ob(c.key()) is NodeType e)
                                for (var d = e.label.OnInsert(cx,0L).First(); d != null; d = d.Next())
                                    if (d.key() is NodeType f)
                                    {
                                        var rb = new RoleGraphLabelBookmark(cx, res, 0, b, c, d, g, e, f.name);
                                        if (rb.Match(res) && Eval(res.where, cx))
                                            return rb;
                                    }
                        }
                    else if (b.value() is GraphType gt)
                        for (var c = gt.constraints.First(); c != null; c = c.Next())
                        {
                            if (cx._Ob(c.key()) is NodeType e)
                                for (var d = e.label.OnInsert(cx,0L).First(); d != null; d = d.Next())
                                    if (d.key() is NodeType f)
                                    {
                                        var rb = new RoleGraphLabelBookmark(cx, res, 0, b, c, d, gt, e, f.name);
                                        if (rb.Match(res) && Eval(res.where, cx))
                                            return rb;
                                    }
                        }
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                var obmk = _obmk;
                var mbmk = _mbmk;
                var bmk = _bmk;
                var g = _g;
                var nt = _nt;
                for (; obmk != null;)
                {
                    for (var c = bmk?.Next(); c != null; c = c.Next())
                        if (mbmk!=null && c.key() is NodeType e)
                        {
                            var rb = new RoleGraphLabelBookmark(cx, res, _pos + 1, obmk, mbmk, c, g, nt, e.name);
                            if (rb.Match(res) && Eval(res.where, cx))
                                return rb;
                        }
                    for (mbmk = mbmk?.Next(); mbmk != null; mbmk = mbmk.Next())
                        if (cx.obs[mbmk.key()] is NodeType n)
                        {
                            nt = n;
                            bmk = n.label.OnInsert(cx,0L).First();
                            goto next;
                        }
                    for (obmk = obmk.Next(); obmk != null; obmk = obmk.Next())
                    {
                        if (obmk.value() is Graph g1)
                        {
                            g = g1;
                            mbmk = g1.graphTypes.First();
                            break;
                        }
                        else if (obmk.value() is GraphType gt)
                        {
                            g = gt;
                            mbmk = gt.constraints.First();
                            break;
                        }
                    }
                    next:;
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Graph,name,Owner)
            /// </summary>
            static TRow _Value(SystemRowSet rs, DBObject g, NodeType e, string s)
            {
                return new TRow(rs,
                    new TChar(Uid(e.defpos)),
                    new TChar(Uid(g.defpos)),
                    new TChar(e.name ?? ""),
                    new TChar(s),
                    new TChar(Uid(e.definer))); ;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void RoleGraphNodeTypeResults()
        {
            var t = new SystemTable("Role$GraphNodeType");
            t += new SystemTableColumn(t, "Pos", Char, 1);
            t += new SystemTableColumn(t, "Parent", Char, 1);
            t += new SystemTableColumn(t, "Name", Char, 0);
            t += new SystemTableColumn(t, "Owner", Int, 0);
            t.Add();
        }
        internal class RoleGraphNodeTypeBookmark(Context cx, SystemRowSet r, int pos,
            ABookmark<long, object> obmk, ABookmark<long, bool> bmk, Graph g, NodeType e) : SystemBookmark(cx, r, pos, e.defpos, e.defpos, _Value(r, g, e))
        {
            readonly ABookmark<long, object> _obmk = obmk;
            readonly Graph _g = g;
            readonly ABookmark<long, bool> _bmk = bmk;

            internal static RoleGraphNodeTypeBookmark? New(Context cx, SystemRowSet res)
            {
                for (var b = cx.db.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is Graph g)
                        for (var c = g.graphTypes.First(); c != null; c = c.Next())
                            if (cx._Ob(c.key()) is NodeType e && e.domain.kind!=Qlx.EDGETYPE)
                            {
                                var rb = new RoleGraphNodeTypeBookmark(cx, res, 0, b, c, g, e);
                                if (rb.Match(res) && Eval(res.where, cx))
                                    return rb;
                            }
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                var obmk = _obmk;
                var bmk = _bmk;
                var g = _g;
                for (; obmk != null;)
                {
                    for (var c = bmk?.Next(); c != null; c = c.Next())
                        if (cx._Ob(c.key()) is NodeType e && e.domain.kind!=Qlx.EDGETYPE)
                        {
                            var rb = new RoleGraphNodeTypeBookmark(cx, res, _pos + 1, obmk, c, g, e);
                            if (rb.Match(res) && Eval(res.where, cx))
                                return rb;
                        }
                    for (obmk = obmk.Next(); obmk != null; obmk = obmk.Next())
                        if (obmk.value() is Graph g1)
                        {
                            g = g1;
                            bmk = g.graphTypes.First();
                            break;
                        }
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Graph,name,Owner)
            /// </summary>
            static TRow _Value(SystemRowSet rs, Graph g, NodeType e)
            {
                return new TRow(rs,
                    new TChar(Uid(e.defpos)),
                    new TChar(Uid(g.defpos)),
                    new TChar(e.name ?? ""),
                    new TChar(Uid(e.definer))); ;
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }
        static void RoleGraphPropertyResults()
        {
            var t = new SystemTable("Role$GraphProperty");
            t += new SystemTableColumn(t, "Pos", Char, 1);
            t += new SystemTableColumn(t, "GraphorGraphType", Char, 0);
            t += new SystemTableColumn(t, "NodeorEdgeType", Char, 0);
            t += new SystemTableColumn(t, "Name", Char, 0);
            t += new SystemTableColumn(t, "ValueType", Char, 0);
            t.Add();
        }
        internal class RoleGraphPropertyBookmark(Context cx, SystemRowSet r, int pos,
            ABookmark<long, object> obmk, ABookmark<long, bool> mbmk, ABookmark<int, long?> bmk,
            DBObject g, NodeType nt, TableColumn tc) : SystemBookmark(cx, r, pos, nt.defpos, nt.defpos, _Value(cx, r, g, nt, tc))
        {
            readonly ABookmark<long, object> _obmk = obmk;
            readonly DBObject _g = g;
            readonly ABookmark<long, bool> _mbmk = mbmk;
            readonly NodeType _nt = nt;
            readonly ABookmark<int,long?> _bmk = bmk;

            internal static RoleGraphPropertyBookmark? New(Context cx, SystemRowSet res)
            {
                for (var b = cx.db.objects.PositionAt(0); b != null; b = b.Next())
                    if (b.value() is Graph g)
                        for (var c = g.graphTypes.First(); c != null; c = c.Next())
                        {
                            if (cx.obs[c.key()] is NodeType e)
                                for (var d = e.rowType.First(); d != null; d = d.Next())
                                    if (cx._Ob(d.value()??-1L) is TableColumn tc)
                                    {
                                        var rb = new RoleGraphPropertyBookmark(cx, res, 0, b, c, d, g, e, tc);
                                        if (rb.Match(res) && Eval(res.where, cx))
                                            return rb;
                                    }
                        }
                    else if (b.value() is GraphType gt)
                        for (var c = gt.constraints.First(); c != null; c = c.Next())
                        {
                            if (cx._Ob(c.key()) is NodeType e)
                                for (var d = e.rowType.First(); d != null; d = d.Next())
                                    if (cx._Ob(d.value()??-1L) is TableColumn tc)
                                    {
                                        var rb = new RoleGraphPropertyBookmark(cx, res, 0, b, c, d, gt, e, tc);
                                        if (rb.Match(res) && Eval(res.where, cx))
                                            return rb;
                                    }
                        }
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                var obmk = _obmk;
                var mbmk = _mbmk;
                var bmk = _bmk;
                var g = _g;
                var nt = _nt;
                for (; obmk != null;)
                {
                    for (var c = bmk?.Next(); c != null; c = c.Next())
                        if (mbmk != null && cx._Ob(c.value()??-1L) is TableColumn tc)
                        {
                            var rb = new RoleGraphPropertyBookmark(cx, res, _pos + 1, obmk, mbmk, c, g, nt, tc);
                            if (rb.Match(res) && Eval(res.where, cx))
                                return rb;
                        }
                    for (mbmk = mbmk?.Next(); mbmk != null; mbmk = mbmk.Next())
                        if (cx._Ob(mbmk.key()) is NodeType n)
                        {
                            nt = n;
                            bmk = n.rowType.First();
                            goto next;
                        }
                    for (obmk = obmk.Next(); obmk != null; obmk = obmk.Next())
                    {
                        if (obmk.value() is Graph g1)
                        {
                            g = g1;
                            mbmk = g1.graphTypes.First();
                            break;
                        }
                        else if (obmk.value() is GraphType gt)
                        {
                            g = gt;
                            mbmk = gt.constraints.First();
                            break;
                        }
                    }
                next:;
                }
                return null;
            }
            /// <summary>
            /// the current value: (Pos,Graph,name,Owner)
            /// </summary>
            static TRow _Value(Context cx,SystemRowSet rs, DBObject g, NodeType e, TableColumn tc)
            {
                return new TRow(rs,
                    new TChar(Uid(e.defpos)),
                    new TChar(Uid(g.defpos)),
                    new TChar(e.name ?? ""),
                    new TChar(cx.NameFor(tc.defpos)??""),
                    new TChar(tc.domain.ToString()));
            }
            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
        }

        static void RoleNodeTypeResults()
        {
            var t = new SystemTable("Role$NodeType");
            t += new SystemTableColumn(t, "Pos", Char, 1);
            t += new SystemTableColumn(t, "Name", Char, 0);
            t += new SystemTableColumn(t, "IdName", Char, 0);
            t.Add();
        }
        internal class RoleNodeTypeBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _fb;
            RoleNodeTypeBookmark(Context cx, SystemRowSet r, int pos, ABookmark<long, object> fb, NodeType nt)
                : base(cx, r, pos, nt.defpos, nt.defpos, _Value(cx, r, nt))
            {
                _fb = fb;
            }
            internal static RoleNodeTypeBookmark? New(Context cx, SystemRowSet res)
            {
                var ro = cx.role ?? throw new DBException("42105").Add(Qlx.ROLE);
                for (var outer = cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is NodeType ut && ut.kind!=Qlx.EDGETYPE)
                    {
                        var rb = new RoleNodeTypeBookmark(cx, res, 0, outer, ut);
                        if (rb.Match(res) && Eval(res.where, cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                for (var fb = _fb.Next(); fb != null; fb = fb.Next())
                    if (fb.value() is NodeType nt && nt.kind != Qlx.EDGETYPE)
                    {
                        var rb = new RoleNodeTypeBookmark(cx, res, _pos + 1, fb, nt);
                        if (rb.Match(res) && Eval(res.where, cx))
                            return rb;
                    }
                return null;
            }

            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
            static TRow _Value(Context cx, SystemRowSet rs, NodeType nt)
            {
                return new TRow(rs,
                    Pos(nt.defpos),
                    new TChar(cx.NameFor(nt.defpos) ?? ""),
                    new TChar(cx.NameFor(nt.idCol)??""));
            }
        }
        static void RoleEdgeTypeResults()
        {
            var t = new SystemTable("Role$EdgeType");
            t += new SystemTableColumn(t, "Pos", Char, 1);
            t += new SystemTableColumn(t, "Name", Char, 0);
            t += new SystemTableColumn(t, "LeavingNodeType", Char, 0);
            t += new SystemTableColumn(t, "ArrivingNodeType", Char, 0);
            t += new SystemTableColumn(t, "IdName", Char, 0);
            t += new SystemTableColumn(t, "LeavingName", Char, 0);
            t += new SystemTableColumn(t, "ArrivingName", Char, 0);
            t.Add();
        }
        internal class RoleEdgeTypeBookmark : SystemBookmark
        {
            readonly ABookmark<long, object> _fb;
            RoleEdgeTypeBookmark(Context cx, SystemRowSet r, int pos, ABookmark<long, object> fb, EdgeType nt)
                : base(cx, r, pos, nt.defpos, nt.defpos, _Value(cx, r, nt))
            {
                _fb = fb;
            }
            internal static RoleEdgeTypeBookmark? New(Context _cx, SystemRowSet res)
            {
                var ro = _cx.role ?? throw new DBException("42105").Add(Qlx.ROLE);
                for (var outer = _cx.db.objects.PositionAt(0); outer != null; outer = outer.Next())
                    if (outer.value() is EdgeType ut)
                    {
                        var rb = new RoleEdgeTypeBookmark(_cx, res, 0, outer, ut);
                        if (rb.Match(res) && Eval(res.where, _cx))
                            return rb;
                    }
                return null;
            }
            protected override Cursor? _Next(Context cx)
            {
                for (var fb = _fb.Next(); fb != null; fb = fb.Next())
                    if (fb.value() is EdgeType nt)
                    {
                        var rb = new RoleEdgeTypeBookmark(cx, res, _pos + 1, fb, nt);
                        if (rb.Match(res) && Eval(res.where, cx))
                            return rb;
                    }
                return null;
            }

            protected override Cursor? _Previous(Context cx)
            {
                throw new NotImplementedException();
            }
            static TRow _Value(Context cx, SystemRowSet rs, NodeType nt)
            {
                return new TRow(rs,
                    Pos(nt.defpos),
                    new TChar(cx.NameFor(nt.defpos)??""),
                    new TChar(cx.NameFor(nt.leavingType)??""),
                    new TChar(cx.NameFor(nt.arrivingType)??""),
                    new TChar((nt.idCol>0)?cx.NameFor(nt.idCol)??"":""),
                    new TChar(cx.NameFor(nt.leaveCol)??""),
                    new TChar(cx.NameFor(nt.arriveCol)??""));
            }
        }
        /// <summary>
        /// set up the Role$Privilege table
        /// </summary>
        static void RolePrivilegeResults()
        {
            var t = new SystemTable("Role$Privilege");
            t+=new SystemTableColumn(t, "ObjectType", Char,0);
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Grantee", Char,0);
            t+=new SystemTableColumn(t, "Privilege", Int,0);
            t+=new SystemTableColumn(t, "Definer", Char,0);
            t.Add();
        }
        /// <summary>
        /// enumerate Role$Privilege
        /// 
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
                if (_cx.role is not Role sr ||
                    e.value() is not long p ||
                    _cx.db.objects[p] is not Role ri || t.infos[sr.defpos] is not ObInfo oi
                    || oi.name==null || _cx.db.objects[t.definer] is not Role de)
                    throw new PEException("PE49300");
                return new TRow(rs,
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
            t+=new SystemTableColumn(t, "Type", Char,0); 
            t+=new SystemTableColumn(t, "Name", Char,0);
            t+=new SystemTableColumn(t, "Description", Char, 0);
            t+=new SystemTableColumn(t, "Iri", Char, 0);
            t+=new SystemTableColumn(t, "Metadata", Char,0);
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Object
        /// 
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
                if (_cx.role is not Role ro)
                    throw new PEException("PE49310");
                if (ob.infos[ro.defpos] is not ObInfo oi || oi.name == null)
                    throw new DBException("42105").Add(Qlx.OBJECT);
                var dm = ob as Domain;
                return new TRow(rs,
                    new TChar(ob.GetType().Name),
                    new TChar(oi.name),
                    new TChar(oi.description ?? ""),
                    new TChar(dm?.name ?? ""),
                    new TChar(ObInfo.Metadata(oi.metadata,oi.description??""))); ;
            }
            /// <summary>
            /// Move to the next object
            /// </summary>
            /// <returns>whether there is one</returns>
            protected override Cursor? _Next(Context _cx)
            {
                if (_cx.role is not Role ro)
                    throw new DBException("42105").Add(Qlx.ROLE);
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
            t+=new SystemTableColumn(t, "Table", Char,0);
            t+=new SystemTableColumn(t, "Ordinal", Int,0);
            t+=new SystemTableColumn(t, "Column", Char,0);
            t.Add();
        }
        /// <summary>
        /// an enumerator for Role$Primarykey
        /// 
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
                if (tb.infos[_cx.role.defpos] is not ObInfo oi
                    || e.value() is not long p
                    || _cx.db.objects[p] is not ObInfo ci)
                    throw new DBException("42105").Add(Qlx.OBJECT);
                return new TRow(rs,
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
                for (inner=inner.Next();inner is not null;inner=inner.Next())
                {
                    var rb = new RolePrimaryKeyBookmark(_cx,res, _pos+1, outer, inner);
                    if (rb.Match(res) && Eval(res.where, _cx))
                        return rb;
                }
                for (outer=outer.Next();outer is not null;outer=outer.Next())
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

        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (sysIx != null) { sb.Append(" SysIx:"); sb.Append(Uid(sysIx.defpos)); }
            if (sysFilt is not null) { sb.Append(" SysFilt:"); sb.Append(sysFilt); }
            return sb.ToString();
        }
    }

}


