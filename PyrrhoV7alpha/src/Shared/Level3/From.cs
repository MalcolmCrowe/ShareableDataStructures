using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Text;
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
    /// The interesting bit here is that if we have something like "insert into a(b,c) select d,e from f"
    /// the table-valued subquery silently gets its columns renamed to b,c and types coerced to match a, 
    /// and then the resulting columns get reordered to become candidate rows of a so that trigger processing
    /// etc can proceed.
    /// This is a bit more complex than "insert into a values(..." and requires some planning.
    /// The current approach is that in the above example nominalDataType is a's row type, nominaltype is for (b,c)
    /// and rows is a subquery before the renaming. 
    /// The renaming, reordering and coercion steps complicate the coding.
    /// </summary>
    internal class SqlInsert : Executable
    {
        internal const long
            _Table = -157, // Table
            Provenance = -158, //string
            ValuesDataType = -159, // Domain
            Autokey = -160; // bool
        internal Table table => (Table)mem[_Table];
        /// <summary>
        /// Provenance information if supplied
        /// </summary>
        public string provenance => (string)mem[Provenance];
        internal Domain valuesDataType => (Domain)mem[ValuesDataType];
        internal bool autokey => (bool)(mem[Autokey]??false);
        /// <summary>
        /// Constructor: an INSERT statement from the parser.
        /// </summary>
        /// <param name="cx">The parsing context</param>
        /// <param name="name">The name of the table to insert into</param>
        public SqlInsert(Transaction tr,Lexer lx, Ident name, Correlation cr, string prov,bool ak)
            : base(lx,_Mem(tr,name,cr)+ (Provenance, prov) +(Autokey,ak))
        {
            if (table.rowType.Length == 0)
                throw new DBException("2E111", tr.user, name.ident).Mix();
        }
        protected SqlInsert(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlInsert operator+(SqlInsert s,(long,object)x)
        {
            return new SqlInsert(s.defpos, s.mem + x);
        }
        static BTree<long,object> _Mem(Transaction tr,Ident name,Correlation cr)
        {
            var tb = tr.role.GetObject(name.ident) as Table ??
                throw new DBException("42107", name.ident);
            if (cr!=null)
                tb += (SqlValue.NominalType, cr.Pick(tb.rowType.For(tr, tb, Grant.Privilege.Insert)));
            tb = (Table)tb.AddCols(tb);
            return BTree<long, object>.Empty + (_Table, tb) + (ValuesDataType, tb.rowType);
        }
        public override Transaction Obey(Transaction tr, Context cx)
        {
            Level cl = tr.user.clearance;
            if (tr.user.defpos != tr.owner 
                && table.enforcement.HasFlag(Grant.Privilege.Insert)
                && !cl.ClearanceAllows(table.classification))
                throw new DBException("42105");
            return table.Insert(tr,cx,provenance, cx.data, new Common.Adapters(),
                new List<RowSet>(), classification);
        }
    }
    /// <summary>
    /// QuerySearch is for DELETE and UPDATE 
    /// </summary>
    internal class QuerySearch : Executable
    {
        internal Table table => (Table)mem[SqlInsert._Table];
        internal QuerySearch(Lexer lx,Transaction tr, Ident ic, Correlation cr, Grant.Privilege how,string i)
            : this(Type.DeleteWhere,lx,tr,ic,cr,how,
                  BList<UpdateAssignment>.Empty)// detected for HttpService for DELETE verb
        { }
        /// <summary>
        /// Constructor: a DELETE or UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The parsing context</param>
        protected QuerySearch(Type et,Lexer lx,Transaction tr, Ident ic, Correlation cr, Grant.Privilege how,
            BList<UpdateAssignment> ua=null)
            : base(lx,_Mem(tr,ic,cr,ua)+(_Type,et)+(Table.Assigns,ua))
        {
            if (table.rowType.Length == 0)
                throw new DBException("2E111", tr.user, ic.ident).Mix();
        }
        protected QuerySearch(long dp,BTree<long,object>m) :base(dp,m) { }
        public static QuerySearch operator+(QuerySearch q,(long,object)x)
        {
            return new QuerySearch(q.defpos, q.mem + x);
        }
        static BTree<long, object> _Mem(Transaction tr, Ident name, Correlation cr, BList<UpdateAssignment> ua)
        {
            var tb = tr.role.GetObject(name.ident) as Table ??
                throw new DBException("42107", name.ident);
            var dt = tb.rowType.For(tr, tb, Grant.Privilege.Insert);
            if (cr != null)
                dt = cr.Pick(dt);
            tb += (SqlValue.NominalType, dt);
            return BTree<long, object>.Empty + (SqlInsert._Table, tb);
        }
        /// <summary>
        /// A readable version of the delete statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder("DELETE FROM ");
            if (table != null)
                sb.Append(table.name);
            table.CondString(sb, table.where, " where ");
            return sb.ToString();
        }
        public override Transaction Obey(Transaction tr, Context cx)
        {
            return tr.Execute(this,cx);
        }
    }
    /// <summary>
    /// Implement a searched UPDATE statement as a kind of QuerySearch
    /// </summary>
    internal class UpdateSearch : QuerySearch
    {
        /// <summary>
        /// Constructor: A searched UPDATE statement from the parser
        /// </summary>
        /// <param name="cx">The context</param>
        public UpdateSearch(Lexer lx,Transaction tr, Ident ic, Correlation ca, Grant.Privilege how)
            : base(Type.UpdateWhere, lx, tr, ic, ca, how)
        {  }
        protected UpdateSearch(long dp, BTree<long, object> m) : base(dp, m) { }
        public static UpdateSearch operator+(UpdateSearch u,(long,object)x)
        {
            return new UpdateSearch(u.defpos, u.mem + x);
        }
        /// <summary>
        /// A readable version of the update statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("UPDATE " + table.name + " SET ");
            var c = "";
            for (var a =table.assigns.First();a!=null;a=a.Next())
            {
                sb.Append(c); sb.Append(a.value());
                c = ", ";
            }
            table.CondString(sb, table.where, " where ");
            return sb.ToString();
        }
        public override Transaction Obey(Transaction tr,Context cx)
        {
            return tr.Execute(this,cx); // we need this override even though it looks like the base's one!
        }
    }
}