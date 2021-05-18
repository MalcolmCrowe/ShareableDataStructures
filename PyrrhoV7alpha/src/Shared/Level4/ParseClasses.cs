using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level4
{
    /// <summary>
    /// Used while parsing a QuerySpecification,
    /// and removed at end of the parse (DoStars)
    ///     /// shareable as of 26 April 2021
    /// </summary>
    internal class SqlStar : SqlValue
    {
        public readonly long prefix = -1L;
        internal SqlStar(long dp, long pf) : base(dp,"*",Domain.Content)
        { 
            prefix = pf; 
        }
        protected SqlStar(long dp, long pf, BTree<long,object>m):base(dp,m)
        {
            prefix = pf;
        }
        protected SqlStar(long dp,BTree<long,object> m):base(dp,m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlStar(defpos,prefix,m);
        }
        public static SqlStar operator+(SqlStar s,(long,object)x)
        {
            return (SqlStar)s.New(s.mem + x);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlStar(dp,mem);
        }
        internal override CTree<long, bool> Needs(Context cx)
        {
            return CTree<long,bool>.Empty;
        }
        internal override CTree<long, bool> Needs(Context cx, CTree<long, bool> qn)
        {
            return qn;
        }
        internal override CTree<long, RowSet.Finder> Needs(Context cx, RowSet rs)
        {
            return CTree<long, RowSet.Finder>.Empty;
        }
    }
    /// <summary>
    /// A Method Name for the parser
    /// </summary>
    internal class MethodName
    {
        /// <summary>
        /// The type of the method (static, constructor etc)
        /// </summary>
        public PMethod.MethodType methodType;
        /// <summary>
        /// the name of the method (including $arity)
        /// </summary>
        public Ident mname;
        /// <summary>
        /// The name excluding the arity
        /// </summary>
        public string name; 
        /// <summary>
        /// the target type
        /// </summary>
        public UDType type;
        /// <summary>
        /// the number of parameters of the method
        /// </summary>
        public int arity;
        public CList<long> ins; 
        /// <summary>
        /// The return type
        /// </summary>
        public Domain retType;
        /// <summary>
        /// a string version of the signature
        /// </summary>
        public string signature;
    }
    internal class TablePeriodDefinition
    {
        public Sqlx pkind = Sqlx.SYSTEM_TIME;
        public Ident periodname = new Ident("SYSTEM_TIME", 0);
        public Ident col1 = null;
        public Ident col2 = null;
    }
    internal class PathInfo
    {
        public Database db;
        public Table table;
        public long defpos;
        public string path;
        public Domain type;
        internal PathInfo(Database d, Table tb, string p, Domain t, long dp)
        { db = d; table = tb; path = p; type = t; defpos = dp; }
    }
    internal class PrivNames
    {
        public Sqlx priv;
        public BTree<string,bool> cols;
        internal PrivNames(Sqlx p) { priv = p; cols = BTree<string, bool>.Empty; }
    }
    /// <summary>
    /// when handling triggers etc we need different owner permissions
    /// </summary>
    internal class OwnedSqlValue
    {
        public SqlValue what;
        public long role;
        public long owner;
        internal OwnedSqlValue(SqlValue w, long r, long o) { what = w; role = r; owner = o; }
    }
    /// <summary>
}

