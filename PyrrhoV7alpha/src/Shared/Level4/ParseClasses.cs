using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
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
        public Ident? mname;
        /// <summary>
        /// The name excluding the arity
        /// </summary>
        public Ident? name; 
        /// <summary>
        /// the target type
        /// </summary>
        public UDType? type;
        public Domain ins = Domain.Null; 
        /// <summary>
        /// The return type
        /// </summary>
        public Domain? retType = Domain.Content;
        /// <summary>
        /// a string version of the signature
        /// </summary>
        public string signature = "";
    }
    internal class TablePeriodDefinition
    {
        public Sqlx pkind = Sqlx.SYSTEM_TIME;
        public Ident periodname = new ("SYSTEM_TIME", Iix.None);
        public Ident? col1 = null;
        public Ident? col2 = null;
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
}

