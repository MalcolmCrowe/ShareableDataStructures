using System;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
namespace Pyrrho.Level4
{
    /// <summary>
    /// This structure is used during column definition by the Parser
    /// </summary>
    internal class RowTypeColumn
    {
        /// <summary>
        /// the data type
        /// </summary>
        internal Domain type;
        /// <summary>
        /// the name of the column (may be null)
        /// </summary>
        internal Ident name;
        /// <summary>
        /// a default value
        /// </summary>
        internal string dfs;
        internal Metadata meta = null;
        /// <summary>
        /// constructor: a new ident for a row type
        /// </summary>
        /// <param name="n">the name of the ident</param>
        /// <param name="t">the type of the ident</param>
        /// <param name="q">the default string</param>
        public RowTypeColumn(Ident n, Domain t, string d)
        {
            name = n;
            type = t;
            dfs = d;
        }
        public override string ToString()
        {
            var sb = new System.Text.StringBuilder();
            if (name != null)
                sb.Append(name + " ");
            sb.Append(type.ToString());
            if (dfs!="")
                sb.Append("default=" +dfs);
            return sb.ToString();
        }
     }
    /// <summary>
    /// This class is used to describe a remote column
    /// </summary>
    internal class ColumnSchema
    {
        internal string name;
        internal string typename;
        internal int flags;
        internal ColumnSchema(string n, string t, int f)
        {
            name = n; typename = t; flags = f;
        }
    }
    /// <summary>
    /// This class is used to describe remote rowsets
    /// </summary>
    internal class RowSetSchema
    {
        internal Ident name;
        internal ColumnSchema[] cols;
        internal RowSetSchema(Ident n, int m)
        {
            name = n;
            cols = new ColumnSchema[m];
        }
    }
}