using System;
using System.Collections.Generic;

using System.Text;
using Pyrrho.Level2;
using Pyrrho.Common;
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
    /// Level 3 version of a Period Definition for a Table
    /// </summary>
    internal class PeriodDef : Selector
    {
        internal const long
            EndCol = -172, // long
            IndexDefPos = -381, // long (for when versioning is defined)
            StartCol = -173, // long
            Versioning = -174, // Versioning
            VersionKind = -175; //Sqlx
        public long startColDefpos => (long)mem[StartCol];
        public long endColDefpos => (long)mem[EndCol];
        public Sqlx versionKind => (Sqlx)(mem[VersionKind] ?? Sqlx.SYSTEM);
        public long indexdefpos => (long)(mem[IndexDefPos] ?? -1);
        public Versioning versioning => (Versioning)mem[Versioning];
        /// <summary>
        /// A new PeriodDef
        /// </summary>
        /// <param name="pd">The level 2 data</param>
        /// <param name="t">The date type</param>
        /// <param name="owner">The object owner</param>
        public PeriodDef(PPeriodDef pd, Domain t,Role definer)
            : base(pd.ppos, BTree<long,object>.Empty
                  +(Table,pd.tabledefpos)+(StartCol,pd.startcol)
                  +(EndCol,pd.endcol)+(NominalType,t)
                  +(Definer,definer.defpos))
        { }
        /// <summary>
        /// A change to a period def
        /// </summary>
        /// <param name="p">The PeriodDef</param>
        /// <param name="r">The Change record</param>
        internal PeriodDef(long defpos,BTree<long,object>m) :base(defpos,m)
        {}
        public static PeriodDef operator+(PeriodDef s,(long,object)x)
        {
            return new PeriodDef(s.defpos, s.mem + x);
        }
        internal override Database Drop(Database db, Role ro, long p)
        {
            var tb = db.objects[tabledefpos] as Table;
            tb += (Versioning, 0);
            db += (tb,p);
            return base.Drop(db, ro, p);
        }
        /// <summary>
        /// A readable version of the PeriodDef
        /// </summary>
        /// <returns>a string</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" StartCol=");sb.Append(Uid(startColDefpos));
            sb.Append(" EndCol=");sb.Append(Uid(endColDefpos));
            return sb.ToString();
        }
    }
}
