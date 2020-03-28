using System.Collections.Generic;
using System.Text;
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
        public Domain type;
        /// <summary>
        /// the number of parameters of the method
        /// </summary>
        public int arity;
        public BList<ProcParameter> ins;
        /// <summary>
        /// The return type
        /// </summary>
        public (Domain,ObInfo) retType;
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
    /// <summary>
    /// Helper for metadata
    /// </summary>
    internal class Metadata
    {
        public ulong flags = 0;
        public string description = "";
        public string iri = "";
        public int seq;
        public long refpos;
        public long MaxStorageSize = 0;
        public int MaxDocuments = 0;
        static Sqlx[] keys = new Sqlx[] { Sqlx.ENTITY, Sqlx.ATTRIBUTE, Sqlx.REFERS, Sqlx.REFERRED,
            Sqlx.PIE, Sqlx.POINTS, Sqlx.X, Sqlx.Y, Sqlx.HISTOGRAM, Sqlx.LINE,
            Sqlx.CAPTION, Sqlx.LEGEND, Sqlx.JSON, Sqlx.CSV
#if MONGO
            , Sqlx.USEPOWEROF2SIZES, Sqlx.CAPPED, Sqlx.USEPOWEROF2SIZES, Sqlx.BACKGROUND, Sqlx.DROPDUPS,
            Sqlx.SPARSE
#endif
            , Sqlx.INVERTS, Sqlx.MONOTONIC
        };
        public Metadata(int sq = -1) { seq = sq; }
        public Metadata(Metadata m)
        {
            if (m != null)
            {
                flags = m.flags;
                description = m.description;
                refpos = m.refpos;
                MaxDocuments = m.MaxDocuments;
                MaxStorageSize = m.MaxStorageSize;
                iri = m.iri;
                seq = m.seq;
            }
        }
        internal void Add(Sqlx k)
        {
            ulong m = 1;
            for (int i = 0; i < keys.Length; i++, m = m * 2)
                if (k == keys[i])
                    flags |= m;
        }
        internal void Drop(Sqlx k)
        {
            ulong m = 1;
            for (int i = 0; i < keys.Length; i++, m = m * 2)
                if (k == keys[i])
                    flags &= ~m;
        }
        internal bool Has(Sqlx k)
        {
            ulong m = 1;
            for (int i = 0; i < keys.Length; i++, m = m * 2)
                if (k == keys[i] && (flags & m) != 0)
                    return true;
            return false;
        }
        internal string Flags()
        {
            var sb = new StringBuilder();
            ulong m = 1;
            for (int i = -0; i < keys.Length; i++, m = m * 2)
                if ((flags & m) != 0)
                    sb.Append(" " + keys[i]);
            return sb.ToString();
        }
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
        public string[] names;
        internal PrivNames(Sqlx p) { priv = p; names = new string[0]; }
    }

    internal class Correlation
    {
        public Ident tablealias = null;
        public BList<Ident> cols = BList<Ident>.Empty;
        internal Correlation() { }
        internal Correlation(Ident ta) { tablealias = ta; }
        internal Correlation(Ident[] cs)
        {
            var c = BList<Ident>.Empty;
            for (var i = 0; i < cs.Length; i++)
                c += cs[i];
            cols = c;
        }
        /// <summary>
        /// Select columns from a type.
        /// </summary>
        /// <param name="d"></param>
        /// <returns></returns>
        internal Selection Pick(Ident ic,Selection d)
        {
            if (cols.Length == 0)
                return d;
            var cs = new Selection(ic.iix, ic.ident);
            for (var b = cols.First(); b != null; b = b.Next())
            {
                var id = b.value();
                var s = d[id.ident]??
                    throw new DBException("42112", b.value());
                if (s is SqlTableCol st)
                    s = new SqlCopy(id.iix, id.ident, st.info, d.defpos, st.tableCol.defpos);
                cs += s;
            }
            return cs;
        }
        internal ObInfo Apply(ObInfo d)
        {
            var cs = d.columns;
            if (cols != BList<Ident>.Empty)
            {
                if (cols.Count != cs.Count)
                    throw new DBException("22207");
                for (var b = cs.First(); b != null; b = b.Next())
                    if (cols.Contains(b.key()))
                        cs += (b.key(), b.value() + (Basis.Name, cols[b.key()]));
            }
            if (tablealias != null)
                d += (Basis.Name, tablealias);
            return d + (ObInfo.Columns, cs);
        }
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

