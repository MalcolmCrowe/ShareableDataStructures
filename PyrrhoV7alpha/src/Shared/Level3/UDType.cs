using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
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
    /// A database object for a user defined type
    /// Subclasses: XMLTypeByValueList,XMLTypeUnion
    /// </summary>
    internal class UDType : Domain
    {
        internal static long
            Methods = --_uid, // BTree<string,BList<Method>>
            TableDefPos = --_uid, // long
            UnderDefPos = --_uid, // long
            WithUri = --_uid; // string
        /// <summary>
        /// The set of Methods for this Type
        /// </summary>
        public BTree<string, BList<Method>> methods => (BTree<string, BList<Method>>)mem[Methods];
        public long tabledefpos => (long)(mem[TableDefPos] ?? -1);
        public long underdefpos => (long)(mem[UnderDefPos]?? -1);
        /// <summary>
        /// The WITH uri if specified
        /// </summary>
        internal string withuri => (string)mem[WithUri];
        /// <summary>
        /// Constructor: from a level 2 PType
        /// </summary>
        /// <param name="t">The PType</param>
        public UDType(PType t) : base(t, BTree<long, object>.Empty
            + (UnderDefPos, t.underdefpos) + (Methods, BTree<string, BList<Method>>.Empty))
        { }
        public UDType(long defpos, BTree<long, object> m) : base(Sqlx.TYPE, defpos, m) { }
        public static UDType operator+(UDType ut,Method m)
        {
            var ms = ut.methods[m.name] ?? BTree<int, Method>.Empty;
            ms += (m.arity, m);
            return new UDType(ut.defpos, ut.mem + (Methods, ms));
        }
        public static UDType operator+(UDType ut,(long,object)x)
        {
            return new UDType(ut.defpos, ut.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UDType(defpos,m);
        }
        /// <summary>
        /// Check for dependency between type definitions
        /// </summary>
        /// <param name="t">A rename/drop transaction</param>
        /// <returns>whether this type depends on the type being renamed or dropped</returns>
        public override Sqlx Dependent(Transaction t,Context cx)
        {
            if (t.refObj.defpos == underdefpos)
                return Sqlx.RESTRICT;
            return base.Dependent(t,cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Methods:"); sb.Append(methods);
            if (mem.Contains(UnderDefPos)) { sb.Append(" Underdefpos="); sb.Append(Uid(underdefpos)); }
            if (mem.Contains(WithUri)) { sb.Append(" WithUri="); sb.Append(withuri); }
            return sb.ToString();
        }
    }
}
