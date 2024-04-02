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
 /*   internal class LabelExpression // GQL
    {
        public Sqlx op;
        public LabelExpression? left;
        public LabelExpression? right;
        public TypedValue? label;
        internal LabelExpression (TypedValue s) { op = Sqlx.NO; label = s; }
        internal LabelExpression (Sqlx o, LabelExpression? l, LabelExpression? r)
        { op = o; left = l; right = r; }
        internal CTree<string,bool> ToSet()
        {
            var r = CTree<string,bool>.Empty;
            if (left != null)
                r += left.ToSet();
            if (right!=null)
                r += right.ToSet();
            if (label != null)
                r += (label.ToString(), true);
            return r;
        }
        static char[] cs = new char[0];
        public static LabelExpression FromString(string s)
        {
            cs = s.ToCharArray();
            var (p,left) = FromString(0, cs.Length);
            while (p < cs.Length)
            {
                var tk = cs[p] switch
                {
                    '|' => Sqlx.VBAR,
                    '&' => Sqlx.AMPERSAND,
                    _ => Sqlx.NO
                };
                var (q,right) = FromString(p+1, cs.Length);
                left = new LabelExpression(tk, left, right);
            }
            return left;
        }
        static (int,LabelExpression) FromString(int off,int len)
        {
            switch (cs[off])
            {
                default:
                    {
                        var b = off;
                        for (; b < len; b++)
                            if (!char.IsLetterOrDigit(cs[b]))
                                break;
                        var lf = new LabelExpression(new TChar(new string(cs, off, b - 1)));
                        if (b < len - 1)
                        {
                            var op = cs[b] switch
                            {
                                '|' => Sqlx.VBAR,
                                '&' => Sqlx.AMPERSAND,
                                _ => Sqlx.NO
                            };
                            if (op != Sqlx.NO)
                            {
                                (b, var rg) = FromString(b + 1, len);
                                return (b,new LabelExpression(op, lf, rg));
                            }
                        }
                        return (b, lf);
                    }
                case '!':
                    {
                        var (o, e) = FromString(off + 1, len);
                        return (o, new LabelExpression(Sqlx.EXCLAMATION, e, null));
                    }
                case '(':
                    {
                        var (o, e) = FromString(off + 1, len);
                        if (cs[o] != ')')
                            throw new DBException("42161", "RPAREN");
                        return (o + 1, new LabelExpression(Sqlx.LPAREN, null, e));
                    }
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            if (left != null) sb.Append(left);
            var ch = op switch
            {
                Sqlx.COLON => ':',
                Sqlx.AMPERSAND => '&',
                Sqlx.VBAR => '|',
                Sqlx.EXCLAMATION => '!',
                Sqlx.LPAREN => '(',
                _ => '\0'
            };
            if (ch!='\0') sb.Append(ch);
            if (right!=null) sb.Append(right);
            if (ch == '(')
                sb.Append(')');
            if (label!=null) sb.Append(label);
            return sb.ToString();
        }
    } */
}

