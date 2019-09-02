using System;
using System.IO;
using System.Globalization;
using System.Text;
using System.Collections.Generic;
using System.Xml;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Common
{
    /// <summary>
    /// A class for the lowset levels of Parsing: white space etc
    /// </summary>
    internal class Scanner
    {
        /// <summary>
        /// we only parse things inside transactions
        /// </summary>
        internal Transaction tr;
        /// <summary>
        /// the given input as an array of chars
        /// </summary>
        internal char[] input;
        /// <summary>
        /// The current position in the input
        /// </summary>
        internal int pos;
        /// <summary>
        /// the length of the input
        /// </summary>
        internal int len;
        /// <summary>
        /// the current character
        /// </summary>
        internal char ch;
        /// <summary>
        /// Whether to use XML conventions
        /// </summary>
        internal string mime = "text/plain";
        /// <summary>
        /// Constructor: prepare the scanner
        /// Invariant: ch==input[pos]
        /// </summary>
        /// <param name="s">the input array</param>
        /// <param name="p">the starting position</param>
        internal Scanner(Transaction t,char[] s, int p)
        {
            tr = t;
            input = s;
            len = input.Length;
            pos = p;
            ch = (p<len)?input[p]:'\0';
        }
        /// <summary>
        /// Constructor: prepare the scanner
        /// Invariant: ch==input[pos]
        /// </summary>
        /// <param name="s">the input array</param>
        /// <param name="p">the starting position</param>
        internal Scanner(Transaction t,char[] s, int p, string m)
        {
            tr = t;
            input = s;
            mime = m;
            len = input.Length;
            pos = p;
            ch = (p < len) ? input[p] : '\0';
        }
        /// <summary>
        /// Consume one character
        /// </summary>
        /// <returns>The character (or 0)</returns>
        internal char Advance()
        {
            pos++;
            if (pos >= len)
                ch = (char)0;
            else
                ch = input[pos];
            return ch;
        }
        /// <summary>
        /// Peek at the next character to be consumed
        /// </summary>
        /// <returns>The character (or 0)</returns>
        internal char Peek()
        {
            if (pos + 1 >= len)
                return (char)0;
            return input[pos + 1];
        }
        /// <summary>
        /// Consume white space
        /// </summary>
        /// <returns>The next non-white space character</returns>
        internal char White()
        {
            while (char.IsWhiteSpace(ch))
                Advance();
            return ch;
        }
        /// <summary>
        /// Consume nonwhite space
        /// </summary>
        /// <returns></returns>
        internal string NonWhite()
        {
            int st = pos;
            while (!char.IsWhiteSpace(ch))
                Advance();
            return new string(input, st, pos - st);
        }
        /// <summary>
        /// See if the input matches the given string,
        /// and advance past it if so
        /// </summary>
        /// <param name="mat">The string to test</param>
        /// <returns>Whether we matched and advanced</returns>
        internal bool Match(string mat)
        {
            int n = mat.Length;
            if (n + pos > len)
                return false;
            for (int j = 0; j < n; j++)
                if (input[pos + j] != mat[j])
                    return false;
            pos += n - 1;
            Advance();
            return true;
        }
        /// <summary>
        /// See if the input matches the given string ignoring differences in case,
        /// and advance past it if so
        /// </summary>
        /// <param name="mat">The string to test (guaranteed upper case)</param>
        /// <returns>whether we matched and advanced</returns>
        internal bool MatchNC(string mat)
        {
            int n = mat.Length;
            if (n + pos > len)
                return false;
            for (int j = 0; j < n; j++)
                if (char.ToUpper(input[pos + j]) != mat[j])
                    return false;
            pos += n - 1;
            Advance();
            return true;
        }
         /// <summary>
        /// Construct a string out of a portion of the input.
        /// </summary>
        /// <param name="st">The start</param>
        /// <param name="len">The length</param>
        /// <returns>the string</returns>
        internal string String(int st, int len)
        {
            return new string(input, st, len);
        }
        /// <summary>
        /// This string comparison routine works for Unicode strings
        /// including non-normalized strings.
        /// We compare the strings codepoint by codepoint.
        /// string.CompareTo silently normalizes strings first so that
        /// strings with different codpoints or even lengths can appear to be
        /// equal.
        /// </summary>
        /// <param name="s">a string</param>
        /// <param name="t">another string</param>
        /// <returns>neg,0,pos according as s lt, eq or gt t</returns>
        internal static int Compare(string s, string t)
        {
            int n = s.Length;
            int m = t.Length;
            for (int j = 0; j < n && j < m; j++)
            {
                char c = s[j];
                char d = t[j];
                if (c != d)
                    return c - d;
            }
            return n - m;
        }
        /// <summary>
        /// Watch for named value (REST service)
        /// </summary>
        /// <returns>a name or null</returns>
        internal string GetName()
        {
            int st = pos;
            if (ch == '"')
            {
                Advance();
                st = pos;
                while (ch != '"')
                    Advance();
                Advance();
                return new string(input, st, pos - st - 1);
            }
            if (Char.IsLetter(ch))
            {
                while (Char.IsLetterOrDigit(ch))
                    Advance();
            }
            return new string(input, st, pos - st+1);
        }
    }
    internal class Idents
    {
        ATree<Ident,int> _names = Ident.IdTree<int>.Empty;
        ATree<int, Ident> _list = BTree<int, Ident>.Empty;
        ATree<long, int> _bypos = BTree<long, int>.Empty;
        public Idents() { }
        public Idents(Idents ids,From f) // make a local copy
        {
            for (var b = ids._list.First(); b != null; b = b.Next())
            {
                var a = b.value();
                Add(new Ident(a.ident,a.type,a.segpos));
            }
        }
        internal bool Contains(Ident n)
        {
            return _names.Contains(n);
        }
        internal Ident this[int i]
        {
            get { return _list[i]; }
            set
            {
                while (i > _list.Count)
                    ATree<int, Ident>.Add(ref _list, (int)_list.Count, null);
                if (_list[i] is Ident id)
                    Ident.IdTree<int>.Remove(ref _names, id);
                ATree<int, Ident>.Add(ref _list, i, value);
                Ident.IdTree<int>.Add(ref _names, value, i);
                if (value.segpos > 0)
                    ATree<long, int>.Add(ref _bypos, value.segpos, i);
            }
        }
        /// <summary>
        /// Find the position in the list of an Ident, allowing a partial match
        /// </summary>
        /// <param name="n">The Ident to look up</param>
        /// <param name="s">The unused portion of n</param>
        /// <returns>The position found</returns>
        internal int Get(Ident n, out Ident s)
        {
            s = null;
            if (n == null)
                return -1;
            int iq = -1;
            for (var bm = _names.First(); iq<0 && bm != null; bm = bm.Next())
            {
                var k = bm.key();
                while (k.type == Ident.IDType.Alias && k.sub!=null)
                    k = k.sub;
                if (k.ident.CompareTo(n.ident) != 0)
                    continue;
                iq = bm.value();
                if (iq>=0 && n.segpos > 0)
                {
                    if (_bypos.Contains(n.segpos))
                    {
                        iq = _bypos[n.segpos];
                        break;
                    }
                    var ss = "" + n.segpos;
                    for (var b = _names.First(); b != null; b = b.Next())
                        if (b.key().ident == ss)
                        {
                            Add(n, b.value());
                            break;
                        }
                }
            }
            if (iq>=0)
            {
                var id = _list[iq];
                while (n != null && id != null && n.ident == id.ident)
                {
                    s = n.sub;
                    n = s;
                    id = id.sub;
                }
            }
            return iq;
        }
        /// <summary>
        /// As above, but prefix n by q's alias
        /// </summary>
        /// <param name="q">The query whose alias may have been used</param>
        /// <param name="n">The identifier we are after</param>
        /// <param name="s">The subscript to return if any</param>
        /// <returns>The position of Q.N in our list</returns>
        internal int? Get(Query q,Ident n,out Ident s)
        {
            var qn = q.alias?.ident ?? ("C"+q.cxid);
            s = null;
            if (n == null)
                return null;
            int? iq = null;
            Ident id = null;
            for (var bm = _names.First(); bm != null; bm = bm.Next())
            {
                var k = bm.key();
                if (k.ident.CompareTo(qn) == 0)
                {
                    iq = bm.value();
                    id = _list[iq.Value].sub;
                    if (id != null && id.ident.CompareTo(n.ident) == 0)
                        break;
                }
                if (k.CompareTo(n) > 0)
                    break;
            }
            if (iq.HasValue)
            {
                while (n != null && id != null && n.ident == id.ident)
                {
                    s = n.sub;
                    n = s;
                    id = id.sub;
                }
            }
            return iq;
        }
        internal void Add(Ident n,int i)
        {
            ATree<long, int>.Add(ref _bypos, n.segpos, i);
        }
        internal void Add(Grouping g)
        {
            for (var i = 0; i < g.names.Length; i++)
            {
                var n = g.names[i];
                if (!_names.Contains(n))
                    Add(n);
            }
        }
        /// <summary>
        /// As above but require an exact match
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        internal int this[Ident n]
        {
            get
            {
                if (n == null)
                    return -1;
                int iq = -1;
                if (_names.Contains(n) && n.segpos > 0)
                {
                    iq = _names[n];
                    if (_bypos.Contains(n.segpos))
                    {
                        iq = _bypos[n.segpos];
                        return iq;
                    }
                    var ss = "" + n.segpos;
                    for (var b = _names.First(); b != null; b = b.Next())
                        if (b.key().ident == ss)
                        {
                            Add(n,b.value());
                            break;
                        }
                    if (iq>=0 && _list[iq].CompareTo(n) != 0)
                        return -1;
                }
                if (iq<0)
                    return iq;
                var nm = _list[iq];
                if (nm.segpos > 0 && n.segpos > 0 && nm.segpos != n.segpos)
                    return -1;
                return iq;
            }
        }
        internal int this[string s]
        {
            get
            {
                long p = -1;
                if (s != "" && Char.IsDigit(s[0]))
                    p = long.Parse(s);
                int iq = -1;
                if (_bypos.Contains(p))
                {
                    iq = _bypos[p];
                    return iq;
                }
                for (var a = _list.First(); a != null; a = a.Next())
                {
                    var v = a.value();
                    if (v.Final().ident == s || p==v.segpos)
                        return a.key();
                }
                 return -1;
            }
        }
        internal void Add(Ident n)
        {
            var m = (int)_list.Count;
            Ident.IdTree<int>.Add(ref _names, n, m);
            ATree<int,Ident>.Add(ref _list,m,n);
            if (n.Defpos() > 0)
                Add(n, m);
        }
        internal int Length
        {
            get { return (int)_list.Count;  }
        }
        internal int ForPos(int dx,long cp)
        {
            int iq = -1;
            if (_bypos.Contains(cp))
            {
                iq = _bypos[cp];
                return iq;
            }
            for (int i = 0; i < Length; i++)
                if (_list[i].Defpos() == cp)
                    return i;
            return -1;
        }
        internal void Replace(int i,Ident n)
        {
            Ident.IdTree<int>.Remove(ref _names, _list[i]);
            Ident.IdTree<int>.Add(ref _names, n, i);
            ATree<int, Ident>.Add(ref _list, i, n);
            if (n.segpos > 0)
                Add(n, i);
        }
        internal void Clean()
        {
            for (int i = 0; i < Length; i++)
                _list[i].Clean();
        }

        internal bool Match(Idents n)
        {
            if (n?.Length != Length)
                return false;
            for (var i = 0; i < Length; i++)
                if (n[_list[i]] != i)
                    return false;
            return true;
        }
        internal Idents ForTableType()
        {
            for (var i = 0; i < Length; i++)
                if (this[i].sub != null)
                    goto doit;
            return this;
            doit:
            var r = new Idents();
            for (var i = 0; i < Length; i++)
                r.Add(this[i].ForTableType());
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("(");
            var cm = "";
            for (var i=0; i<Length;i++)
            {
                sb.Append(cm); cm = ",";
                sb.Append(_list[i]);
            }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// Security labels.
    /// Access rules: clearance C can access classification z if C.maxlevel>=z and 
    ///  can update if classification matches C.minlevel.
    /// Clearance allows minlevel LEQ laxlevel.
    /// For classification minlevel==maxlevel always.
    /// In addition clearance must have all the references of the classification 
    /// and at least one of the groups.
    /// The database uses a cache of level descriptors called levels.
    /// </summary>
    public class Level : IComparable
    {
        public byte minLevel = 0, maxLevel = 0; // D=0, C=1, B=3, A=3
        public ATree<string, bool> groups = BTree<string, bool>.Empty;
        public ATree<string, bool> references = BTree<string, bool>.Empty;
        public static Level D = new Level();
        Level() { }
        public Level(byte min, byte max, ATree<string, bool> g, ATree<string, bool> r)
        {
            minLevel = min; maxLevel = max; groups = g; references = r;
        }
        public bool ClearanceAllows(Level classification)
        {
            if (maxLevel < classification.minLevel)
                return false;
            for (var b = classification.references.First(); b != null; b = b.Next())
                if (!references.Contains(b.key()))
                    return false;
            if (classification.groups.Count == 0)
                return true;
            for (var b = groups.First(); b != null; b = b.Next())
                if (classification.groups.Contains(b.key()))
                    return true;
            return false;
        }
        public Level ForInsert(Level classification)
        {
            if (minLevel == 0)
                return this;
            var gps = BTree<string, bool>.Empty;
            for (var b = groups.First(); b != null; b = b.Next())
                if (classification.groups.Contains(b.key()))
                    ATree<string,bool>.Add(ref gps,b.key(),true);
            return new Level(minLevel, minLevel, gps, classification.references);
        }
        public override bool Equals(object obj)
        {
            var that = obj as Level;
            if (that == null || minLevel != that.minLevel || maxLevel != that.maxLevel
                || groups.Count != that.groups.Count || references.Count != that.references.Count)
                return false;
            for (var b = references.First(); b != null; b = b.Next())
                if (!that.references.Contains(b.key()))
                    return false;
            for (var b = groups.First(); b != null; b = b.Next())
                if (!that.groups.Contains(b.key()))
                    return false;
            return true;
        }
        public override int GetHashCode()
        {
            return (int) (minLevel + maxLevel + groups.Count + references.Count);
        }
        char For(byte b)
        {
            return (char)('D' - b);
        }
        public int CompareTo(object obj)
        {
            Level that = obj as Level;
            if (that == null)
                return 1;
            int c = minLevel.CompareTo(that.minLevel);
            if (c != 0)
                return c;
            c = maxLevel.CompareTo(that.maxLevel);
            if (c != 0)
                return c;
            var tb = that.groups.First();
            var b = groups.First();
            for (; c == 0 && b != null && tb != null; b = b.Next(), tb = tb.Next())
                c = b.key().CompareTo(tb.key());
            if (c != 0)
                return c;
            if (b != null)
                return 1;
            if (tb != null)
                return -1;
            tb = that.references.First();
            b = references.First();
            for (; c == 0 && b != null && tb != null; b = b.Next(), tb = tb.Next())
                c = b.key().CompareTo(tb.key());
            if (c != 0)
                return c;
            if (b != null)
                return 1;
            if (tb != null)
                return -1;
            return 0;
        }
        internal void Serialise(Writer wr)
        {
            wr.WriteByte(minLevel);
            wr.WriteByte(maxLevel);
            wr.PutInt((int)groups.Count);
            for (var b = groups.First(); b != null; b = b.Next())
                wr.PutString(b.key());
            wr.PutInt((int)references.Count);
            for (var b = references.First(); b != null; b = b.Next())
                wr.PutString(b.key());
        }
        internal static Level Deserialise(Reader rdr)
        {
            var min = (byte)rdr.ReadByte();
            var max = (byte)rdr.ReadByte();
            var gps = BTree<string, bool>.Empty;
            var n = rdr.GetInt();
            for (var i = 0; i < n; i++)
                ATree<string, bool>.Add(ref gps, rdr.GetString(), true);
            var rfs = BTree<string, bool>.Empty;
            n = rdr.GetInt();
            for (var i = 0; i < n; i++)
                ATree<string, bool>.Add(ref rfs, rdr.GetString(), true);
            return new Level(min, max, gps, rfs);
        }
        void Append(StringBuilder sb, ATree<string, bool> t, char s, char e)
        {
            var cm = "";
            sb.Append(s);
            for (var b = t.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(b.key());
            }
            sb.Append(e);
        }
        public void Append(StringBuilder sb)
        {
            if (maxLevel == 0 && groups.Count == 0 && references.Count == 0)
                return;
            sb.Append(' ');
            sb.Append(For(minLevel));
            if (maxLevel != minLevel)
            {
                sb.Append('-'); sb.Append(For(maxLevel));
            }
            if (groups.Count != 0)
                Append(sb, groups, '{', '}');
            if (references.Count != 0)
                Append(sb, references, '[', ']');
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            Append(sb);
            return sb.ToString();
        }

    }
    /// <summary>
    /// This corresponds to the TYPE element of a column or domain definition and is Role specific.
    /// Types can have parent types. There are no longer any subclasses of SqlDataType.
    /// For example, if this type is a user-defined type, then name is the type name.
    /// Standard types are public and shareable.
    /// </summary>
	internal class SqlDataType : IComparable
	{
        internal readonly Sqlx kind;
        internal readonly long owner; // a role
        internal readonly ATree<int, long> columns;
        internal readonly ATree<string, long> names;
        internal int Length => (int)columns.Count;
        /// <summary>
        /// The supertpe if any
        /// </summary>
        internal readonly long super;
        /// <summary>
        /// NotNull for a data type is like a constraint.
        /// Always true if a default value is supplied.
        /// </summary>
        internal readonly bool NotNull;
        /// <summary>
        /// Numeric types have prec and scale attributes
        /// </summary>
        internal readonly int prec, scale;
        /// <summary>
        /// AscDesc and Nulls control default ordering behaviour.
        /// </summary>
        internal readonly Sqlx AscDesc=Sqlx.NULL, Nulls=Sqlx.NULL;
        /// <summary>
        /// Some attributes for date, timespan, interval etc types
        /// </summary>
        internal readonly Sqlx start=Sqlx.NULL, end=Sqlx.NULL;
        /// <summary>
        /// The character-set attribute for a string
        /// </summary>
        internal readonly CharSet charSet;
        /// <summary>
        /// The culture for a localised string
        /// </summary>
        internal readonly CultureInfo culture = CultureInfo.InvariantCulture;
        /// <summary>
        /// element type for SENSITIVE, PERIOD, ARRAY, MULTISET etc
        /// </summary>
        internal readonly long elType;
        /// <summary>
        /// A default value for the data type
        /// </summary>
        internal readonly string defaultValue = "";
        /// <summary>
        /// An abbreviation for an OWL type (e.g. W for watts)
        /// </summary>
        internal readonly string abbrev;
        /// <summary>
        /// A constraint on the value (used for predefined types)
        /// </summary>
        internal readonly string search;
        /// <summary>
        /// The Ordering function if defined
        /// </summary>
        internal readonly Procedure orderfunc = null;
        internal readonly PColumn.GenerationRule generated = PColumn.GenerationRule.No;
        /// <summary>
        /// The ordering flags
        /// </summary>
        internal readonly OrderCategory orderflags = OrderCategory.None;
        internal readonly ATree<string, long> metadata = BTree<string, long>.Empty;
        internal readonly Grant.Privilege priv;
        protected SqlDataType(SqlDataType dt)
        {
            kind = dt.kind;
            owner = dt.owner;
            columns = dt.columns;
            names = dt.names;
            NotNull = dt.NotNull;
            super = dt.super;
            prec = dt.prec;
            scale = dt.scale;
            AscDesc = dt.AscDesc;
            Nulls = dt.Nulls;
            start = dt.start;
            end = dt.end;
            charSet = dt.charSet;
            culture = dt.culture;
            elType = dt.elType;
            defaultValue = dt.defaultValue;
            search = dt.search;
            orderfunc = dt.orderfunc;
            generated = dt.generated;
            orderflags = dt.orderflags;
            metadata = dt.metadata;
            priv = dt.priv;
        }
        public SqlDataType(Sqlx t,long ow,ATree<int,long> ss,ATree<string,long> ns,
            long su,bool nn,int pc,int sc,Sqlx ad,Sqlx nu,Sqlx st,Sqlx en,
            CharSet cs, CultureInfo ci, long et, string dv, string se, Procedure of, PColumn.GenerationRule ge,
            OrderCategory fl, ATree<string,long> md,Grant.Privilege pr)
        {
            kind = t; owner = ow; columns=ss; names = ns;  super = su;
            NotNull = nn;  prec = pc; scale = sc; AscDesc = ad; Nulls = nu;
            start = st; end = en; charSet = cs; culture = ci; elType = et; defaultValue = dv; search = se;
            orderfunc = of; generated = ge;  orderflags = fl; metadata = md; priv = pr;
        }
        public SqlDataType(SqlDataType dt,Grant.Privilege pr)
        {
            kind = dt.kind;
            owner = dt.owner;
            columns = dt.columns;
            names = dt.names;
            super = dt.super;
            prec = dt.prec;
            scale = dt.scale;
            AscDesc = dt.AscDesc;
            Nulls = dt.Nulls;
            start = dt.start;
            end = dt.end;
            charSet = dt.charSet;
            culture = dt.culture;
            elType = dt.elType;
            defaultValue = dt.defaultValue;
            search = dt.search;
            orderfunc = dt.orderfunc;
            generated = dt.generated;
            orderflags = dt.orderflags;
            metadata = dt.metadata;
            priv = pr;
        }
        /// <summary>
        /// Constructor: A simple type
        /// </summary>
        /// <param name="t">Sqlx token describing the type</param>
		protected SqlDataType(Sqlx t)
		{
            kind = t;
            owner = -1;
            columns = BTree<int, long>.Empty;
            names = BTree<string, long>.Empty;
            NotNull = false;
            prec = 0;
            scale = 0;
            AscDesc = Sqlx.NULL;
            Nulls = Sqlx.NULL;
            charSet = CharSet.UCS;
            start = Sqlx.NULL;
            end = Sqlx.NULL;
            culture = CultureInfo.InvariantCulture;
            defaultValue = "";
            abbrev = null;
            search = null;
		}
        /// <summary>
        /// Constructor: A localised string type (for StringCollation values)
        /// </summary>
        /// <param name="t">usually Sqlx.CHAR</param>
        /// <param name="p">the data length in unicode characters</param>
        /// <param name="cs">the charset</param>
        /// <param name="ci">the culture information</param>
        public SqlDataType(Sqlx t,int p,CharSet cs,CultureInfo ci) : this(t)
        {
            prec = p;
            charSet = cs;
            culture = ci;
        }
        /// <summary>
        /// Constructor: A numeric type
        /// </summary>
        /// <param name="t">The basic data type</param>
        /// <param name="p">the precision (number of digits)</param>
        /// <param name="s">the scale factor (number of Row to shift the decimal point)</param>
        public SqlDataType(Sqlx t, int p, int s)
            : this(t)
        {
            prec = p;
            scale = s;
        }

        /// <summary>
        /// A date/timespan/interval type
        /// </summary>
        /// <param name="t">The basic type</param>
        /// <param name="st">The starting unit e.g. YEAR...</param>
        /// <param name="en">The end unit e.g. MONTH...</param>
        /// <param name="p">The precision for the seconds if used</param>
        /// <param name="s">The scale for seconds if used</param>
        public SqlDataType(Sqlx t, Sqlx st, Sqlx en, int p, int s)
            : this(t,p,s)
        {
            start = st;
            end = en;
            // Intervals must be either YEAR-MONTH or DAY-SECOND but not both
            if (en != Sqlx.NULL && IntervalPart(st) < 2 && IntervalPart(en) > 1)
                throw new DBException("22006");
        }
        /// <summary>
        /// Constructor: A collection or other derived type
        /// </summary>
        /// <param name="t">The collection type</param>
        /// <param name="e">The item type</param>
        public SqlDataType(Sqlx t, long e)
            : this(t)
        {
            switch (t)
            {
                case Sqlx.SENSITIVE:
                case Sqlx.ARRAY:
                case Sqlx.PERIOD:
                case Sqlx.MULTISET: elType = e; break;
            }
        }
        /// <summary>
        /// Constructor: An OWL data type with a constraint
        /// </summary>
        /// <param name="p">The type being added to</param>
        /// <param name="u">The OWL uri</param>
        /// <param name="sc">A check condition</param>
        protected SqlDataType(SqlDataType p, string sc)
            : this(p)
        {
            search = sc;
        }
        /// <summary>
        /// Constructor: an ordering for a type
        /// </summary>
        /// <param name="p">The type</param>
        /// <param name="a">ASC or DESC</param>
        /// <param name="n">Whether nulls are first</param>
        protected SqlDataType(SqlDataType p, Sqlx a, Sqlx n)
            : this(p)
        {
            AscDesc = a;
            Nulls = n;
        }
        /// <summary>
        /// Constructor: a type with a default value or not null requirement
        /// </summary>
        /// <param name="p">The basic type</param>
        /// <param name="nn">The not null condition</param>
        /// <param name="dv">The default value</param>
        protected SqlDataType(SqlDataType p, bool nn, string dv) 
            : this(p)
        {
            NotNull = nn || dv!="";
            defaultValue = dv;
        }
        /// <summary>
        /// Constructor: a type with a check constraint
        /// </summary>
        /// <param name="p">The data type</param>
        /// <param name="c">The check constraint (all we retain is the predicate)</param>
        protected SqlDataType(SqlDataType p, PCheck c) :this(p)
        {
            search = c.check;
        }
        internal static SqlDataType New(Sqlx t)
        {
            switch (t)
            {
                case Sqlx.ROW:
                case Sqlx.TYPE:
                    return new RowType(t);
                case Sqlx.UNION:
                    return new UnionType();
                case Sqlx.ONLY:
                    return new OnlyType();
                default:
                    return new SqlDataType(t);
            }
        }
        protected virtual void Check()
        {
            switch(kind)
            {
                case Sqlx.ROW:
                case Sqlx.TYPE:
                case Sqlx.UNION:
                case Sqlx.ONLY:
                    throw new PEException("PE500");
            }
        }
        internal static Sqlx Equivalent(Sqlx t)
        {
            switch (t)
            {
                case Sqlx.NCHAR:
                case Sqlx.CLOB:
                case Sqlx.NCLOB:
                case Sqlx.VARCHAR: return Sqlx.CHAR;
                case Sqlx.INT:
                case Sqlx.BIGINT:
                case Sqlx.SMALLINT: return Sqlx.INTEGER;
                case Sqlx.DECIMAL:
                case Sqlx.DEC: return Sqlx.NUMERIC;
                case Sqlx.DOUBLE:
                case Sqlx.FLOAT: return Sqlx.REAL;
        //        case Sqlx.TABLE: return Sqlx.ROW; not equivalent!
                default:
                    return t;
            }
        }
        internal bool IsDate
        { 
            get {
                return kind == Sqlx.DATE || kind == Sqlx.TIME || kind == Sqlx.TIMESTAMP;
            }
        }
        internal TypedValue New(Transaction tr)
        {
            if (defaultValue != "")
            {
                var df = new Parser(tr).ParseSqlValue(defaultValue, this);
                return df.Eval(tr,null);
            }
            switch (Equivalent(kind))
            {
                case Sqlx.Null: return TNull.Value;
                case Sqlx.INTEGER: return new TInt(this, null);
                case Sqlx.NUMERIC: return new TNumeric(this, null);
                case Sqlx.REAL: return new TReal(this, double.NaN);
                case Sqlx.PASSWORD: 
                case Sqlx.NCHAR:
                case Sqlx.CHAR: return new TChar(this, null);
                case Sqlx.BOOLEAN: return TBool. Null;
                case Sqlx.TIMESTAMP:
                case Sqlx.DATE: return new TDateTime(this, null);
                case Sqlx.TIME: return new TTimeSpan(this, null);
                case Sqlx.INTERVAL: return new TInterval(this, null);
                case Sqlx.MULTISET: return new TMultiset(this);
                case Sqlx.DOCUMENT: return new TDocument();
                case Sqlx.DOCARRAY: return new TDocArray();
#if MONGO
                case Sqlx.OBJECT: return new TObjectId(cx);
#endif
                case Sqlx.BLOB: return new TBlob(new byte[0]);
                case Sqlx.PERIOD:
                    var et = tr.dataType(elType);
                    return new TPeriod(this,new Period(et.New(tr),et.New(tr)));
                case Sqlx.ARRAY:
                    return new TArray(this, 0);
                case Sqlx.REF:
                case Sqlx.ROW:
                case Sqlx.TYPE:
                    return new TRow(tr,this);
                case Sqlx.LEVEL:
                    return TLevel.D;
                case Sqlx.TABLE:
                    {
                        var q = new Query(tr, "T", this as RowType);
                        var ns = new string[(int)columns.Count];
                        for (var b = names.First(); b != null; b = b.Next())
                            ns[b.value()] = b.key();
                        for (var i = 0; i < columns.Count; i++)
                            new SqlTypeColumn(tr,tr.dataType(columns[i]), new Ident(ns[i],0),true,false,null);
                        q.rowSet = new EmptyRowSet(q);
                        return new TRowSet(q.rowSet);
                    }
                case Sqlx.UNION: return new TUnion(this,null);
                case Sqlx.VALUE:
                case Sqlx.CONTENT: return TNull.Value;
                case Sqlx.CURSOR: return null;
                case Sqlx.SENSITIVE: return new TSensitive(this,tr.dataType(elType).New(tr));
            }
            throw new NotImplementedException();
        }
        internal TypedValue First(Transaction tr)
        {
            if (defaultValue != "")
            {
                var df = new Parser(tr).ParseSqlValue(defaultValue, this);
                return df.Eval(tr,null);
            }
            switch (Equivalent(kind))
            {
                case Sqlx.SENSITIVE: return new TSensitive(this,tr.dataType(elType).New(tr));
                case Sqlx.INTEGER: return new TInt(this, 1);
                case Sqlx.NUMERIC: return new TNumeric(this, new Numeric(1));
                case Sqlx.REAL: return new TReal(this, 1.0);
                case Sqlx.NCHAR:
                case Sqlx.CHAR: return new TChar(this, "A");
                case Sqlx.TIMESTAMP:
                case Sqlx.DATE: return new TDateTime(this, DateTime.Now);
#if MONGO
                case Sqlx.OBJECT: return new TObjectId(cx);
#endif
            }
            throw new DBException("22204",kind.ToString()).ISO();
        }
        internal TypedValue New<K,V>(Transaction tr,CTree<K,V> t) where K:TypedValue
        {
            switch (Equivalent(kind))
            {
#if MONGO
                case Sqlx.CHAR:
                    {
                        if (prec == 0)
                            return new TChar(this, new TObjectId(cx).ToString());
                        var rnd = new Random();
                        for (int i = 0; i < 10000; i++)
                        {
                            var s = new TChar(this, RanString(rnd,prec)) as K;
                            if (t==null || !t.Contains(s))
                                return s;
                        }
                        throw new DBException("22209");
                    }
#endif
                case Sqlx.TIMESTAMP:
                case Sqlx.DATE:
                    {
                        var d = new TDateTime(this, DateTime.Now) as K;
                        if (t == null || !t.Contains(d))
                            return d;
                        throw new DBException("22207").ISO();
                    }
#if MONGO
                case Sqlx.OBJECT: return new TObjectId(cx);
#endif
            }
            if (t==null || t.Count==0)
                return First(tr as Transaction);
            return t.Last().key().Next();
        }
#if MONGO
        string RanString(Random r,int n)
        {
            var c = new char[n];
            for(int i=0;i<n;i++)
            {
                var k = r.Next(60);
                char m ='A';
                if (k < 26)
                    m = (char)('A' + k);
                else if (k < 52)
                    m = (char)('a' + k - 26);
                else
                    m = (char)('0' + k - 52);
                c[i] = m;
            }
            return new string(c);
        }
#endif
        internal virtual SqlDataType Copy()
        {
            return new SqlDataType(this);
        }
        internal virtual SqlDataType Copy(bool nn,string dv)
        {
            return new SqlDataType(this, nn, dv);
        }
        internal virtual SqlDataType Copy(PCheck c)
        {
            return new SqlDataType(this, c);
        }
        internal virtual SqlDataType Copy(Sqlx a,Sqlx n)
        {
            return new SqlDataType(this, a, n);
        }
        internal virtual SqlDataType Copy(string sc)
        {
            return new SqlDataType(this, sc);
        }
        internal TypedValue Copy(Transaction tr,TypedValue v)
        {
            if (v == null || v.IsNull)
                return New(tr);
            switch (Equivalent(kind))
            {
                case Sqlx.CONTENT:
                case Sqlx.VALUE:
                case Sqlx.Null:
                    return v;
                case Sqlx.ARRAY:
                    if (v is TArray)
                        return new TArray(this,((TArray)v).list);
                    break;
                case Sqlx.INTEGER: 
                    if (v is TInt)
                        return new TInt(this, ((TInt)v).value);
                    if (v is TInteger)
                        return new TInt(this,(long)((TInteger)v).ivalue);
                    break;
                case Sqlx.NUMERIC: 
                    if (v is TNumeric)
                        return new TNumeric(this, ((TNumeric)v).value);
                    if (v is TInt)
                        return new TNumeric(this, new Numeric(((TInt)v).value.Value));
                    break;
                case Sqlx.REAL:
                    if (v is TReal)
                    {
                        var r = v as TReal;
                        if (((object)r.nvalue)==null)
                            return new TReal(this, r.dvalue);
                        return new TReal(this,r.nvalue);
                    }
                    if (v is TNumeric)
                    return new TReal(this, ((TNumeric)v).value);
                    if (v is TInt)
                        return new TReal(this, (((TInt)v).value.Value));
                    break;
                case Sqlx.NCHAR:
                case Sqlx.CHAR: 
                    if (v is TChar)
                        return new TChar(this, ((TChar)v).value);
                    break;
                case Sqlx.BOOLEAN: 
                    if (v is TBool)
                        return v;
                    break;
                case Sqlx.DATE:
                case Sqlx.TIMESTAMP:
                    if (v is TDateTime)
                        return new TDateTime(this, ((TDateTime)v).value.Value);
                    if (v is TInt)
                    {
                        var w = v.ToLong();
                        if (w > DateTime.MaxValue.Ticks)
                            w = DateTime.MaxValue.Ticks;
                        return new TDateTime(this,new DateTime(w.Value));
                    }
                    break;
                case Sqlx.TIME:
                    if (v is TTimeSpan)
                        return new TTimeSpan(this,((TTimeSpan)v).value);
                    break;
                case Sqlx.INTERVAL:
                    if (v is TInterval)
                        return new TInterval(this, ((TInterval)v).value);
                    break;
                case Sqlx.MULTISET: 
                    if (v is TMultiset)
                        return new TMultiset(this, (CTree<TypedValue,long?>)(((TMultiset)v).tree));
                    break;
                case Sqlx.DOCUMENT: 
                    if (v is TDocument)
                        return v;
                    break;
                case Sqlx.DOCARRAY: 
                    if (v is TDocArray)
                    return v;
                    break;
#if MONGO
                case Sqlx.OBJECT: 
                    if (v is TObjectId)
                     return new TObjectId(this,cx,(TObjectId)v);
                    break;
#endif
                case Sqlx.PERIOD:
                    if (v is TPeriod)
                        return new TPeriod(this, ((TPeriod)v).value);
                    break;
                case Sqlx.TYPE:
                    if (v is TRow)
                        return new TRow(tr, this,(TRow)v);
                    break;
                case Sqlx.ROW:
                    if (v is TRow)
                        return new TRow(tr, this, (TRow)v);
                    break;
                case Sqlx.TABLE:
                    if (v is TRowSet)
                        return v;
                    break;
                case Sqlx.UNION:
                    return TypeOf(tr,v).Copy(tr,v);
            }
            throw new DBException("22005D",this.ToString(),v.dataType.ToString()).ISO();
        }
        internal byte BsonType()
        {
            switch (Equivalent(kind))
            {
                case Sqlx.Null: return 10;
                case Sqlx.REAL: return 1;
                case Sqlx.CHAR: return 2;
                case Sqlx.DOCUMENT: return 3;
                case Sqlx.DOCARRAY: return 4;
                case Sqlx.BLOB: return 5;
                default: return 6;
                case Sqlx.OBJECT: return 7;
                case Sqlx.BOOLEAN: return 8;
                case Sqlx.TIMESTAMP: return 9;
                case Sqlx.NULL: return 10;
#if MONGO
                case Sqlx.REGULAR_EXPRESSION: return 11;
#endif
                case Sqlx.ROUTINE: return 13;
                case Sqlx.NUMERIC: return 19; // Decimal subtype added for Pyrrho
                case Sqlx.INTEGER: return 16;
            }
        }
        internal long DomainDefPos(Transaction tr,long dom)
        {
            if (name != null)
            {
                var rp = db._Role.names[name];
                if (rp!=null && rp.HasValue)
                    return rp.Value;
            }
            if (kind==Sqlx.Null || (kind!=Sqlx.TABLE && kind!=Sqlx.REF && columns != null && dom!= -2)) // apart from function returns we aren't going to worry about anonymous row types at all
            {
                if (dom>0)
                    return dom;
                return Content.DomainDefPos(cnx,db, -1);
            }
            if (db.pb.types.Contains(this))
                return db.pb.types[this].Value;
            if (name == null)
                name = new Ident(ToString(),0);
            var pd = new PDomain(name, this, db);
            db.Add(cnx,pd);
            return pd.defpos;
        }
        internal long DomainDefPos(PhysBase pb, Reloc r)
        {
            if (name != null)
                return r.namedTypes[name].Value;
            return pb.types[this].Value;
        }
        internal virtual SqlDataType NotUnion(Sqlx k)
        {
            return this;
        }
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
        internal readonly static SqlDataType
            Value = new SqlDataType(Sqlx.VALUE), // Not known whether scalar or row type
            Content = new StandardDataType(Sqlx.CONTENT), // Pyrrho 5.1 default type for Document entries, from 6.2 for generic scalar value
            Bool = new StandardDataType(Sqlx.BOOLEAN),
            Blob = new StandardDataType(Sqlx.BLOB),
            Char = new StandardDataType(Sqlx.CHAR),
            Password = new StandardDataType(Sqlx.PASSWORD),
            XML = new StandardDataType(Sqlx.XML),
            Int = new StandardDataType(Sqlx.INTEGER),
            Numeric = new StandardDataType(Sqlx.NUMERIC),
            Real = new StandardDataType(Sqlx.REAL),
            Date = new StandardDataType(Sqlx.DATE),
            Timespan = new StandardDataType(Sqlx.TIME),
            Timestamp = new StandardDataType(Sqlx.TIMESTAMP),
            Interval = new StandardDataType(Sqlx.INTERVAL),
            Null = new StandardDataType(Sqlx.Null),
            TypeSpec = new StandardDataType(Sqlx.TYPE),
            Level = new StandardDataType(Sqlx.LEVEL),
            Physical = new StandardDataType(Sqlx.DATA),
            MTree = new SqlDataType(Sqlx.M), // pseudo type for MTree implementation
            Partial = new SqlDataType(Sqlx.T), // pseudo type for MTree implementation
            Array = new SqlDataType(Sqlx.ARRAY, Content),
            Multiset = new SqlDataType(Sqlx.MULTISET, Content),
            Collection = new UnionType(new SqlDataType[] { Array, Multiset }),
            Cursor = new SqlDataType(Sqlx.CURSOR),
            UnionNumeric = new UnionType(new SqlDataType[] { Int, Numeric, Real }),
            UnionDate = new UnionType(new SqlDataType[] { Date, Timespan, Timestamp, Interval }),
            UnionDateNumeric = new UnionType(new SqlDataType[] { Date, Timespan, Timestamp, Interval, Int, Numeric, Real }),
            Exception = new SqlDataType(Sqlx.HANDLER),
            Period = new SqlDataType(Sqlx.PERIOD),
            RdfString = Add(new SqlDataType(Char, IriRef.STRING)),
            RdfBool = Add(new SqlDataType(Bool, IriRef.BOOL)),
            RdfInteger = Add(new SqlDataType(Int, IriRef.INTEGER)),
            RdfInt = Add(new SqlDataType(Int, IriRef.INT, "value>=-2147483648 and value<=2147483647")),
            RdfLong = Add(new SqlDataType(Int, IriRef.LONG, "value>=-9223372036854775808 and value<=9223372036854775807")),
            RdfShort = Add(new SqlDataType(Int, IriRef.SHORT, "value>=-32768 and value<=32768")),
            RdfByte = Add(new SqlDataType(Int, IriRef.BYTE, "value>=-128 and value<=127")),
            RdfUnsignedInt = Add(new SqlDataType(Int, IriRef.UNSIGNEDINT, "value>=0 and value<=4294967295")),
            RdfUnsignedLong = Add(new SqlDataType(Int, IriRef.UNSIGNEDLONG, "value>=0 and value<=18446744073709551615")),
            RdfUnsignedShort = Add(new SqlDataType(Int, IriRef.UNSIGNEDSHORT, "value>=0 and value<=65535")),
            RdfUnsignedByte = Add(new SqlDataType(Int, IriRef.UNSIGNEDBYTE, "value>=0 and value<=255")),
            RdfNonPositiveInteger = Add(new SqlDataType(Int, IriRef.NONPOSITIVEINTEGER, "value<=0")),
            RdfNonNegativeInteger = Add(new SqlDataType(Int, IriRef.NEGATIVEINTEGER, "value<0")),
            RdfPositiveInteger = Add(new SqlDataType(Int, IriRef.POSITIVEINTEGER, "value>0")),
            RdfNegativeInteger = Add(new SqlDataType(Int, IriRef.NONNEGATIVEINTEGER, "value>=0")),
            RdfDecimal = Add(new SqlDataType(Numeric, IriRef.DECIMAL)),
            RdfDouble = Add(new SqlDataType(Real, IriRef.DOUBLE)),
            RdfFloat = Add(new SqlDataType(new SqlDataType(Sqlx.REAL, 6, 0), IriRef.FLOAT)),
            RdfDate = Add(new SqlDataType(Date, IriRef.DATE)),
            RdfDateTime = Add(new SqlDataType(Timestamp, IriRef.DATETIME)),
            Iri = Add(new SqlDataType(Char)),
#if MONGO || OLAP || SIMILAR
            Regex = new SqlDataType(Sqlx.REGULAR_EXPRESSION),
#endif
            Document = new StandardDataType(Sqlx.DOCUMENT), // Pyrrho 5.1
            DocArray = new StandardDataType(Sqlx.DOCARRAY), // Pyrrho 5.1
            ObjectId = new StandardDataType(Sqlx.OBJECT), // Pyrrho 5.1
            JavaScript = new StandardDataType(Sqlx.ROUTINE), // Pyrrho 5.1
            ArgList = new StandardDataType(Sqlx.CALL), // Pyrrho 5.1
            Table = new TableType(Sqlx.TABLE),
            Row = new RowType(),
            Delta = new SqlDataType(Sqlx.INCREMENT);
        /// <summary>
        /// Helper to construct the list of standard types
        /// </summary>
        /// <param name="t">A type to add</param>
        /// <returns>a copy of the type parameter</returns>
        static SqlDataType Add(SqlDataType t)
        {
            ATree<string,SqlDataType>.AddNN(ref Context.DefaultTypes,t.iri,t);
            return t;
        }
        internal TypedValue Now
        {
            get
            {
                return new TDateTime(Timestamp,DateTime.Now);
            }
        }
        internal   TypedValue MaxDate
        {
            get 
            {
                return new TDateTime(Timestamp, DateTime.MaxValue);
            }
        }
        internal string StringFor(Database db = null)
        {
            if (name != null)
                return name.ToString();
            var ro = db?._Role;
            StringBuilder sb = new StringBuilder();
            switch (Equivalent(kind))
            {
                default:
                    sb.Append(kind);
                    if (elType != null)
                        sb.Append(" " + elType.ToString());
                    if (AscDesc != Sqlx.NULL)
                        sb.Append(" " + AscDesc);
                    if (Nulls != Sqlx.NULL)
                        sb.Append(" " + Nulls);
                    if (start != Sqlx.NULL)
                    {
                        sb.Append(" " + start);
                        if (end != Sqlx.NULL)
                            sb.Append(" TO " + end);
                    }
                    if (prec != 0)
                    {
                        sb.Append("(" + prec);
                        if (scale != 0)
                            sb.Append("," + scale);
                        sb.Append(")");
                    }
                    if (charSet != CharSet.UCS)
                        sb.Append(" " + charSet);
                    if (CultureName != "")
                        sb.Append(" " + CultureName);
                    if (defaultValue != "")
                        sb.Append(" DEFAULT " + defaultValue);
                    if (search != null)
                        sb.Append(" CHECK(" + search + ")");
                    if (abbrev != null)
                        sb.Append(" " + abbrev);
                    break;
                case Sqlx.ONLY:
                    sb.Append(kind);
                    sb.Append("!");
                    break;
                case Sqlx.REF:
                case Sqlx.UNION:
                case Sqlx.ROW:
                case Sqlx.TYPE:
                case Sqlx.TABLE:
                    sb.Append(kind);
                    if (columns != null)
                    {
                        sb.Append("(");
                        var s = "";
                        for (int i = 0; i < columns.Length; i++)
                        {
                            sb.Append(s); s = ",";
                            if (names.Length > i)
                            {
                                var n = names[i];
                                if (n.segpos != 0)
                                    sb.Append(ro?.defs[n.segpos]?.name.ident ?? PosFor(n));
                                else
                                    sb.Append(n.ident);
                                sb.Append(" ");
                            }
                            sb.Append(columns[i].ToString());
                        }
                        sb.Append(")");
                    }
                    break;
            }
            if (iri != null)
                sb.Append("^^" + iri);
            if (structdefpos > 0)
            {
                sb.Append("[");
                if (structdefpos < Transaction.TransPos)
                    sb.Append(structdefpos);
                else
                    sb.Append("'" + (structdefpos - Transaction.TransPos));
                sb.Append("]");
            }
            return sb.ToString();
        }
        /// <summary>
        /// a version of the above for _Strategy
        /// </summary>
        /// <param name="tr"></param>
        /// <returns></returns>
        public virtual string ToString1(Transaction tr)
        {
            var sb = new StringBuilder();
            switch (Equivalent(kind))
            {
                default:
                    sb.Append(kind);
                    if (elType != null)
                        sb.Append(" " + elType.ToString1(tr));
                    if (AscDesc != Sqlx.NULL)
                        sb.Append(" " + AscDesc);
                    if (Nulls != Sqlx.NULL)
                        sb.Append(" " + Nulls);
                    if (start != Sqlx.NULL)
                    {
                        sb.Append(" " + start);
                        if (end != Sqlx.NULL)
                            sb.Append(" TO " + end);
                    }
                    if (prec != 0)
                    {
                        sb.Append("(" + prec);
                        if (scale != 0)
                            sb.Append("," + scale);
                        sb.Append(")");
                    }
                    if (charSet != CharSet.UCS)
                        sb.Append(" " + charSet);
                    if (CultureName != "")
                        sb.Append(" " + CultureName);
                    if (defaultValue != "")
                        sb.Append(" DEFAULT " + defaultValue);
                    if (search != null)
                        sb.Append(" CHECK(" + search + ")");
                    if (abbrev != null)
                        sb.Append(" " + abbrev);
                    break;
                case Sqlx.ONLY:
                    sb.Append(kind);
                    sb.Append("!");
                    break;
                case Sqlx.REF:
                case Sqlx.UNION:
                case Sqlx.ROW:
                case Sqlx.TYPE:
                case Sqlx.TABLE:
                    sb.Append(kind);
                    if (columns != null)
                    {
                        var cm = '(';
                        for (int i = 0; i < columns.Length; i++)
                        {
                            sb.Append(cm); cm = ',';
                            if (names.Length > i)
                                names[i].ToString1(sb,tr,null);
                            sb.Append(' ');
                            sb.Append(columns[i].ToString1(tr));
                        }
                        sb.Append(")");
                    }
                    break;
            }
            if (iri != null)
                sb.Append("^^" + iri);
            if (structdefpos > 0)
            {
                sb.Append("[");
                if (structdefpos < Transaction.TransPos)
                    sb.Append(structdefpos);
                else
                    sb.Append("'" + (structdefpos - Transaction.TransPos));
                sb.Append("]");
            }
            return sb.ToString();
        }
        string PosFor(Ident n)
        {
            if (n.segpos >= Transaction.TransPos)
                return "'" + (n.segpos - Transaction.TransPos);
            return n.segpos.ToString();
        }
        /// <summary>
        /// Accessor: A readable representation of the type
        /// </summary>
        /// <returns>a readable string</returns>
		public override string ToString()
		{
            return StringFor();
		}
        /// <summary>
        /// Provide an XML version of the type information for the client
        /// </summary>
        /// <param name="transaction"></param>
        /// <returns>an XML string</returns>
        internal string Info()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("<PyrrhoDBType kind=\"" + kind + "\" ");
            if (iri != null)
                sb.Append("iri=\"" + iri + "\" ");
            bool empty = true;
            if (kind == Sqlx.ONLY)
                sb.Append(name);
            if (elType != null)
                sb.Append("elType=" + elType + "]");
            if (AscDesc != Sqlx.NULL)
                sb.Append("," + AscDesc);
            if (Nulls != Sqlx.NULL)
                sb.Append("," + Nulls);
            if (prec != 0)
                sb.Append(",P=" + prec);
            if (scale != 0)
                sb.Append(",S=" + scale);
            if (start != Sqlx.NULL)
                sb.Append(",T=" + start);
            if (end != Sqlx.NULL)
                sb.Append(",E=" + end);
            if (charSet != CharSet.UCS)
                sb.Append("," + charSet);
            if (culture != null)
                sb.Append("," + CultureName);
            if (defaultValue != "")
                sb.Append(",D=" + defaultValue);
            if (search != null)
                sb.Append(",C=" + search);
            if (abbrev != null)
                sb.Append(",A=" + abbrev);
            if (empty)
                sb.Append("/>");
            return sb.ToString();
        }
        /// <summary>
        /// Decide which type we want for a union type and an actual value
        /// </summary>
        /// <param name="v">the value</param>
        /// <returns>the type from the union appropriate for this value</returns>
        internal SqlDataType TypeOf(Transaction tr,TypedValue v)
        {
            if (kind == Sqlx.SENSITIVE)
                return new SqlDataType(Sqlx.SENSITIVE, tr.dataType(elType).TypeOf(tr, v));
            if (kind != Sqlx.UNION)
                return this;
            int n = (int)columns.Count;
            for (int j = 0; j < n; j++)
            {
                var r = tr.dataType(columns[j]);
                if (r.CanTakeValueOf(v.dataType))
                    return r;
            }
            return null;
        }
        /// <summary>
        /// Create an XML string for a given value
        /// </summary>
        /// <param name="tr">The transaction: Common doesn't know about this</param>
        /// <param name="ob">the value to represent</param>
        /// <returns>the corresponding XML</returns>
        public virtual string Xml(Context cx,Database d, long defpos, TypedValue ob)
        {
            if (ob == null)
                return "";
            StringBuilder sb = new StringBuilder();
            switch (kind)
            {
                default:
                    //          sb.Append("type=\"" + ob.DataType.ToString() + "\">");
                    sb.Append(ob.ToString());
                    break;
                case Sqlx.ARRAY:
                    {
                        var a = (TArray)ob;
                        //           sb.Append("type=\"array\">");
                        for (int j = 0; j < a.Length; j++)
                            sb.Append("<item " + elType.Xml(cx,d, elType.DomainDefPos(cx,d, -1), a[j]) + "</item>");
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        var m = (TMultiset)ob;
                        //          sb.Append("type=\"multiset\">");
                        
                        for (var e = m.tree.First();e!=null;e=e.Next())
                            sb.Append("<item " + elType.Xml(cx,d, elType.DomainDefPos(cx,d, -1), e.key()) + "</item>");
                        break;
                    }
                case Sqlx.ROW:
                case Sqlx.TABLE:
                case Sqlx.TYPE:
                    {
                        TRow r = (TRow)ob;
                        if (r.dataType.names.Length == 0)
                            throw new DBException("2200N").ISO();
                        RoleObject ro = null;
                        if (defpos > 0)
                            ro = d._Role.defs[defpos];
                        var sc = sb;
                        if (ro != null)
                            sb.Append("<" + ro.name);
                        var ss = new string[r.dataType.Length];
                        var empty = true;
                        for (int i = 0; i < r.dataType.Length; i++)
                        {
                            var tv = r[i];
                            if (tv == null)
                                continue;
                            var kn = r.dataType.names[i];
                            var p = tv.dataType;
                            Metadata m = null;
                            long? mp = 0;
                            if (ro != null)
                            {
                                mp = ro.props[kn];
                                if (mp.HasValue)
                                    m = d._Role.defs[mp.Value];
                                else
                                {
                                    mp = ro.subs[i];
                                    if (mp.HasValue)
                                        m = (d.GetS(mp.Value) as PMetadata).metadata;
                                }
                            }
                            if (tv!=null && !tv.IsNull && m != null && m.Has(Sqlx.ATTRIBUTE))
                                sb.Append(" " + kn + "=\"" + tv.ToString() + "\"");
  /*                          else if (tv!=null && !tv.IsNull && m != null && m.Has(Sqlx.REFERS))
                            {
                                var rc = m as RoleObject;
                                for (var xp = d.indexes.First();xp!= null; xp = xp.Next())
                                {
                                    var x = d.GetObject(xp.key()) as Index;
                                    if (x.tabledefpos != defpos)
                                        continue;
                                    if (x.cols.Length != 1 || x.cols[0] != mp.Value)
                                        continue;
                                    var rx = d.GetObject(x.refindexdefpos) as Index;
                                    var rt = d.GetObject(x.reftabledefpos) as Table;
                                    var fm = new From(d._Context, "X"+x.reftabledefpos+":"+i, rt);
                                    var irs = new IndexRowSet(d._Context.transaction,fm, rx, null);
                                    ss[i] = fm.nominalDataType.Xml(d, x.reftabledefpos, irs.First()?[i]);
                                    empty = false;
                                    break;
                                }
                            } 
                            else if (m != null && m.Has(Sqlx.REFERRED))
                            {
                                var rc = m as RoleObject;
                                for (var xp = d.indexes.First(); xp != null; xp = xp.Next())
                                {
                                    var x = d.GetObject(xp.key()) as Index;
                                    if (x.reftabledefpos != defpos)
                                        continue;
                                    if (x.cols.Length != 1 || d._Role.defs[x.cols[0]].name.ToString() != kn.ToString())
                                        continue;
                                    var rx = d.GetObject(x.refindexdefpos) as Index;
                                    var rk = d._Role.defs[rx.cols[0]].name;
                                    if (rk == null)
                                        throw new DBException("42102");
                                    var it = d.GetObject(x.tabledefpos) as Table;
                                    var fm = new From(d._Context, "X"+x.tabledefpos+":"+i, it);
                                    var irs = new IndexRowSet(d._Context.transaction,fm, x, null);
                                    fm.rowSet = irs;
                                    var sr = new StringBuilder();
                                    for (var ia = irs.First(); ia != null; ia = ia.Next())
                                        sr.Append(irs.rowType.Xml(d, x.tabledefpos, ia[i]));
                                    ss[i] = sr.ToString();
                                    empty = false;
                                    break;
                                }
                            } */
                            else if (tv!=null && !tv.IsNull)
                            {
                                ss[i] = "<" + kn + " type=\"" + p.ToString() + "\">" +
                                    p.Xml(cx,d, defpos, tv) + "</" + kn + ">";
                                empty = false;
                            }
                        }
                        if (ro != null)
                        {
                            if (empty)
                                sb.Append("/");
                            sb.Append(">");
                        }
                        for (int j = 0; j < ss.Length; j++)
                            if (ss[j] != null)
                                sb.Append(ss[j]);
                        if (ro != null && !empty)
                            sb.Append("</" + ro.name + ">");
                        break;
                    }
                case Sqlx.PASSWORD: sb.Append("*********"); break;
            }
            return sb.ToString();
        }
        /// <summary>
        /// Compare two values of this data type.
        /// (v5.1 allow the second to have type Document in all cases)
        /// </summary>
        /// <param name="a">the first value</param>
        /// <param name="b">the second value</param>
        /// <returns>-1,0,1 according as a LT,EQ,GT b</returns>
        public virtual int Compare(Transaction tr, TypedValue a, TypedValue b)
        {
            if (kind==Sqlx.SENSITIVE)
            {
                a = (a is TSensitive sa) ? sa.value : a;
                b = (b is TSensitive sb) ? sb.value : b;
                return elType.Compare(tr, a, b);
            }
            if (a==null || a.IsNull)
            {
                if (b==null || b.IsNull)
                    return 0;
                return (Nulls == Sqlx.FIRST) ? -1 : 1;
            }
            if (b==null || b.IsNull)
                return (Nulls == Sqlx.FIRST) ? 1 : -1;
            int c;
            if (orderflags!=OrderCategory.None)
            {
                var db = tr?.DbFor(this);
                var n = orderfunc.NameInSession(db);
                var sa = new SqlLiteral(tr, a);
                var sb = new SqlLiteral(tr, b);
                if ((orderflags & OrderCategory.Relative) == OrderCategory.Relative)
                    return orderfunc.Exec(tr,db.dbix,n,new SqlValue[] { sa,sb}).ToInt().Value;
                a = orderfunc.Exec(tr,db.dbix, n, new SqlValue[] { sa });
                b = orderfunc.Exec(tr,db.dbix, n, new SqlValue[] { sb });
                return a.dataType.Compare(cx, a, b);
            }
            switch (Equivalent(kind))
            {
                case Sqlx.BOOLEAN: return (a.ToBool().Value).CompareTo(b.ToBool().Value);
                case Sqlx.CHAR: 
#if WINDOWS_PHONE
                    c = string.Compare(a.ToString(), b.ToString());
#else
                    c = string.Compare(a.ToString(), b.ToString(), false, culture); 
#endif
                    break;
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.INTEGER:
                    if (a.Val(tr) is long)
                    {
                        if (b.Val(tr) is long)
                            c = a.ToLong().Value.CompareTo(b.ToLong().Value);
                        else
                            c = new Integer(a.ToLong().Value).CompareTo(b.Val(tr));
                    }
                    else if (b.Val(tr) is long)
                        c = ((Integer)a.Val(tr)).CompareTo(new Integer(b.ToLong().Value));
                    else
                        c = ((Integer)a.Val(tr)).CompareTo((Integer)b.Val(tr));
                    break;
                case Sqlx.NUMERIC: c = ((Numeric)a.Val(tr)).CompareTo(b.Val(tr)); break;
                case Sqlx.REAL:
                    var da = a.ToDouble();
                    var db = b.ToDouble();
                    if (da == null && db == null)
                        c=0;
                    else if (da == null)
                        c= -1;
                    else if (db == null)
                        c=1;
                    else
                        c = da.Value.CompareTo(db.Value); break;
                case Sqlx.DATE:
                    {
                        var oa = a.Val(tr);
                        if (oa is long)
                            oa = new DateTime((long)oa);
                        if (oa is DateTime)
                            oa = new Date((DateTime)oa);
                        var ob = b.Val(tr);
                        if (ob is long)
                            ob = new DateTime((long)ob);
                        if (ob is DateTime)
                            ob = new Date((DateTime)ob);
                        c = (((Date)oa).date).CompareTo(((Date)ob).date);
                        break;
                    }
                case Sqlx.DOCUMENT: 
                    {
                        var dcb = a as TDocument;
                        return dcb.Query(b);
                    }
                case Sqlx.CONTENT: c = a.ToString().CompareTo(b.ToString()); break;
                case Sqlx.TIME: c = ((TimeSpan)a.Val(tr)).CompareTo(b.Val(tr)); break;
                case Sqlx.TIMESTAMP:
                    c = ((DateTime)a.Val(tr)).CompareTo((DateTime)b.Val(tr));
                    break;
                case Sqlx.INTERVAL:
                    {
                        var ai = (Interval)a.Val(tr);
                        var bi = (Interval)b.Val(tr);
                        if (ai.yearmonth != bi.yearmonth)
                            throw new DBException("22202");
                        if (ai.yearmonth)
                        {
                            c = ai.years.CompareTo(bi.years);
                            if (c != 0)
                                break;
                            c = ai.months.CompareTo(bi.months);
                        } else
                            c = ai.ticks.CompareTo(bi.ticks);
                        break;
                    }
                case Sqlx.ARRAY:
                    {
                        var x = a as TArray;
                        var y = b as TArray;
                        if (x == null || y == null)
                            throw new DBException("22004").ISO();
                        if (x.dataType.elType != y.dataType.elType)
                            throw new DBException("22202").Mix()
                                .AddType(x.dataType.elType).AddValue(y.dataType);
                        int n = x.Length;
                        int m = y.Length;
                        if (n != m)
                            return (n < m) ? -1 : 1;
                        c = 0;
                        for (int j = 0; j < n; j++)
                        {
                            c = x[j].CompareTo(cx,y[j]);
                            if (c != 0)
                                break;
                        }
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        var x = a as TMultiset;
                        var y = b as TMultiset;
                        if (x == null || y == null)
                            throw new DBException("22004").ISO();
                        if (x.dataType.elType != y.dataType.elType)
                            throw new DBException("22202").Mix()
                                .AddType(x.dataType.elType).AddValue(y.dataType.elType);
                        var e = x.tree.First();
                        var f = y.tree.First();
                        for (; ; )
                        {
                            while (e != null && !e.value().HasValue)
                                e = e.Next();
                            while (f != null && !f.value().HasValue)
                                f = f.Next();
                            if (e==null || f==null)
                                break;
                            c = elType.Compare(cx,e.key(), f.key());
                            if (c != 0)
                                return c;
                            c = e.value().Value.CompareTo(f.value().Value);
                            if (c != 0)
                                return c;
                            break;
                        }
                        while (e != null && !e.value().HasValue)
                            e = e.Next();
                        while (f != null && !f.value().HasValue)
                            f = f.Next();
                        c = (e== null) ? ((f== null) ? 0 : -1) : 1;
                        break;
                    }
#if SIMILAR
                case Sqlx.REGULAR_EXPRESSION:
                    {
                        c = RegEx.PCREParse(a.ToString()).Like(b.ToString(), null) ? 0 : 1;
                        break;
                    }
#endif
                case Sqlx.ROW:
                    {
                        int n = Length;
                        TRow ra = a as TRow;
                        TRow rb = b as TRow;
                        if (ra == null || rb == null)
                            throw new DBException("22004").ISO();
                        if (ra.Length != rb.Length)
                            throw new DBException("22202").Mix()
                                .AddType(ra.dataType).AddValue(rb.dataType);
                        c = a.CompareTo(cx, b);
                        break;
                    }
                case Sqlx.PERIOD:
                    {
                        var pa = a.Val(tr) as Period;
                        var pb = b.Val(tr) as Period;
                        c = elType.Compare(cx, pa.start, pb.start);
                        if (c == 0)
                            c = elType.Compare(cx, pa.end, pb.end);
                        break;
                    }
                case Sqlx.UNION:
                    {
                        foreach (var dt in columns)
                            if (dt.CanTakeValueOf(a.ValueType) && dt.CanTakeValueOf(b.ValueType))
                            {
                                c = dt.Compare(cx, a, b);
                                goto skip;
                            }
                        throw new DBException("22202", a.dataType.ToString(), b.dataType.ToString());
                    }
                case Sqlx.PASSWORD:
                    throw new DBException("22202").ISO();
                default: c = a.ToString().CompareTo(b.ToString()); break;
            }
            skip: ;
            if (AscDesc == Sqlx.DESC)
                c = -c;
            return c;
        }
        
        /// <summary>
        /// Creator: Add the given array at the end of this
        /// </summary>
        /// <param name="a"></param>
        /// <returns></returns>
        public TArray Concatenate(Context cx, TArray a, TArray b)
        {
            var r = new TArray(this);
            if (a.dataType.elType != elType || a.dataType.elType != b.dataType.elType)
                throw new DBException("22102").Mix()
                    .AddType(a.dataType.elType).AddValue(b.dataType.elType);
            int j = 0;
            foreach (var e in a.list)
                r[j++] = e;
            foreach (var e in b.list)
                r[j++] = e;
            return r;
        }
         /// <summary>
        /// Test a given type to see if its values can be assigned to this type
        /// </summary>
        /// <param name="dt">The other data type</param>
        /// <returns>whether values of the given type can be assigned to a variable of this type</returns>
        public virtual bool CanTakeValueOf(SqlDataType dt)
        {
            if (dt.kind==Sqlx.SENSITIVE)
            {
                if (kind == Sqlx.SENSITIVE)
                    return elType.CanTakeValueOf(dt.elType);
                return false;
            }
            if (kind == Sqlx.SENSITIVE)
                return elType.CanTakeValueOf(dt);
            if (dt?.kind == Sqlx.ONLY)
                dt = dt.super;
            if (kind == Sqlx.VALUE || kind == Sqlx.CONTENT)
                return true;
            if (dt.kind==Sqlx.CONTENT || dt.kind == Sqlx.VALUE)
                return kind!=Sqlx.REAL && kind!=Sqlx.INTEGER && kind!=Sqlx.NUMERIC;
            if (dt.kind==Sqlx.UNION)
            {
                int n = dt.Length;
                for (int j = 0; j < n; j++)
                    if (CanTakeValueOf(dt[j]))
                        return true;
                return false;
            }
            if (iri !=null && iri!= dt.iri)
                return false;
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            switch (ki)
            {
                default: return ki == dk;
                case Sqlx.CONTENT: return true;
                case Sqlx.NCHAR: return dk == Sqlx.CHAR || dk==ki;
                case Sqlx.NUMERIC: return dk == Sqlx.INTEGER || dk == ki;
                case Sqlx.PASSWORD: return dk == Sqlx.CHAR || dk == ki;
                case Sqlx.REAL: return dk == Sqlx.INTEGER || dk == Sqlx.NUMERIC || dk == ki;
                case Sqlx.TABLE:
                case Sqlx.ROW:
                    {
                        int n = Length;
                        if (n != dt.Length)
                            return false;
                        if (columns == null || columns.Length != n)
                            throw new PEException("PE832");
                        if (dt.columns == null || dt.columns.Length != n)
                            return false;
                        for (int j = 0; j < n; j++) // for CanTakeValueOf, don't require names to match (INSERT)
                            if (!columns[j].CanTakeValueOf(dt[j]))
                                return false;
                        return true;
                    }
                case Sqlx.ARRAY:
                case Sqlx.MULTISET:
                    return kind==dt.kind && elType.CanTakeValueOf(dt.elType);
                case Sqlx.TYPE:
                    {
                        int n = Length;
                        if (n != dt.Length)
                            return false;
                        for (int j = 0; j < n; j++)
                            if (!columns[j].CanTakeValueOf(dt[j]))
                                return false;
                        return true;
                    }
#if SIMILAR
                case Sqlx.CHAR: return dk == Sqlx.REGULAR_EXPRESSION || dk == ki;
#endif
            }
        }
        /// <summary>
        /// Test a given value to see if it belongs to exactly this type (apart from dataLength, precision or scale)
        /// </summary>
        /// <param name="tr">The transaction: Common doesn't know about this</param>
        /// <param name="v">the given value</param>
        /// <returns>whether it belongs to this type</returns>
        public virtual bool HasValue(TypedValue v)
        {
            if (v is TSensitive st)
            {
                if (kind == Sqlx.SENSITIVE)
                    return elType.HasValue(cx,st.value);
                return false;
            }
            if (kind == Sqlx.SENSITIVE)
                return elType.HasValue(cx, v);
            var ki = Equivalent(kind);
            if (ki == Sqlx.ONLY || iri!=null)
                return Equals(v.dataType); // must match exactly
            if ((!NotNull) && v.IsNull)
                return true;
            if (ki == Sqlx.NULL || kind==Sqlx.ANY)
                return true;
            if (ki == Sqlx.UNION)
                return TypeOf(cx,v)!=null;
            if (ki!=v.dataType.kind)
                return false;
            switch (ki) 
            {
                case Sqlx.MULTISET:
                case Sqlx.ARRAY:
                    return elType?.Equals(v.dataType.elType) ?? true;
                case Sqlx.TABLE:
                case Sqlx.TYPE: 
                case Sqlx.ROW:
                    {
                        if (v.dataType.Length != Length)
                            return false;
                        for (int i = 0; i < v.dataType.Length;i++ )
                            if (!v.dataType.columns[i].EqualOrStrongSubtypeOf(columns[i]))
                                return false;
                        break;
                    }
            }
            return true;
        }
        public virtual TypedValue Parse(Transaction tr,string s)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.Parse(tr, s));
            if (kind == Sqlx.DOCUMENT)
                return new TDocument(tr, s);
            return Parse(new Scanner(tr,s.ToCharArray(),0));
        }
        public virtual TypedValue Parse(Transaction tr,string s,string m)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.Parse(tr, s, m));
            if (kind == Sqlx.DOCUMENT)
                return new TDocument(tr, s);
            return Parse(new Scanner(tr,s.ToCharArray(), 0, m));
        }
        /// <summary>
        /// Parse a string value for this type. 
        /// </summary>
        /// <param name="lx">The scanner</param>
        /// <returns>a typedvalue</returns>
        public virtual TypedValue Parse(Scanner lx,bool union=false)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.Parse(lx, union));
            int start = lx.pos;
            if (lx.Match("null"))
                return TNull.Value;
            switch (Equivalent(kind))
            {
                case Sqlx.Null:
                    {
                        int st = lx.pos;
                        int ln = lx.len - lx.pos;
                        var str = new string(lx.input, st, ln);
                        var lxr = new Lexer(str, lx.tr);
                        lx.pos += lxr.pos;
                        lx.ch = lxr.input[lxr.pos];
                        return lxr.val;
                    }
                case Sqlx.BOOLEAN: 
                    if (lx.MatchNC("TRUE"))
                        return TBool.True;
                    if (lx.MatchNC("FALSE"))
                        return TBool.False;
                    break;
                case Sqlx.CHAR:
                    {
                        int st = lx.pos;
                        int ln = lx.len - lx.pos;
                        var str = new string(lx.input, st, ln);
                        var qu = lx.ch;
                        if (qu == '\'' || qu=='"' || qu==(char)8217)
                        {
                            var sb = new StringBuilder();
                            while (lx.pos < lx.len && lx.ch == qu)
                            {
                                lx.Advance();
                                while (lx.pos < lx.len && lx.ch!=qu)
                                {
                                    sb.Append(lx.ch);
                                    lx.Advance();
                                }
                                lx.Advance();
                                if (lx.pos < lx.len && lx.ch==qu)
                                    sb.Append(lx.ch);
                            }
                            str = sb.ToString();
                        }
                        else if (str.StartsWith("null"))
                        {
                            for (var i = 0; i < 4; i++)
                                lx.Advance();
                            return TNull.Value;
                        }
                        else
                        {
                            lx.pos = lx.len;
                            lx.ch = '\0';
                        }
                        if (prec != 0 && prec < str.Length)
                            str = str.Substring(0, prec);
                        if (charSet == CharSet.UCS || Check(str))
                            return new TChar(str);
                        break;
                    }
                case Sqlx.CONTENT:
                    {
                        var st = lx.pos;
                        var s = new string(lx.input, lx.pos, lx.input.Length - lx.pos);
                        var i = 1;
                        var c = TDocument.GetValue(lx.tr, null, s,s.Length,ref i);
                        lx.pos = lx.pos+i;
                        lx.ch = (lx.pos<lx.input.Length)?lx.input[lx.pos]:'\0';
                        return c.typedValue;
                    }
                case Sqlx.PASSWORD: goto case Sqlx.CHAR;
                case Sqlx.XML:
                    {
                        TXml rx = null;
                        var xr = XmlReader.Create(new StringReader(new string(lx.input, start, lx.input.Length - start)));
                        while (xr.Read())
                            switch (xr.NodeType)
                            {
                                case XmlNodeType.Element:
                                    if (rx == null)
                                    {
                                        rx = new TXml(xr.Value);
                                        if (xr.HasAttributes)
                                        {
                                            var an = xr.AttributeCount;
                                            for (int i = 0; i < an; i++)
                                            {
                                                xr.MoveToAttribute(i);
                                                rx = new TXml(rx,xr.Name,new TChar(xr.Value));
                                            }
                                        }
                                        xr.MoveToElement();
                                    }
                                    rx.children.Add(Parse(new Scanner(lx.tr, xr.ReadInnerXml().ToCharArray(), 0)) as TXml);
                                    break;
                                case XmlNodeType.Text:
                                    rx = new TXml(rx, xr.Value);
                                    break;
                                case XmlNodeType.EndElement:
                                    return rx;
                            }
                        break;
                    }
                case Sqlx.NUMERIC:
                    {
                        string str;
                        if (char.IsDigit(lx.ch) || lx.ch == '-' || lx.ch == '+')
                        {
                            start = lx.pos;
                            lx.Advance();
                            while (char.IsDigit(lx.ch))
                                lx.Advance();
                            if (lx.ch=='.' && kind!=Sqlx.INTEGER)
                            {
                                lx.Advance();
                                while (char.IsDigit(lx.ch))
                                    lx.Advance();
                            }
                            else
                            {
                                str = lx.String(start, lx.pos - start);
                                if (lx.pos - start > 18)
                                {
                                    Integer x = Integer.Parse(str);
                                    if (kind == Sqlx.NUMERIC)
                                        return new TNumeric(this,new Common.Numeric(x, 0));
                                    if (kind == Sqlx.REAL)
                                        return new TReal(this,(double)x);
                                    if (lx.ch=='.') // tolerate .00000
                                    {
                                        if (union)
                                            throw new InvalidCastException();
                                        var first = true;
                                        lx.Advance();
                                        if (first && lx.ch > '5')  // >= isn't entirely satisfactory either
                                        {
                                            if (x >= 0)
                                                x = x + Integer.One;
                                            else
                                                x = x - Integer.One;
                                        }
                                        else
                                            first = false;
                                        while (char.IsDigit(lx.ch))
                                            lx.Advance();
                                    }
                                    return new TInt(this,x);
                                }
                                else
                                {
                                    long x = long.Parse(str);
                                    if (kind == Sqlx.NUMERIC)
                                        return new TNumeric(this,new Common.Numeric(x));
                                    if (kind == Sqlx.REAL)
                                        return new TReal(this,(double)x);
                                    if (lx.ch == '.') // tolerate .00000
                                    {
                            //            if (union)
                            //                throw new InvalidCastException();
                                        var first = true;
                                        lx.Advance();
                                        if (first && lx.ch > '5') // >= isn't entirely satisfactory either
                                        {
                                            if (x >= 0)
                                                x++;
                                            else
                                                x--;
                                        }
                                        else
                                            first = false;
                                        while (char.IsDigit(lx.ch))
                                            lx.Advance();
                                    }
                                    return new TInt(this,x);
                                }
                            }
                            if ((lx.ch != 'e' && lx.ch != 'E') || kind == Sqlx.NUMERIC)
                            {
                                str = lx.String(start, lx.pos - start);
                                Common.Numeric x = Common.Numeric.Parse(str);
                                if (kind == Sqlx.REAL)
                                    return new TReal(this,(double)x);
                                return new  TNumeric(this,x);
                            }
                            lx.Advance();
                            if (lx.ch == '-' || lx.ch == '+')
                                lx.Advance();
                            if (!char.IsDigit(lx.ch))
                                throw new DBException("22107").Mix();
                            lx.Advance();
                            while (char.IsDigit(lx.ch))
                                lx.Advance();
                            str = lx.String(start, lx.pos - start);
                            return new TReal(this,(double)Common.Numeric.Parse(str));
                        }
                    }
                    break;
                case Sqlx.INTEGER: goto case Sqlx.NUMERIC;
                case Sqlx.REAL: goto case Sqlx.NUMERIC;
                case Sqlx.DATE:
                    {
                        var st = lx.pos;
                        var da = GetDate(lx,st);
                        if (lx.ch == 'T' || lx.ch == ' ') // tolerate unnecessary time information
                        {
                            lx.Advance();
                            GetTime(lx, st);
                        }
                        return new TDateTime(this, da);
                    }
                case Sqlx.TIME: return new TTimeSpan(this,GetTime(lx,lx.pos));
                case Sqlx.TIMESTAMP: return new TDateTime(this,GetTimestamp(lx,lx.pos));
                case Sqlx.INTERVAL: return new TInterval(this,GetInterval(lx));
                case Sqlx.TYPE: 
                case Sqlx.ROW:
                    {
                        if (lx.ch == '\0')
                            return New(lx.tr);
#if !SILVERLIGHT && !WINDOWS_PHONE
                        if (lx.mime == "text/xml")
                        {
                            // tolerate missing values and use of attributes
                            var db = Database.Get(lx.tr,this);
                            var cols = new TypedValue[columns.Length];
                            var xd = new XmlDocument();
                            xd.LoadXml(new string(lx.input));
                            var xc = xd.FirstChild;
                            if (xc != null && xc is XmlDeclaration)
                                xc = xc.NextSibling;
                            if (xc == null)
                                goto bad;
                            bool blank = true;
                            for (int i = 0; i < columns.Length; i++)
                            {
                                var co = columns[i];
                                TypedValue item = null;
                                if (xc.Attributes != null)
                                {
                                    var att = xc.Attributes[co.name.ToString()];
                                    if (att != null)
                                        item = co.Parse(lx.tr, att.InnerXml, lx.mime);
                                }
                                if (item == null)
                                    for (int j = 0; j < xc.ChildNodes.Count; j++)
                                    {
                                        var xn = xc.ChildNodes[j];
                                        if (xn.Name == columns[i].name.ToString())
                                        {
                                            item = co.Parse(lx.tr, xn.InnerXml, lx.mime);
                                            break;
                                        }
                                    }
                                blank = blank && (item == null);
                                cols[i] = item;
                            }
                            if (blank)
                                return TXml.Null;
                            return new TRow(this, cols);
                        }
                        else
#endif
                            if (lx.mime == "text/csv")
                        {
                            // we expect all columns, separated by commas, without string quotes
                            var cols = new TypedValue[columns.Length];
                            for (int i = 0; i < columns.Length; i++)
                            {
                                var co = columns[i];
                                var dt = co;
                                TypedValue vl = null;
                                try
                                {
                                    switch (dt.kind)
                                    {
                                        case Sqlx.CHAR:
                                            {
                                                int st = lx.pos;
                                                string s = "";
                                                if (lx.ch == '"')
                                                {
                                                    lx.Advance();
                                                    st = lx.pos;
                                                    while (lx.ch != '"')
                                                        lx.Advance();
                                                    s = new string(lx.input, st, lx.pos - st);
                                                    lx.Advance();
                                                }
                                                else
                                                {
                                                    while (lx.ch != ',' && lx.ch != '\n' && lx.ch != '\r')
                                                        lx.Advance();
                                                    s = new string(lx.input, st, lx.pos - st);
                                                }
                                                vl = new TChar(s);
                                                break;
                                            }
                                        case Sqlx.DATE:
                                            {
                                                int st = lx.pos;
                                                char oc = lx.ch;
                                                string s = "";
                                                while (lx.ch != ',' && lx.ch != '\n' && lx.ch != '\r')
                                                    lx.Advance();
                                                s = new string(lx.input, st, lx.pos - st);
                                                if (s.IndexOf("/") >= 0)
                                                {
                                                    var sa = s.Split('/');
                                                    vl = new TDateTime(SqlDataType.Date, new DateTime(int.Parse(sa[2]), int.Parse(sa[0]), int.Parse(sa[1])));
                                                    break;
                                                }
                                                lx.pos = st;
                                                lx.ch = oc;
                                                vl = dt.Parse(lx);
                                                break;
                                            }
                                        default: vl = dt.Parse(lx); break;
                                    }
                                }
                                catch (Exception)
                                {
                                    while (lx.ch != '\0' && lx.ch != ',' && lx.ch != '\r' && lx.ch != '\n')
                                        lx.Advance();
                                }
                                if (i < columns.Length - 1)
                                {
                                    if (lx.ch != ',')
                                        throw new DBException("42101", lx.ch).Mix();
                                    lx.Advance();
                                }
                                else
                                {
                                    if (lx.ch == ',')
                                        lx.Advance();
                                    if (lx.ch != '\0' && lx.ch != '\r' && lx.ch != '\n')
                                        throw new DBException("42101", lx.ch).Mix();
                                    while (lx.ch == '\r' || lx.ch == '\n')
                                        lx.Advance();
                                }
                                cols[i] = vl;
                            }
                            return new TRow(this, cols);
                        }
                        else
                        {
                            //if (names.Length > 0)
                            //    throw new DBException("2200N");
                            //tolerate named columns in SQL version
                            //mixture of named and unnamed columns is not supported
                            var comma = '(';
                            var end = ')';
                            Rvvs tag = null;
                            if (lx.ch == '{')
                            {
                                comma = '{'; end = '}';
                            }
                            if (lx.ch == '[')
                                goto case Sqlx.TABLE;
                            var cols = new TColumn[columns.Length];
                            Ident.Tree<int?> expect = Ident.Tree<int?>.Empty;
                            for (int i = 0; i < columns.Length; i++)
                            {
                                var co = columns[i];
                                Ident.Tree<int?>.Add(ref expect, names[i], i);
                                cols[i] = new TColumn(names[i], co.New(lx.tr));
                            }
                            int j = 0;
                            bool namedOk = true;
                            bool unnamedOk = true;
                            lx.White();
                            while (lx.ch == comma)
                            {
                                lx.Advance();
                                lx.White();
                                var n = lx.GetName();
                                int? k = null;
                                if (n == null) // no name supplied
                                {
                                    if (!unnamedOk)
                                        throw new DBException("22208").Mix();
                                    namedOk = false;
                                    k = j++;
                                }
                                else // column name supplied
                                {
                                    if (lx.ch != ':')
                                        throw new DBException("42124").Mix();
                                    else
                                        lx.Advance();
                                    if (!namedOk)
                                        throw new DBException("22208").Mix()
                                            .Add(Sqlx.COLUMN_NAME, new TChar(n));
                                    unnamedOk = false;
                                    k = (n == "_id") ? -1 : expect[new Ident(n,0,Ident.IDType.Column,null)];
                                }
                                /*   if (k == null)
                                       throw new DBException("42112", n).Mix(); */
                                if (k == null)
                                    k = j++;
                                var ik = (int)k;
                                lx.White();
                                if (ik >= 0)
                                {
                                    var co = columns[ik];
                                    cols[ik] = new TColumn(names[ik], co.Parse(lx));
                                }
                                else
                                    tag = Rvvs.From(Char.Parse(lx).ToString());
                                comma = ',';
                                lx.White();
                            }
                            if (lx.ch != end)
                                break;
                            lx.Advance();
                            return new TRow(lx.tr, tag, cols);
                        }
                    }
                case Sqlx.TABLE:
                    {
                        return Copy().ParseList(lx);
                    }
                case Sqlx.ARRAY:
                    {
                         return elType.ParseList(lx);
                    }
                case Sqlx.UNION:
                    {
                        int st = lx.pos;
                        char ch = lx.ch;
                        for (var i = 0; i < columns.Length; i++)
                        {
                            try
                            {
                                var v = columns[i].Parse(lx,true);
                                lx.White();
                                if (lx.ch == ']' || lx.ch == ',' || lx.ch == '}')
                                    return v;
                            }
                            catch (Exception) {}
                            lx.pos = st;
                            lx.ch = ch;
                        }
                        break;
                    }
                case Sqlx.LEVEL:
                    {
                        lx.MatchNC("LEVEL");
                        lx.White();
                        var min = 'D' - lx.ch;
                        lx.Advance();
                        lx.White();
                        var max = min;
                        if (lx.ch=='-')
                        {
                            lx.Advance();
                            lx.White();
                            max = 'D' - lx.ch;
                        }
                        lx.White();
                        var gps = BTree<string, bool>.Empty;
                        var rfs = BTree<string, bool>.Empty;
                        var rfseen = false;
                        if (lx.MatchNC("groups"))
                        {
                            lx.White();
                            while (lx.pos < lx.len)
                            {
                                var s = lx.NonWhite();
                                if (s.ToUpper().CompareTo("REFERENCES") == 0)
                                    rfseen = true;
                                else if (rfseen)
                                    ATree<string, bool>.Add(ref rfs, s, true);
                                else
                                    ATree<string, bool>.Add(ref gps, s, true);
                                lx.White();
                            }
                        }
                        return TLevel.New(new Common.Level((byte)min, (byte)max, gps, rfs));
                    }
            }
#if !WINDOWS_PHONE
            bad:
#endif
            if (lx.pos+4<lx.len && new string(lx.input,start,4).ToLower() == "null")
            {
                for (int i = 0; i < 4; i++)
                    lx.Advance();
                return TNull.Value;
            }
            var xs = new string(lx.input, start, lx.pos-start);
            throw new DBException("22005E", ToString(), xs).ISO()
                .AddType(this).AddValue(new TChar(xs));
        }
        TypedValue ParseList(Scanner lx)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.ParseList(lx));
            var rv = new TArray(this);
            int j = 0;
#if !SILVERLIGHT && !WINDOWS_PHONE
            if (lx.mime == "text/xml")
            {
                var xd = new XmlDocument();
                xd.LoadXml(new string(lx.input));
                for (int i = 0; i < xd.ChildNodes.Count; i++)
                    rv[j++] = Parse(lx.tr, xd.ChildNodes[i].InnerXml);
            }
            else
#endif
            {
                char delim = lx.ch, end = ')';
                if (delim == '[')
                    end = ']';
                if (delim != '(' && delim != '[')
                {
                    var xs = new string(lx.input, 0, lx.len);
                    throw new DBException("22005F", ToString(), xs).ISO()
                        .AddType(this).AddValue(new TChar(xs));
                }
                lx.Advance();
                for (;;)
                {
                    lx.White();
                    if (lx.ch == end)
                        break;
                    rv[j++] = Parse(lx);
                    if (lx.ch == ',')
                        lx.Advance();
                    lx.White();
                }
                lx.Advance();
            }
            return rv;
        }
        /// <summary>
        /// Helper for parsing Interval values
        /// </summary>
        /// <param name="lx">the scanner</param>
        /// <returns>an Interval</returns>
        Interval GetInterval(Scanner lx)
        {
            int y = 0, M = 0, d = 0, h = 0, m = 0;
            long s = 0;
            bool sign = false;
           if (lx.ch == '-')
                sign = true;
            if (lx.ch == '+' || lx.ch == '-')
                lx.Advance();
            int ks = IntervalPart(start);
            int ke = IntervalPart(end);
            if (ke < 0)
                ke = ks + 1;
            var st = lx.pos;
            string[] parts = GetParts(lx, ke - ks, st);
            if (ks <= 1)
            {
                if (ks == 0)
                    y = int.Parse(parts[0]);
                if (ks <= 1 && ke == 1)
                    M = int.Parse(parts[1 - ks]);
                if (sign)
                { y = -y; M = -M; }
                return new Interval(y, M);
            }
            if (ks <= 2 && ke > 2)
                d = int.Parse(parts[2 - ks]);
            if (ks <= 3 && ke > 3)
                h = int.Parse(parts[3 - ks]);
            if (ks <= 4 && ke > 4)
                m = int.Parse(parts[4 - ks]);
            if (ke > 5)
                s = (long)(double.Parse(parts[5 - ks]) * TimeSpan.TicksPerSecond);
            s = d * TimeSpan.TicksPerDay + h * TimeSpan.TicksPerHour +
                m * TimeSpan.TicksPerMinute + s;
            if (sign)
                s = -s;
            return new Interval(s);
        }
        /// <summary>
        /// Facilitate quick decoding of the interval fields
        /// </summary>
        internal static Sqlx[] intervalParts = new Sqlx[] { Sqlx.YEAR, Sqlx.MONTH, Sqlx.DAY, Sqlx.HOUR, Sqlx.MINUTE, Sqlx.SECOND };
        /// <summary>
        /// helper for encoding interval fields
        /// </summary>
        /// <param name="e">YEAR, MONTH, DAY, HOUR, MINUTE, SECOND</param>
        /// <returns>corresponding integer 0,1,2,3,4,5</returns>
        internal static int IntervalPart(Sqlx e)
        {
            switch (e)
            {
                case Sqlx.YEAR: return 0;
                case Sqlx.MONTH: return 1;
                case Sqlx.DAY: return 2;
                case Sqlx.HOUR: return 3;
                case Sqlx.MINUTE: return 4;
                case Sqlx.SECOND: return 5;
            }
            return -1;
        }
        /// <summary>
        /// Helper for parts of a date value
        /// </summary>
        /// <param name="lx">the scanner</param>
        /// <param name="n">the number of parts</param>
        /// <returns>n strings</returns>
        string[] GetParts(Scanner lx, int n, int st)
        {
            string[] r = new string[n];
            for (int j = 0; j < n; j++)
            {
                if (lx.pos > lx.len)
                    throw new DBException("22007",Diag(lx,st)).Mix();
                r[j] = GetPart(lx,st);
            }
            return r;
        }
        /// <summary>
        /// Helper for extracting parts of a date value
        /// </summary>
        /// <param name="lx">the scanner</param>
        /// <returns>a group of digits as a string</returns>
        string GetPart(Scanner lx,int st)
        {
            st = lx.pos;
            lx.Advance();
            while (char.IsDigit(lx.ch))
                lx.Advance();
            return new string(lx.input, st, lx.pos - st);
        }
        /// <summary>
        /// Get the date part from the string
        /// </summary>
        /// <returns>the DateTime so far</returns>
        DateTime GetDate(Scanner lx,int st)
        {
            try
            {
                int y, m, d;
                int pos = lx.pos;
                // first look for SQL standard date format
                if (lx.pos + 10 <= lx.input.Length && lx.input[lx.pos + 4] == '-' && lx.input[lx.pos + 7] == '-')
                {
                    y = GetNDigits(lx, '-', 0, 4, pos); 
                    m = GetNDigits(lx, '-', 1, 2, pos); 
                    d = GetNDigits(lx, '-', 1, 2, pos);
                }
                else // try to use regional settings
                {
                    y = GetShortDateField(lx, 'y', ref pos, st);
                    m = GetShortDateField(lx, 'M', ref pos, st);
                    d = GetShortDateField(lx, 'd', ref pos, st);
                    lx.pos = pos;
                    lx.ch = (pos < lx.input.Length) ? lx.input[pos] : (char)0;
                }
                return new DateTime(y, m, d);
            }
            catch (Exception)
            {
                throw new DBException("22007", /*e.Message*/Diag(lx, st)).Mix();
            }
        }
        string Diag(Scanner lx,int st)
        {
            var n = lx.input.Length - st;
            if (n > 20)
                n = 20;
            return lx.String(st, n);
        }
        /// <summary>
        /// Get a Timestamp from the string
        /// </summary>
        /// <returns>DateTime</returns>
        DateTime GetTimestamp(Scanner lx,int st)
        {
            DateTime d = GetDate(lx,st);
            if (lx.ch == 0)
                return d;
            if (lx.ch != ' ' && lx.ch != 'T')
                throw new DBException("22008", Diag(lx, st)).Mix();
            lx.Advance();
            TimeSpan r = GetTime(lx,st);
            return d + r;
        }
        /// <summary>
        /// Get the time part from the string (ISO 8601)
        /// </summary>
        /// <returns>a TimeSpan</returns>
        TimeSpan GetTime(Scanner lx,int st)
        {
            int h = GetHour(lx,st);
            int m = 0;
            int s = 0;
            int f = 0;
            if (lx.ch == ':' || System.Char.IsDigit(lx.ch))
            {
                if (lx.ch==':')
                    lx.Advance();
                m = GetMinutes(lx,st);
                if (lx.ch == ':' || System.Char.IsDigit(lx.ch))
                {
                    if (lx.ch==':')
                        lx.Advance();
                    s = GetSeconds(lx,st);
                    if (lx.ch == '.')
                    {
                        lx.Advance();
                        var nst = lx.pos;
                        f = GetUnsigned(lx,st);
                        int n = lx.pos - nst;
                        if (n > 6)
                            throw new DBException("22008", Diag(lx, st)).Mix();
                        while (n < 7)
                        {
                            f *= 10;
                            n++;
                        }
                    }
                }
            }
            TimeSpan r = new TimeSpan(h, m, s);
            if (f!=0)
                r += TimeSpan.FromTicks(f);
            return r + GetTimeZone(lx,st);
        }
        TimeSpan GetTimeZone(Scanner lx,int st)
        {
            if (lx.ch=='Z')
            {
                lx.Advance();
                return TimeSpan.Zero;
            }
            var s = lx.ch;
            if (s != '+' && s != '-')
                return TimeSpan.Zero;
            lx.Advance();
            var z = GetTime(lx,st);
            return (s == '+') ? z : -z;
        }
        /// <summary>
        /// ShortDatePattern: d.M.yy for example means 1 or 2 digits for d and M.
        /// So we need to identify and count delimiters to get the field
        /// </summary>
        /// <param name="f"></param>
        /// <param name="delim">delimiter used in pattern</param>
        /// <param name="delimsBefore">Number of delimiters before the desired field</param>
        /// <param name="len"></param>
        void GetShortDatePattern(char f,ref char delim,out int delimsBefore,out int len)
        {
            var pat = CultureInfo.CurrentCulture.DateTimeFormat.ShortDatePattern;
            var found = false;
            delimsBefore = 0;
            int off = 0;
            for (; off < pat.Length; off++)
            {
                var c = pat[off];
                if (delim == (char)0 && c != 'y' && c != 'M' && c != 'd')
                    delim = c;
                if (c == delim)
                    delimsBefore++;
                if (pat[off] == f)
                {
                    found = true;
                    break;
                }
            }
            if (!found)
                throw new DBException("22007", "Bad Pattern " + pat);
            for (len = 0; off+len < pat.Length && pat[off + len] == f; len++)
                ;
        }
        int GetShortDateField(Scanner lx,char f,ref int pos, int st)
        {
            var p = lx.pos;
            var ch = lx.ch;
            var delim = (char)0;
            GetShortDatePattern(f, ref delim,out int dbef, out int len);
            var r = GetNDigits(lx,delim,dbef,len,st);
            if (f == 'y' && len == 2)
            {
                if (r >= 50)
                    r += 1900;
                else
                    r += 2000;
            }
            if (pos < lx.pos)
                pos = lx.pos;
            lx.pos = p;
            lx.ch = ch;
            return r;
        }
        /// <summary>
        /// Get N (or, if N==1, 2) digits for months, days, hours, minutues, seconds
        /// </summary>
        /// <returns>an int</returns>
        int GetNDigits(Scanner lx,char delim,int dbef,int n,int st)
        {
            for (; lx.ch != (char)0 && dbef > 0; dbef--)
            {
                while (lx.ch != (char)0 && lx.ch != delim)
                    lx.Advance();
                if (lx.ch!=(char)0)
                    lx.Advance();
            }
            if (lx.ch==(char)0)
                throw new DBException("22008", Diag(lx, st)).ISO();
            var s = lx.pos;
            for (int i = 0; i < n; i++)
            {
                if (!System.Char.IsDigit(lx.ch))
                    throw new DBException("22008",Diag(lx, st)).ISO();
                lx.Advance();
            }
            if (n==1 && System.Char.IsDigit(lx.ch))
            {
                n++; lx.Advance();
            }
           return int.Parse(new string(lx.input, s, n));
        }
        /// <summary>
        /// get an hour as 2 digits
        /// </summary>
        /// <returns>an int</returns>
        int GetHour(Scanner lx,int st)
        {
            int h = GetNDigits(lx,':',0,2,st);
            if (h < 0 || h > 23)
                throw new DBException("22008", Diag(lx, st)).ISO();
            return h;
        }
        /// <summary>
        /// get minutes as 2 digits
        /// </summary>
        /// <returns>an int</returns>
        int GetMinutes(Scanner lx,int st)
        {
            int m = GetNDigits(lx,':',0,2,st);
            if (m < 0 || m > 59)
                throw new DBException("22008", Diag(lx, st)).ISO();
            return m;
        }
        /// <summary>
        /// get seconds as 2 digits
        /// </summary>
        /// <returns>an int</returns>
        int GetSeconds(Scanner lx, int st)
        {
            int m = GetNDigits(lx,'.',0, 2, st);
            if (m < 0 || m > 59)
                throw new DBException("22008", Diag(lx, st)).ISO();
            return m;
        }
        /// <summary>
        /// get the fractional seconds part
        /// </summary>
        /// <returns></returns>
        int GetUnsigned(Scanner lx,int st)
        {
            while (char.IsWhiteSpace(lx.ch))
                lx.Advance();
            int s = lx.pos;
            while (char.IsDigit(lx.ch))
                lx.Advance();
            return int.Parse(new string(lx.input, s, lx.pos - s));
        }
        /// <summary>
        /// Coerce a given value to this type, bomb if it isn't possible
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        public TypedValue Coerce(TypedValue v)
        {
            if (v is TSensitive st)
            {
                if (kind == Sqlx.SENSITIVE)
                    return elType.Coerce(tr, st.value);
                throw new DBException("22210");
            }
            if (kind == Sqlx.SENSITIVE)
                return elType.Coerce(tr, v);
            if (NotNull && (v== null || v.IsNull ))
            {
                if (defaultValue == "")
                    throw new DBException("22206", ToString()).ISO();
                return New(tr);
            }
            if (v == null || v.IsNull)
                return v;
            if (abbrev!=null && v.dataType.kind==Sqlx.CHAR && kind!=Sqlx.CHAR)
                v = Parse(new Scanner(tr,v.ToString().ToCharArray(),0));
            if (CompareTo(v.dataType) == 0)
                return v;
            if (kind == Sqlx.ROW && Length==0 || (Length==1 && v.dataType.Length == 0 && columns[0].HasValue(tr,v)))
                return v;
            var vk = Equivalent(v.dataType.kind);
            if (vk == Sqlx.ROW && v.dataType.Length == 1 && CompareTo(v.dataType[0]) == 0)
                return (v as TRow)[0];
            if (kind == Sqlx.TYPE && vk == Sqlx.DOCUMENT)
            {
                var doc = v as TDocument;
                var obs = new TypedValue[Length];
                for (var i=0;i<Length;i++)
                    obs[i] = (doc[names[i]] is TypedValue f)?columns[i].Coerce(tr,f):columns[i].New(tr);
                return new TRow(this, obs);
            }
            if (iri==null || v.dataType.iri==iri)
                switch (Equivalent(kind))
                {
                    case Sqlx.INTEGER:
                        {
                            if (vk == Sqlx.INTEGER)
                            {
                                if (prec != 0)
                                {
                                    Integer iv;
                                    if (v.Val(tr) is long)
                                        iv = new Integer((long)v.Val(tr));
                                    else
                                        iv = v.Val(tr) as Integer;
                                    var limit = Integer.Pow10(prec);
                                    if (iv >= limit || iv <= -limit)
                                        throw new DBException("22003").ISO()
                                            .AddType(this).AddValue(v);
                                    return new TInteger(this, iv);
                                }
                                if (v.Val(tr) is long)
                                    return new TInt(this, v.ToLong());
                                return new TInteger(this, v.ToInteger());
                            }
                            if (vk == Sqlx.NUMERIC)
                            {
                                var a = v.Val(tr) as Common.Numeric;
                                int r = 0;
                                while (a.scale > 0)
                                {
                                    a.mantissa = a.mantissa.Quotient(10, ref r);
                                    a.scale--;
                                }
                                while (a.scale < 0)
                                {
                                    a.mantissa = a.mantissa.Times(10);
                                    a.scale++;
                                }
                                if (prec != 0)
                                {
                                    var limit = Integer.Pow10(prec);
                                    if (a.mantissa >= limit || a.mantissa <= -limit)
                                        throw new DBException("22003").ISO()
                                            .AddType(this).Add(Sqlx.VALUE, v);
                                }
                                return new TInteger(this, a.mantissa);
                            }
                            if (vk == Sqlx.REAL)
                            {
                                var ii = v.ToLong().Value;
                                if (prec != 0)
                                {
                                    var iv = new Integer(ii);
                                    var limit = Integer.Pow10(prec);
                                    if (iv > limit || iv < -limit)
                                        throw new DBException("22003").ISO()
                                             .AddType(this).AddValue(v);
                                }
                                return new TInt(this, ii);
                            }
                            if (vk == Sqlx.CHAR)
                                return new TInt(Integer.Parse(v.ToString()));
                        }
                        break;
                    case Sqlx.NUMERIC:
                        {
                            Common.Numeric a;
                            var ov = v.Val(tr);
                            if (vk == Sqlx.NUMERIC)
                                a = (Numeric)ov;
                            else if (ov == null)
                                a = null;
                            else if (ov is long?)
                                a = new Numeric(v.ToLong().Value);
                            else if (v.Val(tr) is Integer)
                                a = new Common.Numeric((Integer)v.Val(tr));
                            else if (v.Val(tr) is double?)
                                a = new Common.Numeric(v.ToDouble().Value);
                            else
                                break;
                            if (scale != 0)
                            {
                                if ((!a.mantissa.IsZero()) && a.scale > scale)
                                    a = a.Round(scale);
                                int r = 0;
                                while (a.scale > scale)
                                {
                                    a.mantissa = a.mantissa.Quotient(10, ref r);
                                    a.scale--;
                                }
                                while (a.scale < scale)
                                {
                                    a.mantissa = a.mantissa.Times(10);
                                    a.scale++;
                                }
                            }
                            if (prec != 0)
                            {
                                var limit = Integer.Pow10(prec);
                                if (a.mantissa > limit || a.mantissa < -limit)
                                    throw new DBException("22003").ISO()
                                         .AddType(this).AddValue(v);
                            }
                            return new TNumeric(this, a);
                        }
                    case Sqlx.REAL:
                        {
                            var rr = v.ToDouble();
                            if (rr == null)
                                return TNull.Value;
                            var r = rr.Value;
                            if (prec == 0)
                                return new TReal(this, r);
                            decimal d = new decimal(r);
                            d = Math.Round(d, scale);
                            bool sg = d < 0;
                            if (sg)
                                d = -d;
                            decimal m = 1.0M;
                            for (int j = 0; j < prec - scale; j++)
                                m = m * 10.0M;
                            if (d > m)
                                break;
                            if (sg)
                                d = -d;
                            return new TReal(this, (double)d);
                        }
                    case Sqlx.DATE:
                        switch (vk)
                        {
                            case Sqlx.DATE:
                                return v;
                            case Sqlx.CHAR:
                                return new TDateTime(this, DateTime.Parse(v.ToString(),
                                    v.dataType.culture));
                        }
                        if (v.Val(tr) is DateTime)
                            return new TDateTime(this, (DateTime)v.Val(tr));
                        if (v.Val(tr) is long)
                            return new TDateTime(this, new DateTime(v.ToLong().Value));
                        break;
                    case Sqlx.TIME:
                        switch (vk)
                        {
                            case Sqlx.TIME:
                                return v;
                            case Sqlx.CHAR:
                                return new TTimeSpan(this, TimeSpan.Parse(v.ToString(),
                                    v.dataType.culture));
                        }
                        break;
                    case Sqlx.TIMESTAMP:
                        switch(vk)
                        {
                            case Sqlx.TIMESTAMP: return v;
                            case Sqlx.DATE:
                                return new TDateTime(this, ((Date)v.Val(tr)).date);
                            case Sqlx.CHAR:
                                return new TDateTime(this, DateTime.Parse(v.ToString(),
                                    v.dataType.culture));
                        }
                        if (v.Val(tr) is long)
                            return new TDateTime(this, new DateTime(v.ToLong().Value));
                        break;
                    case Sqlx.INTERVAL: if (v.Val(tr) is Interval)
                            return new TInterval(this,v.Val(tr) as Interval);
                        break;
                    case Sqlx.CHAR:
                        {
                            var vt = v.dataType;
                            string str;
                            switch (vt.kind)
                            {
                                case Sqlx.TIMESTAMP: str = ((DateTime)(v.Val(tr))).ToString(culture); break;
                                case Sqlx.DATE: str = ((Date)v.Val(tr)).date.ToString(culture); break;
                                default: str = v.ToString(); break;
                            }
                            if (prec != 0 && str.Length > prec)
                                throw new DBException("22001", "CHAR(" + prec + ")", "CHAR(" + str.Length + ")").ISO()
                                                    .AddType(this).AddValue(vt);
                            return new TChar(this, str);
                        }
                    case Sqlx.ROW:
                        if (vk == Sqlx.ROW)
                        {
                            if (columns.Length != v.dataType.columns.Length)
                                break;
                            TRow vr = v as TRow;
                            var nc = new TypedValue[Length];
                            bool usenc = false;
                            for(int i=0;i<Length;i++)
                            {
                                TypedValue tt = vr[i];
                                if (tt == null || !tt.dataType.EqualOrStrongSubtypeOf(columns[i]))
                                    tt = columns[i].Coerce(tr, tt);
                                if (tt != vr[i])
                                {
                                    var cc = columns[i];
                                    nc[i] = tt;
                                    usenc = true;
                                }
                                else
                                    nc[i] = vr[i];
                            }
                            if (usenc)
                                return new TRow(tr,this,nc);
                            return new TRow(tr,this,vr);
                        }
                        break;
                    case Sqlx.PERIOD:
                        {
                            var pd = v.Val(tr) as Period;
                            return new TPeriod(this, new Period(elType.Coerce(tr,pd.start), elType.Coerce(tr,pd.end)));
                        }
                    case Sqlx.DOCUMENT:
                        {
                            switch (vk)
                            {
                                case Sqlx.CHAR:
                                    {
                                        var vs = v.ToString();
                                        if (vs[0] == '{')
                                            return new TDocument(tr, vs);
                                        break;
                                    }
                                case Sqlx.BLOB:
                                    {
                                        var i = 0;
                                        return new TDocument(tr, (byte[])v.Val(tr),ref i);
                                    }
                            }
                            return v;
                        }
                    case Sqlx.CONTENT: return v;
                    case Sqlx.PASSWORD: return v;
#if MONGO
                    case Sqlx.OBJECT:
                        {
                            switch (v.dataType.kind)
                            {
                                case Sqlx.BLOB: return new TObjectId((byte[])v.Val());
                                case Sqlx.CHAR: return new TObjectId(cx,v.ToString());
                            }
                            break;
                        }
#endif
                    case Sqlx.DOCARRAY: goto case Sqlx.DOCUMENT;
#if SIMILAR
                    case Sqlx.REGULAR_EXPRESSION:
                        {
                            switch (v.DataType.kind)
                            {
                                case Sqlx.CHAR: return new TChar(v.ToString());
                            }
                            break;
                        }
#endif
                    case Sqlx.VALUE:
                    case Sqlx.NULL:
                        return v;
                    default:
                        for (SqlDataType t = this; t != null; t = t.super)
                            if (t.HasValue(tr,v))
                                return v;
                        break;
                }
            throw new DBException("22005G", this, v.ToString()).ISO();
        }
        internal TypedValue Coerce(Transaction tr, RowBookmark rb)
        {
            var q = rb._rs.qry;
            var v = rb.Value();
            if (Length == 0 && q.display==1)
                return Coerce(tr, v[0]);
            if (v.Length > q.display)
            {
                if (q.displayType == null)
                {
                    var et = q.nominalDataType;
                    var cs = new SqlDataType[q.display];
                    var ns = new Ident[q.display];
                    for (int i = 0; i < q.display; i++)
                    {
                        cs[i] = et[i];
                        ns[i] = et.names[i];
                    }
                    q.displayType = new RowType(Sqlx.ROW,cs, ns);
                }
                v = new TRow(tr, q.displayType, v.columns);
            }
            Coerce(tr, v);
            return v;
        }
        internal TypedValue Fix(TypedValue v)
        {
            if (v == null)
                v = New();
            else if (this != v.dataType)
                v = Copy(v);
            return v;
        }
        /// <summary>
        /// The System.Type corresponding to a SqlDataType
        /// </summary>
        public Type SystemType
        {
            get
            {
                switch (Equivalent(kind))
                {
                    case Sqlx.ONLY: return super.SystemType;
                    case Sqlx.NULL: return typeof(DBNull);
                    case Sqlx.INTEGER: return typeof(long);
                    case Sqlx.NUMERIC: return typeof(Decimal);
                    case Sqlx.BLOB: return typeof(byte[]);
                    case Sqlx.NCHAR: goto case Sqlx.CHAR;
                    case Sqlx.CLOB: goto case Sqlx.CHAR;
                    case Sqlx.NCLOB: goto case Sqlx.CHAR;
                    case Sqlx.REAL: return typeof(double);
                    case Sqlx.CHAR: return typeof(string);
                    case Sqlx.PASSWORD: goto case Sqlx.CHAR;
                    case Sqlx.DATE: return typeof(Date);
                    case Sqlx.TIME: return typeof(TimeSpan);
                    case Sqlx.INTERVAL: return typeof(Interval);
                    case Sqlx.BOOLEAN: return typeof(bool);
                    case Sqlx.TIMESTAMP: return typeof(DateTime);
//#if EMBEDDED
                    case Sqlx.DOCUMENT: return typeof(Document);
//#else
//                    case Sqlx.DOCUMENT: return typeof(byte[]);
//#endif
                }
                return typeof(object);
            }
        }
        /// <summary>
        /// Select a predefined data type
        /// </summary>
        /// <param name="t">the token</param>
        /// <returns>the corresponding predefined type</returns>
        public static SqlDataType Predefined(Sqlx t)
        {
            switch (Equivalent(t))
            {
                case Sqlx.BLOB: return SqlDataType.Blob;
                case Sqlx.BLOBLITERAL: return SqlDataType.Blob;
                case Sqlx.BOOLEAN: return SqlDataType.Bool;
                case Sqlx.BOOLEANLITERAL: return SqlDataType.Bool;
                case Sqlx.CHAR: return SqlDataType.Char;
                case Sqlx.CHARLITERAL: return SqlDataType.Char;
                case Sqlx.DATE: return SqlDataType.Date;
                case Sqlx.DOCARRAY: return SqlDataType.Document;
                case Sqlx.DOCUMENT: return SqlDataType.Document;
                case Sqlx.DOCUMENTLITERAL: return SqlDataType.Document;
                case Sqlx.INTEGER: return SqlDataType.Int;
                case Sqlx.INTEGERLITERAL: return SqlDataType.Int;
                case Sqlx.INTERVAL: return SqlDataType.Interval;
                case Sqlx.NULL: return SqlDataType.Null;
                case Sqlx.NUMERIC: return SqlDataType.Numeric;
                case Sqlx.NUMERICLITERAL: return SqlDataType.Numeric;
                case Sqlx.PASSWORD: return SqlDataType.Password;
                case Sqlx.REAL: return SqlDataType.Real;
                case Sqlx.REALLITERAL: return SqlDataType.Real;
                case Sqlx.TIME: return SqlDataType.Timespan;
                case Sqlx.TIMESTAMP: return SqlDataType.Timestamp;
            }
            return new SqlDataType(t);
        }
        public string CultureName
        {
            get
            {
                if (kind == Sqlx.ONLY)
                    return super.CultureName;
                return culture?.Name??"";
            }
            set
            {
                culture = GetCulture(value);
            }
        }
        /// <summary>
        /// Validator
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public bool Check(string s)
        {
            if (kind == Sqlx.SENSITIVE)
                return elType.Check(s);
            if (charSet == CharSet.UCS)
                return true;
            try
            {
                byte[] x = Encoding.UTF8.GetBytes(s); // throws exception if not even UCS
                int n = s.Length;
                if (charSet <= CharSet.ISO8BIT && x.Length != n)
                    return false;
                for (int j = 0; j < n; j++)
                {
                    if (charSet <= CharSet.LATIN1 && x[j] > 128)
                        return false;
                    if (charSet <= CharSet.GRAPHIC_IRV && x[j] < 32)
                        return false;
                    byte b = x[j];
                    if (charSet <= CharSet.SQL_IDENTIFIER &&
                        (b == 0x21 || b == 0x23 || b == 0x24 || b == 0x40 || b == 0x5c || b == 0x60 || b == 0x7e))
                        return false;
                }
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }
        /// <summary>
        /// Set up a CultureInfo given a collation or culture name.
        /// We support the names specified in the SQL2003 standard, together
        /// with the cultures supported by .NET
        /// </summary>
        /// <param name="n">The name of a collation or culture</param>
        /// <returns></returns>
        public static CultureInfo GetCulture(string n)
        {
            if (n == null)
                return null;
            n = n.ToLower();
            try
            {
                switch (n)
                {
                    case "ucs_binary": return null;
                    case "sql_character": return null;
                    case "graphic_irv": return null;
                    case "sql_text": return null;
                    case "sql_identifier": return null;
                    case "latin1": return CultureInfo.InvariantCulture;
                    case "iso8bit": return CultureInfo.InvariantCulture;
                    case "unicode": return CultureInfo.InvariantCulture;
                    default: return new CultureInfo(n);
                }
            }
            catch (Exception e)
            {
                throw new DBException("2H000", e.Message).ISO();
            }
        }
        /// <summary>
        /// Coerce the value to this data type
        /// </summary>
        /// <param name="r">a strongly typed value</param>
        /// <returns>the result of evaluation</returns>
        public virtual TypedValue Eval(Transaction tr,  TypedValue v)
        {
            if (kind == Sqlx.SENSITIVE)
                return Coerce(tr, v);
            if (v.Val(tr) == null)
                return v;
            var d = v.dataType;
    //        if (kind == d.kind) 
      //          return v;
            if (iri==null || v.dataType.iri==iri)
            switch (Equivalent(kind))
            {
                case Sqlx.TIMESTAMP:
                    if (d.kind == Sqlx.INTERVAL)
                    {
                        DateTime dt = DateTime.Now;
                        TimeSpan ts = (TimeSpan)v.Val(tr);
                        return new TDateTime(d,new DateTime(dt.Year, dt.Month, dt.Day, ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds));
                    }
                    break;
                case Sqlx.REAL:
                    if (v.dataType.kind == Sqlx.REAL)
                        break;
                    if (v.Val(tr) is long)
                        v = new TReal(this, (double)v.ToLong());
                    else if (v.Val(tr) is Integer)
                        v = new TReal(this, (Integer)v.Val(tr));
                    else if (v.dataType.kind == Sqlx.NUMERIC)
                        v = new TReal(this, (double)(Numeric)v.Val(tr));
                    break;
                case Sqlx.NUMERIC:
                    if (v.dataType.kind==Sqlx.NUMERIC)
                        break;
                    if (v.Val(tr) is long)
                        v = new TNumeric(this, new Numeric((long)v.Val(tr)));
                    else if (v.dataType.kind==Sqlx.REAL)
                        v = new TNumeric(this, new Numeric((double)v.Val(tr)));
                    else if (v.Val(tr) is Integer)
                        v = new TNumeric(this, new Numeric((Integer)v.Val(tr), 0));
                    break;
                case Sqlx.INTEGER:
                    if (v.Val(tr) is long)
                        return v;
                    if (v.Val(tr) is Integer)
                        return v;
                    break;
                case Sqlx.ARRAY:
                    {
                        if (!(v.dataType.kind==Sqlx.ARRAY))
                            throw new DBException("22005H", "ARRAY", d).ISO();
                        var a = (TRow)v;
                        if (a.dataType.elType != Null)
                        {
                            if (a.dataType.elType == elType || (elType != Null && a.dataType.elType == elType))
                                return v;
                            throw new DBException("22005I", kind, v).ISO();
                        }
                        var b = new TypedValue[Length];
                        for(int i=0;i<Length;i++)
                            b[i] = this[i].Coerce(tr,a[i]);
                        return new TRow(tr,this, b);
                    }
                case Sqlx.MULTISET:
                        {
                            if (v.dataType.kind != Sqlx.MULTISET)
                                throw new DBException("22005J", "MULTISET", d).ISO();
                            TMultiset m = (TMultiset)v;
                            if (m.dataType.elType != Null)
                            {
                                if (m.dataType.elType == elType)
                                    return v;
                                throw new DBException("22005K", kind, v).ISO();
                            }
                            TMultiset b = new TMultiset(tr, elType);
                            for (var a = m.tree.First();a != null; a = a.Next())
                                if (a.value().HasValue)
                                {
                                    var e = elType.Eval(tr, a.key());
                                    for (int i = 0; i < a.value().Value; i++)
                                        b.Add(e);
                                }
                            return b;
                        }
            }
            return Coerce(tr,v);
        }
        /// <summary>
        /// Evaluate a binary operation 
        /// </summary>
        /// <param name="a">The first object</param>
        /// <param name="op">The binary operation</param>
        /// <param name="b">The second object</param>
        /// <returns>The evaluated object</returns>
        public virtual TypedValue Eval(Transaction tr, TypedValue a, Sqlx op, TypedValue b) // op is + - * / so a and b should be compatible arithmetic types
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.Eval(tr, a, op, b));
            if (op == Sqlx.NO)
                return Eval(tr, a);
            if (a == null || a.IsNull || b == null || b.IsNull)
                return New(tr);
            if (a is TUnion)
                a = ((TUnion)a).LimitToValue(tr); // a coercion possibly
            if (b is TUnion)
                b = ((TUnion)b).LimitToValue(tr);
            var knd = Equivalent(kind);
            var ak = Equivalent(a.dataType.kind);
            var bk = Equivalent(b.dataType.kind);
            if (knd == Sqlx.UNION)
            {
                if (ak == bk)
                    knd = ak;
                else if (ak == Sqlx.REAL || bk == Sqlx.REAL)
                    knd = Sqlx.REAL;
                else if (ak != Sqlx.INTEGER || bk != Sqlx.INTEGER)
                    knd = Sqlx.NUMERIC;
            }
            switch (knd)
            {
                case Sqlx.INTEGER:
                    if (ak == Sqlx.NUMERIC)
                        a = new TInteger(a.ToInteger());
                    if (bk == Sqlx.INTERVAL && kind == Sqlx.TIMES)
                        return Eval(tr, b, op, a);
                    if (bk == Sqlx.NUMERIC)
                        b = new TInteger(b.ToInteger());
                    if (ak == Sqlx.INTEGER)
                    {
                        if (a.Val(tr) is long)
                        {
                            if ((bk == Sqlx.INTEGER || bk == Sqlx.UNION) && b.Val(tr) is long)
                            {
                                long aa = a.ToLong().Value, bb = b.ToLong().Value;
                                switch (op)
                                {
                                    case Sqlx.PLUS:
                                        if (aa == 0)
                                            return b;
                                        if (aa > 0 && (bb <= 0 || aa < long.MaxValue - bb))
                                            return new TInt(this, aa + bb);
                                        else if (aa < 0 && (bb >= 0 || aa > long.MinValue - bb))
                                            return new TInt(this, aa + bb);
                                        return new TInteger(this, new Integer(aa) + new Integer(bb));
                                    case Sqlx.MINUS:
                                        if (bb == 0)
                                            return a;
                                        if (bb > 0 && (aa >= 0 || aa > long.MinValue + bb))
                                            return new TInt(this, aa - bb);
                                        else if (bb < 0 && (aa >= 0 || aa < long.MaxValue + bb))
                                            return new TInt(this, aa - bb);
                                        return new TInteger(this, new Integer(aa) - new Integer(bb));
                                    case Sqlx.TIMES:
                                        if (aa < int.MaxValue && aa > int.MinValue && bb < int.MaxValue && bb > int.MinValue)
                                            return new TInt(this, aa * bb);
                                        return new TInteger(this, new Integer(aa) * new Integer(bb));
                                    case Sqlx.DIVIDE: return new TInt(this, aa / bb);
                                }
                            }
                            else if (b.Val(tr) is Integer)
                                return IntegerOps(this, new Integer(a.ToLong().Value), op, (Integer)b.Val(tr));
                        }
                        else if (a.Val(tr) is Integer)
                        {
                            if (b.Val(tr) is long)
                                return IntegerOps(this, (Integer)a.Val(tr), op, new Integer(b.ToLong().Value));
                            else if (b.Val(tr) is Integer)
                                return IntegerOps(this, (Integer)a.Val(tr), op, (Integer)b.Val(tr));
                        }
                    }
                    break;
                case Sqlx.REAL:
                    return new TReal(this, DoubleOps(a.ToDouble(), op, b.ToDouble()));
                case Sqlx.NUMERIC:
                    if (a.dataType.Constrain(Int)!=null)
                        a = new TNumeric(new Numeric(a.ToInteger(), 0));
                    if (b.dataType.Constrain(Int)!=null)
                        b = new TNumeric(new Numeric(b.ToInteger(), 0));
                    if (a is TNumeric && b is TNumeric)
                        return new TNumeric(DecimalOps(((TNumeric)a).value, op, ((TNumeric)b).value));
                    var ca = a.ToDouble();
                    var cb = b.ToDouble();
                    return Coerce(tr, new TReal(this, DoubleOps(ca, op, cb)));
                case Sqlx.TIME:
                case Sqlx.TIMESTAMP:
                case Sqlx.DATE:
                    {
                        var ta = (DateTime)a.Val(tr);
                        switch (bk)
                        {
                            case Sqlx.INTERVAL:
                                {
                                    var ib = (Interval)b.Val(tr);
                                    switch (op)
                                    {
                                        case Sqlx.PLUS: return new TDateTime(this, ta.AddYears(ib.years).AddMonths(ib.months).AddTicks(ib.ticks));
                                        case Sqlx.MINUS: return new TDateTime(this, ta.AddYears(-ib.years).AddMonths(ib.months).AddTicks(-ib.ticks));
                                    }
                                    break;
                                }
                            case Sqlx.TIME:
                            case Sqlx.TIMESTAMP:
                            case Sqlx.DATE:
                                {
                                    if (b.IsNull)
                                        return TNull.Value;
                                    if (op == Sqlx.MINUS)
                                        return DateTimeDifference(ta,(DateTime)b.Val(tr));
                                    break;
                                }
                        }
                        throw new DBException("42161", "date operation");
                    }
                case Sqlx.INTERVAL:
                    {
                        var ia = (Interval)a.Val(tr);
                        Interval ic = null;
                        switch (bk)
                        {
                            case Sqlx.DATE:
                                return Eval(tr, b, op, a);
                            case Sqlx.INTEGER:
                                var bi = b.ToInt().Value;
                                if (ia.yearmonth)
                                {
                                    var m = ia.years * 12 + ia.months;
                                    ic = new Interval(0, 0);
                                    switch (kind)
                                    {
                                        case Sqlx.TIMES: m = m * bi; break;
                                        case Sqlx.DIVIDE: m = m / bi; break;
                                    }
                                    if (start == Sqlx.YEAR)
                                    {
                                        ic.years = m / 12;
                                        if (end == Sqlx.MONTH)
                                            ic.months = m - 12 * (m / 12);
                                    }
                                    else
                                        ic.months = m;
                                    return new TInterval(this,ic);
                                }
                                break;
                            case Sqlx.INTERVAL:
                                var ib = (Interval)b.Val(tr);
                                if (ia.yearmonth != ib.yearmonth)
                                    break;
                                if (ia.yearmonth)
                                    switch (kind)
                                    {
                                        case Sqlx.PLUS: ic = new Interval(ia.years + ib.years, ia.months + ib.months); break;
                                        case Sqlx.MINUS: ic = new Interval(ia.years - ib.years, ia.months - ib.months); break;
                                        default: throw new PEException("PE56");
                                    }
                                else
                                    switch (kind)
                                    {
                                        case Sqlx.PLUS: ic = new Interval(ia.ticks - ib.ticks); break;
                                        case Sqlx.MINUS: ic = new Interval(ia.ticks - ib.ticks); break;
                                        default: throw new PEException("PE56");
                                    }
                                return new TInterval(this, ic);
                        }
                        throw new DBException("42161", "date operation");
                    }
            }
            throw new DBException("22005L", kind, a).ISO();
        }
        /// <summary>
        /// MaxLong bound for knowing if an Integer will fit into a long
        /// </summary>
        static Integer MaxLong = new Integer(long.MaxValue);
        /// <summary>
        /// MinLong bound for knowing if an Integer will fit into a long
        /// </summary>
        static Integer MinLong = new Integer(long.MinValue);
        /// <summary>
        /// Integer operations
        /// </summary>
        /// <param name="a">The left Integer operand</param>
        /// <param name="op">The operator</param>
        /// <param name="b">The right Integer operand</param>
        /// <returns>The Integer result</returns>
        static TypedValue IntegerOps(SqlDataType tp,Integer a, Sqlx op, Integer b)
        {
            Integer r;
            switch (op)
            {
                case Sqlx.PLUS: r = a + b; break;
                case Sqlx.MINUS: r = a - b; break;
                case Sqlx.TIMES: r = a * b; break;
                case Sqlx.DIVIDE: r = a / b; break;
                default: throw new PEException("PE52");
            }
            if (r.CompareTo(MinLong, 0) >= 0 && r.CompareTo(MaxLong, 0) <= 0)
                return new TInt(tp,(long)r);
            return new TInteger(tp,r);
        }
        /// <summary>
        /// Numeric operations
        /// </summary>
        /// <param name="a">The left Numeric operand</param>
        /// <param name="op">The operator</param>
        /// <param name="b">The right Numeric operand</param>
        /// <returns>The Numeric result</returns>
        static Common.Numeric DecimalOps(Common.Numeric a, Sqlx op, Common.Numeric b)
        {
            switch (op)
            {
                case Sqlx.PLUS:
                    if (a.mantissa == null)
                        return b;
                    if (b.mantissa == null)
                        return a;
                    return a + b;
                case Sqlx.MINUS:
                    if (a.mantissa == null)
                        return -b;
                    if (b.mantissa == null)
                        return a;
                    return a - b;
                case Sqlx.TIMES:
                    if (a.mantissa == null)
                        return a;
                    if (b.mantissa == null)
                        return b;
                    return a * b;
                case Sqlx.DIVIDE:
                    if (a.mantissa == null)
                        return a;
                    if (b.mantissa == null)
                        return b;
                    return Common.Numeric.Divide(a, b, (a.precision > b.precision) ? a.precision : b.precision);
                default: throw new PEException("PE53");
            }
        }
        /// <summary>
        /// double operations
        /// </summary>
        /// <param name="a">The left double operand</param>
        /// <param name="op">The operator</param>
        /// <param name="b">The right double operand</param>
        /// <returns>The double result</returns>
        static double DoubleOps(double? aa, Sqlx op, double? bb)
        {
            if (aa == null || bb == null)
                return double.NaN;
            var a = aa.Value;
            var b = bb.Value;
            switch (op)
            {
                case Sqlx.PLUS: return a + b;
                case Sqlx.MINUS: return a - b;
                case Sqlx.TIMES: return a * b;
                case Sqlx.DIVIDE: return a / b;
                default: throw new PEException("PE54");
            }
        }
        TInterval DateTimeDifference(DateTime a,DateTime b)
        {
            Interval it;
            switch (start)
            {
                case Sqlx.YEAR:
                    if (end == Sqlx.MONTH) goto case Sqlx.MONTH;
                    it = new Interval(a.Year - b.Year, 0);
                    break;
                case Sqlx.MONTH:
                    it = new Interval(0, (a.Year - b.Year) * 12 + a.Month - b.Month);
                    break;
                default:
                    it = new Interval(a.Ticks - b.Ticks); break;
            }
            return new TInterval(it);
        }
        public SqlDataType Path(Context cx,Ident field)
        {
            if (kind == Sqlx.SENSITIVE)
                return elType.Path(cx, field);
            if (field == null || kind==Sqlx.CONTENT)
                return this;
            if (kind == Sqlx.DOCUMENT)
                return Content;
            if ((kind == Sqlx.TYPE || kind == Sqlx.ROW) && names.Length > 0)
            {
                var i = names.Get(field,out Ident s);
                if (i.HasValue)
                {
                    var r = columns[i.Value]?[s];
                    if (r != null)
                        field.Set(0, names[i.Value].Defpos(Ident.IDType.Column), Ident.IDType.Column);
                    return r;
                }
            }
            return null;
        }
        /// <summary>
        /// Test for type equality
        /// </summary>
        /// <param name="obj">another type</param>
        /// <returns>whether the types match</returns>
        public override bool Equals(object obj)
        {
            SqlDataType that = (SqlDataType)obj;
            if (that == null)
                return true; // benefit of doubt?
            var ki = Equivalent(kind);
            var tk = Equivalent(that.kind);
            if ((ki != tk && ki!=Sqlx.ROW && tk!=Sqlx.ROW)
                || (columns != null) != (that.columns != null))
                return false;
            if (columns != null)
            {
                var n = columns.Length;
                if (columns.Length != that.columns.Length || names.Length!=that.names.Length)
                    return false;
                for (int i = 0; i < n; i++)
                    if (!columns[i].Equals(that.columns[i]))
                        return false;
                for(int i=0;i<names.Length;i++)
                    if (that.names[i] != names[i])
                        return false;
            }
            if (name!=null && name.CompareTo(that.name)!=0)
                return false;
            if (kind == Sqlx.ONLY && that.kind == Sqlx.ONLY)
                return super.Equals(that.super);
            return prec == that.prec && scale == that.scale && AscDesc == that.AscDesc && Nulls == that.Nulls &&
                iri == that.iri && start == that.start && end == that.end &&
                ((elType == null && that.elType == null) || (elType != null && elType.Equals(that.elType))) &&
                charSet == that.charSet && culture == that.culture && defaultValue == that.defaultValue &&
                search == that.search && generated == that.generated && orderfunc == that.orderfunc && orderflags == that.orderflags;
        }
        /// <summary>
        /// Test for when to record subtype information. We want to do this when a value of
        /// a subtype is recorded in a column of the parent type, and the subtype information is
        /// not obtainable from the value alone. E.g. extra semantic information
        /// </summary>
        /// <param name="dt">The target type to check</param>
        /// <returns>true if this is a strong subtype of dt</returns>
        public bool EqualOrStrongSubtypeOf(SqlDataType dt)
        {
            if (kind == Sqlx.SENSITIVE||dt.kind==Sqlx.SENSITIVE)
            {
                if (kind == Sqlx.SENSITIVE && dt.kind == Sqlx.SENSITIVE)
                    return elType.EqualOrStrongSubtypeOf(dt.elType);
                return false;
            }
            if (dt == null)
                return true;
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            if (dk == Sqlx.CONTENT || dk == Sqlx.Null)
                return true;
            if (ki == Sqlx.ONLY)
                return super.Equals(dt);
            if ((ki != Sqlx.ROW && ki != dk) || (ki == Sqlx.ROW && dk != Sqlx.ROW) || 
                (elType == null) != (dt.elType == null) || orderfunc!=dt.orderfunc || orderflags!=dt.orderflags || generated!=dt.generated)
                return false;
            if (elType != null && !elType.EqualOrStrongSubtypeOf(dt.elType))
                return false;
            if (ki == Sqlx.UNION && dk == Sqlx.UNION)
                for (int i = 0; i < Length; i++)
                {
                    for (int j = 0; j < dt.Length; j++)
                        if (columns[i].Equals(dt[j]))
                            goto ok;
                    return false;
                ok: ;
                }
            if (dk == Sqlx.UNION)
                for (int i = 0; i < dt.Length; i++)
                    if (EqualOrStrongSubtypeOf(dt[i]))
                        return true;
            for (SqlDataType s = this; s != null; s = s.super)
                if (s.Equals(dt))
                    return true;
            if (Length != dt.Length)
                return false;
            for (int i = 0; i < Length; i++)
                if (!columns[i].EqualOrStrongSubtypeOf(dt[i]))
                    return false;
            return (dt.prec == 0 || prec == dt.prec) && (dt.scale == 0 || scale == dt.scale) &&
                (dt.AscDesc == Sqlx.NULL || AscDesc == dt.AscDesc) && (dt.Nulls == Sqlx.NULL || Nulls == dt.Nulls) &&
                (dt.iri == "" || iri == dt.iri) && (dt.start == Sqlx.NULL || start == dt.start) &&
                (dt.end == Sqlx.NULL || end == dt.end) && (dt.charSet == CharSet.UCS || charSet == dt.charSet) &&
                (dt.culture == CultureInfo.InvariantCulture || culture == dt.culture) && (dt.defaultValue == "" || defaultValue == dt.defaultValue) &&
                (dt.search == null || search == dt.search);
        }
        /// <summary>
        /// Compute the datatype resulting from limiting this by another datatype constraint.
        /// this.LimitBy(union) gives this if this is in the union, otherwise
        /// this.LimitBy(dt) gives the same result as dt.LimitBy(this).
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        internal SqlDataType Constrain(SqlDataType dt)
        {
            if (kind == Sqlx.SENSITIVE)
            {
                if (dt.kind == Sqlx.SENSITIVE)
                {
                    var ts = elType.Constrain(dt.elType);
                    if (ts == null)
                        return null;
                    return ts.Equals(elType) ? this : ts.Equals(dt.elType) ? dt :
                        new SqlDataType(Sqlx.SENSITIVE, ts);
                }
                var tt = elType.Constrain(dt);
                if (tt == null)
                    return null;
                return tt.Equals(elType) ? this : new SqlDataType(Sqlx.SENSITIVE, tt);
            }
            if (dt.kind == Sqlx.SENSITIVE)
            {
                var tu = Constrain(dt.elType);
                if (tu == null)
                    return null;
                return tu.Equals(dt.elType) ? dt : new SqlDataType(Sqlx.SENSITIVE,tu);
            }
            if (dt == null||dt==Null)
                return this;
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            SqlDataType r = this;
            if ((ki == Sqlx.ARRAY || ki == Sqlx.MULTISET) && ki == dk && dt.elType == null)
                return this;
           if (ki == Sqlx.CONTENT || ki == Sqlx.VALUE)
                return dt;
            if (dk == Sqlx.CONTENT || dk == Sqlx.VALUE || Equals(dt))
                return this;           
           if (ki == Sqlx.REAL && dk == Sqlx.NUMERIC)
               return dt;
           if (kind == Sqlx.NUMERIC && dt.kind == Sqlx.INTEGER)
               return null;
           if (kind == Sqlx.REAL && dt.kind == Sqlx.INTEGER)
               return null;
           if (kind==Sqlx.INTERVAL && dt.kind==Sqlx.INTERVAL)
            {
                int s = IntervalPart(start), ds = IntervalPart(dt.start),
                    e = IntervalPart(end), de = IntervalPart(dt.end);
                if (s >= 0 && (s <= 1 != ds <= 1))
                    return null;
                if (s <= ds && (e >= de || de < 0))
                    return this;
            }
           if (dk == Sqlx.ONLY && Equals(dt.super))
                return dt;
            if (ki == Sqlx.ONLY && super.Equals(dt))
                return this;
            if (kind == Sqlx.PASSWORD && dt.kind == Sqlx.CHAR)
                return this;
            if (ki==dk && (kind==Sqlx.ARRAY || kind==Sqlx.MULTISET))
            {
                if (elType == null)
                    return dt;
                var ect = elType.Constrain(dt.elType);
                if (ect == elType)
                    return this;
                return dt;
            }
            if (ki == Sqlx.UNION && dk!=Sqlx.UNION)
                foreach (var c in columns)
                    if (c.Constrain(dt)!=null)
                        return c;
            if (ki != Sqlx.UNION && dk == Sqlx.UNION)
                foreach (var c in dt.columns)
                    if (c.Constrain(this) != null)
                        return this;
            if (ki == Sqlx.UNION && dk == Sqlx.UNION)
            {
                var ca = new List<SqlDataType>();
                for (int i = 0; i < Length; i++)
                    for (int j = 0; j < dt.Length; j++)
                    {
                        var u = columns[i].Constrain(dt[j]);
                        if (u != null)
                            ca.Add(u);
                    }
                if (ca.Count == 0)
                    return null;
                if (ca.Count == 1)
                    return ca[0];
                return new UnionType(ca.ToArray());
            }
            else if (elType != null && dt.elType != null)
                r = new SqlDataType(kind, elType.LimitBy(dt.elType));
            else if (ki == Sqlx.ROW && dt == Table)
                return this;
            else if ((ki == Sqlx.ROW || ki == Sqlx.TYPE) && (dk == Sqlx.ROW || dk==Sqlx.TABLE))
            {
                if (columns == null)
                    r = dt;
                else if (dt.columns == null)
                    r = this;
                else
                {
                    if (Length != dt.Length)
                        return null;
                    var cs = new SqlDataType[Length];
                    for (int i = 0; i < Length; i++)
                    {
                        var dc = columns[i].Constrain(dt[i]);
                        if (dc == null)
                            return null;
                        cs[i] = dc;
                    }
                    var ln = names.Length;
                    if (ln == 0)
                        ln = dt.names.Length;
                    var ns = new Ident[ln];
                    for (var i = 0; i < ln; i++)
                        ns[i] = (i < names.Length) ? names[i] : dt.names[i];
                    r = new RowType(Sqlx.ROW,cs, ns);
                }
                return r;
            }
            else if ((ki != Sqlx.ROW && ki != dk) || (ki == Sqlx.ROW && dk != Sqlx.ROW) ||
                    (elType == null) != (dt.elType == null) || orderfunc != dt.orderfunc || orderflags != dt.orderflags ||
                    generated != dt.generated)
                return null;
            if ((dt.prec != 0 && prec != 0 && prec != dt.prec) || (dt.scale != 0 && scale != 0 && scale != dt.scale) ||
                (dt.AscDesc != Sqlx.NULL && AscDesc != Sqlx.NULL && AscDesc != dt.AscDesc) ||
                (dt.Nulls != Sqlx.NULL && Nulls != Sqlx.NULL && Nulls != dt.Nulls) ||
                (dt.iri != "" && iri != "" && iri != dt.iri) || start != dt.start || end != dt.end ||
                (dt.charSet != CharSet.UCS && charSet != CharSet.UCS && charSet != dt.charSet) ||
                (dt.culture != CultureInfo.InvariantCulture && culture != CultureInfo.InvariantCulture && culture != dt.culture) ||
   //             (dt.defaultValue != "" && defaultValue != "" && defaultValue != dt.defaultValue) ||
                (dt.search != null && search != null && search != dt.search))
                return null;
            if ((prec != dt.prec || scale != dt.scale || AscDesc != dt.AscDesc || Nulls != dt.Nulls || iri != dt.iri ||
                charSet != dt.charSet || culture != dt.culture ||
                defaultValue != dt.defaultValue || search != dt.search) && (r == this || r == dt))
                r = new SqlDataType(r);
            if (dt.prec != 0 && dt.prec != r.prec)
                r.prec = dt.prec;
            else if (prec != 0 && prec != r.prec)
                r.prec = prec;
            if (dt.scale != 0 && dt.scale != r.scale)
                r.scale = dt.scale;
            else if (scale != 0 && scale != r.scale)
                r.scale = scale;
            if (dt.AscDesc != Sqlx.NULL && dt.AscDesc != r.AscDesc)
                r.AscDesc = dt.AscDesc;
            else if (AscDesc != Sqlx.NULL && AscDesc != r.AscDesc)
                r.AscDesc = AscDesc;
            if (dt.Nulls != Sqlx.NULL && dt.Nulls != r.Nulls)
                r.Nulls = dt.Nulls;
            else if (Nulls != Sqlx.NULL && Nulls != r.Nulls)
                r.Nulls = Nulls;
            if (dt.iri != "" && dt.iri != r.iri)
                r.iri = dt.iri;
            else if (iri != "" && iri != r.iri)
                r.iri = iri;
            if (dt.charSet != CharSet.UCS && dt.charSet != r.charSet)
                r.charSet = dt.charSet;
            else if (charSet != CharSet.UCS && charSet != r.charSet)
                r.charSet = charSet;
            if (dt.culture != CultureInfo.InvariantCulture && dt.culture != r.culture)
                r.culture = dt.culture;
            else if (culture != CultureInfo.InvariantCulture && culture != r.culture)
                r.culture = culture;
            if (dt.defaultValue != "" && dt.defaultValue != r.defaultValue)
                r.defaultValue = dt.defaultValue;
            else if (defaultValue != "" && defaultValue != r.defaultValue)
                r.defaultValue = defaultValue;
            if (dt.search != null && dt.search != r.search)
                r.search = dt.search;
            else if (search != null && search != r.search)
                r.search = search;
            return r;
        }
        internal SqlDataType LimitBy(SqlDataType dt)
        {
            return Constrain(dt) ?? this;
        }
        /// <summary>
        /// Output a single byte to the DbData
        /// </summary>
        /// <param name="s">the DbData</param>
        /// <param name="b">a byte</param>
        internal static void PutByte(PhysBase pb, byte b) // LOCKED
        {
            pb.df.Check();
            pb.WriteByte(b);
        }
        /// <summary>
        /// Output byte length and byte sequence to the DbData
        /// </summary>
        /// <param name="s">the DbData</param>
        /// <param name="b">The byte sequence</param>
        static void PutBytes0(PhysBase pb,byte[] b) // LOCKED
        {
            pb.df.Check();
            byte n = (byte)b.Length;
            pb.WriteByte(n);
            pb.Write(b, 0, n);
        }
        /// <summary>
        /// Output an int to the DataFile
        /// In the DataFile the format is same as Integer
        /// </summary>
        /// <param name="s">the DbData </param>
        /// <param name="n">An int</param>
        internal static void PutInt(PhysBase pb,int n) // LOCKED
        {
            pb.df.Check();
            PutBytes0(pb, new Integer(n).bytes);
        }
        /// <summary>
        /// Output int length and byte sequence to the DataFile
        /// </summary>
        /// <param name="s">the DbData</param>
        /// <param name="b">The byte sequence</param>
        internal static void PutBytes(PhysBase pb,byte[] b) // LOCKED
        {
            int n = b.Length;
            PutInt(pb,n);
            pb.Write(b, 0, n);
        }
        /// <summary>
        /// PutInt32 to the datafile
        /// Used only for EndOfFile marker
        /// </summary>
        /// <param name="s">the DbData</param>
        /// <param name="n"></param>
        internal static void PutInt32(PhysBase pb,int v) // LOCKED
        {
            pb.df.Check();
            for (int j = 24; j >= 0; j -= 8)
                pb.WriteByte((byte)(v >> j));
        }
        /// <summary>
        /// Output a long to the DataFile
        /// In the DataFile the format is same as Integer
        /// </summary>
        /// <param name="s">the DbData</param>
        /// <param name="n">The long</param>
        public static void PutLong(PhysBase pb,long n) // LOCKED
        {
            pb.df.Check();
            PutBytes0(pb, new Integer(n).bytes);
        }
        /// <summary>
        /// Output a string to the DataFile
        /// </summary>
        /// <param name="s">the DbData</param>
        /// <param name="v"></param>
        public static void PutString(PhysBase pb, string v)
        {
            PutBytes(pb, Encoding.UTF8.GetBytes(v));
        }
        /// <summary>
        /// Output a byte loosely describing this type to the datafile
        /// </summary>
        /// <param name="s">the DbData</param>
        public static void PutDataType(SqlDataType tp, PhysBase pb,SqlDataType nominalType, long role, Reloc r)
        {
            if (tp.kind == Sqlx.ONLY)
            {
                var at = tp.super;
                PutDataType(at, pb, nominalType, role, r);
                return;
            }
            if (pb.types.Contains(tp))
                tp = pb.Tracker(role,pb.types[tp].Value).type;
            if (tp.EqualOrStrongSubtypeOf(nominalType) && !tp.Equals(nominalType))
            {
                PutByte(pb, (byte)DataType.DomainRef);
                PutLong(pb, pb.types[tp].Value);
                return;
            }
            else
            switch (Equivalent(tp.kind))
            {
                case Sqlx.Null: 
                case Sqlx.NULL: PutByte(pb, (byte)DataType.Null); break;
                case Sqlx.ARRAY: PutByte(pb, (byte)DataType.Array); break;
                case Sqlx.BLOB: PutByte(pb, (byte)DataType.Blob); break;
                case Sqlx.BOOLEAN: PutByte(pb, (byte)DataType.Boolean); break;
                case Sqlx.LEVEL:
                case Sqlx.CHAR: PutByte(pb, (byte)DataType.String); break;
                case Sqlx.DOCUMENT: goto case Sqlx.BLOB;
                case Sqlx.DOCARRAY: goto case Sqlx.BLOB;
                case Sqlx.OBJECT: goto case Sqlx.BLOB;
#if MONGO || SIMILAR
                case Sqlx.REGULAR_EXPRESSION: goto case Sqlx.CHAR;
#endif
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.INTEGER: PutByte(pb, (byte)DataType.Integer); break;
                case Sqlx.MULTISET: PutByte(pb, (byte)DataType.Multiset); break;
                case Sqlx.NUMERIC: PutByte(pb, (byte)DataType.Numeric); break;
                case Sqlx.PASSWORD: PutByte(pb, (byte)DataType.Password); break;
                case Sqlx.REAL: PutByte(pb, (byte)DataType.Numeric); break;
                case Sqlx.DATE: PutByte(pb, (byte)DataType.Date); break;
                case Sqlx.TIME: PutByte(pb, (byte)DataType.TimeSpan); break;
                case Sqlx.TIMESTAMP: PutByte(pb, (byte)DataType.TimeStamp); break;
                case Sqlx.INTERVAL: PutByte(pb, (byte)DataType.Interval); break;
                case Sqlx.TYPE:
                case Sqlx.REF:
                case Sqlx.ROW: PutByte(pb, (byte)DataType.Row); break;
             }
        }
        public static void Put(Context cx,PRow m, PhysBase pb, long role, Reloc r)
        {
            if (m == null)
                return;
            for (int i = 0; i < m.Length; i++)
                Put(cx,m[i], pb, m[i].dataType, role, r);
        }
        public static void Put(Context cx,TypedValue tv, PhysBase pb, SqlDataType nt, long role, Reloc r)
        {
            var dt = tv.dataType;
            if (tv == null || tv.IsNull)
                dt = Null;
            PutDataType(dt, pb, nt, role, r);
            Put(cx,dt, pb, tv, role, r);
        }
        /// <summary>
        /// Output value to the PhysBase
        /// </summary>
        /// <param name="tp">The DataType</param>
        /// <param name="p">the PhysBase</param>
        /// <param name="v">the value to output</param>
        /// <param name="r">Relocation information</param>
        public static void Put(Context cx, SqlDataType tp, PhysBase p, TypedValue v, long role, Reloc r) // LOCKED
        {
            var tr = cx as Transaction;
            p.df.Check();
            switch (Equivalent(tp.kind))
            {
                case Sqlx.SENSITIVE: Put(cx, tp.elType, p, v, role, r); break;
                case Sqlx.NULL: break;
                case Sqlx.BLOB: PutBytes(p, (byte[])v.Val(tr)); break;
                case Sqlx.BOOLEAN: PutByte(p, (byte)(v.ToBool().Value ? 1 : 0)); break;
                case Sqlx.CHAR: PutString(p, v?.ToString()); break;
                case Sqlx.DOCUMENT:
                    {
                        var d = v as TDocument;
#if MONGO
                        if (cx != null && !d.Contains("_id"))
                            d.Add("_id", new TObjectId(cx));
#endif
                        PutBytes(p, d.ToBytes(tr,null)); break;
                    }
                case Sqlx.INCREMENT:
                    {
                        var d = v as Delta;
                        PutInt(p, d.details.Count);
                        foreach(var de in d.details)
                        {
                            PutInt(p,de.ix);
                            PutByte(p,(byte)de.how);
                            PutString(p, de.name.ToString());
                            var dt = de.what.dataType;
                            Put(cx, dt, p, de.what, role, r);
                        }
                        break;
                    }
#if MONGO
                case Sqlx.OBJECT: PutBytes(p, ((TObjectId)v).ToBytes()); break;
#endif
                case Sqlx.DOCARRAY:
                    {
                        var d = v as TDocArray;
                        PutBytes(p, d.ToBytes()); break;
                    }
                case Sqlx.PASSWORD: goto case Sqlx.CHAR;
#if SIMILAR
                case Sqlx.REGULAR_EXPRESSION: goto case Sqlx.CHAR;
#endif
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.INTEGER:
                    {
                        var n = v as TInteger;
                        if (n == null)
                            PutLong(p, v.ToLong().Value);
                        else
                            PutBytes0(p, n.ivalue.bytes);
                        break;
                    }
                case Sqlx.NUMERIC:
                    {
                        var d = v.Val(tr) as Numeric;
                        if (v is TInt)
                            d = new Numeric(v.ToLong().Value);
                        if (v is TInteger)
                            d = new Numeric((Integer)v.Val(tr), 0);
                        PutBytes0(p, d.mantissa.bytes);
                        PutInt(p, d.scale);
                        break;
                    }
                case Sqlx.REAL:
                    {
                        Numeric d;
                        if (v == null)
                            break;
                        if (v is TReal)
                            d = new Numeric(v.ToDouble().Value);
                        else
                            d = (Numeric)v.Val(tr);
                        PutBytes0(p, d.mantissa.bytes);
                        PutInt(p, d.scale);
                        break;
                    }
                case Sqlx.DATE:
                    if (v is TInt)
                    {
                        PutLong(p, v.ToLong().Value);
                        return;
                    }
                    PutLong(p, (((TDateTime)v).value.Value).Ticks); break;
                case Sqlx.TIME:
                    if (v is TInt)
                    {
                        PutLong(p, v.ToLong().Value);
                        return;
                    }
                    PutLong(p, (((TTimeSpan)v).value.Value).Ticks); break;
                case Sqlx.TIMESTAMP:
                    if (v is TInt)
                    {
                        PutLong(p, v.ToLong().Value);
                        return;
                    }
                    PutLong(p, (((TDateTime)v).value.Value).Ticks); break;
                case Sqlx.INTERVAL:
                    {
                        Interval n = null;
                        if (v is TInt) // shouldn't happen!
                            n = new Interval(v.ToLong().Value);
                        else
                            n = (Interval)v.Val(tr);
                        PutByte(p, n.yearmonth ? (byte)1 : (byte)0);
                        if (n.yearmonth)
                        {
                            PutInt(p, n.years);
                            PutInt(p, n.months);
                        } else
                            PutLong(p, n.ticks);
                        break;
                    }
                    //the following cases always output their component values prefixed by the DataType. byte
                case Sqlx.ROW:
                    {
                        var rw = v
#if MONGO
                            .MakeRow(cx)
#endif
                            as TRow;
                        var dd = tp.DomainDefPos(p,r); // type in Owner
                        PutLong(p, dd);
                        var st = rw.dataType.elType;
                        PutInt(p, st.columns.Length);
                        for (int j = 0; j < rw.Length; j++)
                        {
                            var x = rw[j];
                            if (st.names[j].Defpos(Ident.IDType.Column) == 0)
                                throw new PEException("PE872");
                            PutLong(p, st.names[j].Defpos(Ident.IDType.Column));
                            Put(cx,x,p,x.dataType, role, r);
                        }
                        break;
                    }
                case Sqlx.TYPE: goto case Sqlx.ROW;
                case Sqlx.REF:
                    {
                        var rw = (TRow)v;
                        var dd = tp.DomainDefPos(p, r); // type in Owner
                        PutLong(p, dd);
                        PutInt(p, tp.columns.Length);
                        for (int j = 0; j < rw.Length; j++)
                        {
                            var x = rw[j];
                            if (tp.names[j].Defpos(Ident.IDType.Column) == 0)
                                throw new PEException("PE872");
                            PutLong(p, tp.names[j].Defpos(Ident.IDType.Column));
                            Put(cx, x, p, x.dataType, role, r);
                        }
                        break;
                    }
                case Sqlx.ARRAY:
                    {
                        var a = (TArray)v;
                        PutLong(p, a.dataType.elType.DomainDefPos(p,r));
                        PutInt(p, a.Length);
                        foreach(var e in a.list)
                            Put(cx,e, p, a.dataType.elType, role, r);
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        TMultiset m = (TMultiset)v;
                        PutLong(p, m.dataType.elType.DomainDefPos(p, r));
                        PutInt(p, (int)m.Count);
                        for (var a = m.tree.First(); a != null; a = a.Next())
                            if (a.value().HasValue)
                                for (int i = 0; i < a.value().Value; i++)
                                    Put(cx, a.key(), p, m.dataType.elType, role, r);
                        break;
                    }
            }
        }
        /// <summary>
        /// We are about to read a data item whose expected datatype has 
        /// a reified domaindefpos of dompos.
        /// We may find null, or a subtype. In these case, dompos will be updated.
        /// </summary>
        /// <param name="buf">The file buffer</param>
        /// <param name="ppos">The place in the data file used for the type system</param>
        /// <returns></returns>
        public SqlDataType GetDataType(Reader rdr,long ppos)
        {
            var b = (DataType)buf.GetByte();
            if (b == DataType.Null)
                return Null;
            if (b == DataType.DomainRef)
                return buf.pbase.FindDataType(ppos, buf.role, buf.GetLong());
            return this;
        }
        /// <summary>
        /// Get data from the file using type system as it was at ppos
        /// </summary>
        /// <param name="buf">the BaseBuffer to read from</param>
        /// <param name="ppos">a historical file position for the type system</param>
        /// <returns></returns>
        public TypedValue Get(Reader rdr)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this,elType.Get(rdr));
            switch (Equivalent(kind))
            {
                case Sqlx.NULL: return TNull.Value;
                case Sqlx.Null: return null;
                case Sqlx.BLOB: return new TBlob(this, rdr.GetBytes());
                case Sqlx.BOOLEAN: return (rdr.ReadByte() == 1)?TBool.True:TBool.False;
                case Sqlx.CHAR: return new TChar(this, rdr.GetString());
                case Sqlx.DOCUMENT:
                    {
                        var i = 0;
                        return new TDocument(rdr.pbase.cnx as Transaction, rdr.GetBytes(),ref i);
                    }
                case Sqlx.DOCARRAY: goto case Sqlx.DOCUMENT;
                case Sqlx.INCREMENT:
                    {
                        var r = new Delta();
                        var n = rdr.GetInt();
                        for (int i=0;i<n;i++)
                        {
                            var ix = rdr.GetInt();
                            var h = (Common.Delta.Verb)rdr.ReadByte();
                            var nm = rdr.GetIdent();
                            r.details.Add(new Delta.Action(ix, h, nm, Get(rdr)));
                        }
                        return r;
                    }
#if SIMILAR
                case Sqlx.REGULAR_EXPRESSION: goto case Sqlx.CHAR;
#endif
#if MONGO
                case Sqlx.OBJECT: return new TObjectId(buf.GetBytes());
#endif
                case Sqlx.PASSWORD: goto case Sqlx.CHAR;
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.INTEGER:
                    {
                        var o = rdr.GetInteger();
                        if (o is long)
                            return new TInt(this, (long)o);
                        return new TInteger(this, (Integer)o);
                    }
                case Sqlx.NUMERIC: return new TNumeric(this, rdr.GetDecimal());
                case Sqlx.REAL0: // merge with REAL (an anomaly happened between v5.0 and 5.5)
                case Sqlx.REAL: return new TReal(this, rdr.GetDouble());
                case Sqlx.DATE: return new TDateTime(this, rdr.GetDateTime());
                case Sqlx.TIME: return new TTimeSpan(this, new TimeSpan(rdr.GetLong()));
                case Sqlx.TIMESTAMP: return new TDateTime(this, new DateTime(rdr.GetLong()));
                case Sqlx.INTERVAL0: return new TInterval(this, rdr.GetInterval0()); //attempt backward compatibility
                case Sqlx.INTERVAL: return new TInterval(this, rdr.GetInterval());
                case Sqlx.ARRAY:
                    {
                        var dp = buf.GetLong();
                        var el = buf.pbase.FindDataType(ppos,buf.role, dp);
                        var a = new TArray(new SqlDataType(Sqlx.ARRAY,el));
                        var n = buf.GetInt();
                        for (int j = 0; j < n; j++)
                        {
                            var dt = el.GetDataType(buf, ppos);
                            a[j] = dt.Get(buf, ppos);
                        }
                        return a;
                    }
                case Sqlx.MULTISET:
                    {
                        var dp = buf.GetLong();
                        var el = buf.pbase.FindDataType(ppos, buf.role, dp);
                        var m = new TMultiset(buf.pbase.cnx,new SqlDataType(Sqlx.MULTISET,el));
                        var n = buf.GetInt();
                        for (int j = 0; j < n; j++)
                        {
                            var dt = el.GetDataType(buf, ppos);
                            m.Add(dt.Get(buf, ppos));
                        }
                        return m;
                    }
                case Sqlx.REF:
                    {
                        var dp = buf.GetLong();
                        var dt = buf.pbase.FindDataType(ppos, buf.role, dp);
                        var r = new TypedValue[dt.Length];
                        var n = buf.GetInt();
                        for (int j = 0; j < n; j++)
                        {
                            var c = buf.GetLong();
                            var pc = buf.pbase.GetS(c) as PColumn;
                            var nt = buf.pbase.FindDataType(ppos, buf.role, c);
                            var at = nt.GetDataType(buf, ppos);
                            r[pc.seq] = at.Get(buf, ppos);
                        }
                        return new TRow(dt, r);
                    } 
                case Sqlx.ROW: 
                    {
                        var dp = buf.GetLong();
                        var dt = buf.pbase.FindDataType(ppos,buf.role, dp);
                        var r = new TypedValue[dt.Length];
                        var n = buf.GetInt();
                        for (int j=0;j<n;j++)
                        {
                            var c = buf.GetLong();
                            var pc = buf.pbase.GetS(c) as PColumn;
                            var nt = buf.pbase.FindDataType(ppos,buf.role, c);
                            var at = nt.GetDataType(buf, ppos);
                            r[pc.seq]=at.Get(buf,ppos);
                        }
                        return new TRow(dt, r);
                    }
                case Sqlx.TYPE: goto case Sqlx.ROW;
            }
            throw new DBException("3D000", buf.pbase.name).ISO();
        }
 
#region IComparable Members
        /// <summary>
        /// compare the types (arbitrary order)
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public int CompareTo(object obj)
        {
            return (obj is SqlDataType s) ? str.CompareTo(s.str) : 1;
        }

#endregion
#if (!EMBEDDED)
        internal void PutType(AsyncStream s)
        {
            if (iri != null)
            {
                s.PutInt((int)Sqlx.TYPE_URI);
                s.PutString(iri);
            }
            if (kind==Sqlx.SENSITIVE)
            {
                s.PutInt((int)Sqlx.SENSITIVE);
                elType.PutType(s);
            }
            var ki = Equivalent(kind);
            s.PutInt((int)ki);
            switch (ki)
            {
                case Sqlx.INTEGER: s.PutInt(prec);
                    s.PutInt(scale);
                    break;
                case Sqlx.NUMERIC: goto case Sqlx.INTEGER;
                case Sqlx.REAL: goto case Sqlx.INTEGER;
                case Sqlx.CHAR:
                    s.PutInt(prec);
                    s.PutInt((int)charSet);
                    s.PutString(culture?.Name??"");
                    break;
                case Sqlx.PASSWORD: goto case Sqlx.CHAR;
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.DATE:
                    s.PutInt((int)start);
                    s.PutInt((int)end);
                    s.PutInt(prec);
                    s.PutInt(scale);
                    break;
                case Sqlx.TIME: goto case Sqlx.DATE;
                case Sqlx.TIMESTAMP: goto case Sqlx.DATE;
                case Sqlx.INTERVAL: goto case Sqlx.INTERVAL;
                case Sqlx.ROW:
                    s.PutInt(Length);
                    for (int j = 0; j < Length; j++)
                        columns[j].PutType(s);
                    break;
            }
            if (elType != null)
                elType.PutType(s);
        }
#endif

        internal bool Matches(SqlDataType sqlDataType)
        {
            return ToString() == sqlDataType.ToString();
        }
        internal int Typecode()
        {
            switch (Equivalent(kind))
            {
                case Sqlx.NULL: return 0;
                case Sqlx.INTEGER: return 1;
                case Sqlx.NUMERIC: return 2;
                case Sqlx.REAL: return 8;
                case Sqlx.NCHAR: return 3;
                case Sqlx.CHAR: return 3;
                case Sqlx.TIMESTAMP: return 4;
                case Sqlx.DATE: return 13;
                case Sqlx.BLOB: return 5;
                case Sqlx.ROW: return 6;
                case Sqlx.ARRAY: return 7;
                case Sqlx.MULTISET: return 7;
                case Sqlx.TABLE: return 7;
                case Sqlx.TYPE: return 12;
                case Sqlx.BOOLEAN: return 9;
                case Sqlx.INTERVAL: return 10;
                case Sqlx.TIME: return 11;
                case Sqlx.XML: return 3;
                case Sqlx.PERIOD: return 7;
                case Sqlx.PASSWORD: return 3;
            }
            return 0;
        }
    }
    internal class StandardDataType : SqlDataType
    {
        public static ATree<Sqlx, SqlDataType> standardTypes = BTree<Sqlx,SqlDataType>.Empty;
        public StandardDataType(Sqlx t)
            : base(t)
        {
            ATree<Sqlx, SqlDataType>.Add(ref standardTypes, t, this);
        }
    }
    internal class UnionType : SqlDataType
    {
        // a UNION: types must all be scalars
        internal SqlDataType[] types;
        internal UnionType() : base(Sqlx.UNION) { }
        internal UnionType(SqlDataType[] cs) : base(Sqlx.UNION)
        {
            types = cs;
            for (var i = 0; i < cs.Length; i++)
                if (cs[i].kind == Sqlx.ROW || cs[i].kind == Sqlx.TABLE)
                    throw new PEException("PE489");
        }
        protected UnionType(UnionType ut) : base(ut) { }
        protected UnionType(UnionType dt, bool nn, string dv) : base(dt, nn, dv) { }
        protected UnionType(UnionType dt, PCheck c) : base(dt, c) { }
        protected UnionType(UnionType ut, Sqlx a, Sqlx n) : base(ut, a, n) { }
        protected UnionType(UnionType ut, string sc) : base(ut, u) { }
        protected override void Check()
        {
            if (kind != Sqlx.UNION)
                throw new PEException("PE500");
        }
        internal override SqlDataType Copy()
        {
            return new UnionType(this);
        }
        internal override SqlDataType Copy(bool nn, string dv)
        {
            return new UnionType(this, nn, dv);
        }
        internal override SqlDataType Copy(PCheck c)
        {
            return new UnionType(this, c);
        }
        internal override SqlDataType Copy(Sqlx a, Sqlx n)
        {
            return new UnionType(this, a, n);
        }
        internal override SqlDataType Copy(string u)
        {
            return new UnionType(this, u);
        }
        internal override SqlDataType Copy(string u, string sc)
        {
            return new UnionType(this, u, sc);
        }
        public override bool CanTakeValueOf(SqlDataType dt)
        {
            if (dt.kind == Sqlx.VALUE || dt.kind == Sqlx.CONTENT)
                return true;
            if (dt is UnionType ut)
            {
                for (var i = 0; i < ut.Length; i++)
                    if (!CanTakeValueOf(ut.types[i]))
                        return false;
                return true;
            }
            int n = types.Length;
            for (int j = 0; j < n; j++)
                if (types[j].CanTakeValueOf(dt))
                    return true;
            return false;
        }
        internal override SqlDataType NotUnion(Sqlx k)
        {
            for (var i = 0; i < Length; i++)
                if (types[i].kind == k)
                    return types[i];
            throw new PEException("PE849");
        }
    }
    internal class OnlyType : SqlDataType
    {
        internal OnlyType(long s) : base(s,Sqlx.ONLY)
        {
        }
        internal OnlyType() : base(Sqlx.ONLY) { }
        protected OnlyType(OnlyType ut) : base(ut) { }
        protected OnlyType(OnlyType dt, bool nn, string dv) : base(dt, nn, dv) { }
        protected OnlyType(OnlyType dt, PCheck c) : base(dt, c) { }
        protected OnlyType(OnlyType ut, Sqlx a, Sqlx n) : base(ut, a, n) { }
        protected OnlyType(OnlyType ut, string u) : base(ut, u) { }
        protected OnlyType(OnlyType ut, string u, string sc) : base(ut, u, sc) { }
        protected override void Check()
        {
            if (kind != Sqlx.ONLY)
                throw new PEException("PE500");
        }
        internal override SqlDataType Copy()
        {
            return new OnlyType(this);
        }
        internal override SqlDataType Copy(bool nn, string dv)
        {
            return new OnlyType(this, nn, dv);
        }
        internal override SqlDataType Copy(PCheck c)
        {
            return new OnlyType(this, c);
        }
        internal override SqlDataType Copy(Sqlx a, Sqlx n)
        {
            return new OnlyType(this, a, n);
        }
        internal override SqlDataType Copy(string u)
        {
            return new OnlyType(this, u);
        }
        internal override SqlDataType Copy(string u, string sc)
        {
            return new OnlyType(this, u, sc);
        }
        public override int Compare(TypedValue a, TypedValue b)
        {
            return super.Compare(cx, a, b);
        }
        public override bool CanTakeValueOf(SqlDataType dt)
        {
            return dt.CompareTo(this) == 0;
        }
        public override TypedValue Parse(Transaction tr,string s)
        {
            return super.Parse(tr, s);
        }
        public override TypedValue Parse(Transaction tr, string s, string m)
        {
            return super.Parse(tr, s, m);
        }
        public override TypedValue Parse(Scanner lx, bool union = false)
        {
            return super.Parse(lx);
        }
        public override TypedValue Eval(Transaction tr, TypedValue v)
        {
            return super.Eval(tr, v);
        }
        public override TypedValue Eval(Transaction tr, TypedValue a, Sqlx op, TypedValue b)
        {
            return super.Eval(tr, a, op, b);
        }
    }
    internal class RowType : SqlDataType
    {
        internal RowType() : base(Sqlx.ROW) { }
        internal RowType(Sqlx k) :base(k) { } 
        protected RowType(RowType rt) : base(rt) { }
        protected RowType(RowType rt, bool nn, string dv) : base(rt, nn, dv) { }
        protected RowType(RowType rt, PCheck c) : base(rt, c) { }
        protected RowType(RowType rt, Sqlx a, Sqlx n) : base(rt, a, n) { }
        protected RowType(RowType rt, string u) : base(rt, u) { }
        protected RowType(RowType rt, string u, string sc) : base(rt, u, sc) { }
        /// <summary>
        /// This is called during parsing by the ArgList constructors where we have strong expectations on the SqlValues supplied.
        /// Some of these may be flexible e.g. subqueries or other union types: we need to constrain them.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="s"></param>
        public RowType(Context cx, SqlValue[] s)
            : base(Sqlx.ROW)
        {
            columns = new SqlDataType[s.Length];
            for (int i = 0; i < s.Length; i++)
                if (s[i] is SqlValue c)
                {
                    columns[i] = c.nominalDataType;
                    names.Add(c.alias ?? c.name);
                }
        }
        internal RowType(Sqlx k, SqlDataType[] cls, Ident[] nms) : base(k)
        {
            columns = cls;
            for (var i = 0; i < nms.Length; i++)
                names.Add(nms[i]);
        }
        public RowType(params TColumn[] kc) : base(Sqlx.ROW)
        {
            columns = new SqlDataType[kc.Length];
            for (int i = 0; i < kc.Length; i++)
            {
                var dt = kc[i].typedValue?.dataType;
                if (dt.kind == Sqlx.Null)
                    dt = kc[i].nominalDataType;
                columns[i] = dt;
                names.Add(kc[i].name);
            }
        }
        internal RowType(Query q) : base(Sqlx.ROW)
        {
            columns = new SqlDataType[q.Size];
            for (int i = 0; i < q.Size; i++)
                if (q.cols[i].nominalDataType is SqlDataType dt)
                    columns[i] = dt;
                else
                    throw new PEException("PE255");
            names = q.names;
        }
        public RowType(List<SqlDataType> fs) :base(Sqlx.ROW)
        {
            columns = fs.ToArray();
        }
        protected override void Check()
        {
            if (kind != Sqlx.ROW && kind != Sqlx.TYPE)
                throw new PEException("PE500");
        }
        internal override SqlDataType Copy()
        {
            return new RowType(this);
        }
        internal override SqlDataType Copy(bool nn, string dv)
        {
            return new RowType(this,nn, dv);
        }
        internal override SqlDataType Copy(PCheck c)
        {
            return new RowType(this,c);
        }
        internal override SqlDataType Copy(Sqlx a, Sqlx n)
        {
            return new RowType(this,a, n);
        }
        internal override SqlDataType Copy(string u)
        {
            return new RowType(this,u);
        }
        internal override SqlDataType Copy(string u, string sc)
        {
            return new RowType(this, u, sc);
        }
    }
    internal class RoleType : SqlDataType
    {
        public RoleType(long d,long p,Grant.Privilege priv):base(Sqlx.ROLE,d,p,0,0,0,Sqlx.NO,Sqlx.NO,Sqlx.NO,Sqlx.NO,CharSet.UCS,
            CultureInfo.InvariantCulture,0,"","",null,PColumn.GenerationRule.No,OrderCategory.None,BTree<string,long>.Empty,
            priv)
            { }
    }
    /// <summary>
    /// A TableType cannot have dotted column names
    /// </summary>
    internal class TableType : RowType
    {
        public TableType(Sqlx kind) : base(kind) { }
        public new static readonly TableType Table = new TableType(Sqlx.TABLE);
        protected TableType(TableType rt) : base(rt) { }
        protected TableType(TableType rt, bool nn, string dv) : base(rt, nn, dv) { }
        protected TableType(TableType rt, PCheck c) : base(rt, c) { }
        protected TableType(TableType tt, Sqlx a, Sqlx n) : base(tt, a, n) { }
        protected TableType(TableType tt, string u) : base(tt, u) { }
        protected TableType(TableType tt, string u, string sc) : base(tt, u, sc) { }
        public TableType(Lexer lx, Sqlx t, List<RowTypeColumn> rt) : base(t)
        {
            columns = new SqlDataType[rt.Count];
            for (int i = 0; i < rt.Count; i++)
            {
                var rc = rt[i];
                columns[i] = rc.type;
                if (rc.type == null)
                    throw new PEException("PE255");
                names.Add(rc.name.ForTableType());
            }
        }
        internal TableType(Query q) : base(Sqlx.ROW)
        {
            columns = new SqlDataType[q.Size];
            for (int i = 0; i < q.Size; i++)
                if (q.cols[i].nominalDataType is SqlDataType dt)
                    columns[i] = dt;
                else
                    throw new PEException("PE255");
            names = q.names.ForTableType();
        }
        internal TableType(SqlDataType dt) : base(Sqlx.ROW)
        {
            columns = dt.columns;
            names = dt.names.ForTableType();
        }
        /// <summary>
        /// This is called during parsing by the ArgList constructors where we have strong expectations on the SqlValues supplied.
        /// Some of these may be flexible e.g. subqueries or other union types: we need to constrain them.
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="s"></param>
        public TableType(Context cx, SqlValue[] s)
            : base(Sqlx.ROW)
        {
            columns = new SqlDataType[s.Length];
            for (int i = 0; i < s.Length; i++)
                if (s[i] is SqlValue c)
                {
                    if (c.nominalDataType is SqlDataType dt)
                        columns[i] = dt;
                    else
                        throw new PEException("PE255");
                    names.Add(c.NameForRowType());
                }
        }
        public TableType(Context cx, ATree<long, SqlValue> s) : base(Sqlx.ROW)
        {
            columns = new SqlDataType[s.Count];
            for (int i = 0; i < s.Count; i++)
                if (s[i] is SqlValue c)
                {
                    if (c.nominalDataType is SqlDataType dt)
                        columns[i] = dt;
                    else
                        throw new PEException("PE255");
                    names.Add(c.NameForRowType());
                }
        }
        public TableType(SqlDataType[] cls, Ident[] nms) : base(Sqlx.ROW)
        {
            columns = cls;
            for (var i = 0; i < nms.Length; i++)
                names.Add(nms[i].ForTableType());
        }
        public TableType(Database db, long dp, ATree<Ident, long?> props) : base(Sqlx.TABLE)
        {
            columns = new SqlDataType[props.Count];
            for (var cp = props.First(); cp != null; cp = cp.Next())
                if (cp.value().HasValue && (db.objects[(long)cp.value()] is TableColumn tc)) // will be null for ColumnPath
                {
                    if (tc.DataType(db) is SqlDataType dt)
                        columns[tc.seq] = dt;
                    else
                        throw new PEException("PE255");
                    names[tc.seq] = cp.key().ForTableType();
                }
        }
        public TableType(params TColumn[] kc) : base(Sqlx.ROW)
        {
            columns = new SqlDataType[kc.Length];
            for (int i = 0; i < kc.Length; i++)
            {
                if (kc[i].typedValue?.dataType is SqlDataType dt)
                {
                    if (dt.kind == Sqlx.Null)
                        dt = kc[i].nominalDataType;
                    columns[i] = dt;
                }
                else
                    throw new PEException("PE255");
                names.Add(kc[i].name.ForTableType());
            }
        }
        public TableType(Ident now, SqlValue was, SqlDataType dt) : base(Sqlx.ROW)
        {
            kind = Sqlx.ROW;
            columns = dt.columns;
            for (int i = 0; i < dt.Length; i++)
            {
                if (dt[i] == null)
                    throw new PEException("PE255");
                if (dt.names[i].CompareTo(was.name) == 0)
                    names.Add(now);
                else
                    names.Add(dt.names[i]);
            }
        }
        /// <summary>
        /// Make a local copy of the RowType (for the current transaction)
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="f"></param>
        public TableType(RowType dt, From f) : base(dt)
        {
            for (int i = 0; i < dt.Length; i++)
                if (dt[i] == null)
                    throw new PEException("PE255");
            names = new Idents(names, f);
        }
        protected override void Check()
        {
            if (kind != Sqlx.TABLE && kind != Sqlx.ROW)
                throw new PEException("PE500");
        }
        internal override SqlDataType Copy()
        {
            return new TableType(this);
        }
        internal override SqlDataType Copy(bool nn, string dv)
        {
            return new TableType(this, nn, dv);
        }
        internal override SqlDataType Copy(PCheck c)
        {
            return new TableType(this, c);
        }
        internal override SqlDataType Copy(Sqlx a, Sqlx n)
        {
            return new TableType(this, a, n);
        }
        internal override SqlDataType Copy(string u)
        {
            return new TableType(this, u);
        }
        internal override SqlDataType Copy(string u, string sc)
        {
            return new TableType(this, u, sc);
        }
    }
    /// <summary>
    /// A class for RdfLiterals
    /// </summary>
    internal class RdfLiteral : TChar
    {
        public object val; // the binary version
        public bool name; // whether str matches val
        public RdfLiteral(SqlDataType t, string s, object v, bool c) :base(t,s)
        {
            val = v;
            name = c;
        }
        internal static RdfLiteral New(SqlDataType it, string v)
        {
            if (it.iri == IriRef.STRING || v == "")
                return new RdfLiteral(it, v, "", false); // non-name to supply datatype for strings
            return new RdfLiteral(it, v, v, false);
        }
        public override string ToString()
        {
            return base.ToString();
        }
    }
     /// <summary>
    /// </summary>
    internal class IriRef
    {
        private IriRef(){}
        public readonly static string xsd = "http://www.w3.org/2001/XMLSchema#";
        public readonly static string BOOL = xsd + "boolean";
        public readonly static string INTEGER = xsd + "integer";
        public readonly static string INT = xsd + "int";
        public readonly static string LONG = xsd + "long";
        public readonly static string SHORT = xsd + "short";
        public readonly static string BYTE = xsd + "byte";
        public readonly static string UNSIGNEDINT = xsd + "unsignedInt";
        public readonly static string UNSIGNEDLONG = xsd + "unsignedLong";
        public readonly static string UNSIGNEDSHORT = xsd + "unsignedShort";
        public readonly static string UNSIGNEDBYTE = xsd + "unsignedByte";
        public readonly static string NONPOSITIVEINTEGER = xsd + "nonPositiveInteger";
        public readonly static string NEGATIVEINTEGER = xsd + "negativeInteger";
        public readonly static string NONNEGATIVEINTEGER = xsd + "nonNegativeInteger";
        public readonly static string POSITIVEINTEGER = xsd + "positiveInteger";
        public readonly static string DECIMAL = xsd + "decimal";
        public readonly static string FLOAT = xsd + "float";
        public readonly static string DOUBLE = xsd + "double";
        public readonly static string STRING = xsd + "string";
        public readonly static string DATETIME = xsd + "dateTime";
        public readonly static string DATE = xsd + "date";
     }
    internal class Period
    {
        public   TypedValue start, end;
        public Period(  TypedValue s,   TypedValue e)
        {
            start = s; end = e;
        }
        public Period(Period p) : this(p.start,p.end) { }
        public override string ToString()
        {
            return "period("+start.ToString()+","+end.ToString()+")";
        }
    }
}
