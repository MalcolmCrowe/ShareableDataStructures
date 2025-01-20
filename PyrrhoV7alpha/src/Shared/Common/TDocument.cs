using System.Text;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Common
{
    /// <summary>
    /// This class is addedd as inspired by MongoDB.
    /// BSON format is used for physical storage and ADO.NET.
    /// Json format is used in SQL.
    /// Alas: Json format has positional fields, so we can't use simple string->Column tree
    /// IMMUTABLE - all fields are private
    ///     
    /// </summary>
    internal class TDocument : TypedValue
    {
        private enum ParseState { StartKey, Key, Colon, StartValue, Comma }
        readonly CList<(string, TypedValue)> content = CList<(string, TypedValue)>.Empty;
        readonly CTree<string, int> names = CTree<string, int>.Empty;
        internal static TDocument Null = new();
        public static string _id = "_id";
        TDocument() : base(Domain.Document)
        { }
        protected TDocument(Domain dt,TDocument d) :base(dt)
        {
            content = d.content; names = d.names;
        }
      
        internal TDocument(CList<(string,TypedValue)> c,CTree<string,int> n) :base(Domain.Document)
        {
            content = c; names = n;
        }
        internal TDocument(Context cx,TRow r, string? id = null) :this()
        {
            var c = CList<(string, TypedValue)>.Empty;
            var n = CTree<string, int>.Empty;
            if (id != null)
            {
                c += (_id, new TChar(id));
                n += (_id, 0);
            }
            for (var b = r.dataType.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is QlValue v &&
                        v.infos[cx.role.defpos] is ObInfo vi && vi.name is not null)
                {
                    n += (vi.name, (int)n.Count);
                    c += (vi.name, r[p]);
                }
            content = c;
            names = n;
        }
        internal TDocument(TDocument d, params (string, TypedValue)[] vs) : base(Domain.Document)
        {
            var c = d.content;
            var ns = d.names;
            foreach (var v in vs)
            {
                var (n, tv) = v;
                if (ns.Contains(n))
                    c = new CList<(string, TypedValue)>(c, ns[n], v);
                else
                {
                    ns += (n, (int)ns.Count);
                    c += (n, tv);
                }
            }
            content = c;
            names = ns;
        }
        internal new ABookmark<int,(string,TypedValue)>? First()
        {
            return content?.First();
        }
        /// <summary>
        /// This ghastly method is only used when importing objects of the Document class.
        /// It is not used for normal database processing
        /// </summary>
        /// <param name="o"></param>
        /// <returns></returns>
        internal static TypedValue GetValue(object o)
        {
            if (o == null || o is DBNull)
                return TNull.Value;
            else if (o is string @string)
                return new TChar(@string);
            else if (o is int @int)
                return new TInt(@int);
            else if (o is long int1)
                return new TInt(int1);
            else if (o is double @double)
                return new TReal(@double);
            else if (o is DateTime time)
                return new TDateTime(time);
            else if (o is byte[] v)
                return new TBlob(v);
#if EMBEDDED
            else if (o is Document)
                return new TDocument((Document)o);
            else if (o is DocArray)
                return new TDocArray((DocArray)o);
#if MONGO
            else if (o is ObjectId)
                return new TObjectId((ObjectId)o);
#endif
            else
                throw new DocumentException("Unexpected value type "+o.GetType().Name);
#else
            return TNull.Value;
#endif
        }
        /// <summary>
        /// Get a value from Json format
        /// </summary>
        /// <param name="s"></param>
        /// <param name="n"></param>
        /// <param name="i">afterwards will be the position just after the value</param>
        /// <returns></returns>
        internal static (string,TypedValue) GetValue(string nm,string s, int n, ref int i)
        {
            if (i < n)
            {
                var c = s[i - 1];
                if (c == '"' || c == '\'' || c == '$' || char.IsLetter(c))
                    return (nm, new TChar(GetString(c, s, n, ref i)));
                if (i + 3 < n && char.IsDigit(c) && s[i + 1] == '/')
                    return (nm, new TDateTime(DateTime.Parse(GetString(c, s, n, ref i))));
                if (c == '{')
                {
                    var d = new TDocument();
                    i = Fields(ref d,s, i, n);
#if MONGO
                    if (d.Contains("$regex"))
                    {
                        var re = d["$regex"];
                        if (re.dataType.kind == Sqlx.CHAR)
                        {
                            var rv = "/" + re.ToString().Trim() + "/";
                            var ro = d["$options"];
                            if (ro != null)
                                rv = rv + ro.ToString().Trim();
                            return new TColumn( nm, new TChar(SqlDataType.Regex, rv));
                        }
                    }
#endif
                    return (nm, d);
                }
                if (c == '[')
                {
                    var d = new TDocArray();
                    i = TDocArray.Fields(ref d, s, i, n);
                    return (nm, d);
                }
#if MONGO
                if (c == '/')
                {
                    var st = i - 1;
                    while (i < n && s[i] != '/')
                        i++;
                    while (i < n && s[i] != ',' && s[i] != '}')
                        i++;
                    return (nm, new TChar(SqlDataType.Regex, s.Substring(st, i - st).Trim()));
                }
#endif
                if (i + 4 < n && s.Substring(i - 1, 4) == "true")
                {
                    i += 3;
                    return (nm, TBool.True);
                }
                if (i + 5 < n && s.Substring(i - 1, 5) == "false")
                {
                    i += 4;
                    return (nm, TBool.False);
                }
                if (i + 4 < n && s.Substring(i - 1, 4) == "null")
                {
                    i += 3;
                    return ("null",TNull.Value);
                }
                var sg = c == '-';
                if (sg && i < n)
                    c = s[i++];
                var whole = 0L;
                if (c != '0' && Char.IsDigit(c))
                {
                    i--;
                    whole = GetHex(s, n, ref i);
                    while (i < n && Char.IsDigit(s[i]))
                        whole = whole * 10 + GetHex(s, n, ref i);
                }
                else if (c != '0')
                    goto bad;
                if (i >= n || (s[i] != '.' && s[i] != 'e' && s[i] != 'E'))
                    return (nm, new TInt(sg ? -whole : whole));
                int scale = 0;
                if (s[i] == '.')
                {
                    if (++i >= n || !Char.IsDigit(s[i]))
                        throw ParseException("decimal part expected");
                    while (i < n && Char.IsDigit(s[i]))
                    {
                        whole = whole * 10 + GetHex(s, n, ref i);
                        scale++;
                    }
                }
                if (i >= n || (s[i] != 'e' && s[i] != 'E'))
                    return (nm, new TNumeric(Domain._Numeric, new Numeric(new Integer(whole), scale)));
                if (++i >= n)
                    throw ParseException("exponent part expected");
                var esg = s[i] == '-';
                if ((s[i] == '-' || s[i] == '+') && (++i >= n || !Char.IsDigit(s[i])))
                    throw ParseException("exponent part expected");
                var exp = 0;
                while (i < n && Char.IsDigit(s[i]))
                    exp = exp * 10 + GetHex(s, n, ref i);
                if (esg)
                    exp = -exp;
                return (nm, new TReal(whole * Math.Pow(10.0, exp - scale)));
            }
        bad:
            throw ParseException("Value expected at " + (i - 1));
        }

        /// <summary>
        /// Parser from typed Json to Document
        /// </summary>
        /// <param name="tr">tr if non-null can be used for _id generation</param>
        /// <param name="s">Json format</param>
        internal TDocument(string s) : base(Domain.Document)
        {
            if (s == null)
                return;
            s = s.Trim();
            int n = s.Length;
            if (s.Length == 0 || (s[0] != '{' && s[0] != '<'))
                throw ParseException("{ or < expected");
            var d = new TDocument();
            var i = Fields(ref d,s, 1, n);
            if (i != n)
                throw ParseException("unparsed input at " + (i - 1));
            content = d.content;
            names = d.names;
        }
        internal TDocument(Register r) : base(Domain.Document)
        {
            var c = CList<(string, TypedValue)>.Empty;
            TypedValue tv = new TInt(r.count);
            if (r.sb != null)
                tv = new TChar(r.sb.ToString());
            else if (r.acc != null)
                tv = r.acc;
            else if (r.mset != null)
            {
                var a = new TDocument();
                var n = 0;
                for (var b = r.mset.tree.First(); b != null; b = b.Next())
                    for (var d = 0; d < b.value(); d++)
                        a.Add((++n).ToString(), b.key());
                tv = a;
            }
            else if (r.row >= 0)
                tv = new TInt(r.row);
            else
            {
                switch (r.sumType.kind)
                {
                    case Qlx.INT:
                    case Qlx.INTEGER:
                        if (r.sumInteger is Integer im)
                            tv = new TInteger(im);
                        else
                            tv = new TInt(r.sumLong);
                        break;
                    case Qlx.REAL:
                        tv = new TReal(r.sum1); break;
                    case Qlx.NUMERIC:
                        tv = new TNumeric(r.sumDecimal ?? throw new PEException("PE0811")); break;
                    case Qlx.BOOLEAN:
                        tv = TBool.For(r.bval); break;
                }
            }
            c += (r.count.ToString(), tv);
            if (r.acc1 != 0.0)
                c += ("acc1", new TReal(r.acc1));
            content = c;
        }
        internal static (TDocument,int) New(string s,int off)
        {
            var d = new TDocument();
            var i = Fields(ref d, s, off, s.Length);
            return (d, i);
        }
        internal TDocument Add(string n, TypedValue tv)
        {
            if (names.Contains(n))
                return new TDocument(new CList<(string, TypedValue)>(content, names[n], (n, tv)),
                    names);
            return new TDocument(content + (n, tv), names+(n,(int)names.Count));
        }
        internal override TypedValue this[string n]
        {
            get
            {
               if (n == null) return this;
               var nk = names[n];
               return (nk>=0L)?content[nk].Item2 :TNull.Value;
            }
        }
        internal bool GetBool(string n, bool def)
        {
            if (this[n].ToBool() is not bool v)
                return def;
            return v;
        }
        internal TDocument Remove(string n)
        {
            if (!Contains(n))
                return this;
            var r = new TDocument();
            for (int i = 0; i < content.Count; i++)
            {
                var (nm, tv) = content[i];
                if (nm.CompareTo(n) != 0)
                    r = r.Add(nm,tv);
            }
            return r;
        }
        /// <summary>
        /// Parse the contents of {} or []
        /// </summary>
        /// <param name="doc">The document so far</param>
        /// <param name="s">the string</param>
        /// <param name="i">the start of the fields</param>
        /// <param name="n">the end of the string</param>
        /// <returns>the position just after the } or ]</returns>
        static int Fields(ref TDocument doc, string s, int i, int n)
        {
            ParseState state = ParseState.StartKey;
            StringBuilder? kb = null;
            char qu = '"';
            while (i < n)
            {
                var c = s[i++];
                switch (state)
                {
                    case ParseState.StartKey:
                        kb = new StringBuilder();
                        if (char.IsWhiteSpace(c))
                            continue;
                        if (c == '}' && doc.content.Count == 0)
                            return i;
                        qu = '\0';
                        if (c == '"' || c == '\'')
                            qu = c;
                        else if (!Char.IsLetter(c) && c != '$' && c!='_')
                            throw ParseException("Expected quote or start of key");
                        else
                            kb.Append(c);
                        state = ParseState.Key;
                        continue;
                    case ParseState.Key:
                        if (qu == '\0' && c != '$' && !Char.IsLetterOrDigit(c))
                        {
                            state = (c == ':') ? ParseState.StartValue : ParseState.Colon;
                            continue;
                        }
                        if (c == qu)
                        {
                            state = ParseState.Colon;
                            continue;
                        }
                        if (c == '\\')
                            c = GetEscape(s, n, ref i);
                        kb?.Append(c);
                        continue;
                    case ParseState.Colon:
                        if (char.IsWhiteSpace(c))
                            continue;
                        if (c != ':')
                            throw ParseException("Expected : at " + (i - 1));
                        state = ParseState.StartValue;
                        continue;
                    case ParseState.StartValue:
                        if (char.IsWhiteSpace(c))
                            continue;
                        if (c == ']' && doc.content.Count == 0)
                            return i;
                        var key = kb?.ToString()??"";
                        doc = doc.Add(key, GetValue(key, s, n, ref i).Item2);
                        state = ParseState.Comma;
                        continue;
                    case ParseState.Comma:
                        if (char.IsWhiteSpace(c))
                            continue;
                        if (c == '}')
                            return i;
                        if (c != ',')
                            throw ParseException("Expected , at " + (i - 1));
                        state = ParseState.StartKey;
                        continue;
                }
            }
            throw ParseException("Incomplete syntax at " + (i - 1));
        }
        /// <summary>
        /// Helper for exceptions when parsing documents
        /// </summary>
        /// <param name="m">a message</param>
        /// <returns>the exception</returns>
        static DBException ParseException(string m)
        {
            return new DBException("22000", m).Pyrrho();
        }
        static string GetString(char qu, string s, int n, ref int i)
        {
            var sb = new StringBuilder();
            if (qu != '"' && qu != '\'')
            {
                sb.Append(qu);
                qu = '\0';
            }
            while (i < n)
            {
                var c = s[i];
                if (qu == '\0' && !char.IsLetter(c))
                    return sb.ToString();
                i++;
                if (c == qu)
                    return sb.ToString();
                if (c == '\\')
                    c = GetEscape(s, n, ref i);
                sb.Append(c);
            }
            throw ParseException("Non-terminated string at " + (i - 1));
        }
        static char GetEscape(string s, int n, ref int i)
        {
            if (i < n)
            {
                var c = s[i++];
                switch (c)
                {
                    case '"': return c;
                    case '\\': return c;
                    case '/': return c;
                    case 'b': return '\b';
                    case 'f': return '\f';
                    case 'n': return '\n';
                    case 'r': return '\r';
                    case 't': return '\t';
                    case 'u':
                        {
                            int v = 0;
                            for (int j = 0; j < 4; j++)
                                v = (v << 4) + GetHex(s, n, ref i);
                            return (char)v;
                        }
                    case 'U': goto case 'u';
                }
            }
            throw ParseException("Illegal escape");
        }
        internal static int GetHex(string s, int n, ref int i)
        {
            if (i < n)
            {
                var c = s[i++];
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return (c - 'a') + 10;
                if (c >= 'A' && c <= 'F') return (c - 'A') + 10;
            }
            throw ParseException("Hex digit expected at " + (i - 1));
        }
        /// <summary>
        /// Parser from BSON to Document
        /// </summary>
        /// <param name="b"></param>
        internal TDocument(byte[] b, ref int off) :base(Domain.Document)
        {
            var nbytes = GetLength(b, off);
            var i = off + 4;
            var d = new TDocument();
            while (i < off + nbytes - 1) // ignoring the final \0
            {
                var t = b[i++];
                var c = 0;
                var s = i;
                while (i < off + nbytes && b[i++] != 0)
                    c++;
                var key = Encoding.UTF8.GetString(b, s, c);
                d=d.Add(key, GetValue(key, t, b, ref i).Item2);
            }
            off += nbytes;
            content = d.content;
            names = d.names;
        }
        internal static (string,TypedValue) GetValue(string nm, byte t, byte[] b, ref int i)
        {
            (string, TypedValue) tv;
            switch (t)
            {
                case 1:
                    {
                        tv = (nm, new TReal(BitConverter.ToDouble(b, i)));
                        i += 8;
                        break;
                    }
                case 2:
                    {
                        var n = GetLength(b, i);
                        i += 4;
                        tv = (nm, new TChar(Encoding.UTF8.GetString(b, i, n - 1)));
                        i += n;
                        break;
                    }
                case 3:
                    {
                        tv = (nm, new TDocument(b, ref i));
                        break;
                    }
                case 4:
                    {
                        tv = (nm, new TDocArray(b, ref i));
                        break;
                    }
                case 5:
                    {
                        var n = GetLength(b, i);
                        var r = new byte[n];
                        for (int j = 0; j < n; j++)
                            r[j] = b[i + j + 4];
                        tv = (nm, new TBlob(r));
                        i += n;
                        break;
                    }
                case 6:
                    {
                        tv = (nm, TNull.Value);
                        break;
                    }
                case 8:
                    {
                        tv = (nm,TBool.For(b[i] == 0));
                        i++;
                        break;
                    }
                case 9:
                    {
                        tv = (nm, new TDateTime(new DateTime(BitConverter.ToInt64(b, i))));
                        i += 8;
                        break;
                    }
                case 10:
                    {
                        tv = (nm, TNull.Value);
                        break;
                    }
                case 13:
                    {
                        var c = 0;
                        var s = i;
                        while (b[i++] != 0)
                            c++;
                        tv = (nm, new TChar(Domain.JavaScript, Encoding.UTF8.GetString(b, s, c)));
                        break;
                    }
                    case 16:
                    {
                        tv = (nm, new TInt(Domain.Int, (long)BitConverter.ToInt32(b, i)));
                        i += 4;
                        break;
                    } 
                case 18:
                    {
                        tv = (nm, new TInt(BitConverter.ToInt64(b, i)));
                        i += 8;
                        break;
                    }
                case 19: // decimal type added for Pyrrho
                    {
                        var n = GetLength(b, i);
                        i += 4;
                        var s = Encoding.UTF8.GetString(b, i, n - 1);
                        i += n;
                        var ix = s.IndexOf('.');
                        if (ix < 0)
                            tv = (nm, new TInteger(Integer.Parse(s)));
                        else
                            tv = (nm, new TNumeric(Numeric.Parse(s)));
                        break;
                    }
                default:
                    throw new PEException("PE387");
            }
            return tv;
        }
        internal static void Field((string,TypedValue) v, StringBuilder sb)
        {
            if (v.Item2 == null)
                sb.Append("<null>");
            else
                switch (v.Item2.dataType.kind)
                {
                    case Qlx.CONTENT: sb.Append('"'); sb.Append(v); sb.Append('"'); break;
                    case Qlx.DOCARRAY: sb.Append('[');
                        var d = (TDocArray)v.Item2;
                        var comma = "";
                        for (int i = 0; i < d.Count; i++)
                        {
                            sb.Append(comma);
                            comma = ", ";
                            Field((""+i,d[i]), sb);
                        }
                        sb.Append(']');
                        break;
                    case Qlx.CHAR:
                        sb.Append('\'');
                        sb.Append(v.Item2.ToString());
                        sb.Append('\'');
                        break;
                    default:
                        sb.Append(v.Item2.ToString());
                        break;
                }
        }
        public override string ToString()
        {
            var sb = new StringBuilder("{");
            var comma = "";
            for (var f = content.First(); f != null; f = f.Next())
            {
                var (n, v) = f.value();
                if (v != null && v != TNull.Value)
                {
                    sb.Append(comma); comma = ", ";
                    sb.Append('"');
                    sb.Append(n);
                    sb.Append("\": ");
                    Field(f.value(), sb);
                }
            }
            sb.Append('}');
            return sb.ToString();
        }
        internal static int GetLength(byte[] b, int off)
        {
            return b[off] + (b[off + 1] << 8) + (b[off + 2] << 16) + (b[off + 3] << 24);
        }
        internal static void SetLength(List<byte> r)
        {
            var n = r.Count;
            r[0] = (byte)(n & 0xff);
            r[1] = (byte)((n >> 8) & 0xff);
            r[2] = (byte)((n >> 16) & 0xff);
            r[3] = (byte)((n >> 24) & 0xff);
        }
        internal static byte[] GetBytes(TypedValue v)
        {
            if (v == TNull.Value)
                return [];
            switch (v.dataType.kind)
            {
                case Qlx.REAL:
                    return BitConverter.GetBytes(((TReal)v).dvalue);
                case Qlx.CHAR:
                    return ToBytes(v.ToString());
                case Qlx.DOCUMENT:
                    {
                        var doc = v as TDocument;
                        doc ??= new TDocument();
                        return doc.ToBytes(null);
                    }
                case Qlx.DOCARRAY:
                    {
                        var d = (TDocArray)v;
                        var r = new List<byte>
                        {
                            0,
                            0,
                            0,
                            0
                        };
                        for (int i = 0; i < d.Count; i++)
                        {
                            var k = "" + i;
                            var e = d[i];
                            r.Add(e.BsonType());
                            var b = Encoding.UTF8.GetBytes(k);
                            foreach (var a in b)
                                r.Add(a);
                            r.Add(0);
                            foreach (var a in GetBytes(e))
                                r.Add(a);
                        }
                        r.Add(0);
                        SetLength(r);
                        return [.. r];
                    }
                case Qlx.BLOB:
                    {
                        var b = (TBlob)v;
                        var r = new byte[b.Length];
                        for (var i = 0; i < b.Length; i++)
                            r[i] = b[i];
                        return r;
                    }
                case Qlx.BOOLEAN:
                    return [(byte)((v.ToBool() == false)?0:1)];
                case Qlx.TIMESTAMP:
                    return BitConverter.GetBytes(v.ToLong()??-1L);
                case Qlx.NUMERIC:
                    return ToBytes(v.ToString());
                case Qlx.INTEGER:
                    if (v is TInteger integer)
                    {
                        var iv = integer.ivalue;
                        if (iv.BitsNeeded() > 64)
                            return ToBytes(iv.ToString());
                        return BitConverter.GetBytes((long)iv);
                    }
                    return BitConverter.GetBytes(v.ToInt()??0);
            }
            return [];
        }
        static byte[] ToBytes(string s)
        {
            var b = Encoding.UTF8.GetBytes(s);
            var n = b.Length + 1;
            var r = new byte[n + 4];
            r[0] = (byte)(n & 0xff);
            r[1] = (byte)((n >> 8) & 0xff);
            r[2] = (byte)((n >> 16) & 0xff);
            r[3] = (byte)((n >> 24) & 0xff);
            for (int i = 0; i < n - 1; i++)
                r[i + 4] = b[i];
            r[n + 3] = 0;
            return r;
        }
        internal byte[] ToBytes(TDocument? proj)
        {
            var r = new List<byte>
            {
                0,
                0,
                0,
                0
            };
            var exclude = false;
            var excludeid = false;
            if (proj != null)
                for (var p=proj.content.First();p!= null;p=p.Next())
                {
                    if (IsZero(p.value()))
                    {
                        if (p.value().Item1 == "_id")
                            excludeid = true;
                        else
                            exclude = true;
                    }
                }
            for (var f=content.First();f!= null;f=f.Next())
            {
                if (f.value().Item1 == "_id" && excludeid)
                    continue;
                if (proj is not null)
                {
                    var fv = proj[f.value().Item1];
                    if ((fv == null) != exclude)
                        continue;
                }
                r.Add(f.value().Item2.BsonType());
                var b = Encoding.UTF8.GetBytes(f.value().Item1.ToString());
                foreach (var a in b)
                    r.Add(a);
                r.Add(0);
                foreach (var a in GetBytes(f.value().Item2))
                    r.Add(a);
            }
            r.Add(0);
            SetLength(r);
            return [.. r];
        }

        static bool IsZero((string,TypedValue) fv)
        {
            switch (fv.Item2.dataType.kind)
            {
                case Qlx.INTEGER:
                case Qlx.INT:
                    if (fv.Item2.ToInteger() is Integer iv && iv==Integer.Zero)
                        return true;
                    break;
                case Qlx.REAL:
                    if (fv.Item2.ToDouble()== 0.0)
                        return true;
                    break;
                case Qlx.Null:
                    return true;
            }
            return false;
        }
        /// <summary>
        /// In MONGO, Assignment of Documents is "always special, using $ keywords" but the jstest suite has some simpler ones.
        /// </summary>
        /// <param name="b">The updategram</param>
        /// <returns>the updated document</returns>
        internal TDocument Update(TDocument b)
        {
            var d = new TDocument();
            d.Add(_id, this[_id]);
            for (var cf = b.content.First(); cf != null; cf = cf.Next())
            {
                var (n, tv) = cf.value();
                if (n != "_id")
                    d = d.Add(n,tv);
            }
            return d;
        }
        /// <summary>
        /// In the absence of $ operators this should work like a comparison function.
        /// However, we also use this function to match against a template, so
        /// we tolerate the absence in b of fields in this.
        /// </summary>
        /// <param name="b">Document to compare with</param>
        /// <returns></returns>
        internal int RowSet(TypedValue b)
        {
            for (var f=content.First();f!= null;f=f.Next())
                        if (RowSet(f.value().Item1,f.value().Item2, b)!=0)
                            return -1;
            return 0;
        }

        static int RowSet(string nm,TypedValue a, TypedValue b)
        {
            var ki = b.dataType.kind;
            if (ki != Qlx.DOCARRAY && ki != Qlx.DOCUMENT)
                return -1;
            var vb = b[nm];
            vb ??= TNull.Value;
            if (a == TNull.Value && b == TNull.Value)
                return 0;
            if (a is not TDocument ef)
            {
                var c = a.CompareTo(vb);
                if (c != 0)
                    return c;
                return 0;
            }
            for (var eef = ef.content.First();eef != null;eef=eef.Next())
            {
                var c = a.CompareTo(vb);
                if (c != 0)
                    return c;
            }
            return 0;
        }
        internal TDocument Add(string n, string v)
        {
            return Add(n, new TChar(v));
        }
        public bool Contains(string n)
        {
            return names.Contains(n);
        }
    }
    /// <summary>
    /// We can calculate a Delta between two documents W,N if
    /// a) they have the same _Id
    /// b) the tree of fields can be merged, that is for names A and B
    /// if W.colNames[A] lt W.colNames[B] then N.colNames[A] lt colNames[B]
    ///     
    /// </summary>
    internal class Delta : TypedValue
    {
        internal enum Verb { Add,Change,Delta,Remove,All }
        
        internal class Action
        {
            public readonly int ix;
            public readonly Verb how;
            public readonly string name;
            public readonly TypedValue what;
            public Action(int i, Verb h, string n, TypedValue w)
            { ix = i; how = h; name = n; what = w; }
            public Action(int i, string n) 
            { ix = i; how = Verb.Remove; name = n; what = TNull.Value; }
        }
        internal readonly BList<Action> details = BList<Action>.Empty;
        internal Delta() : base(Domain.Delta) { }
        internal Delta(BList<Action> d) :base(Domain.Delta) { details = d; }
        internal Delta(TDocument was, TDocument now)
            : base(Domain.Delta)
        {
            var we = was.First();
            var ne = now.First();
            int m = 0;
            while (we is not null && ne is not null)
            {
                m = we.key();
                var (n,v) = we.value();
                if (n == ne.value().Item1)
                {
                    // names match
                    if (v.CompareTo(ne.value().Item2) != 0)
                    {
                        if (n == "_id") // _id field mismatch
                            goto all;
                        if (v.dataType.kind==Qlx.DOCUMENT)
                            details+=new Action(m, Verb.Delta, n,
                                new Delta((TDocument)v,
                                    (TDocument)ne.value().Item2));
                        else 
                            details+=new Action(m, Verb.Change, n, ne.value().Item2);
                    }
                    we = we.Next();
                    ne = ne.Next();
                    continue;
                }
                if (!now.Contains(n))
                {
                    // current field is not in new version
                    details+=new Action(m,n);
                    we = we.Next();
                }
                else if (!was.Contains(ne.value().Item1))
                {
                    // new field is not in current version
                    details+=new Action(m, Verb.Add, ne.value().Item1, ne.value().Item2);
                    ne = ne.Next();
                }
                else // we can't merge the tree of names
                    goto all;
            }
            while (we is not null)
            {
                m = we.key();
                details+=new Action(m, Verb.Remove, we.value().Item1, TNull.Value);
                we = we.Next();
            }
            while (ne is not null)
            {
                if (was.Contains(ne.value().Item1))
                    goto all;
                m++;
                details+=new Action(m, Verb.Add, ne.value().Item1, ne.value().Item2);
            }
            return;
        all: details = new BList<Action>(new Action(0, Verb.All, "", now));
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            var cm = "";
            for (int i = 0; i < details.Count;i++ )
            {
                sb.Append(cm); cm = ",";
                var a = details[i];
                if (a == null)
                    break;
                sb.Append(a.how.ToString());
                sb.Append(' ');
                sb.Append(a.name);
                if (a.how != Verb.Remove)
                {
                    sb.Append(": ");
                    sb.Append(a.what.ToString());
                }
            }
            return sb.ToString();
        }
    }
    
    internal class TDocArray :TypedValue
    {
        readonly BList<TypedValue> content = BList<TypedValue>.Empty;
        internal long Count => content.Count;
        int nbytes = 0;
        internal static TDocArray Null = new();
        internal TDocArray() : base(Domain.DocArray)
        {
        }
        internal TDocArray(string s) : base(Domain.DocArray)
        {
            var i = 0;
            var c = BList<TypedValue>.Empty;
            if (s.Length > 2 && s[i] == '[')
                i++;
            while (i<s.Length-1)
            {
                if (s[i++] != '{')
                    break;
                var (d,n) = TDocument.New(s, i);
                c += d;
                i = n;
                if (s[i++] != ',')
                    break;
            }
            content = c;
        }
        TDocArray(BList<TypedValue> c) :base(Domain.DocArray)
        {
            content = c;
        }
        internal TDocArray(Context _cx, RowSet rs) :base(Domain.DocArray)
        {
            for (var a = rs.First(_cx); a != null; a = a.Next(_cx))
                Add(new TDocument(_cx,a));
        }
        internal TDocArray(Domain dt, BList<TypedValue>t) : base(dt)
        {
            content = t;
        }
#if EMBEDDED
        internal TDocArray(DocArray d) :base(Domain.DocArray)
        {
            foreach (var e in d.items)
                content+=TDocument.GetValue(e);
        }
#endif
        /// <summary>
        /// Parser from BSON to DocArray
        /// </summary>
        /// <param name="b"></param>
        internal TDocArray(byte[] b, ref int off)
            : base(Domain.DocArray)
        {
            var nbytes = TDocument.GetLength(b, off);
            var i = off + 4;
            while (i < off+nbytes-1) // ignoring the final \0
            {
                var t = b[i++];
                var c = 0;
                var s = i;
                while (i < off+nbytes && b[i++] != 0)
                    c++;
                var key = Encoding.UTF8.GetString(b, s, c);
                Add(TDocument.GetValue(key, t, b, ref i).Item2);
            }
            off += nbytes;
        }
        internal TypedValue this[int i]
        {
            get { return content[i]??TNull.Value; }
        }
        internal override TypedValue this[string n]
        {
            get
            {
                if (int.TryParse(n, out int i))
                    return this[i];
                var r = new TDocArray();
                for(var e=content.First();e!= null;e=e.Next())
                    if (e.value() is TDocument d && d.Contains(n))
                            r = r.Add(d[n]);
                if (r.Count == 1) // yuk
                    return r.content[0]??TNull.Value;
                return r;
            }
        }
        /// <summary>
        /// Parse the contents of {} or []
        /// </summary>
        /// <param name="s">the string</param>
        /// <param name="i">the start of the fields</param>
        /// <param name="n">the end of the string</param>
        /// <returns>the position just after the } or ]</returns>
        internal static int Fields(ref TDocArray da,string s, int i, int n)
        {
            while (i < n)
            {
                var c = s[i++];
                if (char.IsWhiteSpace(c))
                    continue;
                if (c == ']' && da.content.Count == 0)
                    return i;
                da = da.Add(TDocument.GetValue("" + da.Count, s, n, ref i).Item2);
                if (i>=n)
                    break;
                c = s[i++];
                if (Char.IsWhiteSpace(c))
                    continue;
                if (c == ']')
                    return i;
                if (c != ',')
                    throw new DBException("22000", "Expected , at " + (i - 1)).Pyrrho();
            }
            throw new DBException("22000", "Incomplete syntax at " + (i - 1)).Pyrrho();
        }
        internal TDocArray Add(TypedValue c)
        {
            return new TDocArray(new BList<TypedValue>(content, c));
        }
        public override string ToString()
        {
            var sb = new StringBuilder("[");
            var comma = "";
            for (var f=content.First();f!= null;f=f.Next())
            {
                sb.Append(comma); comma = ", ";
                TDocument.Field((""+f.key(),f.value()), sb);
            }
            sb.Append(']');
            return sb.ToString();
        }
        internal byte[] ToBytes()
        {
            var r = new List<byte>
            {
                0,
                0,
                0,
                0
            };
            for (var f=content.First();f!= null;f=f.Next())
            {
                r.Add(16);
                var b = Encoding.UTF8.GetBytes(""+f.key());
                foreach (var a in b)
                    r.Add(a);
                r.Add(0);
                foreach (var a in TDocument.GetBytes(f.value()))
                    r.Add(a);
            }
            r.Add(0);
            TDocument.SetLength(r);
            if (nbytes == 0)
                nbytes = r.Count;
            return [.. r];
        }
    }
}
