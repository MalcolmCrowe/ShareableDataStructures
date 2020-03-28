using System;
using System.Globalization;
using System.IO;
using System.Collections.Generic;
using System.Text;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Common
{
    /// <summary>
    /// This class is addedd as inspired by MongoDB.
    /// BSON format is used for physical storage and ADO.NET.
    /// Json format is used in SQL.
    /// Alas: Json format has positional fields, so we can't use simple string->Column tree
    /// IMMUTABLE - all fields are private
    /// </summary>
    internal class TDocument : TypedValue
    {
        private enum ParseState { StartKey, Key, Colon, StartValue, Comma }
        BList<(string, TypedValue)> content = BList<(string, TypedValue)>.Empty;
        BTree<string, int> names = BTree<string, int>.Empty;
        int nbytes = 0;
        internal static TDocument Null = new TDocument();
        public static string _id = "_id";
        internal TDocument() : base(Domain.Document)
        {
        }
        internal TDocument(Context cx,TRow r, string id = null) :this()
        {
            if (id != null)
                Add(_id, id);
            var oi = (ObInfo)cx.db.role.obinfos[r.dataType.defpos];
            for (var b=oi.columns.First();b!=null;b=b.Next())
            {
                var tc = b.value();
                Add(tc.name, r[b.key()]);
            }
        }
        internal TDocument(TDocument d, params (string, TypedValue)[] vs) : base(Domain.Document)
        {
            content = d.content;
            foreach (var v in vs)
                Add(v.Item1, v.Item2);
        }
        internal override object Val()
        {
            return ToString();
        }
        internal ABookmark<int,(string,TypedValue)> First()
        {
            return content.First();
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
            else if (o is string)
                return new TChar((string)o);
            else if (o is int)
                return new TInt((int)o);
            else if (o is long)
                return new TInt((long)o);
            else if (o is double)
                return new TReal((double)o);
            else if (o is DateTime)
                return new TDateTime((DateTime)o);
            else if (o is byte[])
                return new TBlob((byte[])o);
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
                    i = d.Fields(s, i, n);
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
                    i = d.Fields(s, i, n);
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
                    return (nm, new TNumeric(Domain.Numeric, new Numeric(new Integer(whole), scale)));
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
            var i = Fields(s, 1, n);
            if (i != n)
                throw ParseException("unparsed input at " + (i - 1));
        }
        internal void Add(string n, TypedValue tv)
        {
            var i = names.Contains(n) ? names[n] : (int)content.Count;
            content +=(i, (n, tv));
            names+=(n, i);
        }
        internal void Add((string, TypedValue) v)
        {
            Add(v.Item1, v.Item2);
        }
        internal override TypedValue this[string n]
        {
            get
            {
                return (n == null)? this: 
                    names.Contains(n)? content[names[n]].Item2 :TNull.Value;
            }
        }
        internal bool GetBool(string n, bool def)
        {
            var v = this[n] as TBool;
            if (v == null || !v.value.HasValue)
                return def;
            return v.value.Value;
        }
        internal TDocument Remove(string n)
        {
            if (!Contains(n))
                return this;
            var r = new TDocument();
            for (int i = 0; i < content.Count; i++)
                if (content[i].Item1.CompareTo(n)!=0)
                    r.Add(content[i]);
            return r;
        }
        /// <summary>
        /// Parse the contents of {} or []
        /// </summary>
        /// <param name="s">the string</param>
        /// <param name="i">the start of the fields</param>
        /// <param name="n">the end of the string</param>
        /// <returns>the position just after the } or ]</returns>
        int Fields(string s, int i, int n)
        {
            ParseState state = ParseState.StartKey;
            StringBuilder kb = null;
            char qu = '"';
            while (i < n)
            {
                var c = s[i++];
                switch (state)
                {
                    case ParseState.StartKey:
                        kb = new StringBuilder();
                        if (Char.IsWhiteSpace(c))
                            continue;
                        if (c == '}' && content.Count == 0)
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
                        kb.Append(c);
                        continue;
                    case ParseState.Colon:
                        if (Char.IsWhiteSpace(c))
                            continue;
                        if (c != ':')
                            throw ParseException("Expected : at " + (i - 1));
                        state = ParseState.StartValue;
                        continue;
                    case ParseState.StartValue:
                        if (Char.IsWhiteSpace(c))
                            continue;
                        if (c == ']' && content.Count == 0)
                            return i;
                        var key = kb.ToString();
                        Add(key, GetValue(key, s, n, ref i).Item2);
                        state = ParseState.Comma;
                        continue;
                    case ParseState.Comma:
                        if (Char.IsWhiteSpace(c))
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
            return new DBException("22300", m).Pyrrho();
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
                if (c >= 'A' && c <= 'F') return (c = 'A') + 10;
            }
            throw ParseException("Hex digit expected at " + (i - 1));
        }
        /// <summary>
        /// Parser from BSON to Document
        /// </summary>
        /// <param name="b"></param>
        internal TDocument(byte[] b, ref int off) :base(Domain.Document)
        {
            nbytes = GetLength(b, off);
            var i = off + 4;
            while (i < off + nbytes - 1) // ignoring the final \0
            {
                var t = b[i++];
                var c = 0;
                var s = i;
                while (i < off + nbytes && b[i++] != 0)
                    c++;
                var key = Encoding.UTF8.GetString(b, s, c);
                Add(key, GetValue(key, t, b, ref i).Item2);
            }
            off += nbytes;
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
                        var ix = s.IndexOf(".");
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
                    case Sqlx.CONTENT: sb.Append('"'); sb.Append(v); sb.Append('"'); break;
                    case Sqlx.DOCARRAY: sb.Append("[");
                        var d = v.Item2 as TDocArray;
                        var comma = "";
                        for (int i = 0; i < d.Count; i++)
                        {
                            sb.Append(comma);
                            comma = ", ";
                            Field((""+i,d[i]), sb);
                        }
                        sb.Append("]");
                        break;
                    default:
                        sb.Append(v.ToString());
                        break;
                }
        }
        public override string ToString()
        {
            var sb = new StringBuilder("{");
            var comma = "";
            for (var f = content.First();f!= null;f=f.Next())
            {
                sb.Append(comma); comma = ", ";
                sb.Append('"');
                sb.Append(f.value().Item1);
                sb.Append("\": ");
                Field(f.value(), sb);
            }
            sb.Append("}");
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
            if (v == null || v.IsNull)
                return new byte[0];
            switch (v.dataType.kind)
            {
                case Sqlx.REAL:
                    return BitConverter.GetBytes(((TReal)v).dvalue);
                case Sqlx.CHAR:
                    return ToBytes(v.ToString());
                case Sqlx.DOCUMENT:
                    {
                        var doc = v as TDocument;
                        if (doc == null)
                            doc = new TDocument();
                        return doc.ToBytes((TDocument)null);
                    }
                case Sqlx.DOCARRAY:
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
                        return r.ToArray();
                    }
                case Sqlx.BLOB:
                    return ((TBlob)v).value;
                case Sqlx.BOOLEAN:
                    return new byte[] { (byte)(v.ToBool().Value ? 1 : 0) };
                case Sqlx.TIMESTAMP:
                    return BitConverter.GetBytes(v.ToLong().Value);
                case Sqlx.NUMERIC:
                    return ToBytes(v.ToString());
                case Sqlx.INTEGER:
                    if (v is TInteger)
                    {
                        var iv = ((TInteger)v).ivalue;
                        if (iv.BitsNeeded() > 64)
                            return ToBytes(iv.ToString());
                        return BitConverter.GetBytes((long)iv);
                    }
                    return BitConverter.GetBytes(v.ToInt().Value);
            }
            return new byte[0];
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
        internal byte[] ToBytes(TDocument proj)
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
                if (proj!=null)
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
            if (nbytes == 0)
                nbytes = r.Count;
            return r.ToArray();
        }
        bool IsZero((string,TypedValue) fv)
        {
            switch (fv.Item2.dataType.kind)
            {
                case Sqlx.INTEGER:
                case Sqlx.INT:
                    if ((int)fv.Item2.Val() == 0)
                        return true;
                    break;
                case Sqlx.REAL:
                    if ((double)fv.Item2.Val() == 0.0)
                        return true;
                    break;
                case Sqlx.Null:
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
            for (var cf = b.content.First();cf!=null;cf=cf.Next())
                if (cf.value().Item1 != "_id")
                    d.Add(cf.value());
            return d;
        }
        /// <summary>
        /// In the absence of $ operators this should work like a comparison function.
        /// However, we also use this function to match against a template, so
        /// we tolerate the absence in b of fields in this.
        /// </summary>
        /// <param name="b">Document to compare with</param>
        /// <returns></returns>
        internal int Query(TypedValue b)
        {
            for (var f=content.First();f!= null;f=f.Next())
                        if (Query(f.value().Item1,f.value().Item2, b)!=0)
                            return -1;
            return 0;
        }
        int Query(string nm,TypedValue a, TypedValue b)
        {
            var ki = b.dataType.kind;
            if (ki != Sqlx.DOCARRAY && ki != Sqlx.DOCUMENT)
                return -1;
            var vb = b[nm];
            if (vb == null)
                vb = TNull.Value;
            if (a.IsNull && b.IsNull)
                return 0;
            var ef = a as TDocument;
            if (ef == null)
            {
                var c = a._CompareTo(vb);
                if (c != 0)
                    return c;
                return 0;
            }
            for(var eef = ef.content.First();eef != null;eef=eef.Next())
            {
                var c = a._CompareTo(vb);
                if (c != 0)
                    return c;
            }
            return 0;
        }
        internal void Add(string k, Transaction tr, SqlValue c)
        {
            Add(k, c.Eval(null));
        }
        internal void Add(string n, int v)
        {
            Add(n, new TInt(v));
        }
        internal void Add(string n, string v)
        {
            Add(n, new TChar(v));
        }
        internal void Add(string n, bool v)
        {
            Add(n, v?TBool.True:TBool.False);
        }
        internal void Add(string n, double v)
        {
            Add(n, new TReal(v));
        }
        /// <summary>
        /// When comparing two documents we only consider fields with matching names
        /// but we also need to watch out for comparison operators
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override int _CompareTo(object obj)
        {
            return Query((TypedValue)obj);
        }
        public bool Contains(string n)
        {
            return names.Contains(n);
        }
        public override bool IsNull
        {
            get { return this==Null; }
        }
    }
    /// <summary>
    /// We can calculate a Delta between two documents W,N if
    /// a) they have the same _Id
    /// b) the list of fields can be merged, that is for names A and B
    /// if W.colNames[A] lt W.colNames[B] then N.colNames[A] lt colNames[B]
    /// </summary>
    internal class Delta : TypedValue
    {
        internal enum Verb { Add,Change,Delta,Remove,All }
        internal class Action
        {
            public int ix;
            public Verb how;
            public string name;
            public TypedValue what;
            public Action(int i, Verb h, string n, TypedValue w)
            { ix = i; how = h; name = n; what = w; }
            public Action(int i, string n) 
            { ix = i; how = Verb.Remove; name = n; what = TNull.Value; }
        }
        internal List<Action> details = new List<Action>();
        internal Delta() : base(Domain.Delta) { }
        internal Delta(TDocument was, TDocument now)
            : base(Domain.Delta)
        {
            var we = was.First();
            var ne = now.First();
            int m = 0;
            while (we!=null && ne!=null)
            {
                m = we.key();
                var (n,v) = we.value();
                if (n == ne.value().Item1)
                {
                    // names match
                    if (v._CompareTo(ne.value().Item2) != 0)
                    {
                        if (n == "_id") // _id field mismatch
                            goto all;
                        if (v.dataType.kind==Sqlx.DOCUMENT)
                            details.Add(new Action(m, Verb.Delta, n,
                                new Delta(v as TDocument,
                                    ne.value().Item2 as TDocument)));
                        else 
                            details.Add(new Action(m, Verb.Change, n, ne.value().Item2));
                    }
                    we = we.Next();
                    ne = ne.Next();
                    continue;
                }
                if (!now.Contains(n))
                {
                    // current field is not in new version
                    details.Add(new Action(m,n));
                    we = we.Next();
                }
                else if (!was.Contains(ne.value().Item1))
                {
                    // new field is not in current version
                    details.Add(new Action(m, Verb.Add, ne.value().Item1, ne.value().Item2));
                    ne = ne.Next();
                }
                else // we can't merge the list of names
                    goto all;
            }
            while (we!=null)
            {
                m = we.key();
                details.Add(new Action(m, Verb.Remove, we.value().Item1, TNull.Value));
                we = we.Next();
            }
            while (ne!=null)
            {
                if (was.Contains(ne.value().Item1))
                    goto all;
                m = m + 1;
                details.Add(new Action(m, Verb.Add, ne.value().Item1, ne.value().Item2));
            }
            return;
        all: details = new List<Action>();
            details.Add(new Action(0, Verb.All, null, now));
        }
        internal override object Val()
        {
            return details;
        }
        public override int _CompareTo(object obj)
        {
            throw new NotImplementedException();
        }
        public override bool IsNull => throw new NotImplementedException();
        internal TDocument Apply(TDocument w)
        {
            var r = new TDocument();
            var we = w.First();
            int i = 0;
            while (we!=null && i<details.Count)
            {
                var a = details[i];
                if (a.ix>we.key())
                {
                    r.Add(we.value());
                    we = we.Next();
                    continue;
                }
                while (a.ix<=we.key())
                {
                    switch(a.how)
                    {
                        case Verb.Remove: i++; we = we.Next(); goto next;
                        case Verb.Change: r.Add(we.value().Item1, a.what); 
                            i++; we = we.Next(); goto next;
                        case Verb.Delta: r.Add(we.value().Item1,
                            ((Delta)a.what).Apply(we.value().Item2 as TDocument));
                            i++; we = we.Next(); goto next;
                        case Verb.Add: r.Add(a.name,a.what); i++; break;
                        case Verb.All: return a.what as TDocument;
                    }
                }
            next: ;
            }
            while (i<details.Count)
            {
                var a = details[i++];
                r.Add(a.name, a.what);
            }
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            var cm = "";
            for (int i = 0; i < details.Count;i++ )
            {
                sb.Append(cm); cm = ",";
                var a = details[i];
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
        BList<TypedValue> content = BList<TypedValue>.Empty;
        internal long Count => content.Count;
        int nbytes = 0;
        internal static TDocArray Null = new TDocArray();
        internal TDocArray() : base(Domain.DocArray)
        {
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
            get { return content[i]; }
            set { content +=(i, value); }
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
                            r.Add(d[n]);
                if (r.Count == 1) // yuk
                    return r.content[0];
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
        internal int Fields(string s, int i, int n)
        {
            while (i < n)
            {
                var c = s[i++];
                if (Char.IsWhiteSpace(c))
                    continue;
                if (c == ']' && content.Count == 0)
                    return i;
                Add(TDocument.GetValue("" + Count, s, n, ref i).Item2);
                if (i>=n)
                    break;
                c = s[i++];
                if (Char.IsWhiteSpace(c))
                    continue;
                if (c == ']')
                    return i;
                if (c != ',')
                    throw new DBException("22300", "Expected , at " + (i - 1)).Pyrrho();
            }
            throw new DBException("22300", "Incomplete syntax at " + (i - 1)).Pyrrho();
        }
        internal void Add(TypedValue c)
        {
            content +=c;
        }
        internal void Add(int k,TypedValue v)
        {
            content +=(k, v);
        }
        internal void AddToSet((string,TypedValue) v)
        {
            if (!int.TryParse(v.ToString(), out int k))
                k = (int)content.Count;
            Add(k,v.Item2);
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
            sb.Append("]");
            return sb.ToString();
        }
        bool Has(TypedValue v)
        {
            if (v != null)
                for (var f=content.First();f!= null;f=f.Next())
                    if (v.dataType.CanTakeValueOf(f.value().dataType) &&
                        v._CompareTo(f.value()) == 0)
                        return true;
            return false;
        }
        public override int _CompareTo(object obj)
        {
            var that = obj as TDocArray;
            if (that == null)
                return -1;
            // array equality
            var e = content.First();
            var f = that.content.First();
            for (; e!=null && f!=null; e = e.Next(), f = f.Next())
            {
                var c = e.key().CompareTo(f.key());
                if (c != 0)
                    return c;
                c = e.value()._CompareTo(f.value());
                if (c != 0)
                    return c;
            }
            return (e!= null) ? 1 : (f!= null) ? -1 : 0;
        }
        internal override object Val()
        {
            return ToString();
        }
        public override bool IsNull
        {
            get { return this==Null; }
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
            return r.ToArray();
        }
    }
}
