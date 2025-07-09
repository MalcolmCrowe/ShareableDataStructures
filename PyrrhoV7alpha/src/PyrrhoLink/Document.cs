using System.Collections;
using System.Reflection;
using System.Text;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho
{
    /// <summary>
    /// The Document class here represents JavaScript style objects.
    /// The constructor and ToString() support JSon serialisation.
    /// There are methods using reflection to create a Document representing
    /// any CLI object of a known type, 
    /// and for extracting objects from a Document given a path of fieldnames.
    /// There are also methods for binary serialisation using BSON.
    /// See also DocArray,
    /// </summary>
    public class Document : DocBase
    {
#if MONO1
        public ArrayList fields = new ArrayList();
#else
        public List<KeyValuePair<string, object>> fields = new List<KeyValuePair<string,object>>();
#endif
        public Document()
        { }
        public Document(string s)
        {
            if (s == null)
                return;
            s = s.Trim();
            int n = s.Length;
            if (n == 0 || s[0] != '{')
                throw new DocumentException("{ expected");
            var i = Fields(s, 1, n);
            if (i != n)
                throw new DocumentException("unparsed input at " + (i - 1));
        }
        public object? this[string k]
        {
            get {
#if MONO1
                foreach (KeyValuePair p in fields)
#else
                foreach(var p in fields)
#endif
                    if (p.Key == k)
                        return p.Value;
                return null;
            }
            set
            {
                if (value is object o)
                fields.Add(new KeyValuePair
#if !MONO1
                    <string, object>
#endif
                    (k, o));
            }
        }
        public bool Contains(string k)
        {
#if MONO1
            foreach (KeyValuePair p in fields)
#else
            foreach(var p in fields)
#endif
                if (p.Key == k)
                    return true;
            return false;
        }
        /// <summary>
        /// Parse the contents of {} 
        /// </summary>
        /// <param name="s">the string</param>
        /// <param name="i">the start of the fields</param>
        /// <param name="n">the end of the string</param>
        /// <returns>the position just after the }</returns>
        internal int Fields(string s, int i, int n)
        {
            ParseState state = ParseState.StartKey;
            StringBuilder kb = new StringBuilder();
            var keyquote = true;
            while (i < n)
            {
                var c = s[i++];
                switch (state)
                {
                    case ParseState.StartKey:
                        kb = new StringBuilder();
                        keyquote = true;
                        if (char.IsWhiteSpace(c))
                            continue;
                        if (c == '}' && fields.Count == 0)
                            return i;
                        if (c != '"')
                        {
                            if (!char.IsLetter(c) && c!='_' && c!='$' && c!='.')
                                throw new DocumentException("Expected name at " + (i - 1));
                            keyquote = false;
                            kb.Append(c);
                        }
                        state = ParseState.Key;
                        continue;
                    case ParseState.Key:
                        if (c == '"')
                        {
                            state = ParseState.Colon;
                            continue;
                        }
                        if (c == ':' && !keyquote)
                            goto case ParseState.Colon;
                        if (c == '\\')
                            c = GetEscape(s, n, ref i);
                        kb.Append(c);
                        continue;
                    case ParseState.Colon:
                        if (char.IsWhiteSpace(c))
                            continue;
                        if (c != ':')
                            throw new DocumentException("Expected : at " + (i - 1));
                        state = ParseState.StartValue;
                        continue;
                    case ParseState.StartValue:
                        if (char.IsWhiteSpace(c))
                            continue;
                        if (GetValue(s,n, ref i) is object o)
                        fields.Add(new KeyValuePair
#if !MONO1
                                <string, object>
#endif
                                (kb.ToString(), o));
                        state = ParseState.Comma;
                        continue;
                    case ParseState.Comma:
                        if (char.IsWhiteSpace(c))
                            continue;
                        if (c == '}')
                            return i;
                        if (c != ',')
                            throw new DocumentException("Expected , at " + (i - 1));
                        state = ParseState.StartKey;
                        continue;
                }
            }
            throw new DocumentException("Incomplete syntax at " + (i - 1));
        }
        /*
         internal Document(byte[] b) : this(b, 0) { }
        /// <summary>
        /// Parser from BSON to Document
        /// </summary>
        /// <param name="b"></param>
        internal Document(byte[] b, int off) 
        {
            int n = GetLength(b, off);
            var i = off + 4;
            while (i < off + n - 1) // ignoring the final \0
            {
                var t = b[i++];
                var c = 0;
                var s = i;
                while (i < off + n && b[i++] != 0)
                    c++;
                var key = Encoding.UTF8.GetString(b, s, c);
                if (GetValue(t,b,ref i) is object o)
                fields.Add(new KeyValuePair
#if !MONO1
                    <string, object>
#endif
                    (key, o));
            }
        }
        internal static object? GetValue(byte t, byte[] b, ref int i)
        {
            object? tv = null;
            switch (t)
            {
                case 1:
                    {
                        tv = BitConverter.ToDouble(b, i);
                        i += 8;
                        break;
                    }
                case 2:
                    {
                        var n = GetLength(b, i);
                        i += 4;
                        tv = Encoding.UTF8.GetString(b, i, n - 1);
                        i += n;
                        break;
                    }
                case 3:
                    {
                        tv = new Document(b, i);
                        i += GetLength(b, i);
                        break;
                    }
                case 4:
                    {
                        tv = new DocArray(b, i);
                        i += GetLength(b, i);
                        break;
                    }
                case 5:
                    {
                        var n = GetLength(b, i);
                        var r = new byte[n];
                        for (int j = 0; j < n; j++)
                            r[j] = b[i + j + 4];
                        tv = r;
                        i += n;
                        break;
                    }
                case 7:
                    {
                        var r = new byte[12];
                        for (int j = 0; j < 12; j++)
                            r[j] = b[i + j];
                        tv = new ObjectId(r);
                        i += 12;
                        break;
                    }
                case 8:
                    {
                        tv = b[i] != 0;
                        i++;
                        break;
                    }
                case 9:
                    {
                        tv = BitConverter.ToInt64(b, i);
                        i += 8;
                        break;
                    }
                case 16:
                    {
                        tv = (long)BitConverter.ToInt32(b, i);
                        i += 4;
                        break;
                    }
                case 18:
                    {
                        tv = BitConverter.ToInt64(b, i);
                        i += 8;
                        break;
                    }
                case 19: // decimal type added for Pyrrho
                    {
                        var n = GetLength(b, i);
                        i += 4;
                        tv = decimal.Parse(Encoding.UTF8.GetString(b, i, n - 1));
                        i += n;
                        break;
                    }
            }
            return tv;
        } */
        static void Field(object v, StringBuilder sb)
        {
            if (v == null)
                sb.Append("null");
            else if (v is string)
            {
                sb.Append('"');
                sb.Append(v);
                sb.Append('"');
            }
            else if (v is IEnumerable)
            {
                var comma = "";
                sb.Append("[");
                foreach (var a in (IEnumerable)v)
                {
                    sb.Append(comma);
                    comma = ",";
                    sb.Append(new Document(a).ToString());
                }
                sb.Append("]");
            }
            else
                sb.Append(v.ToString()); 
        }
        public override string ToString()
        {
            var sb = new StringBuilder("{");
            var comma = "";
#if MONO1
            foreach(KeyValuePair f in fields)
#else
            foreach (var f in fields)
#endif
            {
                sb.Append(comma); comma = ", ";
                sb.Append('"');
                sb.Append(f.Key);
                sb.Append("\": ");
                Field(f.Value, sb);
            }
            sb.Append("}");
            return sb.ToString();
        }
        /*
        internal static int GetLength(byte[] b, int off)
        {
            return b[off] + (b[off + 1] << 8) + (b[off + 2] << 16) + (b[off + 3] << 24);
        }
#if MONO1
        internal static void SetLength(ArrayList r)
#else
        internal static void SetLength(List<byte> r)
#endif
        {
            var n = r.Count; 
            r[0] = (byte)(n & 0xff);
            r[1] = (byte)((n >> 8) & 0xff);
            r[2] = (byte)((n >> 16) & 0xff);
            r[3] = (byte)((n >> 24) & 0xff);
        }
        internal static byte BsonType(object v)
        {
            if (v == null)
                return 10;
            else if (v is double)
                return 1;
            else if (v is string)
                return 2;
            else if (v is Document)
                return 3;
            else if (v is DocArray)
                return 4;
            else if (v is byte[])
                return 5;
            else if (v is ObjectId)
                return 7;
            else if (v is bool)
                return 8;
            else if (v is DateTime)
                return 9;
            else if (v is long)
                return 18;
            else
                return 6;
        }
        internal static byte[] GetBytes(object v)
        {
            switch (BsonType(v))
            {
                case 1:
                    return BitConverter.GetBytes((double)v);
                case 2:
                    {
                        var b = Encoding.UTF8.GetBytes((string)v);
                        var n = b.Length + 1;
                        var r = new byte[n+4];
                        r[0] = (byte)(n & 0xf);
                        r[1] = (byte)((n >> 8) & 0xff);
                        r[2] = (byte)((n >> 16) & 0xff);
                        r[3] = (byte)((n >> 24) & 0xff);
                        for (int i = 0; i < n - 1; i++)
                            r[i + 4] = b[i];
                        r[n+3] = 0;
                        return r;
                    }
                case 3:
                    return ((Document)v).ToBytes();
                case 4:
                    {
                        var d = (Document)v;
#if MONO1
                        var r = new ArrayList();
#else
                        var r = new List<byte>();
#endif
                        r.Add(0); r.Add(0); r.Add(0); r.Add(0);
                        for (int i = 0; i < d.fields.Count; i++)
                        {
                            var k = "" + i;
                            if (d[k] is object e)
                            {
                                r.Add(BsonType(e));
                                var b = Encoding.UTF8.GetBytes(k);
                                foreach (var a in b)
                                    r.Add(a);
                                r.Add(0);
                                foreach (var a in GetBytes(e))
                                    r.Add(a);
                            }
                        }
                        r.Add(0);
                        SetLength(r);
                        var rb = new byte[r.Count];
                        for(var i=0;i<r.Count;i++)
                            rb[i] = (byte)r[i];
                        return rb;
                    }
                case 7:
                    return ((ObjectId)v).ToBytes();
                case 5:
                    return (byte[])v;
                case 8:
                    return new byte[] { (byte)(((bool)v) ? 1 : 0) };
                case 9:
                    return BitConverter.GetBytes((long)v);
                case 18:
                    return BitConverter.GetBytes((long)v);
            }
            return new byte[0];
        }
        internal byte[] ToBytes()
        {
#if MONO1
            var r = new ArrayList();
#else
            var r = new List<byte>();
#endif
            r.Add(0); r.Add(0); r.Add(0); r.Add(0);
#if MONO1
            foreach(KeyValuePair f in fields)
#else
            foreach (var f in fields)
#endif
            {
                r.Add(BsonType(f.Value));
                var b = Encoding.UTF8.GetBytes(f.Key);
                foreach (var a in b)
                    r.Add(a);
                r.Add(0);
                var v = GetBytes(f.Value);
                foreach (var a in v)
                    r.Add(a);
            }
            r.Add(0);
            SetLength(r);
            var rb = new byte[r.Count];
            for (var i = 0; i < r.Count; i++)
                rb[i] = (byte)r[i];
            return rb;
        } */
        public Document(object ob)
        {
            if (ob == null)
                return;
            var tp = ob.GetType();
            var fs = tp.GetFields();
            foreach (var f in fs)
            {
                var v = f.GetValue(ob);
                if (v != null)
                    fields.Add(new KeyValuePair
#if !MONO1
                    <string, object>
#endif
                    (f.Name, v));
            }
        }
#if !MONO1
        public C[] Extract<C>(params string[] p) where C : new()
        {
            return Extract<C>(p, 0);
        }
        internal C[] Extract<C>(string[] p, int off) where C : new()
        {
            var r = new List<C>();
            if (off >= p.Length)
            {
                if (Extract(typeof(C)) is C v)
                    r.Add(v);
            }
            else
                foreach (var e in fields)
                {
                    if (e.Key == p[off])
                    {
                        C[] s = new C[0];
                        var g = e.Value as Document;
                        if (g != null)
                            s = g.Extract<C>(p, off + 1);
                        var h = e.Value as DocArray;
                        if (h != null)
                            s = h.Extract<C>(p, off + 1);
                        foreach (var a in s)
                            r.Add(a);
                    }
                }
            return r.ToArray();
        }
        internal object? Extract(Type t)
        {
            var r = Activator.CreateInstance(t);
            foreach (var e in fields)
            {
                foreach (var f in t.GetFields())
                    if (e.Key == f.Name)
                    {
                        var v = e.Value;
                        if (f.FieldType == typeof(int))
                        {
                            if (v is string)
                                v = int.Parse((string)v);
                            else if (v is long)
                                v = (int)(long)e.Value;
                        }
                        else if (f.FieldType == typeof(long))
                        {
                            if (v is string)
                                v = long.Parse((string)v);
                            else if (v is int)
                                v = (long)(int)e.Value;
                        }
                        else if (f.FieldType == typeof(double))
                        {
                            if (v is string)
                                v = double.Parse((string)v);
                        }
                        else if (f.FieldType == typeof(DateTime))
                        {
                            if (v is string)
                                v = DateTime.Parse((string)v);
                        }
                        else if (f.FieldType.IsArray && v is DocArray)
                        if (f.FieldType.GetConstructor(new Type[] { typeof(int) }) is ConstructorInfo ei
                                && f.FieldType.GetElementType() is Type ty){
                            var da = (DocArray)v;
                            var al = da.Extract(ty);
                            v = ei.Invoke(new object[]{al.Count});
                            for (int i = 0; i < al.Count; i++)
                                ((Array)v).SetValue(al[i], i);
                        }
                        f.SetValue(r, v);
                    }
            }
            return r;
        }
#endif
    }
     public class DocArray :DocBase
    {
#if MONO1
        public ArrayList items = new ArrayList();
#else
        public List<object> items = new List<object>();
#endif
        public DocArray() { }
        public DocArray(string s)
        {
            if (s == null) 
                return;
            s = s.Trim();
            int n = s.Length;
            if (n<2 || s[0]!='[' || s[n-1]!=']')
                throw new DocumentException("[..] expected");
            int i = Items(s, 1, n);
            if (i != n)
                throw new DocumentException("bad DocArray format");
        }
        internal int Items(string s,int i,int n)
        {
            var state = ParseState.StartValue;
            while (i < n)
            {
                var c = s[i++];
                if (Char.IsWhiteSpace(c))
                    continue;
                if (c == ']' && items.Count == 0)
                    break;
                switch (state)
                {
                    case ParseState.StartValue:
                        if (GetValue(s, n, ref i) is object o)
                            items.Add(o);
                        state = ParseState.Comma;
                        continue;
                    case ParseState.Comma:
                        if (c == ']')
                            return i;
                        if (c != ',')
                            throw new DocumentException(", expected");
                        state = ParseState.StartValue;
                        continue;
                }
            }
            return i;
        }
        /*
        internal DocArray(byte[] b) : this(b, 0) { }
        /// <summary>
        /// Parser from BSON to Document
        /// </summary>
        /// <param name="b"></param>
        internal DocArray(byte[] b, int off)
        {
            int n = Document.GetLength(b, off);
            var i = off + 4;
            while (i < off + n - 1) // ignoring the final \0
            {
                var t = b[i++];
                var c = 0;
                var s = i;
                while (i < off + n && b[i++] != 0)
                    c++;
                if (Document.GetValue(t, b, ref i) is object o)
                    items.Add(o);
            }
        }
        public byte[] ToBytes()
        {
#if MONO1
            var r = new ArrayList();
#else
            var r = new List<byte>();
#endif
            r.Add(0); r.Add(0); r.Add(0); r.Add(0);
            for(int i=0;i<items.Count;i++)
            {
                r.Add(Document.BsonType(items[i]));
                var b = Encoding.UTF8.GetBytes(""+i);
                foreach (var a in b)
                    r.Add(a);
                r.Add(0);
                var v = Document.GetBytes(items[i]);
                foreach (var a in v)
                    r.Add(a);
            }
            r.Add(0);
            Document.SetLength(r);
            var rb = new byte[r.Count];
            for (var i = 0; i < r.Count; i++)
                rb[i] = (byte)r[i];
            return rb;
        } */

        public override string ToString()
        {
            var sb = new StringBuilder("[");
            var comma = "";
            foreach (var e in items)
            {
                sb.Append(comma);
                sb.Append(e.ToString());
                comma = ",";
            }
            sb.Append("]");
            return sb.ToString();
        }
#if !MONO1
        public C[] Extract<C>(params string[] p) where C:new()
        {
            return Extract<C>(p, 0);
        }
        internal C[] Extract<C>(string[] p, int off) where C : new()
        {
            var r = new List<C>();
            foreach(var e in items)
            if (e is Document d)
                foreach (var a in d.Extract<C>(p, off))
                    r.Add(a);
            return r.ToArray();
        }
        internal List<object> Extract(Type t)
        {
            var r = new List<object>();
            foreach (var e in items)
            {
                var d = e as Document;
                if (d != null)
                {
                    if (d.Extract(t) is object o)
                        r.Add(o);
                }
                else
                    if (e.GetType() == t)
                    r.Add(e);
            }
            return r;
        }
#endif
    }
     internal class ObjectId : IComparable
     {
         byte[] bytes;
         internal ObjectId(byte[] b)
         {
             bytes = b;
         }
         internal byte[] ToBytes()
         {
             return bytes;
         }
         static string hex = "0123456789abcdef";
         public override string ToString()
         {
             var sb = new StringBuilder();
             sb.Append("\"");
             foreach (var b in bytes)
             {
                 sb.Append(hex[(b >> 4) & 0xf]);
                 sb.Append(hex[b & 0xf]);
             }
             sb.Append("\"");
             return sb.ToString();
         }


         public int CompareTo(object? obj)
         {
            if (obj == null)
                return 1;
             ObjectId that = (ObjectId)obj;
             if (bytes.Length < that.bytes.Length)
                 return -1;
             if (bytes.Length > that.bytes.Length)
                 return 1;
             for (int i = 0; i < bytes.Length; i++)
             {
                 var c = bytes[i]-that.bytes[i];
                 if (c>0)
                     return 1;
                 if (c<0)
                     return -1;
             }
             return 0;
         }
     }
    public class DocumentException : Exception
    {
        public DocumentException(string msg) : base(msg) { }
    }
    public class DocBase
    {
        protected enum ParseState { StartKey, Key, Colon, StartValue, Comma }
        public DocBase() { }
        protected object? GetValue(string s, int n, ref int i)
        {
            if (i < n)
            {
                var c = s[i - 1];
                if (c == '"' || c=='\'')
                    return GetString(s, n, ref i);
                if (c == '{')
                {
                    var d = new Document();
                    i = d.Fields(s, i, n);
                    return d;
                }
                if (c == '(')
                {
                    var r = new PyrrhoRow();
                    (r, i) = GetEntries(r, s, i, n);
                    return r;
                }
                if (c == '[')
                {
                    var d = new DocArray();
                    i = d.Items(s, i, n);
                    return d;
                }
                if (i + 3 < n && s.Substring(i-1, 4).ToLower() == "true")
                {
                    i += 3;
                    return true;
                }
                if (i + 4 < n && s.Substring(i-1, 5).ToLower() == "false")
                {
                    i += 4;
                    return false;
                }
                if (i + 3 < n && s.Substring(i-1, 4).ToLower() == "null")
                {
                    i += 3;
                    return null;
                }
                var sg = c == '-';
                if (sg && i < n)
                    c = s[i++];
                var whole = 0L;
                if (char.IsDigit(c))
                {
                    i--;
                    whole = GetHex(s, n, ref i);
                    while (i < n && char.IsDigit(s[i]))
                        whole = whole * 10 + GetHex(s, n, ref i);
                }
                else
                    goto bad;
                if (i >= n || (s[i] != '.' && s[i] != 'e' && s[i] != 'E'))
                    return sg ? -whole : whole;
                int scale = 0;
                if (s[i] == '.')
                {
                    if (++i >= n || !char.IsDigit(s[i]))
                        throw new DocumentException("decimal part expected");
                    while (i < n && char.IsDigit(s[i]))
                    {
                        whole = whole * 10 + GetHex(s, n, ref i);
                        scale++;
                    }
                }
                if (i >= n || (s[i] != 'e' && s[i] != 'E'))
                {
                    var m = (decimal)whole;
                    while (scale-- > 0)
                        m /= 10M;
                    return sg ? -m : m;
                }
                if (++i >= n)
                    throw new DocumentException("exponent part expected");
                var esg = s[i] == '-';
                if ((s[i] == '-' || s[i] == '+') && (++i >= n || !char.IsDigit(s[i])))
                    throw new DocumentException("exponent part expected");
                var exp = 0;
                while (i < n && char.IsDigit(s[i]))
                    exp = exp * 10 + GetHex(s, n, ref i);
                if (esg)
                    exp = -exp;
                var dr = whole * Math.Pow(10.0, exp - scale);
                return sg ? -dr : dr;
            }
        bad:
            throw new DocumentException("Value expected at " + (i - 1));
        }
        protected string GetString(string s, int n, ref int i)
        {
            var sb = new StringBuilder();
            var quote = s[i-1];
            var squote = quote == '\'';
            while (i < n)
            {
                var c = s[i++];
                if (c == quote)
                {
                    if (squote && s[i] == quote)
                        i++;
                    else
                        return sb.ToString();
                }
                if (c == '\\')
                    c = GetEscape(s, n, ref i);
                sb.Append(c);
            }
            throw new DocumentException("Non-terminated string at " + (i - 1));
        }
        protected char GetEscape(string s, int n, ref int i)
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
            throw new DocumentException("Illegal escape");
        }
        internal static int GetHex(string s, int n, ref int i)
        {
            if (i < n)
            {
                switch (s[i++])
                {
                    case '0': return 0;
                    case '1': return 1;
                    case '2': return 2;
                    case '3': return 3;
                    case '4': return 4;
                    case '5': return 5;
                    case '6': return 6;
                    case '7': return 7;
                    case '8': return 8;
                    case '9': return 9;
                    case 'a': return 10;
                    case 'b': return 11;
                    case 'c': return 12;
                    case 'd': return 13;
                    case 'e': return 14;
                    case 'f': return 15;
                    case 'A': return 10;
                    case 'B': return 11;
                    case 'C': return 12;
                    case 'D': return 13;
                    case 'E': return 14;
                    case 'F': return 15;
                }
            }
            throw new DocumentException("Hex digit expected at " + (i - 1));
        }
        (PyrrhoRow, int) GetEntries(PyrrhoRow r, string s, int i, int n)
        {
            ParseState state = ParseState.StartKey;
            StringBuilder kb = null;
            var keyquote = true;
            while (i < n)
            {
                var c = s[i++];
                switch (state)
                {
                    case ParseState.StartKey:
                        kb = new StringBuilder();
                        keyquote = true;
                        if (char.IsWhiteSpace(c))
                            continue;
                        if (c == ')' && r.row.Length == 0)
                            return (r, i);
                        if (c != '"')
                        {
                            if (!char.IsLetter(c) && c != '_' && c != '$' && c != '.')
                                throw new DocumentException("Expected name at " + (i - 1));
                            keyquote = false;
                            kb.Append(c);
                        }
                        state = ParseState.Key;
                        continue;
                    case ParseState.Key:
                        if (c == '"')
                        {
                            state = ParseState.Colon;
                            continue;
                        }
                        if (c == '=' && !keyquote)
                            goto case ParseState.Colon;
                        if (c == '\\')
                            c = GetEscape(s, n, ref i);
                        kb.Append(c);
                        continue;
                    case ParseState.Colon:
                        if (Char.IsWhiteSpace(c))
                            continue;
                        if (c != '=')
                            throw new DocumentException("Expected = at " + (i - 1));
                        state = ParseState.StartValue;
                        continue;
                    case ParseState.StartValue:
                        if (Char.IsWhiteSpace(c))
                            continue;
                        var cv = new CellValue();
                        cv.val = GetValue(s, n, ref i);
                        r += (kb.ToString(), cv);
                        state = ParseState.Comma;
                        continue;
                    case ParseState.Comma:
                        if (Char.IsWhiteSpace(c))
                            continue;
                        if (c == ')')
                            return (r, i);
                        if (c != ',')
                            throw new DocumentException("Expected , at " + (i - 1));
                        state = ParseState.StartKey;
                        continue;
                }
            }
            throw new DocumentException("Incomplete syntax at " + (i - 1));

        }
    }
}
