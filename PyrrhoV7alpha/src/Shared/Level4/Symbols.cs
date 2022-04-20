using System;
using System.Collections.Generic;
using System.Text;
using Pyrrho.Level3;
using Pyrrho.Common;
using Pyrrho.Level2;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
namespace Pyrrho.Level4
{
    internal class Iix : IComparable
    {
        internal readonly long lp; // lexical (if not equal to dp)
        internal readonly int sd; // select depth
        internal readonly long dp; // defining
        internal static Iix None = new Iix(-1L);
        internal Iix(long u) { lp = u; sd = 0;  dp = u;  }
        private Iix(long l,int s,long u) { lp = l; sd = s; dp = u; }
        internal Iix(Iix ix,long u) { lp = ix.lp; sd = ix.sd; dp = u; }
        internal Iix(long l,Context cx,long u) { lp = l; sd = cx.selectDepth; dp = u; }
        public static Iix operator+(Iix u,int j)
        {
            return new Iix(u.lp + j, u.sd, u.dp + j);
        }
        internal Iix Fix(Context cx)
        {
            return new Iix(dp);
        }
        public int CompareTo(object obj)
        {
            if (obj == null)
                return -1;
            var that = (Iix)obj;
            return lp.CompareTo(that.lp);
        }        
        public override string ToString()
        {
            var sb = new StringBuilder(DBObject.Uid(lp));
            if (this != None)
            {
                if (dp != lp)
                { sb.Append('|'); sb.Append(DBObject.Uid(dp)); }
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Implement an identifier lexeme class.
    /// // shareable as of 26 April 2021
    /// </summary>
    internal class Ident : IComparable
    {
        internal readonly Ident sub;
        internal readonly Iix iix;
        internal readonly string ident;
        internal Ident(Parser psr, string s = null)
        {
            var lx = psr.lxr;
            ident = s ?? ((lx.tok == Sqlx.ID) ? lx.val.ToString() : lx.tok.ToString());
            iix = psr.LexPos();
            sub = null;
        }
        internal Ident(Parser psr, long q)
        {
            var lx = psr.lxr;
            ident = (lx.tok == Sqlx.ID) ? lx.val.ToString() : lx.tok.ToString();
            iix = psr.LexPos();
            sub = null;
        }
        internal Ident(Ident lf, Ident sb)
        {
            ident = lf.ident;
            iix = lf.iix;
            sub = sb;
        }
        internal Ident(Ident pr,string s)
        {
            if (pr == null)
                ident = s;
            else
            {
                ident = pr.ident;
                sub = new Ident(pr.sub, s);
            }
        }
        internal Ident(string s, Iix dp)
        {
            iix = dp;
            ident = s;
        }
        internal Ident(string s, Iix dp,Ident sb)
        {
            iix = dp;
            ident = s;
            sub = sb;
        }
        internal int Length => 1 + (sub?.Length ?? 0);
        internal Ident Prefix(int n) // if n>=Length we return this
        {
            if (n < 0)
                return null;
            return new Ident(ident, iix, sub?.Prefix(n-1));
        }
        internal Ident this[int i]
        {
            get
            {
                if (i == 0)
                {
                    if (sub==null)
                        return this;
                    return new Ident(ident, iix);
                }
                return sub?[i - 1];
            }

        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            if (ident != null)
                sb.Append(ident);
            else
                sb.Append("??");
            if (sub != null)
            {
                sb.Append(".");
                sb.Append(sub.ToString());
            }
            return sb.ToString();
        }
        internal Ident Relocate(Context cx)
        {
            return new Ident(ident, cx.Fix(iix), sub?.Relocate(cx));
        }
        internal Ident Fix(Context cx)
        {
            return new Ident(ident, cx.Fix(iix), sub?.Fix(cx));
        }
        public int CompareTo(object obj)
        {
            if (obj == null)
                return 1;
            var that = (Ident)obj;
            var c = ident.CompareTo(that.ident);
            if (c != 0)
                return c;
            if (sub != null)
                return sub.CompareTo(that.sub);
            if (that.sub != null)
                return -1;
            return 0;
        }
        // shareable as of 26 April 2021
        internal class Idents : BTree<string, (Iix, Idents)>
        {
            public new static Idents Empty = new Idents();
            Idents() : base() { }
            Idents(BTree<string, (Iix, Idents)> b) : base(b.root) { }
            public static Idents operator +(Idents t, (string, Iix, Idents) x)
            {
                var (n, iix, ids) = x;
                return new Idents(t + (n,(iix,ids)));
            }
            public static Idents operator +(Idents t, (Ident, Iix) x)
            {
                var (id, p) = x;
                if (t.Contains(id.ident))
                {
                    var (to, ts) = t[id.ident];
                    if (id.sub != null)
                        return new Idents(t + (id.ident, (to, ts + (id.sub, p))));
                    else
                        return new Idents(t + (id.ident, (p, ts)));
                }
                else
                {
                    var ts = Empty;
                    if (id.sub != null)
                        ts += (id.sub, p);
                    return new Idents(t + (id.ident, (p, ts)));
                }
            }
            public static Idents operator +(Idents t, (Ident, int) x)
            {
                var (id, n) = x;
                var ts = Empty;
                if (t.Contains(id.ident))
                    ts = t[id.ident].Item2;
                if (id.sub != null && n > 0)
                    ts = ts + (id.sub, n - 1);
                return new Idents(t + (id.ident, (id.iix, ts)));
            }
            /// <summary>
            /// Identifier chain lookup function. Search in this
            /// for a given chain, stopping at a given depth.
            /// Rarely used: cx.Lookup is preferred since it
            /// uses the cx.obs information for well-defined objects.
            /// </summary>
            /// <param name="x">A pair: (chain,depth)</param>
            /// <returns>(Deepest Iix found, descendants, rest of chain)</returns>
            internal (Iix,Idents,Ident) this[(Ident,int) x]
            {
                get
                {
                    var (ic, d) = x;
                    if (ic == null || !Contains(ic.ident) || d < 1)
                        return (Iix.None, null, ic);
                    var (ob, ids) = this[ic.ident];
                    if (ids != Empty && ic.sub != null && d > 1 && ids.Contains(ic.sub.ident))
                        return ids[(ic.sub, d - 1)];
                    return (ob, ids, ic.sub);
                }
            }
            /// <summary>
            /// A simplified call of the above
            /// </summary>
            /// <param name="ic"></param>
            /// <returns></returns>
            internal Iix this[Ident ic]
            {
                get
                {
                    if (!Contains(ic.ident))
                        return Iix.None;
                    var (ob, _, s) = this[(ic, ic.Length)];
                    if (s != null)
                        return Iix.None;
                    return ob;
                }
            }
            public IdBookmark First(int p,Ident pr=null)
            {
                var b = base.First();
                return (b==null)?null:new IdBookmark(b, pr, p);
            }
            internal Idents ApplyDone(Context cx)
            {
                var r = BTree<string, (Iix, Idents)>.Empty;
                for (var b=First();b!=null;b=b.Next())
                {
                    var (p, st) = b.value();
                    if (p.dp!=-1L && cx.done[p.dp] is DBObject nb)
                    {
                        p = cx.Ix(nb.defpos);
                        for (var c=cx._Dom(nb)?.rowType.First();c!=null;c=c.Next())
                        if (cx.done[c.value()] is SqlValue s)
                            st = new Idents(st + (s.name, (s.iix, st[s.name].Item2??Empty)));
                    }
                    st = st?.ApplyDone(cx);
                    r += (b.key(), (p, st)); // do not change the string key part
                }
                return new Idents(r);
            }
            internal Idents Relocate(Context cx)
            {
                var r = Empty;
                for (var b=First();b!=null;b=b.Next())
                {
                    var n = b.key();
                    var (p, ids) = b.value();
                    r += (n, cx.Fix(p), ids?.Relocate(cx));
                }
                return r;
            }
            public override ATree<string, (Iix, Idents)> Add(ATree<string, (Iix, Idents)> a)
            {
                return new Idents((BTree<string,(Iix,Idents)>)base.Add(a));
            }
            public override string ToString()
            {
                var sb = new StringBuilder();
                for (var b=First();b!=null;b=b.Next())
                {
                    sb.Append(b.key()); sb.Append("=(");
                    var (p, ids) = b.value();
                    if (p.dp >= 0)
                        sb.Append(p.ToString());
                    sb.Append(",");
                    if (ids!=Empty)
                        sb.Append(ids.ToString());
                    sb.Append(");");
                }
                return sb.ToString();
            }
        }
        // shareable as of 26 April 2021
        internal class IdBookmark
        {
            internal readonly ABookmark<string, (Iix, Idents)> _bmk;
            internal readonly Ident _parent,_key;
            internal readonly int _pos;
            internal IdBookmark(ABookmark<string,(Iix,Idents)> bmk,
                Ident parent, int pos)
            {
                _bmk = bmk; _parent = parent;  _pos = pos;
                _key = new Ident(_parent,_bmk.key());
            }
            public Ident key()
            {
                return _key;
            }
            public Iix value()
            {
                return _bmk.value().Item1;
            }
            public int Position => _pos;
            public IdBookmark Next()
            {
                var bmk = _bmk;
                var (p, id) = bmk.value(); // assert: ob!=null (it's value())
                for (; ; )
                {
                    if (id?.First(_pos + 1) is IdBookmark ib)
                        return ib;
                    bmk = bmk.Next();
                    if (bmk == null)
                        return null;
                    (p, id) = bmk.value();
                    if (p.dp != -1L)
                        return new IdBookmark(bmk, _parent, _pos + 1);
                    if (id == null) // shouldn't happen
                        return null;
                }
            }
        }
    }
    /// <summary>
    /// Lexical analysis for SQL
    /// </summary>
    internal class Lexer
	{
        /// <summary>
        /// The entire input string
        /// </summary>
		public char[] input;
        /// <summary>
        /// The current position (just after tok) in the input string
        /// </summary>
		public int pos,pushPos;
        /// <summary>
        /// The start of tok in the input string
        /// </summary>
		public int start = 0, pushStart;
        public bool allowminus = false;
        /// <summary>
        /// the current character in the input string
        /// </summary>
		char ch,pushCh;
        /// <summary>
        /// The current token's identifier
        /// </summary>
		public Sqlx tok;
        public Sqlx pushBack = Sqlx.Null;
        public long offset;
        public long Position => offset + start;
        /// <summary>
        /// The current token's value
        /// </summary>
		public TypedValue val = null;
        public TypedValue pushVal;
        /// <summary>
        /// Entries in the reserved word table
        /// If there are more than 2048 reserved words, the server will hang
        /// // shareable as of 26 April 2021
        /// </summary>
		class ResWd
		{
			public readonly Sqlx typ;
			public readonly string spell;
			public ResWd(Sqlx t,string s) { typ=t; spell=s; }
		}
 		static ResWd[] resWds = new ResWd[0x800]; // open hash
        static Lexer()
        {
            int h;
            for (Sqlx t = Sqlx.ABS; t <= Sqlx.YEAR; t++)
                if (t != Sqlx.TYPE) // TYPE is not a reserved word but is in this range
                {
                    string s = t.ToString();
                    h = s.GetHashCode() & 0x7ff;
                    while (resWds[h] != null)
                        h = (h + 1) & 0x7ff;
                    resWds[h] = new ResWd(t, s);
                }
            // while XML is a reserved word and is not in the above range
            h = "XML".GetHashCode() & 0x7ff; 
            while (resWds[h] != null)
                h = (h + 1) & 0x7ff;
            resWds[h] = new ResWd(Sqlx.XML, "XML");
        }
        /// <summary>
        /// Check if a string matches a reserved word.
        /// tok is set if it is a reserved word.
        /// </summary>
        /// <param name="s">The given string</param>
        /// <returns>true if it is a reserved word</returns>
		internal bool CheckResWd(string s)
		{
			int h = s.GetHashCode() & 0x7ff;
			for(;;)
			{
				ResWd r = resWds[h];
				if (r==null)
					return false;
				if (r.spell==s)
				{
					tok = r.typ;
					return true;
				}
				h = (h+1)&0x7ff;
			}
		}
        internal object Diag { get { if (val.IsNull) return tok; return val; } }
       /// <summary>
        /// Constructor: Start a new lexer
        /// </summary>
        /// <param name="s">the input string</param>
        internal Lexer(string s,long off = Transaction.Analysing,bool am=false)
        {
   		    input = s.ToCharArray();
			pos = -1;
            offset = off;
            allowminus = am;
			Advance();
			tok = Next();
        }
        internal Lexer(Ident id) : this(id.ident, id.iix.lp) { }
        /// <summary>
        /// Mutator: Advance one position in the input
        /// ch is set to the new character
        /// </summary>
        /// <returns>The new value of ch</returns>
		public char Advance()
		{
			if (pos>=input.Length)
				throw new DBException("42150").Mix();
			if (++pos>=input.Length)
				ch = (char)0;
			else
				ch = input[pos];
			return ch;
		}
        /// <summary>
        /// Decode a hexadecimal digit
        /// </summary>
        /// <param name="c">[0-9a-fA-F]</param>
        /// <returns>0..15</returns>
		internal static int Hexit(char c)
		{
			switch (c)
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
				default: return -1;
			}
		}
        public Sqlx PushBack(Sqlx old)
        {
            pushBack = tok;
            pushVal = val;
            pushStart = start;
            pushPos = pos;
            pushCh = ch;
            tok = old;
            return tok;
        }
        public Sqlx PushBack(Sqlx old,TypedValue oldVal)
        {
            val = oldVal;
            return PushBack(old);
        }
        /// <summary>
        /// Advance to the next token in the input.
        /// tok and val are set for the new token
        /// </summary>
        /// <returns>The new value of tok</returns>
		public Sqlx Next()
		{
            if (pushBack != Sqlx.Null)
            {
                tok = pushBack;
                val = pushVal;
                start = pushStart;
                pos = pushPos;
                ch = pushCh;
                pushBack = Sqlx.Null;
                return tok;
            }
            val = TNull.Value;
			while (char.IsWhiteSpace(ch))
				Advance();
			start = pos;
			if (char.IsLetter(ch))
			{
				char c = ch;
				Advance();
				if (c=='X' && ch=='\'')
				{
					int n = 0;
					if (Hexit(Advance())>=0)
						n++;
					while (ch!='\'')
						if (Hexit(Advance())>=0)
							n++;
					n = n/2;
					byte[] b = new byte[n];
					int end = pos;
					pos = start+1;
					for (int j=0;j<n;j++)
					{
						while (Hexit(Advance())<0)
							;
						int d = Hexit(ch)<<4;
						d += Hexit(Advance());
						b[j] = (byte)d;
					}
					while (pos!=end)
						Advance();
					tok = Sqlx.BLOBLITERAL;
					val = new TBlob(b);
					Advance();
					return tok;
				}
				while (char.IsLetterOrDigit(ch) || ch=='_')
					Advance();
				string s0 = new string(input,start,pos-start);
                string s = s0.ToUpper();
				if (CheckResWd(s))
				{
					switch(tok)
					{
						case Sqlx.TRUE: val = TBool.True; return Sqlx.BOOLEANLITERAL;
						case Sqlx.FALSE: val = TBool.False; return Sqlx.BOOLEANLITERAL;
                        case Sqlx.NULL: val = TNull.Value; return Sqlx.NULL;
						case Sqlx.UNKNOWN: val = null; return Sqlx.BOOLEANLITERAL;
                        case Sqlx.CURRENT_DATE: val = new TDateTime(DateTime.Today); return tok;
                        case Sqlx.CURRENT_TIME: val = new TTimeSpan(DateTime.Now - DateTime.Today); return tok;
                        case Sqlx.CURRENT_TIMESTAMP: val = new TDateTime(DateTime.Now); return tok;
					}
					return tok;
				}
				val = new TChar(s);
				return tok=Sqlx.ID;
			}
			string str;
            char minusch = ' '; // allow negative number?
			if (char.IsDigit(ch)||(allowminus && ch=='-'))
			{
				start = pos;
                if (ch == '-')
                    Advance();
                if (!char.IsDigit(ch))
                {
                    minusch = ch;
                    ch = '-';
                    goto uminus;
                }
				while (char.IsDigit(Advance()))
					;
				if (ch!='.')
				{
					str = new string(input,start,pos-start);
					if (pos-start>18)
						val = new TInteger(Integer.Parse(str));
					else
						val = new TInt(long.Parse(str));
					tok=Sqlx.INTEGERLITERAL;
					return tok;
				}
				while (char.IsDigit(Advance()))
					;
				if (ch!='e' && ch!='E')
				{
					str = new string(input,start,pos-start);
					val = new TNumeric(Common.Numeric.Parse(str));
					tok=Sqlx.NUMERICLITERAL;
					return tok;
				}
				if (Advance()=='-'||ch=='+')
					Advance();
				if (!char.IsDigit(ch))
					throw new DBException("22107").Mix();
				while (char.IsDigit(Advance()))
					;
				str = new string(input,start,pos-start);
				val = new TReal(Common.Numeric.Parse(str));
				tok=Sqlx.REALLITERAL;
				return tok;
			}
            uminus:
			switch (ch)
			{
				case '[':	Advance(); return tok=Sqlx.LBRACK;
				case ']':	Advance(); return tok=Sqlx.RBRACK;
				case '(':	Advance(); return tok=Sqlx.LPAREN;
				case ')':	Advance(); return tok=Sqlx.RPAREN;
				case '{':	Advance(); return tok=Sqlx.LBRACE;
				case '}':	Advance(); return tok=Sqlx.RBRACE;
				case '+':	Advance(); return tok=Sqlx.PLUS;
				case '*':	Advance(); return tok=Sqlx.TIMES;
				case '/':	Advance(); return tok=Sqlx.DIVIDE;
				case ',':	Advance(); return tok=Sqlx.COMMA;
				case '.':	Advance(); return tok=Sqlx.DOT;
				case ';':	Advance(); return tok=Sqlx.SEMICOLON;
                case '?':   Advance(); return tok = Sqlx.QMARK; //added for Prepare()
/* from v5.5 Document syntax allows exposed SQL expressions
                case '{':
                    {
                        var braces = 1;
                        var quote = '\0';
                        while (pos<input.Length)
                        {
                            Advance();
                            if (ch == '\\')
                            {
                                Advance();
                                continue;
                            }
                            else if (ch == quote)
                                quote = '\0';
                            else if (quote == '\0')
                            {
                                if (ch == '{')
                                    braces++;
                                else if (ch == '}' && --braces == 0)
                                {
                                    Advance();
                                    val = new TDocument(ctx,new string(input, st, pos - st));
                                    return tok = Sqlx.DOCUMENTLITERAL;
                                }
                                else if (ch == '\'' || ch == '"')
                                    quote = ch;
                            }
                        }
                        throw new DBException("42150",new string(input,st,pos-st));
                    } */
				case ':':
					if (ch==':')
					{
						Advance();
						return tok=Sqlx.DOUBLECOLON;
					}
					return tok=Sqlx.COLON;
				case '-':
                    if (minusch == ' ')
                        Advance();
                    else
                        ch = minusch;
					if (ch=='-')
					{
   					    Advance();    // -- comment
						while (pos<input.Length) 
							Advance();
						return Next();
					}
					return tok=Sqlx.MINUS;
				case '|':	
					if (Advance()=='|')
					{
						Advance();
						return tok=Sqlx.CONCATENATE;
					}
					return tok=Sqlx.VBAR;
				case '<' : 
					if (Advance()=='=')
					{
						Advance();
						return tok=Sqlx.LEQ; 
					}
					else if (ch=='>')
					{
						Advance();
						return tok=Sqlx.NEQ;
					}
					return tok=Sqlx.LSS;
				case '=':	Advance(); return tok=Sqlx.EQL;
				case '>':
					if (Advance()=='=')
					{
						Advance();
						return tok=Sqlx.GEQ;
					}
					return tok=Sqlx.GTR;
				case '"':	// delimited identifier
				{
					start = pos;
					while (Advance()!='"')
						;
					val = new TChar(new string(input,start+1,pos-start-1));
                    Advance();
                    while (ch == '"')
                    {
                        var fq = pos;
                        while (Advance() != '"')
                            ;
                        val = new TChar(val.ToString()+new string(input, fq, pos - fq));
                        Advance();
                    }
					tok=Sqlx.ID;
         //           CheckForRdfLiteral();
                    return tok;
				}
				case '\'': 
				{
					start = pos;
					var qs = new Stack<int>();
                    qs.Push(-1);
					int qn = 0;
					for (;;)
					{
						while (Advance()!='\'')
							;
						if (Advance()!='\'')
							break;
                        qs.Push(pos);
						qn++;
					}
					char[] rb = new char[pos-start-2-qn];
					int k=pos-start-3-qn;
					int p = -1;
					if (qs.Count>1)
						p = qs.Pop();
					for (int j=pos-2;j>start;j--)
					{
                        if (j == p)
                            p = qs.Pop();
                        else
                            rb[k--] = input[j];
					}
					val = new TChar(new string(rb));
					return tok=Sqlx.CHARLITERAL;
				}
                /*        case '^': // ^^uri can occur in Type
                        {
                            val = new TChar("");
                            tok = Sqlx.ID;
                            CheckForRdfLiteral();
                            return tok;
                        } */
                // These are for the new Position Domain in v7. Positions are always longs
                case '!':
                    {
                        Advance();
                        while (char.IsDigit(Advance()))
                            ;
                        str = new string(input, start+1, pos - start-1);
                        val = new TInt(long.Parse(str)+Transaction.TransPos);
                        tok = Sqlx.INTEGERLITERAL;
                        return tok;
                    }
                case '#':
                    {
                        Advance();
                        while (char.IsDigit(Advance()))
                            ;
                        str = new string(input, start + 1, pos - start - 1);
                        val = new TInt(long.Parse(str) + Transaction.Analysing);
                        tok = Sqlx.INTEGERLITERAL;
                        return tok;
                    }
                case '`':
                    {
                        Advance();
                        while (char.IsDigit(Advance()))
                            ;
                        str = new string(input, start + 1, pos - start - 1);
                        val = new TInt(long.Parse(str) + Transaction.Executables);
                        tok = Sqlx.INTEGERLITERAL;
                        return tok;
                    }
                case '%':
                    {
                        Advance();
                        while (char.IsDigit(Advance()))
                            ;
                        str = new string(input, start + 1, pos - start - 1);
                        val = new TInt(long.Parse(str) + Transaction.HeapStart);
                        tok = Sqlx.INTEGERLITERAL;
                        return tok;
                    }
                case '\0':
					return tok=Sqlx.EOF;
			}
			throw new DBException("42101",ch).Mix();
		}
 /*       /// <summary>
        /// Pyrrho 4.4 if we seem to have an ID, it may be followed by ^^
        /// in which case it is an RdfLiteral
        /// </summary>
        private void CheckForRdfLiteral()
        {
            if (ch != '^')
                return;
            if (Advance() != '^')
                throw new DBException("22041", "^").Mix();
            string valu = val.ToString();
            Domain t = null;
            string iri = null;
            int pp = pos;
            Ident ic = null;
            if (Advance() == '<')
            {
                StringBuilder irs = new StringBuilder();
                while (Advance() != '>')
                    irs.Append(ch);
                Advance();
                iri = irs.ToString();
            }
    /*        else if (ch == ':')
            {
                Next();// pass the colon
                Next();
                if (tok != Sqlx.ID)
                    throw new DBException("22041", tok).Mix();
                var nsp = ctx.nsps[""];
                if (nsp == null)
                    throw new DBException("2201M", "\"\"").ISO();
                iri = nsp + val as string;
            } else 
            {
                Next();
                if (tok != Sqlx.ID)
                    throw new DBException("22041", tok).Mix();
    /*            if (ch == ':')
                {
                    Advance();
                    iri = ctx.nsps[val.ToString()];
                    if (iri == null)
                        iri = PhysBase.DefaultNamespaces[val.ToString()];
                    if (iri == null)
                        throw new DBException("2201M", val).ISO();
                    Next();
                    if (tok != Sqlx.ID)
                        throw new DBException("22041", tok).Mix();
                    iri = iri + val as string;
                } 
            }
            if (iri != null)
            {
                t = ctx.types[iri];
                if (t==null) // a surprise: ok in provenance and other Row
                {
                    t = Domain.Iri.Copy(iri);
                    ctx.types +=(iri, t);
                }
                ic = new Ident(this,Ident.IDType.Type,iri);
            }
            val = RdfLiteral.New(t, valu);
            tok = Sqlx.RDFLITERAL;
        } */
        /// <summary>
        /// This function is used for XML parsing (e.g. in XPATH)
        /// It stops at the first of the given characters it encounters or )
        /// if the stop character is unquoted and unparenthesised 
        /// ' " are processed and do not nest
        /// unquoted () {} [] &lt;&gt; nest. (Exception if bad nesting)
        /// Exception at EOF.
        /// </summary>
        /// <param name="stop">Characters to stop at</param>
        /// <returns>the stop character</returns>
        public char XmlNext(params char[] stop)
        {
            var nest = new Stack<char>();
            char quote = (char)0;
            int n = stop.Length;
            int start = pos;
            char prev = (char)0;
            for (; ; )
            {
                if (nest == null && quote == (char)0)
                    for (int j = 0; j < n; j++)
                        if (ch == stop[j])
                            goto done;
                switch (ch)
                {
                    case '\0': throw new DBException("2200N").ISO();
                    case '\\': Advance(); break;
                    case '\'': if (quote == ch)
                            quote = (char)0;
                        else if (quote==(char)0)
                            quote = ch;
                        break;
                    case '"': goto case '\'';
                    case '(':  if (quote == (char)0)
                            nest.Push(')'); 
                        break;
                    case '[': if (quote == (char)0)
                            nest.Push(']');
                        break;
                    case '{': if (quote == (char)0)
                            nest.Push('}');
                        break;
                    //     case '<': nest = MTree.Add('>', nest); break; < and > can appear in FILTER
                    case ')': if (quote==(char)0 && nest.Count==0)
                            goto done;
                        goto case ']';
                    case ']': if (quote != (char)0) break;
                        if (nest == null || ch != nest.Peek())
                            throw new DBException("2200N").ISO();
                        nest.Pop();
                        break;
                    case '}': goto case ']';
               //     case '>': goto case ']';
                    case '#': 
                        if (prev=='\r' || prev=='\n')
                            while (ch != '\r' && ch != '\n')
                                Advance();
                        break;
                }
                prev = ch;
                Advance();
            }
        done:
            val = new TChar(new string(input, start, pos - start).Trim());
            return ch;
        }
        public static string UnLex(Sqlx s)
        {
            switch (s)
            {
                default: return s.ToString();
                case Sqlx.EQL: return "=";
                case Sqlx.NEQ: return "<>";
                case Sqlx.LSS: return "<";
                case Sqlx.GTR: return ">";
                case Sqlx.LEQ: return "<=";
                case Sqlx.GEQ: return ">=";
                case Sqlx.PLUS: return "+";
                case Sqlx.MINUS: return "-";
                case Sqlx.TIMES: return "*";
                case Sqlx.DIVIDE: return "/";
            }
        }
     }
}
