using System;
using System.Collections.Generic;
using System.Text;
using Pyrrho.Level3;
using Pyrrho.Common;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.
namespace Pyrrho.Level4
{

    /// <summary>
    /// Implement an identifier lexeme class.
    /// Ident is not immutable and must not be used in any property of any subclass of Basis.
    /// </summary>
    internal class Ident : IComparable
    {
        internal readonly Ident sub;
        internal readonly long iix;
        internal readonly string ident;
        internal Ident(Lexer lx, string s = null)
        {
            ident = s ?? ((lx.tok == Sqlx.ID) ? lx.val.ToString() : lx.tok.ToString());
            iix = lx.Position;
            sub = null;
        }
        internal Ident(Lexer lx, int st, int pos, Ident sb = null)
        {
            iix = lx.Position;
            ident = new string(lx.input, st, pos - st);
            sub = sb;
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
        internal Ident(string s, long dp)
        {
            iix = dp;
            ident = s;
        }
        Ident(string s, long dp,Ident sb)
        {
            iix = dp;
            ident = s;
            sub = sb;
        }
        internal int Length => 1 + (sub?.Length ?? 0);
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
        internal Ident Relocate(Level2.Writer wr)
        {
            return new Ident(ident, wr.Fix(iix), sub?.Relocate(wr));
        }
        internal Ident Fix(Context cx)
        {
            return new Ident(ident, cx.obuids[iix], sub?.Fix(cx));
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
        /// <summary>
        /// TODO: if A(B,C) do not match select D.B from A,..
        /// </summary>
        internal class Idents : BTree<string, (long, Idents)>
        {
            public new static Idents Empty = new Idents();
            Idents() : base() { }
            Idents(BTree<string, (long, Idents)> b) : base(b.root) { }
            public static Idents operator +(Idents t, (string, long, Idents) x)
            {
                return new Idents(t + (x.Item1,(x.Item2,x.Item3)));
            }
            public static Idents operator +(Idents t, (Ident, long) x)
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
            internal (long,Idents,Ident) this[(Ident,int) x]
            {
                get
                {
                    var (ic, d) = x;
                    if (ic == null || !Contains(ic.ident) || d < 1)
                        return (-1L, null, ic);
                    var (ob, ids) = this[ic.ident];
                    if (ids!=null && ic.sub != null && d > 1)
                        return ids[(ic.sub, d - 1)];
                    return (ob, ids, ic.sub);
                }
            }
            internal long this[Ident ic]
            {
                get
                {
                    var (ob, ids, s) = this[(ic, 1)];
                    if (s != null)
                        return -1L;
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
                var r = BTree<string, (long, Idents)>.Empty;
                for (var b=First();b!=null;b=b.Next())
                {
                    var (p, st) = b.value();
                    if (p!=-1L && cx.done[p] is DBObject nb)
                    {
                        p = nb.defpos;
                        for (var c=(nb as Query)?.rowType.First();c!=null;c=c.Next())
                        if (cx.done[c.value()] is SqlValue s)
                            st = new Idents(st + (s.name, (s.defpos, st[s.name].Item2)));
                    }
                    st = st?.ApplyDone(cx);
                    r += (b.key(), (p, st)); // do not change the string key part
                }
                return new Idents(r);
            }
            internal static Idents For(long ob,Database db,Context cx)
            {
                var r = Empty;
                var oi = (ObInfo)db.role.infos[ob];
                for (var b=oi?.domain.representation.First();b!=null;b=b.Next())
                {
                    var p = b.key();
                    var d = b.value();
                    var sc = cx.Inf(p);
                    r += (sc.name, sc.defpos, For(p,db,cx));
                    cx.Add(sc);
                }
                return r;
            }
            internal void Scan(Context cx)
            {
                for (var b=First();b!=null;b=b.Next())
                {
                    var (p, ids) = b.value();
                    cx.ObUnheap(p);
                    ids?.Scan(cx);
                }
            }
            internal Idents Relocate(Context cx)
            {
                var r = Empty;
                for (var b=First();b!=null;b=b.Next())
                {
                    var n = b.key();
                    var (p, ids) = b.value();
                    r += (n, cx.obuids[p], ids?.Relocate(cx));
                }
                return r;
            }
            internal Idents Relocate(Level2.Writer wr)
            {
                var r = Empty;
                for (var b = First(); b != null; b = b.Next())
                {
                    var n = b.key();
                    var (p, ids) = b.value();
                    r += (n, wr.Fix(p), ids?.Relocate(wr));
                }
                return r;
            }
            public override ATree<string, (long, Idents)> Add(ATree<string, (long, Idents)> a)
            {
                return new Idents((BTree<string,(long,Idents)>)base.Add(a));
            }
            public override string ToString()
            {
                var sb = new StringBuilder();
                for (var b=First();b!=null;b=b.Next())
                {
                    sb.Append(b.key()); sb.Append("=(");
                    var (p, ids) = b.value();
                    if (p >= 0)
                        sb.Append(DBObject.Uid(p));
                    sb.Append(",");
                    if (ids!=null)
                        sb.Append(ids.ToString());
                    sb.Append(");");
                }
                return sb.ToString();
            }
        }
        internal class IdBookmark
        {
            internal readonly ABookmark<string, (long, Idents)> _bmk;
            internal readonly Ident _parent,_key;
            internal readonly int _pos;
            internal IdBookmark(ABookmark<string,(long,Idents)> bmk,
                Ident parent, int pos)
            {
                _bmk = bmk; _parent = parent;  _pos = pos;
                _key = new Ident(_parent,_bmk.key());
            }
            public Ident key()
            {
                return _key;
            }
            public long value()
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
                    if (p != -1L)
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
        /// If there are more than 2048 reserved words, the hp will hang
        /// </summary>
		class ResWd
		{
			public Sqlx typ;
			public string spell;
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
        internal object Diag { get { if (val == TNull.Value) return tok; return val; } }
       /// <summary>
        /// Constructor: Start a new lexer
        /// </summary>
        /// <param name="s">the input string</param>
        internal Lexer(string s,long off = 0)
        {
   		    input = s.ToCharArray();
			pos = -1;
            offset = off;
			Advance();
			tok = Next();
        }
        internal Lexer(Ident id) : this(id.ident, id.iix) { }
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
            char minusch = ' '; // allow negative number
			if (char.IsDigit(ch)||ch=='-')
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
