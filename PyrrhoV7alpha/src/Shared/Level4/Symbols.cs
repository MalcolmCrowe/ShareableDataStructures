using System.Text;
using Pyrrho.Level3;
using Pyrrho.Common;
using Pyrrho.Level5;
using System.Reflection.Emit;
using System.Reflection.PortableExecutable;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level4
{
    internal class Iix : IComparable
    {
        internal readonly long lp; // lexical (if not equal to dp)
        internal readonly long sd; // select depth or instance reference
        internal readonly long dp; // defining
        internal static Iix None = new (-1L);
        internal Iix(long u) { lp = u; sd = 0;  dp = u;  }
        internal Iix(long l,long s,long u) { lp = l; sd = s; dp = u;  }
        internal Iix(Iix ix,long u) { lp = ix.lp; sd = ix.sd; dp = u; }
        internal Iix(long l,Context cx,long u) { lp = l; sd = cx.sD; dp = u;  }
        public int CompareTo(object? obj)
        {
            if (obj == null)
                return -1;
            var that = (Iix)obj;
            return lp.CompareTo(that.lp);
        }
        public override string ToString()
        {
            if (dp < 0)
            {
                if (this == None)
                    return "None";
            }
            var sb = new StringBuilder(DBObject.Uid(sd));
            sb.Append(':'); sb.Append(DBObject.Uid(lp));
            if (dp != lp)
            { sb.Append('|'); sb.Append(DBObject.Uid(dp)); }
            return sb.ToString();
        }
    }
    /// <summary>
    /// Implement an identifier lexeme class.
    /// 
    /// </summary>
    internal class Ident : IComparable
    {
        internal readonly Ident? sub;
        internal readonly Iix iix;
        internal readonly string ident;
        /// <summary>
        /// During parsing of Compiled objects this process allocates a new Stmt location 
        /// for every identifier processed, because LexPos() calls GetUid().
        /// This may seem wasteful but isn't really.
        /// </summary>
        /// <param name="psr"></param>
        internal Ident(Parser psr)
        {
            var lx = psr.lxr;
            iix = psr.LexPos();
            ident = ((lx.tok == Qlx.Id) ? lx.val?.ToString() : lx.tok.ToString()) ?? (DBObject.Uid(iix.dp));
            sub = null;
        }
        internal Ident(Ident lf, Ident sb)
        {
            ident = lf.ident;
            iix = lf.iix;
            sub = sb;
        }
        internal Ident(string s, Iix dp)
        {
            iix = dp;
            ident = s;
        }
        internal Ident(string s, Iix dp,Ident? sb)
        {
            iix = dp;
            ident = s;
            sub = sb;
        }
        internal int Length => 1 + (sub?.Length ?? 0);
        internal Ident? Prefix(int n) // if n>=Length we return this
        {
            if (n < 0)
                return null;
            return new Ident(ident, iix, sub?.Prefix(n-1));
        }
        internal Ident? this[int i]
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
                sb.Append('.');
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
        public int CompareTo(object? obj)
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
        /// The class is used for identifiers that have columns or are columns.
        /// Therefore used for select lists as it consists of columns in the result.
        /// (There is an annoying aspect of this implementation: 
        ///  If we have a pair (Iix,Idents) and Idents is not empty, the sd part is an instance reference
        /// </summary>
        internal class Idents : BTree<string, BTree<long,(Iix, Idents)>>
        {
            public new static Idents Empty = new();
            Idents() : base() { }
            internal Idents(BTree<string, BTree<long,(Iix, Idents)>> b) : base(b.root ?? throw new PEException("PE625")) 
            { }
            public static Idents operator +(Idents t, (string, Iix, Idents) x)
            {
                var (n, iix, ids) = x;
                if (n == null || n == "" || iix==null)
                    return t;
                var s = t[n]??BTree<long,(Iix,Idents)>.Empty;
                // discard out-of-scope items
                for (var b = s.PositionAt(iix.sd+1); b != null; b = b.Next())
                    s -= b.key();
                return new Idents(t + (n,s+(iix.sd,(iix,ids))));
            }
            public static Idents operator +(Idents t, (Ident, Iix) x)
            {
                var (id, p) = x;
                if (id.ident == null || id.ident == "")
                    return t;
                if (t.Contains(id.ident))
                {
                    var s = t[id.ident]??BTree<long,(Iix,Idents)>.Empty;
                    if (s.Contains(p.sd))
                    {
                        var (to, ts) = s[p.sd];
                        if (id.sub != null)
                            s += (p.sd, (to, ts + (id.sub, p)));
                        else
                            s += (p.sd, (p, ts));
                    }
                    else if (id.sub != null)
                        s += (p.sd, (new Iix(p, p.lp),
                            new(new BTree<string, BTree<long, (Iix, Idents)>>
                                (id.sub.ident, new BTree<long, (Iix, Idents)>(p.sd, (p, Empty))))));
                    else
                        s += (p.sd, (p, Empty));
                    return new Idents(t + (id.ident, s));
                }
                else
                {
                    var ts = Empty;
                    if (id.sub != null)
                        ts += (id.sub, p);
                    return new Idents(t + (id.ident, new BTree<long,(Iix,Idents)>(p.sd,(p, ts))));
                }
            }
            public static Idents operator +(Idents t, (Ident, long) x)
            {
                var (id, n) = x;
                if (id.ident == null || id.ident == "")
                    return t;
                (Iix where,Idents subs) ts = (id.iix,Empty);
                var s = t[id.ident]??BTree<long,(Iix,Idents)>.Empty;
                if (s[id.iix.sd] is (Iix, Idents) tu)
                    ts = tu;
                var ti = ts.subs;
                if (id.sub != null && n > 0)
                    ti += (id.sub, n - 1);
                return new Idents(t + (id.ident, s + (id.iix.sd,(ts.where, ti))));
            }
            public static Idents operator-(Idents t,string s)
            {
                BTree<string, BTree<long, (Iix, Idents)>> n = t;
                n -= s;
                return (n.root != null) ? new Idents(n) : Empty;
            }
            /// <summary>
            /// Identifier chain lookup function. Search in this
            /// for a given chain, stopping at a given length.
            /// Rarely used: cx.Lookup is preferred since it
            /// uses the cx.obs information for well-defined objects.
            /// </summary>
            /// <param name="x">A triple: (chain,len,breadcrumbtrail)</param>
            /// <returns>(Deepest Iix found, breadcrumbtrail,descendants, rest of chain)</returns>
            internal (BList<Iix>,Idents?,Ident?) this[(Ident,long,BList<Iix>) x]
            {
                get
                {
                    var (ic, d, bc) = x;
                    if (ic == null || !Contains(ic.ident) || d < 1)
                        return (bc, null, ic);
                    var ix = Iix.None;
                    Idents ids = Empty;
                    var s = this[ic.ident] ?? BTree<long, (Iix, Idents)>.Empty;
                    for (var sd = ic.iix.sd;ix == Iix.None && sd>=0;sd--)
                        if (s.Contains(sd))
                           (ix, ids) = s[sd];
                    if (ids != Empty && ic.sub != null && d > 1 && ids.Contains(ic.sub.ident))
                        return ids[(ic.sub, d - 1, bc + ix)];
                    return (bc + ix, ids, ic.sub);
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
                    var (bc, _, s) = this[(ic, ic.Length, BList<Iix>.Empty)];
                    if (s != null)
                        return Iix.None;
                    return bc.Last()?.value()??Iix.None;
                }
            }
            internal (Iix,Idents) this[(string?,long)x]
            {
                get
                {
                    var (s, d) = x;
                    if (s != null && Contains(s))
                        for (var b = PositionAt(s)?.value()?.Last(); b != null; b = b.Previous())
                        {
                            if (b.key() <= d)
                                return b.value();
                        }
                    return (Iix.None, Empty);
                }
            }
            internal Idents ApplyDone(Context cx)
            {
                var r = BTree<string, BTree<long,(Iix,Idents)>>.Empty;
                for (var b=First();b is not null;b=b.Next())
                 if (b.value() is BTree<long,(Iix,Idents)> x){
                    for (var s = x.First(); s != null; s = s.Next())
                    {
                        var (p, st) = s.value();
                        if (p.dp != -1L && cx.done[p.dp] is DBObject nb)
                        {
                            p = new Iix(p.lp,p.sd,nb.defpos);
                            for (var c = (nb as Domain)?.rowType.First(); c != null; c = c.Next())
                                if (c.value() is long cp && cx.done[cp] is QlValue v && v.name is not null)
                                {
                                    var ds = st[v.name] ?? BTree<long,(Iix,Idents)>.Empty;
                                    st = new Idents(st + (v.name, ds +(p.sd,(new Iix(v.defpos,p.sd,v.defpos),Empty))));
                                }
                        }
                        st = st.ApplyDone(cx);
                        r += (b.key(), x+(p.sd,(p, st))); // do not change the string key part
                    }
                }
                return (r.Count>0)?new Idents(r):Idents.Empty;
            }
            internal Idents Relocate(Context cx)
            {
                var r = Empty;
                for (var b = First(); b != null; b = b.Next())
                    if (b.value() is BTree<long, (Iix, Idents)> x)
                    {
                        var n = b.key();
                        for (var c = x.First(); c != null; c = c.Next())
                        {
                            var (p, ids) = c.value();
                            r += (n, cx.Fix(p), ids.Relocate(cx));
                        }
                    }
                return r;
            }
            public override ATree<string, BTree<long,(Iix, Idents)>> Add(ATree<string, BTree<long,(Iix, Idents)>> a)
            {
                return new Idents((BTree<string,BTree<long,(Iix,Idents)>>)base.Add(a));
            }
            public override string ToString()
            {
                var sb = new StringBuilder();
                for (var b = First(); b != null; b = b.Next())
                    if (b.value() is BTree<long, (Iix, Idents)> x)
                    {
                        sb.Append(b.key()); sb.Append("=(");
                        for (var c = x.First(); c != null; c = c.Next())
                        {
                            var (p, ids) = c.value();
                            sb.Append(p.ToString()); sb.Append(',');
                            if (ids != Empty)
                                sb.Append(ids.ToString());
                        }
                        sb.Append(");");
                    }
                return sb.ToString();
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
		public int pos,pushPos,prevPos;
        /// <summary>
        /// The start of tok in the input string
        /// </summary>
		public int start = 0, pushStart;
        public bool allowminus = false;
        public bool caseSensitive = false;
        public bool doublequoted = false;
        public bool docValue = false; // caseSensitive matters if docValue is true
        /// <summary>
        /// the current character in the input string
        /// </summary>
		char ch,pushCh;
        /// <summary>
        /// The current token's identifier
        /// </summary>
		public Qlx tok, prevtok = Qlx.Null;
        public Qlx pushBack = Qlx.Null;
        public long offset;
        public TGParam.Type tgg = TGParam.Type.None;
        public long tga;
        public bool tex = false; // expecting a type?
        public bool cat = false; // for GQL catalog parent (case sensitive, / and . in identifiers)
        public long Position => offset + start;
        /// <summary>
        /// The current token's value
        /// </summary>
		public TypedValue val = TNull.Value, prevval = TNull.Value;
        public TypedValue pushVal = TNull.Value;
        public bool ParsingMatch = false;
        private readonly Context cx; // only used for type prefix/suffix things
        public CTree<long,TGParam> tgs = CTree<long,TGParam>.Empty; // TGParam wizardry
        /// <summary>
        /// Entries in the reserved word table
        /// If there are more than 2048 reserved words, the server will hang
        /// 
        /// </summary>
		class ResWd(Qlx t, string s)
        {
			public readonly Qlx typ = t;
			public readonly string spell = s;
        }
        readonly static ResWd[] resWds = new ResWd[0x800]; // open hash
        static Lexer()
        {
            for (Qlx t = Qlx.ABS; t <= Qlx.ZONED_TIME; t++)
                if (t != Qlx.CLOB && t != Qlx.CURSOR && t != Qlx.INTERVAL0
                    && t != Qlx.MULTISET && t != Qlx.NCHAR && t != Qlx.NCLOB
                    && t != Qlx.NUMERIC && t != Qlx.PASSWORD)
                    AddResWd(t);
            AddResWd(Qlx.SET);
            AddResWd(Qlx.TIMESTAMP);
        }
        static void AddResWd(Qlx t)
        {
            string s = t.ToString();
            var h = s.GetHashCode() & 0x7ff;
            while (resWds[h] != null)
                h = (h + 1) & 0x7ff;
            resWds[h] = new ResWd(t, s);
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
        internal object Diag { get { return (val == TNull.Value) ? tok : val; } }
       /// <summary>
        /// Constructor: Start a new lexer
        /// </summary>
        /// <param name="s">the input string</param>
        internal Lexer(Context cx,string s,long off = Transaction.Analysing,bool am=false)
        {
   		    input = s.ToCharArray();
			pos = -1;
            offset = off;
            allowminus = am;
			Advance();
            this.cx = cx;
			tok = Next();
            caseSensitive = cx.conn.caseSensitive;
        }
        internal Lexer(Context cx,Ident id) : this(cx, id.ident, id.iix.lp) { }
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
            return c switch
            {
                '0' => 0,
                '1' => 1,
                '2' => 2,
                '3' => 3,
                '4' => 4,
                '5' => 5,
                '6' => 6,
                '7' => 7,
                '8' => 8,
                '9' => 9,
                'a' => 10,
                'b' => 11,
                'c' => 12,
                'd' => 13,
                'e' => 14,
                'f' => 15,
                'A' => 10,
                'B' => 11,
                'C' => 12,
                'D' => 13,
                'E' => 14,
                'F' => 15,
                _ => -1,
            };
        }
        public Qlx PushBack(Qlx old)
        {
            pushBack = tok;
            pushVal = val;
            pushStart = start;
            pushPos = pos;
            pushCh = ch;
            tok = old;
            return tok;
        }
        internal void Rescan()
        {
            pos = prevPos;
            ch = input[pos];
        }
        readonly static Domain NodeArray = new (-999, Qlx.ARRAY, Domain.NodeType);
        readonly static Domain CharArray = new (-998, Qlx.ARRAY, Domain.Char);
        /// <summary>
        /// MaybePrefix watches for GQL label expressions and deals with prefixable types.
        /// In this version we can handle simple labels and & and | cobinations only
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        Qlx MaybePrefix(string s)
        {
            var vo = (val is TChar tc) ? tc.value : s;
            var gd = (tgg == TGParam.Type.None) ? Domain.NodeType : NodeArray;
            var gc = (tgg == TGParam.Type.None) ? Domain.Char : CharArray;
            if (tgs != null)
            {
                switch (prevtok)
                {
                    case Qlx.LPAREN:
                        {
                            var tg = new TGParam(Position, vo, gd, TGParam.Type.Node | tgg, tga);
                            tgs += (tg.uid, tg);
                            break;
                        }
                    case Qlx.ARROWBASE:
                    case Qlx.RARROW:
                        {
                            var tg = new TGParam(Position, vo, gd, TGParam.Type.Edge | tgg, tga);
                            tgs += (tg.uid, tg);
                            break;
                        }
                    case Qlx.LBRACE:
                    case Qlx.COMMA:
                        {
                            /*                      if (tgt.Contains(vo))
                                                      break;
                                                  var tg = new TGParam(Position, vo, gc, TGParam.Type.Field|tgg, tga);
                                                  tgs += (tg.uid, tg); */
                            break;
                        }
                    case Qlx.COLON:
                        {
                            var tg = new TGParam(Position, vo, gc,
                                (tex ? TGParam.Type.Type : TGParam.Type.Value) | tgg, tga);
                            tgs += (tg.uid, tg);
                            break;
                        }
                    default:
                        {
                            var tg = new TGParam(Position, vo, gc, TGParam.Type.None | tgg, tga);
                            tgs += (tg.uid, tg);
                            break;
                        }
                }
            }
            if (cx.defs[vo]?[cx.sD].Item1 is not null || cx.role.dbobjects.Contains(s))
                return tok;
            if (cx.parse == ExecuteStatus.Obey && cx.db is not null
                && cx.role is not null && cx.db.prefixes != BTree<string, long?>.Empty
                && cx.db.objects[cx.db.prefixes[s] ?? -1L] is UDType dt && val is not null
                && dt.name is not null)
            {
                var ps = pos;
                Next();
                var sig = new CList<Domain>(val.dataType);
                if (dt.infos[cx.role.defpos] is ObInfo mi && dt.name is not null &&
                    mi.methodInfos[dt.name] is BTree<CList<Domain>, long?> md
                    && cx.db.objects[md[sig] ?? -1L] is Method mt)
                {
                    cx.Add(new SqlLiteral(ps, Domain._Numeric.Coerce(cx, val)));
                    val = mt.Exec(cx, new BList<long?>(ps)).val;
                }
                else
                    val = new TSubType(dt, val);
            }
            return tok;
        }
        Qlx MaybeSuffix()
        {
            if (cx.parse == ExecuteStatus.Obey)
            {
                var oldtok = tok;
                var oldval = val;
                var sig = new CList<Domain>(val.dataType);
                var oldstart = start;
                var oldch = ch;
                var t = Next();
                var vs = val?.ToString();
                if (t == Qlx.Id && vs != null && cx.db is not null && cx.role is not null
                    && cx.db.objects[cx.db.suffixes[vs]??-1L] is UDType dt
                    && dt.name != null && prevval is not null)
                {
                    if (dt.infos[cx.role.defpos] is ObInfo mi 
                        && mi.methodInfos[dt.name] is BTree<CList<Domain>, long?> md
                        && cx.db.objects[md[sig]??-1L] is Method mt
                        && cx.Add(new SqlLiteral(cx.GetUid(), prevval)) is QlValue r)
                        val = mt.Exec(cx, new BList<long?>(r.defpos)).val;
                    else
                        val = new TSubType(dt, prevval);
                    tok = oldtok;
                    return tok;
                }
                PushBack(t);
                tok = oldtok;
                val = oldval;
                start = oldstart;
                ch = oldch;
            }
            return tok;
        }
        /// <summary>
        /// Advance to the next token in the input.
        /// tok and val are set for the new token
        /// </summary>
        /// <returns>The new value of tok</returns>
		public Qlx Next()
        {
            if (pushBack != Qlx.Null)
            {
                tok = pushBack;
                val = pushVal;
                start = pushStart;
                pos = pushPos;
                ch = pushCh;
                pushBack = Qlx.Null;
                return tok;
            }
            prevtok = tok;
            prevval = val;
            prevPos = pos;
            val = TNull.Value;
            while (char.IsWhiteSpace(ch))
                Advance();
            start = pos;
            doublequoted = ch == '"';
            if (char.IsLetter(ch) || (cat && (ch=='/'||ch=='.')))
            {
                char c = ch;
                Advance();
                if (c == 'X' && ch == '\'')
                {
                    int n = 0;
                    if (Hexit(Advance()) >= 0)
                        n++;
                    while (ch != '\'')
                        if (Hexit(Advance()) >= 0)
                            n++;
                    n /= 2;
                    byte[] b = new byte[n];
                    int end = pos;
                    pos = start + 1;
                    for (int j = 0; j < n; j++)
                    {
                        while (Hexit(Advance()) < 0)
                            ;
                        int d = Hexit(ch) << 4;
                        d += Hexit(Advance());
                        b[j] = (byte)d;
                    }
                    while (pos != end)
                        Advance();
                    tok = Qlx.BLOBLITERAL;
                    val = new TBlob(b);
                    Advance();
                    MaybeSuffix();
                    return tok;
                }
                while (char.IsLetterOrDigit(ch) || ch == '_'
                    || (cat && (ch=='/'||ch=='.')))
                    Advance();
                string s0 = new(input, start, pos - start);
                string s = (caseSensitive||cat)?s0:s0.ToUpper();
                if (CheckResWd(s))
                {
                    switch (tok)
                    {
                        case Qlx.TRUE: val = TBool.True; return Qlx.BOOLEANLITERAL;
                        case Qlx.FALSE: val = TBool.False; return Qlx.BOOLEANLITERAL;
                        case Qlx.NULL: val = TNull.Value; return Qlx.NULL;
                        case Qlx.UNKNOWN: val = TNull.Value; return Qlx.BOOLEANLITERAL;
                        case Qlx.CURRENT_DATE: val = new TDateTime(DateTime.Today); return tok;
                        case Qlx.CURRENT_TIME: val = new TTimeSpan(DateTime.Now - DateTime.Today); return tok;
                        case Qlx.CURRENT_TIMESTAMP: val = new TDateTime(DateTime.Now); return tok;
                    }
                    return tok;
                }
                val = new TChar(s);
                tok = Qlx.Id;
                MaybePrefix(s);
                return tok;
            }
            string str;
            char minusch = ' '; // allow negative number?
            if (char.IsDigit(ch) || (allowminus && ch == '-'))
            {
                start = pos;
                if (ch == '-')
                    Advance();
                if (ch == '[')
                {
                    Advance();
                    return tok = Qlx.ARROWBASE;
                }
                if (!char.IsDigit(ch))
                {
                    minusch = ch;
                    ch = '-';
                    goto uminus;
                }
                while (char.IsDigit(Advance()))
                    ;
                if (ch != '.')
                {
                    str = new string(input, start, pos - start);
                    if (pos - start > 18)
                        val = new TInteger(Integer.Parse(str));
                    else
                        val = new TInt(long.Parse(str));
                    tok = Qlx.INTEGERLITERAL;
                    MaybeSuffix();
                    return tok;
                }
                while (char.IsDigit(Advance()))
                    ;
                if (ch != 'e' && ch != 'E')
                {
                    str = new string(input, start, pos - start);
                    val = new TNumeric(Common.Numeric.Parse(str));
                    tok = Qlx.NUMERICLITERAL;
                    MaybeSuffix();
                    return tok;
                }
                if (Advance() == '-' || ch == '+')
                    Advance();
                if (!char.IsDigit(ch))
                    throw new DBException("22107").Mix();
                while (char.IsDigit(Advance()))
                    ;
                str = new string(input, start, pos - start);
                val = new TReal(Numeric.Parse(str));
                tok = Qlx.REALLITERAL;
                MaybeSuffix();
                return tok;
            }
        uminus:
            switch (ch)
            {
                case '[': Advance(); return tok = Qlx.LBRACK;
                case ']': Advance(); 
                    if (ch=='-')
                    {
                        Advance();
                        if (ch=='>')
                        {
                            Advance();
                            return tok = Qlx.ARROW;
                        }
                        return tok = Qlx.RARROWBASE;
                    }
                    return tok = Qlx.RBRACK;
                case '(': Advance(); return tok = Qlx.LPAREN;
                case ')': Advance(); return tok = Qlx.RPAREN;
                case '{': Advance(); return tok = Qlx.LBRACE;
                case '}': Advance(); return tok = Qlx.RBRACE;
                case '+': Advance(); return tok = Qlx.PLUS;
                case '*': Advance(); return tok = Qlx.TIMES;
                case '/': Advance(); return tok = Qlx.DIVIDE;
                case ',': Advance(); return tok = Qlx.COMMA;
                case '.': Advance(); return tok = Qlx.DOT;
                case ';': Advance(); return tok = Qlx.SEMICOLON;
                case '&': Advance(); return tok = Qlx.AMPERSAND; // GQL label expression
                case '~': Advance(); return tok = Qlx.TILDE;  // GQL
                case '?': Advance(); return tok = Qlx.QMARK; // added for Prepare()
                case ':':
                    {
                        Advance();
                        if (ch == ':')
                        {
                            Advance();
                            return tok = Qlx.DOUBLECOLON;
                        }
                        return tok = Qlx.COLON;
                    }
                case '-':
                    if (minusch == ' ')
                        Advance();
                    else
                        ch = minusch;
                    if (ch=='>')
                    {
                        Advance();
                        return tok = Qlx.ARROWR;
                    }
                    if (ch == '-')
                    {
                        Advance();    // -- comment
                        while (pos < input.Length)
                            Advance();
                        return Next();
                    }
                    if (ch=='[')
                    {
                        Advance();
                        return tok = Qlx.ARROWBASE;
                    }
                    return tok = Qlx.MINUS;
                case '|':
                    if (Advance() == '|')
                    {
                        Advance();
                        return tok = Qlx.CONCATENATE;
                    }
                    return tok = Qlx.VBAR;
                case '<':
                    if (Advance() == '=')
                    {
                        Advance();
                        return tok = Qlx.LEQ;
                    }
                    else if (ch == '>')
                    {
                        Advance();
                        return tok = Qlx.NEQ;
                    }
                    if (ch=='-')
                    {
                        Advance();
                        if (ch=='[')
                        {
                            Advance();
                            return tok = Qlx.RARROW;
                        }
                        return tok = Qlx.ARROWL;
                    }
                    return tok = Qlx.LSS;
                case '=': 
                    if (Advance()=='>')
                    {
                        Advance();
                        return Qlx.DOUBLEARROW;
                    }
                    return tok = Qlx.EQL;
                case '>':
                    if (Advance() == '=')
                    {
                        Advance();
                        return tok = Qlx.GEQ;
                    }
                    return tok = Qlx.GTR;
                case '"':   // delimited identifier if caseSensitive is false
                    {
                        start = pos;
                        while (Advance() != '"')
                            ;
                        var v0 = new string(input, start + 1, pos - start - 1);
                        val = new TChar(v0);
                        Advance();
                        while (ch == '"')
                        {
                            var fq = pos;
                            while (Advance() != '"')
                                ;
                            v0 += new string(input, fq, pos - fq);
                            val = new TChar(v0);
                            Advance();
                        }
                        if (caseSensitive && docValue && cx.defs[v0] is null)
                            return tok=Qlx.CHARLITERAL;   
                        tok = Qlx.Id;
                        MaybePrefix(val.ToString());
                        return tok;
                    }
                case '\'':
                    {
                        start = pos;
                        var qs = new Stack<int>();
                        qs.Push(-1);
                        int qn = 0;
                        for (; ; )
                        {
                            while (Advance() != '\'')
                                ;
                            if (Advance() != '\'')
                                break;
                            qs.Push(pos);
                            qn++;
                        }
                        char[] rb = new char[pos - start - 2 - qn];
                        int k = pos - start - 3 - qn;
                        int p = -1;
                        if (qs.Count > 1)
                            p = qs.Pop();
                        for (int j = pos - 2; j > start; j--)
                        {
                            if (j == p)
                                p = qs.Pop();
                            else
                                rb[k--] = input[j];
                        }
                        val = new TChar(new string(rb));
                        return tok = Qlx.CHARLITERAL;
                    }
                // These are for the new Position Domain in v7. Positions are always longs
                case '!':
                    {
                        Advance();
                        while (char.IsDigit(Advance()))
                            ;
                        str = new string(input, start + 1, pos - start - 1);
                        val = new TInt(long.Parse(str) + Transaction.TransPos);
                        tok = Qlx.INTEGERLITERAL;
                        return tok;
                    }
                case '#':
                    {
                        Advance();
                        while (char.IsDigit(Advance()))
                            ;
                        str = new string(input, start + 1, pos - start - 1);
                        val = new TInt(long.Parse(str) + Transaction.Analysing);
                        tok = Qlx.INTEGERLITERAL;
                        return tok;
                    }
                case '`':
                    {
                        Advance();
                        while (char.IsDigit(Advance()))
                            ;
                        str = new string(input, start + 1, pos - start - 1);
                        val = new TInt(long.Parse(str) + Transaction.Executables);
                        tok = Qlx.INTEGERLITERAL;
                        return tok;
                    }
                case '%':
                    {
                        Advance();
                        while (char.IsDigit(Advance()))
                            ;
                        str = new string(input, start + 1, pos - start - 1);
                        val = new TInt(long.Parse(str) + Transaction.HeapStart);
                        tok = Qlx.INTEGERLITERAL;
                        return tok;
                    }
                case '\0':
                    return tok = Qlx.EOF;
            }
            throw new DBException("42101", ch).Mix();
        }
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
            Stack<char> nest = new ();
            char quote = (char)0;
            int n = stop.Length;
            int start = pos;
            char prev = (char)0;
            for (; ; )
            {
                if (nest.Count==0 && quote == (char)0)
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
                        if (nest.Count==0 || ch != nest.Peek())
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
        public static string UnLex(Qlx s)
        {
            return s switch
            {
                Qlx.EQL => "=",
                Qlx.NEQ => "<>",
                Qlx.LSS => "<",
                Qlx.GTR => ">",
                Qlx.LEQ => "<=",
                Qlx.GEQ => ">=",
                Qlx.PLUS => "+",
                Qlx.MINUS => "-",
                Qlx.TIMES => "*",
                Qlx.DIVIDE => "/",
                _ => s.ToString(),
            };
        }
     }
}
