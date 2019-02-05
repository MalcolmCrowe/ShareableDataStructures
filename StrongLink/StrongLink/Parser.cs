using System;
using Shareable;
#nullable enable
namespace StrongLink
{
    public class Parser
    {
        internal enum Sym
        {
            Null = 0,
            ID = 1,
            LITERAL = 2,
            LPAREN = 3,
            COMMA = 4,
            RPAREN = 5,
            EQUAL = 6, // EQUAL to GTR must be adjacent
            NEQ = 7,
            LEQ = 8,
            LSS = 9,
            GEQ = 10,
            GTR = 11,
            DOT = 12,
            PLUS = 13,
            MINUS = 14,
            TIMES = 15,
            DIVIDE = 16,
            //=== RESERVED WORDS
            ADD = 17,
            ALTER = 18,
            AND = 19,
            AS = 20,
            BEGIN = 21,
            BOOLEAN = 22,
            COLUMN = 23,
            COMMIT = 24,
            COUNT = 25,
            CREATE = 26,
            CROSS = 27,
            DATE = 28,
            DELETE = 29,
            DESC = 30,
            DISTINCT = 31,
            DROP = 32,
            FALSE = 33,
            FOR = 34,
            FROM = 35,
            FULL = 36,
            GROUPBY = 37,
            HAVING = 38,
            INDEX = 39,
            INSERT = 40,
            INTEGER = 41,
            IN = 42,
            INNER = 43,
            IS = 44,
            JOIN = 45,
            LEFT = 46,
            MAX = 47,
            MIN = 48,
            NATURAL = 49,
            NOT = 50,
            NULL = 51,
            NUMERIC = 52,
            ON = 53,
            OR = 54,
            ORDERBY = 55,
            OUTER = 56,
            PRIMARY = 57,
            REFERENCES = 58,
            RIGHT = 59,
            ROLLBACK = 60,
            SELECT = 61,
            SET = 62,
            STRING = 63,
            SUM = 64,
            TABLE = 65,
            TIMESPAN = 66,
            TO = 67,
            TRUE = 68,
            UPDATE = 69,
            VALUES = 70,
            WHERE = 71
        }
        internal class Lexer
        {
            public readonly char[] input;
            internal int pos = -1;
            int? pushPos = null;
            internal Sym tok;
            internal Sym pushBack = Sym.Null;
            internal Serialisable val = Serialisable.Null;
            char ch = '\0';
            char? pushCh = null;
            public Lexer(string inp)
            {
                input = inp.ToCharArray();
                Advance();
                tok = Next();
            }
            char Advance()
            {
                if (pos >= input.Length)
                    throw new Exception("Non-terminated string");
                ch = (++pos >= input.Length) ? '\0' : input[pos];
                return ch;
            }
            char Peek()
            {
                return (pos+1 >= input.Length)?'\0':input[pos + 1];
            }
            Sym PushBack(Sym old)
            {
                pushBack = old;
                pushCh = ch;
                pushPos = pos;
                tok = old;
                return tok;
            }
            Integer Unsigned()
            {
                var v = new Integer(ch - '0');
                for (Advance(); ch != '\0' && char.IsDigit(ch); Advance())
                    v = v.Times(10) + new Integer(ch - '0');
                return v;
            }
            Integer Unsigned(Integer v)
            {
                for (Advance(); ch != '\0' && char.IsDigit(ch); Advance())
                    v = v.Times(10) + new Integer(ch - '0');
                return v;
            }
            int Unsigned(int n)
            {
                var st = pos;
                var r = Unsigned();
                if (pos != st + n)
                    throw new Exception("Expected " + n + " digits");
                return r;
            }
            int Unsigned(int n, int low, int high)
            {
                var r = Unsigned(n);
                if (r < low || r > high)
                    throw new Exception("Expected " + low + "<=" + r + "<=" + high);
                return r;
            }
            void Mustbe(char c)
            {
                if (c != ch)
                    throw new Exception("Expected " + c + " got " + ch);
                Advance();
            }
            Serialisable DateTimeLiteral()
            {
                var st = pos;
                var y = Unsigned(4);
                Mustbe('-');
                var mo = Unsigned(2, 1, 12);
                Mustbe('-');
                var d = Unsigned(2, 1, 31);
                if (ch == '\'')
                {
                    Advance();
                    return new SDate(new DateTime(y, mo, d));
                }
                Mustbe('T');
                var p = pos;
                while (ch != '\'')
                    Advance();
                Advance();
                return new SDate(y, mo, 
                    TimeSpan.Parse(new string(input,p,pos-p-1)).Ticks+
                    (d-1)*TimeSpan.TicksPerDay);
            }
            internal Sym Next()
            {
                if (pushBack!=Sym.Null)
                {
                    tok = pushBack;
                    pos = pushPos??0;
                    ch = pushCh??'\0';
                    pushBack = Sym.Null;
                    return tok;
                }
                while (char.IsWhiteSpace(ch))
                    Advance();
                var st = pos;
                if (ch == '\0')
                    return tok=Sym.Null;
                if (char.IsDigit(ch))
                {
                    var n = Unsigned();
                    if (ch == '.')
                    {
                        var p = pos;
                        var m = Unsigned(n);
                        var q = pos;
                        var e = 0;
                        if (ch == 'e' || ch == 'E')
                        {
                            Advance();
                            var esg = 1;
                            if (ch == '-')
                                esg = -1;
                            if (ch == '-' || ch == '+')
                                Advance();
                            e = Unsigned() * esg;
                        }
                        val = new SNumeric(m, q - p, q - p-1-e);
                    } else
                        val = new SInteger(n);
                    return tok = Sym.LITERAL;
                }
                else if (ch == '\'')
                {
                    st++;
                    for (Advance(); ch != '\0'; Advance())
                    {
                        if (ch=='\'')
                        {
                            if (Peek() == '\'')
                                Advance();
                            else
                                break;
                        }
                    }
                    if (ch == '\0')
                        throw new Exception("non-terminated string literal");
                    Advance();
                    val = new SString(new string(input, st, pos - st - 1).Replace("''","'"));
                    return tok = Sym.LITERAL;
                }
                else if (char.IsLetter(ch) || ch=='_')
                {
                    for (Advance(); char.IsLetterOrDigit(ch) || ch == '_'; Advance())
                        ;
                    var s = new string(input, st, pos - st);
                    var su = s.ToUpper();
                    for (var t = Sym.ADD; t <= Sym.WHERE; t++)
                        if (su.CompareTo(t.ToString()) == 0)
                            switch (t)
                            {
                                case Sym.DATE:
                                    if (ch == '\'')
                                    {
                                        Advance();
                                        val = DateTimeLiteral();
                                        return tok = Sym.LITERAL;
                                    }
                                    return tok = t;
                                case Sym.TIMESPAN:
                                    if (ch == '\'')
                                    {
                                        Advance();
                                        var p = pos;
                                        while (ch != '\'')
                                            Advance();
                                        Advance();
                                        val = new STimeSpan(TimeSpan.Parse(new string(input, p, pos - p - 1)));
                                        return tok = Sym.LITERAL;
                                    }
                                    return tok = t;
                                case Sym.FALSE: val = SBoolean.False;
                                    return tok = Sym.LITERAL;
                                case Sym.TRUE:
                                    val = SBoolean.True;
                                    return tok = Sym.LITERAL;
                                default:
                                    return tok = t;
                            }
                    val = new SString(s);
                    return tok = Sym.ID;
                }
                else
                    switch (ch)
                    {
                        case '.': Advance(); return tok = Sym.DOT;
                        case '+': Advance(); return tok = Sym.PLUS;
                        case '-': Advance(); return tok = Sym.MINUS;
                        case '*': Advance(); return tok = Sym.TIMES;
                        case '/': Advance(); return tok = Sym.DIVIDE;
                        case '(': Advance(); return tok = Sym.LPAREN;
                        case ',': Advance(); return tok = Sym.COMMA;
                        case ')': Advance(); return tok = Sym.RPAREN;
                        case '=': Advance(); return tok = Sym.EQUAL;
                        case '!':
                            Advance();
                            if (ch == '=')
                            {
                                Advance();
                                return tok = Sym.NEQ;
                            }
                            else break;
                        case '<':
                            Advance();
                            if (ch == '=')
                            {
                                Advance();
                                return tok = Sym.LEQ;
                            }
                            return tok = Sym.LSS;
                        case '>':
                            Advance();
                            if (ch == '=')
                            {
                                Advance();
                                return tok = Sym.GEQ;
                            }
                            return tok = Sym.GTR;
                    }
                throw new Exception("Bad input " + ch + " at " + pos);
            }
        }
        Lexer lxr;
        Parser(string inp)
        {
            lxr = new Lexer(inp);
        }
        public static Serialisable Parse(string sql)
        {
            return new Parser(sql).Statement();
        }
        Sym Next()
        {
            return lxr.Next();
        }
        void Mustbe(Sym t)
        {
            if (lxr.tok != t)
                throw new Exception(CheckEOF());
            Next();
        }
        SString MustBeID()
        {
            var s = lxr.val;
            if (lxr.tok != Sym.ID || s==null)
                throw new Exception(CheckEOF());
            Next();
            return (SString)s;
        }
        Types For(Sym t)
        {
            switch (t)
            {
  //              case Sym.TIMESTAMP: return Types.STimestamp;
                case Sym.INTEGER: return Types.SInteger;
                case Sym.NUMERIC: return Types.SNumeric;
                case Sym.STRING: return Types.SString;
                case Sym.DATE: return Types.SDate;
                case Sym.TIMESPAN: return Types.STimeSpan;
                case Sym.BOOLEAN: return Types.SBoolean;
            }
            throw new Exception("Syntax error: " + t);
        }
        public Serialisable Statement()
        {
            switch(lxr.tok)
            {
                case Sym.ALTER:
                    return Alter();
                case Sym.CREATE:
                    {
                        Next();
                        switch (lxr.tok)
                        {
                            case Sym.TABLE:
                                return CreateTable();
                            case Sym.INDEX:
                                Next();
                                return CreateIndex();
                            case Sym.PRIMARY:
                                Next(); Mustbe(Sym.INDEX);
                                return CreateIndex(true);
                        }
                        throw new Exception("Unknown Create " + lxr.tok);
                    }
                case Sym.DROP:
                    return Drop();
                case Sym.INSERT:
                    return Insert();
                case Sym.DELETE:
                    return Delete();
                case Sym.UPDATE:
                    return Update();
                case Sym.SELECT:
                    return Select();
                case Sym.BEGIN:
                    return new Serialisable(Types.SBegin);
                case Sym.ROLLBACK:
                    return new Serialisable(Types.SRollback);
                case Sym.COMMIT:
                    return new Serialisable(Types.SCommit);
            }
            throw new Exception(CheckEOF());
        }
        /// <summary>
        /// Alter: ALTER table_id ADD id Type
	    /// | ALTER table_id DROP col_id 
        /// | ALTER table_id[COLUMN col_id] TO id[Type] .
        /// </summary>
        /// <returns></returns>
        Serialisable Alter()
        {
            Next();
            var tb = MustBeID();
            SString? col = null;
            var add = false;
            switch (lxr.tok)
            {
                case Sym.COLUMN:
                    Next();
                    col = MustBeID();
                    Mustbe(Sym.TO);
                    break;
                case Sym.DROP:
                    Next();
                    col = MustBeID();
                    return new SDropStatement(col.str, tb.str); // ok
                case Sym.TO:
                    Next(); break;
                case Sym.ADD:
                    Next(); add = true;
                    break;
            }
            var nm = MustBeID();
            Types dt = Types.Serialisable;
            switch (lxr.tok)
            {
   //             case Sym.TIMESTAMP: Next(); dt = Types.STimestamp; break;
                case Sym.INTEGER: Next(); dt = Types.SInteger; break;
                case Sym.NUMERIC: Next(); dt = Types.SNumeric; break;
                case Sym.STRING: Next(); dt = Types.SString; break;
                case Sym.DATE: Next(); dt = Types.SDate; break;
                case Sym.TIMESPAN: Next(); dt = Types.STimeSpan; break;
                case Sym.BOOLEAN: Next(); dt = Types.SBoolean; break;
                default: if (add)
                        throw new Exception("Type expected");
                    break;
            }
            if (col == null)
                throw new System.Exception("??");
            return new SAlterStatement(tb.str, col.str, nm.str, dt); // ok
        }
        Serialisable CreateTable()
        {
            Next();
            var id = MustBeID();
            var tb = id.str; // ok
            Mustbe(Sym.LPAREN);
            var cols = SList<SColumn>.Empty;
            for (; ; )
            {
                var c = MustBeID();
                var t = For(lxr.tok);
                cols = cols+(new SColumn(c.str, t), cols.Length??0); // ok
                Next();
                switch(lxr.tok)
                {
                    case Sym.RPAREN: Next(); return new SCreateTable(tb, cols);
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception(CheckEOF());
            }
        }
        SList<SSelector> Cols()
        {
            var cols = SList<SSelector>.Empty;
            Mustbe(Sym.LPAREN);
            for (; ;)
            {
                var c = MustBeID();
                cols = cols+(new SColumn(c.str), cols.Length??0); // ok
                switch (lxr.tok)
                {
                    case Sym.RPAREN: Next(); return cols;
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception(CheckEOF());
            }
        }
        SValues Vals()
        {
            Next();
            var cols = SList<Serialisable>.Empty;
            Mustbe(Sym.LPAREN);
            for (; ; )
            {
                cols = cols+(Value(), cols.Length??0);
                switch (lxr.tok)
                {
                    case Sym.RPAREN: Next(); return new SValues(cols);
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception(CheckEOF());
            }
        }
        string CheckEOF()
        {
            switch (lxr.tok)
            {
                case Sym.Null:
                    return "Premature eof at column " + lxr.pos;
                case Sym.ID:
                case Sym.LITERAL:
                    return "Syntax error at " + lxr.val;
                default:
                    return "Syntax error at " + lxr.tok;
            }
        }
        SList<ValueTuple<string,Serialisable>> Selects()
        {
            var r = SList<ValueTuple<string, Serialisable>>.Empty;
            var k = 0;
            for (; ;Next(),k++)
            {
                var c = Value();
                var n = c.Alias(k+1);
                if (c is SExpression se && se.op == SExpression.Op.Dot)
                    n = ((SString)se.left).str + "." + ((SString)se.right).str;
                if (lxr.tok == Sym.AS)
                {
                    Next();
                    n = MustBeID().str;
                }
                r += ((n, c??Serialisable.Null), k);
                if (lxr.tok != Sym.COMMA)
                    return r;
            }
        }
        Serialisable CreateIndex(bool primary=false)
        {
            var id = MustBeID();
            Mustbe(Sym.FOR);
            var tb = MustBeID();
            var cols = Cols();
            Serialisable rt = Serialisable.Null;
            if (lxr.tok==Sym.REFERENCES)
            {
                Next();
                rt = MustBeID();
            }
            return new SCreateIndex(id, tb, SBoolean.For(primary), rt, cols); // ok
        }
        Serialisable Drop() // also see Drop column in Alter
        {
            Next();
            var id = MustBeID();
            return new SDropStatement(id.str, "");
        }
        Serialisable Insert()
        {
            Next();
            var id = MustBeID();
            var cols = SList<SSelector>.Empty;
            if (lxr.tok == Sym.LPAREN)
                cols = Cols();
            Serialisable vals;
            if (lxr.tok == Sym.VALUES)
                vals = Vals();
            else
            {
                Mustbe(Sym.SELECT);
                vals = Select();
            }
            return new SInsertStatement(id.str, cols, vals);
        }
        SQuery Query(SDict<int,string>als,SDict<int,Serialisable>cp)
        {
            var tb = TableExp(als,cp);
            var wh = SList<Serialisable>.Empty;
            var tt = Sym.WHERE;
            var n = 0;
            for (; lxr.tok==tt;n++)
            {
                Next(); tt = Sym.AND;
                wh += (Conjunct(),n);
            }
            SQuery sqry = (wh.Length==0)?tb:new SSearch(tb, wh);
            if (lxr.tok!=Sym.GROUPBY)
                return sqry;
            Next();
            var gp = SDict<int, string>.Empty;
            for (n = 0; lxr.tok==Sym.ID; n++)
            {
                gp += (n, ((SString)lxr.val).str);
                Next();
                if (lxr.tok != Sym.COMMA)
                    break;
                Next();
            }
            var h = SList<Serialisable>.Empty;
            if (lxr.tok == Sym.HAVING)
                for (n =0; ; n++)
                {
                    Next();
                    h += (Conjunct(), n);
                    if (lxr.tok != Sym.AND)
                        break;
                }
            return new SGroupQuery(sqry, sqry.display, sqry.cpos, 
                new Context(sqry.names,null),gp, h);
        }
        SQuery TableExp(SDict<int, string> als, SDict<int, Serialisable> cp)
        {
            if (lxr.tok==Sym.LPAREN)
            {
                SQuery r;
                Next();
                if (lxr.tok == Sym.SELECT)
                    r= (SQuery)Select();
                else
                    r =TableExp(SDict<int,string>.Empty,SDict<int,Serialisable>.Empty);
                Mustbe(Sym.RPAREN);
                return r;
            }
            var id = MustBeID();
            var tb = new STable(id.str);
            if (lxr.tok == Sym.ID && lxr.val != null)
            {
                var alias = ((SString)lxr.val).str;
                Next();
                tb = new SAliasedTable(tb,alias);
            }
            if (lxr.tok==Sym.COMMA)
            {
                Next();
                var ra = TableExp(SDict<int, string>.Empty, SDict<int, Serialisable>.Empty);
                var da = SDict<int, string>.Empty;
                var ca = SDict<int, Serialisable>.Empty;
                var na = SDict<string, Serialisable>.Empty;
                return new SJoin(tb, false, SJoin.JoinType.Cross, ra, SList<SExpression>.Empty,
                    da, ca, new Context(na, null)); 
            }
            return tb;
        }
        Serialisable Value()
        {
            var a = Conjunct();
            while (lxr.tok==Sym.AND)
            {
                Next();
                a = new SExpression(a, SExpression.Op.And, Conjunct());
            }
            return a;
        }
        Serialisable Conjunct()
        {
            var a = Item();
            while (lxr.tok==Sym.OR)
            {
                Next();
                a = new SExpression(a, SExpression.Op.Or, Item());
            }
            return a;
        }
        Serialisable Item()
        {
            var a = OneVal();
            if (lxr.tok >= Sym.EQUAL && lxr.tok <= Sym.GTR)
            {
                var op = SExpression.Op.Eql;
                switch(lxr.tok)
                {
                    case Sym.NEQ: op = SExpression.Op.NotEql; break;
                    case Sym.LEQ: op = SExpression.Op.Leq; break;
                    case Sym.LSS: op = SExpression.Op.Lss; break;
                    case Sym.GEQ: op = SExpression.Op.Geq; break;
                    case Sym.GTR: op = SExpression.Op.Gtr; break;
                    case Sym.AND: op = SExpression.Op.And; break;
                    case Sym.OR: op = SExpression.Op.Or; break;
                }
                Next();
                a = new SExpression(a, op, OneVal());
            }
            return a;
        }
        Serialisable OneVal()
        {
            Serialisable a = Term();
            while (lxr.tok==Sym.PLUS || lxr.tok==Sym.MINUS)
            {
                SExpression.Op op = SExpression.Op.Or;
                switch (lxr.tok)
                {
                    case Sym.PLUS: op = SExpression.Op.Plus; break;
                    case Sym.MINUS: op = SExpression.Op.Minus; break;
                }
                Next();
                a = new SExpression(a, op, Term());
            }
            return a;
        }
        Serialisable Term()
        {
            if (lxr.tok == Sym.MINUS || lxr.tok == Sym.PLUS || lxr.tok == Sym.NOT)
            {
                var op = SExpression.Op.Plus;
                switch (lxr.tok)
                {
                    case Sym.MINUS:
                        op = SExpression.Op.UMinus; break;
                    case Sym.NOT:
                        op = SExpression.Op.Not; break;
                    case Sym.PLUS:
                        Next();
                        return Term();
                }
                Next();
                return new SExpression(Term(), op, Serialisable.Null);
            }
            var a = Factor();
            while (lxr.tok == Sym.TIMES || lxr.tok == Sym.DIVIDE)
            {
                SExpression.Op op = SExpression.Op.And;
                switch (lxr.tok)
                {
                    case Sym.TIMES: op = SExpression.Op.Times; break;
                    case Sym.DIVIDE: op = SExpression.Op.Divide; break;
                }
                Next();
                a = new SExpression(a, op, Factor());
            }
            if (lxr.tok==Sym.IS)
            {
                Next();
                Mustbe(Sym.NULL);
                return new SExpression(a, SExpression.Op.Eql, Serialisable.Null);
            }
            if (lxr.tok==Sym.IN)
            {
                Next();
                return new SInPredicate(a, Value());
            }
            return a;
        }
        Serialisable Factor()
        {
            var v = lxr.val;
            switch (lxr.tok)
            {
                case Sym.LITERAL:
                    Next();
                    return v ?? throw new Exception("??");
                case Sym.ID:
                    {
                        if (v == null)
                            throw new Exception("??");
                        var s = ((SString)v).str;
                        Next();
                        if (lxr.tok == Sym.DOT)
                        {
                            Next();
                            var nv = MustBeID();
                            return new SExpression(v, SExpression.Op.Dot, nv);
                        }
                        return new SColumn(((SString)v).str);
                    }
                case Sym.SUM:
                case Sym.COUNT:
                case Sym.MAX:
                case Sym.MIN:
                    {
                        var t = lxr.tok;
                        Next(); Mustbe(Sym.LPAREN);
                        var a = Value();
                        Mustbe(Sym.RPAREN);
                        return Call(t, a);
                    }
                case Sym.LPAREN:
                    {
                        Next();
                        var a = SList<string>.Empty;
                        var c = SList<Serialisable>.Empty;
                        int n = 0;
                        bool asseen = false;
                        for (; ; n++)
                        {
                            var cv = Value();
                            var als = cv.Alias(n+1);
                            if (lxr.tok == Sym.AS)
                            {
                                Next();
                                als = MustBeID().str;
                                asseen = true;
                            }
                            c += (cv, n);
                            a += (als, n++);
                            if (lxr.tok != Sym.COMMA)
                                break;
                            Next();
                        }
                        Mustbe(Sym.RPAREN);
                        if (n == 1 && !asseen)
                            return c.element;
                        return new SRow(a, c);
                    }
                case Sym.SELECT:
                    return Select();
            }
            throw new Exception("Bad syntax");
        }
        Serialisable Call(Sym s,Serialisable a)
        {
            var f = SFunction.Func.Sum;
            switch (s)
            {
                case Sym.COUNT: f = SFunction.Func.Count; break;
                case Sym.MAX: f = SFunction.Func.Max; break;
                case Sym.MIN: f = SFunction.Func.Min; break;
            }
            return new SFunction(f, a);
        }
        Serialisable Select()
        {
            Next();
            var dct = false;
            if (lxr.tok == Sym.DISTINCT)
            {
                dct = true;
                Next();
            }
            var sels = SList<ValueTuple<string, Serialisable>>.Empty;
            if (lxr.tok!=Sym.FROM)
                sels = Selects();
            var als = SDict<int, string>.Empty;
            var cp = SDict<int, Serialisable>.Empty;
            var k = 0;
            for (var b = sels.First();b!=null;b=b.Next())
            {
                als += (k, b.Value.Item1);
                cp += (k++, b.Value.Item2);
            }
            Mustbe(Sym.FROM);
            var q = Query(als,cp);
            var or = SList<SOrder>.Empty;
            var i = 0;
            if (lxr.tok == Sym.ORDERBY)
            {
                Next();
     //           Mustbe(Sym.BY);
                for (; ; )
                {
                    var c = Value();
                    var d = false;
                    if (lxr.tok == Sym.DESC)
                    {
                        Next();
                        d = true;
                    }
                    or = or+(new SOrder(c, d), i++);
                    if (lxr.tok == Sym.COMMA)
                        Next();
                    else
                        break;
                }
            }
            return new SSelectStatement(dct, als, cp, q, or,Context.Empty);
        }
        Serialisable Delete()
        {
            Next();
            return new SDeleteSearch(Query(SDict<int,string>.Empty,SDict<int,Serialisable>.Empty));
        }
        Serialisable Update()
        {
            Next();
            var q = Query(SDict<int, string>.Empty, SDict<int, Serialisable>.Empty);
            var sa = SDict<string, Serialisable>.Empty;
            if (lxr.tok != Sym.SET)
                throw new Exception("Expected SET");
            var tt = lxr.tok;
            for (; lxr.tok == tt;)
            {
                Next(); tt = Sym.COMMA;
                var c = MustBeID();
                Mustbe(Sym.EQUAL);
                sa += (c.str, Value());
            }
            return new SUpdateSearch(q, sa);
        }
    }
}
