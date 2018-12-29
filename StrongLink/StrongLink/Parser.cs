using System;
using Shareable;

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
            BY = 23,
            COLUMN = 24,
            COMMIT = 25,
            COUNT = 26,
            CREATE = 27,
            DATE = 28,
            DELETE = 29,
            DESC = 30,
            DISTINCT = 31,
            DROP = 32,
            FOR = 33,
            FROM = 34,
            INDEX = 35,
            INSERT = 36,
            INTEGER = 37,
            IN = 38,
            IS = 39,
            MAX = 40,
            MIN = 41,
            NOT = 42,
            NULL = 43,
            NUMERIC = 44,
            OR = 45,
            ORDER = 46,
            PRIMARY = 47,
            REFERENCES = 48,
            ROLLBACK = 49,
            SELECT = 50,
            SET = 51,
            STRING = 52,
            SUM = 53,
            TABLE = 54,
            TIMESPAN = 55,
            TIMESTAMP = 56,
            TO = 57,
            UPDATE = 58,
            VALUES = 59,
            WHERE = 60
        }
        internal class Token
        {
            public Sym type;
            public int pos, len;
            public Serialisable val = null;
            public Sym valType = Sym.Null;
            internal Token(Sym t,int p,int n) { type = t; pos = p; ;len = n; }
            internal static Token EOF = new Token(Sym.Null,-1,0);
            public override string ToString()
            {
                return type.ToString() + " at "+pos;
            }
        }
        internal class Lexer
        {
            public readonly char[] input;
            int pos = 0;
            char ch = '\0';
            public Lexer(string inp)
            {
                input = inp.ToCharArray();
            }
            internal char NextChar()
            {
                ch = (pos >= input.Length) ? '\0' : input[pos++];
                return ch;
            }
            long Unsigned()
            {
                var v = ch - '0';
                for (NextChar(); ch != '\0' && char.IsDigit(ch); NextChar())
                    v = v * 10 + (ch - '0');
                --pos;
                return v;
            }
            long Unsigned(long v)
            {
                for (NextChar(); ch != '\0' && char.IsDigit(ch); NextChar())
                    v = v * 10 + (ch - '0');
                --pos;
                return v;
            }
            int Unsigned(int n)
            {
                var st = pos;
                var r = (int)Unsigned();
                if (pos != st + n)
                    throw new Exception("Expected " + n + " digits");
                return r;
            }
            int Unsigned(int n,int low,int high)
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
                NextChar();
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
                    NextChar();
                    return new SDate(new DateTime(y, mo, d));
                }
                Mustbe(' ');
                var h = Unsigned(2, 0, 23);
                Mustbe(':');
                var mi = Unsigned(2, 0, 59);
                Mustbe(':');
                var s = Unsigned(2, 0, 59);
                Mustbe('\'');
                var dt = new DateTime(y, mo, d, h, mi, s);
                return new STimestamp(dt);
            }
            Sym For(Types t)
            {
                switch (t)
                {
                    case Types.SDate: return Sym.DATE;
                    case Types.STimestamp: return Sym.TIMESTAMP;
                }
                throw new Exception("Unexpected type " + t);
            }
            internal Token Next()
            {
                for (NextChar(); ch != '\0' && char.IsWhiteSpace(ch); NextChar())
                    ;
                var st = pos-1;
                if (ch == '\0')
                    return Token.EOF;
                if (char.IsDigit(ch))
                {
                    var n = Unsigned();
                    if (ch == '.')
                    {
                        var p = pos;
                        var m = Unsigned(n);
                        return new Token(Sym.LITERAL, st, pos - st)
                        { val = new SNumeric(m, pos - p, pos - p), valType = Sym.NUMERIC };
                    }
                    if (n > int.MaxValue)
                        throw new Exception("Inbteger overflow");
                    return new Token(Sym.LITERAL, st, pos - st)
                    { val = new SInteger((int)n), valType = Sym.INTEGER };
                }
                else if (ch == '-')
                {
                    NextChar();
                    var tk = Next();
                    switch (tk.type)
                    {
                        case Sym.INTEGER:
                            {
                                tk.val = new SInteger(-((SInteger)tk.val).value);
                                return tk;
                            }
                        case Sym.NUMERIC:
                            {
                                var sn = (SNumeric)tk.val;
                                tk.val = new SNumeric(-(int)sn.num.mantissa, sn.num.precision, sn.num.scale);
                                return tk;
                            }
                        case Sym.TIMESPAN:
                            {
                                var ts = (STimeSpan)tk.val;
                                tk.val = new STimeSpan(new TimeSpan(-ts.ticks));
                                return tk;
                            }
                    }
                }
                else if (ch == '\'')
                {
                    st++;
                    for (NextChar(); ch != '\0' && ch != '\''; NextChar())
                        ;
                    if (ch == '\0')
                        throw new Exception("non-terminated string literal");
                    return new Token(Sym.LITERAL, st, pos - st - 1)
                    { val = new SString(new string(input, st, pos - st - 1)), valType = Sym.STRING };
                }
                else if (char.IsLetter(ch))
                {
                    for (NextChar(); char.IsLetter(ch) || ch=='_'; NextChar())
                        ;
                    if (ch != '\0')
                        --pos;
                    var s = new string(input, st, pos - st);
                    var su = s.ToUpper();
                    for (var t = Sym.ADD; ; t++)
                    {
                        var r = t.ToString();
                        if (su.CompareTo(r) == 0)
                        {
                            if (t == Sym.DATE && ch == '\'')
                            {
                                var d = DateTimeLiteral();
                                return new Token(Sym.LITERAL, st, pos - st)
                                { val = d, valType = For(d.type) };
                            }
                            if (t==Sym.TIMESPAN && ch=='\'')
                            {
                                NextChar();
                                var ts = new STimeSpan(new TimeSpan(Unsigned()));
                                if (ch != '\'')
                                    throw new Exception("non-terminated string literal");
                                NextChar();
                                return new Token(Sym.LITERAL, st, pos - st)
                                { val = ts, valType = Sym.TIMESPAN };
                            }
                            return new Token(t, st, pos - st);
                        }
                        if (t == Sym.WHERE)
                            break;
                    }
                    return new Token(Sym.ID, st, pos - st)
                    { val = new SString(s), valType = Sym.ID };
                }
                else
                    switch (ch)
                    {
                        case '(': return new Token(Sym.LPAREN, st, 1);
                        case ',': return new Token(Sym.COMMA, st, 1);
                        case ')': return new Token(Sym.RPAREN, st, 1);
                        case '=': return new Token(Sym.EQUAL, st, 1);
                    }
                throw new Exception("Bad input " + ch + " at " + pos);
            }
        }
        Lexer lxr;
        Token tok;
        Parser(string inp)
        {
            lxr = new Lexer(inp);
            tok = lxr.Next();
        }
        public static Serialisable Parse(string sql)
        {
            try
            {
                return new Parser(sql).Statement();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }
        void Next()
        {
            tok = lxr.Next();
        }
        void Mustbe(Sym t)
        {
            if (tok.type != t)
                throw new Exception("Syntax error: " + tok.ToString());
            Next();
        }
        Types For(Sym t)
        {
            switch (t)
            {
                case Sym.TIMESTAMP: return Types.STimestamp;
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
            switch(tok.type)
            {
                case Sym.ALTER:
                    return Alter();
                case Sym.CREATE:
                    {
                        Next();
                        switch (tok.type)
                        {
                            case Sym.TABLE:
                                return CreateTable();
                            case Sym.INDEX:
                                return CreateIndex();
                            case Sym.PRIMARY:
                                Next(); Mustbe(Sym.INDEX);
                                return CreateIndex(true);
                        }
                        throw new Exception("Unknown Create " + tok);
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
            throw new Exception("Syntax Error: " + tok);
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
            var tb = tok.val as SString; Mustbe(Sym.ID);
            SString col = null;
            var add = false;
            switch (tok.type)
            {
                case Sym.COLUMN:
                    Next();
                    col = tok.val as SString; Mustbe(Sym.ID);
                    Mustbe(Sym.TO);
                    break;
                case Sym.DROP:
                    Next();
                    col = tok.val as SString; Mustbe(Sym.ID);
                    return new SDropStatement(col.str, tb.str);
                case Sym.TO:
                    Next(); break;
                case Sym.ADD:
                    Next(); add = true;
                    break;
            }
            var nm = tok.val as SString; Mustbe(Sym.ID);
            Types dt = Types.Serialisable;
            switch (tok.type)
            {
                case Sym.TIMESTAMP: Next(); dt = Types.STimestamp; break;
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
            return new SAlterStatement(tb.str, col.str, nm.str, dt);
        }
        Serialisable CreateTable()
        {
            Next();
            var id = tok.val as SString;  Mustbe(Sym.ID);
            var tb = id.str;
            Mustbe(Sym.LPAREN);
            var cols = SList<SColumn>.Empty;
            for (; ; )
            {
                var c = tok.val as SString; Mustbe(Sym.ID);
                var t = For(tok.type);
                cols = cols.InsertAt(new SColumn(c.str, t), cols.Length.Value);
                Next();
                switch(tok.type)
                {
                    case Sym.RPAREN: Next(); return new SCreateTable(tb, cols);
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception("Syntax error: " + tok);
            }
        }
        SList<SSelector> Cols()
        {
            var cols = SList<SSelector>.Empty;
            Mustbe(Sym.LPAREN);
            for (; ;)
            {
                var c = tok.val as SString; Mustbe(Sym.ID);
                cols = cols.InsertAt(new SColumn(c.str), cols.Length.Value);
                switch (tok.type)
                {
                    case Sym.RPAREN: Next(); return cols;
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception("Syntax error: " + tok);
            }
        }
        SValues Vals()
        {
            Next();
            var cols = SList<Serialisable>.Empty;
            Mustbe(Sym.LPAREN);
            for (; ; )
            {
                var c = tok.val as Serialisable; 
                switch(tok.type)
                {
                    case Sym.LITERAL: Next(); break;
                    case Sym.ID: Next();
                        c = new SColumn(((SString)c).str);
                        if (tok.type==Sym.DOT)
                        {
                            Next();
                            var cc = tok.val as Serialisable;
                            Mustbe(Sym.ID);
                            c = new SExpression(c, SExpression.Op.Dot, new SColumn(((SString)cc).str));
                        }
                        break;
                }
                cols = cols.InsertAt(c, cols.Length.Value);
                switch (tok.type)
                {
                    case Sym.RPAREN: Next(); return new SValues(cols);
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception("Syntax error: " + tok);
            }
        }
        SQuery Selects(SQuery q)
        {
            for (; ;Next())
            {
                var c = Value();
                var nms = q.names;
                if (tok.type == Sym.AS)
                {
                    Next();
                    var n = tok.val;
                    Mustbe(Sym.ID);
                    nms = nms.Add(((SString)n).str, c);
                }
                q = new SQuery(q,q.cols,q.cpos.InsertAt(c, q.cpos.Length.Value),nms);
                if (tok.type != Sym.COMMA)
                    break;
            }
            return q;
        }
        Serialisable CreateIndex(bool primary=false)
        {
            var id = tok.val as SString; Mustbe(Sym.ID);
            Mustbe(Sym.FOR);
            var tb = tok.val as SString; Mustbe(Sym.ID);
            var cols = Cols();
            SString rt = null;
            if (tok.type==Sym.REFERENCES)
            {
                Next();
                rt = tok.val as SString; Mustbe(Sym.ID);
            }
            return new SCreateIndex(id, tb, new SBoolean(primary), rt, cols);
        }
        Serialisable Drop() // also see Drop column in Alter
        {
            Next();
            var id = tok.val as SString; Mustbe(Sym.ID);
            return new SDropStatement(id.str, null);
        }
        Serialisable Insert()
        {
            Next();
            var id = tok.val as SString; Mustbe(Sym.ID);
            var cols = SList<SSelector>.Empty;
            if (tok.type == Sym.LPAREN)
                cols = Cols();
            var vals = (tok.type==Sym.VALUES)? Vals() :Select();
            return new SInsertStatement(id.str, cols, vals);
        }
        SQuery Query()
        {
            var id = tok.val as SString; Mustbe(Sym.ID);
            var tb = new STable(id.str);
            var wh = SList<Serialisable>.Empty;
            var tt = Sym.WHERE;
            for (; tok.type==tt;)
            {
                Next(); tt = Sym.AND;
                wh = wh.InsertAt(Value(),wh.Length.Value);
            }
            if (wh.Length == 0) return tb;
            return new SSearch(tb, Serialisable.Null, wh);
        }
        Serialisable Value()
        {
            var a = OneVal();
            while ((tok.type >= Sym.EQUAL && tok.type <= Sym.GTR)|| tok.type==Sym.AND || tok.type==Sym.ORDER)
            {
                var op = SExpression.Op.Eql;
                switch(tok.type)
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
            Serialisable a = null;
            if (tok.type==Sym.MINUS || tok.type==Sym.NOT)
            {
                var op = (tok.type == Sym.MINUS) ? SExpression.Op.UMinus : SExpression.Op.Not;
                Next();
                a = new SExpression(Term(), op, Serialisable.Null);
            }
            else
                a = Term();
            while (tok.type==Sym.PLUS || tok.type==Sym.MINUS)
            {
                var op = (tok.type == Sym.PLUS) ? SExpression.Op.Plus : SExpression.Op.Minus;
                Next();
                a = new SExpression(a, op, Term());
            }
            return a;
        }
        Serialisable Term()
        {
            var a = Factor();
            while (tok.type == Sym.TIMES || tok.type == Sym.DIVIDE)
            {
                var op = (tok.type == Sym.TIMES) ? SExpression.Op.Times : SExpression.Op.Divide;
                Next();
                a = new SExpression(a, op, Factor());
            }
            if (tok.type==Sym.IS)
            {
                Next();
                Mustbe(Sym.NULL);
                return new SExpression(a, SExpression.Op.Eql, Serialisable.Null);
            }
            if (tok.type==Sym.IN)
            {
                Next();
                return new SInPredicate(a, Value());
            }
            return a;
        }
        Serialisable Factor()
        {
            var v = tok.val;
            switch (tok.type)
            {
                case Sym.LITERAL:
                    Next();
                    return v;
                case Sym.ID:
                    {
                        var s = (tok.val as SString).str;
                        Next();
                        if (tok.type == Sym.DOT)
                        {
                            Next();
                            v = new SExpression(v, SExpression.Op.Dot, tok.val);
                            Mustbe(Sym.ID);
                            return v;
                        }
                        return new SColumn((v as SString).str);
                    }
                case Sym.SUM:
                case Sym.COUNT:
                case Sym.MAX:
                case Sym.MIN:
                    {
                        var t = tok.type;
                        Next(); Mustbe(Sym.LPAREN);
                        var a = Value();
                        Mustbe(Sym.RPAREN);
                        return Call(t, a);
                    }
                case Sym.LPAREN:
                    {
                        Next();
                        var a = SList<Serialisable>.Empty;
                        int n = 0;
                        if (tok.type!=Sym.RPAREN)
                        {
                            a = a.InsertAt(Value(), n++);
                            while (tok.type==Sym.COMMA)
                            {
                                Next();
                                a = a.InsertAt(Value(), n++);
                            }
                        }
                        Mustbe(Sym.RPAREN);
                        return new SRow(a);
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
            if (tok.type == Sym.DISTINCT)
            {
                dct = true;
                Next();
            }
            var q = new SQuery(Types.Serialisable, -1);
            if (tok.type!=Sym.FROM)
                q = Selects(q);
            Mustbe(Sym.FROM);
            var or = SList<SOrder>.Empty;
            var i = 0;
            if (tok.type == Sym.ORDER)
            {
                Next();
                Mustbe(Sym.BY);
                for (; ; )
                {
                    var cr = Serialisable.Null;
                    var c = tok.val;
                    Mustbe(Sym.ID);
                    if (tok.type == Sym.DOT)
                    {
                        Next();
                        cr = c;
                        c = tok.val;
                        Mustbe(Sym.ID);
                    }
                    var d = false;
                    if (tok.type == Sym.DESC)
                    {
                        Next();
                        d = true;
                    }
                    or = or.InsertAt(new SOrder(cr, new SColumn(c.ToString()), d), i++);
                    if (tok.type == Sym.COMMA)
                        Next();
                    else
                        break;
                }
            }
            return new SSelectStatement(dct, q, Query(), or);
        }
        Serialisable Delete()
        {
            Next();
            return new SDeleteSearch(Query());
        }
        Serialisable Update()
        {
            Next();
            var q = Query();
            var sa = SDict<SSelector, Serialisable>.Empty;
            Mustbe(Sym.SET);
            var tt = Sym.SET;
            for (; tok.type == tt;)
            {
                Next(); tt = Sym.COMMA;
                var c = tok.val as SString; Mustbe(Sym.ID);
                var cs = new SColumn(c.str);
                Mustbe(Sym.EQUAL);
                var v = tok.val as Serialisable; Mustbe(Sym.LITERAL); // for now
                sa = sa.Add(cs, v);
            }
            return new SUpdateSearch(q, sa);
        }
    }
}
