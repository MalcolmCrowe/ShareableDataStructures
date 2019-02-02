/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import org.shareabledata.Parser.Sym;

/**
 *
 * @author Malcolm
 */
public class Parser {
    static class Sym
    {
        static final int Null = 0,
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
        WHERE = 71;
    static String[] syms= new String[]{ 
        "Null","ID","LITERAL","LPAREN","COMMA","RPAREN", //0-5
        "EQUAL","NEQ","LEQ","LSS","GEQ","GTR","DOT", // 6-12
        "PLUS","MINUS","TIMES","DIVIDE","ADD","ALTER", //13-18
        "AND","AS","BEGIN","BOOLEAN","COLUMN","COMMIT",//19-24
        "COUNT","CREATE","CROSS","DATE","DELETE","DESC", //25-30
        "DISTINCT","DROP","FALSE","FOR","FROM","FULL", //31-36
        "GROUPBY","HAVING","INDEX","INSERT","INTEGER","IN", //37-42
        "INNER","IS","JOIN","LEFT","MAX","MIN", // 43-48
        "NATURAL","NOT","NULL","NUMERIC","ON","OR", // 49-54
        "ORDERBY","OUTER","PRIMARY","REFERENCES","RIGHT","ROLLBACK",//55-60
        "SELECT","SET","STRING","SUM","TABLE","TIMESPAN",//61-66
        "TO","TRUE","UPDATE","VALUES","WHERE"}; // 67-71
    }
    class Lexer
    {
        public final char[] input;
            int pos = -1;
            Integer pushPos = null;
            int tok;
            int pushBack = Sym.Null;
            Serialisable val = Serialisable.Null;
            char ch = '\0';
            Character pushCh = null;
            public Lexer(String inp) throws Exception
            {
                input = inp.toCharArray();
                Advance();
                tok = Next();
            }
            private char Advance() throws Exception
            {
                if (pos >= input.length)
                    throw new Exception("Non-terminated string");
                ch = (++pos >= input.length) ? '\0' : input[pos];
                return ch;
            }
            char Peek()
            {
                return (pos+1 >= input.length)?'\0':input[pos + 1];
            }
            int PushBack(int old)
            {
                pushBack = old;
                pushCh = ch;
                pushPos = pos;
                tok = old;
                return tok;
            }
            Bigint Unsigned() throws Exception
            {
                var v = new Bigint(ch - '0');
                for (Advance(); ch != '\0' && Character.isDigit(ch); 
                        Advance())
                    v = v.Times10().Plus(new Bigint(ch - '0'));
                return v;
            }
            Bigint Unsigned(Bigint v) throws Exception
            {
                for (Advance(); ch != '\0' && Character.isDigit(ch); 
                        Advance())
                    v = v.Times10().Plus(new Bigint(ch - '0'));
                return v;
            }
            int Unsigned(int n) throws Exception
            {
                var st = pos;
                var r = Unsigned();
                if (pos != st + n)
                    throw new Exception("Expected " + n + " digits");
                return r.toInt();
            }
            int Unsigned(int n, int low, int high) throws Exception
            {
                var r = Unsigned(n);
                if (r < low || r > high)
                    throw new Exception("Expected " + low + "<=" + r + "<=" + high);
                return r;
            }
            void Mustbe(char c) throws Exception
            {
                if (c != ch)
                    throw new Exception("Expected " + c + " got " + ch);
                Advance();
            }
            Serialisable DateTimeLiteral() throws Exception
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
                    return new SDate(y, mo, 
                            new Bigint(d-1).Times(new Bigint(24*60*60))
                                    .Times(new Bigint(10000000)));
                }
                Mustbe('T');
                var r = Unsigned(2, 0, 23)+(d-1)*24;
                Mustbe(':');
                r = Unsigned(2, 0, 59)+r*60;
                Mustbe(':');
                r = Unsigned(2, 0, 59)+r*60;
                var re = new Bigint(r).Times(new Bigint(10000000));
                if (ch=='.')
                {
                    Advance();
                    var n = 0;
                    var v = 0;
                    for (;Character.isDigit(ch);n++)
                    {
                        v = v*10+(ch-'0');
                        Advance();
                    }
                    for (;n<7;n++)
                        v = v*10;
                    re = re.Plus(new Bigint(v));
                }
                Mustbe('\'');
                return new SDate(y, mo,re);
            }
            Serialisable TimeSpanLiteral() throws Exception
            {
                boolean sg = false;
                if (ch=='-')
                {
                    sg = true;
                    Advance();
                }
                var d = Unsigned().toInt();
                int h = 0, m = 0, s = 0, f = 0; 
                if (ch=='\'')
                    return new STimeSpan(sg,d,0,0,0,0);
                if (ch=='.')
                {
                    Advance();
                    h = Unsigned(2);
                }
                else {
                    h = d;  
                    d = 0;
                }
                Mustbe(':');
                m = Unsigned(2);
                if (ch==':')
                {
                    Advance();
                    s = Unsigned(2);
                    if(ch=='.')
                    {
                        Advance();
                        var n = 0;
                        for (;Character.isDigit(ch);n++)
                        {
                            f = (f*10)+(ch-'0');
                            Advance();
                        }
                        for (;n<7;n++)
                            f = f*10;
                    }
                }
                return new STimeSpan(sg,d,h,m,s,f);
            }
            final int Next() throws Exception
            {
                if (pushBack!=Sym.Null)
                {
                    tok = pushBack;
                    pos = pushPos;
                    ch = pushCh;
                    pushBack = Sym.Null;
                    return tok;
                }
                while (Character.isWhitespace(ch))
                    Advance();
                var st = pos;
                if (ch == '\0')
                    return tok=Sym.Null;
                if (Character.isDigit(ch))
                {
                    var n = Unsigned();
                    if (ch == '.')
                    {
                        var p = pos;
                        var m = Unsigned(n);
                        val = new SNumeric(new Numeric(m, pos - p, pos - p));
                        return tok = Sym.LITERAL;
                    }
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
                    val = new SString(new String(input, st, pos - st - 1).replace("''","'"));
                    return tok = Sym.LITERAL;
                }
                else if (Character.isLetter(ch) || ch=='_')
                {
                    for (Advance(); Character.isLetterOrDigit(ch) || ch == '_'; Advance())
                        ;
                    var s = new String(input, st, pos - st);
                    var su = s.toUpperCase();
                    for (var t = Sym.ADD; t <= Sym.WHERE; t++)
                        if (su.compareTo(Sym.syms[t]) == 0)
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
                                        val = TimeSpanLiteral();
                                        if (ch != '\'')
                                            throw new Exception("non-terminated string literal");
                                        Advance();
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
        Parser(String inp) throws Exception
        {
            lxr = new Lexer(inp);
        }
        public static Serialisable Parse(String sql) throws Exception
        {
            return new Parser(sql).Statement();
        }
        int Next() throws Exception
        {
            return lxr.Next();
        }
        void Mustbe(int t) throws Exception
        {
            if (lxr.tok != t)
                throw new Exception("Syntax error: " + Sym.syms[lxr.tok]);
            Next();
        }
        SString MustBeID() throws Exception
        {
            var s = lxr.val;
            if (lxr.tok != Sym.ID || s==null)
                throw new Exception("Syntax error: " + Sym.syms[lxr.tok]);
            Next();
            return (SString)s;
        }
        int For(int t) throws Exception
        {
            switch (t)
            {
                case Sym.INTEGER: return Types.SInteger;
                case Sym.NUMERIC: return Types.SNumeric;
                case Sym.STRING: return Types.SString;
                case Sym.DATE: return Types.SDate;
                case Sym.TIMESPAN: return Types.STimeSpan;
                case Sym.BOOLEAN: return Types.SBoolean;
            }
            throw new Exception("Syntax error: " + t);
        }
        public Serialisable Statement() throws Exception
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
                                return CreateIndex(false);
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
            throw new Exception("Syntax Error: " + lxr.tok);
        }
        /// <summary>
        /// Alter: ALTER table_id ADD id Type
	    /// | ALTER table_id DROP col_id 
        /// | ALTER table_id[COLUMN col_id] TO id[Type] .
        /// </summary>
        /// <returns></returns>
        Serialisable Alter() throws Exception
        {
            Next();
            var tb = MustBeID();
            SString col = null;
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
            int dt = Types.Serialisable;
            switch (lxr.tok)
            {
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
                throw new Exception("??");
            return new SAlterStatement(tb.str, col.str, nm.str, dt); // ok
        }
        Serialisable CreateTable() throws Exception
        {
            Next();
            var id = MustBeID();
            var tb = id.str; // ok
            Mustbe(Sym.LPAREN);
            SList<SColumn> cols = null;
            for (; ; )
            {
                var c = MustBeID();
                var t = For(lxr.tok);
                var col =new SColumn(c.str, t);
                cols = (cols==null)?new SList(col):cols.InsertAt(col,cols.Length); // ok
                Next();
                switch(lxr.tok)
                {
                    case Sym.RPAREN: Next(); return new SCreateTable(tb, cols);
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception("Syntax error: " + lxr.tok);
            }
        }
        SList<SSelector> Cols() throws Exception
        {
            SList<SSelector> cols = null;
            Mustbe(Sym.LPAREN);
            for (; ;)
            {
                var c = MustBeID();
                var col = new SColumn(c.str,Types.Serialisable,-1);
                cols = (cols==null)?new SList(col):cols.InsertAt(col,cols.Length); // ok
                switch (lxr.tok)
                {
                    case Sym.RPAREN: Next(); return cols;
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception("Syntax error: " + lxr.tok);
            }
        }
        SValues Vals() throws Exception
        {
            Next();
            SList<Serialisable> cols = null;
            Mustbe(Sym.LPAREN);
            for (; ; )
            {
                cols = (cols==null)?new SList(Value()):cols.InsertAt(Value(), cols.Length);
                switch (lxr.tok)
                {
                    case Sym.RPAREN: Next(); return new SValues(cols);
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception("Syntax error: " + lxr.tok);
            }
        }
        SList<SSlot<String,Serialisable>> Selects() throws Exception
        {
            SList<SSlot<String, Serialisable>> r = null;
            var k = 0;
            for (; ;Next(),k++)
            {
                var c = Value();
                var n = c.Alias(k+1);
                if (c instanceof SExpression)
                {
                    var se =(SExpression)c;
                    if (se.op == SExpression.Op.Dot)
                    n = ((SString)se.left).str + "." + ((SString)se.right).str;
                }
                if (lxr.tok == Sym.AS)
                {
                    Next();
                    n = MustBeID().str;
                }
                if (c==null)
                    c = Serialisable.Null;
                var ss = new SSlot<String, Serialisable>(n, c);
                r=(r==null)?new SList(ss):r.InsertAt(ss, k);
                if (lxr.tok != Sym.COMMA)
                    return r;
            }
        }
        Serialisable CreateIndex(boolean primary) throws Exception
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
        Serialisable Drop() throws Exception // also see Drop column in Alter
        {
            Next();
            var id = MustBeID();
            return new SDropStatement(id.str, "");
        }
        Serialisable Insert() throws Exception
        {
            Next();
            var id = MustBeID();
            SList<SSelector> cols = null;
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
        SQuery Query(SDict<Integer,String>als,SDict<Integer,Serialisable>cp)
                throws Exception
        {
            var tb = TableExp(als,cp);
            SList<Serialisable> wh = null;
            var tt = Sym.WHERE;
            for (; lxr.tok==tt;)
            {
                Next(); tt = Sym.AND;
                wh=(wh==null)?new SList(Conjunct()):wh.InsertAt(Conjunct(),wh.Length);
            }
            if (wh==null) return tb;
            var sqry = new SSearch(tb, wh);
            if (lxr.tok!=Sym.GROUPBY)
                return sqry;
            Next();
            SDict<Integer, String> gp = null;
            while (lxr.tok==Sym.ID)
            {
                var g = ((SString)lxr.val).str;
                gp=(gp==null)?new SDict(0,g):gp.Add(gp.Length, g);
                Next();
                if (lxr.tok == Sym.COMMA)
                    Next();
                else
                    break;
            }
            SList<Serialisable> h = null;
            if (lxr.tok == Sym.HAVING)
                for (; ; )
                {
                    Next();
                    h=(h==null)?new SList(Conjunct()):h.InsertAt(Conjunct(), h.Length);
                    if (lxr.tok != Sym.AND)
                        break;
                }
            return new SGroupQuery(sqry, sqry.display, sqry.cpos, 
                    new Context(sqry.names,null), gp, h);
        }
        SQuery TableExp(SDict<Integer, String> als, SDict<Integer, Serialisable> cp)
                throws Exception
        {
            if (lxr.tok==Sym.LPAREN)
            {
                SQuery r;
                Next();
                if (lxr.tok == Sym.SELECT)
                    r= (SQuery)Select();
                else
                    r =TableExp(null,null);
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
                var ra = TableExp(null, null);
                SDict<Integer, String> da = null;
                SDict<Integer, Serialisable> ca = null;
                SDict<String, Serialisable> na = null;
                return new SJoin(tb, false, SJoin.JoinType.Cross, ra, null, da, 
                        ca, new Context(na,null)); 
            }
            return tb;
        }
        Serialisable Value() throws Exception
        {
            var a = Conjunct();
            while (lxr.tok==Sym.AND)
            {
                Next();
                a = new SExpression(a, SExpression.Op.And, Conjunct());
            }
            return a;
        }
        Serialisable Conjunct() throws Exception
        {
            var a = Item();
            while (lxr.tok==Sym.OR)
            {
                Next();
                a = new SExpression(a, SExpression.Op.Or, Item());
            }
            return a;
        }
        Serialisable Item() throws Exception
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
        Serialisable OneVal() throws Exception
        {
            Serialisable a = Term();
            while (lxr.tok==Sym.PLUS || lxr.tok==Sym.MINUS)
            {
                int op = SExpression.Op.Or;
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
        Serialisable Term() throws Exception
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
                int op = SExpression.Op.And;
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
        Serialisable Factor() throws Exception
        {
            var v = lxr.val;
            switch (lxr.tok)
            {
                case Sym.LITERAL:
                    Next();
                    if (v==null)
                        throw new Exception("??");
                    return v;
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
                        return new SColumn(((SString)v).str,Types.Serialisable,-1);
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
                        SList<String> a = null;
                        SList<Serialisable> c = null;
                        int n = 0;
                        boolean asseen = false;
                        for (; ; n++)
                        {
                            var cv = Value();
                            var als = cv.Alias(n);
                            if (lxr.tok == Sym.AS)
                            {
                                Next();
                                als = MustBeID().str;
                                asseen = true;
                            }
                            c=(c==null)?new SList(cv):c.InsertAt(cv, n);
                            a=(a==null)?new SList(als):a.InsertAt(als, n);
                            if (lxr.tok != Sym.COMMA)
                                break;
                            Next();
                        }
                        Mustbe(Sym.RPAREN);
                        if (n == 0 && !asseen)
                            return c.element;
                        return new SRow(a, c);
                    }
                case Sym.SELECT:
                    return Select();
            }
            throw new Exception("Bad syntax");
        }
        Serialisable Call(int s,Serialisable a)
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
        Serialisable Select() throws Exception
        {
            Next();
            var dct = false;
            if (lxr.tok == Sym.DISTINCT)
            {
                dct = true;
                Next();
            }
            SList<SSlot<String, Serialisable>> sels = null;
            if (lxr.tok!=Sym.FROM)
                sels = Selects();
            SDict<Integer, String> als = null;
            SDict<Integer, Serialisable> cp = null;
            var k = 0;
            if (sels!=null)
            for (var b = sels.First();b!=null;b=b.Next(),k++)
            {
                var ke= b.getValue().key;
                var ve = b.getValue().val;
                als=(als==null)?new SDict(k,ke):als.Add(k,ke);
                cp=(cp==null)?new SDict(k,ve):cp.Add(k,ve);
            }
            Mustbe(Sym.FROM);
            var q = Query(als,cp);
            SList<SOrder> or = null;
            var i = 0;
            if (lxr.tok == Sym.ORDERBY)
            {
                Next();
     //           Mustbe(Sym.BY);
                for (; ;i++ )
                {
                    var c = Value();
                    var d = false;
                    if (lxr.tok == Sym.DESC)
                    {
                        Next();
                        d = true;
                    }
                    var o =new SOrder(c, d);
                    or = (or==null)?new SList(o):or.InsertAt(o, i);
                    if (lxr.tok == Sym.COMMA)
                        Next();
                    else
                        break;
                }
            }
            return new SSelectStatement(dct, als, cp, q, or,Context.Empty);
        }
        Serialisable Delete() throws Exception
        {
            Next();
            return new SDeleteSearch(Query(null,null));
        }
        Serialisable Update() throws Exception
        {
            Next();
            var q = Query(null, null);
            SDict<String, Serialisable> sa = null;
            if (lxr.tok != Sym.SET)
                throw new Exception("Expected SET");
            var tt = lxr.tok;
            for (; lxr.tok == tt;)
            {
                Next(); tt = Sym.COMMA;
                var c = MustBeID();
                Mustbe(Sym.EQUAL);
                sa=(sa==null)?new SDict(c.str,Value()):sa.Add(c.str, Value());
            }
            return new SUpdateSearch(q, sa);
        }
    }

