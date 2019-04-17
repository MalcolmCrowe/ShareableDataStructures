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
        COLON = 6,
        EQUAL = 7, // EQUAL to GTR must be adjacent
        NEQ = 8,
        LEQ = 9,
        LSS = 10,
        GEQ = 11,
        GTR = 12,
        DOT = 13,
        PLUS = 14,
        MINUS = 15,
        TIMES = 16,
        DIVIDE = 17,
        //=== RESERVED WORDS
        ADD = 18,
        ALTER = 19,
        AND = 20,
        AS = 21,
        BEGIN = 22,
        BOOLEAN = 23,
        CHECK = 24,
        COLUMN = 25,
        COMMIT = 26,
        COUNT = 27,
        CREATE = 28,
        CROSS = 29,
        DATE = 30,
        DEFAULT = 31,
        DELETE = 32,
        DESC = 33,
        DISTINCT = 34,
        DROP = 35,
        FALSE = 36,
        FOR = 37,
        FOREIGN = 38,
        FROM = 39,
        FULL = 40,
        GENERATED = 41,
        GROUPBY = 42,
        HAVING = 43,
        INDEX = 44,
        INSERT = 45,
        INTEGER = 46,
        IN = 47,
        INNER = 48,
        IS = 49,
        JOIN = 50,
        KEY = 51,
        LEFT = 52,
        MAX = 53,
        MIN = 54,
        NATURAL = 55,
        NOT = 56,
        NOTNULL = 57,
        NULL = 58,
        NUMERIC = 59,
        ON = 60,
        OR = 61,
        ORDERBY = 62,
        OUTER = 63,
        PRIMARY = 64,
        REFERENCES = 65,
        RIGHT = 66,
        ROLLBACK = 67,
        SELECT = 68,
        SET = 69,
        STRING = 70,
        SUM = 71,
        TABLE = 72,
        TIMESPAN = 73,
        TO = 74,
        TRUE = 75,
        UNIQUE = 76,
        UPDATE = 77,
        USING = 78,
        VALUES = 79,
        WHERE = 80;
    static String[] syms= new String[]{ 
        "Null","ID","LITERAL","LPAREN","COMMA","RPAREN", //0-5
        "COLON","EQUAL","NEQ","LEQ","LSS","GEQ","GTR","DOT", // 6-13
        "PLUS","MINUS","TIMES","DIVIDE","ADD","ALTER", //14-19
        "AND","AS","BEGIN","BOOLEAN","CHECK","COLUMN","COMMIT",//20-26
        "COUNT","CREATE","CROSS","DATE","DEFAULT","DELETE","DESC", //27-33
        "DISTINCT","DROP","FALSE","FOR","FOREIGN","FROM","FULL", //34-40
        "GENERATED","GROUPBY","HAVING","INDEX","INSERT","INTEGER","IN", //41-47
        "INNER","IS","JOIN","KEY","LEFT","MAX","MIN", // 48-54
        "NATURAL","NOT","NOTNULL","NULL","NUMERIC","ON","OR", // 55-61
        "ORDERBY","OUTER","PRIMARY","REFERENCES","RIGHT","ROLLBACK",//62-67
        "SELECT","SET","STRING","SUM","TABLE","TIMESPAN",//68-73
        "TO","TRUE","UNIQUE","UPDATE","USING","VALUES","WHERE"}; // 74-80
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
        Parser psr;
        public Lexer(Parser p,String inp) throws Exception
        {
            input = inp.toCharArray();
            Advance();
            tok = Next();
            psr = p;
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
                        e = Unsigned().toInt()*esg;
                    }
                    val = new SNumeric(new Numeric(m, q - p-1-e, q - p));
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
                val = psr.SName(s);
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
    class ColumnTriple
    {
        public final long uid;
        public final SColumn col;
        public final SList<SIndex> xs;
        ColumnTriple(long u,SColumn c,SList<SIndex>x) 
        {uid = u; col = c; xs = x; }
    }
    class ConstraintPair
    {
        public final SDict<String,SFunction> cs;
        public final SList<SIndex> xs;
        ConstraintPair(SDict<String,SFunction>c,SList<SIndex> x)
        { cs = c; xs = x;  }
    }
    long _uid = -1;
    SDict<String,Long> names = null;
    SDict<Long,String> uids = null;
    Lexer lxr;
    Parser(String inp) throws Exception
    {
        lxr = new Lexer(this,inp);
    }
    public static ParsePair Parse(String sql) throws Exception
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
    long MustBeID() throws Exception
    {
        var s = lxr.val;
        if (lxr.tok != Sym.ID || s==null)
            throw new Exception("Syntax error: " + Sym.syms[lxr.tok]);
        Next();
        return ((SDbObject)s).uid;
    }
    SDbObject SName(String s)
    {
        long uid;
        if (names!=null && names.Contains(s))
            uid = names.get(s);
        else
        {
            uid = --_uid;
            names = (names==null)?new SDict(s,uid):names.Add(s,uid);
            uids = (uids==null)?new SDict(uid,s):uids.Add(uid,s);
        }
        return new SDbObject(Types.SName,uid);
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
        public ParsePair Statement() throws Exception
        {
            switch(lxr.tok)
            {
                case Sym.ALTER:
                    return new ParsePair(Alter(),uids);
                case Sym.CREATE:
                    {
                        Next();
                        switch (lxr.tok)
                        {
                            case Sym.TABLE:
                                return new ParsePair(CreateTable(),uids);
                            case Sym.INDEX:
                                Next();
                                return new ParsePair(CreateIndex(false),uids);
                            case Sym.PRIMARY:
                                Next(); Mustbe(Sym.INDEX);
                                return new ParsePair(CreateIndex(true),uids);
                        }
                        throw new Exception("Unknown Create " + lxr.tok);
                    }
                case Sym.DROP:
                    return new ParsePair(Drop(),uids);
                case Sym.INSERT:
                    return new ParsePair(Insert(),uids);
                case Sym.DELETE:
                    return new ParsePair(Delete(),uids);
                case Sym.UPDATE:
                    return new ParsePair(Update(),uids);
                case Sym.SELECT:
                    return new ParsePair(Select().UpdateAliases(uids),uids);
                case Sym.BEGIN:
                    return new ParsePair(new Serialisable(Types.SBegin),uids);
                case Sym.ROLLBACK:
                    return new ParsePair(new Serialisable(Types.SRollback),uids);
                case Sym.COMMIT:
                    return new ParsePair(new Serialisable(Types.SCommit),uids);
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
            long col = -1;
            String nm = ""; 
            int dt = Types.Serialisable;
            int sq = -1;
            ConstraintPair cs = null;
            switch (lxr.tok)
            {
                case Sym.COLUMN:
                    Next();
                    col = MustBeID();
                    switch (lxr.tok)
                    {
                        case Sym.ADD:
                            Next();
                            cs = ColumnConstraints(tb, col, cs.cs);
                            if (cs.xs.Length != 0)
                                throw new Exception("Unrecognised column constraint");
                            return new SAlter("", dt, tb, col, -1, cs.cs);
                        case Sym.DROP:
                            Next();
                            if (lxr.tok == Sym.ID)
                            {
                                var v = (SDbObject)lxr.val;
                                Next();
                                return new SDrop(tb, col, uids.get(v.uid));
                            }
                            var sy = Sym.syms[lxr.tok];
                            Next();
                            return new SDrop(tb, col, sy);
                        case Sym.TO:
                            Next();
                            switch (lxr.tok)
                            {
                                case Sym.LITERAL:
                                    {
                                        if (!(lxr.val instanceof SInteger))
                                            throw new Exception("Integer expected");
                                        var n = (SInteger)lxr.val;
                                        var nn = n.value;
                                        Next();
                                        return new SAlter("", dt, tb, col, (int)nn, cs.cs);
                                    }
                                case Sym.ID:
                                    {
                                        var v = (SDbObject)lxr.val;
                                        Next();
                                        return new SAlter(uids.get(v.uid), dt, tb, col, -1, cs.cs);
                                    }
                                case Sym.INTEGER: Next(); dt = Types.SInteger; break;
                                case Sym.NUMERIC: Next(); dt = Types.SNumeric; break;
                                case Sym.STRING: Next(); dt = Types.SString; break;
                                case Sym.DATE: Next(); dt = Types.SDate; break;
                                case Sym.TIMESPAN: Next(); dt = Types.STimeSpan; break;
                                case Sym.BOOLEAN: Next(); dt = Types.SBoolean; break;
                                default:
                                    throw new Exception("Type expected");
                            }
                            cs = ColumnConstraints(tb, col, cs.cs);
                            return new SAlter("", dt, tb, col, -1, cs.cs);
                        default:
                            throw new Exception("ADD, DROP or TO expected for ALTER COLUMN");
                    }
                case Sym.DROP:
                    {
                        Next();
                        if (lxr.tok == Sym.ID)
                        {
                            col = MustBeID();
                            return new SDrop(col, tb, "");
                        }
                        Mustbe(Sym.KEY);
                        return new SDropIndex(tb, Cols());
                    }
                case Sym.TO:
                    Next();
                    var tn = MustBeID();
                    return new SAlter(uids.get(tn), dt, tb, -1, -1, cs.cs);
                case Sym.ADD:
                    Next(); 
                    if (lxr.tok != Sym.ID)
                        return TableConstraint(tb);
                    return ColumnDef(tb).col;
            }
            throw new Exception("Bad Alter syntax");
        }
        SIndex TableConstraint(long tb) throws Exception
        {
            boolean p = true;
            long r = -1;
            SList<Long> c = null;
            switch (lxr.tok)
            {
                case Sym.PRIMARY:
                    Next();
                    Mustbe(Sym.KEY);
                    c = Cols();
                    break;
                case Sym.UNIQUE:
                    Next();
                    p = false;
                    c = Cols();
                    break;
                case Sym.FOREIGN:
                    Next();
                    Mustbe(Sym.KEY);
                    c = Cols();
                    Mustbe(Sym.REFERENCES);
                    r = MustBeID();
                    break;
                default:
                    throw new Exception("Syntax error at end of create table statement");
            }
            if (c.Length == 0)
                throw new Exception("Table constraint expected");
            return new SIndex(tb, p, r, c);            
        }
        Serialisable CreateTable() throws Exception
        {
            Next();
            var tb = MustBeID();
            var ctb = TableDef(tb,new SCreateTable(tb, null, null));
            while (lxr.tok==Sym.PRIMARY||lxr.tok==Sym.UNIQUE||lxr.tok==Sym.REFERENCES)
            {
                var x = TableConstraint(tb);
                var cs = ctb.constraints;
                cs = (cs==null)?new SList(x):cs.InsertAt(x,0);
                ctb = new SCreateTable(tb, ctb.coldefs, cs);
                if (lxr.tok == Sym.COMMA)
                    Next();
            }
            return ctb;
        }
        SCreateTable TableDef(long tb,SCreateTable ctb)
                throws Exception
        {
            Mustbe(Sym.LPAREN);
            var cols = ctb.coldefs;
            var cons = ctb.constraints;
            for (; ; )
            {
                var cd = ColumnDef(tb);
                cols = (cols==null)?new SList(cd.col):cols.InsertAt(cd.col,cols.Length); // tb updated with the new column
                if (cd.xs != null) // tableconstraint?
                    cons =(cons==null)?cd.xs:cons.Append(cd.xs);
                switch (lxr.tok)
                {
                    case Sym.RPAREN:
                        Next();
                        return new SCreateTable(ctb.tdef, cols,cons);
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception("Syntax error");
            }
        }
        SList<Long> Cols() throws Exception
        {
            SList<Long> cols = null;
            Mustbe(Sym.LPAREN);
            for (; ;)
            {
                var c = MustBeID();
                cols = (cols==null)?new SList(c):cols.InsertAt(c,cols.Length); // ok
                switch (lxr.tok)
                {
                    case Sym.RPAREN: Next(); return cols;
                    case Sym.COMMA: Next(); continue;
                }
                throw new Exception("Syntax error: " + lxr.tok);
            }
        }
        ColumnTriple ColumnDef(long tb) throws Exception
        {
            var c = MustBeID();
            var t = For(lxr.tok);
            Next();
            var ccs = ColumnConstraints(tb, c, null);
            return new ColumnTriple(c, new SColumn(tb, t, c, ccs.cs), ccs.xs);
        }
        ConstraintPair ColumnConstraints(long tb, long cn,SDict<String,SFunction> cs)
                throws Exception
        {
            SList<SIndex> x = null;
            for (; ; )
                switch (lxr.tok)
                {
                    case Sym.CHECK:
                    {
                        Next();
                        var id = MustBeID();
                        Mustbe(Sym.COLON);
                        if (cs!=null && cs.Contains(uids.get(id)))
                            throw new Exception("Check constraint " + uids.get(id) + " already declared");
                        var c = new SFunction(SFunction.Func.Constraint, Value());
                        var k = uids.get(id);
                        cs = (cs==null)?new SDict(k,c):cs.Add(k, c);
                        break;
                    }
                    case Sym.DEFAULT:
                    {
                        Next();
                        if (cs!=null)
                        {
                            if (cs.Contains("DEFAULT"))
                                throw new Exception("Default is already declared");
                            if (cs.Contains("NOTNULL"))
                                throw new Exception("A column with a default value cannot be declared notnull");
                            if (cs.Contains("GENERATED"))
                                throw new Exception(" generated column cannot specify a default value");
                        }
                        var c = new SFunction(SFunction.Func.Default, Value());
                        var k = "DEFAULT";
                        cs = (cs==null)?new SDict(k,c):cs.Add(k, c);
                        break;
                    }
                    case Sym.GENERATED:
                    {
                        Next();
                        if (cs!=null)
                        {
                            if (cs.Contains("GENERATED"))
                                throw new Exception("Generated expression already defined");
                            if (cs.Contains("NOTNULL"))
                                throw new Exception("A generated columnn cannot be declared notnull");
                            if (cs.Contains("DEFAULT"))
                                throw new Exception(" generated column cannot specify a default value");
                        }
                        var c = new SFunction(SFunction.Func.Generated, Value());
                        var k = "GENERATED";
                        cs = (cs==null)?new SDict(k,c):cs.Add(k, c);                       break;
                    }
                    case Sym.NOTNULL:
                    {
                        Next();
                        if (cs!=null)
                        {
                            if (cs.Contains("GENERATED"))
                                throw new Exception("A generated column cannot be declared notnull");
                            if (cs.Contains("NOTNULL"))
                                throw new Exception("Notnull already specified");
                            if (cs.Contains("DEFAULT"))
                                throw new Exception("A column with a default value cannot be declared notnull");
                        }
                        var c = new SFunction(SFunction.Func.NotNull, SArg.Value);
                        var k = "NOTNULL";
                        cs = (cs==null)?new SDict(k,c):cs.Add(k, c);     
                        break;
                    }
                    case Sym.PRIMARY:
                    {
                        Next();
                        Mustbe(Sym.KEY);
                        var v = new SIndex(tb, true, -1, new SList(cn));
                        x =(x==null)?new SList(v):x.InsertAt(v,x.Length);
                        break;
                    }
                    case Sym.REFERENCES:
                    {
                        Next();
                        var v = new SIndex(tb, false, MustBeID(), new SList(cn));
                        x =(x==null)?new SList(v):x.InsertAt(v,x.Length);
                        break;
                    }
                    default:
                        return new ConstraintPair(cs, x);
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
        SList<SSlot<Long,Serialisable>> Selects() throws Exception
        {
            SList<SSlot<Long, Serialisable>> r = null;
            var k = 0;
            for (; ;Next(),k++)
            {
                var p = SelectItem(k);
                r = (r==null)?new SList(p):r.InsertAt(p,k);
                if (lxr.tok!=Sym.COMMA)
                    return r;
            }
        }
        SSlot<Long,Serialisable> SelectItem(int k) throws Exception
        {
            var c = Value();
            long uid;
            if (lxr.tok == Sym.AS)
            {
                Next();
                uid = Alias(MustBeID());
            }
            else
            {
                var p = NameFor(c);
                if (p != null)
                    uid = p.key;
                else
                    uid = Alias(SName("col" + (k + 1)).uid);
            }
            return new SSlot(uid, c);            
        }
        long Alias(long u)
        {
            var s = uids.get(u);
            var uid = u - 1000000;
            uids = uids.Add(uid, s); // leave in u for now: see UpdateAliases
            names = names.Add(s, uid);
            return uid;
        }
        SSlot<Long,String> NameFor(Serialisable s)
        {
            if (s instanceof SDbObject)
            {
                var so = (SDbObject)s;
                if (uids.Contains(so.uid))
                    return new SSlot(so.uid,uids.get(so.uid));
            }
            if (s instanceof SExpression)
            {
                var se = (SExpression)s;
                if (se.op==SExpression.Op.Dot)
                {
                    var rp = NameFor(se.right);
                    /* var lp = NameFor(se.left);
                    if (lp!=null && rp!=null)
                    {
                        var str = lp.Value.Item2 + "." + rp.Value.Item2;
                        return (Alias(SName(str).uid), str);
                    } */
                    return rp;
                }
            }
            return null;
        }
        Serialisable CreateIndex(boolean primary) throws Exception
        {
            var xn = MustBeID();
            Mustbe(Sym.FOR);
            var tb = MustBeID();
            var cols = Cols();
            long rt = -1;
            if (lxr.tok==Sym.REFERENCES)
            {
                Next();
                rt = MustBeID();
            }
            return new SIndex(tb, primary, rt, cols); // ok
        }
        Serialisable Drop() throws Exception // also see Drop column in Alter
        {
            Next();
            var id = MustBeID();
            return new SDrop(id, -1, "");
        }
        Serialisable Insert() throws Exception
        {
            Next();
            var id = MustBeID();
            SList<Long> cols = null;
            if (lxr.tok == Sym.LPAREN)
                cols = Cols();
            Serialisable vals;
            if (lxr.tok == Sym.VALUES)
                vals = Vals();
            else if (lxr.tok==Sym.SELECT)
                vals = Select();
            else
                throw new Exception("Unknown kind of Insert");
            return new SInsert(id, cols, vals);
        }
        SQuery Query(SDict<Integer,Ident>als,SDict<Integer,Serialisable>cp)
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
            SQuery sqry = tb;
            if (wh!=null) 
                sqry = new SSearch(tb, wh);
            if (lxr.tok!=Sym.GROUPBY)
                return sqry;
            Next();
            SDict<Integer, Long> gp = null;
            while (lxr.tok==Sym.ID)
            {
                var g = ((SDbObject)lxr.val).uid;
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
            return new SGroupQuery(sqry, sqry.display, sqry.cpos, gp, h);
        }
        SQuery TableExp(SDict<Integer,Ident> als, SDict<Integer, Serialisable> cp)
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
            SQuery tb = new STable(id);
            if (lxr.tok == Sym.ID && lxr.val != null)
            {
                var alias = Alias(((SDbObject)lxr.val).uid);
                Next();
                tb = new SAlias(tb,alias,id);
            }
            var jt = SJoin.JoinType.None;
            if (lxr.tok==Sym.COMMA)
            {
                Next();
                jt = SJoin.JoinType.Cross;
            }
            else if (lxr.tok==Sym.CROSS)
            {
                Next();
                Mustbe(Sym.JOIN);
                jt = SJoin.JoinType.Cross;
            } else
            {
                if (lxr.tok==Sym.NATURAL)
                {
                    Next();
                    jt += SJoin.JoinType.Natural;
                    Mustbe(Sym.JOIN);
                }
                else {
                    if (lxr.tok == Sym.INNER)
                    {
                        Next();
                        jt |= SJoin.JoinType.Inner;
                    }
                    else {
                        if (lxr.tok==Sym.LEFT)
                        {
                            Next();
                            jt += SJoin.JoinType.Left;
                        }
                        else if (lxr.tok==Sym.RIGHT)
                        {
                            Next();
                            jt += SJoin.JoinType.Right;
                        }
                        else if (lxr.tok==Sym.FULL)
                        {
                            Next();
                            jt += SJoin.JoinType.Left+SJoin.JoinType.Right;
                        }
                        if (jt!=SJoin.JoinType.None && lxr.tok==Sym.OUTER)
                            Next();
                    }
                    if (jt!=SJoin.JoinType.None)
                        Mustbe(Sym.JOIN);
                }
            }
            if (jt!=SJoin.JoinType.None)
            {
                SList<SExpression> on = null;
                var ra = TableExp(als, cp);
                SDict<Long,Long> us = null;
                if ((jt&(SJoin.JoinType.Cross|SJoin.JoinType.Natural))==0)
                {
                    if (lxr.tok==Sym.USING)
                    {
                        Next();
                        jt += SJoin.JoinType.Named;
                        for (var n=0;;n++)
                        {
                            var v = ((SDbObject)lxr.val).uid;
                            Mustbe(Sym.ID);
                            us=(us==null)?new SDict<Long,Long>(v,v):us.Add(v,v);
                            if (lxr.tok==Sym.COMMA)
                                Next();
                            else
                                break;
                        }
                    }
                    else
                    {
                        Mustbe(Sym.ON);
                        for (var n=0;;n++)
                        {
                            var ex = Conjunct();
                            if (!(ex instanceof SExpression) ||
                                    ((SExpression)ex).op!=SExpression.Op.Eql
                                    || ((SExpression)ex).left.type!=Types.SName
                                || ((SExpression)ex).right.type!=Types.SName)
                                throw new Exception("Column matching expression expected");
                            on = (on==null)?new SList<SExpression>((SExpression)ex):
                                    on.InsertAt((SExpression)ex,n);
                            if (lxr.tok == Sym.AND)
                                Next();
                            else
                                break;
                        }
                    }
                }
                return new SJoin(tb, false, jt, ra, on, us, als, cp); 
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
                return new SInPredicate(a, Factor());
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
                        throw new Exception("PE26");
                    return v;
                case Sym.ID:
                    {
                        if (v == null)
                            throw new Exception("PE27");
                        Next();
                        if (lxr.tok == Sym.DOT)
                        {
                            Next();
                            var nv = lxr.val;
                            MustBeID();
                            return new SExpression(v, SExpression.Op.Dot, nv);
                        }
                        return v;
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
                        if (lxr.tok==Sym.SELECT)
                        {
                            var sq = Select();
                            Mustbe(Sym.RPAREN);
                            return sq;
                        }
                        SList<Ident> a = null;
                        SList<Serialisable> c = null;
                        int n = 0;
                        boolean asseen = false;
                        for (; ; n++)
                        {
                            var p = SelectItem(n);
                            var id = new Ident(p.key,uids.get(p.key));
                            c=(c==null)?new SList(p.val):c.InsertAt(p.val, n);
                            a=(a==null)?new SList(id):a.InsertAt(id, n);
                            if (lxr.tok != Sym.COMMA)
                                break;
                            Next();
                        }
                        Mustbe(Sym.RPAREN);
                        if (n == 0 && !asseen)
                            return c.element;
                        return new SRow(a, c);
                    }
                case Sym.NULL:
                    Next();
                    return Serialisable.Null;
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
            SList<SSlot<Long, Serialisable>> sels = null;
            if (lxr.tok!=Sym.FROM)
                sels = Selects();
            SDict<Integer, Ident> als = null;
            SDict<Integer, Serialisable> cp = null;
            var k = 0;
            if (sels!=null)
            for (var b = sels.First();b!=null;b=b.Next(),k++)
            {
                var ke= b.getValue().key;
                var ve = b.getValue().val;
                var id = new Ident(ke,uids.get(ke));
                als=(als==null)?new SDict(k,id):als.Add(k,id);
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
            return new SSelectStatement(dct, als, cp, q, or);
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
            SDict<Long, Serialisable> sa = null;
            if (lxr.tok != Sym.SET)
                throw new Exception("Expected SET");
            var tt = lxr.tok;
            for (; lxr.tok == tt;)
            {
                Next(); tt = Sym.COMMA;
                var c = MustBeID();
                Mustbe(Sym.EQUAL);
                sa=(sa==null)?new SDict(c,Value()):sa.Add(c, Value());
            }
            return new SUpdateSearch(q, sa);
        }
    }

