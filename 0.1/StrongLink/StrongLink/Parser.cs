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
            WHERE = 80
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
            Parser psr;
            public Lexer(Parser ps,string inp)
            {
                psr = ps;
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
                return (int)r;
            }
            int Unsigned(int n, int low, int high)
            {
                var r = Unsigned(n);
                if (r < low || r > high)
                    throw new Exception("Expected " + low + "<=" + r + "<=" + high);
                return (int)r;
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
                            e = (int)Unsigned() * esg;
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
                        case ':': Advance(); return tok = Sym.COLON;
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
        internal long _uid = -1;
        internal SDict<string, long> names = SDict<string, long>.Empty;
        internal SDict<long, string> uids = SDict<long, string>.Empty;
        Lexer lxr;
        Parser(string inp)
        {
            lxr = new Lexer(this,inp);
        }
        public static (Serialisable, SDict<long,string>) Parse(string sql)
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
        long MustBeID()
        {
            var s = lxr.val;
            if (lxr.tok != Sym.ID || s==null)
                throw new Exception(CheckEOF());
            Next();
            return ((SDbObject)s).uid;
        }
        internal SDbObject SName(string s)
        {
            long uid;
            if (names.Contains(s))
                uid = names[s];
            else
            {
                uid = --_uid;
                names += (s, uid);
                uids += (uid, s);
            }
            return new SDbObject(Types.SName,uid);
        }
        Types DataType(Sym t)
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
        public (Serialisable,SDict<long,string>) Statement()
        {
            switch(lxr.tok)
            {
                case Sym.ALTER:
                    return (Alter(),uids);
                case Sym.CREATE:
                    {
                        Next();
                        switch (lxr.tok)
                        {
                            case Sym.TABLE:
                                return (CreateTable(),uids);
                            case Sym.INDEX:
                                Next();
                                return (CreateIndex(), uids);
                            case Sym.PRIMARY:
                                Next(); Mustbe(Sym.INDEX);
                                return (CreateIndex(true), uids);
                        }
                        throw new Exception("Unknown Create " + lxr.tok);
                    }
                case Sym.DROP:
                    return (Drop(), uids);
                case Sym.INSERT:
                    return (Insert(), uids);
                case Sym.DELETE:
                    return (Delete(), uids);
                case Sym.UPDATE:
                    return (Update(), uids);
                case Sym.SELECT:
                    return (Select().UpdateAliases(uids),uids);
                case Sym.BEGIN:
                    return (new Serialisable(Types.SBegin), uids);
                case Sym.ROLLBACK:
                    return (new Serialisable(Types.SRollback), uids);
                case Sym.COMMIT:
                    return (new Serialisable(Types.SCommit), uids);
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
            long col = -1;
            var add = false;
            var cs = SDict<string, SFunction>.Empty;
            switch (lxr.tok)
            {
                case Sym.COLUMN:
                    Next();
                    col = MustBeID();
 /*                   if (lxr.tok == Sym.ADD)
                        return AddColumnConstraints(tb, col,cs);
                    if (lxr.tok == Sym.DROP)
                        return DropColumnConstraints(tb, col); */
                    Mustbe(Sym.TO);
                    break;
                case Sym.DROP:
                    Next();
/*                    if (lxr.tok != Sym.ID)
                        return DropTableConstraint(tb); */
                    col = MustBeID();
                    return new SDrop(col, tb,""); 
                case Sym.TO:
                    Next(); break;
                case Sym.ADD:
                    Next(); add = true;
/*                    if (lxr.tok != Sym.ID)
                        return AddTableConstraint(tb); */
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
            return new SAlter(uids[nm],dt, tb, col,cs); 
        }
        SCreateTable CreateTable()
        {
            Next();
            var tb = MustBeID();
            var ctb = TableDef(tb,new SCreateTable(tb, SList<SColumn>.Empty, SList<SIndex>.Empty));
            for (; ; )
            {
                bool p = true;
                long r = -1;
                var c = SList<long>.Empty;
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
                        p = false;
                        c = Cols();
                        Mustbe(Sym.REFERENCES);
                        r = MustBeID();
                        break;
                    case Sym.Null:
                        return ctb;
                    case Sym.COMMA:
                        Next();
                        break;
                    default:
                        throw new Exception("Syntax error at end of create table statement");
                }
                if (c.Length != 0)
                {
                    var x = new SIndex(tb, p, r, c);
                    ctb = new SCreateTable(tb, ctb.coldefs, ctb.constraints + x);
                }
            }
        }
        SCreateTable TableDef(long tb,SCreateTable ctb)
        {
            Mustbe(Sym.LPAREN);
            var cols = ctb.coldefs;
            var cons = ctb.constraints;
            for (; ; )
            {
                var cd = ColumnDef(tb);
                cols += (cd.Item2,cols.Length??0); // tb updated with the new column
                if (cd.Item3 != null) // tableconstraint?
                    cons += cd.Item3;
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
        SCreateTable TableConstraints(long tb,SCreateTable ctb)
        {
            for (; ; )
            {
                bool p = true;
                long r = -1;
                var c = SList<long>.Empty;
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
                    case Sym.COMMA:
                        Next();
                        break;
                    case Sym.Null:
                        return ctb;
                    default:
                        throw new Exception("Syntax error at end of create table statement");
                }
                if (c.Length != 0)
                {
                    var x = new SIndex(tb, p, r, c);
                    ctb = new SCreateTable(ctb.tdef, ctb.coldefs, ctb.constraints + x);
                }
            }
        }
        (long, SColumn, SList<SIndex>) ColumnDef(long tb)
        {
            var c = MustBeID();
            var t = DataType(lxr.tok);
            Next();
            var ccs = ColumnConstraints(tb, c, SDict<string,SFunction>.Empty);
            return (c, new SColumn(tb, t, c, ccs.Item1), ccs.Item2);
        }
        (SDict<string,SFunction>, SList<SIndex>) ColumnConstraints(long tb, long cn,SDict<string,SFunction> cs)
        {
            var x = SList<SIndex>.Empty;
            for (; ; )
                switch (lxr.tok)
                {
                    case Sym.CHECK:
                        Next();
                        var id = MustBeID();
                        Mustbe(Sym.COLON);
                        if (cs.Contains(uids[id]))
                            throw new Exception("Check constraint " + uids[id] + " already declared");
                        cs += (uids[id],new SFunction(SFunction.Func.Constraint, Value()));
                        break;
                    case Sym.DEFAULT:
                        Next();
                        if (cs.Contains("DEFAULT"))
                            throw new Exception("Default is already declared");
                        if (cs.Contains("NOTNULL"))
                            throw new Exception("A column with a default value cannot be declared notnull");
                        if (cs.Contains("GENERATED"))
                            throw new Exception(" generated column cannot specify a default value");
                        cs += ("DEFAULT",new SFunction(SFunction.Func.Default, Value()));
                        break;
                    case Sym.GENERATED:
                        Next();
                        if (cs.Contains("GENERATED"))
                            throw new Exception("Generated expression already defined");
                        if (cs.Contains("NOTNULL"))
                            throw new Exception("A generated columnn cannot be declared notnull");
                        if (cs.Contains("DEFAULT"))
                            throw new Exception(" generated column cannot specify a default value");
                        cs += ("GENERATED",new SFunction(SFunction.Func.Generated, Value()));
                        break;
                    case Sym.NOTNULL:
                        Next();
                        if (cs.Contains("GENERATED"))
                            throw new Exception("A generated column cannot be declared notnull");
                        if (cs.Contains("NOTNULL"))
                            throw new Exception("Notnull already specified");
                        if (cs.Contains("DEFAULT"))
                            throw new Exception("A column with a default value cannot be declared notnull");
                        cs += ("NOTNULL",new SFunction(SFunction.Func.NotNull, SArg.Value));
                        break;
                    case Sym.PRIMARY:
                        Next();
                        Mustbe(Sym.KEY);
                        x += new SIndex(tb, true, -1, SList<long>.New(cn));
                        break;
                    case Sym.REFERENCES:
                        Next();
                        x += new SIndex(tb, false, MustBeID(), SList<long>.New(cn));
                        break;
                    default:
                        return (cs, x);
                }
        }
        SList<long> Cols()
        {
            var cols = SList<long>.Empty;
            Mustbe(Sym.LPAREN);
            for (; ;)
            {
                var c = MustBeID();
                cols = cols+(c, cols.Length??0); // ok
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
                    return "Syntax error at " + uids[((SDbObject)lxr.val).uid];
                case Sym.LITERAL:
                    return "Syntax error at " + lxr.val;
                default:
                    return "Syntax error at " + lxr.tok;
            }
        }
        SList<(long,Serialisable)> Selects()
        {
            var r = SList<(long, Serialisable)>.Empty;
            var k = 0;
            for (; ;Next(),k++)
            {
                var p = SelectItem(k);
                r += ((p.Item1, p.Item2), k);
                if (lxr.tok != Sym.COMMA)
                    return r;
            }
        }
        (long,Serialisable) SelectItem(int k)
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
                    uid = p.Value.Item1;
                else
                    uid = Alias(SName("col" + (k + 1)).uid);
            }
            return (uid, c);
        }
        long Alias(long u)
        {
            var s = uids[u];
            var uid = u - 1000000;
            uids = uids + (uid, s); // leave in u for now: see UpdateAliases
            names += (s, uid);
            return uid;
        }
        (long,string)? NameFor(Serialisable s)
        {
            if (s is SDbObject so && uids.Contains(so.uid))
                return (so.uid,uids[so.uid]);
            if (s is SExpression se && se.op==SExpression.Op.Dot)
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
            return null;
        }
        Serialisable CreateIndex(bool primary=false)
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
            return new SIndex(tb, primary, rt, cols); 
        }
        Serialisable Drop() // also see Drop column in Alter
        {
            Next();
            var id = MustBeID();
            return new SDrop(id, -1,"");
        }
        Serialisable Insert()
        {
            Next();
            var id = MustBeID();
            var cols = SList<long>.Empty;
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
            var cs = SList<long>.Empty;
            for (var b = cols.First(); b != null; b = b.Next())
                cs += (b.Value, cs.Length ?? 0);
            return new SInsert(id, cs, vals);
        }
        SQuery Query(SDict<int,(long,string)>als,SDict<int,Serialisable>cp)
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
            var gp = SDict<int, long>.Empty;
            for (n = 0; lxr.tok==Sym.ID; n++)
            {
                gp += (n, ((SDbObject)lxr.val).uid);
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
            return new SGroupQuery(sqry, sqry.display, sqry.cpos,gp, h);
        }
        SQuery TableExp(SDict<int, (long,string)> als, SDict<int, Serialisable> cp)
        {
            if (lxr.tok==Sym.LPAREN)
            {
                SQuery r;
                Next();
                if (lxr.tok == Sym.SELECT)
                    r= (SQuery)Select();
                else
                    r =TableExp(als,cp);
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
            if (lxr.tok == Sym.COMMA)
            {
                Next();
                jt = SJoin.JoinType.Cross;
            }
            else if (lxr.tok == Sym.CROSS)
            {
                Next();
                Mustbe(Sym.JOIN);
                jt = SJoin.JoinType.Cross;
            }
            else
            {
                if (lxr.tok == Sym.NATURAL)
                {
                    Next();
                    jt |= SJoin.JoinType.Natural;
                    Mustbe(Sym.JOIN);
                }
                else
                {
                    if (lxr.tok == Sym.INNER)
                    {
                        Next();
                        jt |= SJoin.JoinType.Inner;
                    }
                    else
                    {
                        if (lxr.tok == Sym.LEFT)
                        {
                            Next();
                            jt |= SJoin.JoinType.Left;
                        }
                        else if (lxr.tok == Sym.RIGHT)
                        {
                            Next();
                            jt |= SJoin.JoinType.Right;
                        }
                        else if (lxr.tok == Sym.FULL)
                        {
                            Next();
                            jt |= (SJoin.JoinType.Left | SJoin.JoinType.Right);
                        }
                        if (jt != SJoin.JoinType.None && lxr.tok == Sym.OUTER)
                            Next();
                    }
                    if (jt!= SJoin.JoinType.None)
                        Mustbe(Sym.JOIN);
                }
            }
            if (jt!=SJoin.JoinType.None)
            {
                var on = SList<SExpression>.Empty;
                var ra = TableExp(als, cp);
                var us = SDict<long,long>.Empty;
                if ((jt&(SJoin.JoinType.Cross|SJoin.JoinType.Natural))==0)
                {
                    if (lxr.tok == Sym.USING)
                    {
                        Next();
                        jt |= SJoin.JoinType.Named;
                        for (; ; )
                        {
                            var v = MustBeID();
                            us += (v,v);
                            if (lxr.tok == Sym.COMMA)
                                Next();
                            else
                                break;
                        }
                    }
                    else
                    {
                        Mustbe(Sym.ON);
                        for (; ; )
                        {
                            var ex = Conjunct();
                            if (!(ex is SExpression e) || e.op != SExpression.Op.Eql
                                || e.left.type!=Types.SName || 
                                e.right.type!=Types.SName)
                                throw new Exception("Column matching expression expected");
                            on += (SExpression)ex;
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
        bool Relation(Serialisable s)
        {
            return (s is SExpression e) && e.op >= SExpression.Op.Eql &&
                e.op <= SExpression.Op.Geq;
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
                    return v ?? throw new Exception("PE26");
                case Sym.ID:
                    {
                        if (v == null)
                            throw new Exception("PE27");
                        var s = ((SDbObject)v).uid;
                        Next();
                        if (lxr.tok == Sym.DOT)
                        {
                            Next();
                            var nv = MustBeID();
                            return new SExpression(v, SExpression.Op.Dot, new SDbObject(Types.SName,nv));
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
                        var a = SList<(long,string)>.Empty;
                        var c = SList<Serialisable>.Empty;
                        int n = 0;
                        for (; ; n++)
                        {
                            var p = SelectItem(n);
                            c += (p.Item2, n);
                            a += ((p.Item1,uids[p.Item1]), n);
                            if (lxr.tok != Sym.COMMA)
                                break;
                            Next();
                        }
                        Mustbe(Sym.RPAREN);
                        if (n+1 == 1 && a.element.Item1<-1000000)
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
            var sels = SList<(long, Serialisable)>.Empty;
            if (lxr.tok!=Sym.FROM)
                sels = Selects();
            var als = SDict<int, (long,string)>.Empty;
            var cp = SDict<int, Serialisable>.Empty;
            var k = 0;
            for (var b = sels.First();b!=null;b=b.Next())
            {
                als += (k, (b.Value.Item1,uids[b.Value.Item1]));
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
            return new SSelectStatement(dct, als, cp, q, or);
        }
        Serialisable Delete()
        {
            Next();
            return new SDeleteSearch(Query(SDict<int,(long,string)>.Empty,SDict<int,Serialisable>.Empty));
        }
        Serialisable Update()
        {
            Next();
            var q = Query(SDict<int, (long,string)>.Empty, SDict<int, Serialisable>.Empty);
            var sa = SDict<long, Serialisable>.Empty;
            if (lxr.tok != Sym.SET)
                throw new Exception("Expected SET");
            var tt = lxr.tok;
            for (; lxr.tok == tt;)
            {
                Next(); tt = Sym.COMMA;
                var c = MustBeID();
                Mustbe(Sym.EQUAL);
                sa += (c, Value());
            }
            return new SUpdateSearch(q, sa);
        }
    }
}
