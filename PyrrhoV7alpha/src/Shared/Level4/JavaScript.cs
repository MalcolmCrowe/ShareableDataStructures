using System;
using System.Collections.Generic;
using System.Text;
using PyrrhoBase;
using Pyrrho.Common;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level4
{
#if MONGO || JAVASCRIPT
    /// <summary>
    /// The LL(1) grammar and other details here are taken from http://hepunx.rl.ac.uk/~adye/jsspec11/llr.htm
    /// </summary>
    internal class JavaScript 
    {
        internal enum Token
        { // keywords
            ABSTRACT,BOOLEAN,BREAK,BYTE,CASE,CATCH,CHAR,CLASS,CONST,CONTINUE,DEFAULT,DELETE,DO,DOUBLE,
            ELSE,EXTENDS,FINAL,FINALLY,FLOAT,FOR,FUNCTION,GOTO,IF,IMPLEMENTS,IMPORT,IN,INSTANCEOF,INT,
            INTERFACE,LONG,NATIVE,NEW,PACKAGE,PRIVATE,PROTECTED,PUBLIC,RETURN,SHORT,STATIC,SUPER,SWITCH,SYNCHRONIZED,
            THIS,THROW,THROWS,TRANSIENT,TRY,TYPEOF,VAR,VOID,VOLATILE,WHILE,WITH,
            //special chars and literals
            TRUE,FALSE,ID,STRINGLITERAL,INTEGERLITERAL,FLOATINGPOINTLITERAL,NULL,
            LPAREN,RPAREN,LBRACE,RBRACE,LBRACK,RBRACK,SEMICOLON,COMMA,
            EQL,GT,LT,BANG,TILDE,QMARK,COLON,DOT,
            EEQ,LEQ,GEQ,NEQ,ANDAND,OROR,PLUSPLUS,MINUSMINUS,
            PLUS,MINUS,TIMES,DIV,AND,OR,CIRCUMFLEX,REM,LLT,GGT,GGGT,
            PEQ,MEQ,TEQ,DEQ,AEQ,OEQ,CEQ,REQ,LLEQ,GGEQ,GGGEQ,
            EOF
        }
        internal class Lexer
        {
            class ResWd { public Token typ; public string spell; public ResWd(Token t, string s) { typ = t; spell = s; } }
            static ResWd[] resWds = new ResWd[0x100]; // open hash
            static Lexer()
            {
                int h;
                for (var t = Token.ABSTRACT; t <= Token.FALSE; t++)
                {
                    string s = t.ToString();
                    h = s.GetHashCode() & 0xff;
                    while (resWds[h] != null)
                        h = (h + 1) & 0xff;
                    resWds[h] = new ResWd(t, s);
                }
            }
            internal char ch;
            internal char[] input;
            internal int pos;
            internal int start;
            internal Token tok;
            internal object val;
            internal Lexer(char[] inp, int off)
            {
                input = inp; pos = off-1;
                Advance();
            }
            internal bool CheckResWd(string s)
            {
                int h = s.GetHashCode() & 0xff;
                for (; ; )
                {
                    var r = resWds[h];
                    if (r == null)
                        return false;
                    if (r.spell == s)
                    {
                        tok = r.typ;
                        return true;
                    }
                    h = (h + 1) & 0xff;
                }
            }
            char Advance()
            {
                if (pos >= input.Length)
                    throw new DBException("42150").Mix();
                if (++pos >= input.Length)
                    ch = (char)0;
                else
                    ch = input[pos];
                return ch;
            }
            internal Token Next()
            {
                while (Char.IsWhiteSpace(ch))
                    Advance();
                start = pos;
                if (Char.IsLetter(ch))
                {
                    while (Char.IsLetterOrDigit(ch) || ch == '_')
                        Advance();
                    var s0 = new string(input, start, pos - start);
                    CheckResWd(s0.ToUpper());
                    val = s0;
                    return tok = Token.ID;
                }
                string str;
                if (Char.IsDigit(ch))
                {
                    start = pos;
                    while (Char.IsDigit(Advance()))
                        ;
                    if (ch != '.')
                    {
                        str = new string(input, start, pos - start);
                        if (pos - start > 18)
                            val = Integer.Parse(str);
                        else
                            val = long.Parse(str);
                        tok = Token.INTEGERLITERAL;
                        return tok;
                    }
                    while (Char.IsDigit(Advance()))
                        ;
                    if (ch != 'e' && ch != 'E')
                    {
                        str = new string(input, start, pos - start);
                        val = Common.Numeric.Parse(str);
                        tok = Token.FLOATINGPOINTLITERAL;
                        return tok;
                    }
                    if (Advance() == '-' || ch == '+')
                        Advance();
                    if (!Char.IsDigit(ch))
                        throw new DBException("22107").Mix();
                    while (Char.IsDigit(Advance()))
                        ;
                    str = new string(input, start, pos - start);
                    val = Common.Numeric.Parse(str);
                    tok = Token.FLOATINGPOINTLITERAL;
                    return tok;
                }
                switch (ch)
                {
                    case '[': Advance(); return tok = Token.LBRACK;
                    case ']': Advance(); return tok = Token.RBRACK;
                    case '(': Advance(); return tok = Token.LPAREN;
                    case ')': Advance(); return tok = Token.RPAREN;
                    case '{': Advance(); return tok = Token.LBRACE;
                    case '}': Advance(); return tok = Token.RBRACE;
                    case '+': Advance(); tok = Token.PLUS;
                        if (ch == '+') { Advance(); tok = Token.PLUSPLUS; }
                        else if (ch == '=') { Advance(); tok = Token.PEQ; }
                        return tok;
                    case '-': Advance(); tok = Token.MINUS;
                        if (ch == '-') { Advance(); tok = Token.MINUSMINUS; }
                        else if (ch == '=') { Advance(); tok = Token.MEQ; }
                        return tok;
                    case '*': Advance(); tok = Token.TIMES;
                        if (ch == '=') { Advance(); tok = Token.TEQ; }
                        return tok;
                    case '/': Advance(); tok = Token.DIV;
                        if (ch == '=') { Advance(); tok = Token.DEQ; }
                        return tok;
                    case '=': Advance(); tok = Token.EQL;
                        if (ch == '=') { Advance(); tok = Token.EEQ; }
                        return tok;
                    case '<': Advance(); tok = Token.LT;
                        if (ch == '=') { Advance(); tok = Token.LEQ; }
                        else if (ch == '<')
                        {
                            Advance(); tok = Token.LLT;
                            if (ch == '=') { Advance(); tok = Token.LLEQ; }
                        }
                       return tok;
                    case '>': Advance(); tok = Token.GT;
                        if (ch == '=') { Advance(); tok = Token.GEQ; }
                        else if (ch == '>')
                        {
                            Advance(); tok = Token.GGT;
                            if (ch == '=') { Advance(); tok = Token.GGEQ; }
                            else if (ch == '>')
                            {
                                Advance(); tok = Token.GGGT;
                                if (ch == '=') { Advance(); tok = Token.GGGEQ; }
                            }
                        }
                        return tok;
                    case '!': Advance(); tok = Token.BANG;
                        if (ch == '=') { Advance(); tok = Token.NEQ; }
                        return tok;
                    case '^': Advance(); tok = Token.CIRCUMFLEX;
                        if (ch == '=') { Advance(); tok = Token.CEQ; }
                        return tok;
                    case '%': Advance(); tok = Token.REM;
                        if (ch == '=') { Advance(); tok = Token.REQ; }
                        return tok;
                    case '&': Advance(); tok = Token.AND;
                        if (ch == '=') { Advance(); tok = Token.AEQ; }
                        else if (ch == '&') { Advance(); tok = Token.ANDAND; }
                        return tok;
                    case '|': Advance(); tok = Token.OR;
                        if (ch == '=') { Advance(); tok = Token.OEQ; }
                        else if (ch == '|') { Advance(); tok = Token.OROR; }
                        return tok;
                    case ',': Advance(); return tok = Token.COMMA;
                    case '.': Advance(); return tok = Token.DOT;
                    case ';': Advance(); return tok = Token.SEMICOLON;
                    case '\'': val = GetString(ch); return Token.STRINGLITERAL;
                    case '"': goto case '\'';
                }
                return Token.EOF;
            }
            string GetString(char qu)
            {
                var sb = new StringBuilder();
                while (true)
                {
                    Advance();
                    if (ch == '\0')
                        throw new DBException("22300", "Non-terminated string at " + pos);
                    if (ch == qu)
                        return sb.ToString();
                    if (ch == '\\')
                        ch = GetEscape();
                    sb.Append(ch);
                }
            }
            char GetEscape()
            {
                Advance();
                switch (ch)
                {
                    case '"': return ch;
                    case '\\': return ch;
                    case '/': return ch;
                    case 'b': return '\b';
                    case 'f': return '\f';
                    case 'n': return '\n';
                    case 'r': return '\r';
                    case 't': return '\t';
                    case 'u':
                        {
                            int v = 0;
                            for (int j = 0; j < 4; j++)
                                v = (v << 4) + GetHex(ch);
                            return (char)v;
                        }
                    case 'U': goto case 'u';
                }
                throw new DBException("22300", "Illegal escape");
            }
            internal static int GetHex(char ch)
            {
                if (ch >= '0' && ch <= '9') return ch - '0';
                if (ch >= 'a' && ch <= 'f') return (ch - 'a')+10;
                if (ch >= 'A' && ch <= 'F') return (ch = 'A')+10;
                throw new DBException("22300", "Illegal escape");
            }

            internal Common.Ident Ident()
            {
                return new Common.Ident(val as string,Common.Ident.IDType.Alias);
            }
        }
        internal class Parser
        {
            Lexer lxr;
            Context ctx;
            Token tok;
            internal Parser(Context cx, Lexer lx)
            {
                ctx = cx; lxr = lx;
                Next();
            }
            void Next()
            {
                tok = lxr.Next();
            }
            bool Match(params Token[] toks)
            {
                foreach (var t in toks)
                    if (tok == t)
                        return true;
                return false;
            }
            void MustBe(Token t)
            {
                if (t != tok)
                    throw new DBException("40000");
                Next();
            }
            Ident MustbeID()
            {
                var o = lxr.Ident();
                MustBe(Token.ID);
                return o;
            }
            bool StartOfStatement()
            {
                return Match(Token.IF, Token.WHILE, Token.FOR, Token.BREAK, Token.CONTINUE,
                    Token.WITH, Token.RETURN, Token.LBRACE, Token.VAR, Token.ID);
            }
            bool StartOfExpression()
            {
                return StartOfPrimaryExpression() || Match(Token.PLUSPLUS, Token.MINUSMINUS, 
                    Token.PLUS, Token.MINUS, Token.BANG, Token.CIRCUMFLEX);
            }
            bool StartOfPrimaryExpression()
            {
                return Match(Token.ID, Token.LPAREN, Token.INTEGERLITERAL, Token.STRINGLITERAL,
                    Token.FLOATINGPOINTLITERAL, Token.FALSE, Token.TRUE, Token.NULL, Token.THIS,
                    Token.PLUSPLUS, Token.MINUSMINUS, Token.PLUS, Token.MINUS, Token.BANG, Token.CIRCUMFLEX);
            }
            bool RelationalOperator()
            {
                return Match(Token.LT, Token.LEQ, Token.GT, Token.GEQ);
            }
            bool AssignmentOperator()
            {
                return Match(Token.PEQ, Token.MEQ, Token.TEQ, Token.DEQ, Token.AEQ, Token.OEQ,
                    Token.CEQ, Token.REQ, Token.LLEQ, Token.GGEQ, Token.GGGEQ);
            }
            bool UnaryOperator() // excludes IncremeentOperators
            {
                return Match(Token.BANG, Token.TILDE, Token.PLUS, Token.MINUS);
            }
            bool ShiftOperator()
            {
                return Match(Token.GGT, Token.GGGT, Token.LLT);
            }
            bool MultiplicativeOperator()
            {
                return Match(Token.TIMES, Token.DIV, Token.REM);
            }
            internal Executable ParseElement()
            {
                if (Match(Token.FUNCTION))
                {
                    var p = new Ident[0];
                    var o = lxr.Ident();
                    if (tok == Token.ID)
                        Next();
                    MustBe(Token.LPAREN);
                    if (tok != Token.RPAREN)
                        p = ParseParameterList();
                    MustBe(Token.RPAREN);
                    var r = new Function(ctx,p,ParseCompoundStatement());
                    r.label = o;
                    return r;
                }
                return ParseStatement();
            }
            private Executable ParseCompoundStatement()
            {
                var r = new CompoundStatement(null);
                MustBe(Token.LBRACE);
                r.stms = ParseStatements();
                MustBe(Token.RBRACE);
                return r;
            }
            private Ident[] ParseParameterList()
            {
                var r = new List<Ident>();
                r.Add(MustbeID());
                while (tok == Token.COMMA)
                {
                    Next();
                    r.Add(MustbeID());
                }
                return r.ToArray();
            }
            Executable[] ParseStatements()
            {
                var r = new List<Executable>();
                while (tok != Token.RBRACE)
                    r.Add(ParseStatement());
                return r.ToArray();
            }
            private Executable ParseStatement()
            {
                switch (tok)
                {
                    case Token.SEMICOLON: Next(); return null;
                    case Token.IF:
                        {
                            Next();
                            var e = new IfThenElse();
                            e.search = ParseCondition();
                            e.then = new Executable[] {ParseStatement() };
                            if (tok == Token.ELSE)
                            {
                                Next();
                                e.els = new Executable[] {ParseStatement() };
                            }
                            return e;
                        }
                    case Token.WHILE:
                        {
                            Next();
                            var w = new WhileStatement(null);
                            w.search = ParseCondition();
                            w.what = new Executable[] { ParseStatement() };
                            return w;
                        }
                    case Token.FOR:
                        {
                            Next();
                            MustBe(Token.LPAREN);
                            var f = new ForStatement();
                            f.var = ParseVariables();
                            MustBe(Token.SEMICOLON);
                            f.search = ParseExpressionOpt();
                            MustBe(Token.SEMICOLON);
                            f.onIter = ParseExpressionOpt();
                            MustBe(Token.RPAREN);
                            f.body = ParseStatement();
                            return f;
                        }
                    case Token.BREAK:
                        {
                            Next();
                            MustBe(Token.SEMICOLON);
                            return new BreakStatement(null);
                        }
                    case Token.CONTINUE:
                        {
                            Next();
                            MustBe(Token.SEMICOLON);
                            return new ContinueStatement();
                        }
                    case Token.WITH:
                        {
                            Next();
                            MustBe(Token.LPAREN);
                            var w = ParseExpression();
                            MustBe(Token.RPAREN);
                            return new WithStatement(w, ParseStatement());
                        }
                    case Token.RETURN:
                        {
                            var r = new ReturnStatement();
                            if (tok != Token.SEMICOLON)
                                r.ret = ParseExpression();
                            MustBe(Token.SEMICOLON);
                            return r;
                        }
                    case Token.VAR:
                        {
                            Next();
                            return ParseVariables();
                        }
                }
                return new ExpressionStatement(ParseExpression());
            }
            SqlValue ParseCondition()
            {
                MustBe(Token.LPAREN);
                var e = ParseExpression();
                MustBe(Token.RPAREN);
                return e;
            }
            LocalVariables ParseVariables()
            {
                LocalVariables n = null;
                var v = ParseVariable();
                if (tok == Token.COMMA)
                {
                    Next();
                    n = ParseVariables();
                }
                return new LocalVariables(v,n);
            }
            LocalVariableDec ParseVariable()
            {
                return new LocalVariableDec(ctx, MustbeID(), SqlDataType.Content);
            }
            SqlValue ParseExpressionOpt()
            {
                if (!StartOfExpression())
                    return null;
                return ParseExpression();
            }
            internal SqlValue ParseExpression()
            {
                var a = ParseAssignmentExpression();
                while (tok == Token.COMMA)
                {
                    Next();
                    var b = ParseAssignmentExpression();
                    a = new SqlValueExpr(ctx, Sqlx.COMMA, a, b, Sqlx.NO);
                }
                return a;
            }
            SqlValue ParseAssignmentExpression()
            {
                var a = ParseConditionalExpression();
                while (AssignmentOperator())
                {
                    var t = tok;
                    Next();
                    var b = ParseConditionalExpression();
                    var e = new SqlValueExpr(ctx, Sqlx.EQL, a, b, Sqlx.NO);
                    switch (t)
                    {
                        case Token.PEQ: e.kind = Sqlx.PLUS; break;
                        case Token.MEQ: e.kind = Sqlx.MINUS; break;
                        case Token.TEQ: e.kind = Sqlx.TIMES; break;
                        case Token.DEQ: e.kind = Sqlx.DIVIDE; break;
                        case Token.AEQ: e.kind = Sqlx.AND; e.mod = Sqlx.BINARY; break;
                        case Token.OEQ: e.kind = Sqlx.OR; e.mod = Sqlx.BINARY; break;
                        case Token.CEQ: e.kind = Sqlx.OR; e.mod = Sqlx.EXCEPT; break;
                        case Token.REQ: e.kind = Sqlx.MOD; break;
                        case Token.LLEQ: e.kind = Sqlx.UPPER; break;
                        case Token.GGEQ: e.kind = Sqlx.LOWER; break;
                        case Token.GGGEQ: e.kind = Sqlx.LOWER; e.mod = Sqlx.GTR; break;
                    }
                    e.assign = a.LVal(ctx);
                    a = e;
                }
                return a;
            }

            SqlValue ParseConditionalExpression()
            {
                var a = ParseOrExpression();
                if (tok == Token.QMARK)
                {
                    Next();
                    var b = ParseAssignmentExpression();
                    MustBe(Token.COLON);
                    var c = ParseAssignmentExpression();
                    a = new SqlValueExpr(ctx, Sqlx.QMARK, a, new SqlValueExpr(ctx, Sqlx.COLON, b, c, Sqlx.NO), Sqlx.NO);
                }
                return a;
            }
            SqlValue ParseOrExpression()
            {
                var a = ParseAndExpression();
                while (tok == Token.OROR)
                {
                    Next();
                    var b = ParseAndExpression();
                    a = new SqlValueExpr(ctx, Sqlx.OR, a, b, Sqlx.NO);
                }
                return a;
            }
            SqlValue ParseAndExpression()
            {
                var a = ParseBitwiseOrExpression();
                while (tok == Token.ANDAND)
                {
                    Next();
                    var b = ParseBitwiseOrExpression();
                    a = new SqlValueExpr(ctx, Sqlx.AND, a, b, Sqlx.NO);
                }
                return a;
            }
            SqlValue ParseBitwiseOrExpression()
            {
                var a = ParseBitwiseXorExpression();
                while (tok == Token.OR)
                {
                    Next();
                    var b = ParseBitwiseXorExpression();
                    a = new SqlValueExpr(ctx, Sqlx.OR, a, b, Sqlx.BINARY);
                }
                return a;
            }
            SqlValue ParseBitwiseXorExpression()
            {
                var a = ParseBitwiseAndExpression();
                while (tok == Token.CIRCUMFLEX)
                {
                    Next();
                    var b = ParseBitwiseAndExpression();
                    a = new SqlValueExpr(ctx, Sqlx.OR, a, b, Sqlx.EXCEPT);
                }
                return a;
            }
            SqlValue ParseBitwiseAndExpression()
            {
                var a = ParseEqualityExpression();
                while (tok == Token.AND)
                {
                    Next();
                    var b = ParseEqualityExpression();
                    a = new SqlValueExpr(ctx, Sqlx.AND, a, b, Sqlx.BINARY);
                }
                return a;
            }
            SqlValue ParseEqualityExpression()
            {
                var a = ParseRelationalExpression();
                while (tok == Token.EEQ || tok == Token.NEQ)
                {
                    var t = tok;
                    Next();
                    var b = ParseRelationalExpression();

                    a = new SqlValueExpr(ctx, (t == Token.EEQ) ? Sqlx.EQL : Sqlx.NEQ, a, b, Sqlx.NO);
                }
                return a;
            }
            SqlValue ParseRelationalExpression()
            {
                var a = ParseShiftExpression();
                while (RelationalOperator())
                {
                    var t = tok;
                    Next();
                    var b = ParseShiftExpression();
                    Sqlx s = Sqlx.NO;
                    switch (t)
                    {
                        case Token.LT: s = Sqlx.LSS; break;
                        case Token.LEQ: s = Sqlx.LEQ; break;
                        case Token.GT: s = Sqlx.GTR; break;
                        case Token.GEQ: s = Sqlx.GEQ; break;
                    }
                    a = new SqlValueExpr(ctx, s, a, b, Sqlx.NO);
                }
                return a;
            }
            SqlValue ParseShiftExpression()
            {
                var a = ParseAdditiveExpression();
                while (ShiftOperator())
                {
                    var t = tok;
                    Next();
                    var b = ParseAdditiveExpression();
                    var e = new SqlValueExpr(ctx, Sqlx.LOWER, a, b, Sqlx.NO);
                    switch (t)
                    {
                        case Token.LLT: e.kind = Sqlx.UPPER; break;
                        case Token.GGT: e.kind = Sqlx.LOWER; break;
                        case Token.GGGT: e.kind = Sqlx.LOWER; e.mod = Sqlx.GTR; break;
                    }
                    a = e;
                }
                return a;
            }
            SqlValue ParseAdditiveExpression()
            {
                var a = ParseMultiplicativeExpression();
                while (Match(Token.PLUS,Token.MINUS))
                {
                    var t = tok;
                    Next();
                    var b = ParseMultiplicativeExpression();
                    a = new SqlValueExpr(ctx, (t==Token.PLUS)?Sqlx.PLUS:Sqlx.MINUS, a, b, Sqlx.NO);
                }
                return a;
            }
            SqlValue ParseMultiplicativeExpression()
            {
                var a = ParseUnaryExpression();
                while (MultiplicativeOperator())
                {
                    var t = tok;
                    Next();
                    var b = ParseUnaryExpression();
                    var e = new SqlValueExpr(ctx, Sqlx.TIMES, a, b, Sqlx.NO);
                    switch (t)
                    {
                        case Token.TIMES: e.kind = Sqlx.TIMES; break;
                        case Token.DIV: e.kind = Sqlx.DIVIDE; break;
                        case Token.REM: e.kind = Sqlx.MOD; break;
                    }
                    a = e;
                }
                return a;
            }
            SqlValue ParseUnaryExpression()
            {
                if (tok == Token.NEW)
                {
                    Next();
                    return ParseConstructor();
                }
                if (tok == Token.DELETE)
                {
                    Next();
                    return new SqlValueExpr(ctx,Sqlx.DELETE,ParseMemberExpression(),null,Sqlx.NO);
                }
                SqlValue v = null;
                if (UnaryOperator())
                {
                    var t = tok;
                    Next();
                    var a = ParseUnaryExpression();
                    var e = new SqlValueExpr(ctx,Sqlx.NULL, a,null,Sqlx.NO);
                    switch (t)
                    {
                        case Token.MINUS: e.kind = Sqlx.MINUS; e.right = e.left;
                            e.left = new SqlLiteral(ctx, new TInt(0));
                            break;
                        case Token.BANG: e.kind = Sqlx.NOT; break;
                        case Token.TILDE: e.kind = Sqlx.NOT; e.mod = Sqlx.BINARY; break;
                        case Token.PLUS: e.kind = Sqlx.PLUS; e.right = e.left;
                            e.left = new SqlLiteral(ctx, new TInt(0));
                            break;
                    }
                    v = e;
                }
                else if (Match(Token.PLUSPLUS, Token.MINUSMINUS))
                {
                    var t = tok;
                    Next();
                    var a = ParseMemberExpression();
                    v = new SqlValueExpr(ctx, (t==Token.PLUSPLUS)?Sqlx.ASC:Sqlx.DESC, a, null, Sqlx.BEFORE);
                } else
                    v = ParseMemberExpression();
                if (Match(Token.PLUSPLUS, Token.MINUSMINUS))
                {
                    var t = tok;
                    Next();
                    v = new SqlValueExpr(ctx, (t == Token.PLUSPLUS) ? Sqlx.ASC : Sqlx.DESC, v, null,Sqlx.NO);
                }
                return v;
            }
            SqlValue ParseConstructor()
            {
                if (tok == Token.THIS)
                {
                    Next();
                    MustBe(Token.DOT);
                }
                var cx = ctx;
                SqlValue r = null;
                SqlValue v = null;
                for (; ; )
                {
                    var s = MustbeID();
                    v = new SqlName(cx, s);
                    if (tok == Token.LPAREN)
                    {
                        var e = new SqlValueExpr(ctx, Sqlx.CALL, v, null, Sqlx.NO);
                        Next();
                        e.right = (tok == Token.RPAREN) ? null : ParseArgumentList();
                        v = e;
                    }
                    if (r == null)
                        r = v;
                    else
                        r = new SqlValueExpr(cx, Sqlx.DOT, r, v, Sqlx.NO);
                    if (tok != Token.DOT)
                        break;
                    Next();
                }
                return r;
            }
            SqlValue ParseMemberExpression()
            {
                var o = lxr.Ident();
                SqlValue w = null;
                Context cx = ctx;
                if (Match(Token.STRINGLITERAL))
                    return new SqlLiteral(ctx, new TChar((string)lxr.val));
                if (Match(Token.INTEGERLITERAL))
                    return new SqlLiteral(ctx, new TInt((long)lxr.val));
                if (Match(Token.FLOATINGPOINTLITERAL))
                    return new SqlLiteral(ctx, new TReal((double)lxr.val));
                if (Match(Token.TRUE))
                    return new SqlLiteral(ctx, TBool.True);
                if (Match(Token.FALSE))
                    return new SqlLiteral(ctx, TBool.False);
                if (Match(Token.ID))
                {
                    Next();
                    var s = o;
                    SqlValue v = new SqlName(cx,s);
                    if (tok == Token.LPAREN)
                    {
                        Next();
                        v = new SqlValueExpr(cx, Sqlx.CALL,v,ParseArgumentList(),Sqlx.NO);
                    } else

                        while (tok == Token.DOT)
                        {
                            Next();
                            s = MustbeID();
                            v = new SqlName(cx, s);
                            if (tok == Token.LPAREN)
                            {
                                Next();
                                v = new SqlValueExpr(cx, Sqlx.CALL, v, ParseArgumentList(), Sqlx.NO);
                            }
                            if (w == null)
                                w = v;
                            else
                                w = new SqlValueExpr(cx, Sqlx.DOT, w, v, Sqlx.NO);
                        }
                }
                while (Match(Token.LBRACK,Token.LPAREN))
                {
                    var t = tok;
                    Next();
                    if (t == Token.LBRACK)
                    {
                        var e = ParseExpression();
                        MustBe(Token.RBRACK);
                        w = new SqlValueExpr(ctx, Sqlx.ARRAY, w, e, Sqlx.NO);
                    }
                    else // tok==LPAREN
                    {
                        Next();
                        w = new SqlValueExpr(ctx, Sqlx.CALL, w, ParseArgumentList(), Sqlx.NO);
                    }
                }
                return w;
            }
            SqlValue ParseArgumentList()
            {
                return new SqlArgList(ctx, ParseArguments());
            }
            SqlValue[] ParseArguments()
            {
                var r = new List<SqlValue>();
                r.Add(ParseAssignmentExpression());
                while (tok == Token.COMMA)
                {
                    Next();
                    r.Add(ParseAssignmentExpression());
                }
                MustBe(Token.RPAREN);
                return r.ToArray();
            }
            SqlValue ParsePrimaryExpression()
            {
                if (Match(Token.LPAREN))
                {
                    Next();
                    var e = ParseExpression();
                    MustBe(Token.RPAREN);
                    return e;
                }
                var o = lxr.val;
                if (Match(Token.STRINGLITERAL))
                {
                    Next();
                    return new SqlLiteral(ctx, new TChar((string)o));
                }
                if (Match(Token.INTEGERLITERAL))
                {
                    Next();
                    return new SqlLiteral(ctx, new TInt((long)o));
                }
                if (Match(Token.FLOATINGPOINTLITERAL))
                {
                    Next();
                    return new SqlLiteral(ctx, new TReal((double)o));
                }
                if (Match(Token.TRUE, Token.FALSE))
                {
                    var t = tok;
                    Next();
                    return new SqlLiteral(ctx, (t == Token.TRUE) ? TBool.True : TBool.False);
                }
                if (Match(Token.NULL))
                {
                    Next();
                    return new SqlLiteral(ctx, (TypedValue)null);
                }
                if (Match(Token.THIS))
                {
                    Next();
                    return new SqlName(ctx,new Ident("THIS"));
                }
                throw new DBException("4000", "Syntax error");
            }
        }

        internal class Function : Executable
        {
            public Ident[] parms;
            public Executable body;
            internal Function(Context cx,Ident[] p, Executable b) :base(Type.Function) { parms = p; body = b; }
            internal static ATree<string, Function> builtin = BTree<string, Function>.Empty;
            static BuiltIn[] builtins = new BuiltIn[] {
                new BuiltIn("eval", new Eval()) ,
                new BuiltIn("parseInt",new ParseInt()),
                new BuiltIn("parseFloat",new ParseFloat()),
                new BuiltIn("escape",new Escape()),
                new BuiltIn("unescape",new Unescape()),
                new BuiltIn("Array",new Array()),
                new BuiltIn("length",new Length()),
                new BuiltIn("join",new Join()),
                new BuiltIn("reverse",new Reverse()),
                new BuiltIn("sort",new Sort())
            };
            static Function()
            {
                foreach (var b in builtins)
                    ATree<string, Function>.AddNN(ref builtin, b.name, new Function(null, null,b.body));
            }
            class BuiltIn
            {
                internal string name;
                internal Executable body;
                internal BuiltIn(string n, Executable b)
                {
                    name = n;
                    body = b;
                }
            }
        }
        /// <summary>
        /// JavaScript Built-in functions
        /// </summary>
        internal class Eval : ExpressionStatement
        {
            internal Eval():base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as Activation; // from the top of the stack each time
                var e = a.variables["0"];
                var dt = e.dataType;
                if (dt.kind != Sqlx.CHAR)
                    a.ret = e;
                else
                {
                    var s = e.Val() as string;
                    try
                    {
                        var el = new Parser(a, new Lexer(s.ToCharArray(), 0)).ParseElement();
                        el.Obey(tr);
                    }
                    catch
                    {
                        a.ret = e;
                    }
                }
            }
        }
        internal class ParseInt : ExpressionStatement
        {
            internal ParseInt() : base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as StructuredActivation; // from the top of the stack each time
                a.ret = new TInt(Compute(a.variables["0"].ToString(), 
                                         a.variables["1"]?.ToInt().Value??0));
            }
            int Compute(string s, int rx)
            {
                if (s == null)
                    return 0;
                if (rx<=0)
                    rx = 10;
                if (rx > 36)
                    return 0;
                var inp = s.ToCharArray();
                var p = 0;
                if (inp.Length >= 2 && inp[0] == '0')
                {
                    p = 1;
                    rx = 8;
                    if (inp[1] == 'x')
                    {
                        p = 2;
                        rx = 16;
                    }
                }
                int v = 0;
                for (; p < inp.Length; p++)
                {
                    var c = inp[p];
                    var d = 0;
                    if (c >= '0' && c <= '0') d = c - '0';
                    else if (c >= 'a' && c <= 'z') d = (c - 'a')+10;
                    else if (c >= 'A' && c <= 'Z') d = (c - 'A')+10;
                    else break;
                    v = v * rx + d;
                }
                return v;
            }
        }
        internal class ParseFloat : ExpressionStatement
        {
            internal ParseFloat() : base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as StructuredActivation; // from the top of the stack each time
                var t = a.variables["0"];
                if (t == null)
                    return;
                var s = t.ToString();
                if (s == null)
                    return;
                a.ret = new TReal( Compute(s));
            }
            float Compute(string s)
            {
                float r;
                if (float.TryParse(s, out r))
                    return r;
                return float.NaN;
            }
        }
        internal class Escape : ExpressionStatement
        {
            internal Escape() : base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as StructuredActivation; // from the top of the stack each time
                var t = a.variables["0"];
                if (t == null)
                    return;
                var s = t.ToString();
                if (s == null)
                    return;
                a.ret = new TChar( Compute(s));
            }
            string Compute(string s)
            {
                if (s == null)
                    return null;
                var sb = new StringBuilder();
                foreach (var c in s)
                {
                    if (Char.IsLetterOrDigit(c)) sb.Append(c);
                    else if (c == ' ') sb.Append('+');
                    else sb.Append("%" + ((int)c).ToString("X"));
                }
                return sb.ToString();
            }
        }
        internal class Unescape : ExpressionStatement
        {
            internal Unescape() : base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as Activation; // from the top of the stack each time
                a.ret = new TChar( Compute(a.variables["0"].ToString()));
            }
            string Compute(string s)
            {
                if (s == null)
                    return null;
                var inp = s.ToCharArray();
                var sb = new StringBuilder();
                var p = 0;
                while(p<inp.Length)
                {
                    var c = inp[p++];
                    if (c == '%' && p + 1 < inp.Length)
                    { // JavaScript uses exactly two characters
                        c = (char)(Lexer.GetHex(inp[p]) * 16 + Lexer.GetHex(inp[p + 1]));
                        p += 2;
                    }
                    sb.Append(c);
                }
                return sb.ToString();
            }
        }
        internal class Array : ExpressionStatement
        {
            internal Array() : base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as Activation; // from the top of the stack each time
                var d = new TDocArray(a);
                for (int i = 1; i < a.variables.Count; i++)
                    d.Add(a.variables[""+i]);
                a.ret = d;
            }
        }
        internal class Length : ExpressionStatement
        {
            internal Length() : base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as StructuredActivation; // from the top of the stack each time
                var v = a.Eval();
                var d = v[0] as TDocument;
                if (d != null)
                    a.ret = new TInt(d.content.Count);
                var s = v.ToString();
                if (s != null)
                    a.ret = new TInt(s.Length);
            }
        }
        internal class Join : ExpressionStatement
        {
            internal Join() : base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as StructuredActivation; // from the top of the stack each time
                var p = a.Eval();
                if (p == null || p[0].dataType.kind != Sqlx.DOCARRAY)
                    return;
                var d = p[0] as TDocument;
                if (d == null)
                    return;
                var sb = new StringBuilder();
                for (var b = d.content.First();b!=null;b=b.Next())
                    if (b.value() != null)
                        sb.Append(b.value().ToString());
                a.ret = new TChar( sb.ToString());
            }
        }
        internal class Reverse : ExpressionStatement
        {
            internal Reverse() : base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as StructuredActivation; // from the top of the stack each time
                var v = a.Eval();
                var id = v.dataType.names[0];
                if (v == null || v[0].dataType != SqlDataType.DocArray)
                    return;
                var d = v[0] as TDocArray;
                if (d == null)
                    return;
                var r = new TDocArray(a);
                var n = (int)d.content.Count;
                for (int i = n - 1; i >= 0; i--)
                    r.Add(d.content[i]);
                ATree<string,TypedValue>.Add(ref a.variables,id.ident,r);
            }
        }
        internal class Sort : ExpressionStatement
        {
            internal Sort() : base(null) { }
            public override void Obey(IContext tr)
            {
                var a = tr.context as StructuredActivation; // from the top of the stack each time
                var d = a.Eval()[0] as TDocArray;
                if (d == null)
                    return;
                var f = d["0"];
                if (f == null)
                    return;
                var fn = f.Val() as Function;
                if (fn == null)
                    return;
                ATree<TypedValue, TypedValue> t = new JSTree(tr, fn);
                for (var b =d.content.First();b!=null;b=b.Next())
                    ATree<TypedValue, TypedValue>.Add(ref t, b.value(), TBool.True);
                var r = new TDocArray(a);
                for (var e =t.First();e!=null;e=e.Next())
                    r.Add(e.key());
                var n = a.nominalDataType.names[0];
                ATree<string, TypedValue>.Add(ref a.variables, n.ident, r);
            }
            internal class JSTree : CTree<TypedValue,TypedValue>
            {
                IContext tr;
                Function cfn;
                Activation act = null;
                internal JSTree(IContext t, Function fn):base(t.context,SqlDataType.JavaScript) { tr = t; cfn = fn; }
                public override int Compare(TypedValue a,  TypedValue b)
                {
                    var act = tr.context as Activation;
                    if (a == null)
                    {
                        if (b == null)
                            return 0;
                        return -1;
                    }
                    if (cfn.parms.Length==2)
                    {
                        if (act==null)
                            act = new Activation(act,null);
                        var pa = cfn.parms[0];
                        var pb = cfn.parms[1];
                        ATree<string, TypedValue>.Add(ref na.variables, pa.ident, a);
                        ATree<string, TypedValue>.Add(ref na.variables, pb.ident, b);
                        if (act.Push())
                        try {
                            cfn.body.Obey(tr);

                            if (act.variables.ret != null && na.ret.dataType.kind == Sqlx.INTEGER)
                                return na.ret.ToInt().Value;
                        } catch (Exception e) { throw e; }
                        finally {
                            act.Pop();
                        }
                    }
                    return a.ToString().CompareTo(b.ToString());
                }
            }
        }

    }
#endif
}
