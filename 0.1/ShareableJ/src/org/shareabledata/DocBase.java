/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

/**
 *
 * @author Malcolm
 */
public class DocBase {
        public DocBase() { }
        protected ObjPos GetValue(String s, int n, int i)
        {
            if (i < n)
            {
                var c = s.charAt(i - 1);
                if (c == '"' || c=='\'')
                    return GetString(s, n, i);
                if (c == '{')
                {
                    var d = new Document();
                    i = d.Fields(s, i, n);
                    return new ObjPos(d,i);
                }
                if (c == '[')
                {
                    var d = new DocArray();
                    i = d.Items(s, i, n);
                    return new ObjPos(d,i);
                }
                if (i + 4 < n && s.substring(i, i+4) == "true")
                {
                    i += 4;
                    return new ObjPos(true,i);
                }
                if (i + 5 < n && s.substring(i, i+5) == "false")
                {
                    i += 5;
                    return new ObjPos(false,i);
                }
                if (i + 4 < n && s.substring(i, i+4) == "null")
                {
                    i += 4;
                    return new ObjPos(null,i);
                }
                var sg = c == '-';
                if (sg && i < n)
                    c = s.charAt(i++);
                var whole = Bigint.Zero;
                var Ten = new Bigint(10);
                if (Character.isDigit(c))
                {
                    i--;
                    whole = new Bigint(GetHex(s, n, i++));
                    while (i < n && Character.isDigit(s.charAt(i)))
                        whole = whole.Times(Ten).Plus(new Bigint(GetHex(s, n, i++)));
                }
                else
                    throw new Error("bad format");
                c = s.charAt(i);
                if (i >= n || (c != '.' && c != 'e' && c != 'E'))
                    return new ObjPos(sg ? whole.Negate() : whole,i);
                int scale = 0;
                if (c == '.')
                {
                    if (++i >= n || !Character.isDigit(s.charAt(i)))
                        throw new Error("decimal part expected");
                    while (i < n && Character.isDigit(s.charAt(i)))
                    {
                        whole = whole.Times(Ten).Plus(new Bigint(GetHex(s, n, i++)));
                        scale++;
                    }
                }
                c = s.charAt(i);
                if (i >= n || (c != 'e' && c != 'E'))
                {
                    var m = new Numeric(whole,scale);
                    return new ObjPos(sg ? m.Negate() : m,i);
                }
                if (++i >= n)
                    throw new Error("exponent part expected");
                c = s.charAt(i);
                var esg = c == '-';
                if ((c == '-' || c == '+') && (++i >= n || !Character.isDigit(s.charAt(i))))
                    throw new Error("exponent part expected");
                var exp = 0;
                while (i < n && Character.isDigit(s.charAt(i)))
                    exp = exp * 10 + GetHex(s, n, i++);
                if (esg)
                    exp = -exp;
                var dr = new Numeric(whole.toDouble() * Math.pow(10.0, exp - scale));
                return new ObjPos(sg ? dr.Negate() : dr,i);
            }
            throw new Error("Value expected at " + (i - 1));
        }
        /// <summary>
        /// This routine is only used for retrieving an embedded string.
        /// PRE: s[i-1] is ' or " and a matching quote occurs at or before s[n-1].
        /// </summary>
        /// <param name="s"></param>
        /// <param name="n"></param>
        /// <param name="i"></param>
        /// <returns></returns>
        protected ObjPos GetString(String s, int n, int i)
        {
            var sb = new StringBuilder();
            var quote = s.charAt(i - 1);
            while (i < n)
            {
                var c = s.charAt(i++);
                if (c == quote)
                {
                    // Two adjacent quotes are replaced by one.
                    if (i < n && s.charAt(i) == c)
                        i++;
                    else
                        return new ObjPos(sb.toString(),i);
                }
                sb.append(c);
            }
            throw new Error("Non-terminated string at " + (i - 1));
        }
        static int GetHex(String s, int n, int i)
        {
            if (i < n)
            {
                switch (s.charAt(i))
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
            throw new Error("Hex digit expected");
        }
    
}
