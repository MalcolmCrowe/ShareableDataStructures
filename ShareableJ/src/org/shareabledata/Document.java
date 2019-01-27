/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;

import java.nio.charset.StandardCharsets;

/**
 *
 * @author Malcolm
 */
public class Document extends DocBase {
        public SList<SSlot<String,Object>> fields = null;
        public Document()
        { }
        public Document(String s)
        {
            if (s == null)
                return;
            s = s.trim();
            int n = s.length();
            if (n == 0 || s.charAt(0) != '{')
                throw new Error("{ expected");
            var i = Fields(s, 1, n);
            if (i != n)
                throw new Error("unparsed input at " + (i - 1));
        }
        public boolean Contains(String k)
        {
            for (var b=fields.First();b!=null;b=b.Next())
                if (b.getValue().key.compareTo(k)==0)
                    return true;
            return false;
        }
        private enum ParseState { StartKey, Key, Colon, StartValue, Comma }
        /// <summary>
        /// Parse the contents of {} 
        /// </summary>
        /// <param name="s">the string</param>
        /// <param name="i">the start of the fields</param>
        /// <param name="n">the end of the string</param>
        /// <returns>the position just after the }</returns>
        int Fields(String s, int i, int n)
        {
            ParseState state = ParseState.StartKey;
            StringBuilder kb = new StringBuilder(); // zapped below
            var keyquote = true;
            while (i < n)
            {
                var c = s.charAt(i++);
                switch (state)
                {
                    case StartKey:
                        kb = new StringBuilder();
                        keyquote = true;
                        if (Character.isWhitespace(c))
                            continue;
                        if (c == '}' && fields==null)
                            return i;
                        if (c != '"')
                        {
                            if (!Character.isLetter(c) && c!='_' && c!='$' && c!='.')
                                throw new Error("Expected name at " + (i - 1));
                            keyquote = false;
                            kb.append(c);
                        }
                        state = ParseState.Key;
                        continue;
                    case Key:
                        if (c == '"')
                        {
                            state = ParseState.Colon;
                            continue;
                        }
                        if (c == ':' && !keyquote)
                        {
                            state = ParseState.Colon;
                            --i;
                            continue;
                        }
                        kb.append(c);
                        continue;
                    case Colon:
                        if (Character.isWhitespace(c))
                            continue;
                        if (c != ':')
                            throw new Error("Expected : at " + (i - 1));
                        state = ParseState.StartValue;
                        continue;
                    case StartValue:
                        if (Character.isWhitespace(c))
                            continue;
                        var op = GetValue(s, n, i);
                        var ss = new SSlot<String,Object>(kb.toString(), op.ob);
                        fields=(fields==null)?new SList(ss):fields.InsertAt(ss,fields.Length);
                        i = op.pos;
                        state = ParseState.Comma;
                        continue;
                    case Comma:
                        if (Character.isWhitespace(c))
                            continue;
                        if (c == '}')
                            return i;
                        if (c != ',')
                            throw new Error("Expected , at " + (i - 1));
                        state = ParseState.StartKey;
                        continue;
                }
            }
            throw new Error("Incomplete syntax at " + (i - 1));
        } 
        static void Field(Object v, StringBuilder sb)
        {
            if (v == null)
                sb.append("null");
            else if (v instanceof String)
            {
                sb.append('"');
                sb.append(v);
                sb.append('"');
            }
            else if (v instanceof SList)
            {
                var comma = "";
                sb.append("[");
                for (var b=((SList)v).First();b!=null;b=b.Next())
                {
                    sb.append(comma);
                    comma = ",";
                    sb.append(b.getValue());
                }
                sb.append("]");
            }
            else
                sb.append(v); 
        }
        @Override
        public String toString()
        {
            var sb = new StringBuilder("{");
            var comma = "";
            if (fields!=null)
            for (var b=fields.First();b!=null;b=b.Next())
            {
                sb.append(comma); comma = ", ";
                sb.append('"');
                sb.append(b.getValue().key);
                sb.append("\": ");
                Field(b.getValue().val, sb);
            }
            sb.append("}");
            return sb.toString();
        }
}
