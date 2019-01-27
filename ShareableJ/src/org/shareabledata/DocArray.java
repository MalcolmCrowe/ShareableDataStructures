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
public class DocArray extends DocBase
    {
        public SList<Document> items = null;
        public DocArray() { }
        public DocArray(String s)
        {
            if (s == null) 
                return;
            s = s.trim();
            int n = s.length();
            if (n<2 || s.charAt(0)!='[' || s.charAt(n-1)!=']')
                throw new Error("[..] expected");
            int i = Items(s, 1, n);
            if (i != n)
                throw new Error("bad DocArray format");
        }
        private enum ParseState { StartValue, Comma }
        int Items(String s,int i,int n)
        {
            var state = ParseState.StartValue;
            while (i < n)
            {
                var c = s.charAt(i++);
                if (Character.isWhitespace(c))
                    continue;
                if (c == ']' && items==null)
                    break;
                switch (state)
                {
                    case StartValue:
                        var op = GetValue(s, n, i);
                        if (op.ob!=null)
                            items=(items==null)?new SList(op.ob):
                                    items.InsertAt((Document)op.ob,items.Length);
                        i = op.pos;
                        state = ParseState.Comma;
                        continue;
                    case Comma:
                        if (c == ']')
                            return i;
                        if (c != ',')
                            throw new Error(", expected");
                        state = ParseState.StartValue;
                        continue;
                }
            }
            return i;
        }
        public int getLength() { return (items==null)?0:items.Length; }
        public boolean IsEmpty() { return items==null; }
    
}
