/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.pyrrhodb;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
/**
 *
 * @author 66668214
 */
public class DocBase {
    String str;
    int len;
    int pos;
    enum ParseState { StartKey,Key,Colon,StartValue,Comma}
    DocBase(String s,int n,int i)
    {
        str = s;
        len = n;
        pos = i;
    }
    Object getValue() throws DocumentException
    {
        if (pos<len)
        {
        char c = str.charAt(pos-1);
        if (c=='"'||c=='\'')
            return getString();
        }
        throw new DocumentException("value expected",pos);
    }
    private String getString() throws DocumentException
    {
        int start = pos;
        StringBuilder sb = new StringBuilder();
        char quote = str.charAt(pos-1);
        while (pos<len)
        {
            char c = str.charAt(pos++);
            if (c==quote)
                return sb.toString();
            if (c == '\\')
                c = getEscape();
            sb.append(c);
        }
        throw new DocumentException("Non-terminated string",start);
    }
    ArrayList<SimpleEntry<String,Object>> getFields() throws DocumentException
    {
        ParseState state = ParseState.StartKey;
        StringBuilder kb = null;
        boolean keyQuote = true;
        ArrayList<SimpleEntry<String,Object>> r = new ArrayList<SimpleEntry<String,Object>>();
        if (pos>=len || str.charAt(pos++)!='{')
            throw new DocumentException("{ expected",pos-1);
        while (pos<len-1)
        {
            char c = str.charAt(pos++);
            switch (state)
            {
                case StartKey:
                    kb = new StringBuilder();
                    keyQuote = true;
                    if (Character.isWhitespace(c))
                        continue;
                    if (c=='}' && r.size()==0)
                        return r;
                    if (c!='"')
                    {
                        if ((!Character.isLetter(c)) && c!='_' &&c!='$'&&c!='.')
                            throw new DocumentException("Expected name",pos);
                        keyQuote = false;
                        kb.append(c);
                    }
                    state = ParseState.Key;
                    continue;
                case Key:
                    if (c=='"')
                    {
                        state = ParseState.Colon;
                        continue;
                    }
                    if (c=='\\')
                        c = getEscape();
                    if (c!=':')
                    {
                        kb.append(c);
                        continue;
                    }
                    if (keyQuote)
                        throw new DocumentException("Missing quote",pos);
                    // else fall into
                case Colon:
                    if (Character.isWhitespace(c))
                        continue;
                    if (c!=':')
                        throw new DocumentException("Expected :",pos);
                    state = ParseState.StartValue;
                    continue;
                case StartValue:
                    if (Character.isWhitespace(c))
                        continue;  
                    r.add(new SimpleEntry<String,Object>(kb.toString(),getValue()));
                    state = ParseState.Comma;
                    continue;
                case Comma:
                    if (Character.isWhitespace(c))
                        continue;
                    if (c=='}')
                        return r;
                    if (c!=',')
                        throw new DocumentException("Expected ,",pos);
                    state = ParseState.StartKey;
                    continue;
            }
        }
        throw new DocumentException("incomplete syntax",pos-1);
    }
    ArrayList<Object> getItems() throws DocumentException
    {
        ArrayList<Object> r = new ArrayList<Object>();
        ParseState state = ParseState.StartValue;
        while (pos < len)
        {
            char c = str.charAt(pos++);
            if (Character.isWhitespace(c))
                continue;
            if (c == ']' && r.isEmpty())
                break;
            switch (state)
            {
                case StartValue:
                    r.add(getValue());
                    state = ParseState.Comma;
                    continue;
                case Comma:
                    if (c == ']')
                        return r;
                    if (c != ',')
                        throw new DocumentException(", expected",pos);
                    state = ParseState.StartValue;
            }
        }  
        return r;
    }
    private char getEscape() throws DocumentException
    {
        int start = pos;
        if (pos<len)
        {
            char c = str.charAt(pos++);
            switch(c)
            {
                    case '"': return c;
                    case '\\': return c;
                    case '/': return c;
                    case 'b': return '\b';
                    case 'f': return '\f';
                    case 'n': return '\n';
                    case 'r': return '\r';
                    case 't': return '\t';
                    case 'U':
                    case 'u':
                        {
                            int v = 0;
                            for (int j = 0; j < 4; j++)
                                v = (v << 4) + getHex();
                            return (char)v;
                        }               
            }
        }
        throw new DocumentException("illegal escape",start);
    }
    private int getHex() throws DocumentException
    {
        if (pos < len)
        {
            switch (str.charAt(pos++))
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
        throw new DocumentException("Hex digit expected at ",pos - 1);
    }
}
