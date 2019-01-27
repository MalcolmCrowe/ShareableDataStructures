/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareabledata;
import java.io.*;
import java.util.Locale;
/**
 *
 * @author Malcolm
 */
public class StrongCmd {
    static class Column
    {
        public String name;
        public int width = 0;
        public Column(String n, int w)
        {
            name = n;
            width = w;
        }
    }
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static boolean silent = false;
        static boolean checks = false;
        public static void main(String[] args)
        {
            String str;
            String host = "127.0.0.1";
            String port = "50433";
            String line = "";
            Exception lasterr = null;
            boolean interactive = true;
            BufferedReader file = null;
            BufferedReader console = null;
            boolean newfile = false;
            String files = "Temp";
            int k = 0;
            Locale locale = Locale.getDefault();
            console = new BufferedReader(new InputStreamReader(System.in));
            while (args.length > k)
            {
                if (args[k].startsWith("-"))
                    switch (args[k].charAt(1))
                    {
                        case 'c': locale = new Locale(args[k].substring(3),
                        args[k].substring(3)); break;
                        case 'h': host = args[k].substring(3); break;
                        case 'p': port = args[k].substring(3); break;
                        case 'e': line = args[k].substring(3); 
                        interactive = false; break;
                        case 'f':
                            try
                            {
                                file = new BufferedReader(
                                        new FileReader(args[k].substring(3)));
                                newfile = true;
                            }
                            catch (Exception e)
                            {
                                System.out.println(e.getMessage());
                                return;
                            }
                            break;
                    }
                else
                    files = args[k];
                k++;
            }
            try
            {
                StrongConnect db = new StrongConnect(host, Integer.parseInt(port), 
                        files);
                for (boolean done = false; !done; done = !interactive)
                {
                    if (interactive)
                    {
                        if (file != null)
                        {
                            str = file.readLine();
                            if (newfile)
                                newfile = false;
                        }
                        else
                        {
                            if (db.inTransaction)
                                System.out.print("SQL-T>");
                            else
                                System.out.print("SQL> ");
                            str = console.readLine();
                        }
                        if (str == null)
                            return;
                        str = str.trim();
                        if (str.length() == 0)
                            continue;
                        // support QUIT
                        if (str.length() >= 4 && str.toUpperCase().
                                startsWith("QUIT"))
                            break;
                        // support comments
                        if (str.charAt(0) == '/')
                            continue;
                        // support file input
                        if (str.charAt(0) == '@')
                        {
                            try
                            {
                                file = new BufferedReader(
                                        new FileReader(str.substring(1).trim()));
                                newfile = true;
                            }
                            catch (Exception ex)
                            {
                                System.out.println(ex.getMessage());
                            }
                            continue;
                        }
                        // support multiline SQL statements for people who don't like wraparound
                        if (str.charAt(0) == '[')
                        {
                            line = "";
                            for (; ; )
                            {
                                if (str.charAt(str.length() - 1) == ']')
                                    break;
                                if (file != null)
                                    line = file.readLine();
                                else
                                {
                                    System.out.print("> ");
                                    line = console.readLine();
                                }
                                line = RemoveTrailingComment(line);
                                if (line == null)
                                {
                                    str += "]";
                                    break;
                                }
                                if (line.length() > 0)
                                {
                                    line = line.trim();
                                    if (str.charAt(str.length() - 1) == '\'' 
                                            && line.charAt(0) == '\'')
                                        str = str.substring(0, str.length() - 1)
                                                + line.substring(1);
                                    else
                                        str += " " + line;
                                }
                            }
                            str = str.substring(1, str.length() - 2);
                        }
                    }
                    else
                        str = line;
                    try
                    {
                        var strlow = Trim(str.trim(),';').toLowerCase();
                        switch (strlow)
                        {
                            case "begin":
                                if (db.inTransaction)
                                {
                                    System.out.println("Transaction already started");
                                    continue;
                                }
                                db.BeginTransaction();
                                continue;
                            case "rollback":
                                if (!db.inTransaction)
                                {
                                    System.out.println("No current transaction");
                                    continue;
                                }
                                db.Rollback();
                                continue;
                            case "commit":
                                if (!db.inTransaction)
                                {
                                    System.out.println("No current transaction");
                                    continue;
                                }
                                db.Commit();
                                continue;
                        }
                        if (str.startsWith("select"))
                            Show(db, db.ExecuteQuery(str));
                        else
                            db.ExecuteNonQuery(str);
                    }
                    catch (Exception e)
                    {
                        System.out.println(e.getMessage());
                        if (!interactive)
                            break;
                        if (file!=null)
                        {
                            file.close();
                            file = null;
                        }
                    }
                }
                if (file!=null)
                {
                    file.close();
                    file = null;
                }
            } catch(Exception e)
            {
                System.out.println(e.getMessage());
            }
        }
        /// <summary>
        /// remove comment starting with unquoted -- and extending to end of line
        /// </summary>
        /// <param name="line"></param>
        static String RemoveTrailingComment(String line)
        {
            var quote = '\0';
            var dash = false; // we have a saved dash
            var sb = new StringBuilder();
            for (char ch : line.toCharArray())
                switch (ch)
                {
                    case '"':
                    case '\'':
                        if (ch == quote) // matching quote cancels
                            quote = '\0';
                        else if (ch == '\0')
                            quote = ch; // else is a quoted quote
                        if (dash) // copy the saved dash if any
                            sb.append('-');
                        dash = false;
                        sb.append(ch); // copy the quote
                        break;
                    case '-':
                        if (quote != '\0') // quoted - is not special
                        {
                            if (dash) // copy the saved dash if any
                                sb.append('-');
                            dash = false;
                            sb.append(ch);
                            break;
                        }
                        if (dash) // unquoted --
                            return sb.toString(); // truncate the line
                        dash = true;
                        break;
                    default:
                        if (dash) // copy the saved dash if any
                            sb.append('-');
                        dash = false;
                        sb.append(ch);
                        break;
                }
            return sb.toString();
        }
        static void Show(StrongConnect c, DocArray da) throws Exception
        {
            SDict<Integer,Column> cols = null; 
            SDict<String, Integer> names = null;
            SDict<Integer, SDict<String, String>> rows = null;
            var n = 0;
            for (var b = c.description.First(); b != null; b = b.Next(),n++)
            {
                var k = b.getValue().key;
                var v = b.getValue().val;
                names = (names==null)?new SDict(v,k):names.Add(v,k);
                var col = new Column(v,v.length());
                cols = (cols==null)?new SDict(0,col):cols.Add(n,col);
            }
            n = 0;
            for (var b=da.items.First();b!=null;b=b.Next(),n++)
            {
                var dc = b.getValue();
                SDict<String,String> row = null;
                for (var d = dc.fields.First(); d!=null; d=d.Next())
                {
                    var f = d.getValue();
                    if (f.key.startsWith("_"))
                        continue;
                    if (!names.Contains(f.key))
                        throw new Exception("Unexpected column " + f.key);
                    var k = names.Lookup(f.key);
                    var col = cols.Lookup(k);
                    var s = f.val.toString();
                    if (s.length() > col.width)
                        cols=cols.Add(k,new Column(col.name,s.length()));
                    row = (row==null)?new SDict(f.key,s):row.Add(f.key, s);
                }
                rows = (rows==null)?new SDict(0,row):rows.Add(n, row);
            }
            DrawLine(cols);
            ShowHeads(cols);
            DrawLine(cols);
            for (var r=rows.First();r!=null;r=r.Next())
                ShowRow(r.getValue().val, cols);
            DrawLine(cols);
        }
        static void DrawLine(SDict<Integer,Column> a)
        {
            for (var b = a.First();b!=null;b=b.Next())
            {
                System.out.print("|");
                var c = b.getValue().val;
                for (int k = 0; k < c.width; k++)
                    System.out.print("-");
            }
            System.out.println("|");
        }
        static void ShowHeads(SDict<Integer,Column> a)
        {
            for (var b = a.First();b!=null;b=b.Next())
            {
                System.out.print("|");
                var c = b.getValue().val;
                System.out.print(c.name);
                for (int k = 0; k < c.width - c.name.length(); k++)
                    System.out.print(" ");
            }
            System.out.println("|");
        }
        static void ShowRow(SDict<String, String> r, SDict<Integer,Column> cols)
        {
            for (var b = cols.First();b!=null;b=b.Next())
            {
                System.out.print("|");
                var c = b.getValue().val;
                var v = r.Lookup(c.name);
                if (v==null)
                    v = "";
                System.out.print(v);
                for (int k = 0; k < c.width - v.length(); k++)
                    System.out.print(" ");
            }
            System.out.println("|");
        }
        static String Trim(String s,char c)
        {
            int lf,rg;
            for (lf=0;lf<s.length();lf++)
                if (s.charAt(lf)!=c)
                    break;
            if (lf==s.length())
                return "";
            for (rg=s.length()-1;rg>lf;rg--)
                if (s.charAt(rg)!=c)
                    break;
            return s.substring(lf,rg+1);
        }
    }
