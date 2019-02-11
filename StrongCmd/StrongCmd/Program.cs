using System;
using System.Globalization;
using System.Threading;
using System.IO;
using System.Collections.Generic;
using System.Text;
using Shareable;
using StrongLink;

namespace StrongCmd
{
    class Column
    {
        public string name;
        public int width = 0;
        public Column(string n, int w)
        {
            name = n;
            width = w;
        }
    }
    class Link
    {
        public StreamReader head;
        public Link tail;
        public Link(StreamReader h, Link t)
        {
            head = h; tail = t;
        }
    }
    /// <summary>
    /// Summary description for Cons.
    /// </summary>
    class Cons
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static bool silent = false;
        static bool checks = false;
        static void Main(string[] args)
        {
            string str = "Temp";
            string host = "127.0.0.1";
            string port = "50433";
            string line = "";
            Exception lasterr = null;
            bool interactive = true;
            Link stack = null;
            StreamReader file = null;
            bool newfile = false;
            string files = "";
            int k = 0;
            while (args.Length > k)
            {
                if (args[k].StartsWith("-"))
                    switch (args[k][1])
                    {
#if (!NETCF)
                        case 'c': Thread.CurrentThread.CurrentUICulture = new CultureInfo(args[k].Substring(3)); break;
#endif
#if !EMBEDDED
                        case 'h': host = args[k].Substring(3); break;
                        case 'p': port = args[k].Substring(3); break;
#endif
                        case 'e': line = args[k].Substring(3); interactive = false; break;
                        case 'f':
                            try
                            {
                                file = new StreamReader(args[k].Substring(3));
                                newfile = true;
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e.Message);
                                return;
                            }
                            break;
                    }
                else
                    files += ((files.Length > 0) ? "," : "") + args[k];
                k++;
            }
            try
            {
                StrongConnect db = new StrongConnect(host, int.Parse(port), files);
                for (bool done = false; !done; done = !interactive)
                {
                    if (interactive)
                    {
                        if (file != null)
                        {
                            str = file.ReadLine();
                            if (newfile)
                                newfile = false;
                        }
                        else
                        {
                            if (db.inTransaction)
                                Console.Write("SQL-T>");
                            else
                                Console.Write("SQL> ");
                            str = Console.ReadLine();
                        }
                        if (str == null)
                            return;
                        str = str.Trim();
                        if (str.Length == 0)
                            continue;
                        // support QUIT
                        if (str.Length >= 4 && str.ToUpper().StartsWith("QUIT"))
                            break;
                        // support comments
                        if (str[0] == '/')
                            continue;
                        // support file input
                        if (str[0] == '@')
                        {
                            if (file != null)
                                stack = new Link(file, stack);
                            try
                            {
                                file = new StreamReader(str.Substring(1).Trim());
                                newfile = true;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message);
                            }
                            continue;
                        }
                        // support multiline SQL statements for people who don't like wraparound
                        if (str[0] == '[')
                        {
                            line = "";
                            for (; ; )
                            {
                                if (str[str.Length - 1] == ']')
                                    break;
                                if (file != null)
                                    line = file.ReadLine();
                                else
                                {
                                    Console.Write("> ");
                                    line = Console.ReadLine();
                                }
                                RemoveTrailingComment(ref line);
                                if (line == null)
                                {
                                    str += "]";
                                    break;
                                }
                                if (line.Length > 0)
                                {
                                    line = line.Trim();
                                    if (str[str.Length - 1] == '\'' && line[0] == '\'')
                                        str = str.Substring(0, str.Length - 1) + line.Substring(1);
                                    else
                                        str += " " + line;
                                }
                            }
                            str = str.Substring(1, str.Length - 2);
                        }
                    }
                    else
                        str = line;
                    try
                    {
                        var strlow = str.Trim().Trim(';').ToLower();
                        switch (strlow)
                        {
                            case "begin":
                                if (db.inTransaction)
                                {
                                    Console.WriteLine("Transaction already started");
                                    continue;
                                }
                                db.BeginTransaction();
                                continue;
                            case "rollback":
                                if (!db.inTransaction)
                                {
                                    Console.WriteLine("No current transaction");
                                    continue;
                                }
                                db.Rollback();
                                continue;
                            case "commit":
                                if (!db.inTransaction)
                                {
                                    Console.WriteLine("No current transaction");
                                    continue;
                                }
                                db.Commit();
                                continue;
                        }
                        if (strlow.StartsWith("select"))
                            Show(db, db.ExecuteQuery(str));
                        else
                            db.ExecuteNonQuery(str);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        if (!interactive)
                            break;
                        file?.Close();
                        file = null;
                    }
                }
                file?.Close();
            } catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        /// <summary>
        /// remove comment starting with unquoted -- and extending to end of line
        /// </summary>
        /// <param name="line"></param>
        static void RemoveTrailingComment(ref string line)
        {
            var quote = '\0';
            var dash = false; // we have a saved dash
            var sb = new StringBuilder();
            foreach (var ch in line)
                switch (ch)
                {
                    case '"':
                    case '\'':
                        if (ch == quote) // matching quote cancels
                            quote = '\0';
                        else if (ch == '\0')
                            quote = ch; // else is a quoted quote
                        if (dash) // copy the saved dash if any
                            sb.Append('-');
                        dash = false;
                        sb.Append(ch); // copy the quote
                        break;
                    case '-':
                        if (quote != '\0') // quoted - is not special
                            goto default;
                        if (dash) // unquoted --
                        {
                            line = sb.ToString(); // truncate the line
                            return;
                        }
                        dash = true;
                        break;
                    default:
                        if (dash) // copy the saved dash if any
                            sb.Append('-');
                        dash = false;
                        sb.Append(ch);
                        break;
                }
            line = sb.ToString();
        }
        static void Show(StrongConnect c, DocArray da)
        {
            List<Column> cols = new List<Column>(); // of Column
            SDict<string, int> names = SDict<string, int>.Empty;
            SDict<int, SDict<string, string>> rows = SDict<int, SDict<string, string>>.Empty; // of string[]
            for (var b = c.description.First(); b != null; b = b.Next())
            {
                names = names + (b.Value.Item2, b.Value.Item1);
                cols.Add(new Column(b.Value.Item2, b.Value.Item2.Length));
            }
            for (int i = 0; i < da.items.Count; i++)
            {
                var dc = da[i];
                var row = SDict<string, string>.Empty;
                for (var j = 0; j < dc.fields.Count; j++)
                {
                    var f = dc.fields[j];
                    if (f.Key.StartsWith("_"))
                        continue;
                    if (!names.Contains(f.Key))
                        throw new Exception("Unexpected column " + f.Key);
                    var k = names.Lookup(f.Key);
                    var s = f.Value.ToString();
                    if (s.Length > cols[k].width)
                        cols[k].width = s.Length;
                    row = row + (f.Key, s);
                }
                rows = rows + (rows.Length.Value, row);
            }
            DrawLine(cols);
            ShowHeads(cols);
            DrawLine(cols);
            for (var j = 0; j < rows.Length.Value; j++)
                ShowRow(rows.Lookup(j), cols);
            DrawLine(cols);
        }
        static void DrawLine(List<Column> a)
        {
            for (int j = 0; j < a.Count; j++)
            {
                Console.Write("|");
                Column c = (Column)a[j];
                for (int k = 0; k < c.width; k++)
                    Console.Write("-");
            }
            Console.WriteLine("|");
        }
        static void ShowHeads(List<Column> a)
        {
            for (int j = 0; j < a.Count; j++)
            {
                Console.Write("|");
                Column c = a[j];
                Console.Write(c.name);
                for (int k = 0; k < c.width - c.name.Length; k++)
                    Console.Write(" ");
            }
            Console.WriteLine("|");
        }
        static void ShowRow(SDict<string, string> r, List<Column> cols)
        {
            for (int j = 0; j < cols.Count; j++)
            {
                Console.Write("|");
                var c = cols[j];
                var v = r.Lookup(c.name)??"";
                Console.Write(v);
                for (int k = 0; k < c.width - v.Length; k++)
                    Console.Write(" ");
            }
            Console.Write("|");
            Console.WriteLine();
        }
    }
}

