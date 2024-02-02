using System.Text;
using System.Net;
using System.Collections;
using System.Globalization;
using Pyrrho;
using System.ComponentModel.Design;

namespace PyrrhoCmd
{
	class Column
	{
		public string name;
		public Type type;
		public int width = 0;
		public Column(string n,Type t,int w)
		{
			name = n;
			type = t;
			width = w;
		}
	}
	class Link
	{
		public StreamReader head;
		public Link? tail;
		public Link(StreamReader h,Link? t)
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
        static bool allowask = false;
        static bool interactive = true;
        static bool caseSensitive = false;
        static PyrrhoTransaction? transaction = null;
        static PyrrhoConnect? db = null;
        static DatabaseError? lasterr = null;
        static int fileLines = -1;
        static string files = ""; 
        static int nrecs = 0;
        static StreamReader? file = null; // command file from -f flag
        [STAThread]
        static void Main(string[] args)
        {
            string host = "::1";
            string port = "5433";
            string line = "";
            Console.InputEncoding = Encoding.Unicode;
            Console.OutputEncoding = Encoding.Unicode;
            int k = 0;
            while (args.Length > k)
            {
                if (args[k] == "+a")
                    allowask = true;
                else if (args[k].StartsWith("-"))
                    switch (args[k][1])
                    {
#if (!NETCF)
                        case 'c': Thread.CurrentThread.CurrentUICulture = new CultureInfo(args[k].Substring(3)); break;
#endif
#if !EMBEDDED
                        case 'h': host = args[k].Substring(3); break;
                        case 'p': port = args[k].Substring(3); break;
#endif
                        case 's': silent = true; break;
                        case 'U': caseSensitive = true; break;
                        case 'v': checks = true; break;
                        case 'e': line = args[k].Substring(3); interactive = false; break;
                        case 'f':
                            try
                            {
                                file = new StreamReader(args[k].Substring(3));
                                fileLines = 0;
                            }
                            catch (Exception)
                            {
                                Console.WriteLine(Format("0012", args[k].Substring(3)));
                                return;
                            }
                            break;
                        case '?': Usage(); return;
                    }
                else
                    files += ((files.Length > 0) ? "," : "") + args[k];
                k++;
            }
            var cs = "Provider=PyrrhoDBMS;Host=" + host + ";Port=" + port + ";Files=" + files;
            if (Thread.CurrentThread.CurrentUICulture != CultureInfo.InvariantCulture)
                cs += ";Locale=" + Thread.CurrentThread.CurrentUICulture.Name;
            if (allowask)
                cs += ";AllowAsk=true";
            if (caseSensitive)
                cs += ";CaseSensitive=true";
            db = new PyrrhoConnect(cs);
            try
            {
                db.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.ReadLine();
                return;
            }
            Commands(db);
            file?.Close();
        }
        static void Commands(PyrrhoConnect db)
        {
            for (bool done = false; !done; done = !interactive)
            {
                var str = GetCommand(db, null, interactive);
                if (str == null)
                    break;
                str = InsertBlobs(str); // ~file or ~URL is replaced by a binary large object string
                if (str.Length > 0)
                    Obey(str);
                var tr = (transaction != null) ? "0023" : "0020";
                if (nrecs > 0)
                    Console.WriteLine("" + nrecs + Format(tr) + files);
                nrecs = 0;
            }
        }
        static void Obey(string str)
        {
            DatabaseError? lasterr = null;
            if (db is null)
                return;
            var cmd = db.CreateCommand();
            try
            {
                cmd.CommandText = str;
                var str0 = str.Trim().Trim(';');
                str = str0.ToLower();
                if (str.StartsWith("begin") || str.StartsWith("start"))
                    str = str.Substring(5).Trim();
                if (str.StartsWith("set"))
                {
                    var s1 = str0.Substring(3).Trim();
                    if (s1.StartsWith("role"))
                    {
                        var rn = s1.Substring(4).Trim();
                        db.SetRole(rn);
                        return;
                    }
                }
                switch (str)
                {
                    case "transaction": // begin transaction
                        if (transaction != null)
                        {
                            Console.WriteLine(Format("0016"));
                            return;
                        }
                        transaction = db.BeginTransaction();
                        return;
                    case "rollback":
                        if (transaction == null)
                        {
                            Console.WriteLine(Format("0017"));
                            return;
                        }
                        transaction.Rollback();
                        transaction = null;
                        return;
                    case "commit":
                        {
                            if (transaction == null)
                            {
                                Console.WriteLine(Format("0018"));
                                return;
                            }
                            var c = transaction.Commit();
                            nrecs += c;
                            transaction = null;
                            return;
                        }
                    case "show diagnostics":
                        if (lasterr == null)
                            Console.WriteLine(Format("0019"));
                        else
                        {
                            Console.WriteLine("Last error: " + lasterr.Message);
#if MONO1
                                foreach(KeyValuePair s in lasterr.info)
#else
                            foreach (var s in lasterr.info)
#endif
                                Console.WriteLine(s.Key + ": " + s.Value);
                        }
                        return;
                }
                if (str.StartsWith("delete") || str.StartsWith("update") || str.StartsWith("insert")
                    || str.StartsWith("create"))
                {
                    var n = cmd.ExecuteNonQuery();
                    ShowWarnings(db);
                    if (file is null)
                    {
                        if (n < 0) // For cascade actions we don't get #affected rows
                            Console.WriteLine(Format("0019")); // OK
                        else
                        nrecs += n;
                    }
                }
                else if (str.StartsWith("match"))
                {
                    (var n, var rdr) = cmd.ExecuteMatch();
                    if (rdr is null)
                    {
                        ShowWarnings(db);
                        if (file is null)
                        {
                            if (n < 0) // For cascade actions we don't get #affected rows
                                Console.WriteLine(Format("0019")); // OK
                            else
                                nrecs += n;
                        }
                        else if (n == 0)
                            Console.WriteLine("Fault at line " + fileLines);
                    }
                    else
                        try
                        {
                            ShowWarnings(db);
                            Show(rdr);
                        }
                        catch (Exception e)
                        {
                            rdr.Close();
                            throw e;
                        }
                    if (rdr != null && !rdr.IsClosed)
                        rdr.Close();
                    return;
                }
                else
                {
                    var rdr = cmd.ExecuteReader();
                    if (rdr != null)
                        try
                        {
                            ShowWarnings(db);
                            Show(rdr);
                        }
                        catch (Exception e)
                        {
                            rdr.Close();
                            throw e;
                        }
                    if (rdr != null && !rdr.IsClosed)
                        rdr.Close();
                }
            }
            catch (TransactionConflict e)
            {
                lasterr = e;
                while (Console.KeyAvailable)
                    Console.ReadKey(true);
                Console.WriteLine(e.Message);
                transaction = null;
                if (!interactive)
                    return;
                file?.Close();
                file = null;
            }
            catch (DatabaseError e)
            {
                lasterr = e;
                Console.WriteLine(e.Message);
                if (transaction != null)
                {
                    Console.WriteLine("The transaction has been rolled back");
                    transaction = null;
                }
                file?.Close();
                file = null;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                if (!interactive)
                    return;
                file?.Close();
                file = null;
            }
        }
    static string? GetCommand(PyrrhoConnect db, StreamReader? file, bool interactive)
        {
            Console.InputEncoding = Encoding.Unicode;
            Console.OutputEncoding = Encoding.Unicode;
            bool newfile = false;
            int fileLines = -1;
            string? str;
            string? line = "";
            if (interactive)
            {
                if (file != null)
                {
                    str = file.ReadLine();
                    if (newfile)
                    {
                        if (file.CurrentEncoding == Encoding.Default)
                            Console.WriteLine(Format("0013"));
                    }
                    if (++fileLines % 1000 == 0)
                    {
                        Console.Write(fileLines); Console.WriteLine(Format("0024"));
                    }
                }
                else
                {
                    if (db.State != ConnectionState.Open)
                    {
                        Console.WriteLine(Format("0015"));
                        return null;
                    }
                    if (transaction != null)
                        Console.Write("SQL-T>");
                    else
                        Console.Write("SQL> ");
                    str = Console.ReadLine();
                }
                if (str == null)
                    return null;
                str = str.Trim();
                if (str.Length == 0)
                    return str;
                // support QUIT
                if (str.Length >= 4 && str.ToUpper().StartsWith("QUIT"))
                    return null;
                // support file input
                if (str[0] == '@')
                {
                    try
                    {
                        file = new StreamReader(str.Substring(1).Trim());
                        newfile = true;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    return null;
                }
                // support multiline SQL statements for people who don't like wraparound
                if (str[0] == '[')
                {
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
                else
                    RemoveTrailingComment(ref line);
            }
            else
                str = line;
            return str;
        }
            /// <summary>
            /// remove comment starting with unquoted -- and extending to end of line
            /// </summary>
            /// <param name="line"></param>
            static void RemoveTrailingComment(ref string line)
        {
            var quote = '\0';
            var dash = false; // we have a saved dash
            var slash = false; // we have a saved slash
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
                    case '/':
                        if (quote != '\0') // quoted - is not special
                            goto default;
                        if (slash) // unquoted //
                        {
                            line = sb.ToString(); // truncate the line
                            return;
                        }
                        slash = true;
                        break;
                    default:
                        if (dash) // copy the saved dash or slash if any
                            sb.Append('-');
                        if (slash)
                            sb.Append('/');
                        dash = false;
                        slash = false;
                        sb.Append(ch);
                        break;
                }
            if (dash)
                sb.Append('-');
            if (slash)
                sb.Append('/');
            line = sb.ToString();
        } 
        static void ShowWarnings(PyrrhoConnect db)
        {
            var ww = db.Warnings;
            if (ww.Length > 0)
                Console.Write("Warning:");
            foreach (var w in ww)
                Console.WriteLine("  " + w.ToString());
        }
		static string InsertBlobs(string str)
		{
            var it = str.IndexOf("~");
			if (it<0)
				return str;
            var ts = InsertText(str, it);
            if (ts != null)
                return ts;
			StringBuilder r = new StringBuilder();
			char quote = '\x0'; // will not match any char in str
			int j = 0;
			while(j<str.Length)
			{
				char c = str[j++];
				if (quote==c)
					quote = '\x0';
				else if (quote=='\x0')
				{
					if (c=='\'' || c=='"')
						quote = c;
					else if (c=='~')
					{
						StringBuilder s = new StringBuilder();
						char d = str[j++];
						if (d=='\''||d=='"')
							quote = d;
						else
							s.Append(d);
						while (j<str.Length)
						{
							d = str[j++];
							if (d==quote)
							{
								quote = '\x0';
								break;
							}
							else if (quote=='\x0' && (d==' '||d==','||d==')'))
							{
								j--;
								break;
							}
							s.Append(d);
						}
						r.Append(InsertBlob(s.ToString()));
						c = str[j++];
					}
				}
				r.Append(c);
			}
			return r.ToString();
		}
		static string InsertBlob(string source)
		{
			Stream? str = null;
			try 
			{
				if (source.StartsWith("http://"))
				{
					WebRequest wrq = WebRequest.Create(source);
					wrq.Timeout = 10000;
					WebResponse wrs = wrq.GetResponse();
					str = wrs.GetResponseStream();
				} 
				else
					str = new FileStream(source,FileMode.Open);
			} 
			catch (Exception e)
			{
				throw new Exception(Format("0011",e.Message));
			}
			if ((!silent) && blobwarning++==0)
				Console.WriteLine(Format("0001",source));
			StringBuilder r = new StringBuilder();
			r.Append("X'");
			string f = "";
			int n=0;
			for (;;)
			{
				int x = str.ReadByte();
				if (x<0)
					break;
				f = x.ToString("x2");
				r.Append(x.ToString("x2"));
				n++;
			}
			r.Append("'");
			str.Close();
			return r.ToString();
		}
		static int blobwarning;
        /// <summary>
        /// Handle input text files such as csv, only if the ~ was preceded by keyword VALUES
        /// </summary>
        /// <param name="str"></param>
        /// <param name="it">position of ~</param>
        /// <returns></returns>
        static string InsertText(string str, int it)
        {
            nrecs = 0;
            var iv = it - 1;
            while (iv > 7 && char.IsWhiteSpace(str[iv]))
                iv--;
            if (str.Substring(iv - 5, 6).ToLower() != "values")
                return "";
            var ef = it + 1;
            while (ef < str.Length && !char.IsWhiteSpace(str[ef]))
                ef++;
            try
            {
                var r = new StringBuilder(str.Substring(0, it - 1));
                var st = new StreamReader(str.Substring(it + 1, ef - it - 1));
                st.ReadLine();
                var cr = r;
                while (st.ReadLine() is string rs)
                {
                    r = new StringBuilder();
                    r.Append(cr);
                    r.Append('(');
                    var ss = rs.Split('|');
                    var cm = "'";
                    for (var i = 0; i < ss.Length; i++)
                    {
                        r.Append(cm); cm = ",'";
                        r.Append(ss[i].Replace("'", "''"));
                        r.Append("'");
                    }
                    r.Append(")");
                    Obey(r.ToString());
                }
                st.Close();
                return "";
            }
            catch (DatabaseError e)
            {
                lasterr = e;
                Console.WriteLine(e.Message);
                if (transaction != null)
                {
                    Console.WriteLine("The transaction has been rolled back");
                    transaction = null;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                if (!interactive)
                    return "";
                transaction = null;
            }
            return "";
        }
        static void Show(PyrrhoReader rdr)
		{
			ArrayList cols = new ArrayList(); // of Column
			ArrayList rows = new ArrayList(); // of string[]
            ArrayList checks = new ArrayList(); // of PyrrhoCheck
			int j;
			for (j=0;j<rdr.FieldCount;j++)
				cols.Add(new Column(rdr.GetName(j),rdr.GetFieldType(j),rdr.GetName(j).Length));
            while (rdr.Read())
            {
                string[] row = new string[cols.Count];
                for (j = 0; j < rdr.FieldCount; j++)
                {
                    Column c = (Column)cols[j];
                    if (c.type == typeof(byte[])) // blob
                    {
                        var ob = rdr[j];
                        if (ob == DBNull.Value)
                            row[j] = "";
                        else
                        {
                            byte[] b = (byte[])ob;
                            int n = 0;
                            while (File.Exists("Blob" + n))
                                n++;
                            string fname = "Blob" + n;
                            FileStream f = new FileStream(fname, FileMode.Create);
                            f.Write(b, 0, b.Length);
                            f.Close();
                            row[j] = "@" + fname;
                        }
                        if ((!silent) && blobwarning++ == 0)
                            Console.WriteLine(Format("0002"));
                    }
                    else
                    {
                        object o = rdr[j];
                        row[j] = o.ToString();
                    }
                    if (row[j].Length > c.width)
                        c.width = row[j].Length;
                }
                rows.Add(row);
                checks.Add(rdr.version);
            }
            if (rdr.IsClosed)
                throw new Exception(Format("0015"));
			rdr.Close();
			DrawLine(cols);
			ShowHeads(cols);
			DrawLine(cols);
			for (j=0;j<rows.Count;j++)
				ShowRow((string[])rows[j],cols,(string)checks[j]);
			DrawLine(cols);
		}
		static void DrawLine(ArrayList a)
		{
			for (int j=0;j<a.Count;j++)
			{
				Console.Write("|");
				Column c = (Column)a[j];
				for (int k=0;k<c.width;k++)
					Console.Write("-");
			}
			Console.WriteLine("|");
		}
		static void ShowHeads(ArrayList a)
		{
			for (int j=0;j<a.Count;j++)
			{
				Console.Write("|");
				Column c = (Column)a[j];
				Console.Write(c.name);
				for (int k=0;k<c.width-c.name.Length;k++)
					Console.Write(" ");
			}
			Console.WriteLine("|");
		}
		static void ShowRow(string[] r, ArrayList a, string v)
		{
			for (int j=0;j<a.Count;j++)
			{
				Console.Write("|");
				Column c = (Column)a[j];
				Console.Write(r[j]);
				for (int k=0;k<c.width-r[j].Length;k++)
					Console.Write(" ");
			}
			Console.Write("|");
            if (checks && v != null)
                Console.Write(v);
            Console.WriteLine();
		}
		static Hashtable dict = null;
		static void GetSatellite(string rname,string cu)
		{
			try
			{
				string fname = rname+".lox";
				if (cu!="")
					fname = rname+"."+cu+".lox";
				StreamReader stream = new StreamReader(fname,Encoding.Unicode);
				for (;;)
				{
					string line = stream.ReadLine();
					if (line==null)
						break;
					if (dict == null)
						dict = new Hashtable();
					int k = line.IndexOf('=');
					if (k>0)
						dict.Add(line.Substring(0,k),line.Substring(k+1));
				}
			}
			catch (Exception)
			{
			}
		}
		static void Init()
		{
			string asm ="PyrrhoCmd";
			string cu = CultureInfo.CurrentUICulture.Name;
			GetSatellite(asm,cu);
			if (cu.IndexOf("zh")>=0)
			{
				if (dict==null) GetSatellite(asm,"zh-CHS");
				if (dict==null) GetSatellite(asm,"zh-CHT");
			}
			else
				if (dict==null) GetSatellite(asm,cu.Substring(0,2));
			if (dict==null)
			{
				dict = new Hashtable();
				dict.Add("0001","Note: the contents of {0}} are being copied as a blob to the server");
				dict.Add("0002","Note: blob(s) from database copied to file(s)");
				dict.Add("0003","Pyrrho Command Line Utility");
				dict.Add("0004","Usage: [-h:host] [-p:port] [-s] [-e:command|-f:file] [-c:locale] [-v] database ...");
				dict.Add("0005","  -h  Pyrrho server host name. Default is localhost");
				dict.Add("0006","  -p  Pyrrho server port name. Default is 5433");
				dict.Add("0007","  -e  Non-interactive mode: use given single command");
				dict.Add("0008","  -f  Non-interactive mode: get commands from file");
				dict.Add("0009","  -c  Use this locale (e.g. -c:fr) overriding the .NET default");
				dict.Add("0010","  -s  Silent mode: do not warn about blob uploads and downloads");
				dict.Add("0011","Blob upload error: {0}");
				dict.Add("0012","Could not read from file {0}");
				dict.Add("0013","Warning: this file is in ANSI. Other encodings are better for globalisation");
                dict.Add("0014", "Warning: input text file has bad format at row {0}");
                dict.Add("0015", "Connection closed");
                dict.Add("0016", "There is already a transaction in progress");
                dict.Add("0017", "There is no current transaction");
                dict.Add("0018", "There is no current transaction");
                dict.Add("0019", "OK");
                dict.Add("0020", " records affected in ");
                dict.Add("0021", "Usage: [-s] [-e:command|-f:file] [-c:locale] database ...");
                dict.Add("0022", "  -v  Show version and readCheck information for each row of data");
                dict.Add("0023", " records in transaction ");
                dict.Add("0024", " lines processed");
            }
		}
		public static string Format(string sig,params string[] obs)
		{
			try
			{
				if (dict==null)
					Init();
                string fmt = "Unknown signal " + sig;
                if (dict.Contains(sig))
                    fmt = (string) dict[sig];
				StringBuilder sb = new StringBuilder();
				int j=0;
				while(j<fmt.Length)
				{
					char c = fmt[j++];
					if (c!='{')
						sb.Append(c);
					else 
					{
						int k = fmt[j++]-'0';
						j++; // }
						if (k<obs.Length)
							sb.Append(obs[k]);
					}
				}
				return sb.ToString();
			}
			catch(Exception ex)
			{
				return ex.Message;
			}
		}
		static void Usage()
		{
			Console.WriteLine(Format("0003"));
#if EMBEDDED
			Console.WriteLine(Format("0021"));
#else
            Console.WriteLine(Format("0004"));
			Console.WriteLine(Format("0005"));
			Console.WriteLine(Format("0006"));
#endif
			Console.WriteLine(Format("0007"));
			Console.WriteLine(Format("0008"));
			Console.WriteLine(Format("0009"));
			Console.WriteLine(Format("0010"));
            Console.WriteLine(Format("0022"));
		}
	}
}
