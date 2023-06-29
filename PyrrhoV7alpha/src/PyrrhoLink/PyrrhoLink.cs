using System.Globalization;
using System.Reflection;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Security;
using System.Net;
using System.Net.Sockets;
using System.Threading;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
//
// This software is without support and no liability for damage consequential to use
// This code may be used to connect to the Pyrrho Server or in Embedded Pyrrho
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland
namespace Pyrrho
{
    public class PyrrhoConnect
    {
        internal string hostName;
        internal string hostAddress = "::1";
        internal Socket socket;
        internal Crypt crypt;
        public Stream stream;
        public bool isOpen = false;
        public bool isClosed = false;
        Reflection reflection;
        string connectionString;
        internal List<Versioned> posted = new List<Versioned>();
        internal List<DatabaseError> warnings = new List<DatabaseError>();
        Thread? transaction = null; // if both are non-null they will be equal
        Thread? execution = null;
        static int _cid = 0, _req = 0;
        int cid = ++_cid;
        public static StreamWriter? reqs = null;
        static DateTime start = DateTime.Now;
        public void RecordRequest(string sql)
        {
            if (reqs == null)
                return;
            var t = DateTime.Now - start;
            lock (reqs)
                reqs.WriteLine("" + t.TotalMilliseconds + ";" + (++_req) + ";" + cid + ";" + inTransaction + "; " + sql);
        }
        public void RecordRequest(string nm, string[] actuals)
        {
            if (reqs == null)
                return;
            var sb = new StringBuilder(nm);
            foreach (var a in actuals)
            {
                sb.Append(','); sb.Append(a);
            }
            RecordRequest(sb.ToString());
        }
        public void RecordResponse(long ts, long te, Responses r)
        {
            if (reqs == null)
                return;
            var t = DateTime.Now - start;
            lock (reqs)
                reqs.WriteLine("" + t.TotalMilliseconds + ";" + (++_req) + ";" + cid + ";" + inTransaction + ";" + ts + ";" + te + " " + r.ToString());
        }
        public static void OpenRequests()
        {
            if (reqs == null)
                reqs = new StreamWriter("requests.txt");
        }
        public static void CloseRequests()
        {
            if (reqs != null)
                reqs.Close();
            reqs = null;
        }
        public int inTransaction = 0;
        internal void AcquireTransaction()
        {
        retry:
            if (transaction == Thread.CurrentThread)
                throw new DatabaseError("08C03");
            while (transaction != null)
                Thread.Sleep(100);
            lock (this)
            {
                if (transaction != null)
                    goto retry;
                transaction = Thread.CurrentThread;
            }
        }
        internal void ReleaseTransaction()
        {
            transaction = null;
        }
        internal void AcquireExecution()
        {
            if (execution == Thread.CurrentThread)
                //            throw new DatabaseError("25000"); // really should not occur
                return;
            retry:
            while (execution != null)
                Thread.Sleep(100);
            lock (this)
            {
                if (execution != null)
                    goto retry;
                execution = Thread.CurrentThread;
            }
        }
        internal void ReleaseExecution()
        {
            execution = null;
        }
        public void Recovering()
        {
            ReleaseExecution();
            ReleaseTransaction();
        }
        public PyrrhoConnect(string cs)
        {
            connectionString = cs;
        }
        public PyrrhoConnect() { }
        public int Act(string sql, Versioned? ob = null)
        {
            if (!isOpen)
                Open();
            var cmd = CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteNonQuery(ob);
        }
        public int ActTrace(string sql, Versioned? ob = null)
        {
            if (!isOpen)
                Open();
            var cmd = CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteNonQueryTrace(ob);
        }
        public int Execute(string name, params string[] actuals)
        {
            Send(Protocol.Execute);
            PutString(name);
            PutInt(actuals.Length);
            foreach (var s in actuals)
                PutString(s);
            var p = Receive();
            if (p != Responses.Done)
                throw new DatabaseError("2E203");
            return GetInt();
        }
        public int ExecuteTrace(string name, params string[] actuals)
        {
            Send(Protocol.ExecuteTrace);
            PutString(name);
            PutInt(actuals.Length);
            foreach (var s in actuals)
                PutString(s);
            RecordRequest(name, actuals);
            var p = Receive();
            if (p != Responses.DoneTrace)
                throw new DatabaseError("2E203");
            var ts = GetLong();
            RecordResponse(ts, GetLong(), p);
            return GetInt();
        }
        public PyrrhoReader ExecuteReader(string name, params string[] actuals)
        {
            Send(Protocol.Execute);
            PutString(name);
            PutInt(actuals.Length);
            foreach (var s in actuals)
                PutString(s);
            RecordRequest(name, actuals);
            return PyrrhoReader.New(CreateCommand());
        }
        public object ExecuteScalar(string name, params string[] actuals)
        {
            var rdr = ExecuteReader(name, actuals);
            rdr.Read();
            object o = rdr[0];
            rdr.Close();
            return o;
        }
        /// <summary>
        /// Find for a given key, locking is done inside Get<>()
        /// </summary>
        /// <typeparam name="C"></typeparam>
        /// <param name="w"></param>
        /// <returns></returns>
        public C FindOne<C>(params (string,IComparable)[] w) where C : new()
        {
            return FindWith<C>(w)[0];
        }
        public C[] FindWith<C>(params (string,IComparable)[] w) where C : new()
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var i=0;i<w.Length;i++)
            {
                var (x, v) = w[i];
                sb.Append(cm); cm = ",";
                sb.Append("\""); sb.Append(x); sb.Append("\"=");
                if (v is string s && (s.Length<2 || s[0]!='\'')) 
                    v = "'" + v+ "'";
                sb.Append(v);
            }
            return Get<C>(sb.ToString());
        }
        public C[] FindIn<C>(string sel) where C : new()
        {
            var cmd = CreateCommand();
            cmd.CommandText = sel;
            var r = new List<C>();
            var rdr = cmd.ExecuteReader();
            while (rdr.Read())
            {
                var w = new (string,IComparable)[rdr.FieldCount];
                for (var i = 0; i < rdr.FieldCount; i++)
                    w[i] = (rdr.GetName(i),(IComparable)rdr[i]);
                r.Add(FindOne<C>(w));
            }
            rdr.Close();
            return r.ToArray();
        }
        public C[] FindAll<C>() where C : new()
        {
            return Get<C>();
        }
        public PyrrhoColumn[] GetInfo(string tname)
        {
            AcquireExecution();
            try
            {
                Send(Protocol.GetInfo, tname);
                if (Receive() == Responses.Columns)
                {
                    var r = new PyrrhoColumn[GetInt()];
                    for (var i = 0; i < r.Length; i++)
                    {
                        var c = GetString();
                        var t = GetString();
                        var f = GetInt();
                        r[i] = new PyrrhoColumn(c, t, f);
                    }
                    return r;
                }
            }
            finally
            {
                ReleaseExecution();
            }
            return new PyrrhoColumn[0];
        }
        /// <summary>
        /// Utility function for splitting up the connection string
        /// </summary>
        /// <param name="cs">Connection string</param>
        /// <param name="field">Field to search for</param>
        /// <returns>Value of the field</returns>
        internal static string[] GetConnectionValues(string cs, string field)
        {
            string[] split = cs.Split(';');
            for (int j = 0; j < split.Length; j++)
                if (split[j].StartsWith(field + "="))
                    return split[j].Substring(field.Length + 1).Split(',');
            return null;
        }
        internal void PutInt(int n)
        {
            byte[] b = new byte[4];
            b[0] = (byte)(n >> 24);
            b[1] = (byte)(n >> 16);
            b[2] = (byte)(n >> 8);
            b[3] = (byte)n;
            stream.Write(b, 0, 4);
        }
        internal void PutLong(long n)
        {
            byte[] b = new byte[8];
            b[0] = (byte)(n >> 56);
            b[1] = (byte)(n >> 48);
            b[2] = (byte)(n >> 40);
            b[3] = (byte)(n >> 32);
            b[4] = (byte)(n >> 24);
            b[5] = (byte)(n >> 16);
            b[6] = (byte)(n >> 8);
            b[7] = (byte)n;
            stream.Write(b, 0, 8);
        }
        internal int GetInt()
        {
            byte[] bytes = new byte[4];
            stream.Read(bytes, 0, 4);
            int n = 0;
            for (int j = 0; j < 4; j++)
                n = (n << 8) + bytes[j];
            return n;
        }
        internal long GetLong()
        {
            byte[] bytes = new byte[8];
            stream.Read(bytes, 0, 8);
            long n = 0L;
            for (int j = 0; j < 8; j++)
                n = (n << 8) + bytes[j];
            return n;
        }
        internal string GetString()
        {
            int n = GetInt();
            byte[] bytes = new byte[n];
            if (n > 0)
                stream.Read(bytes, 0, n);
            return Encoding.UTF8.GetString(bytes, 0, bytes.Length);
        }
        internal void GetData(CellValue cell,int flag)
        {
             switch (flag & 0xf)
            {
                case 0:
                    return;
                case 1:
                    {
                        string s = GetString();
                        if (long.TryParse(s, out long lg))
                            cell.val = lg;
                        else
                            cell.val = s;
                    }
                    break;
                case 2:
                    {
                        string s = GetString();
                        if (decimal.TryParse(s,
                            NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign,
                            CultureInfo.InvariantCulture, out decimal de))
                            cell.val = de;
                        else
                            cell.val = s;
                    }
                    break;
                case 3: cell.val = GetString(); break;
                case 4: cell.val = GetDateTime(); break;
                case 5:
                    {
                        var bb = GetBlob();
                        switch (cell.subType)
                        {
                            case "DOCUMENT": cell.val = new Document(bb); break;
                            case "DOCARRAY": cell.val = new DocArray(bb); break;
                            case "OBJECT": cell.val = new ObjectId(bb); break;
                            default: cell.val = bb; break;
                        }
                        break;
                    }
                case 6: cell.val = GetRow(); break;
                case 7: cell.val = GetArray(); break;
                case 8:
                    {
                        var s = GetString();
                        try
                        {
                            var e = s.IndexOf('E');
                            var lf = s[0..e];
                            var f = Math.Min(lf.Length,16);
                            lf = lf[0..f];
                            var rg = s[e..];
                            s = lf + rg;
                            cell.val = double.Parse(s, CultureInfo.InvariantCulture);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("|" + s + "| " + e.Message);
                            throw e;
                        }
                    }
                    break;
                case 9: cell.val = GetInt() != 0; break;
                case 10: cell.val = GetInterval(); break;
                case 11: cell.val = GetTimeSpan(); break;
                case 12: cell.val = GetRow(cell.subType); break;
                case 13: cell.val = new Date(GetDateTime()); break;
                case 14: cell.val = GetTable(); break;
                default: throw new DatabaseError("2E204", "" + flag);
            }
        }
        internal byte[] GetBlob()
        {
            int n = GetInt();
            byte[] bytes = new byte[n];
            stream.Read(bytes, 0, n);
            return bytes;
        }
        internal PyrrhoRow GetRow(string tn = "Table")
        {
            int n = GetInt();
            PyrrhoTable t = new PyrrhoTable(tn);
            object[] data = new object[n];
            var rdr = new PyrrhoReader(t);
            for (int j = 0; j < n; j++)
            {
                var cn = GetString();
                var dn = GetString();
                var fl = GetInt();
                t.Columns.Add(new PyrrhoColumn(cn, dn, fl));
                data[j] = rdr.GetCell(this, dn, fl);
            }
            PyrrhoRow r = new PyrrhoRow(t);
            for (int j = 0; j < n; j++)
                r[j] = data[j];
            t.Rows.Add(r);
            return r;
        }
        internal PyrrhoArray GetArray()
        {
            PyrrhoArray r = new PyrrhoArray();
            r.kind = GetString();
            var dn = GetString();
            var fl = GetInt();
            int n = GetInt();
            var rdr = new PyrrhoReader(new PyrrhoTable());
            r.data = new object[n];
            for (int j = 0; j < n; j++)
                r.data[j] = rdr.GetCell(this, dn, fl);
            return r;
        }
        internal void GetSchema(PyrrhoTable pt, int ncols)
        {
            int k = 0;
            for (int j = 0; j < ncols; j++)
            {
                var cn = GetString();
                if (cn == "" || pt.columns.ContainsKey(cn))
                    cn = "Col" + pt.columns.Count;
                var dn = GetString();
                var p = new PyrrhoColumn(cn, dn, GetInt());
                pt.columns.Add(cn, j);
                pt.Columns.Add(p);
                int ki = (p.type >> 4) & 0xf;
                if (ki > k)
                    k = ki;
            }
            if (k > 0)
            {
                var pk = new PyrrhoColumn[k];
                for (int j = 0; j < ncols; j++)
                {
                    var p = (PyrrhoColumn)pt.Columns[j];
                    int ki = (p.type >> 4) & 0xf;
                    if (ki > 0)
                        pk[ki - 1] = p;
                }
                pt.PrimaryKey = pk;
            }
        }
        internal PyrrhoTable GetTable()
        {
            var n = GetInt();
            if (n == 0)
                return null;
            var s = GetString();
            var dt = new PyrrhoTable();
            GetSchema(dt, n);
            var rdr = new PyrrhoReader(dt);
            int nrows = GetInt();
            for (int j = 0; j < nrows; j++)
            {
                var r = new PyrrhoRow(dt);
                for (int i = 0; i < n; i++)
                {
                    var c = dt.Columns[i];
                    r[i] = rdr.GetCell(this, c.datatypename, c.type);
                }
                dt.Rows.Add(r);
            }
            return dt;
        }
        internal void PutString(string text)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(text);
            int n = bytes.Length;
            PutInt(n);
            stream.Write(bytes, 0, n);
        }
        internal DateTime GetDateTime()
        {
            return new DateTime(GetLong());
        }
        internal TimeSpan GetTimeSpan()
        {
            return new TimeSpan(GetLong());
        }
        internal PyrrhoInterval GetInterval()
        {
            PyrrhoInterval iv = new PyrrhoInterval();
            var b = stream.ReadByte();
            if (b == 1)
            {
                iv.years = (int)GetLong();
                iv.months = (int)GetLong();
            }
            else
                iv.ticks = GetLong();
            return iv;
        }
        internal string[] GetStrings()
        {
            int n = GetInt();
            string[] obs = new string[n];
            for (int j = 0; j < n; j++)
                obs[j] = GetString();
            return obs;
        }
        internal void Send(Protocol proto, string text)
        {
            stream.WriteByte((byte)proto);
            PutString(text);
        }
        internal void Send(Protocol proto)
        {
            stream.WriteByte((byte)proto);
        }
        internal virtual Responses Receive()
        {
            warnings.Clear();
            var proto = (Responses)stream.ReadByte();
            while (proto == Responses.Warning || proto == Responses.AskClient)
            {
                var sig = GetString();
                if (proto==Responses.Warning)
                    warnings.Add(new DatabaseError(sig, GetStrings()));
                if (proto==Responses.AskClient)
                {
                    Console.Write(sig);
                    var s = Console.ReadLine();
                    if (s != null)
                        Send(Protocol.ClientAnswer, s);
                }
                proto = (Responses)stream.ReadByte();
            }
            if (proto < 0)
                _Close();
            return proto;
        }
        public string[] GetFileNames()
        {
            Send(Protocol.GetFileNames);
            if (Receive() != Responses.Files)
                return null;
            int n = GetInt();
            string[] r = new string[n];
            for (int j = 0; j < n; j++)
                r[j] = GetString();
            return r;
        }
        public void SetRole(string s)
        {
            if (transaction != null || execution != null)
                throw new DatabaseError("08C06");
            Send(Protocol.Authority);
            PutString(s);
            Receive();
        }
        public void ResetReader()
        {
            if (execution != Thread.CurrentThread)
                throw new DatabaseError("08C00");
            Send(Protocol.ResetReader);
            Receive();
        }
        public void DetachDatabase(string s)
        {
            if (transaction != null || execution != null)
                throw new DatabaseError("08C06");
            Send(Protocol.Detach);
            PutString(s);
            Receive();
            Close();
        }
        internal object Cast(Type tp, object v)
        {
            if (v == null || v is DBNull)
                return v;
            PyrrhoRow row = v as PyrrhoRow;
            if (row != null)
            {
                ConstructorInfo ci = tp.GetConstructor(BindingFlags.Instance
                    | BindingFlags.Public, null, CallingConventions.HasThis, new Type[0], null);
                object ob = ci.Invoke(new object[0]);
                Fields(tp, ob, row);
                return ob;
            }
            string s = tp.FullName;
            switch (s)
            {
                case "System.Int32":
                    if (v is string)
                        return int.Parse((string)v);
                    return (int)(long)v;
                case "System.Int64":
                    if (v is string)
                        return long.Parse((string)v);
                    return (long)v;
                case "System.Decimal":
                    if (v is string)
                        return decimal.Parse((string)v);
                    return (decimal)v;
                case "System.Float":
                    return (float)(double)v;
            }
            return v;
        }
        internal bool Castable(Type p, Type s)
        {
            if (p.FullName == s.FullName)
                return true;
            switch (s.FullName)
            {
                case "System.Int32":
                    return p.FullName == "System.Int64";
                case "System.Float":
                    return p.FullName == "System.Double";
            }
            return false;
        }
        internal void Fields(Type tp, object ob, PyrrhoRow r)
        {
            int n = r.ItemArray.Length;
            FieldInfo[] fs = tp.GetFields();
            for (int j = 0; j < n; j++)
            {
                FieldInfo fi = fs[j];
                fi.SetValue(ob, Cast(fi.FieldType, r[j]));
            }
        }
        void SendConnectionString(string cs)
        {
            crypt.key = GetLong(); // nonce
            Send(0);
            if (cs.Substring(0, 2) == "[{")
            {
                var ss = cs.Substring(2, cs.Length - 4).Split(new string[] { "},{" }, StringSplitOptions.RemoveEmptyEntries);
                for (int i = 0; i < ss.Length; i++)
                {
                    SendConnectionString1(ss[i]);
                    crypt.Send(Connecting.Details);
                }
            }
            else
                SendConnectionString1(cs);
            crypt.Send(Connecting.Done);
        }
        void SendConnectionString1(string cs)
        {
            string[] fields = cs.Split(';');
            crypt.Send(Connecting.User, Environment.UserDomainName + "\\" + Environment.UserName);
            crypt.Send(Connecting.Culture, Thread.CurrentThread.CurrentUICulture.Name);
            for (int j = 0; j < fields.Length; j++)
            {
                string f = fields[j];
                int m = f.IndexOf('=');
                if (m < 0)
                    throw new DatabaseError("2E208", f);
                string n = f.Substring(0, m);
                string v = f.Substring(m + 1);
                switch (n)
                {
                    case "Provider": break;
                    case "Host": break;
                    case "Port": break;
                    case "Files": crypt.Send(Connecting.Files, v); break;
                    case "Role": crypt.Send(Connecting.Role, v); break;
                    case "Stop": crypt.Send(Connecting.Stop, v); break;
                    case "Base": crypt.Send(Connecting.Base, v); break;
                    case "Modify": crypt.Send(Connecting.Modify, v); break;
                    case "AllowAsk": crypt.Send(Connecting.AllowAsk, v); break;
                    default: throw new DatabaseError("2E209", n);
                }
            }
        }
        public bool Check(string check)
        {
            if (transaction != Thread.CurrentThread)
                throw new DatabaseError("08C02");
            Send(Protocol.Check);
            PutString(check);
            return Receive() == Responses.Valid;
        }
        public void Prepare(string nm, string sql)
        {
            Send(Protocol.Prepare);
            PutString(nm);
            PutString(sql);
            Receive();
        }
        public void ChangeDatabase(string databaseName)
        {
        }
        public void Get(Versioned ob)
        {
            reflection.Get(ob);
        }
        public void Delete(Versioned ob)
        {
            reflection.Delete(ob);
        }
        public void Put(Versioned ob)
        {
            reflection.Put(ob);
        }
        public void Post(Versioned ob)
        {
            reflection.Post(ob);
            posted.Add(ob);
        }
        public C[] Get<C>(string? w=null) where C : new()
        {
            return reflection.Get<C>(w);
        }

        public PyrrhoTransaction BeginTransaction()
        {
            AcquireTransaction();
            return new PyrrhoTransaction(this);
        }

        public string ConnectionString
        {
            get
            {
                return connectionString;
            }
            set
            {
                connectionString = value;
            }
        }

        public PyrrhoCommand CreateCommand()
        {
            return new PyrrhoCommand(this);
        }
        public void Open()
        {
            if (isOpen)
                return;
            string[] names = GetConnectionValues(connectionString, "Host");
            hostName = (names != null) ? names[0] : "localhost";
            int port = 5433;
            string[] ports = GetConnectionValues(connectionString, "Port");
            if (ports != null)
                port = int.Parse(ports[0]);
            string[] locales = GetConnectionValues(connectionString, "Locale");
            if (locales != null)
                Thread.CurrentThread.CurrentUICulture = new CultureInfo(locales[0]);
            try
            {
                IPEndPoint ep;
                if (char.IsDigit(hostName[0]) || hostName[0] == ':')
                {
                    var af = AddressFamily.InterNetwork;
                    if (hostName[0] == ':')
                        af = AddressFamily.InterNetworkV6;
                    socket = new Socket(af, SocketType.Stream, ProtocolType.Tcp);
                    IPAddress ip = IPAddress.Parse(hostName);
                    ep = new IPEndPoint(ip, port);
                    socket.Connect(ep);
                }
                else
                {
                    IPHostEntry he = Dns.GetHostEntry(hostName);
                    for (int j = 0; j < he.AddressList.Length; j++)
                        try
                        {
                            IPAddress ip = he.AddressList[j];
                            socket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                            ep = new IPEndPoint(ip, port);
                            socket.Connect(ep);
                            if (socket.Connected)
                                break;
                        }
                        catch (Exception) { }
                }
            }
            catch (Exception)
            {
            }
            if (socket == null || !socket.Connected)
                throw new DatabaseError("08004", hostName, "" + port);
            crypt = new Crypt(new AsyncStream(this, socket));
            stream = crypt.stream;
            SendConnectionString(connectionString);
            isOpen = true;
            isClosed = false;
            var b = Receive();
            if (b != Responses.Primary) // PRIMARY
                ((AsyncStream)stream).GetException(b);
            reflection = new Reflection(this);
        }
        public void Close()
        {
            Send(Protocol.CloseConnection);
            stream.Close();
            stream = null;
            socket.Close();
            isOpen = false;
        }
        void _Close()
        {
            isOpen = false;
            isClosed = true;
            stream = null;
        }
        public string Database
        {
            get
            {
                // TODO:  Add PyrrhoConnection.Database getter implementation
                return null;
            }
        }

        public int ConnectionTimeout
        {
            get
            {
                // TODO:  Add PyrrhoConnection.ConnectionTimeout getter implementation
                return 0;
            }
        }
        public DatabaseError[] Warnings
        {
            get
            {
                var r = new DatabaseError[warnings.Count];
                int i = 0;
                foreach (DatabaseError w in warnings)
                    r[i++] = w;
                return r;
            }
        }
        public ConnectionState State
        {
            get
            {
                return isOpen ? ConnectionState.Open :
                    isClosed ? ConnectionState.Closed :
                    ConnectionState.Connecting;
            }
        }
    }

    public enum ConnectionState { Open, Closed, Connecting }
    public class PyrrhoCommand
    {
        internal string commandText = "";
        internal PyrrhoConnect conn;
        PyrrhoTransaction trans;
        Thread thread = Thread.CurrentThread;
        public PyrrhoParameterCollection parameters = new PyrrhoParameterCollection();
        public PyrrhoCommand(PyrrhoConnect c)
        {
            conn = c;
        }
        internal void CheckThread()
        {
            if (thread != Thread.CurrentThread)
                throw new DatabaseError("08C01");
        }

        public PyrrhoReader ExecuteReaderCrypt()
        {
            if (!conn.isOpen)
                throw new DatabaseError("2E201");
            conn.AcquireExecution();
            conn.Send(Protocol.ExecuteReaderCrypt);
            conn.crypt.PutString(commandText);
            return PyrrhoReader.New(this);
        }
        public PyrrhoReader ExecuteReader()
        {
            if (!conn.isOpen)
                throw new DatabaseError("2E201");
            if (thread != Thread.CurrentThread)
                throw new DatabaseError("08C00");
            conn.AcquireExecution();
            conn.Send(Protocol.ExecuteReader, commandText);
            return PyrrhoReader.New(this);
        }
        internal PyrrhoReader _ExecuteReader<T>(PyrrhoTable<T> t) where T : class
        {
            if (!conn.isOpen)
                throw new DatabaseError("2E201");
            conn.AcquireExecution();
            conn.Send(Protocol.ExecuteReader, commandText);
            return PyrrhoReader.New<T>(this, t);
        }
        public object ExecuteScalar()
        {
            var rdr = (PyrrhoReader)ExecuteReader();
            try
            {
                rdr.Read();
                object o = rdr[0];
                rdr.Close();
                return o;
            }
            finally
            {
                conn.ReleaseExecution();
            }
        }
        public object ExecuteScalarCrypt()
        {
            var rdr = (PyrrhoReader)ExecuteReaderCrypt();
            try
            {
                rdr.Read();
                object o = rdr[0];
                rdr.Close();
                return o;
            }
            finally
            {
                conn.ReleaseExecution();
            }
        }
        public int ExecuteNonQuery()
        {
            return ExecuteNonQuery(null);
        }
        public int ExecuteNonQuery(Versioned? ob)
        {
            if (!conn.isOpen)
                throw new DatabaseError("2E201");
            if (thread != Thread.CurrentThread)
                throw new DatabaseError("08C01");
            conn.AcquireExecution();
            try
            {
                conn.Send(Protocol.ExecuteNonQuery, commandText);
                var p = conn.Receive();
                if (p != Responses.Done)
                    throw new DatabaseError("2E203");
                return conn.GetInt();
            }
            finally
            {
                conn.ReleaseExecution();
            }
        }
        public int ExecuteNonQueryTrace(Versioned? ob)
        {
            if (!conn.isOpen)
                throw new DatabaseError("2E201");
            if (thread != Thread.CurrentThread)
                throw new DatabaseError("08C01");
            conn.AcquireExecution();
            conn.RecordRequest(CommandText);
            conn.Send(Protocol.ExecuteNonQueryTrace, commandText);
            var p = conn.Receive();
            if (p != Responses.DoneTrace)
                throw new DatabaseError("2E203");
            var ts = conn.GetLong();
            conn.RecordResponse(ts, conn.GetLong(), p);
            var r = conn.GetInt();
            conn.ReleaseTransaction();
            return r;
        }
        public int ExecuteNonQueryCrypt()
        {
            if (!conn.isOpen)
                throw new DatabaseError("2E201");
            if (thread != Thread.CurrentThread)
                throw new DatabaseError("08C01");
            conn.AcquireExecution();
            try
            {
                conn.Send(Protocol.ExecuteNonQueryCrypt);
                conn.crypt.PutString(commandText);
                var p = conn.Receive();
                if (p != Responses.Done)
                    throw new DatabaseError("2E203");
                return conn.GetInt();
            }
            finally
            {
                conn.ReleaseExecution();
            }
        }
        public int CommandTimeout
        {
            get
            {
                // TODO:  Add PyrrhoCommand.CommandTimeout getter implementation
                return 0;
            }
            set
            {
                // TODO:  Add PyrrhoCommand.CommandTimeout setter implementation
            }
        }
        public PyrrhoParameter CreateParameter()
        {
            if (thread != Thread.CurrentThread)
                throw new DatabaseError("08C01");
            return new PyrrhoParameter();
        }

        public PyrrhoConnect Connection
        {
            get
            {
                return conn;
            }
            set
            {
            }
        }

        public string CommandText
        {
            get
            {
                if (thread != Thread.CurrentThread)
                    throw new DatabaseError("08C01");
                return commandText;
            }
            set
            {
                if (thread != Thread.CurrentThread)
                    throw new DatabaseError("08C01");
                commandText = value;
            }
        }
        public PyrrhoParameterCollection Parameters
        {
            get
            {
                if (thread != Thread.CurrentThread)
                    throw new DatabaseError("08C01");
                return parameters;
            }
        }

        public PyrrhoTransaction Transaction
        {
            get
            {
                return trans;
            }
            set
            {
                // this is allowed for compatibility: makes no sense in Pyrrho
                trans = value;
            }
        }
    }
    /// <summary>
    /// The DataParameterCollection turns out slightly more interesting than it sounds.
    /// First: although ADO.NET supports both named and positional parameters, the data sources
    /// only support one kind: SqlClient and Oracle nonly allow named parameters, and so
    /// Pyrrho disallows positional parameters, see exception handling below.
    /// Second: there is a nvery neat algorithm for processing the parameters that depends
    /// reverse alphabetical order of parameters, so that explains why we sort the parameter
    /// collection in the the code below.
    /// </summary>
    public class PyrrhoParameterCollection
    {
        // items are maintained in reverse alphabetical order (important)
        List<PyrrhoParameter> items = new List<PyrrhoParameter>();
        // private variable for helping with Add etc
        bool found = false;
        /// <summary>
        /// example: if the list has cde cd b and given bc we want to return 2
        /// </summary>
        /// <param name="parameterName">a given parameter name</param>
        /// <returns>the last place with a parameter less than the given one</returns>
        int PositionFor(string parameterName)
        {
            if (parameterName == null)
                throw new ArgumentNullException("Parameter Name cannot be null");
            var at = items.Count;
            found = false;
            while (at > 0)
            {
                var p = items[at - 1] as PyrrhoParameter;
                var c = p.ParameterName.CompareTo(parameterName);
                if (c > 0)
                    break;
                at--;
                if (c == 0)
                {
                    found = true;
                    break;
                }
            }
            return at;
        }
        public bool Contains(string parameterName)
        {
            PositionFor(parameterName);
            return found;
        }
        /// <summary>
        /// This ought not to be useful, but might be used instead of Contains
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        public int IndexOf(string parameterName)
        {
            var i = PositionFor(parameterName);
            return found ? i : -1;
        }

        public void RemoveAt(string parameterName)
        {
            var i = PositionFor(parameterName);
            if (found)
                items.RemoveAt(i);
        }

        public object? this[string parameterName]
        {
            get
            {
                var k = PositionFor(parameterName);
                if (found)
                    return items[k];
                return null;
            }
            set
            {
                var p = value as PyrrhoParameter;
                if (p == null)
                    return;
                p.ParameterName = parameterName;
                var k = PositionFor(parameterName);
                if (found)
                    items[k] = p;
                else
                    items.Insert(k, p);
            }
        }

        public int Add(object value)
        {
            if (value is PyrrhoParameter p)
            {
                var k = PositionFor(p.ParameterName);
                if (found)
                {
                    items[k] = p;
                    return k;
                }
                items.Insert(k, p);
            }
            return items.Count - 1;
        }

        public void Clear()
        {
            items.Clear();
        }

        public bool Contains(object value)
        {
            if (value is PyrrhoParameter p)
                return Contains(p.ParameterName);
            return false;
        }

        public int IndexOf(object value)
        {
            if (value is PyrrhoParameter p)
                return IndexOf(p.ParameterName);
            return -1;
        }

        public void Insert(int index, object value)
        {
            throw new NotImplementedException("This DBMS does not support positional command parameters");
        }

        public bool IsFixedSize
        {
            get { return false; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public void Remove(object value)
        {
            if (value is PyrrhoParameter p)
                RemoveAt(p.ParameterName);
        }

        public void RemoveAt(int index)
        {
            throw new NotImplementedException("This DBMS does not support positional command parameters");
        }

        public object this[int index]
        {
            get
            {
                throw new NotImplementedException("This DBMS does not support positional command parameters");
            }
            set
            {
                throw new NotImplementedException("This DBMS does not support positional command parameters");
            }
        }

        public void CopyTo(Array array, int index)
        {
            for (; index < items.Count; index++)
                array.SetValue(items[index], index);
        }

        public int Count
        {
            get { return items.Count; }
        }

        public bool IsSynchronized
        {
            get { return false; }
        }

        public object SyncRoot
        {
            get { return items; }
        }

        public System.Collections.IEnumerator GetEnumerator()
        {
            return items.GetEnumerator();
        }
    }
    public class PyrrhoTransaction
    {
        readonly PyrrhoConnect conn;
        internal PyrrhoTransaction(PyrrhoConnect c)
        {
            conn = c;
            conn.Send(Protocol.BeginTransaction);
        }
        public void Rollback()
        {
            try
            {
                conn.Send(Protocol.Rollback);
                var p = conn.Receive();
            }
            finally
            {
                conn.ReleaseTransaction();
            }
        }
        public int Commit(string mess = "")
        {
            return CommitTrace(mess, new Versioned[0]);
        }

        public int Commit(params Versioned[] obs)
        {
            int r = 0;
            conn.Send(Protocol.CommitAndReport1);
            conn.PutInt(obs.Length);
            for (int i = 0; i < obs.Length; i++)
            {
                var ss = obs[i].version.Split(':');
                conn.PutString(ss[0]);
                conn.PutLong(Pos(ss[1]));
                conn.PutLong(Pos(ss[2]));
            }
            var p = conn.Receive();
            if (p == Responses.TransactionReport)
            {
                r = conn.GetInt();
                conn.GetLong(); // schemaKey
                conn.GetInt(); // obs.Length
                for (int i = 0; i < obs.Length; i++)
                {
                    var pa = conn.GetString();
                    var d = conn.GetLong();
                    var o = conn.GetLong();
                    obs[i].version = pa + ":" + Pos(d) + ":" + Pos(o);
                }
            }
            conn.ReleaseTransaction();
            return r;
        }
        public int CommitTrace(string mess, params Versioned[] obs)
        {
            int r = 0;
            conn.RecordRequest("Commit " + mess);
            conn.Send(Protocol.CommitAndReportTrace1);
            conn.PutInt(obs.Length);
            for (int i = 0; i < obs.Length; i++)
            {
                var ss = obs[i].version.Split(':');
                conn.PutString(ss[0]);
                conn.PutLong(Pos(ss[1]));
                conn.PutLong(Pos(ss[2]));
            }
            var ts = 0L;
            var te = 0L;
            var p = conn.Receive();
            if (p == Responses.TransactionReportTrace)
            {
                r = conn.GetInt();
                ts = conn.GetLong();
                te = conn.GetLong();
                conn.RecordResponse(ts, te, p);
                conn.GetLong(); // schemaKey
                conn.GetInt(); // obs.Length
                for (int i = 0; i < obs.Length; i++)
                {
                    var pa = conn.GetString();
                    var d = conn.GetLong();
                    var o = conn.GetLong();
                    obs[i].version = pa + ":" + Pos(d) + ":" + Pos(o);
                }
            }
            else
                conn.RecordResponse(ts, te, p);
            conn.ReleaseTransaction();
            return r;
        }
        long Pos(string s)
        {
            if (s[0] == '\'')
                return long.Parse(s.Substring(1)) + 0x400000000000;
            return long.Parse(s);
        }
        string Pos(long p)
        {
            if (p > 0x400000000000)
                return "'" + (p - 0x400000000000);
            return "" + p;
        }
    }
    public class PyrrhoTransactionReport
    {
        public long lastSchemaPos;
        public List<string> rowsInserted = new List<string>();
        public List<long> rowIdsInserted = new List<long>();
        internal PyrrhoTransactionReport(PyrrhoConnect conn)
        {
            lastSchemaPos = conn.GetLong();
            var n = conn.GetInt();
            for (int i = 0; i < n; i++)
                rowsInserted.Add(conn.GetString());
            for (int i = 0; i < n; i++)
                rowIdsInserted.Add(conn.GetLong());
        }
    }
    public class Versioned // for Entities
    {
        [Exclude]
        public PyrrhoConnect conn; // null if committed or new instance
        [Exclude]
        public string entity = ""; // [info{,info}] where info is /tabledefpos/defpos
        [Exclude]
        public string version;     // ppos or etag null if new instance 
        public void Get()
        { 
            conn.Get(this); 
        }
        public void Put() 
        {
            conn.Put(this);
        }
        public void Delete()
        {
            conn.Delete(this);
        }
        public static Versioned Parse(string s)
        {
            var r = new Versioned();
            if (s != null)
            {
                var ix = s.IndexOf(' ');
                if (ix > 0)
                {
                    r.entity = s.Substring(0, ix);
                    r.version = s.Substring(ix + 1);
                }
                else r.version = s;
            }
            return r;
        }
        // included for completeness: useful only if conn is supplied on construction
        public void Post()
        {
            conn.Post(this);
        }
    }
    public sealed class TableAttribute : Attribute
    {
        public long tabledefpos, lastschemachange; 
        public TableAttribute(long p, long c) { tabledefpos = p; lastschemachange = c; }
    }
    public sealed class AutoKeyAttribute : Attribute
    {  }
    public sealed class FieldAttribute : Attribute
    {
        public PyrrhoDbType type;
        public long domain;  // Pyrrho's domain cookie
        public string info; // Pyrrho's domain information
        public long subType = -1L;
        public FieldAttribute(PyrrhoDbType tn) { type = tn; }
        public FieldAttribute(PyrrhoDbType type, string info, long subType = -1L): this(type)
        {
            this.info = info;
            this.subType = subType;
        }
        public FieldAttribute(PyrrhoDbType type, long domain, string info, long subType=-1L) 
            : this(type)
        {
            this.domain = domain;
            this.info = info;
            this.subType = subType;
        }
    }
    public sealed class ExcludeAttribute : Attribute { }
    public class PyrrhoReader
    {
        PyrrhoCommand cmd;
        bool active = true;
        internal PyrrhoTable schema;
        IEnumerator<PyrrhoRow> local = null;
        internal CellValue[] row = null; // current row, as obtained by IDataReader.Read
        public string version = null, entity = null;
        internal CellValue[] cells = null; // cells obtained from a single ReaderData call (4.2)
        internal BTree<string, BTree<string, Versioned>> versions = null;
        internal int off = 0; // next cell to use in cells[]
        PyrrhoReader(PyrrhoCommand c, PyrrhoTable pt, int ncols)
        {
            cmd = c;
            cmd.conn.GetSchema(pt, ncols);
            schema = pt;
            row = new CellValue[ncols];
        }
        public PyrrhoReader(PyrrhoTable t)
        {
            schema = t;
            local = schema.Rows.GetEnumerator();
        }
        internal static PyrrhoReader New(PyrrhoCommand c)
        {
            var p = c.conn.Receive();
            if (p != Responses.Schema)
            {
                c.conn.ReleaseExecution();
                return null;
            }
            var ncols = c.conn.GetInt();
            if (ncols == 0)
            {
                c.conn.ReleaseExecution();
                return null;
            }
            var n = c.conn.GetString();
            var t = new PyrrhoTable(n);
            PyrrhoReader rdr = new PyrrhoReader(c, t, ncols);
            return rdr;
        }
        internal static PyrrhoReader New<T>(PyrrhoCommand c, PyrrhoTable<T> t) where T : class
        {
            var p = c.conn.Receive();
            if (p != Responses.Schema)
                return null;
            var ncols = c.conn.GetInt();
            if (ncols == 0)
                return null;
            var n = c.conn.GetString();
            PyrrhoReader rdr = new PyrrhoReader(c, t, ncols);
            return rdr;
        }
        internal CellValue GetCell(PyrrhoConnect c, string tname, int flag)
        {
            var cell = new CellValue()
            {
                subType = tname
            };
        again:
            var b = c.stream.ReadByte();
            switch (b)
            {
                case 0: break;
                case 1:
                    cell.subType = tname;
                    c.GetData(cell, flag);
                    break;
                case 2:
                    tname = c.GetString();
                    flag = c.GetInt();
                    goto case 1;
                case 3:
                    version = c.GetString();
                    goto again;
                case 4:
                    entity = c.GetString();
                    if (entity!="" && !entity.Contains("http"))
                    {
                        var ix = entity.LastIndexOf("/");
                        version = entity.Substring(ix + 1);
                        entity = entity.Substring(0, ix);
                    }
                    goto again;
                case 5:
                    cell.prefix = c.GetString();
                    goto case 2;
                case 6:
                    cell.suffix = c.GetString();
                    goto case 2;
            }
            return cell;
        }
        public int RecordsAffected
        {
            get
            {
                return 0;
            }
        }

        public bool IsClosed
        {
            get
            {
                cmd.CheckThread();
                return (!active) || cmd.conn.stream == null;
            }
        }

        public bool NextResult()
        {
            cmd.CheckThread();
            return Read();
        }

        public void Close()
        {
            cmd.CheckThread();
            if (active)
            {
                if (cmd.conn.stream != null)
                {
                    cmd.conn.Send(Protocol.CloseReader);
                    cmd.conn.stream.Flush();
                }
                active = false;
                cmd.conn.ReleaseExecution();
            }
        }
        public string GetDataSubtypeName(int i)
        {
            cmd.CheckThread();
            return cells[i].subType ?? (schema.Columns[i]).datatypename;
        }
        private bool GetCell(int cx)
        {
            if (cells != null)
            {
                row[cx] = cells[off++];
                if (off == cells.Length)
                    cells = null;
                return true;
            }
            off = 0;
            cmd.conn.Send(Protocol.ReaderData); // new ReaderData call, 4.2
            var p = cmd.conn.Receive();
            if (p != Responses.ReaderData)
                return false;
            int n = cmd.conn.GetInt(); // number of cells to collect
            if (n == 0)
                return false;
            cells = new CellValue[n];
            int j = cx;
            for (int i = 0; i < n; i++)
            {
                var col = schema.Columns[j];
                cells[i] = GetCell(cmd.conn, col.datatypename, col.type);
                if (++j == schema.Columns.Count)
                    j = 0;
            }
            row[cx] = cells[off++];
            if (off == cells.Length)
            {
                cells = null;
                versions = null;
            }
            return true;
        }

        public bool Read()
        {
            cmd.CheckThread();
            if (local != null)
            {
                bool r = local.MoveNext();
                if (r)
                {
                    var rw = local.Current;
                    row = rw.row;
                    version = null;
                }
                return r;
            }
            for (int j = 0; j < schema.Columns.Count; j++)
                if (!GetCell(j))
                    return false; // should only happen on j==0
            return true;
        }

        public int Depth
        {
            get
            {
                // TODO:  Add PyrrhoReader.Depth getter implementation
                return 0;
            }
        }
        public PyrrhoTable GetSchemaTable()
        {
            cmd.CheckThread();
            schema.Fill(this);
            return schema;
        }

        public int GetInt32(int i)
        {
            cmd.CheckThread();
            return (int)(long)row[i].val;
        }

        public object this[string name]
        {
            get
            {
                cmd.CheckThread();
                var k = schema.Find(name);
                return row[k]?.val ?? DBNull.Value;
            }
        }

        public object this[int i]
        {
            get
            {
                cmd.CheckThread();
                return row[i]?.val ?? DBNull.Value;
            }
        }
        public T GetEntity<T>() where T : class
        {
            cmd.CheckThread();
            var t = schema as PyrrhoTable<T>;
            var tp = typeof(T);
            if (t == null)
                throw new DatabaseError("2E303", tp.Name, schema.TableName);
            var ci = tp.GetConstructor(new Type[0]);
            if (ci == null)
                throw new DatabaseError("2E301", tp.Name);
            var e = ci.Invoke(new object[0]) as T;
            var fs = tp.GetFields();
            for (int i = 0; i < fs.Length; i++)
            {
                var f = fs[i];
                f.SetValue(e, GetValue(i));
            }
            return e;
        }
        public object GetValue(int i)
        {
            cmd.CheckThread();
            object o = row[i].val;
            return o ?? DBNull.Value;
        }

        public bool IsDBNull(int i)
        {
            cmd.CheckThread();
            var o = row[i];
            return o == null || o.val == null || o.val is DBNull;
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            cmd.CheckThread();
            // TODO:  Add PyrrhoReader.GetBytes implementation
            return 0;
        }

        public byte GetByte(int i)
        {
            cmd.CheckThread();
            return (byte)row[i].val;
        }

        public Type GetFieldType(int i)
        {
            cmd.CheckThread();
            if ((schema.Columns[i]).DataTypeName == "DOCUMENT")
                return typeof(Document);
            if ((schema.Columns[i]).DataTypeName == "LEVEL")
                return typeof(string);
            return (schema.Columns[i]).DataType;
        }

        public decimal GetDecimal(int i)
        {
            cmd.CheckThread();
            return (decimal)row[i].val;
        }

        public int GetValues(object[] values)
        {
            cmd.CheckThread();
            // TODO:  Add PyrrhoReader.GetValues implementation
            return 0;
        }

        public string GetName(int i)
        {
            cmd.CheckThread();
            return ((PyrrhoColumn)schema.Columns[i]).Caption;
        }

        public int FieldCount
        {
            get
            {
                cmd.CheckThread();
                return row.Length;
            }
        }

        public long GetInt64(int i)
        {
            cmd.CheckThread();
            return (long)row[i].val;
        }

        public double GetDouble(int i)
        {
            cmd.CheckThread();
            return (double)(decimal)row[i].val;
        }

        public bool GetBoolean(int i)
        {
            cmd.CheckThread();
            return ((long)row[i].val) == 1;
        }

        public Guid GetGuid(int i)
        {
            cmd.CheckThread();
            // TODO:  Add PyrrhoReader.GetGuid implementation
            return new Guid();
        }

        public DateTime GetDateTime(int i)
        {
            cmd.CheckThread();
            return (DateTime)row[i].val;
        }

        public int GetOrdinal(string name)
        {
            cmd.CheckThread();
            return schema.Find(name);
        }

        public string GetDataTypeName(int i)
        {
            cmd.CheckThread();
            return schema.Columns[i].datatypename;
        }

        public float GetFloat(int i)
        {
            cmd.CheckThread();
            return (float)(decimal)row[i].val;
        }

        public PyrrhoReader GetData(int i)
        {
            cmd.CheckThread();
            PyrrhoTable t = row[i].val as PyrrhoTable;
            if (t == null)
                return null;
            return new PyrrhoReader(t);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            cmd.CheckThread();
            // TODO:  Add PyrrhoReader.GetChars implementation
            return 0;
        }

        public string GetString(int i)
        {
            cmd.CheckThread();
            return row[i].val.ToString();
        }

        public char GetChar(int i)
        {
            cmd.CheckThread();
            // TODO:  Add PyrrhoReader.GetChar implementation
            return '\0';
        }

        public short GetInt16(int i)
        {
            cmd.CheckThread();
            return (short)(long)row[i].val;
        }
        public PyrrhoRow GetRow()
        {
            cmd.CheckThread();
            var r = new PyrrhoRow(schema);
            return r;
        }

        public string GetSubtypeName(int i)
        {
            cmd.CheckThread();
            return row[i].subType ?? "";
        }
    }
    public class PyrrhoReader<T> : PyrrhoReader
    {
        public PyrrhoReader(PyrrhoTable t)
            : base(t)
        {
        }
    }
    public class CellValue
    {
        public string subType = null;
        public object val = null;
        public string prefix = null, suffix = null;
        public override string ToString()
        {
            var sb = new StringBuilder();
            if (prefix!=null)
                sb.Append(prefix);
            if (val!=null) 
                sb.Append(val.ToString());
            if (suffix!=null)
            { sb.Append(' '); sb.Append(suffix); }
            return sb.ToString();
        }
    }
    public class PyrrhoParameter
    {
        string name = "";
        PyrrhoDbType type = PyrrhoDbType.DBNull;
        object val = null;
        int size = 0;
        string srcColumn = "";
        bool isInput = true;
        bool nullable = true;
        byte prec = 0, scale = 0;
        bool forCurrent = true;
        public PyrrhoParameter() { }
        public PyrrhoParameter(string n, PyrrhoDbType t)
        {
            name = n;
            type = t;
        }
        public PyrrhoParameter(string n, object v)
        {
            name = n;
            val = v;
        }
        public PyrrhoParameter(string n, PyrrhoDbType t, int s)
        {
            name = n;
            type = t;
            size = s;
        }
        public PyrrhoParameter(string n, PyrrhoDbType t, int s, string c)
        {
            name = n;
            type = t;
            size = s;
            srcColumn = c;
        }
        public PyrrhoParameter(string n, PyrrhoDbType t, int s, bool d,
            bool nu, byte pr, byte sc, string c, bool dv, object v)
        {
            name = n;
            type = t;
            isInput = d;
            nullable = nu;
            prec = pr;
            scale = sc;
            srcColumn = c;
            forCurrent = dv;
            val = v;
        }
        public override string ToString()
        {
            switch (type)
            {
                case PyrrhoDbType.String: { var v = val.ToString(); Check(v); return "'" + v + "'"; }
                case PyrrhoDbType.Date: return "DATE'" + ((Date)val).date.ToString("dd-MM-yyyy") + "'";
                case PyrrhoDbType.Time: return "TIME'" + ((DateTime)val).ToString("hh:mm:ss") + "'";
                case PyrrhoDbType.Interval: return ((PyrrhoInterval)val).Format();
                case PyrrhoDbType.Timestamp: return "TIMESTAMP'" + ((DateTime)val).ToString("dd-MM-yyyy hh:mm:ss") + "'";
                case PyrrhoDbType.Blob: return Reflection.Hexits((byte[])val);
                default: { var v = val.ToString(); Check(v); return v; }
            }
        }
        private void Check(string val)
        {
            if (val.Contains("'"))
                throw new Exception("Illegal character ' in parameter");
        }
        public bool IsNullable
        {
            get { return nullable; }
        }

        public string ParameterName
        {
            get
            {
                return name;
            }
            set
            {
                name = value;
            }
        }

        public string SourceColumn
        {
            get
            {
                return srcColumn;
            }
            set
            {
                srcColumn = value;
            }
        }


        public object Value
        {
            get
            {
                return val;
            }
            set
            {
                val = value;
            }
        }
        public byte Precision
        {
            get
            {
                return prec;
            }
            set
            {
                prec = value;
            }
        }

        public byte Scale
        {
            get
            {
                return scale;
            }
            set
            {
                scale = value;
            }
        }

        public int Size
        {
            get
            {
                return size;
            }
            set
            {
                size = value;
            }
        }
    }
    public class PyrrhoTable
    {
        public string TableName;
        public string ConnectString = "";
        public string SelectString = "";
        public new PyrrhoColumn[] PrimaryKey = new PyrrhoColumn[0];
        public List<PyrrhoColumn> Columns = new List<PyrrhoColumn>();
        public List<PyrrhoRow> Rows = new List<PyrrhoRow>();
        internal Dictionary<string, int> columns = new Dictionary<string, int>();

        public bool ReadOnly
        {
            get { return PrimaryKey.Length == 0; }
        }
        internal PyrrhoTable(string n = "Table")
        {
            TableName = n;
        }
        internal PyrrhoTable(PyrrhoTable t, string n)
        {
            for (var i = 0; i < t.Columns.Count; i++)
            {
                Columns.Add(t.Columns[i]);
                columns.Add(t.Columns[i].Caption, i);
            }
            Columns.Add(new PyrrhoColumn(n, "", 0));
            columns.Add(n, t.Columns.Count);
        }
        internal PyrrhoTable(PyrrhoConnect c, string n)
        {
            ConnectString = c.ConnectionString;
            SelectString = "table " + n;
            TableName = n;
        }
        internal PyrrhoTable(PyrrhoCommand c, string n)
        {
            ConnectString = c.conn.ConnectionString;
            SelectString = c.commandText;
            TableName = n;
        }
        internal int Find(string n)
        {
            for (int j = 0; j < Columns.Count; j++)
                if ((Columns[j]).ColumnName == n)
                    return j;
            return -1;
        }

        public PyrrhoReader GetReader()
        {
            return new PyrrhoReader(this);
        }

        internal new PyrrhoRow NewRow()
        {
            PyrrhoRow r = new PyrrhoRow(this);
            return r;
        }
        internal virtual void Fill(PyrrhoReader rdr)
        {
            while (rdr.Read())
            {
                var r = NewRow();
                for (int j = 0; j < Columns.Count; j++)
                    r[j] = rdr.row[j];
                Rows.Add(r);
            }
        }
    }
    public class PyrrhoTable<T> : PyrrhoTable where T : class
    {
        private PyrrhoTable(PyrrhoConnect c, string n)
        {
            var cmd = c.CreateCommand() as PyrrhoCommand;
            cmd.CommandText = "table \"" + TableName + "\"";
            var r = cmd._ExecuteReader<T>(this);
            Fill(r);
            r.Close();
        }
        internal override void Fill(PyrrhoReader rdr)
        {
            while (rdr.Read())
            {
                var r = NewRow();
                for (int j = 0; j < Columns.Count; j++)
                    r[j] = rdr.row[j]?.val ?? DBNull.Value;
                Rows.Add(r);
                var k = (IComparable)(r[PrimaryKey[0]].val);
                if (PrimaryKey.Length != 1)
                {
                    var sb = new StringBuilder();
                    var comma = "";
                    for (int i = 0; i < PrimaryKey.Length; i++)
                    {
                        var c = r[PrimaryKey[i]].val;
                        sb.Append(comma); comma = ",";
                        if (c is string)
                        {
                            sb.Append("'"); sb.Append(c); sb.Append("'");
                        }
                        else
                            sb.Append(c);
                    }
                    k = sb.ToString();
                }
            }
        }
    }
    public class PyrrhoColumn
    {
        internal string ColumnName, Caption;
        internal Type DataType;
        internal bool AllowDBNull, ReadOnly;
        internal int type; // least-sig 4 bits: type, next 4 bits: pKey info
        internal string datatypename = null;
        internal PyrrhoColumn(string n, string dn, int t)
        {
            ColumnName = n;
            datatypename = dn;
            type = t;
            DataType = SystemType;
            AllowDBNull = (t & 0x100) == 0;
            Caption = n;
            ReadOnly = (t & 0x200) != 0;
        }
        internal Type SystemType
        {
            get
            {
                int t = type & 0xf;
                switch (t)
                {
                    case 0: return typeof(object);
                    case 1: return typeof(long);
                    case 2: return typeof(decimal);
                    case 3: return typeof(string);
                    case 4: return typeof(DateTime);
                    case 5: return typeof(byte[]);
                    case 6: return typeof(string);
                    case 7: return typeof(PyrrhoArray);
                    case 8: return typeof(double);
                    case 9: return typeof(bool);
                    case 10: return typeof(PyrrhoInterval);
                    case 11: return typeof(DateTime);
                    case 12: return typeof(PyrrhoRow);
                    case 13: return typeof(Date);
                    case 14: return typeof(PyrrhoTable);
                    case 15: return typeof(PyrrhoArray);
                }
                throw new DatabaseError("2E204", "" + t);
            }
        }
        public string DataTypeName
        {
            get
            {
                if (datatypename != null)
                    return datatypename;
                int t = type & 0xf;
                switch (t)
                {
                    case 0: return "unknown";
                    case 1: return "int";
                    case 2: return "numeric";
                    case 3: return "char";
                    case 4: return "timestamp";
                    case 5: return "blob";
                    case 6: return "row";
                    case 7: return "array"; // or multiset or table
                    case 8: return "real";
                    case 9: return "boolean";
                    case 10: return "interval";
                    case 11: return "time";
                    case 12: return "type";
                    case 13: return "date";
                    case 14: return "instance";
                    case 15: return "instances";
                }
                return ""; // notreached
            }
        }
        public override string ToString()
        {
            return Caption;
        }
    }
    [Flags]
    public enum PyrrhoRowState { Original = 0, Current = 1, Proposed = 2 };
    public class PyrrhoRow
    {
        internal CellValue[] row;
        internal PyrrhoRowState state = PyrrhoRowState.Current;
        internal Versioned? check;
        PyrrhoTable schema;
        internal PyrrhoRow(PyrrhoTable t, Versioned? c = null)
        {
            schema = t;
            check = c;
            row = new CellValue[t.Columns.Count];
        }
        internal PyrrhoRow(params CellValue[] r)
        {
            schema = new PyrrhoTable();
            row = r;
        }
        protected PyrrhoRow(PyrrhoRow r, (string, CellValue) c)
        {
            schema = new PyrrhoTable(r.schema, c.Item1);
            row = new CellValue[r.row.Length + 1];
            for (var i = 0; i < r.row.Length; i++)
                row[i] = r.row[i];
            row[r.row.Length] = c.Item2;
        }
        public PyrrhoRowState RowState { get { return state; } }
        public PyrrhoTable Table { get { return schema; } }
        public object this[string s]
        {
            get { return row[(int)schema.columns[s]].val; }
            set
            {
                int i = (int)schema.columns[s];
                row[i] = new CellValue { val = value };
            }
        }
        public static PyrrhoRow operator +(PyrrhoRow r, (string, CellValue) c)
        {
            return new PyrrhoRow(r, c);
        }
        public CellValue this[PyrrhoColumn c]
        {
            get { return row[(int)schema.columns[c.ColumnName]]; }
            set { row[(int)schema.columns[c.ColumnName]] = value; }
        }
        public object this[int i]
        {
            get { return row[i].val; }
            set
            {
                row[i] = new CellValue { val = value };
            }
        }
        public CellValue[] ItemArray
        {
            get { return row; }
            set { row = value; }
        }
        public override string ToString()
        {
            string str = "(";
            if (schema.TableName != "Table")
                str = schema.TableName + "(";
            for (int j = 0; j < schema.Columns.Count; j++)
            {
                var c = schema.Columns[j];
                if (c.Caption != "")
                    str += c.Caption + "=";
                str += row[j].ToString() + ((j < schema.Columns.Count - 1) ? "," : ")");
            }
            return str;
        }
    }
    public class PyrrhoArray
    {
        public string kind;
        public object[] data;
        public override string ToString()
        {
            string str = kind + "[";
            for (int j = 0; j < data.Length; j++)
            {
                str += data[j].ToString() + ((j < data.Length - 1) ? "," : "]");
            }
            if (data.Length == 0)
                str += ']';
            return str;
        }
    }
    public class PyrrhoInterval
    {
        public int years;
        public int months;
        public long ticks;
        public static long TicksPerSecond
        {
            get { return TimeSpan.TicksPerSecond; }
        }
        public string Format()
        {
            TimeSpan ts = new TimeSpan(ticks);
            return "INTERVAL '" + years + "-" + months.ToString("d2") +
                "-" + ts.Days.ToString("d2") + " " +
                ts.Hours.ToString("d2") + ":" +
                ts.Minutes.ToString("d2") + ":" + ts.Seconds.ToString("d2") +
                "'YEAR TO SECOND";
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            if (years != 0)
            {
                sb.Append(years); sb.Append('y');
            }
            if (months != 0)
            {
                sb.Append(months); sb.Append('M');
            }
            var t = ticks;
            var b = t < 0;
            if (b)
                t = -t;
            var d = t / TimeSpan.TicksPerDay;
            t -= d * TimeSpan.TicksPerDay;
            if (d > 0)
            {
                if (b)
                {
                    sb.Append('-'); b = false;
                }
                sb.Append(d); sb.Append('d');
            }
            var h = t / TimeSpan.TicksPerHour;
            t -= h * TimeSpan.TicksPerHour;
            if (h > 0)
            {
                if (b)
                {
                    sb.Append('-'); b = false;
                }
                sb.Append(h); sb.Append('h');
            }
            var m = t / TimeSpan.TicksPerMinute;
            t -= m * TimeSpan.TicksPerMinute;
            if (m > 0)
            {
                if (b)
                {
                    sb.Append('-'); b = false;
                }
                sb.Append(m); sb.Append('m');
            }
            var ss = t * 1.0 / TimeSpan.TicksPerSecond;
            if (ss > 0)
            {
                if (b)
                {
                    sb.Append('-'); b = false;
                }
                sb.Append(ss); sb.Append('s');
            }
            return sb.ToString();
        }
    }
    public class Date
    {
        public DateTime date;
        public static Date Today = new Date(DateTime.Now);
        public Date(DateTime d)
        {
            date = d;
        }
        public override string ToString()
        {
            return date.ToShortDateString();
        }
    }
    public enum PyrrhoDbType
    {
        DBNull, Integer, Decimal, String, Timestamp, Blob, Row, Array, Real, Bool,
        Interval, Time, Date, UDType, Multiset, Xml, Document
    }
    public class DatabaseError : Exception
    {
        public string SQLSTATE;
        public Dictionary<string, string> info = new Dictionary<string, string>();
        internal DatabaseError(string sig, params string[] obs)
            : base(Resx.Format(sig, obs))
        {
            SQLSTATE = sig;
        }
    }
    public class TransactionConflict : DatabaseError
    {
        public TransactionConflict(string reason) : base("40001") { }
        public TransactionConflict(string mess, string reason) : base("40001", mess)
        { info.Add("WITH", reason); }
        public TransactionConflict(string sig, string[] obs) : base(sig, obs)
        {
            if (obs.Length > 2)
                info.Add("WITH", obs[1] + " " + obs[2]);
        }
    }
    /// <summary>
    /// For asynchronous IO
    /// </summary>
    internal class AsyncStream : Stream
    {
        // important: all buffers have exactly this size
        const int bSize = 2048;
        internal class Buffer
        {
            // first 2 bytes indicate how many following bytes are good
            internal byte[] bytes = new byte[bSize];
            internal ManualResetEvent wait = null;
        }
        PyrrhoConnect connect = null;
        internal Socket client;
        internal Buffer[] wbufs = new Buffer[2];
        internal Buffer rbuf = new Buffer();
        internal Buffer wbuf = null;
        internal int rx = 0, wx = 0;
        internal int rcount = 0, rpos = 2, wcount = 2;
        internal AsyncStream(PyrrhoConnect pc, Socket c)
        {
            client = c;
            wbufs[0] = new Buffer();
            wbufs[1] = new Buffer();
            wbuf = wbufs[0];
            connect = pc;
        }
        public override int ReadByte()
        {
            if (wcount != 2)
                Flush();
            if (rpos < (rcount + 2))
                return rbuf.bytes[rpos++];
            rpos = 2;
            rcount = 0;
            rx = 0;
            try
            {
                rbuf.wait = new ManualResetEvent(false);
                client.BeginReceive(rbuf.bytes, 0, bSize, 0, new AsyncCallback(Callback), this);
                rbuf.wait.WaitOne();
                if (rcount <= 0)
                    return -1;
                if (rcount == bSize - 1)
                    return GetException();
            }
            catch (SocketException)
            {
                return -1;
            }
            return rbuf.bytes[rpos++];
        }
        void Callback(IAsyncResult ar)
        {
            try
            {
                int rc = client.EndReceive(ar);
                if (rc == 0)
                {
                    rcount = 0;
                    rbuf.wait.Set();
                    return;
                }
                if (rc + rx == bSize)
                {
                    rcount = (((int)rbuf.bytes[0]) << 7) + (int)rbuf.bytes[1];
                    rbuf.wait.Set();
                }
                else
                {
                    rx += rc;
                    client.BeginReceive(rbuf.bytes, rx, bSize - rx, 0, new AsyncCallback(Callback), this);
                }
            }
            catch (SocketException)
            {
                rcount = 0;
                rbuf.wait.Set();
            }
        }
        public override int Read(byte[] buffer, int offset, int count)
        {
            int j;
            for (j = 0; j < count; j++)
            {
                int x = ReadByte();
                if (x < 0)
                    break;
                buffer[offset + j] = (byte)x;
            }
            return j;
        }
        public override void WriteByte(byte value)
        {
            if (wcount < bSize)
                wbuf.bytes[wcount++] = value;
            if (wcount >= bSize)
                WriteBuf();
        }
        private void WriteBuf()
        {
            wbuf.wait = new ManualResetEvent(false);
            // now always send bSize bytes (not wcount)
            wcount -= 2;
            wbuf.bytes[0] = (byte)(wcount >> 7);
            wbuf.bytes[1] = (byte)(wcount & 0x7f);
            try
            {
                client.BeginSend(wbuf.bytes, 0, bSize, 0, new AsyncCallback(Callback1), wbuf);
                wx = (wx + 1) & 1;
                wbuf = wbufs[wx];
                if (wbuf.wait != null)
                    wbuf.wait.WaitOne();
            }
            catch (SocketException)
            {
            }
            wcount = 2;
        }
        void Callback1(IAsyncResult ar)
        {
            Buffer buf = ar.AsyncState as Buffer;
            buf.wait.Set();
        }
        public override void Write(byte[] buffer, int offset, int count)
        {
            for (int j = 0; j < count; j++)
                WriteByte(buffer[offset + j]);
        }
        public override void Flush()
        {
            rcount = 0;
            rpos = 2;
            int ox = (wx + 1) & 1;
            Buffer obuf = wbufs[ox];
            if (obuf.wait != null)
                obuf.wait.WaitOne();
            wbuf.wait = new ManualResetEvent(false);
            // now always send bSize bytes (not wcount)
            wcount -= 2;
            wbuf.bytes[0] = (byte)(wcount >> 7);
            wbuf.bytes[1] = (byte)(wcount & 0x7f);
            try
            {
                IAsyncResult br = client.BeginSend(wbuf.bytes, 0, bSize, 0, new AsyncCallback(Callback1), wbuf);
                if (!br.IsCompleted)
                    br.AsyncWaitHandle.WaitOne();
                wcount = 2;
            }
            catch (SocketException)
            {
            }
        }
        internal int GetException()
        {
            return (int)GetException(Responses.Exception);
        }
        // v2.0 exception handling during server comms
        // an illegal nonzero rcount value indicates an exception
        internal Responses GetException(Responses proto)
        {
            if (proto == Responses.Exception)
            {
                rcount = (((int)rbuf.bytes[rpos++]) << 7) + (((int)rbuf.bytes[rpos++]) & 0x7f);
                rcount += 2;
                proto = (Responses)rbuf.bytes[rpos++];
            }
            Exception e = null;
            string sig;
            switch (proto)
            {
                case Responses.OobException: sig = "2E205"; e = new DatabaseError(sig); break;
                case Responses.Exception:
                    sig = connect.GetString();
                    if (sig.StartsWith("40"))
                        e = new TransactionConflict(sig, connect.GetStrings());
                    else
                        e = new DatabaseError(sig, connect.GetStrings()); break;
                case Responses.FatalError:
                    {
                        var cs = connect.GetString();
                        sig = cs.Contains("(412) Precondition Failed") ? "40082" : "2E206";
                        e = new DatabaseError(sig, cs);
                        break;
                    }
                case Responses.TransactionConflict: sig = "40001"; e = new TransactionConflict(connect.GetString()); break;
                case Responses.TransactionReason:
                    {
                        sig = "40001";
                        var m = connect.GetString();
                        e = new TransactionConflict(m, connect.GetString());
                        break;
                    }
                default: return proto;
            }
            if (e is DatabaseError && !(e is TransactionConflict))
            {
                var dbe = e as DatabaseError;
                while (rpos < rcount)
                {
                    var k = connect.GetString();
                    var v = connect.GetString();
                    dbe.info.Add(k, v);
                }
                dbe.info.Add("CONDITION_NUMBER", sig);
                if (!dbe.info.ContainsKey("RETURNED_SQLSTATE"))
                    dbe.info.Add("RETURNED_SQLSTATE", sig);
                if (!dbe.info.ContainsKey("MESSAGE_TEXT"))
                    dbe.info.Add("MESSAGE_TEXT", e.Message);
                var m = dbe.info["MESSAGE_TEXT"];
                if (!dbe.info.ContainsKey("MESSAGE_LENGTH"))
                    dbe.info.Add("MESSAGE_LENGTH", "" + m.Length);
                if (!dbe.info.ContainsKey("MESSAGE_OCTET_LENGTH"))
                    dbe.info.Add("MESSAGE_OCTET_LENGTH", "" + Encoding.UTF8.GetBytes((string)m).Length);
            }
            rx = 0; wx = 0;
            rcount = 0; rpos = 2; wcount = 2;
            connect.ReleaseExecution();
            connect.ReleaseTransaction();
            throw e;
        }
        public override bool CanRead
        {
            get { return true; }
        }
        public override bool CanWrite
        {
            get { return true; }
        }
        public override bool CanSeek
        {
            get { return false; }
        }
        public override long Length
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }
        public override long Position
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new Exception("The method or operation is not implemented.");
        }
        public override void SetLength(long value)
        {
            throw new Exception("The method or operation is not implemented.");
        }
    }
}
