using System;
using System.Collections;

// classes for use from scripting engines such as PHP

namespace Pyrrho
{
    public class ScriptConnect
    {
        public PyrrhoConnect Connect = null;
        string _connectionString = "";
        public ScriptConnect() { }
        public string ConnectionString
        {
            get { return _connectionString; }
            set
            {
                bool open = (Connect != null);
                if (open)
                    Connect.Close();
                _connectionString = value;
                if (open)
                    Connect = new PyrrhoConnect(_connectionString);
            }
        }
        public void Open()
        {
            Connect = new PyrrhoConnect(_connectionString);
            Connect.Open();
        }
        public ScriptReader Execute(string sql)
        {
            if (Connect == null)
            {
                Connect = new PyrrhoConnect(_connectionString);
                Connect.Open();
            }
            var cmd = Connect.CreateCommand();
            cmd.CommandText = sql;
            var rdr = (PyrrhoReader)cmd.ExecuteReader();
            return new ScriptReader(rdr);
        }
        public void Close()
        {
            if (Connect != null)
                Connect.Close();
        }
    }
    public class ScriptReader
    {
        PyrrhoReader rdr = null;
        internal ScriptReader(PyrrhoReader r)
        {
            rdr = r as PyrrhoReader;
        }
        public object Read()
        {
            if (rdr == null)
                return -1;
            if (!rdr.Read())
            {
                rdr.Close();
                rdr = null;
                return -1;
            }
            int n = rdr.FieldCount;
            object[] r = new object[n];
            for (int j = 0; j < n; j++)
                r[j] = rdr[j];
            return r;
        }
        public object Get(object x)
        {
            if (x is int)
                return rdr[(int)x];
            return rdr[x.ToString()];
        }
        public void Close()
        {
            if (rdr != null)
                rdr.Close();
            rdr = null;
        }
    }
}