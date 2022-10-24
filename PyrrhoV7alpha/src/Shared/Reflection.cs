using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Pyrrho.Common;

namespace Pyrrho
{
    public class Reflection
    {
        PyrrhoConnect conn;
        internal Reflection(PyrrhoConnect c)
        {
            conn = c;
        }
        public C[] Get<C>(string w) where C : new()
        {
            var tp = typeof(C);
            var sa = tp.GetCustomAttribute<TableAttribute>();
            conn.Send(Protocol.Get);
            conn.PutLong(sa.lastschemachange);
            var sb=new StringBuilder();
            sb.Append("/"); sb.Append(sa.tabledefpos); sb.Append("/");
            if (w != null)
                sb.Append(w);
            conn.PutString(sb.ToString());
            return Get0<C>();
        }
        C[] Get0<C>() where C : new()
        {
            var rdr = PyrrhoReader.New(new PyrrhoCommand(conn));
            if (rdr == null)
                return new C[0];
            var tn = rdr.schema.TableName;
            var tp = typeof(C);
            if (tp == null)
                throw new DatabaseError("2E300", tn);
            var list = new List<C>();
            while (rdr.Read())
            {
                var ob = new C();
                for (int i = 0; i < rdr.FieldCount; i++)
                {
                    var f = tp.GetField(rdr.GetName(i));
                    if (f == null)
                        throw new DatabaseError("2E302", tn, rdr.GetName(i));
                    if (f.GetCustomAttribute<ExcludeAttribute>() != null)
                        continue;
                    var iv = rdr.GetValue(i);
                    if (iv != null && !(iv is DBNull))
                        f.SetValue(ob, iv);
                }
                var vo = ob as Versioned;
                if (vo != null)
                {
                    vo.conn = conn;
                    vo.version = rdr.version;
                    vo.entity = rdr.entity;
                }
                list.Add(ob);
            }
            rdr.Close();
            return list.ToArray();
        }
        internal void Get(Versioned ob)
        {
            var tp = ob.GetType();
            if (ob.entity == null || ob.entity.Length == 0 || ob.entity[0] != '/')
                throw new DatabaseError("2E304");
            var ps = BTree<string,(FieldInfo,FieldAttribute)>.Empty;
            foreach (var f in tp.GetFields())
                if (f.GetCustomAttribute<FieldAttribute>() is FieldAttribute fa)
                    ps += (f.Name, (f,fa));
            var sa = tp.GetCustomAttribute<TableAttribute>();
            conn.Send(Protocol.Get);
            conn.PutLong(sa.lastschemachange);
            var sb = new StringBuilder("/"+sa.tabledefpos+"/");
            var cm = "";
            for (var b = ps.First(); b != null; b = b.Next())
            {
                var (f,fa) = b.value();
                sb.Append(cm); cm = ",";
                var wv = f.GetValue(ob);
                sb.Append("\"" + f.Name + "\"=");
                var ws = "";
                if (wv is string)
                {
                    ws = wv.ToString();
                    if (ws != "" && ws[0] != '\'')
                        ws = "'" + ws.Replace("'", "''") + "'";
                }
                else 
                    ws = Sql(fa, ws);
                sb.Append(ws);
            }
            conn.PutString(sb.ToString());
            var rdr = PyrrhoReader.New(new PyrrhoCommand(conn));
            if (rdr == null || !rdr.Read())
            {
                ob.entity = null;
                ob.version = null;
                return;
            }
            var tn = rdr.schema.TableName;
            if (tp == null)
                throw new DatabaseError("2E300", tn);
            for (int i = 0; i < rdr.FieldCount; i++)
            {
                var f = tp.GetField(rdr.GetName(i));
                if (f == null)
                    throw new DatabaseError("2E302", tn, rdr.GetName(i));
                if (f.GetCustomAttribute<ExcludeAttribute>() != null)
                    continue;
                var iv = rdr.GetValue(i);
                if (iv != null && !(iv is DBNull))
                    f.SetValue(ob, iv);
            }
            ob.conn = conn;
            ob.version = rdr.version;
            ob.entity = rdr.entity;
            rdr.Close();
        }
        internal void Put(Versioned ob)
        {
            var tp = ob.GetType();
            var sa = tp.GetCustomAttribute<TableAttribute>();
            var fs = tp.GetFields();
            var sb = new StringBuilder("(");
            var comma = "";
            foreach (var f in fs) // declaration order, inherited fields come later
            {
                var ca = f.GetCustomAttributes(false);
                FieldAttribute fa = null;
                foreach (var an in ca)
                {
                    if (an is ExcludeAttribute)
                        goto skip;
                    if (fa == null)
                        fa = an as FieldAttribute;
                }
                var v = f.GetValue(ob);
                if (v != null)
                {
                    if (f.FieldType.Name == "String")
                        v = "'" + v.ToString().Replace("'", "''") + "'";
                    else if (f.FieldType.Name == "Byte[]")
                        v = Hexits((byte[])v);
                    else if (fa != null)
                        v = Sql(fa, v);
                }
                sb.Append(comma); comma = ",";
                sb.Append(v);
            skip: ;
            }
            sb.Append(")");
            var cmd = sb.ToString();
            if (sa == null)
                throw new Exception("Missing Table Attribute");
            if (ob.entity == null || ob.entity==""
                ||ob.version==null || ob.version=="")
                throw new Exception("Missing entity and/or version information");
            conn.Send(Protocol.Put);
            conn.PutLong(sa.lastschemachange);
            conn.PutString(ob.entity + "/" + ob.version +"/"+ cmd);
            var b = conn.Receive();
            if (b == Responses.NoData) // version no longer found
                throw new DatabaseError("40001");
            if (b==Responses.Entity)
            {
                var n = conn.GetInt();
                for (var i=0;i<n;i++)
                {
                    var f = tp.GetField(conn.GetString());
                    var c = new CellValue();
                    conn.GetData(c,conn.GetInt());
                    f.SetValue(ob, c.val);
                }
                ob.version = conn.GetLong().ToString();
                b = conn.Receive();
            }
            // b should be 11 now
            if (b != Responses.Done)
                throw new DatabaseError("2E203");
        }
        internal void Post(Versioned ob) 
        {
            var tp = ob.GetType();
            var sa = tp.GetCustomAttribute<TableAttribute>(); 
            var fs = tp.GetFields();
            var sb = new StringBuilder("/" + sa.tabledefpos + "/");
            var kf = new List<FieldInfo>();
            var comma = "(";
            foreach (var f in fs)
            {
                var v = f.GetValue(ob);
                if (v == null)
                    continue;
                var ca = f.GetCustomAttributes(false);
                FieldAttribute fa = null;
                AutoKeyAttribute ka = null;
                foreach (var an in ca)
                {
                    if (an is ExcludeAttribute)
                        goto skip;
                    if (fa == null)
                        fa = an as FieldAttribute;
                    ka = an as AutoKeyAttribute;
                }
                if (fa != null)
                {
                    if (ka!=null &&  kf[0] == f && v is long && (long)v == 0)
                        goto skip;
                    v = Sql(fa, v);
                }
                else if (f.FieldType.Name == "String")
                    v = "'" + v.ToString().Replace("'", "''") + "'";
                else if (f.FieldType.Name == "Byte[]")
                    v = Hexits((byte[])v);
                sb.Append(comma);
                sb.Append("\"");sb.Append(f.Name);sb.Append("\"=");
                sb.Append(v);
                comma = ",";
            skip: ;
            }
            sb.Append(")");
            var cmd = sb.ToString();
            conn.Send(Protocol.Post);
            conn.PutLong(TableSchemaKey(tp));
            conn.PutString(cmd);
            conn.posted.Add(ob);
            var b = conn.Receive();
            if (b == Responses.Entity)
            {
                var n = conn.GetInt();
                for (var i = 0; i < n; i++)
                {
                    var f = tp.GetField(conn.GetString());
                    var c = new CellValue();
                    conn.GetData(c, conn.GetInt());
                    f.SetValue(ob, c.val);
                }
                var ver = conn.GetLong();
                ob.conn = conn;
                ob.entity = "/" + sa.tabledefpos + "/" + ver;
                ob.version = ver.ToString();
                b = conn.Receive();
            }
            // b should be 11 now
            if (b != Responses.Done)
                throw new DatabaseError("2E203");
        }
        internal C[] Update<C>(Document w,Document u) where C:new()
        {
            conn.Send(Protocol.Update);
            conn.PutString(typeof(C).Name);
            var bs = w.ToBytes();
            conn.PutInt(bs.Length);
            conn.stream.Write(bs, 0, bs.Length);
            bs = u.ToBytes();
            conn.PutInt(bs.Length);
            conn.stream.Write(bs, 0, bs.Length);
            return Get0<C>();
        }
        void Fix(Versioned ob)
        {
            var n = conn.GetInt();
            if (n == 0)
                return;
            var dt = new PyrrhoTable();
            var rdr = new PyrrhoReader(dt);
            conn.GetString(); // "Table"
            conn.GetSchema(dt, n);
            var b = conn.stream.ReadByte(); // should be 14 (PutRow)
            var tp = ob.GetType();
            for (int i = 0; i < n; i++)
            {
                var c = dt.Columns[i];
                var f = tp.GetField(c.ColumnName);
                if (f == null)
                    throw new DatabaseError("2E302", tp.Name, c.ColumnName);
                var cv = rdr.GetCell(conn, c.datatypename, c.type);
                f.SetValue(ob, cv.val);
            }
        }
        long TableSchemaKey(Type t)
        {
            var ca = t.GetCustomAttributes(false);
            foreach (var a in ca)
                if (a is TableAttribute sa)
                    return sa.lastschemachange;
            return 0;
        }
        internal static string Sql(FieldAttribute f, object val)
        {
            switch (f.type)
            {
                case PyrrhoDbType.String: { var v = val.ToString().Replace("'", "''"); return "'" + v + "'"; }
                case PyrrhoDbType.Date: return "DATE'" + ((Date)val).date.ToString("yyyy-MM-dd") + "'";
                case PyrrhoDbType.Time: return "TIME'" + ((DateTime)val).ToString("hh:mm:ss") + "'";
                case PyrrhoDbType.Interval: return ((PyrrhoInterval)val).Format();
                case PyrrhoDbType.Timestamp: return "TIMESTAMP'" + ((DateTime)val).ToString("dd-MM-yyyy hh:mm:ss") + "'";
                case PyrrhoDbType.Blob: return Hexits((byte[])val);
                default: { var v = val.ToString().Replace("'", "''"); return v; }
            }
        }
        internal static string Hexits(byte[] v)
        {
            var sb = new StringBuilder("X'");
            for (int i = 0; i < v.Length; i++)
                sb.Append(v[i].ToString("x2"));
            sb.Append("'");
            return sb.ToString();
        }
        internal void Delete(Versioned ob)
        {
            var tp = ob.GetType();        
            conn.Send(Protocol.Delete);
            conn.PutLong(TableSchemaKey(tp));
            conn.PutString(ob.entity + "/" + ob.version);
            var b = conn.Receive();
            if (b != Responses.Done || conn.GetInt() == 0)
                throw new DatabaseError("02000");
        }
    }
}
