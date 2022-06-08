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

        public C[] Get<C>(string rurl) where C : new()
        {
            if (rurl[0] != '/')
                throw new DatabaseError("2E304");
#if EMBEDDED
           var path = rurl.Split('/');
           conn.db = conn.db.RequireTransaction();
           var d = conn.db.Front;
           d.Execute(d._Role, "", path, 1, "");
#else
            conn.Send(Protocol.Get, rurl);
#endif
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
                    vo.version = rdr.version.version;
                    vo.entity = rdr.version.entity;
                }
                list.Add(ob);
            }
            rdr.Close();
            return list.ToArray();
        }
        internal void Put(Versioned ob)
        {
            var tp = ob.GetType();
            var fs = tp.GetFields();
            var sb = new StringBuilder("update \"" + tp.Name + "\"");
            var sw = new StringBuilder(" where ");
            var comma = " set ";
            var where = "";
            foreach (var f in fs)
            {
                if (f.Name == "check" || f.Name == "rowversion")
                    continue;
                var ca = f.GetCustomAttributes(false);
                KeyAttribute ka = null;
                FieldAttribute fa = null;
                foreach (var an in ca)
                {
                    if (an is ExcludeAttribute)
                        goto skip;
                    if (ka == null)
                        ka = an as KeyAttribute;
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
                    if (fa != null)
                        v = Sql(fa, v);
                }
                else
                    v = "null";
                if (ka != null)
                {
                    sw.Append(where + "\"" + f.Name + "\"=" + v);
                    where = " and ";
                }
                else
                {
                    sb.Append(comma + "\"" + f.Name + "\"=" + v);
                    comma = ",";
                }
            skip: ;
            }
            if (ob.version!="")
            {
                sw.Append(where);
                sw.Append(tp.Name);
                sw.Append(".check='"); 
                sw.Append(ob.version);
                sw.Append("'");
            }
            var cmd = sb.ToString()+sw.ToString();
#if EMBEDDED
            var cc = conn.CreateCommand();
            cc.CommandText = cmd;
            if (cc.ExecuteNonQuery(ob) == 0)  // version no longer found
                throw new DatabaseError("40001");
#else
            conn.Send(Protocol.Put);
            conn.PutLong(SchemaKey(tp));
            conn.PutString(cmd);
            var b = conn.Receive();
            if (b == Responses.NoData) // version no longer found
                throw new DatabaseError("40001");
            if (b == Responses.Schema) // triggers and autoKey may have updated fields of ob
            {
                Fix(ob);
                b = conn.Receive();
            }
            // b should be 11 now
            if (b != Responses.Done)
                throw new DatabaseError("2E203");
#endif
        }
        internal void Post(Versioned ob) 
        {
            var tp = ob.GetType();
            var fs = tp.GetFields();
            var sb = new StringBuilder("insert into \"" + tp.Name + "\"");
            var sc = new StringBuilder(") values ");
#if MONO1
            var kf = new ArrayList();
#else
            var kf = new List<FieldInfo>();
#endif
            var comma = "(";
            foreach (var f in fs)
            {
                if (f.Name == "check")
                    continue;
                var v = f.GetValue(ob);
                if (v == null)
                    continue;
                var ca = f.GetCustomAttributes(false);
                KeyAttribute ka = null;
                FieldAttribute fa = null;
                foreach (var an in ca)
                {
                    if (an is ExcludeAttribute)
                        goto skip;
                    if (ka == null)
                        ka = an as KeyAttribute;
                    if (fa == null)
                        fa = an as FieldAttribute;
                }
                if (ka != null)
                    kf.Add(f);
                if (fa != null)
                {
                    if (ka!=null &&  (FieldInfo)kf[0] == f && v is long && (long)v == 0)
                        goto skip;
                    v = Sql(fa, v);
                }
                else if (f.FieldType.Name == "String")
                    v = "'" + v.ToString().Replace("'", "''") + "'";
                else if (f.FieldType.Name == "Byte[]")
                    v = Hexits((byte[])v);
                sb.Append(comma + "\"" + f.Name + "\"");
                sc.Append(comma + v);
                comma = ",";
            skip: ;
            }
            var cmd = sb.ToString() + sc.ToString() + ")";
#if EMBEDDED
            conn.Act(cmd,ob);
            if (ob.version!="")
            {
                sb = new StringBuilder("select * from \"");sb.Append(tp.Name);
                sb.Append("\" where check='");sb.Append(ob.version);sb.Append("'");
                var cm = conn.CreateCommand();
                cm.CommandText = sb.ToString();
                var rdr = cm.ExecuteReader();
                if (rdr.Read())
                   foreach (var f in fs)
                      f.SetValue(ob, rdr[f.Name]);
                rdr.Close();
            }
#else
            conn.Send(Protocol.Post);
            conn.PutLong(SchemaKey(tp));
            conn.PutString(cmd);
            conn.posted.Add(ob);
            var b = conn.Receive();
            if (b == Responses.Schema) // triggers and autoKey may have updated fields of ob
            {
                Fix(ob);
                b = conn.Receive();
            }
            // b should be 11 now
            if (b != Responses.Done)
                throw new DatabaseError("2E203");
#endif
        }
        internal C[] Update<C>(Document w,Document u) where C:new()
        {
#if EMBEDDED
           conn.db = conn.db.RequireTransaction();
           var d = conn.db.Front;
           d.Update(d._Role, "", typeof(C).Name, new Common.TDocument(w), new Common.TDocument(u));
#else
            conn.Send(Protocol.Update);
            conn.PutString(typeof(C).Name);
            var bs = w.ToBytes();
            conn.PutInt(bs.Length);
            conn.stream.Write(bs, 0, bs.Length);
            bs = u.ToBytes();
            conn.PutInt(bs.Length);
            conn.stream.Write(bs, 0, bs.Length);
#endif
            return Get0<C>();
        }
        object GetDefault(Type type)
        {
            if (type == null || !type.IsValueType || type == typeof(void))
                return null;
            var r = Activator.CreateInstance(type);
            return r;
        }
#if !EMBEDDED
        void Fix(Versioned ob)
        {
            var n = conn.GetInt();
            if (n == 0)
                return;
            var dt = new PyrrhoTable();
            conn.GetString(); // "Table"
            conn.GetSchema(dt, n);
            var b = conn.stream.ReadByte(); // should be 14 (PutRow)
            var tp = ob.GetType();
            for (int i = 0; i < n; i++)
            {
                var c = (PyrrhoColumn)dt.Columns[i];
                var f = tp.GetField(c.ColumnName);
                if (f == null)
                    throw new DatabaseError("2E302", tp.Name, c.ColumnName);
                var cv = conn.GetCell(i, c.datatypename, c.type, ref ob);
                f.SetValue(ob, cv.val);
            }
        }
#endif
        long SchemaKey(Type t)
        {
            var ca = t.GetCustomAttributes(false);
            foreach (var a in ca)
                if (a is SchemaAttribute)
                {
                    var sa = a as SchemaAttribute;
                    return sa.key;
                }
            return 0;
        }
        string Sql(FieldAttribute f, object val)
        {
            switch (f.type)
            {
                case PyrrhoDbType.String: { var v = val.ToString().Replace("'", "''"); return "'" + v + "'"; }
                case PyrrhoDbType.Date: return "DATE'" + ((Date)val).date.ToString("yyyy-MM-dd") + "'";
                case PyrrhoDbType.Time: return "TIME'" + ((DateTime)val).ToString("hh:mm:ss") + "'";
                case PyrrhoDbType.Interval: return ((PyrrhoInterval)val).Format();
                case PyrrhoDbType.Timestamp: return "TIMESTAMP'" + ((DateTime)val).ToString("dd-MM-yyyy hh:mm:ss") + "'";
                case PyrrhoDbType.Blob: return Reflection.Hexits((byte[])val);
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
            var fs = tp.GetFields();
            var sb = new StringBuilder("delete from \"");
            sb.Append(tp.Name); sb.Append("\" where ");
            sb.Append(tp.Name); sb.Append(".check ='");
            sb.Append(ob.version); sb.Append("'");
            var cmd = sb.ToString();
#if EMBEDDED
            conn.Act(cmd,ob as Versioned);
#else
            conn.Send(Protocol.Delete);
            conn.PutLong(SchemaKey(tp));
            conn.PutString(cmd);
            var b = conn.Receive();
            if (b != Responses.Done || conn.GetInt() == 0)
                throw new DatabaseError("02000");
#endif
        }
        //        var tp = ass.GetType(tn); // doesn't work, why not?
        Type GetType(Assembly ass, string tn)
        {
            var ts = ass.GetTypes();
            Type tp = null;
            for (int i = 0; i < ts.Length; i++)
                if (ts[i].Name == tn)
                {
                    tp = ts[i];
                    break;
                }
            return tp;
        }
    }
}
