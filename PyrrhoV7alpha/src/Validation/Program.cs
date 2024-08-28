using Pyrrho;
using System.Linq.Expressions;
using System.Text;
using System.Xml.Schema;
namespace Validation
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var inf = args[0].Split(".");
            var conn = new PyrrhoConnect("Files=sf1");// + inf[0]);
            conn.Open();
            var tr = conn.BeginTransaction();
            var pms = new StreamReader(args[0]);
            var s = pms.ReadToEnd();
            var pma = new DocArray(s);
            pms.Close();
            foreach (var doc in pma.items)
            {
                var test = ((Document)pma.items[0]);
                var operation = (Document)test.fields[0].Value;
                var step = (Document)test.fields[0].Value;
                var name = step.fields[0].Key;
                var ps = (Document)step.fields[0].Value;
                var sb = new StringBuilder();
                if (name is string ns)
                {
                    sb.Append("call "); sb.Append(name);
                }
                else
                    goto bad;
                var cm = '(';
                foreach (var field in ps.fields)
                {
                    sb.Append(cm); cm = ',';
                    var v = field.Value;
                    if ((field.Key == "time" || field.Key == "startTime" || field.Key == "endTime")
                        && v is long ep)
                        v = FixTime(ep);
                    sb.Append(v.ToString());
                }
                sb.Append(')');
                var cmd = conn.CreateCommand();
                cmd.CommandText = sb.ToString();
                var rdr = cmd.ExecuteReader();
                var res = new DocArray();
                while (rdr.Read())
                {
                    var rr = new Document();
                    for (var i = 0; i < rdr.FieldCount; i++)
                        rr.fields.Add(new KeyValuePair<string, object>(rdr.GetName(i), rdr.GetValue(i)));
                    res.items.Add(rr);
                }
                rdr.Close();
                var pr = step.fields[1].Value;
                if (pr is DocArray pd)
                {
                    if (pd.items.Count != res.items.Count)
                        Console.WriteLine("Wrong number of items");
                    for (var i = 0; i < res.items.Count && i < pd.items.Count; i++)
                        if (res.items[i].ToString() != pd.items[i].ToString())
                            Console.WriteLine("Items do not match");
                }
            }
            Console.WriteLine("Done");
            Console.ReadLine();
            return;
        bad: Console.WriteLine("Error");
            Console.ReadLine();

        }
        static DocArray GetExecuteDoc(PyrrhoConnect conn, string cmd, Document doc)
        {
            var da = new DocArray();
            try
            {
                var rdr = conn.Call(cmd, doc);
                while (rdr?.Read() == true)
                {
                    var rd = new Document();
                    for (var i = 0; i < rdr.FieldCount; i++)
                        if (rdr[i] != DBNull.Value)
                            rd.fields.Add(new KeyValuePair<string, object>(rdr.GetName(i), rdr[i]));
                    da.items.Add(rd);
                }
                Console.WriteLine(da.ToString());
                rdr?.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return da;
        }

        static DateTime d0 = new DateTime(1970, 1, 1, 0, 0, 0);
        static string FixTime(long t)
        {
            var ts = d0.Add(new TimeSpan(t * 10000));
            var sb = new StringBuilder("timestamp'");
            sb.Append(ts.Year);sb.Append('-');AddTime(sb,ts.Month);sb.Append('-');
            sb.Append(ts.Day);sb.Append(' ');AddTime(sb,ts.Hour);sb.Append(':');
            sb.Append(ts.Minute);sb.Append(":");AddTime(sb, ts.Second);
            sb.Append('.');sb.Append(ts.Microsecond);
            sb.Append('\'');
            return sb.ToString();
        }
        static void AddTime(StringBuilder sb,int t)
        {
            if (t < 10) sb.Append('0');
            sb.Append(t);
        }
    }
}
