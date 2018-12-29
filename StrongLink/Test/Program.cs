using System;
using StrongLink;
using Shareable;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var conn = new StrongConnect("127.0.0.1", 50433, "test");
                new Program().Tests(conn);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message);
            }
            Console.WriteLine("Testing complete");
            Console.ReadLine();
        }
        bool parsing = false;
        StrongConnect conn;
        void Tests(StrongConnect c)
        {
            conn = c;
            Console.Write("Using Parser (Y/N)?");
            if (Console.ReadLine().StartsWith("Y"))
                parsing = true;
            Test1();
            Test2();
        }
        void Test1()
        {
            conn.BeginTransaction();
            if (parsing)
            {
                conn.ExecuteNonQuery("create table A(B integer,C integer,D string)");
                conn.ExecuteNonQuery("create primary index ax for A(B,C)");
                conn.ExecuteNonQuery("insert A values(2,3,'TwentyThree')");
                conn.ExecuteNonQuery("insert A values(1,9,'Nineteen')");
                CheckResults("select from A", "[{B:2,C:3,D:'TwentyThree'},{B:1,C:9,D:'Nineteen'}]");
            }
            else
            {
                conn.CreateTable("A",
                    new SColumn("B", Types.SInteger),
                    new SColumn("C", Types.SInteger),
                    new SColumn("D", Types.SString));
                conn.CreateIndex("A", IndexType.Primary, null, "B", "C");
                conn.Insert("A", null, new Serialisable[] { new SInteger(2), new SInteger(3), new SString("TwentyThree") },
                  new Serialisable[] { new SInteger(1), new SInteger(9), new SString("Nineteen") });
                CheckResults(new STable("A"), "[{B:2,C:3,D:'TwentyThree'},{B:1,C:9,D:'Nineteen'}]");
            }
            conn.Rollback();
        }
        void Test2()
        {
            conn.BeginTransaction();
            if (parsing)
            { 
                conn.ExecuteNonQuery("create table AA(B integer,C string)");
                conn.ExecuteNonQuery("insert AA(B) values(17)");
                conn.ExecuteNonQuery("insert AA(C) values('BC')");
                conn.ExecuteNonQuery("insert AA(C,B) values('GH',67)");
                CheckResults("select from AA","[{B:17},{C:'BC'},{B:67,C:'GH'}]");
            }
            else
            {
                conn.CreateTable("AA",
                    new SColumn("B", Types.SInteger),
                    new SColumn("C", Types.SString));
                conn.Insert("AA", new string[] { "B" }, 
                    new Serialisable[] { new SInteger(17) });
                conn.Insert("AA", new string[] { "C" },
                    new Serialisable[] { new SString("BC") });
                conn.Insert("AA", new string[] { "C", "B" },
                    new Serialisable[] { new SString("GH"), new SInteger(67) });
                CheckResults(new STable("AA"), "[{B:17},{C:'BC'},{B:67,C:'GH'}]");
            }
            conn.Rollback();
        }
        void CheckResults(string c,string d)
        {
            Check(conn.ExecuteQuery(c),new DocArray(d));
        }
        void CheckResults(Serialisable c, string d)
        {
            Check(conn.Get(c), new DocArray(d));
        }
        void Check(DocArray s,DocArray c)
        {
            if (s.items.Count != c.items.Count)
                throw new Exception("Different number of rows");
            for (var i = 0; i < s.items.Count; i++)
                Check(s[i], c[i]);
        }
        void Check(Document s,Document c)
        {
            var nc = 0;
            foreach(var sp in s.fields)
                if (!sp.Key.StartsWith("_"))
                {
                    if (!c.Contains(sp.Key))
                        throw new Exception("Unexpected field " + sp.Key);
                    var cf = c[sp.Key];
                    if (cf == sp.Value || cf?.ToString() == sp.Value?.ToString())
                        nc++;
                    else
                        throw new Exception("Values do not match");
                }
            if (nc != c.fields.Count)
                throw new Exception("Missing field(s)");
        }
    }
}
