﻿using System;
using StrongLink;
using Shareable;
using System.Threading;
using System.Threading.Tasks;
#nullable enable
namespace Test
{
    class Program
    {
        static int test = 0, qry = 0;
        static bool commit = false;
        static StrongConnect conn;
        static void Main(string[] args)
        {
            try
            {
                conn = new StrongConnect("127.0.0.1", 50433, "testdb");
                if (args.Length >= 2)
                {
                    test = int.Parse(args[0]);
                    qry = int.Parse(args[1]);
                }
                if (args.Length == 3)
                    commit = true;
                new Program().Tests();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message);
            }
            Console.WriteLine("Testing complete");
            Console.ReadLine();
        }
        void Begin()
        {
            if (!commit)
                conn.BeginTransaction();
        }
        void Rollback()
        {
            if (!commit)
                conn.Rollback();
        }
        void Tests()
        {
            Test1(test);
            Test2(test);
            Test3(test);
            Test4(test);
            Test5(test);
            Test6(test);
            Test7(test);
            Test8(test);
            Test9(test);
            Test10(test);
        }
        void Test1(int t)
        {
            if (t > 0 && t != 1)
                return;
            Begin();
            conn.ExecuteNonQuery("create table A(B integer,C integer,D string)");
            conn.ExecuteNonQuery("create primary index ax for A(B,C)");
            conn.ExecuteNonQuery("insert A values(2,3,'TwentyThree')");
            conn.ExecuteNonQuery("insert A values(1,9,'Nineteen')");
            CheckResults(1,1,"select from A", "[{B:1,C:9,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            conn.ExecuteNonQuery("update A where C=9 set C=19");
            CheckResults(1,2, "select from A", "[{B:1,C:19,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            Rollback();
            if (!commit)
            {
                Begin();
                conn.CreateTable("A",
                        new SColumn("B", Types.SInteger),
                        new SColumn("C", Types.SInteger),
                        new SColumn("D", Types.SString));
                conn.CreateIndex("A", IndexType.Primary, null, "B", "C");
                conn.Insert("A", new string[0], new Serialisable[] { new SInteger(2), new SInteger(3), new SString("TwentyThree") },
                    new Serialisable[] { new SInteger(1), new SInteger(9), new SString("Nineteen") });
                CheckResults(new STable("A"), "[{B:1,C:9,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
                Rollback();
            }
        }
        void Test2(int t)
        {
            if (t > 0 && t != 2)
                return;
            Begin();
            conn.ExecuteNonQuery("create table AA(B integer,C string)");
            conn.ExecuteNonQuery("insert AA(B) values(17)");
            conn.ExecuteNonQuery("insert AA(C) values('BC')");
            conn.ExecuteNonQuery("insert AA(C,B) values('GH',+67)");
            CheckResults(2,1,"select from AA", "[{B:17},{C:'BC'},{B:67,C:'GH'}]");
            CheckResults(2,2,"select B from AA", "[{B:17},{B:67}]");
            CheckResults(2, 3,"select C as E from AA", "[{E:'BC'},{E:'GH'}]");
            CheckResults(2, 4,"select C from AA where B<20", "[]");
            CheckResults(2, 5,"select C from AA where B>20", "[{C:'GH'}]");
            CheckResults(2, 6,"select count(C) from AA", "[{col1:2}]");
            Rollback();
            if (!commit)
            {
                Begin();
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
                Rollback();
            }
        }
        void Test3(int t)
        {
            if (t > 0 && t != 3)
                return;
            Begin();
            conn.ExecuteNonQuery("create table b(c integer,d string)");
            conn.ExecuteNonQuery("create primary index bx for b(c)");
            conn.ExecuteNonQuery("insert b values(45,'DE')");
            conn.ExecuteNonQuery("insert b values(-23,'HC')");
            CheckResults(3, 1,"select from b", "[{c:-23,d:'HC'},{c:45,d:'DE'}]");
            CheckResults(3, 2,"select from b where c=-23", "[{c:-23,d:'HC'}]");
            Rollback();
            if (!commit)
            {
                Begin();
                conn.CreateTable("b",
                    new SColumn("c", Types.SInteger),
                    new SColumn("d", Types.SString));
                conn.CreateIndex("b", IndexType.Primary, null, new string[] { "c" });
                conn.Insert("b", new string[0], new Serialisable[] { new SInteger(45), new SString("DE") },
                    new Serialisable[] { new SInteger(-23), new SString("HC") });
                CheckResults(new STable("b"), "[{c:-23,d:'HC'},{c:45,d:'DE'}]");
                Rollback();
            }
        }
        void Test4(int t)
        {
            if (t > 0 && t != 4)
                return;
            Begin();
            conn.ExecuteNonQuery("create table e(f integer,g string)");
            conn.ExecuteNonQuery("create primary index ex for e(f,g)");
            conn.ExecuteNonQuery("insert e values(23,'XC')");
            conn.ExecuteNonQuery("insert e values(45,'DE')");
            CheckResults(4, 1,"select from e", "[{f:23,g:'XC'},{f:45,g:'DE'}]");
            Rollback();
            if (!commit)
            {
                Begin();
                conn.CreateTable("e",
                    new SColumn("f", Types.SInteger),
                    new SColumn("g", Types.SString));
                conn.CreateIndex("e", IndexType.Primary, null, new string[] { "f", "g" });
                conn.Insert("e", new string[0], new Serialisable[] { new SInteger(23), new SString("XC") },
                    new Serialisable[] { new SInteger(45), new SString("DE") });
                CheckResults(new STable("e"), "[{f:23,g:'XC'},{f:45,g:'DE'}]");
                Rollback();
            }
        }
        void Test5(int t)
        {
            if (t > 0 && t != 5)
                return;
            Begin();
            conn.ExecuteNonQuery("create table a(b integer,c integer)");
            conn.ExecuteNonQuery("insert a values(17,15)");
            conn.ExecuteNonQuery("insert a values(23,6)");
            CheckResults(5, 1,"select from a", "[{b:17,c:15},{b:23,c:6}]");
            CheckResults(5, 2,"select b-3 as f,22 as g from a", "[{f:14,g:22},{f:20,g:22}]");
            CheckResults(5, 3,"select (a.b) as f,(c) from a", "[{f:17,c:15},{f:23,c:6}]");
            CheckResults(5, 4,"select b+3,d.c from a d", "[{col1:20,\"d.c\":15},{col1:26,\"d.c\":6}]");
            CheckResults(5, 5,"select (b as d,c) from a", "[{col1:{d:17,c:15}},{col1:{d:23,c:6}}]");
            CheckResults(5, 6,"select from a orderby c", "[{b:23,c:6},{b:17,c:15}]");
            CheckResults(5, 7,"select from a orderby b desc", "[{b:23,c:6},{b:17,c:15}]");
            CheckResults(5, 8,"select from a orderby b+c desc", "[{b:17,c:15},{b:23,c:6}]");
            CheckResults(5, 9,"select sum(b) from a", "[{col1:40}]");
            CheckResults(5, 10,"select max(c),min(b) from a", "[{col1:15,col2:17}]");
            CheckResults(5, 11,"select count(c) as d from a where b<20", "[{d:1}]");
            Rollback();
            if (!commit)
            {
                Begin();
                conn.CreateTable("a",
                    new SColumn("b", Types.SInteger),
                    new SColumn("c", Types.SInteger));
                conn.Insert("a", new string[0], new Serialisable[] { new SInteger(17), new SInteger(15) },
                    new Serialisable[] { new SInteger(23), new SInteger(6) });
                CheckResults(new STable("a"), "[{b:17,c:15},{b:23,c:6}]");
                Rollback();
            }
        }
        void Test6(int t)
        {
            if (t > 0 && t != 6)
                return;
            Begin();
            conn.ExecuteNonQuery("create table ta(b date,c timespan,d boolean)");
            conn.ExecuteNonQuery("insert ta values(date'2019-01-06T12:30:00',timespan'02:00:00',false)");
            CheckResults(6, 1, "select from ta", "[{b:\"2019-01-06T12:30:00\",c:\"02:00:00\",d:\"false\"}]");
            Rollback();
            if (!commit)
            {
                Begin();
                conn.CreateTable("a", new SColumn("b", Types.SDate), new SColumn("c", Types.STimeSpan),
                        new SColumn("d", Types.SBoolean));
                conn.Insert("a", new string[0], new Serialisable[] { new SDate(new DateTime(2019,01,06,12,30,0)),
            new STimeSpan(new TimeSpan(2,0,0)),SBoolean.False});
                CheckResults(new STable("a"), "[{b:\"2019-01-06T12:30:00\",c:\"02:00:00\",d:\"false\"}]");
                Rollback();
            }
        }
        void Test7(int t)
        {
            if (t > 0 && t != 7)
                return;
            Begin();
            conn.ExecuteNonQuery("create table TB(S string,D integer,C integer)");
            conn.ExecuteNonQuery("insert TB values('Glasgow',2,43)");
            conn.ExecuteNonQuery("insert TB values('Paisley',3,82)");
            conn.ExecuteNonQuery("insert TB values('Glasgow',4,29)");
            CheckResults(7, 1, "select S,count(C) as occ,sum(C) as total from TB groupby S",
                "[{S:\"Glasgow\",occ:2,total:72},{S:\"Paisley\",occ:1,total:82}]");
            Rollback();
        }
        void Test8(int t)
        {
            if (t > 0 && t != 8)
                return;
            Begin();
            conn.ExecuteNonQuery("create table A(B integer,C integer,D integer)");
            conn.ExecuteNonQuery("insert A values(4,2,43)");
            conn.ExecuteNonQuery("insert A values(8,3,82)");
            conn.ExecuteNonQuery("insert A values(7,4,29)");
            conn.ExecuteNonQuery("create table E(F integer,C integer,G integer)");
            conn.ExecuteNonQuery("insert E values(4,3,22)");
            conn.ExecuteNonQuery("insert E values(11,4,10)");
            conn.ExecuteNonQuery("insert E values(7,2,31)");
            CheckResults(8, 1, "select from A natural join E" ,
                "[{B:4,C:2,D:43,F:7,G:31},{B:8,C:3,D:82,F:4,G:22},{B:7,C:4,D:29,F:11,G:10}]");
            CheckResults(8, 2, "select D,G from A cross join E where D<G",
                "[{D:29,G:31}]");
            CheckResults(8, 3, "select B,D,G from A, E where B=F",
                "[{B:4,D:43,G:22},{B:7,D:29,G:31}]");
            CheckResults(8, 4, "select B,D,G from A H, E where H.C=E.C",
                "[{B:4,D:43,G:31},{B:8,D:82,G:22},{B:7,D:29,G:10}]");
            CheckResults(8, 5, "select from A inner join E on B=F",
                "[{B:4,\"A.C\":2,D:43,F:4,\"E.C\":3,G:22},"+
                "{B:7,\"A.C\":4,D:29,F:7,\"E.C\":2,G:31}]");
            CheckResults(8, 6, "select from A left join E on B=F",
    "[{B:4,\"A.C\":2,D:43,F:4,\"E.C\":3,G:22},{B:7,\"A.C\":4,D:29,F:7,\"E.C\":2,G:31}," +
    "{B:8,\"A.C\":3,D:82}]");
            CheckResults(8, 7, "select from A right join E on B=F",
    "[{B:4,\"A.C\":2,D:43,F:4,\"E.C\":3,G:22},{B:7,\"A.C\":4,D:29,F:7,\"E.C\":2,G:31}," +
    "{F:11,\"E.C\":4,G:10}]");
            CheckResults(8, 8, "select from A full join E on B=F",
    "[{B:4,\"A.C\":2,D:43,F:4,\"E.C\":3,G:22},{B:7,\"A.C\":4,D:29,F:7,\"E.C\":2,G:31}," +
    "{B:8,\"A.C\":3,D:82},{F: 11,\"E.C\":4,G:10}]");
            Rollback();
        }
        void Test9(int t)
        {
            if (t > 0 && t != 9)
                return;
            Begin();
            conn.ExecuteNonQuery("create table a(b integer,c numeric)");
            conn.ExecuteNonQuery("insert a values(12345678901234567890123456789,123.4567)");
            conn.ExecuteNonQuery("insert a values(0,123.4567e-15)");
            conn.ExecuteNonQuery("insert a values(12,1234)");
            conn.ExecuteNonQuery("insert a values(34,0.5678e9)");
            CheckResults(9, 1, "select from a", "[{\"b\": 12345678901234567890123456789, \"c\": 123.4567},{\"b\": 0, \"c\": 1.234567E-13},{\"b\": 12, \"c\": 1234},{\"b\": 34, \"c\": 567800000}]");
            Rollback();
            Begin();
            conn.CreateTable("a", new SColumn("b", Types.SInteger), new SColumn("c", Types.SNumeric));
            conn.Insert("a", new string[0], new Serialisable[] {
                new SInteger(Integer.Parse("12345678901234567890123456789")),
                new SNumeric(Numeric.Parse("123.4567"))});
            conn.Insert("a", new string[0], new Serialisable[] {
                new SInteger(0),new SNumeric(Numeric.Parse("123.4567e-15"))});
            conn.Insert("a", new string[0], new Serialisable[] {
                new SInteger(12),new SNumeric(1234,4,0)});
            conn.Insert("a", new string[0], new Serialisable[] {
                new SInteger(34),new SNumeric(new Numeric(0.5678e9))});
            CheckResults(9, 2, "select from a", "[{\"b\": 12345678901234567890123456789, \"c\": 123.4567},{\"b\": 0, \"c\": 1.234567E-13},{\"b\": 12, \"c\": 1234},{\"b\": 34, \"c\": 567800000}]");
            Rollback();
        }
        /// <summary>
        /// Check transaction clficts for read transactions
        /// </summary>
        /// <param name="t"></param>
        async void Test10(int t)
        {
            if (t > 0 && t != 10)
                return;
            try
            {
                conn.ExecuteNonQuery("create table RDC(A integer,B string)");
                conn.ExecuteNonQuery("create primary index RX for RDC(A)");
            }
            catch (Exception) { }
            conn.ExecuteNonQuery("delete RDC");
            conn.ExecuteNonQuery("insert RDC values(42,'Life, the Universe')");
            conn.ExecuteNonQuery("insert RDC values(52,'Weeks in the year')");
            Begin();
            conn.ExecuteQuery("select from RDC where A=42");
            var task1 = new Task(() => Test10A());
            task1.Start();
            await Task.WhenAll(task1);
            CheckExceptionNonQuery(10, 1, "Commit", "Transaction conflict with read");
            Begin();
            conn.ExecuteQuery("select from RDC where A=52");
            var task2 = new Task(() => Test10B());
            task2.Start();
            await Task.WhenAll(task2);
            conn.Commit();
            Begin();
            CheckResults(10,2,"select from RDC","[{A:42,B:'the product of 6 and 7'},{A:52,B:'Weeks in the year'}]");
            task1 = new Task(() => Test10A());
            task1.Start();
            await Task.WhenAll(task1);
            CheckExceptionNonQuery(10, 3, "Commit", "Transaction conflict with read"); ;
        }
        void Test10A()
        {
            var conn1 = new StrongConnect("127.0.0.1", 50433, "testdb");
            conn1.ExecuteNonQuery("update RDC where A=42 set B='the product of 6 and 9'");
        }
        void Test10B()
        {
            var conn1 = new StrongConnect("127.0.0.1", 50433, "testdb");
            conn1.ExecuteNonQuery("update RDC where A=42 set B='the product of 6 and 7'");
        }
        void CheckExceptionQuery(int t, int q, string c, string m)
        {
            if (qry > 0 && qry != q)
                return;
            try
            {
                conn.ExecuteQuery(c);
            }
            catch (Exception e)
            {
                if (e.Message.CompareTo(m) == 0)
                    return;
                Console.WriteLine("Unexpected exception (" + t + " " + q + ") " + e.Message);
                return;
            }
            Console.WriteLine("Didnt get exception (" + t + " " + q + ") " + m);
        }
        void CheckExceptionNonQuery(int t, int q, string c, string m)
        {
            if (qry > 0 && qry != q)
                return;
            try
            {
                conn.ExecuteNonQuery(c);
            }
            catch (Exception e)
            {
                if (e.Message.CompareTo(m) == 0)
                    return;
                Console.WriteLine("Unexpected exception (" + t + " " + q + ") " + e.Message);
                return;
            }
            Console.WriteLine("Didnt get exception (" + t + " " + q + ") " + m);
        }
        void CheckResults(int t,int q,string c,string d)
        {
            if (qry > 0 && qry != q)
                return;
            try
            {
                Check(conn.ExecuteQuery(c), new DocArray(d));
            } catch(Exception e)
            {
                Console.WriteLine("Exception (" + t + " " + q + ") " + e.Message);
            }
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
                    if (sp.Value is decimal && cf is decimal)
                    {
                        var ss = sp.Value?.ToString()??"";
                        var cs = cf.ToString();
                        for (var i=0;i<ss.Length && i<cs.Length;i++)
                            if (ss[i]!=cs[i])
                            {
                                Console.WriteLine("Decimal values " +
                                    cf + " and " + ss + " differ at position " + i);
                                break;
                            }
                        nc++;
                    }
                    else if (cf == sp.Value || cf?.ToString() == sp.Value?.ToString())
                        nc++;
                    else
                        throw new Exception("Values do not match");
                }
            if (nc != c.fields.Count)
                throw new Exception("Missing field(s)");
        }
    }
}
