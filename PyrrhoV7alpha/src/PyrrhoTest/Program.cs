using System;
using Pyrrho;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Test
{
    class Program
    {
        static int test = 0, qry = 0;
        bool commit = false;
        PyrrhoConnect conn;
        PyrrhoTransaction tr;
        Program(string[] args)
        {

            conn = new PyrrhoConnect("Files=testdb");
            conn.Open();
            if (args.Length >= 2)
            {
                test = int.Parse(args[0]);
                qry = int.Parse(args[1]);
            }
            if (args.Length == 3)
                commit = true;
        }
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("2 September 2019 Respeatable tests");
                Console.WriteLine("Ensure testdb not present in database folder for any of");
                Console.WriteLine("Test"); 
                Console.WriteLine("Test 10 0");
                Console.WriteLine("Test 0 0 commit");
                Console.WriteLine("The next message should be 'Testing complete'");
                new Program(args).Tests();
                Console.WriteLine("Testing complete");
                Console.ReadLine();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message);
                Console.ReadLine();
            }
        }
        void Begin()
        {
            if (!commit)
                tr = (PyrrhoTransaction)conn.BeginTransaction();
        }
        void Rollback()
        {
            if (!commit)
            {
                tr?.Rollback();
                tr = null;
            }
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
            Test11(test);
            Test12(test);
            Test13(test);
        }
        void Test1(int t)
        {
            if (t > 0 && t != 1)
                return;
            Begin();
            conn.Act("create table A(B int,C int,D char,primary key(B,C))");
            conn.Act("insert into A values(2,3,'TwentyThree')");
            conn.Act("insert into A values(1,9,'Nineteen')");
            CheckResults(1,1,"select * from A", "[{B:1,C:9,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            conn.Act("update A set C=19 where C=9");
            CheckResults(1,2, "select * from A", "[{B:1,C:19,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            conn.Act("delete from A where C=19");
            CheckResults(1, 3, "select * from A", "[{B:2,C:3,D:'TwentyThree'}]");
            CheckExceptionNonQuery(1, 4, "insert into A values(2,3,'What?')","Integrity constraint: duplicate key A(2,3)");
            Rollback();
        }
        void Test2(int t)
        {
            if (t > 0 && t != 2)
                return;
            Begin();
            conn.Act("create table AA(B int,C char)");
            conn.Act("insert into AA(B) values(17)");
            conn.Act("insert into AA(C) values('BC')");
            conn.Act("insert into AA(C,B) values('GH',+67)");
            CheckResults(2,1,"select * from AA", "[{B:17},{C:'BC'},{B:67,C:'GH'}]");
            CheckResults(2,2,"select B from AA", "[{B:17},{B:67}]");
            CheckResults(2, 3,"select C as E from AA", "[{E:'BC'},{E:'GH'}]");
            CheckResults(2, 4,"select C from AA where B<20", "[]");
            CheckResults(2, 5,"select C from AA where B>20", "[{C:'GH'}]");
            CheckResults(2, 6,"select count(C) from AA", "[{COUNT:2}]");
            Rollback();
        }
        void Test3(int t)
        {
            if (t > 0 && t != 3)
                return;
            Begin();
            conn.Act("create table b(c int primary key,d char)");
            conn.Act("insert into b values(45,'DE')");
            conn.Act("insert into b values(-23,'HC')");
            CheckResults(3, 1,"select * from b", "[{C:-23,D:'HC'},{C:45,D:'DE'}]");
            CheckResults(3, 2,"select * from b where c=-23", "[{C:-23,D:'HC'}]");
            Rollback();
        }
        void Test4(int t)
        {
            if (t > 0 && t != 4)
                return;
            Begin();
            conn.Act("create table e(f int,g char,primary key(g,f))");
            conn.Act("insert into e values(23,'XC')");
            conn.Act("insert into e values(45,'DE')");
            CheckResults(4, 1,"select * from e", "[{F:45,G:'DE'},{F:23,G:'XC'}]");
            conn.Act("insert into e(g) values('DE')");
            CheckResults(4, 2, "select * from e",
                "[{F:45,G:'DE'},{F:46,G:'DE'},{F:23,G:'XC'}]");
            // the EvalRowSet loop in the next test should execute only once
            CheckResults(4, 3, "select count(f) from e where g='DE' and f<=45",
                "[{COUNT:1}]");
            Rollback();
        }
        void Test5(int t)
        {
            if (t > 0 && t != 5)
                return;
            Begin();
            conn.Act("create table a(b int,c int)");
            conn.Act("insert into a values(17,15)");
            conn.Act("insert into a values(23,6)");
            CheckResults(5, 1,"select * from a", "[{B:17,C:15},{B:23,C:6}]");
            CheckResults(5, 2,"select b-3 as f,22 as g from a", "[{F:14,G:22},{F:20,G:22}]");
            CheckResults(5, 3,"select (a.b) as f,(c) from a", "[{F:17,col2:15},{F:23,col2:6}]");
            CheckResults(5, 4,"select b+3,d.c from a d", "[{col1:20,C:15},{col1:26,C:6}]");
            CheckResults(5, 5,"select (b as d,c) from a", "[{col1:{D:17,C:15}},{col1:{D:23,C:6}}]");
            CheckResults(5, 6,"select * from a order by c", "[{B:23,C:6},{B:17,C:15}]");
            CheckResults(5, 7,"select * from a order by b desc", "[{B:23,C:6},{B:17,C:15}]");
            CheckResults(5, 8,"select * from a order by b+c desc", "[{B:17,C:15},{B:23,C:6}]");
            CheckResults(5, 9,"select sum(b) from a", "[{col1:40}]");
            CheckResults(5, 10,"select max(c),min(b) from a", "[{col1:15,col2:17}]");
            CheckResults(5, 11,"select count(c) as d from a where b<20", "[{D:1}]");
            Rollback();
        }
        void Test6(int t)
        {
            if (t > 0 && t != 6)
                return;
            Begin();
            conn.Act("create table ta(b date,c timespan,d boolean)");
            conn.Act("insert into ta values(date'2019-01-06T12:30:00',timespan'02:00:00',false)");
            CheckResults(6, 1, "select * from ta", "[{B:\"2019-01-06T12:30:00\",C:\"02:00:00\",D:\"false\"}]");
            Rollback();
        }
        void Test7(int t)
        {
            if (t > 0 && t != 7)
                return;
            Begin();
            conn.Act("create table TB(S char,D int,C int)");
            conn.Act("insert into TB values('Glasgow',2,43)");
            conn.Act("insert into TB values('Paisley',3,82)");
            conn.Act("insert into TB values('Glasgow',4,29)");
            CheckResults(7, 1, "select S,count(C) as occ,sum(C) as total from TB groupby S",
                "[{S:\"Glasgow\",OCC:2,TOTAL:72},{S:\"Paisley\",OCC:1,TOTAL:82}]");
            Rollback();
        }
        void Test8(int t)
        {
            if (t > 0 && t != 8)
                return;
            Begin();
            conn.Act("create table JA(B int,C int,D int)");
            conn.Act("insert into JA values(4,2,43)");
            conn.Act("insert into JA values(8,3,82)");
            conn.Act("insert into JA values(7,4,29)");
            conn.Act("create table JE(F int,C int,G int)");
            conn.Act("insert into JE values(4,3,22)");
            conn.Act("insert into JE values(11,4,10)");
            conn.Act("insert into JE values(7,2,31)");
            CheckResults(8, 1, "select * from JA natural join JE" ,
                "[{B:4,C:2,D:43,F:7,G:31},{B:8,C:3,D:82,F:4,G:22},{B:7,C:4,D:29,F:11,G:10}]");
            CheckResults(8, 2, "select D,G from JA cross join JE where D<G",
                "[{D:29,G:31}]");
            CheckResults(8, 3, "select B,D,G from JA, JE where B=F",
                "[{B:4,D:43,G:22},{B:7,D:29,G:31}]");
            CheckResults(8, 4, "select B,D,G from JA H, JE where H.C=JE.C",
                "[{B:4,D:43,G:31},{B:8,D:82,G:22},{B:7,D:29,G:10}]");
            CheckResults(8, 5, "select * from JA inner join JE on B=F",
                "[{B:4,\"JA.C\":2,D:43,F:4,\"JE.C\":3,G:22},"+
                "{B:7,\"JA.C\":4,D:29,F:7,\"JE.C\":2,G:31}]");
            CheckResults(8, 6, "select * from JA left join JE on B=F",
    "[{B:4,\"JA.C\":2,D:43,F:4,\"JE.C\":3,G:22},{B:7,\"JA.C\":4,D:29,F:7,\"JE.C\":2,G:31}," +
    "{B:8,\"JA.C\":3,D:82}]");
            CheckResults(8, 7, "select * from JA right join JE on B=F",
    "[{B:4,\"JA.C\":2,D:43,F:4,\"JE.C\":3,G:22},{B:7,\"JA.C\":4,D:29,F:7,\"JE.C\":2,G:31}," +
    "{F:11,\"JE.C\":4,G:10}]");
            CheckResults(8, 8, "select * from JA full join JE on B=F",
    "[{B:4,\"JA.C\":2,D:43,F:4,\"JE.C\":3,G:22},{B:7,\"JA.C\":4,D:29,F:7,\"JE.C\":2,G:31}," +
    "{B:8,\"JA.C\":3,D:82},{F: 11,\"JE.C\":4,G:10}]");
            Rollback();
        }
        void Test9(int t)
        {
            if (t > 0 && t != 9)
                return;
            Begin();
            conn.Act("create table ba(b int,c numeric)");
            conn.Act("insert into ba values(12345678901234567890123456789,123.4567)");
            conn.Act("insert into ba values(0,123.4567e-15)");
            conn.Act("insert into ba values(12,1234)");
            conn.Act("insert into ba values(34,0.5678e9)");
            CheckResults(9, 1, "select * from ba", 
                "[{B: 12345678901234567890123456789, C: 123.4567},{B: 0, C: 1.234567E-13},"
                +"{B: 12, C: 1234},{B: 34, C: 567800000}]");
            Rollback();
        }

        /// <summary>
        /// Check transaction conflicts for read transactions
        /// </summary>
        /// <param name="t"></param>
        void Test10(int t)
        {
            if (commit || (t > 0 && t != 10)) //This test runs if commit is false
                return;
            try
            {
                conn.Act("create table RDC(A int primary key,B char)");
            }
            catch (Exception) {
            }
            conn.Act("delete from RDC");
            conn.Act("insert into RDC values(42,'Life, the Universe')");
            conn.Act("insert into RDC values(52,'Weeks in the year')");
            Begin();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "select * from RDC where A=42";
            var r = cmd.ExecuteReader();
            r.Close();
            var task1 = Task.Factory.StartNew(() => Test10A());
            task1.Wait();
            CheckExceptionNonQuery(10, 1, "Commit", "Transaction conflict");
            Begin();
            cmd.CommandText = "select * from RDC where A=52";
            r = cmd.ExecuteReader();
            r.Close();
            var task2 = Task.Factory.StartNew(() => Test10B());
            task2.Wait();
            tr?.Commit();
            tr = null;
            Begin();
            CheckResults(10,2,"select * from RDC","[{A:42,B:'the product of 6 and 7'},{A:52,B:'Weeks in the year'}]");
            task1 = Task.Factory.StartNew(() => Test10A());
            task1.Wait();
            CheckExceptionNonQuery(10, 3, "Commit", "Transaction conflict");
        }
        void Test10A()
        {
            var p = new Program(new string[0]);
            p.conn.Act("update RDC set B='the product of 6 and 9' where A=42");
            p.conn.Close();
        }
        void Test10B()
        {
            var p = new Program(new string[0]);
            p.conn.Act("update RDC set B='the product of 6 and 7' where A=42");
            p.conn.Close();
        }
        void Test11(int t)
        {
            if (commit || (t > 0 && t != 11)) // this test only runs if commit is false
                return;
            if (qry == 0 || qry == 1)
            {
                Begin();
                conn.Act("create table cs(b int notnull,c int default 4,d int generated b+c)");
                CheckExceptionNonQuery(11, 1, "insert into cs(c) values(5)", "Value of b cannot be null");
            }
            if (qry == 0 || qry == 2)
            {
                Begin();
                conn.Act("create table cs(b int notnull,c int default 4,d int generated b+c)");
                conn.Act("insert into cs(b) values(3)");
                CheckExceptionNonQuery(11, 2, "insert into cs values(1,2,3)", "Illegal value for generated column");
            }
            if (qry == 0 || qry == 3)
            {
                Begin();
                conn.Act("create table cs(b int notnull,c int default 4,d int generated b+c)");
                conn.Act("insert into cs(b) values(3)");
                CheckResults(11, 3, "select * from cs", "[{B:3,C:4,D:7}]");
            }
            Rollback();
        }
        void Test12(int t)
        {
            if (t > 0 && t != 12)
                return;
            Begin();
            conn.Act("create table sce(a int,b char)");
            conn.Act("insert into sce values(12,'Zodiac')");
            conn.Act("insert into sce values(13,'Bakers')");
            conn.Act("insert into sce values(14,'Fortnight')");
            conn.Act("create table dst(c int)");
            conn.Act("insert into dst select a from sce where b<'H'");
            CheckResults(12, 1, "select * from dst", "[{C:13},{C:14}]");
            CheckResults(12, 2, "select a from sce where b in('Fortnight','Zodiac')",
                "[{a:12},{a:14}]");
            CheckResults(12, 3, "select * from dst where c in (select a from sce where b='Bakers')",
                "[{c:13}]");
            conn.Act("insert into dst(c) select max(x.a)+4 from sce x where x.b<'H'");
            CheckResults(12, 4, "select * from dst", "[{C:13},{C:14},{C:18}]");
            conn.Act("insert into dst select min(x.c)-3 from dst x");
            CheckResults(12, 5, "select * from dst", "[{C:13},{cC:14},{C:18},{C:10}]");
            Rollback();
        }
        void Test13(int t)
        {
            if (t > 0 && t != 13)
                return;
            Begin();
            conn.Act("create table ad(a int,b char)");
            conn.Act("insert into ad values(20,'Twenty')");
            if (qry == 0 || qry == 1)
            {
                CheckExceptionNonQuery(13, 1, "alter ad add c char notnull", "Table is not empty");
                if (!commit)
                {
                    Begin();
                    conn.Act("create table ad(a int,b char)");
                    conn.Act("insert into ad values(20,'Twenty')");
                }
            }
            conn.Act("alter ad add c char default 'XX'");
            CheckResults(13, 2, "select * from ad", "[{A:20,B:'Twenty',C:'XX'}]");
            conn.Act("alter ad drop b");
            CheckResults(13,3,"select * from ad", "[{A:20,C:'XX'}]");
            conn.Act("alter ad add primary key(a)");
            conn.Act("insert into ad values(21,'AB')");
            conn.Act("create table de (d int references ad)");
            if (qry == 0 || qry == 4)
            {
                CheckExceptionNonQuery(13, 4, "insert into de values(14)", "Referential constraint violation");
                if (!commit)
                {
                    Begin();
                    conn.Act("create table ad(a int,b char)");
                    conn.Act("insert into ad values(20,'Twenty')");
                    conn.Act("alter ad add c char default 'XX'");
                    conn.Act("alter ad drop b");
                    conn.Act("alter ad add primary key(a)");
                    conn.Act("insert into ad values(21,'AB')");
                    conn.Act("create table de (d int references ad)");
                }
            }
            conn.Act("insert into de values(21)");
            if (qry == 0 || qry == 5)
            {
                CheckExceptionNonQuery(13, 5, "delete from ad where c='AB'", "Referential constraint: illegal delete");
                if (!commit)
                {
                    Begin();
                    conn.Act("create table ad(a int,b char)");
                    conn.Act("insert into ad values(20,'Twenty')");
                    conn.Act("alter ad add c char default 'XX'");
                    conn.Act("alter ad drop b");
                    conn.Act("alter ad add primary key(a)");
                    conn.Act("insert into ad values(21,'AB')");
                    conn.Act("create table de (d int references ad)");
                    conn.Act("insert into de values(21)");
                }
            }
            if (qry == 0 || qry == 6)
            {
                CheckExceptionNonQuery(13, 6, "drop ad", "Restricted by reference");
                if (!commit)
                {
                    Begin();
                    conn.Act("create table ad(a int,b char)");
                    conn.Act("insert into ad values(20,'Twenty')");
                    conn.Act("alter ad add c char default 'XX'");
                    conn.Act("alter ad drop b");
                    conn.Act("alter ad add primary key(a)");
                    conn.Act("insert into ad values(21,'AB')");
                    conn.Act("create table de (d int references ad)");
                    conn.Act("insert into de values(21)");
                }
            }
            conn.Act("alter ad column c drop default");
            CheckResults(13,7,"select * from ad", "[{A:20},{A:21,C:'AB'}]");
            if (qry == 0 || qry == 8)
            {
                CheckExceptionNonQuery(13, 8, "alter ad drop key(a)", "Restricted by reference");
                if (!commit)
                {
                    Begin();
                    conn.Act("create table ad(a int,b char)");
                    conn.Act("insert into ad values(20,'Twenty')");
                    conn.Act("alter ad add c char default 'XX'");
                    conn.Act("alter ad drop b");
                    conn.Act("alter ad add primary key(a)");
                    conn.Act("insert into ad values(21,'AB')");
                    conn.Act("create table de (d int references ad)");
                    conn.Act("insert into de values(21)");
                    conn.Act("alter ad column c drop default");
                }
            }
            conn.Act("drop de");
            conn.Act("alter ad drop key(a)");
            // we don't get 'XX' here because the DEFAULT was not used inm an insert into ??
            CheckResults(13,9, "select * from ad", "[{A:20},{A:21,C:'AB'}]");
            conn.Act("insert ad(a) values(13)");
            CheckResults(13, 10, "select * from ad", "[{A:20},{A:21,C:'AB'},{A:13}]");
            conn.Act("drop ad");
            if (qry == 0 || qry == 11)
            {
                CheckExceptionQuery(13, 11, "select * from ad", "No table ad");
                Begin();
            }
            Rollback();
        }
        void CheckExceptionQuery(int t, int q, string c, string m)
        {
            if (qry > 0 && qry != q)
                return;
            try
            {
                var cmd = conn.CreateCommand();
                cmd.CommandText = c;
                var r = cmd.ExecuteReader();
                r.Close();
            }
            catch (Exception e)
            {
                if (e.Message.StartsWith(m))
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
                conn.Act(c);
            }
            catch (Exception e)
            {
                if (e.Message.StartsWith(m))
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
                var cmd = conn.CreateCommand();
                cmd.CommandText = c;
                var r = (PyrrhoReader)cmd.ExecuteReader();
                var da = new DocArray();
                while (r?.Read()==true)
                {
                    var rd = new Document();
                    for (var i = 0; i < r.FieldCount; i++)
                        if (r[i]!=DBNull.Value)
                            rd.fields.Add(new KeyValuePair<string,object>(r.GetName(i), r[i]));
                    da.items.Add(rd);
                }
                r?.Close();
                Check(da, new DocArray(d));
            } catch(Exception e)
            {
                Console.WriteLine("Exception (" + t + " " + q + ") " + e.Message);
            }
        }
        void Check(DocArray s,DocArray c)
        {
            if (s.items.Count != c.items.Count)
                throw new Exception("Different number of rows");
            for (var i = 0; i < s.items.Count; i++)
                Check((Document)s.items[i], (Document)c.items[i]);
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
