using System;
using Pyrrho;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Security.Principal;

namespace Test
{
    class Program
    {
        static int test = 0, qry = 0,testing=0,cur=0;
        bool commit = false;
        PyrrhoConnect conn,connA;
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
                Console.WriteLine("11 May 2021 Repeatable tests");
                if (args.Length == 0)
                {
                    Console.WriteLine("Tests 22,23 need Server with +s");
                    Console.WriteLine("Ensure A,DB,DC,testdb not present in database folder for any of");
                    Console.WriteLine("PyrrhoTest");
                    Console.WriteLine("PyrrhoTest 10 0");
                    Console.WriteLine("PyrrhoTest 0 0 commit");
                    Console.WriteLine("The next message should be 'Testing complete'");
                }
                new Program(args).Tests();
                Console.WriteLine("Testing complete");
                Console.ReadLine();
            }
            catch (Exception e)
            {
                Console.WriteLine("Test " +testing + "," +cur + " Exception: " + e.Message);
                Console.ReadLine();
            }
        }
        void Begin()
        {
            if (!commit)
                tr = conn.BeginTransaction();
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
            Test1();
            Test2();
            Test3();
            Test4();
            Test5();
            Test6();
            Test7();
            Test8();
            Test9(); 
            Test10();
            Test11();
            Test12();
            Test13();
            Test14();
            Test15();
            Test16();
            Test17();
            Test18();
            Test19();
            Test20();
            Test21();
            if (test == 0 || test > 21)
            {
                connA = new PyrrhoConnect("Files=A");
                connA.Act("create table D(e int primary key,f char,g char)");
                connA.Act("insert into D values (1,'Joe','Soap'), (2,'Betty','Boop')");
                connA.Act("create role A");
                connA.Act("grant A to \"" + WindowsIdentity.GetCurrent().Name+"\"");
                Test22();
                ResetA();
                Test23();
            }
      //      Test24();
        }
        void ResetA()
        {
            connA.Act("delete from D");
            connA.Act("insert into D values (1,'Joe','Soap'), (2,'Betty','Boop')");
        }
        void Test1()
        {
            if (test > 0 && test != 1)
                return;
            testing = 1;
            Begin();
            conn.Act("create table A(B int,C int,D char,primary key(B,C))");
            conn.Act("insert into A values(2,3,'TwentyThree')");
            conn.Act("insert into A values(1,9,'Nineteen')");
            CheckResults(1,1,"select * from A", "[{B:1,C:9,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            conn.Act("update A set C=19 where C=9");
            CheckResults(1,2, "select * from A", "[{B:1,C:19,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            conn.Act("delete from A where C=19");
            CheckResults(1, 3, "select * from A", "[{B:2,C:3,D:'TwentyThree'}]");
            conn.Act("insert into A(B,D) (select E.B,upper(E.D) from A E)");
            CheckResults(1, 5, "table A", "[{B:2,C:3,D:'TwentyThree'},{B:2,C:4,D:'TWENTYTHREE'}]");
            Rollback();
        }
        void Test2()
        {
            if (test > 0 && test != 2)
                return;
            testing = 2;
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
        void Test3()
        {
            if (test > 0 && test != 3)
                return;
            testing = 3;
            Begin();
            conn.Act("create table b(c int primary key,d char)");
            conn.Act("insert into b values(45,'DE')");
            conn.Act("insert into b values(-23,'HC')");
            CheckResults(3, 1,"select * from b", "[{C:-23,D:'HC'},{C:45,D:'DE'}]");
            CheckResults(3, 2,"select * from b where c=-23", "[{C:-23,D:'HC'}]");
            Rollback();
        }
        void Test4()
        {
            if (test > 0 && test != 4)
                return;
            testing = 4;
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
        void Test5()
        {
            if (test > 0 && test != 5)
                return;
            testing = 5;
            Begin();
            conn.Act("create table f(b int,c int)");
            conn.Act("insert into f values(17,15)");
            conn.Act("insert into f values(23,6)");
            CheckResults(5, 1,"select * from f", "[{B:17,C:15},{B:23,C:6}]");
            CheckResults(5, 2,"select b-3 as h,22 as g from f", "[{H:14,G:22},{H:20,G:22}]");
            CheckResults(5, 3,"select (f.b) as h,(c) from f", "[{H:17,C:15},{H:23,C:6}]");
            CheckResults(5, 4,"select b+3,d.c from f d", "[{Col0:20,C:15},{Col0:26,C:6}]");
            CheckResults(5, 5,"select (b as d,c) from f", "[{Col0:(D=17,C=15)},{Col0:(D=23,C=6)}]");
            CheckResults(5, 6,"select * from f order by c", "[{B:23,C:6},{B:17,C:15}]");
            CheckResults(5, 7,"select * from f order by b desc", "[{B:23,C:6},{B:17,C:15}]");
            CheckResults(5, 8,"select * from f order by b+c desc", "[{B:17,C:15},{B:23,C:6}]");
            CheckResults(5, 9,"select sum(b) from f", "[{SUM:40}]");
            CheckResults(5, 10,"select max(c),min(b) from f", "[{MAX:15,MIN:17}]");
            CheckResults(5, 11,"select count(c) as d from f where b<20", "[{D:1}]");
            Rollback();
        }
        void Test6()
        {
            if (test > 0 && test != 6)
                return;
            testing = 6;
            Begin();
            conn.Act("create table ta(b date,c interval hour to second,d boolean)");
            conn.Act("insert into ta values(date'2019-01-06T12:30:00',interval'02:00:00'hour to second,false)");
            var d = new DateTime(2019, 1, 6);
            CheckResults(6, 1, "select * from ta", "[{B:\""+d.ToString("d")+"\",C:\"2h\",D:\"False\"}]");
            Rollback();
        }
        void Test7()
        {
            if (test > 0 && test != 7)
                return;
            testing = 7;
            Begin();
            conn.Act("create table TB(S char,D int,C int)");
            conn.Act("insert into TB values('Glasgow',2,43)");
            conn.Act("insert into TB values('Paisley',3,82)");
            conn.Act("insert into TB values('Glasgow',4,29)");
            CheckResults(7, 1, "select S,count(C) as occ,sum(C) as total from TB group by S",
                "[{S:\"Glasgow\",OCC:2,TOTAL:72},{S:\"Paisley\",OCC:1,TOTAL:82}]");
            conn.Act("create table TBB(b int,c int,d int, e int)");
            conn.Act("insert into TBB values(1,21,31,41),(3,22,35,47),(1,22,33,42),(1,22,35,49)");
            conn.Act("insert into TBB values(2,21,37,40),(2,23,33,43),(1,24,31,44),(2,26,37,45)");
            CheckResults(7, 2, "select sum(g) as h,d from (select count(*) as g,b,d from TBB group by b,d) group by d having b<3",
                "[{H:2,D:31},{H:2,D:33},{H:1,D:35},{H:2,D:37}]");
            Rollback();
        }
        void Test8()
        {
            if (test > 0 && test != 8)
                return;
            testing = 8;
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
            // Test for lateral join (the keyword LATERAL is not required and not supported)
            conn.Act("create table SalesPerson(pid int primary key)");
            conn.Act("insert into SalesPerson values(1),(2),(3)"); 
            conn.Act("create table Sales(sid int primary key, spid int, cust int, amount int)"); 
            conn.Act("insert into Sales values(4,3,10,22),(5,2,11,12),(6,2,10,37)"); 
            conn.Act("insert into Sales values(7,1,12,7),(8,3,13,41),(9,1,12,17)");
            CheckResults(8, 9, "select * from SalesPerson,"+
                "(select cust, amount from Sales " +
                "where spid = pid order by amount desc fetch first 1 rows only)",
                "[{PID:1,CUST:12,AMOUNT:17},{PID:2,CUST:10,AMOUNT:37},{PID:3,CUST:13,AMOUNT:41}]");
            Rollback();
        }
        void Test9()
        {
            if (test > 0 && test != 9)
                return;
            testing = 9;
            Begin();
            conn.Act("create table ba(b int,c real,d numeric)");
            conn.Act("insert into ba values(12345678901234567890123456789,123.4567,0.1234)");
            conn.Act("insert into ba values(0,123.4567e-15,1234)");
            conn.Act("insert into ba values(12,1234.0,0.00045)");
            conn.Act("insert into ba values(34,0.5678e9,0)");
            CheckResults(9, 1, "select * from ba", 
                "[{B: \"12345678901234567890123456789\", C: 123.4567, D: 0.1234}," +
                "{B: 0, C: 1.234567E-13, D: 1234},{B: 12, C: 1234.0,D: 0.00045}," +
                "{B: 34, C: 567800000, D: 0}]");
            Rollback();
        }

        /// <summary>
        /// Check transaction conflicts for read transactions
        /// </summary>
        /// <param name="t"></param>
        void Test10()
        {
            if (commit || (test > 0 && test != 10)) //This test runs if commit is false
                return;
            testing = 10;
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
        void Test11()
        {
            if (commit || (test > 0 && test != 11)) // this test only runs if commit is false
                return;
            testing = 11;
            if (qry == 0 || qry == 1)
            {
                Begin();
                conn.Act("create table cs(b int not null,c int default 4,d int generated always as b+c)");
                CheckExceptionNonQuery(11, 1, "insert into cs(c) values(5)", "Null value not allowed in column B");
            }
            if (qry == 0 || qry == 2)
            {
                Begin();
                conn.Act("create table cs(b int not null,c int default 4,d int generated always as (b+c))");
                conn.Act("insert into cs(b) values(3)");
                CheckExceptionNonQuery(11, 2, "insert into cs values(1,2,3)", "Attempt to assign to a non-updatable column");
            }
            if (qry == 0 || qry == 3)
            {
                Begin();
                conn.Act("create table cs(b int not null,c int default 4,d int generated always as (b+c))");
                conn.Act("insert into cs(b) values(3)");
                CheckResults(11, 3, "select * from cs", "[{B:3,C:4,D:7}]");
            }
            Rollback();
        }
        void Test12()
        {
            if (test > 0 && test != 12)
                return;
            testing = 12;
            Begin();
            conn.Act("create table sce(a int,b char)");
            conn.Act("insert into sce values(12,'Zodiac')");
            conn.Act("insert into sce values(13,'Bakers')");
            conn.Act("insert into sce values(14,'Fortnight')");
            conn.Act("create table dst(c int)");
            conn.Act("insert into dst (select a from sce where b<'H')");
            CheckResults(12, 1, "select * from dst", "[{C:13},{C:14}]");
            CheckResults(12, 2, "select a from sce where b in('Fortnight','Zodiac')",
               "[{A:12},{A:14}]");
            CheckResults(12, 3, "select * from dst where c in (select a from sce where b='Bakers')",
                "[{C:13}]");
            conn.Act("insert into dst(c) (select max(x.a)+4 from sce x where x.b<'H')");
            CheckResults(12, 4, "select * from dst", "[{C:13},{C:14},{C:18}]");
            conn.Act("insert into dst (select min(x.c)-3 from dst x)");
            CheckResults(12, 5, "select * from dst", "[{C:13},{C:14},{C:18},{C:10}]");
            if (commit)
            {
                conn.Prepare("Ins1", "insert into sce values(?,?)");
                conn.Prepare("Upd1", "update sce set a=? where b=?");
                conn.Prepare("Del1", "delete from dst where c>?");
                conn.Prepare("Ins2", "insert into dst (select char_length(b) from sce where a=?)");
                conn.Prepare("Sel1", "select * from dst where c<?");
                conn.Execute("Ins1", "" + 5, "'HalfDozen'");
                conn.Execute("Upd1", "" + 6, "'HalfDozen'");
                conn.Execute("Del1", "" + 10);
                conn.Execute("Ins2", "" + 6);
                CheckExecuteResults(12, 6, "[{C:9}]", "Sel1", "" + 10);
            }
            conn.Act("create table p(q int primary key,r char,a int)");
            conn.Act("create view v as select q,r as s,a from p");
            conn.Act("insert into v(s) values('Twenty'),('Thirty')");
            conn.Act("update v set s='Forty two' where q=1");
            CheckResults(12, 7, "select r from p", "[{R:'Forty two'},{R:'Thirty'}]");
            conn.Act("delete from v where s='Thirty'");
            conn.Act("insert into p(r) values('Fifty')");
            conn.Act("create table t(s char,u int)");
            conn.Act("insert into t values('Forty two',42),('Fifty',48)");
            conn.Act("create view w as select * from t natural join v");
            conn.Act("update w set u=50,a=21 where q=2");
            CheckResults(12,8,"table p", "[{Q:1,R:'Forty two'},{Q:2,R:'Fifty',A:21}]");
            CheckResults(12,9, "table t", "[{S:'Forty two',U:42},{S:'Fifty',U:50}]");
            Rollback();
        }
        void Test13()
        {
            if (test > 0 && test != 13)
                return;
            testing = 13;
            Begin();
            conn.Act("create table ad(a int,b char)");
            conn.Act("insert into ad values(20,'Twenty')");
            if (qry == 0 || qry == 1)
            {
                CheckExceptionNonQuery(13, 1, "alter table ad add c char not null", "Table is not empty");
                if (!commit)
                {
                    Begin();
                    conn.Act("create table ad(a int,b char)");
                    conn.Act("insert into ad values(20,'Twenty')");
                }
            }
            conn.Act("alter table ad add c char default 'XX'");
            conn.Act("insert into ad(a,b) values(2,'Two')");
            CheckResults(13, 2, "select * from ad", "[{A:20,B:'Twenty'},{A:2,B:'Two',C:'XX'}]");
            conn.Act("alter table ad drop b");
            CheckResults(13,3,"select * from ad", "[{A:20},{A:2,C:'XX'}]");
            conn.Act("alter table ad add primary key(a)");
            conn.Act("insert into ad values(21,'AB')");
            conn.Act("create table de (d int references ad)");
            if (qry == 0 || qry == 4)
            {
                CheckExceptionNonQuery(13, 4, "insert into de values(14)", "Integrity constraint: missing foreign key DE(14)");
                if (!commit)
                {
                    Begin();
                    conn.Act("create table ad(a int,b char)");
                    conn.Act("insert into ad values(20,'Twenty')");
                    conn.Act("alter table ad add c char default 'XX'");
                    conn.Act("insert into ad(a,b) values(2,'Two')");
                    conn.Act("alter table ad drop b");
                    conn.Act("alter table ad add primary key(a)");
                    conn.Act("insert into ad values(21,'AB')");
                    conn.Act("create table de (d int references ad)");
                }
            }
            conn.Act("insert into de values(21)");
            if (qry == 0 || qry == 5)
            {
                CheckExceptionNonQuery(13, 5, "delete from ad where c='AB'", "Integrity constraint: RESTRICT - foreign key in use");
                if (!commit)
                {
                    Begin();
                    conn.Act("create table ad(a int,b char)");
                    conn.Act("insert into ad values(20,'Twenty')");
                    conn.Act("alter table ad add c char default 'XX'");
                    conn.Act("insert into ad(a,b) values(2,'Two')");
                    conn.Act("alter table ad drop b");
                    conn.Act("alter table ad add primary key(a)");
                    conn.Act("insert into ad values(21,'AB')");
                    conn.Act("create table de (d int references ad)");
                    conn.Act("insert into de values(21)");
                }
            }
            if (qry == 0 || qry == 6)
            {
                CheckExceptionNonQuery(13, 6, "drop ad", "RESTRICT: Index");
                if (!commit)
                {
                    Begin();
                    conn.Act("create table ad(a int,b char)");
                    conn.Act("insert into ad values(20,'Twenty')");
                    conn.Act("alter table ad add c char default 'XX'");
                    conn.Act("insert into ad(a,b) values(2,'Two')");
                    conn.Act("alter table ad drop b");
                    conn.Act("alter table ad add primary key(a)");
                    conn.Act("insert into ad values(21,'AB')");
                    conn.Act("create table de (d int references ad)");
                    conn.Act("insert into de values(21)");
                }
            }
            conn.Act("drop de cascade");
            conn.Act("alter table ad drop primary key(a)");
            CheckResults(13,7, "select * from ad", "[{A:20},{A:2,C:'XX'},{A:21,C:'AB'}]");
            conn.Act("insert into ad(a) values(13)");
            CheckResults(13,8, "select * from ad", "[{A:20},{A:2,C:'XX'},{A:21,C:'AB'},{A:13,C:'XX'}]");
            conn.Act("drop ad");
            if (qry == 0 || qry == 11)
            {
                CheckExceptionQuery(13, 11, "select * from ad", "Table AD undefined");
                Begin();
            }
            Rollback();
        }
        void Test14()
        {
            if (test > 0 && test != 14)
                return;
            testing = 14;
            Begin();
            conn.Act("create table fi(a int primary key, b char)");
            conn.Act("create table se(c char primary key, d int references fi on delete cascade)");
            conn.Act("insert into fi values(1066,'invasion'),(1953,'accession'),(2019, 'brexit')");
            conn.Act("insert into se values('johnson', 2019),('elizabeth',1953),('disaster',2019)");
            conn.Act("insert into se values('normans',1066),('hastings', 1066)");
            conn.Act("delete from fi where a = 1066");
            CheckResults(14, 1, "table se", "[{C:'disaster',D:2019},{C:'elizabeth',D:1953},{C:'johnson',D:2019}]");
            conn.Act("alter table se set (d) references fi on delete restrict");
            CheckExceptionNonQuery(14,2,"delete from fi where a = 2019","Integrity constraint: RESTRICT");
            if (!commit)
            {
                Begin();
                conn.Act("create table fi(a int primary key, b char)");
                conn.Act("create table se(c char primary key, d int references fi on delete cascade)");
                conn.Act("insert into fi values(1066,'invasion'),(1953,'accession'),(2019, 'brexit')");
                conn.Act("insert into se values('johnson', 2019),('elizabeth',1953),('disaster',2019)");
                conn.Act("insert into se values('normans',1066),('hastings', 1066)");
                conn.Act("delete from fi where a = 1066");
            }
            conn.Act("alter table se set(d) references fi on delete set null on update cascade");
            conn.Act("update fi set a = 2020 where a = 2019");
            CheckResults(14,3, "table se", "[{C:'disaster',D:2020},{C:'elizabeth',D:1953},{C:'johnson',D:2020}]");
            conn.Act("delete from fi where a = 2020");
            CheckResults(14,4, "table se", "[{C:'disaster'},{C:'elizabeth',D:1953},{C:'johnson'}]");
            Rollback();
        }
        public void Test15()
        {
            if (test > 0 && test != 15)
                return;
            testing = 15;
            Begin();
            conn.Act("create table ca(a char,b int check (b>0))");
            CheckExceptionNonQuery(15, 1, "insert into ca values('Neg',-99)","Column B Check constraint fails");
            if (!commit)
            {
                Begin();
                conn.Act("create table ca(a char,b int check (b>0))");
            }
            conn.Act("insert into ca values('Pos',45)");
            CheckResults(15, 2, "table ca", "[{A:'Pos',B:45}]");
            Rollback();
        }
        public void Test16()
        {
            if (test > 0 && test != 16)
                return;
            testing = 16;
            Begin();
            conn.Act("create table xa(b int,c int,d char)");
            conn.Act("create table xb(tot int)");
            conn.Act("insert into xb values (0)");
            conn.Act("create trigger ruab before update on xa referencing old as mr new as nr " +
            "for each row begin atomic update xb set tot=tot-mr.b+nr.b; " +
            "set d='changed' end");
            conn.Act("create trigger riab before insert on xa " +
            "for each row begin atomic set c=b+3; update xb set tot=tot+b end");
            conn.Act("insert into xa(b,d) values (7,'inserted')");
            conn.Act("insert into xa(b, d) values(9, 'Nine')");
            CheckResults(16,1,"table xa", "[{B:7,C:10,D:'inserted'},{B:9,C:12,D:'Nine'}]");
            CheckResults(16,2,"table xb", "[{TOT:16}]");
            conn.Act("update xa set b=8,d='updated' where b=7");
            CheckResults(16,3,"table xa", "[{B:8,C:10,D:'changed'},{B:9,C:12,D:'Nine'}]");
            CheckResults(16,4,"table xb", "[{TOT:17}]");
            conn.Act("create table xc(totb int,totc int)");
            conn.Act("create trigger sdai instead of delete on xa referencing old table as ot " +
            "for each statement begin atomic insert into xc (select b,c from ot) end");
            conn.Act("delete from xa where d='changed'");
            CheckResults(16,5,"table xc", "[{TOTB:8,TOTC:10}]");
            CheckResults(16,6,"table xa", "[{B:8,C:10,D:'changed'},{B:9,C:12,D:'Nine'}]"); // INSTEAD OF!
            Rollback();
        }
        public void Test17()
        {
            if (test > 0 && test != 17)
                return;
            testing = 17;
            Begin();
            conn.Act("create function reverse(a char) returns char "+
                "if char_length(a)<=1 then return a "+
                "else return reverse(substring(a from 1 for char_length(a)-1))"+
                "   ||substring(a from 0 for 1) end if");
            CheckResults(17, 1, "select reverse('Fred')", "[{REVERSE:'derF'}]");
            Rollback();
        }
        public void Test18()
        {
            if (test > 0 && test != 18)
                return;
            testing = 18;
            Begin();
            conn.Act("create table author(id int primary key,aname char)");
            conn.Act("create table book(id int primary key,authid int references author,title char)");
            conn.Act("insert into author values (1,'Dickens'),(2,'Conrad')");
            conn.Act("insert into book(authid,title) values (1,'Dombey & Son'),(2,'Lord Jim'),(1,'David Copperfield')");
            conn.Act("create function booksby(auth char) returns table(title char) "+
                "return table (select title from author inner join book b "+
                "on author.id=b.authid where aname=booksby.auth)");
            CheckResults(18, 1, "select title from author inner join book b on author.id=b.authid where aname='Dickens'",
                "[{TITLE:'Dombey & Son'},{TITLE:'David Copperfield'}]");
            CheckResults(18, 2, "select * from table(booksby('Dickens'))", 
                "[{TITLE:'Dombey & Son'},{TITLE:'David Copperfield'}]");
            CheckResults(18, 3, "select count(*) from table(booksby('Dickens'))",
                "[{COUNT:2}]");
            Rollback();
        }
        public void Test19()
        {
            if (test > 0 && test != 19)
                return;
            testing = 19;
            Begin();
            conn.Act("create table ga(a1 int primary key,a2 char)");
            conn.Act("insert into ga values(1,'First'),(2,'Second')");
            conn.Act("create function gather1() returns char "+
                " begin declare c cursor for select a2 from ga;" +
                "  declare done Boolean default false;" +
                "  declare continue handler for sqlstate '02000' set done=true;" +
                "  declare a char default '';" +
                "  declare p char;" +
                "  open c;" +
                "  repeat" +
                "   fetch c into p; " +
                "   if not done then " +
                "    if a='' then " +
                "     set a=p " +
                "    else " +
                "     set a=a||', '||p " +
                "    end if" +
                "   end if" +
                "  until done end repeat;" +
                "  close c;" +
                "  return a end");
            CheckResults(19, 1, "select gather1()", "[{GATHER1:'First, Second'}]");
            conn.Act("create function gather2() returns char "+
                "  begin declare b char default '';" +
                "   for select a2 from ga do " +
                "    if b='' then " +
                "     set b=a2 " +
                "    else " +
                "     set b=b||', '||a2 " +
                "    end if " +
                "   end for;" +
                "   return b end");
            CheckResults(19, 2, "select gather2()", "[{GATHER2:'First, Second'}]");
            Rollback();
        }
        public void Test20()
        {
            if (test > 0 && test != 20)
                return;
            testing = 20;
            Begin();
            conn.Act("create type point as (x int, y int)");
            conn.Act("create type size as (w int,h int)");
            conn.Act("create type line as (strt point,en point)");
            conn.Act("create type rect as (tl point,sz size) "
              +"constructor method rect(x1 int, y1 int, x2 int, y2 int),"
              +"method centre() returns point");
            conn.Act("create table figure(id int primary key,title char)");
            conn.Act("create table figureline(id int primary key,fig int references figure,what line)");
            conn.Act("create table figurerect(id int primary key,fig int references figure,what rect)");
            conn.Act("create constructor method rect(x1 int,y1 int,x2 int,y2 int) "
              +"begin tl = point(x1, y1); sz = size(x2 - x1, y2 - y1) end");
            conn.Act("create method centre() returns point for rect "
              +"return point(tl.x + sz.w / 2, tl.y + sz.h / 2)");
            conn.Act("create function centrerect(a int) returns point "
              +"return (select what.centre() from figurerect where id = centrerect.a)");
            conn.Act("insert into figure values(1,'Diagram')");
            conn.Act("insert into figurerect values(1,1,rect(point(1,2),size(3,4)))");
            conn.Act("insert into figurerect values(2,1,rect(4,5,6,7))");
            conn.Act("insert into figureline values(1,1,line(centrerect(1),centrerect(2)))");
            CheckResults(20, 1, "select what from figureline",
                "[{WHAT:'LINE(STRT=POINT(X=2,Y=4),EN=POINT(X=5,Y=6))'}]");
            Rollback();
        }
        public void Test21()
        {
            if (test > 0 && test != 21)
                return;
            testing = 21;
            Begin();
            conn.Act("create table members (id int primary key,firstname char)");
            conn.Act("create table played (id int primary key, winner int references members,"
              + "loser int references members,agreed boolean)");
            conn.Act("grant select on members to public");
            conn.Act("grant select on played to public");
            conn.Act("create procedure claim(won int,beat int)"
              + "insert into played(winner,loser) values(claim.won,claim.beat)");
            conn.Act("create procedure agree(p int)"
              + "update played set agreed=true "
               + "where winner=agree.p and loser in"
                + "(select m.id from members m where current_user like '%'||firstname escape '^')");
            conn.Act("insert into members(firstname) values(current_user)");
            CheckResults(21, 1, "select id from members where current_user like '%'||firstname escape'^'",
              "[{ID:1}]");
            conn.Act("insert into members(firstname) values('Fred')");
            conn.Act("insert into played(winner,loser) values(2,1)");
            conn.Act("create role membergames");
            conn.Act("grant execute on procedure claim(int,int) to role membergames");
            conn.Act("grant execute on procedure agree(int) to role membergames");
            conn.Act("grant membergames to public");
            conn.Act("set role membergames");
            conn.Act("call agree(2)");
            conn.Act("call claim(1,2)");
            CheckResults(21, 2, "table played", "[{ID:1,WINNER:2,LOSER:1,AGREED:true},{ID:2,WINNER:1,LOSER:2}]");
            Rollback();
        }
        public void Test22()
        {
            if (test > 0 && test != 22)
                return;
            testing = 22;
            Begin();
            conn.Act("create view WU of (e int, f char, g char) as get " +
                "etag url 'http://localhost:8180/A/A/D'");
            CheckResults(22, 1, "select * from wu", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}]");
            conn.Act("create table HU (e int primary key, k char, m int)");
            conn.Act("insert into HU values (1,'Cleaner',12500), (2,'Manager',31400)");
            conn.Act("create view VU as select * from wu natural join HU");
            CheckResults(22, 2, "select e, f, m from VU where e=1",
                "[{E:1,F:'Joe',M:12500}]"); // CHECK only works for committed tables/views
            conn.Act("insert into wu values(3,'Fred','Bloggs')");
            CheckResults(22, 3, "select * from wu", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'},"+
                "{E:3,F:'Fred',G:'Bloggs'}]");
            conn.Act("update vu set f='Elizabeth' where e=2");
            CheckResults(22, 4, "select * from wu where e=2", "[{E:2,F:'Elizabeth',G:'Boop'}]");
            Rollback();
        }
        public void Test23()
        {
            if (test > 0 && test != 23)
                return;
            testing = 23;
            // this test has its own explicit transaction control
            conn.Act("create view W of (e int, f char, g char) as get " +
                "etag 'http://localhost:8180/A/A/D'");
            CheckResults(23, 1, "select * from w", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}]");
            if (qry < 5)
            {
                conn.Act("create table H (e int primary key, k char, m int)");
                conn.Act("insert into H values (1,'Cleaner',12500), (2,'Manager',31400)");
                conn.Act("create view V as select * from w natural join H");
                CheckResults(22, 2, "select e, f, m from V where e=1", "[{E:1,F:'Joe',M:12500}]");
                conn.Act("insert into w values(3,'Fred','Bloggs')");
                CheckResults(23, 3, "select * from w", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}," +
                    "{E:3,F:'Fred',G:'Bloggs'}]");
                conn.Act("update v set f='Elizabeth' where e=2");
                CheckResults(23, 4, "select * from w where e=2", "[{E:2,F:'Elizabeth',G:'Boop'}]");
                ResetA();
            }
            // read-write conflicts
            if (qry == 0 || qry == 5)
            {
                tr = conn.BeginTransaction();
                Touch("select * from w where e=2");
                connA.Act("update d set f='Liz' where e=2");
                CheckExceptionCommit(23, 5, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 6)
            {
                tr = conn.BeginTransaction();
                Touch("select * from w where e=2");
                connA.Act("delete from d where e=2");
                CheckExceptionCommit(23, 6, "ETag validation failure");
                ResetA();
            }
            // read/write non conflicts
            if (qry == 0 || qry == 7)
            {
                tr = conn.BeginTransaction();
                Touch("select * from w where e=1");
                connA.Act("update d set f='Liz' where e=2");
                tr.Commit();
                CheckResults(23, 7, "select * from w", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Liz',G:'Boop'}]");
                ResetA();
            }
            if (qry == 0 || qry == 8)
            {
                tr = conn.BeginTransaction();
                Touch("select * from w where e=1");
                connA.Act("delete from d where e=2");
                CheckResults(23, 8, "select * from w", "[{E:1,F:'Joe',G:'Soap'}]");
                tr.Commit();
                ResetA();
            }
            // write/write conflicts UU
            if (qry == 0 || qry == 9)
            {
                tr = conn.BeginTransaction();
                conn.Act("update w set f='Liz' where e=2");
                connA.Act("update d set f='Eliza' where e=2");
                CheckExceptionCommit(23, 9, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 10)
            {
                tr = conn.BeginTransaction(); // II
                conn.Act("insert into w values (3,'Fred','Bloggs')");
                connA.Act("insert into d values (3,'Anyone','Else')");
                CheckExceptionCommit(23, 10, "Integrity constraint:");
                ResetA();
            }
            if (qry == 0 || qry == 11)
            {
                tr = conn.BeginTransaction();  // UD
                conn.Act("update w set f='Liz' where e=2");
                connA.Act("delete from d where e=2");
                CheckExceptionCommit(23, 11, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 12)
            {
                tr = conn.BeginTransaction();  // DU
                conn.Act("delete from w where e=2");
                connA.Act("update d set f='Eliza' where e=2");
                CheckExceptionCommit(23, 12, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 13)
            {
                tr = conn.BeginTransaction();  // DD
                conn.Act("delete from w where e=2");
                connA.Act("delete from d where e=2");
                CheckExceptionCommit(23, 13, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 14)
            {
                tr = conn.BeginTransaction();  // non UU
                conn.Act("update w set f='Joseph' where e=1");
                connA.Act("update d set f='Eliza' where e=2");
                tr.Commit();
                CheckResults(23, 14, "select * from w", "[{E:1,F:'Joseph',G:'Soap'},{E:2,F:'Eliza',G:'Boop'}]");
                ResetA();
            }
            if (qry == 0 || qry == 15)
            {
                tr = conn.BeginTransaction();  // non II
                conn.Act("insert into w values (4,'Some','Other')");
                connA.Act("insert into d values (3,'Anyone','Else')");
                tr.Commit();
                CheckResults(23, 15, "select * from w", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}," +
                    "{E:3,F:'Anyone',G:'Else'},{E:4,F:'Some',G:'Other'}]");
                ResetA();
            }
            if (qry == 0 || qry == 16)
            {
                tr = conn.BeginTransaction(); // non UD
                conn.Act("update w set f='Joseph' where e=1");
                connA.Act("delete from d where e=2");
                tr.Commit();
                CheckResults(23, 16, "select * from w", "[{E:1,F:'Joseph',G:'Soap'}]");
                ResetA();
            }
            if (qry == 0 || qry == 17)
            {
                tr = conn.BeginTransaction(); // non DU
                conn.Act("delete from w where e=2");
                connA.Act("update d set f='Joseph' where e=1");
                tr.Commit();
                CheckResults(23, 17, "select * from w", "[{E:1,F:'Joseph',G:'Soap'}]");
                ResetA();
            }
        }
        public void Test24()
        {
            var connB = new PyrrhoConnect("Files=DB");
            connB.Act("create table T(E int,F char)");
            connB.Act("insert into T values(3,'Three'),(6,'Six'),(4,'Vier'),(6,'Sechs')");
            connB.Act("create role DB");
            connB.Act("grant DB to \"" + WindowsIdentity.GetCurrent().Name + "\"");
            connB.Close();
            var connC = new PyrrhoConnect("Files=DC");
            connC.Act("create table U(E int,F char)");
            connC.Act("insert into U values(5,'Five'),(4,'Four'),(8,'Ate')");
            connB.Act("create role DC");
            connB.Act("grant DC to \"" + WindowsIdentity.GetCurrent().Name + "\"");
            connB.Close();
            conn.Act("create view V of (E int,F char) as get 'http://localhost:8180/DB/DB/t'");
            conn.Act("create table VU (d char primary key, k int, u char)");
            conn.Act("insert into VU values('B',4,'http://localhost:8180/DB/DB/t'");
            conn.Act("insert into VU values('C',1,'http://localhost:8180/DC/DC/u'");
            conn.Act("create view W of (E int, D char, K int, F char) as get using VU");
            conn.Act("create table M (e int primary key, n char, unique(n))");
            conn.Act("insert into M values (2,'Deux'),(3,'Trois'),(4,'Quatre')");
            conn.Act("insert into M values (5,'Cinq'),(6,'Six'),(7,'Sept')");
            CheckResults(24, 1, "select * from v", "[{E:3,F:'Three'},{E:6,F:'Six'}," +
                "{E:4,F:'Vier'},{E:6,F:'Sechs'}]");
            CheckResults(24, 2, "select * from V where e=6", "[{E:6,F:'Six'},{E:6,F:'Sechs'}]");
            CheckResults(24, 3, "select * from w", "[{E:3,D:'B',K:4,F:'Three'}," +
                "{E:6,D:'B',K:4,F:'Six'},{E:4,D:'B',K:4,F:'Vier'},{E:6,D:'B',K:4,F:'Sechs'}," +
                "{E:5,D:'C',K:1,F:'Five'},{E:4,D:'C',K:1,F:'Four'},{E:8,D:'C',K:1,F:'Ate'}]");
            CheckResults(24, 4, "select * from w where e<6", "[{E:3,D:'B',K:4,F:'Three'}," +
                "{E:4,D:'B',K:4,F:'Vier'},{E:5,D:'C',K:1,F:'Five'},{E:4,D:'C',K:1,F:'Four'}]");
            CheckResults(24, 5, "select * from w where k=1", "[{E:5,D:'C',K:1,F:'Five'}," +
                "{E:4,D:'C',K:1,F:'Four'},{E:8,D:'C',K:1,F:'Ate'}]");
            CheckResults(24, 6, "select count(e) from w", "[{COUNT:7}]");
            CheckResults(24, 7, "select count(*) from w", "[{COUNT:7}]");
            CheckResults(24, 8, "select max(f) from w", "[{MAX:'Vier'}]");
            CheckResults(24, 9, "select max(f) from w where e>4", "[{MAX:'Six'}]");
            CheckResults(24, 10, "select count(*) from w where k>2", "[{COUNT:4}]");
            CheckResults(24, 11, "select min(f) from w", "[{MIN:'Ate'}]");
            CheckResults(24, 12, "select sum(e)*sum(e),d from w group by d",
                "[{Col0:97,D:'B'},{Col0:105,D:'C'}]");
            CheckResults(24, 13, "select count(*),k/2 as k2 from w group by k2",
                "[{COUNT:4,K2:2},{COUNT:3,K2:0}]");
            CheckResults(24, 14, "select avg(e) from w", "[{AVG:7.14582}]");
            /* E	D	K	F	    N
                3	B	4	Three	Trois
                4	B	4	Vier	Quatre
                4	C	1	Four	Quatre
                5	C	1	Five	Cinq
                6	B	4	Six	    Six
                6	B	4	Sechs	Six	 */
            CheckResults(24, 15, "select f,n from w natural join m", "[{F:'Three',N:'Trois'}," +
                "{F:'Vier',N:'Quatre'},{F:'Four',N:'Quatre'},{F:'Five',N:'Cinq'}," +
                "(F:'Six',N:'Six'},(F:'Sechs',N:'Six'}]");
            CheckResults(24, 16, "select e+char_length(f) as x,n from w natural join m",
                "[{Col0:8,N:'Trois'},{Col0:8,N:'Quatre'},{Col0:8,N:'Quatre'}," +
                "{Col0:9,N:'Cinq'},{Col0:9, N::'Six'},{Col0:10,N:'Six'}]");
            CheckResults(24, 17, "select char_length(f)+char_length(n) from w natural join m",
                "[{Col0:10},{Col0:10},{Col0:10},{Col0:8},{Col0:6},{Col0:8}]");
            CheckResults(24, 18, "select sum(e)+char_length(max(f)) from w", "[{Col0:32}]");
            CheckResults(24, 19, "select count(*),e+char_length(f) as x from w group by x",
                "[{COUNT:3,Col1:8},{COUNT:2,Col1:9},{COUNT:1,Col1:11}]");
            CheckResults(24, 20, "select count(*),e+char_length(n) as x from w natural join m group by x",
                "[{COUNT:1,Col1:8},{COUNT:2,Col1:10},{COUNT:3,Col1:9}]");
            CheckResults(24, 21, "select sum(e)+char_length(f),f  from w natural join m group by f",
                "[{Col0:8,F:'Three'},{Col0:8,F:'Vier'},{Col0:8,F:'Four'},{Col0:9,F:'Five'}," +
                "{Col0:9,F:'Six'},{Col0:11,'F:'Sechs'}]");
            CheckResults(24, 22, "select sum(char_length(f))+char_length(n) as x,n from w natural join m group by n",
                "{Col0:10,N:'Trois'},{Col0:14,N:'Quatre'},{Col0:8,N:'Cinq'},{Col0:11,N:'Six'}]");
            CheckResults(24, 23, "Select count(*) from w natural join m","[{COUNT:6}]");
            conn.Act("update v set F='Tri' where E=3");
            conn.Act("insert into V values (9,'Nine')");
            CheckResults(24, 24, "select * from V", "[{E:3,F:'Tri'},{E:6,F:'Six'}," +
                "{E:4,F:'Vier'},{E:6,F:'Sechs'},{E:9,F:'Nine'}]");
            conn.Act("update w set f='Eight' where e=8");
            conn.Act("insert into w(D,E,F) values('B',7,'Seven')");
            CheckResults(24, 25, "select * from V","[{E:3,F:'Tri'},{E:6,F:'Six'},"+
                "{E:4,F:'Vier'},{E:6,F:'Sechs'},{E:9,F:'Nine'},{E:8,F:'Seven'}]");
            CheckResults(24, 26, "select * from W", "[{E:3,D:'B',K:4,F:'Three'}," +
                "{E:6,D:'B',K:4,F:'Six'},{E:4,D:'B',K:4,F:'Vier'},{E:6,D:'B',K:4,F:'Sechs'}," +
                "{E:9,D:'B',K:4,F:'Nine'},{E:7,D:'B',K:4,F:'Seven'},"+
                "{E:5,D:'C',K:1,F:'Five'},{E:4,D:'C',K:1,F:'Four'},{E:8,D:'C',K:1,F:'Eight'}]");
            conn.Act("delete from w where E=7");
            conn.Act("update v set f='Ate' where e=8");
            CheckResults(24, 27, "select * from v", "[{E:3,F:'Tri'},{E:6,F:'Six'}," +
                "{E:4,F:'Vier'},{E:6,F:'Sechs'},{E:9,F:'Nine'}]");
            CheckResults(24, 28, "select * from w", "[{E:3,D:'B',K:4,F:'Three'}," +
                "{E:6,D:'B',K:4,F:'Six'},{E:4,D:'B',K:4,F:'Vier'},{E:6,D:'B',K:4,F:'Sechs'}," +
                "{E:9,D:'B',K:4,F:'Nine'},{E:5,D:'C',K:1,F:'Five'},{E:4,D:'C',K:1,F:'Four'},"+
                "{E:8,D:'C',K:1,F:'Eight'}]");
        }
        void CheckExceptionCommit(int t, int q,string m)
        {
            try
            {
                tr.Commit();
            }
            catch(Exception e)
            {
                if (e.Message.StartsWith(m))
                    return;
                Console.WriteLine("Unexpected exception (" + t + " " + q + ") " + e.Message);
                return;
            }
            Console.WriteLine("Didnt get exception (" + t + " " + q + ") " + m);
            tr.Rollback();
        }
        void CheckExceptionQuery(int t, int q, string c, string m)
        {
            if (qry > 0 && qry != q)
                return;
            cur = q;
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
            cur = q;
            try
            {
                if (c.ToUpper() == "COMMIT")
                    tr.Commit();
                else
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
        void Touch(string s)
        {
            try
            {
                var cmd = conn.CreateCommand();
                cmd.CommandText = s;
                var r = cmd.ExecuteReader();
                while (r?.Read()==true)
                    ;
                r?.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception (" + test + " " + qry + ") " + e.Message);
            }
        }
        void CheckResults(int t,int q,string c,string d)
        {
            if (qry > 0 && qry != q)
                return;
            cur = q;
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
        void CheckExecuteResults(int t, int q, string d, string c, params string[] ps)
        {
            if (qry > 0 && qry != q)
                return;
            cur = q;
            try
            {
                var r = conn.ExecuteReader(c,ps);
                var da = new DocArray();
                while (r?.Read() == true)
                {
                    var rd = new Document();
                    for (var i = 0; i < r.FieldCount; i++)
                        if (r[i] != DBNull.Value)
                            rd.fields.Add(new KeyValuePair<string, object>(r.GetName(i), r[i]));
                    da.items.Add(rd);
                }
                r?.Close();
                Check(da, new DocArray(d));
            }
            catch (Exception e)
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
                        var ss = sp.Value?.ToString() ?? "";
                        var cs = cf.ToString();
                        for (var i = 0; i < ss.Length && i < cs.Length; i++)
                            if (ss[i] != cs[i])
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
                        throw new Exception("Values |" + (cf?.ToString() ?? "")
                            + "|" + (sp.Value?.ToString()) ?? "" + "| do not match");
                }
            if (nc != c.fields.Count)
                throw new Exception("Missing field(s)");
        }
    }
}
