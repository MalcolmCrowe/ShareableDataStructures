using System;
using Pyrrho;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Security.Principal;

namespace Test
{
    class Program
    {
        static int test = 0, qry = 0, testing = 0, cur=0;
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
                Console.WriteLine("3 June 2021 Repeatable tests");
                if (args.Length == 0)
                {
                    Console.WriteLine("Tests 22,23,24 need Server with +s");
                    Console.WriteLine("Tests 12.8,12.9,22,23 need RowSet Review so no -R flag");
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
        void Act(int q,string cmd)
        {
            cur = q;
            conn.Act(cmd);
        }
        void Commit(int q,string m=null)
        {
            cur = q;
            tr?.Commit(m);
            tr = null;
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
        void Prepare(int c,string n,string s)
        {
            cur = c;
            conn.Prepare(n,s);
        }
        void Execute(int c,string n,params string[] s)
        {
            cur = c;
            conn.Execute(n, s);
        }
        PyrrhoReader Read(PyrrhoCommand cm,int c)
        {
            cur = c;
            return cm.ExecuteReader();
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
    //        Test24();
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
            Act(10,"create table A(B int,C int,D char,primary key(B,C))");
            Act(11,"insert into A values(2,3,'TwentyThree')");
            Act(12,"insert into A values(1,9,'Nineteen')");
            CheckResults(1,1,"select * from A", "[{B:1,C:9,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            Act(13,"update A set C=19 where C=9");
            CheckResults(1,2, "select * from A", "[{B:1,C:19,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            Act(14,"delete from A where C=19");
            CheckResults(1,3, "select * from A", "[{B:2,C:3,D:'TwentyThree'}]");
            Act(15,"insert into A(B,D) (select E.B,upper(E.D) from A E)");
            CheckResults(1,4, "table A", "[{B:2,C:3,D:'TwentyThree'},{B:2,C:4,D:'TWENTYTHREE'}]");
            Rollback();
        }
        void Test2()
        {
            if (test > 0 && test != 2)
                return;
            testing = 2;
            Begin();
            Act(20,"create table AA(B int,C char)");
            Act(21,"insert into AA(B) values(17)");
            Act(22,"insert into AA(C) values('BC')");
            Act(23,"insert into AA(C,B) values('GH',+67)");
            CheckResults(2,1,"select * from AA", "[{B:17},{C:'BC'},{B:67,C:'GH'}]");
            CheckResults(2,2,"select B from AA", "[{B:17},{B:67}]");
            CheckResults(2,3,"select C as E from AA", "[{E:'BC'},{E:'GH'}]");
            CheckResults(2,4,"select C from AA where B<20", "[]");
            CheckResults(2,5,"select C from AA where B>20", "[{C:'GH'}]");
            CheckResults(2,6,"select count(C) from AA", "[{COUNT:2}]");
            Rollback();
        }
        void Test3()
        {
            if (test > 0 && test != 3)
                return;
            testing = 3;
            Begin();
            Act(30,"create table b(c int primary key,d char)");
            Act(31,"insert into b values(45,'DE')");
            Act(32,"insert into b values(-23,'HC')");
            CheckResults(3,1,"select * from b", "[{C:-23,D:'HC'},{C:45,D:'DE'}]");
            CheckResults(3,2,"select * from b where c=-23", "[{C:-23,D:'HC'}]");
            Rollback();
        }
        void Test4()
        {
            if (test > 0 && test != 4)
                return;
            testing = 4;
            Begin();
            Act(40,"create table e(f int,g char,primary key(g,f))");
            Act(41,"insert into e values(23,'XC')");
            Act(42,"insert into e values(45,'DE')");
            CheckResults(4,1,"select * from e", "[{F:45,G:'DE'},{F:23,G:'XC'}]");
            Act(43,"insert into e(g) values('DE')");
            CheckResults(4,2,"select * from e",
                "[{F:45,G:'DE'},{F:46,G:'DE'},{F:23,G:'XC'}]");
            // the EvalRowSet loop in the next test should execute only once
            CheckResults(4,3,"select count(f) from e where g='DE' and f<=45",
                "[{COUNT:1}]");
            Rollback();
        }
        void Test5()
        {
            if (test > 0 && test != 5)
                return;
            testing = 5;
            Begin();
            Act(50,"create table f(b int,c int)");
            Act(51,"insert into f values(17,15)");
            Act(52,"insert into f values(23,6)");
            CheckResults(5,1,"select * from f", "[{B:17,C:15},{B:23,C:6}]");
            CheckResults(5,2,"select b-3 as h,22 as g from f", "[{H:14,G:22},{H:20,G:22}]");
            CheckResults(5,3,"select (f.b) as h,(c) from f", "[{H:17,C:15},{H:23,C:6}]");
            CheckResults(5,4,"select b+3,d.c from f d", "[{Col0:20,C:15},{Col0:26,C:6}]");
            CheckResults(5,5,"select (b as d,c) from f", "[{Col0:(D=17,C=15)},{Col0:(D=23,C=6)}]");
            CheckResults(5,6,"select * from f order by c", "[{B:23,C:6},{B:17,C:15}]");
            CheckResults(5,7,"select * from f order by b desc", "[{B:23,C:6},{B:17,C:15}]");
            CheckResults(5,8,"select * from f order by b+c desc", "[{B:17,C:15},{B:23,C:6}]");
            CheckResults(5,9,"select sum(b) from f", "[{SUM:40}]");
            CheckResults(5,10,"select max(c),min(b) from f", "[{MAX:15,MIN:17}]");
            CheckResults(5,11,"select count(c) as d from f where b<20", "[{D:1}]");
            Rollback();
        }
        void Test6()
        {
            if (test > 0 && test != 6)
                return;
            testing = 6;
            Begin();
            Act(60,"create table ta(b date,c interval hour to second,d boolean)");
            Act(61,"insert into ta values(date'2019-01-06T12:30:00',interval'02:00:00'hour to second,false)");
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
            Act(70,"create table TB(S char,D int,C int)");
            Act(71,"insert into TB values('Glasgow',2,43)");
            Act(72,"insert into TB values('Paisley',3,82)");
            Act(73,"insert into TB values('Glasgow',4,29)");
            CheckResults(7, 1, "select S,count(C) as occ,sum(C) as total from TB group by S",
                "[{S:\"Glasgow\",OCC:2,TOTAL:72},{S:\"Paisley\",OCC:1,TOTAL:82}]");
            Act(74,"create table TBB(b int,c int,d int, e int)");
            Act(75,"insert into TBB values(1,21,31,41),(3,22,35,47),(1,22,33,42),(1,22,35,49)");
            Act(76,"insert into TBB values(2,21,37,40),(2,23,33,43),(1,24,31,44),(2,26,37,45)");
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
            Act(80,"create table JA(B int,C int,D int)");
            Act(81,"insert into JA values(4,2,43)");
            Act(82,"insert into JA values(8,3,82)");
            Act(83,"insert into JA values(7,4,29)");
            Act(84,"create table JE(F int,C int,G int)");
            Act(85,"insert into JE values(4,3,22)");
            Act(86,"insert into JE values(11,4,10)");
            Act(87,"insert into JE values(7,2,31)");
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
            Act(88,"create table SalesPerson(pid int primary key)");
            Act(89,"insert into SalesPerson values(1),(2),(3)"); 
            Act(90,"create table Sales(sid int primary key, spid int, cust int, amount int)"); 
            Act(91,"insert into Sales values(4,3,10,22),(5,2,11,12),(6,2,10,37)"); 
            Act(92,"insert into Sales values(7,1,12,7),(8,3,13,41),(9,1,12,17)");
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
            Act(93,"create table ba(b int,c real,d numeric)");
            Act(94,"insert into ba values(12345678901234567890123456789,123.4567,0.1234)");
            Act(95,"insert into ba values(0,123.4567e-15,1234)");
            Act(96,"insert into ba values(12,1234.0,0.00045)");
            Act(97,"insert into ba values(34,0.5678e9,0)");
            CheckResults(9, 1, "select * from ba", 
                "[{B: \"12345678901234567890123456789\", C: 123.4567, D: 0.1234}," +
                "{B: 0, C: 1.234567E-13, D: 1234},{B: 12, C: 1234.0,D: 0.00045}," +
                "{B: 34, C: 567800000, D: 0}]");
            Rollback();
        }

        /// <summary>
        /// Check transaction conflicts for read and write transactions
        /// </summary>
        /// <param name="t"></param>
        void Test10()
        {
            if (commit || (test > 0 && test != 10)) //This test runs if commit is false
                return;
            testing = 10;
            try
            {
                Act(1,"create table RWC(A int primary key,B char,C int)");
                Act(2,"create table RRC(D char primary key,E int references RWC)");
            }
            catch (Exception) {
            }
            Act(100,"delete from RWC");
            Act(101,"delete from RRC");
            Act(102,"insert into RWC values(42,'Life, the Universe',1)");
            Act(103,"insert into RWC values(52,'Weeks in the year',2)");
            Act(104,"insert into RRC values('Douglas Adams',42)");
            Begin();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "select * from RWC where A=42";
            var r = Read(cmd,105);
            r.Close();
            var task1 = Task.Factory.StartNew(() => Test10A(106));
            task1.Wait();
            CheckExceptionNonQuery(10, 1, "Commit", "Transaction conflict");
            Begin();
            cmd.CommandText = "select * from RWC where A=52";
            r = Read(cmd,107);
            r.Close();
            var task2 = Task.Factory.StartNew(() => Test10B(108));
            task2.Wait();
            Commit(109);
            Act(110,"update RWC set C=3 where A=42");
            Begin();
            task1 = Task.Factory.StartNew(() => Test10A(111));
            task1.Wait();
            Commit(112);
            Begin();
            Act(113,"update RWC set C=3 where A=52");
            task1 = Task.Factory.StartNew(() => Test10C(114));
            task1.Wait();
            CheckExceptionNonQuery(10, 2, "Commit", "Transaction conflict");
            Begin();
            CheckResults(10,3,"select * from RWC","[{A:42,B:'the product of 6 and 9',C:3},{A:52,B:'Weeks in the year',C:4}]");
            task1 = Task.Factory.StartNew(() => Test10B(115));
            task1.Wait();
            CheckExceptionNonQuery(10, 4, "Commit", "Transaction conflict");
            Begin();
            Act(116,"insert into RWC values(13,'Lucky for some',4)");
            task1 = Task.Factory.StartNew(() => Test10D(117));
            task1.Wait();
            CheckExceptionNonQuery(10, 5, "Commit", "Transaction conflict");
            Begin();
            Act(118,"delete from RWC where a=52"); 
            task1 = Task.Factory.StartNew(() => Test10E(119));
            task1.Wait();
            CheckExceptionNonQuery(10, 6, "Commit", "Transaction conflict");
            Begin();
            Act(120,"update RRC set E=42 where D='Douglas Adams'");
            task1 = Task.Factory.StartNew(() => Test10F(121));
            task1.Wait();
            CheckExceptionNonQuery(10, 7, "Commit", "Transaction conflict"); 
            Begin();
            Act(122,"update RWC set A=12,B='Dozen' where A=13");
            task1 = Task.Factory.StartNew(() => Test10G(123));
            task1.Wait();
            CheckExceptionNonQuery(10, 8, "Commit", "Transaction conflict");
            Act(124,"delete from RWC where A=12");
            Begin();
            Act(125,"insert into RWC values (12,'Dozen',7)");
            task1 = Task.Factory.StartNew(() => Test10H(126));
            task1.Wait();
            CheckExceptionNonQuery(10, 7, "Commit", "Transaction conflict"); 
            Act(131,"insert into RWC values (13,'Black Friday',6)");
            Begin();
            Act(132,"delete from RWC where A=13");
            task1 = Task.Factory.StartNew(() => Test10I(133));
            task1.Wait();
            CheckExceptionNonQuery(10, 10, "Commit", "Transaction conflict");
        }
        void Test10A(int c)
        {
            var p = new Program(new string[0]);
            p.Act(c,"update RWC set B='the product of 6 and 9' where A=42");
            p.conn.Close();
        }
        void Test10B(int c)
        {
            var p = new Program(new string[0]);
            p.Act(c,"update RWC set B='the product of 6 and 7' where A=42");
            p.conn.Close();
        }
        void Test10C(int c)
        {
            var p = new Program(new string[0]);
            p.Act(c,"update RWC set C=4 where A=52");
            p.conn.Close();
        }
        void Test10D(int c)
        {
            var p = new Program(new string[0]);
            p.Act(c,"insert into RWC values(13, 'Bakers dozen', 5)");
            p.conn.Close();
        }
        void Test10E(int c)
        {
            var p = new Program(new string[0]);
            p.Act(c,"update RRC set E=52 where D='Douglas Adams'");
            p.conn.Close();
        }
        void Test10F(int c)
        {
            var p = new Program(new string[0]);
            p.Act(c,"delete from RWC where A=42");
            p.conn.Close();
        }
        void Test10G(int c)
        {
            var p = new Program(new string[0]);
            p.Act(c,"insert into RWC values (12,'Dozen',6)");
            p.conn.Close();
        }
        void Test10H(int c)
        {
            var p = new Program(new string[0]);
            p.Act(c,"update RWC set A=12,B='Dozen' where A=13");
            p.conn.Close();
        }
        void Test10I(int c)
        {
            var p = new Program(new string[0]);
            p.Act(c,"insert into RRC values ('Last Supper',13)");
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
                Act(110,"create table cs(b int not null,c int default 4,d int generated always as b+c)");
                CheckExceptionNonQuery(11, 1, "insert into cs(c) values(5)", "Null value not allowed in column B");
            }
            if (qry == 0 || qry == 2)
            {
                Begin();
                Act(111,"create table cs(b int not null,c int default 4,d int generated always as (b+c))");
                Act(112,"insert into cs(b) values(3)");
                CheckExceptionNonQuery(11, 2, "insert into cs values(1,2,3)", "Attempt to assign to a non-updatable column");
            }
            if (qry == 0 || qry == 3)
            {
                Begin();
                Act(113,"create table cs(b int not null,c int default 4,d int generated always as (b+c))");
                Act(114,"insert into cs(b) values(3)");
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
            Act(120,"create table sce(a int primary key,b char)");
            Act(121,"insert into sce values(12,'Zodiac')");
            Act(122,"insert into sce values(13,'Bakers')");
            Act(123,"insert into sce values(14,'Fortnight')");
            Act(124,"create table dst(c int)");
            Act(125,"insert into dst (select a from sce where b<'H')");
            CheckResults(12, 1, "select * from dst", "[{C:13},{C:14}]");
            CheckResults(12, 2, "select a from sce where b in('Fortnight','Zodiac')",
               "[{A:12},{A:14}]");
            CheckResults(12, 3, "select * from dst where c in (select a from sce where b='Bakers')",
                "[{C:13}]");
            Act(126,"insert into dst(c) (select max(x.a)+4 from sce x where x.b<'H')");
            CheckResults(12, 4, "select * from dst", "[{C:13},{C:14},{C:18}]");
            Act(127,"insert into dst (select min(x.c)-3 from dst x)");
            CheckResults(12, 5, "select * from dst", "[{C:13},{C:14},{C:18},{C:10}]");
            if (commit)
            {
                Prepare(128,"Ins1", "insert into sce values(?,?)");
                Prepare(129,"Upd1", "update sce set a=? where b=?");
                Prepare(130,"Del1", "delete from dst where c>?");
                Prepare(131,"Ins2", "insert into dst (select char_length(b) from sce where a=?)");
                Prepare(132,"Sel1", "select * from dst where c<?");
                Execute(133,"Ins1", "" + 5, "'HalfDozen'");
                Execute(134,"Upd1", "" + 6, "'HalfDozen'");
                Execute(135,"Del1", "" + 10);
                Execute(136,"Ins2", "" + 6);
                CheckExecuteResults(12, 6, "[{C:9}]", "Sel1", "" + 10);
            }
            Act(137,"create table p(q int primary key,r char,a int)");
            Act(138,"create view v as select q,r as s,a from p");
            Act(139,"insert into v(s) values('Twenty'),('Thirty')");
            Act(140,"update v set s='Forty two' where q=1");
            CheckResults(12, 7, "select r from p", "[{R:'Forty two'},{R:'Thirty'}]");
            Act(141,"delete from v where s='Thirty'");
            Act(142,"insert into p(r) values('Fifty')");
            Act(143,"create table t(s char,u int)");
            Act(144,"insert into t values('Forty two',42),('Fifty',48)");
            Act(145,"create view w as select * from t natural join v");
            Act(146,"update w set u=50,a=21 where q=2");
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
            Act(130,"create table ad(a int,b char)");
            Act(131,"insert into ad values(20,'Twenty')");
            if (qry == 0 || qry == 1)
            {
                CheckExceptionNonQuery(13, 1, "alter table ad add c char not null", "Table is not empty");
                if (!commit)
                {
                    Begin();
                    Act(132,"create table ad(a int,b char)");
                    Act(133,"insert into ad values(20,'Twenty')");
                }
            }
            Act(134,"alter table ad add c char default 'XX'");
            Act(135,"insert into ad(a,b) values(2,'Two')");
            CheckResults(13, 2, "select * from ad", "[{A:20,B:'Twenty'},{A:2,B:'Two',C:'XX'}]");
            Act(136,"alter table ad drop b");
            CheckResults(13,3,"select * from ad", "[{A:20},{A:2,C:'XX'}]");
            Act(137,"alter table ad add primary key(a)");
            Act(138,"insert into ad values(21,'AB')");
            Act(139,"create table de (d int references ad)");
            if (qry == 0 || qry == 4)
            {
                CheckExceptionNonQuery(13, 4, "insert into de values(14)", "Integrity constraint: missing foreign key DE(14)");
                if (!commit)
                {
                    Begin();
                    Act(140,"create table ad(a int,b char)");
                    Act(141,"insert into ad values(20,'Twenty')");
                    Act(142,"alter table ad add c char default 'XX'");
                    Act(143,"insert into ad(a,b) values(2,'Two')");
                    Act(144,"alter table ad drop b");
                    Act(145,"alter table ad add primary key(a)");
                    Act(146,"insert into ad values(21,'AB')");
                    Act(147,"create table de (d int references ad)");
                }
            }
            Act(148,"insert into de values(21)");
            if (qry == 0 || qry == 5)
            {
                CheckExceptionNonQuery(13, 5, "delete from ad where c='AB'", "Integrity constraint: RESTRICT - foreign key in use");
                if (!commit)
                {
                    Begin();
                    Act(149,"create table ad(a int,b char)");
                    Act(150,"insert into ad values(20,'Twenty')");
                    Act(151,"alter table ad add c char default 'XX'");
                    Act(152,"insert into ad(a,b) values(2,'Two')");
                    Act(153,"alter table ad drop b");
                    Act(154,"alter table ad add primary key(a)");
                    Act(155,"insert into ad values(21,'AB')");
                    Act(156,"create table de (d int references ad)");
                    Act(157,"insert into de values(21)");
                }
            }
            if (qry == 0 || qry == 6)
            {
                CheckExceptionNonQuery(13, 6, "drop ad", "RESTRICT: Index");
                if (!commit)
                {
                    Begin();
                    Act(158,"create table ad(a int,b char)");
                    Act(159,"insert into ad values(20,'Twenty')");
                    Act(160,"alter table ad add c char default 'XX'");
                    Act(161,"insert into ad(a,b) values(2,'Two')");
                    Act(162,"alter table ad drop b");
                    Act(163,"alter table ad add primary key(a)");
                    Act(164,"insert into ad values(21,'AB')");
                    Act(165,"create table de (d int references ad)");
                    Act(166,"insert into de values(21)");
                }
            }
            Act(167,"drop de cascade");
            Act(168,"alter table ad drop primary key(a)");
            CheckResults(13,7, "select * from ad", "[{A:20},{A:2,C:'XX'},{A:21,C:'AB'}]");
            Act(169,"insert into ad(a) values(13)");
            CheckResults(13,8, "select * from ad", "[{A:20},{A:2,C:'XX'},{A:21,C:'AB'},{A:13,C:'XX'}]");
            Act(170,"drop ad");
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
            Act(140,"create table fi(a int primary key, b char)");
            Act(141,"create table se(c char primary key, d int references fi on delete cascade)");
            Act(142,"insert into fi values(1066,'invasion'),(1953,'accession'),(2019, 'brexit')");
            Act(143,"insert into se values('johnson', 2019),('elizabeth',1953),('disaster',2019)");
            Act(144,"insert into se values('normans',1066),('hastings', 1066)");
            Act(145,"delete from fi where a = 1066");
            CheckResults(14, 1, "table se", "[{C:'disaster',D:2019},{C:'elizabeth',D:1953},{C:'johnson',D:2019}]");
            Act(146,"alter table se set (d) references fi on delete restrict");
            CheckExceptionNonQuery(14,2,"delete from fi where a = 2019","Integrity constraint: RESTRICT");
            if (!commit)
            {
                Begin();
                Act(147,"create table fi(a int primary key, b char)");
                Act(148,"create table se(c char primary key, d int references fi on delete cascade)");
                Act(149,"insert into fi values(1066,'invasion'),(1953,'accession'),(2019, 'brexit')");
                Act(150,"insert into se values('johnson', 2019),('elizabeth',1953),('disaster',2019)");
                Act(151,"insert into se values('normans',1066),('hastings', 1066)");
                Act(152,"delete from fi where a = 1066");
            }
            Act(153,"alter table se set(d) references fi on delete set null on update cascade");
            Act(154,"update fi set a = 2020 where a = 2019");
            CheckResults(14,3, "table se", "[{C:'disaster',D:2020},{C:'elizabeth',D:1953},{C:'johnson',D:2020}]");
            Act(155,"delete from fi where a = 2020");
            CheckResults(14,4, "table se", "[{C:'disaster'},{C:'elizabeth',D:1953},{C:'johnson'}]");
            Rollback();
        }
        public void Test15()
        {
            if (test > 0 && test != 15)
                return;
            testing = 15;
            Begin();
            Act(150,"create table ca(a char,b int check (b>0))");
            CheckExceptionNonQuery(15, 1, "insert into ca values('Neg',-99)","Column B Check constraint fails");
            if (!commit)
            {
                Begin();
                Act(151,"create table ca(a char,b int check (b>0))");
            }
            Act(152,"insert into ca values('Pos',45)");
            CheckResults(15, 2, "table ca", "[{A:'Pos',B:45}]");
            Rollback();
        }
        public void Test16()
        {
            if (test > 0 && test != 16)
                return;
            testing = 16;
            Begin();
            Act(160,"create table xa(b int,c int,d char)");
            Act(161,"create table xb(tot int)");
            Act(162,"insert into xb values (0)");
            Act(163,"create trigger ruab before update on xa referencing old as mr new as nr " +
            "for each row begin atomic update xb set tot=tot-mr.b+nr.b; " +
            "set d='changed' end");
            Act(164,"create trigger riab before insert on xa " +
            "for each row begin atomic set c=b+3; update xb set tot=tot+b end");
            Act(165,"insert into xa(b,d) values (7,'inserted')");
            Act(166,"insert into xa(b, d) values(9, 'Nine')");
            CheckResults(16,1,"table xa", "[{B:7,C:10,D:'inserted'},{B:9,C:12,D:'Nine'}]");
            CheckResults(16,2,"table xb", "[{TOT:16}]");
            Act(167,"update xa set b=8,d='updated' where b=7");
            CheckResults(16,3,"table xa", "[{B:8,C:10,D:'changed'},{B:9,C:12,D:'Nine'}]");
            CheckResults(16,4,"table xb", "[{TOT:17}]");
            Act(168,"create table xc(totb int,totc int)");
            Act(169,"create trigger sdai instead of delete on xa referencing old table as ot " +
            "for each statement begin atomic insert into xc (select b,c from ot) end");
            Act(170,"delete from xa where d='changed'");
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
            Act(171,"create function reverse(a char) returns char "+
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
            Act(180,"create table author(id int primary key,aname char)");
            Act(181,"create table book(id int primary key,authid int references author,title char)");
            Act(182,"insert into author values (1,'Dickens'),(2,'Conrad')");
            Act(183,"insert into book(authid,title) values (1,'Dombey & Son'),(2,'Lord Jim'),(1,'David Copperfield')");
            Act(184,"create function booksby(auth char) returns table(title char) "+
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
            Act(190,"create table ga(a1 int primary key,a2 char)");
            Act(191,"insert into ga values(1,'First'),(2,'Second')");
            Act(192,"create function gather1() returns char "+
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
            Act(193,"create function gather2() returns char "+
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
            Act(200,"create type point as (x int, y int)");
            Act(201,"create type size as (w int,h int)");
            Act(202,"create type line as (strt point,en point)");
            Act(203,"create type rect as (tl point,sz size) "
              +"constructor method rect(x1 int, y1 int, x2 int, y2 int),"
              +"method centre() returns point");
            Act(204,"create table figure(id int primary key,title char)");
            Act(205,"create table figureline(id int primary key,fig int references figure,what line)");
            Act(206,"create table figurerect(id int primary key,fig int references figure,what rect)");
            Act(207,"create constructor method rect(x1 int,y1 int,x2 int,y2 int) "
              +"begin tl = point(x1, y1); sz = size(x2 - x1, y2 - y1) end");
            Act(208,"create method centre() returns point for rect "
              +"return point(tl.x + sz.w / 2, tl.y + sz.h / 2)");
            Act(209,"create function centrerect(a int) returns point "
              +"return (select what.centre() from figurerect where id = centrerect.a)");
            Act(210,"insert into figure values(1,'Diagram')");
            Act(211,"insert into figurerect values(1,1,rect(point(1,2),size(3,4)))");
            Act(212,"insert into figurerect values(2,1,rect(4,5,6,7))");
            Act(213,"insert into figureline values(1,1,line(centrerect(1),centrerect(2)))");
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
            Act(210,"create table members (id int primary key,firstname char)");
            Act(211,"create table played (id int primary key, winner int references members,"
              + "loser int references members,agreed boolean)");
            Act(212,"grant select on members to public");
            Act(213,"grant select on played to public");
            Act(214,"create procedure claim(won int,beat int)"
              + "insert into played(winner,loser) values(claim.won,claim.beat)");
            Act(215,"create procedure agree(p int)"
              + "update played set agreed=true "
               + "where winner=agree.p and loser in"
                + "(select m.id from members m where current_user like '%'||firstname escape '^')");
            Act(216,"insert into members(firstname) values(current_user)");
            CheckResults(21, 1, "select id from members where current_user like '%'||firstname escape'^'",
              "[{ID:1}]");
            Act(217,"insert into members(firstname) values('Fred')");
            Act(218,"insert into played(winner,loser) values(2,1)");
            Act(219,"create role membergames");
            Act(220,"grant execute on procedure claim(int,int) to role membergames");
            Act(221,"grant execute on procedure agree(int) to role membergames");
            Act(222,"grant membergames to public");
            Act(223,"set role membergames");
            Act(224,"call agree(2)");
            Act(225,"call claim(1,2)");
            CheckResults(21, 2, "table played", "[{ID:1,WINNER:2,LOSER:1,AGREED:true},{ID:2,WINNER:1,LOSER:2}]");
            Rollback();
        }
        public void Test22()
        {
            if (test > 0 && test != 22)
                return;
            testing = 22;
            Begin();
            Act(226,"create view WU of (e int, f char, g char) as get " +
                "etag url 'http://localhost:8180/A/A/D'");
            CheckResults(22, 1, "select * from wu", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}]");
            Act(227,"create table HU (e int primary key, k char, m int)");
            Act(228,"insert into HU values (1,'Cleaner',12500), (2,'Manager',31400)");
            Act(229,"create view VU as select * from wu natural join HU");
            CheckResults(22, 2, "select e, f, m from VU where e=1",
                "[{E:1,F:'Joe',M:12500}]"); // CHECK only works for committed tables/views
            Act(230,"insert into wu values(3,'Fred','Bloggs')");
            CheckResults(22, 3, "select * from wu", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'},"+
                "{E:3,F:'Fred',G:'Bloggs'}]");
            Act(231,"update vu set f='Elizabeth' where e=2");
            CheckResults(22, 4, "select * from wu where e=2", "[{E:2,F:'Elizabeth',G:'Boop'}]");
            Rollback();
        }
        public void Test23()
        {
            if (test > 0 && test != 23)
                return;
            testing = 23;
            // this test has its own explicit transaction control
            Act(232,"create view W of (e int, f char, g char) as get " +
                "etag 'http://localhost:8180/A/A/D'");
            CheckResults(23, 1, "select * from w", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}]");
            if (qry < 5)
            {
                Act(233,"create table H (e int primary key, k char, m int)");
                Act(234,"insert into H values (1,'Cleaner',12500), (2,'Manager',31400)");
                Act(235,"create view V as select * from w natural join H");
                CheckResults(22, 2, "select e, f, m from V where e=1", "[{E:1,F:'Joe',M:12500}]");
                Act(236,"insert into w values(3,'Fred','Bloggs')");
                CheckResults(23, 3, "select * from w", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}," +
                    "{E:3,F:'Fred',G:'Bloggs'}]");
                Act(237,"update v set f='Elizabeth' where e=2");
                CheckResults(23, 4, "select * from w where e=2", "[{E:2,F:'Elizabeth',G:'Boop'}]");
                ResetA();
            }
            // read-write conflicts
            if (qry == 0 || qry == 5)
            {
                tr = conn.BeginTransaction();
                cur = 238;
                Touch("select * from w where e=2");
                cur = 239;
                connA.Act("update d set f='Liz' where e=2");
                CheckExceptionCommit(23, 5, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 6)
            {
                tr = conn.BeginTransaction();
                Touch("select * from w where e=2");
                cur = 240;
                connA.Act("delete from d where e=2");
                cur = 241;
                CheckExceptionCommit(23, 6, "ETag validation failure");
                ResetA();
            }
            // read/write non conflicts
            if (qry == 0 || qry == 7)
            {
                tr = conn.BeginTransaction();
                cur = 242;
                Touch("select * from w where e=1");
                cur = 243;
                connA.Act("update d set f='Liz' where e=2");
                Commit(244);
                CheckResults(23, 7, "select * from w", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Liz',G:'Boop'}]");
                ResetA();
            }
            if (qry == 0 || qry == 8)
            {
                tr = conn.BeginTransaction();
                cur = 245;
                Touch("select * from w where e=1");
                cur = 246;
                connA.Act("delete from d where e=2");
                CheckResults(23, 8, "select * from w", "[{E:1,F:'Joe',G:'Soap'}]");
                tr.Commit();
                ResetA();
            }
            // write/write conflicts UU
            if (qry == 0 || qry == 9)
            {
                tr = conn.BeginTransaction();
                Act(247,"update w set f='Liz' where e=2");
                cur = 248;
                connA.Act("update d set f='Eliza' where e=2");
                CheckExceptionCommit(23, 9, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 10)
            {
                tr = conn.BeginTransaction(); // II
                Act(249,"insert into w values (3,'Fred','Bloggs')");
                cur = 250;
                connA.Act("insert into d values (3,'Anyone','Else')");
                CheckExceptionCommit(23, 10, "Integrity constraint:");
                ResetA();
            }
            if (qry == 0 || qry == 11)
            {
                tr = conn.BeginTransaction();  // UD
                Act(251,"update w set f='Liz' where e=2");
                cur = 252;
                connA.Act("delete from d where e=2");
                CheckExceptionCommit(23, 11, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 12)
            {
                tr = conn.BeginTransaction();  // DU
                Act(253,"delete from w where e=2");
                cur = 254;
                connA.Act("update d set f='Eliza' where e=2");
                CheckExceptionCommit(23, 12, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 13)
            {
                tr = conn.BeginTransaction();  // DD
                Act(255,"delete from w where e=2");
                cur = 256;
                connA.Act("delete from d where e=2");
                CheckExceptionCommit(23, 13, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 14)
            {
                tr = conn.BeginTransaction();  // non UU
                Act(257,"update w set f='Joseph' where e=1");
                cur = 258;
                connA.Act("update d set f='Eliza' where e=2");
                Commit(259);
                CheckResults(23, 14, "select * from w", "[{E:1,F:'Joseph',G:'Soap'},{E:2,F:'Eliza',G:'Boop'}]");
                ResetA();
            }
            if (qry == 0 || qry == 15)
            {
                tr = conn.BeginTransaction();  // non II
                Act(260,"insert into w values (4,'Some','Other')");
                cur = 261;
                connA.Act("insert into d values (3,'Anyone','Else')");
                Commit(262);
                CheckResults(23, 15, "select * from w", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}," +
                    "{E:3,F:'Anyone',G:'Else'},{E:4,F:'Some',G:'Other'}]");
                ResetA();
            }
            if (qry == 0 || qry == 16)
            {
                tr = conn.BeginTransaction(); // non UD
                Act(263,"update w set f='Joseph' where e=1");
                cur = 264;
                connA.Act("delete from d where e=2");
                Commit(265);
                CheckResults(23, 16, "select * from w", "[{E:1,F:'Joseph',G:'Soap'}]");
                ResetA();
            }
            if (qry == 0 || qry == 17)
            {
                tr = conn.BeginTransaction(); // non DU
                Act(266,"delete from w where e=2");
                cur = 267;
                connA.Act("update d set f='Joseph' where e=1");
                Commit(268);
                CheckResults(23, 17, "select * from w", "[{E:1,F:'Joseph',G:'Soap'}]");
                ResetA();
            }
        }
        public void Test24()
        {
            var user = Environment.UserDomainName + "\\" + Environment.UserName;
            var connB = new PyrrhoConnect("Files=DB");
            cur = 269;
            connB.Act("create table T(E int,F char)");
            cur = 270;
            connB.Act("insert into T values(3,'Three'),(6,'Six'),(4,'Vier'),(6,'Sechs')");
            cur = 271;
            connB.Act("create role DB");
            cur = 272;
            connB.Act("grant DB to \"" + user + "\"");
            connB.Close();
            var connC = new PyrrhoConnect("Files=DC");
            cur = 273;
            connC.Act("create table U(E int,F char)");
            cur = 274;
            connC.Act("insert into U values(5,'Five'),(4,'Four'),(8,'Ate')");
            cur = 275;
            connC.Act("create role DC");
            cur = 276;
            connC.Act("grant DC to \"" + user + "\"");
            cur = 277;
            connC.Close();
            Act(278,"create view V of (E int,F char) as get 'http://localhost:8180/DB/DB/t'");
            Act(279,"create table VU (d char primary key, k int, u char)");
            Act(280,"insert into VU values('B',4,'http://localhost:8180/DB/DB/t')");
            Act(281,"insert into VU values('C',1,'http://localhost:8180/DC/DC/u')");
            Act(282,"create view W of (E int, D char, K int, F char) as get using VU");
            Act(283,"create table M (e int primary key, n char, unique(n))");
            Act(284,"insert into M values (2,'Deux'),(3,'Trois'),(4,'Quatre')");
            Act(285,"insert into M values (5,'Cinq'),(6,'Six'),(7,'Sept')");
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
            Act(286,"update v set F='Tri' where E=3");
            Act(287,"insert into V values (9,'Nine')");
            CheckResults(24, 24, "select * from V", "[{E:3,F:'Tri'},{E:6,F:'Six'}," +
                "{E:4,F:'Vier'},{E:6,F:'Sechs'},{E:9,F:'Nine'}]");
            Act(288,"update w set f='Eight' where e=8");
            Act(289,"insert into w(D,E,F) values('B',7,'Seven')");
            CheckResults(24, 25, "select * from V","[{E:3,F:'Tri'},{E:6,F:'Six'},"+
                "{E:4,F:'Vier'},{E:6,F:'Sechs'},{E:9,F:'Nine'},{E:8,F:'Seven'}]");
            CheckResults(24, 26, "select * from W", "[{E:3,D:'B',K:4,F:'Three'}," +
                "{E:6,D:'B',K:4,F:'Six'},{E:4,D:'B',K:4,F:'Vier'},{E:6,D:'B',K:4,F:'Sechs'}," +
                "{E:9,D:'B',K:4,F:'Nine'},{E:7,D:'B',K:4,F:'Seven'},"+
                "{E:5,D:'C',K:1,F:'Five'},{E:4,D:'C',K:1,F:'Four'},{E:8,D:'C',K:1,F:'Eight'}]");
            Act(290,"delete from w where E=7");
            Act(291,"update v set f='Ate' where e=8");
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
                if (e.Message.StartsWith(m)||e.Message.StartsWith("Exception: "+m))
                    return;
                Console.WriteLine("Unexpected exception (" + t + " " + q + ") " + e.Message);
                return;
            }
            Console.WriteLine("Didnt get exception (" + t + " " + q + ") " + m);
            tr.Rollback();
        }
        void CheckExceptionQuery(int t, int q, string s, string m)
        {
            if (qry > 0 && qry != q)
                return;
            cur = q;
            try
            {
                var cmd = conn.CreateCommand();
                cmd.CommandText = s;
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
        void CheckExceptionNonQuery(int t, int q, string s, string m)
        {
            if (qry > 0 && qry != q)
            {
                if (s.ToUpper() == "COMMIT")
                    tr?.Rollback();
                return;
            }
            try
            {
                if (s.ToUpper() == "COMMIT")
                    Commit(q);
                else
                    Act(q,s);
            }
            catch (Exception e)
            {
                if (e.Message.StartsWith(m) || e.Message.StartsWith("Exception: "+m))
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
        void CheckResults(int t,int q,string s,string d)
        {
            if (qry > 0 && qry != q)
                return;
            cur = q;
            try
            {
                var cmd = conn.CreateCommand();
                cmd.CommandText = s;
                var r = cmd.ExecuteReader();
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
        void CheckExecuteResults(int t, int q, string d, string s, params string[] ps)
        {
            if (qry > 0 && qry != q)
                return;
            cur = q;
            try
            {
                var r = conn.ExecuteReader(s,ps);
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
