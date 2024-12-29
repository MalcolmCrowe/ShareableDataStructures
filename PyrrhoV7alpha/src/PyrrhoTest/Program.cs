using System;
using Pyrrho;

namespace Test
{
    class Program
    {
        static int test = 0, qry = 0, testing = 0, cur=0;
        bool commit = false;
        PyrrhoConnect conn;
        PyrrhoConnect? connA = null;
        PyrrhoTransaction? tr;
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
                Console.WriteLine("29 December 2024 Repeatable tests, AVOIDING TESTS 22-23  ");
                if (args.Length == 0)
                {
                    Console.WriteLine("Tests 22,23,24 need Server with +s");
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
        void Commit(int q,string? m=null)
        {
            cur = q;
            tr?.Commit(m??"");
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
                var user = Environment.UserDomainName + "\\" + Environment.UserName;
                connA = new PyrrhoConnect("Files=A");
                connA.Act("create table D(e int primary key,f char,g char)");
                connA.Act("insert into D values (1,'Joe','Soap'), (2,'Betty','Boop')");
                connA.Act("create role A");
                connA.Act("grant A to \"" + user +"\"");
              //  Test22();
              //  ResetA();
              //  Test23();
            } 
            Test24(); 
            Test25();
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
            Act(611, "create table td(e int,f int)");
            Act(612, "insert into td values(2,5),(4,6),(2,7),(6,8)");
            CheckResults(6, 2, "select distinct e from td", "[{E:2},{E:4},{E:6}]");
            CheckResults(6, 3, "select avg(distinct e) from td", "[{AVG:4}]");
            Rollback();
        }
        void Test7()
        {
            if (test > 0 && test != 7)
                return;
            testing = 7;
            Begin();
            Act(70, "create table TB(S char,D int,C int)");
            Act(71, "insert into TB values('Glasgow',2,43)");
            Act(72, "insert into TB values('Paisley',3,82)");
            Act(73, "insert into TB values('Glasgow',4,29)");
            CheckResults(7, 1, "select S,count(C) as occ,sum(C) as total from TB group by S",
                "[{S:\"Glasgow\",OCC:2,TOTAL:72},{S:\"Paisley\",OCC:1,TOTAL:82}]");
            // contributed by Fritz Laux
            Act(400, "CREATE TABLE people (Id INT(11) NOT NULL, Name VARCHAR(50) , Salary NUMERIC(7,2), "
                + "country VARCHAR(50), city VARCHAR(50) , PRIMARY KEY (Id) )");
            /* UK*/
            Act(401, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(1, 'Tom', 50000, 'UK', 'London')");
            Act(402, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(2, 'Alex', 60000, 'UK', 'London')");
            Act(403, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(3, 'Bob', 66000, 'UK', 'London')");
            Act(404, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(4, 'Anne', 62000, 'UK', 'London')");
            Act(405, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(5, 'Pam', 72000, 'UK', 'London')");
            Act(406, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(6, 'Liz', 52000, 'UK', 'London')");

            Act(407, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(7, 'Anne', 62000, 'UK', 'Glasgow')");
            Act(408, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(8, 'Claire', 62000, 'UK', 'Glasgow')");
            Act(409, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(9, 'Marc', 58000, 'UK', 'Glasgow')");
            Act(410, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(10, 'Carla', 62000, 'UK', 'Glasgow')");
            Act(411, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(11, 'Mary', 64000, 'UK', 'Glasgow')");
            Act(412, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(12, 'Martha', 63000, 'UK', 'Glasgow')");

            Act(413, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(20, 'Mike', 62000, 'UK', 'Liverpool')");
            Act(414, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(21, 'Paul', 62000, 'UK', 'Liverpool')");
            Act(415, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(22, 'John', 62000, 'UK', 'Liverpool')");

            /* Germany*/
            Act(416, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(61, 'Tom', 50000, 'GER', 'Berlin')");
            Act(417, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(62, 'Alex', 60000, 'GER', 'Berlin')");
            Act(418, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(63, 'Bob', 66000, 'GER', 'Berlin')");
            Act(419, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(64, 'Anne', 62000, 'GER', 'Berlin')");
            Act(420, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(65, 'Pam', 72000, 'GER', 'Berlin')");
            Act(421, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(66, 'Liz', 52000, 'GER', 'Berlin')");

            Act(422, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(67, 'Anne', 62000, 'GER', 'Munich')");
            Act(423, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(68, 'Claire', 62000, 'GER', 'Munich')");
            Act(424, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(69, 'Marc', 58000, 'GER', 'Munich')");
            Act(425, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(70, 'Carla', 62000, 'GER', 'Munich')");
            Act(426, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(71, 'Mary', 64000, 'GER', 'Munich')");
            Act(427, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(72, 'Martha', 63000, 'GER', 'Munich')");

            Act(428, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(80, 'Mike', 62000, 'GER', 'Tuebingen')");
            Act(429, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(81, 'Paul', 62000, 'GER', 'Tuebingen')");
            Act(430, "INSERT INTO people(Id, Name, Salary, country, city) VALUES(82, 'John', 62000, 'GER', 'Tuebingen')");

            /* Queries */

            CheckResults(7, 2, "select city, avg(Salary), count(*) as numPeople from people where country = 'GER' group by city having count(*) > 4",
                "[{CITY:'Berlin',AVG:60333.3333333333,NUMPEOPLE:6},{CITY:'Munich',AVG:61833.3333333333,NUMPEOPLE:6}]");
            /* MariaDB examples */ 
            Act(431, "CREATE TABLE students (\"name\" CHAR, test CHAR, score INT)");
            Act(432, "INSERT INTO students VALUES" +
              "('Chun', 'SQL', 75), ('Chun', 'Tuning', 73)," +
              "('Esben', 'SQL', 43), ('Esben', 'Tuning', 31)," +
              "('Kaolin', 'SQL', 56), ('Kaolin', 'Tuning', 88)," +
              "('Tatiana', 'SQL', 87), ('Tatiana', 'Tuning', 83)");
            CheckResults(7, 3, "SELECT \"name\", test, score, " +
                "AVG(score) OVER (PARTITION BY test) AS average_by_test," +
                "AVG(score) OVER (PARTITION BY \"name\") AS average_by_name FROM students",
                "[{name:'Chun',TEST:'SQL',SCORE:75,AVERAGE_BY_TEST:65.25,AVERAGE_BY_NAME:74}," +
                "{name:'Chun',TEST:'Tuning',SCORE:73,AVERAGE_BY_TEST:68.75,AVERAGE_BY_NAME:74}," +
                "{name:'Esben',TEST:'SQL',SCORE:43,AVERAGE_BY_TEST:65.25,AVERAGE_BY_NAME:37}," +
                "{name:'Esben',TEST:'Tuning',SCORE:31,AVERAGE_BY_TEST:68.75,AVERAGE_BY_NAME:37}," +
                "{name:'Kaolin',TEST:'SQL',SCORE:56,AVERAGE_BY_TEST:65.25,AVERAGE_BY_NAME:72}," +
                "{name:'Kaolin',TEST:'Tuning',SCORE:88,AVERAGE_BY_TEST:68.75,AVERAGE_BY_NAME:72}," +
                "{name:'Tatiana',TEST:'SQL',SCORE:87,AVERAGE_BY_TEST:65.25,AVERAGE_BY_NAME:85}," +
                "{name:'Tatiana',TEST:'Tuning',SCORE:83,AVERAGE_BY_TEST:68.75,AVERAGE_BY_NAME:85}]");
            Act(433, "CREATE TABLE users (email CHAR,first_name CHAR, last_name CHAR,  account_type CHAR)");
            Act(434, "INSERT INTO users VALUES ('admin@boss.org', 'Admin', 'Boss', 'admin')," +
                "('bob.carlsen@foo.bar', 'Bob', 'Carlsen','regular'),('eddie.stevens@data.org', 'Eddie', 'Stevens', 'regular')," +
                "('john.smith@xyz.org', 'John', 'Smith','regular'),('root@boss.org', 'Root', 'Chief', 'admin')");
            CheckResults(7, 4, "SELECT row_number() OVER (PARTITION BY account_type ORDER BY email) AS rnum, " +
                "email, first_name, last_name, account_type FROM users",
                "[{RNUM:1,EMAIL:'admin@boss.org',FIRST_NAME:'Admin',LAST_NAME:'Boss',ACCOUNT_TYPE:'admin'}," +
                "{RNUM:1,EMAIL:'bob.carlsen@foo.bar',FIRST_NAME:'Bob',LAST_NAME:'Carlsen',ACCOUNT_TYPE:'regular'}," +
                "{RNUM:2,EMAIL:'eddie.stevens@data.org',FIRST_NAME:'Eddie',LAST_NAME:'Stevens',ACCOUNT_TYPE:'regular'}," +
                "{RNUM:3,EMAIL:'john.smith@xyz.org',FIRST_NAME:'John',LAST_NAME:'Smith',ACCOUNT_TYPE:'regular'}," +
                "{RNUM:2,EMAIL:'root@boss.org',FIRST_NAME:'Root',LAST_NAME:'Chief',ACCOUNT_TYPE:'admin'}]");
             /* LearnSQL examples: our result has rows in table order which is ok! */
            Act(435, "create table employee(employee_id int, full_name char, Department char, salary numeric(8, 2))");
            Act(436, "insert into employee values(100, 'Mary Johns', 'SALES', 1000.00)," +
            "(101, 'Sean Moldy', 'IT', 1500.00), (102, 'Peter Dugan', 'SALES', 2000.00)," +
            "(103, 'Lilian Penn', 'SALES', 1700.00), (104, 'Milton Kowarsky', 'IT', 1800.00)," +
            "(105, 'Mareen Bisset', 'ACCOUNTS', 1200.00), (106, 'Airton Graue', 'ACCOUNTS', 1100.00)");
            CheckResults(7, 5, "select rank() over(partition by department order by salary desc) as dept_ranking," +
            "department, employee_id, full_name, salary from employee",
            "[{DEPT_RANKING:3,DEPARTMENT:'SALES',EMPLOYEE_ID:100,FULL_NAME:'Mary Johns',SALARY:1000.00}," +
            "{DEPT_RANKING:2,DEPARTMENT:'IT',EMPLOYEE_ID:101,FULL_NAME:'Sean Moldy',SALARY:1500.00}," +
            "{DEPT_RANKING:1,DEPARTMENT:'SALES',EMPLOYEE_ID:102,FULL_NAME:'Peter Dugan',SALARY:2000.00}," +
            "{DEPT_RANKING:2,DEPARTMENT:'SALES',EMPLOYEE_ID:103,FULL_NAME:'Lilian Penn',SALARY:1700.00}," +
            "{DEPT_RANKING:1,DEPARTMENT:'IT',EMPLOYEE_ID:104,FULL_NAME:'Milton Kowarsky',SALARY:1800.00}," +
            "{DEPT_RANKING:1,DEPARTMENT:'ACCOUNTS',EMPLOYEE_ID:105,FULL_NAME:'Mareen Bisset',SALARY:1200.00}," +
            "{DEPT_RANKING:2,DEPARTMENT:'ACCOUNTS',EMPLOYEE_ID:106,FULL_NAME:'Airton Graue',SALARY:1100.00}]");
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
            CheckResults(8, 9, "select sum(c), g from JA natural join JE group by g",
                "[{SUM:4,G:10},{SUM:3,G:22},{SUM:2,G:31}]");
            CheckResults(8, 10, "select sum(f),g from JA natural join JE where c=2 group by g",
                "[{SUM:7,G:31}]");
            CheckResults(8, 11, "select c,sum(f),g from JA natural join JE group by g,c having sum(f)<6",
                "[{C:3,SUM:4,G:22}]");
            CheckResults(8, 12, "select c,sum(d),sum(f),g from JA natural join JE group by g,c",
                "[{C:2,SUM:43,Col2:7,G:31},{C:3,SUM:82,Col2:4,G: 22},{C:4,SUM:29,Col2:11,G:10}]");
            // Test for lateral join (the keyword LATERAL is not required and not supported)
            Act(88,"create table SalesPerson(pid int primary key)");
            Act(89,"insert into SalesPerson values(1),(2),(3)"); 
            Act(90,"create table Sales(sid int primary key, spid int, cust int, amount int)"); 
            Act(91,"insert into Sales values(4,3,10,22),(5,2,11,12),(6,2,10,37)"); 
            Act(92,"insert into Sales values(7,1,12,7),(8,3,13,41),(9,1,12,17)");
            CheckResults(8, 13, "select * from SalesPerson,"+
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
                "{B: 0, C: 1.234567E-13, D: 1234},{B: 12, C: 1234,D: 0.00045}," +
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
            Act(100,"create table RWC(A int primary key,B char,C int)");
            Act(101,"create table RRC(D char primary key,E int references RWC)");
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
            cmd.CommandText = "select * from RRC";
            r = Read(cmd, 111);
            task1 = Task.Factory.StartNew(() => Test10A(112));
            task1.Wait();
            Commit(113);
            Begin();
            Act(114,"update RWC set C=3 where A=52");
            task1 = Task.Factory.StartNew(() => Test10C(115));
            task1.Wait();
            CheckExceptionNonQuery(10, 2, "Commit", "Transaction conflict");
            Begin();
            CheckResults(10,3,"select * from RWC","[{A:42,B:'the product of 6 and 9',C:3},{A:52,B:'Weeks in the year',C:4}]");
            task1 = Task.Factory.StartNew(() => Test10B(116));
            task1.Wait();
            CheckExceptionNonQuery(10, 4, "Commit", "Transaction conflict");
            Begin();
            Act(117,"insert into RWC values(13,'Lucky for some',4)");
            task1 = Task.Factory.StartNew(() => Test10D(118));
            task1.Wait();
            CheckExceptionNonQuery(10, 5, "Commit", "Transaction conflict");
            Begin();
            Act(119,"delete from RWC where a=52"); 
            task1 = Task.Factory.StartNew(() => Test10E(120));
            task1.Wait();
            CheckExceptionNonQuery(10, 6, "Commit", "Transaction conflict");
            Begin();
            Act(121,"update RRC set E=42 where D='Douglas Adams'");
            task1 = Task.Factory.StartNew(() => Test10F(122));
            task1.Wait();
            CheckExceptionNonQuery(10, 7, "Commit", "Transaction conflict"); 
            Begin();
            Act(123,"update RWC set A=12,B='Dozen' where A=13");
            task1 = Task.Factory.StartNew(() => Test10G(124));
            task1.Wait();
            CheckExceptionNonQuery(10, 8, "Commit", "Transaction conflict");
            Act(125,"delete from RWC where A=12");
            Begin();
            Act(126,"insert into RWC values (12,'Dozen',7)");
            task1 = Task.Factory.StartNew(() => Test10H(127));
            task1.Wait();
            CheckExceptionNonQuery(10, 9, "Commit", "Transaction conflict"); 
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
                Act(134,"create table cs(b int not null,c int default 4,d int generated always as b+c)");
                CheckExceptionNonQuery(11, 1, "insert into cs(c) values(5)", "Null value not allowed");
            }
            if (qry == 0 || qry == 2)
            {
                Begin();
                Act(135,"create table cs(b int not null,c int default 4,d int generated always as b+c)");
                Act(136,"insert into cs(b) values(3)");
                CheckExceptionNonQuery(11, 2, "insert into cs values(1,2,3)", "Record data, field unassignable");
            }
            if (qry == 0 || qry == 3)
            {
                Begin();
                Act(137,"create table cs(b int not null,c int default 4,d int generated always as b+c)");
                Act(138,"insert into cs(b) values(3)");
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
            Act(139, "create table sce(a int primary key,b char)");
            Act(140, "insert into sce values(12,'Zodiac')");
            Act(141, "insert into sce values(13,'Bakers')");
            Act(142, "insert into sce values(14,'Fortnight')");
            Act(143, "create table dst(c int)");
            Act(144, "insert into dst (select a from sce where b<'H')");
            CheckResults(12, 1, "select * from dst", "[{C:13},{C:14}]");
            CheckResults(12, 2, "select a from sce where b in('Fortnight','Zodiac')",
               "[{A:12},{A:14}]");
            CheckResults(12, 3, "select * from dst where c in (select a from sce where b='Bakers')",
                "[{C:13}]");
            Act(145, "insert into dst(c) (select max(x.a)+4 from sce x where x.b<'H')");
            CheckResults(12, 4, "select * from dst", "[{C:13},{C:14},{C:18}]");
            Act(146, "insert into dst (select min(x.c)-3 from dst x)");
            CheckResults(12, 5, "select * from dst", "[{C:13},{C:14},{C:18},{C:10}]");
            if (commit)
            {
                Prepare(147, "Ins1", "insert into sce values(?,?)");
                Prepare(148, "Upd1", "update sce set a=? where b=?");
                Prepare(149, "Del1", "delete from dst where c>?");
                Prepare(150, "Ins2", "insert into dst (select char_length(b) from sce where a=?)");
                Prepare(151, "Sel1", "select * from dst where c<?");
                Execute(152, "Ins1", "" + 5, "'HalfDozen'");
                Execute(153, "Upd1", "" + 6, "'HalfDozen'");
                Execute(154, "Del1", "" + 10);
                Execute(156, "Ins2", "" + 6);
                CheckResults(12, 6, "select * from sce where a=6", "[{A:6,B:'HalfDozen'}]");
                CheckResults(12, 7, "select * from dst", "[{C:10},{C:9}]");
                CheckExecuteResults(12, 8, "[{C:9}]", "Sel1", "" + 10);
            }
            Act(157, "create table pt(q int primary key,r char,a int)");
            Act(158, "create view v as select q,r as s,a from pt");
            Act(159, "insert into v(s) values('Twenty'),('Thirty')");
            Act(160, "update v set s='Forty two' where q=1");
            CheckResults(12, 9, "select r from pt", "[{R:'Forty two'},{R:'Thirty'}]");
            Act(161, "delete from v where s='Thirty'");
            Act(162, "insert into pt(r) values('Fifty')");
            Act(163, "create table t(s char,u int)");
            Act(164, "insert into t values('Forty two',42),('Fifty',48)");
            Act(165, "create view w as select * from t natural join v");
            Act(166, "update w set u=50,a=21 where q=2");
            CheckResults(12, 10, "table pt", "[{Q:1,R:'Forty two'},{Q:2,R:'Fifty',A:21}]");
            CheckResults(12, 11, "table t", "[{S:'Forty two',U:42},{S:'Fifty',U:50}]");
            /* Fritz Laux example */
            Act(450, "create table umsatz (kunde char(12) primary key, KdUmsatz numeric(8,2))");
            Act(451, "insert into umsatz values ('Bosch' , 17000.00),('Boss' ,  13000.00), ('Daimler',20000.00)");
            Act(452, "insert into umsatz values ('Siemens', 9000.00),('Porsche', 5000.00), ('VW'     , 8000.00), ('Migros' , 4000.00)");
            Act(453, "CREATE VIEW umsatz_V(kunde, KdUmsatz, runningSalesShare)" +
                "AS SELECT kunde, KdUmsatz," +
                        "(SELECT SUM(KdUmsatz) FROM umsatz WHERE KdUmsatz >= u.KdUmsatz) /" +
                    "(SELECT SUM(KdUmsatz) FROM umsatz)" +
                "FROM umsatz AS u");
            CheckResults(12, 12, "SELECT CASE WHEN runningSalesShare <= 0.5 THEN 'A' "+
                            "WHEN runningSalesShare > 0.5  AND "+
                                 "runningSalesShare <= 0.85 THEN 'B' "+
                            "WHEN runningSalesShare > 0.85 THEN 'C' "+
                            "ELSE NULL "+
                            "END AS Category,"+
                       "kunde, KdUmsatz,"+
                       "CAST(CAST(KdUmsatz / (SELECT SUM(KdUmsatz) FROM umsatz_V) * 100 "+
                            "as decimal(6, 2))"+
                       "as char(6)) || ' %' AS share "+
                "FROM umsatz_V "+
                "ORDER BY KdUmsatz DESC", 
                "[{CATEGORY:'A',KUNDE:'Daimler',KDUMSATZ:20000.00,SHARE:'26.32 %'},"+
                "{CATEGORY:'A',KUNDE:'Bosch',KDUMSATZ:17000.00,SHARE:'22.37 %'}," +
                "{CATEGORY:'B',KUNDE:'Boss',KDUMSATZ:13000.00,SHARE:'17.11 %'}," +
                "{CATEGORY:'B',KUNDE:'Siemens',KDUMSATZ:9000.00,SHARE:'11.84 %'}," +
                "{CATEGORY:'C',KUNDE:'VW',KDUMSATZ:8000.00,SHARE:'10.53 %'}," +
                "{CATEGORY:'C',KUNDE:'Porsche',KDUMSATZ:5000.00,SHARE:'6.58 %'}," +
                "{CATEGORY:'C',KUNDE:'Migros',KDUMSATZ:4000.00,SHARE:'5.26 %'}]");
            Rollback();
        }
        void Test13()
        {
            if (test > 0 && test != 13)
                return;
            testing = 13;
            Begin();
            Act(167,"create table ad(a int,b char)");
            Act(168,"insert into ad values(20,'Twenty')");
            Act(169, "alter table ad add c char not null"); 
            Act(170,"alter table ad alter c set default 'XX'");
            Act(172,"insert into ad(a,b) values(2,'Two')");
            CheckResults(13, 2, "select * from ad", "[{A:20,B:'Twenty'},{A:2,B:'Two',C:'XX'}]"); // results for column C modified Feb 24
            Act(173,"alter table ad drop b");
            CheckResults(13,3,"select * from ad", "[{A:20},{A:2,C:'XX'}]");
            Act(174,"alter table ad add primary key(a)");
            Act(175,"insert into ad values(21,'AB')");
            Act(176,"create table de (d int references ad)");
            if (qry == 0 || qry == 4)
            {
                CheckExceptionNonQuery(13, 4, "insert into de values(14)", "Integrity constraint: missing foreign key");
                if (!commit)
                {
                    Begin();
                    Act(177,"create table ad(a int,b char)");
                    Act(178,"insert into ad values(20,'Twenty')");
                    Act(179, "alter table ad add c char not null");
                    Act(180, "alter table ad alter c set default 'XX'");
                    Act(181,"insert into ad(a,b) values(2,'Two')");
                    Act(182,"alter table ad drop b");
                    Act(183,"alter table ad add primary key(a)");
                    Act(184,"insert into ad values(21,'AB')");
                    Act(185,"create table de (d int references ad)");
                }
            }
            Act(186,"insert into de values(21)");
            if (qry == 0 || qry == 5)
            {
                CheckExceptionNonQuery(13, 5, "delete from ad where c='AB'", "Integrity constraint: RESTRICT - foreign key in use");
                if (!commit)
                {
                    Begin();
                    Act(187,"create table ad(a int,b char)");
                    Act(188,"insert into ad values(20,'Twenty')");
                    Act(189, "alter table ad add c char not null");
                    Act(190, "alter table ad alter c set default 'XX'");
                    Act(191,"insert into ad(a,b) values(2,'Two')");
                    Act(192,"alter table ad drop b");
                    Act(193,"alter table ad add primary key(a)");
                    Act(194,"insert into ad values(21,'AB')");
                    Act(195,"create table de (d int references ad)");
                    Act(196,"insert into de values(21)");
                }
            }
            if (qry == 0 || qry == 6)
            {
                CheckExceptionNonQuery(13, 6, "drop ad", "RESTRICT: Index");
                if (!commit)
                {
                    Begin();
                    Act(197,"create table ad(a int,b char)");
                    Act(198,"insert into ad values(20,'Twenty')");
                    Act(199, "alter table ad add c char not null");
                    Act(200, "alter table ad alter c set default 'XX'");
                    Act(201,"insert into ad(a,b) values(2,'Two')");
                    Act(202,"alter table ad drop b");
                    Act(203,"alter table ad add primary key(a)");
                    Act(204,"insert into ad values(21,'AB')");
                    Act(205,"create table de (d int references ad)");
                    Act(206,"insert into de values(21)");
                }
            }
            Act(207,"drop de cascade");
            Act(208,"alter table ad drop primary key(a)");
            CheckResults(13,7, "select * from ad", "[{A:20},{A:2,C:'XX'},{A:21,C:'AB'}]");
            Act(209,"insert into ad(a) values(13)");
            CheckResults(13,8, "select * from ad", "[{A:20},{A:2,C:'XX'},{A:21,C:'AB'},{A:13,C:'XX'}]");
            Act(210,"drop ad");
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
            Act(211,"create table fi(a int primary key, b char)");
            Act(212,"create table se(c char primary key, d int references fi on delete cascade)");
            Act(213,"insert into fi values(1066,'invasion'),(1953,'accession'),(2019, 'brexit')");
            Act(214,"insert into se values('disaster', 2019),('elizabeth',1953),('johnson',2019)");
            Act(215,"insert into se values('normans',1066),('hastings', 1066)");
            Act(216,"delete from fi where a = 1066");
            CheckResults(14, 1, "table se", "[{C:'disaster',D:2019},{C:'elizabeth',D:1953},{C:'johnson',D:2019}]");
            Act(217,"alter table se set (d) references fi on delete restrict");
            CheckExceptionNonQuery(14,2,"delete from fi where a = 2019","Integrity constraint: RESTRICT");
            if (!commit)
            {
                Begin();
                Act(218,"create table fi(a int primary key, b char)");
                Act(219,"create table se(c char primary key, d int references fi on delete cascade)");
                Act(220,"insert into fi values(1066,'invasion'),(1953,'accession'),(2019, 'brexit')");
                Act(221,"insert into se values('disaster', 2019),('elizabeth',1953),('johnson',2019)");
                Act(222,"insert into se values('normans',1066),('hastings', 1066)");
                Act(223,"delete from fi where a = 1066");
            }
            Act(224,"alter table se set(d) references fi on delete set null on update cascade");
            Act(225,"update fi set a = 2020 where a = 2019");
            CheckResults(14,3, "table se", "[{C:'disaster',D:2020},{C:'elizabeth',D:1953},{C:'johnson',D:2020}]");
            Act(226,"delete from fi where a = 2020");
            CheckResults(14,4, "table se", "[{C:'disaster'},{C:'elizabeth',D:1953},{C:'johnson'}]");
            Rollback();
        }
        public void Test15()
        {
            if (test > 0 && test != 15)
                return;
            testing = 15;
            Begin();
            Act(227,"create table ca(a char,b int check (b>0))");
            CheckExceptionNonQuery(15, 1, "insert into ca values('Neg',-99)","Column check");
            if (!commit)
            {
                Begin();
                Act(228,"create table ca(a char,b int check (b>0))");
            }
            Act(229,"insert into ca values('Pos',45)");
            CheckResults(15, 2, "table ca", "[{A:'Pos',B:45}]");
            Rollback();
        }
        public void Test16()
        {
            if (test > 0 && test != 16)
                return;
            testing = 16;
            Begin();
            Act(230,"create table xa(b int,c int,d char)");
            Act(231,"create table xb(tot int)");
            Act(232,"insert into xb values (0)");
            Act(233,"create trigger ruab before update on xa referencing old as mr new as nr " +
            "for each row begin atomic update xb set tot=tot-mr.b+nr.b; " +
            "set d='changed' end");
            Act(234,"create trigger riab before insert on xa " +
            "for each row begin atomic set c=b+3; update xb set tot=tot+b end");
            Act(235,"insert into xa(b,d) values (7,'inserted')");
            Act(236,"insert into xa(b, d) values(9, 'Nine')");
            CheckResults(16,1,"table xa", "[{B:7,C:10,D:'inserted'},{B:9,C:12,D:'Nine'}]");
            CheckResults(16,2,"table xb", "[{TOT:16}]");
            Act(237,"update xa set b=8,d='updated' where b=7");
            CheckResults(16,3,"table xa", "[{B:8,C:10,D:'changed'},{B:9,C:12,D:'Nine'}]");
            CheckResults(16,4,"table xb", "[{TOT:17}]");
            Act(238,"create table xc(totb int,totc int)");
            Act(239,"create trigger sdai instead of delete on xa referencing old table as ot " +
            "for each statement begin atomic insert into xc (select b,c from ot) end");
            Act(240,"delete from xa where d='changed'");
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
            Act(241,"create function reverse(a char) returns char "+
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
            Act(242,"create table author(id int primary key,aname char)");
            Act(243,"create table book(id int primary key,authid int references author,title char)");
            Act(244,"insert into author values (1,'Dickens'),(2,'Conrad')");
            Act(245,"insert into book(authid,title) values (1,'Dombey & Son'),(2,'Lord Jim'),(1,'David Copperfield')");
            Act(246,"create function booksby(auth char) returns table(title char) "+
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
            Act(247,"create table ga(a1 int primary key,a2 char)");
            Act(248,"insert into ga values(1,'First'),(2,'Second')");
            Act(249,"create function gather1() returns char "+
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
            Act(250,"create function gather2() returns char "+
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
            Act(251,"create type point as (x int, y int)");
            Act(252,"create type rectsize as (w int,h int)");
            Act(253,"create type line as (strt point,en point)");
            Act(254,"create type rect as (tl point,sz rectsize) "
              +"constructor method rect(x1 int, y1 int, x2 int, y2 int),"
              +"method centre() returns point");
            Act(255,"create table figure(id int primary key,title char)");
            Act(256,"create table figureline(id int primary key,fig int references figure,what line)");
            Act(257,"create table figurerect(id int primary key,fig int references figure,what rect)");
            Act(258,"create constructor method rect(x1 int,y1 int,x2 int,y2 int) "
              +"begin tl = point(x1, y1); sz = rectsize(x2 - x1, y2 - y1) end");
            Act(259,"create method centre() returns point for rect "
              +"return point(tl.x + sz.w / 2, tl.y + sz.h / 2)");
            Act(260,"create function centrerect(a int) returns point "
              +"return (select what.centre() from figurerect where id = centrerect.a)");
            Act(261,"insert into figure values(1,'Diagram')");
            Act(262,"insert into figurerect values(1,1,rect(point(1,2),rectsize(3,4)))");
            Act(263,"insert into figurerect values(2,1,rect(4,5,6,7))");
            Act(264,"insert into figureline values(1,1,line(centrerect(1),centrerect(2)))");
            CheckResults(20, 1, "select what from figureline",
                "[{WHAT:'LINE(STRT=POINT(X=2,Y=4),EN=POINT(X=5,Y=6))'}]");
            Rollback();
        }
        public void Test21()
        {
            if (!commit) // not allowed to set role during transaction
                return;
            if (test > 0 && test != 21)
                return;
            testing = 21;
            Begin();
            Act(265,"create table members (id int primary key,firstname char)");
            Act(266,"create table played (id int primary key, winner int references members,"
              + "loser int references members,agreed boolean)");
            Act(267,"grant select on members to public");
            Act(268,"grant select on played to public");
            Act(269,"create procedure claim(won int,beat int)"
              + "insert into played(winner,loser) values(claim.won,claim.beat)");
            Act(270,"create procedure agree(p int)"
              + "update played set agreed=true "
               + "where winner=agree.p and loser in"
                + "(select m.id from members m where user like '%'||firstname escape '^')");
            Act(271,"insert into members(firstname) values(user)");
            CheckResults(21, 1, "select id from members where user like '%'||firstname escape'^'",
              "[{ID:1}]");
            Act(272,"insert into members(firstname) values('Fred')");
            Act(273,"insert into played(winner,loser) values(2,1)");
            Act(274, "create role admin");
            Act(275,"create role membergames");
            Act(276,"grant execute on procedure claim(int,int) to role membergames");
            Act(277,"grant execute on procedure agree(int) to role membergames");
            Act(278,"grant membergames to public");
            Act(279,"set role membergames");
            Act(280,"call agree(2)");
            Act(281,"call claim(1,2)");
            Act(282, "set role admin");
            CheckResults(21, 2, "table played", "[{ID:1,WINNER:2,LOSER:1,AGREED:true},{ID:2,WINNER:1,LOSER:2}]");
            Rollback();
        }
        public void Test22()
        {
            if (test > 0 && test != 22)
                return;
            testing = 22;
            Begin();
            Act(283,"create view WU of (e int, f char, g char) as get " +
                "etag url 'http://localhost:8180/A/A/D'");
            CheckResults(22, 1, "select * from wu", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}]");
            Act(284,"create table HU (e int primary key, k char, m int)");
            Act(285,"insert into HU values (1,'Cleaner',12500), (2,'Manager',31400)");
            Act(286,"create view VU as select * from wu natural join HU");
            CheckResults(22, 2, "select e, f, m from VU where e=1",
                "[{E:1,F:'Joe',M:12500}]"); // CHECK only works for committed tables/views
            Act(287,"insert into wu values(3,'Fred','Bloggs')");
            CheckResults(22, 3, "select * from wu", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'},"+
                "{E:3,F:'Fred',G:'Bloggs'}]");
            Act(288,"update vu set f='Elizabeth' where e=2");
            CheckResults(22, 4, "select * from wu where e=2", "[{E:2,F:'Elizabeth',G:'Boop'}]");
            Rollback();
        }
        public void Test23()
        {
            if (test > 0 && test != 23)
                return;
            testing = 23;
            // this test has its own explicit transaction control
            Act(284,"create view WW of (e int, f char, g char) as get " +
                "etag 'http://localhost:8180/A/A/D'");
            CheckResults(23, 1, "select * from ww", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}]");
            if (qry < 5)
            {
                Act(289, "create table H (e int primary key, k char, m int)");
                Act(290, "insert into H values (1,'Cleaner',12500), (2,'Manager',31400)");
                Act(291, "create view VV as select * from ww natural join H");
                CheckResults(23, 2, "select e, f, m from VV where e=1", "[{E:1,F:'Joe',M:12500}]");
                Act(292, "insert into ww values(3,'Fred','Bloggs')");
                CheckResults(23, 3, "select * from ww", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}," +
                    "{E:3,F:'Fred',G:'Bloggs'}]");
                Act(293, "update vv set f='Elizabeth' where e=2");
                CheckResults(23, 4, "select * from ww where e=2", "[{E:2,F:'Elizabeth',G:'Boop'}]");
                ResetA();
            }
            // read-write conflicts
            if (qry == 0 || qry == 5)
            {
                tr = conn.BeginTransaction();
                cur = 294;
                Touch("select * from ww where e=2");
                cur = 295;
                connA.Act("update d set f='Liz' where e=2");
                CheckExceptionCommit(23, 5, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 6)
            {
                tr = conn.BeginTransaction();
                Touch("select * from ww where e=2");
                cur = 296;
                connA.Act("delete from d where e=2");
                cur = 297;
                CheckExceptionCommit(23, 6, "ETag validation failure");
                ResetA();
            }
            // read/write non conflicts
            if (qry == 0 || qry == 7)
            {
                tr = conn.BeginTransaction();
                cur = 298;
                Touch("select * from ww where e=1");
                cur = 299;
                connA.Act("update d set f='Liz' where e=2");
                Commit(300);
                CheckResults(23, 7, "select * from ww", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Liz',G:'Boop'}]");
                ResetA();
            }
            if (qry == 0 || qry == 8)
            {
                tr = conn.BeginTransaction();
                cur = 301;
                Touch("select * from ww where e=1");
                cur = 302;
                connA.Act("delete from d where e=2");
                CheckResults(23, 8, "select * from ww", "[{E:1,F:'Joe',G:'Soap'}]");
                tr.Commit();
                ResetA();
            }
            // write/write conflicts UU
            if (qry == 0 || qry == 9)
            {
                tr = conn.BeginTransaction();
                Act(303,"update ww set f='Liz' where e=2");
                cur = 304;
                connA.Act("update d set f='Eliza' where e=2");
                CheckExceptionCommit(23, 9, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 10)
            {
                tr = conn.BeginTransaction(); // II
                Act(305,"insert into ww values (3,'Fred','Bloggs')");
                cur = 306;
                connA.Act("insert into d values (3,'Anyone','Else')");
                CheckExceptionCommit(23, 10, "Integrity constraint:");
                ResetA();
            }
            if (qry == 0 || qry == 11)
            {
                tr = conn.BeginTransaction();  // UD
                Act(307,"update ww set f='Liz' where e=2");
                cur = 308;
                connA.Act("delete from d where e=2");
                CheckExceptionCommit(23, 11, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 12)
            {
                tr = conn.BeginTransaction();  // DU
                Act(309,"delete from ww where e=2");
                cur = 310;
                connA.Act("update d set f='Eliza' where e=2");
                CheckExceptionCommit(23, 12, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 13)
            {
                tr = conn.BeginTransaction();  // DD
                Act(311,"delete from ww where e=2");
                cur = 312;
                connA.Act("delete from d where e=2");
                CheckExceptionCommit(23, 13, "ETag validation failure");
                ResetA();
            }
            if (qry == 0 || qry == 14)
            {
                tr = conn.BeginTransaction();  // non UU
                Act(313,"update ww set f='Joseph' where e=1");
                cur = 314;
                connA.Act("update d set f='Eliza' where e=2");
                Commit(315);
                CheckResults(23, 14, "select * from ww", "[{E:1,F:'Joseph',G:'Soap'},{E:2,F:'Eliza',G:'Boop'}]");
                ResetA();
            }
            if (qry == 0 || qry == 15)
            {
                tr = conn.BeginTransaction();  // non II
                Act(316,"insert into ww values (4,'Some','Other')");
                cur = 317;
                connA.Act("insert into d values (3,'Anyone','Else')");
                Commit(318);
                CheckResults(23, 15, "select * from ww", "[{E:1,F:'Joe',G:'Soap'},{E:2,F:'Betty',G:'Boop'}," +
                    "{E:3,F:'Anyone',G:'Else'},{E:4,F:'Some',G:'Other'}]");
                ResetA();
            }
            if (qry == 0 || qry == 16)
            {
                tr = conn.BeginTransaction(); // non UD
                Act(319,"update ww set f='Joseph' where e=1");
                cur = 320;
                connA.Act("delete from d where e=2");
                Commit(321);
                CheckResults(23, 16, "select * from ww", "[{E:1,F:'Joseph',G:'Soap'}]");
                ResetA();
            }
            if (qry == 0 || qry == 17)
            {
                tr = conn.BeginTransaction(); // non DU
                Act(322,"delete from ww where e=2");
                cur = 323;
                connA.Act("update d set f='Joseph' where e=1");
                Commit(324);
                CheckResults(23, 17, "select * from ww", "[{E:1,F:'Joseph',G:'Soap'}]");
                ResetA();
            }
        }
        public void Test24()
        {
            if (test>0 && test != 24)
                return;
            testing = 24;
            var user = Environment.UserDomainName + "\\" + Environment.UserName;
            var connB = new PyrrhoConnect("Files=DB");
            cur = 325;
            connB.Act("create table T(E int,F char)");
            cur = 326;
            connB.Act("insert into T values(3,'Three'),(6,'Six'),(4,'Vier'),(6,'Sechs')");
            cur = 327;
            connB.Act("create role DB");
            cur = 328;
            connB.Act("grant DB to \"" + user + "\"");
            connB.Close();
            var connC = new PyrrhoConnect("Files=DC");
            cur = 329;
            connC.Act("create table U(E int,F char)");
            cur = 330;
            connC.Act("insert into U values(5,'Five'),(4,'Four'),(8,'Ate')");
            cur = 331;
            connC.Act("create role DC");
            cur = 332;
            connC.Act("grant DC to \"" + user + "\"");
            cur = 333;
            connC.Close();
            Act(334,"create view BV of (E int,F char) as get 'http://localhost:8180/DB/DB/t'");
            Act(335,"create table BU (d char primary key, k int, u char)");
            Act(336,"insert into BU values('B',4,'http://localhost:8180/DB/DB/t')");
            Act(337,"insert into BU values('C',1,'http://localhost:8180/DC/DC/u')");
            Act(338,"create view BW of (E int, D char, K int, F char) as get using BU");
            Act(339,"create table M (e int primary key, n char, unique(n))");
            Act(340,"insert into M values (2,'Deux'),(3,'Trois'),(4,'Quatre')");
            Act(341,"insert into M values (5,'Cinq'),(6,'Six'),(7,'Sept')");
            CheckResults(24, 1, "select * from bv", "[{E:3,F:'Three'},{E:6,F:'Six'}," +
                "{E:4,F:'Vier'},{E:6,F:'Sechs'}]");
            CheckResults(24, 2, "select * from BV where e=6", "[{E:6,F:'Six'},{E:6,F:'Sechs'}]");
            CheckResults(24, 3, "select * from bw", "[{E:3,D:'B',K:4,F:'Three'}," +
                "{E:6,D:'B',K:4,F:'Six'},{E:4,D:'B',K:4,F:'Vier'},{E:6,D:'B',K:4,F:'Sechs'}," +
                "{E:5,D:'C',K:1,F:'Five'},{E:4,D:'C',K:1,F:'Four'},{E:8,D:'C',K:1,F:'Ate'}]");
            CheckResults(24, 4, "select * from bw where e<6", "[{E:3,D:'B',K:4,F:'Three'}," +
                "{E:4,D:'B',K:4,F:'Vier'},{E:5,D:'C',K:1,F:'Five'},{E:4,D:'C',K:1,F:'Four'}]");
            CheckResults(24, 5, "select * from bw where k=1", "[{E:5,D:'C',K:1,F:'Five'}," +
                "{E:4,D:'C',K:1,F:'Four'},{E:8,D:'C',K:1,F:'Ate'}]");
            CheckResults(24, 6, "select count(e) from bw", "[{COUNT:7}]");
            CheckResults(24, 7, "select count(*) from bw", "[{COUNT:7}]");
            CheckResults(24, 8, "select max(f) from bw", "[{MAX:'Vier'}]");
            CheckResults(24, 9, "select max(f) from bw where e>4", "[{MAX:'Six'}]");
            CheckResults(24, 10, "select count(*) from bw where k>2", "[{COUNT:4}]");
            CheckResults(24, 11, "select min(f) from bw", "[{MIN:'Ate'}]");
            CheckResults(24, 12, "select sum(e)*sum(e),d from bw group by d",
                "[{Col0:361,D:'B'},{Col0:289,D:'C'}]");
            CheckResults(24, 13, "select count(*),k/2 as k2 from bw group by k2",
                "[{COUNT:3,K2:0},{COUNT:4,K2:2}]");
            CheckResults(24, 14, "select avg(e) from bw", "[{AVG:5.14285714285714}]");
            /* E	D	K	F	    N
                3	B	4	Three	Trois
                4	B	4	Vier	Quatre
                4	C	1	Four	Quatre
                5	C	1	Five	Cinq
                6	B	4	Six	    Six
                6	B	4	Sechs	Six	 */
            CheckResults(24, 15, "select f,n from bw natural join m", "[{F:'Three',N:'Trois'}," +
                "{F:'Vier',N:'Quatre'},{F:'Four',N:'Quatre'},{F:'Five',N:'Cinq'}," +
                "{F:'Six',N:'Six'},{F:'Sechs',N:'Six'}]");
            CheckResults(24, 16, "select e+char_length(f) as x,n from bw natural join m",
                "[{X:8,N:'Trois'},{X:8,N:'Quatre'},{X:8,N:'Quatre'}," +
                "{X:9,N:'Cinq'},{X:9, N:'Six'},{X:11,N:'Six'}]");
            CheckResults(24, 17, "select char_length(f)+char_length(n) from bw natural join m",
                "[{Col0:10},{Col0:10},{Col0:10},{Col0:8},{Col0:6},{Col0:8}]");
            CheckResults(24, 18, "select sum(e)+char_length(max(f)) from bw", "[{Col0:40}]");
            CheckResults(24, 19, "select count(*),e+char_length(f) as x from bw group by x",
                "[{COUNT:3,X:8},{COUNT:2,X:9},{COUNT:2,X:11}]");
            CheckResults(24, 20, "select count(*),e+char_length(n) as x from bw natural join m group by x",
                "[{COUNT:1,X:8},{COUNT:3,X:9},{COUNT:2,X:10}]");
            CheckResults(24, 21, "select sum(e)+char_length(f),f  from bw natural join m group by f",
                "[{Col0:9,F:'Five'},{Col0:8,F:'Four'},{Col0:11,F:'Sechs'}," +
                "{Col0:9,F:'Six'},{Col0:8,F:'Three'},{Col0:8,F:'Vier'}]");
            CheckResults(24, 22, "select sum(char_length(f))+char_length(n) as x,n from bw natural join m group by n",
                "[{X:8,N:'Cinq'},{X:14,N:'Quatre'},{X:11,N:'Six'},{X:10,N:'Trois'}]");
            CheckResults(24, 23, "Select count(*) from bw natural join m","[{COUNT:6}]");
            Act(342,"update bv set F='Tri' where E=3");
            Act(343,"insert into BV values (9,'Nine')");
            CheckResults(24, 24, "select * from BV", "[{E:3,F:'Tri'},{E:6,F:'Six'}," +
                "{E:4,F:'Vier'},{E:6,F:'Sechs'},{E:9,F:'Nine'}]");
            Act(344,"update bw set f='Eight' where e=8");
            Act(345,"insert into bw(D,E,F) values('B',7,'Seven')");
            CheckResults(24, 25, "select * from BV","[{E:3,F:'Tri'},{E:6,F:'Six'},"+
                "{E:4,F:'Vier'},{E:6,F:'Sechs'},{E:9,F:'Nine'},{E:7,F:'Seven'}]");
            CheckResults(24, 26, "select * from BW", "[{E:3,D:'B',K:4,F:'Tri'}," +
                "{E:6,D:'B',K:4,F:'Six'},{E:4,D:'B',K:4,F:'Vier'},{E:6,D:'B',K:4,F:'Sechs'}," +
                "{E:9,D:'B',K:4,F:'Nine'},{E:7,D:'B',K:4,F:'Seven'},"+
                "{E:5,D:'C',K:1,F:'Five'},{E:4,D:'C',K:1,F:'Four'},{E:8,D:'C',K:1,F:'Eight'}]");
            Act(346,"delete from BW where E=7");
            Act(347,"update bv set f='Ate' where e=8");
            CheckResults(24, 27, "select * from bv", "[{E:3,F:'Tri'},{E:6,F:'Six'}," +
                "{E:4,F:'Vier'},{E:6,F:'Sechs'},{E:9,F:'Nine'}]");
            CheckResults(24, 28, "select * from bw", "[{E:3,D:'B',K:4,F:'Tri'}," +
                "{E:6,D:'B',K:4,F:'Six'},{E:4,D:'B',K:4,F:'Vier'},{E:6,D:'B',K:4,F:'Sechs'}," +
                "{E:9,D:'B',K:4,F:'Nine'},{E:5,D:'C',K:1,F:'Five'},{E:4,D:'C',K:1,F:'Four'},"+
                "{E:8,D:'C',K:1,F:'Eight'}]");
        }
        public void Test25() 
        {
            if (test > 0 && test != 25)
                return;
            testing = 25;
            Begin();
            Act(348, "create type person as (name string) nodetype");
            Act(349, "create type student under person as (matric string)");
            Act(350, "insert into student values ('Fred','22/456')");
            Act(352, "create type staff under person as (title string)");
            Act(353, "insert into staff values ('Anne','Prof')");
            CheckResults(25,1, "select *,specifictype() from person",
                "[{NAME:'Fred',SPECIFICTYPE:'STUDENT'},{NAME:'Anne',SPECIFICTYPE:'STAFF'}]");
            Act(354, "create type married edgetype(bride=person,groom=person)");
            Act(355, "insert into person values('Joe'),('Mary')");
            Act(356, "insert into married(bride,groom) values ("+
                "(select position from person where name='Mary'),"+
                "(select position from person where name='Joe'))");
            CheckResults(25, 2, "select count(*) from married", "[{COUNT:1}]");
            CheckResults(25, 3, "match ({name:'Joe'})<-[:married]-(x)", "[{X:'PERSON(NAME=Mary)'}]");
            Act(357, "CREATE\r\n(:Product:WoodScrew {spec:'16/8x4'}),(:Product: WallPlug{spec:'18cm'}),"+
                "(Joe:Customer {Name:'Joe Edwards', Address:'10 Station Rd.'}),"+
                "(Joe)-[:Ordered {\"Date\":date'2002-11-22'} ]->(:\"Order\"{id:201})");
            Act(358, "MATCH (O:\"Order\"{id:201})"+
                "begin MATCH(X: Product{ spec: '16/8x4'}) CREATE(O)-[:Item{Qty:5}]->(X);"+
                "MATCH(X: Product{ spec: '18cm'}) CREATE(O)-[:Item{Qty:3}]->(X)end");
            CheckResults(25, 4, "match ()-[{Qty:QT}]->(:ST{spec:SA}) where QT>4",
                "[{ST:'WOODSCREW',QT:5,SA:'16/8x4'}]");
            Act(359, "CREATE (p1:Person {name:'Fred Smith'})<-[:Child]-(p2:Person {name:'Pete Smith'})," +
                "(p2)-[:Child]->(:Person {name:'Mary Smith'})");
            CheckResults(25, 5, "MATCH (n)-[:Child]->(c) RETURN n.name,c.name AS child",
                "[{NAME:'Pete Smith',CHILD:'Fred Smith'},{NAME:'Pete Smith',CHILD:'Mary Smith'}]");
            Act(360, "MATCH (n {name:'Pete Smith'}) SET n.name='Peter Smith' ");
            CheckResults(25, 6, "MATCH ({name:'Peter Smith'})[()-[:Child]->()]+(x) RETURN x.name",
                "[{NAME:'Fred Smith'},{NAME:'Mary Smith'}]");
            Act(361, "CREATE (e1:Person {name:'Emil', born:1975 }), (k1:Person {name:'Karin', born:1977 }),"
                +"(e1)<-[:married {since:2000}]-(k1)");
            Act(362, "CREATE (Sue:Person {name:'Sue Hill', born:1975}),"
                +"(Joe:Person {name:'Joe Hill', born:1952}),(Joe)<-[:married {since:1995}]-(Sue)");
            CheckResults(25, 7, "MATCH (n:Person) RETURN collect(n.born)",
                "[{COLLECT:'MULTISET[1952,1975,1975,1977]'}]");
            CheckResults(25, 8, "MATCH (n:Person) RETURN avg(n.born)","[{AVG:1969.75}]");
            Act(363, "MATCH (nm:Person {name:'Joe Hill'})<-[mr { since:1995}]-() "
                +"BEGIN nm.born = 1962; mr.since = 1996 END");
            CheckResults(25, 9, "MATCH (n:Person)<-[:married{since:dt}]-() RETURN n.name,dt", 
                "[{NAME:'Emil',DT:2000},{NAME:'Joe Hill',DT:1996}]");
            Act(364, "MATCH (ee:Person {name:'Emil'}) CREATE (d:Dog {name:'Rex'}),"
                +"(ee)-[:owns]->(d),(d)-[:owned_by]->(ee)");
            Act(365, "MATCH (dg:Dog {name: 'Rex'})-[ro:owned_by]->() DELETE ro");
            Act(366, "MATCH (ka{name:'Karin'}),()-[ow:owns ]->({name:'Rex'}) SET ow.leaving=ka");
            CheckResults(25, 10, "select count(*) from owned_by", "[{COUNT:0}]");
            CheckResults(25, 11, "MATCH (n)-[:owns]->(dg) RETURN n.name,dg.name as dog", 
                "[{NAME:'Karin',DOG:'Rex'}]");
            Rollback();
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
