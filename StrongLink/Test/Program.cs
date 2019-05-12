using System;
using StrongLink;
using Shareable;
using System.Threading;
using System.Threading.Tasks;
namespace Test
{
    class Program
    {
        static int test = 0, qry = 0;
        bool commit = false;
        StrongConnect conn;
        Program(string[] args)
        {

            conn = new StrongConnect("127.0.0.1", 50433, "testdb");
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
                Console.WriteLine("13 March 2019 Respeatable tests");
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
            Test11(test);
            Test12(test);
            Test13(test);
        }
        void Test1(int t)
        {
            if (t > 0 && t != 1)
                return;
            Begin();
            conn.ExecuteNonQuery("create table A(B integer,C integer,D string) primary key(B,C)");
            conn.ExecuteNonQuery("insert A values(2,3,'TwentyThree')");
            conn.ExecuteNonQuery("insert A values(1,9,'Nineteen')");
            CheckResults(1,1,"select from A", "[{B:1,C:9,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            conn.ExecuteNonQuery("update A where C=9 set C=19");
            CheckResults(1,2, "select from A", "[{B:1,C:19,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
            conn.ExecuteNonQuery("delete A where C=19");
            CheckResults(1, 3, "select from A", "[{B:2,C:3,D:'TwentyThree'}]");
            Rollback();
            if (!commit)
            {
                Begin();
                conn.CreateTable("A");
                conn.CreateColumn("A", Types.SInteger, "B");
                conn.CreateColumn("A", Types.SInteger, "C");
                conn.CreateColumn("A", Types.SString, "D");
                conn.CreateIndex("A", IndexType.Primary, null, "B", "C");
                conn.Insert("A", new string[0], new Serialisable[] { new SInteger(2), new SInteger(3), new SString("TwentyThree") },
                    new Serialisable[] { new SInteger(1), new SInteger(9), new SString("Nineteen") });
                CheckResults(1,4, "select from A", "[{B:1,C:9,D:'Nineteen'},{B:2,C:3,D:'TwentyThree'}]");
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
                conn.CreateTable("AA");
                conn.CreateColumn("AA", Types.SInteger, "B");
                conn.CreateColumn("AA", Types.SString, "C");
                conn.Insert("AA", new string[] { "B" },new Serialisable[] { new SInteger(17) });
                conn.Insert("AA", new string[] { "C" },new Serialisable[] { new SString("BC") });
                conn.Insert("AA", new string[] { "C", "B" },
                    new Serialisable[] { new SString("GH"), new SInteger(67) });
                CheckResults(2,7,"select from AA","[{B:17},{C:'BC'},{B:67,C:'GH'}]");
                Rollback();
            }
        }
        void Test3(int t)
        {
            if (t > 0 && t != 3)
                return;
            Begin();
            conn.ExecuteNonQuery("create table b(c integer primary key,d string)");
            conn.ExecuteNonQuery("insert b values(45,'DE')");
            conn.ExecuteNonQuery("insert b values(-23,'HC')");
            CheckResults(3, 1,"select from b", "[{c:-23,d:'HC'},{c:45,d:'DE'}]");
            CheckResults(3, 2,"select from b where c=-23", "[{c:-23,d:'HC'}]");
            Rollback();
            if (!commit)
            {
                Begin();
                conn.CreateTable("b");
                conn.CreateColumn("b", Types.SInteger, "c");
                conn.CreateColumn("b", Types.SString, "d");
                conn.CreateIndex("b", IndexType.Primary, null,"c");
                conn.Insert("b", new string[0], new Serialisable[] { new SInteger(45), new SString("DE") },
                    new Serialisable[] { new SInteger(-23), new SString("HC") });
                CheckResults(3,3,"select from b", "[{c:-23,d:'HC'},{c:45,d:'DE'}]");
                Rollback();
            }
        }
        void Test4(int t)
        {
            if (t > 0 && t != 4)
                return;
            Begin();
            conn.ExecuteNonQuery("create table e(f integer,g string) primary key(g,f)");
            conn.ExecuteNonQuery("insert e values(23,'XC')");
            conn.ExecuteNonQuery("insert e values(45,'DE')");
            CheckResults(4, 1,"select from e", "[{f:45,g:'DE'},{f:23,g:'XC'}]");
            conn.ExecuteNonQuery("insert e(g) values('DE')");
            CheckResults(4, 2, "select from e",
                "[{f:45,g:'DE'},{f:46,g:'DE'},{f:23,g:'XC'}]");
            // the EvalRowSet loop in the next test should execute only once
            CheckResults(4, 3, "select count(f) from e where g='DE' and f<=45",
                "[{col1:1}]");
            Rollback();
            if (!commit)
            {
                Begin();
                conn.CreateTable("e");
                conn.CreateColumn("e", Types.SInteger, "f");
                conn.CreateColumn("e", Types.SString, "g"); 
                conn.CreateIndex("e", IndexType.Primary, null, "g", "f");
                conn.Insert("e", new string[0], new Serialisable[] { new SInteger(23), new SString("XC") },
                    new Serialisable[] { new SInteger(45), new SString("DE") });
                CheckResults(4,4,"select from e", "[{f:45,g:'DE'},{f:23,g:'XC'}]");
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
            CheckResults(5, 3,"select (a.b) as f,(c) from a", "[{f:17,col2:15},{f:23,col2:6}]");
            CheckResults(5, 4,"select b+3,d.c from a d", "[{col1:20,c:15},{col1:26,c:6}]");
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
                conn.CreateTable("a");
                conn.CreateColumn("a", Types.SInteger, "b");
                conn.CreateColumn("a", Types.SInteger, "c"); 
                conn.Insert("a", new string[0], new Serialisable[] { new SInteger(17), new SInteger(15) },
                    new Serialisable[] { new SInteger(23), new SInteger(6) });
                CheckResults(5,12,"select from a","[{b:17,c:15},{b:23,c:6}]");
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
                conn.CreateTable("ta");
                conn.CreateColumn("ta", Types.SDate, "b");
                conn.CreateColumn("ta", Types.STimeSpan, "c");
                conn.CreateColumn("ta", Types.SBoolean, "d"); 
                conn.Insert("ta", new string[0], new Serialisable[] { new SDate(new DateTime(2019,01,06,12,30,0)),
            new STimeSpan(new TimeSpan(2,0,0)),SBoolean.False});
                CheckResults(6,2,"select from ta", "[{b:\"2019-01-06T12:30:00\",c:\"02:00:00\",d:\"false\"}]");
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
            conn.ExecuteNonQuery("create table JA(B integer,C integer,D integer)");
            conn.ExecuteNonQuery("insert JA values(4,2,43)");
            conn.ExecuteNonQuery("insert JA values(8,3,82)");
            conn.ExecuteNonQuery("insert JA values(7,4,29)");
            conn.ExecuteNonQuery("create table JE(F integer,C integer,G integer)");
            conn.ExecuteNonQuery("insert JE values(4,3,22)");
            conn.ExecuteNonQuery("insert JE values(11,4,10)");
            conn.ExecuteNonQuery("insert JE values(7,2,31)");
            CheckResults(8, 1, "select from JA natural join JE" ,
                "[{B:4,C:2,D:43,F:7,G:31},{B:8,C:3,D:82,F:4,G:22},{B:7,C:4,D:29,F:11,G:10}]");
            CheckResults(8, 2, "select D,G from JA cross join JE where D<G",
                "[{D:29,G:31}]");
            CheckResults(8, 3, "select B,D,G from JA, JE where B=F",
                "[{B:4,D:43,G:22},{B:7,D:29,G:31}]");
            CheckResults(8, 4, "select B,D,G from JA H, JE where H.C=JE.C",
                "[{B:4,D:43,G:31},{B:8,D:82,G:22},{B:7,D:29,G:10}]");
            CheckResults(8, 5, "select from JA inner join JE on B=F",
                "[{B:4,\"JA.C\":2,D:43,F:4,\"JE.C\":3,G:22},"+
                "{B:7,\"JA.C\":4,D:29,F:7,\"JE.C\":2,G:31}]");
            CheckResults(8, 6, "select from JA left join JE on B=F",
    "[{B:4,\"JA.C\":2,D:43,F:4,\"JE.C\":3,G:22},{B:7,\"JA.C\":4,D:29,F:7,\"JE.C\":2,G:31}," +
    "{B:8,\"JA.C\":3,D:82}]");
            CheckResults(8, 7, "select from JA right join JE on B=F",
    "[{B:4,\"JA.C\":2,D:43,F:4,\"JE.C\":3,G:22},{B:7,\"JA.C\":4,D:29,F:7,\"JE.C\":2,G:31}," +
    "{F:11,\"JE.C\":4,G:10}]");
            CheckResults(8, 8, "select from JA full join JE on B=F",
    "[{B:4,\"JA.C\":2,D:43,F:4,\"JE.C\":3,G:22},{B:7,\"JA.C\":4,D:29,F:7,\"JE.C\":2,G:31}," +
    "{B:8,\"JA.C\":3,D:82},{F: 11,\"JE.C\":4,G:10}]");
            Rollback();
        }
        void Test9(int t)
        {
            if (t > 0 && t != 9)
                return;
            Begin();
            conn.ExecuteNonQuery("create table ba(b integer,c numeric)");
            conn.ExecuteNonQuery("insert ba values(12345678901234567890123456789,123.4567)");
            conn.ExecuteNonQuery("insert ba values(0,123.4567e-15)");
            conn.ExecuteNonQuery("insert ba values(12,1234)");
            conn.ExecuteNonQuery("insert ba values(34,0.5678e9)");
            CheckResults(9, 1, "select from ba", "[{\"b\": 12345678901234567890123456789, \"c\": 123.4567},{\"b\": 0, \"c\": 1.234567E-13},{\"b\": 12, \"c\": 1234},{\"b\": 34, \"c\": 567800000}]");
            Rollback();
            Begin();
            conn.CreateTable("Ba");
            conn.CreateColumn("Ba", Types.SInteger, "b");
            conn.CreateColumn("Ba", Types.SNumeric, "c"); 
            conn.Insert("Ba", new string[0], new Serialisable[] {
                new SInteger(Integer.Parse("12345678901234567890123456789")),
                new SNumeric(Numeric.Parse("123.4567"))});
            conn.Insert("Ba", new string[0], new Serialisable[] {
                new SInteger(0),new SNumeric(Numeric.Parse("123.4567e-15"))});
            conn.Insert("Ba", new string[0], new Serialisable[] {
                new SInteger(12),new SNumeric(1234,4,0)});
            conn.Insert("Ba", new string[0], new Serialisable[] {
                new SInteger(34),new SNumeric(new Numeric(0.5678e9))});
            CheckResults(9, 2, "select from Ba", "[{\"b\": 12345678901234567890123456789, \"c\": 123.4567},{\"b\": 0, \"c\": 1.234567E-13},{\"b\": 12, \"c\": 1234},{\"b\": 34, \"c\": 567800000}]");
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
                conn.ExecuteNonQuery("create table RDC(A integer primary key,B string)");
            }
            catch (Exception) {
            }
            conn.ExecuteNonQuery("delete RDC");
            conn.ExecuteNonQuery("insert RDC values(42,'Life, the Universe')");
            conn.ExecuteNonQuery("insert RDC values(52,'Weeks in the year')");
            Begin();
            conn.ExecuteQuery("select from RDC where A=42");
            var task1 = Task.Factory.StartNew(() => Test10A());
            task1.Wait();
            CheckExceptionNonQuery(10, 1, "Commit", "Transaction conflict");
            Begin();
            conn.ExecuteQuery("select from RDC where A=52");
            var task2 = Task.Factory.StartNew(() => Test10B());
            task2.Wait();
            conn.Commit();
            Begin();
            CheckResults(10,2,"select from RDC","[{A:42,B:'the product of 6 and 7'},{A:52,B:'Weeks in the year'}]");
            task1 = Task.Factory.StartNew(() => Test10A());
            task1.Wait();
            CheckExceptionNonQuery(10, 3, "Commit", "Transaction conflict");
        }
        void Test10A()
        {
            var p = new Program(new string[0]);
            p.conn.ExecuteNonQuery("update RDC where A=42 set B='the product of 6 and 9'");
            p.conn.Close();
        }
        void Test10B()
        {
            var p = new Program(new string[0]);
            p.conn.ExecuteNonQuery("update RDC where A=42 set B='the product of 6 and 7'");
            p.conn.Close();
        }
        void Test11(int t)
        {
            if (commit || (t > 0 && t != 11)) // this test only runs if commit is false
                return;
            if (qry == 0 || qry == 1)
            {
                Begin();
                conn.ExecuteNonQuery("create table cs(b integer notnull,c integer default 4,d integer generated b+c)");
                CheckExceptionNonQuery(11, 1, "insert cs(c) values(5)", "Value of b cannot be null");
            }
            if (qry == 0 || qry == 2)
            {
                Begin();
                conn.ExecuteNonQuery("create table cs(b integer notnull,c integer default 4,d integer generated b+c)");
                conn.ExecuteNonQuery("insert cs(b) values(3)");
                CheckExceptionNonQuery(11, 2, "insert cs values(1,2,3)", "Illegal value for generated column");
            }
            if (qry == 0 || qry == 3)
            {
                Begin();
                conn.ExecuteNonQuery("create table cs(b integer notnull,c integer default 4,d integer generated b+c)");
                conn.ExecuteNonQuery("insert cs(b) values(3)");
                CheckResults(11, 3, "select from cs", "[{\"b\":3,\"c\":4,\"d\":7}]");
            }
            Rollback();
            Begin();
            conn.CreateTable("cs");
            conn.CreateColumn("cs", Types.SInteger, "b",
                 ("NOTNULL", new SFunction(SFunction.Func.NotNull, SArg.Value)));
            conn.CreateColumn("cs", Types.SInteger, "c",
                ("DEFAULT", new SFunction(SFunction.Func.Default, new SInteger(4))));
            var b = conn.Prepare("b");
            var c = conn.Prepare("c");
            conn.CreateColumn("cs", Types.SInteger, "d",
                    ("GENERATED", new SFunction(SFunction.Func.Generated,
                        new SExpression(new SDbObject(Types.SName,b), SExpression.Op.Plus, new SDbObject(Types.SName,c)))));
            Rollback();
        }
        void Test12(int t)
        {
            if (t > 0 && t != 12)
                return;
            Begin();
            conn.ExecuteNonQuery("create table sce(a integer,b string)");
            conn.ExecuteNonQuery("insert sce values(12,'Zodiac')");
            conn.ExecuteNonQuery("insert sce values(13,'Bakers')");
            conn.ExecuteNonQuery("insert sce values(14,'Fortnight')");
            conn.ExecuteNonQuery("create table dst(c integer)");
            conn.ExecuteNonQuery("insert dst select a from sce where b<'H'");
            CheckResults(12, 1, "select from dst", "[{c:13},{c:14}]");
            CheckResults(12, 2, "select a from sce where b in('Fortnight','Zodiac')",
                "[{a:12},{a:14}]");
            CheckResults(12, 3, "select from dst where c in (select a from sce where b='Bakers')",
                "[{c:13}]");
            conn.ExecuteNonQuery("insert dst(c) select max(x.a)+4 from sce x where x.b<'H'");
            CheckResults(12, 4, "select from dst", "[{c:13},{c:14},{c:18}]");
            conn.ExecuteNonQuery("insert dst select min(x.c)-3 from dst x");
            CheckResults(12, 5, "select from dst", "[{c:13},{c:14},{c:18},{c:10}]");
            Rollback();
        }
        void Test13(int t)
        {
            if (t > 0 && t != 13)
                return;
            Begin();
            conn.ExecuteNonQuery("create table ad(a integer,b string)");
            conn.ExecuteNonQuery("insert ad values(20,'Twenty')");
            if (qry == 0 || qry == 1)
            {
                CheckExceptionNonQuery(13, 1, "alter ad add c string notnull", "Table is not empty");
                if (!commit)
                {
                    Begin();
                    conn.ExecuteNonQuery("create table ad(a integer,b string)");
                    conn.ExecuteNonQuery("insert ad values(20,'Twenty')");
                }
            }
            conn.ExecuteNonQuery("alter ad add c string default 'XX'");
            CheckResults(13, 2, "select from ad", "[{a:20,b:'Twenty',c:'XX'}]");
            conn.ExecuteNonQuery("alter ad drop b");
            CheckResults(13,3,"select from ad", "[{a:20,c:'XX'}]");
            conn.ExecuteNonQuery("alter ad add primary key(a)");
            conn.ExecuteNonQuery("insert ad values(21,'AB')");
            conn.ExecuteNonQuery("create table de (d integer references ad)");
            if (qry == 0 || qry == 4)
            {
                CheckExceptionNonQuery(13, 4, "insert de values(14)", "Referential constraint violation");
                if (!commit)
                {
                    Begin();
                    conn.ExecuteNonQuery("create table ad(a integer,b string)");
                    conn.ExecuteNonQuery("insert ad values(20,'Twenty')");
                    conn.ExecuteNonQuery("alter ad add c string default 'XX'");
                    conn.ExecuteNonQuery("alter ad drop b");
                    conn.ExecuteNonQuery("alter ad add primary key(a)");
                    conn.ExecuteNonQuery("insert ad values(21,'AB')");
                    conn.ExecuteNonQuery("create table de (d integer references ad)");
                }
            }
            conn.ExecuteNonQuery("insert de values(21)");
            if (qry == 0 || qry == 5)
            {
                CheckExceptionNonQuery(13, 5, "delete ad where c='AB'", "Referential constraint: illegal delete");
                if (!commit)
                {
                    Begin();
                    conn.ExecuteNonQuery("create table ad(a integer,b string)");
                    conn.ExecuteNonQuery("insert ad values(20,'Twenty')");
                    conn.ExecuteNonQuery("alter ad add c string default 'XX'");
                    conn.ExecuteNonQuery("alter ad drop b");
                    conn.ExecuteNonQuery("alter ad add primary key(a)");
                    conn.ExecuteNonQuery("insert ad values(21,'AB')");
                    conn.ExecuteNonQuery("create table de (d integer references ad)");
                    conn.ExecuteNonQuery("insert de values(21)");
                }
            }
            if (qry == 0 || qry == 6)
            {
                CheckExceptionNonQuery(13, 6, "drop ad", "Restricted by reference");
                if (!commit)
                {
                    Begin();
                    conn.ExecuteNonQuery("create table ad(a integer,b string)");
                    conn.ExecuteNonQuery("insert ad values(20,'Twenty')");
                    conn.ExecuteNonQuery("alter ad add c string default 'XX'");
                    conn.ExecuteNonQuery("alter ad drop b");
                    conn.ExecuteNonQuery("alter ad add primary key(a)");
                    conn.ExecuteNonQuery("insert ad values(21,'AB')");
                    conn.ExecuteNonQuery("create table de (d integer references ad)");
                    conn.ExecuteNonQuery("insert de values(21)");
                }
            }
            conn.ExecuteNonQuery("alter ad column c drop default");
            CheckResults(13,7,"select from ad", "[{a:20},{a:21,c:'AB'}]");
            if (qry == 0 || qry == 8)
            {
                CheckExceptionNonQuery(13, 8, "alter ad drop key(a)", "Restricted by reference");
                if (!commit)
                {
                    Begin();
                    conn.ExecuteNonQuery("create table ad(a integer,b string)");
                    conn.ExecuteNonQuery("insert ad values(20,'Twenty')");
                    conn.ExecuteNonQuery("alter ad add c string default 'XX'");
                    conn.ExecuteNonQuery("alter ad drop b");
                    conn.ExecuteNonQuery("alter ad add primary key(a)");
                    conn.ExecuteNonQuery("insert ad values(21,'AB')");
                    conn.ExecuteNonQuery("create table de (d integer references ad)");
                    conn.ExecuteNonQuery("insert de values(21)");
                    conn.ExecuteNonQuery("alter ad column c drop default");
                }
            }
            conn.ExecuteNonQuery("drop de");
            conn.ExecuteNonQuery("alter ad drop key(a)");
            // we don't get 'XX' here because the DEFAULT was not used inm an INSERT ??
            CheckResults(13,9, "select from ad", "[{a:20},{a:21,c:'AB'}]");
            conn.ExecuteNonQuery("insert ad(a) values(13)");
            CheckResults(13, 10, "select from ad", "[{a:20},{a:21,c:'AB'},{a:13}]");
            conn.ExecuteNonQuery("drop ad");
            if (qry == 0 || qry == 11)
            {
                CheckExceptionQuery(13, 11, "select from ad", "No table ad");
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
                conn.ExecuteQuery(c);
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
                conn.ExecuteNonQuery(c);
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
                Check(conn.ExecuteQuery(c), new DocArray(d));
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
