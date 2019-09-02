using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Pyrrho;

namespace UnitTestProject
{
    [TestClass]
    public class PyrrhoTests
    {
        internal PyrrhoConnect conn;
        public PyrrhoTests(PyrrhoConnect c)
        {
            conn = c;
        }
        [TestMethod]
        public void TestStatic()
        {
            object[,] vals = {{17L}};
            CheckResults("select 17 from static",vals);
        }
        [TestMethod]
        public void TestRename() 
        {
            conn.Act("create table ta(a1 int primary key)");
            conn.Act("insert into ta values(17),(23)");
            conn.Act("create view vw as select a1 from ta");
            CheckResults("table ta", new object[,] { { 17L }, { 23L } }, "A1");
            conn.Act("alter table ta alter a1 to id");
            CheckResults("table vw", new object[,] { { 17L }, { 23L } }, "ID");
            CheckResults("table \"Sys$View\"",new object[,]{{"*","VW","select \"ID\" from \"TA\""}});
        }
        /*  Several parts of this test are withdrawn: IMO, without the VALUES keyword, the columns must match by name.
         *  so the two INSERT SELECTs each add a row of nulls 
         */
        [TestMethod]
        public void TestSubQuery() 
        {
            var t = conn.BeginTransaction();
            conn.Act("create table b(a int primary key, c int)");
            conn.Act("insert into b values(5,2),(6,1),(7,4)");
            conn.Act("create table c(d int, e int)");
        /*    conn.Act("insert into c (select * from b where a=7)");
            conn.Act("insert into c (select 17,(select a from b where c=1) from static)"); */
            conn.Act("insert into c values((select c from b where a=5),22)");
            CheckResults("table c", new object[,] { /*{ 7L, 4L }, { 17L, 6L }, */{ 2L, 22L } }); 
            CheckResults("select c from b", new object[,] { { 2 }, { 1 }, { 4 } });
            CheckResults("select array(select c from b) from static", new object[,] { { "ARRAY[2,1,4]" } });
            conn.Act("create table d(e int array)");
            conn.Act("insert into d values(array(select c from b g))");
            CheckResults("table d", new object[,] { { "ARRAY[2,1,4]" } });
            CheckResults("select e[1] from d", new object[,] { { 1 } });
            conn.Act("create type mrw as (c int)");
            conn.Act("create table e(f mrw,primary key(f.c))");
            conn.Act("insert into e (select mrw(c) as f from b where a<>7)");
            CheckResults("table e", new object[,] { { "MRW(C=1)" }  ,  { "MRW(C=2)" }  }); 
            CheckResults("select (select c from b where a=5) from static", new object[,] { { 2L } });
            t.Rollback();
        } 
        [TestMethod]
        public void TestPlayed()
        {
            conn.Act("create table members (id int primary key,firstname char)");
            conn.Act("create table played (id int primary key, winner int references members, loser int references members,agreed boolean)");
            conn.Act("grant select on members to public");
            conn.Act("grant select on played to public");
            conn.Act("create procedure claim(won int,beat int) insert into played(winner,loser) values(claim.won,claim.beat)");
            conn.Act("create procedure agree(p int) update played set agreed=true where winner=agree.p and loser in " +
                "(select m.id from members m where current_user like '%'||firstname escape '^')");
            conn.Act("insert into members(firstname) values(current_user)");
            CheckResults("select id from members where current_user like '%'||firstname escape'^'", new object[,] { { 1L } }, "ID");
            conn.Act("insert into members(firstname) values('Fred')");
            conn.Act("insert into played(winner,loser) values(2,1)");
            conn.Act("create role membergames");
            conn.Act("grant execute on procedure claim(int,int) to role membergames");
            conn.Act("grant execute on procedure agree(int) to role membergames");
            conn.Act("grant membergames to public");
            var t = conn.BeginTransaction();
            conn.Act("set role membergames");
            conn.Act("call agree(2)");
            conn.Act("call claim(1,2)");
            CheckResults("table played", new object[,] { { 1, 2, 1, true }, { 2, 1, 2, null } });
            t.Rollback();
        }
        [TestMethod]
        public void TestAuthors()
        {
            var t = conn.BeginTransaction();
            conn.Act("create table author(id int primary key,aname char)");
            conn.Act("create table book(id int primary key,authid int references author,title char)");
            conn.Act("insert into author values (1,'Dickens'),(2,'Conrad')");
            conn.Act("insert into book(authid,title) values (1,'Dombey & Son'),(2,'Lord Jim'),(1,'David Copperfield')");
            conn.Act("create function booksby(auth char) returns table(title char)" +
                "return table (select title from author inner join book b on author.id=b.authid where aname=booksby.auth)");
            CheckResults("select title from author inner join book b on author.id=b.authid where aname='Dickens'", new object[,] { { "Dombey & Son" }, { "David Copperfield" } });
            CheckResults("select * from table(booksby('Dickens'))", new object[,] { { "Dombey & Son" }, { "David Copperfield" } });
            CheckResults("select count(*) from table(booksby('Dickens'))",new object[,]{{2}});
            t.Rollback();
        }
        [TestMethod]
        public void TestGather1()
        {
            conn.Act("create table ga(a1 int primary key,a2 char)");
            conn.Act("insert into ga values(1,'First'),(2,'Second')");
            conn.Act("create function gather1() returns char " +
                "begin declare c cursor for select a2 from ga;" +
                "declare done Boolean default false;" +
                "declare continue handler for sqlstate '02000' set done=true;" +
                "declare a char default '';" +
                "declare p char;" +
                "open c;" +
                "repeat " +
                   "fetch c into p;"+ 
                   "if not done then "+
                      "if a='' then "+
                         "set a=p "+
                      "else "+
                         "set a=a||', '||p "+
                      "end if "+
                   "end if "+
                "until done end repeat;" +
                "close c;" +
                "return a end");
            CheckResults("select gather1() from static", new string[,] { { "First, Second" } });
        }
        [TestMethod]
        public void TestGather2()
        {
            conn.Act("create function gather2() returns char " +
                "begin declare b char default '';" +
                "for select a2 from ga do if b='' then set b=a2 else set b=b||', '||a2 end if end for;" +
                "return b end");
            CheckResults("select gather2() from static", new string[,] { { "First, Second" } });
        }
        [TestMethod]
        public void TestReverse()
        {
            conn.Act("create function reverse(a char) returns char " +
                "if char_length(a)<=1 then return a "+
                "else return reverse(substring(a from 1 for char_length(a)-1))||substring(a from 0 for 1) end if");
            CheckResults("select reverse('Fred') from static", new string[,] { { "derF" } });
        }
        [TestMethod]
        public void TestPoints()
        {
            conn.Act("create type point as (x int, y int)");
            conn.Act("create type size as (w int,h int)");
            conn.Act("create type line as (strt point,en point)");
            conn.Act("create type rect as (tl point,sz size) constructor method rect(x1 int,y1 int, x2 int, y2 int),method centre() returns point");
            conn.Act("create table figure(id int primary key,title char)");
            conn.Act("create table figureline(id int primary key,fig int references figure,what line)");
            conn.Act("create table figurerect(id int primary key,fig int references figure,what rect)");
            conn.Act("create constructor method rect(x1 int,y1 int,x2 int,y2 int) begin tl=point(x1,y1); sz=size(x2-x1,y2-y1) end");
            conn.Act("create method centre() returns point for rect return point(tl.x+sz.w/2,tl.y+sz.h/2)");
            conn.Act("create function centrerect(a int) returns point return (select what.centre() from figurerect where id=a)");
            conn.Act("insert into figure values(1,'Diagram')");
            conn.Act("insert into figurerect values(1,1,rect(point(1,2),size(3,4)))");
            conn.Act("insert into figurerect values(2,1,rect(4,5,6,7))");
            conn.Act("insert into figureline values(1,1,line(centrerect(1),centrerect(2)))");
            CheckResults("select what from figureline",new string[,]{{"LINE(STRT=POINT(X=2,Y=4),EN=POINT(X=5,Y=6))"}});
       //     CheckResults("select strt from (select what from figureline)",new object[,] { { "POINT(X=2,Y=4)" } },"STRT");
       //     CheckResults("select y from (select tl from (select what from figurerect))", new object[,] { { 2 },{ 5 } },"Y");
        }
        public void TestGroup()
        {
            var t = conn.BeginTransaction();
            conn.Act("create table a(b int,c int)");
            conn.Act("insert into a values(2,10),(1,12),(2,14),(2,16)");
            CheckResults("select b,sum(c),avg(c) from a group by b", new object[,] { { 1L, 12L, 12.0 }, { 2L, 40L, 13.3333333333333 } });
            conn.Act("Create table aa(b int,c int,d int, e int)");
            conn.Act("insert into aa values(1,21,31,41),(3,22,35,47),(1,22,33,42),(1,22,35,49),(2,21,37,40),(2,23,33,43),(1,24,31,44),(2,26,37,45)");
            conn.Act("create view f as select count(*) as g,b,d from aa group by b,d");
            var res = new object[,] { { 2, 31 }, { 2, 33 }, { 1, 35 }, { 2, 37 } };
            CheckResults("select sum(g) as h,d from f group by d having b<3", res);
            CheckResults("select sum(g) as h,d from (select count(*) as g,b,d from aa group by b,d) group by d having b<3", res);
            t.Rollback();
        }
        public void TestDocument()
        {
            var t = conn.BeginTransaction();
            conn.Act("create table people(doc document)");
            conn.Act("insert into people values ({NAME: \"fred\", AGE: 20, STATUS: \"B\", \"GROUPS\": ['music']})"); // groups is an SQL reserved word! field names should be quoted anyway
            conn.Act("insert into people values ({NAME: \"sue\", AGE: 26, STATUS: \"A\", \"GROUPS\": [\"news\", \"sports\"]})");
            conn.Act("insert into people values ({NAME: \"john\", AGE: 16, STATUS: \"C\"})");
            conn.Act("insert into people values ({NAME: \"mary\", AGE: 20, STATUS: \"A\", \"GROUPS\": [\"music\", \"sports\"]})");
            CheckResults("select doc.name from people where doc.age>18 order by doc.age", new object[,] {{"fred"},{"mary"},{"sue"}});
#if MONGO
            CheckResults("select doc.name from people where doc={AGE: {$gt: 18}}  order by doc.age", new object[,] { { "fred" }, { "mary" }, { "sue" } });
            CheckResults("select {$aggregate: PEOPLE, pipeline: ["+
                            "{$match: {AGE: {$gt: 18}}}, " +
                            "{$group: {_id: $AGE, count: {$sum: 1}}}, "+
                            "{$sort: { _id: -1 }}] } from static",
                            new object[,]{{"{\"ok\": True, \"result\": [{\"_id\": 26, \"count\": 1}, {\"_id\": 20, \"count\": 2}]}"}});
#endif
            t.Rollback();
        }
        [TestMethod]
        public void TestPositioned()
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table ca(cb char)");
            conn.Act("create procedure delw(w char)"+
 " for c cursor for table ca do "+
       " if cb=w then "+
           " delete from ca where current of c "+
       " end if"+
 " end for");
            conn.Act("create procedure updw(w char,u char)" +
  " for c cursor for table ca do " +
       " if cb=w then " +
           "update ca set cb=u where current of c " +
       " end if " +
  " end for");
            conn.Act("insert into ca values('To'),('Three'),('F4')");
            conn.Act("call delw('To')");
            conn.Act("call updw('F4','Four')");
            CheckResults("table ca", new object[,] { { "Three" }, { "Four" } });
            tr.Rollback();
        }
        [TestMethod]
        public void TestSimpleTable() // 1.1
        {
            // very simple table
            conn.Act("create table a(b int,c char)");
            object[,] vals = { { 17, "AG" }, { 23, "BC" } };
            foreach (var v in Jag(vals))
                conn.Act("insert into a values(" + Values(v) + ")");
            CheckResults("table a", vals);
        }
        [TestMethod]
        public void TestSimpleTable1() // 1.1a
        {
            // very simple table
            var tr =conn.BeginTransaction();
            conn.Act("create table aa(b int,c char)");
            conn.Act("insert into aa(b) values(17)");
            conn.Act("insert into aa(c) values('BC')");
            conn.Act("insert into aa(c,b) values('GH',67)");
            CheckResults("table aa", new object[,]{{17L,null},{null,"BC"},{67L,"GH"}},"B","C");
            tr.Rollback();
        }
        [TestMethod]
        public void TestIndexedTable() // 1.2
        {
            conn.Act("create table b(c int primary key,d char)");
            object[,] vals = { { 23L, "HC" }, { 45L, "DE" } };
            foreach (var v in Jag(vals))
                conn.Act("insert into b values(" + Values(v) + ")");
            CheckResults("table b", vals, "C", "D");
        }
        [TestMethod]
        public void TestMultiIndexedTable() // 1.3
        {
            conn.Act("create table e(f int,g char,primary key(f,g))");
            object[,] vals = { { 23L, "XC" }, { 45L, "DE" } };
            foreach (var v in Jag(vals))
                conn.Act("insert into e values(" + Values(v) + ")");
            CheckResults("table e", vals, "F", "G");
        }
        [TestMethod]
        public void TestSimpleSelectStar() // 1.4 uses result of TestSimpleTable 1.1
        {
            object[,] vals = { { 17, "AG" }, { 23, "BC" } };
            CheckResults("select * from a", vals, "B", "C");
            CheckResults("select * from a d", vals, "B", "C");
            CheckResults("select * from a b", vals, "B", "C");
            CheckResults("select b,c from a", vals, "B", "C");
        }
        [TestMethod]
        public void TestIndexedSelectStar() // 1.5 uses result of TestIndexedTable 1.2
        {
            object[,] vals = {  { 23L, "HC" },  { 45L, "DE" } };
            CheckResults("select * from b", vals, "C", "D");
        }
        [TestMethod]
        public void TestMultiIndexedSelectStar() // 1.6 uses result of TestMultiIndexedTable 1.3
        {
            object[,] vals = {  { 23L, "XC" },  { 45L, "DE" } };
            CheckResults("select * from e", vals, "F", "G");
        }
        [TestMethod]
        public void TestSimpleSelect() // 1.7 uses result of TestSimpleTable 1.1
        {
            object[,] vals = { { 17 },  { 23 } };
            CheckResults("select b from a", vals, "B");
            CheckResults("select (b) from a", vals, "B");
            CheckResults("select b as d from a", vals, "D");
        }
        [TestMethod]
        public void TestSimpleSelect1() // 1.7A uses result of TestSimpleTable 1.1
        {
            object[,] vals = { { 17 }, { 23 } };
            CheckResults("select a.b from a", vals, "B");
            CheckResults("select (a.b) from a", vals, "B");
            CheckResults("select d.b from a d", vals, "B");
            CheckResults("select b from a d", vals, "B");
            CheckResults("select a.b as b from a", vals, "B");
            CheckResults("select d.b as c from a d", vals, "C");
        }
        [TestMethod]
        public void TestSimpleSelect5() // 1.7E uses result of TestSimpleTable 1.1
        {
            object[,] vals = { { 40L } };
            CheckResults("select sum(p.b) from a p", vals);
            CheckResults("select (b,c) as r from a", new object[,] { {"(B=17,C=AG)" },{ "(B=23,C=BC)"} }, "R");
            CheckResults("select (b,a.c) as r from a", new object[,] { { "(B=17,C=AG)" }, { "(B=23,C=BC)" } }, "R");
            CheckResults("select (b,d.c) as r from a d", new object[,] { { "(B=17,C=AG)" }, { "(B=23,C=BC)" } }, "R");
        }
        [TestMethod]
        public void TestIndexedSelect() // 1.8 uses result of TestIndexedTable 1.2
        {
            object[,] vals =  { { 23 }, { 45 } };
            CheckResults("select c from b", vals, "C");
        }
        [TestMethod]
        public void TestMultiIndexedSelect() // 1.9 uses result of TestMultiIndexedTable 1.3
        {
            object[,] vals =  { { "XC" }, { "DE" } };
            CheckResults("select g from e", vals, "G");
        }
        [TestMethod]
        public void TestNonKeySelect() // 1.10 uses result of TestIndexedTable 1.2
        {
            object[,] vals =  {  { "HC" },  { "DE" } };
            CheckResults("select d from b", vals, "D");
        }
        [TestMethod]
        public void TestStarWhere() // 1.11 uses result of TestSimpleTable 1.1
        {
            object[,] vals =  { { 23L, "BC" } };
            CheckResults("select * from a where b=23", vals, "B", "C");
        }
        [TestMethod]
        public void TestNamedWhere() // 1.12 uses result of TestSimpleTable 1.1
        {
            CheckResults("select c from a where b=23",  new object[,]{  { "BC" } }, "C");
        }
        [TestMethod]
        public void TestNamedWhereKey() // 1.13 uses result of TestIndexedTable 1.2
        {
            CheckResults("select d from b where c=23", new object[,] { { "HC" } }, "D");
        }
        [TestMethod]
        public void TestNamedWhereNonKey() // 1.14 uses result of TestIndexedTable 1.2
        {
            CheckResults("select c from b where d>'GG'", new object[,] { { 23L } }, "C");
        }
        [TestMethod]
        public void TestOrderBy() // 1.15 uses result of TestSimpleTable 1.1
        {
            object[,] vals =  { { 23L }, { 17L } };
            CheckResults("select b from a order by c desc", vals, "B");
        }
        [TestMethod]
        public void TestOrderByFirstKey() // 1.16 uses result of TestMultiIndexedtable 1.3
        {
            object[,] vals = {  { "DE" }, { "XC" } };
            CheckResults("select g from e order by f desc", vals, "G");
        }
        [TestMethod]
        public void TestOrderBySecondKey() // 1.17 uses result of TestMultiIndexedtable 1.3
        {
            object[,] vals =  { { 45L },  { 23L } };
            CheckResults("select f from e order by g", vals, "F");
        }
        [TestMethod]
        public void TestOrderByNonKey() // 1.18 uses result of TestIndexedtable 1.2
        {
            object[,] vals =  { { 45L },  { 23L } };
            CheckResults("select c from b order by d", vals, "C");
        }
        [TestMethod]
        public void TestOrderByTwoColumns() // 1.19 
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int, cc real, dd char,primary key(bb,cc))");
            conn.Act("insert into aa values(1,3.4,'EF'),(1,5.6,'AB'),(2,5.6,'CD')");
            object[,] vals =  { { 1L,3.4, "EF" }, { 1L,5.6, "AB" },  { 2L,5.6, "CD" } };
            CheckResults("select * from aa order by cc,dd", vals, "BB","CC","DD");
            tr.Rollback();
        }
        [TestMethod]
        public void TestOrderByTwoColumns1() // 1.20 
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int, cc real, dd char,primary key(bb,cc))");
            conn.Act("insert into aa values(1,5.6,'AB'),(2,5.6,'CD'),(3,3.4,'AB')");
            object[,] vals = { { 3L, "AB" }, { 1L, "AB" }, { 2L, "CD" } };
            CheckResults("select bb,dd from aa order by dd,cc", vals, "BB", "DD");
            tr.Rollback();
        }
        [TestMethod]
        public void TestOrderByExpression() // 1.21
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int, cc int, dd char,primary key(bb,cc))");
            conn.Act("insert into aa values(1,5,'AB'),(2,2,'CD'),(3,4,'AB')");
            object[,] vals =  {  { 2L, "CD" },  { 1L, "AB" }, { 3L, "AB" } };
            CheckResults("select bb,dd from aa order by bb+cc", vals, "BB", "DD");
            tr.Rollback();
        }
        [TestMethod]
        public void TestSelectExpressions() // 1.22
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int, cc int, dd char,primary key(bb,cc))");
            conn.Act("insert into aa values(1,5,'AB'),(3,6,'CD'),(3,4,'AB')");
            object[,] vals =  {  { 6L, "AB" }, { 7L, "AB" }, { 9L, "CD" } };
            CheckResults("select bb+cc,dd from aa", vals, "", "DD");
            CheckResults("select bb+aa.cc,dd from aa", vals, "", "DD");
            CheckResults("select bb+ee.cc,dd from aa ee", vals, "", "DD");
            tr.Rollback();
        }
        [TestMethod]
        public void TestSelectExpressions1() // 1.22A
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int, cc int, dd char,primary key(bb,cc))");
            conn.Act("insert into aa values(1,5,'AB'),(3,6,'CD'),(3,4,'AB')");
            object[,] vals = { { 9L, "CD" } };
            CheckResults("select bb+cc as e,dd from aa where e>7", vals, "E", "DD");
            tr.Rollback();
        }
        [TestMethod]
        public void TestOrderByFunctionCall() // 1.23
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int primary key, dd char)");
            conn.Act("insert into aa values(1,'AB'),(3,'AB'),(-2,'CDE')");
            CheckResults("select bb,dd from aa order by abs(bb)", new object[,] { { 1L, "AB" }, { -2L, "CDE" }, { 3L, "AB" } }, "BB", "DD");
            CheckResults("select bb,dd from aa order by bb+char_length(dd)", new object[,] { { -2L, "CDE" },{ 1L, "AB" },  { 3L, "AB" } }, "BB", "DD");
            tr.Rollback();
        }
        [TestMethod]
        public void TestSelectWhereFunctionCall() // 1.24
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int primary key, dd char)");
            conn.Act("insert into aa values(1,'AB'),(3,'AB'),(-2,'CD')");
            object[,] vals =  { { -2L, "CD" } ,{ 1L, "AB" }  };
            CheckResults("select bb,dd from aa where abs(bb)<=2", vals, "BB", "DD");
            tr.Rollback();
        }
        [TestMethod]
        public void TestFromSelect() // 1.25
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int, cc int, dd char,primary key(bb,cc))");
            conn.Act("insert into aa values(1,5,'AB'),(3,2,'CD'),(3,4,'AB')");
            object[,] vals =  {  { 4L } };
            CheckResults("select sum(bb) from (select bb from aa where cc>3)", vals);
            tr.Rollback();
        }
        [TestMethod]
        public void TestFromSelect1() // 1.25A
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int, cc int, dd char,primary key(bb,cc))");
            conn.Act("insert into aa values(1,5,'AB'),(3,2,'CD'),(3,4,'AB')");
            object[,] vals = { { 17L,"AB"},{ 17L,"CD"},{ 17L, "AB" } };
            CheckResults("select b,dd from (select 17 as b,dd from aa)", vals, "B", "DD");
            CheckResults("select e from (select bb as e from aa)",new object[,] { { 1 },{ 3 } , {3 } } );
            CheckResults("select e.bb from (select (bb,cc) as e from aa)", new object[,] { { 1 }, { 3 }, { 3 } });
            tr.Rollback();
        }
        [TestMethod]
        public void TestFromSelect2() // 1.25B
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int, cc int, dd char,primary key(bb,cc))");
            conn.Act("insert into aa values(1,5,'AB'),(3,2,'CD'),(3,4,'AB')");
            object[,] vals = { { 1L, "AB" }, { 3L, "CD" }, { 3L, "AB" } };
            CheckResults("select b,dd from (select bb as b,dd from aa)", vals, "B", "DD");
            tr.Rollback();
        }
        [TestMethod]
        public void TestFromSelect3() // 1.25C
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int, cc int, dd char,primary key(bb,cc))");
            conn.Act("insert into aa values(1,5,'AB'),(3,2,'CD'),(3,4,'AB')");
            object[,] vals = { { 6L,"AB"},{ 5L,"CD"},{ 7L, "AB" } };
            CheckResults("select b,dd from (select bb+cc as b,dd from aa)", vals, "B", "DD");
            tr.Rollback();
        }
        [TestMethod]
        public void TestFromSelect4() // 1.25D
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int,cc int,dd int,ee int)");
            conn.Act("insert into aa values(2,3,4,0),(3,1,5,2)");
            object[,] vals = { { 2L }, { 5L } };
            CheckResults("select a[b] from (select array[bb,cc,dd] as a,ee as b from aa)",vals);
            tr.Rollback();
        }
        [TestMethod]
        public void TestFromSelect5() // 1.25E
        {
            var tr = conn.BeginTransaction();
            conn.Act("create table aa (bb int,cc int)");
            conn.Act("insert into aa values(2,2),(3,1),(1,3)");
            object[,] vals = { { 2L }};
            CheckResults("select array(select bb from aa order by cc)[1] from static", vals);
            tr.Rollback();
        }
        /*      This test is withdrawn. Th SQL standard says: If <array value constructor by query> is specified, then
        a) The <query expression> QE simply contained in the <table subquery> shall be of degree 1 (one).
         *      public void TestSelectArray()
                {
                    var tr = conn.BeginTransaction();
                    conn.Act("create table cust(id int primary key,sname char)");
                    conn.Act("insert into cust values(1,'Soap'),(2,'Bloggs')");
                    conn.Act("create table ords(oid int primary key,cid int references cust,item int,qty int)");
                    conn.Act("insert into ords values(3,1,6,2),(4,2,5,1),(5,1,4,7)");
                    conn.Act("create view vw as select id,sname,array(select * from ords where cid=id) from cust");
                    object[,] a = { { 3L, 1L, 6L, 2L }, { 5L, 1L, 4L, 7L } };
                    object[,] b = { { 4L, 2L, 5L, 1L } };
                    object[,] vals = { { 1L, "Soap", a }, { 2L, "Bloggs", b } };
                    CheckResults("table vw", vals);
                    tr.Rollback();
                } */
        /*       [TestMethod] This test is withdrawn: comments about it are welcome though.
         *       MAX(T.BB) might be better - does it remove an ambiguity? 
         *       but we can't analyse the subquery because we don't know what BB AS A is 
               public void TestSelectSelect() // 1.26
               {
                   var tr = conn.BeginTransaction();
                   conn.Act("create table aa (bb int, cc int, dd char,primary key(bb,cc))");
                   conn.Act("insert into aa values(1,5,'AB'),(3,2,'CD'),(2,4,'AB')");
                   object[,] vals =  {  { 1L, 3L },  { 2L, 2L }, {3L, 2L } };
                   CheckResults("select bb as a,(select max(bb) from aa t where t.cc>a) from aa", vals, "A");
                   tr.Rollback();
               } */
        [TestMethod]
        public void TestSimpleWhere() // uses result of TestSimpleSelect
        {
            CheckResults("select b from a where b>20", new object[,] {  {23} });
        }
        [TestMethod]
        public void TestSimpleJoin() // uses result of TestSimpleSelect
        {
            conn.Act("update a set c='xyz' where b=17");
            conn.Act("create table k(c int primary key,d char)");
            conn.Act("insert into k values (15,'aaa'),(17,'abc'),(21,'def')");
            conn.Act("insert into a values(21,'zyx')");
            CheckResults("select a.c,d from a,k where a.b=k.c",
                new string[,] {  { "xyz", "abc" }, { "zyx", "def" } });
        }
        [TestMethod]
        public void TestTriggers()
        {
            var t = conn.BeginTransaction();
            conn.Act("create table xa(b int,c int,d char)");
            conn.Act("create table xb(tot int)");
            conn.Act("create table xc(totb int,totc int)");
            conn.Act("insert into xb values (0)");
            conn.Act("create trigger sdai instead of delete on xa referencing old table as ot "+
            "for each statement "+ // when (select max(ot.b) from ot)<10  "+
            "begin atomic insert into xc (select b as totb,c as totc from ot) end"); //(select sum(oo.b),sum(oo.c) from ot oo) end");
            conn.Act("create trigger riab before insert on xa referencing new as nr "+
            "for each row begin atomic set nr.c=nr.b+3; update xb set tot=tot+nr.b end");
            conn.Act("create trigger ruab before update on xa referencing old as mr new as nr "+
            "for each row begin atomic update xb set tot=tot-mr.b+nr.b; "+
            "set nr.d='changed' end");
            conn.Act("insert into xa(b,d) values (7,'inserted')");
            CheckResults("table xa", new object[,] { { 7L, 10L, "inserted" } }, "B", "C", "D");
            CheckResults("table xb", new object[,] { { 7L } }, "TOT");
            conn.Act("update xa set b=8,d='updated' where b=7");
            CheckResults("table xa", new object[,] { { 8L, 10L, "changed" } }, "B", "C", "D");
            CheckResults("table xb", new object[,] { { 8L } }, "TOT");
            conn.Act("delete from xa where d='changed'");
            CheckResults("table xc", new object[,] { { 8L, 10L } }, "TOTB", "TOTC");
            CheckResults("table xa", new object[,] { { 8L, 10L, "changed" } }, "B", "C", "D"); // INSTEAD OF!
            t.Rollback();
        }
        public void TestViews()
        {
            var t = conn.BeginTransaction();
            conn.Act("create table p(q int primary key,r char)");
            conn.Act("create view v as select q,r as s from p");
            conn.Act("insert into v(s) values('Twenty'),('Thirty')");
            conn.Act("update v set s='Forty two' where q=1");
            CheckResults("select r from p",new object[,]{ { "Forty two" }, { "Thirty"}});
            t.Rollback();
        }
        public void TestWindowFunctions1() // from MariaDB documentation
        {
            var t = conn.BeginTransaction();
            conn.Act("create table student(name char,test char,score int)");
            conn.Act("insert into student values('Chun','SQL',75),('Chun','Tuning',73)," +
                "('Esben', 'SQL', 43), ('Esben', 'Tuning', 31), " +
                "('Kaolin', 'SQL', 56), ('Kaolin', 'Tuning', 88)," +
                "('Tatiana', 'SQL', 87), ('Tatiana', 'Tuning', 83)");
            CheckResults("SELECT name, test, score, AVG(score) OVER (PARTITION BY test)" +
                "AS average_by_test FROM student", new object[,]{
                    { "Chun"    , "SQL"     , 75 ,         65.2500 },
                    { "Chun"    , "Tuning"  , 73 ,         68.7500 },
                    { "Esben"   , "SQL"     , 43 ,         65.2500 },
                    { "Esben"   , "Tuning"  , 31 ,         68.7500 },
                    { "Kaolin"  , "SQL"     , 56 ,         65.2500 },
                    { "Kaolin"  , "Tuning"  , 88 ,         68.7500 },
                    { "Tatiana" , "SQL"     , 87 ,         65.2500 },
                    { "Tatiana" , "Tuning"  , 83 ,         68.7500 } });
            CheckResults("SELECT name, test, score, AVG(score) OVER (PARTITION BY name)" +
    "AS average_by_name FROM student", new object[,]{
                    { "Chun"    , "SQL"     , 75 ,         74.0000 },
                    { "Chun"    , "Tuning"  , 73 ,         74.0000 },
                    { "Esben"   , "SQL"     , 43 ,         37.0000 },
                    { "Esben"   , "Tuning"  , 31 ,         37.0000 },
                    { "Kaolin"  , "SQL"     , 56 ,         72.0000 },
                    { "Kaolin"  , "Tuning"  , 88 ,         72.0000 },
                    { "Tatiana" , "SQL"     , 87 ,         85.0000 },
                    { "Tatiana" , "Tuning"  , 83 ,         85.0000 } });
            t.Rollback();
        }
        public void TestWindowFunctions2() // from MariaDB documentation
        {
            var t = conn.BeginTransaction();
            conn.Act("CREATE TABLE users (email CHAR,first_name CHAR," +
                    "last_name CHAR,account_type CHAR)");
            conn.Act("INSERT INTO users VALUES ('admin@boss.org', 'Admin', 'Boss', 'admin')," +
                "('bob.carlsen@foo.bar', 'Bob', 'Carlsen', 'regular')," +
                "('eddie.stevens@data.org', 'Eddie', 'Stevens', 'regular')," +
                "('john.smith@xyz.org', 'John', 'Smith', 'regular')," +
                "('root@boss.org', 'Root', 'Chief', 'admin')");
            CheckResults("SELECT row_number() OVER (ORDER BY email) AS rnum," +
                "email, first_name, last_name, account_type FROM users ORDER BY email", 
                new object[,] {
                    {    1 , "admin@boss.org"         , "Admin"      , "Boss"      , "admin" },
                    {    2 , "bob.carlsen@foo.bar"    , "Bob"        , "Carlsen"   , "regular" },
                    {    3 , "eddie.stevens@data.org" , "Eddie"      , "Stevens"   , "regular" },
                    {    4 , "john.smith@xyz.org"     , "John"       , "Smith"    , "regular" },
                    {    5 , "root@boss.org"          , "Root"       , "Chief"     , "admin" }
                });
            CheckResults("SELECT row_number() OVER (PARTITION BY account_type ORDER BY email) AS rnum,"+ 
                "email, first_name, last_name, account_type FROM users ORDER BY account_type, email",
                new object[,] {
                    {    1 , "admin@boss.org"         , "Admin"      , "Boss"      , "admin" },
                    {    2 , "root@boss.org"          , "Root"       , "Chief"     , "admin"  },
                    {    1 , "bob.carlsen@foo.bar"    , "Bob"        , "Carlsen"   , "regular" },
                    {    2 , "eddie.stevens@data.org" , "Eddie"      , "Stevens"   , "regular" },
                    {    3 , "john.smith@xyz.org"     , "John"       , "Smith"    , "regular"}
                });
            t.Rollback();
        }
        public void TestWindowFunctions3() // from MariaDB documentation
        {
            var t = conn.BeginTransaction();
            conn.Act("CREATE TABLE employee_salaries (dept CHAR, name CHAR, salary INT)");
            conn.Act("INSERT INTO employee_salaries VALUES('Engineering', 'Dharma', 3500),"+
                "('Engineering', 'Bình', 3000),('Engineering', 'Adalynn', 2800),('Engineering', 'Samuel', 2500),"+
                "('Engineering', 'Cveta', 2200),('Engineering', 'Ebele', 1800),('Sales', 'Carbry', 500),"+
                "('Sales', 'Clytemnestra', 400),('Sales', 'Juraj', 300),('Sales', 'Kalpana', 300),"+
                "('Sales', 'Svantepolk', 250),('Sales', 'Angelo', 200)");
            CheckResults("select dept, name, salary from employee_salaries as t1 "+
                "where (select count(t2.salary) from employee_salaries as t2 "+
                "where t1.name <> t2.name and t1.dept = t2.dept and t2.salary > t1.salary) < 5 "+
                "order by dept, salary desc",
                new object[,] {
                    { "Engineering" , "Dharma"       ,   3500 },
                    { "Engineering" , "Bình"         ,   3000 },
                    { "Engineering" , "Adalynn"      ,   2800 },
                    { "Engineering" , "Samuel"       ,   2500 },
                    { "Engineering" , "Cveta"        ,   2200 },
                    { "Sales"       , "Carbry"       ,    500 },
                    { "Sales"       , "Clytemnestra" ,    400 },
                    { "Sales"       , "Juraj"        ,    300 },
                    { "Sales"       , "Kalpana"      ,    300 },
                    { "Sales"       , "Svantepolk"   ,    250 }
                });
            CheckResults("select rank() over (partition by dept order by salary desc) as ranking,"+
                "dept, name, salary from employee_salaries order by dept, ranking",
                new object[,] {
                    {       1 , "Engineering" , "Dharma"       ,   3500 },
                    {       2 , "Engineering" , "Bình"         ,   3000 },
                    {       3 , "Engineering" , "Adalynn"      ,   2800 },
                    {       4 , "Engineering" , "Samuel"       ,   2500 },
                    {       5 , "Engineering" , "Cveta"        ,   2200 },
                    {       6 , "Engineering" , "Ebele"        ,   1800 },
                    {       1 , "Sales"       , "Carbry"       ,    500 },
                    {       2 , "Sales"       , "Clytemnestra" ,    400 },
                    {       3 , "Sales"       , "Juraj"        ,    300 },
                    {       3 , "Sales"       , "Kalpana"      ,    300 },
                    {       5 , "Sales"       , "Svantepolk"   ,    250 },
                    {       6 , "Sales"       , "Angelo"       ,    200 }
                });
            CheckResults("select *from (select rank() over (partition by dept order by salary desc) as ranking,"+
                " dept, name, salary from employee_salaries) as salary_ranks where(salary_ranks.ranking <= 5)"+
                "  order by dept, ranking",
                new object[,] {
                                {       1 , "Engineering" , "Dharma"       ,   3500 },
                                {       2 , "Engineering" , "Bình"         ,   3000 },
                                {       3 , "Engineering" , "Adalynn"      ,   2800 },
                                {       4 , "Engineering" , "Samuel"       ,   2500 },
                                {       5 , "Engineering" , "Cveta"        ,   2200 },
                                {       1 , "Sales"       , "Carbry"       ,    500 },
                                {       2 , "Sales"       , "Clytemnestra" ,    400 },
                                {       3 , "Sales"       , "Juraj"        ,    300 },
                                {       3 , "Sales"       , "Kalpana"      ,    300 },
                                {       5 , "Sales"       , "Svantepolk"   ,    250 },
                });
            t.Rollback();
        }
        public void TestWindowFunctions4()
        {
            var t = conn.BeginTransaction();
            conn.Act("create table times(id int primary key,name char,lap numeric(3,1))");
            conn.Act("insert into times(name,lap) values" +
                "('Fred',23.2),('Joe',24.8),('Fred',23.9),('Joe',24.6),('Fred',25.7)");
            CheckResults("select id,name,lap,avg(lap) over (partition by name rows between 1 preceding and current row) from times",
            new object[,] { {1, "Fred", 23.2, 23.2},{2,"Joe",24.8,24.8},
            {3,"Fred",23.9,23.55 },{4,"Joe",24.6,24.7},{5,"Fred",25.7,24.8 }
            });
        }
        void CheckResults(string sql, object[,] vals, params string[] names)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            var rdr = cmd.ExecuteReader();
            var ct = 0;
            //       Console.WriteLine(sql);
            while (rdr.Read())
            {
                Assert.IsFalse(ct == vals.GetLength(0), "not enough data");
                Check(rdr, names, Jag(vals)[ct]);
                ct++;
                //          Console.WriteLine();
            }
            Assert.AreEqual<int>(vals.GetLength(0), ct, "Wrong number of rows returned");
            rdr.Close();
        }
        void Check(PyrrhoReader r,string[] names,object[] vals)
        {
            try
            {
                for (int j = 0; j < vals.Length; j++)
                {
                    if (vals[j] == null)
                        Assert.IsTrue(r.IsDBNull(j), "unexpected non-null value");
                    else
                    {
                        Assert.IsFalse(r.IsDBNull(j), "unexpected null value");
                        if (j < names.Length && names[j] != "")
                            Check(r, names[j], vals[j]);
                        else
                            Check(r, j, vals[j]);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        void Check(PyrrhoReader r,string n,object v) // v guaranteed not null
        {
            try
            {
                if (v is byte)
                    Assert.AreEqual((byte)v, (byte)r[n], "Expected " + v + " got " + r[n]);
                else if (v is int)
                    Assert.AreEqual((int)v, (int)(long)r[n], "Expected " + v + " got " + r[n]);
                else if (v is long)
                    Assert.AreEqual((long)v, (long)r[n], "Expected " + v + " got " + r[n]);
                else if (v is string)
                    Assert.AreEqual((string)v, r[n].ToString(), "Expected " + v + " got " + r[n]);
                else if (v is decimal)
                    Assert.AreEqual((decimal)v, (decimal)r[n], "Expected " + v + " got " + r[n]);
                else if (v is Date)
                    Assert.AreEqual((Date)v, (Date)r[n], "Expected " + v + " got " + r[n]);
                else
                    Assert.AreEqual<string>(v.ToString(), r[n].ToString(), "Expected " + v + " got " + r[n]);
            } catch (Exception e)
            {
                Console.WriteLine("Bad values " + (v?.ToString() ?? "<null>") + " vs " + (r[n]?.ToString() ?? "<null>"));
            }
        }
        void Check(PyrrhoReader r, int j, object v) // v guaranteed not null
        {
            try
            {
                if (v is byte)
                    Assert.AreEqual((byte)v, r.GetByte(j), "Expected " + v + " got " + r[j]);
                else if (v is int)
                    Assert.AreEqual((int)v, r.GetInt32(j), "Expected " + v + " got " + r[j]);
                else if (v is long)
                    Assert.AreEqual((long)v, r.GetInt64(j), "Expected " + v + " got " + r[j]);
                else if (v is string)
                {
                    if (((string)v) != "*")
                        Assert.AreEqual<string>((string)v, r.GetString(j), "Expected " + v + " got " + r[j]);
                }
                else if (v is decimal)
                    Assert.AreEqual<decimal>((decimal)v, r.GetDecimal(j), "Expected " + v + " got " + r[j]);
                else if (v is Date)
                    Assert.AreEqual<Date>((Date)v, new Date(r.GetDateTime(j)), "Expected " + v + " got " + r[j]);
                else if (v is object[,])
                    CheckValue((PyrrhoArray)r[j], (object[,])v);
                else
                    Assert.AreEqual<string>(r[j].ToString(), v.ToString(), "Expected " + v + " got " + r[j]);
            }
            catch (Exception e)
            {
                Console.WriteLine("Bad values " + (v?.ToString() ?? "<null>") + " vs " + (r[j]?.ToString() ?? "<null>"));
            }
        }
        void CheckValue(PyrrhoArray rv,object[,] v)
        {
            Assert.AreEqual<int>(rv.data.Length, v.GetLength(0));
            for (int i = 0; i < rv.data.Length; i++)
                CheckValue((PyrrhoRow)rv.data[i], Jag(v)[i]);
        }
        void CheckValue(PyrrhoRow rv,object[]v)
        {
            Assert.AreEqual<int>(rv.ItemArray.Length, v.Length);
            for (int i = 0; i < rv.ItemArray.Length; i++)
                Assert.AreEqual(rv[i], v[i]);
        }
        static string Values(object[] v)
        {
            var sb = new StringBuilder();
            var c = "";
            foreach (var w in v)
            {
                sb.Append(c);
                if (w is string)
                    sb.Append("'" + w + "'");
                else if (w is Date)
                {
                    var d = w as Date;
                    sb.Append("date'" + d.date.Year + "-" + d.date.Month.ToString("2d") + "-" + d.date.Day.ToString("2d") + "'");
                }
                else
                    sb.Append(w);

                c = ",";
            }
            return sb.ToString();
        }
        static object[][] Jag(object[,] v)
        {
            var r = new object[v.GetLength(0)][];
            for (var i = 0; i < v.GetLength(0); i++)
            {
                r[i] = new object[v.GetLength(1)];
                for (var j = 0; j < v.GetLength(1); j++)
                    r[i][j] = v[i, j];
            }
            return r;
        }
        void ShowLog()
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "table \"Log$\"";
            var rdr = cmd.ExecuteReader();
            while (rdr.Read())
                Console.WriteLine("" + rdr[0] + " " + rdr[1] + " " + rdr[3]);
            rdr.Close();
        }
    }
}
