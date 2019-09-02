from OSPLink import *
con = PyrrhoConnect('Files=unittests')
con.open()
def val(r,n):
    if isinstance(n,str):
        return r.col(n)
    else:
        return r.val(n)
def areEqual(r,v,n):
    assert len(n)==0 or len(n)==len(v)
    for i in range(len(n)):
        if i<len(n) and n[i]!='':
            assert r.col(n[i]) == v[i]
        else:
            assert r.val(i) == v[i]
def results(sql,vals,names=[]):
    cmd = con.createCommand()
    cmd.commandText = sql
    rdr = cmd.executeReader()
    assert rdr!=None
    ct = 0
    while rdr.read():
        assert ct<len(vals)
        areEqual(rdr,vals[ct],names)
        ct+=1
    assert ct==len(vals)
    rdr.close()
def values(v):
    s = ''
    c = ''
    for a in v:
        s += c
        c = ','
        if isinstance(a,str):
            s += "'"+a+"'"
        else:
            s += str(a)
    return s
#testStatic
results('select 17 from static',[[17]])
#TestSimpleTable1
tr = con.beginTransaction()
con.act("create table aa(b int,c char)")
con.act("insert into aa(b) values(17)")
con.act("insert into aa(c) values('BC')")
con.act("insert into aa(c,b) values('GH',67)")
results("table aa", [(17,None),(None,"BC"),(67,"GH")],["B","C"]);
tr.rollback()
#TestAuthors
tr = con.beginTransaction()
con.act("create table author(id int primary key,aname char)")
con.act("create table book(id int primary key,authid int references author,title char)")
con.act("insert into author values (1,'Dickens'),(2,'Conrad')")
con.act("insert into book(authid,title) values (1,'Dombey & Son'),(2,'Lord Jim'),(1,'David Copperfield')")
con.act("create function booksby(auth char) returns table(title char)" +
    "return table (select title from author a inner join book b on a.id=b.authid where aname=booksby.auth)")
results("select title from author a inner join book b on a.id=b.authid where aname='Dickens'", [ ( "Dombey & Son" ), ( "David Copperfield" ) ]);
results("select * from table(booksby('Dickens'))", [ ( "Dombey & Son" ), ( "David Copperfield" ) ]);
results("select count(*) from table(booksby('Dickens'))",[[2]]);
tr.rollback()
#TestRename
tr = con.beginTransaction()
con.act("create table ta(a1 int primary key)")
con.act("insert into ta values(17),(23)")
con.act("create view vw as select a1 from ta")
results("table ta", [ [ 17], [ 23 ]], ["A1"])
con.act("alter table ta alter a1 to id");
results("table vw", [ [ 17 ], [23 ] ], ["ID"])
results("table \"Sys$View\"",[("*","VW","select \"ID\" from \"TA\"")])
tr.rollback()
#TestPlayed
tr = con.beginTransaction()
con.act("create table members (id int primary key,firstname char)")
con.act("create table played (id int primary key, winner int references members, loser int references members,agreed boolean)")
con.act("grant select on members to public")
con.act("grant select on played to public")
con.act("create procedure claim(won int,beat int) insert into played(winner,loser) values(claim.won,claim.beat)")
con.act("create procedure agree(p int) update played set agreed=true where winner=agree.p and loser in " +
    "(select m.id from members m where current_user like '%'||firstname escape '^')")
con.act("insert into members(firstname) values(current_user)")
results("select id from members where current_user like '%'||firstname escape'^'", [[ 1 ]], ["ID"])
con.act("insert into members(firstname) values('Fred')")
con.act("insert into played(winner,loser) values(2,1)")
con.act("create role membergames")
con.act("grant execute on procedure claim(int,int) to role membergames")
con.act("grant execute on procedure agree(int) to role membergames")
con.act("grant membergames to public")
con.act("set role membergames")
con.act("call agree(2)")
con.act("call claim(1,2)")
results("table played", [ ( 1, 2, 1, True ), ( 2, 1, 2, None )])
tr.rollback()
#TestGather1
tr = con.beginTransaction()
con.act("create table ga(a1 int primary key,a2 char)");
con.act("insert into ga values(1,'First'),(2,'Second')");
con.act("create function gather1() returns char " +
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
    "return a end")
results("select gather1() from static", [ ( "First, Second" )]);
tr.rollback()
#TestGather2
tr = con.beginTransaction()
con.act("create table ga(a1 int primary key,a2 char)")
con.act("insert into ga values(1,'First'),(2,'Second')")
con.act("create function gather2() returns char " +
    "begin declare b char default '';" +
    "for select a2 from ga do if b='' then set b=a2 else set b=b||', '||a2 end if end for;" +
    "return b end")
results("select gather2() from static", [( "First, Second" )])
tr.rollback()
#TestPoints
tr = con.beginTransaction()
con.act("create type point as (x int, y int)")
con.act("create type size as (w int,h int)")
con.act("create type line as (strt point,en point)")
con.act("create type rect as (tl point,sz size) constructor method rect(x1 int,y1 int, x2 int, y2 int),method centre() returns point")
con.act("create table figure(id int primary key,title char)")
con.act("create table figureline(id int primary key,fig int references figure,what line)")
con.act("create table figurerect(id int primary key,fig int references figure,what rect)")
con.act("create constructor method rect(x1 int,y1 int,x2 int,y2 int) begin tl=point(x1,y1); sz=size(x2-x1,y2-y1) end")
con.act("create method centre() returns point for rect return point(tl.x+sz.w/2,tl.y+sz.h/2)")
con.act("create function centrerect(a int) returns point return (select what.centre() from figurerect where id=a)")
con.act("insert into figure values(1,'Diagram')")
con.act("insert into figurerect values(1,1,rect(point(1,2),size(3,4)))")
con.act("insert into figurerect values(2,1,rect(4,5,6,7))")
con.act("insert into figureline values(1,1,line(centrerect(1),centrerect(2)))")
results("select what from figureline",[("LINE(STRT=POINT(X=2,Y=4),EN=POINT(X=5,Y=6))")]);
tr.rollback()
#TestSubQuery
tr = con.beginTransaction()
con.act("create table b(a int primary key, c int)")
con.act("insert into b values(5,2),(6,1),(7,4)")
con.act("create table c(d int, e int)")
#con.act("insert into c (select * from b where a=7)")
#con.act("insert into c (select 17,(select a from b where c=1) from static)") 
con.act("insert into c values((select c from b where a=5),22)")
results("table c", [ #( 7, 4 ), ( 17, 6), 
    ( 2, 22 ) ])
con.act("create table d(e int array)")
con.act("insert into d values(array(select c from b g))")
con.act("create type mrw as (c int)")
con.act("create table e(f mrw,primary key(f.c))")
con.act("insert into e (select mrw(c) as f from b where a<>7)")
results("table e", [( "MRW(C=1)" )  ,  ( "MRW(C=2)" )]); 
results("select (select c from b where a=5) from static", [( 2 )]);
tr.rollback()
#TestGroup
tr = con.beginTransaction()
con.act("create table a(b int,c int)")
con.act("insert into a values(2,10),(1,12),(2,14),(2,16)")
results("select b,sum(c),avg(c) from a group by b", [( 1, 12, 12.0 ),( 2, 40, 13.3333333333333 )])
tr.rollback()
#TestDocument
tr = con.beginTransaction()
con.act("create table people(doc document)")
con.act("insert into people values ({NAME: \"fred\", AGE: 20, STATUS: \"B\", GROUPS: [\"music\"]})")
con.act("insert into people values ({NAME: \"sue\", AGE: 26, STATUS: \"A\", GROUPS: [\"news\", \"sports\"]})")
con.act("insert into people values ({NAME: \"john\", AGE: 16, STATUS: \"C\"})")
con.act("insert into people values ({NAME: \"mary\", AGE: 20, STATUS: \"A\", GROUPS: [\"music\", \"sports\"]})")
results("select doc.name from people where doc.age>18 order by doc.age", [("fred"),("mary"),("sue")])
results("select doc.name from people where doc={AGE: {$gt: 18}}  order by doc.age", [( "fred" ), ( "mary" ),("sue" )])
results("select {$aggregate: PEOPLE, pipeline: ["+
                "{$match: {AGE: {$gt: 18}}}, " +
                "{$group: {_id: $AGE, count: {$sum: 1}}}, "+
                "{$sort: { _id: -1 }}] } from static",
                [("{\"ok\": True, \"result\": [{\"_id\": 26, \"count\": 1}, {\"_id\": 20, \"count\": 2}]}")])
tr.rollback()
#TestPositioned
tr = con.beginTransaction()
con.act("create table ca(cb char)")
con.act("create procedure delw(w char)"+
" for c cursor for table ca do "+
" if cb=w then "+
" delete from ca where current of c "+
" end if"+
" end for")
con.act("create procedure updw(w char,u char)" +
" for c cursor for table ca do " +
" if cb=w then " +
"update ca set cb=u where current of c " +
" end if " +
" end for")
con.act("insert into ca values('To'),('Three'),('F4')")
con.act("call delw('To')")
con.act("call updw('F4','Four')")
results("table ca", [( "Three" ), ( "Four" )]);
tr.rollback()
#TestSimpleTable //1.1
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("table a", vals)
tr.rollback()
#TestSimpleTable1 //1.1
tr = con.beginTransaction()
con.act("create table aa(b int,c char)")
con.act("insert into aa(b) values(17)")
con.act("insert into aa(c) values('BC')")
con.act("insert into aa(c,b) values('GH',67)")
results("table aa", [(17,None),(None,"BC"),(67,"GH")],["B","C"])
tr.rollback()
#TestIndexedTable //1.2
tr = con.beginTransaction()
con.act("create table b(c int primary key,d char)")
vals = [( 23, "HC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into b values(" + values(v) + ")")
results("table b", vals, ["C", "D"])
tr.rollback()
#TestMultiIndexedTable //1.3
tr = con.beginTransaction()
con.act("create table e(f int,g char,primary key(f,g))")
vals = [( 23, "XC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into e values(" + values(v) + ")")
results("table e", vals, ["F", "G"])
tr.rollback()
#TestSimpleSelectStar //1.4
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select * from a", vals, ["B", "C"])
tr.rollback()
#TestIndexedSelectStar //1.5
tr = con.beginTransaction()
con.act("create table b(c int primary key,d char)")
vals = [( 23, "HC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into b values(" + values(v) + ")")
results("select * from b", vals, ["C", "D"])
tr.rollback()
#TestMultiIndexedSelectStar //1.6
tr = con.beginTransaction()
con.act("create table e(f int,g char,primary key(f,g))")
vals = [( 23, "XC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into e values(" + values(v) + ")")
results("select * from e", vals, ["F", "G"])
tr.rollback()
#TestSimpleSelect //1.7
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select b from a", [[17],[23]], ["B"])
tr.rollback()
#TestSimpleSelect1 //1.7A
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select a.b from a", [[17],[23]], ["A.B"])
tr.rollback()
#TestSimpleSelect2 //1.7B
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select a.b as c from a", [[17],[23]], ["C"])
tr.rollback()
#TestSimpleSelect3 //1.7C
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select b as c from a", [[17],[23]], ["C"])
tr.rollback()
#TestSimpleSelect4 //1.7D
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select p.b from a p", [[17],[23]])
tr.rollback()
#TestSimpleSelect5 //1.7E
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select sum(p.b) from a p", [[40]])
tr.rollback()
#TestIndexedSelect //1.8
tr = con.beginTransaction()
con.act("create table b(c int primary key,d char)")
vals = [( 23, "HC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into b values(" + values(v) + ")")
results("select c from b", [[23],[45]])
tr.rollback()
#TestMultiIndexedSelect //1.9
tr = con.beginTransaction()
con.act("create table e(f int,g char,primary key(f,g))")
vals = [( 23, "XC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into e values(" + values(v) + ")")
results("select g from e", [["XC"],["DE"]], ["G"])
tr.rollback()
#TestNonKeySelect //1.10
tr = con.beginTransaction()
con.act("create table b(c int primary key,d char)")
vals = [( 23, "HC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into b values(" + values(v) + ")")
results("select d from b",[["HC"],["DE"]],["D"])
tr.rollback()
#TestStarWhere // 1.11
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select * from a where b=23",[(23,"BC")],["B","C"])
tr.rollback()
#TestNamedWhere // 1.12
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select c from a where b=23",[["BC"]],["C"])
tr.rollback()
#TestNamedWhereKey // 1.13
tr = con.beginTransaction()
con.act("create table b(c int primary key,d char)")
vals = [( 23, "HC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into b values(" + values(v) + ")")
results("select d from b where c=23",[["HC"]],["D"])
tr.rollback()
#TestNamedWhereNonKey // 1.14
tr = con.beginTransaction()
con.act("create table b(c int primary key,d char)")
vals = [( 23, "HC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into b values(" + values(v) + ")")
results("select c from b where d>'GG'",[[23]],["C"])
tr.rollback()
#TestOrderBy // 1.15
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select b from a order by c desc",[[23],[17]],["B"])
tr.rollback()
#TestOrderByFirstKey // 1.16
tr = con.beginTransaction()
con.act("create table e(f int,g char,primary key(f,g))")
vals = [( 23, "XC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into e values(" + values(v) + ")")
results("select g from e order by f desc",[["DE"],["XC"]],["G"])
tr.rollback()
#TestOrderBySecondKey // 1.17
tr = con.beginTransaction()
con.act("create table e(f int,g char,primary key(f,g))")
vals = [( 23, "XC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into e values(" + values(v) + ")")
results("select f from e order by g",[[45],[23]],["F"])
tr.rollback()
#TestOrderByNonKey // 1.18
tr = con.beginTransaction()
con.act("create table b(c int primary key,d char)")
vals = [( 23, "HC" ), ( 45, "DE" )]
for v in vals:
    con.act("insert into b values(" + values(v) + ")")
results("select c from b order by d",[[45],[23]],["C"])
tr.rollback()
#TestOrderByTwoColumns // 1.19
tr = con.beginTransaction()
con.act("create table aa (bb int, cc real, dd char,primary key(bb,cc))")
con.act("insert into aa values(1,3.4,'EF'),(1,5.6,'AB'),(2,5.6,'CD')")
results("select * from aa order by cc,dd",[(1,3.4,"EF"),(1,5.6,"AB"),(2,5.6,"CD")],["BB","CC","DD"])
tr.rollback()
#TestOrderByTwoColumns1 // 1.20
tr = con.beginTransaction()
con.act("create table aa (bb int, cc real, dd char,primary key(bb,cc))")
con.act("insert into aa values(1,5.6,'AB'),(2,5.6,'CD'),(3,3.4,'AB')")
vals = [( 3, "AB" ), ( 1, "AB" ), ( 2, "CD" )]
results("select bb,dd from aa order by dd,cc", vals, ["BB", "DD"])
tr.rollback()
#TestOrderByExpression // 1.21
tr = con.beginTransaction()
con.act("create table aa (bb int, cc int, dd char,primary key(bb,cc))")
con.act("insert into aa values(1,5,'AB'),(2,2,'CD'),(3,4,'AB')")
vals =  [( 2, "CD" ),  ( 1, "AB" ), ( 3, "AB" )]
results("select bb,dd from aa order by bb+cc", vals, ["BB", "DD"])
tr.rollback()
#TestSelectExpressions // 1.22
tr = con.beginTransaction()
con.act("create table aa (bb int, cc int, dd char,primary key(bb,cc))")
con.act("insert into aa values(1,5,'AB'),(3,6,'CD'),(3,4,'AB')")
vals =  [( 6, "AB" ), ( 7, "AB" ), ( 9, "CD" )]
results("select bb+cc,dd from aa", vals, ["", "DD"])
tr.rollback()
#TestSelectExpressions1 // 1.22A
tr = con.beginTransaction()
con.act("create table aa (bb int, cc int, dd char,primary key(bb,cc))")
con.act("insert into aa values(1,5,'AB'),(3,6,'CD'),(3,4,'AB')")
vals =  [( 6, "AB" ),(7, "AB" ),( 9, "CD" )]
results("select bb+cc,dd from aa", vals, ["", "DD"])
tr.rollback()
#TestOrderByFunctionCall // 1.23
tr = con.beginTransaction()
con.act("create table aa (bb int primary key, dd char)")
con.act("insert into aa values(1,'AB'),(3,'AB'),(-2,'CD')")
vals = [( 1, "AB" ), ( -2, "CD" ), ( 3, "AB" )]
results("select bb,dd from aa order by abs(bb)", vals, ["BB", "DD"])
tr.rollback()
#TestSelectWhereFunctionCall // 1.24
tr = con.beginTransaction()
con.act("create table aa (bb int primary key, dd char)")
con.act("insert into aa values(1,'AB'),(3,'AB'),(-2,'CD')")
vals =  [( -2, "CD" ) ,( 1, "AB" )]
results("select bb,dd from aa where abs(bb)<=2", vals, ["BB", "DD"])
tr.rollback()
#TestFromSelect // 1.25
tr = con.beginTransaction()
con.act("create table aa (bb int, cc int, dd char,primary key(bb,cc))")
con.act("insert into aa values(1,5,'AB'),(3,2,'CD'),(3,4,'AB')")
results("select sum(bb) from (select bb from aa where cc>3)", [[4]])
tr.rollback()
#TestFromSelect1 // 1.25A
tr = con.beginTransaction()
con.act("create table aa (bb int, cc int, dd char,primary key(bb,cc))")
con.act("insert into aa values(1,5,'AB'),(3,2,'CD'),(3,4,'AB')")
vals = [( 17,"AB"),( 17,"CD"),(17, "AB" )]
results("select b,dd from (select 17 as b,dd from aa)", vals, ["B", "DD"])
tr.rollback()
#TestFromSelect2 // 1.25B
tr = con.beginTransaction()
con.act("create table aa (bb int, cc int, dd char,primary key(bb,cc))")
con.act("insert into aa values(1,5,'AB'),(3,2,'CD'),(3,4,'AB')")
vals = [( 1, "AB" ), ( 3, "CD" ), ( 3, "AB" )]
results("select b,dd from (select bb as b,dd from aa)", vals, ["B", "DD"])
tr.rollback()
#TestFromSelect3 // 1.25C
tr = con.beginTransaction()
con.act("create table aa (bb int, cc int, dd char,primary key(bb,cc))")
con.act("insert into aa values(1,5,'AB'),(3,2,'CD'),(3,4,'AB')")
vals = [( 6,"AB"),( 5,"CD"),( 7, "AB" )]
results("select b,dd from (select bb+cc as b,dd from aa)", vals, ["B", "DD"])
tr.rollback()
#TestFromSelect4 // 1.25D
tr = con.beginTransaction()
con.act("create table aa (bb int,cc int,dd int,ee int)")
con.act("insert into aa values(2,3,4,0),(3,1,5,2)")
results("select a[b] from (select array(bb,cc,dd) as a,ee as b from aa)",[[2],[5]])
tr.rollback()
#TestFromSelect5 // 1.25E
tr = con.beginTransaction()
con.act("create table aa (bb int,cc int)")
con.act("insert into aa values(2,2),(3,1),(1,3)")
results("select array(select bb from aa order by cc)[1] from static", [[2]])
tr.rollback()
#TestSimpleWhere
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
results("select b from a where b>20",[[23]])
tr.rollback()
#TestSimpleJoin
tr = con.beginTransaction()
con.act("create table a(b int,c char)")
vals = [( 17, "AG" ), ( 23, "BC" )]
for v in vals:
    con.act("insert into a values(" + values(v) + ")")
con.act("update a set c='xyz' where b=23")
con.act("create table k(c int primary key,d char)")
con.act("insert into k values (15,'aaa'),(17,'abc'),(21,'def')")
con.act("insert into a values(21,'zyx')")
results("select a.c,d from a,k where a.b=k.c",
    [( "AG", "abc" ), ( "zyx", "def" ) ])
tr.rollback()
#TestTriggers
tr = con.beginTransaction()
con.act("create table xa(b int,c int,d char)")
con.act("create table xb(tot int)")
con.act("create table xc(totb int,totc int)")
con.act("insert into xb values (0)")
con.act("create trigger sdai instead of delete on xa referencing old table as ot "+
"for each statement "+ # when (select max(ot.b) from ot)<10  "+
"begin atomic insert into xc (select b as totb,c as totc from ot) end") #(select sum(oo.b),sum(oo.c) from ot oo) end");
con.act("create trigger riab before insert on xa referencing new as nr "+
"for each row begin atomic set nr.c=nr.b+3; update xb set tot=tot+nr.b end")
con.act("create trigger ruab before update on xa referencing old as mr new as nr "+
"for each row begin atomic update xb set tot=tot-mr.b+nr.b; "+
"set nr.d='changed' end")
con.act("insert into xa(b,d) values (7,'inserted')")
results("table xa", [( 7, 10, "inserted" )], ["B", "C", "D"])
results("table xb", [[ 7]], ["TOT"])
con.act("update xa set b=8,d='updated' where b=7")
results("table xa", [( 8, 10, "changed" )], ["B", "C", "D"])
results("table xb", [[ 8 ]], ["TOT"])
con.act("delete from xa where d='changed'");
results("table xc", [( 8, 10) ], ["TOTB", "TOTC"]);
results("table xa", [( 8, 10, "changed")], ["B", "C", "D"]); # INSTEAD OF!
tr.rollback()
#TestViews
t = con.beginTransaction()
con.act("create table p(q int,r char)")
con.act("create view v as select r as s from p")
con.act("insert into v(s) values('Twenty')")
con.act("update p set q=7")
con.act("create view w as select q+1 from p")
results("select * from w",[[8]])
t.rollback();
print('Testing complete')