create table author(id int primary key,name char)
create table book(id int primary key,title char,aid int references author)
insert into author values (1,'Dickens'),(2,'Conrad')
insert into book values(10,'Lord Jim',2),(11,'Nicholas Nickelby',1)
table book
