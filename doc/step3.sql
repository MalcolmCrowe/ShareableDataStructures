alter table book add "Author" char generated always as (select name from author a where a.id=aid)
alter table book alter title to "Title"
select "Title","Author" from book
