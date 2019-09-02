alter table book add aname char generated always as (select name from author a where a.id=aid)
table book
