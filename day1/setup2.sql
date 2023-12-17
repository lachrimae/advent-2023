\set example2 `cat example2.txt`

begin transaction;
  set schema 'day1';
  insert into inputs (tag, data) values ('example2', :'example2');
commit transaction;
