\set example `cat example.txt`
\set challenge `cat challenge.txt`

begin transaction;
  create schema day1;
  set schema 'day1';
  
  create table inputs (
    id int primary key generated always as identity,
    tag text not null,
    data text not null
  );
  
  insert into inputs (tag, data) values ('example', :'example'), ('challenge', :'challenge');
commit transaction;
