set schema 'day1';

begin transaction;

  with
  lines (input_id, row_num, content) as (
    select i.id, a.num, a.content
    from inputs i, unnest(string_to_array(i.data, E'\n'))
    with ordinality as a(content, num)
  ),
  characters (input_id, row_num, col_num, content) as (
    select l.input_id, l.row_num, a.num, a.content
    from lines l, unnest(string_to_array(l.content, null))
    with ordinality as a(content, num)
  ),
  numerical_characters (input_id, row_num, col_num, content) as (
    select * from characters c
    where c.content in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '0')
  ),
  initial_numerical_characters (input_id, row_num, col_num, content) as (
    select distinct on (nc.input_id, nc.row_num)
    nc.input_id, nc.row_num, nc.col_num, nc.content
    from numerical_characters nc
    order by nc.input_id, nc.row_num, nc.col_num asc
  ),
  final_numerical_characters (input_id, row_num, col_num, content) as (
    select distinct on (nc.input_id, nc.row_num)
    nc.input_id, nc.row_num, nc.col_num, nc.content
    from numerical_characters nc
    order by nc.input_id, nc.row_num, nc.col_num desc
  ),
  extreme_numerical_characters (input_id, row_num, col_num, content) as (
    select * from initial_numerical_characters
    union all
    select * from final_numerical_characters
  ),
  joined_numbers (input_id, row_num, num) as (
    select enc.input_id, enc.row_num, string_agg(enc.content, '' order by enc.col_num asc)::int
    from extreme_numerical_characters enc
    group by enc.input_id, enc.row_num
  )
  select sum(jn.num)
  from joined_numbers jn
  group by jn.input_id;

rollback transaction;
