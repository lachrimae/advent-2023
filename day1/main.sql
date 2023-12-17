set schema 'day1';

begin transaction;

  create temporary table numerical_characters as
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
  )
  select * from characters c
  where c.content in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '0');

  with
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
    select
    enc.input_id,
    enc.row_num,
    string_agg(enc.content, '' order by enc.col_num asc)::int
    from extreme_numerical_characters enc
    group by enc.input_id, enc.row_num
  )
  select sum(jn.num)
  from joined_numbers jn
  group by jn.input_id;

  create temporary table number_words (n, w) as
    values
    (1, 'one'),
    (2, 'two'),
    (3, 'three'),
    (4, 'four'),
    (5, 'five'),
    (6, 'six'),
    (7, 'seven'),
    (8, 'eight'),
    (9, 'nine'),
    (0, 'zero')
  ;

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
  extant_number_words (input_id, row_num, col_num, content) as (
    select c.input_id, c.row_num, c.col_num, nw.n
    from characters c
    join lines l on l.input_id = c.input_id and l.row_num = c.row_num
    inner join number_words nw on substring(l.content, c.col_num::int, length(nw.w)) = nw.w
  ),
  initial_numbers (input_id, row_num, col_num, content) as (
    select distinct on (t.input_id, t.row_num) * from (
      select * from extant_number_words
      union
      select nc.input_id, nc.row_num, nc.col_num, nc.content::int from numerical_characters nc
    ) t
    order by t.input_id, t.row_num, t.col_num asc
  ),
  terminal_numbers (input_id, row_num, col_num, content) as (
    select distinct on (t.input_id, t.row_num) * from (
      select * from extant_number_words
      union
      select nc.input_id, nc.row_num, nc.col_num, nc.content::int from numerical_characters nc
    ) t
    order by t.input_id, t.row_num, t.col_num desc
  ),
  extreme_numbers (input_id, row_num, col_num, content) as (
    select * from initial_numbers
    union all
    select * from terminal_numbers
  ),
  joined_numbers (input_id, row_num, num) as (
    select
    enc.input_id,
    enc.row_num,
    string_agg(enc.content::text, '' order by enc.col_num asc)::int
    from extreme_numbers enc
    group by enc.input_id, enc.row_num
  )
  select sum(jn.num)
  from joined_numbers jn
  group by jn.input_id;

commit;
