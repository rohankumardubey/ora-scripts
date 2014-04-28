create or replace
package body housekeep_partitions
as

/*

  TODO - 

  * Add 11G compression syntax to enable OLAP
  * Allow the start of the week to be an other day than sunday
  * Add Quaterly partitions
  * Consider adding hourly partitions

*/

g_override_dtm date default null;

function storage_clause(i_storage_clause in varchar2 default null)
  return varchar2;
function partition_range_clause(i_less_than_dtm in date)
  return varchar2;
function tablespace_clause(i_tablespace in varchar2 default null)
  return varchar2;
function compress_clause(i_compress in boolean)
  return varchar2;
function increment_partition_dtm(i_partition_scheme in varchar2,
                                 i_starting_dtm     in date,
                                 i_step             in integer default 1)
  return date;


procedure drop_partitions(i_owner              in varchar2,
                          i_table              in varchar2,
                          i_partition_scheme   in varchar2,
                          i_history_partitions in integer,
                          i_starting_dtm       in date);

procedure add_partitions(i_owner              in varchar2,
                         i_table              in varchar2,
                         i_partition_scheme   in varchar2,
                         i_future_partitions  in integer,
                         i_tablespace         in varchar2,
                         i_storage_parameters in varchar2,
                         i_compress           in boolean,
                         i_starting_dtm       in date);

procedure create_partition(i_owner              in varchar2,
                           i_table         in varchar2,
                           i_partition_scheme in varchar2,
                           i_tablespace    in varchar2,
                           i_storage_parameters in varchar2,
                           i_compress      in boolean default false,
                           i_partition_dtm in date);

procedure override_dtm(i_dtm in date)
is
begin
  g_override_dtm := i_dtm;
end override_dtm;


procedure housekeep(i_owner              in varchar2,
                    i_table              in varchar2,
                    i_history_partitions in integer,
                    i_future_partitions  in integer,
                    i_tablespace         in varchar2 default null,
                    i_storage_parameters in varchar2 default null,
                    i_compress           in boolean  default false)
is
  v_current_date date := sysdate;
  v_partition_scheme  varchar2(10);
begin
  if g_override_dtm is not null then
    v_current_date := g_override_dtm;
  end if;

  if i_history_partitions < 0 then
    raise_application_error(-20001, 'i_history_partitions must be greater or equal to zero');
  end if;
  if i_future_partitions < 0 then
    raise_application_error(-20001, 'i_future_partitions must be greater or equal to zero');
  end if;

  declare
    v_table_name varchar2(255);
  begin
    select table_name
    into   v_table_name
    from   all_tables
    where  table_name = upper(i_table)
    and    owner      = upper(i_owner);
  exception
    when no_data_found then
      raise_application_error(-20001, i_owner||'.'||i_table||' does not exist');
  end;

  v_partition_scheme := partition_scheme(upper(i_owner), upper(i_table));
  add_partitions(i_owner, upper(i_table), v_partition_scheme, i_future_partitions, i_tablespace, i_storage_parameters, i_compress, v_current_date);
  drop_partitions(i_owner, upper(i_table), v_partition_scheme, i_history_partitions, v_current_date);
end housekeep;


procedure drop_partitions(i_owner              in varchar2,
                          i_table              in varchar2,
                          i_partition_scheme   in varchar2,
                          i_history_partitions in integer,
                          i_starting_dtm       in date)
is
  v_keep_on_or_after_date  date;
  v_max_partition_name     varchar2(255);
  v_current_partition_date date;
begin
  if i_history_partitions < 0 then
    raise_application_error(-20001, 'History Days must be great than zero');
  end if;

  v_current_partition_date   := partition_start_date(i_partition_scheme, i_starting_dtm);
  v_keep_on_or_after_date    := increment_partition_dtm(i_partition_scheme, v_current_partition_date, -1*i_history_partitions);
  v_max_partition_name       := partition_name(i_partition_scheme, v_keep_on_or_after_date);
  for row in (select partition_name
              from  all_tab_partitions
              where table_name       = upper(i_table)
              and   table_owner      = upper(i_owner)
              and   partition_name   < v_max_partition_name) loop
    execute immediate 'lock table '||i_owner||'.'||i_table||' in exclusive mode';
    execute immediate 'alter table '||i_owner||'.'||i_table||' drop partition '||row.partition_name||' UPDATE GLOBAL INDEXES';
  end loop;
end drop_partitions;


procedure add_partitions(i_owner in varchar2,
                         i_table in varchar2,
                         i_partition_scheme   in varchar2,
                         i_future_partitions  in integer,
                         i_tablespace         in varchar2,
                         i_storage_parameters in varchar2,
                         i_compress           in boolean,
                         i_starting_dtm       in date)
is
  v_current_partition_dtm date;
begin
  if i_future_partitions < 0 then
    raise_application_error(-20001, 'FUTURE_DAYS must be greater or equal to zero');
  end if;
  
  -- ensure that the starting_dtm lines up on a partition boundry (important for
  -- year, week and month
  v_current_partition_dtm := partition_start_date(i_partition_scheme, i_starting_dtm);
  
  -- now we need to create the 'current partition' and then X future ones.
  -- So, increment the partition date in a loop for the number of future partitions.
  for i in 0 .. i_future_partitions loop
    declare
      v_selected_partition_name varchar2(4000);
      v_partition_name varchar2(4000);
    begin
      v_partition_name := partition_name(i_partition_scheme,v_current_partition_dtm);
      select partition_name
      into  v_selected_partition_name
      from  all_tab_partitions
      where table_name       = upper(i_table)
      and   table_owner      = upper(i_owner)
      and   partition_name   = v_partition_name;
    exception
      when no_data_found then
        -- the partition doesn't exist, so create it.
        create_partition(i_owner, i_table, i_partition_scheme, i_tablespace, i_storage_parameters, i_compress, v_current_partition_dtm);
    end;
    v_current_partition_dtm := increment_partition_dtm(i_partition_scheme, v_current_partition_dtm);
  end loop;
end add_partitions;


procedure create_partition(i_owner               in varchar2,
                           i_table               in varchar2,
                           i_partition_scheme    in varchar2,
                           i_tablespace          in varchar2,
                           i_storage_parameters  in varchar2,
                           i_compress            in boolean default false,
                           i_partition_dtm       in date)
is
  v_statement varchar2(32767);
begin
  v_statement := 'alter table '||i_owner||'.'||i_table||' add partition '||partition_name(i_partition_scheme, i_partition_dtm)
    ||' values less than '||partition_range_clause(partition_less_than_date(i_partition_scheme, i_partition_dtm))
    ||' '||tablespace_clause(i_tablespace)
    ||' '||storage_clause(i_storage_parameters)
    ||' '||compress_clause(i_compress);
  execute immediate 'lock table '||i_owner||'.'||i_table||' in exclusive mode';
  execute immediate v_statement;
end create_partition;


function increment_partition_dtm(i_partition_scheme in varchar2,
                                 i_starting_dtm     in date,
                                 i_step             in integer default 1)
return date
is
  v_starting_dtm date := partition_start_date(i_partition_scheme, i_starting_dtm);
begin
  if i_partition_scheme = 'DAY' then
    return trunc(v_starting_dtm + 1*i_step);
  elsif i_partition_scheme = 'WEEK' then
    return trunc(v_starting_dtm + 7*i_step, 'DY');
  elsif i_partition_scheme = 'MONTH' then
    return trunc(add_months(v_starting_dtm, 1*i_step), 'MONTH');
  elsif i_partition_scheme = 'YEAR' then
    return trunc(add_months(v_starting_dtm, 12*i_step), 'YEAR');
  else
    raise_application_error(-20001, i_partition_scheme||' is not a valid partitioning scheme');
  end if;
end increment_partition_dtm;


function storage_clause(i_storage_clause in varchar2 default null)
  return varchar2
is
begin
  if i_storage_clause != '' or i_storage_clause is not null then
    return ' storage('||i_storage_clause||') ';
  else
    return '';
  end if;
end storage_clause;


function tablespace_clause(i_tablespace in varchar2 default null)
  return varchar2
is
begin
  if i_tablespace != '' or i_tablespace is not null then
    return ' tablespace '||i_tablespace||' ';
  else
    return '';
  end if;
end tablespace_clause;


function compress_clause(i_compress in boolean)
  return varchar2
is
begin
  if i_compress then
    return ' compress ';
  else
    return ' ';
  end if;
end compress_clause;


function partition_range_clause(i_less_than_dtm in date)
  return varchar2
is
  v_clause varchar2(4000) := '';
begin
  v_clause := v_clause||'(TO_DATE('''||to_char(i_less_than_dtm, 'YYYYMMDD')||' 00:00:00'', ''YYYYMMDD HH24:MI:SS''))';
  return v_clause;
end partition_range_clause;


function partition_scheme(i_owner in varchar2,
                          i_table in varchar2)
  return varchar2
is
  v_partition_name varchar2(255);
begin
  begin
    select partition_name
    into v_partition_name
    from all_tab_partitions
    where table_name = upper(i_table)
    and   table_owner      = upper(i_owner)
    and   rownum           = 1;
  exception
    when no_data_found then
      raise_application_error(-20002, 'The table is not partitioned');
  end;

  if instr(v_partition_name, 'PY',1,1) > 0 then
    return 'YEAR';
  elsif instr(v_partition_name, 'PD',1,1) > 0 then
    return 'DAY';
  elsif instr(v_partition_name, 'PW',1,1) > 0 then
    return 'WEEK';
  elsif instr(v_partition_name, 'PM',1,1) > 0 then
    return 'MONTH';
  else
    raise_application_error(-20001, 'The table partitions are not named correctly');
  end if;
end partition_scheme;


function partition_name(i_partition_scheme in varchar2,
                        i_partition_date   in date)
return varchar2
is
begin
  if i_partition_scheme = 'DAY' then
    return 'PD'||to_char(partition_start_date(i_partition_scheme, i_partition_date),'YYYYMMDD');
  elsif i_partition_scheme = 'WEEK' then
    return 'PW'||to_char(partition_start_date(i_partition_scheme, i_partition_date),'YYYYMMDD');
  elsif i_partition_scheme = 'MONTH' then
    return 'PM'||to_char(partition_start_date(i_partition_scheme, i_partition_date),'YYYYMM');
  elsif i_partition_scheme = 'YEAR' then
    return 'PY'||to_char(partition_start_date(i_partition_scheme, i_partition_date),'YYYY');
  else
    raise_application_error(-20001, i_partition_scheme||' is not a valid partitioning scheme');
  end if;
end partition_name;


function partition_less_than_date(i_partition_scheme in varchar2,
                                  i_partition_date   in date)
return date
is
begin
  if i_partition_scheme = 'DAY' then
    return trunc(i_partition_date + 1);
  elsif i_partition_scheme = 'WEEK' then
    return trunc(i_partition_date + 7, 'DY');
  elsif i_partition_scheme = 'MONTH' then
    return trunc(add_months(i_partition_date, 1), 'MONTH');
  elsif i_partition_scheme = 'YEAR' then
    return trunc(add_months(i_partition_date, 12), 'YEAR');
  else
    raise_application_error(-20001, i_partition_scheme||' is not a valid partitioning scheme');
  end if;
end partition_less_than_date;


function partition_start_date(i_partition_scheme in varchar2,
                              i_partition_date   in date)
return date
is
begin
  if i_partition_scheme = 'DAY' then
    return trunc(i_partition_date);
  elsif i_partition_scheme = 'WEEK' then
    return trunc(i_partition_date, 'DY');
  elsif i_partition_scheme = 'MONTH' then
    return trunc(i_partition_date, 'MONTH');
  elsif i_partition_scheme = 'YEAR' then
    return trunc(i_partition_date, 'YEAR');
  else
    raise_application_error(-20001, i_partition_scheme||' is not a valid partitioning scheme');
  end if;
end partition_start_date;


end housekeep_partitions;
/
show err
