create or replace package body ut_housekeep_partitions
as

/*
  TODO 

  * Test storage parameters are correctly passed through

*/

  procedure earliest_partition_details(i_table_name in varchar2,
                                       o_min_partition out varchar2, 
                                       o_high_value    out date);

  procedure max_partition_details(i_table_name in varchar2,
                                  o_max_partition out varchar2, 
                                  o_high_value    out date);


  procedure ut_setup
  is
  begin
    execute immediate 'create table UT_NOT_PARTITIONED (c1 date, c2 varchar2(100))';
    execute immediate 'create table UT_PARTITIONED_INVALID (c1 date, c2 varchar2(100))
                       partition by range (c1) (
                         partition T20000101 values less than (to_date(''20000102 00:00:00'', ''YYYYMMDD HH24:MI:SS''))
                       )';
    execute immediate 'create table UT_PARTITIONED_DAY (c1 date, c2 varchar2(100))
                       partition by range (c1) (
                         partition PD20000101 values less than (to_date(''20000102 00:00:00'', ''YYYYMMDD HH24:MI:SS''))
                       )';
    execute immediate 'create table UT_PARTITIONED_DAY_C (c1 date, c2 varchar2(100))
                       partition by range (c1) (
                         partition PD20000101 values less than (to_date(''20000102 00:00:00'', ''YYYYMMDD HH24:MI:SS''))
                       )';
    execute immediate 'create table UT_PARTITIONED_WEEK (c1 date, c2 varchar2(100))
                       partition by range (c1) (
                         partition PW20000102 values less than (to_date(''20000109 00:00:00'', ''YYYYMMDD HH24:MI:SS''))
                       )';
    execute immediate 'create table UT_PARTITIONED_MONTH (c1 date, c2 varchar2(100))
                       partition by range (c1) (
                         partition PM200001 values less than (to_date(''20000201 00:00:00'', ''YYYYMMDD HH24:MI:SS''))
                       )';
    execute immediate 'create table UT_PARTITIONED_YEAR (c1 date, c2 varchar2(100))
                       partition by range (c1) (
                         partition PY1980 values less than (to_date(''19810101 00:00:00'', ''YYYYMMDD HH24:MI:SS''))
                       )';
  end;

  procedure ut_teardown
  is
  begin  
    execute immediate 'drop table UT_NOT_PARTITIONED';
    execute immediate 'drop table UT_PARTITIONED_INVALID';
    execute immediate 'drop table UT_PARTITIONED_DAY';
    execute immediate 'drop table UT_PARTITIONED_DAY_C';
    execute immediate 'drop table UT_PARTITIONED_WEEK';
    execute immediate 'drop table UT_PARTITIONED_MONTH';
    execute immediate 'drop table UT_PARTITIONED_YEAR';
  exception
    when others then 
      null;
  end ut_teardown;

  procedure ut_partition_start_date
  is
    -- Tuesday 5th April 2011
    v_test_date date := to_date('20110405', 'YYYYMMDD');
  begin
    utassert.eq(
      'Daily partitions start on the specified day',
      housekeep_partitions.partition_start_date('DAY', v_test_date),
      v_test_date
    );

    utassert.eq(
      'Weekly partitions start on the previous sunday',
      housekeep_partitions.partition_start_date('WEEK', v_test_date),
      to_date('20110403', 'YYYYMMDD')
    );

    utassert.eq(
      'Monthly partitions start on the 1st of the month',
      housekeep_partitions.partition_start_date('MONTH', v_test_date),
      to_date('20110401', 'YYYYMMDD')
    );

    utassert.eq(
      'Yearly partitions start on the 1st day of the year',
      housekeep_partitions.partition_start_date('YEAR', v_test_date),
      to_date('20110101', 'YYYYMMDD')
    );
  end;

  procedure ut_partition_less_than_date
  is
    -- Tuesday 5th April 2011
    v_test_date date := to_date('20110405', 'YYYYMMDD');
  begin
    utassert.eq(
      'Daily partitions end at mid-night of the next day',
      housekeep_partitions.partition_less_than_date('DAY', v_test_date),
      to_date('20110406 00:00:00', 'YYYYMMDD HH24:MI:SS')
    );

    utassert.eq(
      'Weekly partitions end at midnight on the next sunday',
      housekeep_partitions.partition_less_than_date('WEEK', v_test_date),
      to_date('20110410 00:00:00', 'YYYYMMDD HH24:MI:SS')
    );

    utassert.eq(
      'Monthly partitions end at midnight on the 1st of the next month',
      housekeep_partitions.partition_less_than_date('MONTH', v_test_date),
      to_date('20110501 00:00:00', 'YYYYMMDD HH24:MI:SS')
    );

    utassert.eq(
      'Yearly partitions end at midnight of the 1st day of the next year',
      housekeep_partitions.partition_less_than_date('YEAR', v_test_date),
      to_date('20120101 00:00:00', 'YYYYMMDD HH24:MI:SS')
    );
  end ut_partition_less_than_date;

  procedure ut_partition_name
  is
    -- Tuesday 5th April 2011
    v_test_date date := to_date('20110405', 'YYYYMMDD');
  begin
    utassert.eq(
      'Daily partitions named with YYYYMMDD',
      housekeep_partitions.partition_name('DAY', v_test_date),
      'PD20110405'
    );

    utassert.eq(
      'Weekly partitions named with correct date and format',
      housekeep_partitions.partition_name('WEEK', v_test_date),
      'PW20110403'
    );

    utassert.eq(
      'Monthly partitions named with correct date and format',
      housekeep_partitions.partition_name('MONTH', v_test_date),
      'PM201104'
    );

    utassert.eq(
      'Yearly partitions named with correct date and format',
      housekeep_partitions.partition_name('YEAR', v_test_date),
      'PY2011'
    );
      
  end ut_partition_name;

  procedure ut_partition_scheme
  is
  begin
    utassert.eq(
      'DAY partitioned table returns correct scheme',
      housekeep_partitions.partition_scheme(user, 'UT_PARTITIONED_DAY'),
      'DAY'
    );

    utassert.eq(
      'WEEK partitioned table returns correct scheme',
      housekeep_partitions.partition_scheme(user, 'UT_PARTITIONED_WEEK'),
      'WEEK'
    );

    utassert.eq(
      'DAY partitioned table returns correct scheme',
      housekeep_partitions.partition_scheme(user, 'UT_PARTITIONED_MONTH'),
      'MONTH'
    );

    utassert.eq(
      'DAY partitioned table returns correct scheme',
      housekeep_partitions.partition_scheme(user, 'UT_PARTITIONED_YEAR'),
      'YEAR'
    );
 
    utAssert.throws(
      'Non partitioned table throws application error',
      'declare 
         v_scheme varchar2(100);
       begin
         v_scheme := housekeep_partitions.partition_scheme('''||user||''', ''UT_PARTITIONED_INVALID'');
       end;',
      -20001
    );

    utAssert.throws(
      'Non partitioned table throws application error',
      'declare 
         v_scheme varchar2(100);
       begin
         v_scheme := housekeep_partitions.partition_scheme('''||user||''', ''UT_NON_PARTITIONED'');
       end;',
      -20002
    );
     

  end ut_partition_scheme;


  procedure ut_housekeep_day
  is 
  begin
    -- need a table with some past and future partitions.  Can use
    -- the housekeep package to set this up by overriding the system
    -- date.
    -- Set it 10 days in the past.
    housekeep_partitions.override_dtm(sysdate - 10);
    -- now keep zero history partitions, and create 20 in the future.
    housekeep_partitions.housekeep(user,
                                   'UT_PARTITIONED_DAY',
                                   i_history_partitions => 0,
                                   i_future_partitions  => 20);
    -- this should leave the 'current partition' (which is for sysdate - 20 days)
    -- and 20 future partitions, ie 21 in total.
    declare 
      v_partition_count integer;
    begin
      select count(*)
      into v_partition_count 
      from user_tab_partitions
      where table_name = 'UT_PARTITIONED_DAY';
      utassert.eq(
        'Ensure all partitions are created',
        v_partition_count,
        21
      );
    end;
    -- reseting the date back to the current dtm should give 10 history partitions
    -- the current one and 10 future ones
    housekeep_partitions.override_dtm(sysdate);
    -- Check the earliest partition is 10 days old and check its max_value
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_DAY', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct',
        v_partition_name,
        'PD'||to_char(sysdate - 10, 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(sysdate - 10 + 1)
      );
    end;
    -- now drop the 5 oldest partitions, but keep the 10 future ones.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_DAY', 5, 10);

    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_DAY', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct',
        v_partition_name,
        'PD'||to_char(sysdate - 5, 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(sysdate - 5 + 1)
      );
    end;
    
    -- running the same command again should yeild no errors.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_DAY', 5, 10);

    -- running with zero history partition should give 'today' as the earliest partition
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_DAY', 0, 10);

    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_DAY', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct',
        v_partition_name,
        'PD'||to_char(sysdate, 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(sysdate + 1)
      );
    end;

    -- previously setup 10 future partitions, so check the details of the max one.
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_DAY', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PD'||to_char(sysdate + 10, 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(sysdate + 10 + 1)
      );
    end;

    -- running with a less number of future days will not remove any existing ones.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_DAY', 5, 5);
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_DAY', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PD'||to_char(sysdate + 10, 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(sysdate + 10 + 1)
      );
    end;

   -- running with more future partitions will add more to the end.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_DAY', 5, 12);
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_DAY', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PD'||to_char(sysdate + 12, 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(sysdate + 12 + 1)
      );
    end;

  end ut_housekeep_day;

  procedure ut_housekeep_week
  is
  begin
   -- need a table with some past and future partitions.  Can use
    -- the housekeep package to set this up by overriding the system
    -- date.
    -- Set it 10 weeks in the past.
    housekeep_partitions.override_dtm(trunc(sysdate - 10*7, 'DY'));
    -- now keep zero history partitions, and create 20 in the future.
    housekeep_partitions.housekeep(user,
                                   'UT_PARTITIONED_WEEK',
                                   i_history_partitions => 0,
                                   i_future_partitions  => 20);
    -- this should leave the 'current partition' (which is for sysdate - 10 weeks)
    -- and 20 future partitions, ie 21 in total.
    declare 
      v_partition_count integer;
    begin
      select count(*)
      into v_partition_count 
      from user_tab_partitions
      where table_name = 'UT_PARTITIONED_WEEK';
      utassert.eq(
        'Ensure all weekly partitions are created',
        v_partition_count,
        21
      );
    end;
    -- reseting the date back to the current dtm should give 10 history partitions
    -- the current one and 10 future ones
    housekeep_partitions.override_dtm(sysdate);
    -- Check the earliest partition is 10 days old and check its max_value
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_WEEK', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct',
        v_partition_name,
        'PW'||to_char(trunc(sysdate - 10*7, 'DY'), 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(sysdate - (10 - 1)*7, 'DY')
      );
    end;
    -- now drop the 5 oldest partitions, but keep the 10 future ones.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_WEEK', 5, 10);

    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_WEEK', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct',
        v_partition_name,
        'PW'||to_char(trunc(sysdate - (5*7), 'DY'), 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(sysdate - 4*7, 'DY')
      );
    end;
    
    -- running the same command again should yeild no errors.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_WEEK', 5, 10);

    -- running with zero history partition should give 'today' as the earliest partition
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_WEEK', 0, 10);

    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_WEEK', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct for zero history',
        v_partition_name,
        'PW'||to_char(trunc(sysdate, 'DY'), 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(sysdate + 7, 'DY')
      );
    end;

    -- previously setup 10 future partitions, so check the details of the max one.
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_WEEK', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PW'||to_char(trunc(sysdate + 7*10, 'DY'), 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(sysdate + (10 + 1)*7, 'DY')
      );
    end;

    -- running with a less number of future days will not remove any existing ones.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_WEEK', 5, 5);
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_WEEK', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PW'||to_char(trunc(sysdate + 10*7, 'DY'), 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(sysdate + (10 + 1)*7, 'DY')
      );
    end;

   -- running with more future partitions will add more to the end.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_WEEK', 5, 12);
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_WEEK', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PW'||to_char(trunc(sysdate + 12*7, 'DY'), 'YYYYMMDD')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(sysdate + (12 + 1)*7, 'DY')
      );
    end;
  end ut_housekeep_week;

  procedure ut_housekeep_month
  is
  begin
    -- need a table with some past and future partitions.  Can use
    -- the housekeep package to set this up by overriding the system
    -- date.
    -- Set it 10 weeks in the past.
    housekeep_partitions.override_dtm(trunc(add_months(sysdate, - 10), 'MON'));
    -- now keep zero history partitions, and create 20 in the future.
    housekeep_partitions.housekeep(user, 
                                   'UT_PARTITIONED_MONTH',
                                   i_history_partitions => 0,
                                   i_future_partitions  => 20);
    -- this should leave the 'current partition' (which is for sysdate - 10 weeks)
    -- and 20 future partitions, ie 21 in total.
    declare 
      v_partition_count integer;
    begin
      select count(*)
      into v_partition_count 
      from user_tab_partitions
      where table_name = 'UT_PARTITIONED_MONTH';
      utassert.eq(
        'Ensure all monthly partitions are created',
        v_partition_count,
        21
      );
    end;
    -- reseting the date back to the current dtm should give 10 history partitions
    -- the current one and 10 future ones
    housekeep_partitions.override_dtm(sysdate);
    -- Check the earliest partition is 10 months old and check its max_value
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_MONTH', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct for monthly',
        v_partition_name,
        'PM'||to_char(trunc(add_months(sysdate, -10), 'MON'), 'YYYYMM')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, -9), 'MON')
      );
    end;
    -- now drop the 5 oldest partitions, but keep the 10 future ones.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_MONTH', 5, 10);

    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_MONTH', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct',
        v_partition_name,
        'PM'||to_char(trunc(add_months(sysdate, -5), 'MON'), 'YYYYMM')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, -4), 'MON')
      );
    end;
    
    -- running the same command again should yeild no errors.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_MONTH', 5, 10);

    -- running with zero history partition should give 'today' as the earliest partition
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_MONTH', 0, 10);

    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_MONTH', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct for zero history',
        v_partition_name,
        'PM'||to_char(trunc(sysdate, 'MON'), 'YYYYMM')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, 1), 'MON')
      );
    end;

    -- previously setup 10 future partitions, so check the details of the max one.
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_MONTH', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PM'||to_char(trunc(add_months(sysdate, 10), 'MON'), 'YYYYMM')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, 11), 'MON')
      );
    end;

    -- running with a less number of future days will not remove any existing ones.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_MONTH', 5, 5);
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_MONTH', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PM'||to_char(trunc(add_months(sysdate, 10), 'MON'), 'YYYYMM')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, 11), 'MON')
      );
    end;

   -- running with more future partitions will add more to the end.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_MONTH', 5, 12);
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_MONTH', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PM'||to_char(trunc(add_months(sysdate, 12), 'MON'), 'YYYYMM')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, 13), 'MON')
      );
    end;
  end ut_housekeep_month;


  procedure ut_housekeep_year
  is
  begin
    -- need a table with some past and future partitions.  Can use
    -- the housekeep package to set this up by overriding the system
    -- date.
    -- Set it 10 years in the past.
    housekeep_partitions.override_dtm(trunc(add_months(sysdate, - 10*12), 'MON'));
    -- now keep zero history partitions, and create 20 in the future.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_YEAR',
                                   i_history_partitions => 0,
                                   i_future_partitions  => 20);
    -- this should leave the 'current partition' (which is for sysdate - 10 weeks)
    -- and 20 future partitions, ie 21 in total.
    declare 
      v_partition_count integer;
    begin
      select count(*)
      into v_partition_count 
      from user_tab_partitions
      where table_name = 'UT_PARTITIONED_YEAR';
      utassert.eq(
        'Ensure all yearly partitions are created',
        v_partition_count,
        21
      );
    end;
    -- reseting the date back to the current dtm should give 10 history partitions
    -- the current one and 10 future ones
    housekeep_partitions.override_dtm(sysdate);
    -- Check the earliest partition is 10 years old and check its max_value
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_YEAR', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct for yearly',
        v_partition_name,
        'PY'||to_char(trunc(add_months(sysdate, -10*12), 'YEAR'), 'YYYY')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, -9*12), 'YEAR')
      );
    end;
    -- now drop the 5 oldest partitions, but keep the 10 future ones.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_YEAR', 5, 10);

    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_YEAR', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct',
        v_partition_name,
        'PY'||to_char(trunc(add_months(sysdate, -5*12), 'YEAR'), 'YYYY')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, -4*12), 'YEAR')
      );
    end;
    
    -- running the same command again should yeild no errors.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_YEAR', 5, 10);

    -- running with zero history partition should give 'today' as the earliest partition
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_YEAR', 0, 10);

    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      earliest_partition_details('UT_PARTITIONED_YEAR', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure min partition name is correct for zero history',
        v_partition_name,
        'PY'||to_char(trunc(sysdate, 'YEAR'), 'YYYY')
      );

      utassert.eq(
        'Ensure min partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, 1*12), 'YEAR')
      );
    end;

    -- previously setup 10 future partitions, so check the details of the max one.
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_YEAR', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PY'||to_char(trunc(add_months(sysdate, 10*12), 'YEAR'), 'YYYY')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, 11*12), 'YEAR')
      );
    end;

    -- running with a less number of future days will not remove any existing ones.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_YEAR', 5, 5);
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_YEAR', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PY'||to_char(trunc(add_months(sysdate, 10*12), 'YEAR'), 'YYYY')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, 11*12), 'YEAR')
      );
    end;

   -- running with more future partitions will add more to the end.
    housekeep_partitions.housekeep(user, 'UT_PARTITIONED_YEAR', 5, 12);
    declare
      v_max_date date;
      v_partition_name varchar2(255);
    begin
      max_partition_details('UT_PARTITIONED_YEAR', v_partition_name, v_max_date);

      utassert.eq(
        'Ensure max partition name is correct',
        v_partition_name,
        'PY'||to_char(trunc(add_months(sysdate, 12*12), 'YEAR'), 'YYYY')
      );

      utassert.eq(
        'Ensure max partition high value is correct',
        v_max_date,
        trunc(add_months(sysdate, 13*12), 'YEAR')
      );
    end;
  end ut_housekeep_year;

  procedure ut_housekeep_parameters
  is
  begin
    utAssert.throws(
      'History partitions less than zero raises error',
      'begin
         housekeep_partitions.housekeep('''||user||''', ''UT_PARTITIONED_DAY'', -1, 5);
       end;',
      -20001
    );

    utAssert.throws(
      'Future partitions less than zero raises error',
      'begin
         housekeep_partitions.housekeep('''||user||''', ''UT_PARTITIONED_DAY'', 1, -5);
       end;',
      -20001
    );

    utAssert.throws(
      'Non existent table raises error',
      'begin
         housekeep_partitions.housekeep('''||user||''', ''UT_PARTITIONED_NOT_EXIST'', 1, 5);
       end;',
      -20001
    );

    
    -- Ensure that the compress option is passed through to the partition 
    declare
      v_compress varchar2(255);
    begin
      housekeep_partitions.housekeep(user, 'UT_PARTITIONED_DAY_C', 0, 1, i_compress => true);
      select compression
      into v_compress
      from user_tab_partitions
      where table_name = 'UT_PARTITIONED_DAY_C'
      and   rownum     = 1;
      utassert.eq(
        'Ensure compress option is passed to the partition',
        v_compress,
        'ENABLED'
      );
    end;
    
    -- Ensure the tablespace clause is passed through correct.
    declare
      v_tablespace varchar2(255);
    begin
      housekeep_partitions.housekeep(user, 'UT_PARTITIONED_DAY_C', 0, 2, i_tablespace => 'INVENTORY_TS_01');
      select tablespace_name
      into v_tablespace
      from user_tab_partitions
      where table_name      = 'UT_PARTITIONED_DAY_C'
      and   tablespace_name = 'INVENTORY_TS_01'
      and   rownum          = 1;
      utassert.eq(
        'Ensure compress option is passed to the partition',
        v_tablespace,
        'INVENTORY_TS_01'
      );
    end;


  end ut_housekeep_parameters;

  /**********************************************
     Helper functions below here.

  **********************************************/


  procedure earliest_partition_details(i_table_name in varchar2,
                                       o_min_partition out varchar2, 
                                       o_high_value    out date)
  is
    v_max_value varchar2(255);
  begin
    select partition_name, high_value
    into   o_min_partition, v_max_value
    from
    (
      select partition_name, high_value
      from user_tab_partitions
      where table_name = upper(i_table_name)
      order by partition_name asc
    ) r where rownum = 1;

    execute immediate 'begin :date := '||v_max_value||'; end;' using out o_high_value;
  end earliest_partition_details;

  procedure max_partition_details(i_table_name in varchar2,
                                  o_max_partition out varchar2, 
                                  o_high_value    out date)
  is
    v_max_value varchar2(255);
  begin
    select partition_name, high_value
    into   o_max_partition, v_max_value
    from
    (
      select partition_name, high_value
      from user_tab_partitions
      where table_name = upper(i_table_name)
      order by partition_name desc
    ) r where rownum = 1;

    execute immediate 'begin :date := '||v_max_value||'; end;' using out o_high_value;
  end max_partition_details;


end;
/
show error