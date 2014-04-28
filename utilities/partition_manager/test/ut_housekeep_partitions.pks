create or replace package ut_housekeep_partitions
as

  procedure ut_setup;
  procedure ut_teardown;
  procedure ut_partition_start_date;
  procedure ut_partition_less_than_date;
  procedure ut_partition_name;
  procedure ut_partition_scheme;
  procedure ut_housekeep_day;
  procedure ut_housekeep_week;
  procedure ut_housekeep_month;
  procedure ut_housekeep_year;
  procedure ut_housekeep_parameters;


end ut_housekeep_partitions;
/
show erro