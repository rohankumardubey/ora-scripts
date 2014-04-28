create or replace
package housekeep_partitions
as

/*
  This package can be used to maintain the partitions on a date range partitioned table, 
  provided the partitions are named correctly.

  4 Partitioning schemes are supported:

  DAY - partitions are named PDYYYYMMDD, with values less than the following day at midnight.
        ie, PD20110201 has values less than to_date('20110202 00:00:00', 'YYYYMMDD HH24:MI:SS')

  WEEK - partitions are named PWYYYYMMDD.  The start of a week is considered a Sunday
         eg PW20110131 (monday) has values less than midnight of the following monday,
         ie   less than to_date('20110207 00:00:00', 'YYYYMMDD HH24:MI:SS')
         Note that the start of the week is based on oracle NLS_TERRITORY setting. This
         can be set at the client, database or session level. If it is set to UNITED KINGDOM
         Monday is the first day of the week, but if it is set to AMERICA then Sunday is the 
         start of the week. Take care with this when using WEEK partitions.

  MONTH - partitions are named PMYYYYMM.  The start of a month is the first day of the month, 
          and has values less than the first day of the next month at midnight.

  YEAR - partitions are named PYYYYY (note the first Y is the letter 'Y' and not the symbol
         for a date.  Each partition has values less than the first day of the next year.

  Normally there is only 1 procedure that should be executed day to day which is HOUSEKEEP.

  When the housekeep proceedure is executed on a particular day, the code will work out
  what partitioning scheme is used by the table based on existing partition names.  Given the values
  passed as i_future_partitions and i_history_partitions, the relevant partitions will be created
  or dropped accordingly.

  Note that the current partition is never touched or considered.  For example consider the table:

    P-5 P-4 P-3 P-2 P-1 Current P+1

  If housekeep is passed i_hisory_partition = 2 and i_future_partitions = 2, then the resulting table will 
  look like 

    P-2 P-1 Current P+1 P+2

  ie all but the most recent two historical partitions are dropped, and it is ensured that two future 
  partitions exist.  If more than 2 future partitions already exist, then no new ones will be added.
  Similarly, if there are only two historical partitions, then no partitions will be dropped.

  Normally HOUSEKEEP uses the current sysdate to figure out what the current partition is.  Sometimes
  you may want to override this.  One pattern could be an installation script that creates  table
  with a single partition away in the past.  You may then want to create more recent historical 
  and future partitions without having to code up a script for it.  Using the OVERRIDE_DTM procedure
  you can use HOUSEKEEP to create all the partitions for you, eg:

      -- create a table with a single partition for the 1st Jan 2000
      create table PARTITIONED_DAY (c1 date, c2 varchar2(100))
        partition by range (c1) (
          partition PD20000101 values less than (to_date(''20000102 00:00:00'', ''YYYYMMDD HH24:MI:SS''))
      );
      housekeep_partitions.override_dtm(sysdate - 20);
      housekeep_partitions.housekeep('PARTITIONED_DAY', i_history_partitions => 0, i_future_partitions => 20);
      housekeep_partitions.override_dtm(sysdate);

  Doing this will create 20 'future partitions' starting from 20 days ago, as well as one for the current day.
  This means the table will actually have 10 historical partitions, one for the current day and 10 future ones 
  on based on the current sysdate.

  **NOTE** If you use override_dtm, make sure you set it back to sysdate before using the package again in
  the same session.

*/

  /*
    Normal entry point for this package.
      i_table - the table to have partitions added or removed.
      i_history_partitions - number of partitions in the past to *KEEP*. If 10 is passed, but only 5 exist,
                             then nothing will happen.  No new ones will be created.
      i_future_partitions  - number of new partitions to create in the future.  If 10 is passed, and 10 or more
                             exist, nothing will happen.  No future ones will be removed.
      i_tablespace         - Specify the tablespace name to create the new partition in.
      i_storage_parameters - Specify general storage parameters for the partition, like freelists, pctfree etc, eg
                               'freelists 10, pctfree 25%'
      i_compress           - Specify true to enable batch compression on the table.
  */
  procedure housekeep(i_owner              in varchar2,
                      i_table              in varchar2,
                      i_history_partitions in integer,
                      i_future_partitions  in integer,
                      i_tablespace         in varchar2 default null,
                      i_storage_parameters in varchar2 default null,
                      i_compress           in boolean default false);

  /*
    Override what is used as the date when determining the current partition.
    Useful when testing or in table creation scripts (see example above)
  */
  procedure override_dtm(i_dtm in date);


  /*  Helper functions - may be useful and are all readonly */

  /* Given a partitioned table, this procedure will return the 
     scheme it is partitioned in (DAY, WEEK, MONTH, YEAR) or an 
     error if it is not partitioned or no partition in the way
     expected
  */
  function partition_scheme(i_owner in varchar2,
                            i_table in varchar2)
    return varchar2;

  /*
    Given a partition scheme, and a date, the name that would be used
    for that partition is returned.

    valid values for i_partition_scheme are 'DAY', 'WEEK', 'MONTH', 'YEAR'
  */
  function partition_name(i_partition_scheme in varchar2,
                          i_partition_date   in date)
    return varchar2;

  /*
    Given a partition scheme, and a date, partition 'less than' date is return.

    valid values for i_partition_scheme are 'DAY', 'WEEK', 'MONTH', 'YEAR'
  */
  function partition_less_than_date(i_partition_scheme in varchar2,
                                    i_partition_date   in date)
    return date;


  /*
    Given a partition scheme, and a date (which can be mid-week, mid-month,
    mid-year etc, the start_date of the partition for that date will be returned 

    valid values for i_partition_scheme are 'DAY', 'WEEK', 'MONTH', 'YEAR'

    eg i_partition_scheme => 'MONTH'
       i_partition_date   => to_date('20110404', 'YYYYMMDD')
 
       will return to_date('20110401', 'YYYYMMDD')
  */  
  function partition_start_date(i_partition_scheme in varchar2,
                                i_partition_date   in date)
    return date;


end housekeep_partitions;