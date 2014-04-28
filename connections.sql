set echo off
set verify off

break on report
compute sum of connection_count on report


select machine, count(*) connection_count
from v$session
group by machine;


set pages 80
set verify on
set echo on