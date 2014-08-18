set echo off
set verify off

column machine format a30

break on report
compute sum of connection_count on report


select machine, count(*) connection_count
from gv$session
group by machine
order by 2 asc;


set pages 80
set verify on
set echo on