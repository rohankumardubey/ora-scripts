set lines 20000
set pages 50000
set head off
set feedback off
set verify off
set tab off
set trimout on
set trimspool on


select text
from all_source
where owner = upper('&&1')
and name = upper('&&2')
order by line asc;

set head on
set feedback on
set verify on
set trimout off
set tab on
set trimspool off
set lines 250
set pages 500