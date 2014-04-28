select hash_value hash,
       executions execs,
       round((elapsed_time/executions)/1000000,0) Elapsed_Time,
       round(buffer_gets/executions, 0) b_to_e,
       round(rows_processed/executions,0) rowsproc,
       module,
       sql_text
from v$sql 
where sql_text not like 'DECLARE%' 
and sql_text not like 'BEGIN%'
--and module='DBMS_SCHEDULER' 
and executions > 100
order by b_to_e desc