-- &&1 is object
set echo off
set verify off
set pages 250
set lines 250
set feedback on

col column_name for a30
col owner       for a30
col object_type for a30


select owner, object_name, object_type
from all_objects
where object_name like upper('&&1')
order by owner, object_name;

undefine &&1
