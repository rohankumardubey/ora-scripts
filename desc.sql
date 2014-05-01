-- &&1 is object

set echo off
set verify off
set pages 250
set lines 250


col data_type for a20
col column_name for a30

accept table prompt 'Table Name: '
accept schema prompt 'Schema: '

PROMPT TABLE INFORMATION

select column_name, data_type, data_length, data_precision, nullable 
from all_tab_columns 
where table_name = upper('&&table')
and owner = nvl(upper('&&schema'), owner);

PROMPT
PROMPT
PROMPT INDEXES

break on index_name skip 2 on report

select a.index_name, decode(b.uniqueness, 'NONUNIQUE', 'N', 'Y') uniq, b.partitioned, a.column_name, a.column_position, b.status
from all_ind_columns a, all_indexes b
where a.table_name = upper('&&table')
and a.table_owner = nvl(upper('&&schema'), a.table_owner)
and a.index_owner = b.owner
and a.index_name  = b.index_name
order by a.index_name asc, a.column_position asc;

set verify on

undefine table
undefine schema
