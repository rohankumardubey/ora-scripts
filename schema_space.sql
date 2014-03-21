
set echo off
set verify off

compute sum label 'Total Size(MB)' of sizeMB on report
compute sum label 'Total Size(MB)' of sizeMB on table_name

break on table_name skip 1 on report
set pages 5000

col objtype for a10
col table_name for a30
col partition_name for a25
col lob_column_name for a20
col segment_name for a30
col sizemb for 99999990.99

PROMPT Database information
PROMPT **********************
select instance_name, host_name, status, sysdate timenow from v$instance;

PROMPT
PROMPT
PROMPT Segment report for &&1 schema
PROMPT *******************************


--	
-- TABLES - non-partitoned, permanent
--
select 	'TABLE' objtype,
	segment_name table_name,
	'' partition_name,
	'' lob_column_name,
	s.segment_name,
	round(s.bytes/1024/1024,2) sizeMB
from 	dba_segments s
where 	owner = upper('&&1')
and	s.segment_name not like 'BIN$%'
and	segment_type = 'TABLE'
union all
--
-- PARTITIONED TABLES - PARTITIONS
--
select 	'TABLE PART' objtype,
	s.segment_name table_name,
	s.partition_name partition_name,
	'' lob_column_name,
	s.segment_name,
	round(s.bytes/1024/1024,2) sizeMB
from 	dba_segments s
where	s.segment_type = 'TABLE PARTITION'
and 	s.owner = upper('&1')
union all
--
-- PARTITIONED TABLES - INDEXES
--
select 	'  INDX_PRT' objtype,
	i.table_name table_name,
	ip.partition_name,
	'' lob_column_name,
	s.segment_name,
	round(s.bytes/1024/1024,2) sizeMB
from	dba_indexes i, dba_ind_partitions ip, dba_segments s
where	( i.owner = ip.index_owner and i.index_name = ip.index_name )
and	( ip.partition_name  = s.partition_name and ip.index_owner = s.owner)
and	( i.owner = s.owner and i.index_name = s.segment_name )
and	i.partitioned = 'YES'
and 	s.segment_type = 'INDEX PARTITION'
and	s.owner = upper('&&1')
union all
--
-- INDEXES
--
select 	'  INDX' objtype,
	i.table_name,
	'' partition_name,
	'' lob_column_name,
	s.segment_name,
	round(s.bytes/1024/1024,2) sizeMB
from dba_segments s, dba_indexes i
where s.owner = upper('&&1')
and s.segment_name not like 'BIN$%'
and s.segment_type = 'INDEX'
and s.segment_name = i.index_name
union all
--
-- LOBS
--
select 	'  LOB' objtype,
	l.table_name,
	'' partition_name,
	l.column_name lob_column_name,
	s.segment_name,
	round(s.bytes/1024/1024,2) sizeMB
from dba_segments s, dba_lobs l
where s.owner = upper('&&1')
and s.segment_name not like 'BIN$%'
and l.table_name not like 'BIN$%'
and s.segment_type = 'LOBSEGMENT'
and s.segment_name = l.segment_name
union all
--
-- LOBS - PARTITIONS
--
select 	'  LOB_PRT' objtype,
	l.table_name,
	s.partition_name partition_name,
	l.column_name lob_column_name,
	s.segment_name,
	round(s.bytes/1024/1024,2) sizeMB
from dba_segments s, dba_lobs l
where s.owner = upper('&&1')
and s.segment_name not like 'BIN$%'
and l.table_name not like 'BIN$%'
and s.segment_type like '%PARTITION%'
and s.segment_name = l.segment_name
order by 2, 1 desc, 3;

undefine 1

cl columns
cl computes

set pages 80
set verify on
