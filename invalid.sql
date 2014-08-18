set lines 250

select owner, object_type, object_name
from all_objects
where status = 'INVALID'
and owner not in ('SYS', 'SYSTEM', 'ORACLE_OCM', 'OUTLN', 'DIP', 'DBSNMP', 'APPQOSSYS', 'BDNA_SCAN_DB')
order by owner, object_name;