begin
  for row in (select username from dba_users
              where username not in ('SYS', 'SYSTEM', 'ORACLE_OCM', 'OUTLN', 'DIP', 'DBSNMP', 'APPQOSSYS', 'BDNA_SCAN_DB')) loop
    dbms_utility.compile_schema(row.username, false, true);
  end loop;
end;
/
