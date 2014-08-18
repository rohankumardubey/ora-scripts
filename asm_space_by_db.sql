column database format a20

SELECT SUBSTR(alias_path,2,INSTR(alias_path,'/',1,2)-2) Database,
       ROUND(SUM(alloc_bytes)/1024/1024/1024,1) "GB"
FROM
(
   SELECT SYS_CONNECT_BY_PATH(alias_name, '/') alias_path,
   alloc_bytes
   FROM
  (
     SELECT g.name disk_group_name, 
            a.parent_index pindex, 
            a.name alias_name, 
            a.reference_index rindex, 
            f.space alloc_bytes, 
            f.type type
     FROM v$asm_file f 
       RIGHT OUTER JOIN v$asm_alias a USING (group_number, file_number)
       JOIN v$asm_diskgroup g USING (group_number)
  )
  WHERE type IS NOT NULL
  START WITH (MOD(pindex, POWER(2, 24))) = 0
  CONNECT BY PRIOR rindex = pindex
)
GROUP BY SUBSTR(alias_path,2,INSTR(alias_path,'/',1,2)-2)
ORDER BY 2 asc;