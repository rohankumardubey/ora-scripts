-- This query will find all indexes that have the same leading columns
-- and are hence largely redundant.  It is not perfect, and will identify
-- a few indexes that are not duplicates in some cases. The report will need 
-- careful inspection before dropping any indexes.

select a.index_owner, a.table_name, a.index_name || '(' || a.cols || ')' cols,
           b.index_name || '(' || b.cols || ')' cols
      from (select index_owner, index_name,  table_name,
                   rtrim(
                         max(decode(column_position,1,column_name,null)) ||','||
                     max(decode(column_position,2,column_name,null)) ||','||
                     max(decode(column_position,3,column_name,null)) ||','||
                     max(decode(column_position,4,column_name,null)) ||','||
                     max(decode(column_position,5,column_name,null)) ||','||
                    max(decode(column_position,6,column_name,null)) ||','||
                    max(decode(column_position,7,column_name,null)) ||','||
                    max(decode(column_position,8,column_name,null)) ||','||
                    max(decode(column_position,9,column_name,null)) ||','||
                    max(decode(column_position,10,column_name,null)) , ',' ) cols
           from all_ind_columns
           where index_owner not in ('SYS', 'SYSTEM', 'WMSYS')
           and table_name = 'CONSUMER_DETAIL'
          group by index_owner, table_name, index_name ) a,
          (select index_owner, index_name,  table_name,
                  rtrim(
                        max(decode(column_position,1,column_name,null)) ||','||
                    max(decode(column_position,2,column_name,null)) ||','||
                    max(decode(column_position,3,column_name,null)) ||','||
                    max(decode(column_position,4,column_name,null)) ||','||
                    max(decode(column_position,5,column_name,null)) ||','||
                    max(decode(column_position,6,column_name,null)) ||','||
                    max(decode(column_position,7,column_name,null)) ||','||
                    max(decode(column_position,8,column_name,null)) ||','||
                    max(decode(column_position,9,column_name,null)) ||','||
                    max(decode(column_position,10,column_name,null)) , ',' ) cols
           from all_ind_columns
          where index_owner not in ('SYS', 'SYSTEM', 'WMSYS')
          group by index_owner, table_name, index_name ) b
    where a.table_name = b.table_name
      and a.index_owner = b.index_owner
      and a.index_name <> b.index_name
      and a.cols like b.cols || '%';