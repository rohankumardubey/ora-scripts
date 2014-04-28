drop type free_space_row;
drop type free_space_type;
drop function assm_free_space;

CREATE OR REPLACE type free_space_type
as object 
(
   total_blocks        number,
   total_bytes         number,
   unused_blocks       number,
   unused_bytes        number,
   unformatted_blocks  number,
   unformatted_bytes   number,
   fs1_blocks          number,
   fs1_bytes           number,
   fs2_blocks          number,
   fs2_bytes           number,
   fs3_blocks          number,
   fs3_bytes           number,
   fs4_blocks          number,
   fs4_bytes           number,
   full_blocks         number,
   full_bytes          number
);
/

CREATE OR REPLACE type free_space_row is table of free_space_type;
/


CREATE OR REPLACE function assm_free_space(i_segment_owner  in varchar2,
                                           i_segment_name   in varchar2,
                                           i_segment_type   in varchar2,
                                           i_partition_name in varchar2 default null)
  return free_space_row
  pipelined
  
/*

  This procedure can be called from a select using lateral joins, eg
  
  select a.table_name, b.*
  from
  (
    select table_name from user_tables where partitioned = 'NO' 
  ) a, table(assm_free_space(user, a.table_name, 'TABLE'))  b 
  
  The record type defined above is returned  - free_space_type.
  
  Most of the fields are self explanatory.
  
  TOTAL_BYTES     should generally line up with the value from dba_segments.
                  and is the total size of the table.
  UNUSED_BLOCKS   Is generally block above HWM 
            
  FS1_BLOCKS      block that have 0 - 25% free space
  
  FS2_BLOCKS      blocks that have 26 - 50% free space
  
  FS3_BLOCKS      blocks that have 51 - 75% free space
  
  FS4_BLOCKS      blocks that have 76 - 100% free space
  
  FULL_BLOCKS     number of blocks that are considered full up.
  
  In general Total_Blocks != unused + FS1 + FS2 + FS3 + FS4 + FULL because
  block segment header and ASSM blocks are in the table but not included. 
  
  If the table is not in an ASSM tablespace many of the fields will not be returned.                     

*/  
as
  v_unformatted_blocks number;
  v_unformatted_bytes  number;
  v_fs1_blocks         number;
  v_fs1_bytes          number;
  v_fs2_blocks         number;
  v_fs2_bytes          number;
  v_fs3_blocks         number;
  v_fs3_bytes          number;
  v_fs4_blocks         number;
  v_fs4_bytes          number;
  v_full_blocks        number;
  v_full_bytes         number; 
  v_total_blocks       number;
  v_total_bytes        number;
  v_unused_blocks      number;
  v_unused_bytes       number;
  v_last_file_id       number;
  v_last_block_id      number;
  v_last_block         number; 
  v_last_analyzed      date;
  v_num_rows           number;
  v_avg_row_length     number;
--  v_days_since_analyze number;  
--  v_computed_size      number;
begin

--   select last_analyzed, num_rows, avg_row_length
--   into   v_last_analyzed, v_num_rows, v_avg_row_length
--   from   all_tab_statistics 
--   where  owner       = i_segment_owner
--   and    table_name  = i_segment_name
--   and    object_type = i_segment_type
--   and    nvl(partition_name, '**') = (i_partition_name, '**')
-- 
--   -- for MSSM tables, this is the only way to get an idea of
--   -- whether the table is fragmented.
--   v_days_since_analyze := round(sysdate - v_last_analysed);
--   v_computed_size      := v_num_rows * v_avg_row_length;
  
  begin
    DBMS_SPACE.SPACE_USAGE(
     segment_owner       => i_segment_owner,
     segment_name        => i_segment_name,
     segment_type        => i_segment_type,
     unformatted_blocks  => v_unformatted_blocks,
     unformatted_bytes   => v_unformatted_bytes,
     fs1_blocks          => v_fs1_blocks,
     fs1_bytes           => v_fs1_bytes,
     fs2_blocks          => v_fs2_blocks,
     fs2_bytes           => v_fs2_bytes,
     fs3_blocks          => v_fs3_blocks,
     fs3_bytes           => v_fs3_bytes,
     fs4_blocks          => v_fs4_blocks,
     fs4_bytes           => v_fs4_bytes,
     full_blocks         => v_full_blocks,
     full_bytes          => v_full_bytes,
     partition_name      => i_partition_name);
   exception
     when others then
       -- if the object is not ASSM then this procedure will
       -- error.  Better error handling would be better!
       null;
   end;
   
   begin
     DBMS_SPACE.UNUSED_SPACE (
     segment_owner       => i_segment_owner,
     segment_name        => i_segment_name,
     segment_type        => i_segment_type,
     total_blocks        => v_total_blocks,
     total_bytes         => v_total_bytes,
     unused_blocks       => v_unused_blocks,
     unused_bytes        => v_unused_bytes,
     last_used_extent_file_id  => v_last_file_id,
     last_used_extent_block_id => v_last_block_id,
     last_used_block           => v_last_block, 
     partition_name      => i_partition_name);
   exception
     when others then
       -- things like external tables cause this to error.
       -- Ideally should not call procedure with these types of tables
       -- and error is valid ... 
       null;
   end;     

   -- v_total_blocks is the number of blocks allocated to the table
   -- v_unused_blocks are blocks over the HWM.
   
   -- For MSSM tables, there could be lots of free blocks under the HWM
   -- but there is no real way to see that except by using num_rows * avg_length
   -- as reported by DMBS_STATS.
   -- The free space is therefore approximately (total size - (avg_row_length * num_rows)) 
   
   pipe row(free_space_type(
     v_total_blocks,
     v_total_bytes,
     v_unused_blocks,
     v_unused_bytes,
     v_unformatted_blocks,
     v_unformatted_bytes,
     v_fs1_blocks,
     v_fs1_bytes,
     v_fs2_blocks,
     v_fs2_bytes,
     v_fs3_blocks,
     v_fs3_bytes,
     v_fs4_blocks,
     v_fs4_bytes,
     v_full_blocks,
     v_full_bytes
   ));
end assm_free_space;
/
