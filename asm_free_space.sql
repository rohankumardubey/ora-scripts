set lines 150

select name, type, total_mb, free_mb, required_mirror_free_mb,
usable_file_mb from v$asm_diskgroup;