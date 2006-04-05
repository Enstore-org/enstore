create table volatile_files (
        pnfsid          bytea     primary key, 
	date            timestamp without time zone not null,
	unix_date       int       not null,
        pnfsid_string   varchar   not null,	
        pnfs_path       varchar   not null,
	layer1          char(1)           ,
	layer2          char(1)           ,
        layer4          char(1)           
);

create index vf_date_idx        on volatile_files (date);
create index vf_unix_date_idx   on volatile_files (unix_date);
create index vf_pnfs_id_string  on volatile_files (pnfsid_string);
create index vf_pnfs_path_idx   on volatile_files (pnfs_path);




