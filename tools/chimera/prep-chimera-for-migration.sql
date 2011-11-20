ALTER TABLE t_dirs               ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_dirs               ALTER COLUMN iparent TYPE varchar(36);
ALTER TABLE t_inodes             ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_inodes_checksum    ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_inodes_data        ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_level_1            ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_level_2            ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_level_3            ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_level_4            ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_level_5            ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_level_6            ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_level_7            ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_locationinfo       ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_locationinfo_trash ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_storageinfo        ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_tags               ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_retention_policy   ALTER COLUMN ipnfsid TYPE varchar(36);
ALTER TABLE t_access_latency     ALTER COLUMN ipnfsid TYPE varchar(36);

CREATE OR REPLACE FUNCTION update_tag(varchar, varchar,varchar, int) RETURNS void AS $$
DECLARE
    v_pnfsid varchar := $1;
    v_tagname varchar := $2;
    v_tagid varchar := $3;
    v_isorigin int := $4;
BEGIN
   BEGIN
       INSERT INTO t_tags VALUES(v_pnfsid , v_tagname, v_tagid, v_isorigin);
   EXCEPTION WHEN unique_violation THEN
       RAISE NOTICE 'Tag % for % exist, updating.', v_tagname, v_pnfsid;
       UPDATE t_tags SET itagid=v_tagid,isorign=v_isorigin WHERE ipnfsid=v_pnfsid AND itagname=v_tagname;
   END;

END;

$$
LANGUAGE 'plpgsql';

