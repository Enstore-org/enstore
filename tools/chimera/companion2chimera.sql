---
--- migrate companion info into t_locationinfo table
---
--- v0.1
---


CREATE OR REPLACE FUNCTION "public"."companion2chimera" ()  RETURNS void AS $$
DECLARE
    ichain  RECORD;
 BEGIN
     FOR ichain IN SELECT * FROM cacheinfo LOOP
     	 BEGIN
	        INSERT INTO t_locationinfo VALUES ( ichain.pnfsid, 1, ichain.pool, 10, NOW(), NOW(), 1);
	     EXCEPTION WHEN foreign_key_violation THEN
          RAISE NOTICE 'not in chimera %', ichain.pnfsid;
	       --- INSERT INTO t_locationinfo_trash VALUES ( ichain.pnfsid, 1, ichain.pool, 10, NOW(), NOW(), 0);
        END;
     END LOOP;
END;
$$
LANGUAGE 'plpgsql';

