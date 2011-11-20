---
--- migrate level4 based HSM info into t_locationinfo table
---


CREATE OR REPLACE FUNCTION "public"."enstore2chimera" ()  RETURNS void AS $$
DECLARE
    ichain  RECORD;
    istring text[] ;
    igroup text ;
    istore text ;
    l_entries text[];
    ilocation text;
BEGIN
     FOR ichain IN SELECT * FROM t_level_4 LOOP
     	ilocation = f_enstore2uri(encode(ichain.ifiledata,'escape'));
	IF ilocation IS NULL THEN
	   raise warning 'iloation is NULL %',ichain.ipnfsid;
	   CONTINUE;
	ELSE
	   BEGIN
	      INSERT INTO t_locationinfo VALUES ( ichain.ipnfsid, 0, ilocation, 10, NOW(), NOW(), 1);
	      EXCEPTION WHEN unique_violation THEN
              -- do nothing
	        RAISE NOTICE 'Tape location for % aready exist.', ichain.ipnfsid;
	        CONTINUE;
           END;
	   l_entries = string_to_array(encode(ichain.ifiledata,'escape'), E'\n');
	   BEGIN
	      INSERT INTO t_storageinfo
	      	  VALUES (ichain.ipnfsid,'enstore','enstore',l_entries[4]);
	      EXCEPTION WHEN unique_violation THEN
              -- do nothing
	      RAISE NOTICE 'Storage info for % aready exist.', ichain.ipnfsid;
	      CONTINUE;
           END;
     END IF;
     END LOOP;
END;
$$
LANGUAGE 'plpgsql';
