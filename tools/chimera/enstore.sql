---
--- this trigger populates t_locationinfo with data from level4
---
CREATE OR REPLACE FUNCTION f_enstore2uri(varchar) RETURNS varchar AS $$
DECLARE
    l_level4 varchar := $1;
    l_entries text[];
BEGIN
    -- convert level4 data into array of strings
    l_entries = string_to_array(l_level4, E'\n');

    -- string_to_array skips empty lines. as a result we get 9 lines instead of 11
    return 'enstore://enstore/?volume=' || l_entries[1] || '&location_cookie=' || l_entries[2]  ||
           '&size='                     || l_entries[3] || '&file_family='     || l_entries[4]  ||
           '&original_name='            || l_entries[5] || '&map_file='        || l_entries[6]  ||
           '&pnfsid_file='              || l_entries[7] || '&pnfsid_map='      || l_entries[8]  ||
           '&bfid='                     || l_entries[9] || '&origdrive='       || l_entries[10] ||
           '&crc=' || l_entries[11]  ;
END;
$$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION f_enstorelevel2locationinfo() RETURNS TRIGGER
AS $$
DECLARE
	l_entries text[];
        location text;
BEGIN
	IF (TG_OP = 'INSERT') THEN
           location := f_enstore2uri(encode(NEW.ifiledata,'escape'));
	   IF location IS NULL THEN
	      -- encp only creates empty layer 4 file
	      -- so NEW.ifiledata is null
	      INSERT INTO t_locationinfo VALUES (NEW.ipnfsid,0,'enstore:',10,NOW(),NOW(),1);
	      INSERT INTO t_storageinfo
	      	  VALUES (NEW.ipnfsid,'enstore','enstore','enstore');
	   ELSE
              l_entries = string_to_array(encode(NEW.ifiledata,'escape'), E'\n');
 	      INSERT INTO t_locationinfo VALUES (NEW.ipnfsid,0,location,10,NOW(),NOW(),1);
	      INSERT INTO t_storageinfo
	      	  VALUES (NEW.ipnfsid,'enstore','enstore',l_entries[4]);
           END IF;

           -- we assume all files coming through level4 to be CUSTODIAL-NEARLINE
	   BEGIN
           INSERT INTO t_access_latency VALUES (NEW.ipnfsid, 0);
	   EXCEPTION WHEN unique_violation THEN
	   RAISE WARNING 't_access_latency already exists %',NEW.ipnfsid;
	   END;
	   BEGIN
	   INSERT INTO t_retention_policy VALUES (NEW.ipnfsid, 0);
	   EXCEPTION WHEN unique_violation THEN
	   RAISE WARNING 't_retention_policy  exists %',NEW.ipnfsid;
	   END;
           -- storage info
	ELSEIF (TG_OP = 'UPDATE')  THEN
           location := f_enstore2uri(encode(NEW.ifiledata,'escape'));
	   IF location IS NOT NULL THEN
	   UPDATE t_locationinfo
	   	  SET ilocation = f_enstore2uri(encode(NEW.ifiledata, 'escape'))
           WHERE ipnfsid = NEW.ipnfsid and itype=0;
           l_entries = string_to_array(encode(NEW.ifiledata,'escape'), E'\n');
	   UPDATE t_storageinfo SET istoragesubgroup=l_entries[4]
	   WHERE  ipnfsid = NEW.ipnfsid;
           END IF;
        END IF;
        RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER tgr_enstore_location BEFORE INSERT OR UPDATE ON t_level_4 FOR EACH ROW EXECUTE PROCEDURE f_enstorelevel2locationinfo();
