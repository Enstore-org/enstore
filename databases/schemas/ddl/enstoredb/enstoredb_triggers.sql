DROP TRIGGER IF EXISTS set_update ON file;

CREATE TRIGGER set_update
    BEFORE INSERT OR UPDATE ON file
    FOR EACH ROW
    EXECUTE PROCEDURE set_update();


--
-- Name: update_volume_counters; Type: TRIGGER; Schema: public; Owner: enstore
--

DROP TRIGGER IF EXISTS  update_volume_counters ON file;

CREATE TRIGGER update_volume_counters
    AFTER INSERT OR DELETE OR UPDATE ON file
    FOR EACH ROW
    EXECUTE PROCEDURE update_volume_file_counters();

DROP TRIGGER IF EXISTS populate_file_table ON file;

CREATE TRIGGER populate_file_table
    BEFORE INSERT OR UPDATE ON file
    FOR EACH ROW
    EXECUTE PROCEDURE populate_file_table();

DROP TRIGGER IF EXISTS populate_files_in_transition_table ON file;

CREATE TRIGGER populate_files_in_transition_table
    AFTER INSERT OR UPDATE ON file
    FOR EACH ROW
    EXECUTE PROCEDURE populate_files_in_transition_table();

DROP TRIGGER IF EXISTS volume_audit_counter ON volume_audit;

CREATE TRIGGER volume_audit_counter
    AFTER INSERT OR DELETE ON volume_audit
    FOR EACH ROW
    EXECUTE PROCEDURE volume_audit();
