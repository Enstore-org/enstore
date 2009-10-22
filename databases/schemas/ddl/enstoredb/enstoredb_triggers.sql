DROP TRIGGER set_update;

CREATE TRIGGER set_update
    BEFORE INSERT OR UPDATE ON file
    FOR EACH ROW
    EXECUTE PROCEDURE set_update();


--
-- Name: update_volume_counters; Type: TRIGGER; Schema: public; Owner: enstore
--

DROP TRIGGER update_volume_counters;

CREATE TRIGGER update_volume_counters
    AFTER INSERT OR DELETE OR UPDATE ON file
    FOR EACH ROW
    EXECUTE PROCEDURE update_volume_file_counters();

