--
-- Name: progress_set_job_finish_trigger; Type: TRIGGER; Schema: public; Owner: enstore
--

DROP TRIGGER progress_set_job_finish_trigger ON progress;

CREATE TRIGGER progress_set_job_finish_trigger
    AFTER UPDATE ON progress
    FOR EACH ROW
    EXECUTE PROCEDURE set_job_finish();


--
-- Name: reset_job_task_trigger; Type: TRIGGER; Schema: public; Owner: enstore
--

DROP TRIGGER reset_job_task_trigger ON task;

CREATE TRIGGER reset_job_task_trigger
    AFTER DELETE ON task
    FOR EACH ROW
    EXECUTE PROCEDURE reset_job_tasks();


--
-- Name: set_job_task_trigger; Type: TRIGGER; Schema: public; Owner: enstore
--

DROP TRIGGER set_job_task_trigger ON task;

CREATE TRIGGER set_job_task_trigger
    AFTER INSERT ON task
    FOR EACH ROW
    EXECUTE PROCEDURE set_job_tasks();

