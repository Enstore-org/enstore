--
-- Name: plpgsql_call_handler(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION plpgsql_call_handler() RETURNS language_handler
    AS '$libdir/plpgsql', 'plpgsql_call_handler'
    LANGUAGE c;


ALTER FUNCTION public.plpgsql_call_handler() OWNER TO enstore;

--
-- Name: plpgsql_validator(oid); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION plpgsql_validator(oid) RETURNS void
    AS '$libdir/plpgsql', 'plpgsql_validator'
    LANGUAGE c;


ALTER FUNCTION public.plpgsql_validator(oid) OWNER TO enstore;

--
-- Name: reset_job_tasks(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION reset_job_tasks() RETURNS "trigger"
    AS $$
begin
update job_definition set tasks = (select max(seq) from task where task.job_type = OLD.job_type) where job_definition.id = OLD.job_type;
return OLD;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.reset_job_tasks() OWNER TO enstore;

--
-- Name: set_job_finish(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION set_job_finish() RETURNS "trigger"
    AS $$
begin
update job set finish = (select progress.finish from progress, job, job_definition where job.id = NEW.job and progress.job = job.id and job.type = job_definition.id and job_definition.tasks = progress.task) where job.id = NEW.job;
return NEW;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.set_job_finish() OWNER TO enstore;

--
-- Name: set_job_tasks(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION set_job_tasks() RETURNS "trigger"
    AS $$
begin
update job_definition set tasks = (select max(seq) from task where task.job_type = NEW.job_type) where job_definition.id = NEW.job_type;
return NEW;
end;
$$
    LANGUAGE plpgsql;


ALTER FUNCTION public.set_job_tasks() OWNER TO enstore;
