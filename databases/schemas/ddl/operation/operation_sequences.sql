--
-- Name: job_definition_id; Type: SEQUENCE; Schema: public; Owner: enstore
--

CREATE SEQUENCE job_definition_id
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.job_definition_id OWNER TO enstore;

--
-- Name: job_id; Type: SEQUENCE; Schema: public; Owner: enstore
--

CREATE SEQUENCE job_id
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.job_id OWNER TO enstore;

--
-- Name: task_id; Type: SEQUENCE; Schema: public; Owner: enstore
--

CREATE SEQUENCE task_id
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.task_id OWNER TO enstore;