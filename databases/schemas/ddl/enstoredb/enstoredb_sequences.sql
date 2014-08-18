--
-- Name: state_type_seq; Type: SEQUENCE; Schema: public; Owner: enstore
--

CREATE SEQUENCE state_type_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.state_type_seq OWNER TO enstore;

--
-- Name: volume_seq; Type: SEQUENCE; Schema: public; Owner: enstore
--

CREATE SEQUENCE volume_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.volume_seq OWNER TO enstore;


--
-- Name: volume_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: enstore
--

CREATE SEQUENCE volume_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.volume_audit_id_seq OWNER TO enstore;
