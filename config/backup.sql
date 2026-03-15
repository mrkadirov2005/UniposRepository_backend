--
-- PostgreSQL database dump
--

\restrict BB2WrxsohBdE62FraoJqHfWAvPxCwmk4xbqPlp9g0IukW7uJUCHuCA8EfPqJVm7

-- Dumped from database version 18.1
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- Name: update_wagon_timestamp(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.update_wagon_timestamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: admin; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.admin (
    id integer NOT NULL,
    first_name character varying NOT NULL,
    last_name character varying NOT NULL,
    phone_number character varying NOT NULL,
    password character varying NOT NULL,
    isloggedin boolean DEFAULT false NOT NULL,
    salary numeric DEFAULT 0 NOT NULL,
    sales integer DEFAULT 0 NOT NULL,
    ispaidthismonth boolean DEFAULT false NOT NULL,
    expenses numeric DEFAULT 0 NOT NULL,
    bonuses numeric DEFAULT 0 NOT NULL,
    permissions text[] DEFAULT '{}'::text[] NOT NULL,
    img_url text,
    createdat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updatedat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    uuid character varying(255),
    refreshtoken text,
    accesstoken text,
    shop_id character varying(255),
    work_start character varying(20),
    work_end character varying(20),
    branch integer
);


--
-- Name: admin_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.admin_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: admin_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.admin_id_seq OWNED BY public.admin.id;


--
-- Name: branches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.branches (
    location character varying,
    employees integer,
    shop_id character varying(255),
    id integer NOT NULL,
    name character varying(255)
);


--
-- Name: brand; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.brand (
    id integer NOT NULL,
    brand_name character varying NOT NULL,
    provider_name character varying NOT NULL,
    provider_last_name character varying NOT NULL,
    provider_phone character varying NOT NULL,
    provider_card_number character varying NOT NULL,
    provider_email character varying,
    product_counts integer DEFAULT 0 NOT NULL,
    createdat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updatedat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    uuid uuid
);


--
-- Name: brand_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.brand_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: brand_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.brand_id_seq OWNED BY public.brand.id;


--
-- Name: category; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.category (
    id integer NOT NULL,
    category_name character varying NOT NULL,
    products_available integer DEFAULT 0 NOT NULL,
    createdat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updatedat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    uuid uuid
);


--
-- Name: category_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.category_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: category_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.category_id_seq OWNED BY public.category.id;


--
-- Name: debt_table; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.debt_table (
    id character varying(255) CONSTRAINT debts_id_not_null NOT NULL,
    day integer,
    month integer,
    year integer,
    name character varying(255),
    amount double precision,
    product_names character varying(400)[],
    branch_id integer,
    shop_id integer,
    admin_id character varying(255),
    isreturned boolean
);


--
-- Name: edu_centers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.edu_centers (
    center_id integer NOT NULL,
    center_name character varying(255) NOT NULL,
    center_code character varying(50) NOT NULL,
    email character varying(100),
    phone character varying(20),
    address text,
    city character varying(100),
    principal_name character varying(100),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: edu_centers_center_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.edu_centers_center_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: edu_centers_center_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.edu_centers_center_id_seq OWNED BY public.edu_centers.center_id;


--
-- Name: finance_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.finance_records (
    id integer NOT NULL,
    amount numeric(10,2) NOT NULL,
    description text,
    type character varying(50) NOT NULL,
    category character varying(50),
    date date NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: finance_records_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.finance_records_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: finance_records_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.finance_records_id_seq OWNED BY public.finance_records.id;


--
-- Name: payments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.payments (
    id integer NOT NULL,
    finance_record_id integer NOT NULL,
    amount numeric(10,2) NOT NULL,
    payment_date date NOT NULL,
    payment_method character varying(50),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: payments_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.payments_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: payments_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.payments_id_seq OWNED BY public.payments.id;


--
-- Name: permission; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.permission (
    id integer NOT NULL,
    name character varying NOT NULL,
    description text,
    createdat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updatedat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: permission_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.permission_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: permission_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.permission_id_seq OWNED BY public.permission.id;


--
-- Name: product; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.product (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    name character varying NOT NULL,
    scale integer NOT NULL,
    img_url text,
    availability integer DEFAULT 0 NOT NULL,
    total integer DEFAULT 0 NOT NULL,
    receival_date timestamp without time zone,
    expire_date timestamp without time zone,
    is_expired boolean DEFAULT false NOT NULL,
    net_price numeric DEFAULT 0 NOT NULL,
    sell_price numeric NOT NULL,
    supplier character varying,
    cost_price numeric,
    last_restocked timestamp without time zone,
    location character varying,
    description text,
    is_active boolean DEFAULT true NOT NULL,
    createdat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updatedat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    category_id integer,
    shop_id character varying(255),
    brand_id character varying(255),
    branch integer,
    unit character varying(10) DEFAULT 'pcs'::character varying NOT NULL
);


--
-- Name: reports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.reports (
    target_id character varying(255),
    day integer,
    month integer,
    year integer,
    shop_id character varying,
    log character varying(500),
    uuid uuid NOT NULL
);


--
-- Name: sales; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sales (
    id integer CONSTRAINT sale_id_not_null NOT NULL,
    sale_id character varying CONSTRAINT sale_sale_id_not_null NOT NULL,
    admin_number character varying CONSTRAINT sale_admin_number_not_null NOT NULL,
    admin_name character varying CONSTRAINT sale_admin_name_not_null NOT NULL,
    total_price numeric CONSTRAINT sale_total_price_not_null NOT NULL,
    total_net_price numeric CONSTRAINT sale_total_net_price_not_null NOT NULL,
    profit numeric CONSTRAINT sale_profit_not_null NOT NULL,
    sale_time timestamp without time zone,
    sale_day integer,
    sales_month integer,
    sales_year integer,
    createdat timestamp without time zone DEFAULT CURRENT_TIMESTAMP CONSTRAINT sale_createdat_not_null NOT NULL,
    updatedat timestamp without time zone DEFAULT CURRENT_TIMESTAMP CONSTRAINT sale_updatedat_not_null NOT NULL,
    branch integer,
    shop_id character varying(100),
    payment_method character varying(50)
);


--
-- Name: sale_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.sale_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sale_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.sale_id_seq OWNED BY public.sales.id;


--
-- Name: shopname; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.shopname (
    id character varying NOT NULL,
    name character varying,
    superadmin character varying,
    location character varying
);


--
-- Name: soldproduct; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.soldproduct (
    id integer NOT NULL,
    product_name character varying NOT NULL,
    amount integer NOT NULL,
    net_price numeric NOT NULL,
    sell_price numeric NOT NULL,
    productid uuid,
    salesid character varying,
    shop_id character varying(200)
);


--
-- Name: soldproduct_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.soldproduct_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: soldproduct_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.soldproduct_id_seq OWNED BY public.soldproduct.id;


--
-- Name: superuser; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.superuser (
    id integer NOT NULL,
    uuid uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    name character varying NOT NULL,
    lastname character varying NOT NULL,
    email character varying NOT NULL,
    phonenumber character varying NOT NULL,
    isloggedin boolean DEFAULT false NOT NULL,
    password character varying NOT NULL,
    refreshtoken text,
    accesstoken text,
    shopname character varying,
    img_url text,
    createdat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updatedat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    shop_id character varying(100)
);


--
-- Name: superuser_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.superuser_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: superuser_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.superuser_id_seq OWNED BY public.superuser.id;


--
-- Name: wagons; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wagons (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    wagon_number character varying(100) NOT NULL,
    products jsonb DEFAULT '[]'::jsonb NOT NULL,
    total numeric(15,2) DEFAULT 0,
    indicator character varying(50) NOT NULL,
    shop_id uuid,
    branch character varying(100),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_by uuid,
    CONSTRAINT wagons_indicator_check CHECK (((indicator)::text = ANY ((ARRAY['debt_taken'::character varying, 'debt_given'::character varying, 'none'::character varying])::text[])))
);


--
-- Name: TABLE wagons; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.wagons IS 'Stores wagon information with products and debt indicators';


--
-- Name: COLUMN wagons.wagon_number; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.wagons.wagon_number IS 'Unique wagon identifier/number';


--
-- Name: COLUMN wagons.products; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.wagons.products IS 'JSONB array of products with structure: [{product_id, product_name, amount, price, subtotal}]';


--
-- Name: COLUMN wagons.total; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.wagons.total IS 'Total value of all products in the wagon';


--
-- Name: COLUMN wagons.indicator; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.wagons.indicator IS 'Debt type indicator: debt_taken (I took debt), debt_given (I gave debt), or none';


--
-- Name: COLUMN wagons.shop_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.wagons.shop_id IS 'Reference to shop if applicable';


--
-- Name: COLUMN wagons.branch; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.wagons.branch IS 'Branch identifier';


--
-- Name: weekstats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.weekstats (
    id integer NOT NULL,
    month character varying NOT NULL,
    net_sales numeric DEFAULT 0 NOT NULL,
    net_profit numeric DEFAULT 0 NOT NULL,
    createdat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updatedat timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    week_end integer,
    week_start integer
);


--
-- Name: weekstats_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.weekstats_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: weekstats_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.weekstats_id_seq OWNED BY public.weekstats.id;


--
-- Name: admin id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin ALTER COLUMN id SET DEFAULT nextval('public.admin_id_seq'::regclass);


--
-- Name: brand id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.brand ALTER COLUMN id SET DEFAULT nextval('public.brand_id_seq'::regclass);


--
-- Name: category id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.category ALTER COLUMN id SET DEFAULT nextval('public.category_id_seq'::regclass);


--
-- Name: edu_centers center_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.edu_centers ALTER COLUMN center_id SET DEFAULT nextval('public.edu_centers_center_id_seq'::regclass);


--
-- Name: finance_records id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.finance_records ALTER COLUMN id SET DEFAULT nextval('public.finance_records_id_seq'::regclass);


--
-- Name: payments id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payments ALTER COLUMN id SET DEFAULT nextval('public.payments_id_seq'::regclass);


--
-- Name: permission id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.permission ALTER COLUMN id SET DEFAULT nextval('public.permission_id_seq'::regclass);


--
-- Name: sales id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sales ALTER COLUMN id SET DEFAULT nextval('public.sale_id_seq'::regclass);


--
-- Name: soldproduct id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.soldproduct ALTER COLUMN id SET DEFAULT nextval('public.soldproduct_id_seq'::regclass);


--
-- Name: superuser id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.superuser ALTER COLUMN id SET DEFAULT nextval('public.superuser_id_seq'::regclass);


--
-- Name: weekstats id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.weekstats ALTER COLUMN id SET DEFAULT nextval('public.weekstats_id_seq'::regclass);


--
-- Data for Name: admin; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.admin (id, first_name, last_name, phone_number, password, isloggedin, salary, sales, ispaidthismonth, expenses, bonuses, permissions, img_url, createdat, updatedat, uuid, refreshtoken, accesstoken, shop_id, work_start, work_end, branch) FROM stdin;
3	Kadirov	what a hell	9740881058	Ifromurgut2005$	f	56468	0	f	0	0	{DELETE_PRODUCT}	https://picsum.photos/200	2025-12-14 20:36:46.636	2025-12-14 23:30:10.491	555	eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwaG9uZV9udW1iZXIiOiI5NzQwODgxMDU4Iiwicm9sZSI6ImFkbWluIiwiaWF0IjoxNzY1NzQ0NjA2LCJleHAiOjE3NjgzMzY2MDZ9.hQBJgReu2DvPWHb85vpUv1MGS1QCwZRpoJ6N8mH8ZB4	eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwaG9uZV9udW1iZXIiOiI5NzQwODgxMDU4Iiwicm9sZSI6ImFkbWluIiwiaWF0IjoxNzY1NzQ0NjA2LCJleHAiOjE3NjU3NDgyMDZ9.vnfcBMSbu-F0ZVxq0SldOqkBtajdkSFTHkZfrvhJI5g	0000	9:30	22:20	0
2	Abduqodirov	Muzaffar	97403814508	salom	f	974088108	0	f	0	0	{DEL_REPORT,CRUD,UPDATE_PRODUCT,DELETE_PRODUCT,PRODUCT_DETAILS}	https://picsum.photos/200	2025-12-10 19:50:47.254	2026-01-18 23:54:05.944421	550e8300-e29b-41d4-a716-446655440000	\N	eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiQWJkdXFvZGlyb3YiLCJyb2xlIjoiYWRtaW4iLCJpYXQiOjE3Njg3NjI0NzAsImV4cCI6MTc2ODg0ODg3MH0.3GQXI4RisDWfAafFbbz2OJixxSYiL9czxH1sH-GWOPI	0000	09:30	22:20	0
4	Xayrullo	Rozmatov	56789	1234	f	4	0	f	0	0	{PRODUCT_DETAILS,DELETE_PRODUCT}	https://picsum.photos/200	2025-12-16 15:45:17.651	2025-12-16 15:45:42.532	0.438587255385826	\N	\N	0000	9:30	22:20	1
6	haskfhbsdfjs	sdfhbsjdh	65165	45644	f	6556	0	f	0	0	{DELETE_PRODUCT,PRODUCT_DETAILS}	https://picsum.photos/200	2025-12-16 19:31:00.727	2025-12-21 18:55:22.534	0.7171970106433649	eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwaG9uZV9udW1iZXIiOiI2NTE2NSIsInJvbGUiOiJhZG1pbiIsImlhdCI6MTc2NTkxMzQ2MCwiZXhwIjoxNzY4NTA1NDYwfQ.OW9vYWFTSLUhU6-jMVSIHP_RAjWVlnkPEGzsRNOnxnI	eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwaG9uZV9udW1iZXIiOiI2NTE2NSIsInJvbGUiOiJhZG1pbiIsImlhdCI6MTc2NTkxMzQ2MCwiZXhwIjoxNzY1OTE3MDYwfQ.jH5HY9qdGULt_wU4dlbswSNHcMKD7mjkrGpazcYRFBU	0000	9:30	22:20	0
5	Rozimatov	Xayirullo	123456789	salom	f	613	0	f	0	0	{UPDATE_PRODUCT,CRUD,DELETE_PRODUCT}	https://picsum.photos/200	2025-12-16 16:23:07.394	2026-01-01 14:01:17.332847	0.7329386920447608	\N	\N	0000	9:30	22:20	0
1	Abduqodirov	Muzaffar	974088108	1234	f	974088108	0	f	0	0	{DEL_REPORT,CRUD,UPDATE_PRODUCT,DELETE_PRODUCT,PRODUCT_DETAILS}	https://picsum.photos/200	2025-12-10 19:45:05.321	2026-01-18 23:54:13.215306	550e8400-e29b-41d4-a716-446655440000	\N	\N	0000	09:30	22:20	1
\.


--
-- Data for Name: branches; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.branches (location, employees, shop_id, id, name) FROM stdin;
Urgut	1	0000	0	Yetti uylik branch
salom	15	0000	2	New
\.


--
-- Data for Name: brand; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.brand (id, brand_name, provider_name, provider_last_name, provider_phone, provider_card_number, provider_email, product_counts, createdat, updatedat, uuid) FROM stdin;
2	CocaColas	Kadirov	Muzaffar	974088108	3484 1565 1664 1656	muzaffar571181@gmail.com	0	2025-12-22 11:12:46.202	2025-12-22 11:12:46.202	58c14a5b-02dc-4fcd-9c1a-792d841cb90b
3	Non	Gaybulla	Bilmadim	16164 6648 	8734-4544-4545-4544	islommuhammad571181@gmail.com	0	2025-12-22 11:14:26.963	2025-12-22 11:14:26.963	38aff5fd-03d4-4201-ad1c-bc1a6769169c
1	Pepsis	fsdsd	sdfsdfs	15455	166465	fghfhfgfhfhfg@gmail.com	1	2025-12-11 09:08:41.144	2025-12-22 11:19:07.01	c75f7c66-e858-47d6-bb82-7ea5547c800c
\.


--
-- Data for Name: category; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.category (id, category_name, products_available, createdat, updatedat, uuid) FROM stdin;
4	lazer guruch	0	2026-01-06 09:47:08.736949	2026-01-06 09:47:08.736949	872d43a8-7bc6-4d7f-b067-eea7620b3602
\.


--
-- Data for Name: debt_table; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.debt_table (id, day, month, year, name, amount, product_names, branch_id, shop_id, admin_id, isreturned) FROM stdin;
15e5f9d0-75e4-4b7d-b482-9a19c94afce1	26	2	2026	Qobil	200	{NOn*20*10*}	1	0	admin-uuid	t
5b6b7474-7e7e-4579-9ec6-8a4dd686d1b8	26	2	2026	dfgd	300	{dkfnjkdf*20*15*0*kg}	1	0	admin-uuid	f
57a26376-8aa4-404b-85a0-b8421e7ed5b3	26	2	2026	Qobil	683390	{non*123*25*0*pcs,"Olma nok*1321*515*0*pcs"}	1	0	admin-uuid	f
c0451be7-01ce-48c9-8fb3-f66bf51fb968	17	1	2026	Sobir	31829	{sdfsdsd*15*15*14|sdfsd*1152*27*1500|dkgbd*25*20*25}	0	0	admin-uuid	t
91fd45a6-3de1-4153-afe8-57e25345ff1d	26	2	2026	djfngkfdk	4488	{dfgndjk*132*34*34}	1	0	admin-uuid	f
b0ce10da-a0ad-4855-8433-42c34c402686	26	2	2026	Qobil	11730	{wrwe*34*345*0}	1	0	admin-uuid	f
\.


--
-- Data for Name: edu_centers; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.edu_centers (center_id, center_name, center_code, email, phone, address, city, principal_name, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: finance_records; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.finance_records (id, amount, description, type, category, date, created_at, updated_at) FROM stdin;
1	15000.00	salom: nimadir	expense	sales	2026-01-12	2026-01-12 20:23:37.562824	2026-01-12 20:23:37.562824
2	16000.00	salom: ooook	income	sales	2026-01-12	2026-01-12 20:23:57.564462	2026-01-12 20:23:57.564462
3	13000.00	Vagon: Salam	income	sales	2026-02-26	2026-02-26 15:21:09.09308	2026-02-26 15:21:09.09308
4	1200.00	Qobil: nimadir	income	sales	2026-02-26	2026-02-26 15:31:46.164969	2026-02-26 15:31:46.164969
\.


--
-- Data for Name: payments; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.payments (id, finance_record_id, amount, payment_date, payment_method, created_at) FROM stdin;
\.


--
-- Data for Name: permission; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.permission (id, name, description, createdat, updatedat) FROM stdin;
2	DEL_REPORT	access to delete the data	2025-12-10 19:59:30.011	2025-12-10 19:59:30.011
1	CRUD	create and read and update	2025-12-10 19:59:30.011	2025-12-10 19:59:30.011
3	UPDATE_PRODUCT	update the product	2025-12-13 06:58:28.682	2025-12-13 06:58:28.682
4	DELETE_PRODUCT	access to delete the product	2025-12-13 06:59:12.271	2025-12-13 06:59:12.271
5	PRODUCT_DETAILS	see product details	2025-12-13 07:12:33.159	2025-12-13 07:12:33.159
\.


--
-- Data for Name: product; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.product (id, name, scale, img_url, availability, total, receival_date, expire_date, is_expired, net_price, sell_price, supplier, cost_price, last_restocked, location, description, is_active, createdat, updatedat, category_id, shop_id, brand_id, branch, unit) FROM stdin;
bb6a9aac-5661-4a4a-bb1f-7ced48435432	fgjgjgh	1	\N	5675	57556	2026-02-24 11:37:00	\N	f	567	56767	67	456	2026-02-24 11:37:00	56	5756	t	2026-02-24 16:40:35.525352	2026-02-24 16:40:35.525352	4	0000		0	t
ab450fc0-8b95-4c6f-907e-d72b8e621929	nimadir	1	\N	30	40	2026-02-26 08:02:00	\N	f	0	5000	d	4500	2026-02-26 08:02:00	dfgd	\N	t	2026-02-26 13:08:47.960304	2026-02-26 13:08:47.960304	\N	0000		0	pcs
fd3fffe6-044c-4ee3-b815-471be7dd887f	Lazer guruch*KG	1	\N	79	40	2026-01-03 19:30:00	\N	f	1000	10000	Qobil	8000	2026-01-03 19:30:00	Salom	\N	t	2026-01-04 10:31:44.877443	2026-02-26 13:21:09.197838	2	0000	2	0	pcs
\.


--
-- Data for Name: reports; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.reports (target_id, day, month, year, shop_id, log, uuid) FROM stdin;
Muzaffar	22	12	2025	0000	Fetched products for shop	605cfb3e-c011-43a2-85f2-d290404ef6e0
Muzaffar	22	12	2025	0000	Fetched products for shop	c0429b82-4745-4171-a5d4-e090accd950a
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	a7360929-7ed8-4f6b-8609-754aef8f9e28
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	ecbe6e35-46a1-46a5-82e4-5cc9ec550793
Muzaffar	22	12	2025	0000	Fetched products for shop	911fe52f-c0e6-45a3-bcd7-8495f0529465
Muzaffar	22	12	2025	0000	Fetched products for shop	b69526c3-93c0-4f09-9c43-6fe1c5944600
\N	22	12	2025	\N	Product restocked: 7dc6d744-d616-4927-aa66-22ed8373b7a7 (added 12)	e22668a8-aee3-4bec-ba15-d8839bf99d2d
Muzaffar	22	12	2025	0000	Fetched products for shop	026abe65-67f2-4112-b652-140584ccafef
Muzaffar	22	12	2025	0000	All admins fetched	51080905-68c7-449b-85d1-237fd9fd3a8a
Muzaffar	22	12	2025	0000	All admins fetched	e922ace5-beda-40bb-9b26-d5af37e86f09
Muzaffar	22	12	2025	0000	Fetched products for shop	3da027be-5f16-4d10-af2c-6d9225dadb86
Muzaffar	22	12	2025	0000	Fetched products for shop	b3695825-7b78-4cc6-ac7c-77f171a38226
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	1e2ed3f3-bc8d-4d7f-a561-d48120314a73
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	50c5089c-9fb3-4875-905e-92a8394bcdd5
Muzaffar	30	12	2025	0000	Fetched products for shop	c5a5ee47-865a-478d-be7a-30539cda1db2
Muzaffar	30	12	2025	0000	Fetched products for shop	aec20cff-8e0f-4bae-bd8c-a2c536bdbc48
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	84fa933c-1525-4fbc-a772-6e2bba00aa70
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	c10d7f34-5ce3-4def-8ac8-20be12cb6a7a
Muzaffar	30	12	2025	0000	Fetched products for shop	4c6d48ca-ecb4-4daf-be8b-7c185955507d
Muzaffar	30	12	2025	0000	Fetched products for shop	2706a9a6-5315-4175-8479-e3ccdf0b288b
Muzaffar	30	12	2025	0000	All admins fetched	573c719b-5fba-4520-b5ad-88dd5637184d
Muzaffar	30	12	2025	0000	All admins fetched	91c2b3a0-6ce2-410f-9929-8f305dc2ba91
Muzaffar	1	1	2026	\N	Fetched main finance statistics	9a07ca6f-932a-4097-84dd-e3c81bae9e88
Muzaffar	1	1	2026	\N	Fetched main finance statistics	8c074cec-d86a-45e8-8e58-225ad1443626
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	46027a1a-8c76-4488-9899-24feec22563b
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	45b331d1-887b-4215-a5c7-1c6299f3c032
Muzaffar	1	1	2026	\N	Fetched high stock products	cf05a654-cacf-47c8-a194-9438c359c514
Muzaffar	1	1	2026	\N	Fetched high stock products	113ea1ee-9b29-4070-99c7-0bd70e955ce2
Muzaffar	1	1	2026	\N	Fetched low stock products	9623b84a-0350-4131-bcc3-87e119f13355
Muzaffar	1	1	2026	\N	Fetched low stock products	d4a03cf7-8341-441c-925b-a3610da6c065
Muzaffar	1	1	2026	0000	Fetched products for shop	e9a3663f-bff3-4b80-8a75-c120ca2dc140
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c7dbdb1f-7968-4b94-8f89-342181998ff2
Muzaffar	1	1	2026	0000	Fetched products for shop	b7b8ff6a-9954-498a-b823-c513ced6218c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	10992155-71ed-4571-81d4-db6b43c35cd7
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	69e81e55-c3c4-4901-a13e-63c551a784e2
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	0f3a3443-77c1-4da6-8160-61351f507b24
Muzaffar	1	1	2026	0000	Fetched products for shop	7802fcc3-4dfb-45e4-af50-af6cc5baf9c0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c61b2525-d042-4ce2-abd9-dcad2de4d293
Muzaffar	1	1	2026	0000	Fetched products for shop	78fa3008-1670-4370-aec7-878c6365b552
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	b1f7fd77-95db-49ed-9c47-60326f170404
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a905e852-30c8-43e2-ae9e-9222b75321bf
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	1d19ed69-f762-4267-9438-b7805aef89ec
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	67cfb4a9-9a45-4fac-8c97-50f840579ff8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	61a28e93-caa8-4890-9d5d-198032a2a302
Muzaffar	1	1	2026	0000	Fetched products for shop	f1f2333d-80e3-44f0-aa08-ec8b9ef65490
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	08bda5df-2dbc-428a-a7cf-a23b6ae9d1fa
Muzaffar	1	1	2026	0000	Fetched products for shop	34679182-a099-4f77-9421-82556a561198
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	313ed96a-a583-43e5-86cc-ce197e9e83cb
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	b401a2c6-b48b-40c5-bfc4-ed26930c2970
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c6041819-43dc-472e-8c26-31e83d8d59d1
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	2b8f166b-4820-409d-93c4-56492bf21e43
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	94722012-2ca6-481e-806d-9843881f5fd8
Muzaffar	1	1	2026	0000	Fetched products for shop	dfcef67d-a2a9-4316-9c06-9a625fa578da
Muzaffar	1	1	2026	0000	Fetched products for shop	ddf522e3-6f60-4ff5-b8bf-34bc35c412d3
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	38acbea9-21a7-45d9-b3aa-d74208adbd25
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	c398d4e9-fe42-4cc7-8f8f-c124ce7413a0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	804f884c-391a-465e-ac5f-c9f018466e03
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	e498fed0-0db5-4e6e-8181-bd8fb1ce6ba7
Muzaffar	1	1	2026	0000	Fetched products for shop	9d2b7d7c-4a20-48c6-b28a-964d43f8699b
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	e4183c4a-82e3-4109-9857-f96c1f8953d2
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7586c4c6-948a-4a9d-89c5-b0eb5bcde37f
Muzaffar	1	1	2026	0000	Fetched products for shop	a893e72c-46f1-46b8-abd7-519176189d85
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	583dc7a1-ae71-490c-bfb2-1d402a3a41f3
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	a2ab2ad6-c7d9-4c10-90a4-1c783bf1b873
Muzaffar	1	1	2026	0000	Fetched products for shop	92a73611-2d4f-4b8a-a1f0-6c42b04d91d8
Muzaffar	1	1	2026	0000	Fetched products for shop	5322d3a5-4472-4dd4-ab60-c84d6196d86f
Muzaffar	1	1	2026	0000	Fetched products for shop	aef05f48-492f-43b6-b243-555b238ae9f0
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	3b0755ae-a7a6-4f90-b945-8d005ae8d963
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	6064bd84-89fa-41bf-aa11-d361b0c3152d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	310fc3f3-1b68-4994-9046-2808bf4d1e3a
Muzaffar	1	1	2026	0000	Fetched products for shop	fb26c7cb-bcf0-46ff-ba1c-023dc882c798
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	422d4070-b379-4f99-86ae-3bb485cac132
Muzaffar	1	1	2026	0000	Fetched products for shop	4bbd3c0a-0e63-4e25-a92b-d1a29652467a
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	18b1c723-498b-4308-b2bd-287890a52947
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	3e9310f1-8326-4d2f-898d-bb0973b67d1c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	823207ae-7109-4da3-bdf5-b322785b2c81
Muzaffar	1	1	2026	0000	Fetched products for shop	ade211da-b885-49ad-808e-6ef218413e28
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	e1016505-dca5-447b-80ec-e635fa27a840
Muzaffar	1	1	2026	0000	Fetched products for shop	d0ae21aa-71f8-4a3f-b905-1ed1c429b54e
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	08a7a331-0dcf-4d9c-b377-236f5faf367e
550e8400-e29b-41d4-a716-446655440000	22	12	2025	\N	Superuser logged out	92508226-7209-4d83-b847-f9322322c785
Muzaffar	22	12	2025	0000	Super logged in	63b1e475-2ec2-437d-84e6-f94b699e0dc4
Muzaffar	22	12	2025	0000	Fetched products for shop	a5c271bb-1596-4761-97e2-89ae21fe8238
Muzaffar	22	12	2025	0000	Fetched products for shop	bf86d2bb-9353-4012-b78a-e1bfbd6bfb06
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	0b09ea3b-eba5-40de-ad9f-f3071b62de16
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	753021f1-7b99-423a-8444-82cbe309d7fa
Muzaffar	22	12	2025	0000	Fetched products for shop	2d326003-935e-49f3-ac0d-4af7f7b3434c
Muzaffar	22	12	2025	0000	Fetched products for shop	fd4e74b8-d16c-4db3-846e-4635f6cc3b8b
Muzaffar	22	12	2025	0000	All admins fetched	00b7adfe-e811-4e62-9ee4-48b629061027
Muzaffar	22	12	2025	0000	All admins fetched	fe505bfd-ff14-4fb6-91ac-c16f6fb6b5c0
Muzaffar	22	12	2025	0000	Fetched products for shop	df35f71c-0c81-4b93-9e75-96c35738b808
Muzaffar	22	12	2025	0000	Fetched products for shop	10755ad0-26bd-46be-ae8e-83f9f44ee253
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	b26cc37c-b48c-45a4-a001-3b567733c08f
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	2e9641fd-8703-45ab-a19d-db496c91d6b4
Muzaffar	30	12	2025	\N	Fetched main finance statistics	1b26c2ad-5ea0-43aa-8618-d19de848f155
Muzaffar	30	12	2025	\N	Fetched main finance statistics	ff490326-60b0-4c98-9844-d91c55526bc2
Muzaffar	30	12	2025	\N	Fetched week statistics (last 7 days)	046ec520-3efc-4133-a057-c294845130c9
Muzaffar	30	12	2025	\N	Fetched week statistics (last 7 days)	23729daf-13c2-4a8f-a58a-3b46ea150d7b
Muzaffar	30	12	2025	\N	Fetched high stock products	07dbbe89-4435-4ae9-9a16-d8ca7c254daa
Muzaffar	30	12	2025	\N	Fetched high stock products	8a13b08a-0c5e-4327-9f30-aa2f8057bd4f
Muzaffar	30	12	2025	\N	Fetched low stock products	c5bd89d9-c8f5-4a1a-85f0-947387489df0
Muzaffar	30	12	2025	\N	Fetched low stock products	f7372be3-4cfd-46fb-9806-7f2630ee6f02
Muzaffar	30	12	2025	0000	Fetched products for shop	82fe89ea-9876-49aa-a8b3-b2d08b5c4663
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	68ec17da-7278-4326-9950-b2a48e3d03e3
Muzaffar	30	12	2025	0000	Fetched products for shop	e06af15b-fed6-4c55-95fa-56e5e75b9d1f
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	3f51e8e3-52e4-422f-a8fd-ba264673dcfb
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	c5c2edf4-aec9-4427-80d3-97142d9f7930
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	bd6bef0f-795e-436e-a620-9efc36b0c91e
Muzaffar	30	12	2025	0000	Fetched products for shop	e7e91288-7129-43a9-85b1-eb657ee93c9b
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	e4659da2-01a9-4ce3-9350-b3885c9a536a
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	aab4c7ef-8fe7-4f19-b021-60b5ca7923fc
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	467b50b4-250a-43cb-a187-542e87d46409
Muzaffar	30	12	2025	0000	Fetched products for shop	1d05a211-2d7f-4552-aaba-a8e8bca934df
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	53d0b29f-ce76-4a1f-83a7-4cf366f7e5d7
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	13f4b81d-5422-47c5-a418-eb88750e71e3
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	d3333da4-c45b-4d80-b480-da5ee16ab760
Muzaffar	1	1	2026	0000	Fetched products for shop	5c5a3c18-bb5e-45f5-b60e-4ea83f814690
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	316a324e-7a79-4d28-924f-5199a1b7e7bc
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b8179449-4766-40de-ab6f-04d2048c6e0d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	2db2e89a-8792-4943-9990-f49f1f7add9f
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	9bfcc8dc-54b6-48ca-b11d-c3ce9854dd44
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7975b023-4ce4-4ff1-8cb3-52d81d2acb70
Muzaffar	1	1	2026	0000	Fetched products for shop	6f635b3c-b4a9-499a-a7c5-989a95440dd7
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	2a36b976-b739-4823-a78e-8946953e4200
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	1e7bf0b0-4cd2-4c1a-b64c-10775bd79dc1
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	1c60fd60-4f3f-4165-a20c-9e7709b8171e
Muzaffar	1	1	2026	0000	Fetched products for shop	bbc1587c-d238-4589-938a-6d3a8ec9e843
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	1c56f8a6-25a8-4ef0-afbd-371a152e5134
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	5185a6db-c91e-489c-8aff-81b6fbb2a431
Muzaffar	1	1	2026	0000	Fetched products for shop	2ba2cb34-48f2-42bd-a32b-a1a8023d73aa
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	e0380a92-ab95-4c76-885c-70c14367eb47
Muzaffar	1	1	2026	0000	Fetched products for shop	2f933c00-d173-4462-9d5f-81e0550b7454
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	3351d036-25cc-4b8e-9eff-b337da15f6e4
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	91379d09-4647-4a29-abb2-22eebe8abaf0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b76eadc3-88ea-4d18-9ab8-d45c7896a81b
Muzaffar	1	1	2026	0000	Fetched products for shop	e05587ac-4689-462d-830d-b9a7a98743de
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	14978f17-3fcb-4cfa-b78e-965691237951
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	0580d56f-9d9e-4ca3-a6b0-8cf6b5e241f8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	243e71cc-ec2d-4234-bf45-86bac978e1d8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	358da011-08d8-47b2-86dd-704398ae07ce
Muzaffar	1	1	2026	0000	Fetched products for shop	d1a9d94e-70bf-4115-9365-59044a373b8b
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	d02dfc7a-694d-45a8-9083-7074a28e7b57
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	077991ec-99eb-4f80-8a0a-3956b7101a9e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	2ad51dc9-34c0-49df-8f1a-6bb80d4808ef
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ed73666f-1871-457b-aba4-5432479f5620
Muzaffar	1	1	2026	0000	Fetched products for shop	7c53d3fd-ea36-41d6-b21b-5b2eb7f3a2fa
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	66e34a5f-d682-44ca-acd9-bb577979592b
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	4e176f00-fd65-4f81-812d-4aa2fb97b57b
Muzaffar	1	1	2026	0000	Fetched products for shop	833e6b72-0a69-4d4d-8064-462e3ec4077b
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	2f9d5298-f011-439d-ada9-3aec0ad26cc5
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	2c01f9bd-f77a-4f71-83f0-af9457ff89a3
Muzaffar	1	1	2026	0000	Fetched products for shop	49ded1bf-031f-4985-9c34-fecd89ec6edd
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	fb9097a3-4240-4867-90f3-30651c53777d
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	65d507c6-f644-4d5b-b367-f636d7a69b7d
Muzaffar	1	1	2026	0000	Fetched products for shop	f1e2c43b-1ee4-4abd-af15-ac64e5119626
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	414ded3f-9ae3-4dcb-9f22-4d0816b1a8a0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	106d8281-c913-46bc-82ce-3a347bc33c94
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b8bc14c6-4142-4ca4-bd20-2d77c00f7155
Muzaffar	1	1	2026	0000	Fetched products for shop	e09cfe6a-6363-459b-87a3-29a71e75b45f
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	e14276f5-5824-4dd8-a6c2-4331b33e5800
Muzaffar	30	12	2025	\N	Fetched main finance statistics	8eb5c652-4bb9-43e7-99c5-6a8e8840ef38
Muzaffar	30	12	2025	\N	Fetched main finance statistics	cf8cffe8-59d6-4421-a0de-a6c9df73b7a4
Muzaffar	30	12	2025	\N	Fetched week statistics (last 7 days)	b7d16632-708b-42ef-baeb-b5bd0ab42faf
Muzaffar	30	12	2025	\N	Fetched high stock products	f6e3f01a-614b-4e0a-8327-81fdc8f2d5ea
Muzaffar	30	12	2025	\N	Fetched week statistics (last 7 days)	aa7fa600-93d2-4acc-875e-b4934880de92
Muzaffar	30	12	2025	\N	Fetched low stock products	d00735d9-c04a-4a63-b31b-6dcc0f9f7978
Muzaffar	30	12	2025	\N	Fetched high stock products	fcfa30cb-1c01-45d9-8e91-ec0ea2d53308
Muzaffar	30	12	2025	\N	Fetched low stock products	b9ccb7b3-6c3b-49f5-82d8-500c1c5dd54d
Muzaffar	30	12	2025	0000	Fetched products for shop	aacfc942-f457-4404-8aee-d720954767be
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	abe66464-147c-4949-83ec-1b5ca2d70509
Muzaffar	30	12	2025	0000	Fetched products for shop	931c1e93-9800-4ab2-8498-22bc499c2ac5
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	56b2568e-2edc-46c9-94c2-da6e138801b7
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	007de043-a92b-4ae7-ad0f-8c78d4097cfb
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	1065a273-e5d8-4d8c-bd21-3d702960a127
Muzaffar	30	12	2025	0000	Fetched products for shop	687df2fa-2409-46d4-a0df-b061d022af8d
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	0cfa702d-7c50-4d16-a564-45bcf69da498
Muzaffar	30	12	2025	0000	Fetched products for shop	248154b2-21fd-4325-90dd-a8e16a31a1d7
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	d1ad9f3d-ec58-4a0a-bb76-b27844c1b11f
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	f6fdfa06-edc3-4c30-a61b-09801f0a8641
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	0a4f81bd-72db-493d-877d-fcd2423bf4dc
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	5212661e-ab19-4427-91e3-1a9c511fb9a2
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	7e4cf3f4-687c-45a8-bafe-61b6940ed230
Muzaffar	30	12	2025	0000	All admins fetched	5dbdbfda-98b8-4149-9d9f-81da943d94ba
Muzaffar	30	12	2025	\N	Fetched all permissions - count: 5	11689efc-884f-4722-9c1c-d012f956c546
Muzaffar	30	12	2025	0000	All admins fetched	4b99b97a-fa3e-4d9f-a9f6-f35b23ec66a9
Muzaffar	30	12	2025	\N	Fetched all permissions - count: 5	daceb9c0-b279-4722-9844-a7ce1301e3a5
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	cfc86645-3ea2-4302-9fa8-420ef833ede6
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	8c84dc67-e80b-456c-a060-7992246899d0
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	d6fbcf4e-f224-43b5-82f7-4fa304e3dcf6
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	77dc7917-b8a2-4968-b5be-4daa5428f44b
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	873efd17-cdca-42fd-bf21-985363156c7b
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	9076973e-23c9-4f28-9fd5-5cd4b3a06596
Muzaffar	30	12	2025	0000	Fetched all debts - count: 5	05165935-2ba7-4182-a256-7b21fff4f51d
Muzaffar	30	12	2025	0000	Fetched debt statistics	7981e90e-14c7-4aae-bfd2-f030a2bb26a1
Muzaffar	30	12	2025	0000	Fetched all debts - count: 5	af27c8ea-d0a6-4c5b-85ec-284788cde680
Muzaffar	30	12	2025	0000	Fetched debt statistics	c133ea37-0bf0-4b14-bc2e-1aefdbe7b8c8
Muzaffar	30	12	2025	\N	Database backup downloaded - tables: 13	6d5a5040-92fe-48c1-850a-914204071cad
Muzaffar	30	12	2025	\N	Database backup downloaded - tables: 13	74d04fd1-4a43-4659-8771-3b55fca4931b
Muzaffar	30	12	2025	0000	All admins fetched	45e0db0a-17c9-4b67-addb-bd30956a0303
Muzaffar	30	12	2025	\N	Fetched all permissions - count: 5	73848f6c-aa7a-46e0-ab0d-d15286eaaaa6
Muzaffar	30	12	2025	0000	All admins fetched	c32ed833-1a6a-4c10-9d9f-f6a855d52b32
Muzaffar	30	12	2025	\N	Fetched all permissions - count: 5	65c682e6-ae94-49aa-9037-fe1beca6e1e6
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	e4c6dd75-93c6-464f-948f-b70c3b09dd3b
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	c3ebd7df-c5d1-4871-b395-f7d85c4f8bd3
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	ed5171e3-1fd0-4d42-9b47-d346bad86389
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	3027cb1f-6816-466f-8d8f-e9c301a2cd57
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	6ab0da9c-0506-4c3c-9c35-c478ec677e64
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	421d8db1-b5ae-4e07-945d-92ddc113300d
Muzaffar	30	12	2025	0000	Fetched all debts - count: 5	dbb24c72-f30e-48e9-bd2c-7424cb72c267
Muzaffar	30	12	2025	0000	Fetched debt statistics	3e35bff1-5ff5-4b95-9d54-75cee6e50d4f
Muzaffar	30	12	2025	0000	Fetched all debts - count: 5	f9297b5c-de59-4da3-b931-cd1636404be5
Muzaffar	30	12	2025	0000	Fetched debt statistics	47c284e3-a06d-43da-adbf-b03c0ac2e888
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	f85e4c6a-c42c-4bc1-88b9-90cac7b1da1b
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	37049336-cc07-4335-a9dc-0615aa501584
Muzaffar	30	12	2025	0000	Fetched products for shop	b5f85dc3-fd2d-4638-acbf-c527cb09b480
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	aef347aa-e914-4477-9afd-01a9014b3006
Muzaffar	30	12	2025	0000	Fetched products for shop	c22c1f14-72f1-4044-a1b0-358bf1ea7a20
Muzaffar	30	12	2025	\N	Fetched all categories - count: 3	601dae02-9cf6-420a-b910-1aa161cfbdc9
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	b5b393fd-f277-46d5-966d-78c5cd84d96e
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	206ff79f-cdab-4abc-916b-938b96b81c80
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	4414a1b9-b63d-440a-aac6-081d8d9e64e4
Muzaffar	30	12	2025	0000	Fetched shop branches - count: 1	c115ad5a-880d-419e-9859-014c786f7756
Muzaffar	30	12	2025	\N	Fetched shop reports as superuser - count: 55	f86b4be4-4690-4e12-90e2-466af63872ce
Muzaffar	30	12	2025	\N	Fetched shop reports as superuser - count: 55	4ee9d89f-5993-443d-81cb-11cd1ecbe36e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	a4c09d07-0b77-456b-bb9c-a278ad022dd9
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	c74e8872-9635-4f20-8306-ca7aaaeac7b5
Muzaffar	1	1	2026	0000	Fetched products for shop	f0e9f3d8-dbd6-4682-bbda-804b52450fa3
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	726e6840-fdad-4efa-816d-d5c117476dc5
Muzaffar	1	1	2026	0000	Fetched products for shop	78110131-c864-446e-a1b2-a31c456d8968
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	5cb6b154-8407-4648-a83e-f0f57c46509e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	cfc6a6a5-f8df-4d5a-9d41-2ce97956eda4
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b8b8cdd1-aacb-4511-8875-14fe766042b6
Muzaffar	1	1	2026	0000	Fetched products for shop	176d8450-ba54-4e61-8345-0091a1544d92
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	b638cf91-6419-478c-9c0b-966097489780
Muzaffar	1	1	2026	0000	Fetched products for shop	5c4b8a39-690c-4f02-87c6-546e592b36fb
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	6a576f73-2283-4a7b-8041-666afbda758b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7aa199c9-d1d2-4119-8aed-1e57bd66b90e
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	66d6262f-306f-4728-9b00-916feb3d380b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	03d22767-f18e-4504-b570-a19692715c54
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	8d5a5fa9-8a36-4b82-bf77-30ab3151ca91
Muzaffar	30	12	2025	Muzaffar	Fetched all sales	c836d418-c3d1-4b5f-a0a9-77ad50153089
Muzaffar	30	12	2025	0000	Fetched products for shop	05174619-51ff-437e-9352-80f0c9a6ff43
Muzaffar	30	12	2025	0000	Fetched products for shop	b93e4d1f-9129-46ce-b000-59d444aff8c6
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	d10ee425-4b9d-4c21-be5c-c1ca04b86e44
Muzaffar	30	12	2025	\N	Fetched all brands - count: 3	d891f315-8535-4062-96d6-21e1d86f9af9
Muzaffar	30	12	2025	\N	Fetched main finance statistics	77e59695-f8de-4159-914a-5aa97e87eaa0
Muzaffar	30	12	2025	\N	Fetched main finance statistics	e7b5bb56-701b-4835-ae26-d8c2431ff982
Muzaffar	30	12	2025	\N	Fetched week statistics (last 7 days)	ce133072-ee64-44ea-a0cd-ec1a40a04450
Muzaffar	30	12	2025	\N	Fetched high stock products	40a70e38-c318-4ea1-866f-f08257c9ea7c
Muzaffar	30	12	2025	\N	Fetched week statistics (last 7 days)	769bc9d0-f19e-4860-a1d6-bef01594023d
Muzaffar	30	12	2025	\N	Fetched low stock products	f81294db-6f07-430c-ac7a-1973a1195c69
Muzaffar	30	12	2025	\N	Fetched high stock products	12f80c11-90af-4620-a841-1d5cc6b7b856
Muzaffar	30	12	2025	\N	Fetched low stock products	da560146-1985-4438-89e7-087b9ef58bc0
Muzaffar	30	12	2025	0000	Fetched all debts - count: 5	daac0dd5-a269-4cf6-b313-31baef16383e
Muzaffar	30	12	2025	0000	Fetched debt statistics	7a4a1b44-1abe-4af0-9342-07872d58ab5a
Muzaffar	30	12	2025	0000	Fetched all debts - count: 5	28bd154e-9c33-494c-9131-05e5660f0ce2
Muzaffar	30	12	2025	0000	Fetched debt statistics	0be2f946-992d-45c7-968f-fb031de65e43
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	ce4f4a99-c38c-4f16-b14c-e80f6773f7e2
Muzaffar	1	1	2026	0000	Fetched products for shop	518e8d3b-cd88-4841-a30c-2d764b110b78
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	b3bcc4b2-4290-490a-939a-fd0ef5a032eb
Muzaffar	1	1	2026	0000	Fetched products for shop	a98ff666-1c6c-4d15-a007-3e179799a275
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	1e2c3ea6-c9b4-4a8e-bf4a-c020ad616a4e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f7683316-a7d4-44a6-98ad-4f4a629e23f6
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	f586c22e-d4e1-476c-8721-7d071615ce00
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ec246552-8278-4db3-a894-86d3cea44637
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	14ce939a-638b-4d3d-a7e6-9d1b854a046e
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	7483a051-4a0f-4d45-8144-96f865141345
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	96493d93-5b4c-47bc-bbb6-4dbb4ac62a1f
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	0ff82809-5c07-4f5a-968f-36141f70d96c
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	72f971a7-448d-4c76-ade5-50e18e6c1bf7
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	e29c15aa-cfbf-4e27-ad9e-a99bd944bc6b
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	327a6025-d241-4020-bb87-7f6d97360787
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ac8409c5-8e2a-4384-a70f-d589ca4aa49d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	9a526912-7386-4ea4-a585-103df4c8c38b
Muzaffar	1	1	2026	0000	Fetched products for shop	2b976799-46b9-4e01-8fd4-7afc7bff940b
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	4649b188-85fd-4da5-b12f-c457c9b26756
Muzaffar	1	1	2026	0000	Fetched products for shop	68974100-db63-427c-b2b3-48922d91e725
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	43b5fccf-05ea-4bf2-b439-71541fcd8f6e
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	1bef98b4-b84d-426a-a18f-e4939a3a8beb
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	85d0939e-3cae-465d-82ab-5e9742c50cc1
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	a123de18-2ba3-4f9d-8441-f53d01010fc6
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	b242212c-2597-4320-805a-f9441779d501
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	51ef6de0-d382-4046-9fae-3440bfc1e15e
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	05c105bb-de4f-46a9-ad9f-2dd259ab39ea
Muzaffar	1	1	2026	0000	All admins fetched	bf8c192e-6c55-4dbd-82f5-dc8b0d48eb56
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	d7ceafb9-9a46-412c-871f-cff34289dd0f
Muzaffar	1	1	2026	0000	All admins fetched	60d49b21-7852-4d18-af42-821f7b550d65
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	6781d2f9-d09b-4528-9e3c-a0ad6176fd84
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	2d09d0d3-627c-4edc-b240-d84b1758eaa1
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d827f6c7-3ba6-4636-9e60-2488f219640d
Muzaffar	1	1	2026	0000	All admins fetched	f3efedd0-f7ec-4193-a683-eb34990c040e
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	83b7cdc0-fb13-4808-9074-32c42a5667bb
Muzaffar	1	1	2026	0000	All admins fetched	6dce749d-bd06-49a5-bf0d-56718e7ec8e2
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	f8103c3d-160f-4ae3-86bf-942d5c69b7dd
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	69c857b7-1a33-449c-9d0c-398eb270d911
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7e7991dd-6dd5-4950-9111-fd2aa4a578ff
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	e26bac26-d00b-461c-99ba-0e37371309e8
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	8c67617e-4002-4c8c-911d-ddd906252857
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	3d051ac0-72fd-4ea0-9541-ed85a47899aa
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	0e6b0998-65ff-4128-9960-281505c083d5
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	13b28f4d-d73b-4794-84e7-7079fbb0eaf8
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	3bbd5949-8e20-47f9-a8e8-0785f65eb14f
Muzaffar	1	1	2026	0000	Fetched debt statistics	9f4b7ae6-5de1-4d84-acc8-fbea13b1b29b
Muzaffar	1	1	2026	0000	Fetched debt statistics	67155cbd-7ee9-485e-aeb4-ed6978edb4d0
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	15fcf2dc-4fb0-45a5-9c13-eb2e2aa39cc7
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	08b0f0e4-6085-43d7-8cf6-8fc0f326e475
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	751a690b-4887-48ba-a389-8de6a3fd1093
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	7c2bb093-3626-47e9-b5a2-fb8b75cd10ce
Muzaffar	1	1	2026	0000	Create branch failed - error: invalid input syntax for type integer: "01abda99-fe1d-4cd0-88dc-4527defb3a94"	d1a9afce-bf28-4d40-8da2-0016e6e131ee
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	18167091-9f56-42fc-b342-a1dc17175867
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	f06adb31-c0d0-4474-ba03-7edb62159872
Muzaffar	1	1	2026	0000	Fetched products for shop	c14e27fa-22e1-4e7b-b78a-c77059cae438
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	37912f2e-ca7c-44bd-be8d-7fe800a16475
Muzaffar	1	1	2026	0000	Fetched products for shop	1ce62407-d359-420a-afba-0d0a19055913
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	869ce25e-82fe-4468-9f6a-5cd6e2f9bb66
Muzaffar	1	1	2026	0000	Fetched products for shop	331c7ff0-0554-4fce-91ae-4e3c3c45ba05
Muzaffar	30	12	2025	\N	Database backup downloaded - tables: 13	c22e1a19-db61-4f74-87d1-7aabd4e89186
Muzaffar	30	12	2025	\N	Database backup downloaded - tables: 13	a2d1d7c8-0b51-4fd5-9124-0cfb28ab5439
Muzaffar	30	12	2025	\N	Fetched main finance statistics	e9095545-3e12-4044-b4ad-793dc07a8cbf
Muzaffar	30	12	2025	\N	Fetched main finance statistics	132ea9a8-844d-4171-9fa2-e066366b7b0c
Muzaffar	30	12	2025	\N	Fetched week statistics (last 7 days)	9328536b-8dd5-45f5-b284-84f1d4ef8d56
Muzaffar	30	12	2025	\N	Fetched high stock products	671e49b5-80e3-4083-a9e4-59e0d4fb9bd4
Muzaffar	30	12	2025	\N	Fetched week statistics (last 7 days)	147e99d1-5abf-4f94-b722-d397475b1a92
Muzaffar	30	12	2025	\N	Fetched low stock products	9814b23a-a4f7-4d58-822e-a8ad179ad071
Muzaffar	30	12	2025	\N	Fetched high stock products	fb38c7f8-6491-438d-8d79-985e3f6b4a35
Muzaffar	30	12	2025	\N	Fetched low stock products	1a3b0f3b-12a6-4c31-8306-8ec3d6a8f794
550e8400-e29b-41d4-a716-446655440000	30	12	2025	\N	Superuser logged out	535339c4-1651-4844-815a-30d9057e9538
Muzaffar	1	1	2026	0000	Branch created successfully: New	4afd0d18-43f2-40e8-ac3e-e867266f4794
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	e308e8f4-eb98-45a9-b488-b042b0eb0a15
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	6a46af80-1a39-4130-80c7-24867259a0fe
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	4c75b530-35e1-4a61-baf4-5561d655cd34
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	8c4b80f0-b4c8-4a5d-bf2d-0753739521da
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	10cfe749-052c-430c-bf5a-458cf686b86b
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	36105cee-4656-485f-869a-843c83951f0c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	2c4f3c9d-39c5-4b81-9fcd-ef9bcc985d6c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c20a1ace-f342-4019-a5f1-64c7d8fd5b2f
Muzaffar	1	1	2026	0000	Fetched products for shop	0402b362-79f4-4b37-8a18-2a8c90527f4e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	74cc8f99-41bc-4f10-9870-17e46dfb33a8
Muzaffar	1	1	2026	0000	Fetched products for shop	7f5a303a-160d-4035-be7a-1ac0370c1df8
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	e23fefc6-853f-42e7-abb9-d4688c7c5346
550e8400-e29b-41d4-a716-446655440000	2	1	2026	\N	Superuser logged out	a86f664e-c0a8-4b27-8a5c-64574172e3e1
Muzaffar	2	1	2026	0000	Super logged in	3759ff4c-f531-4ae0-bfa9-a3d310a4d7b3
Muzaffar	2	1	2026	\N	Fetched main finance statistics	5566f058-76be-4583-8b38-e083630076ed
Muzaffar	2	1	2026	\N	Fetched main finance statistics	ccb633dc-acdb-48d7-96f2-05c778707a37
Muzaffar	2	1	2026	\N	Fetched week statistics (last 7 days)	574e2f25-cc7d-41be-b147-9e65a0bd8a1c
Muzaffar	2	1	2026	\N	Fetched high stock products	7dc09e0e-4c3c-4085-b528-edd94c01f7e6
Muzaffar	2	1	2026	\N	Fetched week statistics (last 7 days)	40afd020-321f-4fe9-96ad-36cdfbfcf100
Muzaffar	2	1	2026	\N	Fetched low stock products	0e00d398-a301-45b0-84e0-86d598f591ce
Muzaffar	2	1	2026	\N	Fetched high stock products	b0c4e2cf-cf13-4099-ad65-972e6a8f796b
Muzaffar	2	1	2026	\N	Fetched low stock products	c2c34301-dbf8-4404-90a5-9ee4b4c520e0
Muzaffar	2	1	2026	0000	Fetched products for shop	33103fb2-6679-4a94-9313-b3a0019dcfb4
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	90a4a737-74af-4a4e-810d-6b89e76b659e
Muzaffar	2	1	2026	0000	Fetched products for shop	87c9d30f-ba02-433c-9c38-9a666c61a4f9
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	532564d1-d13f-4e5e-b674-4b476c38def5
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	1c8f6f2f-979a-4264-a320-6db28aa1a0fe
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	699f3e09-09c1-41bb-a596-eeceb4fd9642
Muzaffar	2	1	2026	0000	Fetched products for shop	390f7c89-0da5-428c-9dd9-a37ac478e920
Muzaffar	2	1	2026	\N	Fetched all categories - count: 3	2a86433d-9f31-4f70-a2d5-ac9701f3360a
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	d8fb9646-f08f-453c-8827-926c1de018e1
Muzaffar	2	1	2026	0000	Fetched shop branches - count: 2	4dd3819a-7bea-4bd8-a187-b70782a9a743
Muzaffar	2	1	2026	0000	Fetched products for shop	8472062e-e803-457f-8a37-9787a9f30dcf
Muzaffar	2	1	2026	\N	Fetched all categories - count: 3	5440e93a-3478-4055-8ae2-8c7c2e626e83
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	5d07b107-cb7a-40ca-9e8f-81023b1fd81a
Muzaffar	2	1	2026	0000	Fetched shop branches - count: 2	1710e9b3-6635-4d92-838d-39a2965e05a1
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	51af7bea-124c-4506-bbae-52a8363508f8
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	30be5c04-3d43-4ea1-b336-dcdd77458719
Muzaffar	2	1	2026	0000	Fetched products for shop	c0ad5bc9-40fd-4137-a344-bd27358c930d
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	47e3ee4e-fdea-4b99-9448-e292b1ed3c13
Muzaffar	2	1	2026	0000	Fetched products for shop	20a759e4-b1cc-4d40-b996-612920012f58
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	a493d6d8-2b13-4800-8963-66e96c95776c
Muzaffar	2	1	2026	\N	Fetched main finance statistics	851ada3a-4283-47ad-9f40-12d6ba9a51a0
Muzaffar	2	1	2026	\N	Fetched main finance statistics	78e5956a-6c4f-42ca-8661-1907768280ba
Muzaffar	2	1	2026	\N	Fetched week statistics (last 7 days)	17ffe146-5299-4c19-bb75-28d0d58e31d4
Muzaffar	2	1	2026	\N	Fetched high stock products	3d9d0b8e-6d6f-4c97-8f25-977330bd7a9e
Muzaffar	2	1	2026	\N	Fetched week statistics (last 7 days)	c4d41e79-8afc-46ba-bce3-b3d2a71ef9a2
Muzaffar	2	1	2026	\N	Fetched high stock products	1ef1fd38-4910-4f89-b750-9b94874784d9
Muzaffar	2	1	2026	\N	Fetched low stock products	3b81168c-1175-448b-9b13-2dcc94442384
Muzaffar	2	1	2026	\N	Fetched low stock products	2517700b-70a2-4e39-bd41-50ba2aa1cb10
Muzaffar	2	1	2026	0000	Fetched products for shop	c58c9337-8762-40b5-8217-2012b1b60326
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	736a8584-ea1a-4aa8-a19a-512d3a6c774c
Muzaffar	2	1	2026	0000	Fetched products for shop	731b0c44-dafa-4eeb-b561-715258c44be2
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	4303a659-01e5-451c-85f1-8a4adba4fcd1
Muzaffar	2	1	2026	0000	Fetched products for shop	31669c42-e720-48c5-a7fb-fb810afc90ce
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	5be63fb8-916f-490d-bf52-d6eb4a0b6ef2
Muzaffar	2	1	2026	0000	Fetched products for shop	e716c395-699f-433e-9968-6b6209a4f3a3
Muzaffar	2	1	2026	0000	Fetched products for shop	23340138-481b-4f96-a6b6-c80f7cd55a57
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	819d5efc-9b6b-48bf-8b5d-ec50a9479b75
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	88362eb0-134f-4a05-a0ff-6a40d72db105
Muzaffar	2	1	2026	0000	Fetched products for shop	1827e13d-1052-4506-83ad-4106f4bf57a7
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	2912fd4b-5b47-4133-a0ba-c3eae528ce2f
Muzaffar	2	1	2026	0000	Fetched products for shop	e93cf776-0a71-46d1-96a1-01db695812fa
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	42bc7d49-65b9-46c3-a74c-a6125d56667a
\N	2	1	2026	0000	Sale created successfully with sales_id: d1be09e3-e1ba-40c2-9b54-835d8beeb70b	47495a04-3a8e-4e8f-bde9-684ccb3d7205
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	a03f6cb7-9f94-42d7-b759-c9b6900bf21a
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	878020b0-21f7-47e3-94e3-f0cedf3bc2b9
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	f907980d-481e-4172-ad1f-6bab86a058ef
Muzaffar	1	1	2026	0000	Fetched debt statistics	7f1d9299-e4d7-4ee1-bc45-053e8bc06c0e
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	447f0c59-b90a-4909-bae0-fd4d65d00800
Muzaffar	1	1	2026	0000	Fetched debt statistics	f9fca49e-cb75-4a35-a1cb-c9ffa45d3b36
Muzaffar	1	1	2026	\N	Get unreturned debts failed - missing shop_id	e6bb878d-de48-48fe-9338-a335f61f3160
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	a619ca37-8989-4295-ac3a-4a9692c54fda
Muzaffar	1	1	2026	0000	Fetched debt statistics	0dc35e7c-eabe-4e27-8a88-e74e6c5c52e6
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	d17f2edc-025a-484e-b91e-2151577a965a
Muzaffar	1	1	2026	0000	Fetched debt statistics	6c2ee389-0e1e-43f8-9090-563afb5d26c0
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	c5398f33-7a55-4bec-a2e9-70e91bfeae52
Muzaffar	1	1	2026	0000	Fetched debt statistics	da419ed9-a9e3-4c5f-af5c-ac6933a61c67
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	1e08ec5b-f143-4af6-8337-4cd6e32e4d1e
Muzaffar	1	1	2026	0000	Fetched debt statistics	376709dd-eea2-4c29-a70e-3050912eaaa6
Muzaffar	1	1	2026	\N	Get unreturned debts failed - missing shop_id	fa5a0b21-6f29-414f-8f25-29b9b4abc7c1
Muzaffar	1	1	2026	0000	Fetched debt statistics	1dfa7a6d-34bc-42a4-bddb-9aaa76d368d2
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	9b196f6d-564a-46c5-a874-42b1d0e2b456
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	9e38604f-7d1f-430e-bfd8-7e9a5dd097b7
Muzaffar	1	1	2026	0000	Fetched debt statistics	eff9fc8c-ee9a-4a24-ada2-c48cfb9f1720
Muzaffar	1	1	2026	\N	Get unreturned debts failed - missing shop_id	ed0c6ec9-f79a-4fe4-a0ca-6a642ab5cfac
Muzaffar	1	1	2026	\N	Get unreturned debts failed - missing shop_id	55895a4a-cb74-43de-8008-83857387bdea
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	5195eb69-8501-46c0-8f4b-952633878926
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	8425ec2f-6ff4-4d26-acc2-838f49a74f06
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	69458c52-cf11-45e8-aa7c-b9058ba09e1c
Muzaffar	1	1	2026	0000	Fetched debt statistics	46c7f870-0b16-416a-9492-ea47b2112fe2
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	cf51ce9f-5264-40ae-b278-eaa6bd04efc0
Muzaffar	1	1	2026	0000	Fetched debt statistics	b6dda06f-8f78-48cf-9c0f-72da9d83a7a2
Muzaffar	1	1	2026	\N	Get unreturned debts failed - missing shop_id	d71dd716-32cb-4a36-99ef-497531780202
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	33b123b6-1e2a-48b8-9ac6-d79d15818f89
Muzaffar	1	1	2026	0000	Fetched debt statistics	8566ceb0-9abd-4354-a9d9-89e9d4afadd1
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	18b6ab02-65aa-44c8-a746-dead4df7953a
Muzaffar	1	1	2026	0000	Fetched debt statistics	8b75214f-cc3f-4c07-b128-dba1c74b699f
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	1fcf1930-05e2-4a98-885a-fda1960d4e72
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	dc73dbfd-59d3-4a1d-a448-39c59535491d
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	d87cb03d-6a44-42e2-92d1-66279a2fde7a
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	b95a36df-2f06-4205-93b3-76425dcbb0ba
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	d1e40dfd-1498-4249-869a-8f916a9bacb0
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	dfff789f-aec8-4621-9cc6-c4c3d15b9de8
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	54c3d47e-4e29-422d-bbcb-93b39f19108c
Muzaffar	1	1	2026	0000	Fetched debt statistics	8c3f3c70-346c-4eb2-bd30-36699e4657c8
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	90010f99-216e-471c-b900-eb9be16f8200
Muzaffar	1	1	2026	0000	Fetched debt statistics	d4e9f76d-16a3-4935-963f-2b8270e013ea
Muzaffar	1	1	2026	\N	Get unreturned debts failed - missing shop_id	4f120406-808a-485b-b794-7a779afaa3bd
Muzaffar	1	1	2026	\N	Fetched main finance statistics	d27e2a53-d265-45a6-a686-dbf5dff4349d
Muzaffar	1	1	2026	\N	Fetched main finance statistics	7cb67014-1c9a-4f9b-a587-dd959c4b8886
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	35db670e-79b7-4005-9d90-81e5d7d1d515
Muzaffar	1	1	2026	\N	Fetched high stock products	b2449121-9abc-4dbf-91e8-e169fb43bbde
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	5d6ed106-065c-4ec8-a8eb-a668c54fd1b8
Muzaffar	1	1	2026	\N	Fetched high stock products	f8115f10-b03f-4e1e-8be5-37bc8b91c350
Muzaffar	1	1	2026	\N	Fetched low stock products	a3712e6b-8e91-4af5-aea9-e5deecddfbf7
Muzaffar	1	1	2026	\N	Fetched low stock products	7799e69d-d33a-4d9a-97aa-45ed2ebe893c
Muzaffar	1	1	2026	0000	Fetched products for shop	09e53300-4346-4135-91cc-fb4e08d1afcd
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	18bc3733-17d9-4ca3-97ef-d11d6700b8ac
Muzaffar	1	1	2026	0000	Fetched products for shop	e24f0a3b-165a-4c11-8f35-1a586a2f90e0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	748d4a9d-d128-46ca-b269-a52edca16ea5
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	bdf38c7c-b5e3-49f6-a4ad-0ab6e1de9b39
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	928e448a-6510-4cd1-b035-1537aca32990
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	080cc46d-3ab9-41c1-b96f-b23586235132
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	c007c3d8-8e49-41cf-b157-5868fc7eb501
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	90b1b34e-410a-42aa-802b-c39078f85736
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	6c750c01-4943-47c3-82f8-688bdb029e03
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	2033c83e-5a3c-458d-a88d-096414a03579
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	160e3625-56dd-4ba9-bb22-b4f80d6f0975
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	be19bd2c-dd30-4a8d-936a-ab3c9f7de8fe
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	57e840f9-b7c2-49ec-861a-a9e42425289e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	3d2bdb92-1cd4-49e1-9359-a02a5e7d4526
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	bd2c6a92-e2e1-4674-a3bd-cce99a29070d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	39fa4a06-9040-4280-adb9-32644dc564c8
Muzaffar	1	1	2026	\N	Fetched main finance statistics	f74bea92-7f47-4323-9006-169da15fc559
Muzaffar	1	1	2026	\N	Fetched main finance statistics	bbd2c205-2da3-4372-ac31-8bf37cda63b7
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	7e4695d1-b279-434d-afd8-bf5915e236b6
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	c177feee-70d7-4c6e-87f2-e4156011133e
Muzaffar	1	1	2026	\N	Fetched high stock products	c3233e70-95c5-40e0-b42e-eb63e2489d34
Muzaffar	1	1	2026	\N	Fetched high stock products	eb88367a-0ce1-4d29-8c0b-84a76df064b7
Muzaffar	1	1	2026	\N	Fetched low stock products	fb6d2929-32ea-46f5-a943-868393e25c50
Muzaffar	1	1	2026	\N	Fetched low stock products	7bc1bf4e-c341-40d6-97bb-f1ce110135a4
Muzaffar	1	1	2026	0000	Fetched products for shop	c70d0ded-b220-4675-8c77-6c335799de93
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f6449004-4498-4133-b8cf-9db32b409955
Muzaffar	1	1	2026	0000	Fetched products for shop	96854eee-4e06-4ed5-97e7-5a87674c99fd
Muzaffar	1	1	2026	0000	Fetched unreturned debts - count: 1	bd6f4ab6-bcca-49a7-acf7-5da57abfa37a
Muzaffar	1	1	2026	\N	Debt marked as returned: c1c7b0a2-d99e-40a7-9142-40d965baeba7 - customer: Xayrullo	0482e963-7fb1-44a5-aace-a161d04e3f29
Muzaffar	1	1	2026	0000	Fetched debt statistics	cf45ab55-72aa-416a-94db-d874115c055d
Muzaffar	1	1	2026	0000	Fetched unreturned debts - count: 0	43ca4f71-d2d6-4378-ba61-4506bf3fd9d6
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	3727aabc-fafa-48ff-a41b-e1a6563f566d
Muzaffar	1	1	2026	0000	Fetched debt statistics	573b887f-691d-4eec-bec4-4b29f0d32bfb
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	6ce92867-3f28-4f15-be18-f5be962880b0
Muzaffar	1	1	2026	0000	Fetched debt statistics	b9715089-e08f-4237-a7e1-9b5d72fe22b9
Muzaffar	1	1	2026	\N	Debt deleted successfully: 6c2923c3-b064-4943-bee3-faf8a947871d - customer: Muzaffar	e6fe50d1-8061-4975-a08f-1d57220f2f6b
Muzaffar	1	1	2026	0000	Fetched debt statistics	7aaa943d-fec6-4e41-b2e0-4938b446ea1e
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	9d6228a5-ce0a-4c07-9322-d3c96f8b278e
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	c0fb26b3-da92-4754-afc8-5a2befb20652
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	7c563d00-9247-459c-9cf3-2d72f469bed7
Muzaffar	1	1	2026	0000	Fetched debt statistics	e3a7c8dc-2c04-4e38-8f89-2bbb448204f4
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	5c0c67d5-1992-4561-b76b-ae74d5d32f5a
Muzaffar	1	1	2026	0000	Fetched debt statistics	ae082129-1eb7-4864-afbc-dd68921ddc58
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	df1971cd-96fe-4415-97dd-6906bf37752b
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	5709515e-8295-4207-870f-98e0adf6bf6c
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	74c59cb3-c90c-454f-8851-3a3fcec347d3
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	577579b4-794e-40eb-b232-f3e19b0c7a3b
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	fe41c7da-a7e9-4121-813c-136a3251a06d
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	1cd791cf-cfbe-4cc0-a635-4614eac2a6ec
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	8e060296-4c7f-463c-b18d-46f5ba5a49d9
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	3e3b7065-3abc-4a04-8558-3eaf7d10beca
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	c9f083f2-3979-406e-a9cd-69cb1b5eb0ff
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	73fffe80-6dea-4e00-9767-4640673a6008
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	efbc15db-0ef0-43d7-a872-25fa1f57b02c
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	3cdd5e66-fefe-4feb-9656-a227883529ce
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	390a4024-e0e0-4576-beb3-4c4b7c1c5706
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	bb3cfe75-e3df-4fc0-9e02-e1e01a24759b
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	bbe50b54-115a-4177-91f2-a39997daac05
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	190a12ef-855c-4af3-9bab-5fecccb68bf4
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	4db80664-0007-441b-90dc-8adf239f02d9
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	479426b5-cd55-4854-8f4d-aa41f39caa50
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	b163ee03-005f-43a1-8b2b-811914960f4b
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	f82c04b0-abff-445b-9f9a-f52e12b9b07b
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	dd99a3e1-83a0-47e5-bf13-c6173ed52b5d
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	9affec2f-cf60-4317-a8aa-9dcb4d14af03
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	541f8c06-2082-46b7-a63d-66553f831a1d
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	06456839-6bd6-4b08-9558-0bfa804e92cc
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	00edb835-d968-4733-944b-223de256f2b1
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	7180ccc8-4da2-4507-b9b3-abdbc7fb5e9a
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	f59c5fa8-6578-442c-8eae-2a20ee720be4
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	61305437-5d97-4328-b033-143c81a390d1
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	5f163179-8021-4cb9-8794-154d055474eb
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	bd9f1563-d64e-4fd2-acbc-68d505e1b93d
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	46a09c44-4fd3-43c1-8b5d-0fc7b578994c
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 1	ba523c5a-b411-429c-b60d-7e10744fe49b
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	a864b689-e29c-49f8-bb8e-b671a15f302e
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	8fd48011-5be1-4d73-9f5d-1f8a50513441
0000	1	1	2026	\N	Shop updated successfully: Oqil boboda	3e4c5f48-bc77-4d6f-9cf9-ac72967bd4bf
Muzaffar	1	1	2026	\N	Fetched main finance statistics	3d2f5017-fefc-4f2c-9686-515e2c844821
Muzaffar	1	1	2026	\N	Fetched main finance statistics	9cb58d1a-9206-4ce7-9a9a-71f606fac42e
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	d4b635d0-30c2-49c8-acf3-26beaaa803a5
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	9d7d2ff4-f22b-4e95-b049-682d58fdc13a
Muzaffar	1	1	2026	\N	Fetched high stock products	c97e27bc-208a-4538-8a40-d343a75fde68
Muzaffar	1	1	2026	\N	Fetched high stock products	819b3651-a672-4dcf-9547-7a1cff91a4e5
Muzaffar	1	1	2026	\N	Fetched low stock products	896f7624-75a0-459b-95fe-d8a512713d41
Muzaffar	1	1	2026	\N	Fetched low stock products	9b830b85-f543-4796-9206-edbbc7643e69
Muzaffar	1	1	2026	0000	Fetched products for shop	80c6b462-0016-47e4-8990-1c6e39ec5019
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	82e1a223-63ea-4ed7-b331-a0c4ed4ee9bd
Muzaffar	1	1	2026	0000	Fetched products for shop	9b14cbe0-edb7-46ac-89bf-ad4e4fec850f
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	9e45669d-d446-4faf-a0dd-dcded9dc02f3
\N	1	1	2026	0000	Sale created successfully with sales_id: 73a21334-f928-4345-b5e8-6d006f1b97c8	5d2c28ab-e1c8-41a2-9473-d3b4d4cbaf45
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	df54153d-ecd3-4f32-a9c4-7eacdce1cad6
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	73f220e4-eb4d-4cac-8a47-9304599fad3e
Muzaffar	1	1	2026	0000	Fetched products for shop	c47b0d3f-8a80-4b2f-a702-f5b0653cb046
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	f09df877-5c1a-4e46-8a0b-8cbc8bf6df5b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	9ce6b394-2329-40a8-8cb4-d8e6c3ff941a
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	048f5dfe-b4f9-4df7-886e-2672ff2a755f
Muzaffar	1	1	2026	0000	Fetched products for shop	cccca73a-fd5f-42ed-9f73-1dece8898843
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	3ac998e1-ee98-45bb-a7ab-74c08b408a4b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	5e89c93b-723f-455e-8802-068fcf97bfcb
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	365a0a16-43a7-4111-b7eb-2b40e29df290
Muzaffar	1	1	2026	0000	Fetched products for shop	df1b786d-9da7-460d-96b5-d4b25b56d42c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	8a4edfc0-12d5-44bd-9a5a-149272dea0ce
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	21f89b83-8472-48a0-85cc-29898482a959
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	b27d92f0-0a62-4ddb-81ab-c6fb5583b6aa
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	3bd34d38-b919-482f-8923-8e897cd5dd1c
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	920e052d-6031-400c-b0c9-32b977df729a
Muzaffar	1	1	2026	0000	Fetched products for shop	34bd0ad9-bb52-4ed9-bf22-de2e41bbeb48
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	cbb0a718-a3f3-4386-9198-648455c14c12
\N	1	1	2026	0000	Sale created successfully with sales_id: 1b112df1-a9d3-4319-be30-888acf11ae26	f3759a0d-67ec-4ec4-a306-64b1dd1b3fc8
Muzaffar	1	1	2026	0000	Fetched products for shop	4ba518f3-6573-4b4e-ad5a-72adaa3bd75e
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	ce45d681-e9e3-4306-9dd3-f955cebbe46d
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	4977baf7-e7cb-4626-a0d1-08f46767fe22
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	1b2d56f1-5418-4034-a464-ffd35efdbafd
Muzaffar	1	1	2026	0000	Fetched products for shop	c899b795-81b8-4194-a2ad-b71e538e258c
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	d77642f3-745e-4de6-9bdd-30b16492e321
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	9aa7972d-9dcb-419b-b144-8da176c972af
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	b4134a38-39c3-4586-8c34-daaf05c82eb3
\N	1	1	2026	0000	Product created: 117b6de9-167e-4092-8f77-43ed091bea0a	780f64bc-96ea-4d4f-b7f4-63780dec3466
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	655043db-5565-42ba-bb49-6b8a01749979
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 56	a5679010-64fc-4033-a973-cb6ffd952526
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	3112310b-d42a-4f63-ac69-b4897426dc27
Muzaffar	1	1	2026	0000	Fetched debt statistics	434b7bfc-5d02-4204-af01-27c49e7270dd
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	00903c85-fd31-466b-9fc2-b9947c7309a2
Muzaffar	1	1	2026	0000	Fetched debt statistics	fb9c8b03-c592-40e8-9b3f-291c48981832
Muzaffar	1	1	2026	0000	Debt created successfully - customer: Davlat, amount: 2000	189dbf5b-abe9-44c3-8314-d447fe420345
Muzaffar	1	1	2026	0000	Fetched debt statistics	f1bbc47a-546d-4db7-a29a-015287a6878a
Muzaffar	1	1	2026	\N	Fetched debt by ID: a1d481b5-95e3-4c9c-9cb3-8d7aa415d07e	509e04b6-da49-41a9-bec0-666da9c20462
Muzaffar	1	1	2026	\N	Debt marked as returned: a1d481b5-95e3-4c9c-9cb3-8d7aa415d07e - customer: Davlat	f67c5ece-0f48-4076-b3ca-1377968c19ee
Muzaffar	1	1	2026	0000	Fetched debt statistics	b3d94126-b64a-47ac-a8fa-ad21d23562ed
Muzaffar	1	1	2026	\N	Debt deleted successfully: 670ae07e-b524-4251-a5d3-3cb2605f37d4 - customer: Kamoliddin	92f5241a-9588-445c-b006-7c7221e43958
Muzaffar	1	1	2026	0000	Fetched debt statistics	1e412d16-5ba1-4ff3-90df-1757df117dec
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	8d8026f5-679c-4332-abbc-0ba11015e42d
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	a3c76840-8336-4f05-9ef2-cd53a6a6b7f5
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	65172475-8e07-4886-b2c4-e24f6ed6b9a8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	f74f06ee-fe77-4a70-9547-d38635ccb18a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c293ad34-8f15-4807-8e49-888fdd624a01
\N	1	1	2026	0000	Sale created successfully with sales_id: 4a9d4b29-9ed3-4c77-abf5-e5d11c4d685b	c09fdac7-8ce1-4ede-bc97-a2c3a8cbc5a5
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	11abc7bd-513d-4002-aa42-c2d0e8c0fd21
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	d575e9da-c9c1-4ba7-a96e-b54d559c90d9
\N	1	1	2026	Muzaffar	Fetched sale by ID: 4a9d4b29-9ed3-4c77-abf5-e5d11c4d685b	4af15175-3ea8-4607-b194-c010751d3f2e
\N	1	1	2026	Muzaffar	Fetched sale by ID: 4a9d4b29-9ed3-4c77-abf5-e5d11c4d685b	fe241636-6e38-4651-ac83-e1c9645920ac
Muzaffar	1	1	2026	0000	Fetched products for shop	85ab9ab8-e3e4-44a4-8bf5-2f7ad9938f0b
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	db1cf23d-800b-4500-ace3-0d8fbab3ef42
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	91462a41-e6a2-40f0-9253-0fa7ee82963f
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	4a819b61-505e-4784-8d94-c0d3c4e186ed
Muzaffar	1	1	2026	0000	Fetched products for shop	1388ad69-8ef6-48bb-ae42-98666b5c0ffc
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	798ddad0-b54a-4fa8-bba1-3088dfb15b3c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	94a3e4d4-0d6b-4f2c-89d2-e997992f9e9e
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	53db6e79-2e0f-452d-b26d-b575561efd2f
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	f1ad0121-b03b-401f-8548-8c9a600a41ef
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	f2b6287e-3e06-47cb-b7b1-d3d10105060c
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	123b8b31-1727-4cd9-8164-b78d592ba9ea
Muzaffar	1	1	2026	0000	All admins fetched	b3512b52-64cc-4f08-b6a5-172f4178cb29
Muzaffar	1	1	2026	0000	All admins fetched	0b19095a-a53f-42e6-8596-b7ca40e47c3b
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	fc90a25a-3f11-4acb-acd6-80e7200bffbf
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b345cd0c-ca83-4568-a8b7-35e955dd227e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d489e91b-1310-4374-8a60-cc05c3d166ec
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	4a9550dd-690d-4a0d-b336-3fea015e5d37
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	1642d824-7ea6-490a-871c-3154f39fa73f
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	e4e1a5db-d7f7-454b-92da-feaf3a404a53
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	f969bb1a-37be-4c33-a541-418cb2416a4b
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	d8f13277-76eb-4a08-a585-b2beb9d3b452
Muzaffar	1	1	2026	0000	Fetched debt statistics	d40dbfa4-e2fb-46c8-8c15-ccd715d2097f
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	e37efc51-011a-4fe3-a587-937912a9385a
Muzaffar	1	1	2026	0000	Fetched debt statistics	b166f87d-4eb4-45bf-a9b8-2d7d43f5fa09
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	429f16db-eff5-4803-85cc-3771f8b1732d
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	730e2587-4110-4103-8145-930cae40bbf6
Muzaffar	1	1	2026	\N	Fetched main finance statistics	a97cf41c-525b-463c-bbe4-b0579c8300c2
Muzaffar	1	1	2026	\N	Fetched main finance statistics	686e6a19-baf0-434a-afe3-4e37e0b69427
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	ce79e3ea-03a0-4af3-bfff-91f5c35ad5cb
Muzaffar	1	1	2026	\N	Fetched high stock products	9e17b321-c6cd-4610-a161-f551591a158c
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	122d461f-5a1f-4e8e-b7c7-1f5f51da2308
Muzaffar	1	1	2026	\N	Fetched high stock products	1e202eb1-0c0b-4c49-bf6b-b701a402d944
Muzaffar	1	1	2026	\N	Fetched low stock products	f4baecd5-46e3-4b50-9c02-fe9708756a78
Muzaffar	1	1	2026	\N	Fetched low stock products	f4579f98-a093-4c9e-9734-1dcb2bba3f38
Muzaffar	1	1	2026	0000	Fetched products for shop	96cf9326-34b0-4273-8958-4d055066ceb3
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	4422e05a-b867-4910-b10f-f1bc63018ec8
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	d2192cbe-d83a-45aa-80a7-b5c3af5dce72
Muzaffar	1	1	2026	0000	Fetched debt statistics	96eced64-a1ec-4ae9-91ed-f76d9fabbac8
Muzaffar	1	1	2026	0000	Fetched debt statistics	87d5f9b3-3d90-4322-8c38-42367854b75f
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	c73af2ae-fec9-4c24-8950-528dadb277b4
Muzaffar	1	1	2026	0000	Fetched products for shop	72a43d87-8ed6-4e2e-8faa-9b15e5a9284c
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	d6866e01-d890-4f14-beeb-89f8e9fa8e50
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ff70fe12-c41f-4c0c-b248-c23b03ba7040
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	027c6a20-d224-4ac3-af37-c07ee564ac45
Muzaffar	1	1	2026	0000	Fetched products for shop	8c78a661-d213-487b-afb7-b4527e650773
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	68ebc8d6-3a91-4bbd-b0ed-41ddc4925937
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6f562dd0-7fc2-48bb-8c45-19924163e390
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	3e1c4947-1206-4b70-a5bc-dc946716432d
Muzaffar	1	1	2026	0000	Fetched products for shop	63b80887-b112-4bee-9a13-e509452a8be3
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b8b39e7b-a28b-4dce-92e5-6f2a6502a470
Muzaffar	1	1	2026	0000	Fetched products for shop	3bd35018-9851-428f-affc-cde7741c6132
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	eb2956e6-b385-464e-9035-0bb043e561ac
Muzaffar	1	1	2026	\N	Fetched main finance statistics	b436727c-4f59-40c6-874a-349aefc7978e
Muzaffar	1	1	2026	\N	Fetched main finance statistics	600032d8-aacb-44c9-b868-140ba4eadc39
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	42ad465f-ec7c-4142-9d18-781dcde060a0
Muzaffar	1	1	2026	\N	Fetched high stock products	8e7b27c1-d783-4895-a96b-ccbd7fce7d06
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	2ffd6eb0-7b40-4735-9c80-6c8531b5de7a
Muzaffar	1	1	2026	\N	Fetched high stock products	05d3e565-39b6-43b1-8188-aa23757df8b2
Muzaffar	1	1	2026	\N	Fetched low stock products	60d91bfe-3932-433f-bfed-6df5913d4acf
Muzaffar	1	1	2026	\N	Fetched low stock products	9d82a3e2-4185-4eb4-8f23-fcf30fa63c1c
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	e98f3063-56fe-4b32-b293-1e2e7985ff71
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	8a68026e-1ab9-448a-8843-db2125062ee3
Muzaffar	1	1	2026	\N	Fetched main finance statistics	77e93dba-26a2-4772-96eb-d7500d0cb379
Muzaffar	1	1	2026	\N	Fetched main finance statistics	ec6c2963-c448-4f98-a6ca-3b424d5691d0
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	7c21cfcc-8ab2-48da-8e48-1c0f4ac1cbe4
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	6fbc7a43-88ac-4bba-8ffc-b2485ebd3fdc
Muzaffar	1	1	2026	\N	Fetched high stock products	9f23bdfd-5385-49ec-bfaf-d3a7c8a1ceb6
Muzaffar	1	1	2026	\N	Fetched high stock products	6a8e3d71-5b0e-4b5f-84a9-6fdad7d39eec
Muzaffar	1	1	2026	\N	Fetched low stock products	93d51cd9-422c-4e1c-8fc1-a3745ce30ea1
Muzaffar	1	1	2026	\N	Fetched low stock products	2b1378fe-d376-4894-867f-5f7f6f3008b9
550e8400-e29b-41d4-a716-446655440000	1	1	2026	\N	Superuser logged out	4c0ca057-aa66-4246-98fe-d1756109124b
Muzaffar	1	1	2026	0000	Super logged in	0c9434ed-d7f5-478b-b8a4-1d92a1bfbb62
Muzaffar	1	1	2026	\N	Fetched main finance statistics	cf9ad87d-f7f3-4040-a6b6-5a4ff6535ea5
Muzaffar	1	1	2026	\N	Fetched main finance statistics	2fad7450-5b8a-4804-af72-749c1b42cc23
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	b4e37841-66d3-4771-b4b5-7c5afc659b18
Muzaffar	1	1	2026	\N	Fetched high stock products	17e6112b-ef3d-4be4-80f1-0ee44045fa7e
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	d0532714-76f4-4823-834b-bc51c7205fb4
Muzaffar	1	1	2026	\N	Fetched high stock products	b5901eaa-a192-4a50-a37c-a2ef7252df29
Muzaffar	1	1	2026	\N	Fetched low stock products	2c9042bf-ecad-4f65-9b7e-6d3548a085ce
Muzaffar	1	1	2026	\N	Fetched low stock products	56991cf9-8896-4354-9b1d-62ac04ae8191
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b37d62d9-3a85-4a11-ac14-d923bc5ab86b
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	6f4f7816-7d2d-447b-b2c6-ca3fb75672b6
Muzaffar	1	1	2026	0000	Fetched products for shop	143223bf-7c87-4fdb-9f35-6be73311099e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7a0f7e06-47e7-4615-ba86-ed9a944db5f2
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	06863aad-4f2f-4225-87a4-6d1c1548345a
Muzaffar	1	1	2026	0000	Fetched products for shop	ed575014-7a66-44d7-bd42-9c7c68c13dd8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	0e6e749e-a713-4916-b9a0-4ab52d0745cb
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b496e24b-c9ef-4b4c-a88a-0e7757ffbafe
Muzaffar	1	1	2026	0000	Fetched products for shop	b33f1a19-a897-414f-a396-fa189b843d4a
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	5f1fa2f8-bd45-43ca-81ec-506630a0871e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6ecf0827-1c87-44a5-b529-13587ec41427
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	9bda1f13-20fa-41c5-9687-a8277c7fe1f2
Muzaffar	1	1	2026	0000	Fetched products for shop	2f59c07c-04ee-4a7f-8672-71639f79ee88
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	349c788c-f547-4cce-9449-0abf2bfecb05
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	895bcf33-3240-47e8-b0fe-92492d4b85b1
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	cc5e6dfb-9744-415a-9907-30f049d158ee
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	edc0a19f-b0ce-4272-84c7-3955c395ad1f
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	3be9be7c-0372-4456-bb7d-dc5a71256742
Muzaffar	1	1	2026	0000	All admins fetched	c51644e5-de0e-4d5a-a3d3-077a7a34607b
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	18c9534c-944b-4b48-a9f2-175b69475fdd
Muzaffar	1	1	2026	0000	All admins fetched	97c85b32-b2c5-45ac-ac90-d99f5a3cee9d
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	6e91a2ed-a941-41c9-bf61-8bd9ecbe4473
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	8074ec4d-9456-4c09-91bb-7f1fb3b025f4
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	9c350182-fc24-4515-a1e2-91d315397434
Muzaffar	1	1	2026	0000	All admins fetched	58d932d4-afcd-489a-a3d6-b092a81c951d
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	a9b86e9c-635f-426d-b168-1d87d33d3a0d
Muzaffar	1	1	2026	0000	All admins fetched	80ae404e-2b1c-4d12-ad50-0fc85644c56b
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	a74e426d-d127-407d-b826-bc42e9bf6dd7
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	b3410de3-e5ff-4a04-a2a7-bbc17704c0b3
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	63484857-21e8-4ea6-a54e-12a48321df86
Muzaffar	1	1	2026	0000	All admins fetched	b3ae7ea3-ceaa-49f3-ad4b-3a006ec49946
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	e1512292-cebe-41d9-ac64-8cf9d5d05681
Muzaffar	1	1	2026	0000	All admins fetched	c0944fc0-9bcc-4d7d-b601-223874781cab
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	d833a604-6d3c-45bf-9531-5a7f8b590a8f
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	6f10707d-2bae-4809-b412-8380591fcb2d
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	0996cb2d-c142-4a8f-97a1-08327b8cca09
Muzaffar	1	1	2026	0000	All admins fetched	483eeafa-8486-4325-9a5f-76ceae8494ac
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	062b9df1-c9de-425a-9b50-58e520e11783
Muzaffar	1	1	2026	0000	All admins fetched	203f0420-202d-47ac-896e-d8686ec6f89a
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	39f3efe1-9f03-4065-a04d-e69e153bf2cf
Muzaffar	1	1	2026	\N	Fetched main finance statistics	bbe0fd53-c5e6-4266-b718-5ce2409abe8f
Muzaffar	1	1	2026	\N	Fetched main finance statistics	12354b07-a0e5-4647-9351-a077d64bdf79
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	f8607bd9-3141-4925-ac6d-78084854b9f6
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	0645c7e9-5bff-458d-9879-2f6d4a6125e2
Muzaffar	1	1	2026	\N	Fetched high stock products	91bc4547-c3fa-4b79-a506-72c31bea8d84
Muzaffar	1	1	2026	\N	Fetched high stock products	21bc8bbe-9da5-4c1c-bacf-b248fb57a834
Muzaffar	1	1	2026	\N	Fetched low stock products	5c79b683-98b3-4b8d-898e-121648334ac4
Muzaffar	1	1	2026	\N	Fetched low stock products	71914255-dedd-49ea-9a4f-66d3e487eb63
Muzaffar	1	1	2026	0000	Fetched products for shop	ea4482fc-9e19-42dd-ad02-2fba0f22aab3
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	4e5ddec3-1b1a-4e54-938f-4ffa915388cd
Muzaffar	1	1	2026	0000	Fetched products for shop	3b90f8a1-39a5-4399-ad61-4b2c82a5e189
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d38f9c8e-28ef-4013-bb17-d67d819bb92c
\N	1	1	2026	0000	Sale created successfully with sales_id: 1ef0bd11-30d2-46f8-b0e6-7c0c3ebe3e89	12168e51-bfb3-4506-84a5-deb99029b8da
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	8ab7a035-d1f8-4a2e-a29a-dae214617c63
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	31fd1841-870a-4226-9694-d27a3b84ea46
Muzaffar	1	1	2026	0000	Fetched products for shop	70e401c0-febe-401e-b52b-77782c771e1e
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	c4928a85-2508-4352-b5bf-abbdabe8793a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ce34c989-3768-4bd4-a237-eb1cf0695d2d
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	ea7ea58f-e5af-47e3-9fd1-51439c0ebcec
Muzaffar	1	1	2026	0000	Fetched products for shop	74bfa613-cee4-4079-abbf-5fee636ebe24
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	970cf3a0-76b9-42c6-a961-49d356755cfe
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	563d7677-9868-4dcf-8c6d-824a10383f9e
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	50558fb0-8434-4801-afba-e370b49e104f
\N	1	1	2026	\N	Product restocked: 2b40b1b1-5532-422b-80f5-335513df1286 (added 150)	edd4a28d-8105-4335-923e-42f080e86d64
Muzaffar	1	1	2026	0000	Fetched products for shop	203a8dc6-8a0d-43d3-81e8-ab7e923bfa63
Muzaffar	1	1	2026	0000	Fetched products for shop	0d298146-1ff7-49a6-be5c-9d947fef8820
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a765cad3-f718-4d2c-8e6d-9550a3825487
Muzaffar	1	1	2026	0000	Fetched products for shop	932cc0bc-792f-4216-a376-3cb56247506a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	37c43408-9135-4f68-b42d-f3375add7006
Muzaffar	1	1	2026	0000	Fetched products for shop	21a7e13c-d648-4c2c-a014-49cfe930ddfb
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	145bea9c-628a-45cd-9bd6-cb9ede923788
Muzaffar	1	1	2026	0000	Fetched products for shop	b7f5d0cf-52ed-4f87-9a45-28c8602cc874
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a99d5a04-7ec6-4f5a-93f7-07121fb41c56
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	e23516ee-7519-4505-b6ac-cad7e687c10c
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	c18cb8af-fbb8-41b9-9154-bf660465cde2
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	ec6494b6-90cf-450e-8cea-26cca083896d
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	7c879726-5d33-4a1f-9f91-19bd2341b84b
Muzaffar	1	1	2026	0000	Fetched products for shop	69b6f983-ef59-4e2d-887e-264439287451
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ff771f6b-84de-4754-a86a-4b941b4a732d
Muzaffar	1	1	2026	0000	Fetched products for shop	5caa30c3-5a6b-434c-9448-3d8603a89516
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ae3591f9-5ce2-4d16-8a92-01a3f39c5c24
\N	1	1	2026	0000	Sale created successfully with sales_id: 399d091a-2afc-4133-aa76-197ac782f0c3	c152bf90-36f6-46b5-b357-06606b0c5c72
Muzaffar	1	1	2026	0000	Fetched products for shop	e06fcaee-7dfb-4ce3-90d9-1036d132eded
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	aab563eb-4be1-4137-a0b8-d51cce878296
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	0a14f453-8234-43f4-af4f-d5e7c1cc6641
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	2edfbd7e-d03e-415b-b075-be6e7b354b6d
Muzaffar	1	1	2026	0000	Fetched products for shop	7df1efaa-4dfe-42ed-a699-ead4d0eff9a3
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	6d91e161-14fb-472f-b5de-0e0bc4612739
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c07b97a2-3d71-4217-bf54-f2cffe943e60
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	44130ea7-e5d1-4555-aa0c-3ac2977e3393
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	6019f790-cc72-430f-ab00-14071c23d71a
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ce194f1d-4ff2-4f81-96a3-557b7b49a508
Muzaffar	1	1	2026	0000	Fetched products for shop	9a7534fa-02ee-43ac-af8b-dfdeecac982d
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a96f883a-c814-441d-b697-0bcc0bee2fc6
Muzaffar	1	1	2026	0000	Fetched products for shop	e68b5d5a-862f-4260-914a-40d3b175709a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	2acdfdc5-405b-4ed1-8e58-08cbdb7fe2f0
Muzaffar	1	1	2026	0000	Fetched products for shop	8ee9f962-88bd-4995-8747-544222be173a
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	dde7dd50-a40d-48a7-8f7e-135d67a9fc3c
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	f7c9be84-fdff-4197-8a75-e3a44176bfd6
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c6778545-5860-48c6-8c20-66eea4b81bf1
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	766f6d66-d36d-4aed-bca6-56b4797fc8a2
Muzaffar	1	1	2026	0000	Fetched products for shop	9af45807-a75d-443a-bebb-c23f0e031652
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	131f01e0-d8ec-4b00-adfa-af45167faaf0
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	49378626-91c4-4f3e-bd19-8c7e5ea3d4bf
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	6969b870-0b6a-436b-b3dc-091d9f53bdfe
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	eba092b0-54ad-4fbc-9125-f09818e9d524
Muzaffar	1	1	2026	0000	Fetched products for shop	1f417444-d228-4b9a-97ec-200f5168dc99
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	99410fbc-e17e-4eee-824a-84a037deea5e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	cd5f41e3-8cb7-4d87-b89c-7e3ff3935220
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	dd0cb572-097e-4096-b4be-d7f3f990afbc
Muzaffar	1	1	2026	0000	Fetched products for shop	26a60d77-c3df-4e54-a1cc-c8b7f7e74392
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	e775c450-f345-438d-8c06-7ce1c35b62d4
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	847b0f03-4e92-4947-90d7-3cdab6e33e97
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	dea092c7-9b2d-4ed1-8fdb-12603ba0ac54
\N	1	1	2026	0000	Product created: 90efd7fc-1711-4e46-9a55-c7a40bbb41e5	46b3f89a-f69d-4b58-8639-e70d29edfc47
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	4a6e4bdd-be6d-440b-b967-b27b728c28d5
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	9a74c128-662b-4003-b751-021ac576b858
Muzaffar	1	1	2026	0000	All admins fetched	3647c7cc-de88-4d43-9e50-72ddf3638b07
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	9767279c-604d-49e3-84f6-f0d864204907
Muzaffar	1	1	2026	0000	All admins fetched	89ecc338-91dc-4494-be17-c6255d257c59
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	26eeddf8-de1c-4413-ad67-a005eded3c28
Muzaffar	1	1	2026	0000	Admin updated	77d80cc7-c2b5-415b-9286-e6e1bc884c6a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a4c89f8e-27f1-49c1-8a3c-713d07f95e3f
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ef94dc52-56d6-49da-90b4-4a1871734899
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	170e1f4e-1e44-4cef-a410-9fe40fcf4a74
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	177d6054-ee23-4950-8d2e-bdf8178f10c3
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	49ffe8e5-10d6-40da-b3ad-e2d7e671ab57
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	8a61cc1e-b28e-4b30-86db-1226c15fdf04
Muzaffar	1	1	2026	\N	Fetched branch by ID: Yetti uylik branch	9fb3387c-fc9f-4a28-89b2-4642c76f029f
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	7c55f9b7-1640-4d35-ba3e-245e261fa291
Muzaffar	1	1	2026	0000	Fetched debt statistics	70df591f-5cd3-4c04-bd83-c1f3b51ab5d2
Muzaffar	1	1	2026	0000	Fetched all debts - count: 4	94fb3b2a-3cfe-4e43-b54e-8e1637f5ed2d
Muzaffar	1	1	2026	0000	Fetched debt statistics	93bd8086-e988-4cf7-ace0-341d6e1cb524
Muzaffar	1	1	2026	0000	Debt created successfully - customer: daVlat, amount: 18020.01	fb68020a-5a61-4210-8d70-731bd1c4aeb0
Muzaffar	1	1	2026	0000	Fetched debt statistics	2fc3d56d-a9b3-48cb-9046-fe9557c4a13f
Muzaffar	1	1	2026	\N	Fetched debt by ID: 2f88bbbc-fc93-480b-ada0-c77508ee2ac1	d81ba7a2-ab8e-4a40-9f61-3c81ef551b29
Muzaffar	1	1	2026	\N	Debt marked as returned: 2f88bbbc-fc93-480b-ada0-c77508ee2ac1 - customer: daVlat	bd69beda-b4fd-4008-adf4-2268e3b77c47
Muzaffar	1	1	2026	0000	Fetched debt statistics	757f19e0-b560-4e11-a108-7fb1543ea629
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	ee2a4aa2-51cd-47cb-a49b-559a2f04c76f
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	45ed9244-f497-4925-9eab-256ed6564c21
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	cb4dade9-812b-42d0-8e1a-a83378ba85f5
Muzaffar	1	1	2026	\N	Fetched main finance statistics	b9a0ea7d-0178-4b5f-8d80-e74564906cb2
Muzaffar	1	1	2026	\N	Fetched main finance statistics	82607e25-1651-4f9d-a00c-70e1a86a6e1d
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	6eb4db5e-69bd-46ee-b35d-a95ab0a445ae
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	468956fc-0580-4c3b-a17c-e312ac190108
Muzaffar	1	1	2026	\N	Fetched high stock products	6718f80d-4c2e-4c85-a6b3-e8acb780850c
Muzaffar	1	1	2026	\N	Fetched high stock products	be810fc5-5f62-4105-b9bf-8791d6a82a82
Muzaffar	1	1	2026	\N	Fetched low stock products	6118e5ff-e63e-4279-a761-4058a8bfcc56
Muzaffar	1	1	2026	\N	Fetched low stock products	518efc30-dd94-4002-be04-61aa3b0b584d
Muzaffar	1	1	2026	0000	Fetched products for shop	ea9daa62-bab9-406c-b0b8-2e6c8df748dc
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f9ceb86a-4b91-4281-8a2b-1ec9b85246bb
Muzaffar	1	1	2026	0000	Fetched products for shop	37ad754d-6e26-4ba6-a88e-ff9ed31fa0d6
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6abb39d8-b3fe-47ad-89f0-1924c6eb3a4c
Muzaffar	1	1	2026	0000	Fetched products for shop	02452753-76ad-4e0d-b735-340bab37a66c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a59b4e44-3518-4853-bfa3-7bb0d7100b19
Muzaffar	1	1	2026	0000	Fetched products for shop	1600e37e-6607-42f0-9b42-1b7b9a0e50ce
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ff302458-2ebb-4bfa-bda7-b912e55326ee
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	2c2f5b6a-cc03-4b3f-b0fb-b017d8147a29
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	de756f2b-85b1-45c6-aebc-bf307b214d21
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	4eeb2ee7-5167-4bb0-a63b-49c052894157
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	8051a14e-f00d-4fee-88fe-582960ea83c1
Muzaffar	1	1	2026	\N	Fetched main finance statistics	440d66c4-ffac-494c-9c7b-30200f79d507
Muzaffar	1	1	2026	\N	Fetched main finance statistics	3c0db667-ae86-4a2b-8f98-eececd92fb7c
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	087e9411-1ffd-4d48-919b-ac5183830ec9
Muzaffar	1	1	2026	\N	Fetched high stock products	a8616992-a22a-4d29-ba1e-1c4d2db83853
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	b87b6b0e-0589-4648-bee8-f2f687aa0f68
Muzaffar	1	1	2026	\N	Fetched high stock products	cdca42d7-9be3-4c89-ac39-b5123bb9561d
Muzaffar	1	1	2026	\N	Fetched low stock products	a176c922-fdb5-4296-9b71-3e92ef90eccc
Muzaffar	1	1	2026	\N	Fetched low stock products	e7e00040-378d-46a8-bd38-3a9760ca4dbb
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	e25aa0d3-0406-4467-a296-3b64309aa5b5
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	f6975c71-fc50-40d9-bf80-1408696be483
Muzaffar	1	1	2026	0000	Fetched products for shop	62b7ab44-4661-488f-93c2-fd7c84fb9622
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	9fca5ed1-9e12-4b50-88cd-0a338a532f2a
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	c46042f3-1fd8-492a-b62e-65b47f0e6148
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	84c3f01a-6467-4b66-8d42-ae30f60d775c
Muzaffar	1	1	2026	0000	Fetched products for shop	fc2a50c5-7eb1-475c-8e91-52c53ab2477b
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	92333369-17c3-4185-9454-79cb61e0e7d2
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	3378b4e5-1783-4279-a53a-b739b075e027
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	c0839f4b-87d4-4c3f-a04e-73098d1bb0e4
Muzaffar	1	1	2026	0000	Fetched products for shop	9d6857a9-5c33-4d1f-b2c1-d2caace7195c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	5474ff8c-9c8c-4528-b057-e6c0abf414ca
Muzaffar	1	1	2026	0000	Fetched products for shop	db2ac398-5f50-4b8d-b144-dba222fa778b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	e01931ff-3385-463c-bdfd-09278f663f45
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	95de5585-ceba-4543-9776-2ee10c2f39f9
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	2192d9f1-8cc7-4f79-bfca-5a27d6fd2691
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	76df79b1-8c12-418e-9cb7-026c5dcff45a
Muzaffar	1	1	2026	0000	Fetched debt statistics	0145f5bb-ebee-49ab-aa4b-f39c3d69cac0
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	9e728bfe-8181-49b0-9d18-12be1c154b5f
Muzaffar	1	1	2026	0000	Fetched debt statistics	c314449a-7e32-4a7b-94af-7d0902fa4c24
Muzaffar	1	1	2026	0000	Fetched products for shop	078f047e-5c0c-44b6-9302-b027e41c88a3
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f70e9b81-f8e0-4070-9c08-34c546788656
Muzaffar	1	1	2026	0000	Fetched products for shop	a3e9c7e3-60ed-4e1f-b917-0fd1606a0e40
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6ad3592e-b100-4c68-ac73-22f99d49ee9b
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	9640cfe5-dae8-4d01-a788-2b7843c2d307
Muzaffar	1	1	2026	0000	Fetched debt statistics	c164b2d9-1a9d-422e-b273-ffb54f9c9fd4
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	2206736f-3ff8-4820-bc20-d199b983798e
Muzaffar	1	1	2026	0000	Fetched debt statistics	b901165a-f4d3-4723-b480-9cdab28d5a0e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	86fc129a-e5ce-46ab-a80c-f1aa59e1c4b2
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	c57a2704-271e-44cc-9720-e462f64d789d
Muzaffar	1	1	2026	0000	Fetched products for shop	69fa995a-2048-48f9-8ddd-0d378feafe39
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b9f38591-23d5-4c04-8377-a541314892e1
Muzaffar	1	1	2026	0000	Fetched products for shop	113f787d-707d-4b7a-a4cb-fd7a61ad5461
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	568d531b-6f77-46db-b550-a855a9e1b444
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	6ae42aaf-3c1a-4b75-a101-a45297278a7b
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	4b58c76d-70c9-49c2-a849-4ea3122706f7
Muzaffar	1	1	2026	\N	Fetched main finance statistics	dfe6488a-c0ed-46af-ab52-016a801b1df7
Muzaffar	1	1	2026	\N	Fetched main finance statistics	46705303-7ae0-4cfe-b9a0-a1b4f7e20b67
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	6eb1b43b-ea7d-4593-ad44-010e04504a40
Muzaffar	1	1	2026	\N	Fetched high stock products	2c644775-59fb-4473-bbc0-45a5de555218
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	47cd942a-0cba-456f-b967-b3005b4943cf
Muzaffar	1	1	2026	\N	Fetched low stock products	96dd2121-6f1f-488e-ab9a-0cb27ad221ae
Muzaffar	1	1	2026	\N	Fetched high stock products	dca78d67-bfa1-4e50-a4b3-1a05df5c5e63
Muzaffar	1	1	2026	\N	Fetched low stock products	597a3553-de22-4841-aac3-59322e20a2a2
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	1f4b0618-9d9c-4340-9a25-8d7e90173fbe
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	0b922e7b-b158-49c0-8637-6aab6bd64d09
Muzaffar	1	1	2026	0000	Fetched products for shop	616a1569-b8de-4a88-8bcc-77f5148cd694
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	c8a0fd28-1bd5-45ed-b49d-c39da76350b2
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	5e81be1d-b957-43cb-b5b9-20d416051be1
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	db304b1a-5b73-466a-a9db-cbe096d87486
Muzaffar	1	1	2026	0000	Fetched products for shop	22358ae2-49e3-4d52-ae95-c02351cd2008
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	98249624-afce-4e83-8658-7db9843964ac
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6b96da0c-13ac-422a-9f40-70656c4edef1
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	31dfbcf8-af29-4b75-af38-c0c9642c5f09
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	a43b4f5b-d1ba-4879-92ae-051b08bbea3f
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	0df80f8b-2db4-4df5-968e-72c47d83ae8a
Muzaffar	1	1	2026	0000	Fetched products for shop	f912256d-38f4-41e6-a0b6-4e797e33d20a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	9d7f0771-7b91-44ff-b574-d49a93c88665
Muzaffar	1	1	2026	0000	Fetched products for shop	cb6e8014-bc66-41e3-8b97-06c45ec1ff83
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	57112a96-0d73-4517-aa77-96a876a17dc7
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	08b9c080-b2b9-4430-a0f8-2e5e6997c274
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	143ce2bc-5538-4e32-a30b-919f5182148b
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	7337407e-d4bc-44c5-a6be-ffc8662bc4f0
Muzaffar	1	1	2026	0000	All admins fetched	73bbf68d-7f44-4a21-9e47-0c9ce0673e82
Muzaffar	1	1	2026	0000	All admins fetched	0c7ba49c-43bc-4a31-97b1-8a73f12cf4d9
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	f33df5ec-99fd-4bce-8b77-71b7326ac9bc
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	f8a32e7c-cf2d-4662-b53b-5d075376a2bd
Muzaffar	1	1	2026	0000	Fetched debt statistics	01f48f9c-e101-425f-a05b-1fdbd51edc4b
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	1ebaed7d-efab-43df-9eee-2bb550c6f5d9
Muzaffar	1	1	2026	0000	Fetched debt statistics	060da7db-6e94-45e8-a013-b1408b2dbce0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f35f08e7-e05d-424f-9f6a-4dc8412b1898
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d0e58974-74c9-49b8-a9a1-0cf87d6595a4
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	76c5b72b-a28c-4476-8e8d-8da688b2d52a
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	8c1e81d4-01f3-4edd-9eb8-4f8db5c9ad0c
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	ebbe7911-9ad6-48ff-bf20-080fcec94313
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	b2bbff9a-3f10-430d-9256-fea7548c3a27
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	5c360588-c6b5-410b-b93e-b955741b21c6
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	74f2b70a-93ab-4e71-a16f-466ab1d3dd05
Muzaffar	1	1	2026	0000	Fetched debt statistics	ab6003cc-1d7a-4c5d-a4f9-6b180bc71405
Muzaffar	1	1	2026	0000	Fetched debt statistics	530e0d85-b5fa-4d43-a55c-30d5d2085670
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	8af50756-aff0-4be3-9f41-0987419595a3
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	36b24d16-a805-42f5-83ba-bdbb9546887c
Muzaffar	1	1	2026	\N	Fetched main finance statistics	2d32ad0a-91aa-4e62-8bf1-538760b77f34
Muzaffar	1	1	2026	\N	Fetched main finance statistics	f6925252-7818-45c4-a196-9f3c7911e69e
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	e5df7853-a901-417e-ac2d-96064f2e4ed3
Muzaffar	1	1	2026	\N	Fetched high stock products	2f5f7e85-a2a9-49fe-afda-3d6f4cec9c86
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	a45463d3-e543-42a4-9981-f6926505741e
Muzaffar	1	1	2026	\N	Fetched high stock products	b996c04b-d700-49e0-8c97-357748b32463
Muzaffar	1	1	2026	\N	Fetched low stock products	8b7bb505-202e-43ec-a2d4-8a61e333d75e
Muzaffar	1	1	2026	\N	Fetched low stock products	70cda6a8-1df5-484b-b41e-623384637fc7
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	31cb471f-2d00-4813-b023-203024ec134e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	fcb71a6e-f461-4051-a9e1-7b2bfbaafc9f
Muzaffar	1	1	2026	\N	Fetched main finance statistics	91dd57df-26b8-49c4-a213-9dfb54ae746c
Muzaffar	1	1	2026	\N	Fetched main finance statistics	663ee3dd-3985-42b9-a577-11ae128e4e8b
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	641803b0-8d6b-4568-8d55-95645a465f01
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	34c1281f-a911-4432-89ab-7ad07495ffb7
Muzaffar	1	1	2026	\N	Fetched high stock products	6f701896-b264-4d66-9944-7fb733ef556f
Muzaffar	1	1	2026	\N	Fetched high stock products	e4e26af2-73a6-4411-8c51-ec65c7a7fa2f
Muzaffar	1	1	2026	\N	Fetched low stock products	404675bb-4019-4587-a33c-a58f9ffed956
Muzaffar	1	1	2026	\N	Fetched low stock products	bfa304f9-9511-4c37-bafb-724452aa2e07
Muzaffar	1	1	2026	0000	Fetched products for shop	f9c3460b-8441-4702-a42c-b6ddb1045247
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	1ff79593-8e73-4d6c-a5c3-00c3a74d48b0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	212b3890-322c-43f7-b3dc-d80fff18016e
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	827ba0af-2bee-424e-9357-bfdf99e1bf47
Muzaffar	1	1	2026	0000	Fetched products for shop	c7c694af-84c4-4fbb-86ed-40f5fe4baa4e
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	a7b8f928-9000-42e1-9b40-2f43d1644edd
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	cead5dff-e07a-4fe1-a741-38600ca01a95
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	ba5b3245-7a22-4aa6-9bc5-6ac901107c2e
Muzaffar	1	1	2026	0000	Fetched products for shop	f45589fd-142c-4afe-a4f3-9d92b91e8f47
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	98b3eb67-785d-4bd1-b452-bc695cd35882
Muzaffar	1	1	2026	0000	Fetched products for shop	c9b976e7-ab9a-45ab-9c62-9436a2165732
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ab00e025-23e7-4656-9dc4-9714484a8806
Muzaffar	1	1	2026	\N	Fetched main finance statistics	789d53e2-1435-481a-be3c-9751587dacb3
Muzaffar	1	1	2026	\N	Fetched main finance statistics	adca4771-069a-4534-962a-87c511909931
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	b66a5323-a13d-436a-b1dc-433c73d5774b
Muzaffar	1	1	2026	\N	Fetched high stock products	beb4a51e-1cbe-478b-b7be-62eda407fb55
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	5667d095-c336-484e-a98f-5552f3945619
Muzaffar	1	1	2026	\N	Fetched low stock products	c4acbdad-f0f5-471a-a25c-3ec04ad8120e
Muzaffar	1	1	2026	\N	Fetched high stock products	9131d5ca-f11c-45be-9c77-85399d186d05
Muzaffar	1	1	2026	\N	Fetched low stock products	97e05a4d-cce2-4eb3-84be-3b490767943f
Muzaffar	1	1	2026	0000	Fetched products for shop	a7a72c0a-b9c7-4289-b5ea-0ada67b5517b
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	e30f4cff-a107-42d6-976f-5461d21a6572
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d2d426e8-af2f-4af8-85e9-8d720090ef0d
Muzaffar	1	1	2026	0000	Fetched products for shop	ca20d493-ec06-422f-973e-43d6f6040746
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	709008b3-a827-4d43-9f3f-f6d4c946cbb2
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	d0b4f395-7f54-4b10-946f-b8de53803604
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	be62338a-bdeb-4310-b4f0-447396f83a7a
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	69c8a626-32a9-48c4-beaa-601b31889806
Muzaffar	1	1	2026	0000	Fetched products for shop	006b2345-e971-4ac2-98f9-96c627b6351f
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	37feaab6-1c78-47ae-9ce0-a81003d14521
Muzaffar	1	1	2026	0000	Fetched products for shop	a40cb09f-51c2-4049-84c4-22a7759aa86a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	1e2cbeab-d489-4a86-8aef-aab701e1b2d6
Muzaffar	1	1	2026	\N	Fetched main finance statistics	b966330f-3a19-47bc-8225-e71487212953
Muzaffar	1	1	2026	\N	Fetched main finance statistics	41212ce3-c4c1-4e05-9c85-7ab26adfd6c8
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	76e47a56-6069-4f21-ad6d-0f4ae00a8c09
Muzaffar	1	1	2026	\N	Fetched high stock products	d71d5b98-0dd4-498d-88ae-734e94142ceb
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	91617294-1d52-42de-9e59-13b467e3d244
Muzaffar	1	1	2026	\N	Fetched high stock products	0e579066-539f-47de-80d7-ec64a52ee51e
Muzaffar	1	1	2026	\N	Fetched low stock products	e49209fe-a668-4cdc-bd73-f5fcb6af8170
Muzaffar	1	1	2026	\N	Fetched low stock products	ef734978-6977-494f-a13e-4296420caa28
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	458a2852-367d-42d4-a11e-dc831c2a13de
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	8aad6e2b-fa40-4c93-999e-3691917abcf5
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	66b6dde5-ff82-41cd-97c9-ed25c362cef6
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	232d60cc-a5a5-4cd1-89f6-ee5491dee5e3
\N	1	1	2026	Muzaffar	Fetched sale by ID: 8ed4739d-d27f-4d1a-862d-6a0946d5d316	f45edcd4-84e6-47f8-a57b-551785d11867
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	6319301a-c4af-4f92-81c8-cb3dff6e8c22
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	6824e48a-764b-4609-bcd4-a5c90e2f42eb
\N	1	1	2026	Muzaffar	Fetched sale by ID: 8ed4739d-d27f-4d1a-862d-6a0946d5d316	c757446c-7c46-428e-913d-863951630465
\N	1	1	2026	Muzaffar	Fetched sale by ID: 3c2e6db1-c6c6-4a15-b01e-d671e02df760	5b411c5c-2cf3-4279-b54b-a2dccf9d01ef
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b6c5c3bc-6b6b-4959-82de-71b8068409c0
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	af459f3d-ba88-4f47-b0f8-28b2dbec387e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	1f608cc1-f892-47c6-b8c9-50913d5372a2
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	430d24b0-953b-4431-81a2-5e471249da39
\N	1	1	2026	Muzaffar	Sale updated successfully: 8ed4739d-d27f-4d1a-862d-6a0946d5d316	59d357cd-ebc8-427c-908c-442fc50d3969
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	57105e78-5189-4c0e-a664-2243f4654174
\N	1	1	2026	Muzaffar	Fetched sale by ID: 8ed4739d-d27f-4d1a-862d-6a0946d5d316	0b713b8a-cd9f-4274-b5c0-4976e056f42e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	8ede7dd5-db87-4907-b5b0-e037ed90d6e4
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	9db1368a-d86b-4011-a604-ab57d90d86a7
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	02978fc8-8563-44ee-870a-3307bb807f14
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	fa3f27d3-a6a6-4c0b-9c3b-799cf078093d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	9866afb4-f577-49eb-8976-b8a832674b42
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	30b63936-9166-4a1b-8321-c72feaa4b162
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	31d395a3-1229-446a-9009-00fd3f8e9978
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b9a2e701-d9ec-494a-a62a-7fa4fefe05e9
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	3cb365b2-383a-4ce4-9361-5e5387cae776
\N	1	1	2026	Muzaffar	Fetched sale by ID: 8ed4739d-d27f-4d1a-862d-6a0946d5d316	0b2348a3-4db6-44e4-950b-275d3f334b7f
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	9c5bb5a0-c0b0-4626-b6f2-58de6037c589
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	f5821201-20d6-4ab1-a5d2-fb88e3387083
Muzaffar	1	1	2026	0000	Fetched products for shop	59915517-9fde-460e-8c25-4c61c0372a03
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	f71d199e-d1f5-46e4-9050-739245e12db9
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	eca04f2b-d11f-4f0f-8308-b525231aa190
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	31d6c2a6-7222-4c66-880f-7736d3740a3f
Muzaffar	1	1	2026	0000	Fetched products for shop	225cf89a-d5a4-49a0-b067-01304fcbd0dd
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	40de7215-b996-4f9b-8779-157f6cd0d998
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7a36bbc9-ae72-48d7-b1d4-31619befad80
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	a2a3f8f8-847d-4277-99f1-54e19ad8e15d
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	6ab51e8c-c33c-45b3-9d08-4d9c83a73dbf
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	c7054b51-0ac6-405d-8bff-88dcba12f6c6
Muzaffar	1	1	2026	0000	Fetched products for shop	adfc8c44-71f5-45ca-902b-2a3791f0b698
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	74ee5764-f9ff-42ff-83e2-b5934aed2d69
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	07d837b1-ad5f-4742-8bd1-1c0df18cd315
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	7b8d7e21-e87b-4bfe-8e91-6f2a9fc67988
Muzaffar	1	1	2026	0000	Fetched products for shop	e6deb7cd-5248-4452-aafc-b550d780e98a
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	41c8798e-844e-45f1-a934-a11f560e80cf
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	28d5b513-f800-45dc-bac6-4cae0dcae34f
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	7ef67786-eee9-4094-8d88-4a39a6b75220
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	89cb1fa9-d822-4a8d-a574-1e70121b64e8
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	a86efccf-23cd-479c-a532-0e45eee7d350
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	c6b3dc42-3ae8-4752-99c0-0bf151756a64
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	d774dd12-925e-4b53-b7fa-6c7b3e2fb226
Muzaffar	1	1	2026	0000	Fetched products for shop	3647b368-4094-4567-9312-6530ed40d81f
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	3416c5f7-8f42-45a9-8cca-c8efe89a34bd
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	11030145-90ef-49b4-9122-00bdbac9a6f9
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	01da0b49-e1bf-4943-b83c-0c5fde536a77
Muzaffar	1	1	2026	0000	Fetched products for shop	b585f74f-8e98-4e1b-bc06-e7d02831d049
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	383a0d93-a771-4931-8325-11a220661b35
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	211fbd93-9436-4b7c-9169-5a7a64abed6e
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	01bcf934-77d7-4b6e-8362-4252baaad579
Muzaffar	1	1	2026	0000	All admins fetched	1b4bc6c5-8e33-4abe-9cb2-a53a3e4b000b
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	69c7b649-f8f2-4280-9f4c-8cee569b47ea
Muzaffar	1	1	2026	0000	All admins fetched	9669dd49-1fa6-4bb9-9cda-bab244e83279
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	8690227c-5a88-4bb9-96f6-f3f80e97b5e0
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	0961667e-01c4-4679-a73d-399da43f7f95
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	d3702ef3-72c5-4351-ac91-3f1db69ea329
Muzaffar	1	1	2026	0000	Fetched products for shop	df1a55b5-5eb7-47f8-a45b-911927cfb22c
Muzaffar	1	1	2026	0000	Fetched products for shop	3accc576-8ee5-413e-8494-eb725b769ded
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	842a36bc-dc9c-4376-9a70-7c036f9dfcd6
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	157e75a7-6813-4038-8ea2-ebb93a52b341
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	0de04541-d057-4db2-a2a0-80d2b363a252
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	e8daf5bf-4a4d-4163-8e2e-c1b7711437dc
Muzaffar	1	1	2026	0000	Fetched debt statistics	f559c43b-57ee-427b-9a9f-0d73d690cce2
Muzaffar	1	1	2026	0000	Fetched debt statistics	e7cec245-e04e-4390-bd07-41e93351aebc
Muzaffar	1	1	2026	0000	Fetched products for shop	2370d55b-b0eb-45b8-b335-fe9c4a8dae19
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	662220ac-8d3b-49a3-b7aa-d06420f76ff9
Muzaffar	1	1	2026	0000	Fetched products for shop	9335b6f2-f722-4709-93d1-816be1806839
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	cbf5ffbb-5925-479a-a5de-7e6353602ad3
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	97a7e082-0174-4183-afee-38036c59c6fc
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b95683f4-249a-41cd-a217-21baf91f30e7
Muzaffar	1	1	2026	0000	Fetched products for shop	8be5f3fa-7b12-4152-9b14-0ebdc18b07b8
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d86f3995-2dcc-4969-a76e-f715bd0011b7
Muzaffar	1	1	2026	0000	Fetched products for shop	02dc4cc9-6e7d-4ca4-9222-28d820a6b31e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ff8d0537-893a-4b3e-84c2-173200017c45
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	60509af4-ff9b-4d2e-98c0-7ce23100926b
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	016487b5-b4ef-46c8-9d95-1e8001076260
Muzaffar	1	1	2026	0000	Fetched products for shop	b3c3f1a8-a922-43e4-8879-69aa95a0750a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7c324ddc-3377-483f-aa76-ab746b1150e4
Muzaffar	1	1	2026	0000	Fetched products for shop	2b5c82bd-dc72-4a37-905e-a9deafb4942d
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	07df6f20-b269-4574-8ef0-ec8e4c9f6c33
Muzaffar	1	1	2026	0000	Fetched products for shop	720a3a23-467c-4b15-abeb-c0de0043c44d
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	d810100b-6936-4fc5-96cc-2c8f236ce13e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6d07eaad-9aeb-475f-a740-9af669b1875b
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	65fba880-60c9-48a5-8bfb-19fb19f3d118
Muzaffar	1	1	2026	0000	Fetched products for shop	a6a0f541-742f-420d-9d4e-20dc444c6350
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	3a0e0310-77f0-4e6e-ae59-3209236e8cfc
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	0673e65a-039d-4a68-9ea2-aa256f119a89
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	ffbab301-7017-4de9-b552-e27e7db152af
Muzaffar	1	1	2026	\N	Fetched main finance statistics	09d6dc30-c2ba-449b-8c1f-e5a5688449fa
Muzaffar	1	1	2026	\N	Fetched main finance statistics	f355d565-ceec-4071-90d6-ccadd1dbae70
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	8fe8d50e-6cd6-498b-bcca-5cabbcd4c3d2
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	85512d8f-1801-4bc3-8999-730b57914b45
Muzaffar	1	1	2026	\N	Fetched high stock products	33dbd362-c8c1-45b7-b85e-7c806f30d207
Muzaffar	1	1	2026	\N	Fetched high stock products	da97cbd2-7f01-4fda-9f2c-4c99ac792cd3
Muzaffar	1	1	2026	\N	Fetched low stock products	b6019032-6164-476e-ba05-f732b2528aa7
Muzaffar	1	1	2026	\N	Fetched low stock products	c4c7b82d-655e-43ae-9fa3-eef43f626874
Muzaffar	1	1	2026	0000	Fetched products for shop	468363e0-91fa-408c-9c8a-85ec7a148800
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	14c4274a-43e7-4a5b-a899-c91f6af157ec
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	72472cdb-b9cb-4b08-9cb1-9b181f8de5a5
Muzaffar	1	1	2026	0000	Fetched products for shop	a152876d-d0ad-45f1-b095-1aceac5b2d84
Muzaffar	1	1	2026	\N	Fetched main finance statistics	a10b132f-7278-4e38-964c-2faae8c4cbba
Muzaffar	1	1	2026	\N	Fetched main finance statistics	a59b13f4-cbd4-4717-b8a2-53ecaa53caaa
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	018122d0-291e-40dd-8234-b764fa62882a
Muzaffar	1	1	2026	\N	Fetched high stock products	d1bc0e21-7149-4ba1-9eb4-32f581d9341a
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	724dd311-c616-452e-b342-67068249704f
Muzaffar	1	1	2026	\N	Fetched low stock products	bc241707-2552-4ab4-8912-5a38a2c66e44
Muzaffar	1	1	2026	\N	Fetched high stock products	f6ab4c6b-8ce2-4e7c-946e-52b4aa796552
Muzaffar	1	1	2026	\N	Fetched low stock products	d9ce56de-9856-422d-ae58-faabeea7d7f6
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	40c4c5ed-3901-4c2b-a00b-843e39c452ab
Muzaffar	1	1	2026	0000	Fetched debt statistics	36cef8d9-7089-4413-a170-dbaed649f91f
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	ffb27655-b33c-4069-b374-50a02dd3addd
Muzaffar	1	1	2026	0000	Fetched debt statistics	1d129144-b846-4717-99b3-32d65ed8a4a5
Muzaffar	1	1	2026	\N	Fetched debt by ID: 5364429d-0828-4eda-9e13-e1873e38784a	cf4d4acf-473f-41fd-b7c7-8f862bf60944
Muzaffar	1	1	2026	\N	Fetched main finance statistics	850ada66-be6c-4d0c-b7b9-0b1032154301
Muzaffar	1	1	2026	\N	Fetched main finance statistics	dc5aa210-7971-4ba2-89a3-5062b1ba9e91
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	92766433-7d22-4ab8-820e-13d5e07305af
Muzaffar	1	1	2026	\N	Fetched high stock products	0c2e55ab-59f4-433e-99e8-a9cc399a4761
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	05cb812e-e090-4b4a-b622-8e8a4193c148
Muzaffar	1	1	2026	\N	Fetched high stock products	c889ffb6-88c9-4085-859e-47833a4cbcf4
Muzaffar	1	1	2026	\N	Fetched low stock products	a59b4c01-a78d-43dc-9552-60cd32070b4b
Muzaffar	1	1	2026	\N	Fetched low stock products	4e8e9f23-1aa9-469a-9856-70a8aa9751c5
Muzaffar	1	1	2026	0000	Fetched products for shop	8e8a7c34-e0fe-4296-b4dd-9ca071840d62
Muzaffar	1	1	2026	0000	Fetched products for shop	74b13a11-dca3-4ccd-82e4-dcf1d5acccd0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d8836b3a-9a61-43c1-9478-e50cef6e64dc
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a58cf563-a1f2-4e5d-a7f3-9a7f02cf0512
\N	1	1	2026	0000	Sale created successfully with sales_id: f5e85c20-1282-43bf-a360-3ab2241a592e	706c4035-3d35-472f-93f9-a362e4de8ff8
Muzaffar	1	1	2026	0000	All admins fetched	36e10d69-a1be-47cb-87af-99547f00a686
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	2a24d23c-f1b5-48dc-89b3-d84212d10cad
Muzaffar	1	1	2026	0000	All admins fetched	1a84fd7e-9970-4e8b-a058-501031c766a4
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	db5a7375-9e48-40fa-ad79-e673ce65b277
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	233bd2b5-c9dc-437b-8171-d8b2fc6d6b39
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	e28c6517-596f-48ae-a398-3c2a0c5c72a0
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	16dd2a7e-6cac-4438-ad34-265d99ee28dc
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	17f1a914-bd98-4352-aec9-b18908966306
Muzaffar	1	1	2026	\N	Fetched main finance statistics	a86005c3-00f4-45b8-8cfe-bcb16b6ebf3a
Muzaffar	1	1	2026	\N	Fetched main finance statistics	302dd1af-cc58-4002-b6e7-e9cf41560b66
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	a017173c-6951-41c4-9cf2-19206f5c8f0f
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	c8bb348e-2a86-4c0a-be4e-c0d584e392b1
Muzaffar	1	1	2026	\N	Fetched high stock products	f7fc9058-a855-4f7f-978b-45731e4d606a
Muzaffar	1	1	2026	\N	Fetched high stock products	6299d540-d1b8-4d71-a232-07735ab1ac3c
Muzaffar	1	1	2026	\N	Fetched low stock products	ca3b87a3-af47-4d0c-bd0f-ccd0ed12cdb7
Muzaffar	1	1	2026	\N	Fetched low stock products	8d3c220a-1b62-456f-adf5-adb85fadcfe3
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	8ad775c7-17fa-433e-b0be-8826bdea72c0
Muzaffar	1	1	2026	0000	Fetched debt statistics	f195e0b9-265b-409c-9fab-ae18bdc66951
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	0f5b467a-9c5e-487e-9d20-8f59ab3ec380
Muzaffar	1	1	2026	0000	Fetched debt statistics	687defbd-7c31-4f5a-a478-ec8f967a8bd1
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	07edabd9-593a-4531-8816-c65a7928ab47
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	aa042ac3-2e99-453a-a5ff-6b312a7c9c7b
Muzaffar	1	1	2026	0000	Fetched products for shop	4431cd7f-bb1a-4a24-ab9b-ef4cff2dcac1
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	4a3b34c2-758d-4b91-ab2c-6145f02bbd05
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	8bb97e1f-832b-449d-9837-a875469ddf8d
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	b2c5301b-0d7c-4b7b-b116-3141bbd32a06
Muzaffar	1	1	2026	0000	Fetched products for shop	a4fec0b4-908d-4bf6-96e0-fa9a0ba3c4b9
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	ba3a1fe2-15e6-4bbe-ba52-e86142cc7bc4
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ddd61099-80a1-418f-9912-24cb5e2d94ff
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	5baa9ef2-a988-498e-8118-20778a12e3af
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	06be36de-e928-4871-97ef-2f81fd544a1c
Muzaffar	1	1	2026	0000	Fetched debt statistics	4779a7ff-5c9c-4fe3-b2ec-164eb17426e5
Muzaffar	1	1	2026	0000	Fetched debt statistics	e3cfb112-2692-44df-9d87-ab1954eb7b76
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	a901ffd7-b8e8-4046-bcfc-5d484d0e2fc1
Muzaffar	1	1	2026	0000	Fetched products for shop	787983cb-523e-4c96-851e-0994cf20d8de
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	a89d0375-28a5-4206-991c-7f7ff0553ffa
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d7af6dac-187e-4f7e-8567-5976371f47a8
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	2985ce89-2d40-4d29-8007-5d99dd6a39dd
Muzaffar	1	1	2026	0000	Fetched products for shop	87a7ec88-6cd9-421b-883b-d6a441b1542c
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	9f0ca129-37c3-42bb-8516-ea7b56e52b56
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	5b1644e9-cb92-4f26-9384-82a526f2476c
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	bcd75f28-6f08-4f05-acb3-b3f287820f0d
Muzaffar	1	1	2026	0000	Fetched products for shop	1df3b994-6178-4ad5-9780-fc1c73d86588
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	1de2ab6c-1647-4172-805b-7851bec3b005
Muzaffar	1	1	2026	0000	Fetched products for shop	ba6f883c-ced5-4ef4-81b0-ff9d79461af7
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a50582b6-b31d-4423-b7b4-01386abb4dea
Muzaffar	1	1	2026	0000	Fetched products for shop	2512a07a-f33b-4aad-8a9f-ce9b0a635817
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	3606b90b-fb69-41d4-994c-e7a3def12de4
Muzaffar	1	1	2026	0000	Fetched products for shop	3000f012-17b7-4177-80c5-338be0ae81e3
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	4f7fc7b5-8254-4c32-bbff-9b764eb6e540
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	27634879-8dcc-4df3-9ac1-f7ee10e5db50
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	9ccd0cb6-6be3-49b9-becb-e5734f90149c
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	842afce8-2c54-47da-94ee-0941103109b6
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	98ff1dee-2230-4146-8159-15911051d2ec
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	100aa2c6-2bf9-4291-a8f5-42e14e04e549
Muzaffar	1	1	2026	0000	Fetched debt statistics	e1a3fbb1-4c53-4804-b5be-42b91ba6680a
Muzaffar	1	1	2026	0000	Fetched debt statistics	e8d8aef1-aa36-4e49-bd14-303c3e2455b6
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	7982f21b-4049-413b-a708-acc1c29adad2
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	9df84828-4705-4ae3-b3fb-81247554b8d3
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	73735c48-ce42-4b57-b80f-1b7d61888a3f
Muzaffar	1	1	2026	0000	Fetched debt statistics	1c90a2d4-b8d4-401a-94ae-7bf9f5787874
Muzaffar	1	1	2026	0000	Fetched debt statistics	9eba1cf4-b830-4d5e-bf19-8683d5744e95
Muzaffar	1	1	2026	\N	Fetched main finance statistics	ac6354d3-5969-4d46-bcdc-226fbca88e2f
Muzaffar	1	1	2026	\N	Fetched main finance statistics	b923949d-1d21-4718-aa5a-6756176e7799
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	028f1b28-0a83-4533-b8a0-8d50a27461c2
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	cac03d5a-200d-46e0-8adc-e3a92d3aedc8
Muzaffar	1	1	2026	\N	Fetched high stock products	62eaf496-9016-4b47-873b-ad5fb76d7658
Muzaffar	1	1	2026	\N	Fetched high stock products	66875b88-0aeb-4d5c-bc1a-942bf9f67892
Muzaffar	1	1	2026	\N	Fetched low stock products	0421dd97-1785-42d7-9904-8c84f3212327
Muzaffar	1	1	2026	\N	Fetched low stock products	5e7d9466-7d6c-4331-baf6-57ea2f5f1186
Muzaffar	1	1	2026	0000	Fetched products for shop	e520d2f9-eb5f-433b-8c45-37b4e42d8e73
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	5912bdaf-2850-4be0-a996-9c27af6f64b3
Muzaffar	1	1	2026	0000	Fetched products for shop	05b0474a-49cd-44db-b4c2-131c0d2409e1
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	e0a986c3-22e9-4d87-a01d-ed234e9e067f
Muzaffar	1	1	2026	0000	Fetched products for shop	01d49a61-41e8-4ff5-846e-c53cffa7e1b2
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7c817847-aa88-4201-b41f-e8a6b2786d97
Muzaffar	1	1	2026	0000	Fetched products for shop	59dbcbf1-2c24-4285-93fd-b0847ba1f4f9
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	12060dab-594c-4468-82bd-29bb302ca9b6
Muzaffar	1	1	2026	0000	Fetched products for shop	cb111caa-3200-4409-9a49-64c19f693c44
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	8a10997f-6a03-49ca-a5b8-fc8c6d488edf
Muzaffar	1	1	2026	0000	Fetched products for shop	85be18e6-2efe-4521-969d-9b6782e60dd2
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6dcf1da7-c7c2-4afd-b5c1-3e644b74be0b
Muzaffar	1	1	2026	0000	Fetched products for shop	7c453eeb-db74-4777-af7b-eb813bb1ed83
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6faf54fc-09f3-4238-94de-a5b6315e9169
Muzaffar	1	1	2026	0000	Fetched products for shop	1cf1cd27-0826-4fc9-9a63-5b15597cb902
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6565e3d0-c1f3-4d24-aea9-d57e22dfb9d0
Muzaffar	1	1	2026	0000	Fetched products for shop	6857ecd5-2922-462b-9e69-8ea79605eada
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	86027068-4cc7-4027-9948-e6b74627f62d
Muzaffar	1	1	2026	0000	Fetched products for shop	5aaebd27-92a2-4a19-9aef-ff7e4d5812ec
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	013f4b99-6006-4c7a-aaa0-8dee320f766b
Muzaffar	1	1	2026	0000	Fetched products for shop	f8e69883-d3c0-446a-8d32-513c04305a92
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	cd5953ae-28ad-4153-8cc6-50f902ff8e11
Muzaffar	1	1	2026	0000	Fetched products for shop	22a25bda-7ad7-42dd-b33c-ebf429ad5af2
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7e091428-25d4-4cd1-a2e0-c2c79b2dd008
\N	1	1	2026	0000	Insufficient stock for product 'student'	4481855c-0f09-4155-b0f3-5600555b5f96
\N	1	1	2026	0000	Sale created successfully with sales_id: 9b769fa1-29bf-4ad5-9d7a-4b8b623848ac	1f83754d-5fbd-4642-8b65-6333d05496c4
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	58e57aba-4316-4bf8-b19e-48a8f0edf0f7
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	0bad1bb5-af9f-4371-94eb-78eb8f464e06
Muzaffar	1	1	2026	\N	Fetched main finance statistics	81c79efc-2394-42ce-9fc3-38209133aabe
Muzaffar	1	1	2026	\N	Fetched main finance statistics	d01bde82-93a5-4b38-8a6a-5cfff8160373
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	d26cfc9a-8820-4d13-9227-6344bb43c602
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	216ea2aa-8cde-42c2-9523-05a6c775c9f0
Muzaffar	1	1	2026	\N	Fetched high stock products	c64aaf7a-52c3-4eca-aad2-f680a2742eff
Muzaffar	1	1	2026	\N	Fetched high stock products	c0faaae4-4f25-440b-ae12-717c6de17144
Muzaffar	1	1	2026	\N	Fetched low stock products	c03f9fda-0a00-434b-a767-20e4b0eae66b
Muzaffar	1	1	2026	\N	Fetched low stock products	ee6ec0c6-6d5f-4ee2-b729-5fdbc70bffa2
Muzaffar	1	1	2026	0000	Fetched products for shop	8bae4b16-b55e-49d3-b611-3a5d5c331c34
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f75661c0-a227-4252-a100-df863fbce30d
Muzaffar	1	1	2026	0000	Fetched products for shop	38b55748-dc47-469d-bad1-56704a4074c9
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	e9cb95a2-236d-4c91-a0a7-7a8df5eaca5d
\N	1	1	2026	0000	Sale created successfully with sales_id: 3c2e6db1-c6c6-4a15-b01e-d671e02df760	9ba91dd0-dfe4-47a2-a5e7-df9b73e62904
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	cd7ff373-8a86-460d-9943-97d88ab19118
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	f7ccbdbf-5ee1-4f60-a3a8-4792816cae32
Muzaffar	1	1	2026	\N	Fetched main finance statistics	9361a42f-3ea3-43d1-bd8e-f74c56687a3f
Muzaffar	1	1	2026	\N	Fetched main finance statistics	debb9f17-83a3-4d1b-8a63-6ec2c954d146
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	02fc240e-198c-4343-bffc-d6ceb8f1c9a9
Muzaffar	1	1	2026	\N	Fetched high stock products	c0a74cd0-98da-4bb8-8e62-67965fe8ba5e
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	74dda374-ffa8-415f-8e21-ec61b79da334
Muzaffar	1	1	2026	\N	Fetched low stock products	1082155e-d5f0-4b74-b67e-1d5372863b7a
Muzaffar	1	1	2026	\N	Fetched high stock products	b4fb16d2-eb40-4fdc-88c1-1b0eeee194b5
Muzaffar	1	1	2026	\N	Fetched low stock products	4ae32bd2-434c-448a-a16f-a99886e1b801
Muzaffar	1	1	2026	0000	Fetched products for shop	20c3a68f-b2c4-4b35-81fc-763b3ef5c18d
Muzaffar	1	1	2026	0000	Fetched products for shop	081d618e-2f06-4970-868c-d841fc686523
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c1450743-ae79-4d75-b1e8-7a6957ce9018
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7a7faa86-32e1-4eb4-9f70-027c44d1c849
Muzaffar	1	1	2026	0000	Fetched products for shop	c7c0d7ca-117a-4e58-beb2-9a5248c03f6d
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	bde1c764-2753-41d6-ac51-b2d0c09c23ab
Muzaffar	1	1	2026	0000	Fetched products for shop	d1daad27-8f06-4cc8-b5b2-636958840582
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c920621e-a709-4a4b-9bf2-0372b3674637
Muzaffar	1	1	2026	0000	Fetched products for shop	3c18a7ff-5024-483e-aec0-6b73324ae08b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f87aa70a-8d1c-4506-aa4f-6a76f1088895
Muzaffar	1	1	2026	0000	Fetched products for shop	02c1f379-9ae9-497d-8722-90fcf1c7b39a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	5439c242-7cb5-4bfb-881f-4af8bc0b87e3
Muzaffar	1	1	2026	0000	Fetched products for shop	a13d19c2-1d51-4169-a5f1-00a07f0a6586
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	332341b4-f867-455e-b642-9c7ea1d716f4
Muzaffar	1	1	2026	0000	Fetched products for shop	25f9f23e-7876-44f8-a0f0-b8b8c03b52ba
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	77b043e2-b204-44eb-8b6b-bbe548819269
\N	1	1	2026	0000	Sale created successfully with sales_id: 9661769d-b01a-4d58-bf5d-f67bf6fc2b8c	0a41b187-c687-4285-adcd-68df1667fcf6
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b1ba8193-0c87-41a2-b0be-a3a818212f0f
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	655a5640-c254-4378-9dc6-d90045c6c2fe
Muzaffar	1	1	2026	\N	Fetched main finance statistics	5e583e7f-587a-4e4e-b21d-1f34c876362f
Muzaffar	1	1	2026	\N	Fetched main finance statistics	d9e35e35-4eae-44de-bb10-c46f75b6bc30
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	9e777bbb-d33f-41a0-89ad-6738196c586d
Muzaffar	1	1	2026	\N	Fetched high stock products	c9460697-b844-41c8-91a2-7843f2640f4d
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	ed9a2596-0f37-4a43-8575-1f6a9933de50
Muzaffar	1	1	2026	\N	Fetched high stock products	f3ac484c-09dc-4e0f-9a56-f9c17dc2c0cc
Muzaffar	1	1	2026	\N	Fetched low stock products	c1959604-576b-4c0b-b160-a94b3ef016e2
Muzaffar	1	1	2026	\N	Fetched low stock products	530e3f42-154a-4552-ad74-358a23ce013b
Muzaffar	1	1	2026	0000	Fetched products for shop	933974d4-c78f-490c-a07c-22698dc225ae
Muzaffar	1	1	2026	0000	Fetched products for shop	42f7b4d8-a428-4cac-9711-528d2d9603a1
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	0c1e8786-df37-46ce-b391-d8aae9002485
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ad740bf5-9e8a-4b7e-8cd6-ddfb0df024c6
Muzaffar	1	1	2026	0000	Fetched products for shop	00c65523-cd15-4e0b-8f11-16c5da5ace93
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c98c8894-8d5e-41ce-8b2d-f8e38f379aec
Muzaffar	1	1	2026	0000	Fetched products for shop	24818fdc-64a0-4359-9a84-bfcaece94d5e
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ee3cd0cd-8a38-47ae-8e54-d16d26816074
Muzaffar	1	1	2026	0000	Fetched products for shop	c5802bee-5100-4cfb-87d8-f987223c56d5
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	910e6fd8-8976-4e29-a00b-93a215a7f011
Muzaffar	1	1	2026	0000	Fetched products for shop	9ca53a2a-781a-47d8-bff2-1bbb62e13f51
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	53a1f9d6-2c77-454b-9e4a-b27d4fcc1eb1
\N	1	1	2026	0000	Sale created successfully with sales_id: 3dd24248-ff2d-4783-a0e6-7275792c3f1e	1d510593-1988-464d-b17f-2723394af65a
Muzaffar	1	1	2026	0000	Fetched products for shop	b80f88f9-54fd-4a0f-9ecc-f648aaf97a4d
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ba87a115-f952-423e-8b73-0501bef72d9f
Muzaffar	1	1	2026	0000	Fetched products for shop	3acf5241-890a-4237-b5c5-0f30245d854b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	47d8dbca-032f-4abb-a14b-e51b46c256d9
Muzaffar	1	1	2026	0000	Fetched products for shop	979d2991-0c5a-4cfd-95dd-5fb7d48e727f
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ab256b07-06b4-49f7-89f0-99a42bce7b76
Muzaffar	1	1	2026	0000	Fetched products for shop	bdd3baf5-ea4e-41bc-9ef1-ba971036c041
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d2814223-3706-4bd0-baf9-d8d990750986
Muzaffar	1	1	2026	0000	Fetched products for shop	6b81fde7-43e8-4cf5-9ec4-5b6d4ada6ece
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7403afb8-3502-4367-bd14-732bfa07dafd
Muzaffar	1	1	2026	0000	Fetched products for shop	79b3fb2b-3e80-4857-a0ed-74fb5876de89
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b5335100-7e88-4ec0-87e4-39994e752180
Muzaffar	1	1	2026	0000	Fetched products for shop	611b4b60-1767-4277-84e0-45c9528709c4
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	be8f7346-a65a-4cce-ab4f-7e5079da2cfd
Muzaffar	1	1	2026	0000	Fetched products for shop	a19e49bf-cda2-48b4-b1ca-f198f9d12c13
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	9805c6ac-8c7c-4d1c-b477-e18f42698f7c
Muzaffar	1	1	2026	0000	Fetched products for shop	769fac95-0fb5-4cb2-812e-3753d57dec46
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	171b8fcd-f719-4870-82f8-1688d636adf1
Muzaffar	1	1	2026	0000	Fetched products for shop	38c19e64-cd6e-456d-8ea6-f972f7993baf
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b095f2ec-bc5c-483a-8688-e2188247de67
Muzaffar	1	1	2026	0000	Fetched products for shop	6c0aebcc-daee-4853-8752-1971c8192941
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	00bd4e08-9b78-46c6-9599-0a7896f30e63
\N	1	1	2026	0000	Insufficient stock for product 'Qizil Olma'	3c9ff797-cbf9-4e17-ad12-2b787be2c22e
\N	1	1	2026	0000	Sale created successfully with sales_id: 6ebe20c2-2c04-448c-ad28-55a749f69536	601eabf2-51f5-433a-a3eb-8014e7d001c9
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	d5828a5c-cea4-4325-b2c8-602cb085335c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	544d0739-94c3-4321-85aa-beedfc549116
Muzaffar	1	1	2026	0000	Fetched products for shop	eeba5ad8-8266-480d-9490-9a020b7ce370
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	4d40e850-482b-4cba-b208-77a8f64af6e7
Muzaffar	1	1	2026	0000	Fetched products for shop	6ffe08ff-6439-4e97-b235-bb3dfb128edd
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6737149a-e593-4ec4-9305-e281c937e881
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	aaad925c-f50d-4e82-962e-0f9f4bd6f8b4
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	1051a4e5-f5b0-440a-bdfb-64c2cd511bb2
Muzaffar	1	1	2026	0000	Fetched products for shop	b4573c96-a099-4aac-9f12-b578596e844c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f6a3e6e3-c3d4-4e51-ab9f-78565e8d5744
Muzaffar	1	1	2026	0000	Fetched products for shop	2b4441e7-7a26-4c29-a0c2-cc73e6598149
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c5cf8ab0-4455-42f1-9a8e-b87084ce45b3
Muzaffar	1	1	2026	0000	Fetched products for shop	df4e39ce-3632-43df-9b53-b35a67994607
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	672dcede-6abc-4142-9f8a-c688c3a82029
Muzaffar	1	1	2026	0000	Fetched products for shop	00dc8edd-ec03-4564-b673-c07bdd6ea7f5
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	dbcb9b0e-a782-4a90-b4a5-486df35480e9
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b0df9b69-78d7-4e2c-97f7-ed56b7629bc8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	489acd95-8976-4daf-b41b-8dc0116796d8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	e8d9263f-b373-4d17-80a5-8473feafce86
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	50f73e82-8f10-41ef-9aca-16e486866ea3
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ad3a1123-875b-40b0-83a0-1505635a7ccb
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	beb70a03-6cd1-4f8a-955f-8cbb6254361d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	d712099a-1c89-4a87-8ed7-5bdd134cca2d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	734ad61b-7bb6-4f14-ad8c-52281e86706e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	f8eab7b6-c13d-4784-a206-18f93e993d0c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	30752574-2744-4d54-830b-95064e6d818e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	5d6cf432-1506-46af-96c1-b953c9027336
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	5c18e40b-1425-46a2-af5a-547753ce61f8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	991c9eb5-7bd1-40f4-a694-4909539168b1
Muzaffar	1	1	2026	0000	All admins fetched	37254e34-17f7-4070-9c16-6ca2fc056936
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	e41ff90f-d38a-4da5-87ef-068d10a6f7a3
Muzaffar	1	1	2026	0000	All admins fetched	8565d227-906a-41dd-8548-b188bb6b03bf
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	ad09543b-7f89-4382-8637-bbf8c0bfb0bb
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	a634e1cb-6891-4e4c-b738-5736fc872a54
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	917f912e-4c9d-458e-b564-b751b3e01055
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	57594b37-1118-4378-a580-0d2d6f7ee393
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	9fda3063-83d7-45cd-a9d6-a1fca4d4082a
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	3382a182-3e29-469f-b44f-690a952c120b
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	1136bb16-10a6-4446-87a4-4b2fc02ca50d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	cc4d2749-e03b-4906-948c-f6a43f7beae8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	98376dba-59d7-4bba-b120-b05a051c0046
Muzaffar	1	1	2026	0000	Fetched products for shop	a1c1cf94-02e3-4959-858d-6ff48f9e3725
Muzaffar	1	1	2026	0000	Fetched products for shop	fc33179b-21ca-4ee7-b268-c318551eff96
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	0f0a3779-7fd3-4a18-8427-54ab0b6ff2c8
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	3673660e-feb6-4c52-922c-95a71166f73a
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f788e1e1-2a49-4aee-9708-547c3a3fcf0a
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	f9b7557c-6a71-4be2-aee5-67b04a519f77
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	8a1c0f1b-064d-4fc5-a8af-83f47bca4483
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	9b44033b-79c7-45f3-976f-7bdf19d59f01
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	cadb4fe1-e8df-47e8-9bbd-adee688eb54d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b12cbd38-78ff-4329-b731-51503be7f45b
\N	1	1	2026	Muzaffar	Fetched sale by ID: 24a705b8-af18-4d7d-ba31-34941ae61abc	5e27ff28-14ef-4635-ae6c-2c534e45acc7
Muzaffar	1	1	2026	0000	Fetched products for shop	078642ab-2679-4c25-b507-95a7540b1684
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	05a24728-dff3-4b83-b06f-57f428188c7f
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	4518e37f-edae-4211-a75f-766ca55e1f60
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	aaab63e2-7869-4724-8efa-cce56b108788
Muzaffar	1	1	2026	0000	Fetched products for shop	83ec58ba-d2e1-4d38-af36-739c9f494ed9
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	0cdf685a-dff0-498a-968f-1aebd018cfce
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	ad55d79c-2a35-4775-9b86-0c529bf130b3
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	d9f3cb82-4ff0-4b23-af81-dc218afa120f
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	631c0f55-d0bb-4007-b70d-1f3c17f6dbfb
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	fe6c192b-a6da-42fc-ab0e-5ec3153f22fb
Muzaffar	1	1	2026	0000	Fetched products for shop	49537f43-e4da-4aef-8915-a833a450c7e0
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	675c1e98-0c2b-4fad-8288-9b8c1b81c7a8
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7b435f1b-b87c-41d3-938e-277a10d78a35
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	c76d9a04-57d7-482e-b954-265c77e65252
Muzaffar	1	1	2026	0000	Fetched products for shop	ad5392f8-21dc-44cf-bffa-4cdb5db0ea03
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	f61f3b39-d771-4b75-94b0-220db830ffe1
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7dbbe82d-310b-45af-bd14-3b08acf1455a
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	3f996545-f718-478b-bd92-9cd550b2424a
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ec0dfdb2-ede7-4096-b3ed-deff362885f2
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	2a8a2826-f713-478c-b98b-a1b50ce0658c
Muzaffar	1	1	2026	0000	Fetched products for shop	576e5ab2-53e3-4534-869b-ed114e9818b1
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	739cc4dd-386e-4ac4-8137-56707521d309
Muzaffar	1	1	2026	0000	Fetched products for shop	acecc8f2-73f9-48ad-b3a4-cfd03c5fab82
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	77f9b521-a85d-4b5d-8559-c95bc714bf57
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	bf0aa864-f0f0-4313-9784-b443af0f261c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	e51fffe4-35f6-423e-82c4-99b774cd762c
Muzaffar	1	1	2026	0000	Fetched products for shop	2f02278f-85c6-4de7-9d68-cf92ce2bf97c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	aeb116df-447e-4c03-9ec5-d061393b02ec
Muzaffar	1	1	2026	0000	Fetched products for shop	5c2b9774-b35b-404d-971b-0fb69e3493a7
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	00601390-81cb-4b5b-a66d-e2cb373ee3ac
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	dcf1aa28-f370-4356-9fe3-80ca13e371ee
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	9c62d015-a9a5-4820-9b6e-868e2fb4d58a
Muzaffar	1	1	2026	0000	Fetched products for shop	af55b3b8-a86f-4598-a37d-df0b3ca68ff0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	9f700d27-8930-4672-b16d-db32c53d0f88
Muzaffar	1	1	2026	0000	Fetched products for shop	c89d311c-feb4-4be4-9e6b-bc3c6907777b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d0b335b8-bfec-430b-934c-16a4311175c8
Muzaffar	1	1	2026	0000	Fetched products for shop	344a7434-8b63-4245-bbc7-2e0e6f17cabe
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	5b2a410f-97b6-411a-b6a0-d2f8f0336e5c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	8ae07ef8-f8d7-4563-b599-5e354b0353c9
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	6e31e39e-00bc-486f-b7c5-5e37ffb60d93
Muzaffar	1	1	2026	0000	Fetched products for shop	d522921c-9419-451c-b7e2-1ed8946cdad5
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	2c24f292-63b8-42e1-8a05-e7533db44b06
Muzaffar	1	1	2026	0000	Fetched products for shop	981e80fe-6577-4e50-9d15-3731f23d28f0
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a394a516-10b5-4d32-88e4-b247b03c3e2a
Muzaffar	1	1	2026	0000	Fetched products for shop	06616233-0861-415d-84cb-b0b02356454c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	cc187cd3-a434-45e3-90a3-f36f586bab46
Muzaffar	1	1	2026	0000	Fetched products for shop	07492114-8260-4974-8153-fa879e09c5af
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	663aa2e8-a006-44a2-9ab7-b33bb685d042
Muzaffar	1	1	2026	0000	Fetched products for shop	332ac613-a93e-4c4a-9ac2-55f441db794f
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	71101c7f-01c2-474c-80f5-31b2620eb3e0
Muzaffar	1	1	2026	0000	Fetched products for shop	00b14e67-1f39-4ba5-b8aa-572b1c17e29d
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	d86bc070-9b9d-4088-9421-4374e5d8af5b
Muzaffar	1	1	2026	0000	Fetched products for shop	d6419575-11f3-4573-8a4c-45bd28d5629d
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	849bac0b-20fa-4f04-b545-d5e71119cf42
\N	1	1	2026	0000	Sale created successfully with sales_id: 1b9142c2-0b9f-4ca8-9b8e-7b42b978e1fd	9a6b418e-20b3-432e-991b-d8e6633a411c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	0f12b0a2-08d8-4a92-bab6-35e63ff6b541
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	0ac61b7c-46d9-4205-9d70-c9e3b0d90a13
Muzaffar	1	1	2026	0000	Fetched products for shop	ab18429f-88aa-4ca5-945f-7ad2393139d2
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	3f49d263-4787-42db-83ae-72891d2381eb
Muzaffar	1	1	2026	0000	Fetched products for shop	497541d5-394a-41f7-9712-142060b64435
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	29b06f21-a90c-4172-ac3f-159979cb7377
\N	1	1	2026	0000	Sale created successfully with sales_id: 41831bd7-76ae-4918-a12c-dd1d1894335c	a524a38b-9e64-42f6-9654-d4586fd6c36e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	3a9963f3-51b4-4535-b7bb-19e256d07eee
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	7486cdaf-91e6-457b-98c6-53118c8fc9e8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	bb165c0c-1025-4d1b-acc2-afbdefb43651
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	a261023e-d694-4534-945e-b373955d88c3
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	dad38734-9b35-4c9b-9b64-23a68cadeee5
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	17f0c88e-3ad0-445f-976b-35d102ec3b58
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	a8f5755a-294f-48dc-b290-9965bc43ab80
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	d3211c24-6c73-4e98-9bd7-a355c40df0ef
\N	1	1	2026	Muzaffar	Sale updated successfully: 6ebe20c2-2c04-448c-ad28-55a749f69536	77b8a0be-8944-481c-8477-f91cefc2bdab
\N	1	1	2026	Muzaffar	Sale updated successfully: 3dd24248-ff2d-4783-a0e6-7275792c3f1e	e5c613af-c030-4c09-b19a-d4d484fe3c14
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	a1bf8564-ccd4-4b1a-87ec-ac3bca0d258f
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	28116e75-d27e-4d25-8fd0-f4bc4648effd
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	42ef524e-72ed-413d-95cd-d89bd5d2b820
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ccd5863c-f9e0-4e91-8360-1dabc78960bd
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	569b68e1-3204-4a51-9cd3-2c4139c9de03
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	0c8c7dae-502e-4dbe-8657-bab71a4db759
\N	1	1	2026	Muzaffar	Sale updated successfully: df62c835-ef45-4ef0-a6cf-adcd5308e0ab	843aeb87-a16f-4712-910b-132c3918bd86
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	58c47b0f-9e7c-45c7-8d10-632cdaf4f047
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	e26bcf17-dee7-456b-8b5a-41e4fef9da33
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	39324598-713d-447b-946f-d84ab8745f3c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	46a24381-86dc-468c-9949-7910edd2057c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	17e8225b-a85b-4508-8aad-bf5061b15778
\N	1	1	2026	Muzaffar	Sale updated successfully: aad19139-4b8c-4dd8-b152-43063f46282c	141f7a97-0158-4dc0-8026-a91fcdb9d484
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ed554fef-5457-466c-9bef-29c076b2d202
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	8ca548bb-8944-4395-bf41-ad01b2729be3
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	83c3522e-3c1f-4b14-9325-3d069891a341
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	a868d7da-23a5-43d1-afe8-51b063e07345
\N	1	1	2026	Muzaffar	Sale updated successfully: 9c005e83-33f1-49be-8125-51c800b3b256	ca849337-fa99-4698-92b0-d68b3794ba59
\N	1	1	2026	Muzaffar	Sale updated successfully: 9c005e83-33f1-49be-8125-51c800b3b256	1fcfa30a-b1d9-434a-b4b0-a737af9b511e
\N	1	1	2026	Muzaffar	Sale updated successfully: 9c005e83-33f1-49be-8125-51c800b3b256	63c7b2a6-4674-483d-a889-925c8a5c40bf
\N	1	1	2026	Muzaffar	Sale updated successfully: ef514f67-5538-47ba-9ede-7d2b891fe7f5	f8ff5a94-f5d9-449a-82ad-e1e744f101f1
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	d9b85a7e-f65e-4e95-9866-1e17ceba655a
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	f949f43e-e44e-4171-ac4b-18f60f1e5c98
\N	1	1	2026	Muzaffar	Sale updated successfully: 44aef04f-6704-4a48-8232-46bb6c60fb6b	61858946-92d7-4f83-988b-641a24fa2994
\N	1	1	2026	Muzaffar	Sale updated successfully: df01c05a-1dbe-4fc4-b92c-352d5ba47017	59142d82-d988-4e6b-9dc9-cf07abd86f71
\N	1	1	2026	Muzaffar	Sale updated successfully: 449fccaa-e68e-4fc7-a288-0da259760025	042d63dd-fb9a-42fc-968d-8d05b6b53929
\N	1	1	2026	Muzaffar	Sale updated successfully: c32bf7eb-1f9f-46e5-abff-1d63eccc0e8e	b7e34782-c8d8-49df-b62a-c359be9ffb9a
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	881350cc-e97c-42f1-918f-8db5c56b68e4
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	e64ce999-be96-45c5-a9ef-b8f329eed324
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	2abe9900-30b0-4a24-b33a-bf2c0fe17b73
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	30876f9b-5efa-447c-b3ed-846195f923c6
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	d8f57d58-96cb-4e26-a119-37621ccba4d8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	2af2b33e-5a4f-4213-b6d8-47efb913f462
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	e289f57a-7676-4891-ab43-c0bc3e8d5bfc
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	5797cf97-b4d5-4dbf-bc90-b2286ab1407f
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	00ac5701-612c-4b14-87dd-409624eb31cc
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b598d568-57bc-4be9-a256-03753a290f1c
Muzaffar	1	1	2026	0000	Fetched products for shop	bc44257c-634b-4780-97c1-90aa39ea2974
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	e7834d8e-6558-426b-beff-c2e4aee67ec9
Muzaffar	1	1	2026	0000	Fetched products for shop	605c837c-ace9-4b2e-bed7-5a051068ceb5
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	24f81073-ef93-45e4-9424-010a1f8c8137
\N	1	1	2026	0000	Sale created successfully with sales_id: 528cef18-f13f-4f0e-b359-27992e1af384	6ff9c5b7-f4da-4cce-9ddb-7b16ce1c294c
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	612c194b-aca8-4f54-8c01-b5c2b1627a70
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	af567c9a-2701-4878-a976-9ca4a46b3726
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	2bf80a1f-7110-4d06-afa1-aa01afbb71fe
\N	1	1	2026	Muzaffar	Fetched sale by ID: 528cef18-f13f-4f0e-b359-27992e1af384	546a4358-6e15-4918-b704-32504b10fe0e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	4e6d3211-c8ff-4bc4-a23f-f741ede47483
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	3598d955-1f65-48c6-97f0-80fa8852c5ca
Muzaffar	1	1	2026	0000	Fetched products for shop	d8596d34-d134-439c-b6c9-7082541fa4d5
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	4f3d85cd-f83d-4e3a-886e-ca2c67e84311
Muzaffar	1	1	2026	0000	Fetched products for shop	45c3d0a6-3930-4ec8-84aa-237b57d4cc78
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b861c68e-391f-46a7-b1b2-9114ff95343c
\N	1	1	2026	0000	Sale created successfully with sales_id: 2edabfdb-1608-4513-8709-aa082703786f	513611ae-e4bf-4009-b532-a5a3997f61b2
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	769e926e-9f19-4dc1-8b2c-310dbdcedf5f
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ecaac8d8-e1ac-47d5-ba94-89b1615bf245
\N	1	1	2026	Muzaffar	Fetched sale by ID: 2edabfdb-1608-4513-8709-aa082703786f	45030f29-fc41-494f-8575-f93e994bee5d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	9bf93bb3-9f1c-41d3-b218-fb3a3d8b05c8
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	da62bfff-3d7d-4774-b9b7-162fbf25e9fb
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	dd662d1b-dc7d-4b17-a83e-3ef71f1c78b1
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	16dd0263-9631-4543-b9d6-fa01f94b0861
Muzaffar	1	1	2026	0000	Fetched products for shop	6a59be8d-f3bf-4c77-969d-42138bc68f9f
Muzaffar	1	1	2026	0000	Fetched products for shop	072590a5-89c4-4bd6-baf6-c5001d348e2b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	0a06033a-cc67-4a03-a4b2-d37a4d5443d9
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	43df6ee9-0ccc-4db5-a6aa-72e6054647d6
\N	1	1	2026	0000	Sale created successfully with sales_id: adbc4bfe-bbfa-42f8-82e9-5be1217904dc	ff83dd0d-4210-4928-b907-780d0f87fa75
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	cec30eb5-9036-4ede-96f1-90a2675e8020
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	d99ba9d2-ab4f-4bff-9010-9ab7384c5685
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	805b517d-cda8-43ff-b3d9-119d4ed5620e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	84728032-e4d6-47b8-8a4e-f1d7c396c8bd
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	1767b107-e36a-4d71-b824-12bf2bca1178
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	19b2a61a-a7ae-4711-9cb7-5f6f50b25c58
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	b7742337-c68a-4b2d-a3dc-326516bcee22
\N	1	1	2026	Muzaffar	Sale updated successfully: 6ebe20c2-2c04-448c-ad28-55a749f69536	3c2558ca-3a14-4b89-9e76-1bedf8175e2f
\N	1	1	2026	Muzaffar	Sale updated successfully: 6ebe20c2-2c04-448c-ad28-55a749f69536	9c6481f4-c7b2-436b-a00b-16bd216656ad
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	fa4761d3-08fe-49d0-acfd-0c16de0805af
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ac88514a-f070-4cf9-917d-eaf6346fa283
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	3d88a153-e569-493a-b663-807d769105e2
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	24e44d4b-a341-49de-901d-ead89c498da2
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	a9038348-dae6-41f9-b496-fe3306782532
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ae45663e-0802-4859-b804-e4403812b1c9
\N	1	1	2026	Muzaffar	Delete sale failed - sale not found: :saleId	1a6e8839-32cd-4e6e-a63a-40e8471c6238
\N	1	1	2026	Muzaffar	Delete sale failed - sale not found: :saleId	5ee08250-1a79-40a2-8fcb-903535b2b687
\N	1	1	2026	Muzaffar	Delete sale failed - sale not found: :saleId	3e209142-421b-4f61-811c-2b9a203fc210
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	552d97da-d873-4b87-b024-0f6f672a3dc7
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	cc1d5174-e78c-4d74-a611-7fd44cb1a1ef
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	7ed7b1b6-0727-49bf-b544-8004cf23fd80
Muzaffar	1	1	2026	0000	Fetched products for shop	d39be60e-783b-4adc-93c8-81a6280ab3e7
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a32dbf38-97e4-47f3-bdf7-a8eafe48e413
Muzaffar	1	1	2026	0000	Fetched products for shop	1614601f-a2b7-4c30-81a3-83f07697de27
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a6298dc0-a19f-4d76-a4e2-f0be360862c7
\N	1	1	2026	0000	Sale created successfully with sales_id: 5faa2eda-98d7-483c-83f6-60d6c79a6838	6d87ba6d-443b-4bd1-a873-e4583d04ab79
Muzaffar	1	1	2026	0000	Fetched products for shop	a1b52c23-1346-497e-a267-63668eb98970
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	70c92a42-7a53-4164-be61-3369b2321d07
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	df4738b1-3f68-441b-b77e-e710082303fe
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	c25a6842-8b1a-41bd-87d1-1d24b01fa541
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	50c8dbed-1b87-4ee3-8e13-a3969d64d58f
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	1891deca-36f3-4c67-9827-49c009625212
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	9e25df6f-a3b2-47dd-b07f-ce03ade96230
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	982cd351-e746-4a43-bf99-aaa75178f25f
\N	1	1	2026	Muzaffar	Sale updated successfully: 5faa2eda-98d7-483c-83f6-60d6c79a6838	7d85bf74-88a8-41e2-8a40-44588b4d786e
Muzaffar	1	1	2026	0000	Fetched products for shop	f0990e08-6f05-4414-a970-320e39acd980
Muzaffar	1	1	2026	0000	Fetched products for shop	32a9cd5f-5f9a-4d5a-8ef2-8bb3fc0e461d
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a245f82a-bd66-400f-ac90-520db6cf5f85
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	b35d5e4d-8c91-4aef-8d8b-143689216cd7
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	f7526eb6-725d-4d25-8c3d-7d2e6bcf392e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	f59c8978-9452-423d-b12b-acf70a14e469
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ea169f8f-57d6-442b-becd-bf03053757fb
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	6202e60d-c514-4eb7-8fa4-22d8069eef77
Muzaffar	1	1	2026	0000	Fetched products for shop	9f656690-06ff-4f4f-bedf-c4e12d32945f
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	38f68834-e608-4e01-a553-f7452ea4d456
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	040aec88-ef6f-414d-9fd6-bd5c1dad26cd
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	dc86744c-1356-48ea-93a1-c66316c8058f
Muzaffar	1	1	2026	0000	Fetched products for shop	74f68c1a-9408-44d6-a275-24f6a788766e
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	637f1bd5-e0be-4cd3-93fe-c3739836aebb
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	1b5289d5-4bc3-41a3-bb94-a055d4f43fb2
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	49fb0019-f1b4-4381-94ff-ce0098d9d9de
Muzaffar	1	1	2026	0000	Fetched products for shop	25cd1067-cc90-4028-b15b-2b55f5b746dc
\N	1	1	2026	\N	Product restocked: 52b77d14-5bfb-428a-98ad-f4d2a6480fa5 (added 15)	f691c365-4a02-44f1-a37f-bf0d24d27649
Muzaffar	1	1	2026	0000	Fetched products for shop	c24e868d-8738-4663-b423-d09aa99eb8fe
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	a55c3ef3-e6ae-427c-ae90-56725213a786
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	85b4f550-14ef-4825-b87a-d5226fa6e051
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	8ab95efc-52f9-4454-ab72-c0a19ed6fab4
Muzaffar	1	1	2026	0000	Fetched products for shop	0d2d0525-fdc2-4b29-9e6d-fd58ab7f4847
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	f2bd3d5e-03b4-4005-8139-cd6378ccb45b
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	17d246ac-7ce4-4bd6-9462-ff40f45d9c60
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	583b3a06-7800-48fb-a865-1a87d1201da3
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	a5c14964-66a2-4fc8-bb8f-0196fc15097d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	3b012223-41e1-4bca-83c4-747699825176
Muzaffar	1	1	2026	0000	Fetched products for shop	847a635c-d69e-4902-8959-6e88a9e05b92
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	219400b3-cea6-4f9a-bb4d-2fc0e438d8e7
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	6771c14a-264d-4133-bbbf-71970cccf7f5
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	b210b98b-6de9-4f66-96e6-24318fd9cbcb
Muzaffar	1	1	2026	0000	Fetched products for shop	60635fbc-9c49-4e8e-bb58-831978a0d4f1
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	f552ea6c-1d76-4dac-8b51-2b157fa7ffb7
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	4849f9df-056b-4199-b2e0-e2242c1b36c8
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	9a6062de-14db-45cf-a383-39279d314b07
Muzaffar	1	1	2026	\N	Fetched main finance statistics	59598f4d-dbf9-4904-afa5-02654ab421d7
Muzaffar	1	1	2026	\N	Fetched main finance statistics	1091181b-db7c-4138-b798-d8a3bab08253
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	802186f6-c6a2-4fda-b96a-b00af9ad40d7
Muzaffar	1	1	2026	\N	Fetched high stock products	28805f7c-6234-42f8-8ad0-56f2ca478a02
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	829c10d5-8d57-4dee-bcbf-b8d2336ea596
Muzaffar	1	1	2026	\N	Fetched high stock products	233182e2-8376-4d75-9f45-2c546d453be5
Muzaffar	1	1	2026	\N	Fetched low stock products	7947544f-266d-4bd8-95e3-e9430a2c5c2a
Muzaffar	1	1	2026	\N	Fetched low stock products	4bd1b17d-88b2-4dcc-964b-3a55a1691e9f
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	33ba1a5a-4eea-45bb-8be0-f1b434f11c81
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	ab057286-979b-4d20-a34b-f25631546f7d
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	4298edb1-8998-405e-8b8f-9b09be32790e
\N	1	1	2026	Muzaffar	Sale deleted successfully: 3dd24248-ff2d-4783-a0e6-7275792c3f1e	33a19ca7-4faa-4e4d-8097-2ac409578153
\N	1	1	2026	Muzaffar	Sale deleted successfully: 6ebe20c2-2c04-448c-ad28-55a749f69536	edc45a32-ecd2-4983-8218-541282d35792
Muzaffar	1	1	2026	\N	Fetched main finance statistics	7aeddf10-a009-4b76-942a-140f29f33e27
Muzaffar	1	1	2026	\N	Fetched main finance statistics	01714159-1599-4573-9103-b75ebed83240
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	2a2c285c-4952-4c81-baed-b9a20f1b193c
Muzaffar	1	1	2026	\N	Fetched high stock products	10c8e6d9-bf25-450d-8af0-95ca542687e7
Muzaffar	1	1	2026	\N	Fetched week statistics (last 7 days)	d0933cbd-6e7d-4998-b542-3b8e7b29e696
Muzaffar	1	1	2026	\N	Fetched low stock products	64336494-838c-42cd-80f7-6147812af4ba
Muzaffar	1	1	2026	\N	Fetched high stock products	47271881-b846-4f87-b3b2-37f012480cf5
Muzaffar	1	1	2026	\N	Fetched low stock products	28db0a82-820f-4a6e-b00a-a39ae140639c
Muzaffar	1	1	2026	0000	Fetched products for shop	6e9f89c4-1644-4da9-acaa-c1743cf79932
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	4bd312d9-4884-4f55-939f-3d515bb43f19
Muzaffar	1	1	2026	0000	Fetched products for shop	f2486a32-a842-4ef8-8ad1-ae4e552b289c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	7730f6a1-5c55-47ee-a3c8-acf4fef58ff2
\N	1	1	2026	0000	Sale created successfully with sales_id: b78c4dbc-a3d5-4829-be79-414893413efe	9655bc3c-13f5-411e-b5a8-9aa594bccb19
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	05372b4d-07c4-4103-9a54-0552af64da7e
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	bad4091b-73af-4f76-9cd3-0d8e2bcfe4ec
\N	1	1	2026	Muzaffar	Sale updated successfully: b78c4dbc-a3d5-4829-be79-414893413efe	720403d2-8604-4976-b9b5-7b2bf6ad50f1
Muzaffar	1	1	2026	0000	Fetched products for shop	9b845480-f988-46b1-bd17-4aeddd9bc3e7
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	c2f8ae26-ca9b-4618-85d1-e227c2889d42
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	16b2acb9-b612-46fc-ac79-867c911f37ca
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	fb4b4395-53ca-49cc-b5c1-b6a0ac89b5ec
Muzaffar	1	1	2026	0000	Fetched products for shop	a7caacd0-ac9b-45cd-99f5-32e0efd1e2a3
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	c68e0b7b-7131-49b3-ba4f-6fc5f06f8cbe
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	0f2a9a48-a5fc-4869-9c57-b696fabf1b00
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	418a6467-cf7e-461e-879c-a1e7b9604270
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	30c87716-9054-4e32-b4ed-aeab0dcb92be
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	69a7efde-0866-443c-aa02-af702f430560
\N	1	1	2026	Muzaffar	Sale updated successfully: b78c4dbc-a3d5-4829-be79-414893413efe	a5454808-fbc2-4bb3-bf6a-77933edfde61
\N	1	1	2026	Muzaffar	Sale updated successfully: b78c4dbc-a3d5-4829-be79-414893413efe	21070d1d-8798-4970-80bf-01ec8c68973e
Muzaffar	1	1	2026	0000	Fetched products for shop	1d7dd81c-08b7-4414-b985-5e5b82da4124
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	e76dba73-c7b1-4c87-b6e7-317b9d89305c
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c8ae7c5c-0c4d-4641-bc51-f75a10a67178
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	d99af547-0703-4469-bf86-6e38c163cda5
Muzaffar	1	1	2026	0000	Fetched products for shop	8aee006e-ed68-4b2c-b8c8-0fab51ed6527
Muzaffar	1	1	2026	\N	Fetched all categories - count: 3	c1e1d94a-4bf7-4f56-91f6-ef79a9814121
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	1e7bfa03-beb4-4466-a7fa-9cdc963675c0
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	dbcea53e-6220-48a7-ae3d-64dd070389a8
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	e6c8fbb5-39fc-46bb-adc5-4c86c2d9b68a
Muzaffar	1	1	2026	\N	Fetched shop reports as superuser - count: 57	9645ad0f-2508-4cce-bbac-1c10330ae39c
Muzaffar	1	1	2026	0000	All admins fetched	cf350f4a-9ed8-4c89-9493-aabb291d61b3
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	c83a8a0b-1402-44bb-921e-b8547ea59468
Muzaffar	1	1	2026	0000	All admins fetched	c43ae70e-ac39-441b-842b-332e341a99e5
Muzaffar	1	1	2026	\N	Fetched all permissions - count: 5	e8a2b522-6bc3-44a9-b067-9a78e4dbe4ea
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	98974799-66f6-471a-9ac1-867f22f8c1cd
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	0c7221d1-b7b4-4cd2-a151-79a20644dc4d
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	a8e5fdc9-3b01-49f5-87c4-9dd365f232f6
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	5da2b8e2-8390-4275-a0f9-a5ee84e3fed9
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	5866678a-bea3-4b19-83b9-c5497f058266
Muzaffar	1	1	2026	0000	Fetched debt statistics	823a4471-52bd-425d-b2df-b459b34a1b1f
Muzaffar	1	1	2026	0000	Fetched all debts - count: 5	897d8af2-cbf4-4dd2-a05f-0aa895465de3
Muzaffar	1	1	2026	0000	Fetched debt statistics	689c12d7-a502-4d37-8257-b5f32392c4d7
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	80dfa979-b810-404f-a5d8-a21bb2d2e419
Muzaffar	1	1	2026	\N	Database backup downloaded - tables: 13	07cc6a7a-50e0-4b35-a3f7-7abfce37b8f3
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	62a9e963-c82b-4647-a8bc-131c259b7101
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	0d260f9b-53b6-4fec-91ce-2d59df2e4093
\N	1	1	2026	Muzaffar	Sale updated successfully: b78c4dbc-a3d5-4829-be79-414893413efe	a94de77d-b4f8-4ccc-bbd0-4cc6c8f36d9a
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	03c264b9-80c4-49b7-939a-4668aba5ac7a
Muzaffar	1	1	2026	0000	Fetched shop branches - count: 2	d2ed1572-8a6d-4577-b7d9-0d3fc7b638f0
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	5c41f97d-65b9-4520-98a0-d8cc60b29427
Muzaffar	1	1	2026	Muzaffar	Fetched all sales	cc3f8f55-3a25-4ea7-9268-8551500935d2
\N	1	1	2026	Muzaffar	Sale updated successfully: 9661769d-b01a-4d58-bf5d-f67bf6fc2b8c	2b5e9f5f-8fff-4645-9d2a-07b0c5b38c36
\N	1	1	2026	Muzaffar	Sale updated successfully: 9661769d-b01a-4d58-bf5d-f67bf6fc2b8c	67b308bb-fd04-43bc-9f66-535d132765fd
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	851919a4-82ab-4b46-a799-cbc6c7c214d9
Muzaffar	1	1	2026	0000	Fetched products for shop	3884c01d-bfb4-4237-82ef-6cca82d4ad97
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	50c1e3a2-2d04-47fd-8c70-d9ad7f4eada0
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	fb1d5dfd-225c-4a03-a7c7-662d143aa66d
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	bc30e5af-23ab-4ff1-92ad-44510a29a3ff
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	1fca4349-f080-4f0b-8a90-2a7b1fab974c
\N	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Sale created successfully with sales_id: 327e54c0-ba77-4792-a6bb-36b206a628f7	d35ad75f-4e71-462a-809b-b311ffcb6b2d
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	4731e34c-cb09-4c96-b63e-dac8da42687a
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	79f9d49c-34cc-4844-af29-a4e085609fd8
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	a7e6e0a5-3036-493e-a75f-bafdcb90d770
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	65a3de30-dcee-4eaa-ae2d-de88fb00eea0
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	fe2ed1d6-41ca-4822-85f8-bf3555b0066e
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	0498cf21-0947-4e30-877c-ac3b83d78499
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	3cb8b6c7-20e4-4835-a6f4-bc4f7a403a3c
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	981e66f0-885b-4938-ac10-aa29bb7a95c2
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	f3f82708-3840-4753-91b2-a6087823fd13
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	f8a935ec-23e3-40a1-8429-bc389567ce73
\N	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Sale created successfully with sales_id: c27382d8-453f-49bc-ba7a-a5f6a2047fd9	4d33fcb9-0c95-4892-9337-6448cdf86f2a
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	0c671e5c-0c73-4c62-b744-b95bf9008727
dsds	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	66894dc6-d0f5-4db2-bd46-e9ff9ccbe9d8
\N	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Sale created successfully with sales_id: ab286359-e009-4e00-a864-3e3eb9d0d6fa	aeb0c153-6b8e-4bbb-9cd9-1884f06cda64
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Logged In	a025c6e9-c560-47e8-abc1-ae9fe8fa748c
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	36c34d4d-6d1b-4749-aaf5-8c74dac09fb1
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	45fc3021-8e46-4b60-94ed-67ba76cdf206
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	4e5d57e8-4769-4146-a4ba-6483fb9609d3
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	30bbf081-2711-44eb-a399-56502892893e
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	10a8f673-779a-4a08-a172-4eb8803a0a36
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	b8f470e6-ba0d-43c2-a78d-7771fc8a7637
\N	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Sale created successfully with sales_id: f59e30f0-82d1-44fc-9866-47e6cf720657	c940c95e-2bbe-41cc-bc8c-ec6cc461794b
\N	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Sale created successfully with sales_id: 1194592b-c914-4bb0-be3f-f33326f2711f	099070c3-c0f6-4913-9781-11a701daaf71
\N	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Sale created successfully with sales_id: cfb4e91c-e58a-4770-bf5f-e75e9b33fb62	309a6c24-572a-42b8-ad8c-e8b7840569d8
\N	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Sale created successfully with sales_id: 857b7fad-ba08-48b6-bdfb-b4f4f28a36e5	30cbccc4-1b3a-4aa6-a398-ba7e15a08d5c
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	8f19515b-fd8a-416a-bee2-ad89f7ef05b5
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	960df838-f258-4f1a-816f-dde34eabd526
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	e009b54e-609a-42e6-9b3c-5e616455f5be
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	284c7759-2eca-4386-8007-fafbb2fea012
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	f98445d5-8c73-4dce-a64b-5ed9412a8d06
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	a226f575-f6ed-45b3-82f7-1d22101e315a
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	fd3cf668-51cf-479b-bd8f-0312f9982719
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	88df6a4b-cd5c-45e3-bf30-5300670d6366
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	2ecd478c-1c8f-4843-b1dc-eb0a3af8ccd2
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	f75a69a5-4984-4b44-abcc-1f17d4d3bdcd
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	3d32a011-b35e-4fff-9db1-a42cec1e9c45
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Fetched products for shop	691f3acd-226f-4b75-a5bd-17bfd6f9a911
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	All admins fetched	9e829680-6a7b-4b0c-ac36-451a27849b69
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	All admins fetched	34be7412-92d2-4be9-b30f-c6b437c545e1
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	All admins fetched	3a189b9e-2e12-44b1-975a-363a1a984e3b
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	All admins fetched	1cb0328d-28ba-4711-835b-2f84b397d119
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	All admins fetched	134f0c7d-e646-416a-babc-d6936fe98e79
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	All admins fetched	70408609-3760-4632-8cf4-0ba2d97bf37e
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	Generated SuperAdmin Tokens	36473c48-b6fe-469e-bb3d-5441a5fedc6f
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	All admins fetched	53a57fec-b052-4f46-b4b3-c7b98bb91ac7
fgf	11	12	2025	550e8400-e29b-41d4-a716-446655440000	All admins fetched	2e03ca4f-ba7a-46ed-9381-5dd82e4f9e1a
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Logged In	64f46450-9ec4-41c1-a567-a4ad4915a638
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Logged In	fda32c90-dd44-46ef-b5b2-2baa990a9241
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Logged In	fbd0fa07-00cc-44d7-b72b-637fbe5410f2
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	Generated SuperAdmin Tokens	4ac50cf9-55b9-463f-9dd2-a7d3ea26e586
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	Generated SuperAdmin Tokens	b7a11b9b-d280-4484-8f4f-9e28927719db
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	Generated SuperAdmin Tokens	66a1f94e-9041-4adf-8a63-be15969e8819
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Login FAILED - not found	f0bf05cd-30d0-4cd7-bd4a-814ea29876cf
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	Generated SuperAdmin Tokens	11b873e7-1e54-4047-b222-6abc3da4384a
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Login FAILED - not found	8e74ca56-795a-41f9-bfb4-7e8ac7168c33
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Login FAILED - not found	a4677119-4ffd-4341-8aee-38b606cc4ac8
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Login FAILED - not found	f5e519c4-9779-499f-a6c7-ab3ef176906f
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Login FAILED - not found	244ed88d-4a50-4f17-b14f-4c7763230bb5
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Login FAILED - not found	2eddcb40-4459-4bcb-ba7e-5e5606782d83
550e8400-e29b-41d4-a716-446655440000	11	12	2025	\N	SuperUser Login FAILED - not found	da4dd128-eaa0-49d9-a741-6bb222b7de4a
550e8400-e29b-41d4-a716-446655440000	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Super logged in	51f26744-c5d8-4802-b1fe-e378da1bde14
0000	11	12	2025	550e8400-e29b-41d4-a716-446655440000	Super logged in	bc3045e4-996d-4f25-bcf4-a662b371618e
550e8400-e29b-41d4-a716-446655440000	11	12	2025	0000	Super logged in	492417bb-2978-4559-9fd8-4c2f90c54707
muzaffar	11	12	2025	0000	No products found for shop	c0b09417-ecc5-4fe2-85ca-1b1bee38ac48
muzaffar	11	12	2025	0000	No products found for shop	cf16e85a-3d94-4fd0-835c-995dc461f57c
muzaffar	11	12	2025	0000	No products found for shop	bd00358a-671a-4a41-9449-7f64b2013b45
muzaffar	11	12	2025	0000	No products found for shop	2a8fa50b-5256-4901-8b40-6cb62ed36869
muzaffar	11	12	2025	0000	No products found for shop	23ab13bb-bcb7-4858-a76d-2569c207a391
muzaffar	11	12	2025	0000	No products found for shop	d5494e2b-a377-482c-9492-2ed75ca78a51
muzaffar	11	12	2025	0000	Fetched products for shop	04f045c2-b733-4c3e-b7f2-bf0af47b289b
muzaffar	11	12	2025	0000	Fetched products for shop	cab07508-17f5-47d7-a83b-c8b499727076
muzaffar	11	12	2025	0000	Fetched products for shop	8b4d581f-62b8-4585-b7fe-6f56ccdae13c
muzaffar	11	12	2025	0000	Fetched products for shop	38288a01-9a72-44af-9794-af6140a06ef9
muzaffar	11	12	2025	0000	Fetched products for shop	c749f597-9dab-4cdd-b1a8-5e401fca168d
muzaffar	11	12	2025	0000	Fetched products for shop	4f085e14-ac02-472b-843a-bccbfdce9632
muzaffar	11	12	2025	0000	Fetched products for shop	aaa60727-48f6-443f-845b-47d9edc62936
muzaffar	11	12	2025	0000	Fetched products for shop	1645361f-aec8-4772-a0fb-c945a1b2fa73
550e8400-e29b-41d4-a716-446655440000	11	12	2025	0000	Super logged in	04c27497-0ed2-44b3-8d99-d7d8eb68d84b
sfsfsd	11	12	2025	0000	Fetched products for shop	ad7d30bf-8575-4314-9240-8c58096820e0
sfsfsd	11	12	2025	0000	Fetched products for shop	01d268fc-bf58-4348-870c-7d7d8a622ccf
sfsfsd	11	12	2025	0000	Fetched products for shop	8f2fad30-beae-4c9a-b449-9237d02c6462
sfsfsd	11	12	2025	0000	Fetched products for shop	9ddd0dd8-3a1d-4876-af11-a3e1cb3e0f75
sfsfsd	11	12	2025	0000	Fetched products for shop	1efb4064-5921-4be8-9a42-80459b9fe2bd
sfsfsd	11	12	2025	0000	Fetched products for shop	6dac7437-cae9-4b32-a473-ebe6e87adf2d
sfsfsd	11	12	2025	0000	All admins fetched	a1966e8f-301f-43e0-b157-fc80193aaad5
sfsfsd	11	12	2025	0000	All admins fetched	9316dd72-a9a9-4df6-b03c-8168b855238b
sfsfsd	11	12	2025	0000	All admins fetched	ce232e09-487b-43fd-9410-c7243ec646b1
sfsfsd	11	12	2025	0000	All admins fetched	b2e5077a-78c0-4eeb-835c-206532e872ef
sfsfsd	11	12	2025	0000	All admins fetched	5bc81b44-47e6-4ae5-b438-682a27602cd9
sfsfsd	11	12	2025	0000	All admins fetched	faaf2821-b71a-40cb-b0e6-b293cd42ab59
sfsfsd	11	12	2025	0000	Fetched products for shop	dc87ad2b-f314-41a7-a790-12864b1d12d9
sfsfsd	11	12	2025	0000	Fetched products for shop	18ffdab7-01c2-498e-b74a-63cf8b118dac
sfsfsd	11	12	2025	0000	Fetched products for shop	393e3902-7a7f-4496-8abd-6c8968bfe6e3
sfsfsd	11	12	2025	0000	Fetched products for shop	126da171-774c-4914-bc0b-02201ddc5e2b
001	12	12	2025	\N	SuperUser Login ERROR	b83384f0-e66d-4e54-a140-55c317dca5c9
550e8400-e29b-41d4-a716-446655440000	12	12	2025	0000	Super logged in	b1a33bf6-e05f-4820-8491-7dedf99d3b95
123456	12	12	2025	0000	Fetched products for shop	679536f0-5b24-41b6-a79d-dc37bbb1875c
123456	12	12	2025	0000	Fetched products for shop	2d23018e-ea32-4f46-9b97-23c2bbd17ba9
123456	12	12	2025	0000	Fetched products for shop	0424e4e4-53ad-4ff6-8791-4296a32a5a7f
123456	12	12	2025	0000	Fetched products for shop	86fc12cb-57a6-4997-8cb0-cf700acaf668
123456	12	12	2025	0000	Fetched products for shop	721d42a4-0aa9-4e27-b372-e1e9812797f7
123456	12	12	2025	0000	Fetched products for shop	f612ebc9-c5b3-4398-9d7d-73470156c3b4
123456	12	12	2025	0000	Fetched products for shop	4b6e9e06-e94f-481a-a4fc-1c7138edb543
123456	12	12	2025	0000	Fetched products for shop	271ad629-21aa-4fc7-9a6c-59b2e5b4c663
123456	12	12	2025	0000	Fetched products for shop	01e8374d-3dc0-43cb-8ef5-6a92eb1f5088
123456	12	12	2025	0000	Fetched products for shop	6b4bf979-e9ec-4afc-9728-c0e79f1b645b
550e8400-e29b-41d4-a716-446655440000	12	12	2025	0000	Super logged in	3c7e0638-42ce-43e1-8b77-8cb82c4d6f84
lihbb	12	12	2025	0000	Fetched products for shop	b1c356e4-1137-45fe-8e1d-b8f0e1cc7f71
lihbb	12	12	2025	0000	Fetched products for shop	3aeb5616-1449-44cf-a3da-1db4f050ac9c
\N	12	12	2025	0000	Error creating sale: invalid input syntax for type uuid: "0000"	4e73dacf-d90a-45a2-a189-a3b6b9656db5
123456	12	12	2025	0000	Fetched products for shop	7a3fdcb4-ff39-4f8c-b914-2d9d19da2852
123456	12	12	2025	0000	Fetched products for shop	529672e7-d0eb-44fe-9820-04e61df5b55b
\N	12	12	2025	0000	Error creating sale: invalid input syntax for type uuid: "0000"	36cf8974-9ea4-4809-882f-2c8c25533c8e
\N	12	12	2025	0000	Error creating sale: invalid input syntax for type uuid: "0000"	793dc6ae-c52f-41fd-8837-fb8c41e2aa21
\N	12	12	2025	0000	Error creating sale: invalid input syntax for type uuid: "0000"	b75819f1-d661-4cab-b36c-deab5fae3153
\N	12	12	2025	0000	Error creating sale: invalid input syntax for type uuid: "0000"	1ba323d8-a00a-4875-820f-419f437f31cf
\N	12	12	2025	0000	Error creating sale: invalid input syntax for type uuid: "0000"	639938b0-3823-4a2f-8da7-9c6beea46511
550e8400-e29b-41d4-a716-446655440000	12	12	2025	0000	Super logged in	01a3f65d-949f-45c9-9c17-72707e92b876
12wxecftyubh	12	12	2025	0000	Fetched products for shop	b9eae6c4-35bb-4119-b431-c0fb0188df18
12wxecftyubh	12	12	2025	0000	Fetched products for shop	3f865787-f413-4b11-b03a-53807731c1b6
12wxecftyubh	12	12	2025	0000	Fetched products for shop	8e639ecc-0732-4ca9-a8bc-a5e03408d9e9
12wxecftyubh	12	12	2025	0000	Fetched products for shop	4ef0c8cb-dc35-4d4d-8c5f-114c8fb005c8
\N	12	12	2025	0000	Error creating sale: invalid input syntax for type uuid: "0000"	e9844654-4950-4daa-9172-0d426e8f3899
\N	12	12	2025	0000	Error creating sale: invalid input syntax for type uuid: "0000"	a994b21d-815b-4679-ac8a-ced1371567f5
\N	12	12	2025	0000	Sale created successfully with sales_id: da339bba-194a-41a5-af13-f56b013ca35b	2b9e1aae-75ef-4320-be0d-6de14fe04dae
\N	12	12	2025	0000	Sale created successfully with sales_id: 21957812-0c2f-4fe9-97f4-c41e0ae03e2d	b8436ad8-1987-4df0-821c-1d4dcd605da1
lihbb	12	12	2025	0000	Fetched products for shop	ee17523f-341a-4fc2-89a1-b221c3396dc7
lihbb	12	12	2025	0000	Fetched products for shop	c11e4af4-d2e7-47c1-bfb6-6157c65bf56b
lihbb	12	12	2025	0000	Fetched products for shop	d314e294-d62c-4c98-a52f-4b98d34dcd30
lihbb	12	12	2025	0000	Fetched products for shop	ce42568a-c63e-4aae-9218-0df495e17c2c
\N	12	12	2025	0000	Sale created successfully with sales_id: f7316cb2-0f72-48fc-8931-a3ec355b7a01	033bbeae-26ee-4cae-bc28-b7c77ad8d60b
lihbb	12	12	2025	0000	Fetched products for shop	36cd0431-fe81-43a0-8d0c-b432c73dbe61
lihbb	12	12	2025	0000	Fetched products for shop	8b5284fa-11c1-4e46-9150-d3bde2c0f844
lihbb	12	12	2025	0000	Fetched products for shop	c0f080f8-c400-4b6a-b0e8-fcd53881de53
lihbb	12	12	2025	0000	Fetched products for shop	fb4cf8f1-0100-43c9-ace8-327e6e1d148d
lihbb	12	12	2025	0000	All admins fetched	1f60a58f-4729-4eb3-b5b8-eb3c241f0e33
lihbb	12	12	2025	0000	All admins fetched	f4a96f04-d732-4f8f-87bf-2bcdb90b7936
lihbb	12	12	2025	0000	Fetched products for shop	9fb5649e-dbc3-4b35-823d-dae942df27d2
lihbb	12	12	2025	0000	Fetched products for shop	5a548733-8137-43ce-8510-34998e563643
lihbb	12	12	2025	0000	Fetched products for shop	a6ca8e14-2f56-484a-a1f7-b4f90d6c05af
lihbb	12	12	2025	0000	Fetched products for shop	04fa05f4-71a4-41a9-bc52-d0654cc53fd6
lihbb	12	12	2025	0000	Fetched products for shop	b3afc68f-dada-408a-8fe4-2aaf2e5e7a14
lihbb	12	12	2025	0000	Fetched products for shop	7e7a3ff9-ccba-476e-bd0e-1e2ccbfe452c
lihbb	12	12	2025	0000	Fetched products for shop	56e107d8-3f5a-4e22-983b-f83eed1f5c60
lihbb	12	12	2025	0000	Fetched products for shop	4e2fd5b9-a5db-4eef-bc30-60ad04a91d5b
lihbb	12	12	2025	0000	All admins fetched	d7bea312-ffb4-4ffe-b77c-87b2fa5c4495
lihbb	12	12	2025	0000	All admins fetched	90642487-3e5d-4822-9bb2-15d3e60945bf
lihbb	12	12	2025	0000	All admins fetched	abfb3560-547e-4b5c-a6c0-4f709322c464
lihbb	12	12	2025	0000	All admins fetched	3abb3a11-ab7d-48ac-810d-49c7dd119380
lihbb	12	12	2025	0000	Fetched products for shop	a3329106-85f8-4592-98bf-7233a2b93690
lihbb	12	12	2025	0000	Fetched products for shop	ab954913-1d71-4967-9e89-1bc0fff2b19e
\N	12	12	2025	\N	Product restocked: f18ea1c0-14ba-4d44-be1e-376b743a62cc (added 30)	7fec149a-3453-4a5a-9416-c6b04838895b
lihbb	12	12	2025	0000	Fetched products for shop	bbcd530c-b38d-46e8-b56f-12f613aa6e2c
lihbb	12	12	2025	0000	All admins fetched	09a4a64c-6ebf-471d-8b53-8f07b11c6cdd
lihbb	12	12	2025	0000	All admins fetched	ff1a8db9-0629-4b91-8b7a-5c594f50dac2
lihbb	12	12	2025	0000	All admins fetched	5d54c34b-55c2-4820-a875-bedc728ff836
lihbb	12	12	2025	0000	All admins fetched	dfbe6ce6-949d-4200-9788-ed6115664981
lihbb	12	12	2025	0000	Fetched products for shop	42c5e908-7fb4-4e9f-9003-32270007e815
lihbb	12	12	2025	0000	Fetched products for shop	e582ef0f-e035-4be4-97c4-5f61efad5f4b
lihbb	12	12	2025	0000	Fetched products for shop	ad3edd38-b80f-4180-8c5e-b475ca544749
lihbb	12	12	2025	0000	Fetched products for shop	3c315cd1-c0fb-47fa-9da9-b3075598b376
lihbb	12	12	2025	0000	Fetched products for shop	628bcbc9-d26e-441f-9595-bd37961e07ba
lihbb	12	12	2025	0000	Fetched products for shop	2fed9f3e-ceb1-44e6-8e79-8279a5640fdf
lihbb	12	12	2025	0000	Fetched products for shop	8f68d48c-df57-4de2-a7f1-84971c309bb9
lihbb	12	12	2025	0000	Fetched products for shop	3d43e592-6f64-47ed-9144-5109c5bd283d
lihbb	12	12	2025	0000	Fetched products for shop	1db8add5-42dd-48c6-9891-bc1fa26fa520
lihbb	12	12	2025	0000	Fetched products for shop	de297531-34bc-48d3-ad6a-74b31b8ba257
lihbb	12	12	2025	0000	Fetched products for shop	edb9c830-a235-4bce-af31-a06dc1e7114d
lihbb	12	12	2025	0000	Fetched products for shop	d5e19c90-5887-4d59-9243-f5583a59db36
lihbb	12	12	2025	0000	Fetched products for shop	739c14ec-4b3a-41bc-b310-891539ee56b3
lihbb	12	12	2025	0000	Fetched products for shop	948c5586-1110-4ed6-9bb1-b2a67eb8a566
lihbb	12	12	2025	0000	All admins fetched	cbbde3cf-fb45-40d5-91de-59e69eb63cf9
lihbb	12	12	2025	0000	All admins fetched	47577500-8062-42a2-819c-9e889c965aa3
lihbb	12	12	2025	0000	Fetched products for shop	4b3eb440-e0b2-47c5-9c10-28ceecb0ce63
lihbb	12	12	2025	0000	Fetched products for shop	ddad78b4-f556-401c-bdc2-fa09b757c0c7
lihbb	12	12	2025	0000	Fetched products for shop	83b90926-4092-490c-b842-017bfc88a002
lihbb	12	12	2025	0000	Fetched products for shop	2d638db6-d7b9-4ff5-b127-cfb5fb2c471a
lihbb	12	12	2025	0000	Fetched products for shop	277dbfdb-8412-4b3f-9556-a849876fff4d
lihbb	12	12	2025	0000	Fetched products for shop	02997b8c-cdbf-403c-a61b-62aa307267ee
lihbb	12	12	2025	0000	All admins fetched	1e52909a-ab96-425c-88b5-6810e7d232b1
lihbb	12	12	2025	0000	All admins fetched	93aa7799-f673-4c93-8e5c-d5191429734a
lihbb	12	12	2025	0000	All admins fetched	b2637bdc-0493-43a1-a1c1-b4f5b7b2aea2
lihbb	12	12	2025	0000	All admins fetched	ec37efdc-455c-4346-969e-0fe8e0b2594d
lihbb	12	12	2025	0000	Fetched products for shop	48a5030a-b60b-43e6-8367-5429504bfd4f
lihbb	12	12	2025	0000	Fetched products for shop	2a3b3630-d81b-4fd3-8252-262c32056a31
lihbb	12	12	2025	0000	Fetched products for shop	5e12bfdc-6199-4882-9447-a69f929060e2
lihbb	12	12	2025	0000	Fetched products for shop	b1814ce1-7714-4ad3-8954-852f9b00745d
lihbb	12	12	2025	0000	Fetched products for shop	f8454b84-84bb-4d02-960f-4f51ea390e62
lihbb	12	12	2025	0000	Fetched products for shop	b781bef0-b407-471d-81a4-7dae733d129b
\N	12	12	2025	0000	Sale created successfully with sales_id: e00d63e5-b4b8-4aca-a27a-c2711ab122e6	6c3774a3-23c1-4dfb-98ba-dbfcf69bddae
550e8400-e29b-41d4-a716-446655440000	12	12	2025	0000	Super logged in	8d4e877f-ad76-4a79-86a6-ab3980b2dab3
lfsifsdbfi	12	12	2025	0000	Fetched products for shop	35b8c322-3199-4946-9dca-c53df5b92b1e
lfsifsdbfi	12	12	2025	0000	Fetched products for shop	b739d5c8-93c0-4b11-a28c-2c1b60d4471d
lfsifsdbfi	12	12	2025	0000	Fetched products for shop	db9ad50d-ca06-425a-a1e0-2a655c200102
lfsifsdbfi	12	12	2025	0000	Fetched products for shop	87e9b41d-d3b7-4ed2-9100-adf54a7d72cb
lfsifsdbfi	12	12	2025	0000	Fetched products for shop	e69bf7e7-e232-4d54-953e-9d33d1c7e0e6
lfsifsdbfi	12	12	2025	0000	Fetched products for shop	5658332c-eeb7-4752-9d84-97b254f632ce
\N	12	12	2025	\N	Product updated: f18ea1c0-14ba-4d44-be1e-376b743a62cc	0022ed1f-da2e-45f7-853d-cb26f037733b
lfsifsdbfi	12	12	2025	0000	All admins fetched	e3256b90-4ad8-46b3-89f1-23fcf0cd6ad5
lfsifsdbfi	12	12	2025	0000	All admins fetched	421188e4-a28c-432f-bbf7-fd870230af55
Muzaffar	13	12	2025	\N	SuperUser Login FAILED - not found	4583f04c-240f-4370-bf39-9ac3aebf89be
Muzaffar	13	12	2025	0000	Super logged in	e5d03136-dc91-488d-aac4-bc6572cfb2b9
Muzaffar	13	12	2025	0000	Fetched products for shop	fb38a464-e5f6-4df2-8225-9f0a714499a7
Muzaffar	13	12	2025	0000	Fetched products for shop	fcea09dd-4d6c-4236-acaa-508fa0591bd9
\N	13	12	2025	0000	Sale created successfully with sales_id: 5975cb0a-3482-417b-b2b5-14df90b0ca05	bd885e44-3a35-4688-9e72-09d4b1a04367
Muzaffar	13	12	2025	0000	Super logged in	709c9c1f-168b-4d48-9c3f-e504ba2f633e
Abduqodirov	13	12	2025	\N	SuperUser Login FAILED - not found	4df553dd-5c57-4cd9-8ff4-64a7e2b0e8ad
Abduqodirov	13	12	2025	\N	Admin Logged In	20ea51bc-999f-4617-aaaf-815f2046e965
Abduqodirov	13	12	2025	\N	Admin Logged In	9d83e3fd-4bd1-4996-b117-32948a9ab6ed
Abduqodirov	13	12	2025	0000	Fetched products for shop	1ec56576-29fb-4487-b035-14e91f403ba1
Abduqodirov	13	12	2025	0000	Fetched products for shop	f24905f0-bc4a-4b95-9171-2d7ac9120086
Abduqodirov	13	12	2025	0000	Fetched products for shop	1f340e26-5c34-4f88-a7e4-370173b9c784
Abduqodirov	13	12	2025	0000	Fetched products for shop	a0bdef75-977f-4061-8580-6c23d902a2e4
Abduqodirov	13	12	2025	0000	Fetched products for shop	cd00c81a-e0a7-443d-aed3-f8d33dafe4d9
Abduqodirov	13	12	2025	0000	Fetched products for shop	2ea8fce0-21dc-4124-a30f-e9740248d046
Abduqodirov	13	12	2025	0000	Fetched products for shop	ce34c20b-c452-4d8d-849f-3f58044f9d9b
Abduqodirov	13	12	2025	0000	Fetched products for shop	466242bd-c265-444d-a673-9d1e3f290e7b
Abduqodirov	13	12	2025	0000	Fetched products for shop	84f6d406-740e-4750-b64d-c9e79a63f831
Abduqodirov	13	12	2025	0000	Fetched products for shop	06d7ece0-6050-4afb-9e58-7733f63bfe07
Abduqodirov	13	12	2025	0000	Fetched products for shop	f0d06b16-74c6-4588-a561-7cd64b6e0a16
Abduqodirov	13	12	2025	0000	Fetched products for shop	0a62a5e2-e93c-4604-8cff-27527d0d0933
Abduqodirov	13	12	2025	\N	SuperUser Login FAILED - not found	496db8cf-6569-49c7-a022-a7af3bbaaebc
Abduqodirov	13	12	2025	\N	Admin Logged In	8f3b4370-ce43-4649-ab85-7d99f7b7aee6
Abduqodirov	13	12	2025	0000	Fetched products for shop	fe6db9cd-6af6-4df0-a03b-868d50245261
Abduqodirov	13	12	2025	0000	Fetched products for shop	023f4550-7af7-4211-9e9a-26e446bb5d08
Muzaffar	13	12	2025	0000	Super logged in	89c5acc4-d3a3-4c4c-b917-2174bd222ad2
Muzaffar	13	12	2025	0000	All admins fetched	da8d0fd1-fa5b-40b3-982a-8ae0d849f693
Muzaffar	13	12	2025	0000	All admins fetched	4aad0b75-c04b-41e5-bbb4-1ffdd39c3561
Muzaffar	13	12	2025	0000	Admin updated	934c6df2-e528-4f43-8eac-23f2e46075e1
Muzaffar	13	12	2025	0000	Admin updated	e6acb3b5-09ce-4034-b087-e101e733832f
Abduqodirov	13	12	2025	\N	SuperUser Login FAILED - not found	3b42af67-23a7-4ee2-a6ba-ee3e4f754001
Abduqodirov	13	12	2025	\N	Admin Logged In	c664949b-4391-4ee7-a69a-aac5373c042e
Abduqodirov	13	12	2025	0000	Fetched products for shop	ef54f7bc-9457-494c-939f-dbed1478c1d3
Abduqodirov	13	12	2025	0000	Fetched products for shop	f11e5bf7-5cd3-4469-bc99-b95c4c081a0d
Abduqodirov	13	12	2025	\N	Admin Logged In	ac1a9c23-852b-4d3f-bfe2-faad26ea7c1b
Abduqodirov	13	12	2025	0000	Fetched products for shop	1b771984-ca79-44dd-ae6a-81485ac879a9
Abduqodirov	13	12	2025	0000	Fetched products for shop	1e9803ba-6ce2-481a-92e7-1f72950589db
Abduqodirov	13	12	2025	\N	SuperUser Login FAILED - not found	c41dfa5f-1d0d-437c-8b38-bc2664d1ad61
Abduqodirov	13	12	2025	\N	Admin Logged In	65baf28f-38cb-4e78-8c24-591e81d02285
Abduqodirov	13	12	2025	0000	Fetched products for shop	07baf91a-3755-4ae4-83f2-ccb21f2580a5
Abduqodirov	13	12	2025	0000	Fetched products for shop	f58b0b1e-413f-45b9-9927-43a716320e9f
Abduqodirov	13	12	2025	\N	SuperUser Login FAILED - not found	699e4bad-1494-4f5e-af5d-6cc9ddd1b8ac
Abduqodirov	13	12	2025	\N	Admin Logged In	e1eec579-41a8-4504-939d-13eb6c7cfd34
Abduqodirov	13	12	2025	0000	Fetched products for shop	a0a539e3-639d-44b1-a287-dc8510bbb534
Abduqodirov	13	12	2025	0000	Fetched products for shop	f2032769-ff0d-4b61-b878-fee95c6e2a30
Abduqodirov	13	12	2025	0000	Fetched products for shop	19d02a08-178d-4497-9cf0-68abb9b82ba3
Abduqodirov	13	12	2025	0000	Fetched products for shop	61867c24-7c6b-4bd8-ab96-13a82880b64c
Abduqodirov	13	12	2025	0000	Fetched products for shop	e7dabba4-3773-4eab-95ee-4e703b28263a
Abduqodirov	13	12	2025	0000	Fetched products for shop	ee7c7213-c40d-438e-8438-26062871ab81
Abduqodirov	13	12	2025	0000	Fetched products for shop	139bc625-81ab-4761-821a-c4f9f9921094
Abduqodirov	13	12	2025	0000	Fetched products for shop	53d2ea7d-43ab-4fda-9450-97d49387b519
Abduqodirov	13	12	2025	0000	Fetched products for shop	dab52706-ae1c-4134-bb8c-aabf38d06c91
Abduqodirov	13	12	2025	0000	Fetched products for shop	7eb8d942-972b-4121-8046-9913f1b80cad
Abduqodirov	13	12	2025	0000	Fetched products for shop	7b1e32f9-6192-4a4b-b069-bd075c4678c9
Abduqodirov	13	12	2025	0000	Fetched products for shop	b2255850-8246-4058-9ad1-aa9a7c2033a4
Muzaffar	13	12	2025	0000	Super logged in	36369de2-4e0f-4d10-8e89-70e01021f965
Muzaffar	13	12	2025	0000	All admins fetched	463cee54-2cb9-4b4b-8e82-6b83b87252bb
Muzaffar	13	12	2025	0000	All admins fetched	14aec22c-627e-42c1-aa33-4176ff56a386
Muzaffar	13	12	2025	0000	Admin updated	9cb1afa6-8ff4-4378-b39c-c5fb7845defd
Abduqodirov	13	12	2025	\N	Admin Logged In	ade6d64d-6f2c-4dac-969d-b49888e0638b
Abduqodirov	13	12	2025	0000	Fetched products for shop	aa8eae06-242e-4fa2-8057-334e93e7c915
Abduqodirov	13	12	2025	0000	Fetched products for shop	5607835c-f786-4aca-b81b-a37fb64cfe51
Abduqodirov	13	12	2025	\N	Admin Logged In	e147264b-9810-4b31-a6e7-10ee830124de
Abduqodirov	13	12	2025	0000	Fetched products for shop	5d526747-0cf5-440b-8d28-777a10c3c11c
Abduqodirov	13	12	2025	0000	Fetched products for shop	f4ab9576-6e5d-482d-9531-b90c6f84bd60
Muzaffar	13	12	2025	\N	SuperUser Login FAILED - not found	69f41424-3d71-45fb-a84d-07ca69677743
Muzaffar	13	12	2025	\N	SuperUser Login FAILED - not found	f5f37d05-c4e1-42f5-983a-944725ca9a4b
Muzaffar	13	12	2025	0000	Super logged in	4c2681c1-ffa0-4658-82fe-95a6928ef37f
Muzaffar	13	12	2025	0000	All admins fetched	92420ee6-6257-4dd1-afb0-b122a53b5824
Muzaffar	13	12	2025	0000	All admins fetched	6674e6f4-75cf-46c1-b48c-e5adc66e2d84
Muzaffar	13	12	2025	0000	All admins fetched	33e0d60b-69e6-4656-8866-c5dfd623e51a
Muzaffar	13	12	2025	0000	All admins fetched	60f1b891-70dc-4997-b8a8-c56820ce61c4
Muzaffar	13	12	2025	0000	Fetched products for shop	4bc97502-8305-4e38-9ee3-c570cd33208f
Muzaffar	13	12	2025	0000	Fetched products for shop	87d3be0e-0012-4841-9d7a-38a77ceafec2
Muzaffar	13	12	2025	0000	All admins fetched	981d4cf8-7804-450b-87ff-64dae75817d4
Muzaffar	13	12	2025	0000	All admins fetched	167912ae-f296-42c6-bdbb-a673718a1327
Muzaffar	13	12	2025	0000	Fetched products for shop	192a6263-4ce1-4254-900d-397c5ef1ee2e
Muzaffar	13	12	2025	0000	Fetched products for shop	20f7acc2-8847-4864-abec-1f2c666ddb43
Muzaffar	13	12	2025	0000	Fetched products for shop	97dd5166-611d-4d9c-b006-b2410749fd34
Muzaffar	13	12	2025	0000	Fetched products for shop	6ccd1d28-6959-4fbe-a6c9-018a233400aa
Muzaffar	13	12	2025	0000	Fetched products for shop	a3bcec44-e058-4bf3-8051-09031b790159
Muzaffar	13	12	2025	0000	Fetched products for shop	1333d571-3d25-4b55-9772-a585937dd072
Muzaffar	13	12	2025	0000	Fetched products for shop	c6819489-fadb-4444-a1fd-698aa52b0266
Muzaffar	13	12	2025	0000	Fetched products for shop	840cfcef-9d72-4159-92a5-fcf471d364f9
Muzaffar	13	12	2025	0000	Fetched products for shop	cfea3d60-e41d-4140-8af8-7300a3c336c3
Muzaffar	13	12	2025	0000	Fetched products for shop	2438a553-fee2-410c-a2d2-8259888cbc76
Muzaffar	13	12	2025	0000	Fetched products for shop	1b154395-cc2d-4178-9f56-c9f2c181a310
Muzaffar	13	12	2025	0000	Fetched products for shop	3d09159c-01b0-46ad-8f07-1d95b72b44d4
\N	13	12	2025	0000	Sale created successfully with sales_id: e36a8b06-8d00-4eb5-9d5f-6d4157af2bfa	cc074a50-350f-477f-994b-d33fcb437624
Muzaffar	13	12	2025	0000	Fetched products for shop	28813b42-fabf-41e9-a252-36e606f3b12d
Muzaffar	13	12	2025	0000	Fetched products for shop	6184279b-e509-4404-b268-c3ef6a02a494
Muzaffar	13	12	2025	0000	Fetched products for shop	27204a95-eab8-48db-a671-2a25e66f2f86
Muzaffar	13	12	2025	0000	Fetched products for shop	bd9becb4-3f00-47c0-9900-ffc4390a78d8
Muzaffar	13	12	2025	0000	Fetched products for shop	09f88174-6f3e-43ad-b579-fcb78e95630e
Muzaffar	13	12	2025	0000	Fetched products for shop	498e57c3-1c3a-41ef-961b-9c6aaffadc2f
Muzaffar	13	12	2025	0000	Fetched products for shop	4991e8c2-0538-49f6-91d6-f6f4de6febf8
Muzaffar	13	12	2025	0000	Fetched products for shop	8be3f973-4446-4f65-8a6d-0eb05e71360f
Muzaffar	13	12	2025	0000	Fetched products for shop	4cc3562e-7f1b-4482-ae35-2a2f66d8f00f
Muzaffar	13	12	2025	0000	Fetched products for shop	848d6b9d-778e-49cd-ab08-c73fa3545644
Abduqodirov	13	12	2025	\N	SuperUser Login FAILED - not found	87332b07-f631-4568-87aa-5e19f9ec038e
Abduqodirov	13	12	2025	\N	Admin Logged In	0f0aad87-81d0-45e1-a407-cecec4671120
Abduqodirov	13	12	2025	0000	Fetched products for shop	581d9a19-70fe-4bc1-8189-73d215881553
Abduqodirov	13	12	2025	0000	Fetched products for shop	fec678f5-75b2-4f90-abe5-9e0e75d0d67f
Abduqodirov	13	12	2025	\N	Admin Logged In	b804e005-9993-44ea-bf67-e14c866edb3d
Abduqodirov	13	12	2025	0000	Fetched products for shop	1d9601a4-e025-4336-afe9-d7a500159f87
Abduqodirov	13	12	2025	0000	Fetched products for shop	6b142ea3-c50f-4242-a692-95fdb4c26265
Abduqodirov	13	12	2025	0000	Fetched products for shop	30f2a841-c9fe-45d5-b156-f9711dc8beec
Abduqodirov	13	12	2025	0000	Fetched products for shop	827a3a40-0497-4d3f-b854-df22d91f9831
Abduqodirov	13	12	2025	0000	Fetched products for shop	693dd99b-bed7-46d6-ae37-ba4a2dd1f73a
Abduqodirov	13	12	2025	0000	Fetched products for shop	a0d054f7-edb8-4769-bc51-49a23a9686bc
Abduqodirov	13	12	2025	0000	Fetched products for shop	936f83df-d918-42c9-b85b-559701460fb6
Abduqodirov	13	12	2025	0000	Fetched products for shop	3511dcde-f358-4c1a-970e-a67120bffca7
\N	13	12	2025	0000	Sale created successfully with sales_id: b8e6add6-e1bf-4cb9-8598-0f2b7d1374f9	8618be19-c567-45fe-acac-91a6039ab183
Abduqodirov	13	12	2025	0000	Fetched products for shop	3216682b-a184-426f-bc93-bc2a01a80976
Abduqodirov	13	12	2025	0000	Fetched products for shop	f29a85c4-a238-4965-a42a-6c5fcbc73c19
\N	13	12	2025	0000	Sale created successfully with sales_id: f3c0c46d-4b53-4dde-8fb9-4f052e41edbc	4f50243f-8322-491f-9b58-7adf766220dc
Muzaffar	13	12	2025	0000	Super logged in	880b7079-e1b4-433d-a9fc-45d2ccb5444e
Muzaffar	13	12	2025	0000	Super logged in	97ee89d5-7dfc-4616-8352-4771985f8479
550e8400-e29b-41d4-a716-446655440000	13	12	2025	\N	Superuser logged out	486e2396-4eff-4323-b4bd-852645248697
Abduqodirov	13	12	2025	\N	SuperUser Login FAILED - not found	4b34f14d-0ee4-48dc-9acd-cadec960d5cf
Abduqodirov	13	12	2025	\N	Admin Login FAILED - not found	d2d6f97f-a191-47b9-a264-f7ce590ad3a9
Abduqodirov	13	12	2025	\N	Admin Logged In	9e7f3969-9f27-4748-b557-f9de2fba5f46
550e8400-e29b-41d4-a716-446655440000	13	12	2025	\N	Admin logged out	171f6653-4517-441b-8ebf-f55a70b6da3d
Muzaffar	13	12	2025	0000	Super logged in	8299e67c-c764-4ff1-a6f9-f01949a91ec8
Muzaffar	13	12	2025	0000	Fetched products for shop	a23f244a-3a60-4ea7-8ab2-004246fe0fd5
Muzaffar	13	12	2025	0000	Fetched products for shop	047063f6-e9fd-4fac-8f2e-22a65f90356c
550e8400-e29b-41d4-a716-446655440000	13	12	2025	\N	Superuser logged out	4cbd98c1-0ad8-44cb-8195-1a5c2186a734
Abduqodirov	13	12	2025	\N	Admin Login FAILED - not found	819c91a5-8145-41af-b131-76bdda89bb48
Abduqodirov	13	12	2025	\N	Admin Logged In	d6778d88-6c1a-4281-8440-68922d9b8a54
Abduqodirov	13	12	2025	0000	Fetched products for shop	9b7e3f35-f947-4918-a3ff-c33ee08e15ff
Abduqodirov	13	12	2025	0000	Fetched products for shop	b2cd7fad-7dd8-4fbb-b6a6-c73c521ca953
550e8400-e29b-41d4-a716-446655440000	13	12	2025	\N	Admin logged out	62c58519-15fe-4882-9b26-d7217a22a434
Muzaffar	13	12	2025	0000	Super logged in	da075b6b-da06-4977-a202-894ab7014f94
Muzaffar	13	12	2025	0000	Fetched products for shop	36478178-064e-447f-bb15-977bb64dc6f5
Muzaffar	13	12	2025	0000	Fetched products for shop	3a72f496-7ef9-4662-9149-ad61bd442d73
Muzaffar	13	12	2025	0000	Fetched products for shop	207fdf35-67a4-4ba6-937f-fba79931a13a
Muzaffar	13	12	2025	0000	Fetched products for shop	e61938ac-86fc-46f9-a153-a4709b8e5f41
Muzaffar	13	12	2025	0000	Fetched products for shop	461a8ebe-da15-48cc-ba54-b3de15131de3
Muzaffar	13	12	2025	0000	Fetched products for shop	d6be052d-5e73-4aeb-85e9-95fdb7ccfda7
Muzaffar	13	12	2025	0000	All admins fetched	4bb4b5cb-2707-490f-81ac-567f81f2e385
Muzaffar	13	12	2025	0000	All admins fetched	b104e96d-5269-4205-aeeb-6c09a78ad888
Muzaffar	13	12	2025	0000	All admins fetched	c92b35c1-bfda-4472-abc9-142a22ecd5d8
Muzaffar	13	12	2025	0000	All admins fetched	2a7174ce-bc44-4ccf-b86c-4e7dc6aa01ae
550e8400-e29b-41d4-a716-446655440000	13	12	2025	\N	Superuser logged out	ee0c23d9-2fde-41da-b104-c855001bc582
Abduqodirov	13	12	2025	\N	Admin Logged In	9c0a4de7-0542-452c-bec7-ec7d30461e88
Abduqodirov	13	12	2025	0000	Fetched products for shop	a3c12a02-d6e9-407e-a8d8-789f928ecf6b
Abduqodirov	13	12	2025	0000	Fetched products for shop	7148d081-97c4-46a9-9325-2fd3557d8978
Abduqodirov	13	12	2025	0000	Fetched products for shop	ca08e956-da8a-45c2-a16b-a7a90bcb849d
Abduqodirov	13	12	2025	0000	Fetched products for shop	f5cad25e-2962-4c6f-bea6-e7a5edc6c8f5
550e8400-e29b-41d4-a716-446655440000	13	12	2025	\N	Admin logged out	f7cc9c1a-7be4-4cb4-bb2d-773647e776df
Abduqodirov	13	12	2025	\N	SuperUser Login FAILED - not found	879852df-90d0-471e-8eca-bf7a7cfbdebb
Abduqodirov	13	12	2025	\N	SuperUser Login FAILED - not found	69bf76e3-fa2c-4869-8384-b24b7b1cd960
Abduqodirov	13	12	2025	\N	Admin Logged In	f6aa906b-b2e9-4925-87b6-bb1a2d33ebba
Abduqodirov	13	12	2025	0000	Fetched products for shop	889e5b4a-6771-4a74-9e2d-4ebe99ff7d32
Abduqodirov	13	12	2025	0000	Fetched products for shop	e72e1666-117f-4069-acea-4209aedea7ae
Abduqodirov	13	12	2025	0000	Fetched products for shop	34082462-bee6-4a93-ad7d-c5d0eded5d89
Abduqodirov	13	12	2025	0000	Fetched products for shop	c5a57bc8-0a4c-40ea-a240-5c2428168f8d
Abduqodirov	13	12	2025	0000	Fetched products for shop	931b84c3-b9e8-4828-98bd-97c73b39d644
Abduqodirov	13	12	2025	0000	Fetched products for shop	96822de3-1219-45ef-b1e1-a93f56499d98
Abduqodirov	13	12	2025	0000	Fetched products for shop	ce69ef87-7075-4183-a2b0-fe1601432c3b
Abduqodirov	13	12	2025	0000	Fetched products for shop	ab65f805-833b-465b-8fbe-37f2db227554
550e8300-e29b-41d4-a716-446655440000	13	12	2025	\N	Admin logged out	95365b16-dbdb-45ed-b1c5-34f25f0f4994
Muzaffar	13	12	2025	0000	Super logged in	1153c508-627d-4f23-b3da-768d64868c44
Muzaffar	13	12	2025	0000	Fetched products for shop	cec1cd1e-d94d-4d7c-bafe-003a8dcba475
Muzaffar	13	12	2025	0000	Fetched products for shop	83dcfe38-3e91-49f4-a46e-a752b107c4b8
Muzaffar	13	12	2025	0000	Fetched products for shop	1d381964-477d-480c-a64a-e88ab9c46b7d
Muzaffar	13	12	2025	0000	Fetched products for shop	180ea8d2-b719-42ff-844e-a7bde1d40686
Muzaffar	13	12	2025	0000	Fetched products for shop	a393c6f7-1a0d-4921-b49e-ac2ca10d83be
Muzaffar	13	12	2025	0000	Fetched products for shop	1aaa44d1-5d29-4249-a53b-b5d932b204ea
Muzaffar	13	12	2025	0000	Fetched products for shop	e19bf5a8-c29d-494e-96d5-046598e4323d
Muzaffar	13	12	2025	0000	Fetched products for shop	b4107858-e530-4108-a707-533990079267
\N	13	12	2025	0000	Error creating sale: Cannot access 'payment_method' before initialization	2a8fb614-f2d9-4ded-9e8e-642e3ff6f2da
\N	13	12	2025	0000	Error creating sale: Cannot access 'payment_method' before initialization	c0b12370-02af-4a45-a703-1fc708368fe3
Muzaffar	13	12	2025	0000	Fetched products for shop	59ed25aa-6a13-449e-8f53-cb1d8d67a595
Muzaffar	13	12	2025	0000	Fetched products for shop	76cd2b3c-6bdf-42c5-aa6e-e2a10b7dc349
\N	13	12	2025	0000	Error creating sale: Cannot access 'payment_method' before initialization	7f72198a-13ae-4c7b-8a5c-58479e6dfc8d
\N	13	12	2025	0000	Error creating sale: Cannot access 'payment_method' before initialization	2cd98ed5-bf2f-450d-a496-0fecc760a84f
\N	13	12	2025	0000	Sale created successfully with sales_id: 1363c8ad-64b8-4689-8269-0d884a885074	b4a75c20-0a1e-4c34-b34b-90a0c335ec6f
Muzaffar	13	12	2025	0000	Fetched products for shop	0132a2d3-3e6f-49de-86ce-0df7ef96615c
Muzaffar	13	12	2025	0000	Fetched products for shop	942fc57f-7b48-4295-bc68-cafe94801508
Muzaffar	13	12	2025	0000	Fetched products for shop	aea9d32d-ad0e-4e6e-9876-c7aa09a8b6ba
Muzaffar	13	12	2025	0000	Fetched products for shop	76dc3596-9438-4542-a0c9-a5b81fab9023
\N	13	12	2025	0000	Sale created successfully with sales_id: eab2e5f3-b929-4218-85c0-294ea25f2e8f	09814345-5f84-46bc-82ed-eeba4640c7a5
Muzaffar	13	12	2025	0000	Fetched products for shop	e3e50d72-5bb6-4868-bde3-e3f6282b9a16
Muzaffar	13	12	2025	0000	Fetched products for shop	0d601c58-6d82-45c6-9a65-eab6f11ede70
\N	13	12	2025	0000	Sale created successfully with sales_id: 4b56bc9b-ab48-4596-bdeb-053c283bf030	5742728b-f2fc-4990-b1a0-0583fc77a6ae
Muzaffar	13	12	2025	0000	Fetched products for shop	d4090ca5-cbe1-418d-a3ae-fd2d3251fc3c
Muzaffar	13	12	2025	0000	Fetched products for shop	ef93b912-960e-4a57-a626-e251d1d8351e
\N	13	12	2025	0000	Sale created successfully with sales_id: 657e85cb-ff60-47c3-9597-7374c4a48e12	3bb9a55a-505f-4658-aa4e-cd86adc1b601
Muzaffar	13	12	2025	0000	Fetched products for shop	2426bd34-7af0-48e6-bede-83db84d65507
Muzaffar	13	12	2025	0000	Fetched products for shop	d780a0f1-9f3d-4ab8-b120-59e34458147c
\N	13	12	2025	0000	Sale created successfully with sales_id: 1bb2bac3-3dde-4f53-a5b0-8e1b3861fd2c	fe19fab3-f043-409f-892f-392085555915
Muzaffar	13	12	2025	0000	Fetched products for shop	cbb6084c-f5b8-4432-b099-27679e02c824
Muzaffar	13	12	2025	0000	Fetched products for shop	0d733440-23f3-44af-894a-169e37188523
\N	13	12	2025	0000	Sale created successfully with sales_id: c0dd6072-006d-40a6-a4a6-6f7363edc670	373011e4-b771-4fbc-9c72-bc91e68e7a6a
Muzaffar	13	12	2025	0000	Fetched products for shop	1106627c-b51f-4038-ac2c-ca97c210e21e
Muzaffar	13	12	2025	0000	Fetched products for shop	46135dc1-797d-45d3-a829-5ad00fb2c197
\N	13	12	2025	0000	Sale created successfully with sales_id: df62c835-ef45-4ef0-a6cf-adcd5308e0ab	7f2ac7a9-0294-4b84-bb9f-9bc5659d625f
Muzaffar	13	12	2025	0000	Fetched products for shop	14bedca2-3fab-437e-9afa-fd0ed3bc3fa4
Muzaffar	13	12	2025	0000	Fetched products for shop	1c0e4abd-6913-4007-aca6-a1caceb3c200
\N	13	12	2025	0000	Error creating sale: Cannot access 'payment_method' before initialization	7db41fd6-3eb9-4af9-aa79-5941ce1e3d48
\N	13	12	2025	0000	Error creating sale: payment_method is not defined	b91b2730-b52b-4472-b687-97e8d222050d
\N	13	12	2025	0000	Error creating sale: payment_method is not defined	ffa060ff-26c5-493f-aecb-696dab596d3c
550e8400-e29b-41d4-a716-446655440000	15	12	2025	\N	Superuser logged out	1dafc950-1397-4bb3-8da9-07c7f4220fae
Muzaffar	15	12	2025	0000	Super logged in	0cf2abe4-43cb-4a75-913b-775f049078e2
Muzaffar	15	12	2025	0000	Fetched products for shop	7ee70fee-1fd2-46df-893f-cfadf9c98824
Muzaffar	15	12	2025	0000	Fetched products for shop	3751f4cd-eb99-467f-8c25-bc74627c431b
\N	15	12	2025	0000	Sale created successfully with sales_id: 082ac668-07be-464e-b011-13ae49f0673b	9d1a65d6-292b-4541-b832-42219f49c69b
Muzaffar	15	12	2025	0000	Fetched products for shop	ad586bed-2bff-40ef-8f08-0239c2248d16
Muzaffar	15	12	2025	0000	Fetched products for shop	9a630da5-04bd-485d-9fb6-03ca300911b2
Muzaffar	15	12	2025	0000	Fetched products for shop	5da784de-3ee8-4679-8244-d2c45ab26fd1
Muzaffar	15	12	2025	0000	Fetched products for shop	2786d072-afd5-4156-a8a7-ef5bd2566178
\N	15	12	2025	0000	Sale created successfully with sales_id: 1597ec04-d461-44ca-87f5-64bd56877292	84fb614a-9199-4861-a26e-4df210e1ef84
Muzaffar	15	12	2025	0000	Fetched products for shop	9683b165-c858-4964-835d-82c8b7d8db3c
Muzaffar	15	12	2025	0000	Fetched products for shop	93fcc17a-0fce-4530-bb2f-106577a472f4
Muzaffar	15	12	2025	0000	Fetched products for shop	f3e4076a-4899-4645-acf8-76fb808457bf
Muzaffar	15	12	2025	0000	Fetched products for shop	b70b5ca6-6fa4-45c6-9963-e867f7deb8bd
Muzaffar	15	12	2025	0000	Fetched products for shop	6b8561fe-1470-4e95-80f6-2b4f9dedc293
Muzaffar	15	12	2025	0000	Fetched products for shop	aa83d014-818a-4f70-96d3-c44279ec594a
Muzaffar	15	12	2025	0000	All admins fetched	a8c252d1-cc38-41f4-ad70-3247912ade80
Muzaffar	15	12	2025	0000	All admins fetched	2bef63a0-8a23-4212-848b-8d7af644d024
Muzaffar	15	12	2025	0000	Admin creation failed	8afb7df0-f6dd-47b7-a4d3-ec522ba206cd
Muzaffar	15	12	2025	0000	Admin creation failed	6118edab-0158-40ff-bf43-1f618081d42a
Muzaffar	15	12	2025	0000	Admin creation failed	8a07349c-ecb3-4f42-91e5-98ce50de572b
Muzaffar	15	12	2025	0000	Admin created	3eb83d70-55ee-4bfe-ae73-d71a30b3887c
Muzaffar	15	12	2025	0000	Fetched products for shop	e79ca1c0-bb3a-4eb1-b286-79b12fb69436
Muzaffar	15	12	2025	0000	Fetched products for shop	98a28229-b0e9-4dd2-be2c-0e6a52b3d139
Muzaffar	15	12	2025	0000	Fetched products for shop	3722cf97-7ddc-461f-ad4d-c4aacd4714dd
Muzaffar	15	12	2025	0000	Fetched products for shop	c515a810-2f32-4780-96a2-e3cb21d5a062
Muzaffar	15	12	2025	0000	All admins fetched	9519371c-d3e8-493c-b180-1ed5d4e3dc11
Muzaffar	15	12	2025	0000	All admins fetched	4130bd7b-c6f4-4c91-89da-83a3a5e0be11
Muzaffar	15	12	2025	0000	Fetched products for shop	48524b10-8f22-4155-ad38-3ad4d3f8e692
Muzaffar	15	12	2025	0000	Fetched products for shop	4bd39f09-05d4-430f-b79c-e3d0d44d9da1
Muzaffar	15	12	2025	0000	Fetched products for shop	43d864cd-6f09-4788-9c5e-b01d4626bd40
Muzaffar	15	12	2025	0000	Fetched products for shop	f0454d36-590c-424a-870d-83b2998d97f4
Muzaffar	15	12	2025	0000	Fetched products for shop	fa010ebd-def1-48f2-b932-6f0125212f59
Muzaffar	15	12	2025	0000	Fetched products for shop	62896199-8211-410c-a3b3-5aed186fb190
Muzaffar	15	12	2025	0000	Fetched products for shop	da01c5a3-beb4-4bef-acb9-4044b5603aa7
Muzaffar	15	12	2025	0000	Fetched products for shop	0e60b00a-389b-46c1-989d-0a472c74415b
\N	15	12	2025	0000	Sale created successfully with sales_id: 2d826d36-06b2-44da-b1c8-ff13e456f269	00868bd3-07f5-4c67-a66f-a6ce7b0720f2
Muzaffar	15	12	2025	0000	Fetched products for shop	d68e2607-12fb-496d-b37e-d6bfab4311db
Muzaffar	15	12	2025	0000	Fetched products for shop	a425027f-fa68-4ea9-b5e1-e68869fdef72
\N	15	12	2025	0000	Sale created successfully with sales_id: c8178332-1b8e-4c18-8052-d71b231e923f	deb5b84f-473d-46ed-b90b-af83ce8b18e3
Muzaffar	15	12	2025	0000	Fetched products for shop	25f3be1b-e074-439c-8cfa-9c628c25d61b
Muzaffar	15	12	2025	0000	Fetched products for shop	e0ea4e1f-d4ea-44ea-aa08-9a1dd77bc77c
Muzaffar	15	12	2025	0000	Fetched products for shop	39df6830-c6dd-4b10-936d-6a50471b5f05
Muzaffar	15	12	2025	0000	Fetched products for shop	1fda5b4b-d6f9-4ee6-9094-3e0d40f0663f
Muzaffar	15	12	2025	0000	Fetched products for shop	85e02b6d-a75d-4de4-8e7f-9f7554d95658
Muzaffar	15	12	2025	0000	Fetched products for shop	d1315097-992e-4dd5-9295-5dc0ece2284f
Muzaffar	15	12	2025	0000	Fetched products for shop	6653401a-654a-49f3-95bf-edddf568145d
Muzaffar	15	12	2025	0000	Fetched products for shop	fac3b3a0-6084-4bdc-8fa9-33f97aec0fb5
Muzaffar	15	12	2025	0000	Fetched products for shop	509a58c5-4908-476d-b55d-c649030df8e6
Muzaffar	15	12	2025	0000	Fetched products for shop	2b96c495-7431-4b01-b8a9-b95c1afa5016
Muzaffar	15	12	2025	0000	Fetched products for shop	7df64455-e506-4957-ad04-6f5b09ef3127
Muzaffar	15	12	2025	0000	Fetched products for shop	f921a832-772d-41ff-b4bb-d34b764aa8a9
Muzaffar	15	12	2025	0000	All admins fetched	f39de3b8-a6c6-4e51-b33e-04c663b9612a
Muzaffar	15	12	2025	0000	All admins fetched	6f8626e9-cbcc-4ac5-800a-025187bd48b6
Muzaffar	15	12	2025	0000	All admins fetched	8256266f-465d-4018-9ada-ff9e83457916
Muzaffar	15	12	2025	0000	All admins fetched	bb54d4c0-c817-4aa7-a4c2-3c3f10d20b5f
Muzaffar	15	12	2025	0000	Fetched products for shop	eb8d8d11-4a35-4194-ba3b-d643aa552675
Muzaffar	15	12	2025	0000	Fetched products for shop	2f09fb71-e3d7-4d77-a0c7-97c7ef2b42c5
Muzaffar	15	12	2025	0000	Fetched products for shop	4c30c00a-f321-4586-bb5f-b64a2db3d046
Muzaffar	15	12	2025	0000	Fetched products for shop	2fb99294-c6bf-49e5-93d1-f4ea8ec4dbfb
Muzaffar	15	12	2025	0000	Fetched products for shop	d9f8fe7a-9efc-4d35-93ab-1c582066b1d3
Muzaffar	15	12	2025	0000	Fetched products for shop	e5db36e5-1769-4da7-b5b1-619d8ead5710
Muzaffar	15	12	2025	0000	Fetched products for shop	7f772dbb-2928-42e2-a490-ab47d1579649
Muzaffar	15	12	2025	0000	Fetched products for shop	0f7904de-7b72-456e-a6e8-05dbff02108f
550e8400-e29b-41d4-a716-446655440000	15	12	2025	\N	Superuser logged out	3a204e45-fde5-490d-96de-610da3725c37
Abduqodirov	15	12	2025	\N	SuperUser Login FAILED - not found	a5aaca26-f78b-49e2-9cee-0720cd9ff922
Abduqodirov	15	12	2025	\N	Admin Login FAILED - not found	ad61f4fc-d86b-4aaf-b103-1c21227c33eb
Abduqodirov	15	12	2025	\N	Admin Login FAILED - not found	76d8a397-04d1-454f-bd0e-774774c7786f
Abduqodirov	15	12	2025	\N	Admin Logged In	6c635ade-1b73-4611-a872-a7dce28e6748
Abduqodirov	15	12	2025	0000	Fetched products for shop	94910768-0eee-4fd1-9fe6-cc0005faea9b
Abduqodirov	15	12	2025	0000	Fetched products for shop	df22e1a2-b449-4219-95da-e4533253a0f3
Abduqodirov	15	12	2025	0000	Fetched products for shop	fd204611-fe4e-4945-98f2-47c414f71c12
Abduqodirov	15	12	2025	0000	Fetched products for shop	4d1d4736-51a2-4db4-8df9-150304e0ee03
Abduqodirov	15	12	2025	0000	Fetched products for shop	79dca96b-cbfb-41db-a07e-a59e2e26cb70
Abduqodirov	15	12	2025	0000	Fetched products for shop	4281a1d7-0cf1-477c-a9d1-d56ef2c4a853
Abduqodirov	15	12	2025	0000	Fetched products for shop	bb90059b-3e23-4730-89e2-ff3dfe20458b
Abduqodirov	15	12	2025	0000	Fetched products for shop	370d0659-6cfa-4f17-a33c-5ec9ddf919cb
Abduqodirov	15	12	2025	0000	Fetched products for shop	f437925d-6958-4814-b477-64d30124c1e0
Abduqodirov	15	12	2025	0000	Fetched products for shop	916020bf-25b9-42c5-b94b-4dd9adfe4601
Abduqodirov	15	12	2025	0000	Fetched products for shop	c5688d2b-73a5-4cae-876a-06824c4a1274
Abduqodirov	15	12	2025	0000	Fetched products for shop	42d21604-01a1-44d6-b98b-4268e4ab57a5
Abduqodirov	15	12	2025	0000	Fetched products for shop	71c2437f-b07f-45f6-aff4-a9b7f6b1f902
Abduqodirov	15	12	2025	0000	Fetched products for shop	82c243a4-32dd-4050-b607-e5506481a82f
Abduqodirov	15	12	2025	0000	Fetched products for shop	e4b99360-f448-4d8c-bdc2-1fda6c58d4ca
Abduqodirov	15	12	2025	0000	Fetched products for shop	f83f3c3e-c5a5-4731-93f0-956ecfdaad60
Abduqodirov	15	12	2025	0000	Fetched products for shop	fce51a6d-88c0-491a-9c98-9fa7d2fa26d4
Abduqodirov	15	12	2025	0000	Fetched products for shop	1fdf2168-7da0-4c71-b73d-5386b7929d7f
Abduqodirov	15	12	2025	0000	Fetched products for shop	af9e9f01-15f0-47a7-b322-ad030f5eb2b6
Abduqodirov	15	12	2025	0000	Fetched products for shop	6bfd1531-03ea-42f3-a204-344e400a327b
Abduqodirov	15	12	2025	0000	Fetched products for shop	52193789-d0d2-431e-ad1b-b54444ce8f70
Abduqodirov	15	12	2025	0000	Fetched products for shop	dadbc2bc-34f4-4a93-9218-3d4da1bebb9a
Abduqodirov	15	12	2025	0000	Fetched products for shop	462ac284-5358-4b00-b86b-76f2f6351aa8
Abduqodirov	15	12	2025	0000	Fetched products for shop	04435960-7735-43a9-ad78-9ecbe0210453
Abduqodirov	15	12	2025	0000	Fetched products for shop	2e5796ac-21e4-4600-8360-a4e42892fdca
Abduqodirov	15	12	2025	0000	Fetched products for shop	f0060526-66f5-4cb5-b8f1-d1710cef2e8e
Abduqodirov	15	12	2025	0000	Fetched products for shop	0fa0183d-aab2-4196-81ff-af5d42ea3415
Abduqodirov	15	12	2025	0000	Fetched products for shop	98cafd0b-0cdd-44ef-8322-a7c936498ea0
Abduqodirov	15	12	2025	0000	Fetched products for shop	250bf1b1-a8c7-4687-afbd-fb47c2d2e9d5
Abduqodirov	15	12	2025	0000	Fetched products for shop	1c048a94-c705-4a19-aee6-81ed5455d86f
Abduqodirov	15	12	2025	0000	Fetched products for shop	cd68c895-49f7-44de-9811-561d1d64a16a
Abduqodirov	15	12	2025	0000	Fetched products for shop	9a01f01c-3896-47e7-b60e-3ef21744da3a
Abduqodirov	15	12	2025	0000	Fetched products for shop	513ac5dd-eb8d-4269-9dbf-11d4d5087731
Abduqodirov	15	12	2025	0000	Fetched products for shop	93428b3a-dde0-44fe-bf14-6cdfdb456510
Abduqodirov	15	12	2025	0000	Fetched products for shop	9265748e-586f-45fb-881f-205faf57f6ce
Abduqodirov	15	12	2025	0000	Fetched products for shop	a200b76a-0684-4a8a-8216-07bd64f2fb1c
Abduqodirov	15	12	2025	0000	Fetched products for shop	6a7379b1-a8f2-4872-a34e-b1cef48e8566
Abduqodirov	15	12	2025	0000	Fetched products for shop	136aeed9-6b0f-4270-a10e-ef30efde8104
Abduqodirov	15	12	2025	0000	Fetched products for shop	aed21d07-f5bf-4b43-82a2-260cb26ac0c5
Abduqodirov	15	12	2025	0000	Fetched products for shop	b3775d2a-7d29-4520-bca3-0d6b80040286
Abduqodirov	15	12	2025	0000	Fetched products for shop	3e83ca62-ef8f-47cf-b266-e8a11903ee6b
Abduqodirov	15	12	2025	0000	Fetched products for shop	6a008429-ebda-40cc-b593-b5094bac398c
550e8400-e29b-41d4-a716-446655440000	15	12	2025	\N	Admin logged out	2e14b352-c0e8-4780-9a6f-0bd43f45376d
Muzaffar	15	12	2025	0000	Super logged in	40934115-fb37-4ce1-b429-68af4a9f9920
Muzaffar	15	12	2025	0000	Fetched products for shop	605b7e91-64bb-43a9-a18e-0d06cad5bb30
Muzaffar	15	12	2025	0000	Fetched products for shop	19a4e778-77df-4bd1-ba5a-3b7b297ee8dc
Muzaffar	15	12	2025	0000	All admins fetched	1a470112-1715-459a-9f2d-efa6890e883b
Muzaffar	15	12	2025	0000	All admins fetched	9b196f82-f4d0-4ed0-b047-8369066ee859
Muzaffar	15	12	2025	0000	All admins fetched	b7d528b2-20b3-4979-8335-b774a83b34cb
Muzaffar	15	12	2025	0000	All admins fetched	b51cfba4-7a7e-477f-b6fd-981a68faf346
Muzaffar	15	12	2025	0000	Fetched products for shop	7c950660-3839-4890-bb49-073d3f606648
Muzaffar	15	12	2025	0000	Fetched products for shop	892ac1ae-0762-4e2d-8a1b-0a560b3e2419
Muzaffar	15	12	2025	0000	Fetched products for shop	1c6ba806-2e18-4364-ae7a-06f38ecc1a0e
Muzaffar	15	12	2025	0000	Fetched products for shop	ab896dc9-13b7-4648-a6ae-dfc9a0823b3e
Muzaffar	15	12	2025	0000	All admins fetched	53002a82-46c9-4d4e-9ace-47e14f77c043
Muzaffar	15	12	2025	0000	All admins fetched	3951fd1c-6f48-4f09-a73c-a20cfda3ad5f
550e8400-e29b-41d4-a716-446655440000	15	12	2025	\N	Superuser logged out	31d133e7-4f74-44d1-89fe-79527cc35e0c
Abduqodirov	15	12	2025	\N	SuperUser Login FAILED - not found	40222cf7-aaff-4958-b714-5bbd85d855b4
Abduqodirov	15	12	2025	\N	Admin Logged In	25657827-569a-4c0f-ac65-fe0d9ef442a1
Abduqodirov	15	12	2025	0000	Fetched products for shop	14b050ea-6c5c-4560-b279-776028fa8b69
Abduqodirov	15	12	2025	0000	Fetched products for shop	9c6cd3cb-070e-4502-9715-625015204f3b
Abduqodirov	15	12	2025	0000	Fetched products for shop	f5543470-eb50-453f-a18d-f738bd064edb
Abduqodirov	15	12	2025	0000	Fetched products for shop	a5f50c7b-77b1-486a-b8c1-40ae8da4cf4b
Abduqodirov	15	12	2025	0000	Fetched products for shop	5b4ffef7-a77a-454a-bf4a-017401b7b9de
Abduqodirov	15	12	2025	0000	Fetched products for shop	0d0f10df-5efa-4243-ad9b-b51298f917f5
Abduqodirov	15	12	2025	0000	Fetched products for shop	9d868a88-b8da-4e2b-a43b-37fa0c4a3117
Abduqodirov	15	12	2025	0000	Fetched products for shop	0c5d1048-e709-4241-90a0-d3091e59bb99
Abduqodirov	15	12	2025	0000	Fetched products for shop	7e683890-3307-404e-b28e-b35c3a8c88f7
Abduqodirov	15	12	2025	0000	Fetched products for shop	d6cb2981-49e2-495f-b586-a8c69f2f049c
Abduqodirov	15	12	2025	0000	Fetched products for shop	06ed992b-7e85-47b7-9ebe-991c5f2a3173
Abduqodirov	15	12	2025	0000	Fetched products for shop	90c9f839-b81f-4168-b72d-502aba8dc41d
Abduqodirov	15	12	2025	0000	Fetched products for shop	431a6749-af92-40f0-ae18-3071b5c8bb3e
Abduqodirov	15	12	2025	0000	Fetched products for shop	4d66a11d-b1e1-40b7-85b1-e48c7973ad35
Abduqodirov	15	12	2025	0000	Fetched products for shop	e0db21c8-531a-4a31-963f-1b7a72f77bc9
Abduqodirov	15	12	2025	0000	Fetched products for shop	ce381a3b-3dd3-4074-a667-ca3a0096f009
Abduqodirov	15	12	2025	0000	Fetched products for shop	d2605381-58a1-4cc4-b004-12b61558298b
Abduqodirov	15	12	2025	0000	Fetched products for shop	9d663ba5-4da7-4fd9-b540-f44ea6110403
\N	15	12	2025	0000	Sale created successfully with sales_id: 6701baaf-2d95-4059-b4be-85688b94ef0e	69dad129-2636-43ad-b660-2741f2c2e91a
Abduqodirov	15	12	2025	0000	Fetched products for shop	30f0db4e-54e3-4bc0-8e0e-06a532b3fe91
Abduqodirov	15	12	2025	0000	Fetched products for shop	91eba3af-f8f2-4e51-8100-b32d23249497
Abduqodirov	15	12	2025	0000	Fetched products for shop	1057bf36-2e7c-4686-bfb0-cf1234300314
Abduqodirov	15	12	2025	0000	Fetched products for shop	607bd9af-5d93-4d11-8fc5-9839a33e8794
\N	15	12	2025	0000	Sale created successfully with sales_id: 5846cf51-024e-402f-911b-3b136ef1db53	9cebe328-9239-42a4-a4c9-cc39bf08af61
Abduqodirov	15	12	2025	0000	Fetched products for shop	a69c583a-2005-48ff-8ffa-60d4f709a69b
Abduqodirov	15	12	2025	0000	Fetched products for shop	8ca97f8c-263b-487e-9b37-425b08902dd8
\N	15	12	2025	0000	Sale created successfully with sales_id: d44dfc32-f7af-48e2-8941-2825fda09f38	8643eb3f-60b7-4742-b468-4205c5412bb4
Abduqodirov	15	12	2025	0000	Fetched products for shop	ef41aa6f-34f6-4081-b281-4276690db622
Abduqodirov	15	12	2025	0000	Fetched products for shop	715cb89f-5478-4dd7-b45a-22fb1f33cf9d
Abduqodirov	15	12	2025	0000	Fetched products for shop	e654fd74-402f-4403-a891-b984e456d48e
Abduqodirov	15	12	2025	0000	Fetched products for shop	0609df78-fdc5-4ba4-b3a4-9d7a43a22206
Abduqodirov	15	12	2025	0000	Fetched products for shop	cfa88d05-5962-4a64-927b-376c99eaeac4
Abduqodirov	15	12	2025	0000	Fetched products for shop	286cc9f5-b239-4e31-8aa9-42a4e7eb300e
Abduqodirov	15	12	2025	0000	Fetched products for shop	ec8e525a-fae1-4079-8697-c99e60c97195
Abduqodirov	15	12	2025	0000	Fetched products for shop	372f0066-7a22-411e-9250-d2ed3ac10191
Abduqodirov	15	12	2025	0000	Fetched products for shop	74845a3a-5446-4fdf-a3c5-9e1cbb6aefe8
Abduqodirov	15	12	2025	0000	Fetched products for shop	9677ba52-b863-4204-998e-257152228737
Abduqodirov	15	12	2025	0000	Fetched products for shop	c3dda71d-e15c-484c-9bac-c505c68005b6
Abduqodirov	15	12	2025	0000	Fetched products for shop	22ae84cc-e9f4-4e10-87dc-2cfdb6fae774
Abduqodirov	15	12	2025	0000	Fetched products for shop	94ef4bc0-7e04-483d-b283-cbe4f6c14a0f
Abduqodirov	15	12	2025	0000	Fetched products for shop	bc4aac51-a07e-446a-b0d0-ac324ff0106d
Abduqodirov	15	12	2025	0000	Fetched products for shop	96d2f119-f731-4dd6-ba97-98fd3c7649ef
Abduqodirov	15	12	2025	0000	Fetched products for shop	105aad05-1ce2-4e89-993b-42313fc6bbf9
Abduqodirov	15	12	2025	0000	Fetched products for shop	29fb8d14-2607-49e8-9bd4-040249070a80
Abduqodirov	15	12	2025	0000	Fetched products for shop	b2cf2d12-d37d-4466-a44d-d66d629156dd
Abduqodirov	15	12	2025	0000	Fetched products for shop	333d0bb5-8187-4916-a383-05b0e3c3c9ac
Abduqodirov	15	12	2025	0000	Fetched products for shop	28668680-1095-491d-a061-00e80ff7969f
Abduqodirov	15	12	2025	0000	Fetched products for shop	07b97847-9c89-414d-8874-12cd5c050492
Abduqodirov	15	12	2025	0000	Fetched products for shop	f6dcba7c-b745-4d80-8ce6-dc5cfa57438d
Abduqodirov	15	12	2025	0000	Fetched products for shop	af0f48df-db71-47ef-9ebe-5b2f2c0f4385
Abduqodirov	15	12	2025	0000	Fetched products for shop	84f1f501-d61a-4a65-9bff-c47bfa6e8c9d
\N	15	12	2025	0000	Error creating product: invalid input syntax for type integer: "Ichimliklar"	022c85a3-1128-4e8a-8fcb-004eabf6c76e
\N	15	12	2025	0000	Product created: 52b77d14-5bfb-428a-98ad-f4d2a6480fa5	d43e9c9b-f63b-41af-9b11-2c4fa8524c3a
Abduqodirov	15	12	2025	0000	Fetched products for shop	3ca4dd34-aadf-4638-9d79-dbe40bd19230
Abduqodirov	15	12	2025	0000	Fetched products for shop	d36f841c-0f9e-4053-9e31-8639c755fb58
\N	15	12	2025	0000	Sale created successfully with sales_id: 8878b4b4-07f3-49fe-aa62-caa30c8e6681	9aa75037-c111-4431-b187-5dc6a89f34f7
Abduqodirov	15	12	2025	0000	Fetched products for shop	b9749643-efee-4c2f-81bb-a4e199d691f8
Abduqodirov	15	12	2025	0000	Fetched products for shop	07712c45-fbdc-4a4f-a516-db6458bd9b0b
Abduqodirov	15	12	2025	0000	Fetched products for shop	2ac1d157-e6f6-4cb2-b50f-0d2fbeb546b3
Abduqodirov	15	12	2025	0000	Fetched products for shop	1f6441ba-f399-4648-a3e7-2d6815ab6923
\N	15	12	2025	0000	Sale created successfully with sales_id: 3802513d-acd0-4225-8d39-cb3e6ca3b555	9103eca0-5eff-4693-93ff-e0ccf0f21436
\N	15	12	2025	0000	Sale created successfully with sales_id: 3b73e0e6-0347-41e2-bfac-285785e66f6d	fcd36385-1ae8-448a-9656-fd757878c98c
Abduqodirov	15	12	2025	0000	Fetched products for shop	b434b8bd-3887-4682-9763-9b3f2fd070d0
Abduqodirov	15	12	2025	0000	Fetched products for shop	007fd7ab-9568-4225-b31b-cedc2b302f67
Abduqodirov	15	12	2025	0000	Fetched products for shop	c363307f-7ceb-4b46-80f4-f68e1c7d27ca
Abduqodirov	15	12	2025	0000	Fetched products for shop	8ef0f85c-39d6-4d9e-b2b8-dabddfe15b00
\N	15	12	2025	0000	Sale created successfully with sales_id: 9c005e83-33f1-49be-8125-51c800b3b256	a2ac976c-dfcd-4f70-ac61-1f3e56a88ecf
Abduqodirov	15	12	2025	0000	Fetched products for shop	e430e450-8c81-4c64-a975-f463b407ca5a
Abduqodirov	15	12	2025	0000	Fetched products for shop	5b495a44-4273-4a35-98ef-000efb44fd32
Abduqodirov	15	12	2025	0000	Fetched products for shop	38970bec-68fd-4fe4-bfb0-4eb0a25229ef
Abduqodirov	15	12	2025	0000	Fetched products for shop	f82fd8ef-b0de-4b52-92f7-0d4d03bc9857
Abduqodirov	15	12	2025	0000	Fetched products for shop	f327eafa-1bfe-488c-8b20-ec621d5d03df
Abduqodirov	15	12	2025	0000	Fetched products for shop	9b489d00-fe77-421e-be11-eea8552d2bff
Abduqodirov	15	12	2025	0000	Fetched products for shop	6c396580-1e2e-4195-b0e7-f4bc5f1bbee5
Abduqodirov	15	12	2025	0000	Fetched products for shop	4a8f3c01-0835-49a9-ae7c-c6d25e789f35
Abduqodirov	15	12	2025	0000	Fetched products for shop	2bd70a47-ff96-443e-a88b-9f542a55ae14
\N	15	12	2025	0000	Sale created successfully with sales_id: 3e35a658-6812-433d-9258-28f2f564f85f	9f97144c-19df-45fd-91b1-e7c86d4cba6a
Abduqodirov	15	12	2025	0000	Fetched products for shop	a82506bf-d271-427b-9ad7-96d957abd371
Abduqodirov	15	12	2025	0000	Fetched products for shop	f8fbbe94-6962-4086-ac44-b5c642a3017e
Abduqodirov	15	12	2025	0000	Fetched products for shop	eb23d1db-7e7b-45bd-86dd-8e0660706605
Abduqodirov	15	12	2025	0000	Fetched products for shop	702f9393-5339-48d5-89d6-560407200496
Abduqodirov	15	12	2025	0000	Fetched products for shop	52d13c11-04e3-46fa-9873-7453ab4b062e
Abduqodirov	15	12	2025	0000	Fetched products for shop	6bfdde09-5730-4f47-9670-82eb8a04c972
Abduqodirov	15	12	2025	0000	Fetched products for shop	1ab53d3f-07a0-4d15-bbda-465d98af78ce
\N	15	12	2025	0000	Product created: 7dc6d744-d616-4927-aa66-22ed8373b7a7	82ba8b1b-0b28-4125-a562-02608e2bac58
Abduqodirov	15	12	2025	0000	Fetched products for shop	d7c2870d-03fd-4f81-8c65-5fc67867926f
Abduqodirov	15	12	2025	0000	Fetched products for shop	7dd692e9-31d1-4a51-9e59-034f3ccdbfa1
550e8400-e29b-41d4-a716-446655440000	15	12	2025	\N	Admin logged out	a3c53be6-916f-43a5-b346-cd65fc9af8ec
Muzaffar	15	12	2025	0000	Super logged in	a86f18d7-4501-4832-888b-6527df3e8839
Muzaffar	15	12	2025	0000	Fetched products for shop	33ed34e2-0a3e-472a-8ad6-4a4ff919a568
Muzaffar	15	12	2025	0000	Fetched products for shop	0dde35be-884b-4cf7-bf6e-d30e6ea66c69
Muzaffar	15	12	2025	0000	Fetched products for shop	a796cae3-2a93-490a-92c0-5b4a74030cf8
Muzaffar	15	12	2025	0000	Fetched products for shop	aa4f9d17-34df-41da-aaf0-ff2017ef1b93
Muzaffar	15	12	2025	0000	All admins fetched	19d950d3-c848-4b37-926c-76bb5c378a51
Muzaffar	15	12	2025	0000	All admins fetched	3a400f44-0ed0-45fb-98ce-7323104b22b0
Muzaffar	15	12	2025	0000	Fetched products for shop	f216b2b7-69d1-4adf-a4b9-940c8114f9fe
Muzaffar	15	12	2025	0000	Fetched products for shop	7d691709-62b9-4c54-ab0e-09c6c6fc23af
Muzaffar	15	12	2025	0000	Fetched products for shop	7e3f046f-db95-4aab-9e39-e63d92687e5b
Muzaffar	15	12	2025	0000	Fetched products for shop	90ee9170-396e-4689-8323-1440dc5949f1
Muzaffar	15	12	2025	0000	Fetched products for shop	68497e4f-b3cc-44cf-ac55-4071f09b0ddc
Muzaffar	15	12	2025	0000	Fetched products for shop	1bd9aa0a-4990-4cd2-9718-1e95e699cd79
Muzaffar	15	12	2025	0000	Fetched products for shop	72edf266-821d-4968-a2eb-a3b17c805027
Muzaffar	15	12	2025	0000	Fetched products for shop	5b1b286d-475e-4d93-89a7-069ddf496e5b
Muzaffar	15	12	2025	0000	Fetched products for shop	ef6dcc7a-0e0d-4d6a-a9ef-58ed7ce49bd5
Muzaffar	15	12	2025	0000	Fetched products for shop	6ded24c6-b834-42a6-aedc-1a4685c4b267
Muzaffar	15	12	2025	0000	Fetched products for shop	fa99db28-995d-46b1-baf5-0833a2c37e05
Muzaffar	15	12	2025	0000	Fetched products for shop	53c29c0a-f410-4790-8234-59d616cd4419
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	e42b1d03-2d30-41ee-9a82-47711463f2c5
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	d04b64f4-d875-4657-8fb4-8ca9b3ec6f99
Muzaffar	15	12	2025	0000	Fetched products for shop	8c4d7ab9-d4e0-4c08-b36e-a8b053641598
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	eddbb40a-0467-4b0d-9356-cc0bceaf7feb
Muzaffar	15	12	2025	0000	Fetched products for shop	4b231b4e-b493-49f0-8504-17eba2f109ff
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	1a3ac08e-97f8-4f9d-bef6-25b81c0043d6
Muzaffar	15	12	2025	0000	Fetched products for shop	8e8f1de8-a430-4c6f-bf9e-4f40701cfbd7
Muzaffar	15	12	2025	0000	Fetched products for shop	c856274d-41bc-4502-bb41-08c3222ff533
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	a9ba11cf-8d0b-43af-80a7-504a5f474a77
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	c48e0f57-5619-4310-8174-5daf5430f3d0
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	ce44ae8d-f54b-420e-a9ee-6414641847f4
Muzaffar	15	12	2025	0000	Fetched products for shop	d44ae984-b19a-41df-a4e4-02517df25dce
Muzaffar	15	12	2025	0000	Fetched products for shop	f43f3be6-e404-40e3-834a-5df21b373a66
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	088ce6ab-8857-4e6f-8a5d-09035c426296
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	676739bb-a219-4325-ab5b-1231ca6f55d1
Muzaffar	15	12	2025	0000	Fetched products for shop	8d3084fe-6ee3-4a1d-88a8-e8d3df0c4eb8
Muzaffar	15	12	2025	0000	Fetched products for shop	52ce74d5-213b-4c39-9e62-308ac54b3c59
Muzaffar	15	12	2025	0000	Fetched products for shop	38b46a96-090e-44d5-aca9-54fda87b8014
Muzaffar	15	12	2025	0000	Fetched products for shop	312d63bf-4e2e-4757-8c18-7103d3800b14
Muzaffar	15	12	2025	0000	Fetched products for shop	035ab991-5bd0-40be-9637-79dcfc088fec
Muzaffar	15	12	2025	0000	Fetched products for shop	5769868c-efcf-4382-900e-7c895c19c02d
Muzaffar	15	12	2025	0000	All admins fetched	14fbd34b-7704-47c9-a7ed-1c85088a1513
Muzaffar	15	12	2025	0000	All admins fetched	1a228ace-f27b-4ab9-800c-3600ae4d4b8c
Muzaffar	15	12	2025	0000	Admin updated	bf92afad-b816-45bc-99ba-44059b4b58d9
Muzaffar	15	12	2025	0000	Admin updated	c9f45417-71b1-4bc6-9424-81c2e3d2f260
550e8400-e29b-41d4-a716-446655440000	15	12	2025	\N	Superuser logged out	612b208e-8185-476b-8fae-52b9eb8454c4
Muzaffar	15	12	2025	0000	Super logged in	ad79e757-a027-4b03-bde7-898f577e2148
Muzaffar	15	12	2025	0000	Fetched products for shop	8ae873cb-ab81-4ca7-af0a-6c26afa5fcd9
Muzaffar	15	12	2025	0000	Fetched products for shop	c39c9ed6-32e8-4e3a-8fe0-421a9d35496e
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	9bb23731-0772-4bf3-8a94-928c3d1f7191
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	b4498d4d-4d50-48b2-b301-3bd8ccb723c4
Muzaffar	15	12	2025	0000	Fetched products for shop	55847a66-b9b7-49c1-9e1b-710ca5a75b5b
Muzaffar	15	12	2025	0000	Fetched products for shop	179b507b-d853-49c5-9dbe-2ed0d619650a
Muzaffar	15	12	2025	0000	All admins fetched	be4c95d0-61a7-46b4-9b42-318a08180a91
Muzaffar	15	12	2025	0000	All admins fetched	bf9210f3-4513-4392-90e7-b3d0227305b9
Muzaffar	15	12	2025	0000	All admins fetched	d05b162d-6041-4ae9-8848-4467ce746d52
Muzaffar	15	12	2025	0000	All admins fetched	3fc50639-4d2c-47da-b3a0-55016c2f9535
550e8400-e29b-41d4-a716-446655440000	15	12	2025	\N	Superuser logged out	2baa3a04-e231-4c8b-9946-3d00987198ac
Abduqodirov	15	12	2025	\N	Admin Logged In	fedf9b44-86b0-4643-b039-26b53e442b68
Abduqodirov	15	12	2025	0000	Fetched products for shop	df228401-61b6-4600-a48b-137a13f0f21c
Abduqodirov	15	12	2025	0000	Fetched products for shop	6b240c03-7bb6-4492-9c5a-6bc85eecb71f
Abduqodirov	15	12	2025	0000	Fetched products for shop	0a8c6856-d3a4-45fb-adef-8ef1a501a27d
Abduqodirov	15	12	2025	0000	Fetched products for shop	bc3d69db-6b2b-41c7-88d2-b08c6d5b1da8
Abduqodirov	15	12	2025	0000	Fetched products for shop	e4ccad55-e8fe-4e90-be3d-eb470a4eff4b
Abduqodirov	15	12	2025	0000	Fetched products for shop	f973885a-4e97-4d00-bde9-62204bf9e35d
550e8400-e29b-41d4-a716-446655440000	15	12	2025	\N	Admin logged out	045877c0-080b-40d6-a8d8-8d51a4a1a960
Abduqodirov	15	12	2025	\N	Admin Logged In	08fa9b33-6528-4515-9ed2-f713bcbc802e
Abduqodirov	15	12	2025	0000	Fetched products for shop	c93921a1-9bf9-4b19-9130-8b847b80d456
Abduqodirov	15	12	2025	0000	Fetched products for shop	56f95fdb-3d3f-4003-84b5-adb343caa40f
550e8400-e29b-41d4-a716-446655440000	15	12	2025	\N	Admin logged out	3e664818-b6f2-4510-802a-28f863ac5bde
Muzaffar	15	12	2025	0000	Super logged in	be76ff1a-13d2-4427-9468-bdc47d74ce66
Muzaffar	15	12	2025	0000	Fetched products for shop	7412cc53-e7d0-41b2-a248-205691523185
Muzaffar	15	12	2025	0000	Fetched products for shop	debb54c8-83ea-44d8-840b-e6cf1562f7d4
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	f657b17e-d5b6-404b-91b6-f9c2772295fa
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	a69f7938-aa57-4c50-b953-5075370a178d
Muzaffar	15	12	2025	0000	Fetched products for shop	204153ac-e88c-47fe-8b4e-62415397e4de
Muzaffar	15	12	2025	0000	Fetched products for shop	e1c85890-52e3-4bb9-80c4-9306cc5f0066
Muzaffar	15	12	2025	0000	Fetched products for shop	a86f06cb-20a9-4ad4-9600-377338853712
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	f0882715-ec2c-4310-a68f-da81a3f30c26
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	534da6db-4b5f-4e94-bd04-c8e66c8ca2d3
Muzaffar	15	12	2025	0000	Fetched products for shop	7bf5f2c6-bd4e-4522-b634-6e77cf6e5d3d
Muzaffar	15	12	2025	0000	Fetched products for shop	65b63e0b-9893-4c5e-b6ad-0a2e5a936d95
Muzaffar	15	12	2025	0000	Fetched products for shop	5eb62498-b4a2-419e-a4a7-4e0a7cee27db
Muzaffar	15	12	2025	0000	Fetched products for shop	d339a381-d434-4e25-b8cf-3b38e4a3aaa8
Muzaffar	15	12	2025	0000	Fetched products for shop	8749b3f2-aee4-43fc-8c79-06b61cefc431
Muzaffar	15	12	2025	0000	Fetched products for shop	fb60ce71-0592-475f-afaa-da1258a3bb5c
Muzaffar	15	12	2025	0000	Fetched products for shop	e0339eaa-7a67-4433-b5c2-5c3ebe89ed73
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	a0beffa8-58b2-43cb-a454-9759965578e5
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	46c95c8c-df8d-4cdb-9154-c3f81b178139
Muzaffar	15	12	2025	0000	Fetched products for shop	42934aa7-8093-4439-924a-85b87a850085
Muzaffar	15	12	2025	0000	Fetched products for shop	32440657-848b-453b-9d91-4acbbe73a12d
Muzaffar	15	12	2025	0000	Fetched products for shop	d9906a36-a490-4546-95b8-5eb9e5cd52ef
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	cb28a853-52f9-40ad-bda6-6c34e08e87f1
Muzaffar	15	12	2025	0000	Fetched products for shop	51bca8ab-acb1-4048-8e03-cdd5b09e20f8
Muzaffar	15	12	2025	Muzaffar	Fetched all sales	5fa7f0c1-1d46-4bc2-9f8c-0fa9f663aa3c
Muzaffar	15	12	2025	0000	Fetched products for shop	9c945388-2f1a-4f45-97fe-5ba65ae68354
Muzaffar	15	12	2025	0000	Fetched products for shop	8cad51e4-862c-4f98-8bba-b5e29741eff0
550e8400-e29b-41d4-a716-446655440000	16	12	2025	\N	Superuser logged out	d9ffa4bc-946e-4a0a-973e-525395b54791
Muzaffar	16	12	2025	0000	Super logged in	0b0797c7-f760-44df-8857-c2fa66207b08
Muzaffar	16	12	2025	0000	Fetched products for shop	c4a8025a-0e6e-4eb1-96a2-b2409847a026
Muzaffar	16	12	2025	0000	Fetched products for shop	e40db37c-434d-40ec-a9ba-c40b642d27e5
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	6ba8b32e-d2df-41a7-b3cd-d2c2afc9759d
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	64a24a8f-8088-43ce-b504-1749a16149a7
Muzaffar	16	12	2025	0000	Fetched products for shop	b0c3afc3-7160-41fa-b37f-0a46154ec88e
Muzaffar	16	12	2025	0000	Fetched products for shop	23d1c26c-0747-4aa8-a72c-a88e2bdbe0c7
Muzaffar	16	12	2025	0000	All admins fetched	3c18fa08-2532-44f9-b114-d2d0a2aba4c4
Muzaffar	16	12	2025	0000	All admins fetched	ffe5cf9c-6da8-4175-84a9-b634391ea36e
Muzaffar	16	12	2025	0000	Fetched products for shop	d6dd2eb5-14e8-417f-8bf6-dafc329b96ad
Muzaffar	16	12	2025	0000	Fetched products for shop	77960a0c-873c-4c0b-8e0d-505b456a6d49
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	e858aa50-cdf7-4a70-844d-b8096777af1b
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	a82293d7-06c7-41a5-8f06-fea50f593fe7
Muzaffar	16	12	2025	0000	Fetched products for shop	506e776b-d4ec-462f-92ca-191625df3f2f
Muzaffar	16	12	2025	0000	Fetched products for shop	64f18cc3-ee15-4a1e-9658-bacf608c9726
Muzaffar	16	12	2025	0000	All admins fetched	f03accba-a954-4500-b732-13f9fefe2691
Muzaffar	16	12	2025	0000	All admins fetched	d095c122-fcba-465c-a8b5-63bb5b26c891
Muzaffar	16	12	2025	0000	Fetched products for shop	a084dfb9-be57-4138-9ade-44bd301c895e
Muzaffar	16	12	2025	0000	Fetched products for shop	18ee11a3-503d-45e9-8ed3-0b0ebc22e023
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	7c42a5bc-d8a1-4185-819b-e8d025d811cb
Muzaffar	16	12	2025	0000	Fetched products for shop	b08e0da4-c66b-4202-8aec-8e9e06b82d06
Muzaffar	16	12	2025	0000	Fetched products for shop	f578fb48-fbd9-48b4-a20d-d0c7d1de6d2a
Muzaffar	16	12	2025	0000	All admins fetched	c8366ba2-95cd-496a-9c73-139b8faeed0c
Muzaffar	16	12	2025	0000	All admins fetched	abf9a287-7888-4c1f-b5ff-1806de310e82
Muzaffar	16	12	2025	0000	All admins fetched	690263db-a6f9-4f0f-a6c6-fe36e67ccd5f
Muzaffar	16	12	2025	0000	All admins fetched	88526e0b-e03a-4bc6-bcec-5ab8030ba958
Muzaffar	16	12	2025	0000	All admins fetched	df7864d5-4992-40b6-b96e-03fedb37c7b0
Muzaffar	16	12	2025	0000	All admins fetched	5820516d-9989-49aa-a6a7-ab03a085c17f
Muzaffar	16	12	2025	0000	Admin created	ccab035e-4156-4ad8-bd09-b62710bd2cc8
Muzaffar	16	12	2025	0000	All admins fetched	9b2b5c3c-a771-4534-9e2a-b58a60b043fd
Muzaffar	16	12	2025	0000	All admins fetched	29b1d03d-ce49-40d2-bb58-3432713a4060
Muzaffar	16	12	2025	0000	Admin updated	b60e39ed-2ddd-4269-8fd6-5653571b84d7
550e8400-e29b-41d4-a716-446655440000	16	12	2025	\N	Superuser logged out	dbf9c4b4-f4b6-4746-9da8-88a508222ded
Rozmatov	16	12	2025	\N	Admin Login FAILED - not found	45019e80-cbed-40cd-b544-c44d961fd0d2
Xayrullo	16	12	2025	\N	Admin Logged In	c1ab832c-8913-40fe-aa71-066ed7a90385
Xayrullo	16	12	2025	0000	Fetched products for shop	c8bb6c6c-6a97-4d41-ba46-92aa49d06c17
Xayrullo	16	12	2025	0000	Fetched products for shop	f59d58c8-c403-49b8-8fb4-45454d08b87e
Xayrullo	16	12	2025	Xayrullo	Fetched all sales	1fbe3fda-4e5e-44d9-84e9-8493b9cb08a5
Xayrullo	16	12	2025	Xayrullo	Fetched all sales	4ba96488-71e0-4559-aace-d5e640e17abd
Xayrullo	16	12	2025	0000	Fetched products for shop	88c0c391-7b54-423d-ba9b-e90f1d2fe522
Xayrullo	16	12	2025	0000	Fetched products for shop	be0b5f85-d996-4d32-aa3a-bcf3ca41852e
Xayrullo	16	12	2025	0000	Fetched products for shop	879a3efa-f59b-4628-975e-31c651da5332
Xayrullo	16	12	2025	0000	Fetched products for shop	a2bef35d-62e9-4307-82ab-5fb582c7e71d
Xayrullo	16	12	2025	0000	Fetched products for shop	b388fc53-8098-4a5c-a8e4-e899389d973e
Xayrullo	16	12	2025	0000	Fetched products for shop	6d0cf500-9f71-46a4-8795-d4d6a3818043
Xayrullo	16	12	2025	Xayrullo	Fetched all sales	8ec1aba6-5c45-491c-af40-02197fe11f7f
0.438587255385826	16	12	2025	\N	Admin logged out	f97b1569-3719-4229-bfc9-2497d8631c71
Muzaffar	16	12	2025	0000	Super logged in	b470b598-64b5-4462-b097-50692033a505
Muzaffar	16	12	2025	0000	Fetched products for shop	8119a318-55d5-4370-bfda-3bfc9e7dc8a8
Muzaffar	16	12	2025	0000	Fetched products for shop	e5459825-2c7e-4dd1-a9e9-7e19674586fa
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	d6064bc4-77bc-4efa-be49-b251cc3b5a6d
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	dcfd3ecd-0c1b-4661-bf3a-bd8d21ea202b
Muzaffar	16	12	2025	0000	Fetched products for shop	1ef0d1c7-58bc-4c34-ad97-d2ed36f15318
Muzaffar	16	12	2025	0000	Fetched products for shop	27ef5615-69a3-45d8-8b14-99665234259d
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	d0c592d7-8489-4c8a-a154-cf4fd15dafb7
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	a79e64e0-e604-4eb6-918e-ad460390d5cd
Muzaffar	16	12	2025	0000	Fetched products for shop	abad13a4-468e-463d-b71b-b6cffa854331
Muzaffar	16	12	2025	0000	Fetched products for shop	5185279a-e49d-427d-b8ce-561a68580334
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	7c6938c8-2ad8-428a-97f5-8f4c4462cec9
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	fe1742ad-f8a0-4f7a-bce9-df3e5cf5b585
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	f5ec2044-7283-45f0-84d1-c42568e697df
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	f4355485-7541-45c2-8135-f62839bd4adf
Muzaffar	16	12	2025	0000	Fetched products for shop	af65ee4d-384a-4134-8c6a-6d41fa8de0ca
Muzaffar	16	12	2025	0000	Fetched products for shop	22e2751a-7eed-4131-b9d3-2edcf93c89ee
Muzaffar	16	12	2025	0000	Fetched products for shop	93fb7455-b0cb-44ac-bd3b-5f5a9c780cc4
Muzaffar	16	12	2025	0000	Fetched products for shop	1f9ab31b-a265-4405-939d-98e8e8982a05
Muzaffar	16	12	2025	0000	Fetched products for shop	8e8a64b3-996d-4e7d-b809-3c858d7a715e
Muzaffar	16	12	2025	0000	Fetched products for shop	0fbaaa0d-a3d4-438f-8fd3-a91c040955a5
Muzaffar	16	12	2025	0000	Fetched products for shop	c8ddc878-13dd-4af7-83e7-3a60c4f401d8
Muzaffar	16	12	2025	0000	Fetched products for shop	4fc9925e-fb6d-4781-a649-c2de3ec0509c
Muzaffar	16	12	2025	0000	Fetched products for shop	06b54189-780d-4c48-9835-2e7e0f7018e2
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	aa94e7b6-945c-4d13-a0e3-d245f1e659d2
Muzaffar	16	12	2025	0000	Fetched products for shop	c4577287-d87c-4424-9191-efdb60c9e8d1
Muzaffar	16	12	2025	0000	Fetched products for shop	46be263a-d3ca-4560-8045-d418506ca073
Muzaffar	16	12	2025	0000	Fetched products for shop	21faf0e1-bb92-4121-b338-458910702912
Muzaffar	16	12	2025	0000	Fetched products for shop	60ff0ec7-f3d6-4d69-8abd-d08f8c2a47d0
Muzaffar	16	12	2025	0000	Fetched products for shop	ff5b911e-0687-46b1-9496-224b1b254741
Muzaffar	16	12	2025	0000	Fetched products for shop	e98c75e9-e136-4b26-8d06-2e71290708e9
Muzaffar	16	12	2025	0000	Fetched products for shop	14cc7f50-9f5c-472f-aea9-ee15789c2aaf
\N	16	12	2025	0000	Sale created successfully with sales_id: 301cf017-d0ee-4949-954d-4520774ddb72	6dbe97eb-6ae3-4744-813b-8f0b57e8ccc7
Muzaffar	16	12	2025	0000	Fetched products for shop	4caff329-951b-4e48-9198-b704d1964483
Muzaffar	16	12	2025	0000	Fetched products for shop	f58d3835-023b-48fd-bf83-f173e0ecaa5c
Muzaffar	16	12	2025	0000	Fetched products for shop	768471f3-788b-4c87-9130-395a18f96b5d
Muzaffar	16	12	2025	0000	Fetched products for shop	b466e782-9291-4366-ae36-f0d20d5c4440
Muzaffar	16	12	2025	0000	Fetched products for shop	81789097-bc55-4b0c-b7bc-e3c09d71f870
Muzaffar	16	12	2025	0000	Fetched products for shop	39697a40-dea8-49b2-a261-ba6786e1c617
\N	16	12	2025	0000	Sale created successfully with sales_id: 3181a300-bda0-419d-af57-70941345f9b5	9a989fea-5366-4cde-b023-4d2a8194f0ea
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	e45a74a7-b1e3-492e-815b-c9d0f370a231
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	3318134c-835b-4bd9-b1e9-cc028ffebf03
Muzaffar	16	12	2025	0000	All admins fetched	c32fb476-cb57-4f08-93c6-206c715f8b71
Muzaffar	16	12	2025	0000	All admins fetched	262f9e66-c2aa-49d9-834e-d98ca991d6fc
Muzaffar	16	12	2025	0000	Admin created	62cf7039-fa8f-4858-9e6f-869b3df4e828
Muzaffar	16	12	2025	0000	All admins fetched	296a6b2b-a571-462c-8442-2cd7ea0b03ac
Muzaffar	16	12	2025	0000	All admins fetched	0aa016fc-268a-49b1-a8c1-24658891c2ad
Muzaffar	16	12	2025	0000	Fetched products for shop	a60e6349-5375-4151-a90a-87e6bf5c239d
Muzaffar	16	12	2025	0000	Fetched products for shop	b7def837-5a54-4b20-b557-e14d469593af
Muzaffar	16	12	2025	0000	Fetched products for shop	1f15c2fa-29b4-434c-8553-adfd061b3787
\N	16	12	2025	0000	Sale created successfully with sales_id: 4288378e-6020-4235-9a76-d18a4b56ecfb	bf3787a2-a591-481c-9c93-ac33861a6552
Muzaffar	16	12	2025	0000	Fetched products for shop	7303cfae-a1fa-44a5-91a9-47f18dcf37a9
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	42e31425-8892-467b-a2f7-4868631dc9c9
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	80f73c0e-0f19-45c8-a9b1-32c5ea820d31
Muzaffar	16	12	2025	0000	Fetched products for shop	0d0ecc5e-954d-434d-a75c-9b68669abc1e
Muzaffar	16	12	2025	0000	Fetched products for shop	4a63c8fb-83c8-4a40-90d4-e221c1d6e638
\N	16	12	2025	0000	Error creating product: invalid input syntax for type integer: "1.5"	a089d43a-4ef5-4a1a-9221-c64a23210143
Muzaffar	16	12	2025	0000	Fetched products for shop	547b6e98-dc9a-4878-9fe9-aa1c46e24ab9
\N	16	12	2025	0000	Product created: 2b40b1b1-5532-422b-80f5-335513df1286	4ac834fe-85e2-4d03-a8e6-538b1c3469aa
Muzaffar	16	12	2025	0000	Fetched products for shop	2372bd87-757e-455d-82ec-205271f8107f
550e8400-e29b-41d4-a716-446655440000	16	12	2025	\N	Superuser logged out	f586388c-8463-48ee-8a16-3c61de77a87d
Abduqodirov	16	12	2025	\N	Admin Logged In	1ac44225-5d5b-4e92-a341-3ca6196dc621
Abduqodirov	16	12	2025	0000	Fetched products for shop	58878f85-fe1f-4002-af82-6bcfb7d75743
Abduqodirov	16	12	2025	0000	Fetched products for shop	3eeb78ab-5c24-417f-b223-0be2adc32d32
\N	16	12	2025	0000	Sale created successfully with sales_id: ef514f67-5538-47ba-9ede-7d2b891fe7f5	f25ed777-7b44-4015-abae-1863cf1893e2
Abduqodirov	16	12	2025	0000	Fetched products for shop	f2845b61-de1a-4443-8808-95b3c8a1f45f
Abduqodirov	16	12	2025	0000	Fetched products for shop	89b73917-8a55-41da-a410-bf598f78d319
550e8400-e29b-41d4-a716-446655440000	16	12	2025	\N	Admin logged out	aa45c6a4-c448-4ed8-a231-06d06cb68eea
Rozimatov	16	12	2025	\N	Admin Logged In	328cbd1f-0e8e-43f9-b7f0-9d3cd858f5c0
Rozimatov	16	12	2025	0000	Fetched products for shop	de614572-877f-4585-a216-2a9dd6e03341
Rozimatov	16	12	2025	0000	Fetched products for shop	a973d300-da3b-4e4f-ade4-3dc59ae8a950
\N	16	12	2025	0000	Sale created successfully with sales_id: 24a705b8-af18-4d7d-ba31-34941ae61abc	0cf08b53-bc83-4698-a919-9138826da632
Rozimatov	16	12	2025	0000	Fetched products for shop	4ed72026-3189-4bcd-a674-64a2649833e1
Rozimatov	16	12	2025	0000	Fetched products for shop	d0ecc8fb-0da1-44a0-ad53-bd5f29aef4f4
Rozimatov	16	12	2025	0000	Fetched products for shop	2b49363b-e329-45eb-ae93-87920a35d069
Rozimatov	16	12	2025	0000	Fetched products for shop	c85e5463-fe42-4154-b661-69a6d46b8892
\N	16	12	2025	0000	Sale created successfully with sales_id: aad19139-4b8c-4dd8-b152-43063f46282c	f3cb9338-d7b1-4755-852c-9b84b6d580cc
Rozimatov	16	12	2025	0000	Fetched products for shop	8267fece-dd41-46a8-b158-ab0aee1f19e9
Rozimatov	16	12	2025	0000	Fetched products for shop	0b0d3290-a869-4e3c-b9dd-ac16a15ab61a
\N	16	12	2025	0000	Sale created successfully with sales_id: faeefa31-68a8-4ff0-ae03-c678565d49db	1a37221c-b792-4424-b99b-207341ef9f5b
\N	16	12	2025	0000	Sale created successfully with sales_id: b240b112-9666-4c80-b612-26e055094d91	21c642a8-c29b-4cc6-86e2-382060f57bc5
0.7329386920447608	16	12	2025	\N	Admin logged out	281aed28-724d-4a7b-ab36-669955c945b0
Muzaffar	16	12	2025	0000	Super logged in	ad98e95e-f872-4fae-bfff-6c20ae71d552
Muzaffar	16	12	2025	0000	All admins fetched	b41c3b36-e066-489b-8dc3-e9c54006e479
Muzaffar	16	12	2025	0000	All admins fetched	0874d438-3e9d-4979-b266-ae1514845aed
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	4d36291e-f568-4a29-a95e-a0c4c903e542
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	cbbf7190-fe94-4fd5-b8cc-aaa2dbaf3125
Muzaffar	16	12	2025	0000	All admins fetched	94ab8add-2414-46eb-8468-0cbf7757f3c8
Muzaffar	16	12	2025	0000	All admins fetched	b270c359-d3f2-433f-acc3-fcdbe64bf93d
Muzaffar	16	12	2025	0000	All admins fetched	6610b1b2-d4d0-4867-a6f2-8607f901fcd3
Muzaffar	16	12	2025	0000	All admins fetched	8a2d3a8c-0094-4d43-bbd4-bdc60cbbfc7d
Muzaffar	16	12	2025	0000	Fetched products for shop	2f779c83-c6ee-402b-b96b-bff09acb4bba
Muzaffar	16	12	2025	0000	Fetched products for shop	079d9112-aa21-4fba-b356-f7a22216afd3
Muzaffar	16	12	2025	0000	Fetched products for shop	ba58a382-0944-42e2-9dfd-3fd4d7f29491
Muzaffar	16	12	2025	0000	Fetched products for shop	6318092f-c590-4a3b-bf59-0c9acee1f766
Muzaffar	16	12	2025	0000	Fetched products for shop	52f1eab2-5e06-4085-9503-92eac3d24fca
Muzaffar	16	12	2025	0000	Fetched products for shop	b2fcf3d5-cdcd-47a5-83f3-8ef3e3d63a36
Muzaffar	16	12	2025	0000	Fetched products for shop	cc28486d-703f-4add-a869-7c83f7abe1af
Muzaffar	16	12	2025	0000	Fetched products for shop	7b5a4cbd-c8dc-4b50-896e-2a304d875df4
Muzaffar	16	12	2025	0000	Fetched products for shop	b5f45480-7bb3-4a6b-a1ed-9ed6f0642a02
Muzaffar	16	12	2025	0000	Fetched products for shop	20ddc3ad-7136-45f3-a0ff-03f1a58ab5da
Muzaffar	16	12	2025	0000	Fetched products for shop	e466314b-34c9-4685-999c-7076b055e80a
Muzaffar	16	12	2025	0000	Fetched products for shop	b19ee8a9-10f1-4d14-9108-f5dedbef7a47
Muzaffar	16	12	2025	0000	Fetched products for shop	756847c6-727b-472c-a4a3-eeb90e7e1a13
Muzaffar	16	12	2025	0000	Fetched products for shop	27ef11ec-d984-4968-aced-ec741b7f6207
Muzaffar	16	12	2025	0000	Fetched products for shop	105fe3de-acae-4b7e-a431-347424cd9f39
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	0c5d0428-2b20-4217-9589-dee7b79c46f7
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	1e738745-0e29-42a0-97e4-2251d8aa7ed4
Muzaffar	16	12	2025	0000	Fetched products for shop	226bf396-75fc-471b-809f-c3cb40bafdce
Muzaffar	16	12	2025	0000	Fetched products for shop	8cc1b29d-0c87-4b5b-974c-0ddd328abe53
Muzaffar	16	12	2025	0000	Fetched products for shop	7be59f82-98ec-455b-98be-b8dfaba12d5c
Muzaffar	16	12	2025	0000	Fetched products for shop	66ea8e4c-6d5f-45a8-affb-122c27a74367
Muzaffar	16	12	2025	0000	Fetched products for shop	0d07afaf-05f8-4652-8165-b32eb032008b
Muzaffar	16	12	2025	0000	Fetched products for shop	b6ef8542-edaa-4589-8b18-466bb00d3908
Muzaffar	16	12	2025	0000	Fetched products for shop	e988ae0c-2753-48b3-b7da-5a10e171726c
\N	16	12	2025	0000	Sale created successfully with sales_id: 449fccaa-e68e-4fc7-a288-0da259760025	6f89d292-5075-484f-a2b5-b3e83c8508ba
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	f4613c04-f9c7-402a-b5bd-9eca9d0f4586
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	a491dbae-9b70-4992-bb4c-9f7dae73849c
Muzaffar	16	12	2025	0000	Fetched products for shop	0802134a-6358-41e2-afa0-776348245c7e
Muzaffar	16	12	2025	0000	Fetched products for shop	93d4e28d-e549-4bae-a1bd-89f5009017a1
550e8400-e29b-41d4-a716-446655440000	16	12	2025	\N	Superuser logged out	16f3bfb5-41c7-46ee-a268-eddbffa804e4
Abduqodirov	16	12	2025	\N	SuperUser Login FAILED - not found	d2ed41d5-12f0-46d9-b467-f58eaf403387
Abduqodirov	16	12	2025	\N	Admin Logged In	b33b4e54-3205-4221-bc34-eabee49c4680
Abduqodirov	16	12	2025	0000	Fetched products for shop	3b89d6a3-9789-4696-9658-b8932d1e9381
Abduqodirov	16	12	2025	0000	Fetched products for shop	c8950108-2e5b-43a2-80cf-fdc413cea89e
Abduqodirov	16	12	2025	0000	Fetched products for shop	77ef3485-1f4e-4f8c-acec-16ed4471d55a
Abduqodirov	16	12	2025	0000	Fetched products for shop	93c952b6-3a39-4a42-8546-1c1a3970031e
Abduqodirov	16	12	2025	0000	Fetched products for shop	a6f82909-e791-46a5-a26e-5da310d5a30e
Abduqodirov	16	12	2025	0000	Fetched products for shop	c8997197-bf69-4acc-be74-ba4d5cbf2865
Abduqodirov	16	12	2025	0000	Fetched products for shop	4721f3dc-22ea-461f-aece-32569a14c92e
Abduqodirov	16	12	2025	0000	Fetched products for shop	7473402b-3a78-4826-91e7-90351f418065
Abduqodirov	16	12	2025	0000	Fetched products for shop	9c56fde2-4f3b-43d2-872f-887cdf3376fe
Abduqodirov	16	12	2025	0000	Fetched products for shop	882a6891-d135-4cf9-ae5d-bd94f0a02760
Abduqodirov	16	12	2025	0000	Fetched products for shop	3be565f2-c981-467e-beed-ee35ee25e9d9
\N	16	12	2025	0000	Missing required fields in createNewProduct	5adef6c2-5911-4024-a09f-a6e70c309d24
Abduqodirov	16	12	2025	0000	Fetched products for shop	5477a70b-c2de-4c71-95d6-3dfa02531d4e
Abduqodirov	16	12	2025	0000	Fetched products for shop	41e179b8-c52e-42cf-b049-ef82aa72e8bb
Abduqodirov	16	12	2025	0000	Fetched products for shop	c8da2325-17de-44cd-a104-9022f2742cab
Abduqodirov	16	12	2025	0000	Fetched products for shop	23b43b34-84b4-466b-b02c-2fbc2c180b54
Abduqodirov	16	12	2025	0000	Fetched products for shop	2341a402-25d7-4b54-ae07-e43816febca2
Abduqodirov	16	12	2025	0000	Fetched products for shop	c3a7415a-3eff-427f-81bc-b1195c5910fe
550e8400-e29b-41d4-a716-446655440000	16	12	2025	\N	Admin logged out	531601ed-0736-48f3-8c16-06a71e513aa9
Abduqodirov	16	12	2025	\N	Admin Logged In	b9bd71aa-7da7-46e7-813c-cb32e1a5dfce
Abduqodirov	16	12	2025	0000	Fetched products for shop	abcc3f9d-ad80-43a4-a213-3e1ee4e26079
Abduqodirov	16	12	2025	0000	Fetched products for shop	481a6ec3-1da8-4e86-8624-6b68fdc44118
Abduqodirov	16	12	2025	0000	Fetched products for shop	9c7d7d02-51b2-4b39-b131-7043e95c9747
Abduqodirov	16	12	2025	0000	Fetched products for shop	4cc7f940-ef30-411b-be84-e1a26c985dd1
\N	16	12	2025	0000	Sale created successfully with sales_id: dd9faf32-b915-4252-a250-e14c412a6d06	ea911803-d77c-4064-8d89-3c1e4408da79
Muzaffar	16	12	2025	0000	Super logged in	1f2aa217-6cf9-4e1d-9ec2-44cda7df2bb8
Muzaffar	16	12	2025	0000	Fetched products for shop	950d73eb-0194-467c-8102-db2714d9f929
Muzaffar	16	12	2025	0000	Fetched products for shop	a000d67d-c13b-4e2c-a655-3a88dfec1cbd
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	5df422ad-6254-407a-af62-7b80ee5e1402
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	007bed15-c3c0-4fbb-a1b0-ef3f88ac202a
Muzaffar	16	12	2025	0000	Fetched products for shop	dae1b260-5974-4e98-b72c-3dd39090989e
Muzaffar	16	12	2025	0000	Fetched products for shop	1bb3f182-c4b1-4dcf-a4ad-aff22ddf6881
Muzaffar	16	12	2025	0000	All admins fetched	18a502ae-d7dc-49e3-977f-4d340bf2d548
Muzaffar	16	12	2025	0000	All admins fetched	e47b01c3-3711-403f-a0fc-5b2c1106d029
Muzaffar	16	12	2025	0000	Fetched products for shop	723079b3-fbe7-4c61-8f6e-40699ee7cd53
Muzaffar	16	12	2025	0000	Fetched products for shop	30f9f05f-25fc-4997-a24a-31863bc11296
Muzaffar	16	12	2025	0000	All admins fetched	a4bdcda2-3132-45ce-8eb7-f069cee203eb
Muzaffar	16	12	2025	0000	All admins fetched	8436d4b4-1ab6-4e28-ab37-53e2921f77fb
550e8400-e29b-41d4-a716-446655440000	16	12	2025	\N	Superuser logged out	f8bbb7e9-e495-4319-9105-c6e8ac0b49de
Xayirullo	16	12	2025	\N	Admin Login FAILED - not found	026e424f-5b31-4f76-aa0a-e643f1d22265
Xayirullo	16	12	2025	\N	Admin Login FAILED - not found	4d121bfd-d5ff-41f5-b0ac-c5396054aa76
Rozimatov	16	12	2025	\N	Admin Logged In	6ceb12fd-f7dc-4af8-b9e7-aeb7a018099e
Rozimatov	16	12	2025	0000	Fetched products for shop	f0388eb1-07d8-476f-a53a-03194f08a149
Rozimatov	16	12	2025	0000	Fetched products for shop	98a1cd19-8b91-4894-93d7-68fb4ae7391b
Rozimatov	16	12	2025	0000	Fetched products for shop	1cec92d4-100a-496c-8b95-5d4be115bb66
Rozimatov	16	12	2025	0000	Fetched products for shop	eb364f26-7f1c-47ba-a167-20a3a227bad7
Rozimatov	16	12	2025	0000	Fetched products for shop	1a03cc00-192a-4dfd-907a-d6ba93ab3c8e
Rozimatov	16	12	2025	0000	Fetched products for shop	40dc883d-abba-42e7-be7a-5a3323489c29
\N	16	12	2025	0000	Sale created successfully with sales_id: bba120b3-8337-4326-9d56-39ed5523605d	1c94aa79-5698-45e2-bf0f-816387c710cf
Abduqodirov	16	12	2025	0000	Fetched products for shop	f3f89f4f-f338-452b-9a94-60d3eb62f109
Abduqodirov	16	12	2025	0000	Fetched products for shop	b074152c-10d2-4472-b3f2-1c5c5785e8d6
550e8400-e29b-41d4-a716-446655440000	16	12	2025	\N	Admin logged out	15d3b5ea-b553-4f8f-958e-ae25858605ea
Muzaffar	16	12	2025	0000	Super logged in	b50064ff-5814-4086-a174-cd86636c3871
Muzaffar	16	12	2025	0000	Fetched products for shop	e5c6bc87-111d-4ff6-ab8c-d3045f02dccf
Muzaffar	16	12	2025	0000	Fetched products for shop	3b5ad04d-936d-43dd-9aa4-1e12016908f2
\N	16	12	2025	0000	Sale created successfully with sales_id: df01c05a-1dbe-4fc4-b92c-352d5ba47017	617a5fe9-b680-42f6-99c7-8b556cbc2986
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	4a10f605-3d4d-4298-a8d9-a43af9399bcf
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	6314c668-c331-4662-9161-37efb4b1d810
Muzaffar	16	12	2025	0000	Fetched products for shop	db0be37d-c3df-488b-9cd9-2cbb17e27439
Muzaffar	16	12	2025	0000	Fetched products for shop	4b9e1a62-4897-4c09-b23e-5ed707b0ba6c
\N	16	12	2025	0000	Sale created successfully with sales_id: 44aef04f-6704-4a48-8232-46bb6c60fb6b	508108e8-2409-433f-8fce-0ac8d95bfda3
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	8b9a100c-e6b1-4d71-b886-1858d5e8cfb6
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	8f137248-67b3-4824-9461-376d26e11df6
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	98dfb438-47a2-45cd-a116-8dc8bac67e26
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	a7168393-1ede-4a8f-b1c3-035412b26f36
Muzaffar	16	12	2025	0000	Fetched products for shop	0a8819a7-814d-4941-b03e-0994aead2f83
Muzaffar	16	12	2025	0000	Fetched products for shop	8513fc9a-678e-4cbf-a41e-3b1afe057cfd
\N	16	12	2025	0000	Sale created successfully with sales_id: c32bf7eb-1f9f-46e5-abff-1d63eccc0e8e	2216641e-39cd-45c0-88ff-0050df14f487
Muzaffar	16	12	2025	0000	Fetched products for shop	a4807e48-09d6-4f17-b30c-2c30d2b4ed5d
Muzaffar	16	12	2025	0000	Fetched products for shop	c393b712-896b-4051-b16b-33e0df40c669
\N	16	12	2025	0000	Sale created successfully with sales_id: 4f895b3f-a048-4c23-8d27-f2d3614ec751	253b36ee-61e3-447b-8aa2-5e0a0f7e00e7
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	36cd097b-93c6-48a7-98e7-39fc87e2e404
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	b1273bef-752f-4cc8-aca0-8008b59cfc61
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	1241b738-09cb-482d-a3ef-5b53507bd11a
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	a2a615fe-b1d6-4ca3-9500-95fd3d125c95
Muzaffar	16	12	2025	0000	All admins fetched	94a10adc-e434-4069-bee3-9d01eea984c0
Muzaffar	16	12	2025	0000	All admins fetched	e810c269-a4e8-459b-b96e-0c7e032dc761
Rozimatov	16	12	2025	0000	Fetched products for shop	173df5d2-04a2-4ac5-84f3-6ba6853d7030
Rozimatov	16	12	2025	0000	Fetched products for shop	28267529-1291-4139-bc25-bb9c09beb86c
0.7329386920447608	16	12	2025	\N	Admin logged out	1bac0583-7d43-42d9-8700-2a2bdce651ee
Muzaffar	16	12	2025	0000	Super logged in	00f9acb0-82dc-43d1-84ae-69518487ea7d
Muzaffar	16	12	2025	0000	Fetched products for shop	bbe18d28-610f-4ee3-808b-dc9516746b4f
Muzaffar	16	12	2025	0000	Fetched products for shop	850fe91f-2e22-4812-b901-b4938a74280f
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	922136c4-7bdf-4094-bbd5-cf72b407e1b3
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	62257a31-1a79-451e-9c5a-b96f665eae81
Muzaffar	16	12	2025	0000	Fetched products for shop	afa3637e-e808-4f56-89d3-7f27642eaef7
Muzaffar	16	12	2025	0000	Fetched products for shop	2bcc9ebe-b4cf-403e-aa0a-3c9fe59a3342
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	58a6a732-d7d3-44ef-83f0-0a78a0caeffa
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	f0aeb0ab-7153-4527-98b7-9003562cd305
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	51f2b5eb-536d-436f-997d-e96807e073ba
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	061fff7f-a36b-4652-b2ae-cec019a964ac
Muzaffar	16	12	2025	0000	Fetched products for shop	7ad5d494-68a8-4571-8f73-9a3199c3671c
Muzaffar	16	12	2025	0000	Fetched products for shop	da23784b-b835-44f0-9ca9-77af7ab3e752
\N	16	12	2025	0000	Sale created successfully with sales_id: c61a6ab2-849f-456e-874b-c6128fb82374	5b02becd-f28a-4615-94f9-a21a7cc5d4fa
Muzaffar	16	12	2025	0000	Fetched products for shop	e5134d18-f3ac-484f-99fd-cec7d526bae0
Muzaffar	16	12	2025	0000	Fetched products for shop	cbe508fc-881f-49c4-9175-0a4814456b9c
Muzaffar	16	12	2025	0000	Fetched products for shop	e9d347cd-a3df-44e8-a0fb-2a892c41cf2a
Muzaffar	16	12	2025	0000	Fetched products for shop	cdb077cf-c93e-4995-bff5-aa05b0db9145
Muzaffar	16	12	2025	0000	Fetched products for shop	75ad0bd4-d705-491f-88f0-7b586650a8a4
Muzaffar	16	12	2025	0000	Fetched products for shop	9ccd31df-acf0-4ef7-9428-bc5a711369f7
Muzaffar	16	12	2025	0000	Fetched products for shop	c30c86af-fa10-48c9-a235-26a408496c85
Muzaffar	16	12	2025	0000	Fetched products for shop	4585aad3-3513-4606-aaf9-d189ef009fc1
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	2697883c-4045-4c24-930f-269a86e39915
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	5b12dfa0-c3d1-4755-9352-4e4e419982b9
Muzaffar	16	12	2025	0000	All admins fetched	4142769a-3771-43ad-b933-b5f92b95692e
Muzaffar	16	12	2025	0000	All admins fetched	d9ca189d-0f48-495d-8673-58cf61945004
Muzaffar	16	12	2025	0000	Fetched products for shop	86c4582d-2cc6-4d73-b0ec-2eff4b851bf1
\N	16	12	2025	0000	Sale created successfully with sales_id: 38f74ef1-0593-4ed5-8043-178e76da5ec4	d7702660-9555-4433-aff2-2a7ae29e9a22
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	e2d9d0cf-b94e-4d6d-9c75-64f09a757e45
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	d916e761-7b6d-490b-bdde-e0474a0c13c3
Muzaffar	16	12	2025	0000	Fetched products for shop	ac0a66fd-ab01-4f86-93e2-ef57976bf389
Muzaffar	16	12	2025	0000	Fetched products for shop	4e830378-8ca1-4d03-8448-71d52b31af48
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	8d1caf8c-630e-48e0-8fc0-477b5a9effea
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	50f1afe3-2033-4dc3-8231-b72e06a50c18
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	8135989a-c85e-4c56-acd7-d4734db2cea8
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	0ebe5186-b279-4bc3-844e-820ae3c55998
Muzaffar	16	12	2025	0000	Fetched products for shop	0f9baff5-74d5-4dd7-aba8-38cb254080ac
Muzaffar	16	12	2025	0000	Fetched products for shop	58e7cf1f-b3fc-41de-8ba8-6d7dad089ab6
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	527cc1f4-75d6-488f-ae63-ede2c3142d01
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	708a2f46-810d-4d13-a2fa-e944ca5917e1
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	ab0123ed-2a27-4f0a-b297-f8d457610965
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	19da6e61-532d-4cbd-8611-1b17c5d1c341
Muzaffar	16	12	2025	0000	Fetched products for shop	b4334391-5525-4a5c-89fe-db98b644f7cf
Muzaffar	16	12	2025	0000	Fetched products for shop	39aba2d1-0edf-4c15-8944-3c8c13415a82
Muzaffar	16	12	2025	0000	Fetched products for shop	2a98cee3-2b09-4a52-a3c5-dc02bb7cd10d
Muzaffar	16	12	2025	0000	Fetched products for shop	52cb5494-bcd6-46c2-bdab-ddaf3ea16d00
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	f1ea790d-454a-44bd-a0af-03079de60673
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	495eb607-d7d6-4021-ab40-7eb889d8c594
Muzaffar	16	12	2025	0000	Fetched products for shop	a8b23e61-2a29-419e-b26b-3068599b0954
Muzaffar	16	12	2025	0000	Fetched products for shop	86f0e33c-959b-4153-95fe-e77adfba3250
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	08a94685-e8b7-449c-a5ce-73eddb093195
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	5433afff-f044-4636-9208-94e4e7de0d0e
Muzaffar	16	12	2025	0000	Fetched products for shop	fba69b84-c441-44c1-b1c0-7f632e2b03cc
Muzaffar	16	12	2025	0000	Fetched products for shop	05983136-4d30-4727-ad68-2e8242dad018
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	bfe9de70-31b4-4200-a40b-b479a2df67c0
Muzaffar	16	12	2025	Muzaffar	Fetched all sales	2d4171cb-3a30-46e1-8735-22e87530dc48
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	bb89b8de-de90-4083-bafb-e2c0f2254f15
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	0f885952-658e-4317-aeba-25432d39488c
Muzaffar	17	12	2025	0000	Fetched products for shop	d721fc94-4083-40b0-9c67-06865baf1b0d
Muzaffar	17	12	2025	0000	Fetched products for shop	4c9a1ee9-5a9c-4061-979c-f0a1d495391c
Muzaffar	17	12	2025	0000	Fetched products for shop	a33fee19-05cf-479a-959a-64e932e2c064
Muzaffar	17	12	2025	0000	Fetched products for shop	7496f585-a42b-4237-84b6-03e8dd1cec89
Muzaffar	17	12	2025	0000	Fetched products for shop	56066276-a3dc-46f8-ab20-e09a951ccc37
Muzaffar	17	12	2025	0000	Fetched products for shop	610f9df6-237f-411d-9531-1b945b7bffc8
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	1daecf17-1607-4145-a858-29998f053093
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	eb7cf8aa-31cd-4e89-91ac-b4f491feb215
Muzaffar	17	12	2025	0000	Fetched products for shop	ac0a913f-297c-4281-8758-f650721f5730
Muzaffar	17	12	2025	0000	Fetched products for shop	f236996d-8b20-481b-9e73-f73c0b844fa9
\N	17	12	2025	0000	Sale created successfully with sales_id: 17d3f1d4-4930-4bc6-8dc0-504faf222c1d	2b0599f3-9c34-4cc9-a4d7-526a8eabd617
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	81c7e8c6-9bc6-41fa-8213-e5725c896008
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	10a2cfc6-9565-41cb-ab34-478326d1e237
Muzaffar	17	12	2025	0000	Fetched products for shop	06c0ed94-c25a-4e43-b968-5d0625833478
Muzaffar	17	12	2025	0000	Fetched products for shop	02f57ef1-0caa-4c47-8cc9-87333c957a41
\N	17	12	2025	0000	Sale created successfully with sales_id: 4b18c951-a7e4-491f-94c4-c0657912b0de	6b774401-9b6a-4189-a468-e758d06f3774
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	215db396-8fbc-43b2-9b7d-240e5624248f
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	8b713bf0-9bcc-4545-9250-af9cc26ebaab
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	aac1c5e5-3769-4e84-aa92-74b853aee911
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	8b001082-59ee-41fb-982d-aa899961da4a
Muzaffar	17	12	2025	0000	Fetched products for shop	d1f63101-9621-414c-b1a0-89420a39ff9e
Muzaffar	17	12	2025	0000	Fetched products for shop	e8eb2770-edf9-402e-bf4b-b9443c0f506f
Muzaffar	17	12	2025	0000	All admins fetched	c8364bc6-60db-4be9-88e4-dbfd720375cc
Muzaffar	17	12	2025	0000	All admins fetched	c69057ae-b73b-4b5b-88c6-b6e8599a063d
Muzaffar	17	12	2025	0000	Fetched products for shop	2f7b12c3-0264-4c39-9bf3-6c37ab1ae6ef
Muzaffar	17	12	2025	0000	Fetched products for shop	14e39340-086a-49fd-9eeb-fe38d8e85b82
Muzaffar	17	12	2025	0000	Fetched products for shop	9cd66a41-dd3d-438a-8f5c-b0cbf8753216
Muzaffar	17	12	2025	0000	Fetched products for shop	7665cfdc-ffb0-4b6b-8513-5b91054b059b
Muzaffar	17	12	2025	0000	Fetched products for shop	267bd89a-bd2d-4f11-8370-fc8f397adcb7
Muzaffar	17	12	2025	0000	Fetched products for shop	7f4ed2c8-313c-4bd3-9b10-f8363fa85099
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	3d9080d3-a1f0-47ad-a495-69f24720be02
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	b84a482d-7404-41c2-b4de-4e022fd3ebe5
Muzaffar	17	12	2025	0000	Fetched products for shop	4b56fecc-792a-4b7a-b02e-b6bded877d70
Muzaffar	17	12	2025	0000	Fetched products for shop	a8bfa448-ff47-458a-9e9f-cd7cd2e76fe2
Muzaffar	17	12	2025	0000	All admins fetched	161ba28c-f953-4f62-bc63-ecf133527a63
Muzaffar	17	12	2025	0000	All admins fetched	070f5eac-b96f-4bb5-a506-0650b4414db8
Muzaffar	17	12	2025	0000	Fetched products for shop	8cbcbbd5-4f8a-45ee-bda9-526ea4a5416c
Muzaffar	17	12	2025	0000	Fetched products for shop	d9cefb64-c670-4f02-b918-aef8219ff7c2
\N	17	12	2025	0000	Missing required fields in createNewProduct	dfc2544a-be49-4375-849d-b25817ee7383
\N	17	12	2025	0000	Missing required fields in createNewProduct	6234dd5c-9bbb-4207-9f7c-5936d8ba309e
\N	17	12	2025	0000	Product created: c1fa21a0-8889-4b05-8b80-da647ed38001	a49b2ee9-1812-40a5-b6af-1a4de21454e1
Muzaffar	17	12	2025	0000	All admins fetched	537907a7-0ef7-409f-8777-1ef08a0bdf5f
Muzaffar	17	12	2025	0000	All admins fetched	340c4ed4-34c3-4f0a-9f0c-2987c1a32c72
Muzaffar	17	12	2025	0000	All admins fetched	e9132241-2497-4e5e-8137-eb1a56ab71a0
Muzaffar	17	12	2025	0000	All admins fetched	77f42a94-f95b-4642-87fe-b73b416cace6
Muzaffar	17	12	2025	0000	Admin created	a0dda491-ce26-4f6a-9578-0aea73dc4b8e
Muzaffar	17	12	2025	0000	All admins fetched	7d265b32-82db-48e5-b026-dfeac8cb526a
Muzaffar	17	12	2025	0000	All admins fetched	9e6f84f7-a4d4-4681-85e8-afd218028563
Muzaffar	17	12	2025	0000	Fetched products for shop	f14a8eb9-0352-45dc-8061-f9a423fd653b
Muzaffar	17	12	2025	0000	Fetched products for shop	6f58b9bd-6e3a-48ea-b48b-aa0bdbb5d70e
Muzaffar	17	12	2025	0000	Fetched products for shop	abf68858-9f78-4818-a38d-e40b5e4caffa
Muzaffar	17	12	2025	0000	Fetched products for shop	eb721a7b-9df6-4e97-a3b3-4b3471cca7f0
Muzaffar	17	12	2025	0000	Fetched products for shop	48160f16-3540-4c2a-8968-a7c06797691c
Muzaffar	17	12	2025	0000	Fetched products for shop	88a7584c-c151-4ae8-8772-619ac750b7df
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	314f0867-209d-4179-8b25-63401ad1bdb6
Muzaffar	17	12	2025	Muzaffar	Fetched all sales	fb281ae4-11d2-4df0-acb7-15a0e81c6a10
Muzaffar	17	12	2025	0000	Fetched products for shop	8bd28fa8-02ce-4972-bac3-6388c6158a9d
Muzaffar	17	12	2025	0000	Fetched products for shop	201d221e-d51f-4091-a834-4671b288ce13
Muzaffar	17	12	2025	0000	All admins fetched	7e7c48ea-7b5d-4dc4-99c8-b3b8d7a68ef6
Muzaffar	17	12	2025	0000	All admins fetched	1e4cc26a-9033-4ffd-8a94-95faeced82e1
Muzaffar	19	12	2025	0000	Super logged in	f94071e0-9676-4988-8805-0bc7ef33e76e
550e8400-e29b-41d4-a716-446655440000	21	12	2025	\N	Superuser logged out	63193a3f-f5cb-4e38-af3e-9ecac335452c
550e8400-e29b-41d4-a716-446655440000	21	12	2025	\N	Superuser logged out	2fd5a9cf-ca70-4eac-a280-bbbd4bacd31c
Muzaffar	21	12	2025	0000	Super logged in	1f82805b-739f-4150-865c-ae756c06b250
Muzaffar	21	12	2025	0000	Fetched products for shop	d8b79884-3108-46d5-8dab-8f6f8a559e63
Muzaffar	21	12	2025	0000	Fetched products for shop	6c31a9c5-5d83-4b5b-a504-5cc1917ddba8
Muzaffar	21	12	2025	0000	Fetched products for shop	5d31fb9f-95b5-4219-89cb-dde9d30e5931
Muzaffar	21	12	2025	0000	Fetched products for shop	d6892ea0-5e15-4fcf-9b21-b78c3944e031
Muzaffar	21	12	2025	Muzaffar	Fetched all sales	fe841d19-9cfc-4e61-a05b-a0f43b4cf619
Muzaffar	21	12	2025	Muzaffar	Fetched all sales	2ad18e6e-1b1e-4ae2-9473-10c2d163415a
Muzaffar	21	12	2025	0000	Fetched products for shop	0109a93e-74f5-41e6-8f42-a1f849f4f5cf
Muzaffar	21	12	2025	0000	Fetched products for shop	fd5017c2-d542-444e-840a-1bb9e55d842d
Muzaffar	21	12	2025	Muzaffar	Fetched all sales	434c484c-2fc9-43b9-8cfb-7095d8439879
Muzaffar	21	12	2025	Muzaffar	Fetched all sales	d6838146-3e23-494b-a8a7-ff6f9835ed9f
Muzaffar	21	12	2025	0000	Fetched products for shop	2cbd8441-a158-4f08-a99e-6ecd09c902a8
Muzaffar	21	12	2025	0000	Fetched products for shop	c6087343-1890-4948-a5c5-44329ce8dc33
Muzaffar	21	12	2025	0000	All admins fetched	52aecd30-1cb7-41ac-90c1-51a996f085bf
Muzaffar	21	12	2025	0000	All admins fetched	a49c34c3-2b22-468d-a34a-74f9eaaa7cba
Muzaffar	21	12	2025	0000	Admin updated	c58cd9eb-afca-44fa-8494-dbd93d9da5f5
550e8400-e29b-41d4-a716-446655440000	21	12	2025	\N	Superuser logged out	5f1bcdac-9048-4186-89e8-97dc7125d2e2
Muzaffar	21	12	2025	0000	Super logged in	b7f6ac21-04f6-4937-8367-d0cdc6905fa0
Muzaffar	22	12	2025	0000	Fetched products for shop	37f164cc-7f9e-4524-8d7a-f1e9d167e151
Muzaffar	22	12	2025	0000	Fetched products for shop	67b63b13-60e7-4b04-a3d0-8d79da5ec6ee
Muzaffar	22	12	2025	0000	Fetched products for shop	066826fa-7c6e-4a1d-a942-09aa5c9dcf9d
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	e197cffc-0f4d-4206-b221-bd58def608e1
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	dd66534a-6420-4723-9f8e-48261c2b4601
Muzaffar	22	12	2025	0000	Fetched products for shop	080f237f-f664-4265-8cfd-8ea11b6f4946
Muzaffar	22	12	2025	0000	Fetched products for shop	9941f1b2-0cac-4513-9c59-d309f4682721
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	16fb28d7-7416-4da4-a4f6-714b841ede40
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	df4103fc-affd-48e7-83c5-0b49a4d0fe9f
Muzaffar	22	12	2025	0000	Fetched products for shop	97d53e4e-bf0d-479d-98e4-b8b52848561c
Muzaffar	22	12	2025	0000	Fetched products for shop	bc460378-af36-4fac-913e-b16421316ac6
Muzaffar	22	12	2025	0000	Fetched products for shop	1b8ee08d-f6d3-4352-a078-c4511a63a25d
Muzaffar	22	12	2025	0000	Fetched products for shop	0ca580d9-43d6-4e9b-bf9a-8bb4821cf3e2
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	227b2bc5-b84a-4e44-b98f-b6ba0ea0fcab
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	9f4d752c-8a52-44f9-b59b-ac540d444ee6
Muzaffar	22	12	2025	0000	Fetched products for shop	48a91f07-20c4-489d-8155-ff3c795bf42b
Muzaffar	22	12	2025	0000	Fetched products for shop	076c09ba-330f-40c3-a987-5a40c1040779
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	e1243495-2a68-4a64-886c-dbc278d0efb1
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	7359f254-3f1f-47e8-90ec-c96071144cd0
Muzaffar	22	12	2025	0000	Fetched products for shop	33d96762-82a9-4905-850a-c81bd4e403b7
Muzaffar	22	12	2025	0000	Fetched products for shop	a366adc3-d93a-41d7-a39a-3a554e712a2c
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	36011e03-e204-47de-b4f0-ab934f1d2bee
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	ff106c5e-25cc-4085-ac12-2b09f15e69d2
Muzaffar	22	12	2025	0000	Fetched products for shop	69596620-d179-44fa-918f-a4b918e1dd1c
Muzaffar	22	12	2025	0000	Fetched products for shop	383bbd9b-d5d8-480c-adc9-69aba2cfd0a5
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	d6e0201d-288e-4ed6-baca-f3ea08e8ea2b
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	57094a07-0a23-44d4-8489-1972e09d0bc7
Muzaffar	22	12	2025	0000	Fetched products for shop	9376c967-4406-40f6-a9da-89c710447b16
Muzaffar	22	12	2025	0000	Fetched products for shop	8f121d0d-46a3-49ea-aa49-61d5598af7a7
Muzaffar	22	12	2025	0000	Fetched products for shop	9bd12fa2-0878-4c2a-b29c-c13939055821
Muzaffar	22	12	2025	0000	Fetched products for shop	b279bf76-2dbb-4154-85d2-1d712ada82ed
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	6757edc4-165e-4547-9d49-f614a2f58c70
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	fea2c4d3-7154-41ea-99f7-9af57b7ac1c4
Muzaffar	22	12	2025	0000	Fetched products for shop	ad55905b-faed-4412-b044-3b01ed38530f
Muzaffar	22	12	2025	0000	Fetched products for shop	059b6c7a-ad75-43f6-aaa7-046819eb63ce
Muzaffar	22	12	2025	0000	All admins fetched	bea27075-3343-4945-90e4-729729f29f94
Muzaffar	22	12	2025	0000	All admins fetched	22b1cd13-3de5-49fc-8fd4-24fa068eb619
Muzaffar	22	12	2025	0000	All admins fetched	0119a89a-7a85-46b7-a915-3af5530d612b
Muzaffar	22	12	2025	0000	All admins fetched	d0281f24-05e4-40c1-aacc-33e8a144f224
Muzaffar	22	12	2025	0000	Fetched products for shop	04d2ec16-98af-4c9b-a6f5-4e04096ea8e9
Muzaffar	22	12	2025	0000	Fetched products for shop	c7c1907e-b120-4170-a5c9-7fd9ac513bb2
Muzaffar	22	12	2025	0000	Fetched products for shop	1719d5a4-3d70-4459-b495-ae52a92e5077
Muzaffar	22	12	2025	0000	Fetched products for shop	0bcbb886-8f0d-4407-bb24-3c1211c5213d
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	55485b8a-c950-4cc6-8140-213508d19e70
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	c8e3b297-4eae-40b9-a197-fe3cafe938e3
Muzaffar	22	12	2025	0000	Fetched products for shop	64232845-70a1-4eda-b2ae-51da4b184203
Muzaffar	22	12	2025	0000	Fetched products for shop	9e20bb37-650f-4cbd-a96f-10f57f9428aa
Muzaffar	22	12	2025	0000	Fetched products for shop	a0016118-c3c6-4778-a0a4-abe579ac4da0
Muzaffar	22	12	2025	0000	Fetched products for shop	bbefdd26-8770-4672-8661-2f1908802f2c
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	f4c4a362-aee6-49e4-9958-2395e654c2c9
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	1ebc2ccc-8bc6-476f-a1d9-bff42dd51c13
Muzaffar	22	12	2025	0000	Fetched products for shop	895df9e4-926a-4cd4-ad89-afa26a1243a6
Muzaffar	22	12	2025	0000	Fetched products for shop	9d3d6680-a6dc-4a3d-abab-8fa5c3162080
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	c18cc889-db1d-4d67-8f6c-1ac5d424733c
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	6d03af2e-bf02-4514-8156-86decef03d5b
Muzaffar	22	12	2025	0000	Fetched products for shop	7318843d-a246-4b96-aee6-c434e5389b95
Muzaffar	22	12	2025	0000	Fetched products for shop	f75f17d6-b4ab-498a-81ea-8a89f2c54ba0
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	014f4413-d427-4062-948b-571128185273
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	3643d57e-58c3-4dc7-bf28-a252d488e787
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	54f51564-b0b5-417f-a894-403573fec60e
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	dd828dd5-1062-497a-a9f9-0a5cfa6ffed4
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	02b158c1-e01a-4a3f-8ef2-909cc60efc86
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	a296d3be-b303-42ec-bb0b-3f44a065ef5b
Muzaffar	22	12	2025	0000	Fetched products for shop	96f55164-fd01-4194-95ed-dcbc892faf15
Muzaffar	22	12	2025	0000	Fetched products for shop	7998cb0a-e015-4e93-9735-6b9978073a09
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	c6a86d72-0c0c-4af8-9f9f-2bab05621fe1
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	e0867e52-44db-4eb4-b1c8-c9c3ca40ab77
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	20a1de2a-5fcd-4710-822f-09b93cbf9121
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	3cd6f3ff-c329-468a-a73a-08d89e68f2c8
Muzaffar	22	12	2025	0000	Fetched products for shop	c9399368-737c-49e9-bf91-c878835b87eb
Muzaffar	22	12	2025	0000	Fetched products for shop	d25e19bd-ce56-4f0a-a46f-d9295d9752f8
Muzaffar	22	12	2025	0000	Fetched products for shop	706cef97-0368-4e51-aaad-0f78efb67e84
Muzaffar	22	12	2025	0000	Fetched products for shop	7a78c966-28ba-495b-92f7-5c421f02058b
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	437d83d8-f85c-4819-a4b2-df8481d69072
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	d26bac2e-08fd-4c37-ab1a-51b064c5fe2f
Muzaffar	22	12	2025	0000	Fetched products for shop	ea6de398-63b9-498c-a26f-2eea1bc1de3b
Muzaffar	22	12	2025	0000	Fetched products for shop	8a2d1936-ee1f-4ebb-8d10-1795b4474a3c
Muzaffar	22	12	2025	0000	Fetched products for shop	09ccfa15-aa25-43cd-b0ff-e24117c388e6
Muzaffar	22	12	2025	0000	Fetched products for shop	b591e7dc-3b44-4026-a2cb-d7dea9d2d080
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	fd00abd7-431f-4ddb-96b4-1fb46711a687
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	a1e2bb20-0e25-428f-a167-e7b34c8115a8
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	c93e97e6-33d2-40d8-a5d9-a1e2c1a59c3d
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	739e344a-f7a4-4888-bd9b-a718e1b48ed2
Muzaffar	22	12	2025	0000	Fetched products for shop	c3901d7b-fc10-4441-b19b-92223557ab15
Muzaffar	22	12	2025	0000	Fetched products for shop	c6047a0a-bc0d-4b7f-8933-dff6ef070af4
\N	22	12	2025	0000	Sale created successfully with sales_id: 0ddf9c90-f21e-493f-b5db-e933712b78f2	705a86d7-908f-40e4-9409-8951e98bd711
Muzaffar	22	12	2025	0000	Fetched products for shop	6687b044-8174-488f-923f-f69c0a5dedac
Muzaffar	22	12	2025	0000	Fetched products for shop	ed9eb55f-d556-41cb-8baf-1923f7ee017d
Muzaffar	22	12	2025	0000	Fetched products for shop	0a17ba33-756a-48a9-9bb2-d4f2ef53933e
Muzaffar	22	12	2025	0000	Fetched products for shop	5eae1d14-9540-4a67-94ed-6428c598ce04
Muzaffar	22	12	2025	0000	Fetched products for shop	b07f6b6f-92ec-4daf-902a-78d067a1e540
Muzaffar	22	12	2025	0000	Fetched products for shop	45847757-bdf1-40a1-b847-814c1be0265c
Muzaffar	22	12	2025	0000	Fetched products for shop	7db2022f-e871-4142-be78-e8cb25562630
Muzaffar	22	12	2025	0000	Fetched products for shop	060f8ca4-757b-45fc-95d2-6b7a94b69184
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	fdb22f01-c9c6-4a0c-8498-ae439c15078c
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	e42ffa08-0657-45e0-94ea-4144f964d7d5
Muzaffar	22	12	2025	0000	Fetched products for shop	c9c299f3-f389-47d8-bf73-8a12bfb78bc3
Muzaffar	22	12	2025	0000	Fetched products for shop	859687d8-ddaa-424d-8b6e-5ca7763fcb3b
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	e8b0ace7-ca1a-488a-abe0-3275809e3676
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	4ee759c9-c3b8-47d6-a2ab-defd6ac5daa0
Muzaffar	22	12	2025	0000	Fetched products for shop	470d7dbb-c899-442a-9040-e9036749a10d
Muzaffar	22	12	2025	0000	Fetched products for shop	92866e23-b94e-4700-be41-d42a54102a06
Muzaffar	22	12	2025	0000	Fetched products for shop	bfd8828e-44c0-4a39-8786-b9d98802f8a1
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	c4aa3e9a-a63b-4181-9eef-1b12834c687d
Muzaffar	22	12	2025	0000	Fetched products for shop	79097338-1d4d-4f09-99e6-0b47e0f0f4dc
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	f114d704-06b1-4522-8d17-619cad7aa390
Muzaffar	22	12	2025	0000	Fetched products for shop	3230f90e-692b-4c92-a5fe-c2b31ab10911
Muzaffar	22	12	2025	0000	Fetched products for shop	b9f7edf9-badc-475a-8cac-405cb7e9f536
Muzaffar	22	12	2025	0000	Fetched products for shop	6781a581-15f1-4fe2-994d-c4230c176d59
Muzaffar	22	12	2025	0000	Fetched products for shop	525c4366-d19f-453a-8b4a-4a6edc4ff289
Muzaffar	22	12	2025	0000	All admins fetched	ab080c95-3626-44e8-87a1-609fc25c4d84
Muzaffar	22	12	2025	0000	All admins fetched	dceebf07-288e-446f-8c15-e9c135496155
Muzaffar	22	12	2025	0000	Fetched products for shop	4d81850f-db47-4e1f-91f3-5fc7b5da3791
Muzaffar	22	12	2025	0000	Fetched products for shop	2b0747c5-a8a9-4526-8ded-175ab785208f
Muzaffar	22	12	2025	0000	Fetched products for shop	bab1c467-22d6-42a8-95a6-8b68a7fbf70e
Muzaffar	22	12	2025	0000	Fetched products for shop	a16ddfb4-ca2d-4901-881a-050ba904fa0f
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	37c61ee2-9626-4645-9701-c22c7cf276b8
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	a99f6449-6889-4835-a493-083f8dd8eac2
Muzaffar	22	12	2025	0000	Fetched products for shop	ff4ed41d-f0e9-41bf-a4f0-e80dff3a36ca
Muzaffar	22	12	2025	0000	Fetched products for shop	ebd5d501-bf4c-4b24-bda7-48d3d9d6985d
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	30d54c74-52de-442e-ba8f-b8aa7db71020
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	8f82aea1-3d90-4143-b9d3-13ec065feb99
Muzaffar	22	12	2025	0000	Fetched products for shop	f80d2b72-4a16-442c-a8a9-dc6400477129
Muzaffar	22	12	2025	0000	Fetched products for shop	3b45a2ff-b011-434c-bbf8-17ef3d39d4a9
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	c61105c5-1577-41b0-8375-ddf822776b19
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	d5f2ad97-7638-4b50-99c0-56c4871be2b7
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	138a88af-94a5-4161-9ae7-1c5b489a4abb
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	62118e19-5d7a-4321-8733-9eb5735e4ad6
Muzaffar	22	12	2025	0000	Fetched products for shop	16fc1cf6-a2b2-4bd8-8bfd-acbee38123f7
Muzaffar	22	12	2025	0000	Fetched products for shop	65e2eaa9-1b20-4938-9faa-80b7108168b4
Muzaffar	22	12	2025	0000	Fetched products for shop	b0391913-be99-4d2e-a38a-c754bbd282f1
Muzaffar	22	12	2025	0000	Fetched products for shop	16c5b6bf-20ed-47aa-8bcd-9d0bc33530e7
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	850a4364-bd4d-4d0d-a930-2e40ccbf3a27
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	15a6b0c2-d35a-4c28-bacd-82a510e5c752
Muzaffar	22	12	2025	0000	Fetched products for shop	3d35e99a-4b23-4e22-b10b-91a02c26e944
Muzaffar	22	12	2025	0000	Fetched products for shop	878ee476-9e5b-4b13-8c39-6af540155f9d
Muzaffar	22	12	2025	0000	Fetched products for shop	761d8912-1ef4-40be-92f0-5f6cc629b63a
Muzaffar	22	12	2025	0000	Fetched products for shop	e278c2ec-8754-4632-aa8a-55fc57993408
Muzaffar	22	12	2025	0000	Fetched products for shop	91c4191b-6869-465e-9da5-6fe2fe35ec03
Muzaffar	22	12	2025	0000	Fetched products for shop	7bbc8def-c926-47c2-8874-3a3954d9dc74
Muzaffar	22	12	2025	0000	Fetched products for shop	b2ab2872-ea64-494a-917f-e55082a10dc0
Muzaffar	22	12	2025	0000	Fetched products for shop	91654ed4-ef5a-4424-b130-433a6a94a9b2
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	230fa833-d8bf-432f-b2e2-e705d9b2e923
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	14b6f2a9-14cf-46ab-abaf-3516f610ddfc
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	6b7fc916-c83a-4dd5-a0c6-adef7870e5b0
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	64e9393b-61be-4596-9df2-a93c2b35cbde
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	a2bfaa43-8d37-4a88-855f-a319e5086f30
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	003c809f-10fe-4841-a86a-bdd37e294f96
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	c883c57a-9c99-45a3-9f82-3e0d4915faae
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	a33c05e5-2a4e-4145-9197-666243ee6777
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	36da005e-125f-45c9-935d-4f93f4f559d1
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	702133f2-2d08-49bc-a9d3-7ee8c2e4d8c3
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	09374150-cb92-4d15-bf05-de603b3553e5
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	6a71e1ab-f671-4f60-801f-d500c93f7fcf
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	bb65df4b-4b17-4ea1-bb17-9ecb2f0480bf
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	2a4ea723-f60e-4242-9c83-9eb2b407b252
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	eca731b5-f616-4ef5-999d-474781b5db60
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	e6d107ce-5f76-4375-a79c-bfbfc03c47a8
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	d5946e9b-e6cb-4e7a-bc18-dfeb6e3e05bd
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	f041460e-6d47-4347-869c-af03db55677f
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	59f179f0-4ced-4368-a452-def37f99c3c6
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	47535f32-c22f-4bef-981a-16dcd1b8be7f
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	5a95eabe-5547-4f0b-81cc-790b0083877f
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	9e53f20e-f76a-49d7-8d4c-369cb81387ab
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	8c99d15d-1dcf-448e-8b4e-0bbc70e07ff5
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	c0cca225-4491-4830-ac88-2cd4870d1020
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	e0680b05-ccee-40ef-8325-fb5f22c5fc84
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	eaf4a780-79c3-4717-a8d1-e28b922e2702
Muzaffar	22	12	2025	Muzaffar	Failed to get shop products: missing shop_id header	e51192ca-2656-4608-abc6-47f64e444b6d
Muzaffar	22	12	2025	0000	Fetched products for shop	2df153b3-5acf-47d9-ba0d-d595a6cbb335
Muzaffar	22	12	2025	0000	Fetched products for shop	8d043e10-e78d-44d8-a031-573959c795da
Muzaffar	22	12	2025	0000	Fetched products for shop	1b1b9043-070f-42c6-8453-e9400a7bb642
Muzaffar	22	12	2025	0000	Fetched products for shop	bd6862e5-ed3d-4812-a43f-0f833c31e9df
Muzaffar	22	12	2025	0000	Fetched products for shop	ec923e66-4832-473e-bdb7-05202edc4102
Muzaffar	22	12	2025	0000	Fetched products for shop	2f5a948a-6017-4cd3-92bd-2eddc4e4749a
Muzaffar	22	12	2025	0000	Fetched products for shop	9ed9e264-1211-498d-9803-9ab80cc4bee3
Muzaffar	22	12	2025	0000	Fetched products for shop	519b89a7-a6ca-4ab4-87d3-cd3ede3cd218
Muzaffar	22	12	2025	0000	Fetched products for shop	41051698-e666-4276-929d-c2396bdd264c
Muzaffar	22	12	2025	0000	Fetched products for shop	5e86626a-6231-42e6-a6a5-06b56be1fae3
Muzaffar	22	12	2025	0000	Fetched products for shop	3e88c8a1-5088-4ee6-9420-0bbc83d75228
Muzaffar	22	12	2025	0000	Fetched products for shop	dbc33f30-f72c-4f03-9446-d157a7f8598b
Muzaffar	22	12	2025	0000	Fetched products for shop	c404b6ff-fae0-468b-ab47-0c015ef55860
Muzaffar	22	12	2025	0000	Fetched products for shop	c758717a-218b-4f8b-9140-9e1c9fd42a64
Muzaffar	22	12	2025	0000	Fetched products for shop	1037651d-b79a-45a0-add3-72752336da36
Muzaffar	22	12	2025	0000	Fetched products for shop	1dc5131f-5f7e-4fe4-afbc-e13a561eaad9
Muzaffar	22	12	2025	0000	Fetched products for shop	a5cb9d82-6340-41d7-84a2-3c3f5bd98f40
Muzaffar	22	12	2025	0000	Fetched products for shop	963312d1-a521-40ad-a0d9-eb5098042197
Muzaffar	22	12	2025	0000	Fetched products for shop	223b7c67-ed33-4461-aeea-16a9f5803bf9
Muzaffar	22	12	2025	0000	Fetched products for shop	7c062530-8f2b-4ebc-b239-4d40bab1b877
Muzaffar	22	12	2025	0000	Fetched products for shop	791bb47e-fe14-4b3d-971f-0db4e5e9f649
Muzaffar	22	12	2025	0000	Fetched products for shop	21ac0168-7711-4f3b-879c-824527efdb59
Muzaffar	22	12	2025	0000	Fetched products for shop	32688371-2722-4f14-be57-8d9bb5036604
Muzaffar	22	12	2025	0000	Fetched products for shop	de8908d7-b5d6-49d3-a353-f1d4429e8100
Muzaffar	22	12	2025	0000	Fetched products for shop	ca9a072e-de07-41e7-8fa3-e40a5bea6f87
Muzaffar	22	12	2025	0000	Fetched products for shop	ee026804-38d2-43fa-a4de-bb008976269a
Muzaffar	22	12	2025	0000	Fetched products for shop	cf13590b-62af-43fc-9e8d-6d4ace87f915
Muzaffar	22	12	2025	0000	Fetched products for shop	ff78c275-5d9a-41b2-8c39-041f7465d0ed
Muzaffar	22	12	2025	0000	Fetched products for shop	d2f6285e-2931-440d-b18f-5f01c18360c2
Muzaffar	22	12	2025	0000	Fetched products for shop	18d7b179-b3b3-412d-9894-099cd130fc82
Muzaffar	22	12	2025	0000	Fetched products for shop	d1660665-315e-4484-9659-676a308a77a1
Muzaffar	22	12	2025	0000	Fetched products for shop	6f5afeb2-320c-4433-be87-e4e140dd93da
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	39eb31ee-f7bb-465f-ba12-4fd44defa3dd
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	ffaed34e-3fdd-47de-9298-f5f746becaaa
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	97cca0e3-4bc6-4973-b211-ad88f36513f5
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	83381469-18c9-4047-94d7-b6cafbdf9f4c
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	fafd6425-671d-4656-9b7a-db5ed6b4f365
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	09e9a915-8e1a-4635-af44-30f01e2e4db3
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	8fa0389e-fe3f-4dc1-89f6-4277bb22c0a4
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	d482c243-6b2d-4d0b-98fb-91ebe77e6a2c
Muzaffar	22	12	2025	0000	Fetched products for shop	4b9795e6-0677-4dc6-9369-a060b68d7860
Muzaffar	22	12	2025	0000	Fetched products for shop	ba09c477-1240-4edc-b5f1-72cae8f4a9cf
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	cb91b405-7e9a-4cec-8d01-b8958bd1ebc4
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	c8537ce0-b4e3-4fda-bbfd-337d431ba7d2
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	2f1523bf-149f-498d-8bf3-73a359922cb1
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	e2b93763-7c83-4321-9508-de920a3b976e
Muzaffar	22	12	2025	0000	Fetched products for shop	c40969d9-85cb-46e6-8189-aa3004d8ccc5
Muzaffar	22	12	2025	0000	Fetched products for shop	fdc51620-ce83-4ed8-a5a9-f82e0332e054
Muzaffar	22	12	2025	0000	Fetched products for shop	652d097e-a00e-4467-8964-3bf0a8f19b3a
Muzaffar	22	12	2025	0000	Fetched products for shop	157c87a1-f420-4d87-9fbb-e46d11ba160d
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	68c5e929-9936-44a3-b00f-1a157fe86f03
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	fd4bbd12-4be3-4399-9849-33a115aedae6
Muzaffar	22	12	2025	0000	Fetched products for shop	27a387f3-3c49-4c6d-969d-8ada1a16d963
Muzaffar	22	12	2025	0000	Fetched products for shop	881633f1-b344-453e-896b-e404a86e4f07
Muzaffar	22	12	2025	0000	All admins fetched	c55f083b-0911-4572-9710-2d0463709675
Muzaffar	22	12	2025	0000	All admins fetched	1b0d1647-783e-44a5-8397-4130dfcc97fa
Muzaffar	22	12	2025	0000	Fetched products for shop	6e298551-5854-4b79-b607-3cd27d6aa881
Muzaffar	22	12	2025	0000	Fetched products for shop	7a03f46f-8447-49de-8a02-4163d665908f
Muzaffar	22	12	2025	0000	All admins fetched	1fe395fb-f919-45f4-9498-9dbecf7c4a1b
Muzaffar	22	12	2025	0000	All admins fetched	36b11e3e-9807-451d-b66f-776cd5e9e9e3
Muzaffar	22	12	2025	0000	Fetched products for shop	97010acf-5b1f-43e5-b344-8a32f9bc5d1f
Muzaffar	22	12	2025	0000	Fetched products for shop	f44e89f3-8f6a-4e5e-ae88-f0dba8b10601
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	0dffb407-aeba-4f0d-b9cf-2e1f1fe65c20
Muzaffar	22	12	2025	0000	All admins fetched	35ef2d80-bd3b-4aaf-a961-2a42a32b1f69
Muzaffar	22	12	2025	0000	All admins fetched	8435b82a-db8b-4fa9-a22d-3c4766c7953c
Muzaffar	22	12	2025	0000	Fetched products for shop	6f6e4b35-3483-40af-a148-e60e042bd10a
Muzaffar	22	12	2025	0000	Fetched products for shop	f9a21fc5-64e6-491d-b444-ac7bc0208cc8
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	dca90745-1533-4931-a6c8-4469e9144fbf
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	3ff45304-e0bc-4383-8bb5-ff89daabf1a9
Muzaffar	22	12	2025	0000	Fetched products for shop	90260a5d-9abc-499d-a041-5a55945ba40f
Muzaffar	22	12	2025	0000	Fetched products for shop	f4fcce94-6fc0-44bd-abd3-5437744bbcb4
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	8a57c6c4-d38a-4bfb-9a2c-43ad6609c747
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	d2426917-dda8-4c4c-a007-20cd9e836010
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	81b87abf-eb2d-4ac4-8846-730a4e05f5b9
Muzaffar	22	12	2025	0000	Fetched products for shop	5c485fd8-55c9-4bcc-b3e3-9191a7520e49
Muzaffar	22	12	2025	0000	Fetched products for shop	fe0e3cef-d316-4a73-8dd4-3722c764ba59
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	f23de612-c5dc-4e45-8182-eb7a09d1821f
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	981d9bfd-f8b1-4925-b758-02e920efb634
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	e3ed70f4-a7da-41f5-8628-78eede619774
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	6bca6376-4965-498d-8553-78179d88f9bc
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	8bdaef79-83c5-4bfe-bca0-e780a5db8111
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	d005d896-7bf2-41e4-9aa9-609126ab03b4
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	d6918e57-1357-4015-84f0-d8afbea6ee91
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	6b1f0250-e27f-47e4-a0c6-ae41ebe98ac1
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	566233fb-760f-4204-88be-c3675f950be9
Muzaffar	22	12	2025	0000	Fetched products for shop	113a3c26-bce0-4fb9-bc98-597dfbcdba35
Muzaffar	22	12	2025	0000	Fetched products for shop	667a5c02-cada-4252-bdaf-23546e01b402
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	e34566e0-4d8f-4914-a8c0-8a09aa911511
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	c3dba099-9df4-4dd5-9b75-2981a86f305b
Muzaffar	22	12	2025	0000	Fetched products for shop	b68a1395-5f9f-4ce8-ab9d-d39e6303a765
Muzaffar	22	12	2025	0000	Fetched products for shop	958feffb-2e17-4f98-b73e-7430804feca6
Muzaffar	22	12	2025	0000	All admins fetched	801afc65-e313-4c96-964b-101ce78b0330
Muzaffar	22	12	2025	0000	All admins fetched	7ff7bc6e-7603-455f-9b93-f9fb60524cb4
Muzaffar	22	12	2025	0000	All admins fetched	1b915a93-037d-41d7-a75b-21c5b0cf25a3
Muzaffar	22	12	2025	0000	All admins fetched	a9501fa3-f4bb-470b-b252-c7631c29fdc3
Muzaffar	22	12	2025	0000	All admins fetched	d7dabcab-a96e-4737-8fe7-ac7fa79d8a7c
Muzaffar	22	12	2025	0000	All admins fetched	b59d473f-13af-4d99-b35a-1583314b3c97
Muzaffar	22	12	2025	0000	All admins fetched	62be21ad-ac95-4dee-9ed3-06aae58561cc
Muzaffar	22	12	2025	0000	All admins fetched	cd6c6445-0719-4e0d-9380-e8b28488e1ef
Muzaffar	22	12	2025	0000	All admins fetched	07ee70ea-63d5-44b5-90e2-354660289932
Muzaffar	22	12	2025	0000	All admins fetched	589bafbc-a3c2-4553-960b-935e34f8cd8e
Muzaffar	22	12	2025	0000	All admins fetched	b9562c76-28dc-421a-a8f3-9c6e446ce29b
Muzaffar	22	12	2025	0000	All admins fetched	7b65dc29-d632-4d80-bb96-c8847de1e115
Muzaffar	22	12	2025	0000	All admins fetched	9bbae7fd-0dc9-4c8e-984a-cb57e980f694
Muzaffar	22	12	2025	0000	All admins fetched	96eadfd9-35d2-4985-a42f-e9f868ee744e
Muzaffar	22	12	2025	0000	All admins fetched	86f94948-31fa-4ac8-baa0-db3699f7a880
Muzaffar	22	12	2025	0000	All admins fetched	a103c87e-11e6-4eb9-a010-f14c73a901a4
Muzaffar	22	12	2025	0000	All admins fetched	a0253faf-483c-4d53-87bd-53dc2ff378e7
Muzaffar	22	12	2025	0000	All admins fetched	a64baaf9-c45a-42cf-87e4-ac3c95852b98
Muzaffar	22	12	2025	0000	All admins fetched	ad10d972-8978-4f56-a8c5-264f62272295
Muzaffar	22	12	2025	0000	All admins fetched	5710f29f-ecf1-4653-98cc-24719449381e
Muzaffar	22	12	2025	0000	All admins fetched	78356221-cc82-4ff2-b6c5-554e79befca7
Muzaffar	22	12	2025	0000	All admins fetched	70d5f7a9-5189-471f-afda-e7f33db24362
Muzaffar	22	12	2025	0000	All admins fetched	e166298a-0f2a-4866-8650-8b3ae4bbb6ac
Muzaffar	22	12	2025	0000	All admins fetched	986a3057-750b-434f-82bd-12a03c5e40a4
Muzaffar	22	12	2025	0000	All admins fetched	31c78386-70ee-48dc-84c9-6a125f3fe314
Muzaffar	22	12	2025	0000	All admins fetched	d6b44a9c-19d0-46e8-820d-a8e7728d07fa
Muzaffar	22	12	2025	0000	All admins fetched	02ef10d8-b4b2-4fcd-8978-07d24aaad14c
Muzaffar	22	12	2025	0000	All admins fetched	eaea2f73-d5ec-4cd8-a4be-0801fcf8b8e3
Muzaffar	22	12	2025	0000	All admins fetched	73a25556-4849-4176-b110-1ff47bf804ce
Muzaffar	22	12	2025	0000	All admins fetched	4617c007-ef47-4c47-bf90-78dbe905f970
Muzaffar	22	12	2025	0000	All admins fetched	be3c91c0-aa5e-47ec-8083-8524b0972aec
Muzaffar	22	12	2025	0000	All admins fetched	87118bb4-95ca-473f-9556-c006681519f2
Muzaffar	22	12	2025	0000	Fetched products for shop	8b8c6a3e-2e29-4df7-b847-a4636800cf7c
Muzaffar	22	12	2025	0000	Fetched products for shop	428f1f5e-8f19-445c-b1ca-3d79bc949b7c
\N	22	12	2025	0000	Sale created successfully with sales_id: e1fab004-8dec-472d-9541-6a59e3804ef9	4e81c46a-f89c-42e0-84b1-71cd826f1a32
Muzaffar	22	12	2025	0000	Fetched products for shop	6d1319a0-42e8-4734-958f-54af8d158df2
Muzaffar	22	12	2025	0000	Fetched products for shop	16fab9dc-79ad-446d-8a18-18dc0a683b37
Muzaffar	22	12	2025	0000	Fetched products for shop	5e9eb8af-eb28-41e9-8859-abc14a32c94c
Muzaffar	22	12	2025	0000	Fetched products for shop	50e82aa3-1e0a-4d8e-bd45-417cb2bf77bf
Muzaffar	22	12	2025	0000	Fetched products for shop	e1ed45b8-624c-45e0-92d9-abc948b2b20b
Muzaffar	22	12	2025	0000	Fetched products for shop	f282283f-f990-4e37-9110-a5256a33443b
Muzaffar	22	12	2025	0000	Fetched products for shop	87cefc3c-e479-45e4-ab87-136b88c275b1
Muzaffar	22	12	2025	0000	Fetched products for shop	42cebf71-bb77-45cd-b872-1d1506c1ef70
Muzaffar	22	12	2025	0000	Fetched products for shop	4b343419-968c-4767-8722-d8d759d55b2f
Muzaffar	22	12	2025	0000	Fetched products for shop	166b382c-3edf-4ce6-a33d-f5f0515e1a64
Muzaffar	22	12	2025	0000	Fetched products for shop	bc825936-103b-4599-a5dc-c97e2801d587
Muzaffar	22	12	2025	0000	Fetched products for shop	fd91d5d2-d361-4eb9-b4a5-408419547c7d
Muzaffar	22	12	2025	0000	Fetched products for shop	74d8a4f9-2425-440d-aa94-e99f4b1156e2
Muzaffar	22	12	2025	0000	Fetched products for shop	7cf39e92-ba45-4347-9efc-83cd86bf0323
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	4e360c82-a0a4-44fb-a01e-f152bb497bee
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	bdf58b0d-c9a5-45b4-b95f-29dd2bd9b06f
Muzaffar	22	12	2025	0000	Fetched products for shop	55efff7c-a358-4435-a529-02a2f732d670
Muzaffar	22	12	2025	0000	Fetched products for shop	ab56ee08-8f1f-4160-90a8-68ca77bc0060
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	e7374ab4-dbcf-46ed-aa1d-afd4bc38945a
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	86825161-572b-4d24-af9b-423e114d3b66
Muzaffar	22	12	2025	0000	All admins fetched	46209a2d-670f-4757-8a3d-cbd75f768749
Muzaffar	22	12	2025	0000	All admins fetched	1351f708-0924-4014-8aa2-b55b14a6385f
Muzaffar	22	12	2025	0000	All admins fetched	f7cc7a72-8f46-4c64-8481-a1de3a4afc49
Muzaffar	22	12	2025	0000	All admins fetched	f08a6548-8caf-42f8-b7a4-69f79d97ee39
Muzaffar	22	12	2025	0000	Fetched products for shop	28fee916-36dd-4fa8-ad1b-70f9e01f9130
Muzaffar	22	12	2025	0000	Fetched products for shop	8db0621b-5b71-4673-a2ef-259dc8b7d56f
Muzaffar	22	12	2025	0000	All admins fetched	1ac469d1-db0d-4f49-9d01-9ce8c1b304d4
Muzaffar	22	12	2025	0000	All admins fetched	d0228018-51e1-4008-b6e5-52e7cfe2e919
Muzaffar	22	12	2025	0000	Fetched products for shop	7a28d37a-a08e-48b3-869c-9d0eb97c159a
Muzaffar	22	12	2025	0000	Fetched products for shop	cb35d4de-ffc1-476c-bf26-d617ed090c7a
Muzaffar	22	12	2025	0000	All admins fetched	ca4c0892-b6ef-45fa-8f40-b07360753864
Muzaffar	22	12	2025	0000	All admins fetched	316ed4a5-a5fe-4272-9b83-b72fc5539a6f
Muzaffar	22	12	2025	0000	Fetched products for shop	76ee80b7-c2a5-499a-825e-f8d03304f429
Muzaffar	22	12	2025	0000	Fetched products for shop	c2f28fa6-e9c4-4b6f-ba87-6c17d4949ad9
Muzaffar	22	12	2025	0000	Fetched products for shop	6b837ba9-5dd9-40c3-aab8-85b77e89c51f
Muzaffar	22	12	2025	0000	Fetched products for shop	aefa3c5b-938a-4f4d-bbe4-cf4193929aee
Muzaffar	22	12	2025	0000	Fetched products for shop	ceb6f861-5060-4e9a-bf11-b83e10a41531
Muzaffar	22	12	2025	0000	Fetched products for shop	db994fa3-2d35-4c8d-9696-480b57ae19b9
Muzaffar	22	12	2025	0000	All admins fetched	65cf5285-126d-4ad6-ad2f-2092c8af6e2a
Muzaffar	22	12	2025	0000	All admins fetched	5cf0964a-d324-4111-b779-6c77b018b5f0
Muzaffar	22	12	2025	0000	Fetched products for shop	f328a922-7ea6-4363-8b52-ce9bf44796d9
Muzaffar	22	12	2025	0000	Fetched products for shop	0180f0bf-244f-4e74-ad82-15cf07c8bb9c
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	eedab30a-5099-454f-82b3-958d445110c6
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	d2934bba-0475-4aca-9a2d-d753a4eebfac
Muzaffar	22	12	2025	0000	Fetched products for shop	0792bd74-7ced-41f6-ac39-67b2dc0be7a6
Muzaffar	22	12	2025	0000	Fetched products for shop	bd66c031-0677-4889-9c6a-9e739d6fe9bb
\N	22	12	2025	0000	Sale created successfully with sales_id: fcacfc45-4ffe-49e5-91f9-74d515a30501	670a29a5-a17c-4322-815f-b3cc718f3fb7
Muzaffar	22	12	2025	0000	Fetched products for shop	57983b1a-b399-4f3f-aea5-1c4ab9617b31
Muzaffar	22	12	2025	0000	Fetched products for shop	12e51ed1-78de-4c78-a956-a339f2d7b0e5
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	c7d587b9-d976-4333-acd9-696f372f21b2
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	6b516df8-978e-4ec8-9dd4-cec0253c2e4d
Muzaffar	22	12	2025	0000	Fetched products for shop	1a458f16-be69-45e2-a7ad-3f420617db93
Muzaffar	22	12	2025	0000	Fetched products for shop	6c000a3e-4af8-4c9b-8ec6-c3d548f7cfc1
Muzaffar	22	12	2025	0000	All admins fetched	7312643f-494b-4081-a6bb-7cb6d2788a8f
Muzaffar	22	12	2025	0000	All admins fetched	fd22c11f-7a26-48d6-bf56-7a679183d925
Muzaffar	22	12	2025	0000	Admin updated	375a57cc-9c18-4d5e-b959-58128a220765
Muzaffar	22	12	2025	0000	Admin updated	db0e72c7-0a63-43c4-a920-4ec4981f3632
Muzaffar	22	12	2025	0000	Admin updated	053c5038-9e55-42d1-a6f3-ba4e3668e9a5
Muzaffar	22	12	2025	0000	Admin updated	fd327a98-e7c9-4323-8ca0-474df6c17f5e
Muzaffar	22	12	2025	0000	Admin updated	efab0b64-a2e2-42df-952e-fe4d75bb211d
Muzaffar	22	12	2025	0000	Admin updated	7d9fd91e-92f4-48cd-b65a-81210a87938b
Muzaffar	22	12	2025	0000	Admin updated	76ac0084-3af6-40c9-b617-def4a646c75b
Muzaffar	22	12	2025	0000	Admin updated	82c6b9ee-834f-4126-b0b6-4574b42461b9
550e8400-e29b-41d4-a716-446655440000	22	12	2025	\N	Superuser logged out	0cafca0b-8290-41cf-805a-20eda4f9d474
Abduqodirov	22	12	2025	\N	Admin Logged In	5e12e485-e136-4540-8bd5-707c998cf660
Abduqodirov	22	12	2025	0000	Fetched products for shop	935aecd6-8a81-4dcc-b7fa-42bff004104e
Abduqodirov	22	12	2025	0000	Fetched products for shop	5b19a7e5-ce07-4e05-b5bf-2a80435505fa
Abduqodirov	22	12	2025	0000	Fetched products for shop	388cef5a-c239-4c97-afd5-f6c7de879220
Abduqodirov	22	12	2025	0000	Fetched products for shop	1c173ac3-8f72-4b6c-baed-0142c0fb08d3
Abduqodirov	22	12	2025	0000	Fetched products for shop	40d287b7-8239-411c-b176-ec5c44a19a5e
Abduqodirov	22	12	2025	0000	Fetched products for shop	575219e5-4c44-4de2-9f77-9ab92b690c18
550e8400-e29b-41d4-a716-446655440000	22	12	2025	\N	Admin logged out	e6e34018-96a0-47f5-a55d-8173da0e6588
Muzaffar	22	12	2025	0000	Super logged in	3440c78c-3a94-4251-b7d5-ba6293fb290e
Muzaffar	22	12	2025	0000	All admins fetched	7efcf7c6-1bfc-4447-8ed0-bdb06c5e9c1b
Muzaffar	22	12	2025	0000	All admins fetched	191b8539-11e9-41c8-8734-25d248ec1507
Muzaffar	22	12	2025	0000	Admin updated	7cfe446c-c1e2-4e9e-8fa5-92f416dd1727
Muzaffar	22	12	2025	0000	Admin updated	d7afc903-ae68-4c72-9639-7aad3e6940b3
Muzaffar	22	12	2025	0000	Admin updated	3a7e6d60-bc6d-440a-8da9-068a5296dcc8
Muzaffar	22	12	2025	0000	Admin updated	04a8a471-9832-4800-b1d7-a6d033ed184d
Muzaffar	22	12	2025	0000	Admin updated	271b4e0c-d5b8-435d-ab1e-77b38924c688
550e8400-e29b-41d4-a716-446655440000	22	12	2025	\N	Superuser logged out	b77a958d-1646-4de3-a548-d25de6140a70
Abduqodirov	22	12	2025	\N	Admin Logged In	267e5cfe-218c-47f1-a06f-1b3afd72d6d7
Abduqodirov	22	12	2025	0000	Fetched products for shop	a2f3ec7b-5ca9-4cd3-98f0-d8bc4c843bdd
Abduqodirov	22	12	2025	0000	Fetched products for shop	e96486fc-70f1-42ff-a235-cfab7626ee1a
Abduqodirov	22	12	2025	0000	Fetched products for shop	002694a1-7564-46f2-947c-97040b4a641d
Abduqodirov	22	12	2025	0000	Fetched products for shop	69c6f59d-eb7d-4a37-8c9f-456bbb310c8b
Abduqodirov	22	12	2025	0000	Fetched products for shop	629dfdad-f470-4e87-8a91-1af3ed373b65
Abduqodirov	22	12	2025	0000	Fetched products for shop	e68bc1f9-d60e-4164-9214-8e495766cd40
Abduqodirov	22	12	2025	0000	Fetched products for shop	f6360c3a-d2e6-488e-b87b-9aad73d6399f
Abduqodirov	22	12	2025	0000	Fetched products for shop	c564f95a-e07b-4152-bfde-7f3d20853de7
Abduqodirov	22	12	2025	0000	Fetched products for shop	bc3c99e0-74e5-4eb3-83f7-61c396d0a8b1
Abduqodirov	22	12	2025	0000	Fetched products for shop	fa0c3388-6391-4e7a-8cfb-4a392780ed5e
550e8400-e29b-41d4-a716-446655440000	22	12	2025	\N	Admin logged out	7af08ec8-da9d-4a59-8153-229d433629fc
Muzaffar	22	12	2025	0000	Super logged in	4bfa94c9-7a2f-443f-841a-83d7bc695657
Muzaffar	22	12	2025	0000	Fetched products for shop	5e9ceef4-7ee4-4277-aafb-add604761334
Muzaffar	22	12	2025	0000	Fetched products for shop	8e2a165b-9059-4891-a4f7-90c3d64aac67
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	542ab444-ccc2-4a98-92fa-1a70cdaa36a7
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	cb36b977-44fc-4cdc-bd95-0da4fb8c3738
Muzaffar	22	12	2025	0000	Fetched products for shop	3013b8ef-a640-4fde-a726-9069ee1ad1f8
Muzaffar	22	12	2025	0000	Fetched products for shop	3369dc42-3948-486b-970e-1093e3db5974
Muzaffar	22	12	2025	0000	All admins fetched	90d2e78c-620d-4ed5-b6cd-85a1f994dfec
Muzaffar	22	12	2025	0000	All admins fetched	1d3e055b-b7a9-4ee3-b737-fa4eb94f8a10
Muzaffar	22	12	2025	0000	Fetched products for shop	d6a2ef29-6528-4375-8edb-73181d998046
Muzaffar	22	12	2025	0000	Fetched products for shop	d13af96d-ee80-4d00-b33d-8c5e4553140b
Muzaffar	22	12	2025	0000	Fetched products for shop	d1d4c120-187e-4684-a5a3-4a0dd10af246
Muzaffar	22	12	2025	0000	Fetched products for shop	d114c554-9ac5-4d7c-b068-11b61e21c080
Muzaffar	22	12	2025	0000	All admins fetched	722cbae1-49cb-4866-858b-1384b0f15739
Muzaffar	22	12	2025	0000	All admins fetched	b355733e-0c88-40d9-8375-c490c7bbbe2d
Muzaffar	22	12	2025	0000	All admins fetched	89add171-3ec9-4fe0-bae3-fe632fa392c3
Muzaffar	22	12	2025	0000	All admins fetched	0ab33f8f-b89a-4f58-89a6-a4975713e95a
Muzaffar	22	12	2025	0000	Fetched products for shop	4af97d97-5e95-4745-bec1-d8330248ae12
Muzaffar	22	12	2025	0000	Fetched products for shop	63d2af26-6b7e-4619-9e04-66f545281fb2
Muzaffar	22	12	2025	0000	All admins fetched	fa36a876-b9d6-448d-93b0-29ac54a13383
Muzaffar	22	12	2025	0000	All admins fetched	bc5d08e6-ff1c-4cee-ada9-01ef86825a69
Muzaffar	22	12	2025	0000	All admins fetched	b8a667a4-c19c-46c0-9704-51dbced5c099
Muzaffar	22	12	2025	0000	All admins fetched	10e9dea8-b9e8-488e-9498-b653f3435aa7
Muzaffar	22	12	2025	0000	All admins fetched	c11bac53-6094-43de-8793-3d8d106eb09d
Muzaffar	22	12	2025	0000	All admins fetched	737da73b-eaf0-4d85-b6b0-f5c4c28bcd88
Muzaffar	22	12	2025	0000	All admins fetched	2efbc4ac-5551-4d4a-a048-8bcb58f87021
Muzaffar	22	12	2025	0000	All admins fetched	e7977bb6-1530-4d21-8339-5f656d85563b
Muzaffar	22	12	2025	0000	All admins fetched	b34a60c8-f773-4525-a293-c4604cdf9cf9
Muzaffar	22	12	2025	0000	All admins fetched	d618bc90-9c34-4c34-a433-da18f21104fa
Muzaffar	22	12	2025	0000	Fetched products for shop	66ff94df-e5b5-43e5-b9d0-fc402f052d9d
Muzaffar	22	12	2025	0000	Fetched products for shop	2c09efde-3b54-42c2-897e-3981728a799d
Muzaffar	22	12	2025	0000	All admins fetched	7880d497-f56c-4fbf-aec1-d7a6307c9b21
Muzaffar	22	12	2025	0000	All admins fetched	be8e6437-ad90-4d41-94f4-ca2e7fdd441a
Muzaffar	22	12	2025	0000	Fetched products for shop	1f2ae592-8772-4468-a42c-35accf2d3420
Muzaffar	22	12	2025	0000	Fetched products for shop	80f8f79e-f7ae-4da5-b282-d573b27c634c
Muzaffar	22	12	2025	0000	All admins fetched	76873a99-e5e9-4f36-9c39-1de2d93b644e
Muzaffar	22	12	2025	0000	All admins fetched	0868cb06-8bf9-4b00-902b-51b4b9954ebf
Muzaffar	22	12	2025	0000	All admins fetched	e3fdb990-9fcd-4adf-a8e1-0d323445db0f
Muzaffar	22	12	2025	0000	All admins fetched	6564a6f5-f5ff-4f44-ba20-8924ca46e404
Muzaffar	22	12	2025	0000	Fetched products for shop	5edb64cc-7fb0-44a4-9e57-92ade7607b8c
Muzaffar	22	12	2025	0000	Fetched products for shop	90c30b84-e760-4b54-811b-8f9b6b389a9f
Muzaffar	22	12	2025	0000	All admins fetched	452543a1-8df5-45c3-bfc8-804b90893ab2
Muzaffar	22	12	2025	0000	All admins fetched	90e6a645-e36b-41b1-a400-5192dbeb6daf
Muzaffar	22	12	2025	0000	Admin updated	da773886-30f0-46b2-a0af-e928a33f5c7b
Muzaffar	22	12	2025	0000	All admins fetched	89670231-3bed-4603-a228-413ae029e064
Muzaffar	22	12	2025	0000	All admins fetched	f3c5080e-2175-454e-aede-f964ed96ef39
Muzaffar	22	12	2025	0000	Fetched products for shop	84db70ca-ea8d-4a67-83c3-7774a356ec21
Muzaffar	22	12	2025	0000	Fetched products for shop	61156f23-e5b7-4def-bebc-a04fc00e4fa2
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	1c9e5baf-9b53-4b89-b156-1f44a09f9114
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	5f80558f-6734-4d0f-a1ee-e2b6e6e812df
Muzaffar	22	12	2025	0000	Fetched products for shop	487ea680-1a72-4c0d-81f8-21141dd2c490
Muzaffar	22	12	2025	0000	Fetched products for shop	4958f17e-cf92-48b2-a206-b2711e3121ce
Muzaffar	22	12	2025	0000	All admins fetched	d62242c6-0132-4dec-9a79-927431f47ffd
Muzaffar	22	12	2025	0000	All admins fetched	1961084c-9f40-4550-a12a-15cbe8c62b25
Muzaffar	22	12	2025	0000	Fetched products for shop	07531f1e-95f2-4fe0-8c29-6d6cd9c2839b
Muzaffar	22	12	2025	0000	Fetched products for shop	038dc60b-d63d-4c14-bc62-41853ddb1aef
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	208f2472-bcb8-4df2-b512-0578239af800
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	f4a075fd-0728-49b4-b0f9-f2cfd270348a
Muzaffar	22	12	2025	0000	Fetched products for shop	2773f7b3-15ae-4ede-aabe-af8b12f8a6b7
Muzaffar	22	12	2025	0000	Fetched products for shop	0576d5c5-7751-4a62-8cee-4c7705b71610
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	5e0c1327-1a16-48aa-8711-ce0faf826c18
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	77868f42-4ab3-45fe-b696-5748f47044cd
Muzaffar	22	12	2025	0000	Fetched products for shop	cbdc4df9-271a-445e-a738-080fb91d1f34
Muzaffar	22	12	2025	0000	Fetched products for shop	c0b77638-6632-4d39-ba96-5a2dae4a6d00
Muzaffar	22	12	2025	0000	Fetched products for shop	5d3df53c-4222-4c87-acc6-c664f3447d83
Muzaffar	22	12	2025	0000	Fetched products for shop	75de20dd-b3bc-4d22-b500-bc43bf8e1c93
Muzaffar	22	12	2025	0000	All admins fetched	50d1da81-a436-49c9-bb66-d3e51a361ad5
Muzaffar	22	12	2025	0000	All admins fetched	bd7b5b26-d953-4f97-8b02-1ff2438384cd
Muzaffar	22	12	2025	0000	Fetched products for shop	28be2ce9-48ca-4604-963c-ae45df2e8473
Muzaffar	22	12	2025	0000	Fetched products for shop	f9794c60-b9ed-4cfe-9023-3c58fdd0350c
Muzaffar	22	12	2025	0000	All admins fetched	49abcf70-1495-4104-9086-4250e4fd13e8
Muzaffar	22	12	2025	0000	All admins fetched	329509b7-0877-4eef-9ae8-4fbd4b4140f1
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	cb16b970-6969-496e-82ee-a1d0ec190f7d
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	ad9d1baa-71fc-46d1-bcb7-e79b33e1d0d4
Muzaffar	22	12	2025	0000	Fetched products for shop	32cb26e6-6681-45fb-8990-a7558941fa9f
Muzaffar	22	12	2025	0000	Fetched products for shop	ca36500a-410f-4db9-9d92-8f1de42ad09b
Muzaffar	22	12	2025	0000	All admins fetched	acfb9793-562c-4321-b923-ceddc6ea43a0
Muzaffar	22	12	2025	0000	All admins fetched	0b780976-491a-49ea-bbd2-d04af5780334
Muzaffar	22	12	2025	0000	All admins fetched	a034be71-1b52-40e7-88e5-052beafb2b19
Muzaffar	22	12	2025	0000	All admins fetched	544d9a09-8edc-4685-be7a-3608d394e399
550e8400-e29b-41d4-a716-446655440000	22	12	2025	\N	Superuser logged out	a9fa28c8-9e62-4fe0-b164-4622cff117d8
Muzaffar	22	12	2025	0000	Super logged in	3522b5a8-e8c7-468a-9fda-be93dec3a5fd
Muzaffar	22	12	2025	0000	Fetched products for shop	6f996dca-9ca1-4a63-9876-09d8c22fee01
Muzaffar	22	12	2025	0000	Fetched products for shop	6ce8e71e-f564-4469-bff7-2181c8f1e099
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	eca7838f-fa68-4c71-a802-14c4f817aac6
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	46764e15-34b9-43a7-ae8f-7fa1b84d0dfa
Muzaffar	22	12	2025	0000	Fetched products for shop	a16afb1f-8595-4449-af94-e92e4ac194d5
Muzaffar	22	12	2025	0000	Fetched products for shop	922a9c52-03a2-46a6-ad56-224a2953ea25
Muzaffar	22	12	2025	0000	All admins fetched	bdd6055e-0c4d-45cf-a7fd-379a80d16f74
Muzaffar	22	12	2025	0000	All admins fetched	e50aa814-bba4-4618-a2c4-001a8cb44dd0
Muzaffar	22	12	2025	0000	Fetched products for shop	3aa449cd-53b4-4e61-90cc-f3a023f55aa5
Muzaffar	22	12	2025	0000	Fetched products for shop	1970a468-6b6f-480c-abf0-9e2a95eca9d0
Muzaffar	22	12	2025	0000	Fetched products for shop	22806a0d-94eb-4e13-951d-26ab53699714
Muzaffar	22	12	2025	0000	Fetched products for shop	bfb95927-ec1f-46e3-bcc9-0f96dfb11e6c
Muzaffar	22	12	2025	0000	Fetched products for shop	0fd100bf-8dc2-48b3-9c20-4b24558e5522
Muzaffar	22	12	2025	0000	Fetched products for shop	dca7ba92-d638-490c-9b51-f252b979324e
Muzaffar	22	12	2025	0000	Fetched products for shop	5577f851-646e-4e72-bca2-a81ebd6f0625
Muzaffar	22	12	2025	0000	Fetched products for shop	a7f4ec84-f17d-4ef1-be77-faee2adcc0aa
Muzaffar	22	12	2025	0000	Fetched products for shop	3498ab0f-35d4-4b44-964d-c6686c5c7d07
Muzaffar	22	12	2025	0000	Fetched products for shop	8c6a7088-456c-4221-b740-1fbddd7a6237
Muzaffar	22	12	2025	0000	Fetched products for shop	e9be244b-a0b7-4d16-841f-8ce3862ebba1
Muzaffar	22	12	2025	0000	Fetched products for shop	5ead09b9-cb25-4527-baea-e67819933eab
Muzaffar	22	12	2025	0000	Fetched products for shop	96fa788d-23bf-4565-b3c2-96686d2578b7
Muzaffar	22	12	2025	0000	Fetched products for shop	8a680397-eb0c-4564-89cb-db894049480d
Muzaffar	22	12	2025	0000	Fetched products for shop	c54c8b54-4e1b-443b-95ca-ef8a3383cd6a
Muzaffar	22	12	2025	0000	Fetched products for shop	17b6e2a1-c75d-4389-8e76-e9fc85213203
Muzaffar	22	12	2025	0000	Fetched products for shop	94b36068-10d6-479d-b045-ca7f07e89cb3
Muzaffar	22	12	2025	0000	Fetched products for shop	be356216-4190-4e30-8fe8-184021e0a75b
Muzaffar	22	12	2025	0000	Fetched products for shop	d64814f5-711e-44d0-b518-32a8e5f66183
Muzaffar	22	12	2025	0000	Fetched products for shop	ddb63076-afc9-4739-8004-0bf8675a5202
Muzaffar	22	12	2025	0000	Fetched products for shop	86f366f3-dd13-451f-807d-c1c869b76099
Muzaffar	22	12	2025	0000	Fetched products for shop	7f3c74e3-dd26-401d-b863-9b74fc86d605
Muzaffar	22	12	2025	0000	Fetched products for shop	34db82d1-978f-453f-a0ae-15b24dcb1acf
Muzaffar	22	12	2025	0000	Fetched products for shop	26d0abd8-c1e4-4e3e-b5f5-1bbe6ba5700b
Muzaffar	22	12	2025	0000	Fetched products for shop	0058378c-b05d-4617-a962-24bed772948d
Muzaffar	22	12	2025	0000	Fetched products for shop	52c80de6-a561-4f7f-8ff1-0894a1314f86
Muzaffar	22	12	2025	0000	Fetched products for shop	91aa8949-160c-47d7-8896-134e5379bd2e
Muzaffar	22	12	2025	0000	Fetched products for shop	d62a9b59-54ce-4aa4-934d-6f490fedeb6e
Muzaffar	22	12	2025	0000	Fetched products for shop	324ab22f-33a9-4f5d-9858-c60b07dab4f4
Muzaffar	22	12	2025	0000	Fetched products for shop	d45ef327-5be9-4cd0-886d-b517da7a734c
Muzaffar	22	12	2025	0000	Fetched products for shop	b6c2b76c-ff19-4e26-a07e-38525b47ccba
Muzaffar	22	12	2025	0000	Fetched products for shop	c0a3f2c1-1e38-4474-b3f8-ad19e3788cb0
Muzaffar	22	12	2025	0000	Fetched products for shop	20877d6d-b790-4900-918e-6159de4ae9de
Muzaffar	22	12	2025	0000	Fetched products for shop	b476b143-b9d8-4261-b8f9-7f7b33045b6c
Muzaffar	22	12	2025	0000	Fetched products for shop	3b17cc16-e846-46f2-9630-73416e877982
Muzaffar	22	12	2025	0000	Fetched products for shop	40ec1b5a-461e-4f4b-8bd4-449214bf1407
Muzaffar	22	12	2025	0000	Fetched products for shop	657b672d-6549-4fcd-bb95-1a1d605f8db6
Muzaffar	22	12	2025	0000	Fetched products for shop	23b07102-6ff2-4e71-b8ed-9f12d939265d
Muzaffar	22	12	2025	0000	Fetched products for shop	52876580-8c6e-4fa7-bb52-f1ce99577460
Muzaffar	22	12	2025	0000	Fetched products for shop	6400575d-242c-4c4e-b9c0-15c27ac73bc2
Muzaffar	22	12	2025	0000	All admins fetched	9c44753d-6224-47fa-9882-284af0a503fc
Muzaffar	22	12	2025	0000	All admins fetched	681c5a11-a353-4098-b803-f3c6d5c0ce82
Muzaffar	22	12	2025	0000	Fetched products for shop	1c14ce0c-5a57-495e-8657-953b872d8ff9
Muzaffar	22	12	2025	0000	Fetched products for shop	e47a09e5-48cd-4a56-a469-3235823b283f
Muzaffar	22	12	2025	0000	Fetched products for shop	9c5adff8-8a57-475b-ab50-831fdb2ea0a2
Muzaffar	22	12	2025	0000	Fetched products for shop	c28da446-b417-4d22-82fb-fbaf10de9bbb
Muzaffar	22	12	2025	0000	Fetched products for shop	c95ee5e3-e622-41b5-bcb9-4067744dce8f
Muzaffar	22	12	2025	0000	Fetched products for shop	fb8ee11e-bc5b-4a9f-96e6-16d4a9999215
Muzaffar	22	12	2025	0000	Fetched products for shop	6f6fb8f5-e81c-48cf-9b1c-f574912bc32d
Muzaffar	22	12	2025	0000	Fetched products for shop	a6cb91e0-69bf-4370-b8cb-7bce416cdf6a
Muzaffar	22	12	2025	0000	All admins fetched	c708484b-2f5c-432a-a79f-23e664193f40
Muzaffar	22	12	2025	0000	All admins fetched	7064d754-84e7-4fbd-92bc-279e96b9824d
Muzaffar	22	12	2025	0000	All admins fetched	f7e4dafa-243c-45de-b69a-7c7a8ffadd52
Muzaffar	22	12	2025	0000	All admins fetched	936fb66a-8e37-47c8-bcef-55f3718feb6b
Muzaffar	22	12	2025	0000	All admins fetched	10b29d1b-6b9f-41a8-aa76-e9fcc041a8d9
Muzaffar	22	12	2025	0000	All admins fetched	73962a03-9098-46c4-9dcb-e0ebe6be9159
Muzaffar	22	12	2025	0000	All admins fetched	f52e3a45-cb5d-4f79-93a7-b26842842d46
Muzaffar	22	12	2025	0000	All admins fetched	50548c58-a1a6-4aa4-a5c0-aff497d91228
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	0856453b-944b-46ef-8714-1fe84048eb09
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	3ab9feb4-90a9-4fe8-b095-0411ea0d6353
Muzaffar	22	12	2025	0000	Fetched products for shop	692a2d88-1a02-4fdb-8c60-3e823d54a5ef
Muzaffar	22	12	2025	0000	Fetched products for shop	d7590580-a7db-4c00-ae1c-859fd342b2ad
Muzaffar	22	12	2025	0000	Fetched products for shop	18816b5f-a8fa-416e-93bd-5b499ae7272c
Muzaffar	22	12	2025	0000	Fetched products for shop	c122c099-b69c-4bf1-9493-7750850e1662
Muzaffar	22	12	2025	0000	All admins fetched	cae0c18c-cd72-48ad-bdfe-a282a7609197
Muzaffar	22	12	2025	0000	All admins fetched	453f0fc9-80b7-4bdf-807c-0f5f3de4b7a5
Muzaffar	22	12	2025	0000	Fetched products for shop	b6ac2dc6-0362-44ac-9401-8ebcdeef7a3e
Muzaffar	22	12	2025	0000	Fetched products for shop	485e47dc-f579-4428-8a48-2f006edce603
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	f9b90a7e-d246-4d7d-b9c9-895ca5045b83
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	1a1354ed-8872-4d6a-881d-5f7f66670bcb
Muzaffar	22	12	2025	0000	Fetched products for shop	1ed7b921-9441-45e6-88a1-08446b5318fe
Muzaffar	22	12	2025	0000	Fetched products for shop	92b24d78-8aa4-46cb-8254-0abb87d15de0
Muzaffar	22	12	2025	0000	Fetched products for shop	023d586b-7a30-4230-9ddb-8f6c7f465f90
Muzaffar	22	12	2025	0000	Fetched products for shop	190c921f-c133-4bc8-b24d-af915e3e04e0
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	61588900-2a0e-42fd-bfa2-ba176f6bf41a
Muzaffar	22	12	2025	Muzaffar	Fetched all sales	a12c38c3-1cd0-4d89-a746-c3f8a459951d
Muzaffar	22	12	2025	0000	Fetched products for shop	7e528138-e5f7-4bb9-94b6-7bf20f4fe2a9
Muzaffar	22	12	2025	0000	Fetched products for shop	4b131d81-1256-4ace-8a75-c3f7a4596f33
Muzaffar	22	12	2025	0000	All admins fetched	2ad04cc8-5415-4bea-bee2-56bce8dacc97
Muzaffar	22	12	2025	0000	All admins fetched	28e93033-21b6-468d-a123-5d455f195294
Muzaffar	1	1	2026	0000	Fetched products for shop	31470470-9354-4d34-a8d3-d515c29073e3
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	f575613e-2f07-4e60-8eff-691e297443b8
\N	1	1	2026	0000	Sale created successfully with sales_id: 8ed4739d-d27f-4d1a-862d-6a0946d5d316	4f88a028-d457-484b-937f-83884ca4ca74
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	9a5759ff-5edf-4736-87ab-0e724011ec3f
Muzaffar	1	1	2026	0000	Fetched products for shop	a59996fd-cefe-407f-a313-6846a15ab7df
Muzaffar	1	1	2026	\N	Fetched all brands - count: 3	c72208ef-17d7-41fc-9f2b-c530c6ad1eed
Muzaffar	1	1	2026	0000	Fetched products for shop	5bcb584f-4d86-482e-9852-e67e8c92c760
Muzaffar	2	1	2026	0000	Fetched products for shop	0afa925a-0e9e-4557-b3c0-b7fcbd2851f3
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	c61f117d-0e1b-43a8-ab8e-32f91ae8536f
Muzaffar	2	1	2026	0000	Fetched products for shop	e0613e6c-7191-4667-8d58-baa2064a86e5
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	20507c62-741b-4d65-9202-5076bde46ebe
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	2209f570-d20c-4ee6-9dd3-c3e4cddcec76
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	a497ad1b-a476-4674-9958-e1e0da7b41bc
Muzaffar	2	1	2026	0000	Fetched products for shop	03aa7b0c-14a1-4c1d-9d52-41d309bd3f91
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	f51e64ce-1e38-4d33-8f19-dba9759c27c9
Muzaffar	2	1	2026	0000	Fetched products for shop	d30b1b88-3bfe-4318-a77a-2fdc912dce44
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	ac8001d2-0fec-4609-b2b0-844f3e740816
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	59c26a4e-fddd-4812-bb40-b038111a770b
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	97986077-d3ba-4088-bc2d-6be8ce5506b6
Muzaffar	2	1	2026	0000	Fetched products for shop	4bd61342-1904-4a72-a4df-888f20655d39
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	513f2762-f222-4b3e-b93b-39a2fe07b234
Muzaffar	2	1	2026	0000	Fetched products for shop	bdef0716-913b-47d4-92f1-6a618eacaa49
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	583e8fca-e418-403c-99a7-001519a21216
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	b914a98d-5a33-415c-922b-b8d453de2c24
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	94ca3b73-dff5-41fd-a5fb-ba64fec73a4a
Muzaffar	2	1	2026	0000	Fetched products for shop	8e3f7de5-e102-4152-9ab9-c8e0d9961d09
Muzaffar	2	1	2026	\N	Fetched all categories - count: 3	8d6150a5-8bfe-4360-ac60-ec114250c7ea
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	b7c38581-f470-4469-9ad6-41cfce35b60d
Muzaffar	2	1	2026	0000	Fetched shop branches - count: 2	9feeff25-0f10-4edf-a029-aacb5b6fee70
Muzaffar	2	1	2026	0000	Fetched products for shop	317c3751-2808-48e6-84d6-8c89b6951dfb
Muzaffar	2	1	2026	\N	Fetched all categories - count: 3	9d43ed29-d7af-4ba3-a81c-14e8b4754585
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	7b2fa2de-826e-4e04-8e99-03705369ab57
Muzaffar	2	1	2026	0000	Fetched shop branches - count: 2	d516d588-d50c-4b25-b4e8-f19f0e2b704c
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	dc1f6ae4-b89f-4124-8cb5-017e9272e0d8
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	f6d141f7-a2af-4d23-98ba-b89019f6ab49
\N	2	1	2026	Muzaffar	Fetched sale by ID: f7316cb2-0f72-48fc-8931-a3ec355b7a01	909c5e9e-8083-48bd-b7e2-5bd31192573d
Muzaffar	2	1	2026	0000	Fetched products for shop	a7711ce5-e4a7-4584-a077-8ecf669d3eae
Muzaffar	2	1	2026	\N	Fetched all categories - count: 3	a8d4c241-c2e6-4327-89dd-9a271cc32b6d
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	bd2083c3-47e9-4155-8f5d-e5cef4648cbf
Muzaffar	2	1	2026	0000	Fetched shop branches - count: 2	6cf63499-7baa-41d3-b079-23c79b78ca1c
Muzaffar	2	1	2026	0000	Fetched products for shop	29d7fa8d-c3c3-445f-a9ee-8de39b8968b4
Muzaffar	2	1	2026	\N	Fetched all categories - count: 3	3c1e9da7-45cd-4c12-9156-1203bd037ac2
Muzaffar	2	1	2026	\N	Fetched all brands - count: 3	b06bd556-8508-4385-84c8-08947d83db99
Muzaffar	2	1	2026	0000	Fetched shop branches - count: 2	667d8cda-0e6e-41cf-9c14-516357d6ae25
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	e3629512-c6db-428f-b33b-3733fc6ea4b4
Muzaffar	2	1	2026	Muzaffar	Fetched all sales	0ddb3893-c2e3-4e99-9a21-1ce4ef203619
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	b961e53f-d5f7-4def-aff0-76fe28f2b680
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	3a6d5a88-7d25-408c-85ca-ed58651489e7
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	a0dc5ea7-e01d-44ce-b7e8-14313783538a
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	e15e6960-2dfd-4520-951b-348e7193549f
Muzaffar	3	1	2026	0000	Fetched products for shop	13fb4ea8-3ec1-43ae-8cbe-ab083cd2f21c
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	2c540986-1bc2-479e-be91-cc0996f82825
Muzaffar	3	1	2026	0000	Fetched products for shop	95525885-49e1-497a-a39f-0b9321ccb484
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	67e7e3c6-2c73-48e4-8919-1bd74a5b9116
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	db64c88b-cb14-4ac2-9817-f621c1714352
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	0434756c-9679-4194-8a88-ae70e7e7c5e3
Muzaffar	3	1	2026	0000	Fetched products for shop	179800b9-c230-40bf-9972-9085240220e0
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	0ed317a3-0d53-4020-9e74-418a641bb2e3
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	02c02a52-d512-46f5-9693-9f9ab81ae0c2
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	c9f26a9c-fee3-4418-b757-257bb922a4a4
Muzaffar	3	1	2026	0000	Fetched products for shop	493bb224-6076-404e-8356-6cd6945f0166
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	5f846a5d-a8b7-4499-a78d-70996d19f402
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	53dc6cc0-dce1-4b4b-87f8-ae519b7ee916
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	940f37e1-3a54-4029-b05d-9faffc8dd407
Muzaffar	3	1	2026	0000	Fetched products for shop	630a3460-5f50-46b5-a320-e982bd1db06d
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	62a6cf14-e3a5-4858-bf72-53ef1d149689
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	c58bf615-6aa8-4b10-abb9-3f2ece3568a0
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	afc84bee-9014-4480-97ef-470f361bb571
Muzaffar	3	1	2026	0000	Fetched products for shop	2902af29-d439-43d1-ab45-e1a23e920d4c
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	f42ab9a8-17a6-4441-9da5-f7e6fb409f8a
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	7042e460-1da7-4e5f-ade6-59c783fe8bdd
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	178453f9-5c57-4289-9b7d-cf589dde8d5b
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	87ec544b-a021-4db2-9c02-6c4c2e390b52
Muzaffar	3	1	2026	0000	Fetched products for shop	6ccd2558-5830-42da-9c40-6fa12c191a1c
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	103fe11f-224c-4c64-94b5-45e4be074dce
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	c3fb4511-863b-4234-9e0f-7a6d0cf82f78
Muzaffar	3	1	2026	0000	Fetched products for shop	aa5b6839-c639-4ddc-9fd1-d2691dbcca12
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	cb337d46-af59-4378-9318-aaf3d955d55a
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	e9a5a550-3e28-442f-91c6-25205e80489c
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	c307dca3-1cab-4028-9e0f-6dc6c31c3b41
Muzaffar	3	1	2026	0000	Fetched products for shop	5afd1407-ed68-44b5-b2d8-d167b95e620a
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	5e2e8cdf-d5e0-4f49-a52b-00e23c196385
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	200d4b00-77d3-46e7-9ff4-e0df940f94c2
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	c7ec2f59-2973-479e-9d5f-80e95f2fdf36
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	6b9c2288-3a23-4ae3-902e-1a4b1d138f2d
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	71b220d9-a571-4a78-9d99-aea227f89bac
Muzaffar	3	1	2026	0000	All admins fetched	ee83fe44-e5cb-4461-b1a4-360de80f2143
Muzaffar	3	1	2026	\N	Fetched all permissions - count: 5	e7d45b39-773c-4150-b528-8ac5ef492d72
Muzaffar	3	1	2026	0000	All admins fetched	0efdd18c-c142-4ed5-b152-08fe7547d280
Muzaffar	3	1	2026	\N	Fetched all permissions - count: 5	27e6bddb-6b50-43c1-a291-f9786a1f26de
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	cb63b1d3-ed6c-4610-a4ba-33f8f37bf89f
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	1159f3bb-dc9e-40bd-b090-41fce3741b90
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	2b14de91-7a9f-4058-a455-36f1a7b38797
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	59246398-39ee-4b34-84c4-5c28bcd541a7
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	e2c7aed4-000d-4ad8-9929-ce92f4a51e09
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	d8cdafe1-93cf-4b1a-9f82-e51ba13e8cb2
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	1e96de10-e305-4d09-92b5-0ff0a07163ec
Muzaffar	3	1	2026	0000	Fetched debt statistics	c63f8da1-e4cf-415b-8823-937614f1fb33
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	13f113d0-7f32-475d-81f9-5b71caa94a6b
Muzaffar	3	1	2026	0000	Fetched debt statistics	3d722c39-8847-4f03-9162-9379ddf319b8
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	a877d853-fca5-4013-80aa-cccc93bded82
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	783d15fa-844b-4fc1-a7ed-054dd918ebd9
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	404f05ad-9ea8-4840-8016-46990397e40d
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	0adf9733-1e7c-480f-a97a-d96ade7a895f
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	2c86b9d3-f835-438f-b434-3c81681f0be0
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	8652ba9c-5899-41ad-9845-922967e5aba1
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	1eb29849-5a41-4652-9bea-ade51bcc3941
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	bdebecdb-701f-4397-84e2-326ff3d82a74
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	c8bca0e0-b43a-4bc0-98cc-4a2749d4ec56
Muzaffar	3	1	2026	0000	Fetched products for shop	5cefafd6-1b29-40b7-9f85-5493df6fc5c7
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	3e36b552-0499-45f7-824b-62958bd803c7
Muzaffar	3	1	2026	0000	Fetched products for shop	6dad920b-e669-429a-a1e8-3671e456cbc6
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	a665fd61-36e5-4391-9bc2-55bef315cc86
\N	3	1	2026	0000	Insufficient stock for product 'Qizil Olma'	b06e2012-1332-4758-bbf6-cb486f57595e
\N	3	1	2026	0000	Sale created successfully with sales_id: ee2ace91-ecaf-4c4b-b892-2b720ad4ce5a	1f3522e8-1c3c-4bbb-b4ae-cb37b0e08ca7
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	74fed703-6adf-4c38-8522-13183127f5f6
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	7d7ccfe3-9020-481d-9be4-9d3243e8eb15
Muzaffar	3	1	2026	\N	Fetched main finance statistics	79de819c-3858-4ce7-aa12-946a4bb524aa
Muzaffar	3	1	2026	\N	Fetched main finance statistics	016e4fcb-da9d-4c39-978d-e93634c1d101
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	8a3f4e76-7704-4e69-918e-51fbd6bc65d0
Muzaffar	3	1	2026	\N	Fetched high stock products	c2abd2f2-8395-478d-8bf0-23ceca9bd15b
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	370eb8b1-246a-4c34-9726-cc0442999adb
Muzaffar	3	1	2026	\N	Fetched high stock products	424f6b13-66f0-47d8-835c-d76c1ccd8031
Muzaffar	3	1	2026	\N	Fetched low stock products	ec7c6680-8b62-44b8-b34e-966bff5c072e
Muzaffar	3	1	2026	\N	Fetched low stock products	d74021ea-6fb6-4ce5-8436-3bb88f789993
Muzaffar	3	1	2026	0000	Fetched products for shop	d4a814d1-1c3c-45d7-98b3-84a0a015c347
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	1f9c0cc2-ca33-4841-b37c-dc952a60452d
Muzaffar	3	1	2026	0000	Fetched products for shop	921798b2-8459-46a5-a3f0-c734b5241b33
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	cde09488-4583-4598-ac44-17a19d3ef4eb
\N	3	1	2026	0000	Sale created successfully with sales_id: a9a36cf7-1091-44da-b1c9-55c0185058b7	37ab6a44-4c94-4566-b656-e72f2be4847b
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	faf09aa1-eae2-4758-95cc-65d0588cc886
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	c717a85a-a2d2-4e4c-b8ed-e764c854ca11
Muzaffar	3	1	2026	\N	Fetched main finance statistics	d8711f52-da48-41bd-a146-3c58c21a1a45
Muzaffar	3	1	2026	\N	Fetched main finance statistics	e2892c3c-287d-42fd-a1b6-b5db5fbf3ef2
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	1e0fefde-6596-43bc-b08d-d25953a68b60
Muzaffar	3	1	2026	\N	Fetched high stock products	f89ef74e-b937-472a-a778-b0aa010bd717
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	a1133010-6df9-451e-b7fc-17bc1ff0081b
Muzaffar	3	1	2026	\N	Fetched low stock products	a3825402-d87a-4199-988c-c0b77e396959
Muzaffar	3	1	2026	\N	Fetched high stock products	13fce999-124f-4c69-8d4f-ae9adbb33a20
Muzaffar	3	1	2026	\N	Fetched low stock products	6a171997-9fa9-439b-8f19-bfba2dfbe190
Muzaffar	3	1	2026	0000	Fetched products for shop	b8057860-60d8-4d18-b504-eceaf94364a4
Muzaffar	3	1	2026	0000	Fetched products for shop	d520e508-6742-4ad6-b99c-aded1653a2cf
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	a63d5471-e5a7-4a67-98d5-d5a9dafcb650
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	808e592e-73a7-40c4-9115-b9bdff3ffa61
\N	3	1	2026	0000	Sale created successfully with sales_id: 6987b7b2-531f-4164-aa77-45e74833cb06	70640794-b0b5-4f23-a72d-15648a74c293
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	0985fc84-3c42-4d85-be97-7cc41ee00bc4
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	1c2c430b-c56f-4b03-86d8-614c55f5162f
Muzaffar	3	1	2026	\N	Fetched main finance statistics	a36cee25-3731-4f26-9bd8-fa71881e83b4
Muzaffar	3	1	2026	\N	Fetched main finance statistics	416dade7-fa8a-4559-b8d2-bd5002ac1352
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	818e1ea4-797a-4cc5-b467-02bbf046be38
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	d815b9c7-e7ca-4aa3-b878-48715d666b1c
Muzaffar	3	1	2026	\N	Fetched high stock products	0b717ab7-8b03-42c6-a603-80e57abf42e2
Muzaffar	3	1	2026	\N	Fetched high stock products	4b89b4da-c3a6-4c91-a57f-055e3952191c
Muzaffar	3	1	2026	\N	Fetched low stock products	1e3b4029-9fc9-4b7e-b333-097c76bfefe0
Muzaffar	3	1	2026	\N	Fetched low stock products	6a3870b3-d5b7-441a-b569-f284978b80e4
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	8e8deea4-b52c-469d-96bc-c99fbf764be1
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	c202da0f-5fe0-4afd-9e79-b926acb61c9c
Muzaffar	3	1	2026	0000	Fetched products for shop	a3f3e292-c90b-4126-a1ab-4f2fc3ab8687
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	dc0099ae-67dc-40cd-bddf-fc232126bd1c
Muzaffar	3	1	2026	0000	Fetched products for shop	77fc61f6-0548-43e7-b147-a1b61b88275c
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	b52b8900-6548-4857-8cd3-e055ddbb327d
\N	3	1	2026	0000	Sale created successfully with sales_id: ecdc6bf0-73ab-4814-a51b-90d5a6e0e286	a6b9db7e-096f-454b-8ab4-0990553b9e96
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	5efa5cdb-ba2f-4c16-bf1f-6a870f06de80
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	a108457d-48ce-4406-a27e-569f180cd184
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	37fae86e-afb6-400d-a955-4fcbb6763ee1
\N	3	1	2026	Muzaffar	Fetched sale by ID: ecdc6bf0-73ab-4814-a51b-90d5a6e0e286	1b772043-b8d0-4d20-bce6-9eb8b52c6ab9
Muzaffar	3	1	2026	0000	Fetched products for shop	73beb135-da05-4eb5-949c-dcbde09ab854
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	c22070af-3b1a-42d6-9f5f-463338e00198
Muzaffar	3	1	2026	0000	Fetched products for shop	0e42467d-7e4d-412b-baee-7e6eb01b50f7
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	5d7088fc-967b-4775-b291-e1e9298b4adc
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	42a5373d-a66a-494e-8b6e-87564487b40e
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	cf88bf0f-2712-4402-ac8c-d07a5b798e8c
Muzaffar	3	1	2026	0000	Fetched products for shop	24c0480b-7e83-4a18-8a40-b08c6cd0c8c0
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	efbb628c-055e-4b71-8615-e44b1b868700
Muzaffar	3	1	2026	0000	Fetched products for shop	f24ce761-aa91-4e58-9c8c-a16d529fdc83
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	856690fa-7d69-453f-89fe-8bb7f6d3e1b4
\N	3	1	2026	0000	Sale created successfully with sales_id: 8415afdb-5ee4-4417-a1a6-7f2d8edd8f16	ef634574-85c6-464c-b238-6915edb60f17
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	07607c6d-e325-4dc3-a041-a7cc354d7ab8
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	95515a5b-145c-4c5a-8c55-6f6e54c970a8
Muzaffar	3	1	2026	0000	Fetched products for shop	f0f8eed8-7028-438e-a1e8-5956f88a3ec2
Muzaffar	3	1	2026	0000	Fetched products for shop	318489de-f394-46bb-8539-fa9a266737c6
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	a53d1bdd-4c40-49f1-8d78-a011ade00cbb
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	1ecbcfd5-534c-4ca5-a3bd-af475bc8b2fb
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	bc8feac7-ae7f-45be-95c3-d9e26ff2b802
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	cf743067-f6e6-4f05-bdda-245c02fd9174
Muzaffar	3	1	2026	0000	Fetched products for shop	5c2724c8-d64d-4e5a-bb23-531fb6673aa2
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	2016458d-ea4f-47c4-ad77-a565bc78cdc9
Muzaffar	3	1	2026	0000	Fetched products for shop	1ec0ff0b-3807-444b-bc20-a680f0e77ff9
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	ecba84fa-c13a-49b5-a042-55469abd0e6a
Muzaffar	3	1	2026	0000	Fetched products for shop	7cc432be-73e0-43d2-9dd3-dfe2f1c85452
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	07223d00-dca3-4e90-911c-a397f11c448d
Muzaffar	3	1	2026	0000	Fetched products for shop	6c99d8fc-80dd-4e45-8f54-634078f31df6
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	dfaceac8-f89b-47f1-b434-ad5004367434
Muzaffar	3	1	2026	0000	Fetched products for shop	51a61de1-6762-47ca-8ec4-be427c9dad6f
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	9b2df20f-4f35-4813-8b2d-c6b3b947a9a2
Muzaffar	3	1	2026	0000	Fetched products for shop	c723ea8e-645e-4dd6-a190-95dc58d696e9
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	b83efc6b-0078-4043-b111-454f196984d7
\N	3	1	2026	0000	Sale created successfully with sales_id: 44c0ce3f-b1c8-4fec-a702-b75396712a57	90ae5f32-9537-4faa-ac66-521f7da441b5
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	66801cc6-4ac1-4660-b8b8-1e53181c30e7
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	d347d1d2-80d5-4fd1-b257-c17edb1cd943
Muzaffar	3	1	2026	0000	Fetched products for shop	0378d25d-4156-479f-8ce9-02914c247d7c
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	fecfca8d-7836-42a3-a8f6-2da059b06c42
Muzaffar	3	1	2026	0000	Fetched products for shop	88d3f9ed-c2fe-4fde-8dad-c17189a5e8ce
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	3fbd42e2-1049-48fc-ac23-496dabf1de4a
Muzaffar	3	1	2026	0000	Fetched products for shop	49965c31-b7bc-4379-a510-c9ad77f97ed7
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	bdaef4db-4e5f-4675-9cba-b9c9492fe593
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	f658cec0-afda-43e1-9638-16e3af51d846
Muzaffar	3	1	2026	0000	Fetched products for shop	d9e472f6-d214-4991-9f50-30a216d5fa53
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	bb9dfdb1-af9e-4f9d-9493-bee4122bc96d
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	56fe4816-d57d-4cf7-9668-a829396be35a
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	7b3637e9-ec04-4ca1-8c54-75b22ef44132
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	f55d29d3-cd3e-45f0-b939-5db7bccb7ba2
\N	3	1	2026	\N	Product restocked: 90efd7fc-1711-4e46-9a55-c7a40bbb41e5 (added 150)	d37a13e5-bb39-4a2b-b3e3-8511cd305c60
Muzaffar	3	1	2026	0000	Fetched products for shop	515c0ecc-bd8e-48c0-8404-646d20aeaa52
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	ef6a89c9-1ee0-4f69-89b6-b718f9f09ddd
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	9f7e7186-7eaf-48ac-88b7-82f22c9e4ba6
Muzaffar	3	1	2026	0000	Fetched products for shop	81a7d78d-4c5e-4ef1-9088-16af2cbc2383
Muzaffar	3	1	2026	0000	Fetched products for shop	f4ed4f35-b683-43c4-8dc2-9841fa96b4fd
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	32aa0780-93f1-4903-bc98-c86815bcf3e0
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	9a9f7c2a-91ec-4f9c-b168-7dd35acb3703
\N	3	1	2026	0000	Sale created successfully with sales_id: ec616336-618d-496d-b7ae-b3feabd9ebd2	899adf12-697f-400a-9a58-af8367339b24
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	58618870-e07d-4a25-be25-821f10f1167d
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	d2a1fc22-8740-443b-857b-7916b64bec40
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	5e739657-e9d0-4a5f-9772-ceafa2766c24
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	d4e0c1bc-7734-4c41-b35d-c18f327702c0
Muzaffar	3	1	2026	0000	Fetched products for shop	ba63ef22-08c0-43ab-8513-ae36515816bd
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	bd884998-bd25-43e8-b3f9-3a512e8f3e72
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	91e17606-f894-43b3-9c23-b330eab44cdc
Muzaffar	3	1	2026	0000	Fetched products for shop	215bde8c-f94d-46b7-9480-522d9cbf7e6b
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	a6d6dfa6-72e1-46a7-8a25-58d45040710a
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	e62eed96-46d4-4872-91a7-20dd64e2affe
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	54e0dbad-ff71-4885-a902-13d9ae53c425
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	c619d6c7-fc22-4309-861c-2a5c34b504b5
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	beb4dc3a-be4c-424e-8b7c-3be3ce3ff373
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	b10196b2-0c78-40e2-bc30-1d83573fe661
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	be65e523-10e9-4d1a-b411-94a19b1070e4
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	ece06071-1ccd-4ef6-9404-3887705f7641
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	fcea9e0b-1e7d-4e30-b0a4-e6f968ec90ed
Muzaffar	3	1	2026	0000	Fetched products for shop	267cccda-e482-4f69-bc31-e52179c13b46
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	cedaf6e5-1f32-4713-b313-aa0529432204
Muzaffar	3	1	2026	0000	Fetched products for shop	6aac1179-5dc9-4401-ad02-102992471224
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	211ebe05-3c85-4d38-9000-fae9d333ba0e
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	bc02563d-ce76-47cf-b6e7-b49ed35080aa
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	4cc602c8-b7d8-4f75-b34b-638a930f1752
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	f16b693a-8424-4854-92cf-4b39ec7e6e84
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	c194fc97-a206-4cf0-869d-c6451175e54b
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	9d09802e-ba51-4ff0-a245-3c501189e5f4
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	c0686bd0-662e-45b7-8dfe-518a6c655cc6
\N	3	1	2026	Muzaffar	Fetched sale by ID: ec616336-618d-496d-b7ae-b3feabd9ebd2	cab36063-1674-4141-b416-24a7da5a11c8
Muzaffar	3	1	2026	0000	Fetched products for shop	0204bff4-ce1c-499d-8f59-e142978bcdf0
Muzaffar	3	1	2026	0000	Fetched products for shop	43b2757f-747b-4a66-9eda-0c2eaf1a228c
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	031bc759-3571-4a87-b752-986403a6e11a
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	185f641a-ddf6-4cd1-a187-9c379dba43af
\N	3	1	2026	0000	Sale created successfully with sales_id: 6b270c55-fc71-4684-9492-591bd2cf748d	2782a5f4-3b45-4cbd-9f9d-1dd1d56658ff
\N	3	1	2026	0000	Sale created successfully with sales_id: 9313157e-9bd2-48af-b160-a9a3d1e33473	9b3a78e7-dba5-40cf-8478-3f68b9220274
\N	3	1	2026	0000	Sale created successfully with sales_id: 127d8faa-9bff-49a2-9f79-19efb09fc322	43ab56e5-deb4-4d0d-8ca3-ae68901c44ed
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	c51f22e5-2b68-49bd-8292-f5de3ae42a30
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	4416c260-ae3e-4121-a17c-38c79b2a7e0e
\N	3	1	2026	Muzaffar	Fetched sale by ID: 127d8faa-9bff-49a2-9f79-19efb09fc322	95c59c30-e28b-4db9-a717-67b15d455dbe
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	05339756-dbbd-4727-b90a-01f49653ae0f
Muzaffar	3	1	2026	0000	Fetched products for shop	7d0c998d-f4a8-4c69-9188-821df07738df
Muzaffar	3	1	2026	0000	Fetched products for shop	b62f1ec6-eea9-4262-91b2-680fd2bc756c
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	c7533750-26a5-4660-9273-1835172f51d8
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	e496cee7-4a16-4789-98a1-c09b4b111d0d
\N	3	1	2026	0000	Sale created successfully with sales_id: 232192b8-27fe-49d0-a456-6b3ae2e0de25	e4bdd453-4f5d-48f2-bc43-335b9548e79c
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	ffd886dd-00d7-4f1f-aa6d-2a5bb3da3a28
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	6a36cdbd-4686-4a72-834d-4eecd0ef64be
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	9f4a5725-63c0-4b30-8297-ed007c992c9e
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	1a9aeb9a-3ad3-4e6d-8bad-5f357cdb5b0c
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	6766f1cf-3f75-444d-80f3-a8c6b5ca014c
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	ec9c787b-7ea4-4da4-96d3-688a4e6bef0a
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	d8c7a757-27e2-43b3-88ab-e42a14ef3463
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	3762f9e3-28c4-4ee2-97fe-7c17f6ce4805
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	497f5c5d-1fe2-44cd-b424-52bb8867ff94
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	6087a355-a989-479d-85e4-4f78f8d8e270
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	852047be-b5eb-4583-910a-1f99d38eb2bc
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	edaace59-eedc-447b-926d-52bbe3132230
Muzaffar	3	1	2026	0000	Fetched products for shop	4b66f8e5-996b-4dae-9eca-e0cbcfaed535
Muzaffar	3	1	2026	0000	Fetched products for shop	c0f0c7bf-c19c-4afe-ad50-15260f8e060d
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	03a23955-4e5b-4e5a-8573-c30e4e9064e8
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	53823cb2-b27c-4c7f-9d53-537d6e5196d5
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	7d8bccc5-2d8e-456a-b7dc-822f3f75ad10
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	8af18006-2f3d-44d3-b5da-fa340d26a7f3
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	e531ec75-d463-4be0-9192-25fa840ec631
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	1a88bdc1-512c-4e22-9184-d639c78a9535
Muzaffar	3	1	2026	0000	Fetched products for shop	89766de8-43b9-4b99-b854-b5486584865e
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	fd27792f-77de-49a2-9647-fe92c094b3ed
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	5ca78ff1-839f-4d76-be72-9ed3e5ea3f50
Muzaffar	3	1	2026	0000	Fetched products for shop	2c7e1501-ba81-4c0d-8bea-716a4cf286ed
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	9002d14f-5f80-45b7-9504-003b60347d16
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	4ac19c21-445f-4ebf-b996-d45fbe92d6b4
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	90770da0-3d0c-447c-b003-3b3fe834b4ec
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	c69b2cc6-857c-4f46-bd53-d705ba86c5ce
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	fff33aa9-7150-4a11-b429-d1166b6e23e7
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	6b49bf69-9546-4d3a-a17d-cfa4baf66063
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	c6c13120-c862-4f7c-9e41-ead81970cf82
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	d95accb1-1cd1-47e4-baa1-4ff5fd6784a1
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	10d9796e-6dee-4e0a-b227-103e0f1b2a79
\N	3	1	2026	Muzaffar	Fetched sale by ID: 232192b8-27fe-49d0-a456-6b3ae2e0de25	33c2b09f-ff0f-4a87-8c4e-ecc0c0882cd1
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	7f53e0de-ec10-4b41-adcd-909aceb0117f
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	01427501-bfce-44fc-b884-ee46c51ef7af
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	7f779e8d-f336-4afc-b2ab-8ebc00ab5b05
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	be7f2f8e-2979-4e0a-8c27-36f888a25e50
Muzaffar	3	1	2026	0000	Fetched products for shop	4e57804b-dde6-4df9-816b-e9dab4b8e6e2
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	60bac4a3-cd54-4a4e-9e6a-bc4672ba1a4b
Muzaffar	3	1	2026	0000	Fetched products for shop	f8a07486-8d90-4940-9c51-8a9204e9343f
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	fe8d45fc-8dbf-4525-9ac8-9ffd5bccf7fb
\N	3	1	2026	0000	Sale created successfully with sales_id: ad664556-2909-439d-a69a-d4edc1ea1d4c	2548de3b-d73a-49fa-aea3-a01315b9b952
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	fb76e1c2-8458-4d94-98d3-9638a9ab2925
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	ebd2fd21-9dd4-4f34-9b21-7b7fe724f992
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	ac244079-0fd7-4ec7-a5e3-b8f3abc25f17
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	9c925740-282e-4396-a687-07d4b7212793
Muzaffar	3	1	2026	\N	Fetched main finance statistics	3f478706-90fa-4c9a-a32e-19811fccc29e
Muzaffar	3	1	2026	\N	Fetched main finance statistics	648ae3a2-80c5-4980-b20b-b070b189be1f
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	6d2e71be-afc2-4365-9849-af5c0a2f3550
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	1e1e1f1d-849d-4789-97ca-69f57e69c227
Muzaffar	3	1	2026	\N	Fetched high stock products	32c9152c-9f0d-48e0-a55d-a9cbff936f89
Muzaffar	3	1	2026	\N	Fetched high stock products	3493c613-3ab2-4b96-a423-8685323e46b6
Muzaffar	3	1	2026	\N	Fetched low stock products	9a9dd3ef-c764-4d8e-b6d1-7af52acf3e66
Muzaffar	3	1	2026	\N	Fetched low stock products	e033a1c4-f039-4080-9228-99c1a8eaece8
Muzaffar	3	1	2026	0000	Fetched products for shop	9e9a078b-8788-4379-81e9-09eea7008c8f
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	dfdfccdc-d6d0-4602-b9f3-f6f0fa6b3207
Muzaffar	3	1	2026	0000	Fetched products for shop	0a9d7d68-6e76-455e-9b13-d1cc5554b5ff
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	03174587-213c-402c-808f-304d3b9e7981
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	31d0a6ed-758a-4465-a1b3-b435d0548fa1
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	f23542ad-7328-48d7-91d9-42911c73cf12
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	cfaad4f3-393c-4675-9782-37d3236c33a1
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	5cd760b1-83e6-4f88-889a-86fe325e494a
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	3db6ecde-7145-443b-b6d1-3aaa622a5443
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	ceba7866-c452-464a-b778-4a8f78c650b4
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	93a5a42c-eb30-4d29-a833-17cbb1805e27
Muzaffar	3	1	2026	0000	Fetched products for shop	73be32bb-697f-41f7-9440-e9e7367d2e0a
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	ebe0252c-dd82-4045-aba9-5a79331d0386
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	c673c3c3-8e09-4d45-96b5-d244552e87ab
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	9f4ba4a8-fce3-4609-b98b-0104fa7cc33b
Muzaffar	3	1	2026	0000	Fetched products for shop	ba131ecf-568d-4161-8af9-5d08981d1e2e
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	6c8511e3-b0d9-491e-bb85-a47ac6d763aa
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	589413f2-4e14-4574-b197-44669dfc60cc
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	17c3e26a-5dd0-41d2-b6bf-83df21bfbb7b
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	989487fc-53a5-44ad-89b2-b7e43d0cc06e
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	3c142889-824c-4f82-9ffe-d209caecb1a3
\N	3	1	2026	Muzaffar	Sale updated successfully: e36a8b06-8d00-4eb5-9d5f-6d4157af2bfa	869be2ff-2299-410c-99f8-785e6576792f
Muzaffar	3	1	2026	0000	Fetched products for shop	8ae2c256-bffc-40b3-b652-0bcbdfb523c9
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	555ed661-30a1-452e-8070-d45019c74747
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	c3c33307-4643-49f4-9498-d71a53dd9eb2
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	2ebe157f-a5cd-4484-8697-8b2ff66fa77e
Muzaffar	3	1	2026	0000	Fetched products for shop	5dc21589-f539-4bc8-80df-af4278463369
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	df1b408e-0be4-48ae-888e-e820b0c399b4
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	f50e67bf-0c5a-4b5c-bb19-e9531446b0cc
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	3daf2146-60ab-411c-b404-db794d924f15
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	8a2332b4-0da9-4c54-936d-6a1265a86458
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	a7736d00-924c-4757-be75-d612e1ebf31e
Muzaffar	3	1	2026	0000	All admins fetched	85daa4af-1343-4601-ac10-23f2dd792cd2
Muzaffar	3	1	2026	\N	Fetched all permissions - count: 5	a8f93a47-135d-4121-99f6-72d40e511250
Muzaffar	3	1	2026	0000	All admins fetched	51f4bfa7-1e9d-4f71-b9de-738906a33062
Muzaffar	3	1	2026	\N	Fetched all permissions - count: 5	c1e0c635-9805-4b14-8d42-0b28a46cdfc1
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	cff88f61-f76b-4052-a9a7-a2abf702e237
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	047d2e02-f74f-4609-a31d-9d8441bb3906
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	f52883e1-dd3f-40da-bbc8-17361753f244
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	c69af3a5-21cc-461b-9f4f-3ca55328c797
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	75581d54-0a21-408e-b5c2-755073c86268
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	11f11db1-dc2e-4a61-b907-5a5796b27cea
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	b6552351-39f8-40a6-aa73-7403f9c2ea51
Muzaffar	3	1	2026	0000	Fetched debt statistics	bba8661b-1bf3-4c43-929e-9f7f2b8877c8
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	37e3b8bc-4f0d-43eb-b13e-f5bd181ae7ec
Muzaffar	3	1	2026	0000	Fetched debt statistics	eb1a49c9-ded5-4bd6-9700-295017404e3a
Muzaffar	3	1	2026	\N	Database backup downloaded - tables: 13	60f3bf8f-aa2f-41b8-9c30-1572b643bfe3
Muzaffar	3	1	2026	\N	Database backup downloaded - tables: 13	b4b30196-b067-41ca-b69a-673bef928293
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	fa964f15-6063-46c2-86a8-f3f8db17d638
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	e6ce8bb1-5f26-444f-9e2f-68c77fc2e43b
Muzaffar	3	1	2026	0000	All admins fetched	d66ecd3e-0532-4e93-a431-80e7569c9e09
Muzaffar	3	1	2026	\N	Fetched all permissions - count: 5	62eb5ec2-2c1b-4f18-af97-3f5ab832f3bc
Muzaffar	3	1	2026	0000	All admins fetched	5aac9ce5-f9ff-4cce-a8ad-66f7ed14e858
Muzaffar	3	1	2026	\N	Fetched all permissions - count: 5	70bbf3ee-f4b7-4252-9ba1-6888daabff4f
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	0135ce71-fcc3-4b18-b4ce-06f7085f919d
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	63911e11-16bf-4c21-b14d-37d6e92b0be2
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	22084360-6051-475b-aec5-ce4b0a5912b5
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	aa888101-d762-41b0-a5b5-ea3d33b714da
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	56bc8314-56e7-49bf-882f-45c6d0d773c7
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	88744b72-c247-42e4-bd84-d61e00bd43df
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	72fdf8bb-8b4b-452d-83d5-805a01e61197
Muzaffar	3	1	2026	0000	Fetched debt statistics	5195c106-601c-485e-9dd2-a92d86316f9e
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	51863194-873b-4784-bc32-2f1ef4a4a314
Muzaffar	3	1	2026	0000	Fetched debt statistics	687f69c8-c3de-4320-92d0-936dfa09a2a6
Muzaffar	3	1	2026	\N	Database backup downloaded - tables: 13	337236d1-d1d5-4cf1-afba-da0b005748b6
Muzaffar	3	1	2026	\N	Database backup downloaded - tables: 13	c029d306-ff10-4ae7-bbf1-8922c981cdc0
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	9002cf50-e47c-473f-bbea-d4fba027fe58
Muzaffar	3	1	2026	0000	Fetched debt statistics	a58637b2-e0d8-4f21-8983-bf539ac319da
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	49f98a8a-cab5-4463-8b6f-31574573f07a
Muzaffar	3	1	2026	0000	Fetched debt statistics	48bd2d85-6d42-42da-a0cf-691b56d34eb1
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	137c6af8-b672-4ccf-be01-fab84c532c6e
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	cbf38030-cd57-4410-b24b-388047c7cd1d
Muzaffar	3	1	2026	\N	Fetched main finance statistics	1ccb8798-f451-4129-aacb-73ca1db5680e
Muzaffar	3	1	2026	\N	Fetched main finance statistics	82f128fd-a0ec-4720-bf65-8cb135cead0b
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	7f33e12a-3226-4c5e-8497-cf0c2bd0e747
Muzaffar	3	1	2026	\N	Fetched high stock products	e5466950-97cf-4bb0-a3d8-cb314de32422
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	16199707-7803-49df-9d22-fc81e55e8fd3
Muzaffar	3	1	2026	\N	Fetched high stock products	1216bf29-a800-420b-93d1-398518613dcf
Muzaffar	3	1	2026	\N	Fetched low stock products	e8e73819-d658-411d-b5b7-86fe63b8e71b
Muzaffar	3	1	2026	\N	Fetched low stock products	02d76088-4a25-41ab-aac9-5c0c340c5f89
Muzaffar	3	1	2026	0000	Fetched products for shop	4ae91f16-18f5-4e5a-8a47-07465fb1f395
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	acb63341-c669-4bda-a82e-425e3c1a008c
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	deea2209-0a19-4d57-87fe-89f6f7ec7d3d
Muzaffar	3	1	2026	0000	Fetched products for shop	7dfd5d99-6ca5-4e42-9457-cbe7af02c4b4
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	2326f9ee-4355-4d48-b6dd-f5b09807a85a
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	176fce90-4377-4a9f-ac9a-7a6e2b8d64b5
Muzaffar	3	1	2026	0000	Fetched products for shop	f671a0fe-c045-473b-96ce-f2bb48b728f8
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	3e89341f-50b5-4968-9230-19b8a51a46d4
Muzaffar	3	1	2026	0000	Fetched products for shop	bacd54bd-05cd-4ab7-ae41-e5fceb837311
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	59b78e9b-c8ba-46ea-8c32-cf65de87f10a
\N	3	1	2026	0000	Sale created successfully with sales_id: 4ca34cad-19a9-489d-9c9e-11055a25102b	ce5dc835-54f1-4778-bfce-12e3e2f45fdf
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	82ebb532-2381-4302-bd2c-0ab36eb1007d
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	b3140d61-f4f7-4297-9ca2-e8cbf5b24a33
Muzaffar	3	1	2026	\N	Fetched main finance statistics	5ef72919-0104-4a0b-872f-64711d315efd
Muzaffar	3	1	2026	\N	Fetched main finance statistics	a7bda966-c7a0-45fc-b27f-a1d4f3d0411f
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	c9565d22-7dc3-42a3-b588-5fb27d23d2cb
Muzaffar	3	1	2026	\N	Fetched high stock products	e7c17a8a-2794-44de-a5de-c29342320162
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	78def45f-a303-4397-9dce-95ec17b1f3ae
Muzaffar	3	1	2026	\N	Fetched low stock products	66b6c30b-3c0d-4ba4-a16a-79c4fa25d4bc
Muzaffar	3	1	2026	\N	Fetched high stock products	4110224e-5002-4095-8a3a-6a808c1b9824
Muzaffar	3	1	2026	\N	Fetched low stock products	2c75aeea-592a-49c5-a7b6-5cea2b3887a6
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	ac53d159-553d-43ca-82da-9e3cbf4b9814
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	957f52e2-915c-47f8-b20a-cab83adbfe70
Muzaffar	3	1	2026	\N	Fetched main finance statistics	1216ba91-75e6-4cbd-bc00-16c370926e35
Muzaffar	3	1	2026	\N	Fetched main finance statistics	510b948e-c41c-45c7-b058-6c93ea960682
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	462140ea-e857-452f-b930-2c7d50e81696
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	9e45fe35-0989-46aa-8027-e0f418b35c5c
Muzaffar	3	1	2026	\N	Fetched high stock products	ad9825c7-c1db-43f0-836a-478b112a6816
Muzaffar	3	1	2026	\N	Fetched high stock products	4ef54637-3670-43d5-bdf2-9eeeb91be05b
Muzaffar	3	1	2026	\N	Fetched low stock products	0dfd5910-cbff-4f81-9002-4fc4dafefb4c
Muzaffar	3	1	2026	\N	Fetched low stock products	1900d784-258b-49db-a090-6cdb6c81f758
Muzaffar	3	1	2026	\N	Fetched main finance statistics	a6babc5d-6d89-4511-a727-da2eb78bb46d
Muzaffar	3	1	2026	\N	Fetched main finance statistics	06b9e987-df7b-48cc-be75-115410e0be93
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	db11cf04-8517-4126-9df7-8122ea2974fa
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	40006f41-6d36-4548-8bd3-3e11586d1a25
Muzaffar	3	1	2026	\N	Fetched high stock products	88d9910c-8526-4648-a04b-d768170183c5
Muzaffar	3	1	2026	\N	Fetched high stock products	372a4345-d206-409e-925f-b7dd4344b28f
Muzaffar	3	1	2026	\N	Fetched low stock products	ad9dba0e-cbc9-4738-9f9b-fbb16819acd4
Muzaffar	3	1	2026	\N	Fetched low stock products	f3e0a1d6-5364-46ed-859b-f4ab49c72185
Muzaffar	3	1	2026	\N	Fetched main finance statistics	809b874f-ca16-41d5-962a-03e35ea78ba8
Muzaffar	3	1	2026	\N	Fetched main finance statistics	db30da90-1363-428d-b600-0320d986dc3d
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	e3b18f71-c4bd-4f7f-b28e-b1da60110a27
Muzaffar	3	1	2026	\N	Fetched high stock products	705f1db2-f328-40ea-ae46-6dcf3f8e00e9
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	9f31db96-4e95-4ad3-8cfb-32ceb3214a15
Muzaffar	3	1	2026	\N	Fetched high stock products	0b8b96cc-206f-41db-9bec-f812bc50b66c
Muzaffar	3	1	2026	\N	Fetched low stock products	90efb2bd-f5bd-4881-b6ef-d1f6a2754a09
Muzaffar	3	1	2026	\N	Fetched low stock products	ac3a96a4-e72d-41bb-aacf-f5830bf1be29
Muzaffar	3	1	2026	0000	Fetched products for shop	7674a5b4-e033-4a77-bc3d-9265d93a9116
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	c78973a7-5be2-46b5-b3ea-90180f672dff
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	14c66dbf-1841-45dd-87b5-d16e2bc1e803
Muzaffar	3	1	2026	0000	Fetched products for shop	2af41c5b-061b-43ae-8b96-cd54e4541904
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	7aef9575-118c-43d4-ae2f-fb5ac46c48fe
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	30dd1109-120f-4bc6-a1f0-27a31aab3c02
Muzaffar	3	1	2026	0000	Fetched products for shop	2cc18336-8080-4709-a55a-7d447f9d203c
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	2807fe20-3915-470e-8c02-6bedf6732253
Muzaffar	3	1	2026	0000	Fetched products for shop	79205713-7cb9-4296-a22d-6ef414456f47
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	ba6abc33-9678-489b-8fc3-992aefebebd2
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	3db7c60b-a54c-492d-bf4a-9a2dd8344c3c
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	e57523d8-628d-4b33-babd-1a7fcb348b21
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	a6b8ac76-b457-4427-8905-99c04ee43a5e
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	ad6d3e12-9f3d-4b55-8dac-f3405eee4d39
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	1d713dc2-14af-4270-bd59-722549bc2e30
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	992ea050-a7ac-48ff-9228-5a53e62386fc
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	b87111d1-d92d-4ff0-b0b2-f8b2a4ebdd0b
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	2aec3ff3-20f6-497b-971d-a911145383c6
Muzaffar	3	1	2026	0000	Fetched products for shop	f1cb0292-c311-4c5f-87f5-152afd60eac3
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	6729cf0b-ece0-42e5-9016-c80d9c46c808
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	caa81cf1-a15d-47ec-8bdf-e184f48c37a4
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	3dae8bd5-affc-4057-957b-31aa29a0b184
Muzaffar	3	1	2026	0000	Fetched products for shop	0542ddd4-8239-4b1d-892a-102ea6a2603f
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	66490e30-1352-4635-8d73-5091c7be0f67
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	8a6858a1-ef41-4b17-8f19-f2f6527f78c1
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	0107f413-3d1e-4e36-80b1-9638f932e6a1
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	3626c6e0-2a23-4664-975b-4015ba428c9d
Muzaffar	3	1	2026	\N	Fetched shop reports as superuser - count: 58	6b227dea-dcc9-4f9c-a609-18f322cbaba7
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	bcbcfe0c-b4fc-497d-b23f-a3bfe9a1560e
Muzaffar	3	1	2026	0000	Fetched debt statistics	6e5cb12f-5133-49c6-9287-dbde506b73eb
Muzaffar	3	1	2026	0000	Fetched all debts - count: 5	344a54ba-5d04-49ad-bedd-7f210891a743
Muzaffar	3	1	2026	0000	Fetched debt statistics	e3dbd45c-fff6-4d0a-87dd-c1c4ef5f338d
Muzaffar	3	1	2026	\N	Database backup downloaded - tables: 13	52dff9ad-0e6f-4e59-bd95-1bbf911f53f4
Muzaffar	3	1	2026	\N	Database backup downloaded - tables: 13	3354b510-249e-4958-b1c5-372895f0d3ef
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	ee7c5e66-05c9-4c81-b007-fa622f033829
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	2eea9251-1346-4ad1-8ddc-c96e5e192f34
Muzaffar	3	1	2026	0000	Fetched products for shop	3f5a09de-a313-4f82-83dc-ce2df4b7821a
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	659011c1-68d0-4d31-bc16-cb9f8d01155c
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	143502ee-5f2f-4239-87d4-cecdcc97ec71
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	bc5f3c7d-8fa4-48bf-88ed-ad7dfc66e337
Muzaffar	3	1	2026	0000	Fetched products for shop	ce9058e8-4628-4d74-b52f-ad708bc1b9d7
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	86dfedc3-ac05-4795-85d6-77e8cf03c3fb
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	aa0eee32-d3c9-4f4c-98e8-ffafaf600ff5
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	ce3f1d6b-e04d-420b-ae09-688ec00086c8
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	d4050191-2214-4ca1-8eb3-e070d2ce7229
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	201e7a80-2306-4a6c-bc8f-56c2357e8504
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	7446f44f-5c07-48a5-9e5b-b32bb4bff227
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	56cf172f-04c8-45b3-9c0c-930fb5465d6d
Muzaffar	3	1	2026	0000	Fetched products for shop	4f1d7738-1958-434f-92be-779375f14175
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	717c03fb-d562-494f-9154-94b23c1d4767
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	76c25c07-3cb2-478d-abae-23deb1d78ae9
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	594028c9-546c-4f28-9865-8bdc4a2cf1c0
Muzaffar	3	1	2026	0000	Fetched products for shop	97e36c3b-a5fc-4ccc-a64b-9f967885b78d
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	d1daf08e-4405-40dc-b888-84e748e0e636
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	97c9365d-b4d8-4b16-ae72-2f9304ccb572
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	0843f8da-9ddc-42e5-9df8-cc68004db1d2
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	1ddce227-f925-42c2-8232-02b645542073
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	59b6bb11-e06e-4504-b853-cd3c06dd490f
Muzaffar	3	1	2026	0000	Fetched products for shop	8a2aadcf-aba6-4c39-b45e-e323136cc3a8
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	cd7406de-ab07-4339-aef2-6a52b9236695
Muzaffar	3	1	2026	0000	Fetched products for shop	5aac378c-6a69-426a-b7f6-2aef5e544cc1
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	4671bdd9-1c21-471a-9a77-7a1416e84719
Muzaffar	3	1	2026	\N	Fetched main finance statistics	50e90b1f-204e-49d7-9fd7-f1c7b85ddbf6
Muzaffar	3	1	2026	\N	Fetched main finance statistics	eece629c-1b5c-4d6d-8720-5d6e5f1601a8
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	2de2a89d-659c-417e-ac00-c81a9b9bbef5
Muzaffar	3	1	2026	\N	Fetched high stock products	29cf019b-006d-4c93-a431-28eadf67e50d
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	d2bd1668-211c-4f8d-8251-8e268674a91f
Muzaffar	3	1	2026	\N	Fetched low stock products	e28235e8-2945-487f-8c63-73196b2a0a0d
Muzaffar	3	1	2026	\N	Fetched high stock products	3746453a-6cae-4b12-8f5d-89870daac0e7
Muzaffar	3	1	2026	\N	Fetched low stock products	d31f154d-d930-4fde-a90f-5aaa142898c0
Muzaffar	3	1	2026	0000	Fetched products for shop	c0993930-894f-45c2-a56f-6ec82434bddc
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	4a5adb29-4942-4bd6-b104-75acf9cca464
Muzaffar	3	1	2026	0000	Fetched products for shop	72693413-7631-4259-90d4-b9d892a9882e
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	19d557f6-e6c4-4a79-9f6c-479003b53f01
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	aad43449-df82-45bf-b9ba-97254eec5043
Muzaffar	3	1	2026	Muzaffar	Fetched all sales	cc563405-1567-4c57-9dcd-66d3df2f2b65
Muzaffar	3	1	2026	0000	Fetched products for shop	f20cfa03-cc29-4915-b740-9f7c01af40ff
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	32d02ad4-b8ce-4fc7-8c15-d153ca79d9f5
Muzaffar	3	1	2026	\N	Fetched all brands - count: 3	9d6f16e4-69ff-4a5b-8639-433857a73168
Muzaffar	3	1	2026	0000	Fetched products for shop	9737ad32-8fd1-4652-a9a8-5439111e7f8c
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	3017faa9-7ec9-4369-9483-5096fea67422
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	e9ccb7a0-29c0-4776-a667-0e9203c35cda
Muzaffar	3	1	2026	\N	Fetched all categories - count: 3	5fdd21e1-da81-47dc-8cc1-20b962a40b2a
Muzaffar	3	1	2026	0000	Fetched shop branches - count: 2	6d0b6314-5ffd-4c0e-9a5a-07742337d454
Muzaffar	3	1	2026	\N	Fetched main finance statistics	d055562c-5a3d-4af0-8442-915f8fb966e2
Muzaffar	3	1	2026	\N	Fetched main finance statistics	cf6f701f-942b-4bc6-aa2e-5ae0d3b6c801
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	f2b381df-d6e8-4ec7-b259-e43e5cd06499
Muzaffar	3	1	2026	\N	Fetched high stock products	8cd7fa73-b1ef-4769-930b-37f6002218b8
Muzaffar	3	1	2026	\N	Fetched week statistics (last 7 days)	fda5c120-2735-4bcb-9638-3d89f132d2a7
Muzaffar	3	1	2026	\N	Fetched high stock products	9d49e226-cfc5-4241-80a4-fbf9501040ab
Muzaffar	3	1	2026	\N	Fetched low stock products	d2c23a56-94ed-460d-932d-e11b24abf8f7
Muzaffar	3	1	2026	\N	Fetched low stock products	8ff7e3b3-55a9-4b0b-a943-7dbbdcfdf3c4
550e8400-e29b-41d4-a716-446655440000	4	1	2026	\N	Superuser logged out	d68b297d-c5ec-4de0-9152-9eaa1c687f93
Muzaffar	4	1	2026	0000	Super logged in	5c204c25-9a91-4213-8f02-0c78fcc4466e
Muzaffar	4	1	2026	\N	Fetched main finance statistics	e6d7e7cd-4bcb-44b0-bde0-0c3df2ebd55e
Muzaffar	4	1	2026	\N	Fetched main finance statistics	690ba13d-6bcb-4ec6-a253-648ca2d54c89
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	003e6da2-0f37-4e3a-ac99-b26ceef73ed0
Muzaffar	4	1	2026	\N	Fetched high stock products	0608159e-507a-4a19-bf0e-987df33f5814
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	fbd5168a-ac0b-4e62-a3e9-fe7da09b418c
Muzaffar	4	1	2026	\N	Fetched high stock products	9bc48a07-1f29-4afc-925b-11e15b4366a2
Muzaffar	4	1	2026	\N	Fetched low stock products	ba04eed1-70ff-4c0a-89c7-7e919e501075
Muzaffar	4	1	2026	\N	Fetched low stock products	db6c6419-04c5-48a6-88c6-3e69272f4fed
Muzaffar	4	1	2026	0000	Fetched products for shop	40350266-4501-41e2-9902-4025a13fbeb7
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	5f1e224f-c69e-4674-9a8a-f89669a0668f
Muzaffar	4	1	2026	0000	Fetched products for shop	ac43cd5b-6fbe-4726-8191-f72fb3e71579
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	3eed27c0-538d-4500-bd55-ca8567ffb22e
\N	4	1	2026	0000	Sale created successfully with sales_id: 731d3045-bc6a-4689-bf64-4856448ca4d2	3fb53286-5249-4acd-a4c8-b6fe2e245e82
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	2fef0511-c950-4ac1-8eac-2d9900a3e448
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	e3153896-4178-4c35-bd96-42e61edc5898
Muzaffar	4	1	2026	\N	Fetched main finance statistics	8212a239-e212-4904-b5e3-6b40c78a2111
Muzaffar	4	1	2026	\N	Fetched main finance statistics	45af0f8e-cf93-4c43-9eb8-d0801623b444
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	902d036e-def4-461a-b17b-07da66bc820a
Muzaffar	4	1	2026	\N	Fetched high stock products	dc7e4324-8585-4ade-a0d9-92fc22dccaff
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	c41ed36e-7fd7-42a1-aebc-0407cfa5e540
Muzaffar	4	1	2026	\N	Fetched low stock products	407e0fce-3b30-4c5f-806c-db07a3f41269
Muzaffar	4	1	2026	\N	Fetched high stock products	8f172cb7-d043-4913-bce3-23747b30e637
Muzaffar	4	1	2026	\N	Fetched low stock products	bcc54918-c648-4534-8520-1d502e2bd075
Muzaffar	4	1	2026	\N	Fetched main finance statistics	5eb86400-cbe5-4d41-b917-02c251e57562
Muzaffar	4	1	2026	\N	Fetched main finance statistics	c0c4da86-d9b8-4f53-a592-2224dbbb6231
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	21ae3c7a-d0c5-4e43-9bd1-2f0d0472504b
Muzaffar	4	1	2026	\N	Fetched high stock products	f5bdd6ae-2786-4169-b784-8dd989d9d6b8
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	3cbfeaa8-e8c2-402a-b6ac-255b425932d4
Muzaffar	4	1	2026	\N	Fetched low stock products	7d684697-f1ab-43d5-9935-576d4ce99207
Muzaffar	4	1	2026	\N	Fetched high stock products	a1ebf416-4a68-4db4-9e71-30938c0d6eee
Muzaffar	4	1	2026	\N	Fetched low stock products	61c3d36b-8ca8-457b-8dd5-26c95d77be13
Muzaffar	4	1	2026	\N	Fetched main finance statistics	bd00c762-cf2a-432c-8598-c8cf70c22986
Muzaffar	4	1	2026	\N	Fetched main finance statistics	34a20508-65cf-4b72-a6ae-ee165560d731
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	36a4cd12-3b6a-4c74-963c-439928300c27
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	0fedbce8-e1bf-474b-a742-171435cbb3ce
Muzaffar	4	1	2026	\N	Fetched high stock products	3dff89ee-d7aa-47f5-b5cc-0adc80ab2a2b
Muzaffar	4	1	2026	\N	Fetched high stock products	0513be28-fd3b-445e-b67f-ec734a34c92e
Muzaffar	4	1	2026	\N	Fetched low stock products	7ff8a84f-ae2f-480e-8a3a-2b79a16401d2
Muzaffar	4	1	2026	\N	Fetched low stock products	0f836772-0e11-421b-ab55-6f20086a55b3
Muzaffar	4	1	2026	0000	Fetched products for shop	b74fe48b-b468-4ae4-a0bc-c38c8f272494
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	62b42266-1eb4-472d-8461-8d42cbfaf36a
Muzaffar	4	1	2026	0000	Fetched products for shop	533b4739-c975-4077-b4dd-2ef847e6b2d1
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	83661930-a09c-4fe9-8502-26a45d9e042c
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	c4898a55-0427-4d73-966b-70432efbc2fa
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	6ad85454-3d9e-4887-9268-4f99cc8597b2
Muzaffar	4	1	2026	0000	Fetched products for shop	0cee295e-447d-4757-b7c3-1df75fc752e8
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	06910523-cb01-461f-964e-a8690b7755bf
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	29b7e251-fd5f-4897-874b-cee0418cb9e6
Muzaffar	4	1	2026	0000	Fetched products for shop	d406de02-d0f5-455d-aafd-d980684cea67
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	dc94e192-0016-4c64-ac95-4c47ed28d3a3
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	be62c002-e683-4df0-8bef-ee9c29cfefe0
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	dda7f249-1783-4a3e-9e59-ff0e30e9adf4
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	e3935256-0497-4361-bf16-ccf956c49940
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	c83bf54c-1c63-4935-94dc-2d5384f04bf8
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	3a2321fd-87c2-4be5-a81c-335acb7ae987
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	30d8c46d-b228-46bc-8cae-6ccdcbff55fb
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	0eb34273-c95a-4912-a5b8-2bd0d686ead8
Muzaffar	4	1	2026	0000	Fetched products for shop	2e03a828-e5b4-4e22-bd89-1e1abf6f8f33
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	1fe52364-1d35-4d72-8930-8bc5a9ca63ba
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	d563660f-1227-4855-ae7b-340b3f89e211
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	19e875cf-581e-42b2-8915-32234f4ec0a3
Muzaffar	4	1	2026	0000	Fetched products for shop	11b8a03b-9fb0-44e2-a748-96313923d52d
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	a91275df-57b6-487f-a61e-d500e8d97604
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	19fbd65c-2e23-4e9a-b38a-999847eebfb0
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	9873ba77-16c6-4d51-9f1d-af0c9bb55a21
\N	4	1	2026	0000	Product created: fd3fffe6-044c-4ee3-b815-471be7dd887f	7145dcc0-2b66-41e8-ad45-07278683c5f1
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	3aa43dd0-db60-4979-b0ac-b390ec218b06
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	478ec86a-015b-4a6b-9a9a-09dae9d411ff
Muzaffar	4	1	2026	0000	Fetched debt statistics	75e574ef-e09d-48ac-8e53-5323004755b3
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	398d3fcf-eb0d-4981-af13-19852ff670fb
Muzaffar	4	1	2026	0000	Fetched debt statistics	cc975e07-3c12-4348-89dc-3a174eb3b79a
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	5d91d54c-7c62-410a-91f6-903933e0ec15
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	095ece06-e991-4695-ac8d-fc24e4a8c599
Muzaffar	4	1	2026	0000	Fetched debt statistics	3538514e-bc31-4002-bed9-238468c27162
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	c0570c86-9bd1-43c2-8761-baa0c8695ee2
Muzaffar	4	1	2026	0000	Fetched debt statistics	3588a260-8b64-49e6-9170-c04988ceb01f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	637d91ae-6026-48db-b4af-71014dcc3a18
Muzaffar	4	1	2026	0000	Fetched debt statistics	b054a238-febb-4f35-954d-da9279b06ffd
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	e7cdf319-4773-4ff4-9e65-0445c15e29a9
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	96977ce7-12f9-407e-94d7-9406e23799e3
Muzaffar	4	1	2026	0000	Fetched debt statistics	1fe53563-a833-4d8d-b2a7-57062fc455e4
Muzaffar	4	1	2026	0000	Fetched debt statistics	04004171-c054-42a7-b4ee-e9119a247137
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	6fb15642-97e1-436d-b836-695304fdcd47
Muzaffar	4	1	2026	0000	Fetched debt statistics	3ffd7f0d-95c2-414f-ab12-38705af46072
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	a30dd288-79af-4558-b843-1885cdd10704
Muzaffar	4	1	2026	0000	Fetched debt statistics	a4f2ff26-27ed-4a62-af18-e9a1cdaa1a7d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	55ec2246-3768-4b3e-970a-d53cd845a5bb
Muzaffar	4	1	2026	0000	Fetched debt statistics	72d8ecc0-93e4-467b-96d1-d31be7a05e2b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	9bb1402e-1ef5-477a-a255-35d58139a7ee
Muzaffar	4	1	2026	0000	Fetched debt statistics	d5fbb118-113f-492f-beb5-8f6b2a86efa8
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	0e749e97-62fe-476e-9ab5-e520d5786e11
Muzaffar	4	1	2026	0000	Fetched debt statistics	c544e650-28ca-4424-bb17-c09e8379b305
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	65764d32-b863-4f13-b97f-81f4626218df
Muzaffar	4	1	2026	0000	Fetched debt statistics	5a070b02-89ff-475a-bfac-97a21397fa77
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	e3dfd53b-ac7e-496f-bcbc-7c1132ee3b4b
Muzaffar	4	1	2026	0000	Fetched debt statistics	5ddaa7a4-f015-4aff-968c-27532a9ef251
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	0ab362a5-07de-4cd2-a0ed-57e8a51eebf5
Muzaffar	4	1	2026	0000	Fetched debt statistics	94290ed8-1213-485f-8335-270062b82b46
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	76675f8d-177a-4773-b74a-3fba89a202c8
Muzaffar	4	1	2026	0000	Fetched debt statistics	b8e4a502-042b-4444-9740-d59f7e3d8257
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	1bdacb0a-e356-46f0-9dcf-5e083c2c66f0
Muzaffar	4	1	2026	0000	Fetched debt statistics	a4249bc9-93d7-4afa-8f4e-697f81346fa1
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	3fa2a072-a843-4d94-8210-eaf4d281d9a4
Muzaffar	4	1	2026	0000	Fetched debt statistics	a5b578e3-3064-49b1-8db9-04051d9f2961
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	601cda35-bcd0-4c15-a16d-fc3cecf90dfe
Muzaffar	4	1	2026	0000	Fetched debt statistics	a8279829-2cd6-4d26-94e5-839449fdac0f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	2aaa4148-3330-4047-8a62-f6c6644a316f
Muzaffar	4	1	2026	0000	Fetched debt statistics	9d248f56-907c-4bc9-80cc-a1e4c04c2e18
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	fa5aa832-9e99-45ea-a5cc-3243f98c5a36
Muzaffar	4	1	2026	0000	Fetched debt statistics	1d9e8c8a-b151-49cc-8060-56bce095266c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	d8f6cff0-a910-4c91-9a45-58f426038adc
Muzaffar	4	1	2026	0000	Fetched debt statistics	bd172b94-4fb0-45cf-b69b-e5648dfd955e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	3f82bc3a-bd74-47bd-9b20-fbc5daae1d7a
Muzaffar	4	1	2026	0000	Fetched debt statistics	f363b46e-06b8-4d48-8b23-0cfb6ed662b4
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	33aa5362-d26c-4fb7-84a4-029f25878345
Muzaffar	4	1	2026	0000	Fetched debt statistics	aded5d13-3382-44cb-980b-0d8255635ea0
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	48ae63f9-d3d6-4111-98c0-7477f2ec9fca
Muzaffar	4	1	2026	0000	Fetched debt statistics	43f9e14c-f957-4afd-95d8-1443853d0a28
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	1e7e2d64-528c-44aa-b1d7-bba32b38dad0
Muzaffar	4	1	2026	0000	Fetched debt statistics	2e9dc71b-2408-49e5-ab56-e6dc8e663ea6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	bd335ba2-c01b-4ebf-b970-fb3e28884afd
Muzaffar	4	1	2026	0000	Fetched debt statistics	fa912a7e-958c-4863-a7e3-b91db6ffb388
Muzaffar	4	1	2026	0000	Fetched unreturned debts - count: 0	20e6d063-936e-447a-8991-092129350a7a
Muzaffar	4	1	2026	0000	Fetched unreturned debts - count: 0	e8cebdca-586f-4202-9b9a-dca949907774
Muzaffar	4	1	2026	0000	Fetched unreturned debts - count: 0	c97d11fc-71a2-4540-ba0e-2f7021d3c587
Muzaffar	4	1	2026	0000	Fetched unreturned debts - count: 0	9ae0bfd6-e663-44f2-9374-49539eee6edc
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 13	654bb731-603a-45dd-93e8-9e044be673c8
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 13	c2832d50-5c3d-43c7-9f70-10065ad3b43e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	32122d73-720e-418d-831c-7cc53a94783f
Muzaffar	4	1	2026	0000	Fetched debt statistics	0c92589e-848a-45ac-a4c9-84c01a7823a8
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	1cc65602-de80-4041-8167-e1ea52e9b178
Muzaffar	4	1	2026	0000	Fetched debt statistics	c541fc35-634b-4cbb-8fd4-a24af9c5a419
Muzaffar	4	1	2026	\N	Fetched debt by ID: a1d481b5-95e3-4c9c-9cb3-8d7aa415d07e	659501f8-e754-47e9-aa46-223fdbd874f6
Muzaffar	4	1	2026	\N	Fetched debt by ID: a1d481b5-95e3-4c9c-9cb3-8d7aa415d07e	dfdea6a1-8a3b-4d14-b34f-cf1311536837
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	88197abe-0f7e-4a38-ae1d-4c8c179f2c24
Muzaffar	4	1	2026	0000	Fetched debt statistics	480a81da-7a2a-4cbd-93f8-71e31cb1017e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	b19f4499-6b60-4ce0-9128-e6b44926cca3
Muzaffar	4	1	2026	0000	Fetched debt statistics	4b5f7f4e-89e8-4e1a-8d71-6ab08b3cdb24
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	432ee294-ae44-4ba7-9268-24e62d40d8c7
Muzaffar	4	1	2026	0000	Fetched debt statistics	e311a284-a8af-452b-8e8b-665bb2b07993
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	73ffecef-86dc-4390-b9f6-25b30dda8aec
Muzaffar	4	1	2026	0000	Fetched debt statistics	5c708b2e-2c17-4467-b081-b6fe23ed9fcc
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	c8bc8e1a-f152-415f-99fd-9c5b784bfa9a
Muzaffar	4	1	2026	0000	Fetched debt statistics	4f4a63f8-144a-4616-8d15-341ca8c83b3e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	6e23663b-fcc1-457b-bdcf-3a872db378ee
Muzaffar	4	1	2026	0000	Fetched debt statistics	13cc7f2d-1fcf-4023-be1a-251eb9f433c2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	7be3a6c6-c44c-4fc1-aa1e-fcc22e8a0fed
Muzaffar	4	1	2026	0000	Fetched debt statistics	91595684-abe2-4b5a-8226-3c34dfa70fa6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	d0b9a19b-2d0a-49ca-abe3-7f343c4ed7fb
Muzaffar	4	1	2026	0000	Fetched debt statistics	c057bbf2-eaa5-4f94-b955-94c6328f0c1f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	a7d8fa18-e4d4-4d5f-b17d-b43118f5a890
Muzaffar	4	1	2026	0000	Fetched debt statistics	c0697a55-1d85-4ab2-a9e5-be9c0986532f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	5cb6628d-ae2d-47e2-bba5-b1379d131e19
Muzaffar	4	1	2026	0000	Fetched debt statistics	7e08cfe1-e639-4345-8c6f-6ff83908b8dd
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	50d951e6-c455-41ed-80ab-8e920a6b5422
Muzaffar	4	1	2026	0000	Fetched debt statistics	96287f40-9a32-4da3-a3ca-0b3409ae94b5
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	68e6d2f0-0815-4644-bfcc-b7952099a03c
Muzaffar	4	1	2026	0000	Fetched debt statistics	75ac636b-6c61-48f5-be8b-ae40706d9888
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	1f8550cd-79a1-43fa-88f1-83458d16b226
Muzaffar	4	1	2026	0000	Fetched debt statistics	7241d612-0f11-4213-b50c-2ea23d31929c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	7a010ae2-9aef-4e55-99e2-ba5b3e9016b6
Muzaffar	4	1	2026	0000	Fetched debt statistics	938d7f40-f57a-4923-89a5-46b2dc55fa0e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	48f2f8c9-954e-4091-bd0f-6a7c4a56bfc3
Muzaffar	4	1	2026	0000	Fetched debt statistics	07d34433-af10-46b0-80a1-a05409ebb821
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	17b4e8fc-835a-4fa5-85ac-0ea2afe38a39
Muzaffar	4	1	2026	0000	Fetched debt statistics	2ebc32ed-e993-46da-873f-db1d2ec91550
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	81f2d600-e862-4915-a115-a0d31f808465
Muzaffar	4	1	2026	0000	Fetched debt statistics	6b9d3422-19c5-4995-b578-dc7d1c68d7cf
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	56c0beeb-2ee8-4524-a691-597fdc01a983
Muzaffar	4	1	2026	0000	Fetched debt statistics	3c3a976c-cea8-45fb-8aaf-086786e682c2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	61d4f5b2-91dc-4743-9171-7405ad4ab83b
Muzaffar	4	1	2026	0000	Fetched debt statistics	6eb9b0d3-9a48-4c31-a2dc-06dc55d3617f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	713562e9-2c4b-4aeb-b280-dc4834c333ac
Muzaffar	4	1	2026	0000	Fetched debt statistics	12824968-0961-4820-a39e-9fe99901963b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	75258c4d-12b7-49f8-b8e6-ebe826e1aef9
Muzaffar	4	1	2026	0000	Fetched debt statistics	21962c1c-9294-4e61-8c3f-f96693df419f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	3b4d125c-3ee6-4955-83a0-9d1af20ffe5a
Muzaffar	4	1	2026	0000	Fetched debt statistics	9ba335d9-3756-4414-99fb-178c69e55b58
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	424a00d1-f093-4016-9eb7-11f7b579b27e
Muzaffar	4	1	2026	0000	Fetched debt statistics	c74c65c7-e885-430b-836f-71265498e7bc
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	0193c3a2-ffa7-4ab4-a475-abed9ad99ee0
Muzaffar	4	1	2026	0000	Fetched debt statistics	6c47389f-e632-4d01-9978-2709133a1a00
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	55f7f47b-47ee-4a13-b1cf-c1028e1cd314
Muzaffar	4	1	2026	0000	Fetched debt statistics	42486f0d-0ed4-45fa-9a25-b78af0966a52
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	8dac544e-5eef-4450-8c87-6eaf77822b72
Muzaffar	4	1	2026	0000	Fetched debt statistics	45aebc98-abe8-4b68-9f6f-c1d723d56f9b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	ac2bf431-1b6e-429c-891b-25faf713a950
Muzaffar	4	1	2026	0000	Fetched debt statistics	ef51e173-6e97-4439-97c7-3bab817b1af7
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	33438643-af83-40a8-a73c-a3f67dad6342
Muzaffar	4	1	2026	0000	Fetched debt statistics	24ec6b2d-9a5c-4a36-addd-0aa608c44b25
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	a6a0aa7c-f7d1-4d75-831e-00dfeb6395e6
Muzaffar	4	1	2026	0000	Fetched debt statistics	344fe5ad-d32d-4491-83f6-1bb27ebb2a57
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	5852d56d-891f-482b-94bb-2ac9990d4850
Muzaffar	4	1	2026	0000	Fetched debt statistics	2b56df1c-0bfa-4503-97b6-3898d3f6a784
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	68da068f-76f6-4216-a052-d838e267ff83
Muzaffar	4	1	2026	0000	Fetched debt statistics	6fb86994-3bef-4f6a-b1b1-f3745784724e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	2714b78e-b9db-4037-adb5-c74e7a4e6a16
Muzaffar	4	1	2026	0000	Fetched debt statistics	f760bc9f-8bc6-426a-a27d-b44f54832672
Muzaffar	4	1	2026	0000	Fetched all debts - count: 5	ba29b1a1-b947-475c-a5e8-251786e94ddc
Muzaffar	4	1	2026	0000	Fetched debt statistics	306910d8-8f31-447f-bed5-24dd3ea82aa1
Muzaffar	4	1	2026	0000	Debt created successfully - customer: Kumush, amount: 21650.989999999998	8272bf15-2291-460b-acb0-62624fb8acba
Muzaffar	4	1	2026	0000	Fetched debt statistics	06a2771f-4919-458d-aaf0-4abff63c0bd0
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 13	eed2c0ae-36aa-45ef-89c1-360ca941e9f7
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 13	df9d937b-72f3-49ad-b305-a811f4937551
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	4d7fee14-3ca0-4252-bef2-100e9efef342
Muzaffar	4	1	2026	0000	Fetched debt statistics	ebfa6e51-b24f-401e-8a36-c6e488ab9a39
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	c6b24753-239c-4c27-b154-a64aa44c985b
Muzaffar	4	1	2026	0000	Fetched debt statistics	7e4e1605-adf4-47f7-8aa7-97d816da2fbb
Muzaffar	4	1	2026	\N	Update debt failed - error: malformed array literal: "sifhbshu545, sdfssdfsd, student"	25cfc2c6-cab3-4c78-aad0-08487bda71d0
Muzaffar	4	1	2026	\N	Update debt failed - error: malformed array literal: "sifhbshu545, sdfssdfsd, student"	b4176728-ee46-44a1-b9fc-4df77fb88928
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	0c629c16-36fb-4b9c-96b3-3639a63038a5
Muzaffar	4	1	2026	0000	Fetched debt statistics	cac08d5e-9499-4e66-aee4-0694cf894ca0
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	334177ec-b563-438c-8417-f961027f3752
Muzaffar	4	1	2026	0000	Fetched debt statistics	f0eb56cb-cf2d-4ece-99b2-7588371bfa38
Muzaffar	4	1	2026	\N	Update debt failed - error: malformed array literal: "sifhbshu545, sdfssdfsd, student"	a0eff59e-00cf-4db4-87bb-51e96e5f8333
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	4d77d61b-134f-4067-b9dc-a00643ad6b40
Muzaffar	4	1	2026	0000	Fetched debt statistics	528a5b8d-5746-4da6-a162-f698bdac19e6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	55bad59a-1a34-4e0c-be16-a9242b9289b8
Muzaffar	4	1	2026	0000	Fetched debt statistics	e36bc1e1-1e4f-41b5-bb50-0dd9deb379ab
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	31863361-a70a-47b9-8144-7e296edfcf3e
Muzaffar	4	1	2026	0000	Fetched debt statistics	8d706e5d-2002-407f-b779-052332b76952
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	42bbb90a-2d4c-4fbd-80f9-bb5fc4d717f9
Muzaffar	4	1	2026	0000	Fetched debt statistics	da000d33-cca6-467e-aa06-6c6e935b30db
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	5966f9d6-5cdc-4f35-b00a-7e0e0c6db7d9
Muzaffar	4	1	2026	0000	Fetched debt statistics	61a80c65-27c0-434c-991a-4c33e47e7679
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	752a9af5-2d20-408a-bef8-d9216488f80e
Muzaffar	4	1	2026	0000	Fetched debt statistics	6ec7e85d-e252-408e-bbf1-becba1d7d981
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	8ccaa3ab-bbb8-4bdd-b454-d4373cf7f8ce
Muzaffar	4	1	2026	0000	Fetched debt statistics	93443726-c61a-432d-9441-fba4b9aa4e95
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	35944ad2-ddb8-4f13-9a45-5aad022df364
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	c2f41064-8fc5-4ea1-835d-bc9be00de2cc
Muzaffar	4	1	2026	0000	Fetched products for shop	e5aa470e-177e-4216-ae70-ceaa59850fd5
Muzaffar	4	1	2026	0000	Fetched products for shop	aa46154e-c45f-41c4-ae1c-30bc56e3d442
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	901d45aa-a639-43dc-9000-445c7f56683c
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	088554a2-a545-48fd-a030-163569383e25
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	f014198c-3e4e-49e1-9d9f-5cba0bc71aa4
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	e1de09a4-84a8-4e14-8b78-c5fddec0ea74
Muzaffar	4	1	2026	0000	Fetched debt statistics	16fd385b-4826-46c2-acb4-a07e2b5ca7b1
Muzaffar	4	1	2026	0000	Fetched debt statistics	623d5e4f-f90d-4361-9843-eafc805e8b7c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	288dcb26-4a6f-4851-8358-79881d72ab26
Muzaffar	4	1	2026	0000	Fetched debt statistics	0e8fea08-46cf-47d0-8aca-011e289ab674
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	79beb6d4-4568-46fd-a2e1-f393a8815ad7
Muzaffar	4	1	2026	0000	Fetched debt statistics	4e678c75-10fc-49d5-a9bc-bd4acdf140c2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	914252b9-4ecc-4dfb-9cc5-1d11c8a99191
Muzaffar	4	1	2026	0000	Fetched debt statistics	1ba16b85-e2b5-4725-8941-bbd94dc01475
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	34153c39-e18a-45f1-91d5-a752e82d901f
Muzaffar	4	1	2026	0000	Fetched debt statistics	244dbbc2-4a7f-4398-9423-4dfa8b7a6de2
Muzaffar	4	1	2026	0000	Debt created successfully - customer: Kumush, amount: 12000	8ddd283d-ab6f-4e9f-9cf7-12b422a72ed4
Muzaffar	4	1	2026	0000	Fetched debt statistics	045a39be-9598-4110-aa24-35982af6d044
Muzaffar	4	1	2026	\N	Fetched debt by ID: c59a8906-516a-4967-91db-70c10ea4a1e4	d068d46a-99d5-47f5-985d-26fd1df4b3aa
Muzaffar	4	1	2026	\N	Debt updated successfully: c59a8906-516a-4967-91db-70c10ea4a1e4	398a402a-9ee1-4c36-a9d9-dbe932cb7a4c
Muzaffar	4	1	2026	0000	Fetched debt statistics	ce4668ac-c2b5-4b6f-ae2d-d746f7704ea8
Muzaffar	4	1	2026	0000	Create debt failed - error: invalid input syntax for type integer: ""	cfe7782d-d3d3-4f03-addd-95cbfef21f52
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	27a62ab7-ac58-4472-bbe1-d20090f69e66
Muzaffar	4	1	2026	0000	Fetched debt statistics	84efb8a5-f5c4-443d-b868-ff9dce9ea1e5
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	5dd9583d-f6de-48e2-ae67-e051e75414e1
Muzaffar	4	1	2026	0000	Fetched debt statistics	2261c939-8c27-4cb1-9e3e-00734b72c28e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	c89e4dec-a72e-49d2-87c6-53e313bcd81b
Muzaffar	4	1	2026	0000	Fetched debt statistics	83dedcbc-7463-4b86-bec1-1b11d47410d6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	a2078cdb-3956-4ee4-bb28-499403f984ec
Muzaffar	4	1	2026	0000	Fetched debt statistics	c955111d-feee-4eaa-b54a-a6411c21780c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	1eaad4b2-9620-4431-b83e-a1e4a047ae11
Muzaffar	4	1	2026	0000	Fetched debt statistics	d656f81b-9d26-4167-88c2-f7d07827725f
Muzaffar	4	1	2026	0000	Create debt failed - error: invalid input syntax for type integer: ""	e6b9e901-e873-41c5-a1a5-410718d147ee
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	e2d76e04-d574-42fa-a7e7-e653b6598564
Muzaffar	4	1	2026	0000	Fetched debt statistics	51317e5b-3304-4629-a637-d2a81ac8f79b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	2d5a7838-968f-4896-9c7d-224d6dc1c2e0
Muzaffar	4	1	2026	0000	Fetched debt statistics	bf9ef839-9c25-4634-9b5e-84f9c902ac8d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	942a425f-2ea7-47b2-b9b5-72d5b53a4adf
Muzaffar	4	1	2026	0000	Fetched debt statistics	40662e25-7886-402d-b40b-9ae0cd9696c6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	53846a10-3b3d-4b77-b111-0658f5b5bb26
Muzaffar	4	1	2026	0000	Fetched debt statistics	42ee78c7-6775-4a0a-a303-a65c34034a80
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	96e6121c-1273-484b-b17c-cdd183d492b5
Muzaffar	4	1	2026	0000	Fetched debt statistics	2d0bfdaa-6ed5-4d4d-89e4-12a5c28ef8a3
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	9f29f6cc-d2fa-4f28-8aa7-3ee2fe44200e
Muzaffar	4	1	2026	0000	Fetched debt statistics	c087e64d-0fe2-4718-b283-aeedb1480656
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	41a2e074-5eb3-4cce-81a8-c60f69ba19c4
Muzaffar	4	1	2026	0000	Fetched debt statistics	a041c4dc-a16e-4095-a879-5b9a6bb5767b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	f4d5139b-3b30-48f2-a0fa-a25928e82e98
Muzaffar	4	1	2026	0000	Fetched debt statistics	3cb0152b-b8c4-4944-a5ec-6bb0d94fae6c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	2ed7e837-7486-4a66-960f-3318c8c9054c
Muzaffar	4	1	2026	0000	Fetched debt statistics	7a76c30d-369f-42fe-84d3-75315a4bbf0e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	1e995f43-61af-4b05-9fd5-f665115f9e37
Muzaffar	4	1	2026	0000	Fetched debt statistics	e9f0d350-0c24-439b-8d14-70a62624bd55
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	36d49fac-00de-41da-b7bc-64bcfa641b69
Muzaffar	4	1	2026	0000	Fetched debt statistics	ed14fb8e-d728-44d8-901e-bed7c84a8877
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	911d067e-b8aa-460e-a883-8ca07326eeb9
Muzaffar	4	1	2026	0000	Fetched debt statistics	acd7ac83-5cf1-45ea-b241-d265816bad96
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	41e8f674-5e38-4119-ab12-f542ce5a8e58
Muzaffar	4	1	2026	0000	Fetched debt statistics	4c2cf060-6c49-4b6f-acbc-e61625b35f29
Muzaffar	4	1	2026	0000	Fetched all debts - count: 7	97a39264-41c4-411d-a10b-578ed71e6818
Muzaffar	4	1	2026	0000	Fetched debt statistics	57b81c8b-3775-42be-a953-e56e5dd8f096
Muzaffar	4	1	2026	0000	Debt created successfully - customer: johny, amount: 7000	2800b159-ffc0-4519-8fab-a1051c1bd3ff
Muzaffar	4	1	2026	0000	Fetched debt statistics	254f3a7a-89df-4b22-b455-a9035b34a4a4
Muzaffar	4	1	2026	0000	Fetched debts by customer: dav - count: 2	64971cc4-7ad0-438d-8728-39e994a63d6a
Muzaffar	4	1	2026	0000	Fetched debts by customer: davl - count: 2	8b3fa9b1-e38e-4d5f-8d04-7337f11eaa8b
Muzaffar	4	1	2026	0000	Fetched debts by customer: davla - count: 2	9a517aad-3e31-4e86-a2e3-fd9de61b9b2a
Muzaffar	4	1	2026	0000	Fetched debts by customer: davlat - count: 2	a3629a73-7128-43de-839b-471302610648
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	119283ce-5080-494a-9afa-6979970c3c1e
Muzaffar	4	1	2026	0000	Fetched debt statistics	62d7d5d8-c049-4344-97f6-d945a754682c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	6295fe0b-57bd-474c-908f-5436d63f06c5
Muzaffar	4	1	2026	0000	Fetched debt statistics	426b7551-12b4-4c68-a10e-3ce1789f7563
Muzaffar	4	1	2026	0000	Fetched products for shop	d00fb2df-a4ec-4c57-8278-d678f2bebbc1
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	31d2e82d-9a08-4636-ad7b-9eb461199b34
Muzaffar	4	1	2026	0000	Fetched products for shop	2fd15307-13f6-4837-8637-a6cfa86762aa
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	4bb4fcce-5729-4ba8-bdea-912a78e4ffaa
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	ac954b2a-1d16-4053-9389-4044ae58f7a2
Muzaffar	4	1	2026	0000	Fetched debt statistics	f26b23c3-f2da-4467-b776-a763468f4a86
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	bbd54719-b94c-4789-902c-b3ae8e1b5419
Muzaffar	4	1	2026	0000	Fetched debt statistics	2945076f-2153-42dd-b5e7-68291c1f72e9
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	87fa5d60-bba8-4e25-a1f4-5e98c22ea681
Muzaffar	4	1	2026	0000	Fetched debt statistics	1d119696-669e-4e17-9673-d16a6b236863
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	e0a71e5a-1df2-4669-a8f2-086ec7f7ff98
Muzaffar	4	1	2026	0000	Fetched debt statistics	c94af78f-6c8c-4653-857e-1b0596ef2ea9
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	4f84ff30-cd74-4484-b645-683d3771e748
Muzaffar	4	1	2026	0000	Fetched debt statistics	21e86e68-c488-452d-8308-fd94f990353d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	7ebaa893-07cc-483f-901e-c8f399569448
Muzaffar	4	1	2026	0000	Fetched debt statistics	f4621c94-3a56-4cb8-80d3-da7a3c723223
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	e7842c02-fae1-4887-aca9-581c56c38e78
Muzaffar	4	1	2026	0000	Fetched debt statistics	d511ceae-fe01-48b6-b67a-aa463d0f257a
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	b8aab333-c4ac-4cbd-9ab5-55ae41509376
Muzaffar	4	1	2026	0000	Fetched debt statistics	88efc205-3e3e-4abb-936b-640ccf0c6c03
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	08f7386e-3517-486a-9af3-936ae0efc49b
Muzaffar	4	1	2026	0000	Fetched debt statistics	7a91b868-cf48-4fee-b479-51b93afff1cf
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	afd7f987-fa19-4aaf-ac62-6029ef3ef615
Muzaffar	4	1	2026	0000	Fetched debt statistics	37b947be-8932-430c-9ae3-5d892d8f306b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	5b543fea-7a9c-42fa-aaa6-1d26f2530052
Muzaffar	4	1	2026	0000	Fetched debt statistics	b178101d-ddd8-4c06-93d6-6b7d4a63ba22
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	776f428c-bd64-411b-908d-15e26dd1beff
Muzaffar	4	1	2026	0000	Fetched debt statistics	0e52d032-f320-4fbe-98e2-d7dbe87de33b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	6dba0673-49f7-477c-83e8-b891f0fcd0f5
Muzaffar	4	1	2026	0000	Fetched debt statistics	d4482df6-b953-4fdf-a748-65c776c5e4aa
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	08c69c7d-1360-4867-80f4-ef7c47d54701
Muzaffar	4	1	2026	0000	Fetched debt statistics	f95bcf9b-844c-40a7-9a1f-d7911e20e843
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9816c6a7-a1c7-414d-bbe5-bdb03f59c361
Muzaffar	4	1	2026	0000	Fetched debt statistics	e6901f90-f226-4167-869b-5acfc88da6f0
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	6101b3d9-4249-49eb-9dfa-edd14c65108f
Muzaffar	4	1	2026	0000	Fetched debt statistics	6aab0e62-1566-4b86-b0fd-211356c7a9bd
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	0fd72a9f-bc71-4539-a504-6e2bd0359650
Muzaffar	4	1	2026	0000	Fetched debt statistics	27968169-3420-40a7-b593-1be25f7fb23b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	1307248c-a570-4502-be03-395b6f802d11
Muzaffar	4	1	2026	0000	Fetched debt statistics	5f15ae5e-1357-4892-b17f-ac8fec97fa20
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9469ef5c-7fa5-4cea-9495-5b8ceb5536d4
Muzaffar	4	1	2026	0000	Fetched debt statistics	1953880f-c684-498e-aa21-376283718fe4
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	c2e59eaa-3c46-4721-9da3-5ecbcf44e4bb
Muzaffar	4	1	2026	0000	Fetched debt statistics	d5a8d598-d049-44dd-b822-ca3024684aa1
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	be9446b6-dcf7-43a9-926f-33734bbaf069
Muzaffar	4	1	2026	0000	Fetched debt statistics	e9c6f0f3-27fe-4ba4-938f-84a7661f2cf2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	58394700-8b05-42a8-bbcc-8e548c41ea52
Muzaffar	4	1	2026	0000	Fetched debt statistics	e4a47aee-dcb2-4ea2-ac09-c6b6eb5b3a41
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	0459d110-3d3a-458b-94c3-880263bbe8b8
Muzaffar	4	1	2026	0000	Fetched debt statistics	b823ab11-79e4-402f-a27c-fd9902eea637
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	d2b87a20-4392-4dd9-b6a8-308afc056da5
Muzaffar	4	1	2026	0000	Fetched debt statistics	d0cc53ff-7bfe-42d5-875f-4cda8250b004
Muzaffar	4	1	2026	0000	Fetched unreturned debts - count: 3	0ea0c022-3901-4929-9242-284e6f104f1a
Muzaffar	4	1	2026	0000	Fetched unreturned debts - count: 3	a5069b1f-e2c8-4538-9508-41391f4899dc
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	f639c97b-6508-476c-9c44-510dbe41fe03
Muzaffar	4	1	2026	0000	Fetched debt statistics	54cf6b22-4488-4035-b371-fa5d4aa60016
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	cb9e4b43-663e-4240-a868-76879fd0e5da
Muzaffar	4	1	2026	0000	Fetched debt statistics	b6420a9f-87dc-4987-9929-712e17edbea9
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a2fcf0c5-fbd2-448e-a926-3addeca15d8d
Muzaffar	4	1	2026	0000	Fetched debt statistics	dcf2f505-7969-4848-9819-c5f640f18622
Muzaffar	4	1	2026	\N	Debt updated successfully: 2a2f8128-c4bf-4067-9796-261e15036109	425fc487-ef1f-4357-be55-7cd38ea9da95
Muzaffar	4	1	2026	0000	Fetched debt statistics	ff268279-957e-4c95-9593-e44040a4becf
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	e504270c-6018-46cf-92a8-b2a22ace52cc
Muzaffar	4	1	2026	0000	Fetched debt statistics	e4011ebd-0b83-49f4-a787-5aa172c0cb36
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	bd223e48-cd42-409b-a20f-5eabc74f79c3
Muzaffar	4	1	2026	0000	Fetched debt statistics	64b98d64-c925-4af0-9661-e8e0d8513b52
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	f1b63357-8fab-4214-bac4-9c49c41c48de
Muzaffar	4	1	2026	0000	Fetched debt statistics	0e2a9a26-98bb-4493-9694-ea2987d6c599
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2c50c2a9-eb31-4532-8324-c564cf116289
Muzaffar	4	1	2026	0000	Fetched debt statistics	7a65146a-334b-468c-ab80-9a8d809e69b9
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	daa3fd91-0edd-444c-84fc-679cc6c5e03b
Muzaffar	4	1	2026	0000	Fetched debt statistics	b38ce828-ca34-4efc-83ee-a6b5d4052886
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	648d1635-6d45-4f42-bf41-5e27aa486edf
Muzaffar	4	1	2026	0000	Fetched debt statistics	bfc4134e-608d-4869-8a1d-d44d27df2458
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	1fac8dd1-48c3-4abd-b0da-6778e9998a58
Muzaffar	4	1	2026	0000	Fetched debt statistics	ed36d39a-7b6a-42c5-a41b-921c23e77417
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	c871205f-73e8-4dc9-96ca-87b1bbc1bf15
Muzaffar	4	1	2026	0000	Fetched debt statistics	d49e7bbf-8f79-4ea9-b9d4-f7b3a35c6784
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	29f0a127-8a93-4aae-9f8f-16ad76862c0e
Muzaffar	4	1	2026	0000	Fetched debt statistics	5e43a169-d1ff-41e2-970d-73bebe5c73c0
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	50acb4f7-930d-4a6c-8fe5-69b87b8ad3f1
Muzaffar	4	1	2026	0000	Fetched debt statistics	a0ddf11a-ffdd-48b7-bdba-e11c938f4b06
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	42d7f6ea-c189-44cf-935e-9a6e08a6f701
Muzaffar	4	1	2026	0000	Fetched debt statistics	c4032086-1c19-496b-9043-e42b67eb9544
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	ec5a1b41-53a6-499f-8b1b-e41e3408086c
Muzaffar	4	1	2026	0000	Fetched debt statistics	03ab8e06-3d0d-4c7a-b222-72492eaf3060
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	173256b3-c937-4148-89b7-a9a71eeb3cd8
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	1814e61a-7859-4183-a8b3-ba95e3bb78eb
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	481162d1-5ae4-4c0a-9bf2-b093dfa8cb50
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	36e69278-41c7-4dbf-8205-687d925d63a0
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	7ba26849-9d85-48d3-abc0-68043c97e3de
Muzaffar	4	1	2026	0000	Fetched products for shop	d7f75f2e-59dc-4253-8841-bbc8d7b80a1e
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	d6601cc4-dedc-400f-8110-8e6467bdcca6
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	5e310e5d-9deb-423a-a2a4-4c4151196263
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	4d4f9c2c-e6c8-49b9-932c-faadb2080046
Muzaffar	4	1	2026	0000	Fetched products for shop	414adb29-221b-44d7-a814-6b70ca648df1
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	3c8dd2f6-e55b-484e-8e20-4d92b75d0bc7
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	430e59b6-18be-4bbf-9e85-61647766dd4c
Muzaffar	4	1	2026	0000	Fetched products for shop	ec6273a9-f383-4dae-926e-7c38ef37f81c
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	0cede4c0-1634-404e-bb97-c679d4829e08
Muzaffar	4	1	2026	0000	Fetched products for shop	31a147bd-6eef-4dce-9178-922755647d0d
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	10e83b60-a140-4f41-b0b3-ea664ad95fab
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	18f8d3f1-f6cb-4804-ace7-756afc65356d
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	c82f3555-624d-4087-9bf6-07c67a71e9ed
Muzaffar	4	1	2026	0000	Fetched products for shop	186b9f3c-baf6-48ff-9abe-1364d9159ac9
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	f019240f-80d1-4126-b301-9220c174816a
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	d63991cc-8065-4871-81f7-a31a82bc2f27
Muzaffar	4	1	2026	0000	Fetched products for shop	bc0e612e-c0e7-4ffa-83e7-356ac11ba1a5
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	b27d1569-627a-4a80-b57e-666f7727f291
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	d28e5c54-00c2-441d-ab7f-54c17a5c2701
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	132552bc-7347-4764-a679-d7b08256b7ea
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	e56a1e9e-40bf-4381-9150-d8077a946288
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	2a349ee0-23c3-4eff-aa61-c8fb47e18bf9
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	8fdf30bd-0ac3-4137-8377-3061bfa87ceb
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2ad61d51-6268-481c-bb8d-f8c59bfe49b2
Muzaffar	4	1	2026	0000	Fetched debt statistics	ec9adb44-f285-4fbe-92cb-8fee2d7622e7
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	f72e0e60-6c2b-4dd5-b661-51eb5a2f0088
Muzaffar	4	1	2026	0000	Fetched debt statistics	8e2c4518-745f-457d-873b-f17f76007c44
Muzaffar	4	1	2026	\N	Debt deleted successfully: ee7b9aea-9124-4d52-a885-3609cb4346f0 - customer: Kumush	fa3498ef-7c5e-4532-b2f0-c0f065dd215d
Muzaffar	4	1	2026	0000	Fetched debt statistics	22234728-675a-4796-8525-4085688124ce
Muzaffar	4	1	2026	\N	Debt deleted successfully: c59a8906-516a-4967-91db-70c10ea4a1e4 - customer: Kumush	3b44f0b2-2ee4-4d36-8605-b3bf8e2e9dfc
Muzaffar	4	1	2026	0000	Fetched debt statistics	e40e40b0-29eb-4ef0-ab76-9b42623e8467
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 13	607f93fc-5929-4b9d-969a-6af4fc53340f
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 13	a86686ef-756d-4440-853d-6d2113686071
Muzaffar	4	1	2026	0000	Fetched debt statistics	895380a4-045e-418e-8472-2e793f50b70e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	a98dd3db-8f96-4598-9956-0c8c271afe25
Muzaffar	4	1	2026	0000	Fetched all debts - count: 6	fb7562e9-fad5-4ad1-9900-89e87f28c6dd
Muzaffar	4	1	2026	0000	Fetched debt statistics	4923c2ea-fa11-4e3f-9dd2-105537891611
Muzaffar	4	1	2026	0000	Debt created successfully - customer: Salom, amount: 3000	d5dc8315-42ea-49b2-8625-ce789ba609f8
Muzaffar	4	1	2026	0000	Fetched debt statistics	a3b217d3-b0f4-4707-9d4e-4bc8c95b03ca
Muzaffar	4	1	2026	\N	Fetched debt by ID: 930b3fde-5d50-4492-ad8f-39a1c8ba767b	db54388b-880d-413d-9cd3-24baf5541693
Muzaffar	4	1	2026	\N	Debt updated successfully: 930b3fde-5d50-4492-ad8f-39a1c8ba767b	d12a21cf-f3e5-4a98-989f-2c605af94417
Muzaffar	4	1	2026	0000	Fetched debt statistics	5cd6d7ac-c5fe-4534-a2e5-d94e40dc2f7a
Muzaffar	4	1	2026	\N	Fetched debt by ID: 930b3fde-5d50-4492-ad8f-39a1c8ba767b	43d91475-58b2-4d94-af40-ff6f3bb8287b
Muzaffar	4	1	2026	0000	Debt created successfully - customer: Abdulaziz, amount: 14999.99	ca1e1556-06f6-4dc5-a60d-c52368ceb5eb
Muzaffar	4	1	2026	0000	Fetched debt statistics	91b5d538-0bfd-40bb-a817-ba9bce3a505c
Muzaffar	4	1	2026	\N	Fetched debt by ID: beb947c5-20c4-4447-9c2e-4072bee69a7e	59145873-8d5f-439d-a17d-ec7e890789c2
Muzaffar	4	1	2026	\N	Debt updated successfully: beb947c5-20c4-4447-9c2e-4072bee69a7e	8310c5e1-198c-472f-b46c-38ad7c4f0dfc
Muzaffar	4	1	2026	0000	Fetched debt statistics	177b5d79-471e-4432-956a-52f643d3f8e6
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	2ad6d21b-2d37-4a96-8f72-cad046c7ae3a
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	3d44cef2-cd7a-4a52-b2d2-2654a4659cf1
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	cd8c33b6-7ad8-438a-b131-bb54c7178d8d
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	8dff31f5-f51c-491f-b7e6-2b21dd9021de
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	c36cff91-bfd7-4333-918d-ccd1b32b5e22
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	023ec21c-52c3-4964-89c8-5f7868fd97b6
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	3852416c-358e-4d37-8097-87189e120dc4
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	715dccee-7429-433d-84e1-03b92b9c8705
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	ab994749-472b-4eb9-9f69-45da2915b853
Muzaffar	4	1	2026	0000	Fetched debt statistics	d1d0b7c8-ca6f-4c91-9aad-e847f3e6acbf
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	036dad5f-40fd-44e7-9299-a80191f1a004
Muzaffar	4	1	2026	0000	Fetched debt statistics	8b4bffb5-07ac-422b-b41c-1e04f2d54403
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	3d7f1688-c2e6-4838-8047-cb117bc33469
Muzaffar	4	1	2026	0000	Fetched debt statistics	b3292895-84e1-4ba3-847f-295873b2dbf3
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	3091b681-402a-4bdf-9a8a-a40b36a60ea2
Muzaffar	4	1	2026	0000	Fetched debt statistics	e08fa495-2d3a-482b-b699-5db3d5301132
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a31f153b-5453-437c-bd52-0518fc3eb910
Muzaffar	4	1	2026	0000	Fetched debt statistics	2dececb8-ff37-415f-9ae4-f818b86aa112
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	82056459-584d-4a44-9838-48ea1d0631b2
Muzaffar	4	1	2026	0000	Fetched debt statistics	03a72d12-94a4-4402-9e9b-be78e02487ca
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	916545b3-4ee6-4584-a952-6e9c4323ed32
Muzaffar	4	1	2026	0000	Fetched debt statistics	52780c59-00ce-4ab3-a242-c08f60cf4fa4
Muzaffar	4	1	2026	0000	Fetched debt statistics	8252b32e-1c44-45e2-803d-cfe8fade0062
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	b81dfb19-c190-49a7-80ce-15aa019c0990
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	3b5085d4-5e0a-4bad-90da-585ed07113f4
Muzaffar	4	1	2026	0000	Fetched debt statistics	30f7b2a9-94cc-4078-abc5-eccef173bf3e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	d56ecf4d-3384-467c-b652-22359a7979c0
Muzaffar	4	1	2026	0000	Fetched debt statistics	f8593253-b74d-4c06-9af2-9e6275731227
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	1f4f63e0-1d9d-4d64-a111-5e5bf50e60a4
Muzaffar	4	1	2026	0000	Fetched debt statistics	69f2e345-0e71-429a-9c4d-8527d015bc22
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	ebf601aa-9dbd-4d2e-a0bb-2cefe06a5586
Muzaffar	4	1	2026	0000	Fetched debt statistics	ea576408-400a-4949-b48c-3d627168f0e1
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	7e0a3cae-333b-4725-8bf7-62b237781c01
Muzaffar	4	1	2026	0000	Fetched debt statistics	9e62ed53-db64-4948-b87f-6f3e34b7b3f2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2c4a5e7a-4817-4182-84c5-3189f5eebd25
Muzaffar	4	1	2026	0000	Fetched debt statistics	7f1abf73-2ed7-460a-ae45-47a81304a73e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	7feeb5b8-ed6c-4f4e-b4b4-fd1059ef1096
Muzaffar	4	1	2026	0000	Fetched debt statistics	c649ff45-2c5b-40e9-9b44-112f861284ef
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	df61efbf-3274-4ef8-8d2e-c927c91b0847
Muzaffar	4	1	2026	0000	Fetched debt statistics	bfafd0bc-026b-4ca1-8ac2-c82813b72041
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9176494c-223b-4ff1-9f7b-0c214fbfa5ac
Muzaffar	4	1	2026	0000	Fetched debt statistics	7d57b0c9-9cbf-49d2-9bb9-61019c9e4581
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	29c3531b-f9ef-44d4-aa6b-ff48754523c7
Muzaffar	4	1	2026	0000	Fetched debt statistics	b9f21f53-2808-4c40-81d4-7737c92189a2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	8352e53e-a823-42c9-b7d0-72fd2aeb4852
Muzaffar	4	1	2026	0000	Fetched debt statistics	e2400aa3-c4e9-4176-8a20-3cb49355401a
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2587747f-b64a-479e-a6f9-98f3e8ce8afa
Muzaffar	4	1	2026	0000	Fetched debt statistics	7c6be024-f035-4d41-93ea-27d5283325d0
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	e8dae8fd-7286-4651-9c30-786782d624ac
Muzaffar	4	1	2026	0000	Fetched debt statistics	8e8c4561-fde6-4ecd-b6e7-05bca647e88f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	7adc2aff-3e7e-4324-8a0c-d1da98cc27e1
Muzaffar	4	1	2026	0000	Fetched debt statistics	f995036e-3ad9-4873-87f4-4cdf6fd660a6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	95c5e4eb-231f-4c99-9e78-c679c4b85f04
Muzaffar	4	1	2026	0000	Fetched debt statistics	3ed2ec40-630c-4f61-be1f-9e6df031cd31
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a7034630-0d74-4b59-ad29-6f90ee5d3b7c
Muzaffar	4	1	2026	0000	Fetched debt statistics	f02f9a2f-9b12-45cf-a117-2ae942c2d44c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	f031d2c7-030e-4368-9a4a-3204081fc969
Muzaffar	4	1	2026	0000	Fetched debt statistics	bef9fe5f-0882-4156-b20c-4d9ceba22220
Muzaffar	4	1	2026	0000	Fetched debt statistics	093847cc-8d1b-4636-855f-fbec9ed77e40
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	4ea3b75d-3ba1-458f-9a9e-7efc081c1427
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	01d23e1a-aa9f-46c0-9f6c-dcfd8e48bf24
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	4b06aa59-8f66-469f-a00b-1dc07238180e
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	af086765-2be2-4ca9-a92d-d798c73689af
Muzaffar	4	1	2026	0000	Fetched products for shop	b0474b83-8d7d-4d61-b1f4-a95eeffb0c61
Muzaffar	4	1	2026	0000	Fetched products for shop	08f94786-3125-4469-b8f6-76fceb2d664e
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	7c32ce40-75f6-4afa-8cd8-4f11bf9ab5cc
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	a19405af-55a7-4478-9021-15afe79df23e
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	bc0be7dc-f7d2-40c1-a9d7-dd67b73ad9ca
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	5759176c-9bfc-4729-b3a6-b330d089b041
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	99307374-0ceb-4cbf-910e-f8df9cc1b752
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	58a1c391-dbec-461e-b7c0-7b0a0402f902
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	5ef14113-1903-42f2-a98b-74d50bcb7f74
\N	4	1	2026	Muzaffar	Fetched sale by ID: 9313157e-9bd2-48af-b160-a9a3d1e33473	32cc3bd0-ad5c-4f59-9fc0-a4cfedad346d
Muzaffar	4	1	2026	\N	Fetched main finance statistics	96504299-0357-4e9d-a701-5a8088e58063
Muzaffar	4	1	2026	\N	Fetched main finance statistics	97747d3a-3954-423c-abe4-94af36e5047d
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	3c227e39-2efe-4524-9948-95e503785985
Muzaffar	4	1	2026	\N	Fetched high stock products	d6646fa5-6bec-4b2e-840e-d1b3591b43ae
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	78fbb863-4681-4883-8e2f-c668251df6bd
Muzaffar	4	1	2026	\N	Fetched low stock products	13cf182a-49d5-4da6-8bd1-4f0670a0aebb
Muzaffar	4	1	2026	\N	Fetched high stock products	6763cd9f-9796-4d1f-ba11-9e11757e6655
Muzaffar	4	1	2026	\N	Fetched low stock products	244e492a-a6c1-49ef-a090-2f2a83bfe8dd
Muzaffar	4	1	2026	0000	Fetched products for shop	d1193dd5-340a-4939-8c00-4ff2768ef2ed
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	a7546917-2837-4423-b06a-1bee63fbf084
Muzaffar	4	1	2026	0000	Fetched products for shop	c323c388-5191-4e96-b32d-24fa01b43efe
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	6e2bf313-72e5-4380-80b4-7b62d5ac6b81
\N	4	1	2026	Muzaffar	Failed to fetch wagons: relation "wagons" does not exist	3a45a61c-7c58-4aa0-b8ad-9e69cff1adb2
\N	4	1	2026	Muzaffar	Failed to fetch wagons: relation "wagons" does not exist	995b26ee-3ab6-4356-baca-008110712d14
\N	4	1	2026	Muzaffar	Failed to fetch wagons: relation "wagons" does not exist	91a18a3b-bd4b-4d78-9b40-bea1a2f6ed29
\N	4	1	2026	Muzaffar	Failed to fetch wagons: relation "wagons" does not exist	886a1e8c-68e9-42e1-98c0-5c7282a8e7fc
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	7634226a-d0fd-4e54-9f82-06bb279afab6
Muzaffar	4	1	2026	0000	Fetched debt statistics	1b2f8518-5ba2-4b86-92f3-4cd93c48f453
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2d88f1ea-719b-4996-8634-dea8b6ddac8c
Muzaffar	4	1	2026	0000	Fetched debt statistics	3ac674b2-faf3-437a-b0a6-d94e72b4bbd7
\N	4	1	2026	Muzaffar	Failed to fetch wagons: relation "wagons" does not exist	e059e57d-785e-468e-bafb-f7c1ba3a1980
\N	4	1	2026	Muzaffar	Failed to fetch wagons: relation "wagons" does not exist	a008c6a0-22d4-41e2-bd8e-b231354fee59
\N	4	1	2026	Muzaffar	Fetched all wagons	33385a10-20d7-472c-995e-a1894315d185
\N	4	1	2026	Muzaffar	Fetched all wagons	024d3019-9328-4ca8-ad0c-e7f35c4c12bf
\N	4	1	2026	Muzaffar	Fetched all wagons	16bf7eb8-5622-433d-bdd9-5e1db82a14b7
\N	4	1	2026	Muzaffar	Fetched all wagons	0afa04a5-2804-4b34-bf81-8470a0e3f5cb
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	5425b6b8-4ff4-4249-9a4a-e2ccd8dd6dd2
Muzaffar	4	1	2026	0000	Fetched debt statistics	be1d3566-83fc-4039-8ee4-b47e3a8d5e6c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	c77d5288-9b93-4800-8f73-5063f94f30ac
Muzaffar	4	1	2026	0000	Fetched debt statistics	63d8a0a1-8769-4322-99c2-a5f079adc94e
\N	4	1	2026	Muzaffar	Fetched all wagons	2721c63b-0563-4f56-9d2d-7f0967cacef8
\N	4	1	2026	Muzaffar	Fetched all wagons	972c6f8d-b230-4d9c-ac3d-0920dbb544b9
\N	4	1	2026	Muzaffar	Fetched all wagons	61642411-48a4-4780-bc0a-396e49e109c4
\N	4	1	2026	Muzaffar	Fetched all wagons	9a8685f3-9daf-4336-9a72-8ffa9f0ac57b
\N	4	1	2026	Muzaffar	Fetched all wagons	2cee77e6-04cb-438c-b0a2-a8e7f6dbd565
\N	4	1	2026	Muzaffar	Fetched all wagons	feba325e-1f69-41ba-abe6-c34d75c58ab0
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	b5cd2dff-8e0d-4471-9ada-1e01a73334ab
Muzaffar	4	1	2026	0000	Fetched debt statistics	48c3ab79-169e-452e-a048-fe0632caf76d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	0fd6f453-1207-45ad-979a-e8b406cba9d2
Muzaffar	4	1	2026	0000	Fetched debt statistics	a7a6ece2-7a9e-47c2-b3a6-7ef8c8a3fff7
\N	4	1	2026	Muzaffar	Fetched all wagons	02f7dde7-5a2b-4ac2-b51b-3d903152a513
\N	4	1	2026	Muzaffar	Fetched all wagons	7d29d2af-756b-4bb1-ac1c-103b4c6a5dde
\N	4	1	2026	Muzaffar	Created wagon: Guruch yangi	31dcf98b-c368-47de-9f24-b13c98a90aa7
\N	4	1	2026	Muzaffar	Fetched all wagons	d6571671-1184-45fb-96d1-ec7ab94ed597
\N	4	1	2026	Muzaffar	Fetched all wagons	bd3cafac-e508-41fa-a740-f634d9749aa9
\N	4	1	2026	Muzaffar	Fetched all wagons	e8ed943a-2593-4f10-be3c-3120ec597c1a
\N	4	1	2026	Muzaffar	Fetched all wagons	9c56ce9a-6955-4b5e-8655-cda07f777829
\N	4	1	2026	Muzaffar	Fetched all wagons	03e97c88-262e-435f-9842-47eae94c8e71
\N	4	1	2026	Muzaffar	Fetched all wagons	d72b0b6b-867d-4643-a9f7-9302259b67d2
\N	4	1	2026	Muzaffar	Fetched all wagons	07d3b6a3-13c9-4566-9cbd-f6aa114c2fb0
\N	4	1	2026	Muzaffar	Fetched all wagons	37659e12-d4b4-498f-8f18-e8d35a106ba2
\N	4	1	2026	Muzaffar	Fetched all wagons	d1577619-ff31-4c5d-8549-b97410375cc6
\N	4	1	2026	Muzaffar	Fetched all wagons	564b4c3f-4505-40ae-95c9-7f69ec0f97a7
\N	4	1	2026	Muzaffar	Fetched all wagons	4b57683a-b386-4169-92b8-e85445b4bd69
\N	4	1	2026	Muzaffar	Fetched all wagons	0bf14688-2c4a-4bd5-9d7e-49aa05602668
\N	4	1	2026	Muzaffar	Fetched all wagons	63b97cec-f91b-4b56-b956-56cde8927da5
\N	4	1	2026	Muzaffar	Fetched all wagons	9b5f735e-7b4b-4ef7-b5c3-370748c8618e
\N	4	1	2026	Muzaffar	Fetched all wagons	7a500703-79c5-4d0e-bd59-4eb02987fd7f
\N	4	1	2026	Muzaffar	Fetched all wagons	1caff535-107b-4d3d-a8f4-fea8ed5ada45
\N	4	1	2026	Muzaffar	Fetched all wagons	d64f3714-ed08-4fd7-8c4e-72c8359648d6
\N	4	1	2026	Muzaffar	Fetched all wagons	ca0fb8cc-1d06-45b3-b7a1-292b7402830c
\N	4	1	2026	Muzaffar	Fetched all wagons	da0309ae-82f1-4a47-894e-7873df5362f9
\N	4	1	2026	Muzaffar	Created wagon: Muhriddin	c88e96a7-8129-491f-969c-f58062c5eb5e
\N	4	1	2026	Muzaffar	Fetched all wagons	7f586b17-8de5-4d12-a450-be0f1b2ac742
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	69ecc51e-0591-4dc3-91d4-5a5cb461b555
Muzaffar	4	1	2026	0000	Fetched debt statistics	c2bbab24-e54a-4d74-9cf6-cd1d8e53988d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	0ddf8683-a71f-4cca-b88c-202d41e1ad29
Muzaffar	4	1	2026	0000	Fetched debt statistics	5a8ec38a-a6db-4ee6-877a-69d2be3811b6
\N	4	1	2026	Muzaffar	Fetched all wagons	1a2ca8aa-910b-4936-aa40-ba175116e68d
\N	4	1	2026	Muzaffar	Fetched all wagons	52e55a4c-4c13-4f45-809c-935e062c6514
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 14	ef30ac78-6824-445a-8fe1-4b0b93c6a989
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 14	936985b6-52eb-4ec0-9116-aa74c9d88e0d
\N	4	1	2026	Muzaffar	Fetched all wagons	3a80c764-f4bf-4bc0-987a-068ca622851e
\N	4	1	2026	Muzaffar	Fetched all wagons	6a5109e0-7eef-47e5-9d8b-42940f9e6ca5
Muzaffar	4	1	2026	0000	Fetched products for shop	4db65a68-7faa-4a84-a51d-6060f5da7f14
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	6e3f5f12-04b1-4dac-84d4-771df272ebfb
Muzaffar	4	1	2026	0000	Fetched products for shop	3d5977ba-eef6-4fc1-b83b-9afafb4e6378
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	99a3149d-4cd0-4df2-bbbf-eb59ce880f5b
Muzaffar	4	1	2026	\N	Fetched main finance statistics	50fe763a-7c4f-440d-96be-2ea6ff54eb0f
Muzaffar	4	1	2026	\N	Fetched main finance statistics	79b3a4e1-c346-4470-822f-503b293e17a9
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	9a444d62-224c-41c7-8d2f-b84ea14d901b
Muzaffar	4	1	2026	\N	Fetched high stock products	2dd6837c-56a3-4ad7-9fcb-0d027167cc39
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	ef9f12b5-dcc8-454b-bad8-74200acb4520
Muzaffar	4	1	2026	\N	Fetched high stock products	c8cceaa7-1009-4be8-8352-5d954cff2a99
Muzaffar	4	1	2026	\N	Fetched low stock products	eec132d8-127b-459a-8791-9790b98e93c4
Muzaffar	4	1	2026	\N	Fetched low stock products	7efcc101-fbce-48d8-935b-6b6b115080cb
Muzaffar	4	1	2026	0000	Fetched products for shop	56df593a-b0d3-47ef-820c-d4475b9ffcf3
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	b5d2607f-cd16-455d-bfbf-8cc3863ad28b
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	b32fe4f1-1450-4208-998c-a9fbc1041737
Muzaffar	4	1	2026	0000	Fetched products for shop	a12a1abb-59b7-4179-9800-09b8fb426b92
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	77f926f6-c2db-4421-bb23-6866f9ca586b
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	410af0ff-ac4c-4f1c-9b62-93f5fd9b4922
Muzaffar	4	1	2026	0000	Fetched products for shop	853e2016-75a2-4cfb-9125-d37ec879198a
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	02772ea4-054d-4d0f-bbc2-3011de078855
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	03964b43-cffc-4e7f-b9b0-c95f089fae4b
Muzaffar	4	1	2026	0000	Fetched products for shop	8233ab8e-57ae-423c-80b5-11f4dbed03c7
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	60284091-d3d6-41d3-bb5b-7eeb7cf38be0
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	0e280309-e09e-4714-afe4-0c6e39ccfddc
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	e4899892-a54d-47bb-9487-a43a22566a73
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	54f4e748-627b-4eac-b2c4-955356b09382
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	200f192d-ed77-4698-8333-5a361cd99612
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	3198259c-c813-4a9a-846a-707331cfdd59
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	33a04a24-a959-409b-a424-9b4820631459
Muzaffar	4	1	2026	0000	Fetched debt statistics	99753af1-48cd-488c-b726-85e59672192b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	42455b59-744f-47aa-b425-deec2bbbe4ac
Muzaffar	4	1	2026	0000	Fetched debt statistics	89659743-6217-481f-83cf-42cabc051426
\N	4	1	2026	Muzaffar	Fetched all wagons	c1b9ab0f-65bf-4996-ad8a-51769c084143
\N	4	1	2026	Muzaffar	Fetched all wagons	25615c84-31d9-4c3d-b043-c58afb6a37ba
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 14	d77b4cfb-3988-4581-863a-82bdf65468b9
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 14	328fda4d-2837-42a7-acd6-261e1f6a673b
\N	4	1	2026	Muzaffar	Fetched all wagons	1de09336-3466-479b-83c6-08079574d90e
\N	4	1	2026	Muzaffar	Fetched all wagons	a73b0d8d-f07a-43f9-a657-166da2d37c4d
Muzaffar	4	1	2026	0000	Fetched debt statistics	b95fc9e9-1c02-45f9-a3bb-5cd5cd463b4f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9f50b810-f1ea-457a-8d27-926055661442
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	eb2bd5ec-7c59-48f7-9f1d-2dfc62bfe3ec
Muzaffar	4	1	2026	0000	Fetched debt statistics	bdf4b568-f375-4151-b56b-4db05e221fe3
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	e433b589-af9a-4e2c-9882-49dcc5cc2827
Muzaffar	4	1	2026	0000	Fetched debt statistics	00cfb02f-a0cb-40f3-a50c-fc63c7ad730b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	04d37d62-6243-4451-9298-84960c27bb57
Muzaffar	4	1	2026	0000	Fetched debt statistics	a6bc72d6-d46e-4ea7-ab79-439c93ed790a
\N	4	1	2026	Muzaffar	Fetched all wagons	0a3e1bb4-d388-4b2a-803b-8a7649210524
\N	4	1	2026	Muzaffar	Fetched all wagons	1395f429-63a5-45f9-aa29-ec9645b519a3
\N	4	1	2026	Muzaffar	Created wagon: Xayrullo	13352abc-77c8-472c-84a2-6dbe33b6fcd5
\N	4	1	2026	Muzaffar	Fetched all wagons	b2475978-e485-4c36-89de-7dc11edc40e9
\N	4	1	2026	Muzaffar	Updated wagon: 87489772-c0be-4b5e-9cd6-3b32213f262d	c2f9463c-2b5d-4c89-9b01-8227a6d011fc
\N	4	1	2026	Muzaffar	Fetched all wagons	11714af6-c11e-49cd-b3a3-de505b4212d3
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 14	e3a87cd5-1630-45ab-8337-6328d323eebc
Muzaffar	4	1	2026	\N	Database backup downloaded - tables: 14	dbc446e7-c372-4bf4-b329-ead3896790b3
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a4ed64d0-c25c-4657-97bf-2fa780ec417b
Muzaffar	4	1	2026	0000	Fetched debt statistics	5971ada6-2b15-49e4-a61f-6b7ab716aed1
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	c7a3f687-d9a0-408b-b79d-a03d2e6007b0
Muzaffar	4	1	2026	0000	Fetched debt statistics	be3417f3-da26-4d27-a887-bf787097a48e
\N	4	1	2026	Muzaffar	Fetched all wagons	396b840e-4cbf-423a-8447-b5b9c658589a
\N	4	1	2026	Muzaffar	Fetched all wagons	cac81be8-0ca0-488b-89c4-45a4c60477eb
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	59dbd8d4-05ed-4469-81b8-eb46e73086e8
Muzaffar	4	1	2026	0000	Fetched debt statistics	73782ce2-f5fc-44ea-be22-9851b284338f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	0592ddc1-dd60-4a1a-b436-5b5e407f8630
Muzaffar	4	1	2026	0000	Fetched debt statistics	9db1bcab-985a-4df7-81f2-f2149dedb2b8
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	4e992abe-8fd5-4847-8aab-41f0ddcf388e
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	0b8164ba-4a92-4aca-ab1a-c292779d1b16
Muzaffar	4	1	2026	0000	Fetched products for shop	ce531b6d-341c-4edb-895d-55502727ee41
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	b0956d64-a669-414c-b059-06188e1dd5a2
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	ea5ca3a7-d9b7-4aa5-8b79-f89338847b2d
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	272d91c5-2cb3-48f9-a871-fb4721a0c985
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	62f8c3a4-4daf-4192-9ff1-acee10b70415
Muzaffar	4	1	2026	0000	Fetched products for shop	fa80ea01-e2d1-4429-aacf-a816a8265927
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	7db5bf95-acab-464b-980b-efa0e6540f4e
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	7731dc26-0a7a-4eca-9241-c224dad329e9
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	72b9b8f9-7002-40ca-81e0-509b4199df4f
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	8bfe9be8-cb18-4e4e-9f61-d31f892c7ef2
Muzaffar	4	1	2026	0000	Fetched products for shop	f3aeb2dd-e982-4537-bfaf-18f2f6312f8d
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	87356a77-5bdf-482f-9b0a-015c62e6594d
Muzaffar	4	1	2026	0000	Fetched products for shop	61df2e8f-64f0-44d5-984b-60204163e3ac
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	eaefce87-09b7-430c-81ca-1a6bdc648423
\N	4	1	2026	0000	Sale created successfully with sales_id: c4c0ca68-f406-4b16-ab25-fb737e31a505	f4978fde-1a06-425f-8fa3-0c72db99074f
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	f7f03817-857b-4e58-b9ff-1ff9ba9db01b
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	c668c392-e2ac-4216-82e5-3fa513d8d773
\N	4	1	2026	Muzaffar	Fetched sale by ID: c4c0ca68-f406-4b16-ab25-fb737e31a505	5a24a434-6b71-4042-9553-3b2d4f71f05f
\N	4	1	2026	Muzaffar	Fetched sale by ID: c4c0ca68-f406-4b16-ab25-fb737e31a505	e3b1fc40-8c88-481e-a94d-6d92756b2734
\N	4	1	2026	Muzaffar	Fetched sale by ID: 3e35a658-6812-433d-9258-28f2f564f85f	7d6c3f29-2ce8-4fa5-93cc-d01de60a8dea
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	068046fd-a131-4201-ab0c-038d4c48d97b
\N	4	1	2026	Muzaffar	Sale updated successfully: 3e35a658-6812-433d-9258-28f2f564f85f	28297e19-d751-4178-acb2-9e6228d14f99
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	e6300140-4b3e-4678-8564-6efe205ced3d
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	f77c03ce-e7f9-412f-bd4e-9259172ddf0f
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	56cdf613-bdd4-4e70-b965-99408dac5c09
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	8c28c0fd-4d82-48b5-8cd9-75bb97375688
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	cfa244a9-705b-4129-b680-765367ad2270
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	8d193cff-5fd9-4622-9a3e-e35298ea5792
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	d752d303-fea5-4792-bf80-6588256367ac
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	88bfa222-5bb1-4f46-aca0-30f29eb2a295
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	7c2814da-fcb6-4603-82c7-d3a3054f8c1d
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	c4e7c64e-4980-467d-973c-f23e2db2d18d
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	cfb86eca-00fa-44e8-a29f-b43795020dce
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	e5512ba1-e564-4576-86f4-8514dc3b9569
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	7918fffe-73df-46cc-addf-65effd9462a4
Muzaffar	4	1	2026	0000	Fetched products for shop	be361879-bc97-4332-ba95-55e896c72838
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	bd2d0678-1a0f-4138-b70d-2e8491ba9ae3
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	eb670d4f-5345-408f-828a-af091517533b
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	82f5c54e-128b-433e-9142-9cd4bc7dc048
Muzaffar	4	1	2026	0000	Fetched products for shop	3446d128-ab6f-4970-b992-736487417aa2
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	9cc9a3bf-2acc-4cf1-aa2f-0e20aa189dc4
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	ea420414-50dd-47f3-a119-07355adc7ef5
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	89de2cf5-8fbb-4a45-90c3-b35185c2c2d0
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	a79d7d4e-337b-4692-a1e6-9412418c60fb
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	fb6dbd77-5d22-4f39-8eef-6ad4800c7b2d
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	25c3c3f1-c437-47bc-86df-3059a79e49d2
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	da79131c-a805-4104-95db-622f8993c2a6
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	40f71aa4-acfc-4e5d-a912-05bcbfe05bdc
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	76d75930-c173-4e5f-949c-1ec881284164
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	27bf4e38-297f-48c9-9c13-7fa925a27249
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	56b10076-7593-456d-b691-004dc6310767
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	6a45800e-25ae-4657-90a9-ec19d4215708
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	dc3127d5-855a-4413-8242-ab04b19aa231
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	311c00f7-0c0f-4448-93bf-dc638f485fb2
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	d16acadd-b384-42bb-a25b-54f4c65346dd
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	15fe64a8-5cc5-4c86-aa60-36d67d625e7d
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	eeeee3be-72bb-41ff-bda9-4d44f4d6e549
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	3fb804f6-ce50-4fb1-9245-f862e631c94e
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	cbac758d-24c0-40ee-9d22-07c3d3226321
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	1cd7bd57-536a-49e1-8784-81020f9905f2
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	f28fb3e5-4912-4f89-8752-e9cf24c5367f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	1a05ea66-262e-490d-a380-50e3842f779e
Muzaffar	4	1	2026	0000	Fetched debt statistics	ce6d6ff7-ab45-4668-ba1b-b2615b9357ed
Muzaffar	4	1	2026	0000	Fetched debt statistics	533cb335-9377-4e36-a72e-d896e8ebd3e5
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9e69ea9e-b82e-436a-a0ba-9bf5326d59c7
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	52f04418-e70f-42a2-a3e1-52af46c83a63
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	b01acedf-4e4f-4ef4-9dde-0c4639f1202e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	44e66026-d8e1-44e9-9247-7a85895868b0
Muzaffar	4	1	2026	0000	Fetched debt statistics	9a755d0e-2714-4438-8d1d-451ae45c71d1
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	71aeae8e-2f13-45a7-b128-28645b72ca67
Muzaffar	4	1	2026	0000	Fetched debt statistics	d437334f-1349-41f8-ac57-118038c723a3
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	8f413bf0-72b8-4b53-aacb-5655c43482d8
Muzaffar	4	1	2026	0000	Fetched debt statistics	813c5e16-e991-4e86-8089-4c152c2489a9
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	bb8e19d6-3d19-4c3c-bd89-a566363d9307
Muzaffar	4	1	2026	0000	Fetched debt statistics	44f28c76-edce-45b7-a7f6-a35eac444a7a
Muzaffar	4	1	2026	0000	Fetched products for shop	dd506e45-0f39-454e-ba4e-11113da0fceb
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	db3c9f76-50b1-4fb2-826f-48483b237af3
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	c6f2282b-fc6a-4824-988d-41770789a657
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	79d708e1-30e4-4e54-aecc-b54854d58a21
Muzaffar	4	1	2026	0000	Fetched products for shop	77994529-f1eb-4923-8363-b454da876b50
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	742c0976-9d22-4bca-b3c2-c45db7ee7f87
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	3b1b3026-2f98-4a1a-81f2-099eaaffae9f
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	f044340e-3fb5-41aa-942a-9bcefd22818c
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	727aa3fe-e351-4c53-9243-f22c4e5b9065
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	3acd0255-1a66-4a8f-a834-d5fecc33171e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	41d4b004-64b9-4498-a7d3-bf4d3801ffb2
Muzaffar	4	1	2026	0000	Fetched debt statistics	b1c78bea-5ed2-468c-a076-d817e2e9a4f6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	831ae49e-a6b0-4d91-87f6-ad9d1c07c634
Muzaffar	4	1	2026	0000	Fetched debt statistics	bafc3313-abc9-4922-b85e-5ce1b8645b91
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	e81c59d1-0dea-4f55-a4dd-edbf3cfa5f23
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	2f4f5e2b-b29e-423e-a581-23eb439d6ec6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	11b8196e-987e-4dab-ae58-a4dc8fe2e017
Muzaffar	4	1	2026	0000	Fetched debt statistics	3509298f-b3bb-468c-8e67-0444e0ee5bba
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2b58bb4d-146a-4fb2-ba66-e49f6d18410b
Muzaffar	4	1	2026	0000	Fetched debt statistics	cc1e4ade-18df-47eb-9eda-7e26881ca3b5
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	1b2da8be-e432-413a-be0f-f6f7591cc423
Muzaffar	4	1	2026	0000	Fetched debt statistics	4bb86e4f-efe6-41f0-a3f1-45787b3a982f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	ae55c277-1dad-4a0c-b5bf-3311cc710e7a
Muzaffar	4	1	2026	0000	Fetched debt statistics	c4063703-b252-4fc0-8560-1887a1c58931
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	ca4c1da9-9c61-4d7e-9171-a0f9b7c1acfd
Muzaffar	4	1	2026	0000	Fetched debt statistics	ca571d3c-438c-4f2f-b2b5-e21339588621
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	8f3e24c7-f014-4669-82cc-d962a0d010bc
Muzaffar	4	1	2026	0000	Fetched debt statistics	3093319c-83ed-429f-be15-a8be6336993e
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	e0bdba2e-b01f-4a79-97de-5caf437aa9e0
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	794bb5d9-9c94-4a91-bca7-140c9c0a064e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	fb80e33a-7d4b-4d06-bbfa-3c8361092240
Muzaffar	4	1	2026	0000	Fetched debt statistics	49fcb347-7223-40d0-9b3c-db15b54aec15
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	771202e3-0f51-4d21-b0e6-6cbfb14bdf6c
Muzaffar	4	1	2026	0000	Fetched debt statistics	09b76597-fa51-4491-a036-a8bdb7436260
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	d1f287b3-a1bb-4570-a680-7d8bb7352abb
Muzaffar	4	1	2026	0000	Fetched debt statistics	0fecca27-09ad-44fa-b3e2-6211db176da1
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	b974e9ad-9271-4489-93de-7278f98564f2
Muzaffar	4	1	2026	0000	Fetched debt statistics	c791b35e-3f1c-4440-ac61-e3cb143d36e2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	b1228fd2-d0cb-4b9d-bea1-a4a740e9efff
Muzaffar	4	1	2026	0000	Fetched debt statistics	93975294-5039-4952-aa50-986c18504c67
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	4dcc8b87-abf1-4341-87a1-2b81eb93d581
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	d08fdcf0-9dc7-4774-9ddc-dca70ca59793
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	e3d98c5a-4807-4c63-ab2c-0b1700511ced
Muzaffar	4	1	2026	0000	Fetched debt statistics	768685eb-0e87-44eb-8db2-cd75acdaf542
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	1e86aaa6-eddf-4df6-a0d1-bef5755514c9
Muzaffar	4	1	2026	0000	Fetched debt statistics	f3ea9ba5-2379-4bae-be3a-83bc329b83b4
\N	4	1	2026	Muzaffar	Fetched all wagons	34fd7cd0-2965-4e7b-82a4-2d23d32e60b6
\N	4	1	2026	Muzaffar	Fetched all wagons	1a594e40-1289-43e5-9c71-c180e1509feb
\N	4	1	2026	Muzaffar	Fetched all wagons	0c7f3c8f-c9c8-4b19-8485-02809f38166d
\N	4	1	2026	Muzaffar	Fetched all wagons	3787c623-f4db-40e0-bac8-a49935bc86ba
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	1f65a2ae-8488-4045-a5bd-4814b2ff7139
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	c15f80b8-e3d8-407d-bae8-c24267a4b77d
\N	4	1	2026	Muzaffar	Fetched all wagons	d2a6657f-fcdd-45ac-ac38-7941ee03673c
\N	4	1	2026	Muzaffar	Fetched all wagons	8d2b7890-dcea-4775-bb17-f1091ea6df0a
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	3e131808-9181-4f12-85a5-ebc85fe4be72
Muzaffar	4	1	2026	0000	Fetched debt statistics	160cdd21-5417-48d0-9302-de79d11999d2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	44ee5c94-eebe-4efd-af93-448d7726b4cc
Muzaffar	4	1	2026	0000	Fetched debt statistics	fed046f2-7fd6-470a-b0f5-72e0d587c60d
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	95b2db8c-a9ad-4ec4-b16e-731ad22cfbbf
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	1cedfd8c-0613-4ce7-abbd-24c94e0f4643
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	8473ff12-333c-45d8-9b33-4f3178fece28
Muzaffar	4	1	2026	0000	Fetched debt statistics	835c1828-0613-4825-ad96-f9c0a8f0fe26
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	ed8c6227-b760-4521-898e-891fd0a35196
Muzaffar	4	1	2026	0000	Fetched debt statistics	ab473ca8-635e-4b57-abd1-48a8531102b8
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	db77e83b-5b2a-44b2-9998-f8a729cf1a6b
Muzaffar	4	1	2026	0000	Fetched debt statistics	0dfb6b73-3fbd-4009-8c54-b6b353a05a5f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	49ccfbdf-17f1-487f-bad7-f30607f5e340
Muzaffar	4	1	2026	0000	Fetched debt statistics	c24bcff6-c2e3-452d-bdff-ba1fcb034dbb
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	0d82b86b-c262-4c96-b707-4aa47e29335c
Muzaffar	4	1	2026	0000	Fetched debt statistics	d1468a9c-b5f0-4c9e-ad9f-e5bcf2847f4b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a6b6b386-89ab-4c5e-9bd6-f25ed4360658
Muzaffar	4	1	2026	0000	Fetched debt statistics	e083453b-3137-4afd-8ac4-7bdfc1e46a49
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	210400d8-092a-48c3-93d1-19b0f06b9d4f
Muzaffar	4	1	2026	0000	Fetched debt statistics	130d6921-fce9-47dc-810c-d63f7f4a30ce
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	ef9760bd-d0c3-4fb0-a89a-54ce8726973f
Muzaffar	4	1	2026	0000	Fetched debt statistics	7bbb74b6-7e57-4429-98cf-3a894159edcf
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	76864546-1c81-4292-a2a7-9a337c69f814
Muzaffar	4	1	2026	0000	Fetched debt statistics	4f16d46f-f6ad-4408-a9a5-bdd1ff017dc6
Muzaffar	4	1	2026	\N	Debt updated successfully: 2a2f8128-c4bf-4067-9796-261e15036109	4fc4d389-587c-4155-a7d8-dbd5505108db
Muzaffar	4	1	2026	0000	Fetched debt statistics	d87512ea-6bdf-4385-b8ca-ab244ab5421d
\N	4	1	2026	Muzaffar	Fetched all wagons	d6c3ccd8-d7a2-4fb7-b624-6eabca407da4
\N	4	1	2026	Muzaffar	Fetched all wagons	b1a73916-883b-41d8-9902-4e75409d7515
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	20cbff6c-7a2a-45e4-86ba-271497f956d7
Muzaffar	4	1	2026	0000	Fetched debt statistics	c73eac43-3744-496e-82c2-c08be3a0ea0b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	72ab3ef7-8475-4827-9eb2-a85e0e34cbd8
Muzaffar	4	1	2026	0000	Fetched debt statistics	8e5a19c7-74be-4037-8c83-7b4d001919f9
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a17f22be-7dd5-498a-853e-96b60996991b
Muzaffar	4	1	2026	0000	Fetched debt statistics	77ac01bd-6ea4-487b-986b-391b448b6b7b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	b12b7445-34f0-458a-9e1c-7396153e15ca
Muzaffar	4	1	2026	0000	Fetched debt statistics	308565a9-831b-4fc7-908a-bc869fcfa047
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	e2e12172-ea28-4750-9a2d-347f979797b4
Muzaffar	4	1	2026	0000	Fetched debt statistics	869fc439-c79f-4b90-a96a-9871dde7c943
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	6a6a06e4-2812-4aea-9f56-5c26cfdfabd2
Muzaffar	4	1	2026	0000	Fetched debt statistics	c794d2bc-0f15-4970-b203-2ad12f842b98
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	06f87e51-9944-4348-904c-3e7cc9577df0
Muzaffar	4	1	2026	0000	Fetched debt statistics	239df280-8fa9-45f6-844e-db2755d54f51
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a83b40c5-9d62-448c-8b8e-a6b25e710e33
Muzaffar	4	1	2026	0000	Fetched debt statistics	74db3a4d-7596-4934-a1fc-14b92806d633
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	79754e0c-85a1-4f1b-8b84-0606aab99ed8
Muzaffar	4	1	2026	0000	Fetched debt statistics	3763a648-09d3-476a-a39f-9819510b4050
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	49ed603f-7991-4dbf-a58e-a2a436417436
Muzaffar	4	1	2026	0000	Fetched debt statistics	a3ec4459-f9fb-4c31-af22-f31174c1b18b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9f5ebfc1-7fbc-4bbd-8ab0-32112ac4dfa9
Muzaffar	4	1	2026	0000	Fetched debt statistics	05f1203a-1161-4524-a3cf-0a015d5d8711
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	3bcf3bac-295c-4731-a35f-17f146ecf69a
Muzaffar	4	1	2026	0000	Fetched debt statistics	8a1fd549-01d9-47cb-bc1b-da5c727ed463
Muzaffar	4	1	2026	0000	Fetched debt statistics	1ea19e1c-44a1-4703-8f1d-7016808fa723
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	e75c1f62-7ff1-4328-a2c5-d19f03acca51
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	44630bda-262f-4597-9d72-2fdfb54ca79b
Muzaffar	4	1	2026	0000	Fetched debt statistics	77f9611d-e9d1-4a74-82f6-c7c164e2b61b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	fe88f94e-af0e-4299-a274-9fd84d000213
Muzaffar	4	1	2026	0000	Fetched debt statistics	0ce95845-27b9-42c3-83c1-24ffdf4e700a
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	df84dd8e-1926-46f2-9b74-cddd8d02093d
Muzaffar	4	1	2026	0000	Fetched debt statistics	7a77654c-6be2-4325-baf9-d17d34cef200
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a4ec35c6-4d5a-474c-b1b4-1583501189b4
Muzaffar	4	1	2026	0000	Fetched debt statistics	68e8a7b7-c263-458f-8434-992267c13c9d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	03cb10df-1e01-44e5-976e-5b8493af224a
Muzaffar	4	1	2026	0000	Fetched debt statistics	6feb11c9-f94f-4d77-a51f-92c750985ffd
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	f1e5c3d2-4004-4870-aa49-1d65b5fe2dd9
Muzaffar	4	1	2026	0000	Fetched debt statistics	b64078ce-affb-43ba-90d1-c31c67f0d01c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a31544bf-ca4e-4a7e-ae7b-d805cd261d93
Muzaffar	4	1	2026	0000	Fetched debt statistics	56b92f4b-0945-48b2-bf62-5a6b75c4da57
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a4409007-4757-4451-96b7-8d9563ca6c1b
Muzaffar	4	1	2026	0000	Fetched debt statistics	4a4c1c89-38c7-4aa8-9717-7450b6c36ada
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	3d94ac7e-9030-4985-8a3e-6b6972009ca7
Muzaffar	4	1	2026	0000	Fetched debt statistics	fc4613d1-3f15-4d9a-a5c3-bfd5249ad570
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	80123c63-f290-498c-b72c-17220882da7d
Muzaffar	4	1	2026	0000	Fetched debt statistics	5df35255-6d6e-4d18-9e07-d5f4003f186b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	bd0c72cc-0864-4744-b532-8fe53b1b425c
Muzaffar	4	1	2026	0000	Fetched debt statistics	fe7f3737-e7a3-4ef9-a338-879a90072899
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	c24b6eb4-523f-4774-b910-619569165790
Muzaffar	4	1	2026	0000	Fetched debt statistics	e99304c3-ffe1-44ff-ae1d-52df07ae2a88
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2c42c5a7-147e-47a4-863a-ef81b1187d67
Muzaffar	4	1	2026	0000	Fetched debt statistics	f8891582-8d82-4192-8bc9-388992a2696f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9dbd98b3-0338-4361-954b-24cfc9b351b0
Muzaffar	4	1	2026	0000	Fetched debt statistics	388295d7-6a4e-43b3-a8fc-b1222147eba6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	5d05762a-036d-43dc-953d-de847008396e
Muzaffar	4	1	2026	0000	Fetched debt statistics	7e7e3d80-d7f2-4bb7-86cc-6d57ea6032f9
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	b25f3b93-45da-4b9e-80bd-e05af8d1a477
Muzaffar	4	1	2026	0000	Fetched debt statistics	fcbd383b-acbe-4e56-8a4d-67c5f322ee99
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9d34d661-aeaf-425d-a4da-9bdabaf61baa
Muzaffar	4	1	2026	0000	Fetched debt statistics	185d2845-0367-470e-b4e0-bf453ddc893f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2829fdbc-89ab-4efb-9cf0-9b4ee298e347
Muzaffar	4	1	2026	0000	Fetched debt statistics	e60abeb2-9e06-431f-b442-e328786fbc21
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2c9c65f8-ad6e-4961-a6d7-8d88155ecf72
Muzaffar	4	1	2026	0000	Fetched debt statistics	83fbd0d1-42f7-4c71-bfe7-3a2d2f43b268
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	934b82e7-f29f-401d-82b2-78769b6e17c2
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	8f72b7d5-8183-4fad-8d94-383d26f7fb28
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	560f7412-8f10-40b4-ad56-200765313b1f
Muzaffar	4	1	2026	0000	Fetched debt statistics	92bb515c-8afd-480c-873c-0ecc93096ec8
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9eb7bb37-a445-4f00-96d7-d1e4b83508a1
Muzaffar	4	1	2026	0000	Fetched debt statistics	a262b29c-159b-4fb2-a651-8c623c649e41
Muzaffar	4	1	2026	\N	Fetched debt by ID: 930b3fde-5d50-4492-ad8f-39a1c8ba767b	551dfad3-06fc-4221-86a0-b49b51c0ee43
Muzaffar	4	1	2026	\N	Debt updated successfully: 930b3fde-5d50-4492-ad8f-39a1c8ba767b	3406d0ff-a57c-45f3-9954-4778b18b2e15
Muzaffar	4	1	2026	0000	Fetched debt statistics	b9b3046d-0924-44c2-9b45-1e4dd3387b4c
\N	4	1	2026	Muzaffar	Fetched all wagons	bd275240-99db-40b3-956b-ba305030e546
\N	4	1	2026	Muzaffar	Fetched all wagons	28c83eee-9b4f-4765-b90a-cd2f5a3f10cb
\N	4	1	2026	Muzaffar	Fetched all wagons	b5e14033-d6d8-4430-a977-15de5536a697
\N	4	1	2026	Muzaffar	Fetched all wagons	763a1d0c-e793-48b4-85f6-ed63f0fcd50c
\N	4	1	2026	Muzaffar	Fetched all wagons	bf815f92-bd1f-4757-b5b7-762603fe2fb4
\N	4	1	2026	Muzaffar	Fetched all wagons	33e9855f-bb48-473d-9aa9-62f19997d614
\N	4	1	2026	Muzaffar	Fetched all wagons	56d478c6-b21e-4ac5-89b2-a1ab2e4ee77b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	07d39348-7668-4d5e-851e-0d61becfb77d
Muzaffar	4	1	2026	0000	Fetched debt statistics	784ca7bf-6e46-4f4e-8aee-e0342d618d4e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	d8c422ca-e2da-444c-9b67-37628011367b
Muzaffar	4	1	2026	0000	Fetched debt statistics	f13290d7-f4a2-4f2d-a88e-4ea640bcc87f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a04858e4-5b17-4e0b-b890-a669f2a118c9
Muzaffar	4	1	2026	0000	Fetched debt statistics	751e0eeb-7520-414e-862f-abd524f0a367
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	f51bdf10-c1b5-4f41-b5d5-4c0be256b7db
Muzaffar	4	1	2026	0000	Fetched debt statistics	b099cd63-3534-456a-b1b0-40bcd019254b
Muzaffar	4	1	2026	0000	Fetched products for shop	b5da28fb-526d-4a90-a323-8e16d6b2a235
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	68ddeeb5-1f6b-4d8e-a44c-c09001edef31
Muzaffar	4	1	2026	0000	Fetched products for shop	214198e4-59c3-45d5-b519-8b416278067a
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	93d70bbe-75de-404d-bf26-348ad61be21b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	3f2796f4-d58d-4bca-b798-ffb38d6c94a6
Muzaffar	4	1	2026	0000	Fetched debt statistics	40173fb7-50d4-40d5-ac2b-ce36a767a5b1
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	cfe32246-8063-4696-83c2-f6af17287794
Muzaffar	4	1	2026	0000	Fetched debt statistics	9383b3bb-d5e5-4cee-b6a4-6f77734a94fb
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	f55c4a96-0d1c-4956-afa2-5696cafebdc3
Muzaffar	4	1	2026	0000	Fetched debt statistics	17ddf1d8-bc17-4d77-9f3b-53989bbc5882
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	4ead96cb-d944-4e4d-b8a6-922e9d09e9b6
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	14578a73-23a9-406d-a35f-c1074dbd3bfb
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	a3b8406f-5eab-46d9-ba6f-3670e78b221d
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	bdd72586-d0b1-4ef2-b897-a751a31cd704
Muzaffar	4	1	2026	0000	Fetched products for shop	fdb10121-a8af-4b90-8721-a495b008ff96
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	abf7140c-8dab-4327-a6b6-76fcf82d307a
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	627fdc61-63af-4017-a0c4-654a76b0c7e6
Muzaffar	4	1	2026	0000	Fetched products for shop	e8b2b3ef-4d79-4982-9f1d-daec58f139c6
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	143cad44-5dc9-4f5f-8ebf-7181fa343126
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	71871532-a2d9-439a-84e0-653941af20c5
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	c503fe4d-0a9c-40f5-9918-4bc8a0532add
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	4715c3e9-9d56-4fad-bca2-dd70d9858a68
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	d1909c9e-13b4-49b4-afe0-0d4a1e9ffb7a
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	cd32ccce-251a-4212-b21b-62a5239759f5
Muzaffar	4	1	2026	\N	Fetched main finance statistics	9f8480ed-f481-4901-9c70-cf0f219c3f41
Muzaffar	4	1	2026	\N	Fetched main finance statistics	3e48e815-fa0f-44ca-ba56-9fdbc7ac5390
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	598a9225-ca67-4ebf-8d6e-e309c6b62b6f
Muzaffar	4	1	2026	\N	Fetched high stock products	ddcb2bcf-7053-4d8f-871f-b4e28e5ef544
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	f8158459-46f8-442e-961b-3a58fcf7d591
Muzaffar	4	1	2026	\N	Fetched high stock products	ac65675a-2807-4993-8522-c1a57a862f5e
Muzaffar	4	1	2026	\N	Fetched low stock products	e14be398-46c5-487f-948e-8e96d0bdfd94
Muzaffar	4	1	2026	\N	Fetched low stock products	3b6af54e-a372-4283-bede-84eaf43f1a3d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	24d00033-1bce-4a01-8b37-469414e9cbfa
Muzaffar	4	1	2026	0000	Fetched debt statistics	e58b2b91-b954-41fc-ab9e-431bc5b00cd3
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a210b9d3-90b1-4eb6-b543-455861019a4a
Muzaffar	4	1	2026	0000	Fetched debt statistics	6e15629c-ec32-4590-8753-7a3105633a9b
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	bad78a08-c4cc-48ea-a227-dacfccfb0bbb
Muzaffar	4	1	2026	0000	Fetched debt statistics	b6ffe265-e9cc-4814-9b5a-350fe640c7d9
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a69a86a3-b400-4449-b527-66da01cb12d3
Muzaffar	4	1	2026	0000	Fetched debt statistics	a99a58e3-7ff9-42e0-bbfe-d6568a66d850
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	54edb6b9-1e53-41cd-991a-7d1acd244ea3
Muzaffar	4	1	2026	0000	Fetched debt statistics	94e5e735-1681-4b29-b50c-1600ba9a5642
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	b8b24f6d-8f56-4fb6-a3c1-058a58022ef0
Muzaffar	4	1	2026	0000	Fetched debt statistics	b3b7c040-c564-44d6-b252-f263b9049b95
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	d5ddbe64-2ae6-49d1-b4b5-a1a98a7eef0c
Muzaffar	4	1	2026	0000	Fetched debt statistics	4babc01d-b2a9-4b50-a99b-abc8fa8fd626
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	c53418fd-ae11-4dfc-a51c-aba55a4443c9
Muzaffar	4	1	2026	0000	Fetched debt statistics	93715bff-4b7e-4de8-9b13-097d983b3f67
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	4351cd7f-bdd3-498e-98e9-71f9ca221431
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	df522a64-8b14-4f50-aed8-53da5baa5017
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	de3383d6-681a-41f5-9d80-8e7ebbcb6a4f
Muzaffar	4	1	2026	0000	Fetched debt statistics	ac66d19a-e538-492c-ac01-a88d14126fb2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	f532f9ee-cefd-4e9f-9cc1-80743859dea0
Muzaffar	4	1	2026	0000	Fetched debt statistics	fae98225-4185-4bda-abc1-5f5eb9b81b42
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	2c17f5a1-e44b-4794-8413-ab4b979d2004
Muzaffar	4	1	2026	0000	Fetched products for shop	dde39cb0-d6b0-4913-8c40-835fc8c20736
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	cc07d07f-84f5-439f-8345-f2e19b5848d1
Muzaffar	4	1	2026	0000	Fetched products for shop	f7c9c2b1-becd-47b5-acbf-4b66e92f5895
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	93be5512-5fd8-4812-9c43-c82250ea65e6
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	ffaf82a9-c81f-4922-936a-4c06c4a30832
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	5d505738-e5c8-44b8-be03-402b9b8790a6
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	ae8b5101-f64c-4387-bfa0-42a6ce6be80f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2bda5833-af63-4922-8f73-9d196d6d7146
Muzaffar	4	1	2026	0000	Fetched debt statistics	8089d9dc-7f1a-44a2-b280-87b6de6703da
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	7fac771d-05df-4bc7-99ea-e00b5568d4d6
Muzaffar	4	1	2026	0000	Fetched debt statistics	209cad64-adf6-42a8-9c1a-109ed5125bcc
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	8b0799db-9772-470b-8b0e-681259a4df6d
Muzaffar	4	1	2026	0000	Fetched debt statistics	5cedda8f-8651-4781-bf96-ad0772d6c8c8
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9f6b683a-5a5d-4a3e-af1c-8a10a1c5718b
Muzaffar	4	1	2026	0000	Fetched debt statistics	bf1f4fa5-4fd3-4405-9217-bee9f996ac29
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	88dfe782-c9c5-4ff9-ba28-d9393bfc46ee
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	70b31519-a9c8-403a-880a-6ae91a576a53
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	8eace9be-897a-43e9-829e-d8b51900ea6c
Muzaffar	4	1	2026	0000	Fetched debt statistics	2f7b5197-a055-4cdd-97ca-0caa1f9737ed
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	cb5fcd95-5899-4ad0-bcc2-f7dcb48a16a6
Muzaffar	4	1	2026	0000	Fetched debt statistics	2f452cd4-732b-446c-8f4b-43b777f5ea4d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	1ecab5de-903c-44c2-9b50-af9d5b175e13
Muzaffar	4	1	2026	0000	Fetched debt statistics	de919684-927e-4691-adf3-57b0df98d858
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	ba48bb6f-fe6c-4fe7-b266-2f012b9b694d
Muzaffar	4	1	2026	0000	Fetched debt statistics	6143a671-5c10-4e26-9633-ae419742b3d3
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	fa1fe0f0-75a4-48bb-b0a6-c63ebce90625
Muzaffar	4	1	2026	0000	Fetched debt statistics	0e3a82a7-6a20-4d69-a977-23ce923f489f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	c8532447-e687-4941-a07e-98134733590a
Muzaffar	4	1	2026	0000	Fetched debt statistics	7996b2da-a1e3-4773-8406-976bafba93a6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	7a1cdb84-d80a-4150-8160-98ad2da8d83d
Muzaffar	4	1	2026	0000	Fetched debt statistics	67228e74-d85b-473c-abc7-15b9f4304468
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	5b56f326-a684-4dc2-b0ec-00e43e44e1b3
Muzaffar	4	1	2026	0000	Fetched debt statistics	a6de11d0-b310-40e6-a7d6-5da43822cf98
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	19d5c9f3-8832-45eb-a0b6-b34e0df7b9fb
Muzaffar	4	1	2026	0000	Fetched debt statistics	6d9faaba-c60b-4df1-af34-d7428074c082
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	01d09ba5-842c-492a-b232-e0225e0834b7
Muzaffar	4	1	2026	0000	Fetched debt statistics	2c82adcc-2ea6-4aad-a986-eac4eac1d17c
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	5944445e-9baf-4535-8bdb-3895fd040264
Muzaffar	4	1	2026	0000	Fetched debt statistics	f606e386-dd8d-4f3f-97b4-84148fdb1ea8
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	16633e48-aeda-43ec-a6ab-7c23de8027e7
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	7fa63439-f528-4159-aaad-8357766f9209
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	9f791b15-b34e-4729-ad64-09d4c54c859f
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	43d388a2-a180-430e-ba19-8bbab7f6024a
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	a2219c79-dc83-4ea6-ae98-d1d8dd58290f
Muzaffar	4	1	2026	0000	Fetched products for shop	c530af29-cb0d-4f0d-bfa6-3f44ebe842a0
Muzaffar	4	1	2026	0000	Fetched products for shop	ae16b758-977b-489b-ab93-1e3c011cb93b
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	3212d548-f3f7-40ad-aeff-4087094d29b3
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	ba6ea9a1-9123-4005-a6a8-9e059cf3d1cc
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	0009908e-8b48-4b9d-b538-978f72a848ce
Muzaffar	4	1	2026	0000	Fetched products for shop	946f1975-2b64-447a-bba5-01dd093c758b
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	60718004-433b-41ec-97c4-b6157214439f
Muzaffar	4	1	2026	0000	Fetched products for shop	34657e6b-bbd8-4a68-800c-5bba93c6af4f
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	a9243437-35d6-4028-8ea3-6eb3073002c9
\N	4	1	2026	0000	Sale created successfully with sales_id: f3d924a2-f918-4c27-84a5-524573cacd9d	75122736-801e-4c28-ad92-cf4125eac707
\N	4	1	2026	0000	Insufficient stock for product 'Lazer guruch'	e92527f8-a60b-4bcc-89eb-f8f880cd0155
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	ba6724c5-c497-49d0-b0e6-6f40080060cd
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	b0dafc8f-6233-48d2-9ff6-f101248ae762
Muzaffar	4	1	2026	0000	Fetched products for shop	39778130-a2cd-4866-8e16-9963db507ad2
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	e8cefc59-985e-478c-92d1-f70845e7d90a
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	b93aa331-e65e-4504-9c92-24a4ef735df1
Muzaffar	4	1	2026	0000	Fetched products for shop	c3e4fa22-5a0e-491a-99c9-10ee4afa3428
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	5b325554-41ec-497b-b5a7-bed1bcb55c0e
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	7312f642-34b7-43c2-aa8b-ee65531e190a
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	a36b33c6-51b9-4889-a85d-cd59830362b4
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	e66204ec-abc2-4d0d-ba2c-78b76688afe4
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	ba0efa12-4905-402a-96dc-8c8f459e7817
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	a7503211-328d-4d93-a41c-e0c1a6f5337c
Muzaffar	4	1	2026	0000	Fetched products for shop	a5197430-ab12-4893-a033-7319a6751f35
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	f8562128-16ce-4a8c-9b08-a2c74f8b40df
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	fa13de7d-d4fd-4943-a4cc-dca9d28243a7
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	628cc8e6-0433-4f40-a035-37c8adfd43c2
Muzaffar	4	1	2026	0000	Fetched products for shop	48dbae6c-b769-44de-9678-b0bb78967281
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	1c46a9df-8b0e-455a-bdfc-6fcf417676e6
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	40a2ef46-46a5-4553-974a-baf377c8fe86
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	8b2e44ef-e04a-4a65-8563-518eeb174bea
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	9861b83f-2df8-4f38-8880-d38bb90037bd
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	9f0c8a87-d504-4336-b022-fdc9ce053247
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	6cdf8414-67bc-4364-a38c-25f08cd382b5
Muzaffar	4	1	2026	0000	Fetched debt statistics	c3d5753d-fbb8-42b5-af82-13b05f1fffea
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	a34f6595-4894-4a0a-a223-bd39d3e2ce75
Muzaffar	4	1	2026	0000	Fetched debt statistics	1427b3a5-6529-4f9e-9223-36aabd7dce3a
\N	4	1	2026	Muzaffar	Fetched all wagons	25c735ca-de67-465c-8929-7f248a721530
\N	4	1	2026	Muzaffar	Fetched all wagons	679a07e2-9a55-42bb-a570-cd75fb350b43
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	26d48e94-2fd3-4b74-b0b3-4f742295544c
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	0e13b980-df7a-40e0-ab7c-b1f10d690f47
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	8ae96a14-c2ee-46b2-a11c-75438ab3dcff
Muzaffar	4	1	2026	0000	Fetched products for shop	1fb59b20-a7ce-40f1-8c00-c1cc8a85f6e6
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	ed1954ed-32e9-44ed-b05d-e2b35de29d2d
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	856433d8-c075-4be5-aadc-445dcad7cd31
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	8f46c89a-fedb-49fd-93d1-56edbde9e8fd
Muzaffar	4	1	2026	0000	Fetched products for shop	22c8af41-7f4b-41cd-99e7-51c6959de293
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	1d3bb7a5-7d5a-4f60-87dc-6e67f2e59455
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	5fbd4760-3837-4343-a2ad-00d83f908dd2
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	cc04a5a5-7ab5-4a41-ac00-843ec53c79e5
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	f1ca7a09-404d-4cda-81f3-25fe608900c7
Muzaffar	4	1	2026	0000	Fetched products for shop	5380a565-2a94-4162-a561-2d0acc30a6ad
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	6a4c0042-5b1c-4e14-8d3f-c7685164f19e
Muzaffar	4	1	2026	0000	Fetched products for shop	14be78c8-7245-4dcd-8879-7e418f4e73e9
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	c7dfa707-9939-40c1-9f3f-683122557968
Muzaffar	4	1	2026	0000	Fetched products for shop	ed23f2f1-5228-427a-861f-abcbcfbc1c82
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	50be291c-be23-45e5-8c28-fb6b5c6371e4
Muzaffar	4	1	2026	0000	Fetched products for shop	16240db2-493f-4ade-a73c-a63febdacda6
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	dcc2dec8-ead3-4e12-812c-30fb830e0c2a
\N	4	1	2026	0000	Sale created successfully with sales_id: 52ff0e1c-8563-400a-a26b-a27b15b15f5a	906afb3d-a0ba-4ca5-8735-25447d96aa99
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	76954589-1394-4dc2-924f-26294bed7c54
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	7de46a5c-2439-4367-b7be-3bfe558b5ef3
Muzaffar	4	1	2026	0000	Fetched products for shop	6721561c-145a-4533-9478-1d1abeafe7e0
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	51f9ce30-705f-4df6-85f2-664f2e489f57
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	a3de3d6e-8563-4c8b-8692-cc6fdf3d6c04
Muzaffar	4	1	2026	0000	Fetched products for shop	71ac92eb-6593-4b33-9239-62feadc37706
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	07e7753c-7a1a-46d2-89ef-243a6be9f64b
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	7c80a163-fcbc-4d2c-931a-edf85e716bfd
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	2a8922e5-874f-496d-9f38-fa3d24c7ab77
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	b4239040-1117-4873-a674-8e22cae1cb94
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	3863336c-8a9a-47cd-8ed3-1a493aad1e96
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	5c61fff2-1dba-470d-bfeb-901eab19bc12
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	9359b470-3f36-4f96-a224-452330b75744
Muzaffar	4	1	2026	0000	Fetched debt statistics	fe444e62-3dee-42e9-8e25-ddfb918b6db6
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	cc7e71a2-d405-4be9-9f2a-ef8d91efbaad
Muzaffar	4	1	2026	0000	Fetched debt statistics	30f30879-efde-485a-a65e-0d214faca411
Muzaffar	4	1	2026	0000	Fetched products for shop	f41fb22e-d846-4d71-992d-00f099537c0d
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	800fe3e4-0720-4bf4-98cb-16cf383a2eac
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	2f9a4a8f-2648-40d4-b9cf-daa2eaf5754e
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	2bfde4a8-5d7a-4dd6-a76a-c2e8ae3d619f
Muzaffar	4	1	2026	0000	Fetched products for shop	cd175dfb-27c6-4094-a2de-a73ebcef7958
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	d6c0e0ef-b9dc-4b65-bc29-6b8c1c1800f6
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	8a36480a-ed56-4e94-8482-1400fd850559
Muzaffar	4	1	2026	0000	Fetched shop branches - count: 2	fe5addd8-838b-4557-ae21-d890a4898ecc
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	cb80c4e7-f9cc-49cb-9ccb-84af56439ff5
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	e0b785a4-01e3-4c8a-9b84-1f7dfa6cb337
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	91537d83-c2a6-4947-9197-8b10a18e0b52
Muzaffar	4	1	2026	0000	Fetched debt statistics	b98ec03b-5736-48a6-989d-d2c154c43c8e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	7bfdf050-f8c2-4240-bf7a-294bbd7ea4bf
Muzaffar	4	1	2026	0000	Fetched debt statistics	141f03e4-9dfe-476f-bab9-e2ca048fa040
\N	4	1	2026	Muzaffar	Fetched all wagons	d837b72e-3fc3-44a1-a094-d2daea3ccab4
\N	4	1	2026	Muzaffar	Fetched all wagons	5a4ec74c-3076-4e93-a964-635eaa393323
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	6c7d6f07-6e81-4176-9b90-6e9c2baafb94
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	8e717b79-f77c-4b67-a0fa-3d93380065ca
Muzaffar	4	1	2026	0000	Fetched debt statistics	a223487d-d727-4f9a-a20d-161d5059f26f
Muzaffar	4	1	2026	0000	Fetched debt statistics	28d38bb9-3eec-447f-a3a7-debf000e5ba6
Muzaffar	4	1	2026	\N	Fetched main finance statistics	beefd5b7-61cb-4d72-b347-a430457041af
Muzaffar	4	1	2026	\N	Fetched main finance statistics	9c1b94b7-cf7f-4ff9-8855-d857e4b29ee4
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	bbc7aed1-7a93-4364-ab75-bdd3ed5db192
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	41f07c51-f10c-47a0-a43f-a68bf600d847
Muzaffar	4	1	2026	\N	Fetched high stock products	fe9ab7a0-880a-4aa0-9ece-811aa701c710
Muzaffar	4	1	2026	\N	Fetched high stock products	b44facce-78e6-49b8-b415-dc771b0aa222
Muzaffar	4	1	2026	\N	Fetched low stock products	5cda8562-811c-45a6-9f7b-97bbe477e619
Muzaffar	4	1	2026	\N	Fetched low stock products	f3d94c59-3f98-4604-b7cd-c978c024aa88
Muzaffar	4	1	2026	0000	No products found for shop	0275b831-87c5-4b35-b0eb-4df7a857c94c
Muzaffar	4	1	2026	0000	No products found for shop	e29a1051-c7e9-4851-89a0-48682119cd7d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	3988ba38-65a9-4f8b-8936-64c700e11e19
Muzaffar	4	1	2026	0000	Fetched debt statistics	2c239462-cbf4-4532-8d5d-ff43ae5c0993
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	4d4fd175-1b69-45ae-a56f-8df22e4878f8
Muzaffar	4	1	2026	0000	Fetched debt statistics	bc367ded-ecf2-48bf-9872-fe66c8b91a08
Muzaffar	4	1	2026	0000	No products found for shop	e4a8bf4e-1fe9-4f71-9de6-7185273a88be
Muzaffar	4	1	2026	0000	No products found for shop	e845366e-b19e-41ca-8937-ec86bee0d2d1
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	e75f79b9-e617-4fc7-bcfe-3364da51c59e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	94878bb5-d684-4da5-9f08-76e5ac86695b
Muzaffar	4	1	2026	0000	Fetched debt statistics	1eb23173-f6ad-49ae-919c-1c6f7162f52b
Muzaffar	4	1	2026	0000	Fetched debt statistics	0aaf955c-dfc6-44d0-8f42-015269fa6c72
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	6df78a23-3365-47b5-ad22-1e22fe1f3e15
Muzaffar	4	1	2026	0000	Fetched debt statistics	2c041b76-7c6c-48c0-b28c-bd8947cf440a
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	21a22b1e-336b-4763-8b41-4466b4b6cf86
Muzaffar	4	1	2026	0000	Fetched debt statistics	6e08ab88-e500-488f-9a97-ff71e7cb0334
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	2dd9511c-8028-413e-824f-86e3dc7a8b8c
Muzaffar	4	1	2026	0000	Fetched debt statistics	86c7dfa0-431c-4311-8b94-44a32ed1e4de
Muzaffar	4	1	2026	\N	Fetched main finance statistics	875811dd-15a2-44f0-9b3d-2dfddbee122e
Muzaffar	4	1	2026	\N	Fetched main finance statistics	e02ab110-b01c-4d35-b42f-19e38377ed6b
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	30d865bb-00ad-4488-96ab-78d130c45d71
Muzaffar	4	1	2026	\N	Fetched week statistics (last 7 days)	9ab7af96-77b4-4a97-a8f5-963a1fc58072
Muzaffar	4	1	2026	\N	Fetched high stock products	4c25872f-83e3-45f6-aaa0-e7783ea9418c
Muzaffar	4	1	2026	\N	Fetched high stock products	d015b1fa-63f5-4294-8e77-e828cdaf2d86
Muzaffar	4	1	2026	\N	Fetched low stock products	ef343ed7-68ca-4704-bb83-6ceb96c9baee
Muzaffar	4	1	2026	\N	Fetched low stock products	19442fcd-80d1-4655-9f01-5d1e88ea84d4
Muzaffar	4	1	2026	0000	Fetched products for shop	fc18c674-d3cf-432a-8ce2-26af86766885
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	93d76819-42b2-4d70-967b-2585bbf381eb
Muzaffar	4	1	2026	0000	Fetched products for shop	e1919af0-f9d8-4c4a-b980-90b7203adb51
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	3fc20e0e-24b1-4303-8d99-d590515dceb7
Muzaffar	4	1	2026	0000	Fetched products for shop	f48c2710-8266-4641-9321-2993b0c9c12c
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	36c70a2f-1cf7-4028-bc21-c6e0ec040db7
Muzaffar	4	1	2026	0000	Fetched products for shop	21c4b42d-e0da-4ef0-9ee0-7c9d9f62332b
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	7304258c-84d6-42f0-a016-10658e5d2183
Muzaffar	4	1	2026	0000	Fetched products for shop	2c088c51-f973-448f-a2cc-09ae47a131ce
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	f9764cd7-f91f-4205-a004-7df768b3cc1d
Muzaffar	4	1	2026	0000	Fetched products for shop	d987bd44-7eed-4d9d-b52c-56919cd7589c
Muzaffar	4	1	2026	\N	Fetched all brands - count: 3	8efaeb2e-ce2c-4e50-8ad9-3f7c31b6fd64
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	71d51cee-df83-45e5-8a4d-692762eb0f5a
Muzaffar	4	1	2026	Muzaffar	Fetched all sales	23431283-a09a-4e6a-9e83-91f0fd6745fa
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	b48690b3-b1e6-4cad-86b6-cc211c042ceb
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	63babb62-932b-4039-9b65-87ec8865290f
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	20e19855-54ca-4696-bb7f-b0de31f5760a
Muzaffar	4	1	2026	0000	Fetched debt statistics	83ba6437-ca5d-4f80-a724-aa1694ed33e0
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	4f1f3703-4b2a-44ba-b51b-deec65c79d5c
Muzaffar	4	1	2026	0000	Fetched debt statistics	2e6aab8d-a2e1-4796-a5d8-0f986c58ac14
Muzaffar	4	1	2026	\N	Fetched debt by ID: 2a2f8128-c4bf-4067-9796-261e15036109	f60d2337-8c17-403d-94fd-7161112f5a9c
\N	4	1	2026	Muzaffar	Fetched all wagons	90d547f5-2eba-4c80-b7ae-690f15bc1274
\N	4	1	2026	Muzaffar	Fetched all wagons	3b01b779-5cfa-4775-b664-71f9a2c63d1d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	168afe02-c501-48da-adfc-5c6baf809e1f
Muzaffar	4	1	2026	0000	Fetched debt statistics	6a60dab3-1e17-4e7d-bfdb-b147dd95921d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	fb48c1e1-acf0-4dfb-b657-a59a59f51263
Muzaffar	4	1	2026	0000	Fetched debt statistics	f4118639-c34c-47cc-821f-50de1f6b7a37
\N	4	1	2026	Muzaffar	Fetched all wagons	f2501a9d-ddf8-4f12-8a16-7d797caf50bf
\N	4	1	2026	Muzaffar	Fetched all wagons	a63d82f0-d794-4a2c-8732-8b81fb7d31eb
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	26c5e850-b4ba-4b1a-ba6e-d0c366686d4b
Muzaffar	4	1	2026	0000	Fetched debt statistics	5f4a0563-bb5a-40dd-8e80-3328ef5b5076
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	8e653539-2cc4-4746-bcce-375f17d26d25
Muzaffar	4	1	2026	0000	Fetched debt statistics	2c74a4e4-957e-4783-8877-808a900a1384
\N	4	1	2026	Muzaffar	Fetched all wagons	6e08f057-335d-416c-8b4a-c39003acceae
\N	4	1	2026	Muzaffar	Fetched all wagons	35e8bf5a-801f-4290-8f47-5ec5a523c9e3
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	40d348af-6eb4-4c05-8166-09bd2f84a162
Muzaffar	4	1	2026	0000	Fetched debt statistics	9915136d-2b57-4461-9488-68cd6ed4aa7d
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	69d89753-d3ef-43bb-b7d7-c7d0446474fe
Muzaffar	4	1	2026	0000	Fetched debt statistics	8a7e85f4-5540-4ddf-b263-8f26f91e9bb8
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	6096db17-776c-4b7f-a226-d925b043aff5
Muzaffar	4	1	2026	0000	Fetched debt statistics	6337f5d5-9aeb-4009-8b2f-f36968726443
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	52e5a1cd-189b-435f-9eb5-e211c246d8d3
Muzaffar	4	1	2026	0000	Fetched debt statistics	61efa5d8-0552-415d-8cc3-9fabf30a40e4
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	4a81e497-eb66-4788-a40d-51db7f035536
Muzaffar	4	1	2026	0000	Fetched debt statistics	ceba356a-c8f7-494e-a083-c45f9b855fb2
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	b86a0780-2a8a-46ec-9690-bdaf1c1eddf0
Muzaffar	4	1	2026	0000	Fetched debt statistics	ac1fbb39-6f5c-42cc-806c-22c00dd69c7e
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	77cb4945-0091-4686-8a90-23fbf3fe07d6
Muzaffar	4	1	2026	0000	Fetched debt statistics	29148cfb-6fd9-4ffa-bb5e-473740038c9b
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	269505d4-98f9-4191-9cf7-752e21effebf
Muzaffar	4	1	2026	\N	Fetched all categories - count: 3	36459f76-050a-4b70-bce8-b5ca889eafb5
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	30f43ae1-9542-4788-8b55-16bcf85db18d
Muzaffar	4	1	2026	0000	Fetched debt statistics	52917a8f-8391-4975-a189-96e9e2fe5118
Muzaffar	4	1	2026	0000	Fetched all debts - count: 8	7f8d506f-fd9d-4191-97ad-b7c72141c9c3
Muzaffar	4	1	2026	0000	Fetched debt statistics	ad6f5dc3-994e-44f4-b9ff-f63df144d62a
Muzaffar	4	1	2026	0000	Debt created successfully - customer: Muzaffar, amount: 7000	1ef9bb12-65df-4e86-8e57-f45d1b953430
Muzaffar	4	1	2026	0000	Fetched debt statistics	da13a287-d034-415c-9cd0-97205ec0e535
550e8400-e29b-41d4-a716-446655440000	5	1	2026	\N	Superuser logged out	471f9b0c-f4a4-4582-8120-271a34250f8c
Muzaffar	5	1	2026	\N	SuperUser Login FAILED - not found	4748f0e5-6fb4-445f-8ca8-df10e06d0cb5
Muzaffar	5	1	2026	0000	Super logged in	bbd118d3-bd55-474e-a79f-4c93c2d2fa52
Muzaffar	5	1	2026	\N	Fetched main finance statistics	faae8a3a-52a1-43f2-9c0f-6402e49e71eb
Muzaffar	5	1	2026	\N	Fetched main finance statistics	b9c1c8ed-cda1-4b98-9d94-239acf9d6aa6
Muzaffar	5	1	2026	\N	Fetched week statistics (last 7 days)	aa2a420e-ae50-4ca4-a943-5f2acb436f33
Muzaffar	5	1	2026	\N	Fetched week statistics (last 7 days)	fe4920d4-ad1b-400d-80f8-a4ec7319b5be
Muzaffar	5	1	2026	\N	Fetched high stock products	c6fc0315-2a52-4fc4-a2d9-11389369fa59
Muzaffar	5	1	2026	\N	Fetched high stock products	09baa74e-be6b-4215-8bf0-2508feadc84d
Muzaffar	5	1	2026	\N	Fetched low stock products	9ac21b8c-1ab7-406f-8768-55bd825819eb
Muzaffar	5	1	2026	\N	Fetched low stock products	7ca7de85-9e2f-4ac2-9ba6-e24899d67782
Muzaffar	5	1	2026	0000	Fetched products for shop	03e23845-a39b-4308-a4f4-c730f59b897e
Muzaffar	5	1	2026	\N	Fetched all brands - count: 3	b304252f-3965-4d65-b902-64eb38bbb21a
Muzaffar	5	1	2026	0000	Fetched products for shop	56274e4d-992d-4d3c-9c46-e22f520ba4d5
Muzaffar	5	1	2026	\N	Fetched all brands - count: 3	f501e49e-2605-4b86-930c-6b0d0dae19a4
Muzaffar	5	1	2026	Muzaffar	Fetched all sales	57a01bc1-89e7-4354-85c4-3fe16474b67d
Muzaffar	5	1	2026	Muzaffar	Fetched all sales	c47345c3-9556-45f1-b492-e013e24879c5
Muzaffar	5	1	2026	0000	Fetched all debts - count: 9	0f13636f-91bd-4e61-8480-8e026b350bdd
Muzaffar	5	1	2026	0000	Fetched debt statistics	a387f460-3b2a-42ac-bd0a-b6f579bec2ba
Muzaffar	5	1	2026	0000	Fetched all debts - count: 9	58510f4e-473c-47b0-99e3-f64fc6694af5
Muzaffar	5	1	2026	0000	Fetched debt statistics	c5f3ea0d-a19f-4f46-a148-3663143615ba
Muzaffar	5	1	2026	0000	Debt created successfully - customer: Sobir, amount: 6250400	970e349e-7ce5-44d7-a611-b190885fed06
Muzaffar	5	1	2026	0000	Fetched debt statistics	0885473c-573e-4206-907d-cd1bf70ba92a
Muzaffar	5	1	2026	\N	Fetched main finance statistics	da4994d4-54c4-4a15-88dc-2fa1b1e6cca9
Muzaffar	5	1	2026	\N	Fetched main finance statistics	78ba8cbe-b789-4b20-bc9b-fb176bd49253
Muzaffar	5	1	2026	\N	Fetched week statistics (last 7 days)	04e69fb5-05d6-4315-aed1-557d51ecb73f
Muzaffar	5	1	2026	\N	Fetched week statistics (last 7 days)	695ac498-fdca-4761-b52e-2c249f415867
Muzaffar	5	1	2026	\N	Fetched high stock products	31a4e153-6ddc-499d-843b-41347c603dbe
Muzaffar	5	1	2026	0000	Fetched all debts - count: 10	cd2143d2-1ac9-48cc-a53b-5c823e293421
Muzaffar	5	1	2026	0000	Fetched debt statistics	ac7f81bf-f3fc-496b-aaab-c4f6eaaa3184
Muzaffar	5	1	2026	0000	Fetched all debts - count: 10	0f23f527-8e1d-467c-ae59-a05d6a6d6c88
Muzaffar	5	1	2026	0000	Fetched debt statistics	3f88bce9-a7ac-471f-96bb-aab6dc7e0881
Muzaffar	5	1	2026	\N	Fetched main finance statistics	5b3a2de6-d0fd-4d8c-871c-7bb64199f156
Muzaffar	5	1	2026	\N	Fetched main finance statistics	1801f1fc-ee6e-472f-8fc6-9a126e98c34b
Muzaffar	5	1	2026	\N	Fetched week statistics (last 7 days)	145253f9-20d6-452b-87e2-0ba21365359b
Muzaffar	5	1	2026	\N	Fetched week statistics (last 7 days)	07f29fa4-34c9-44c4-a3bf-e9c3960e03cc
Muzaffar	5	1	2026	\N	Fetched high stock products	3edd8c8e-e24c-4c74-a486-bafcfa21c827
Muzaffar	5	1	2026	\N	Fetched high stock products	9b106cb8-135f-4b1f-a34c-66ddaced416a
Muzaffar	5	1	2026	\N	Fetched low stock products	0e7a8179-2b3b-45a9-be42-e576c590405a
Muzaffar	5	1	2026	\N	Fetched low stock products	62394237-7100-4749-b8e5-992e6ae32f6e
Muzaffar	6	1	2026	\N	Fetched main finance statistics	8b0914e1-6ec5-4a61-8981-f309658d5298
Muzaffar	6	1	2026	\N	Fetched main finance statistics	5cb85bed-075a-4414-a1a0-f65b07b65afc
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	8832a566-0be6-4943-930b-56eea4ddf5a5
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	7bda5b9b-8bd9-45bf-8946-474682b6c5fa
Muzaffar	6	1	2026	\N	Fetched high stock products	617ef9e1-a1fd-4966-a22d-fea7d89b2a99
Muzaffar	6	1	2026	\N	Fetched high stock products	ba9bb0ad-3164-4f5b-95a4-52797f472c83
Muzaffar	6	1	2026	\N	Fetched low stock products	8454828b-ac7e-475b-929a-e851b284f7c0
Muzaffar	6	1	2026	\N	Fetched low stock products	d986cde8-ba8b-48e4-94ae-ae1655c59549
Muzaffar	6	1	2026	0000	Fetched products for shop	b8ebf1b3-585e-41eb-a03d-277dd7ca270c
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	dfd5d048-af96-4530-ac64-5e9ae642b8ec
Muzaffar	6	1	2026	0000	Fetched products for shop	70158ca2-bf3a-4ba6-a7b0-1afb58008f89
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	94146d51-507e-4837-b657-a34abec95134
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	db5f53d2-e513-43a2-badc-ffd9ba91d6cc
Muzaffar	6	1	2026	0000	Fetched debt statistics	4ab8b2aa-cfa7-4e00-a757-0ef79de49762
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	0c50c5de-f5fa-4745-bfd2-7efe2b18c5d5
Muzaffar	6	1	2026	0000	Fetched debt statistics	21453afd-a50f-404b-b3d6-09a5a3cc760e
Muzaffar	6	1	2026	\N	Fetched debt by ID: 2a2f8128-c4bf-4067-9796-261e15036109	66ff97b6-35b3-459e-833e-31e3d7772d28
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	26b425dd-5914-4c1b-ba0f-8c957ad591b1
Muzaffar	6	1	2026	0000	Fetched debt statistics	0c35ac55-bca6-4eec-890b-71d76b78e396
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	b2bf6bd1-6b49-445a-ab52-7539fdffd30e
Muzaffar	6	1	2026	0000	Fetched debt statistics	a7a478ab-60f7-4bc9-9f86-c3d7396a1be7
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	81484589-3937-4554-971a-745277a86ac7
Muzaffar	6	1	2026	0000	Fetched debt statistics	b7bbd9ec-c5ac-4a1a-bb61-c6a51a11c81c
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	c939f443-fe4b-4b3b-8a68-a99de3e867c9
Muzaffar	6	1	2026	0000	Fetched debt statistics	95ea31e0-b062-4b9b-a6bc-2dd4b71c7df5
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	0c98e02d-75bc-4d9d-87f5-522f71a55da6
Muzaffar	6	1	2026	0000	Fetched debt statistics	616c4557-d517-4fbd-af1b-3437198ea264
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	2e8652f1-3172-4168-bdf8-11e8e7f872cf
Muzaffar	6	1	2026	0000	Fetched debt statistics	d59d78cd-9fa6-41bb-af4a-b1776c129700
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	903d88e7-c9ca-4a9d-bc9d-7eb00cdab281
Muzaffar	6	1	2026	0000	Fetched debt statistics	451f0dda-d338-4559-9f20-2d247aa1723a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	f4f6207c-61c4-4878-84ad-c9b24154f0d7
Muzaffar	6	1	2026	0000	Fetched debt statistics	e957867f-95c1-4a08-b2b1-5aa10a654c9f
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	f2d7545e-d309-4cc8-bdd6-712ae66556a3
Muzaffar	6	1	2026	0000	Fetched debt statistics	0e0ca8f7-0674-47ca-b641-08b5b873ea78
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	0753088c-ca45-4fe0-9584-4a956ceaac43
Muzaffar	6	1	2026	0000	Fetched debt statistics	16753de3-9ab4-4b18-b9c6-3e4d9d8b3974
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	414245f7-fc70-4d9a-b236-6020ba087b9b
Muzaffar	6	1	2026	0000	Fetched debt statistics	46bd186a-6f75-492a-8f2a-efb0c4e6816a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	ac50172d-9195-43ca-af2f-9b65f45e243c
Muzaffar	6	1	2026	0000	Fetched debt statistics	dd2cfc6e-b260-4957-899b-0bb9cac16b04
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	9729f958-c97d-45d7-8002-616e0f03d857
Muzaffar	6	1	2026	0000	Fetched debt statistics	347a7de2-5bd2-4a24-8d5d-92ce6a5f53fe
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	38e5ae52-866e-427c-9e75-35c366c07d05
Muzaffar	6	1	2026	0000	Fetched debt statistics	5e4677fb-23ad-4b5d-a94c-6ba08ead3d2e
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	b4c3a978-779a-480f-8abf-e1e85f22fe2a
Muzaffar	6	1	2026	0000	Fetched debt statistics	e606b618-43ee-4889-b0ad-5f36e5d61358
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	99f26381-ccbe-4bfe-87e8-831e6d013d8f
Muzaffar	6	1	2026	0000	Fetched debt statistics	b05f2c7e-bc5a-484f-97b9-13db79e27fd7
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	9f863136-e7df-4c58-9484-02f1694896fe
Muzaffar	6	1	2026	0000	Fetched debt statistics	73069771-e718-4949-b77c-b05272d6b6f3
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	26317c7c-73b9-4d8c-8b21-31f06f08d5ba
Muzaffar	6	1	2026	0000	Fetched debt statistics	021b9209-eea1-4052-b3fd-49c10ca43933
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	a680c573-a4e2-4204-997e-6d2cbd6284b5
Muzaffar	6	1	2026	0000	Fetched debt statistics	d0cc4768-a3b8-4004-be89-9e1340fb8c26
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	f5ddd4f1-835a-4845-a099-bd634b3e9361
Muzaffar	6	1	2026	0000	Fetched debt statistics	706efde1-5901-4830-a3c4-684c544d5cd5
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	8ad08c19-0688-4b80-b220-2bcf0d825fcd
Muzaffar	6	1	2026	0000	Fetched debt statistics	7a618051-8f74-497d-af65-ae88014ad074
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	d7bb3b5c-238e-4680-80b8-a044a87ef700
Muzaffar	6	1	2026	0000	Fetched debt statistics	38e08dd4-c202-4755-b608-391eedf5620b
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	211cf8f1-75f7-4860-8fbd-dd3da64206a6
Muzaffar	6	1	2026	0000	Fetched debt statistics	a94eec01-7b72-451b-8414-02adb4ae3ac6
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	2afe5035-3204-48fe-afe5-66c1d3b81d90
Muzaffar	6	1	2026	0000	Fetched debt statistics	38393d20-030f-411e-aac8-0421439f7dc0
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	cec2c55e-a0a2-45dd-8189-f1f2e456e2fc
Muzaffar	6	1	2026	0000	Fetched debt statistics	6ee62f02-adaf-4f92-91d4-331358009db7
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	c5a6652c-1e70-44b1-ada3-b738a6b21098
Muzaffar	6	1	2026	0000	Fetched debt statistics	800c2c15-477e-48d7-b712-ccf206b4edd2
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	f4054c3f-0f01-456d-81bc-002538686d87
Muzaffar	6	1	2026	0000	Fetched debt statistics	45b9544c-2ecc-426c-95db-95dfc8a02632
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	dd3eb8a2-6c50-4c63-b578-72acb04abc7b
Muzaffar	6	1	2026	0000	Fetched debt statistics	df81d290-9fa6-4599-aa91-de2be905225b
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	4df8ef45-dd4f-44b1-a64c-888ce7e07b44
Muzaffar	6	1	2026	0000	Fetched debt statistics	4ccea5c5-b103-4153-87b5-737cb75907e1
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	ce1862a1-19ff-4a96-a3e5-83519498b592
Muzaffar	6	1	2026	0000	Fetched debt statistics	e96b88a6-7f09-4183-985f-333bdea032a8
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	11c73c09-c91e-4cc6-a64f-a59010ac6cd9
Muzaffar	6	1	2026	0000	Fetched debt statistics	e2d8f04d-c8dc-4cc2-87a6-bc75ca009ea3
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	618badad-6e68-429c-bcb1-c7477204ffbe
Muzaffar	6	1	2026	0000	Fetched debt statistics	8d6bfbbf-8c5c-4fcd-887f-9d35ecde6585
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	e3f837ed-b8ad-4b0f-bf14-560d76c73498
Muzaffar	6	1	2026	0000	Fetched debt statistics	f9fb1ad9-fb0a-4826-bc09-7218e1d76276
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	029f3c53-27a2-44ee-a311-009eb69665eb
Muzaffar	6	1	2026	0000	Fetched debt statistics	e74a5992-105b-4b92-afcf-f4b401d1135f
Muzaffar	6	1	2026	0000	Fetched products for shop	09e17115-404d-4d05-a5fe-4be2e3c2f599
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	f2b09986-0f52-446c-a017-712a42973a90
Muzaffar	6	1	2026	0000	Fetched products for shop	e4640bae-5dd6-40d4-86d1-65c7da426454
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	b758a6e8-e0c7-4a64-8e1f-3d834dc205f6
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	738b1acb-bded-4e0c-a292-20ea12dd9269
Muzaffar	6	1	2026	0000	Fetched debt statistics	8da3cfce-bcea-441f-b925-8cd7ff2a8d68
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	0baa32f1-1d48-4c94-a8f8-e373cf858f24
Muzaffar	6	1	2026	0000	Fetched debt statistics	48e3abc5-75d4-4f6e-9edc-2e25b9a91c0f
Muzaffar	6	1	2026	\N	Fetched debt by ID: 06cbeaa9-6c8a-4b95-9327-3d006ed50bce	d59fa346-6086-44c5-be2b-829afbdc2fe8
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	80121151-e175-439c-94f7-9241b9778134
Muzaffar	6	1	2026	0000	Fetched debt statistics	86df384e-b2e8-46d5-a3ae-4b07c28fea5a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	4a8edb28-7510-4486-ae92-4ff2602366b0
Muzaffar	6	1	2026	0000	Fetched debt statistics	f761c6c3-9a6d-4648-a318-8b6acff64b7e
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	40f7b827-17a9-40b2-b71d-9a23c9d4afa5
Muzaffar	6	1	2026	0000	Fetched debt statistics	f5837f1d-5944-4091-8181-6304d5953ad6
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	882afa23-459f-4839-8bd0-a4bef100cf24
Muzaffar	6	1	2026	0000	Fetched debt statistics	4275ad06-001f-42c2-b84c-ea1d75a53e0b
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	49ffc814-cb18-4c79-934f-b841f4c2f32c
Muzaffar	6	1	2026	0000	Fetched debt statistics	e2f8f031-32e7-425e-80ab-fe317d22bdb5
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	fc7d0eda-acf7-4c0c-9daf-d13d3b39aec2
Muzaffar	6	1	2026	0000	Fetched debt statistics	d577efcf-b903-483b-872b-dff5f4aec141
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	4a8eec27-5a1d-4f70-aaab-be1534a0f457
Muzaffar	6	1	2026	0000	Fetched debt statistics	22aa46d9-1091-48f4-8aff-2eda74074e00
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	d99409a0-23e8-4f0b-ae3c-ec2afcc1999a
Muzaffar	6	1	2026	0000	Fetched debt statistics	4f82e67e-bb46-4c32-9c5e-7f58f802c2d0
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	f83adee6-19b3-4a1f-b029-6e57966e804a
Muzaffar	6	1	2026	0000	Fetched debt statistics	7d22ff90-ba04-4437-a40f-dfd21976c8ae
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	8af120c3-9024-45a8-b57c-2f4cf515ad0f
Muzaffar	6	1	2026	0000	Fetched debt statistics	f3a11bf8-b198-4fe7-9ab8-bfc3f3115152
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	f1745601-151d-43b9-b245-a5fe0f0d95a5
Muzaffar	6	1	2026	0000	Fetched debt statistics	6971f193-d0f4-49f0-8226-a772b39831b5
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	6d080765-b1c1-447b-8a25-906a5331b052
Muzaffar	6	1	2026	0000	Fetched debt statistics	95ed6fc1-d5a1-4b40-95ca-7873b91c7ea9
Muzaffar	6	1	2026	\N	Fetched debt by ID: a1d481b5-95e3-4c9c-9cb3-8d7aa415d07e	c97d98c3-86ff-4a09-9b4e-29be781e4d17
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	63fc26fa-2030-4c16-9fbf-cf1e4ee62072
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	db15afaf-bda2-49c1-b917-815d7de17d0d
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	4206a665-b736-4647-b6fc-5a400adae6a5
Muzaffar	6	1	2026	0000	Fetched debt statistics	e2398d29-ffa5-4b0e-b0d0-c93f824caeb3
Muzaffar	6	1	2026	0000	Fetched all debts - count: 10	cb385330-da1e-47b2-8e1b-67b91d7d4720
Muzaffar	6	1	2026	0000	Fetched debt statistics	546f3df1-6e6c-4d77-90f6-ba66a513ddc0
Muzaffar	6	1	2026	0000	Debt created successfully - customer: Salom, amount: 225000	86196ecd-4caa-49af-85be-1468c87251e9
Muzaffar	6	1	2026	0000	Fetched debt statistics	3e374c7a-85ab-4540-a52f-e2d1e62be70a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 11	73c0b5b8-bb83-4c1c-9f34-ea83592d764c
Muzaffar	6	1	2026	0000	Fetched debt statistics	279d1305-7588-4000-a409-99fe486851f8
Muzaffar	6	1	2026	0000	Fetched all debts - count: 11	f2ce2149-99ef-42d5-8199-7c93f1f1c350
Muzaffar	6	1	2026	0000	Fetched debt statistics	b1fe7564-4628-4390-9af5-16bd9ee2398c
Muzaffar	6	1	2026	0000	Debt created successfully - customer: Qaysi, amount: 180000	52932166-4781-4717-b6ce-5be91648822e
Muzaffar	6	1	2026	0000	Fetched debt statistics	8122ad57-f189-43d9-ade3-560fbb7bfe38
Muzaffar	6	1	2026	\N	Fetched debt by ID: eba76b0d-d1fd-46fe-873f-ae4277b72fed	bb13b754-bc2a-4591-9bc6-bba20a55f1f6
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	afff0150-4eca-4ea3-80a2-685397767584
Muzaffar	6	1	2026	0000	Fetched debt statistics	cbcb8381-2ce2-4833-8524-dec7cac2a859
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	3d38ccc4-ec7f-4d69-8c37-9e24cc319638
Muzaffar	6	1	2026	0000	Fetched debt statistics	411c9b63-1219-4e3d-9de9-a4838008d966
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	e4f44487-504a-4299-a272-86cbe04e6f47
Muzaffar	6	1	2026	0000	Fetched debt statistics	01483234-eb6a-4681-b5b2-2b68229739ad
Muzaffar	6	1	2026	\N	Fetched debt by ID: eba76b0d-d1fd-46fe-873f-ae4277b72fed	e6aa0a4e-4304-4250-8bbc-55b7ec926120
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	a69506c8-ee2b-47a7-9f07-b68bae87434f
Muzaffar	6	1	2026	0000	Fetched debt statistics	0f6ee0bf-d1bd-41ee-b3c6-474e31c4d399
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	ec2b42c0-2b51-4b22-b548-3b9e58b37d0e
Muzaffar	6	1	2026	0000	Fetched debt statistics	bccae5b0-db8b-48b6-9a4b-9b11faa3dd49
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	0a6740bf-c0a3-4156-bf82-9ed7de312e3b
Muzaffar	6	1	2026	0000	Fetched debt statistics	aaec2fb7-36b7-4a48-9206-af55ea95bbcd
Muzaffar	6	1	2026	\N	Fetched debt by ID: d150be55-9f8c-4d38-87e8-f6b0578c82bf	2d21c5fd-a2b0-4f7a-bab7-f45720dc91fe
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	8d32e14b-9c17-437c-bcca-960649ca0325
Muzaffar	6	1	2026	0000	Fetched debt statistics	32924183-c413-4174-b720-9cf6ae280105
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	ccbadef8-7bc5-41da-829c-878e6b7d0c33
Muzaffar	6	1	2026	0000	Fetched debt statistics	434a8b8b-0e19-4e7a-9837-598ebff34f51
Muzaffar	6	1	2026	\N	Fetched debt by ID: 06cbeaa9-6c8a-4b95-9327-3d006ed50bce	a669e756-a14e-426c-b18c-8d341f9cbd22
Muzaffar	6	1	2026	0000	Fetched debt statistics	4185b678-33de-4b57-b47f-e61c86c0b25b
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	881500db-65e1-4cdc-a8bb-322bb425e662
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	f15cf468-41a9-4f64-bdf3-d47cb1a0b46b
Muzaffar	6	1	2026	0000	Fetched debt statistics	a71820db-f7e9-4c4d-8833-ab28262901bc
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	3fbfce38-c2c0-44bc-bc73-7178e39a9d1c
Muzaffar	6	1	2026	0000	Fetched debt statistics	3751ab8a-a898-4bb7-8e51-9a604c924638
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	1c8b3b22-a9d1-4870-9752-cfab57d6c2a1
Muzaffar	6	1	2026	0000	Fetched debt statistics	5d5a8fb1-65ec-4263-a3e5-6953f8b6fe12
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	dc2a5326-9041-49f7-acda-cc4fd5097391
Muzaffar	6	1	2026	0000	Fetched debt statistics	e3b7b5bb-a136-448c-a47d-fba5af92bd73
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	7a01017d-61ce-4aa4-a5fe-cbf60b685a02
Muzaffar	6	1	2026	0000	Fetched debt statistics	8fb8c783-e8df-42a4-b077-4801b6a83b96
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	ea0330e2-78fa-4a50-ae06-88a58a0bd66b
Muzaffar	6	1	2026	0000	Fetched debt statistics	1f3e1fe6-65c9-4252-ad46-59faf56fcadc
Muzaffar	6	1	2026	\N	Fetched debt by ID: 2a2f8128-c4bf-4067-9796-261e15036109	ec6279b2-5761-4712-ad5c-ebe1d89f8d93
Muzaffar	6	1	2026	\N	Fetched debt by ID: 2a2f8128-c4bf-4067-9796-261e15036109	3b803a56-76d3-4c07-9c0e-f76b56003520
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	389a7903-8ed6-42de-8987-037af6815fd5
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	5e0fbc9e-33a9-4c61-be18-7a9b6f65a911
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	44f8efd6-b96f-437c-acfb-8465aa3916bf
Muzaffar	6	1	2026	0000	Fetched debt statistics	85a81223-cf91-4517-8e86-0ae832692c90
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	16773b8c-0eb6-4092-b26b-6ae1883ff058
Muzaffar	6	1	2026	0000	Fetched debt statistics	a71a1f41-69f0-4170-87fb-c123742f572c
Muzaffar	6	1	2026	\N	Fetched debt by ID: bcef0932-8ad7-4cc9-811e-dbe93a1b5b87	147d98fc-b893-4aa0-9abf-1240398529be
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	97fa0e31-eb1b-43fb-99f2-26ea272c4825
Muzaffar	6	1	2026	0000	Fetched debt statistics	9ee9c886-9f2a-4787-afb4-4118e64ececd
Muzaffar	6	1	2026	\N	Fetched debt by ID: bcef0932-8ad7-4cc9-811e-dbe93a1b5b87	fe85cd5a-97be-40b5-a18a-35b8e0636cd3
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	a00a9a5c-de26-4caa-8981-401d5118a055
Muzaffar	6	1	2026	0000	Fetched debt statistics	772c763d-fea1-4c17-b305-b67222177a65
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	ed3dc030-df18-4b57-8c5c-d3b1d22a381f
Muzaffar	6	1	2026	0000	Fetched debt statistics	a139e411-61cd-4c9e-b910-7f7a1c79c5cd
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	abfa5b57-0f9d-4e0f-a8af-51cec9e52ddd
Muzaffar	6	1	2026	0000	Fetched debt statistics	c2fa0aaf-b4f5-4083-adba-55d73dc1b4bb
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	43ff1367-6fbc-4700-946b-9a7248549f61
Muzaffar	6	1	2026	0000	Fetched debt statistics	a2851806-d6ee-4833-80de-16f3f0101602
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	f10744a1-795c-490c-af51-a19f47917272
Muzaffar	6	1	2026	0000	Fetched debt statistics	65aa54d4-5e60-4c1c-b994-a16029b3fd0d
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	f91fdb10-b6fb-4428-939b-5b64963532f4
Muzaffar	6	1	2026	0000	Fetched debt statistics	8d5df1f6-f9e6-4004-9093-9d09fda74fff
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	f6acd5fe-5d3d-4281-bbe6-fc215c12bd6a
Muzaffar	6	1	2026	0000	Fetched debt statistics	fcabfd6f-de2a-4749-8471-be457ad3abaa
Muzaffar	6	1	2026	0000	Fetched all debts - count: 12	043a7f74-f58c-4ba7-987c-95f7ba20d27f
Muzaffar	6	1	2026	0000	Fetched debt statistics	750549a3-eb09-42ea-9838-fb71774fade6
Muzaffar	6	1	2026	0000	Debt created successfully - customer: Abdulaziz, amount: 1050000	9771776d-06d0-4f9c-a2f8-61ccd5a94118
Muzaffar	6	1	2026	0000	Fetched debt statistics	7018f9ce-87e8-4411-8ced-1c6ed379deec
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	90221976-43d0-4ada-b4f5-cbe0fe4444a7
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	f949a7ae-a378-4e6d-978b-7b36b635c9d5
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	0c3f554f-1371-4a54-a06e-d64ea59dadb0
Muzaffar	6	1	2026	0000	Fetched debt statistics	95ce176f-50ad-463a-9474-32edfda5e24a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	0ded3c37-c211-473f-b679-68a1f7247cf9
Muzaffar	6	1	2026	0000	Fetched debt statistics	31b0df49-6061-4366-ad3e-0df20025d5a5
Muzaffar	6	1	2026	\N	Fetched debt by ID: 1f8698af-1d21-4316-8baf-ddccbee1438c	d432dd9d-19d1-4658-8302-0acd3ed7aea6
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	eda150f6-ecad-4f6c-8956-1a399925ab17
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	e58c31b4-56b3-406c-b696-bcf463cc80c7
Muzaffar	6	1	2026	0000	Fetched debt statistics	4e18c696-9d6e-452f-8d01-6fd92832439b
Muzaffar	6	1	2026	0000	Fetched debt statistics	b1400ffb-059b-4675-a56f-c232cfae1412
Muzaffar	6	1	2026	\N	Fetched debt by ID: 1f8698af-1d21-4316-8baf-ddccbee1438c	850f17d7-282a-40c3-a675-d3c38a0b8048
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	940b7260-d05d-4a1c-ad38-15a7d95a6010
Muzaffar	6	1	2026	0000	Fetched debt statistics	4ffb229c-db00-486c-ac76-5ed9582e3119
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	4f44b2ac-d2c2-4d38-a238-e139c4e1e08d
Muzaffar	6	1	2026	0000	Fetched debt statistics	e33ddd94-fc7f-4941-b9d4-d64338520d66
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	180f4fb7-9d7b-40cf-99ff-d03ac4e1d8e5
Muzaffar	6	1	2026	0000	Fetched debt statistics	58c1e9e7-7d3a-4b0d-bdf0-0b135d4f1dd4
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	fd7684ab-a63e-4e88-912d-1722d4f7ea66
Muzaffar	6	1	2026	0000	Fetched debt statistics	09f2bd4d-0ba7-4345-938e-ed045f474fec
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	fc795aa5-72f3-4742-b256-1157e436ef63
Muzaffar	6	1	2026	0000	Fetched debt statistics	996620f3-8bbc-4474-bcbc-524e98f53430
Muzaffar	6	1	2026	\N	Fetched debt by ID: 1f8698af-1d21-4316-8baf-ddccbee1438c	0b6e94cb-65ee-4e3b-8445-3fc8670e85e9
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	1a3a3910-e843-4d50-ab32-ec6f3ca49188
Muzaffar	6	1	2026	0000	Fetched debt statistics	2cc8c31c-e8dd-483e-ae21-488761b1ce40
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	3526acec-0c94-429a-90d8-7a4b7761f473
Muzaffar	6	1	2026	0000	Fetched debt statistics	86a67bf6-e4ce-4043-830f-69d027280ae4
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	a03b358f-2f2d-47cd-a7c3-0a5afb79f0f2
Muzaffar	6	1	2026	0000	Fetched debt statistics	bf9a202b-df19-4590-b68d-a33379869b33
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	d6771040-5296-4cae-b04e-0d7cc4dd0e97
Muzaffar	6	1	2026	0000	Fetched debt statistics	3aa6a7ee-dd59-40da-aca3-8dac627241ae
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	3507a9f0-692f-4c3b-a4a2-5e18547095c7
Muzaffar	6	1	2026	0000	Fetched debt statistics	6b860350-e59b-4a9a-9e18-29c4608e2165
Muzaffar	6	1	2026	\N	Fetched debt by ID: 1f8698af-1d21-4316-8baf-ddccbee1438c	f81736b8-d758-4b85-8c45-7e7843430e33
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	fb6128ac-d193-4012-a9e1-d0bba7d00696
Muzaffar	6	1	2026	0000	Fetched debt statistics	1e1ae2fd-ed64-4972-91ce-3e8335ca2ec0
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	2f41cd6e-9d59-4322-97ee-7e07f6764313
Muzaffar	6	1	2026	0000	Fetched debt statistics	9b989247-4435-45ab-a0ee-4514e67e9a5e
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	1c1a10f4-c243-4d31-8b6a-8e3cff8ae194
Muzaffar	6	1	2026	0000	Fetched debt statistics	c615014b-d0cc-445e-8a85-5e153f330508
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	2f930581-21c3-4e39-bf33-63e9e225a80b
Muzaffar	6	1	2026	0000	Fetched debt statistics	5f5f9553-ee38-4d84-acdd-167aea4d015a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	3d4666b2-ccaa-44e4-99cc-fa25e7239619
Muzaffar	6	1	2026	0000	Fetched debt statistics	4a78149d-51c8-49cb-97f2-46fce5cf8b0c
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	dbf77e9c-c27e-4d73-aaee-76bec15faa14
Muzaffar	6	1	2026	0000	Fetched debt statistics	156b4873-2837-4e81-805b-8ab5a1757b8e
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	48128c79-d16b-4f4a-9154-84514d2c50cd
Muzaffar	6	1	2026	0000	Fetched debt statistics	3eee7269-944c-437a-a01f-f55847fab223
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	c7a8c8a3-6c35-4f74-9640-0558ccb348a4
Muzaffar	6	1	2026	0000	Fetched debt statistics	199d82a2-7ba1-421e-b6c0-01e5628a8e2d
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	e39b3df2-29f2-40ca-bf1a-6edcc2ecbab3
Muzaffar	6	1	2026	0000	Fetched debt statistics	241b3dd9-3623-40cd-aeaf-6326b3fbb441
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	c192461c-fbdc-4a31-96e2-65b4016c8865
Muzaffar	6	1	2026	0000	Fetched debt statistics	9470cf31-6373-4e22-9b52-4301beee283b
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	8c967af6-1c18-4740-976a-e1a3d388a466
Muzaffar	6	1	2026	0000	Fetched debt statistics	55fe43f9-ee24-4d2b-b021-619a6cc13eae
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	7d70be1b-112a-462a-86d4-625608f9cd15
Muzaffar	6	1	2026	0000	Fetched debt statistics	929301ec-adab-42da-9568-f5414798e81a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	a14dce0c-87c7-4cc7-a2b1-5400af15a9f8
Muzaffar	6	1	2026	0000	Fetched debt statistics	504fddf1-f449-4f5b-8065-cbb7cc5b266a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	1f7be95e-3fec-4561-819c-5c1432aff13a
Muzaffar	6	1	2026	0000	Fetched debt statistics	64f232f2-47f2-48cf-8c03-ff2235fe4caf
Muzaffar	6	1	2026	\N	Fetched debt by ID: 1f8698af-1d21-4316-8baf-ddccbee1438c	2e23b192-114e-4ba1-a0ad-88a6dedba235
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	61b69470-2b65-4022-89d1-6559b7c29035
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	46b4f57a-46ad-4a18-bf2b-bb8dd95dddae
Muzaffar	6	1	2026	0000	Fetched products for shop	68e0081b-1465-46a8-8097-2ee63de8035f
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	bfb0adb8-e0d6-4fe8-ba13-83ba1a7692ab
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	7d65c506-91ef-45cb-bcb5-d662afed6aae
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	b5e04ac6-fa0d-4ba7-b6a2-87236fb73bfe
Muzaffar	6	1	2026	0000	Fetched products for shop	6476ff1b-7135-4260-94a6-05081ce02b74
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	73b5cc63-5701-4396-996a-bfdb688b69e1
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	33c77edf-661d-4f59-b2ff-fd720ff6eb7b
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	02231847-b150-4182-88ec-966fed72f1b7
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	3c92d825-c374-4eae-9549-0334a47d7205
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	e0fa6976-e17a-4611-b337-ab7a071da00a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	8171bfde-3347-486d-882e-db4ac1d441ce
Muzaffar	6	1	2026	0000	Fetched debt statistics	7a92f29e-9ba4-4b2c-9a97-27404e4b41c1
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	b8efd146-7f47-45b8-819c-f017e61115b8
Muzaffar	6	1	2026	0000	Fetched debt statistics	17113696-2db3-48e6-8e33-7e16f7a7a681
\N	6	1	2026	Muzaffar	Fetched all wagons	e2c9fd51-b233-4ec4-9f1d-88cab6fc982d
\N	6	1	2026	Muzaffar	Fetched all wagons	435e48bb-42d0-4a3b-830c-04a9be081690
\N	6	1	2026	Muzaffar	Created wagon: Abdulaziz	d32a2d59-1653-4c2c-8748-3c0afa43b7cf
\N	6	1	2026	Muzaffar	Fetched all wagons	3a140800-174e-480a-9e28-6ce6c430bb7b
\N	6	1	2026	Muzaffar	Fetched all wagons	936d5de0-f22d-4e0c-a465-48f156e18fc5
\N	6	1	2026	Muzaffar	Fetched all wagons	a20d5c76-b462-4e70-8384-e035703845ba
\N	6	1	2026	Muzaffar	Created wagon: sali	41db6164-2c97-4493-beb8-70cd961f1f06
\N	6	1	2026	Muzaffar	Fetched all wagons	29e22845-e02d-46f3-9af1-ff542e1eb3a2
Muzaffar	6	1	2026	0000	Fetched products for shop	7b7274c1-0d6f-4557-b9db-32ef6055aebd
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	c6ef5936-e1ca-429a-bd6a-66c54dde9a2d
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	1a38cc69-696f-4f17-ad9b-cfca7594accc
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	e88af149-26e2-43f7-98db-0eb797fa7f22
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	a45ce6e7-4a17-4577-a96b-b7ac6dceea54
Muzaffar	6	1	2026	0000	Fetched products for shop	1102e57f-2ed5-4075-b50f-fab41b920c17
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	0ba6c769-3fde-4865-95bd-99e792c7f42b
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	a1a02c10-8e09-4190-bceb-0ba0cf41f5aa
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	dee78fd9-bcc0-43df-8724-1ec3e903504f
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	5212ba92-e17b-496c-8ed4-57a142139e5d
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	16b26f82-e030-4c1e-afad-5155914aaa09
Muzaffar	6	1	2026	0000	Fetched products for shop	5a1742ad-ed38-4b2e-a32e-857d60752bfb
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	47acb2a6-a9fe-4bf2-9b19-c298cea0eccb
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	347e193e-ab6f-4b50-bf6c-85fc905fa8a9
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	cc87ee0a-222b-4f1e-86b0-b9b257bf15e6
Muzaffar	6	1	2026	0000	Fetched products for shop	7f04bc12-4a6b-4ad4-8900-38e3f6df5f73
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	15829527-2726-4199-ab70-47f433bfd434
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	cee388d4-6da6-4707-afc1-6da42ca47a8f
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	509020c6-23ec-4ee8-8881-8895bfd0e2ed
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	855baa2d-f15b-4939-b3cf-f5123c139945
Muzaffar	6	1	2026	\N	Fetched all categories - count: 3	ac9ba3f4-e116-4416-859d-66800dc5b4ce
Muzaffar	6	1	2026	\N	Category created successfully: lazer guruch	02aad200-efaf-44a5-9a07-e17777f226bf
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	ef20913b-b3d1-4874-aaeb-084f9f9432fe
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	cd6f2c50-95f2-4ffa-ae80-7e80083d40e2
Muzaffar	6	1	2026	0000	Fetched products for shop	641d7281-d02a-427f-b36f-6f7e150c75e7
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	0c714f16-e8bf-4f80-9485-6b471d69e221
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	3ad943ec-cc71-4703-a58e-e5709f8ff450
Muzaffar	6	1	2026	0000	Fetched products for shop	37a2ece9-23c7-4c04-b39f-9b56e7870a30
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	0f75d71a-4072-46f9-af7b-cf6b41fc4e13
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	c46af0fe-39cb-4015-9588-c9deef2f35c6
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	624622e2-1288-428c-ab94-51a2b6ccbc2c
\N	6	1	2026	0000	Product created: c3e111f7-da9b-4645-bad1-b08582aef8ba	b99abb01-cb86-4cae-83eb-3e9f9019d0cc
Muzaffar	6	1	2026	0000	Fetched products for shop	fda738c3-55f3-4721-a03a-9f58d6a5571b
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	2286fd7a-df0e-4e99-bc74-21122977ea10
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	56d57f4d-73da-4994-b7af-9eda03546f0c
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	f867543f-844d-4ff0-94f1-3fa5a013964f
Muzaffar	6	1	2026	0000	Fetched products for shop	bbe8eb69-7284-4ef5-9dea-b4946158cb3e
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	db4b801b-a84c-4ef8-b67c-1d026550b47d
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	a53da5b6-e074-497f-91ec-84b8d1e214b4
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	575c195a-5c3d-4e85-b805-4e2a9f74587a
Muzaffar	6	1	2026	0000	Fetched products for shop	e0c2082c-1bb8-4a51-9c6b-e6bde7ecd190
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	6f0dd4c8-3aca-46a9-81ff-27a9861c9330
Muzaffar	6	1	2026	0000	Fetched products for shop	6b1d71d8-96b8-444a-83af-fe0a6a445b18
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	c36055fb-22aa-4de2-81a6-02448fed803e
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	04e2cd5c-62d2-4f20-b6e6-63d9076b8eaf
Muzaffar	6	1	2026	0000	Fetched products for shop	6dda74dd-f345-4c07-98ec-dc7a503e2b24
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	62bdbb8a-4c8b-4155-b09a-f7e4688d5793
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	de4bd34d-4799-4b83-9ebd-ca0261927127
Muzaffar	6	1	2026	0000	Fetched products for shop	a4578c37-100c-4a92-afae-bfbe51129863
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	d4ab0415-144d-4f13-bae6-8faee18fc7d4
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	c37876b7-a612-4114-80c5-8946413ac894
Muzaffar	6	1	2026	0000	Fetched products for shop	62b8dc5c-7e8f-4af2-aee9-15cc09d59044
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	6f860733-f369-4ca6-80a5-a955c2bfb94d
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	9c14717c-2f0a-4533-806f-d6df39c23474
Muzaffar	6	1	2026	0000	Fetched products for shop	ae1f5cf2-37ce-4067-9609-94e350b95ba8
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	cef4ad18-99d2-4d59-9c22-bcaf1700440e
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	91a88790-0c82-4ad0-a7b7-b12cec62aa81
Muzaffar	6	1	2026	0000	Fetched products for shop	4e6c1289-1db6-40e4-8fe4-ba3182237c22
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	e5cccfdd-5f7c-49d5-848b-317e5e723680
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	00e15be2-1c28-4cc9-8acc-36984cb8ffb6
Muzaffar	6	1	2026	0000	Fetched products for shop	9051b024-ea48-4640-a0f0-f822b026c620
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	caf12933-627c-422c-818c-b5b57e848ecb
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	c356353a-c99e-427e-89d2-555990903be8
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	cdb486f9-b2ec-4f91-8c76-5cfc092aaf1b
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	3b2d50ff-0ee9-425c-9734-fee5296f145a
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	3d678ce9-fa09-442d-a214-6dd8b58a38ca
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	d7255cde-8fff-4364-9da5-19fef7adaf3e
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	7bd2fca6-1c01-4195-9945-0667f4bb42b1
Muzaffar	6	1	2026	0000	Fetched products for shop	4e732922-9b5e-4450-a42b-063b209fa717
Muzaffar	6	1	2026	0000	Fetched products for shop	baa1bbce-6b48-4188-86f2-beee5d489d11
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	8ce481b2-8221-4c74-9ac8-719a9b03c3b4
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	41c70bd4-cc26-4d63-83c6-3d0d8e98a556
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	6cd74e4c-2f15-4c1d-b8cc-fae1b34b3c7d
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	72ecf46c-2740-4867-b44d-213d126eea6d
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	5102ef42-9837-4d84-b88c-7884861b153c
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	6c934fc3-9283-4fd2-80d4-5a2e9f406faf
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	961e97c7-7e7f-4daa-983f-b5c46ba44d38
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	d2ad66f7-4032-4650-8ad1-d6db5a42783d
Muzaffar	6	1	2026	0000	Fetched products for shop	4acb999d-b67d-4131-8391-88b28b6c8c99
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	6e67a34f-abdc-42c9-b764-735a30961bff
Muzaffar	6	1	2026	0000	Fetched products for shop	af690fe6-6b75-441d-bc32-e33fedea4c76
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	ecb40a92-c582-4f72-83cc-44c2d3841d56
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	3933a9f0-a64c-49c4-b144-ecfca57b3708
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	70860751-86ed-4d62-b49a-56aa812374c3
\N	6	1	2026	Muzaffar	Fetched sale by ID: 232192b8-27fe-49d0-a456-6b3ae2e0de25	0f5f8bda-4aa9-4193-8f31-3fa50efb25d6
Muzaffar	6	1	2026	0000	Fetched products for shop	aa4d7b46-0d14-47d1-a2e3-3268c463f5f8
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	4d8f9212-9fdc-4820-bc49-5c79df29de7b
Muzaffar	6	1	2026	0000	Fetched products for shop	656e15c3-d4da-4ceb-9580-9a137e952ed9
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	5edee5bb-f057-48d6-9476-04bb99a5c303
Muzaffar	6	1	2026	\N	Fetched main finance statistics	f152d376-36ce-4225-bfe0-f9038b61aaee
Muzaffar	6	1	2026	\N	Fetched main finance statistics	dba37748-714c-4ae2-92fc-3698c375686f
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	9cd2e375-5313-4dfd-869b-d819e9c45f72
Muzaffar	6	1	2026	\N	Fetched high stock products	5219fdd4-bfef-410a-9769-d86e8f8d4615
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	0bb9aed3-48a5-44d2-bcd5-d836b410783f
Muzaffar	6	1	2026	\N	Fetched high stock products	ce019ea0-aba8-434d-b5f6-83e4fcb26c2c
Muzaffar	6	1	2026	\N	Fetched low stock products	3845d6f1-213a-4109-b54b-ee9e1b03571e
Muzaffar	6	1	2026	\N	Fetched low stock products	e4f46c7b-99b7-4f09-a6c2-54914616049e
Muzaffar	6	1	2026	0000	Fetched products for shop	0988a85f-e19e-4dcd-8571-fe623b649a32
Muzaffar	6	1	2026	0000	Fetched products for shop	b5688d04-6f18-417e-a9d0-a9a3b6e1ace6
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	c5f202ce-354f-404f-b236-7c667bc783bb
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	43d42075-eb0c-4f62-b0c7-2d9b364e53c1
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	46f4b134-4d90-4a3b-b5fb-695bccdd268f
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	03fbfc51-9e8f-41b7-bdcc-a67bdcc53a4b
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	65eb64b0-ea13-4031-b640-7b7595c7205e
Muzaffar	6	1	2026	0000	Fetched debt statistics	b7fb4622-d4a1-4de8-8487-222bdb7ad7f4
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	dd9792b3-bcae-49dc-a8e9-d0336c73f6ad
Muzaffar	6	1	2026	0000	Fetched debt statistics	e1086cf4-a325-48cb-922b-fd199a43b1dd
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	3c5600ea-bd4d-4d3d-aa86-9631a65d07f3
Muzaffar	6	1	2026	0000	Fetched debt statistics	2eb5a753-819b-4b28-8755-e45dd254f055
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	66e38212-5efb-4984-98cd-d7314063db4d
Muzaffar	6	1	2026	0000	Fetched debt statistics	73de3231-da13-4df1-ac7c-1f8ab70d2e46
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	51d8e66a-28b8-4ffc-887a-5361636857a1
Muzaffar	6	1	2026	0000	Fetched debt statistics	2631550e-5f7a-4563-884d-dac693d5254a
\N	6	1	2026	Muzaffar	Fetched all wagons	2608a8d3-1f14-4889-939b-080de6785aa5
\N	6	1	2026	Muzaffar	Fetched all wagons	e84c07b8-27c4-42e6-897b-ae3b74750668
\N	6	1	2026	Muzaffar	Created wagon: kzhbishbfshjdbhj	f50a893f-ee41-4f5b-b25a-5dccbd98cebb
\N	6	1	2026	Muzaffar	Fetched all wagons	7ebc0d43-89cc-47a5-bb7b-bec10efafeec
\N	6	1	2026	Muzaffar	Created wagon: sdggdf	a98224eb-063a-491a-af3d-76d52c3d1646
\N	6	1	2026	Muzaffar	Fetched all wagons	9cb09c3e-cd38-490a-80b5-49ec986f9d4d
\N	6	1	2026	Muzaffar	Created wagon: blablabal	0bfe1e83-e8c7-4328-a9e1-dfa6d9bfb5d5
\N	6	1	2026	Muzaffar	Fetched all wagons	c3452f37-4cd7-4efa-aab7-da5141058034
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	bc23ce82-7439-41c5-aec5-fd52fe84c206
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	373226d8-267c-4523-a2f0-1ca9e5f0491b
Muzaffar	6	1	2026	0000	Fetched debt statistics	155ae479-2422-4b8f-ab53-6737b144fd3d
Muzaffar	6	1	2026	0000	Fetched debt statistics	c722619a-0fc5-4de9-b798-cf325af5ae2c
\N	6	1	2026	Muzaffar	Fetched all wagons	0c9db371-8158-4c21-91de-f9bd9d9a7e88
\N	6	1	2026	Muzaffar	Fetched all wagons	f3be9754-8d1c-45ad-809f-8ddd61f022c4
\N	6	1	2026	Muzaffar	Updated wagon: 1602bd9b-9919-4b24-b05e-9cd7212f6ef0	bfaf78b2-df56-40dd-bbcc-eebd23e84911
\N	6	1	2026	Muzaffar	Fetched all wagons	f1371ffb-962e-4a03-ab55-71abfe3f8d03
\N	6	1	2026	Muzaffar	Fetched all wagons	6cad6825-0bb5-4dfb-a148-5d2a963170a3
\N	6	1	2026	Muzaffar	Fetched all wagons	978e8675-65da-4627-a557-03440ccf34b4
\N	6	1	2026	Muzaffar	Fetched all wagons	5b05858c-5ff1-4a6b-b83d-e998edb921ac
\N	6	1	2026	Muzaffar	Fetched all wagons	bc314340-beef-491e-8b4e-4e8233055b57
\N	6	1	2026	Muzaffar	Fetched all wagons	72a5fd07-fa31-4396-9bf0-805115346349
\N	6	1	2026	Muzaffar	Fetched all wagons	e4eef02f-be37-49dd-a2b8-8a747af09bb3
\N	6	1	2026	Muzaffar	Failed to update wagon: invalid input syntax for type uuid: ""	72105e0d-55bf-47af-ab10-409cd3256272
\N	6	1	2026	Muzaffar	Failed to update wagon: invalid input syntax for type uuid: ""	4504386d-83a5-4678-b501-89a15fce0657
\N	6	1	2026	Muzaffar	Fetched all wagons	43da61cd-ef1d-451b-9983-fe98b42fbe9d
\N	6	1	2026	Muzaffar	Fetched all wagons	4cb3bf04-2a05-410b-af92-9a20d15a5a53
\N	6	1	2026	Muzaffar	Fetched all wagons	6890beff-4830-4319-88c8-077f684ecc60
\N	6	1	2026	Muzaffar	Updated wagon: cb05b08f-cb3a-40d3-bc64-c7893383eea0	2fb29ac5-0c66-4375-a0a8-45832b3525c5
\N	6	1	2026	Muzaffar	Fetched all wagons	a8d82889-45b8-42e7-82c4-845ca34a54ae
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	d89e4b75-19ea-47a4-9c80-23bdc7b789e1
Muzaffar	6	1	2026	0000	Fetched debt statistics	a7a4137e-eafe-4337-b859-a1832e376339
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	02c66ea9-993a-426d-85a6-6c429c7ff243
Muzaffar	6	1	2026	0000	Fetched debt statistics	a9a6f2f0-8a1e-49af-8bce-16da0fe509f5
\N	6	1	2026	Muzaffar	Fetched all wagons	78292b75-4393-4a68-8c69-ba6e12908040
\N	6	1	2026	Muzaffar	Fetched all wagons	546b49e7-692b-497c-851d-7b6908724538
\N	6	1	2026	Muzaffar	Updated wagon: 1602bd9b-9919-4b24-b05e-9cd7212f6ef0	30c7bac2-7e07-442b-888f-0229e7693d26
\N	6	1	2026	Muzaffar	Fetched all wagons	08a77bb7-9556-48d4-a98c-3113c76f6536
\N	6	1	2026	Muzaffar	Updated wagon: 1602bd9b-9919-4b24-b05e-9cd7212f6ef0	8dfd5169-da74-45c5-b4f7-767bb0b5cca7
\N	6	1	2026	Muzaffar	Fetched all wagons	e4a8f421-3fa8-4b7e-aa43-2ccaa8e5588b
\N	6	1	2026	Muzaffar	Fetched all wagons	e725263a-6876-45f8-89a7-8413028128d0
\N	6	1	2026	Muzaffar	Fetched all wagons	4092e6ba-9eda-4a2d-813a-f95785a2eea6
\N	6	1	2026	Muzaffar	Updated wagon: 1602bd9b-9919-4b24-b05e-9cd7212f6ef0	6f1620eb-b2f4-424d-836b-6901347ff80f
\N	6	1	2026	Muzaffar	Fetched all wagons	130d4461-8810-4d18-8858-2d970b47625b
\N	6	1	2026	Muzaffar	Updated wagon: 1602bd9b-9919-4b24-b05e-9cd7212f6ef0	e58e4adb-9ab3-4115-bbfe-fe509ec625cd
\N	6	1	2026	Muzaffar	Fetched all wagons	d5adda52-e2ac-41a6-96fc-076806ff1eee
\N	6	1	2026	Muzaffar	Fetched all wagons	d111591f-f2f7-4f97-9818-f31234227b8f
\N	6	1	2026	Muzaffar	Fetched all wagons	49f674a1-8c8a-49ed-845d-5437f4f4606d
\N	6	1	2026	Muzaffar	Failed to create wagon: syntax error at or near ")"	bcf159d9-ed35-4481-b45b-c3a528bbacd8
\N	6	1	2026	Muzaffar	Failed to create wagon: syntax error at or near ")"	ae04d2dc-ccd4-41e6-bac9-105d65faa16e
\N	6	1	2026	Muzaffar	Fetched all wagons	17d7779c-8d96-4f5e-a33d-4340cbd1c125
\N	6	1	2026	Muzaffar	Fetched all wagons	0bc4ce73-e2ce-49f2-86a1-5e414c2869fd
\N	6	1	2026	Muzaffar	Created wagon: dsd	8a7180f6-9d89-47cd-b4db-ed6ec1b1aee2
\N	6	1	2026	Muzaffar	Fetched all wagons	0fc9ec20-5b2b-4987-b078-ef1aeb02739d
\N	6	1	2026	Muzaffar	Created wagon: baqir	f236d068-5dba-437b-ad75-278e0107491f
\N	6	1	2026	Muzaffar	Fetched all wagons	c1016f85-5043-4eaf-86ae-e1fa4a500252
\N	6	1	2026	Muzaffar	Fetched all wagons	15aa5551-7159-4e25-ba19-53b40216e43d
\N	6	1	2026	Muzaffar	Fetched all wagons	243de6ee-ca7e-4b1e-b196-0703c930421d
\N	6	1	2026	Muzaffar	Fetched all wagons	459556eb-1ae2-48fe-840b-558243ec90d3
\N	6	1	2026	Muzaffar	Fetched all wagons	e3df778e-dead-4cf2-8753-c95956d8ae39
\N	6	1	2026	Muzaffar	Fetched all wagons	0d0d01f1-edf9-4b1e-889a-7e253808fe6f
\N	6	1	2026	Muzaffar	Fetched all wagons	16d92e8d-0a53-445d-9835-603752395130
\N	6	1	2026	Muzaffar	Fetched all wagons	6a2042ec-06af-4d36-9f8b-1dfcacec5244
\N	6	1	2026	Muzaffar	Fetched all wagons	1ce64483-8dc7-4a56-8477-692fbd38b848
\N	6	1	2026	Muzaffar	Fetched all wagons	03c6db82-d19c-4e1e-b522-5afbbed7931d
\N	6	1	2026	Muzaffar	Fetched all wagons	d39bfe9b-80bc-4124-8da4-a5e67a3dbf3c
\N	6	1	2026	Muzaffar	Fetched all wagons	40f7f7e2-70cf-4121-8228-64802ffd8483
\N	6	1	2026	Muzaffar	Fetched all wagons	3dc1d98d-48de-4190-afdc-5e961f352b0d
\N	6	1	2026	Muzaffar	Fetched all wagons	09609b04-3c76-4f75-95e9-fab349d14e8d
\N	6	1	2026	Muzaffar	Fetched all wagons	9ecd2ecb-05eb-4f94-88fe-4cd21f6e767f
\N	6	1	2026	Muzaffar	Fetched all wagons	6b4dd293-bd99-4bf9-8170-88d459b83b88
\N	6	1	2026	Muzaffar	Fetched all wagons	23329dd6-9cff-44e7-b9be-6d26eaf4d683
\N	6	1	2026	Muzaffar	Fetched all wagons	bc493523-3e79-4d97-92d5-801933c72924
\N	6	1	2026	Muzaffar	Fetched all wagons	420bb554-62cb-4118-840a-9f0bdb4d9f3b
\N	6	1	2026	Muzaffar	Created wagon: saloomat	9778a067-87c3-44cd-a456-a198bef215a9
\N	6	1	2026	Muzaffar	Fetched all wagons	e8a815bd-581d-4b54-8e28-6ee4a5264d41
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	dc8b78e3-3fca-49f3-aeff-85ffbe085a88
Muzaffar	6	1	2026	0000	Fetched debt statistics	096f56ac-6558-492f-bf21-8a22cfbf4c8e
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	2624b5d0-6f13-4030-8c30-92be0d58f50b
Muzaffar	6	1	2026	0000	Fetched debt statistics	82fdb4f3-15a6-419f-8e3d-ae2d034c2157
\N	6	1	2026	Muzaffar	Fetched all wagons	b85ff161-9b60-47c4-b7e5-dbf251134500
\N	6	1	2026	Muzaffar	Fetched all wagons	547a63c0-0629-4ce9-b7ad-16c733f31823
\N	6	1	2026	Muzaffar	Fetched all wagons	5240c661-15c1-42e1-b3e0-0bbcf89d5d1b
\N	6	1	2026	Muzaffar	Fetched all wagons	81b0849a-aac9-4910-b520-b455da6eeb64
\N	6	1	2026	Muzaffar	Fetched all wagons	466f062c-67ce-424d-b357-a4003a629f2c
\N	6	1	2026	Muzaffar	Fetched all wagons	aa1a86f3-e27a-44bf-bca8-a44632ac7544
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	a084db94-acd8-4861-b2cc-b5c52241edfe
Muzaffar	6	1	2026	0000	Fetched debt statistics	84e6ca63-3904-4019-aa09-bee957b974a0
Muzaffar	6	1	2026	0000	Fetched debt statistics	8be90d1a-a037-43f8-9456-51d7c50f8ea2
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	d97e5c49-bca0-4bde-ad9e-4d57a1f39d7c
\N	6	1	2026	Muzaffar	Fetched all wagons	55d7252b-1e0d-4333-a562-f09f2e744c34
\N	6	1	2026	Muzaffar	Fetched all wagons	8ceed600-cfd7-4947-8d46-df55bc808d1e
\N	6	1	2026	Muzaffar	Created wagon: saloooom	210c41c0-0e48-4daa-95e8-d7c30d677635
\N	6	1	2026	Muzaffar	Fetched all wagons	5375fc75-cac3-4e30-9f99-4ecc06b96f0a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	9d1dd1cb-d5a4-4998-8e68-329dc8aee45e
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	c46addfe-4d8d-456f-8c66-fdea5035e70c
Muzaffar	6	1	2026	0000	Fetched debt statistics	2f77f8f2-da1e-4325-8c7b-cfcfe8ca122c
Muzaffar	6	1	2026	0000	Fetched debt statistics	65c361ac-d443-4258-8503-ad5406ca9613
\N	6	1	2026	Muzaffar	Fetched all wagons	bc406689-010f-4a7d-ba32-2df1c035fcd6
\N	6	1	2026	Muzaffar	Fetched all wagons	ac447d95-e778-46a5-bb01-d639584add6a
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	d2440882-7aeb-4989-9c47-86a3f5659645
Muzaffar	6	1	2026	0000	Fetched debt statistics	85aa36cc-8b8c-4600-871d-853cd8aec2f4
Muzaffar	6	1	2026	0000	Fetched debt statistics	ec46f716-e25d-4ae1-a3cb-2e0db9c7f02d
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	3b7bfd2f-2e2c-4492-9a26-b01bbc24e684
\N	6	1	2026	Muzaffar	Fetched all wagons	fbc388cc-f199-4d0b-b50f-5909a9233a2a
\N	6	1	2026	Muzaffar	Fetched all wagons	b60a8ddc-9b95-4d05-b17c-035fa022c93f
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	29fb9662-5cef-4b06-a4b5-29f784c86e2b
Muzaffar	6	1	2026	0000	Fetched debt statistics	f630cee6-146f-414e-a80f-05f7ef664161
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	8419cd0b-df04-48a1-8343-da9d2b9d27e2
Muzaffar	6	1	2026	0000	Fetched debt statistics	540ae2b3-d679-4dcd-8495-f3b29addca5e
Muzaffar	6	1	2026	0000	Fetched unreturned debts - count: 6	beee2a72-0e37-42ff-8328-166a55b512c1
Muzaffar	6	1	2026	0000	Fetched unreturned debts - count: 6	093a3165-a33a-461e-830c-a9df6baa0866
Muzaffar	6	1	2026	0000	Fetched unreturned debts - count: 6	11d39d40-e28c-40b4-b5bc-bcfde4e6099c
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	47fc661d-390d-444f-8385-e7f589670ebe
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	dcec67f7-edc7-45a9-ba2b-1bb3176884ac
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	f9dd8707-c8eb-489b-af90-2c4e114d3c38
Muzaffar	6	1	2026	0000	Fetched debt statistics	adc2ecb7-c888-4aea-badf-1c2e0f7642ce
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	b17c4059-cf1d-4ba0-a83e-7c9d638a114c
Muzaffar	6	1	2026	0000	Fetched debt statistics	0d345946-09e7-43b6-a878-74180365bbcb
\N	6	1	2026	Muzaffar	Fetched all wagons	00c13869-3429-4351-8aff-3827c3898c0d
\N	6	1	2026	Muzaffar	Fetched all wagons	baf6a9ad-611f-4c86-81b9-7435de117006
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	474f7e4b-0baf-4076-b8c8-f5a67c9d5198
Muzaffar	6	1	2026	0000	Fetched debt statistics	dbcb98ff-5b48-46cd-8698-6667b44fc784
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	52de7897-e658-412f-8cd5-0a31419e9cd8
Muzaffar	6	1	2026	0000	Fetched debt statistics	bdaf8f81-3703-42df-97f9-5c7d81273912
\N	6	1	2026	Muzaffar	Fetched all wagons	64da9cd1-78bc-4479-ab69-db165011fd28
\N	6	1	2026	Muzaffar	Fetched all wagons	8027e037-c1f9-499c-9635-13bf7a6cf821
\N	6	1	2026	Muzaffar	Deleted wagon: faca95b4-0609-4d27-b5cd-6573aff3465a	4651a843-72de-4236-92a3-36295c7448b4
\N	6	1	2026	Muzaffar	Fetched all wagons	3eaa3170-ee4e-477a-8c9b-8c01f6466d5b
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	5726e795-0117-4d6e-ab2b-67a46220b413
Muzaffar	6	1	2026	0000	Fetched debt statistics	c18b4714-9764-4200-af4f-5210d803c2ec
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	f908c497-4ce6-4c7a-a5d2-faafd224124d
Muzaffar	6	1	2026	0000	Fetched debt statistics	5665a05f-3d7e-42d6-a1df-d564acfa066d
\N	6	1	2026	Muzaffar	Fetched all wagons	0616cfbf-453d-4dc1-9480-6998782e62f9
\N	6	1	2026	Muzaffar	Fetched all wagons	cf0798e5-23e1-4a75-bc7e-2f71342d267e
\N	6	1	2026	Muzaffar	Deleted wagon: 02b8688d-cd90-45ea-844b-0df0f82debfd	17dcc099-6e3c-4e7f-994d-e85aff8cf980
\N	6	1	2026	Muzaffar	Fetched all wagons	9778b1b6-10db-41f7-a6c7-79a76191566e
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	798ec5d5-eb2e-4baa-80d8-61ab336e239a
Muzaffar	6	1	2026	0000	Fetched debt statistics	ea01e4b2-f6cc-447c-9f62-7daf456f4eb3
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	f4bee49f-509b-4edd-9fbd-0a15e46c0e90
Muzaffar	6	1	2026	0000	Fetched debt statistics	98dd96df-f55f-4b28-a7c9-338a8751d50e
\N	6	1	2026	Muzaffar	Fetched all wagons	30c572e0-405e-4a1e-989c-da0f94e24861
\N	6	1	2026	Muzaffar	Fetched all wagons	119b58e2-c479-40ed-923a-955456e2ace0
\N	6	1	2026	Muzaffar	Deleted wagon: cb7082bc-e0ae-43c9-954b-82ddcca9ef80	c3185729-0945-4798-8b2c-02a5c334042e
\N	6	1	2026	Muzaffar	Fetched all wagons	36e846a2-acd5-4ba2-ae11-414792d05800
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	47681342-286d-455b-9082-a21284c57c00
Muzaffar	6	1	2026	0000	Fetched debt statistics	02442c09-ca6b-4097-aea9-95569ee9e910
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	f1933db5-7d64-43ae-a101-925136247b05
Muzaffar	6	1	2026	0000	Fetched debt statistics	b2cb299f-10bc-46ce-8860-3107ccfd5eee
\N	6	1	2026	Muzaffar	Fetched all wagons	d468bdb4-c559-40c9-9bcf-c1141bc5b3ea
\N	6	1	2026	Muzaffar	Fetched all wagons	042c8b8d-b01f-44cc-87cf-f126e2737a41
\N	6	1	2026	Muzaffar	Deleted wagon: 447739ab-6bc0-4ae0-896d-c8a1de53175b	9cc70634-ac06-4122-a266-c3c8582e9200
\N	6	1	2026	Muzaffar	Fetched all wagons	f72862ba-7be1-48d3-abfe-5051cbfdec12
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	c4ea037a-60f1-4458-870d-097bdf0b183e
Muzaffar	6	1	2026	0000	Fetched debt statistics	7f9bd68f-cb28-4f25-bf92-830f26fec3e3
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	4e4a2cfd-89b2-4d1e-9ced-22b3ec622c52
Muzaffar	6	1	2026	0000	Fetched debt statistics	6afdc245-b830-4f26-98fe-43b3b1edc364
\N	6	1	2026	Muzaffar	Fetched all wagons	1454dae3-efff-45d6-9beb-f433bd51c563
\N	6	1	2026	Muzaffar	Fetched all wagons	261ba365-4d0c-4ed0-833f-b556b8be88b4
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	d20d5fb2-6d58-4cdb-be97-a6f295c6a0c6
Muzaffar	6	1	2026	0000	Fetched debt statistics	8d7540f5-7d6e-41a8-8abf-bbba9fcd9511
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	ec7c3d2c-ef30-4aec-982d-adc47a4a5815
Muzaffar	6	1	2026	0000	Fetched debt statistics	69890650-8c7a-4ba0-8d84-53507129051d
\N	6	1	2026	Muzaffar	Fetched all wagons	575c24e3-83ed-4680-a13b-80a15ccf92cb
\N	6	1	2026	Muzaffar	Fetched all wagons	e24d3a08-aa25-487d-b533-3a51252fd1af
\N	6	1	2026	Muzaffar	Deleted wagon: 19081482-02e8-48d2-a57f-37c512b0ff61	79a712e5-254f-4e38-ab14-eda29db7f673
\N	6	1	2026	Muzaffar	Fetched all wagons	217d5f67-945f-4477-9f4a-55578fa769ef
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	2035d08c-eed5-4b89-ab9d-6e68ecdf1532
Muzaffar	6	1	2026	0000	Fetched debt statistics	0ba6a1d5-5c0b-4dc0-8de4-392fdbd6686d
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	94da0f6d-a7d5-4d55-a009-70d1b48fc8cf
Muzaffar	6	1	2026	0000	Fetched debt statistics	940a1fe3-dd15-41f9-93e3-df907eb327fa
\N	6	1	2026	Muzaffar	Fetched all wagons	2ce99661-597b-4436-afd6-14ed398b4af3
\N	6	1	2026	Muzaffar	Fetched all wagons	844693a2-8422-45c9-b071-72715ba217ed
\N	6	1	2026	Muzaffar	Deleted wagon: 1602bd9b-9919-4b24-b05e-9cd7212f6ef0	0cc36c19-28ad-48d3-8a93-174ebbcc4654
\N	6	1	2026	Muzaffar	Fetched all wagons	f7a68cf4-58eb-46db-a78c-6939d774c69f
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	ac2cc750-5d66-4603-9938-a87dbab862c7
Muzaffar	6	1	2026	0000	Fetched debt statistics	d1a6dad5-5e99-4d7e-b813-875f7d82c259
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	20467852-db7f-433e-bac5-948e75c9b074
Muzaffar	6	1	2026	0000	Fetched debt statistics	87accbd5-e1e2-474b-a78e-e31b12afc23d
\N	6	1	2026	Muzaffar	Fetched all wagons	3c921740-5aa6-435b-ac44-6cf70c8bcc8a
\N	6	1	2026	Muzaffar	Fetched all wagons	efe7d95f-fcf8-4313-8328-d20278640ee7
\N	6	1	2026	Muzaffar	Deleted wagon: 54abdb14-d097-47bf-9ef4-5a9b98ad95a0	30fe6daf-2641-4c5e-ad4b-1682df72bb6b
\N	6	1	2026	Muzaffar	Fetched all wagons	bfc5c3c5-ab54-4df8-8ced-d8ed7f178959
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	c9a89312-cec0-49a5-90ac-04ac5ee3d17d
Muzaffar	6	1	2026	0000	Fetched debt statistics	0352f1e8-266b-464a-9870-1aa204285884
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	593de9a2-1af2-4dae-a223-e3c703ad6341
Muzaffar	6	1	2026	0000	Fetched debt statistics	a523f31e-ff5e-4228-910f-402f3d2dec37
\N	6	1	2026	Muzaffar	Fetched all wagons	a4301283-c412-44cc-82f9-c95c70baf4a6
\N	6	1	2026	Muzaffar	Fetched all wagons	01176bcb-91b9-4d4e-b63b-c31aa8c41060
\N	6	1	2026	Muzaffar	Deleted wagon: a3305c1f-b3d1-4e65-86f1-446e0cfdb471	edcb1909-7f26-4bf3-9e39-47c1d0a0f6ab
\N	6	1	2026	Muzaffar	Fetched all wagons	453158a5-571a-471e-a6d4-1b19cc245dfc
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	55829e63-cf86-466d-bc27-c45ef2fe91f9
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	023dd362-2929-486c-94ba-0578baca16fb
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	a5bacb8c-8863-428e-93dc-7be119109fed
Muzaffar	6	1	2026	0000	Fetched debt statistics	b448f590-aa68-4cdc-842c-28efebce5de4
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	859a5794-3860-45b5-84d1-31377cb244d7
Muzaffar	6	1	2026	0000	Fetched debt statistics	41506dbb-4273-40ec-8049-852d2efe5c86
\N	6	1	2026	Muzaffar	Fetched all wagons	23ea66dc-c8b0-4448-b0b0-20679d74ae10
\N	6	1	2026	Muzaffar	Fetched all wagons	a13f0144-d5f8-47ec-9daa-d309e2218141
Muzaffar	6	1	2026	\N	Database backup downloaded - tables: 14	29b6651e-622c-452b-adb8-3d64d810aa9d
Muzaffar	6	1	2026	\N	Database backup downloaded - tables: 14	da44ee0d-fbef-4085-a538-95a262a53b13
\N	6	1	2026	Muzaffar	Fetched all wagons	08ba7220-9e57-4ab4-87e7-34e7f8e2ec4c
\N	6	1	2026	Muzaffar	Fetched all wagons	0d109928-0261-4a2e-87a8-fb9b25b8f7af
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	88ef425b-c1d1-4855-b5b3-e7123f555b68
Muzaffar	6	1	2026	0000	Fetched debt statistics	1ff89264-84ca-4b85-83d8-74939f5b7297
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	6fab4708-8e00-4ebd-8149-7b9a30b1e814
Muzaffar	6	1	2026	0000	Fetched debt statistics	ce2e6e8c-c0d4-4fba-91c1-5e55f8d18ed6
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	6011ec63-97ad-4004-9c85-6b7fb022938a
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	f1d012e6-cb1b-47f2-9bd4-3847a06b5e0e
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	eaa44e01-a8f5-4547-9542-7bda5ec918c5
Muzaffar	6	1	2026	0000	Fetched products for shop	77517f5c-3672-4878-b81f-9eb421460b37
Muzaffar	6	1	2026	0000	Fetched products for shop	a1f442bc-eb9a-4cc5-b191-81b8e5dd4ea4
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	83907743-0e7d-40b2-ad0d-4845c00a9de5
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	c55dcafc-2a25-42a0-8793-f0c9c1cbbc66
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	8a5ce69b-720c-4703-a733-0c90839ffc45
Muzaffar	6	1	2026	0000	Fetched products for shop	62771c56-65e2-44f7-95a8-7a29743c1e34
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	5297e2f8-764f-4a90-bc23-d3158e1dc105
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	af3ec1da-b1ea-4bc6-916b-1967395b6669
Muzaffar	6	1	2026	0000	Fetched products for shop	9734d1ee-5725-46a4-9687-4b77161080d0
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	c3b54a1f-ca99-4e1a-b96f-9cb51c9ecf1c
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	d18218b5-558f-4700-a9c0-7545277aade5
Muzaffar	6	1	2026	0000	Fetched products for shop	5aa9015c-9fac-44e7-a489-03ed68d5c1fb
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	ffadfa51-78e2-4f97-8d8c-6fed8af43a4d
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	c16d70f1-95c8-44ec-a6d0-a6160d26e54a
Muzaffar	6	1	2026	0000	Fetched products for shop	c95f06b6-8937-4874-8f28-f212de921f91
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	0d5b049f-35a9-474b-881c-ad21f0df165b
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	f61a393d-f16a-42cb-85ab-8913996e3da4
Muzaffar	6	1	2026	\N	Fetched main finance statistics	f4d454ab-ac2d-446e-b90f-ed60cf5bfade
Muzaffar	6	1	2026	\N	Fetched main finance statistics	709b774d-dae4-4ec7-b43b-cd15a624d098
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	c044e2d3-e7b6-4bb5-9e16-068484029744
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	a5cdf117-a789-4122-b15c-ce560b289343
Muzaffar	6	1	2026	\N	Fetched high stock products	f1550267-ad38-49ef-8420-b8255984c27b
Muzaffar	6	1	2026	\N	Fetched high stock products	027e77b1-39aa-43fe-b559-0bd5ad4c2108
Muzaffar	6	1	2026	\N	Fetched low stock products	efc3e7bd-10c8-4440-beac-2124ea726b3c
Muzaffar	6	1	2026	\N	Fetched low stock products	ff56437f-2135-492d-a22b-3ed022761747
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	61d027cd-46d5-4767-9224-9f8166053a2d
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	868f376f-075d-483d-9897-defe54437155
Muzaffar	6	1	2026	0000	Fetched products for shop	af886be7-3930-48ec-92bb-a307ee58403a
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	4c93a4cb-604a-46f5-8937-ebd80881651d
Muzaffar	6	1	2026	0000	Fetched products for shop	356c91a0-7cd2-4e30-a5fb-f3f651083c62
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	7ba5e858-700e-44d2-b6e2-2b62ae5e0ad2
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	e4889a74-827b-4454-bc6e-9a800880361e
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	ca7014ba-37b2-46be-a277-62020acb5c2c
Muzaffar	6	1	2026	\N	Fetched main finance statistics	931e6d88-d313-4450-9acc-8daae4e2f58a
Muzaffar	6	1	2026	\N	Fetched main finance statistics	445c6c4c-f044-4662-8027-55bac01fa0c4
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	acd19155-8f7a-4d99-bef5-582fe0bd66b5
Muzaffar	6	1	2026	\N	Fetched high stock products	b9c58104-2d77-40f5-89ac-e75a71744d84
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	2be058da-cb44-42cc-ad6e-749dc09a6f08
Muzaffar	6	1	2026	\N	Fetched low stock products	bba0e88f-ec3f-4e1c-8422-7c840b4bbf1c
Muzaffar	6	1	2026	\N	Fetched high stock products	fbae17f8-4a4e-42ad-aeb1-d5307cbe4d5b
Muzaffar	6	1	2026	\N	Fetched low stock products	166982bf-e97e-40e0-8411-175b73218836
Muzaffar	6	1	2026	\N	Fetched main finance statistics	0ef20faf-e8a9-4102-bbfb-9685342b65c0
Muzaffar	6	1	2026	\N	Fetched main finance statistics	8263d63e-3497-43a7-8fc3-7abdfec138be
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	59925121-72ed-496f-bd84-8906574a9fff
Muzaffar	6	1	2026	\N	Fetched high stock products	7c0ff4a3-b261-4c4c-beb5-16f385e1bd8e
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	c826813a-c68d-4f64-946b-5247ad23de41
Muzaffar	6	1	2026	\N	Fetched high stock products	2dcd80a6-2371-4ac0-a163-6a6b68550fb3
Muzaffar	6	1	2026	\N	Fetched low stock products	e7d10e55-03cc-424d-862a-62b5dd08600d
Muzaffar	6	1	2026	\N	Fetched low stock products	9c572622-2c6d-405f-aabb-1ed4e45e445e
Muzaffar	6	1	2026	\N	Fetched main finance statistics	fc6963c7-c562-462c-a0b8-09e1d08e9fae
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	72cb0cbe-c640-422d-8ee5-a211c4c5656e
Muzaffar	6	1	2026	\N	Fetched high stock products	0eabc422-e412-4137-8a3e-7e0a9c05921f
Muzaffar	6	1	2026	\N	Fetched low stock products	c9951b8a-ecd0-4478-887f-3df5221f18d2
Muzaffar	6	1	2026	\N	Fetched main finance statistics	befb0c6c-9934-42fc-9af3-b6eb9003a009
Muzaffar	6	1	2026	\N	Fetched main finance statistics	97939cf8-a6b4-4de4-ba9f-348ad0d5b0ef
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	a13059c3-015e-4527-94e8-c1cf8e8e1f47
Muzaffar	6	1	2026	\N	Fetched high stock products	ec0d064c-90dd-4fe4-ac0e-aad96426566e
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	6e822f5e-9606-42b7-9d54-9f26a7f9770e
Muzaffar	6	1	2026	\N	Fetched high stock products	22b499c3-ca69-47d9-b1eb-a04c38f71dc9
Muzaffar	6	1	2026	\N	Fetched low stock products	c2a9f826-6712-47a4-b23e-1fcad6ac1257
Muzaffar	6	1	2026	\N	Fetched low stock products	6b72c912-cfff-4001-a43e-c2deb0249a1d
Muzaffar	6	1	2026	\N	Fetched main finance statistics	8d28342b-d8eb-4bae-a951-2b40ff0afb18
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	742d9c3a-c48f-4448-b591-897bf4ece316
Muzaffar	6	1	2026	\N	Fetched high stock products	23112e33-e446-4f27-be47-3ebdee50f1fa
Muzaffar	6	1	2026	\N	Fetched low stock products	6561a1ab-37bb-47ce-94e6-7e053aaf9d45
Muzaffar	6	1	2026	\N	Fetched main finance statistics	2960449a-581c-455b-9bdf-719563b9e282
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	429ed258-7a7c-47ff-ac84-ef76e8b8b423
Muzaffar	6	1	2026	\N	Fetched high stock products	aaa04e53-c447-4438-8e69-e5e92870ecca
Muzaffar	6	1	2026	\N	Fetched low stock products	6497b022-4438-4639-9cfb-2bc408a7175d
Muzaffar	6	1	2026	\N	Fetched main finance statistics	42ad7322-84df-4beb-8c1c-ba710763fdaf
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	96c36821-715b-4620-8c58-f70121f8285b
Muzaffar	6	1	2026	\N	Fetched high stock products	b19c7003-1cc0-452b-8b01-d9a5844e8854
Muzaffar	6	1	2026	\N	Fetched low stock products	3e2be121-e208-4fbd-8e03-e14ffedd630a
Muzaffar	6	1	2026	\N	Fetched main finance statistics	5a130794-d58a-4adf-88a5-9def0aaa9ea3
Muzaffar	6	1	2026	\N	Fetched main finance statistics	38b507b7-7821-4de2-ab59-6e8ff39c6c5b
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	cb5806c5-fa4d-434a-8159-9929e1719685
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	2ac48f15-9229-45d8-80a9-12b4f6871840
Muzaffar	6	1	2026	\N	Fetched high stock products	a6c3da48-f339-4a12-a1bd-c35f670142de
Muzaffar	6	1	2026	\N	Fetched high stock products	54471354-261d-470a-9784-3813d590707b
Muzaffar	6	1	2026	\N	Fetched low stock products	ef6adf0c-a9bc-4712-9b68-2b17f45b12fc
Muzaffar	6	1	2026	\N	Fetched low stock products	0ffa90c5-bcef-4a77-917f-85b307beadb5
Muzaffar	6	1	2026	\N	Fetched main finance statistics	56a33bd7-959d-45b7-ab23-64a368c7adaf
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	a419fbd3-a5b4-4d01-987c-91a45c7470e1
Muzaffar	6	1	2026	\N	Fetched high stock products	04701374-af3a-47e1-9ae0-51a5e13528fe
Muzaffar	6	1	2026	\N	Fetched low stock products	acc3c9e5-9098-4c0f-a944-cde7cdea80bf
Muzaffar	6	1	2026	0000	Fetched products for shop	f79a7b45-1f11-4e42-bcca-83ef01cefe89
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	fc26069e-610d-4767-97cd-69b762c41f7f
Muzaffar	6	1	2026	0000	Fetched products for shop	bd1bd3e5-5327-4c1e-a0e9-3a5a1e7eaf1b
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	e5ad6a74-bf99-4944-b669-48f20b1bcc54
Muzaffar	6	1	2026	\N	Fetched main finance statistics	d190b7c5-3bbd-4a85-81f1-80bcfd9427cf
Muzaffar	6	1	2026	\N	Fetched main finance statistics	e728d2d1-2e1b-47fc-8b02-8e372f60cae1
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	967a0e13-e202-46e8-a824-e2228393419d
Muzaffar	6	1	2026	\N	Fetched high stock products	3c620334-f23a-4528-baac-09fd611852c3
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	9904a7b3-49cd-4eaa-8dcd-bd5ae96111eb
Muzaffar	6	1	2026	\N	Fetched low stock products	00446748-e490-42f1-977f-c2e5d5b4ccfc
Muzaffar	6	1	2026	\N	Fetched high stock products	dd7f2064-a6e9-448a-b841-196a34894c82
Muzaffar	6	1	2026	\N	Fetched low stock products	27d2d652-8170-4b64-9af1-86a9febd53e3
Muzaffar	6	1	2026	\N	Fetched main finance statistics	825a12b5-9994-497d-9da5-dec2a897e279
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	b789e50b-eda5-49ea-980f-f381bfc77ad0
Muzaffar	6	1	2026	\N	Fetched high stock products	dba3baef-aca4-46a2-a98a-2b33514a886e
Muzaffar	6	1	2026	\N	Fetched low stock products	bee3af24-5f63-4a49-9ae1-24ba1538b555
Muzaffar	6	1	2026	\N	Fetched main finance statistics	063f1d33-5670-4300-a58b-54fc19e4970e
Muzaffar	6	1	2026	\N	Fetched main finance statistics	40144fbb-6bec-49c1-bbc1-a94b51a4991a
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	ce790a67-2e29-4afe-9412-5fecdb3d5f55
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	b0e842e7-475e-4df3-8045-c40874ed8b23
Muzaffar	6	1	2026	\N	Fetched high stock products	13dd9ff9-c2bd-4629-be9d-106892dcb050
Muzaffar	6	1	2026	\N	Fetched high stock products	c5d0983a-5077-4b61-a252-3dd75ec670a3
Muzaffar	6	1	2026	\N	Fetched low stock products	1bcf6424-1ba2-459a-9ef9-ba76ebd60f49
Muzaffar	6	1	2026	\N	Fetched low stock products	3b5fb40a-f86c-4757-954e-eaf76563bd57
Muzaffar	6	1	2026	\N	Fetched main finance statistics	48fc00de-09ab-40cd-b9b2-3edaf2de37cb
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	c838d9d7-5030-4ca4-b182-a4c2522c02c0
Muzaffar	6	1	2026	\N	Fetched high stock products	0d58c944-be04-442e-83ba-3e6311f41de7
Muzaffar	6	1	2026	\N	Fetched low stock products	8f0837fa-afe7-41dc-bcc6-75ce32851ed6
Muzaffar	6	1	2026	\N	Fetched main finance statistics	b26c4c63-476f-43f2-bdac-d9112feba472
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	86ad40d1-0a59-437a-8290-03fe62cdb2bc
Muzaffar	6	1	2026	\N	Fetched high stock products	cc1068f9-8d76-4490-abf2-92c727eae958
Muzaffar	6	1	2026	\N	Fetched low stock products	1125728e-80dd-4e3d-9c96-7a3e1fcc6991
Muzaffar	6	1	2026	\N	Fetched main finance statistics	d2225d04-0c49-4f24-93a1-4ed9ab75c1e9
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	5999efed-a62e-4cd4-b56d-7b7d06db07c6
Muzaffar	6	1	2026	\N	Fetched high stock products	6d64d401-6255-42c4-8342-3a2ab1ad3e35
Muzaffar	6	1	2026	\N	Fetched low stock products	7109eda4-0ed3-4e09-944d-451678285c33
Muzaffar	6	1	2026	\N	Fetched main finance statistics	cd579092-8b75-4b59-8b29-c70406a92261
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	d6e112da-0531-4af9-a9ff-5fdea4cbc530
Muzaffar	6	1	2026	\N	Fetched high stock products	3cca9bb5-1775-4017-80ca-956a599ad4b3
Muzaffar	6	1	2026	\N	Fetched low stock products	3a163768-7eb5-4d9f-a9d3-5cf46ca2591b
Muzaffar	6	1	2026	\N	Fetched main finance statistics	e6c8f23a-c43a-4d56-b40a-ecf0ec30d8ff
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	9fbbbcf6-39ca-4ea0-81a4-107204f3c8a3
Muzaffar	6	1	2026	\N	Fetched high stock products	aba12462-78e0-4066-9d35-8155201a84e8
Muzaffar	6	1	2026	\N	Fetched low stock products	dbc6f03f-3708-4a9e-9bb1-c51fd18660e8
Muzaffar	6	1	2026	\N	Fetched main finance statistics	ba2b8b18-e134-48d6-bfb7-2c972c55b414
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	c055c9d8-2503-4e23-a98f-ee51b217345c
Muzaffar	6	1	2026	\N	Fetched high stock products	b9ff673f-a986-4beb-be9d-6384fd3a00cd
Muzaffar	6	1	2026	\N	Fetched low stock products	c7d9b8b9-fd68-496a-b270-faf789c42391
Muzaffar	6	1	2026	0000	Fetched products for shop	7095c77a-0dc9-428b-aad1-60e4ac74cdd3
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	4a2d8431-4632-4402-ace8-95237d188c41
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	edcbdb10-5e54-452c-a33b-a1c5f4c079f7
Muzaffar	6	1	2026	0000	Fetched products for shop	14dade76-2d1b-4c2a-9474-5caa778bec65
Muzaffar	6	1	2026	\N	Fetched main finance statistics	a3c66bb8-92c4-463a-8553-c6a4915e0a09
Muzaffar	6	1	2026	\N	Fetched main finance statistics	f149deed-796c-4740-b2cd-730908a36aa6
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	32853a78-a13e-4977-8bb5-00eb30b0a9c7
Muzaffar	6	1	2026	\N	Fetched high stock products	8bf3ca10-9568-4fff-9f71-5a1345896c43
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	a8741ae6-1c51-4f85-8585-53bf23f0ac46
Muzaffar	6	1	2026	\N	Fetched low stock products	847c913a-3656-40e2-b519-1efd56b8fd14
Muzaffar	6	1	2026	\N	Fetched high stock products	4ac0f946-f166-419a-b833-73877c10b7b2
Muzaffar	6	1	2026	\N	Fetched low stock products	4192940c-1387-448c-986c-de0f34a4587a
Muzaffar	6	1	2026	\N	Fetched main finance statistics	b569c9f7-bdca-426e-88a9-2d98e31382cb
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	58492415-e771-45c1-aa82-a8a941743088
Muzaffar	6	1	2026	\N	Fetched high stock products	aabdd801-a526-452e-8dbb-7a85091522a2
Muzaffar	6	1	2026	\N	Fetched low stock products	05a19b89-59e3-42d9-947a-0e7d0ae550da
Muzaffar	6	1	2026	0000	Fetched products for shop	94297e97-a3a4-4bbc-a2d6-498f3a4461ae
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	c0b4472a-9d1c-46df-8199-6716b9422517
Muzaffar	6	1	2026	0000	Fetched products for shop	b49d6271-b8ea-4f6e-a05d-7da5cf897345
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	f497aaa5-306c-406d-98ed-ebfb5646b0a4
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	91eba360-b9ae-4ed4-97e7-6c2ce65ff2a3
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	47a5f507-e8c4-4378-a0c0-1f12754580df
Muzaffar	6	1	2026	\N	Fetched main finance statistics	7b5cc3f1-898d-46d7-b864-0994f71c8077
Muzaffar	6	1	2026	\N	Fetched main finance statistics	f147793a-cef7-4188-ac9d-11c7e8540a47
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	763e851a-5bc5-42b9-8a16-c33a1bd63882
Muzaffar	6	1	2026	\N	Fetched high stock products	e4b6ebb8-ac29-4b04-be6e-3a0656670034
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	fcf5f34d-1836-4824-a0db-47a571b70696
Muzaffar	6	1	2026	\N	Fetched low stock products	fdeac5c5-3267-4fed-9a92-017bbfdb9f2c
Muzaffar	6	1	2026	\N	Fetched high stock products	27cf26f9-9ddb-4acc-ba48-b13cf2444471
Muzaffar	6	1	2026	\N	Fetched low stock products	a37dde90-3fa3-4afa-ac37-5561e3e6433b
Muzaffar	6	1	2026	\N	Fetched main finance statistics	374b34cb-2101-4ea5-84ec-661e32d8137f
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	ec2d49a8-ee81-491c-8d82-026834648ff7
Muzaffar	6	1	2026	\N	Fetched high stock products	b15a2a7a-c176-48c3-8d8c-fde579c9eca2
Muzaffar	6	1	2026	\N	Fetched low stock products	834c6f75-17e3-4f1b-a550-3505606bf8de
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	1e1ac8d7-cec1-4597-affd-0db3036d21ef
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	4e18dd71-5317-4858-b93a-bedc9f8227b1
Muzaffar	6	1	2026	\N	Fetched main finance statistics	1a3672b3-4257-4f57-83c7-a79b04afe82c
Muzaffar	6	1	2026	\N	Fetched main finance statistics	ba381ed5-26ec-4692-af34-c1e5a1777fab
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	757fe2b7-f205-4a02-807c-7896875d57f7
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	bde3dda3-1e92-4e25-b6f4-f9d51e1610ac
Muzaffar	6	1	2026	\N	Fetched high stock products	b38a5137-963d-43e9-8693-c6e8598d5fa8
Muzaffar	6	1	2026	\N	Fetched high stock products	1891d601-62ca-4002-971c-e39c8267d7db
Muzaffar	6	1	2026	\N	Fetched low stock products	12e5c4e7-19f7-450f-87ed-8464e1ce5b27
Muzaffar	6	1	2026	\N	Fetched low stock products	2bd46a81-efaf-4eb1-b5be-701512bbfc88
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	58cde04e-9533-4685-86a5-c3845e1baacb
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	53a9468d-bd1e-4e46-ab92-79cf792087d7
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	52708479-f0d5-48b0-a7b1-d51b97fcba4f
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	cc70b5c7-2608-40f8-bd7f-e45dd0c55ef7
Muzaffar	6	1	2026	0000	Fetched products for shop	ccc0e27b-9a95-4454-9bae-081d3e7956e4
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	bcb89cb5-ef74-4e86-b8d5-c367e852e3ee
Muzaffar	6	1	2026	0000	Fetched products for shop	d1a92fb6-cb17-4f81-abac-5c2686d092db
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	cf5adb21-a2b7-46fa-8f43-fe30e87e3d18
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	35ae960e-65a9-4950-a870-13d2474cb06f
Muzaffar	6	1	2026	0000	Fetched products for shop	0a7b9c4d-e2fe-4a75-a6a4-94dc40ff1db0
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	50b194c8-12f1-4a5f-a90c-451e10ac2381
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	555b2c3b-35e7-4721-8393-fdff4432c854
Muzaffar	6	1	2026	0000	Fetched products for shop	db0945a6-0bb2-487c-8982-de82037b84ef
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	ff84c709-30db-434b-8cad-374f76691d76
Muzaffar	6	1	2026	0000	Fetched products for shop	c187fde4-bb85-4dd6-bc9a-f200a9c5bd12
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	355979bd-6ab6-4792-8204-c607abc43f10
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	8e48fd50-1de9-4f33-9d7e-00348b4c7fcc
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	f5fb20a7-f58b-4652-ac8f-a84f8726c221
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	86634d00-6ec3-491b-93a8-797b1b855527
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	a7b5cbd2-272b-455e-bb00-16a42a1c12d6
Muzaffar	6	1	2026	0000	Fetched products for shop	2b4037a0-dfbd-40f1-a498-83bc1660d644
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	eb006160-77c9-4165-b1c0-e1fefbc5eb8d
Muzaffar	6	1	2026	0000	Fetched products for shop	b83975c3-994f-46e7-b359-ebb49c1db4bf
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	75d62703-7e8b-4ad2-8627-ca939381de25
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	f0430afb-ba9b-4c2b-a67b-263dc1de431c
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	d2965920-92d0-4f03-b505-963a1f3fa784
Muzaffar	6	1	2026	0000	Fetched products for shop	1edf322c-cdd4-40c0-b1fa-2e540d263d26
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	942ef9c6-fb7f-4c89-a3b5-a0b627cb32c7
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	339c3eef-8edd-4262-85da-aa9b5300da8a
Muzaffar	6	1	2026	0000	Fetched products for shop	828e970d-2994-4ba7-a7df-b0ce24a5bdb5
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	86ca67d6-72ee-4446-9f45-5843a92b573d
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	7fbba418-1b5c-4be2-8e94-abcc1623477f
Muzaffar	6	1	2026	0000	Fetched products for shop	9fe52b49-ffd2-48bc-986d-6f5448fd03e7
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	3af56888-2f46-492a-93f8-876e44279b13
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	6bd5b037-fe11-46f1-bc9e-d01f54a67a3b
Muzaffar	6	1	2026	0000	Fetched products for shop	7f19dbc6-081d-4fc1-848b-e3b54e34b99d
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	1ef3fd58-ea63-445d-9a87-b9d38ac78056
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	7fadc1c1-0159-40f8-ba3f-ea8b4a80f562
Muzaffar	6	1	2026	0000	Fetched products for shop	9b083b4c-8cbc-43e8-bbab-68684ee684ee
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	229cd084-acc7-4136-b7cd-3964815bc771
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	404ffb7f-8e9f-4478-8bd7-610a299ec9fa
Muzaffar	6	1	2026	0000	Fetched products for shop	8bdc188f-6dd5-44f4-a722-d5810a05346a
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	cf89645a-d4c4-472c-b813-789d9287429e
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	fa8b5038-7c13-4945-b066-d14ac75d1caf
Muzaffar	6	1	2026	0000	Fetched products for shop	fe5b2176-dc15-4dff-9a79-99a8636c6875
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	afc549b0-839a-4ce0-b126-78c3494ac620
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	82f907ac-7916-4883-bbc3-0656098bd4fa
Muzaffar	6	1	2026	0000	Fetched products for shop	dec92a17-fc53-4790-9ee4-4b0f2d6e1ce4
\N	6	1	2026	\N	Product restocked: fd3fffe6-044c-4ee3-b815-471be7dd887f (added 15)	68da8519-a772-4cc6-844c-065cfdc1c126
Muzaffar	6	1	2026	0000	Fetched products for shop	9a4d4fa0-8a39-4e4f-884a-c1456eed5304
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	5b0433d5-463e-4a51-9087-af6717dc382f
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	18d5911c-7def-429d-a541-de727b00add7
Muzaffar	6	1	2026	0000	Fetched products for shop	efadf0d7-ab62-454b-81ea-556b038d2610
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	93d2b08c-eb1d-4931-9c73-425680cdc11c
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	b7921dee-9130-4db3-9af3-592bab58b0d8
\N	6	1	2026	\N	Product restocked: fd3fffe6-044c-4ee3-b815-471be7dd887f (added 25)	9fd05876-f8b8-4514-afa2-5752422ecc01
Muzaffar	6	1	2026	0000	Fetched products for shop	3179fdee-89ea-44fd-ab1a-049c4409dde1
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	48641d94-ee00-4159-bce0-93506d68f395
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	f2f738bf-dab6-4797-b49d-ddde95e0a855
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	94065fc0-23c7-4421-bbaa-9c957cb0b8a6
Muzaffar	6	1	2026	0000	Fetched debt statistics	a23cdaa8-8e04-463d-9b2e-696080084f29
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	0dc8a478-005e-4cd1-9fab-f18068254f59
Muzaffar	6	1	2026	0000	Fetched debt statistics	716f7181-7853-4c0c-8562-68372ab4d102
\N	6	1	2026	Muzaffar	Fetched all wagons	e65db2c0-6f22-417c-9c03-3a09f2d0099f
\N	6	1	2026	Muzaffar	Fetched all wagons	7a9eed17-2285-424a-b642-1f7cd3f0dae8
Muzaffar	6	1	2026	\N	Database backup downloaded - tables: 14	1582677f-da2b-4344-9f55-503d76c25986
Muzaffar	6	1	2026	\N	Database backup downloaded - tables: 14	78207444-119f-445c-91ed-0d99ed90d1de
\N	6	1	2026	Muzaffar	Fetched all wagons	ddee8738-77dc-46cf-b199-c5770d904796
\N	6	1	2026	Muzaffar	Fetched all wagons	9d3c19b2-4d92-4246-add8-6b5400d7e35c
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	625a358c-48a8-4e25-9997-c6466294f76e
Muzaffar	6	1	2026	0000	Fetched debt statistics	99e9d1a5-8805-43ec-b1c1-b0d763826e43
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	def6e41e-1360-489e-a2f0-34800f05c84d
Muzaffar	6	1	2026	0000	Fetched debt statistics	44949bdc-b4df-443a-91cb-7153d15b768f
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	df91a4f0-c9f1-4402-8524-25e96149b34b
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	daaf72d0-0966-46cd-a319-c0feae809304
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	f7a8fbd0-084a-47ef-803a-62194b888924
Muzaffar	6	1	2026	0000	Fetched products for shop	8ad4bae7-b292-45b6-a591-778cc189fe62
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	6c47ae43-14fe-4652-8da3-70ae90701e8c
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	8df0843f-44db-4283-8bde-101fbef1cf8a
Muzaffar	6	1	2026	0000	Fetched products for shop	f57ef417-1030-47c8-8f2d-375dda19c2b3
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	90b1d1ce-0d23-4060-8e14-bcf71bebe2e1
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	f072451a-b474-4110-bede-401a8aeef5c1
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	dd279e4b-8212-4d07-99e3-07f8abc12141
Muzaffar	6	1	2026	0000	Fetched products for shop	9b6c40eb-193d-4b93-a9dc-c90589208235
Muzaffar	6	1	2026	0000	Fetched products for shop	428e72d4-2b97-4f24-b4de-dc9b715fbfbf
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	b879a510-4b1e-40d1-b046-6ec503b29048
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	51067795-222b-4b4d-84ac-47592493801d
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	d6f085c2-4c75-412e-9c5c-e52dc5d1300d
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	7a916804-4ebf-4965-ae06-b9584076c901
Muzaffar	6	1	2026	0000	Fetched products for shop	26a08c92-7148-4282-b193-0455b6f1256e
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	49c2e3b2-60be-4073-a437-e0a6590bdb18
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	688d63a2-52da-4d70-acaf-52c186eb9384
Muzaffar	6	1	2026	0000	Fetched products for shop	7b4d59c3-e5f7-464f-a4d8-74f3f314db38
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	35d395b9-54e5-4288-96b1-8a8ecc14bbc6
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	274d1e56-411d-4623-9d0e-55b416151bf5
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	87b5ab1e-2e7b-41a2-8aaa-6d8e6175c72c
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	6645ffd5-bb39-4066-8440-ff5a9dc64085
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	3a4b6f1a-0be7-4c6c-b04b-214e98beeec9
Muzaffar	6	1	2026	0000	Fetched debt statistics	4a4a9ff8-af92-402e-b654-aa1b79453c00
Muzaffar	6	1	2026	0000	Fetched all debts - count: 13	33ad351b-1811-4d5b-b900-eec6ae3ad315
Muzaffar	6	1	2026	0000	Fetched debt statistics	09a05981-0f7c-4c04-be59-c5e48d1cf6ad
\N	6	1	2026	Muzaffar	Fetched all wagons	505556e8-8207-439e-aa08-8a33df2ea15d
\N	6	1	2026	Muzaffar	Fetched all wagons	05b5b9ba-918a-4491-83e6-dbab1294c964
Muzaffar	6	1	2026	\N	Database backup downloaded - tables: 14	fc5facbb-5a35-4977-a1da-f6133dc66eb3
Muzaffar	6	1	2026	\N	Database backup downloaded - tables: 14	66c14f9a-b6d7-4e0a-b1fd-447cbf4170a1
Muzaffar	6	1	2026	0000	Fetched products for shop	f8f76511-ee31-49a6-bea7-ec67ce312dd2
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	37e9c1e2-466a-4cf5-9871-fc98bfbd395e
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	0db0395d-3491-4964-b71b-0521da0b65ab
Muzaffar	6	1	2026	0000	Fetched products for shop	587bf9bc-1e17-428f-a02c-5251bc8809d5
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	f744d8f4-c391-4e4a-a5c7-2b4d7dc6b2a2
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	a314c9bc-2222-42f2-83e4-60606b308ade
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	e9ddc442-9e2f-4f67-9a85-08ca711c0150
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	34875412-cd38-4e64-aab4-8196d72aadfe
\N	6	1	2026	Muzaffar	Fetched all wagons	2a6dc298-7b6f-4bdd-a3f9-3b5420a34699
\N	6	1	2026	Muzaffar	Fetched all wagons	834627bd-60d5-417d-a331-75d23be83f51
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	7d626735-a241-45e0-bdea-7a31b02faeb7
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	d8d10692-3872-4271-8f0f-307b8ce02819
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	3a5063a2-79d9-4c4b-8ea7-a78ca6a15236
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	0c15ecec-5e6d-4108-b443-854d9d66fb0b
\N	6	1	2026	Muzaffar	Fetched sale by ID: 8415afdb-5ee4-4417-a1a6-7f2d8edd8f16	20b1cde5-9c63-4c43-b26f-5a19ad5a2285
\N	6	1	2026	Muzaffar	Sale updated successfully: 44c0ce3f-b1c8-4fec-a702-b75396712a57	94e89495-e20f-4b7b-a264-6d898fec83e4
\N	6	1	2026	Muzaffar	Sale updated successfully: 8415afdb-5ee4-4417-a1a6-7f2d8edd8f16	e9550b6b-a711-41ac-83df-0a5a75dcbe08
\N	6	1	2026	Muzaffar	Sale updated successfully: 8415afdb-5ee4-4417-a1a6-7f2d8edd8f16	ab837c31-0b49-4931-8de3-53779d0cf368
\N	6	1	2026	Muzaffar	Sale updated successfully: 8415afdb-5ee4-4417-a1a6-7f2d8edd8f16	6d0a1a8e-ae31-4abd-bbe0-088f9be28c42
\N	6	1	2026	Muzaffar	Sale updated successfully: ecdc6bf0-73ab-4814-a51b-90d5a6e0e286	8097c65e-9fee-4ac9-86d5-aacfe916d287
Muzaffar	6	1	2026	0000	Fetched products for shop	8a575c26-b21a-4944-a22f-a597479feccf
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	937c1327-e20d-467f-b791-8532b28025c3
Muzaffar	6	1	2026	0000	Fetched products for shop	6ece28a7-3bbb-4686-ba61-e3fcec94335f
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	8f688e5c-9e0c-44ef-9244-37b8793aaae8
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	5ac7b94e-c5ad-4b77-9267-a194b5794061
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	2fe6e414-fed6-4ba5-8232-81bb47ac1ef6
Muzaffar	6	1	2026	0000	Fetched products for shop	90283cba-3a94-41c6-9eaa-42bbc95cddb4
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	d0833ced-71cd-4df4-b347-4579540be950
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	3d14074c-40fb-4339-a552-b7c8d8def70f
Muzaffar	6	1	2026	0000	Fetched products for shop	251560d6-35f1-4a42-bf13-aca16b7f4299
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	e9395a41-0b8a-4538-bd87-0f94479aa17e
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	29aa0344-214b-429f-b82c-1330bda84a1f
Muzaffar	6	1	2026	0000	Fetched products for shop	e0421213-0caa-43be-a7da-94006ced992b
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	7709c4cc-cb27-4339-b876-ac6e910c18c1
Muzaffar	6	1	2026	0000	Fetched products for shop	1dc81f96-4b18-40b1-aec6-7a0373fe20e8
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	ff2601aa-beaa-4327-8ec0-86ff710e7116
\N	6	1	2026	0000	Sale created successfully with sales_id: 0674804c-59db-45b1-94c7-af7d34941f9d	d800408a-d08e-45f1-98d1-b6434e8e5021
Muzaffar	6	1	2026	0000	Fetched products for shop	555570da-9244-4310-8fc4-2861741f63cc
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	2aba6589-8bcb-423b-aa7b-0b2fbf30a853
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	5869d717-8f0b-4c64-9b2d-5851e745aeef
Muzaffar	6	1	2026	0000	Fetched products for shop	16bfd2ba-1be9-402d-9a86-e335613c0cc6
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	42940e09-1ff0-469c-b07c-c6af1d5e8138
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	3fea4d7c-af70-497b-95b3-54e6262ab894
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	9b6a7e92-5191-4438-b56d-ffac5d0cd4a8
Muzaffar	6	1	2026	Muzaffar	Fetched all sales	4719e286-a39f-4b70-b5ac-bade050c61e2
Muzaffar	6	1	2026	0000	Fetched products for shop	f0854391-329b-4ebb-b6e4-207e98379053
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	12f85f9b-da02-4fd4-8bef-9de55d5774b7
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	982244dd-ca8a-4f32-a5a6-3dc5fbf88cdc
Muzaffar	6	1	2026	0000	Fetched products for shop	57a0b49e-8968-4b87-9bd2-47d94a2a3027
Muzaffar	6	1	2026	\N	Fetched all categories - count: 4	3aa3ec7c-82f8-498d-890d-a5b2dad4ce9a
Muzaffar	6	1	2026	0000	Fetched shop branches - count: 2	9cdd1725-70f8-4bfe-a22a-5843ed9eee19
Muzaffar	6	1	2026	\N	Fetched main finance statistics	d283fd27-2811-4004-89f2-98799ff8665d
Muzaffar	6	1	2026	\N	Fetched main finance statistics	218ffc9a-1069-4982-a486-8a5ad5d9050f
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	bd0abbc0-1973-4a11-9a11-8e4db5a8d72e
Muzaffar	6	1	2026	\N	Fetched high stock products	7a7c5ea9-a591-4d91-96d5-01cdddb0319a
Muzaffar	6	1	2026	\N	Fetched week statistics (last 7 days)	7473152f-bf5d-49f1-ab6f-d0596cd2e901
Muzaffar	6	1	2026	\N	Fetched high stock products	64764649-50c6-41ac-8034-42868f01cd1f
Muzaffar	6	1	2026	\N	Fetched low stock products	52e4eef2-a423-4de6-ab0a-b8ab15d16156
Muzaffar	6	1	2026	\N	Fetched low stock products	a8515669-7a3f-43a5-ad90-a22b3135d94a
Muzaffar	6	1	2026	0000	Fetched products for shop	315757d4-9bf0-45b5-a22f-26a2aa4959fa
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	98dbf25b-0482-4bb8-aef3-678d2b0d3daf
Muzaffar	6	1	2026	0000	Fetched products for shop	2c5eed87-11a3-4263-b41c-eee19a67f3c0
Muzaffar	6	1	2026	\N	Fetched all brands - count: 3	59a4a509-e387-45f7-b2bc-4606c379a003
550e8400-e29b-41d4-a716-446655440000	8	1	2026	\N	Superuser logged out	f25abce0-763b-4ae8-b558-20542d18e05b
Muzaffar	8	1	2026	0000	Super logged in	2ac06c58-59eb-4c09-85a9-175cf5cb0d56
Muzaffar	8	1	2026	0000	Fetched products for shop	ffe4f23a-e665-4110-8d35-5dded46ac9b7
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	da27f217-db89-4a75-917d-d2686047f5de
Muzaffar	8	1	2026	0000	Fetched products for shop	3c96b1ab-9fb2-453c-8da9-77e9361e66f6
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	df3e9de1-9b30-407c-8745-5bb6d73a3a04
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	4249c06b-0e8f-4dd2-85d9-9fa75856fed2
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	8de7fb06-1e87-4edb-9b52-4fa3cf0baf6f
Muzaffar	8	1	2026	0000	Fetched products for shop	8a97d181-0c2c-4f2e-bb74-594bb59918ae
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	94cfdc12-823d-4a55-8b72-7d0314472238
Muzaffar	8	1	2026	0000	Fetched products for shop	f903a65d-d676-403d-8a58-b0410d5cf971
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	1c99130a-5f6f-4ffc-9d73-ea3f0ebe564f
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	a47367bb-4a54-42c6-a0fc-a8f3246bf6cc
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	f4070678-0603-49b4-9f6f-99fdf90b15a6
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	a8620a53-cd22-4d4e-86ac-f548b8ef9814
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	d1674d93-28d1-4269-baa9-99f865dd6d63
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	74568d6b-91fd-4ad3-b54d-146320607c07
Muzaffar	8	1	2026	0000	Fetched debt statistics	7ace3ff5-d4f8-4599-973b-f02a4b1ea4bc
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	18ecee91-e405-47ed-9aef-41d70cc578d8
Muzaffar	8	1	2026	0000	Fetched debt statistics	8f06a051-333c-4a0a-a4c6-6e9803f29404
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	f5a83d30-27da-4958-9a23-19d41a10b536
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	0f808a45-24b7-4f6f-92e0-f11f7879b6ba
\N	8	1	2026	Muzaffar	Fetched all wagons	9e1b6ef7-6b1b-4d71-bb95-eb0208fc1c15
\N	8	1	2026	Muzaffar	Fetched all wagons	c211fa0c-b8c1-4091-8a1e-eaa35ae69616
\N	8	1	2026	Muzaffar	Fetched all wagons	958fdaac-102e-4c93-bf3d-eb60fcf9a61e
\N	8	1	2026	Muzaffar	Fetched all wagons	8f7b8d35-0d78-4234-951c-65905570da7d
\N	8	1	2026	Muzaffar	Fetched all wagons	e966f0a0-7254-42e9-8e31-b57a2f6b74f5
\N	8	1	2026	Muzaffar	Fetched all wagons	b538b362-7fe4-4a47-88df-16657720b029
\N	8	1	2026	Muzaffar	Fetched all wagons	42f123f7-3f55-41ea-8a16-e2f72b888951
\N	8	1	2026	Muzaffar	Fetched all wagons	56d7ad6f-0cd8-4033-9af3-1065b1f1c2f8
\N	8	1	2026	Muzaffar	Fetched all wagons	f42db6bb-c6af-439d-8ff5-33d319101184
\N	8	1	2026	Muzaffar	Fetched all wagons	cec18071-1b1f-40f2-a032-69785b8f861c
\N	8	1	2026	Muzaffar	Fetched all wagons	cfdd4843-2687-429b-836b-328f0fe7374e
\N	8	1	2026	Muzaffar	Fetched all wagons	9796fe85-c4c8-4bab-9982-be9e0fb16f8c
\N	8	1	2026	Muzaffar	Fetched all wagons	f1fee234-82c5-4b16-b8da-ae366cb15464
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	ce11de3a-1731-4b2f-b61e-6050250c3028
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	b2686cf7-2040-4cb7-a0ca-e77d5534b453
Muzaffar	8	1	2026	0000	Fetched products for shop	afe4b038-5d88-4bde-9c17-5f190a6b7fca
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	8db15e21-59d1-4074-b4e6-e8c07fee3cc6
Muzaffar	8	1	2026	0000	Fetched products for shop	911b0b11-9a8a-49ed-9618-1616df3f1f18
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	5624e098-56f7-4546-82d2-0bdce524f05d
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	d40c5d1c-a8b6-4546-94c6-c708e3d45888
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	f7a0da38-ff92-446e-988c-9eff1362c608
Muzaffar	8	1	2026	0000	Fetched products for shop	005eefd7-f869-4cd5-ae0d-07fbd38134a1
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	53445a65-ddbf-4057-8f80-056eaabbfbc1
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	b002481a-c88f-4a0f-8fe1-72ba68b862f8
Muzaffar	8	1	2026	0000	Fetched products for shop	9480b7f9-2189-4fcc-9a4d-0415e24543b7
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	a073c4c9-9948-4253-9cfe-98ed1b0418ed
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	b5a7e14d-19ae-47df-9a4d-63a7317aa610
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	10d50746-3c82-41ba-8ed3-c379aeacffeb
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	acf1d6d8-f581-4147-8801-3217c2d7f7b6
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	f6ed225e-7b76-4789-b00e-896c7258a9ac
Muzaffar	8	1	2026	0000	Fetched debt statistics	846cd444-81ce-43e8-b023-69b59fdd613c
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	314e69c1-994f-41be-8e80-15de6bfe5b65
Muzaffar	8	1	2026	0000	Fetched debt statistics	0f316414-f88e-4f7f-bffb-a5c275ce9c42
Muzaffar	8	1	2026	\N	Fetched debt by ID: d150be55-9f8c-4d38-87e8-f6b0578c82bf	d4049a51-06d6-40ed-9605-112b0a1c4dd8
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	a5d163cd-d64a-477f-85be-51693078c398
Muzaffar	8	1	2026	0000	Fetched debt statistics	325b4e23-5116-4d33-945e-9085087883a4
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	1b7bfa11-7543-4c9d-9c79-5e4e612ed2d5
Muzaffar	8	1	2026	0000	Fetched debt statistics	1baf4d0f-ae2a-4ea3-b86d-361ee06ac1e0
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	c8b5ba37-72ca-452a-b598-48ffe0265d4a
Muzaffar	8	1	2026	0000	Fetched debt statistics	219721c5-2108-4d3e-b62c-12f3e3de01c5
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	49d3e33f-5621-4b93-ba40-43acc35340cd
Muzaffar	8	1	2026	0000	Fetched debt statistics	aa07aece-5dad-456b-a0be-ab3c7315f80d
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	9243af04-0ea6-414a-8b58-71e439e62e9b
Muzaffar	8	1	2026	0000	Fetched debt statistics	9f6ddddb-2496-4a30-ab8a-f2821edb70e1
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	e5f18b05-0cae-48f1-8777-47fbe1fed88a
Muzaffar	8	1	2026	0000	Fetched debt statistics	bc10754d-a68b-43a0-9d6e-8165d3807de7
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	bfec1dd6-b93c-4fb9-a821-e4e3e9ca213f
Muzaffar	8	1	2026	0000	Fetched debt statistics	128b9ec2-071d-4196-b40f-fa7b6b53eb26
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	db73d205-e49b-4eb9-980b-708b8f7c5d1c
Muzaffar	8	1	2026	0000	Fetched debt statistics	2d9ff78d-db9f-435e-8c31-2d02d40fdbd6
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	ebb8659e-9628-4979-9e7b-2363cda64c96
Muzaffar	8	1	2026	0000	Fetched debt statistics	f1c948a9-7bcb-4bb6-823b-8ec5835b2abc
Muzaffar	8	1	2026	\N	Fetched debt by ID: d150be55-9f8c-4d38-87e8-f6b0578c82bf	f4d6f4e6-d48b-4170-9f97-93ca3f967e39
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	0fd9f606-9f96-4527-81d2-6de68d3484a8
Muzaffar	8	1	2026	0000	Fetched debt statistics	b3271a72-a84e-4edb-83f9-13a340c3b0d9
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	5e5af258-4459-470b-86d4-dde13011837a
Muzaffar	8	1	2026	0000	Fetched debt statistics	fb2f58b0-0aea-441f-84a9-947ebc074e11
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	61058463-6d68-47b2-9ddd-3892ad0fc3c0
Muzaffar	8	1	2026	0000	Fetched debt statistics	29783c71-7ecb-4ae6-96ef-2128305a79fd
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	5f70e91c-51a8-4554-9b8d-62488cc7c484
Muzaffar	8	1	2026	0000	Fetched debt statistics	8e176232-fcb5-4905-9fb9-20dde36f1866
Muzaffar	8	1	2026	\N	Fetched debt by ID: d150be55-9f8c-4d38-87e8-f6b0578c82bf	ce6d418e-0734-42bf-8d5e-fc982541f8e9
\N	8	1	2026	Muzaffar	Fetched all wagons	a3c61987-67c9-4bfa-bf7e-0adcf24a61e7
\N	8	1	2026	Muzaffar	Fetched all wagons	7feb65a8-9d74-4a9d-8dd9-0251d621647b
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	16a6d70b-00a8-4903-9ef4-0e1d77b3c4d0
Muzaffar	8	1	2026	0000	Fetched debt statistics	94a265b6-0845-4295-9ca6-9c7cf57ef08a
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	6b0d5839-9dfc-45f0-8128-64ebf7e38521
Muzaffar	8	1	2026	0000	Fetched debt statistics	e78387a4-4b97-49ab-9294-2a7b21556f7f
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	59e816c7-1d49-44d6-b66d-6796a03173a9
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	f579290a-a7f3-4291-93de-4d2438ba33ce
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	022b3a07-2f2b-4bc8-a27d-c3ba8bbfcc51
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	ffd338b4-a9dd-4c49-8687-62d7b4c2cec1
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	e2298b49-5abb-420c-82d9-7ff9321a0e1c
Muzaffar	8	1	2026	0000	Fetched products for shop	77836d65-a1d9-4763-bd9d-90bc33f34d3b
Muzaffar	8	1	2026	0000	Fetched products for shop	9146c34c-9195-4806-9ed1-fcbb2bba2c56
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	39a2cc81-b6e3-4e4c-a5cc-97d4558de577
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	2620974d-5bcd-4f1e-97d7-36380384b0cb
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	b624d7d7-7f0f-4ff1-813c-f1895d8c5227
Muzaffar	8	1	2026	0000	Fetched products for shop	26941c7e-36a9-4730-a800-cb430e3b259c
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	2902adc0-44e5-4126-85ea-6fe4e7956b77
Muzaffar	8	1	2026	0000	Fetched products for shop	1f4008b1-9184-4e71-9f30-94332fadbcfd
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	8cb455f4-c525-4afe-8eb6-4f9b70111de4
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	09c8c0a6-6ea3-4650-a537-29bea4b416c3
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	938867a6-ec49-4939-8929-77c5e81bf13b
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	00dfe9a6-8b3e-460a-a0f1-73300f7868fd
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	95d24dd7-7869-4580-b8f0-3496a28b4766
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	07490995-ace5-479d-a73a-63f47f5e3c1f
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	ff2adb87-59e3-485e-bc52-7f72814676b1
\N	8	1	2026	Muzaffar	Fetched sale by ID: b90ffb9d-5622-4b1b-a5ff-9802548e8ca3	a8e4ad54-3c0c-45f7-b540-cf8615d99967
\N	8	1	2026	Muzaffar	Fetched sale by ID: b90ffb9d-5622-4b1b-a5ff-9802548e8ca3	62d1fe9a-7dc8-41e2-8157-83294bfd308d
\N	8	1	2026	Muzaffar	Fetched sale by ID: 399d091a-2afc-4133-aa76-197ac782f0c3	ec7b8f6d-111f-45c1-814d-59234cb50c72
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	8c3d4e7d-0dfd-4406-aac3-984d76942596
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	74b969f1-63ad-4978-838f-71c1b0ed2783
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	f9442c62-19f7-4563-9052-a54f8e88c36d
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	bde0cc81-617d-4ff7-842b-215df7aeb120
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	19485dba-3048-46e7-b058-60789f85d536
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	528e4f94-86ea-4df3-b3db-00ff4db599ce
\N	8	1	2026	Muzaffar	Fetched sale by ID: 0674804c-59db-45b1-94c7-af7d34941f9d	90f3db15-7c65-4e61-9eb3-3b4ee34e5d25
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	18c09402-498a-4009-9dd4-b674093d39e3
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	472d1618-0b22-4c9e-b0a2-74483125d982
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	5632c7b4-b8a0-442e-8734-3929c4e7ed9a
\N	8	1	2026	Muzaffar	Fetched sale by ID: 6987b7b2-531f-4164-aa77-45e74833cb06	ad0bc560-f1e0-4c16-9b36-83eb1c491656
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	7433e8ae-7c1a-4e4d-b4a8-355424163a95
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	62749fae-a733-477a-b3b2-3caa83c89dee
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	46b042b8-9a90-4cf4-b2f8-c34330c3dcc4
Muzaffar	8	1	2026	0000	Fetched products for shop	4aace03d-e857-4d22-b274-a5e0608ef96c
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	b563c4b3-c180-4754-b9bc-fd83683a9e76
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	54a1d99c-7946-45fa-89a4-413031aa8506
Muzaffar	8	1	2026	0000	Fetched products for shop	22570569-812d-4a93-8f22-de83c39a943e
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	e619ed74-2610-40b0-b92a-25937b4b0e20
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	4ec6e289-5554-4541-bc17-efa6466cbcc7
Muzaffar	8	1	2026	0000	Fetched debt statistics	8c2828a0-20a2-42fe-b1f8-1ed904bcaafd
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	66a9023d-698e-4235-a2a0-257a800d4b2b
Muzaffar	8	1	2026	0000	Fetched debt statistics	9898e4da-37bc-404b-bc64-a90c41a27a3e
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	3e9e6909-f45c-44c0-a100-514024d1419b
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	569baa7e-ec2a-40e2-bdc5-06a653a1a49b
Muzaffar	8	1	2026	0000	Fetched products for shop	a8a7d270-b726-446f-a352-4d1d0b3302fd
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	9f846f65-348a-4a7c-b061-b07106bb96f1
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	2135e852-9822-4c50-bff2-143b6e778bc5
Muzaffar	8	1	2026	0000	Fetched products for shop	4ee766ce-9188-45bd-9499-6cca95dac841
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	45f29624-1731-4f24-b792-6c48ccf4e0c8
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	22f9918a-27e4-468a-bba0-038256c6113c
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	daf86463-9c10-48c6-bd54-db848d248657
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	f07656a6-4514-47cb-910d-2b865334d326
Muzaffar	8	1	2026	0000	Fetched products for shop	0d816170-adfc-4ef9-8baa-f147516b2e50
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	093534ef-4b1c-4ed4-992a-21b782b509fe
Muzaffar	8	1	2026	0000	Fetched products for shop	b21a4bdf-978c-481a-b879-c0b37923d7d4
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	b78fabaa-7395-4eee-bf7a-83e968dd965d
Muzaffar	8	1	2026	0000	Fetched products for shop	651fbd9e-275b-4976-905e-fc30df527da5
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	65cb3b21-148f-4c92-974a-37cef7319e4e
Muzaffar	8	1	2026	0000	Fetched products for shop	921d9d6b-fd49-4533-879a-54eddee33ac8
Muzaffar	8	1	2026	0000	Fetched products for shop	5b3bae85-50fa-4766-a046-ab882eafe79e
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	63232263-337a-4f4f-9459-daeb2fdb8c2f
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	c191b392-6194-4ec8-9764-993e9012844d
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	250cfa1b-cd94-4f31-9893-58cfca71d752
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	cc699585-c163-4735-9898-93575a49401b
Muzaffar	8	1	2026	0000	Fetched products for shop	97aa8c9e-fb86-4c84-b44e-e15508259496
Muzaffar	8	1	2026	0000	Fetched products for shop	5307942a-f182-4b88-890f-cba413886a4d
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	0245eed0-b039-4866-88a0-65d559f336af
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	49ec9a4d-4cf1-4804-9a4d-aa778841c7f6
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	92ee1386-52b4-4f08-bfc7-6c8c23cdb8e3
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	308c271e-5fdb-4106-8268-1a5a6a9e4fb4
\N	8	1	2026	\N	Product restocked: c3e111f7-da9b-4645-bad1-b08582aef8ba (added 15)	736bb19b-ede1-47f2-8a91-5bb6da8b181e
Muzaffar	8	1	2026	0000	Fetched products for shop	28afa22f-8ce3-4aca-9ef5-2207ca3c8d11
\N	8	1	2026	\N	Product updated: 2b40b1b1-5532-422b-80f5-335513df1286	193687e6-4212-4ec3-848c-02618cd3486b
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	e262e1b0-010b-4a0c-a1f9-310d173c8000
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	a671a3b3-257c-45ae-a7a7-0b8c3f6ee434
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	b29bac4d-9f3c-44ba-a681-9c1cfd8bb018
Muzaffar	8	1	2026	0000	Fetched debt statistics	993200f6-b109-4e2e-86b7-0eaafee13bd7
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	d06a2271-004d-421f-a07c-41b75c0219a9
Muzaffar	8	1	2026	0000	Fetched debt statistics	ba29cd59-b0af-46a9-95ea-1de51fafb9d8
\N	8	1	2026	Muzaffar	Fetched all wagons	2bc907ad-008d-4c2b-9c6e-635e29972f9e
\N	8	1	2026	Muzaffar	Fetched all wagons	4c97c8e9-9491-41f8-a5c4-0b9d361d6987
Muzaffar	8	1	2026	\N	Database backup downloaded - tables: 14	a8e35cf3-e0a0-4a02-8cc2-d5995558588b
Muzaffar	8	1	2026	\N	Database backup downloaded - tables: 14	3ed50456-dfdf-4fb9-9cc2-841fd2851b13
Muzaffar	8	1	2026	0000	Fetched products for shop	c426a47c-d8ea-4a7a-88c7-46526343c957
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	d8a44f4f-a176-4a7b-964c-d142f71a502d
Muzaffar	8	1	2026	0000	Fetched products for shop	6a715e14-21c6-438d-bd29-cd6188fe0d5e
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	ddc74ef1-0012-4fe0-b5c2-49e83b50facc
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	078933c2-e89b-4252-8252-6df7443851fb
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	d9b9df13-aed1-4913-ba86-3b2802cf370e
Muzaffar	8	1	2026	0000	Fetched products for shop	1c28e82d-0589-4452-9a32-f5e114143de6
Muzaffar	8	1	2026	0000	Fetched products for shop	640a50f7-2eb1-42c6-8262-91c27e8ae949
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	aaacbdb0-1aa3-4ee3-a4d7-f67dff6d6f08
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	f9b390d6-bef1-42cf-926a-cc33114f8d32
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	f7fe2706-244f-4742-97a1-fcad4a31d6eb
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	517bff14-cb23-4712-9ec9-ae71f0fa98a5
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	ad99d7ab-2fd1-48d6-9d73-163801299ca6
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	465405ef-62ac-4503-94d4-c17a84cb1d57
\N	8	1	2026	Muzaffar	Fetched sale by ID: 4ca34cad-19a9-489d-9c9e-11055a25102b	928fb291-28ff-4513-a801-60c6e006871b
\N	8	1	2026	Muzaffar	Fetched sale by ID: 4ca34cad-19a9-489d-9c9e-11055a25102b	359426d9-adff-46ec-8765-08ecab9a00af
Muzaffar	8	1	2026	0000	Fetched products for shop	8899a3b3-21ae-4726-9607-a5d332fd164b
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	212655c9-6a1f-4210-83c2-4e9abe564878
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	0ffcccca-66ff-4c88-b452-c365d7f656c1
Muzaffar	8	1	2026	0000	Fetched products for shop	fc91ff75-6762-471b-9098-19b3bc81d95a
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	b491d1a6-aa99-4244-bc14-4c625c1e5d86
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	b7d9707a-1ccf-4e60-80ed-5516adcf771a
\N	8	1	2026	0000	Product created: 8a3a966a-62d7-44d1-9e2d-0a1eae4ac6eb	532c156e-ab4a-4861-9402-b70a953ebcc5
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	c3463d95-7971-4928-b8e4-ef10d70499bd
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	cd71b836-7578-40e4-9070-0459891eb20e
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	9540f700-ef5b-4857-ab2f-d6dc38239229
Muzaffar	8	1	2026	0000	Fetched debt statistics	abf7ede8-aec8-4644-b7ec-c4e6fc52192f
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	7298744a-0901-427c-b552-3ce9cb5877d4
Muzaffar	8	1	2026	0000	Fetched debt statistics	dfcdc6a1-98f3-4ee8-ba4e-1a953808961c
\N	8	1	2026	Muzaffar	Fetched all wagons	0d3f5564-1248-46d2-b940-356476ccc5af
\N	8	1	2026	Muzaffar	Fetched all wagons	ab61c0b6-d8e0-4277-bb9e-2e32ded723aa
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	d2d19a1d-30c0-4274-9ac3-f8dcefd2b7c1
Muzaffar	8	1	2026	0000	Fetched debt statistics	889ff9c0-a52e-4122-aa72-c2c642d67687
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	02ea9bc6-2963-458e-95a3-4885911f16ba
Muzaffar	8	1	2026	0000	Fetched debt statistics	2e730013-57bf-444f-8ffb-6a9cf7b99d56
Muzaffar	8	1	2026	0000	Fetched products for shop	5c35032e-42e6-4af7-a4d6-588c00398270
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	cb464916-731e-45c0-8ac1-42c339b81505
Muzaffar	8	1	2026	0000	Fetched products for shop	6bdd43e0-5ded-42ed-ab1e-e49d06e691bf
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	0b2c97f1-de51-42b2-ba7b-fb055b79b7ea
\N	8	1	2026	0000	Sale created successfully with sales_id: 7f5e871b-c9df-479f-be45-5af4d5a82c88	a141d7a7-649b-49a1-b46f-5cbb08e7cb9a
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	2b206f04-bce4-46f4-a765-e1566d6157c2
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	bd02608e-8443-471f-81d4-15e9fec6d5e0
Muzaffar	8	1	2026	0000	Fetched products for shop	1149eb00-9bf0-4e9f-ac2b-942426a40bf3
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	b4731208-b7f1-4878-9435-db84a82e79e0
Muzaffar	8	1	2026	0000	Fetched products for shop	162f862e-6604-4de1-9bdd-88a63cdfd4b0
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	30cf4bf0-e6a7-40ce-b3bd-28a6e05ff7f3
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	6fd83093-2a93-4faf-b99c-94b558cc5404
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	0fcf9f75-71fb-47bd-b8f3-bea95621bf96
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	28248a60-a6ba-4a45-8e22-7ff724d23d60
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	51a8f832-e401-4145-bb10-42203fb19941
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	3e79b3f0-fe9b-4dbc-a66b-815f55ddbbe8
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	819fab19-421d-4fa8-9ac7-369849f52fdb
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	7cd956ea-eda3-439a-85a1-bda23d6b7bda
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	59941286-f329-4ed7-98c4-e4e2b6281f36
Muzaffar	8	1	2026	0000	Fetched products for shop	cdb9a08f-0cc6-4445-8ef2-32fbb55db390
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	8a6ba63a-cfe9-4c2e-9e31-bef6ea3fa014
Muzaffar	8	1	2026	0000	Fetched products for shop	6d937649-879a-4df9-93a8-86113d7272e0
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	b6385498-49d8-4a1d-8c99-d8ff98fdda09
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	e2427f49-c44b-4613-85d0-c1e942b21cfd
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	c4aac31d-a38b-414b-b67e-9c8172c8717d
\N	8	1	2026	Muzaffar	Fetched sale by ID: 0674804c-59db-45b1-94c7-af7d34941f9d	07986c18-ef03-4b5e-954e-5c51f0d16fa6
Muzaffar	8	1	2026	0000	Fetched products for shop	6e3b3de7-908d-456f-b90d-916595daa211
Muzaffar	8	1	2026	0000	Fetched products for shop	8d98cdfd-5cde-4f6b-ad8f-23b3c6920638
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	37253223-1470-44bd-a638-26a38488e328
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	e62c0666-5e19-436a-8c2c-438ec1284541
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	5b71ae4e-f7f3-45d8-9014-b664705fdf3a
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	f896a9cd-8059-4161-b80c-3ae6cce4ec55
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	c347b487-bf91-41b0-bd6f-8ce8ed4a844d
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	e97aa35c-17a5-4664-a5ee-c95f73ca30e5
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	239772d4-d0c3-4592-ae46-aac663ba855b
Muzaffar	8	1	2026	0000	Fetched debt statistics	84d7392e-9c37-41ac-af3e-3af02f21ca73
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	a1aa7b74-7da0-478b-8812-f584e9d406ea
Muzaffar	8	1	2026	0000	Fetched debt statistics	340e2747-8c55-4f71-bdd3-3f61fc2e95c8
\N	8	1	2026	Muzaffar	Fetched all wagons	ded32aa1-606e-44c5-8ed0-d31eb1fb2f99
\N	8	1	2026	Muzaffar	Fetched all wagons	92d1b10f-dcc9-42cc-b56f-9e91db74c028
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	3f64df43-9823-46a1-b4c2-ea89e7fe8fe0
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	c8415a47-36f7-4eb9-9251-6b80fdc61ecb
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	e02ae939-b4fa-4ff6-8054-f69cb9db2220
Muzaffar	8	1	2026	0000	Fetched debt statistics	30847be9-8d68-4729-9d26-5adc045910f5
Muzaffar	8	1	2026	0000	Fetched debt statistics	1f4f05d6-a351-4f6a-8fc9-1d9aeb555d99
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	d112e41a-f41c-4a2e-8064-389257e8f655
\N	8	1	2026	Muzaffar	Fetched all wagons	daf4e93f-19c9-4ca1-8754-713b958a9536
\N	8	1	2026	Muzaffar	Fetched all wagons	4c06ff97-fd08-46a0-b745-b1d9cd667fba
Muzaffar	8	1	2026	\N	Database backup downloaded - tables: 14	6db71453-5de8-4721-bebb-21a88214fa4a
Muzaffar	8	1	2026	\N	Database backup downloaded - tables: 14	ec68f390-e8af-443e-9992-55a3ccbc9d0b
Muzaffar	8	1	2026	0000	Fetched products for shop	8f177245-e085-4757-b9e7-b07541f6904a
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	0730834b-6225-4cbb-81bb-699e1c35424c
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	0e1a1d26-c9b5-4265-bbaa-ccedfa34ce72
Muzaffar	8	1	2026	0000	Fetched products for shop	90ad8ca4-0411-43ad-9f59-265dd0f63c4e
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	b6ab9dc1-ce6a-4118-9b3f-9cba9bb262e4
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	2914be4c-c325-41c2-8d65-a28bbb21cc1e
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	45a25240-ff93-461c-858d-77eb3ec369bd
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	6bb2c04c-e368-4beb-9376-3cde00e17741
Muzaffar	8	1	2026	0000	Fetched products for shop	fefd907b-0a4a-46c1-8b2f-22aba8f27cb5
Muzaffar	8	1	2026	0000	Fetched products for shop	c96aa807-1e4b-4be9-957b-43410c35ef2a
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	293fe143-cd3b-44fb-bb51-dc06fdd49df1
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	ae5d8914-bbff-44a5-ad23-d52a50b02851
Muzaffar	8	1	2026	0000	Fetched products for shop	634ea1cd-a907-4fdf-8fd9-82c060329c5a
Muzaffar	8	1	2026	0000	Fetched products for shop	efe47f19-90cc-45f9-a122-6c7cc48eb971
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	57b2e5ab-cdd0-4c88-893a-8f3736a11b1b
Muzaffar	8	1	2026	\N	Fetched all brands - count: 3	8f1b7f19-31f7-45d7-8715-dd5b44ba1d37
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	09d6578a-b4af-43b9-9844-274527e55350
Muzaffar	8	1	2026	Muzaffar	Fetched all sales	759f9a21-76e3-4194-8013-07cf1ffb0b31
Muzaffar	8	1	2026	0000	Fetched products for shop	17ce44fa-0d72-4ba2-900a-0e71a70e2775
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	aca8a23b-16b1-48e9-97fe-1370ee0302ad
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	a33e78ae-8c34-4d47-9d72-bd1ffdba8806
Muzaffar	8	1	2026	0000	Fetched products for shop	15fb9fcc-807b-4c1b-acb1-1f892581db1c
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	b2cb4195-f91a-49c5-a191-efda6ceec79c
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	5a30cec6-c9d4-4362-8f44-9287fd675f82
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	abbfc54e-7a12-4ba6-a7e6-02e5567febd3
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	bac0beaa-3927-414d-9726-3429a7658c59
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	de7266e7-5fcb-41d6-a998-0243e975c1ee
Muzaffar	8	1	2026	0000	Fetched debt statistics	66fb2d28-881b-460e-b470-d9135e0f595e
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	f4599c2e-d0ee-45ea-a70f-927439081bc6
Muzaffar	8	1	2026	0000	Fetched debt statistics	998d977a-8e83-410b-a0b6-7fca393ebca2
\N	8	1	2026	Muzaffar	Fetched all wagons	ccf874a0-77f5-42d0-9a9f-2d73c6e530e4
\N	8	1	2026	Muzaffar	Fetched all wagons	59bf4737-fe3f-4b00-a66d-d94aeb5e4e1d
Muzaffar	8	1	2026	0000	Fetched products for shop	a1ddb441-9357-4dd3-9ee2-5fdeecef2258
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	ac500d16-1797-4938-b74b-2f784be3c722
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	b930c125-0ad2-4f01-a7aa-32a5e4920c3a
Muzaffar	8	1	2026	0000	Fetched products for shop	fdbcc94b-c1bc-4dd0-8370-d6cc8051ae95
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	8ed371cf-b5ef-4415-8875-ba015d2f49f3
Muzaffar	8	1	2026	0000	Fetched shop branches - count: 2	107ee4cb-b1b0-4796-b144-b713c9ce9460
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	aeed6427-2b0c-4cfb-ab1d-28ddabbdb0b6
Muzaffar	8	1	2026	\N	Fetched all categories - count: 4	0db88022-40d5-4439-bd60-c6d5b059dda3
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	fef80171-9b75-483b-b510-3db79fa1e781
Muzaffar	8	1	2026	0000	Fetched debt statistics	837968f2-d9b0-4918-9088-88780623d42b
Muzaffar	8	1	2026	0000	Fetched all debts - count: 13	2e4530b5-f2ed-4ae6-90dd-8cacffe1ee55
Muzaffar	8	1	2026	0000	Fetched debt statistics	0da76431-cb94-4a72-b6b2-f0e56fb7be37
\N	8	1	2026	Muzaffar	Fetched all wagons	f7cbf880-6db6-4055-84a1-8bf1e3db19f0
\N	8	1	2026	Muzaffar	Fetched all wagons	8ec4a66c-1301-46d9-99ff-8bc083b36909
Muzaffar	8	1	2026	\N	Database backup downloaded - tables: 14	e36c87b2-e237-42f2-a6b5-5fe31c9f95a8
Muzaffar	8	1	2026	\N	Database backup downloaded - tables: 14	2cf5fd32-08a9-4a8c-bd37-a96295f43550
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	999466ba-77a2-4316-a304-e3d8cc0b1180
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	4e6072cb-7a5f-4870-98e3-4b01b73e6555
Muzaffar	9	1	2026	0000	Fetched products for shop	716dd263-a9ac-4767-9da9-b5b285318275
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	6d58259a-5011-447c-b03a-87d14e2d891b
Muzaffar	9	1	2026	0000	Fetched products for shop	8f0bc234-4eb9-4746-97c2-675b067f574b
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	7e445706-f4d1-4739-86a1-d3d27fcf8585
Muzaffar	9	1	2026	0000	Fetched products for shop	43280147-fd45-4771-9a38-cab52ce0c29e
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	0cd2097a-9c2d-4c73-89ba-2b2d0a3bae13
Muzaffar	9	1	2026	0000	Fetched products for shop	e8a58d7c-396b-4030-b9ce-40913cfcb015
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	544ede98-3131-46d9-a9fe-08ebf7ddb25c
Muzaffar	9	1	2026	0000	Fetched products for shop	cea38997-ee46-42d4-a524-ecea86e41623
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	a17bbe02-14cf-4b3a-801b-cab23708c14c
Muzaffar	9	1	2026	0000	Fetched products for shop	3dac6b18-8a19-4d73-8388-a7cec5adb852
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	abb0b82e-f030-466e-b2a0-ecf27f3fdfbd
550e8400-e29b-41d4-a716-446655440000	9	1	2026	\N	Superuser logged out	557391f4-e38c-429d-99b5-60026299687f
Muzaffar	9	1	2026	\N	SuperUser Login FAILED - not found	c6be43e2-29f2-460a-99c0-5b1ef6652816
Abdumannon	9	1	2026	\N	SuperUser Login FAILED - not found	77f68038-199d-4088-bb8c-886ff34b8309
Abdumannon	9	1	2026	\N	SuperUser Login FAILED - not found	e2f327e6-0dc7-4cf4-b520-94476dbaad39
Abdumannon	9	1	2026	0000	Super logged in	8785b8c6-4880-4d7c-a045-9d491c15eb57
Abdumannon	9	1	2026	0000	Fetched products for shop	987cfbe1-3ae0-41df-94a9-846dd758e223
Abdumannon	9	1	2026	\N	Fetched all brands - count: 3	0649aedb-eded-425f-9f5b-b7b4c4ae965e
Abdumannon	9	1	2026	0000	Fetched products for shop	3d22b42c-facf-4c41-bb72-4f7ccc7b5cde
Abdumannon	9	1	2026	\N	Fetched all brands - count: 3	113caa8c-fb38-4060-9c80-fbc07c337bcc
Abdumannon	9	1	2026	Abdumannon	Fetched all sales	d9a344c5-bb7c-426d-a305-bd25811b7c4a
Abdumannon	9	1	2026	Abdumannon	Fetched all sales	9ed77988-96af-4be9-a370-f5ed849a21fe
Abdumannon	9	1	2026	0000	Fetched products for shop	75801889-afc0-4e07-bf33-d6ba7f80185c
Abdumannon	9	1	2026	0000	Fetched products for shop	166b94e2-a003-439f-9912-d970ab7a6a24
Abdumannon	9	1	2026	\N	Fetched all categories - count: 4	5e85af56-b69a-4cbe-b588-b107af8a37e5
Abdumannon	9	1	2026	\N	Fetched all categories - count: 4	f21f3153-8f70-4cdf-b1ad-4a07d696f929
Abdumannon	9	1	2026	Abdumannon	Fetched all sales	2bd50eaf-e200-4216-8ac4-0e3ed9e90087
Abdumannon	9	1	2026	Abdumannon	Fetched all sales	23a8a96e-6e40-4c58-ad7b-d9b00ab4e86b
Abdumannon	9	1	2026	0000	Fetched products for shop	23ad3dac-7b37-432d-b2b5-88cbefd60c08
Abdumannon	9	1	2026	0000	Fetched products for shop	0b3ab0ac-01a1-44a0-aa93-9682dfd75d65
Abdumannon	9	1	2026	\N	Fetched all categories - count: 4	c9430a2f-4aa2-4055-b595-235d7f5e8254
Abdumannon	9	1	2026	\N	Fetched all categories - count: 4	3b34cb1f-8768-4cea-a789-c406f7eb606d
Abdumannon	9	1	2026	\N	Fetched all categories - count: 4	76bbbc63-46dc-4c72-aef0-d1bc46a8a0fd
Abdumannon	9	1	2026	\N	Fetched all categories - count: 4	7c452d0f-920c-4e46-990b-6d3c9d8167e8
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	220c0144-3603-4d52-8c8a-518a73d76be1
Abdumannon	9	1	2026	0000	Fetched debt statistics	5163de33-d812-489a-b1b3-b8b5b1026234
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	292f03d2-79e9-4708-a035-3504675f4e21
Abdumannon	9	1	2026	0000	Fetched debt statistics	1138d0e9-f420-48ae-be91-b50f94fcb895
\N	9	1	2026	Abdumannon	Fetched all wagons	a4b93f5d-a0b1-4d3b-9e99-73cb7dda0906
\N	9	1	2026	Abdumannon	Fetched all wagons	58f8f789-a68a-4a1e-bcc8-af78aa5fffc3
Abdumannon	9	1	2026	\N	Database backup downloaded - tables: 14	7e708c34-df51-44d9-93da-2852562215f6
Abdumannon	9	1	2026	\N	Database backup downloaded - tables: 14	1663e934-099a-43d3-b30b-74e868c0e0c6
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	1f3e6a4b-f9de-41a7-b7d9-488d99195e5a
Abdumannon	9	1	2026	0000	Fetched debt statistics	23e4b448-2031-4207-a95a-cef67118cca9
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	7358be84-be3c-4af8-81f6-fa48ff18d3b4
Abdumannon	9	1	2026	0000	Fetched debt statistics	a56c08ea-0145-477b-8fb0-43930ee09958
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	de6122f1-a635-474a-8465-645fd9ea5cc0
Abdumannon	9	1	2026	0000	Fetched debt statistics	a5180e85-3242-43cf-83ef-aa8152eb9cd4
Abdumannon	9	1	2026	\N	Delete debt failed - missing debt ID	70224982-dd1a-4b10-93db-4e6e05d696fa
Abdumannon	9	1	2026	\N	Delete debt failed - missing debt ID	24c81869-183e-432b-bfc6-1ec994f89a15
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	fde46b67-c05d-40f8-8f40-c8ccd036d6b9
Abdumannon	9	1	2026	0000	Fetched debt statistics	92c6540a-c67e-490c-96d4-f57090781e4d
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	cb58cd93-b4e3-4fff-a0e3-97b287476ebb
Abdumannon	9	1	2026	0000	Fetched debt statistics	9dac40ae-d814-469b-bfd0-46727918d9d5
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	d01ee669-1ba9-4661-ad30-6d1c78629f1e
Abdumannon	9	1	2026	0000	Fetched debt statistics	6ea90160-56ac-417d-99b1-26fd3a7a088e
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	5c1cc8e8-4bf1-4784-b9f0-22ec4fb03f9b
Abdumannon	9	1	2026	0000	Fetched debt statistics	e728fa33-dac0-4834-8659-941f39ca58ab
Abdumannon	9	1	2026	\N	Fetched all categories - count: 4	64e2c207-36e7-4859-b548-a0b1eecfdddd
Abdumannon	9	1	2026	\N	Fetched all categories - count: 4	134dc0c1-e933-46d6-8626-623d9496dc49
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	10876a3f-4428-4b8a-bacd-cc3ed08be0d2
Abdumannon	9	1	2026	0000	Fetched debt statistics	38a4dfab-ffe2-4216-a17b-d7ad078179a6
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	389cfa77-7b9e-4df2-9213-3e6448ccd6e5
Abdumannon	9	1	2026	0000	Fetched debt statistics	fef2d695-7112-4107-aff1-97929b33be25
Abdumannon	9	1	2026	\N	Delete debt failed - missing debt ID	b0474c3a-6bb1-4ea7-8ef8-8165fca1fbbc
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	a1ded80a-a153-4f99-bd6c-ddfb33a8f5c3
Abdumannon	9	1	2026	0000	Fetched debt statistics	36fcb9eb-89d2-4459-876e-3d2f9d19205e
Abdumannon	9	1	2026	0000	Fetched all debts - count: 13	55b341e2-5d10-4f54-b50b-cce0951fda43
Abdumannon	9	1	2026	0000	Fetched debt statistics	62fb78ab-4638-45c9-a113-2e11d7709c5f
Abdumannon	9	1	2026	\N	Debt deleted successfully: 06cbeaa9-6c8a-4b95-9327-3d006ed50bce - customer: Sobir	9efcd08e-25ee-4741-945b-707386dd1b41
Abdumannon	9	1	2026	0000	Fetched debt statistics	3dfb948a-c90a-471c-b114-88dbf814ef8f
\N	9	1	2026	Abdumannon	Fetched all wagons	c3a3c174-85f3-4f13-8447-46b943ea942a
\N	9	1	2026	Abdumannon	Fetched all wagons	29035a1b-3083-4f89-8a8e-c0bb65c0e6cb
Abdumannon	9	1	2026	0000	Fetched debt statistics	84923ad1-09e9-43e1-9677-bf0e64ebd8ac
Abdumannon	9	1	2026	0000	Fetched debt statistics	2f13689f-e933-4986-b201-e4fbdc8bafa7
Abdumannon	9	1	2026	0000	Fetched all debts - count: 12	63dc5d5a-1cf6-4b3d-b253-8395c2cbfbf3
Abdumannon	9	1	2026	0000	Fetched all debts - count: 12	9e96b89b-fe1a-4f83-8a3d-663685ce828d
Muzaffar	9	1	2026	0000	Fetched all debts - count: 12	59a62a63-0c5b-4139-b1ae-604a44b0c250
Muzaffar	9	1	2026	0000	Fetched debt statistics	cfc0356b-3abc-48ed-a1e0-06eeae0dd363
Muzaffar	9	1	2026	\N	Debt deleted successfully: 1f8698af-1d21-4316-8baf-ddccbee1438c - customer: Abdulaziz	0a8424c2-7241-402e-a7be-2741bf387aa3
Muzaffar	9	1	2026	0000	Fetched debt statistics	4f72fd78-a64c-460a-85c2-eb7326220e39
Muzaffar	9	1	2026	\N	Debt deleted successfully: beb947c5-20c4-4447-9c2e-4072bee69a7e - customer: Abdulaziz	555ebead-b471-4317-a891-f65df761a56b
Muzaffar	9	1	2026	0000	Fetched debt statistics	d260263a-0894-48bc-a646-2eee39196591
Muzaffar	9	1	2026	\N	Fetched all categories - count: 4	0f4af5fb-32b3-4c51-bf61-7a47c156f5e8
Muzaffar	9	1	2026	0000	Fetched debt statistics	e17b204f-2b3c-4aeb-8072-7ef93cdb6883
Muzaffar	9	1	2026	0000	Fetched all debts - count: 10	87ced06b-e8fa-46b1-b6af-97c2d79bf799
Muzaffar	9	1	2026	\N	Debt deleted successfully: bcef0932-8ad7-4cc9-811e-dbe93a1b5b87 - customer: Salom	cf4b9c38-b253-4a68-9ff8-0ed189d2cc2f
Muzaffar	9	1	2026	0000	Fetched debt statistics	dd32f04d-b3ba-4b35-b6ea-95d2129f082d
Muzaffar	9	1	2026	\N	Debt deleted successfully: 930b3fde-5d50-4492-ad8f-39a1c8ba767b - customer: Salom	b469befc-ccc9-44c1-b683-844e244bdb8a
Muzaffar	9	1	2026	0000	Fetched debt statistics	53ad83f2-52c4-4bee-ac5d-7e5d2a27c431
Muzaffar	9	1	2026	\N	Fetched all categories - count: 4	661a0c12-42fa-4f91-a299-40e43a4c9078
Muzaffar	9	1	2026	0000	Fetched debt statistics	db3cfbce-7931-4c6f-abc9-baf09f46c77d
Muzaffar	9	1	2026	0000	Fetched all debts - count: 8	5ef614fd-d21b-4d6a-904b-016f59a0e059
Muzaffar	9	1	2026	\N	Debt deleted successfully: eba76b0d-d1fd-46fe-873f-ae4277b72fed - customer: Qaysi	e555c098-1639-486c-8729-575cb538369f
Muzaffar	9	1	2026	0000	Fetched debt statistics	10612700-b625-4bb1-a157-bd03a0936fe6
Muzaffar	9	1	2026	\N	Fetched all categories - count: 4	bbbfa47f-c053-4857-964d-6e350a663856
Muzaffar	9	1	2026	0000	Fetched debt statistics	025ae68b-4cb0-449b-bae8-1dabdcbf700d
Muzaffar	9	1	2026	0000	Fetched all debts - count: 7	387351eb-3552-4bd3-b6b7-5cc2561c10d9
Muzaffar	9	1	2026	\N	Debt deleted successfully: d150be55-9f8c-4d38-87e8-f6b0578c82bf - customer: Muzaffar	e0a95e8b-214d-4894-a579-92b417055192
Muzaffar	9	1	2026	0000	Fetched debt statistics	716fb8c7-ac3d-46ad-b8ae-2ce2b90bbe94
Muzaffar	9	1	2026	\N	Fetched all categories - count: 4	87f3c006-cb9c-4444-a2d0-b6e6f9697f67
Muzaffar	9	1	2026	0000	Fetched debt statistics	e65c322a-4bec-44e4-b7dd-cf8017defb48
Muzaffar	9	1	2026	0000	Fetched all debts - count: 6	794aedb0-b369-4cba-8e28-1db74fed7fbe
Muzaffar	9	1	2026	\N	Debt deleted successfully: 2a2f8128-c4bf-4067-9796-261e15036109 - customer: johny	eb420c3e-e03b-4480-bef4-26e682b7fab6
Muzaffar	9	1	2026	0000	Fetched debt statistics	ec0fc409-9574-4e5f-8653-e9eccf18febc
\N	9	1	2026	Muzaffar	Fetched all wagons	7ff3ed4c-ac26-4f12-a0e4-187b6310b567
Muzaffar	9	1	2026	0000	Fetched debt statistics	4e0a349b-4c4f-4ff8-bcc3-2f4799ce2105
Muzaffar	9	1	2026	0000	Fetched all debts - count: 5	a9f93bb3-c55b-4603-be79-c745b285c12b
Muzaffar	9	1	2026	\N	Fetched all categories - count: 4	acfb0fd7-1012-4920-a636-7fd9c9c0b7a5
Muzaffar	9	1	2026	0000	Fetched all debts - count: 5	8946c034-5dee-4901-ba66-7584c9344d9c
Muzaffar	9	1	2026	0000	Fetched debt statistics	2e44980a-4632-4df1-ad45-07d0a4d08697
Muzaffar	9	1	2026	\N	Debt deleted successfully: c1c7b0a2-d99e-40a7-9142-40d965baeba7 - customer: Xayrullo	9cbd7e52-d8c1-4bee-8ad7-ebd5ef5d1212
Muzaffar	9	1	2026	0000	Fetched debt statistics	a7bcb1d4-2b67-495b-947a-cd8e01264b16
Muzaffar	9	1	2026	\N	Fetched all categories - count: 4	ec33384b-85b9-4209-8f67-53c39f314072
Muzaffar	9	1	2026	0000	Fetched all debts - count: 4	7c24b5d8-e139-41f1-b715-a45bb158a4a9
Muzaffar	9	1	2026	0000	Fetched debt statistics	7f3222a6-50ae-4411-9e26-e509237801f8
Muzaffar	9	1	2026	\N	Debt deleted successfully: a1d481b5-95e3-4c9c-9cb3-8d7aa415d07e - customer: Davlat	3962d7b0-481e-43b4-8c57-7ecea09c1d52
Muzaffar	9	1	2026	0000	Fetched debt statistics	ffe59289-4bb9-4c62-a6b6-b06c7d0970e5
Muzaffar	9	1	2026	\N	Debt deleted successfully: 2f88bbbc-fc93-480b-ada0-c77508ee2ac1 - customer: daVlat	296102e5-f694-4f1a-ac30-319aa4213621
Muzaffar	9	1	2026	0000	Fetched debt statistics	ae3daa7e-066f-4440-823a-ac5f7ca67fcc
Muzaffar	9	1	2026	\N	Fetched all categories - count: 4	e1a38ee1-2cf2-498b-b8fd-67b0575a885c
\N	9	1	2026	Muzaffar	Fetched all wagons	caedb5da-54d6-482c-b00d-3fce4384422d
Muzaffar	9	1	2026	0000	Fetched debt statistics	125eeab6-6095-439e-8a2a-30bab25a8524
Muzaffar	9	1	2026	0000	Fetched all debts - count: 2	0327dca1-61dc-4bb3-9926-c0121d06df88
\N	9	1	2026	Muzaffar	Fetched all wagons	1f2e7ac7-cb19-46f0-aa58-f0defe58eba5
Muzaffar	9	1	2026	0000	Fetched all debts - count: 2	414b40b6-98fe-4b3d-843f-b44f877b7a13
Muzaffar	9	1	2026	0000	Fetched debt statistics	bed00be6-c393-4270-a9b8-c59ce67794e3
Muzaffar	9	1	2026	\N	Debt deleted successfully: 5364429d-0828-4eda-9e13-e1873e38784a - customer: Kamoliddin	0fab2f9e-4852-461d-bebd-86f4ef9ba8b6
Muzaffar	9	1	2026	0000	Fetched debt statistics	8d6b2ef6-358c-4024-bc6f-b0c8c821b8bf
Muzaffar	9	1	2026	\N	Debt deleted successfully: 4bf5d75b-c82c-4fa0-9e28-f60a7e72df94 - customer: Kamoliddin	0e09aafc-bc8b-496a-a275-6591842214a9
Muzaffar	9	1	2026	0000	Fetched debt statistics	6646d570-ea6a-47ed-a1e7-653313a05370
Muzaffar	9	1	2026	\N	Fetched all categories - count: 4	0aba382e-07f6-4801-b83e-a8d782f6c2f0
Muzaffar	9	1	2026	0000	Fetched debt statistics	7c8bcd5e-737a-4adf-b5af-6d3dba7bf025
Muzaffar	9	1	2026	0000	Fetched all debts - count: 0	f1109c7d-7015-463d-9560-bd74eef69114
\N	9	1	2026	Muzaffar	Fetched all wagons	0ce2ddeb-de9d-4107-af17-0c4916155840
\N	9	1	2026	Muzaffar	Deleted wagon: 9258ddcf-bf7a-4f4d-ad64-7b37e014b3cf	8942268c-b9b6-4c0f-a84b-8ec8256bd738
\N	9	1	2026	Muzaffar	Fetched all wagons	448e918e-0a58-48a6-8608-844c0d2d5a41
Muzaffar	9	1	2026	0000	Fetched all debts - count: 0	de5dbb08-9061-486b-b5c5-32454a8a4617
Muzaffar	9	1	2026	0000	Fetched debt statistics	b36476be-18e5-4239-a960-0113f75d3060
\N	9	1	2026	Muzaffar	Fetched all wagons	a44c1203-4951-4bf2-aa68-e351200ee47f
\N	9	1	2026	Muzaffar	Deleted wagon: 87489772-c0be-4b5e-9cd6-3b32213f262d	d99ee5ee-dda9-4204-b487-9ebc9f090400
\N	9	1	2026	Muzaffar	Fetched all wagons	92a91e72-284d-445a-86bd-1ae33e2f6025
Muzaffar	9	1	2026	0000	Fetched all debts - count: 0	53bede19-5fbf-4c6c-98f2-3f345598cce8
Muzaffar	9	1	2026	0000	Fetched debt statistics	5c9f9bca-362e-4691-94cd-08339deb9a23
\N	9	1	2026	Muzaffar	Fetched all wagons	ac2a6460-885c-40ae-b4d3-04b9c87103da
\N	9	1	2026	Muzaffar	Deleted wagon: cb05b08f-cb3a-40d3-bc64-c7893383eea0	f5b7e57a-0834-4b5a-bded-a3e843221ae8
\N	9	1	2026	Muzaffar	Fetched all wagons	82b83e58-25d0-4974-9fa9-098f63eb7450
Muzaffar	9	1	2026	0000	Fetched all debts - count: 0	fbe46dd1-5a0d-47b1-bee7-b7e8deb37526
Muzaffar	9	1	2026	0000	Fetched debt statistics	ef79df24-343f-47bc-ae0d-c7fc51aa57fd
\N	9	1	2026	Muzaffar	Fetched all wagons	60aa5caf-299b-49eb-9dd4-d1fbe4e00a0e
\N	9	1	2026	Muzaffar	Deleted wagon: 5dee2a59-cb68-4986-adf5-705647306d2d	ef797906-ecae-45e3-bcea-efe8ecc9c7e2
\N	9	1	2026	Muzaffar	Fetched all wagons	be6d5c2e-d4b0-44e9-b48d-1be7f355362e
Muzaffar	9	1	2026	0000	Fetched all debts - count: 0	cbfb7d2a-7406-4874-b00b-d650e46792d6
Muzaffar	9	1	2026	0000	Fetched debt statistics	049b8940-c574-4fd2-b4e6-b5f19f4d18ea
\N	9	1	2026	Muzaffar	Fetched all wagons	e076a047-3661-40a7-b9ef-1761df3aef71
Muzaffar	9	1	2026	\N	Fetched all categories - count: 4	e1a8d48a-d9cb-4da5-8a6c-223a33368d22
Muzaffar	9	1	2026	\N	Category deleted successfully: Food	94a645f5-c391-4302-a619-3f76bc9b06b1
Muzaffar	9	1	2026	\N	Fetched all categories - count: 3	3a99c766-1e77-46ac-8f2c-07a70047b46f
Muzaffar	9	1	2026	\N	Category deleted successfully: Ichimlik	eeabce1b-b32d-45d5-a1db-698d0012b940
Muzaffar	9	1	2026	\N	Fetched all categories - count: 2	4a95e8ab-06f5-48f9-9851-dc10d7bb92e7
Muzaffar	9	1	2026	\N	Category deleted successfully: Non	7706241c-f281-4ac6-9bc6-ba3165fd18ff
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	a2033c81-f6d0-4f9c-bf90-a644517bb84c
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	dbcc0ddd-ac71-4cb9-8ae9-b3ccece12dd6
Muzaffar	9	1	2026	0000	Fetched products for shop	761602bb-c4c1-4b0a-a438-b85591d8f329
\N	9	1	2026	\N	Product deleted: 52b77d14-5bfb-428a-98ad-f4d2a6480fa5	c4e72037-ef20-48f5-b56c-efa06ae97dce
\N	9	1	2026	\N	Product deleted: f18ea1c0-14ba-4d44-be1e-376b743a62cc	88c8da7f-06a0-4ad6-a610-d2a4072a0f14
\N	9	1	2026	\N	Product deleted: 90efd7fc-1711-4e46-9a55-c7a40bbb41e5	415422d8-ced9-4a4b-89e3-7b952c2c2bf6
\N	9	1	2026	\N	Product deleted: c3e111f7-da9b-4645-bad1-b08582aef8ba	02d68546-8efc-4d2b-9edb-82d3a6d7d7b2
\N	9	1	2026	\N	Product deleted: 2b40b1b1-5532-422b-80f5-335513df1286	81e260d0-dff4-432a-b316-b6d927161cdf
\N	9	1	2026	\N	Product deleted: da18ad8d-220d-45df-b208-df2d19cba6b9	77dcd9b5-d24e-4c87-aec3-3e3595dc138f
\N	9	1	2026	\N	Product deleted: 8a3a966a-62d7-44d1-9e2d-0a1eae4ac6eb	e38e8d46-809f-411c-8efc-fda798e44733
\N	9	1	2026	\N	Product deleted: 7dc6d744-d616-4927-aa66-22ed8373b7a7	c21ea7c1-322d-4b22-8e21-2a7809ba90ad
\N	9	1	2026	\N	Product deleted: 117b6de9-167e-4092-8f77-43ed091bea0a	a27ac57e-bb1c-4e79-ad68-1aa3f2347ee0
\N	9	1	2026	\N	Product deleted: c1fa21a0-8889-4b05-8b80-da647ed38001	32d13fcb-4531-4b46-b584-38744f519f0d
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	9ed00ac7-effc-4144-a6fd-21ddebb942a4
\N	9	1	2026	Muzaffar	Sale deleted successfully: 7f5e871b-c9df-479f-be45-5af4d5a82c88	9dabaea7-7026-46f6-9d2e-3592bf862fb4
\N	9	1	2026	Muzaffar	Sale deleted successfully: 0674804c-59db-45b1-94c7-af7d34941f9d	b78b99a1-14f8-4f7a-ab0d-726043fd357d
\N	9	1	2026	Muzaffar	Sale deleted successfully: 52ff0e1c-8563-400a-a26b-a27b15b15f5a	68c35989-e7bf-44f5-b2b8-1d1906562418
\N	9	1	2026	Muzaffar	Sale deleted successfully: 9313157e-9bd2-48af-b160-a9a3d1e33473	7301ea17-619f-4e7d-9227-8155d88127eb
\N	9	1	2026	Muzaffar	Sale deleted successfully: 6987b7b2-531f-4164-aa77-45e74833cb06	3d6e4942-fe5f-4bcd-abfa-c459d89258cd
\N	9	1	2026	Muzaffar	Sale deleted successfully: ee2ace91-ecaf-4c4b-b892-2b720ad4ce5a	d91e20b7-4504-49c3-b158-a23c6e668856
\N	9	1	2026	Muzaffar	Sale deleted successfully: 4a9d4b29-9ed3-4c77-abf5-e5d11c4d685b	33f63745-5718-4eb0-a95c-c68eb2d017ff
\N	9	1	2026	Muzaffar	Sale deleted successfully: adbc4bfe-bbfa-42f8-82e9-5be1217904dc	0dc161ee-232b-4813-8d25-3ab4a213990f
\N	9	1	2026	Muzaffar	Sale deleted successfully: 528cef18-f13f-4f0e-b359-27992e1af384	4bf3e724-fcf2-4e9c-a997-b0ddd0f6a4d6
\N	9	1	2026	Muzaffar	Sale deleted successfully: 2edabfdb-1608-4513-8709-aa082703786f	534a2390-2124-498b-990a-70acb5073616
\N	9	1	2026	Muzaffar	Sale deleted successfully: 41831bd7-76ae-4918-a12c-dd1d1894335c	26c283f3-ea07-4bfa-bfc5-98a4fcf38913
\N	9	1	2026	Muzaffar	Sale deleted successfully: 1b9142c2-0b9f-4ca8-9b8e-7b42b978e1fd	9de4dc53-4642-46b4-b54c-2625eebc7f2a
\N	9	1	2026	Muzaffar	Sale deleted successfully: 8ed4739d-d27f-4d1a-862d-6a0946d5d316	52cc93f2-97c6-4605-9358-da09454b4ee9
\N	9	1	2026	Muzaffar	Sale deleted successfully: 9661769d-b01a-4d58-bf5d-f67bf6fc2b8c	58f3054e-54ac-4f90-8cd6-07566f98419d
\N	9	1	2026	Muzaffar	Sale deleted successfully: 3c2e6db1-c6c6-4a15-b01e-d671e02df760	44299c9c-9e0d-478f-8bbe-c68a03ef5e34
\N	9	1	2026	Muzaffar	Sale deleted successfully: 9b769fa1-29bf-4ad5-9d7a-4b8b623848ac	a4e82d5b-59c8-4382-8127-8701bc942b62
\N	9	1	2026	Muzaffar	Sale deleted successfully: f5e85c20-1282-43bf-a360-3ab2241a592e	3a0d23db-cede-4e9c-bd75-21b6896704a9
\N	9	1	2026	Muzaffar	Sale deleted successfully: 399d091a-2afc-4133-aa76-197ac782f0c3	927e845c-cb11-47c3-9152-b9e2d6e778c6
\N	9	1	2026	Muzaffar	Sale deleted successfully: 1ef0bd11-30d2-46f8-b0e6-7c0c3ebe3e89	956b4e0a-f24f-4e4e-8be8-45f53f94a432
\N	9	1	2026	Muzaffar	Sale deleted successfully: 73a21334-f928-4345-b5e8-6d006f1b97c8	45aa3eac-5359-40d1-849f-5bbfcbe0787a
\N	9	1	2026	Muzaffar	Sale deleted successfully: b90ffb9d-5622-4b1b-a5ff-9802548e8ca3	234ec353-a333-411c-ab55-b575768cd771
\N	9	1	2026	Muzaffar	Sale deleted successfully: 06b4e529-3e02-40a9-af9c-deac167af6c2	8c8987a3-93c8-4c8d-91fd-23779a30e728
\N	9	1	2026	Muzaffar	Sale deleted successfully: 18a83901-5c75-47f2-88eb-ef72c8306e1b	2c6fd2ab-14c5-4721-ad1a-5d7c2666c063
\N	9	1	2026	Muzaffar	Sale deleted successfully: f224331c-0a63-4c4a-b405-ca658a159355	da3d1e06-011e-4a7c-9794-3eaa6c99ba7c
\N	9	1	2026	Muzaffar	Sale deleted successfully: 953c7411-4fe6-4019-a67a-264df4fb8d5e	46db4062-03b9-4175-954b-fc39e9c7178f
\N	9	1	2026	Muzaffar	Sale deleted successfully: da339bba-194a-41a5-af13-f56b013ca35b	bed815a1-a77c-4304-a6f1-722f8748bf9e
\N	9	1	2026	Muzaffar	Sale deleted successfully: 21957812-0c2f-4fe9-97f4-c41e0ae03e2d	7e0f0570-2bea-48e2-bb25-f798111b61c4
\N	9	1	2026	Muzaffar	Sale deleted successfully: f7316cb2-0f72-48fc-8931-a3ec355b7a01	7ef57254-6530-48a3-93b5-0e898501e3a0
\N	9	1	2026	Muzaffar	Sale deleted successfully: e00d63e5-b4b8-4aca-a27a-c2711ab122e6	35301de8-d393-4bba-9d2f-8a94b3d1caea
\N	9	1	2026	Muzaffar	Sale deleted successfully: 5975cb0a-3482-417b-b2b5-14df90b0ca05	d6056d45-3ca8-4fcb-84c1-fdf62c505ce2
\N	9	1	2026	Muzaffar	Sale deleted successfully: e36a8b06-8d00-4eb5-9d5f-6d4157af2bfa	2455c8fb-784b-4583-be03-20ba5f5d8c89
\N	9	1	2026	Muzaffar	Sale deleted successfully: 1363c8ad-64b8-4689-8269-0d884a885074	d70e7139-ef55-4e0e-852e-e498ff2a7438
\N	9	1	2026	Muzaffar	Sale deleted successfully: eab2e5f3-b929-4218-85c0-294ea25f2e8f	fa163dc2-d7db-4e5a-a3df-5213a7a25c1c
\N	9	1	2026	Muzaffar	Sale deleted successfully: 4b56bc9b-ab48-4596-bdeb-053c283bf030	eeaec1b2-e727-45da-b9a9-500b9d7fae7e
\N	9	1	2026	Muzaffar	Sale deleted successfully: 657e85cb-ff60-47c3-9597-7374c4a48e12	8c87ccda-a2c5-458e-b5a1-29b8d5ea5cb1
\N	9	1	2026	Muzaffar	Sale deleted successfully: 1bb2bac3-3dde-4f53-a5b0-8e1b3861fd2c	43e387eb-41e1-40ff-9e83-5f0bf515edbc
\N	9	1	2026	Muzaffar	Sale deleted successfully: c0dd6072-006d-40a6-a4a6-6f7363edc670	7dda8f45-66f8-4452-8f4f-a37fcba47fb2
\N	9	1	2026	Muzaffar	Sale deleted successfully: df62c835-ef45-4ef0-a6cf-adcd5308e0ab	fdcb63f7-9067-462a-a64b-92d983bd5e6f
\N	9	1	2026	Muzaffar	Sale deleted successfully: 082ac668-07be-464e-b011-13ae49f0673b	2e98841a-edcb-4f71-8130-b222d773e2eb
\N	9	1	2026	Muzaffar	Sale deleted successfully: 1597ec04-d461-44ca-87f5-64bd56877292	7173fca0-e93b-4c18-bf56-3538851a0930
\N	9	1	2026	Muzaffar	Sale deleted successfully: 2d826d36-06b2-44da-b1c8-ff13e456f269	421e5390-61c5-4f4d-83ae-e5c253709fa7
\N	9	1	2026	Muzaffar	Sale deleted successfully: c8178332-1b8e-4c18-8052-d71b231e923f	6880c853-9930-467b-a6aa-076605295c28
\N	9	1	2026	Muzaffar	Sale deleted successfully: 301cf017-d0ee-4949-954d-4520774ddb72	57be81ce-fb5b-4857-8769-7ecdb64db4bf
\N	9	1	2026	Muzaffar	Sale deleted successfully: 3181a300-bda0-419d-af57-70941345f9b5	f27afed8-1fea-4ffd-90a4-b7e93f61aeb6
\N	9	1	2026	Muzaffar	Sale deleted successfully: 4288378e-6020-4235-9a76-d18a4b56ecfb	bb3a8f63-42bd-484e-9d80-0fa504ba7682
\N	9	1	2026	Muzaffar	Sale deleted successfully: 449fccaa-e68e-4fc7-a288-0da259760025	0b31b788-f1c7-4288-ab1b-ab80b1d6f3b6
\N	9	1	2026	Muzaffar	Sale deleted successfully: df01c05a-1dbe-4fc4-b92c-352d5ba47017	223dc09e-f027-4dbb-8163-b6ada399ef67
\N	9	1	2026	Muzaffar	Sale deleted successfully: 44aef04f-6704-4a48-8232-46bb6c60fb6b	e02fdcdf-a14f-44d7-aed3-d1480a652bb2
\N	9	1	2026	Muzaffar	Sale deleted successfully: c32bf7eb-1f9f-46e5-abff-1d63eccc0e8e	58bf4b6f-5b4b-4106-afd5-50cc1f98f8e4
\N	9	1	2026	Muzaffar	Sale deleted successfully: 4f895b3f-a048-4c23-8d27-f2d3614ec751	c2517f10-f19e-4f33-9b3e-448632abaeb3
\N	9	1	2026	Muzaffar	Sale deleted successfully: c61a6ab2-849f-456e-874b-c6128fb82374	b956c8c6-9efc-4b48-8eca-506df74e615e
\N	9	1	2026	Muzaffar	Sale deleted successfully: 38f74ef1-0593-4ed5-8043-178e76da5ec4	ff7ebaf2-544c-4de0-a33d-d3a630ba1a2f
\N	9	1	2026	Muzaffar	Sale deleted successfully: 17d3f1d4-4930-4bc6-8dc0-504faf222c1d	dbc6370d-0632-4dc3-956f-0286fe22746f
\N	9	1	2026	Muzaffar	Sale deleted successfully: 4b18c951-a7e4-491f-94c4-c0657912b0de	936784aa-9680-436e-a4a1-cb1e252a50fa
\N	9	1	2026	Muzaffar	Sale deleted successfully: 0ddf9c90-f21e-493f-b5db-e933712b78f2	23a5f1d1-7f05-41ad-93f7-d5dc82be3995
\N	9	1	2026	Muzaffar	Sale deleted successfully: e1fab004-8dec-472d-9541-6a59e3804ef9	8022e07c-9bd5-466a-a219-852528fa72d0
\N	9	1	2026	Muzaffar	Sale deleted successfully: fcacfc45-4ffe-49e5-91f9-74d515a30501	4284e0f1-42c6-44dd-ac61-430733feea08
\N	9	1	2026	Muzaffar	Sale deleted successfully: 1b112df1-a9d3-4319-be30-888acf11ae26	d486a6fb-2088-41d4-ba95-b4b6a4bc910f
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	103c735c-8499-480a-90a4-4d5009516ff0
Muzaffar	9	1	2026	0000	Fetched products for shop	9cc289a9-4eb7-4b31-8957-d13a57a26d77
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	17460b00-0232-44d1-badc-96fcbbd7adbb
\N	9	1	2026	Muzaffar	Sale deleted successfully: 5faa2eda-98d7-483c-83f6-60d6c79a6838	b555229a-1390-4663-81b0-cb9663fde2c6
\N	9	1	2026	Muzaffar	Sale deleted successfully: 3e35a658-6812-433d-9258-28f2f564f85f	3bf56832-4d41-47b7-a628-a7007e3c311a
\N	9	1	2026	Muzaffar	Sale deleted successfully: 3b73e0e6-0347-41e2-bfac-285785e66f6d	cb8af865-4f74-4f60-a503-7004b6d40b00
\N	9	1	2026	Muzaffar	Sale deleted successfully: 3802513d-acd0-4225-8d39-cb3e6ca3b555	6eb6c3ef-f7ba-4494-baac-fb4237031a05
\N	9	1	2026	Muzaffar	Sale deleted successfully: 8878b4b4-07f3-49fe-aa62-caa30c8e6681	93c963f8-20bf-420a-a0c9-89ab4e19eaae
\N	9	1	2026	Muzaffar	Sale deleted successfully: d44dfc32-f7af-48e2-8941-2825fda09f38	97475a75-6085-438a-8de4-e803b5c89023
\N	9	1	2026	Muzaffar	Sale deleted successfully: 5846cf51-024e-402f-911b-3b136ef1db53	8509bd21-e1cd-40c9-a1d8-06f8423924bd
\N	9	1	2026	Muzaffar	Sale deleted successfully: 6701baaf-2d95-4059-b4be-85688b94ef0e	8f0977c8-1635-4390-ae1c-5e00d52a5cb8
\N	9	1	2026	Muzaffar	Sale deleted successfully: f3c0c46d-4b53-4dde-8fb9-4f052e41edbc	2025a456-1d77-4cc8-9704-01449c2025d4
\N	9	1	2026	Muzaffar	Sale deleted successfully: b8e6add6-e1bf-4cb9-8598-0f2b7d1374f9	6243d3da-e691-421c-bf2d-ac6a615cd827
Muzaffar	9	1	2026	0000	Fetched products for shop	27348b21-9c4e-407a-8490-45d5207b3939
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	0793977e-b79b-4644-ab14-94a8394b3e90
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	779026dd-3798-43b4-88f5-744e2960c48e
\N	9	1	2026	Muzaffar	Sale deleted successfully: ef514f67-5538-47ba-9ede-7d2b891fe7f5	c64872dc-226d-4bfe-ac15-8ff0ed7c4844
Muzaffar	9	1	2026	0000	Fetched products for shop	c6e52623-d8a0-41e3-8236-dd91f3c540ea
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	884781da-9f29-4500-8216-f730c0099824
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	28fc73f3-58b1-46ca-8db3-d764ac3f3ccc
\N	9	1	2026	Muzaffar	Sale deleted successfully: 9c005e83-33f1-49be-8125-51c800b3b256	b07d6653-6349-4ef3-98f9-e289fa07defd
Muzaffar	9	1	2026	0000	Fetched products for shop	8171e44a-e5d5-42f2-98c9-01ee0865d566
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	1841fb8a-89ec-4600-b6b6-a095828a23e7
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	18392950-21a9-4c82-be43-68e67ca02171
Muzaffar	9	1	2026	0000	Fetched products for shop	45a3cb1d-76d2-4622-a926-d5bafb5908c5
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	5c54fba6-c0ed-442f-8fee-eff7c153aab4
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	8ea7545c-a789-4d6a-8321-25cad39b3e8c
\N	9	1	2026	Muzaffar	Sale deleted successfully: aad19139-4b8c-4dd8-b152-43063f46282c	1c273399-5e33-4e20-9012-929bd25573c4
Muzaffar	9	1	2026	0000	Fetched products for shop	17ff9518-78e9-461d-b540-4873312d97ae
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	03f0fe35-8955-49b9-ba49-46652458f3b3
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	cbf10440-c058-4fe1-97bc-99c75eeb8c58
\N	9	1	2026	Muzaffar	Sale deleted successfully: 4ca34cad-19a9-489d-9c9e-11055a25102b	1fa26428-a671-4b74-bad1-0cfb79412c4b
Muzaffar	9	1	2026	0000	Fetched products for shop	539b4012-8bdb-496a-aa37-e1d1ec39c32c
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	25666447-529b-47e0-a5a3-6d97f1cb3e5a
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	462d6a3e-f6fe-43ef-8ccb-a822fed24050
\N	9	1	2026	Muzaffar	Sale deleted successfully: c4c0ca68-f406-4b16-ab25-fb737e31a505	fc4e3497-270d-49e6-893f-6b5f29498a39
Muzaffar	9	1	2026	0000	Fetched products for shop	0dd89221-9dcc-441a-b0a8-532418eb5e0f
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	8b66a318-d6db-4638-8d03-967f98c98828
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	99f3de9d-404b-4fe2-943d-706d37841af6
\N	9	1	2026	Muzaffar	Sale deleted successfully: bba120b3-8337-4326-9d56-39ed5523605d	bb370b8f-ac60-4bcb-a206-9a2a56035cf8
\N	9	1	2026	Muzaffar	Sale deleted successfully: b240b112-9666-4c80-b612-26e055094d91	4206f208-5b0b-4cd4-a6c5-c90a2f528137
\N	9	1	2026	Muzaffar	Sale deleted successfully: faeefa31-68a8-4ff0-ae03-c678565d49db	2b771179-4e45-4e17-8bc8-dd35bf57d798
Muzaffar	9	1	2026	0000	Fetched products for shop	d7b31668-372e-4cdf-91ed-de1bd75827f5
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	f95d3081-cf72-4702-a910-5d48d935b511
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	c062123d-ec65-477c-998b-ebc0507f0e48
\N	9	1	2026	Muzaffar	Sale deleted successfully: f3d924a2-f918-4c27-84a5-524573cacd9d	8ab176ec-1ade-4c43-b0fd-53099897a926
Muzaffar	9	1	2026	0000	Fetched products for shop	b3f72cb4-e316-4697-9524-c4c762385f6e
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	d726a1b6-398e-46be-8083-0ee607554f12
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	695e433a-3ffb-4819-bfcb-aba992c407c3
\N	9	1	2026	Muzaffar	Sale deleted successfully: 232192b8-27fe-49d0-a456-6b3ae2e0de25	4d6c55ff-eb67-406b-b59f-2d314e812827
\N	9	1	2026	Muzaffar	Sale deleted successfully: 127d8faa-9bff-49a2-9f79-19efb09fc322	6ea89d46-ea4c-4170-87a4-b076a0a39311
Muzaffar	9	1	2026	0000	Fetched products for shop	160e7270-8cbe-4259-9d66-36b30e4d8246
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	68e51a9f-0ba2-4bde-a52e-4030a3cbc201
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	84116750-8ee3-477c-9fb7-511c6523446e
\N	9	1	2026	Muzaffar	Sale deleted successfully: ad664556-2909-439d-a69a-d4edc1ea1d4c	3c9f39d6-245d-4c7a-affd-bc23788ce77d
Muzaffar	9	1	2026	0000	Fetched products for shop	d7dd5d9f-2f8c-47a7-b14d-17a82ba5859f
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	410fccea-2f13-483a-a60f-f4ee962e353f
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	310566db-d093-463a-9c47-cd1d5bc673f7
\N	9	1	2026	Muzaffar	Sale deleted successfully: b78c4dbc-a3d5-4829-be79-414893413efe	46087e83-1a5d-4853-9c49-9cf6cc71d55d
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	d0063ab3-0f7d-4351-9967-51c6e3ab5620
Muzaffar	9	1	2026	0000	Fetched products for shop	18880847-e102-47be-abc1-b1b38dc2e7b4
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	f45c095f-2a78-4358-970d-b54bee271e7c
\N	9	1	2026	Muzaffar	Sale deleted successfully: d1be09e3-e1ba-40c2-9b54-835d8beeb70b	2a4da7f8-6ff8-4575-93d3-6ff32e75d4c8
Muzaffar	9	1	2026	0000	Fetched products for shop	46b16a70-5d04-4e0f-9880-7e529f3f81d1
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	5abf2fd1-642c-499e-ac3a-01d1743e2b4f
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	cedb16d3-61c1-464f-a8f6-01466b94e493
\N	9	1	2026	Muzaffar	Sale deleted successfully: 8415afdb-5ee4-4417-a1a6-7f2d8edd8f16	150029d0-ba4b-4136-825f-8275d81830d9
\N	9	1	2026	Muzaffar	Sale deleted successfully: 44c0ce3f-b1c8-4fec-a702-b75396712a57	5900e721-5af9-427d-959d-183dbbb794d3
\N	9	1	2026	Muzaffar	Sale deleted successfully: 6b270c55-fc71-4684-9492-591bd2cf748d	b2dfe9f0-c9e0-4f4c-bef0-3e1a3a761ef1
\N	9	1	2026	Muzaffar	Sale deleted successfully: ec616336-618d-496d-b7ae-b3feabd9ebd2	be8276f7-f5d7-4a32-86b7-4418b81a3e20
\N	9	1	2026	Muzaffar	Sale deleted successfully: ecdc6bf0-73ab-4814-a51b-90d5a6e0e286	5ac9406b-5548-4adf-ae3e-7c6c1901c1ec
\N	9	1	2026	Muzaffar	Sale deleted successfully: a9a36cf7-1091-44da-b1c9-55c0185058b7	a0b220aa-cec9-4c9e-9319-7c263ce137c2
Muzaffar	9	1	2026	0000	Fetched products for shop	61c5a029-e46f-4a38-90e6-923e548b1f17
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	47bfc1d6-980e-4502-a573-105584455eeb
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	c5e9279f-3d98-476a-bdb6-c658d3f9d440
\N	9	1	2026	Muzaffar	Sale deleted successfully: 24a705b8-af18-4d7d-ba31-34941ae61abc	7ed3504b-ad19-4932-af0d-cfa202f6dffe
Muzaffar	9	1	2026	0000	Fetched products for shop	821c054b-2f69-4b3a-b9ae-bb4e7bab96f7
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	34d0ace3-bfcd-48fe-85c0-68597d67691d
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	d4ab29e8-b8bd-4fe5-9433-99e9e5f69727
\N	9	1	2026	Muzaffar	Sale deleted successfully: 731d3045-bc6a-4689-bf64-4856448ca4d2	a531fd30-7ad6-4ad6-9952-95e1ecb44c9e
Muzaffar	9	1	2026	0000	Fetched products for shop	f5de194a-30d3-4178-b69c-1d3e096fb1db
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	911f3cd4-1da5-4754-9b65-804d268e0d26
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	05220e14-4982-488e-9b1f-417ddb91141b
\N	9	1	2026	Muzaffar	Sale deleted successfully: dd9faf32-b915-4252-a250-e14c412a6d06	dc472358-71c7-4b78-a101-3c0d67dbd8ad
Muzaffar	9	1	2026	0000	Fetched products for shop	e3adafb9-7624-4089-9a81-3cd9d0aebf6e
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	50dafdbf-05f5-4f11-96a7-dba14caf34ed
Muzaffar	9	1	2026	0000	Fetched products for shop	694dd020-a386-4f37-a473-2ed05603ab30
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	e4436da3-d297-455c-b9c6-e8c33fd510f1
Muzaffar	9	1	2026	0000	Fetched products for shop	9dbfdad7-d16f-4437-a431-3dbd06ffee60
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	885ecc5b-f377-4095-844b-f8bf8a7f2e16
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	9414bb2a-0761-40a5-9029-9fd26bdeee8a
Muzaffar	9	1	2026	0000	Fetched products for shop	eeca53eb-337c-4f7e-810b-8bb77a32c22d
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	f865ef0f-f59c-4823-b3ab-fb76c150fa14
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	bab3747e-5123-486c-bb20-2a8da0cff875
Muzaffar	9	1	2026	0000	Fetched all debts - count: 0	88c962f7-ee20-4a43-8915-2171aae56b5f
Muzaffar	9	1	2026	0000	Fetched debt statistics	dae197b4-4445-4b6d-8611-af29112f74ce
\N	9	1	2026	Muzaffar	Fetched all wagons	fa042686-e0c9-4dd1-96d0-63e5d0a3e0a2
Muzaffar	9	1	2026	\N	Database backup downloaded - tables: 14	f3b0546f-f2d3-4a8f-99fe-5b87c7551192
Muzaffar	9	1	2026	\N	Database backup downloaded - tables: 14	f711b7ce-3a78-42e7-87d2-07a0bb433199
\N	9	1	2026	Muzaffar	Fetched all wagons	ee74ab02-5bc7-45fd-80bb-1731ac6236af
Muzaffar	9	1	2026	\N	Database backup downloaded - tables: 14	a1268643-ef2c-48cb-926a-bfbcd9d11bc0
Muzaffar	9	1	2026	0000	Fetched products for shop	19733189-901d-4922-a1cf-9700b9a66730
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	e515fffd-8358-4ff6-9eb4-05ddbbb51067
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	c804b9db-67d1-4dce-b46c-4c9b519a7fd0
Muzaffar	9	1	2026	0000	Fetched products for shop	107b6fed-ed76-40c8-9bef-89c7fc614d44
Muzaffar	9	1	2026	\N	Fetched all brands - count: 3	f107e0f2-f2b0-45b6-95cf-bdb3c98d60c1
Muzaffar	9	1	2026	Muzaffar	Fetched all sales	bdf9411b-527a-4d31-a4ef-e6813bb0a4f5
Muzaffar	9	1	2026	0000	Fetched products for shop	4efb6dd5-2777-43d7-8ca7-44367062eb4a
Muzaffar	9	1	2026	\N	Fetched all categories - count: 1	dd774a59-41d8-4818-a597-3932558167d1
Muzaffar	9	1	2026	\N	SuperUser Login FAILED - not found	5decca57-aa8a-41cb-a030-eccb19ca4c40
Muzaffar	9	1	2026	\N	SuperUser Login FAILED - not found	f963ad7f-4c1f-48a0-95f8-09239765269e
Abdumannon	9	1	2026	0000	Super logged in	5bb79164-1d8e-4a37-83dd-97cf52d12854
Abdumannon	9	1	2026	0000	Fetched products for shop	7c774f48-9577-469f-9a11-65ed97c42903
Abdumannon	9	1	2026	\N	Fetched all brands - count: 3	ebb81cb3-76c5-47da-a4fb-d9b1ea7b454e
Abdumannon	9	1	2026	Abdumannon	Fetched all sales	37043f83-5327-4c28-8e07-b76bf8b659dc
Abdumannon	9	1	2026	0000	Fetched products for shop	2546756a-c23a-4797-83a3-19a387421d9b
Abdumannon	9	1	2026	\N	Fetched all categories - count: 1	eaaf1a1b-74a3-44aa-9f1d-14da6004cf61
Abdumannon	9	1	2026	0000	Fetched products for shop	167f5cb8-947d-492f-a2f0-46986db4bcf9
Abdumannon	9	1	2026	\N	Fetched all brands - count: 3	494f7713-ff4c-4f50-bda7-90516566fe2f
Abdumannon	9	1	2026	Abdumannon	Fetched all sales	534f7688-7486-4bd7-a492-0188e1eb41e1
Abdumannon	9	1	2026	\N	Fetched all categories - count: 1	1e97dc77-1399-44da-b8a7-2d6913028709
Abdumannon	9	1	2026	0000	Fetched all debts - count: 0	4bb03262-c72b-4476-b751-40b596d7aae7
Abdumannon	9	1	2026	0000	Fetched debt statistics	8eb6b395-f7e8-47ff-a069-3733b90b5dab
\N	9	1	2026	Abdumannon	Fetched all wagons	7e56641e-81b5-4176-9474-f7dd73aef841
Abdumannon	9	1	2026	\N	Database backup downloaded - tables: 14	2f4a5695-bd76-4297-9718-7dc759d0572f
\N	9	1	2026	Abdumannon	Fetched all wagons	e421dba6-d3c8-4fab-9820-cee31d801e73
Abdumannon	9	1	2026	0000	Fetched all debts - count: 0	79b466d4-2141-43dd-a300-87eb3597c824
Abdumannon	9	1	2026	0000	Fetched debt statistics	252464ff-145f-4584-99d6-5b7b54363f7b
Abdumannon	9	1	2026	\N	Fetched all categories - count: 1	e81c413c-365a-4fa2-a0cf-13db049896a9
Abdumannon	9	1	2026	\N	Fetched all categories - count: 1	8734fedb-8c85-4400-83e7-7011638331f0
Abdumannon	9	1	2026	0000	Fetched products for shop	bad845c2-7103-4ff1-8a77-5d9203c6e0c3
Abdumannon	9	1	2026	Abdumannon	Fetched all sales	bcb85198-1346-447d-bdf7-798478394b1c
Abdumannon	9	1	2026	0000	Fetched products for shop	da3d7b07-713a-468f-8f7d-02c3f8bbf961
Abdumannon	9	1	2026	\N	Fetched all brands - count: 3	a39e3dfb-821c-492a-8501-915129adf124
Abdumannon	12	1	2026	0000	Fetched products for shop	1f485970-6de1-488b-a374-f09572c88c60
Abdumannon	12	1	2026	\N	Fetched all brands - count: 3	a31aafa4-cf0a-4b0a-884c-9dd1f68926a2
Abdumannon	12	1	2026	0000	Fetched products for shop	de81f1d8-55ca-4e54-95d1-e42c1e32bfbd
Abdumannon	12	1	2026	\N	Fetched all brands - count: 3	9449d602-e5bc-4f72-959e-9078ff81d841
Abdumannon	12	1	2026	\N	Fetched all categories - count: 1	7a2133a3-8d38-4fcd-8f12-d87d707cd42d
Abdumannon	12	1	2026	\N	Fetched all categories - count: 1	0696d0b4-9d46-418c-a04a-ea17ce493011
\N	12	1	2026	Abdumannon	Fetched all wagons	abd5ec8c-d2ff-4751-af70-b3009502035e
\N	12	1	2026	Abdumannon	Fetched all wagons	768053d6-6d28-474a-aa9d-09ac71dc018a
\N	12	1	2026	Abdumannon	Created wagon: salom	8c92460e-769c-4aa3-b063-83af671ed2a9
\N	12	1	2026	Abdumannon	Fetched all wagons	6401c0b3-4ef3-4acc-aab3-cec9e7f31bc3
\N	12	1	2026	Abdumannon	Fetched all wagons	0d992dbc-69b6-4c93-9520-397a6c0a10d3
\N	12	1	2026	Abdumannon	Fetched all wagons	f619fe24-bb22-452d-ab07-f72c39b79981
\N	12	1	2026	Abdumannon	Fetched all wagons	abf33e6b-d803-4c70-b97f-c5789223d889
\N	12	1	2026	Abdumannon	Fetched all wagons	68644fa5-2e6b-4616-a5a4-01b24425328b
\N	12	1	2026	Abdumannon	Created wagon: Vagon	efd5cdeb-3672-485f-bb77-cf951a6846ba
\N	12	1	2026	Abdumannon	Fetched all wagons	f35cbd80-5bdc-432e-85f1-620398b4b056
\N	12	1	2026	Abdumannon	Fetched all wagons	00aeb753-b97f-4a5c-a2c6-8f7ae1e6da50
\N	12	1	2026	Abdumannon	Fetched all wagons	4a90fd17-2c53-4c8e-981a-48e81090581e
\N	12	1	2026	Abdumannon	Fetched all wagons	4a419555-68d1-45e8-83c6-d9d6dfbae9a7
\N	12	1	2026	Abdumannon	Fetched all wagons	02ca29de-67be-48db-a70f-08210aeb6a1b
\N	12	1	2026	Abdumannon	Fetched all wagons	05c830d6-3a9c-4529-bffb-31ea88d55e44
\N	12	1	2026	Abdumannon	Fetched all wagons	4ffe9c9e-5325-427b-8faa-39d827d1c521
Abdumannon	12	1	2026	0000	Fetched products for shop	089f4be7-31e4-454e-a283-308be26e18ca
Abdumannon	12	1	2026	\N	Fetched all brands - count: 3	59c44bd3-98d4-47d0-ae0e-5cd7e1fa87f7
Abdumannon	12	1	2026	0000	Fetched products for shop	63c5cb40-b66f-4992-8fc1-799a7fa1bf92
Abdumannon	12	1	2026	\N	Fetched all brands - count: 3	6621aa7a-fa5c-4f4c-aacb-64cd7dbe5178
\N	12	1	2026	Abdumannon	Fetched all wagons	fd14e275-431d-4743-9911-7fb31007f883
\N	12	1	2026	Abdumannon	Fetched all wagons	6ac3cd8d-02ef-4803-96eb-50c2ff14c67a
\N	12	1	2026	Abdumannon	Fetched all wagons	1e9da3dc-1f63-4dd6-b4c3-ee67d3dc6c62
\N	12	1	2026	Abdumannon	Fetched all wagons	b2eb286f-c1a9-4c12-b8e4-2c5be5683e60
\N	12	1	2026	Abdumannon	Fetched all wagons	ece165b9-fb5e-4ec1-83bf-65136860544d
\N	12	1	2026	Abdumannon	Fetched all wagons	163d9e92-5220-4d0e-98ea-d472183d7d18
\N	12	1	2026	Abdumannon	Fetched all wagons	979cb072-5fd9-4ee1-9b99-8a447935cba1
\N	12	1	2026	Abdumannon	Fetched all wagons	bdecdf0f-c66e-4649-a1d8-7564e89c3829
\N	12	1	2026	Abdumannon	Fetched all wagons	6889eeb0-00be-4192-9a7f-c356dcf94fbc
\N	12	1	2026	Abdumannon	Fetched all wagons	5bb92efb-ed9a-47bb-9d90-a4eecfdede10
\N	12	1	2026	Abdumannon	Fetched all wagons	33406c0c-6138-4fd9-9402-c2ee788ef5d9
\N	12	1	2026	Abdumannon	Fetched all wagons	8e048cb5-b683-433b-9c74-0fa1ecb5f97d
\N	12	1	2026	Abdumannon	Fetched all wagons	64c3c0b5-3e00-4c72-964a-e4539e4f691e
\N	12	1	2026	Abdumannon	Fetched all wagons	399d267c-fb56-4df8-bd21-879c569746ba
\N	12	1	2026	Abdumannon	Fetched all wagons	4dfa358e-7260-4cd8-bb62-b2cea002c6c6
\N	12	1	2026	Abdumannon	Fetched all wagons	0b06a4a4-54c5-4845-825c-971bf9335737
Abdumannon	12	1	2026	0000	Fetched all debts - count: 0	052d5c17-419c-4a9a-b24f-1ab93a8f4924
Abdumannon	12	1	2026	0000	Fetched debt statistics	b49b0d46-2b49-4d89-9124-a58c3bce81f9
Abdumannon	12	1	2026	0000	Fetched all debts - count: 0	523e727a-27e8-4ffc-9a12-0c9422b174dd
Abdumannon	12	1	2026	0000	Fetched debt statistics	fc37a96e-126a-423d-a64a-4fbe3740399c
\N	12	1	2026	Abdumannon	Fetched all wagons	0a85c997-5fd1-46d1-9850-47262fa126f7
\N	12	1	2026	Abdumannon	Fetched all wagons	e245f26e-6a80-439d-9819-93f8d443034f
\N	12	1	2026	Abdumannon	Fetched all wagons	3b451fcb-f74f-49ae-b351-c389aa73e37c
\N	12	1	2026	Abdumannon	Fetched all wagons	b1a2d3d0-fa74-4d54-bf99-16a839695570
\N	12	1	2026	Abdumannon	Fetched all wagons	aef41144-b4f1-4e4c-babe-43c2bcda3f1b
\N	12	1	2026	Abdumannon	Fetched all wagons	29a20cf7-0c7c-494f-b273-c09f511b9967
\N	12	1	2026	Abdumannon	Fetched all wagons	712b6794-53db-4f66-9fcf-4d1175cac72b
\N	12	1	2026	Abdumannon	Fetched all wagons	ec8f25d1-b8ba-4806-9c9a-f7f85d810173
\N	12	1	2026	Abdumannon	Fetched all wagons	12ad9e4a-7b0d-4e7e-bd2c-aedd4a557c47
\N	12	1	2026	Abdumannon	Fetched all wagons	264fc8d3-c2b2-469a-8519-27d0b080257d
\N	12	1	2026	Abdumannon	Fetched all wagons	27c3abb7-a339-4388-a0aa-1f44048d4c6f
\N	12	1	2026	Abdumannon	Fetched all wagons	8ca86878-5c3b-4454-9c64-bbc354870f0c
\N	12	1	2026	Abdumannon	Fetched all wagons	a39e0969-24c8-4da2-8aed-83cdd0760fd3
\N	12	1	2026	Abdumannon	Fetched all wagons	c16fe82b-c068-4897-a25c-0474cf6021ac
\N	12	1	2026	Abdumannon	Fetched all wagons	f1283374-7adf-4abd-8885-728fbaf1ae5d
\N	12	1	2026	Abdumannon	Fetched all wagons	53d5cca5-20dd-4339-a83b-6289b595471b
Abdumannon	12	1	2026	\N	Database backup downloaded - tables: 16	9d23339e-8286-4316-9081-e3892adad965
Abdumannon	12	1	2026	\N	Database backup downloaded - tables: 16	268d8a1e-7e4e-4a90-bcdf-5484e718f365
\N	12	1	2026	Abdumannon	Fetched all wagons	45dc6109-2baf-4f10-91de-f35b8a95fed4
\N	12	1	2026	Abdumannon	Fetched all wagons	09abe625-b9d2-4706-be3c-58f087f66fc6
\N	12	1	2026	Abdumannon	Fetched all wagons	573bec24-0afd-4872-aa06-2e12a7289352
\N	12	1	2026	Abdumannon	Fetched all wagons	cc3e5656-01ef-40e2-9a87-0eecc7f0efdd
\N	12	1	2026	Abdumannon	Fetched all wagons	3c6666c3-7921-4719-9c32-7a35ebcaf45b
\N	12	1	2026	Abdumannon	Fetched all wagons	b40016d2-7087-49cc-ae00-fca0da7edc56
\N	12	1	2026	Abdumannon	Fetched all wagons	f60f5e46-405e-4ec9-bc03-d6edfae549a4
\N	12	1	2026	Abdumannon	Fetched all wagons	e4fec138-b6f0-44aa-a19c-24f9ec67c3a2
\N	12	1	2026	Abdumannon	Fetched all wagons	03422e35-edea-40c2-878f-c9af7c540771
\N	12	1	2026	Abdumannon	Fetched all wagons	fbeae8e3-98af-4d46-9b32-260f00fac6ab
\N	12	1	2026	Abdumannon	Fetched all wagons	8a1d37cb-e985-4cd4-a769-786e8b050c84
\N	12	1	2026	Abdumannon	Fetched all wagons	52a76a7b-9aa6-42d2-9b9c-d0eff4495a0c
\N	12	1	2026	Abdumannon	Fetched all wagons	2abaa63d-348a-4713-957a-6fcf55dd7648
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	6c8e0dcb-3ba9-49da-a529-b93e9f7ff525
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	bbae5f7a-d61e-45df-8e69-dd54620cae35
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	e103f740-a97e-46ac-8df8-c8932e32f013
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	928fb05f-33a9-433a-ac2b-318c5366b415
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	23446082-7a1d-4b2f-8ea7-6c1df09606b4
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	d5b20ec2-2002-4d82-9f22-d45f666245dc
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	75f46333-2cc1-40b0-a018-71c5dcc857aa
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	90ec3d5d-aabb-4e63-9dfc-6a472dc4a7fe
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	a5152e4e-8d30-47e3-bb25-2a320ab88cc6
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	236bd7b1-e305-43b2-a850-91d1e9c60070
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	7b4f3688-0646-44e6-9575-72275edb7044
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	85f47277-e429-4208-844f-71c8ed4ee9ec
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	904e337e-e37a-4263-a1b4-3f06ddf72bc8
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	4cc38202-f71d-44b6-afc5-a4036535538e
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	2171b5db-d923-4f19-bfce-f640597633f8
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	0e15c5fd-9989-4227-9340-0154054b3128
\N	12	1	2026	Abdumannon	Fetched all wagons	aeed8827-c328-4586-a24a-1aac96787187
\N	12	1	2026	Abdumannon	Fetched all wagons	cfd79211-eb01-4532-833c-5b37fd4e4c71
\N	12	1	2026	Abdumannon	Fetched all wagons	a08049cb-f11a-4215-8707-495fd288f566
\N	12	1	2026	Abdumannon	Fetched all wagons	b99c65e3-d36a-4372-bb5c-2709d9326ab0
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	ba4676fe-87c6-4f3c-b968-2155b79206be
\N	12	1	2026	Abdumannon	Fetched all wagons	4918c9f1-c72d-4b8e-9870-ec202094cfb9
\N	12	1	2026	Abdumannon	Fetched all wagons	e5bd99eb-db96-4be3-bca9-adc014b55262
\N	12	1	2026	Abdumannon	Fetched all wagons	5e313e2a-4f2a-4cd2-9e06-2b394b35b474
\N	12	1	2026	Abdumannon	Fetched all wagons	4e972c2f-36b1-4d83-810a-a237909fb417
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	b8b76805-fa97-4b38-a3e8-fe19d42dfd91
\N	12	1	2026	Abdumannon	Fetched all wagons	be9dfb7a-5e21-48f2-ade1-51d41cac4db1
\N	12	1	2026	Abdumannon	Fetched all wagons	70cf6e84-db58-498e-9006-9a50ee411c08
\N	12	1	2026	Abdumannon	Fetched all wagons	099e4e80-dbb3-4170-b167-4bbcd1d8b4ae
\N	12	1	2026	Abdumannon	Fetched all wagons	fa4fb89e-4584-4e87-ab1f-b46210e3233d
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	d7a80ec3-9890-4dbf-a51a-c59dc8c576b8
\N	12	1	2026	Abdumannon	Fetched all wagons	f6468a97-ea06-409e-9d8c-50f32a8aeda0
\N	12	1	2026	Abdumannon	Update wagon failed - no fields to update	78bce725-929c-4b4c-82fe-6f1b98e99240
\N	12	1	2026	Abdumannon	Fetched all wagons	dbb4a44f-62b7-4619-8f23-6ab7904c5756
\N	12	1	2026	Abdumannon	Fetched all wagons	b5adc9e8-0373-4080-841a-41a913525907
\N	12	1	2026	Abdumannon	Fetched all wagons	550ae1a5-7a64-4f06-8ee2-7abc1431aad4
\N	12	1	2026	Abdumannon	Fetched all wagons	225ac8f9-4f5d-4c4b-9307-6b91f10a9a29
\N	12	1	2026	Abdumannon	Fetched all wagons	88434c3b-8967-49d5-b0e5-040313077c07
\N	12	1	2026	Abdumannon	Fetched all wagons	0196fb6c-5cee-4166-942c-6241e2d9c94c
\N	12	1	2026	Abdumannon	Fetched all wagons	a1922962-0dfa-4700-a534-a880158daafb
\N	12	1	2026	Abdumannon	Fetched all wagons	83f2e212-006d-4941-bbea-c8f6d9dabf5c
\N	12	1	2026	Abdumannon	Fetched all wagons	1baff50a-acea-4d39-b16e-153b6e7f1bd9
\N	12	1	2026	Abdumannon	Fetched all wagons	9189ee69-2b6d-42bf-bb7e-9cc85caaa0ae
\N	12	1	2026	Abdumannon	Fetched all wagons	fc012f12-c9b5-4657-921a-c9d1739cc1a8
\N	12	1	2026	Abdumannon	Fetched all wagons	3a19addc-d0c9-43e7-9fc9-5ab3808096bc
\N	12	1	2026	Abdumannon	Fetched all wagons	b48a4ab0-d809-4b4e-8ee3-6f5b9b6f3bb3
\N	12	1	2026	Abdumannon	Fetched all wagons	de8df23e-1b0d-4cb9-b070-2fcbe9267c9d
\N	12	1	2026	Abdumannon	Fetched all wagons	59335cad-8c17-490e-90e6-ad01ad7a85d3
\N	12	1	2026	Abdumannon	Fetched all wagons	284c6f0c-57d1-48b1-9d19-997c61589070
\N	12	1	2026	Abdumannon	Fetched all wagons	5fb335c5-785a-4bd9-961a-d926ae881af2
\N	12	1	2026	Abdumannon	Fetched all wagons	f3d77914-96ce-4b14-8dca-b2c4a2fe1144
\N	12	1	2026	Abdumannon	Fetched all wagons	fb1d41c5-2749-4fb0-950b-85deb8b29d65
\N	12	1	2026	Abdumannon	Fetched all wagons	cecbbc91-dddb-48b1-8542-d7a81a4ef982
\N	12	1	2026	Abdumannon	Fetched all wagons	55f8cc83-e996-4d25-a958-73786cea55f4
\N	12	1	2026	Abdumannon	Fetched all wagons	03019e93-5e84-440a-ab5c-cb8bd9924ae7
\N	12	1	2026	Abdumannon	Fetched all wagons	863cea79-6ca3-47de-a075-79f0633b7cb5
\N	12	1	2026	Abdumannon	Fetched all wagons	f61d86e0-416b-4ddd-9adf-8ce3d7886ac7
\N	12	1	2026	Abdumannon	Fetched all wagons	16a677b6-bc14-4ffb-ad26-c7a5923f21de
\N	12	1	2026	Abdumannon	Fetched all wagons	70c081d3-7f4f-46be-9f43-16bb09728bd5
\N	12	1	2026	Abdumannon	Fetched all wagons	833d2cc2-b1d7-4052-bea7-3f660480c2f1
\N	12	1	2026	Abdumannon	Fetched all wagons	2118ef0f-d4c0-434a-a716-0e4e1dd4a533
\N	12	1	2026	Abdumannon	Fetched all wagons	022178cc-d7c8-4cb9-846b-68fd3bcef9af
\N	12	1	2026	Abdumannon	Fetched all wagons	0a0c1a00-b53d-43a1-bd18-d2e846d4a40a
\N	12	1	2026	Abdumannon	Fetched all wagons	f9d783f1-e147-4bf8-a34c-bff4262a1c71
\N	12	1	2026	Abdumannon	Fetched all wagons	1071a262-cb0f-4335-807e-ebc526d465ea
\N	12	1	2026	Abdumannon	Fetched all wagons	325b8efc-aadf-4d4a-9893-35a326e9ac88
\N	12	1	2026	Abdumannon	Fetched all wagons	023e7ee6-db97-460c-875f-a89663c1f18b
\N	12	1	2026	Abdumannon	Fetched all wagons	c03a0cff-f560-4601-8458-a5ac3df15f9d
\N	12	1	2026	Abdumannon	Fetched all wagons	39c72a3a-671b-4ae8-9cc3-1a52f4d06b56
\N	12	1	2026	Abdumannon	Fetched all wagons	34f3d7f9-2f1b-4b25-98c1-f8ea9c6f1b57
\N	12	1	2026	Abdumannon	Fetched all wagons	d39b0998-f7cb-427e-9b91-5a32f976b0c1
Abdumannon	12	1	2026	0000	Fetched all debts - count: 0	e46ae825-cf0b-4ef8-9aca-442b5e7a58ac
Abdumannon	12	1	2026	0000	Fetched all debts - count: 0	316d3ce1-7de3-4165-b9b3-b9a6eddd586a
Abdumannon	12	1	2026	0000	Fetched debt statistics	619da742-6a2c-44ba-b1b3-7dd89846d6d6
Abdumannon	12	1	2026	0000	Fetched debt statistics	da81995f-201a-43fa-badd-0cc71f0c23c7
\N	12	1	2026	Abdumannon	Fetched all wagons	7f7e5cd6-28c1-49c4-b588-501dc13a4d17
\N	12	1	2026	Abdumannon	Fetched all wagons	73f6c1f1-f475-4014-b7ea-b00a61dc4f77
\N	12	1	2026	Abdumannon	Fetched all wagons	8ad225b7-af7d-41f0-a973-b3524aa327f7
\N	12	1	2026	Abdumannon	Fetched all wagons	f399efab-b198-45b0-9632-b0714759a9de
\N	12	1	2026	Abdumannon	Fetched all wagons	a59d418f-fcd9-40f3-8bb8-1eac1b5ca576
Abdumannon	17	1	2026	0000	Fetched all debts - count: 0	482b9c47-e13c-47bb-bd75-7ab58ff8193a
Abdumannon	17	1	2026	0000	Fetched debt statistics	2b05b369-35e2-4c1f-b5f3-bf2cdc261edd
Abdumannon	17	1	2026	0000	Fetched all debts - count: 0	2cb1bd06-a665-450e-9b7d-38f1ccc5c7ee
Abdumannon	17	1	2026	0000	Fetched debt statistics	0aafd6e3-91db-42da-ae00-09ce25d4c3ed
Abdumannon	17	1	2026	0000	Debt created successfully - customer: Sobir, amount: 225	08eb6fab-7527-4df2-a5a5-b7d91178b369
Abdumannon	17	1	2026	0000	Fetched debt statistics	d88b1b4c-f0e3-4bc2-8b4e-e66ca2d80c70
Abdumannon	17	1	2026	\N	Update debt failed - error: malformed array literal: "sdfsdsd*15*15*14|sdfsd*1152*27*1500"	6d283c4e-3642-4ba6-8aad-f3865001e3b2
Abdumannon	17	1	2026	\N	Debt updated successfully: c0451be7-01ce-48c9-8fb3-f66bf51fb968	fb765ecc-53f2-46e1-a75e-327bcd74ca20
Abdumannon	17	1	2026	0000	Fetched debt statistics	46a7082c-e47d-4b80-aab2-66270eac96b0
Abdumannon	17	1	2026	\N	Fetched debt by ID: c0451be7-01ce-48c9-8fb3-f66bf51fb968	f64a340c-4f76-47a6-83c5-24c676b09e0b
Abdumannon	17	1	2026	\N	Debt updated successfully: c0451be7-01ce-48c9-8fb3-f66bf51fb968	bc3fa2d7-2949-4d6b-b801-257b503b55e3
Abdumannon	17	1	2026	0000	Fetched debt statistics	61477f47-e7ca-448e-b7de-64e5c9cd8b54
Abdumannon	18	1	2026	0000	Super logged in	329555f8-d782-46da-b20f-3ed09e3f4945
Abdumannon	18	1	2026	\N	Fetched main finance statistics	039800d3-64d6-4959-9f54-46166564821c
Abdumannon	18	1	2026	\N	Fetched main finance statistics	35a3809c-19c7-40b9-9257-d6d8b0b1f547
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	298fa01a-1b70-4c2b-88fe-3a11d894393c
Abdumannon	18	1	2026	\N	Fetched high stock products	7d1da791-7bc4-46bc-b501-5b71d7b9501f
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	657418bd-b82c-4f1e-958a-9b4536a2dcbc
Abdumannon	18	1	2026	\N	Fetched low stock products	0e6634a6-b7e7-4358-9ea0-997bd59af852
Abdumannon	18	1	2026	\N	Fetched high stock products	e209ce39-5ab4-48d9-aa4e-04132d75391c
Abdumannon	18	1	2026	\N	Fetched low stock products	d1d97b1e-7243-418a-a935-09505a22e382
Abdumannon	18	1	2026	0000	Fetched products for shop	e41e9637-6fe6-4119-bcbc-003b163f4999
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	2f6ccffe-0d3b-4f8d-8e2d-5882dbb87546
Abdumannon	18	1	2026	0000	Fetched products for shop	bb76e08f-a781-4321-a7a9-46b5d50a445c
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	852f19b5-0f31-44b6-9483-a5aaba703c3a
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	86ed5e92-6761-432f-b83c-d7275c30391a
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	3dbce7d3-e446-46e9-8a7f-405ad8c6055b
Abdumannon	18	1	2026	0000	Fetched products for shop	10be66e1-1901-4208-844c-00edca689047
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	beeba131-a578-4aeb-a5f8-1459a26ce90b
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	48c4a00b-fa66-483e-add5-1300be20180d
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	bd990aa7-2850-45ae-8aa0-a585a389daa3
Abdumannon	18	1	2026	0000	Fetched products for shop	722a3fc8-6392-48fb-8107-84c83d051204
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	113ab1b2-7de6-45db-bdc4-86646a506e95
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	30dc4e8f-01ce-4140-9762-0944b460b5b1
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	1364870c-681a-4119-8045-ecbef00b3a13
Abdumannon	18	1	2026	\N	Fetched shop reports as superuser - count: 62	e2e93f0c-7bb9-4ffe-aa40-9140832cbff6
Abdumannon	18	1	2026	\N	Fetched shop reports as superuser - count: 62	a79f547e-8cf8-4752-8892-0f71916dc165
Abdumannon	18	1	2026	0000	All admins fetched	f54012e6-e99d-4cde-bb24-619b3b640fb4
Abdumannon	18	1	2026	\N	Fetched all permissions - count: 5	953321c5-c542-4202-bed4-4153c3fc3f93
Abdumannon	18	1	2026	0000	All admins fetched	7d48af97-78c3-4c84-ae83-2da46cf7712b
Abdumannon	18	1	2026	\N	Fetched all permissions - count: 5	7ceb60e4-4fce-4e14-a5e4-36c132a45b7b
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	a2d3433f-a4ab-446a-90b0-a1d311396878
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	760ff2d9-5d8c-447b-b133-0cc8967f5808
Abdumannon	18	1	2026	0000	All admins fetched	72d4a152-dc9d-4013-9cf4-2c61cfc30114
Abdumannon	18	1	2026	\N	Fetched all permissions - count: 5	07656c6f-8eaa-4569-ae7b-aad7849e4221
Abdumannon	18	1	2026	0000	All admins fetched	089db943-820a-4860-8fb4-028615f5bd18
Abdumannon	18	1	2026	\N	Fetched all permissions - count: 5	c46ef5ca-3093-481a-a005-c0fe69d44b68
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	137ea4db-800b-42d4-a849-fc22e39f7909
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	e62adda9-6c81-485b-8948-636461c2e9af
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	ae421511-6827-4916-83f0-288de3520380
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	e634b1bd-c861-48a6-8278-0e51e3922d41
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	5b5d2109-2b0a-4e53-8e74-fcd3c93f7bd9
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	ac52a9a0-702d-4c04-9b67-b4c7fc579593
Abdumannon	18	1	2026	\N	Fetched branch by ID: Yetti uylik branch	e4e80090-f4da-4d60-a47c-97585caafd08
Abdumannon	18	1	2026	0000	Fetched all debts - count: 1	23ec856b-3d3a-4546-bcdb-f6ddc40b3478
Abdumannon	18	1	2026	0000	Fetched debt statistics	2d02b754-79ca-454b-a085-79baf9869691
Abdumannon	18	1	2026	0000	Fetched all debts - count: 1	2ebd09bd-3c5a-4d18-9d56-234f9dfac587
Abdumannon	18	1	2026	0000	Fetched debt statistics	cb7bf2f9-0391-4ce6-b1c7-1db335a6ae5a
Abdumannon	18	1	2026	0000	Fetched products for shop	78312289-3a50-4a43-99e6-4b74fee62056
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	aa2501b9-f0d1-4bfa-a9e9-3333d0bd22a4
Abdumannon	18	1	2026	0000	Fetched products for shop	22646e4c-1bf8-4394-ad87-2fcb74d2c2f3
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	043884b4-ef4f-4b8f-9c8e-a4280b127ad0
Abdumannon	18	1	2026	\N	Fetched main finance statistics	ab434f63-82f2-41d7-bf29-9c980ca0a5ac
Abdumannon	18	1	2026	\N	Fetched main finance statistics	02aad996-a8cc-4b94-be1a-4d820d4813df
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	f6984b46-4ed0-424a-bb8c-f5fa3e082e8a
Abdumannon	18	1	2026	\N	Fetched high stock products	9bbe3322-71a3-43f2-8191-31c7c4f011e6
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	5a0168c7-4c47-4aee-829e-70f02ca4453e
Abdumannon	18	1	2026	\N	Fetched low stock products	9ffd14d3-b8b9-4753-832b-ca712a724860
Abdumannon	18	1	2026	\N	Fetched high stock products	f2cc8d72-6448-4c6b-84e4-195e3492bc52
Abdumannon	18	1	2026	\N	Fetched low stock products	dfffee59-d5e6-420c-9b3e-2cc875c4099b
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	8756bf90-7e72-4390-a599-4a92d3822421
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	c3be50c9-b1a4-4bc8-8642-15e6748a6d55
Abdumannon	18	1	2026	0000	Fetched products for shop	38999b46-58a7-4618-a2ad-0dfd3546336a
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	a9073ae5-0fc1-4137-b1e6-ab7a89760a82
Abdumannon	18	1	2026	0000	Fetched products for shop	ac681ae2-ca39-4d76-829c-289d984c81fe
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	406c927a-79e2-417f-b614-a8796c296137
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	32e4c090-721d-416f-b634-289f59d16497
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	f08a5c3a-3b63-4186-8e5e-2f5c6b5a899c
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	0784092c-be62-4392-9c73-30b92476a285
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	593b919d-11a6-4c01-abf6-7821608324b4
Abdumannon	18	1	2026	0000	Fetched products for shop	aa461832-0554-45f8-89e0-5a706ce8b9e7
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	6e504a2e-88a8-44b4-9560-ee46514f66a3
Abdumannon	18	1	2026	0000	Fetched products for shop	5e645024-7b30-4a65-b904-9efaa9d27b78
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	f8225f8c-992d-4b52-b41b-8664aa4e9b12
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	515641e1-adc5-4350-be12-94eb04d50eda
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	71c9baa6-d605-4fa4-8070-9643752565ba
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	9871b82a-d659-4319-8446-444ba74583e8
Abdumannon	18	1	2026	0000	Fetched products for shop	fc4e613a-5ea3-4667-9962-a36b65df73bd
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	e5f89694-3f37-431f-bef2-5c95e3874a3e
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	ec2b6bfd-5887-4437-ae8d-eeca25b94f85
Abdumannon	18	1	2026	0000	Fetched products for shop	c02e54a3-6e55-4430-9ba8-5dcf98a6ff54
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	604c27bd-975d-4f46-ac3f-d96128d4f54d
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	09ffed21-b447-426d-9165-43b546147e64
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	c0abc93d-157c-45be-a213-ec2bc3c796d3
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	3fedc557-0aea-41b6-a132-2c91195cdf23
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	1e987a03-f6c6-4b97-ad5b-26ccd9c35ec9
Abdumannon	18	1	2026	0000	Fetched products for shop	929478ba-cff3-4687-9e21-e88b5c4fa71f
Abdumannon	18	1	2026	0000	Fetched products for shop	0750b174-e23c-428c-9248-34d597311478
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	98e2fd08-bbe2-4c12-b3c3-aa793e20a769
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	9ef0046b-b58d-4182-93c8-da202bc532ef
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	2f3a425c-e30e-4856-b9d6-0d5ee831a853
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	a53f48e2-95bd-4056-8f81-c1dcd40413b4
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	1c919a9c-8e0b-4e4e-b4ab-47145df4c023
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	13fe16c8-247d-4d6e-8823-18979755f340
Abdumannon	18	1	2026	\N	Fetched main finance statistics	31a85ade-2787-4d82-82e9-21103f885888
Abdumannon	18	1	2026	\N	Fetched main finance statistics	7d461465-efbf-4274-9c61-57d71e9dbae0
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	83c5ddb0-01d4-4c3c-9261-b2b3683e6b4a
Abdumannon	18	1	2026	\N	Fetched high stock products	61b85d56-f588-40d3-8f55-c29509bff6c7
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	c3fc1acc-5155-4543-8172-3892b891c74d
Abdumannon	18	1	2026	\N	Fetched low stock products	5c722055-a486-41c6-9869-89570db28673
Abdumannon	18	1	2026	\N	Fetched high stock products	51db93fe-8f2a-433e-9789-bbf0daa1e4ef
Abdumannon	18	1	2026	\N	Fetched low stock products	fbc2c4c5-ff0c-4e6c-89d5-ebad1463a537
Abdumannon	18	1	2026	0000	Fetched products for shop	e9f67059-d95a-42bf-a123-9456f42a60a7
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	956e1234-a57f-4367-8e5b-ee8e52749c88
Abdumannon	18	1	2026	0000	Fetched products for shop	e8b63c72-d264-4761-ae44-3e5324d149a8
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	870f21df-652f-48eb-91e7-d085ebc33959
Abdumannon	18	1	2026	0000	Fetched products for shop	c8fb8775-02b8-42ce-92b0-129f1904d671
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	c9f6f06c-4907-4847-9078-27ffbc5617a0
Abdumannon	18	1	2026	0000	Fetched products for shop	048f9ed4-e9fa-45f0-b414-4055a756162f
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	cfe507de-520b-4468-994e-9272f0243563
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	4dd2c2c0-c6b0-4ce9-964c-ec54fdba4a28
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	e95bcaa2-0912-48f9-9700-28d4fa577d25
Abdumannon	18	1	2026	0000	Fetched products for shop	67698788-d456-4484-a284-c9cc3930aa3e
Abdumannon	18	1	2026	0000	Fetched products for shop	fe7e1a10-faf1-401b-ace7-235fe86dc72a
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	c5de46c3-b177-48e0-bbb3-c77607a4215e
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	6ef09b5c-b227-4a1d-82f8-a443465caaa4
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	bb0bb461-8e09-4542-ad1a-8fec68cbf16b
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	1ba1f5e2-56db-4259-a4ad-89f86aed7898
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	eb9f10ac-ae91-49c6-bed8-59e175e433ca
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	e198ee28-2c2f-4e7f-a423-89e5f52499ef
Abdumannon	18	1	2026	\N	Fetched shop reports as superuser - count: 62	fb69e075-3433-4c85-a758-0bbc6fa8b642
Abdumannon	18	1	2026	\N	Fetched shop reports as superuser - count: 62	0ca8d0e2-c4e7-41ec-8706-721cb48d1643
Abdumannon	18	1	2026	0000	All admins fetched	5879b21d-b63a-482c-bd9e-2aaa230f3d43
Abdumannon	18	1	2026	\N	Fetched all permissions - count: 5	8bf62b87-b292-41b8-8961-453fe7ff8555
Abdumannon	18	1	2026	0000	All admins fetched	0cb5965f-20a0-40d8-af93-212e0e4d5201
Abdumannon	18	1	2026	\N	Fetched all permissions - count: 5	b66cdf4d-b56b-45ce-ab7c-1d0ce7fe5560
550e8400-e29b-41d4-a716-446655440000	18	1	2026	\N	Superuser logged out	b324c105-4e9b-41f9-bd97-79efaa72aea9
Abduqodirov	18	1	2026	\N	Admin Logged In	5c6c469e-93a8-4682-8fcd-f38fd8150b05
Abduqodirov	18	1	2026	0000	No products found for shop	9f5e3150-8841-4eb1-be37-ead714ef3843
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	386ac893-c37a-4814-86a2-9a39aaa1075e
Abduqodirov	18	1	2026	0000	No products found for shop	10fd4bb3-9fed-40f6-9770-38527af7c558
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	9874fd1b-c189-4495-a51f-1863fc9f99fa
Abduqodirov	18	1	2026	0000	No products found for shop	0283fb30-aeb0-4c6b-b271-5ba8b5c86158
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	2f629322-bd5c-4d6f-891d-29d06edbae11
Abduqodirov	18	1	2026	0000	No products found for shop	de63af04-dc0e-4e20-a8fc-f26e5fe27d9b
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	744f2ba7-7370-4024-979c-001a0bfe75e4
Abduqodirov	18	1	2026	0000	No products found for shop	1dddeca3-027f-4bed-9e09-262a633d04d4
Abduqodirov	18	1	2026	\N	Fetched all categories - count: 1	20792645-d44c-4b75-9a84-e71f92c5f1d5
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	48b37c15-9c98-47e3-93ee-415bd5d0ff37
Abduqodirov	18	1	2026	0000	Fetched shop branches - count: 2	c4027295-6fc9-4bd2-bf01-1bdb9b018b30
Abduqodirov	18	1	2026	0000	No products found for shop	e1b893da-e26d-4668-b2e5-0ddcad5c7076
Abduqodirov	18	1	2026	\N	Fetched all categories - count: 1	584ed60f-04c7-4727-9efe-7c0de4073cd9
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	b8f5185f-1b4e-4a75-b42a-62e78949f67f
Abduqodirov	18	1	2026	0000	Fetched shop branches - count: 2	df09e56b-4fe6-4439-8ce2-893cec962b62
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	40cc4e30-15fd-4f85-9059-f3f7b9d179df
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	e9382071-be55-4f4c-83f5-2e333774576e
550e8400-e29b-41d4-a716-446655440000	18	1	2026	\N	Admin logged out	00361ce7-ec8e-4460-992a-4f4659c476b5
Abdumannon	18	1	2026	0000	Super logged in	c06338a8-137c-4bb2-9943-12be9384c6e4
Abdumannon	18	1	2026	\N	Fetched main finance statistics	fc4e5345-103a-4dca-a42b-b22b54a991e9
Abdumannon	18	1	2026	\N	Fetched main finance statistics	e90a4d9e-d8a2-4d2d-a215-f786f0d13ebf
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	c6bcd7c2-596a-4882-8004-7ead93bb1e21
Abdumannon	18	1	2026	\N	Fetched high stock products	e33d9eb5-70a4-49d5-8a87-5d4bbb96f46a
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	a197addf-fc7c-46ea-aef5-a062680935ab
Abdumannon	18	1	2026	\N	Fetched low stock products	34d4973a-9ddd-421d-9392-1e6d8c32f95b
Abdumannon	18	1	2026	\N	Fetched high stock products	166b6914-49c0-41f3-ad2f-4e056d077164
Abdumannon	18	1	2026	\N	Fetched low stock products	f74e8ba9-6b00-4423-b5f9-2cb45bdc6bff
Abdumannon	18	1	2026	0000	All admins fetched	a021702c-e50c-4589-b1e1-2c9f5237e510
Abdumannon	18	1	2026	\N	Fetched all permissions - count: 5	bcad632e-c686-4dda-a2f6-99d3d616b49e
Abdumannon	18	1	2026	0000	All admins fetched	297d5511-1d09-4408-ae2b-c622243c49a9
Abdumannon	18	1	2026	\N	Fetched all permissions - count: 5	10357c53-91c3-40ae-b7f6-65cfaca93bf9
Abdumannon	18	1	2026	0000	Admin updated	70bdf22d-6d83-46b1-a85b-30ab4c0d0ef0
Abdumannon	18	1	2026	0000	Admin updated	b6fd40c9-f4d0-4d6f-a0c6-5105d5c3a923
Abdumannon	18	1	2026	0000	Admin updated	8b951df3-a7e0-4bb3-b83f-765763a940c1
Abdumannon	18	1	2026	0000	Admin updated	f9f2860c-e05b-468f-9d2c-ef8706e4112f
550e8400-e29b-41d4-a716-446655440000	18	1	2026	\N	Superuser logged out	8b5ebf7c-6db4-4364-8e8d-e4835063742c
Abduqodirov	18	1	2026	\N	Admin Logged In	a68915a4-7996-4da1-b7d5-c08ad87bc9a1
Abduqodirov	18	1	2026	0000	No products found for shop	f990e6f9-81ad-4e44-a0a8-98b0bcaea65b
Abduqodirov	18	1	2026	\N	Fetched all categories - count: 1	b36417b5-eaad-43f6-84a4-6620e7995741
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	a2a4def4-daf9-40c9-8807-5f06f57b1f3a
Abduqodirov	18	1	2026	0000	Fetched shop branches - count: 2	5cb4df0d-4137-4881-866b-78331378f81f
Abduqodirov	18	1	2026	0000	No products found for shop	f0a09b2b-5f3d-4dcb-8d98-457baf66e608
Abduqodirov	18	1	2026	\N	Fetched all categories - count: 1	e9bdcbb3-a1d1-4c05-bbdd-c75a486a3cff
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	5a33874c-7846-4354-a658-bd9202b9a3a1
Abduqodirov	18	1	2026	0000	Fetched shop branches - count: 2	b9be9cd9-f28b-46d8-8758-b746c305a9aa
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	e0893833-bab7-41b4-8f58-64c9bfc633c3
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	36dfdb2e-9321-4aea-adbe-6c80af847ea9
Abduqodirov	18	1	2026	0000	No products found for shop	899ffd93-23c3-4898-8e19-b6e80ed65724
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	87abb041-5c2a-41c4-a0b4-9595fb31046e
Abduqodirov	18	1	2026	0000	No products found for shop	6a5bd639-54c3-4dc6-a02d-d23ee7ff4f5b
Abduqodirov	18	1	2026	\N	Fetched all categories - count: 1	f73aad23-5013-4b18-a276-cd26f3b2edd3
Abduqodirov	18	1	2026	\N	Fetched all categories - count: 1	1e83070f-9107-409c-9b97-24479440b036
Abduqodirov	18	1	2026	\N	Fetched all brands - count: 3	85cfe66f-e1ca-4b5e-9650-07ee85e489db
Abduqodirov	18	1	2026	0000	Fetched shop branches - count: 2	23661646-c42d-4a0d-b755-68506f7552a2
Abduqodirov	18	1	2026	0000	Fetched shop branches - count: 2	a8536795-5bb6-4340-872e-392650ac5959
Abduqodirov	18	1	2026	0000	Fetched all debts - count: 1	8bb9ddeb-08ef-4c99-826f-52bd21b84e7e
Abduqodirov	18	1	2026	0000	Fetched debt statistics	352c16de-ccf3-43f3-8c76-53b20731a13d
Abduqodirov	18	1	2026	0000	Fetched all debts - count: 1	2e3805a2-2d02-4d16-afb2-52889221d7fa
Abduqodirov	18	1	2026	0000	Fetched debt statistics	d0d84837-799e-4649-ac42-1aa3c5f77c58
550e8400-e29b-41d4-a716-446655440000	18	1	2026	\N	Admin logged out	93cc9fa7-e5ad-45b6-954e-941178fcdd8a
Abdumannon	18	1	2026	0000	Super logged in	f92cb87c-49f8-4e14-81c1-6ea9f3b80e9d
Abdumannon	18	1	2026	\N	Fetched main finance statistics	208271a1-64a6-4ec7-8a88-86b31575b320
Abdumannon	18	1	2026	\N	Fetched main finance statistics	63f36ab7-9d24-4ae8-bbec-02fcf73c7c1c
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	7585270e-32e7-4505-9cd1-935a8a847285
Abdumannon	18	1	2026	\N	Fetched high stock products	ed6acef4-aac2-4ab3-ba81-d9c39ccb70d7
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	20b91676-0293-40d4-92b3-68b06ad8850c
Abdumannon	18	1	2026	\N	Fetched high stock products	79c85c50-eeb2-4125-803b-4339ffc8ea17
Abdumannon	18	1	2026	\N	Fetched low stock products	bee0b284-72b2-4440-90c2-4ef015f6eb83
Abdumannon	18	1	2026	\N	Fetched low stock products	de2b6901-23ea-4dc5-aad3-c09156e2fef7
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	1db5b9a3-e86a-47e7-90a9-a70ee8ee9d3f
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	1f5bbfb0-777c-458d-810c-f40903521d70
Abdumannon	18	1	2026	\N	Fetched main finance statistics	2473e66e-6e65-45c6-8e0b-8449b4401a05
Abdumannon	18	1	2026	\N	Fetched main finance statistics	0326313a-fe20-4e22-8a67-58d774d225d1
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	73d358ec-f65b-4944-835a-447159ba9d6f
Abdumannon	18	1	2026	\N	Fetched high stock products	ad43f416-5922-42e7-8ea4-7072b9ffd245
Abdumannon	18	1	2026	\N	Fetched week statistics (last 7 days)	0633a094-bbcf-4e32-93a1-d0551f9ecff4
Abdumannon	18	1	2026	\N	Fetched high stock products	85ac6405-c5e2-40b0-9b0e-a60686e89c3d
Abdumannon	18	1	2026	\N	Fetched low stock products	2a73dfe9-4be0-4ed5-a893-0ff8bc477bc7
Abdumannon	18	1	2026	\N	Fetched low stock products	73048d3b-0832-4946-8d80-65b8f9b81c08
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	fddc34db-4404-479e-8d17-7c77f35f2293
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	dfe3ea88-a6c3-4699-a713-adbcc7724084
Abdumannon	18	1	2026	\N	Fetched branch by ID: New	d00e3c9a-a12b-4f3c-9521-8e179920c667
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	d36f5271-3b13-45e1-a316-368079af39bc
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	03d6206e-c1b3-4c01-adc5-4906701a3ed7
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	2feb1271-593c-47e0-a124-7b23c4025c8e
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	c8d17dcf-37ea-4d52-89a4-d55e764bf2e6
Abdumannon	18	1	2026	0000	Fetched products for shop	d23f917a-86a5-4bac-b08a-a001cee754d6
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	4ef6ddc9-bb31-4082-a765-26051aa97c20
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	3e0ad9b2-d4b0-45fe-8bd2-4787d4c5af54
Abdumannon	18	1	2026	0000	Fetched products for shop	d5291243-8d01-44e6-866d-a05c9ba026ae
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	145d7238-09f5-4574-83cb-18e46b9f5641
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	5fb0a5d5-6ad0-4ae8-afa7-eab32c65d4cc
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	45e0543d-566e-4cc6-8bed-c329ee4f7510
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	d16c3d41-4240-457c-905b-6c704ecb03b6
Abdumannon	18	1	2026	0000	Fetched products for shop	3592d01c-5e66-4d71-b014-7b74b77e22d5
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	95c62e8c-1b4c-44ee-b8bb-d2c3f13c7cec
Abdumannon	18	1	2026	0000	Fetched products for shop	3f273f97-4392-4b35-a015-94afc5774823
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	cc5011c3-a25a-4e7a-aa61-7ea197f5ea7f
Abdumannon	18	1	2026	0000	Fetched products for shop	fefe159f-4b9d-4292-944c-92e405197655
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	16d8c983-1a34-43d7-aa55-65bac3c57091
Abdumannon	18	1	2026	0000	Fetched products for shop	89b18f2e-4789-4a1c-a3ee-d71e24b8c007
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	8f22323e-6fab-4ac6-b671-5f3a7fa3a58d
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	5109d573-3174-448f-a49a-81b0af88550a
Abdumannon	18	1	2026	Abdumannon	Fetched all sales	12a4190f-9155-4373-9113-bf374ade24bb
Abdumannon	18	1	2026	0000	Fetched products for shop	81dc02d7-1984-4c05-a6db-b0faf6971b58
Abdumannon	18	1	2026	0000	Fetched products for shop	8ae394ec-1811-4449-b949-10fe2cfb369c
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	6b7decc2-c88b-41c7-8b98-541ceb5b7d4e
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	700c1036-e995-4898-90cd-9c5437797866
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	1b1ce242-86ce-4035-9fe2-8d85352ac28c
Abdumannon	18	1	2026	\N	Fetched all categories - count: 1	b6595ffe-9e03-412e-9567-eaacf23e45b6
Abdumannon	18	1	2026	\N	Fetched all brands - count: 3	40500478-a29a-4ceb-996d-c2a10e682284
Abdumannon	18	1	2026	0000	Fetched shop branches - count: 2	587721a6-61e4-4962-9895-e8ff06cebb0d
Abdumannon	19	1	2026	0000	Fetched products for shop	98631179-8ed0-435f-aef7-8c87b82cd9c3
Abdumannon	19	1	2026	0000	Fetched products for shop	c79a8806-4a1d-4126-890e-f1ea11d2c069
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	0991a5b6-2575-4d25-b271-8e036977fd3f
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	41e69a14-8c96-4480-b8cc-80afa9cb742d
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	128356e9-71c6-4679-8982-584e67ec342a
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	c2fbdb3e-4b44-4437-bdeb-e404e1e3e9dc
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	8c6d631c-8cf9-4ab2-854e-a56086441f55
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	ac0381f8-bb94-42b6-8003-9ace95211d1b
\N	19	1	2026	0000	Product created: 5b610ffe-90b0-482d-88a3-f501fda2a790	ab899791-10ae-447c-98b1-09327c2d9f23
Abdumannon	19	1	2026	0000	Fetched products for shop	8a8f4938-4dba-411d-bfb0-08c812e9df72
Abdumannon	19	1	2026	0000	Fetched products for shop	edb46465-dd76-4f40-ac1d-136a1a5aaadf
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	5b6a8980-f0ea-4ee8-adf2-961c636009aa
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	1ecceb57-d4cc-4afa-b13f-ef4d69c95f48
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	1507fb3f-04e3-466c-857b-a7f20e6701e9
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	718edeef-146b-49bf-ba5b-4a8c3f80be29
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	6bbecd29-f9d5-40e0-ba6e-26e8cccd21ce
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	4ad31be7-0392-4307-93b3-cf9b0779deef
\N	19	1	2026	\N	Product deleted: 5b610ffe-90b0-482d-88a3-f501fda2a790	49901c35-ac60-4728-815a-533748dbf283
Abdumannon	19	1	2026	0000	Fetched products for shop	c77b64d3-5c83-404d-8a59-ce26d9729597
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	ef056586-ef92-478f-85e6-cd4e2ae05405
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	1ed9354d-734d-4bd4-bdaa-baedebe72e54
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	2598b278-c1c9-4b16-b0b6-941b1810a01b
Abdumannon	19	1	2026	0000	Fetched products for shop	c9511b6d-2a65-4c57-8aa0-8d6324d2109a
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	2ed2ac96-95d2-4813-a0f1-4309f6db545b
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	c1266090-47dc-4c0a-922f-f12fa63b4995
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	7c78482c-155e-426f-a51f-adf78d43026b
Abdumannon	19	1	2026	0000	Fetched products for shop	ece6e071-4731-482c-8cc5-74679707c3db
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	8999d430-69ba-49e9-afc3-5777191fa434
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	836696cc-cc7a-47fb-95e6-610e0d168dc8
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	62de23c0-5632-41c0-b3c3-19a8053d7f2b
Abdumannon	19	1	2026	0000	Fetched products for shop	315894cd-5b9e-4af7-9eea-4fb5dba9fb1b
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	1f0d9da6-127b-4360-b427-fb11f45805ea
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	4830a6cb-874e-4523-bed9-b386cd558f9f
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	6ac8c8e1-088b-4b9d-88c6-907e05cc5f8f
Abdumannon	19	1	2026	0000	Fetched products for shop	cd5d8060-5fe7-47df-812d-26391c9b5189
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	3ae56d6e-de40-4979-bc7a-399d1ab6aaa5
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	199fdc3d-b654-455c-96ec-f3f4da24a646
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	193b5755-5ab0-4cd8-accf-58dc52e22adb
Abdumannon	19	1	2026	0000	Fetched products for shop	f66211ed-d375-4c3f-bd46-6c16e56ef625
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	16873e9b-09f5-42a7-931f-fd209c16d091
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	bd16e250-2eb6-462b-9f61-a539ddfef214
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	dc23f336-0772-4a0b-a089-207b482f2895
Abdumannon	19	1	2026	0000	Fetched products for shop	ab9ec1b7-8c7a-4b49-89bc-cfa711bf3bee
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	afcf07d4-f98b-41aa-a688-37b43632caf4
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	c317d221-e8f5-476f-bdd7-62fb989b87d4
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	b359bd12-659e-4303-b2d3-62d2dbc12585
Abdumannon	19	1	2026	0000	Fetched products for shop	ab31e578-1fe5-4c62-afa5-cee05f77e937
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	4a9408ff-a812-4225-9199-9e33d62ebc52
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	055367f5-39b2-40ee-8d15-e9d8bff7999b
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	1ac62a60-4d56-48fd-93e0-e2730772ca9c
Abdumannon	19	1	2026	0000	Fetched products for shop	23b00be5-490d-46db-af24-a123f7f4bc51
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	ca5ea7de-a80c-4a4f-9114-3cc9c6423e1c
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	95f64dd8-beb0-4937-aa93-a773ef9d745f
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	bc4f41ee-3da0-4d90-8008-13b884d30813
Abdumannon	19	1	2026	0000	Fetched products for shop	fa3bbfc5-67ac-494a-a854-50b94954121e
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	ccf7e512-619c-453e-9642-1a7a8e01dc85
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	ee174fd9-0aac-405e-b187-3f038edd7883
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	76e512e8-baa1-4931-a854-f8bdd21be712
Abdumannon	19	1	2026	0000	Fetched products for shop	30bb9896-d15c-46f1-b3a5-36c8d6171532
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	02868aee-ca5d-43c9-8842-e9e31251f229
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	ea755c16-b772-4370-af8c-313ebd9a42f6
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	29c022dd-8b60-45a2-879c-616cdacf4748
Abdumannon	19	1	2026	0000	Fetched products for shop	44508f40-6bc3-44bc-bb3e-5390060c75c4
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	25b84b5f-d74d-46e3-959a-4f35874f41ce
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	5ff2d162-79b4-41c0-a361-0f2f6c72ef71
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	73e86507-eefe-419b-9c05-1262ed8e0316
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	ce3fb0fb-9828-4ebf-9029-e2f15e73ee7d
Abdumannon	19	1	2026	0000	Fetched products for shop	16f0392c-2f16-4bb6-b2a4-cf896e7cb2d0
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	98518a66-7199-4bb8-ab30-84e21e3a6d2c
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	b5d915da-e441-425c-8ac4-dbe7bdf1227e
Abdumannon	19	1	2026	0000	Fetched products for shop	041917f8-19fb-4e52-8481-fa70fea66db5
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	0e75a19e-8aa7-4773-9f79-898b14f281d7
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	44d2a27b-3c0d-4693-ac67-79c266f203ef
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	7d81385a-4689-475b-ba5a-18573c361dd3
Abdumannon	19	1	2026	0000	Fetched products for shop	3f7abad6-e97b-47fc-b508-b284d700ab29
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	f158d41f-2b38-4e49-beb0-e3f0019bfe93
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	83fbe7a3-db8c-4847-8402-f9deed816b5f
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	e8a386e3-1afa-4786-b9a8-14bd41b2a6e5
Abdumannon	19	1	2026	0000	Fetched products for shop	0528059f-c84d-485f-b058-1f6dc2b1b942
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	6c2d3d7e-8b28-4e23-98e8-c05634629bda
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	390c1a2b-975f-4e57-b09c-6e5b5f3a567a
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	70f8f9ec-e3b4-47d9-992a-6ac429920859
\N	19	1	2026	\N	Product updated: fd3fffe6-044c-4ee3-b815-471be7dd887f	ff1553dd-4d67-4c21-b907-6bb3e73c2860
Abdumannon	19	1	2026	0000	Fetched products for shop	9bff9d5b-257a-4372-b52c-871d2622a0f4
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	d26b7fe0-c9e9-4a7b-9805-1bfdaab22787
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	ea51e1c4-4045-4309-9d92-5bdbe3ad5aa9
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	53bb3337-94bb-4cc9-b765-de8b8903773b
Abdumannon	19	1	2026	0000	Fetched products for shop	ced4f6d7-eace-4954-8280-8b18930cc5aa
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	a975399e-c060-4f60-9752-2d6ee1e909ba
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	130ba046-66a3-4fd2-aa46-e8164779ed1f
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	f2ef9480-3b92-4587-a128-ddfdb015d14f
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	765549db-ff13-4682-9ca4-acd205b7974d
Abdumannon	19	1	2026	0000	Fetched products for shop	52cff92f-5fa3-49a0-9c03-6d29d770df2c
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	77c06d81-c765-4492-a413-697cdb44feed
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	b1dc5e09-10c8-4d95-b497-eeb6e3d9d22b
Abdumannon	19	1	2026	0000	Fetched products for shop	82d63072-ab29-4bd0-b289-51ba97a43c51
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	21f09c3b-2ce5-450b-aa3d-876841bbc657
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	11e57103-878a-482f-8bb5-9f06298dd03a
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	66d282eb-f381-40b4-af1a-bb9474b5ef24
Abdumannon	19	1	2026	0000	Fetched products for shop	be452ff9-055f-4a92-a0c7-bb5519b48ce9
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	6cac7c45-2ba8-403e-91ae-e7337d68bec4
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	82c8d500-c38a-42dc-8463-322876496731
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	0a3e4481-a09f-445d-95b5-eee0c7ee89a3
Abdumannon	19	1	2026	0000	Fetched products for shop	ee795ac4-5f4a-4bcb-a1b1-301755b953a2
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	a037becd-baa9-4bbb-9e1d-ecfe5e877b03
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	54ed77cf-a75c-4403-bd3f-ac894e82bbc0
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	d0c6c927-ae26-44f0-96bb-0489734c8d6e
Abdumannon	19	1	2026	0000	Fetched products for shop	1717b9f9-2649-4b41-9f4f-08d3966cd5d6
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	160f8046-56de-4999-acf2-bb7e628bd673
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	a41459c4-244a-469b-9107-f9adc82a41ae
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	01279ab5-e5bb-41b5-8354-64aa7bf205bb
Abdumannon	19	1	2026	0000	Fetched products for shop	62eae2cb-aa90-4bf4-b549-feee3f72c100
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	257d0e51-da29-48d3-91ad-35485382570a
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	39c1c174-b737-44f6-ab77-a0a832e83be5
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	047fd0ee-973e-4faf-91e4-acf824af43d7
Abdumannon	19	1	2026	0000	Fetched products for shop	464695a4-8215-4a5b-bf4d-9ef1e48c9758
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	53a278e8-1cf8-4085-bb82-9be6bb56647f
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	d5a7646b-9939-4f7c-92d8-fa350e82751b
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	90e10a8a-bc47-45a8-bf17-d8372ed598ae
Abdumannon	19	1	2026	0000	Fetched products for shop	568a9e1c-4a55-47be-9409-52a093199409
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	cccd9fcb-9da9-4f2a-87cc-032b14a36978
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	cd03a7f3-5528-4e34-aadf-024dca9aa9c3
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	4fddc68d-d453-44f5-90cc-2e4cf8d95161
Abdumannon	19	1	2026	0000	Fetched products for shop	d06e9c2c-5f23-4868-84c2-bc428e278b29
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	c02e01c0-6da7-416d-abaa-dd6ea148fd9b
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	50b97a85-6224-483f-a2c5-720a8ea627f9
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	3cc4313e-3081-49c2-a088-490a94d3d692
Abdumannon	19	1	2026	0000	Fetched products for shop	eb15c005-920e-443f-8a9d-a00f92ebee23
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	e57943e3-14e7-49ee-8237-939a2b696b5c
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	dc7dfa7d-ae81-430e-8b37-112853720f14
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	1e1ebcea-9e83-4cd2-9d53-929cdbcd55b2
Abdumannon	19	1	2026	0000	Fetched products for shop	8c576be6-e1a8-49aa-a8a6-ebd0f08b1fc6
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	1eb1e948-777d-4554-8961-5667719d1ceb
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	b003d261-be0a-463f-a08d-6d3190523a2c
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	138ba2ed-d58e-4e94-9a36-e89b43bd7c8e
\N	19	1	2026	\N	Product updated: fd3fffe6-044c-4ee3-b815-471be7dd887f	4d55a3fa-bb00-49ab-8b4d-df93ffa8907d
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	c0b0437d-b884-46af-9571-a755762f9642
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	40145d2b-e55d-476d-9705-64f02e4154ed
Abdumannon	19	1	2026	0000	Fetched products for shop	82becf64-7d98-45bd-9b88-3c96bb35c47f
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	f7547993-b22b-43d4-b3eb-ebfde1d687fc
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	7cf99118-7b55-47bc-be1b-1c93167e4d08
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	ca065e59-2191-4fa9-8df7-75a15b15965e
Abdumannon	19	1	2026	0000	Fetched products for shop	a91164df-e9ee-4891-8a93-8d27f3a736c5
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	1d95ee22-ac9a-49a2-86e8-f49a21184c5f
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	07bc08d3-13f9-4d21-b4db-3781aeca1b1c
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	4e57866a-b197-40e9-8aff-02692b11de6a
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	055ac6ba-4258-4ae9-8bad-a7ad26d649c6
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	751e2ff8-63ca-46de-812d-e65d282f22bb
Abdumannon	19	1	2026	0000	All admins fetched	548972b4-7c3a-46b1-8d16-95412a07283a
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	eb873e0d-3fa6-4d84-95cc-eac04b25df8e
Abdumannon	19	1	2026	0000	All admins fetched	3fc69bec-8fe2-44ab-93ec-ae87801ec409
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	803a8420-a393-454e-87b2-cb4171467369
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	fdb965e3-87ff-4c46-97ee-83d7349f0679
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	37cf1b82-f514-4c1e-8cba-37e0e44faac6
Abdumannon	19	1	2026	0000	Fetched products for shop	75759476-429a-453e-b827-ce0f9dab65d1
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	f48c2aaa-baa5-4722-b108-10d5a7812871
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	ce59fbb1-4ddb-4356-b7f8-427a86b513c1
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	b4f443f0-7712-44c7-b7a7-963ceccc2f89
Abdumannon	19	1	2026	0000	Fetched products for shop	ec78177f-3131-46d5-9083-72c02e83b106
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	c6add87d-da18-4e14-8df8-1a6d5854a3b3
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	66a2026c-f353-45d5-9773-b8bec831c07d
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	21e883d3-8350-4934-8360-8663b688d790
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	1f45c82a-9000-42ea-957f-011363c9184d
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	e170b578-c1a3-4330-9d2d-49ba05c566ca
Abdumannon	19	1	2026	0000	Fetched products for shop	9dc90ac9-ce37-48eb-94f6-ca53cf98cf48
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	bad6e08e-9d91-46bf-a6ac-ecb73b516def
Abdumannon	19	1	2026	0000	Fetched products for shop	3a1b85cf-ab50-4bc0-a749-3748234404cc
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	0b612aeb-1bde-4e34-b8da-95de1a089f7c
Abdumannon	19	1	2026	\N	Fetched main finance statistics	b390eb6e-a37c-4992-ae82-6e8e0472d97e
Abdumannon	19	1	2026	\N	Fetched main finance statistics	d111816e-6980-44ce-97e1-f8b26543922d
Abdumannon	19	1	2026	\N	Fetched week statistics (last 7 days)	925fa044-12bc-47ec-be37-965216fb74e1
Abdumannon	19	1	2026	\N	Fetched high stock products	123739c0-5a91-41a5-a6fb-5c851a7fc4f1
Abdumannon	19	1	2026	\N	Fetched week statistics (last 7 days)	92a48cd4-12b4-468a-b7b4-dd1768e4efef
Abdumannon	19	1	2026	\N	Fetched high stock products	e79224e0-c141-4c81-a970-d59eb5b177df
Abdumannon	19	1	2026	\N	Fetched low stock products	903d6419-9a34-4844-bab0-60d4e2f66161
Abdumannon	19	1	2026	\N	Fetched low stock products	899e6b2f-45f9-4cde-a0e1-d34a7958ee93
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	db5978b6-00c6-4bbc-b7c9-e2914b37ec36
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	89419bad-830f-4d9e-8e0b-2514b28ca4b1
Abdumannon	19	1	2026	0000	Fetched products for shop	1524ec29-0773-474b-9047-e4ee3346ce50
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	d17c2964-917b-4f93-9adc-78095e93aa0c
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	ebbb06c7-7cff-4785-9575-c6ff44fa9b53
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	9c0d0249-d69c-434f-9dc4-9ccede981558
Abdumannon	19	1	2026	0000	Fetched products for shop	7c23701c-da46-4d60-bb81-ab2bb58a283b
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	4700f9d9-197d-48af-94e0-af4f09962f3d
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	fdc37929-cd14-480f-a043-be0934f9fa04
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	19394db7-810a-402e-8290-f7f503a45023
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	6456dbd0-82b0-463a-ab9b-279ea2d8d94f
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	b3bd867e-7461-4cab-81e7-c3b846440f82
Abdumannon	19	1	2026	0000	All admins fetched	0104e731-dec4-4174-a15a-c6a7d4325312
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	17506d6b-3665-49fe-b498-cd14e1685d07
Abdumannon	19	1	2026	0000	All admins fetched	eac760f9-cd00-4368-937e-556d844c0503
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	fd303b50-a78a-4ecb-bda0-63066fc65ad1
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	535efae2-c568-468e-8ec3-2e5c1be3c8b4
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	130893be-1a64-4e6a-b5df-7642eedf50d2
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	57899ebf-24c0-4393-8d8c-037fe6363f0e
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	d7d2a252-3500-4f04-9a28-b56f7a30b612
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	90cdbd5f-d2f6-4a92-b548-19b0231b2097
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	c97c6b8d-6bb6-418b-ade9-49fca6e69bbb
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	3d68564a-5e67-4fc6-9951-c2c048e5c287
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	478b1f5a-20f1-4db2-946c-c1b2ab50562b
Abdumannon	19	1	2026	0000	All admins fetched	bf79f12f-4b5a-4c0b-a985-ddf7197212a2
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	d13d7c2d-0b93-43e5-a157-c7c59b1a5496
Abdumannon	19	1	2026	0000	All admins fetched	e242c484-365e-471e-a90d-3bcb595d8e0c
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	7e71f9d7-054d-4693-9fa2-3bb7d948a741
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	4bac2690-7851-4bd6-a5c9-889a20c34301
Abdumannon	19	1	2026	0000	Fetched products for shop	0f7033fb-29ca-42c5-a29d-c2f180a65c85
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	dad4829c-24cf-421d-b0c8-828d4c8a74db
Abdumannon	19	1	2026	0000	Fetched products for shop	37c8e4ba-b338-4df1-84a4-b148bf8329bc
Abdumannon	19	1	2026	\N	Fetched main finance statistics	7217cd3a-b969-46bc-b19c-ee6e9725f57e
Abdumannon	19	1	2026	\N	Fetched main finance statistics	2abec156-2af5-42fc-971f-152e155cecf4
Abdumannon	19	1	2026	\N	Fetched week statistics (last 7 days)	c6111d56-9ce9-45f2-8d60-d4c93d10a55f
Abdumannon	19	1	2026	\N	Fetched high stock products	4a878f00-8007-4ba9-9a5b-5512e59e7753
Abdumannon	19	1	2026	\N	Fetched week statistics (last 7 days)	f42649be-addb-43ae-9d48-89d0e9dd862d
Abdumannon	19	1	2026	\N	Fetched high stock products	8303c5c7-946d-4e9f-85ab-4d2fa4cd1bbb
Abdumannon	19	1	2026	\N	Fetched low stock products	ecc2c730-f2de-4dfb-94f5-1cb2c9a25ef6
Abdumannon	19	1	2026	\N	Fetched low stock products	2bfd9f51-6b96-4f93-9798-14bc2a67a7dd
Abdumannon	19	1	2026	0000	Fetched products for shop	e00b66cf-25d6-4731-a68a-7641dfb5fe04
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	1d876f00-ecd5-4f12-8fea-791fec204c4c
Abdumannon	19	1	2026	0000	Fetched products for shop	9064fb7a-c196-4a54-9827-69f003245caa
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	13c19210-e84a-4d72-82b8-f9b93fe039e2
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	e4960e83-fb8b-44b9-bfdf-94d3558ace97
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	742079b0-6be4-4696-9b50-8feff228fe44
Abdumannon	19	1	2026	0000	Fetched products for shop	35031b9f-bacc-4b25-a0a3-7399726dbdd9
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	6ad3f655-1161-4767-bb43-951d7a3d315a
Abdumannon	19	1	2026	0000	Fetched products for shop	9f3f745e-844c-44e6-9b5b-252146fa6e45
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	544a9216-2c58-4a17-9066-a897da3c6ab8
\N	19	1	2026	0000	Sale created successfully with sales_id: 4129506c-38e2-47ae-87d4-85f30cc50464	1e7df40a-4348-4342-9a56-057f3fd57814
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	eb3ffca5-d2c4-4ac3-b8dd-f3f22a549876
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	7e814bf4-b922-4c9a-abd1-8919e2fd2d7a
\N	19	1	2026	Abdumannon	Fetched sale by ID: 4129506c-38e2-47ae-87d4-85f30cc50464	e9114428-1b46-456b-89fd-3ee5d5f6bb2b
Abdumannon	19	1	2026	0000	Fetched products for shop	9180dfda-3c24-4f22-a221-09244ba14cb6
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	d39014d9-3e96-4de2-9b50-9f8399e7faa1
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	cf616c36-9005-4f6c-90d8-65375979bf96
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	5f7b60e8-050f-4169-8ccb-5be67fe083ea
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	ae46f425-e262-4898-bc4e-4a3c2a55cbd1
Abdumannon	19	1	2026	0000	Fetched products for shop	58d2ac10-dbce-4ac3-b794-1574ee3d4bcd
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	7809c0cb-d29e-4580-958d-39cf008f0d9c
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	8138e03d-3fd4-490d-a836-d9420cbaea27
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	30e273f1-a839-4d3f-a9e2-de2d31683356
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	ddd48f6f-1663-4666-8afb-3c4982e7b9b7
Abdumannon	19	1	2026	0000	All admins fetched	81288584-6775-4228-8a95-8e3a0df1f812
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	5e01bce7-55c1-4bbd-9f99-105a90a3694a
Abdumannon	19	1	2026	0000	All admins fetched	0cb711d5-2395-47da-8d61-1e5b7d6ea923
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	24dd3d7d-b067-4586-9903-ff693c33e8b3
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	528db5de-c033-46ab-aee9-14799205c6fc
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	ca5e9070-f2cf-427f-8e7a-4d5b130488b7
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	30085971-ca99-456e-953a-a89c837efe4c
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	79adc84d-fcaa-443c-961d-4c7f1ff1a6f6
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	dd7d951d-0337-4120-82e8-2984934ffb35
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	be77fcbb-3311-47ce-a7ad-9f1286698eb0
Abdumannon	19	1	2026	0000	Fetched all debts - count: 1	2bbdf71d-55e5-4d8e-a621-6a2c97c7881c
Abdumannon	19	1	2026	0000	Fetched debt statistics	bf2ab871-4cd5-4d80-804b-34a257314acb
Abdumannon	19	1	2026	0000	Fetched all debts - count: 1	9ec2bf14-c5a8-449d-a4fc-26eb94d3652e
Abdumannon	19	1	2026	0000	Fetched debt statistics	0686395f-b644-4b48-b326-c76ea1d4799e
Abdumannon	19	1	2026	\N	Database backup downloaded - tables: 17	cea25be3-d576-447a-9e35-d5a4080f6c42
Abdumannon	19	1	2026	\N	Database backup downloaded - tables: 17	4c93c275-6d4d-446d-978f-0c0470417646
Abdumannon	19	1	2026	0000	Fetched products for shop	272a3c71-197b-47df-85b0-18208ccf4e10
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	0ebadf77-22c9-49ca-b82a-443ddb7ac38b
Abdumannon	19	1	2026	0000	Fetched products for shop	28bafa67-05d9-4acb-a296-7d0a3d9830e7
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	31f78e32-d23f-453e-8d53-42426ece3441
Abdumannon	19	1	2026	\N	Database backup downloaded - tables: 17	e266a6f3-454f-45c1-b46c-fd338f5f21a2
Abdumannon	19	1	2026	\N	Database backup downloaded - tables: 17	ad70d1e9-5f5a-4062-95eb-2583a962aa75
Abdumannon	19	1	2026	0000	Fetched products for shop	711cc257-3543-473b-843c-7ed6abff53f0
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	5dcf21df-3ccd-4e51-8fd5-d7a3d20d2ec3
Abdumannon	19	1	2026	0000	Fetched products for shop	e1c0a68b-74cd-4adb-9465-3b24fdb1d4a7
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	f87870e6-eae2-4a74-a67d-cede2968d210
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	6a617616-2540-49ed-a6e8-7ac384caed54
Abdumannon	19	1	2026	Abdumannon	Fetched all sales	ec62a778-fda6-4d35-b193-7ee43dc17c6f
Abdumannon	19	1	2026	0000	Fetched products for shop	3455eb05-616a-4884-8cea-4c1b7b8495e8
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	1fb65699-5521-46b3-94d2-e320f936e250
Abdumannon	19	1	2026	0000	Fetched products for shop	f3bc9ae6-0dde-4cbd-ad5c-0261f119033c
Abdumannon	19	1	2026	\N	Fetched all brands - count: 3	d9a06ca3-5350-4fc0-8cd8-ad6608f2e8d8
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	90de04eb-5538-4fee-982a-4cb9be92b087
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	14fa79fc-74cc-4f4d-9728-8282ac7b3671
Abdumannon	19	1	2026	\N	Fetched all categories - count: 1	4673c4df-bb26-4c7f-a726-3e18fea860dc
Abdumannon	19	1	2026	0000	Fetched shop branches - count: 2	beb823d0-3e4c-48a1-a56e-5356c994dc77
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	c0fab0cb-9278-4013-8cf3-c81d09ea8857
Abdumannon	19	1	2026	\N	Fetched shop reports as superuser - count: 66	3a7f5b88-78b4-4a88-8e69-77b6e4c4af20
Abdumannon	19	1	2026	0000	All admins fetched	5f160658-2c25-47cb-98fc-75222b78c184
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	137e503d-a2fd-4b26-8889-2346c27fec54
Abdumannon	19	1	2026	0000	All admins fetched	c666608a-cba1-4271-a2d6-da9f34e7e0da
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	788fdfce-dd94-42f1-a44f-b77464fe88ed
Abdumannon	19	1	2026	0000	All admins fetched	af86b268-bdac-4df8-ab43-159dcc90a3c8
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	1ec56738-2e7f-4245-9764-7267a2fd5b27
Abdumannon	19	1	2026	0000	All admins fetched	3d87a79b-777c-43f9-8019-47116d308e31
Abdumannon	19	1	2026	\N	Fetched all permissions - count: 5	c135a054-63ee-4664-937e-357f042eba83
abdumannon	24	2	2026	\N	SuperUser Login FAILED - not found	eeee6f2d-9db6-4141-a06d-9852a16bb609
abdumannon	24	2	2026	\N	SuperUser Login FAILED - not found	41cc44ab-b65e-468e-a397-10a93a9147ee
abdumannon	24	2	2026	\N	SuperUser Login FAILED - not found	488dd838-918a-459a-89dd-2f319617f528
abdumannon	24	2	2026	\N	SuperUser Login FAILED - not found	65e4aadd-41db-4e1a-84f2-a13e82f08e2e
abdumannon	24	2	2026	\N	SuperUser Login FAILED - not found	8ee34ad7-a4bf-429a-a1aa-f387a78fe49a
Abdumannon	24	2	2026	0000	Super logged in	cea0ff51-103b-4afb-91d0-f7db1bad4243
Abdumannon	24	2	2026	0000	Fetched products for shop	5369c5dc-53c5-47ce-bba3-c66b3d1fcb16
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	2cb929ff-d181-454a-8389-3907596b1bde
Abdumannon	24	2	2026	0000	Fetched products for shop	d9b00a1b-7e3d-4a3b-892d-e5268b163296
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	041882e7-5026-4683-9f94-f2fff9a158ba
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	4fffb197-e702-42c0-8675-fdb73661b206
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	3c2ca298-4cd3-4fa6-84bf-77032627e49d
Abdumannon	24	2	2026	0000	Fetched products for shop	78922d4f-1d8a-4668-9b20-3a48242c6f69
Abdumannon	24	2	2026	\N	Fetched all categories - count: 1	c003eaff-f3e8-4399-b954-2531c6d05476
Abdumannon	24	2	2026	0000	Fetched products for shop	2807de88-0286-4a43-a61b-0e82b39720f6
Abdumannon	24	2	2026	\N	Fetched all categories - count: 1	92be96f4-9243-4803-9aa3-f7c93d59129f
Abdumannon	24	2	2026	\N	Fetched all categories - count: 1	2ce91a50-5456-49e0-897e-f0e81e5e3ade
Abdumannon	24	2	2026	\N	Fetched all categories - count: 1	89f65f8d-6ae2-48fe-aa90-e1564ee61027
Abdumannon	24	2	2026	0000	Fetched all debts - count: 1	e0fa9db0-77c1-483e-ae3f-9bc84ca79d6a
Abdumannon	24	2	2026	0000	Fetched debt statistics	fd882aae-225a-400b-b22b-94dd59e079c2
Abdumannon	24	2	2026	0000	Fetched all debts - count: 1	8b1f4b1b-73bb-4def-bc37-d17d548da6a3
Abdumannon	24	2	2026	0000	Fetched debt statistics	a3d3ac18-a9e3-499a-bcf7-d86b13ba1ca0
\N	24	2	2026	Abdumannon	Fetched all wagons	18454de7-ceea-4aa3-8a78-dee18b190f96
\N	24	2	2026	Abdumannon	Fetched all wagons	e8b8af47-7f0e-4422-b613-a0220fcbfde2
\N	24	2	2026	Abdumannon	Fetched all wagons	e2e503f7-ae58-4820-aa2a-baa62ee36bcd
\N	24	2	2026	Abdumannon	Fetched all wagons	42316956-0972-4ca2-afdd-f12960b6c95b
Abdumannon	24	2	2026	\N	Database backup downloaded - tables: 17	77d54fe5-f2c9-48e7-946f-071aa8b6b5e6
Abdumannon	24	2	2026	\N	Database backup downloaded - tables: 17	f96cee77-c8cf-485d-ba3a-200d18a6aec9
Abdumannon	24	2	2026	0000	Fetched products for shop	210ce787-0bc2-49ce-8431-0ec63ce3464a
Abdumannon	24	2	2026	0000	Fetched products for shop	784f4ce0-77b5-4ab8-925c-22a30c7f962f
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	f258e3c6-884a-4399-9db0-e6a5b4cc2bc5
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	13936871-e987-4b89-8b78-250f0da83f67
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	36a8aa6f-f27e-49f4-850f-6b00d99b4be5
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	96eaab1b-9956-40e6-937a-e0902fa6e18e
Abdumannon	24	2	2026	0000	Fetched products for shop	c7839d7d-c9d1-4ee1-a186-357d3543a04e
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	a8a87846-bda7-432a-809e-c24ed055975e
Abdumannon	24	2	2026	0000	Fetched products for shop	cec5e771-1d07-437d-872b-aae918b80523
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	fad0afe5-a22a-43f5-9fc9-f557326217c8
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	aae3d68e-73d3-4a08-8493-89a7fc76b0cd
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	c732f9b4-a844-4c11-b032-4e629ea3ac41
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	aeb76129-4ca0-4a81-964c-a07b76a516de
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	79e7b73b-a287-4485-b9c0-b7a6f775ce80
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	23e13d5b-f5f0-4ae3-a684-d1052854f97c
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	7874de54-3d9f-41aa-8a2c-7315346361fd
Abdumannon	24	2	2026	0000	Fetched products for shop	d3dd126b-725e-4ee1-8dbd-65fb82b36145
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	2fea9eab-4602-4eca-8c96-993aed0c5b4f
Abdumannon	24	2	2026	0000	Fetched products for shop	9312cb68-f3a6-44aa-b557-f0060f205434
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	47434c71-25fd-4c31-b88e-94d5bb9bd8d6
Abdumannon	24	2	2026	0000	Fetched products for shop	6de1c058-96a4-44a2-9acf-c8dcb3c3a2ef
Abdumannon	24	2	2026	\N	Fetched all categories - count: 1	be77f90e-1f6e-4412-86fd-2956204220a5
Abdumannon	24	2	2026	0000	Fetched products for shop	56334e30-4d38-4587-9619-93a095b55eae
Abdumannon	24	2	2026	\N	Fetched all categories - count: 1	18aa8356-d16b-45b4-9a49-ddc4170b39b7
\N	24	2	2026	0000	Error creating product: column "unit" of relation "product" does not exist	c0bd432e-8428-4113-b48c-13ec7be6e605
\N	24	2	2026	0000	Error creating product: column "unit" of relation "product" does not exist	d89dc2d0-139e-4c6e-972c-5101a6556fec
\N	24	2	2026	0000	Product created: bb6a9aac-5661-4a4a-bb1f-7ced48435432	314ce545-4451-426d-87b6-984652538ea9
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	16cdc807-ae6a-49b8-8e8e-3a613eb3525e
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	14094b9d-dac4-4cc1-8da9-ff7387b07177
Abdumannon	24	2	2026	0000	Fetched products for shop	6f047f2f-91bf-4ab6-a11c-fad575043c47
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	f7b32d8d-f441-494c-b51b-dcadbd10af2a
Abdumannon	24	2	2026	0000	Fetched products for shop	dcc06c8e-cdee-4e8b-a75d-9986e966a2d3
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	9e9ceb16-1336-4116-aaee-efd98a49ab6f
Abdumannon	24	2	2026	0000	Fetched products for shop	2d7a8b11-5800-4429-bab3-0c82a8edc3a3
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	a78df69d-60f2-4903-bb63-39206b777715
Abdumannon	24	2	2026	0000	Fetched products for shop	9b23f5d6-a250-41a5-87c0-5f0e094d9376
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	4941f8a0-2bb6-4311-9275-0042ccc51734
Abdumannon	24	2	2026	0000	Fetched products for shop	ae072627-b8f7-4151-986e-fe4490a2975f
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	82a99202-718d-44c3-b76b-fdc70fc4c35f
Abdumannon	24	2	2026	\N	Fetched all brands - count: 3	0cf0349b-00f3-45ce-bb30-0daf52103e39
Abdumannon	24	2	2026	0000	Fetched products for shop	c740f502-29ab-4152-9456-76e4c6b220ba
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	068e8f78-7eed-4522-aa7d-2c593a3d7d32
Abdumannon	24	2	2026	Abdumannon	Fetched all sales	67d17efa-0891-4c24-bd33-c12ef38d5184
550e8400-e29b-41d4-a716-446655440000	26	2	2026	\N	Superuser logged out	1e341435-2967-4a01-9ea8-53ac08774e27
abdumannon	26	2	2026	\N	SuperUser Login FAILED - not found	7ff640c5-ea92-4d22-a46e-09c167e16580
Abdumannon	26	2	2026	0000	Super logged in	d4918819-5ab5-4957-840e-7f34c122437d
Abdumannon	26	2	2026	0000	Fetched products for shop	f0747fd4-e4d3-4ca4-b1d2-eb1c434822d5
Abdumannon	26	2	2026	0000	Fetched products for shop	66eaf5ef-f7b6-46a5-b8df-b125d0a3d47f
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	806ddd9d-a0f1-41aa-a2a8-4431d42edea7
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	98fb1646-b1e2-4ceb-9adf-892e472bce77
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	10cdb27e-cc6e-4183-8fd0-55902291d153
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	2611f7e6-c615-400c-b423-06144c25e913
Abdumannon	26	2	2026	0000	Fetched products for shop	b374e583-3729-45b3-b5ab-f1cd3aada700
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	e3bf438c-7c91-4fab-b749-1cf006c80246
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	b45ababa-5bd6-4ea8-92a9-3133d7983168
Abdumannon	26	2	2026	0000	Fetched products for shop	c0573010-8f5a-4033-88c3-c17a0e3c3d44
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	3f904c7c-0f62-4c8d-abd2-720d99c490cf
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	13790dc7-b560-45ba-ada9-aae1bc6d2929
Abdumannon	26	2	2026	0000	Fetched products for shop	eea17221-a790-45af-9ba3-079d72723865
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	1cefe456-936e-4a52-b6f1-bad3f54a4f1b
Abdumannon	26	2	2026	0000	Fetched products for shop	04254688-56e3-445c-9e4d-ad2c1832b521
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	e0033597-1d11-4491-9b4b-f61bcd9020d5
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	a8e2ae85-b201-4832-a4d6-e3bf43ca670e
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	82a026b9-802f-4ca7-a2b7-5a428d14aa5c
Abdumannon	26	2	2026	0000	Fetched products for shop	7be78515-0813-4c75-a6a5-441dde80c4f7
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	a5b4eca0-034e-4822-9f3c-d179bdf35765
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	8d2a29c4-43f0-47d7-a927-ddd20ef03be8
Abdumannon	26	2	2026	0000	Fetched products for shop	e738e998-bb08-42e5-b0ea-cc5727ba16c1
Abdumannon	26	2	2026	0000	Fetched products for shop	0769d48a-2fc5-4093-b48a-bf0121cbc158
Abdumannon	26	2	2026	0000	Fetched products for shop	cc9805fb-a82d-4030-9f12-cfbb7846d1b3
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	ba0f4da2-d798-4fef-a24c-a7e0a36035d8
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	49bc225d-5382-4272-9b64-601e3d548489
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	f3c331b4-d3d5-45b4-bba3-a7b5fd2d4a3f
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	ea92791a-b07e-436d-aff6-4471fc13f0dd
Abdumannon	26	2	2026	0000	Fetched products for shop	651e1b7d-f474-435a-9b21-a7c13368f3f9
Abdumannon	26	2	2026	0000	Fetched products for shop	cdde29f0-09a4-4596-a832-1b6b28cc6a93
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	a925763e-32f4-44b7-98b1-808678e627b8
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	56be6caa-097f-4eea-9dd3-4834e1d89924
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	972a5194-a546-4146-ac87-0ef3291cd270
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	13c69c0a-1949-4c5c-88c4-31db1bbec1d7
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	0dbcad6f-1681-41c9-bc99-c15307a97491
Abdumannon	26	2	2026	0000	Fetched debt statistics	28d91460-73b6-4735-b3e2-5bd3484a3059
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	e21b57f4-3391-4f5f-a745-7ae8a90da1bd
Abdumannon	26	2	2026	0000	Fetched debt statistics	e60e190f-ba5f-4b5e-ac8e-4d65e29231e8
Abdumannon	26	2	2026	0000	Fetched products for shop	0fbf4d78-edb8-4287-88b6-83c112b11073
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	068fc089-f8ea-4bc4-8bb2-08cae6fb07dd
Abdumannon	26	2	2026	0000	Fetched products for shop	137b1a06-f03b-4884-a38e-f778499c1786
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	4493bd59-0843-446f-b897-2d208f2ceb4d
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	277e8ac6-3de1-4b6c-8588-102338b33e3b
Abdumannon	26	2	2026	0000	Fetched debt statistics	9d336a7b-deee-4076-862a-332eb636eaa5
Abdumannon	26	2	2026	0000	Fetched debt statistics	362e15d6-96bd-4846-b0bc-6e369b1b6c2d
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	d8269eb4-1ec7-4478-b9cd-faf0c7931cb9
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	1383e37d-9c4c-4a5e-8fe6-d2076a062066
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	e4bce517-207a-4dee-93ff-84e14a5aaf8d
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	26d975c4-65da-43d3-a8a1-9849d5d3ec92
Abdumannon	26	2	2026	0000	Fetched debt statistics	d774e699-0557-4ecb-b920-540c5a19a357
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	8404377f-a6a3-423d-bb24-59e8d00dcb34
Abdumannon	26	2	2026	0000	Fetched debt statistics	77fee952-d2e1-472f-8379-f7b5ababeb5e
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	0fd179d9-1b1f-4927-ab48-5136e6de3d42
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	be548174-bcc8-41c5-87c7-82e6a97b90c7
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	6127f577-414e-4826-8c4a-42f90dc24213
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	829efe3f-7110-4d75-8caa-1c8a477a144c
Abdumannon	26	2	2026	0000	Fetched products for shop	fb21338a-0110-49e5-90f0-a6729ba46e3b
Abdumannon	26	2	2026	0000	Fetched products for shop	8da62f29-03ff-4c6f-bbe8-45d984dd8899
\N	26	2	2026	0000	Missing required fields in createNewProduct	5b2cfe12-66c2-42d4-a2d9-59dae26b3550
Abdumannon	26	2	2026	0000	Fetched products for shop	6af2a4c7-788b-4ff1-9275-f1ca13f5d1d4
Abdumannon	26	2	2026	0000	Fetched products for shop	06734bf6-79ec-419e-8ba4-265ba352395c
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	5759b9d8-81c6-4321-ba36-2f299fed05e6
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	cde824a2-b485-4f95-9adc-1cead860be3a
\N	26	2	2026	0000	Missing required fields in createNewProduct	96213719-5918-4261-b073-9b72da36b53d
Abdumannon	26	2	2026	0000	Fetched products for shop	0533d0cf-d1d0-4981-ae78-97a7caea8b14
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	1c203a27-05e1-407b-b1ca-23ebb3c6b47e
Abdumannon	26	2	2026	0000	Fetched products for shop	c38e2139-7015-485f-bce4-2a51b20f66e2
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	de1a7dfe-6fac-4d08-8971-2750654807d0
\N	26	2	2026	0000	Missing required fields in createNewProduct	e3891446-4bf5-4491-8bbc-a0dae5fb48de
\N	26	2	2026	0000	Missing required fields in createNewProduct	f73dd3c1-bcef-498a-8042-cf5bfefd649a
\N	26	2	2026	0000	Product created: ab450fc0-8b95-4c6f-907e-d72b8e621929	62194cb3-a3a1-4130-9e8c-d33dcbb0c66c
Abdumannon	26	2	2026	0000	Fetched products for shop	9c52bc02-7cca-4a8e-9830-fdcb2529d96b
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	e6e1c565-e1fd-4560-8b35-062fdadf96ff
Abdumannon	26	2	2026	0000	Fetched products for shop	aed21784-935d-4c12-b4dc-0637aab89888
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	b082b7db-9596-414b-93b7-92fe87752d06
Abdumannon	26	2	2026	0000	Fetched products for shop	27000f14-a337-460e-aa54-26ec190ee8e7
Abdumannon	26	2	2026	0000	Fetched products for shop	fab9be1b-63da-4fd6-a672-e9c3d64fc23b
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	e58af8bc-b073-43c5-a891-22f2435d4bf0
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	944e5034-2e76-4f0c-957b-d86ff539f695
\N	26	2	2026	\N	Product restocked: fd3fffe6-044c-4ee3-b815-471be7dd887f (added 40)	9e786249-0bee-4805-aeb2-25ee0a10cc29
Abdumannon	26	2	2026	0000	Fetched products for shop	ca750b29-5946-4abd-bb92-63de26be1ab1
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	9629a716-7319-468a-8d05-e316848bb302
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	38b7dba1-8d51-44ef-8927-ae3079a9f037
Abdumannon	26	2	2026	0000	Fetched products for shop	12e29f7e-9ad8-460c-a206-148e82ad57e4
Abdumannon	26	2	2026	0000	Fetched products for shop	77ea7619-f098-4417-b487-88949a7e9416
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	742c05f2-9996-49ad-ba36-7a0782667cf0
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	d40b0473-38df-45d2-8737-21688a144ae9
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	ed02db44-ec61-468d-99f2-e6274fd0ebd0
Abdumannon	26	2	2026	0000	Fetched debt statistics	fc00fc95-a00c-49f5-adda-1629595766cf
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	389cdac9-66ca-43b6-a530-7235b000b53f
Abdumannon	26	2	2026	0000	Fetched debt statistics	d8e5be60-5aee-486f-9f9a-fee05911a614
Abdumannon	26	2	2026	0000	Fetched products for shop	5ac65d7e-59be-4a8b-a9f6-ff44051f9a59
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	3e83a390-7799-4706-a076-1dd5b9f74fca
Abdumannon	26	2	2026	0000	Fetched products for shop	081fd4e5-d64f-458e-bae2-68b16e7fac08
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	209871b0-7d03-4cdb-9d4f-f0b0ec0b60ab
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	7107a0dc-d1f3-40bd-b4d1-70f119ab0bd7
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	e04b032c-eba8-4847-8ddc-a6a02567899b
Abdumannon	26	2	2026	0000	Fetched products for shop	cfe0a525-38a1-4abc-9693-c0ded6780433
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	174bb0d5-fa75-4231-b3d1-7a9c46e51b09
Abdumannon	26	2	2026	0000	Fetched products for shop	9b707dba-b59c-4851-ab96-bc7c338924df
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	b584e642-5051-460c-a291-826ebcd54916
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	21a1714e-6a10-4819-914e-f6b70f99a5d9
Abdumannon	26	2	2026	0000	Fetched debt statistics	48edf571-3b87-4904-8490-d78a60dd6328
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	a0f7f694-a69b-4c01-81d2-24924a3c7314
Abdumannon	26	2	2026	0000	Fetched debt statistics	46392ef6-99ca-42d8-92bf-86841f0ae2bc
Abdumannon	26	2	2026	0000	Fetched products for shop	153c4d0c-624d-45ab-960f-2d81ec12a0b4
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	93f3750f-4a68-48dd-9e1c-d480190316d3
Abdumannon	26	2	2026	0000	Fetched products for shop	c94aecfc-d183-4129-bed3-9c4e045c285f
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	d05a49e4-fa01-4847-98fa-bbf5e430228b
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	9736a8e5-14b5-4888-bba7-8195ced39d42
Abdumannon	26	2	2026	0000	Fetched debt statistics	6e57380c-3b90-4c21-bfff-444bde67868c
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	b9a49778-538d-4b83-b31b-3735adfc88c2
Abdumannon	26	2	2026	0000	Fetched debt statistics	83823919-8d94-48b1-9bc2-a917a997148b
Abdumannon	26	2	2026	0000	Fetched products for shop	1798c3fd-7bff-4c5b-919f-2f5bec0f1cef
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	233be348-78d5-4ac6-9cde-0bc8773a266d
Abdumannon	26	2	2026	0000	Fetched products for shop	a753f93f-f46f-4908-bc83-038fa7419c85
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	f9fa2429-a197-476d-8bf3-825bc29d9cba
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	f3d8fb32-35d9-40ef-9da4-289cc7d7cd0a
Abdumannon	26	2	2026	0000	Fetched debt statistics	a15691e8-4f75-486a-978e-9964b559be61
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	7850a30d-8378-455d-9aba-f316a03efdfc
Abdumannon	26	2	2026	0000	Fetched debt statistics	19158881-d5ff-4305-987f-43cdf0865a3e
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	251c5906-9873-4c6d-b01c-aacc5df8cb1f
Abdumannon	26	2	2026	0000	Fetched debt statistics	178ba3c8-794c-455a-ab19-7496ceff53df
Abdumannon	26	2	2026	\N	Fetched debt by ID: c0451be7-01ce-48c9-8fb3-f66bf51fb968	383262c2-1f88-4c13-b73c-1395d79ff3f9
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	a7ac93c9-4daa-4575-913a-65281426cf9a
Abdumannon	26	2	2026	0000	Fetched debt statistics	42ddbef3-4de1-4156-9315-e7df5f4a2067
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	2ea89bcb-4eb5-4b4a-b0a5-903e33813f4c
Abdumannon	26	2	2026	0000	Fetched debt statistics	fc4b7536-7b72-4f72-b89c-89b0fd1cba19
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	d03d3101-35f7-4919-acab-2f4dec66794d
Abdumannon	26	2	2026	0000	Fetched debt statistics	b77d4e7b-42d1-43aa-8268-585a845fedef
Abdumannon	26	2	2026	0000	Fetched all debts - count: 1	d8444c72-5e38-4505-b0e9-6622c896f9da
Abdumannon	26	2	2026	0000	Fetched debt statistics	55b4ff10-bfb8-4f3d-aeaa-3c7bd15a9aac
Abdumannon	26	2	2026	\N	Fetched debt by ID: c0451be7-01ce-48c9-8fb3-f66bf51fb968	46728983-c6af-49e0-bbda-089a19de8c10
Abdumannon	26	2	2026	\N	Debt updated successfully: c0451be7-01ce-48c9-8fb3-f66bf51fb968	c602229f-8b73-4d70-9b5b-3b217e525329
Abdumannon	26	2	2026	0000	Fetched debt statistics	e6787ed3-778a-43d9-80c0-ffd65119e115
Abdumannon	26	2	2026	0000	Debt created successfully - customer: Qobil, amount: 200	b171903c-24b9-47df-b7cb-458324db4a54
Abdumannon	26	2	2026	0000	Fetched debt statistics	154ccfe5-b158-4e75-a571-57e666e35fd6
Abdumannon	26	2	2026	0000	Fetched all debts - count: 2	fb5a278b-09bd-415b-8ada-d8037aed6e52
Abdumannon	26	2	2026	0000	Fetched debt statistics	5d96f231-31fd-49df-ad36-d8f15db8a1ac
Abdumannon	26	2	2026	0000	Fetched all debts - count: 2	38d73a0c-8551-4a4d-9d8a-2db1b9bcfd24
Abdumannon	26	2	2026	0000	Fetched debt statistics	1158ff67-4317-466d-88de-84bf250e3c8a
\N	26	2	2026	Abdumannon	Fetched all wagons	a34d10d2-20a4-412e-a562-1b2a9092587b
\N	26	2	2026	Abdumannon	Fetched all wagons	d390b083-d8aa-4193-9544-3a1971b1218d
Abdumannon	26	2	2026	0000	Fetched all debts - count: 2	ea404cb9-da71-4527-8a46-0fbdc1b305d4
Abdumannon	26	2	2026	0000	Fetched debt statistics	b5b058c3-069f-464e-b15f-9ecb3119e225
Abdumannon	26	2	2026	0000	Fetched all debts - count: 2	63660008-02ba-46f4-ae19-695296c31f25
Abdumannon	26	2	2026	0000	Fetched debt statistics	55192ac4-967f-4779-a69e-7578fbb7263b
\N	26	2	2026	Abdumannon	Fetched all wagons	76c342c4-fc4c-4f7c-999d-2463d5fd3d60
\N	26	2	2026	Abdumannon	Fetched all wagons	09c32ba9-c905-40b1-97d9-28b8b94de729
\N	26	2	2026	Abdumannon	Created wagon: Salom,23434	5d6c2bd4-806b-44b5-801c-b9d42f2bdf7b
\N	26	2	2026	Abdumannon	Fetched all wagons	c33ffd92-d538-46b6-9f5a-b36f19f82d1f
Abdumannon	26	2	2026	0000	Fetched all debts - count: 2	928b5a20-012a-4781-b1fc-c27c19fd3a82
Abdumannon	26	2	2026	0000	Fetched debt statistics	bb4beeff-a1c1-4ca4-b0ad-550de1828fe3
Abdumannon	26	2	2026	0000	Fetched all debts - count: 2	3acfa6b9-2fdc-45ad-8c7e-55092fbb3f3c
Abdumannon	26	2	2026	0000	Fetched debt statistics	4b5c628e-ba2e-48a6-b8a7-39da8f1f6df2
\N	26	2	2026	Abdumannon	Fetched all wagons	882f0f9d-a9c0-449b-b20a-96567974e67b
\N	26	2	2026	Abdumannon	Fetched all wagons	529ea908-995b-45e8-b507-9e58cf91acc4
Abdumannon	26	2	2026	0000	Fetched all debts - count: 2	402395ad-aeac-4023-bdc9-72bc2c0fc830
Abdumannon	26	2	2026	0000	Fetched debt statistics	cd43d138-bcb9-4609-9549-d5a1ab059bde
Abdumannon	26	2	2026	0000	Fetched debt statistics	7b206fc5-e217-4c62-9b13-b7ff05093ca2
Abdumannon	26	2	2026	0000	Fetched all debts - count: 2	caeb6ee7-c925-496f-ade9-59edf0963252
Abdumannon	26	2	2026	0000	Debt created successfully - customer: djfngkfdk, amount: 4488	5f21bd04-4945-4d84-bbf5-9347b5398adc
Abdumannon	26	2	2026	0000	Fetched debt statistics	8c2b5e52-c703-4777-8ce8-dc7e9971de48
Abdumannon	26	2	2026	0000	Debt created successfully - customer: Qobil, amount: 11730	1ca0e330-99ae-4464-b86d-336e8dcd477d
Abdumannon	26	2	2026	0000	Fetched debt statistics	ab161490-caf4-4358-98ac-195ad03527ea
Abdumannon	26	2	2026	\N	Debt updated successfully: 15e5f9d0-75e4-4b7d-b482-9a19c94afce1	54a010c6-c624-4a2d-afba-5714a627885c
Abdumannon	26	2	2026	0000	Fetched debt statistics	9bfe359e-095c-495e-b0e5-8039a7f02993
\N	26	2	2026	Abdumannon	Fetched all wagons	694e62e7-ba7a-4912-bd0e-7b3d955b5882
\N	26	2	2026	Abdumannon	Fetched all wagons	6f3691ff-30f1-41fc-900a-bb0fcb4f9585
\N	26	2	2026	Abdumannon	Fetched all wagons	22a120e6-fd69-4441-b42c-db6912ced09f
\N	26	2	2026	Abdumannon	Fetched all wagons	0d7b82a8-2fe6-49b7-89ef-eca51ca9e62f
\N	26	2	2026	Abdumannon	Fetched all wagons	05ba09ee-4fbb-4896-9957-68aef4c15394
\N	26	2	2026	Abdumannon	Fetched all wagons	e3e19e27-a269-4b2b-9bf6-d999855a4294
\N	26	2	2026	Abdumannon	Fetched all wagons	8ba8b03d-b781-4f9c-b2d9-e8397060b66c
\N	26	2	2026	Abdumannon	Fetched all wagons	b00ee32d-4127-437b-a0e2-7cfe526c948f
\N	26	2	2026	Abdumannon	Fetched all wagons	eb84fd5c-1a60-4c00-a409-e6f24489f3a8
\N	26	2	2026	Abdumannon	Fetched all wagons	fe7ab53a-3c24-4721-9d82-87e56db2646a
Abdumannon	26	2	2026	0000	Fetched products for shop	d1f3725d-df48-4336-8736-38743e1be8f8
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	f4cb2ed0-d7f7-46d3-bc10-902ed49a893d
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	b6079992-42f6-438e-bc9c-b3600b03899c
Abdumannon	26	2	2026	0000	Fetched products for shop	fcfafb1a-79e7-4e79-a594-b4850c4a84f3
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	30520f37-2498-42f9-bc39-164bc1f2dd98
Abdumannon	26	2	2026	0000	Fetched debt statistics	06fe80eb-0da1-40f0-83bd-a2761aec947f
Abdumannon	26	2	2026	0000	Fetched debt statistics	f60909b4-3dcb-4857-87ec-86fb80622498
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	15055501-9014-4aac-9148-9c69b51a15ad
\N	26	2	2026	Abdumannon	Fetched all wagons	d87312d2-a457-4318-9eaa-599ce4d15822
\N	26	2	2026	Abdumannon	Fetched all wagons	6892e9b2-e59f-468a-bed6-7e5a1846f67d
\N	26	2	2026	Abdumannon	Updated wagon: a971059e-9b02-4964-b077-4be55f048c33	73574e0f-5d0c-40a4-9ece-cd0793aa187d
\N	26	2	2026	Abdumannon	Fetched all wagons	0805ec6d-30c4-4502-949e-08e5966081ea
\N	26	2	2026	Abdumannon	Updated wagon: a971059e-9b02-4964-b077-4be55f048c33	1f020e38-8a24-4a25-87e7-589b69474df9
\N	26	2	2026	Abdumannon	Fetched all wagons	47e52da6-abaf-4546-aaab-8507091ff502
\N	26	2	2026	Abdumannon	Created wagon: Salom,1234	e638d645-73df-43eb-b573-8b69d0fee35c
\N	26	2	2026	Abdumannon	Fetched all wagons	eeea94a7-db2f-4381-8009-46761b213a4b
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	1ba982dd-2e5e-43a4-afd3-ee40048e60e0
Abdumannon	26	2	2026	0000	Fetched debt statistics	0bef7704-78e4-479f-97e7-6d4f950277c7
Abdumannon	26	2	2026	0000	Fetched debt statistics	42e50dbf-7ffb-4460-b40d-64dd81b7f4ff
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	5d33a37e-458c-47de-9485-822cfc71f4d2
\N	26	2	2026	Abdumannon	Fetched all wagons	5a20afe7-d920-47ea-ad14-2bd3fd573be5
\N	26	2	2026	Abdumannon	Fetched all wagons	437bac26-7112-4001-960a-d7fe5cb1ea05
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	ba3a1a05-bca3-495c-aeb5-9bc650ab8ad6
Abdumannon	26	2	2026	0000	Fetched debt statistics	e433d889-096f-4576-b646-66d078a8a88d
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	576982cc-0ea4-4fab-8a7c-c90fab02fb5c
Abdumannon	26	2	2026	0000	Fetched debt statistics	1464815b-c047-4087-a781-628f688e760c
\N	26	2	2026	Abdumannon	Fetched all wagons	22dd5c12-55ba-4ea8-844c-31f13c8e558b
\N	26	2	2026	Abdumannon	Fetched all wagons	fbb5701d-408c-44d0-bf1f-a4fbd1e228d1
\N	26	2	2026	Abdumannon	Fetched all wagons	f288a3bc-46f9-4988-a3b6-02c6b8a6c381
\N	26	2	2026	Abdumannon	Fetched all wagons	805f1bb2-5c78-4bc2-a2f8-3fa9341e71b2
Abdumannon	26	2	2026	0000	Fetched products for shop	67c75fc2-28bd-4e46-91b8-8d3f8559ff5b
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	ec7cd808-5f63-405c-9d14-6d7745de19c5
Abdumannon	26	2	2026	0000	Fetched products for shop	a8b07b4d-7ac5-4619-a9f1-5ede54c24060
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	a4a67127-79ac-4a48-b663-105e77e3182e
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	c9525a1a-a126-46f6-b5cc-9c91baf49313
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	e51eae6c-7c18-4c9b-af8e-e37550776411
Abdumannon	26	2	2026	0000	Fetched products for shop	41dbbf83-03c1-45e0-be06-d293c8677717
Abdumannon	26	2	2026	0000	Fetched products for shop	e9cf19a9-f7d6-41a9-973f-11b30185b7b9
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	274c1f77-2937-4328-8a9b-99eb9d25a5f9
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	06818b96-e078-4421-80ef-87d0e4642087
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	2281c660-12a5-405b-8ea3-73472b029a87
Abdumannon	26	2	2026	0000	Fetched debt statistics	37907e96-7e2d-44b7-a323-a752e38f76ac
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	8eb14591-9564-400c-b521-a11e55bea571
Abdumannon	26	2	2026	0000	Fetched debt statistics	2e8a54bd-7222-4746-b67b-b0a5c1d791be
\N	26	2	2026	Abdumannon	Fetched all wagons	1037f71d-38a5-4471-868a-eba3b9561c64
\N	26	2	2026	Abdumannon	Fetched all wagons	5be2947c-154c-4285-882f-054c143b0587
\N	26	2	2026	Abdumannon	Fetched all wagons	a6ef5058-5020-4d35-9103-32f7cdc7bd17
\N	26	2	2026	Abdumannon	Fetched all wagons	60dabf1e-d738-4116-9c83-7d5a6209a541
\N	26	2	2026	Abdumannon	Fetched all wagons	008f3890-8434-42f4-b89f-b5b911a9ff1e
\N	26	2	2026	Abdumannon	Fetched all wagons	e8b5461e-032b-4ca9-b926-9ccf1eec308a
\N	26	2	2026	Abdumannon	Updated wagon: ed6926d5-4555-47ff-841c-133559111389	61e91325-cb63-4751-a498-664e67862a21
\N	26	2	2026	Abdumannon	Fetched all wagons	123998c3-c219-4a58-ac22-648864c015c5
\N	26	2	2026	Abdumannon	Fetched all wagons	83f2b362-a574-42b7-b5d1-f95e87d4f7d6
\N	26	2	2026	Abdumannon	Fetched all wagons	06ce100a-314d-483c-86d4-9a4a41cceb4f
\N	26	2	2026	Abdumannon	Fetched all wagons	2a9c379b-c041-404d-89b6-ac8b0bf73a53
\N	26	2	2026	Abdumannon	Fetched all wagons	916edcd4-ada1-4eef-b085-2df4d29e297b
\N	26	2	2026	Abdumannon	Fetched all wagons	3e0f8778-c6a1-441b-8471-9bbfd9ad9810
\N	26	2	2026	Abdumannon	Fetched all wagons	c83fa558-8e2a-45b4-b296-df451a873731
\N	26	2	2026	Abdumannon	Fetched all wagons	56d2ff1a-1820-4c04-9a48-25a5b19d36df
\N	26	2	2026	Abdumannon	Fetched all wagons	0490ca95-c96d-4a84-989c-0bc0498dac9a
\N	26	2	2026	Abdumannon	Fetched all wagons	cc5e8000-e93b-4112-8aa5-5495d90d7c49
\N	26	2	2026	Abdumannon	Fetched all wagons	67e239f2-0af6-421a-a646-58e7d2101ce8
\N	26	2	2026	Abdumannon	Fetched all wagons	b959f1bc-1069-4ca4-b2bd-6daae4af2f27
\N	26	2	2026	Abdumannon	Fetched all wagons	e9a95b2e-c359-451b-97d1-f3f109723f71
\N	26	2	2026	Abdumannon	Fetched all wagons	2e8e8f1b-7344-4608-bc82-676cfcb17450
\N	26	2	2026	Abdumannon	Fetched all wagons	a4ff0193-ce42-4671-ab2c-10ade2ecb59b
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	323a77ef-eda9-4770-9cde-a563a699b694
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	90faa70d-aec3-44ee-a45b-88aa80ebf19f
\N	26	2	2026	Abdumannon	Fetched all wagons	06612e5c-bb79-4301-9515-d46e2ab60aa0
\N	26	2	2026	Abdumannon	Fetched all wagons	cc87aa16-7e48-4a2c-b593-0c96871cbb06
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	4b48e8a1-8684-4c72-b41e-0f889948c770
\N	26	2	2026	Abdumannon	Fetched all wagons	5d2d0e87-b676-4821-b62e-62e0ab56f493
\N	26	2	2026	Abdumannon	Fetched all wagons	f6099c08-1361-4488-a8d7-e75ef6a5daa0
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	92a595f1-f9ed-4ee6-9199-9ca0e1882ad1
Abdumannon	26	2	2026	0000	Fetched debt statistics	2a70ca95-1cce-4fe1-9a2c-0f49a94005f9
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	bbf8abc9-da21-4f77-adaa-8a059e38f860
Abdumannon	26	2	2026	0000	Fetched debt statistics	e9730914-79a0-4578-b89b-7fd3b813d68b
\N	26	2	2026	Abdumannon	Fetched all wagons	b60e0b2b-3db6-423d-ad59-ab8bbbb7118d
\N	26	2	2026	Abdumannon	Fetched all wagons	216a4ada-3cd8-4a08-a26f-8bf2c85aa0b4
\N	26	2	2026	Abdumannon	Fetched all wagons	78e857d3-7237-400c-96a9-ad76c96ef704
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	266ebbbf-036f-49b6-bc0d-6a7e657c65e1
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	7cdb1afd-b181-476f-b328-63f15a090095
\N	26	2	2026	Abdumannon	Fetched all wagons	f499d0ee-1baa-4184-913a-f50acf39418d
Abdumannon	26	2	2026	\N	Database backup downloaded - tables: 17	fda8b8d1-fb66-4455-a868-96760019d324
Abdumannon	26	2	2026	\N	Database backup downloaded - tables: 17	bb58725b-fe57-426e-a3a4-b8c218586554
Abdumannon	26	2	2026	0000	Fetched products for shop	124114fb-100e-4336-86d1-cc6abc3b9df3
Abdumannon	26	2	2026	0000	Fetched products for shop	bb978d09-7b77-4992-9dee-0c8b40258ef4
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	9cea9bcb-49b2-4792-8547-918b294004d3
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	da0aba87-fc55-4129-ac35-10b1e5ecd235
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	00c50095-b5bd-4f55-8727-43341d162500
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	2bcaf651-0b05-4859-8f7c-f6b0e3b83bd8
Abdumannon	26	2	2026	0000	Fetched products for shop	03010d5b-34c5-4c7f-acc8-87de6f3d9ce1
Abdumannon	26	2	2026	0000	Fetched products for shop	732971d8-ebec-4220-9742-760754a9ccdf
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	6a855814-7669-435d-8ca4-9499b2c651d1
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	0f949866-8931-48fd-9346-94d520596075
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	5d106d50-52ff-4ac7-bbe0-6bb349bbb1c9
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	f548a658-0b3c-4e7a-99ba-bd06fc680468
Abdumannon	26	2	2026	0000	Fetched debt statistics	23767d4d-004c-44aa-91aa-6330f475da88
Abdumannon	26	2	2026	0000	Fetched debt statistics	b1a683bb-7fce-40d8-a0e3-3f77c330b91c
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	0646b8bf-16cb-4936-946c-4e159031a01c
Abdumannon	26	2	2026	0000	Fetched debt statistics	1a475642-5fed-4788-a81f-7412ece6a494
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	bbf592b3-d7a5-4f92-9c1c-a10979921973
Abdumannon	26	2	2026	0000	Fetched debt statistics	74f9dd74-ccd0-4d1b-a21d-441bcc18a7fd
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	2e3da8e5-fa38-46cc-8a66-4359b23bbd5a
Abdumannon	26	2	2026	0000	Fetched debt statistics	c1f2b8d0-fa64-4569-88b5-f8213b3b9e15
Abdumannon	26	2	2026	0000	Fetched debt statistics	3b437203-6a6b-4ea5-9290-cbabba7579f2
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	bf208fd2-dfd0-42f1-8ca9-8a7de5d8ed99
Abdumannon	26	2	2026	0000	Fetched products for shop	fc9f53c9-3fd2-4050-9113-e7a37f468d6c
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	bb4507fd-be9a-4d2f-829f-82d6370dbec0
Abdumannon	26	2	2026	0000	Fetched products for shop	1bb9e95d-db69-4023-b91a-dd17408382b0
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	c65bfc6f-dfc3-4b64-8b0d-fdb1ae46d38a
Abdumannon	26	2	2026	0000	Super logged in	4f78a1ff-a314-4ecd-9074-4504c23f04bd
Abdumannon	26	2	2026	0000	Fetched products for shop	f4fb4f93-0b6a-45a8-b35d-c42b773c21a9
Abdumannon	26	2	2026	0000	Fetched products for shop	5fc6a681-30bb-4fb5-afdd-84a3e70fa446
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	7c4bd5bf-e93e-4ea7-9865-3540dc22e624
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	1917bbae-92db-4c26-8dae-4706aa119d42
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	70a5d701-f925-4481-89bf-a22bb2ce2cf9
Abdumannon	26	2	2026	0000	Fetched debt statistics	ee8dba94-44ef-476c-a0d7-fc2160bd59db
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	72114f86-bfc8-4844-80b9-54ace6214b56
Abdumannon	26	2	2026	0000	Fetched debt statistics	28458841-752c-4630-b8db-696556aca10f
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	17c6fb35-1909-420d-a756-7d0dd2e6429e
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	9ba7e1f8-e32c-405f-bade-b102b5334ff9
\N	26	2	2026	Abdumannon	Fetched all wagons	9a483bad-b049-49ac-a97f-77b7259016b0
\N	26	2	2026	Abdumannon	Fetched all wagons	91a6e203-95d6-41cd-878c-c3eff90d43a6
\N	26	2	2026	Abdumannon	Fetched all wagons	c7445e68-d3d4-4bcf-9bfe-194d2827667d
\N	26	2	2026	Abdumannon	Fetched all wagons	8270d048-5c18-4b48-8b94-a43fbf0ff6d5
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	5918ebef-afb5-4308-87e3-d49d20d069b4
Abdumannon	26	2	2026	0000	Fetched debt statistics	0507ab75-3782-4932-a01b-3325989e8556
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	07c54c47-34ae-4506-a939-c2eed409cee1
Abdumannon	26	2	2026	0000	Fetched debt statistics	a3c599ad-9137-468c-96d5-6b5f33252e31
\N	26	2	2026	Abdumannon	Fetched all wagons	ca6d0014-8841-468c-b5af-c383821f5dfc
\N	26	2	2026	Abdumannon	Fetched all wagons	db551071-f9b5-4473-9e6c-9d90bfb6cee2
\N	26	2	2026	Abdumannon	Fetched all wagons	dd04a139-facd-4939-a226-4fd56e3b1f39
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	410126e8-3399-4feb-ad74-e50939f774c2
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	06a18792-8e95-43ec-8fd8-2dea0cb268d4
\N	26	2	2026	Abdumannon	Fetched all wagons	2f49a409-a1a7-4c57-b105-7944e036807e
\N	26	2	2026	Abdumannon	Fetched all wagons	7979bc0a-3ac2-4acf-a48e-75290bf4d807
\N	26	2	2026	Abdumannon	Fetched all wagons	7a22da99-a7f6-496f-808e-3472637add56
\N	26	2	2026	Abdumannon	Fetched all wagons	ea65b8d9-abaa-44b3-bb29-daf481bb9335
\N	26	2	2026	Abdumannon	Fetched all wagons	caf2476e-e6be-4dc1-937f-2e99b9e29cb2
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	458a1d9b-ff03-43f6-bd20-ea0d57ef06cf
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	b89d55eb-52b0-4b4a-9653-7b2763250bb1
\N	26	2	2026	Abdumannon	Fetched all wagons	9f6ce8a7-a13c-4ee2-b550-f6d16f24c114
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	6f7ce89f-5cd4-4155-a063-d587f60e2dfd
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	696d0a99-4be9-4fc1-98a3-090c1f196406
\N	26	2	2026	Abdumannon	Fetched all wagons	a4ed1e0a-476a-4cf7-8660-cb8e74867cfd
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	7cd4e3b9-16ab-4145-8145-454ce920474a
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	1eb5b259-a2b1-4212-bd21-d338afc72a20
\N	26	2	2026	Abdumannon	Fetched all wagons	4dc9559b-3274-48a4-b5a9-81b6b6d3c7c6
\N	26	2	2026	Abdumannon	Fetched all wagons	604bbe7f-7f8a-4ef1-9ef7-da8601a4b591
Abdumannon	26	2	2026	0000	Fetched products for shop	c3142615-5c0b-4f89-9596-383d67f4d8bb
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	c7b20e8b-aba1-4dff-a7c2-9317fff6570e
Abdumannon	26	2	2026	0000	Fetched products for shop	91e75de2-bfa6-4910-bd27-820dd47912e0
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	3c36f31f-d7ae-4709-9df0-f971d8998bb6
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	f552eab2-b484-4a22-8966-e04a46e745b7
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	59a06b24-eb0a-4308-b371-50aa48d81439
Abdumannon	26	2	2026	0000	Fetched products for shop	94b83900-125b-4483-9faf-0335b9b5a5cb
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	66b8da73-c3ca-46a3-acd6-9a71b216f687
Abdumannon	26	2	2026	0000	Fetched products for shop	7df6b19d-93cf-4897-bd0c-3992af557191
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	99be1034-1a9a-461d-bb28-818c6b5d4d03
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	e4bce9ef-11f4-4ec4-8aec-5c3ee24cb2df
Abdumannon	26	2	2026	0000	Fetched debt statistics	a715c6d1-524f-4c55-9fa9-7ecd118aec1d
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	66ab2a95-8d0f-4530-a8c9-903652c6ee0c
Abdumannon	26	2	2026	0000	Fetched debt statistics	5e7e5b15-714e-4c49-a24a-d5362438cbdc
\N	26	2	2026	Abdumannon	Fetched all wagons	0c1de977-4ba8-4db8-97d0-545dda7e7247
\N	26	2	2026	Abdumannon	Fetched all wagons	75a9f911-fee1-4928-a70e-9690c1d3b8a1
\N	26	2	2026	Abdumannon	Fetched all wagons	a74134ad-c164-4700-8adf-277f21cf261f
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	51610b10-4a0f-443c-abfe-5be854297338
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	ac296739-fc18-4a08-960f-81f695e87368
\N	26	2	2026	Abdumannon	Fetched all wagons	4c361ad9-9161-4e9c-bd85-c9b4748cf5c7
Abdumannon	26	2	2026	\N	Database backup downloaded - tables: 17	c6006d66-0071-4f16-a956-5ae1b9f3dc60
Abdumannon	26	2	2026	\N	Database backup downloaded - tables: 17	75ad3e34-d873-4592-b532-997c9d0a6752
\N	26	2	2026	Abdumannon	Fetched all wagons	456d63f9-9044-4559-a67b-bbce597722bc
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	b0e1f898-17d1-4324-b7e5-da8e9265f6ad
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	0d4b608d-325c-4954-98d5-f0f3ebafee94
\N	26	2	2026	Abdumannon	Fetched all wagons	37a10395-3284-46f4-b05e-d7a2fcbbae92
\N	26	2	2026	Abdumannon	Fetched all wagons	df9c5e04-6a50-4bec-8f66-13b49d87f9a9
\N	26	2	2026	Abdumannon	Fetched all wagons	0bc2ea0e-4f60-4a9c-9fa4-0d1a6938b28f
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	2d1292a8-627d-49ae-a4dc-cd496fc23525
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	85286978-d875-48f0-9278-a3797e87292e
Abdumannon	26	2	2026	0000	Fetched debt statistics	7ba85135-a806-4f72-9ab0-897b3021a441
Abdumannon	26	2	2026	0000	Fetched debt statistics	3a776f9a-5488-4571-8905-9feb09f44f77
Abdumannon	26	2	2026	0000	Fetched products for shop	854b7686-ff44-416d-8561-ad20390ed667
Abdumannon	26	2	2026	0000	Fetched products for shop	bd7144a9-e598-4168-80f0-8dd44fd4ee49
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	45e80aed-f864-4592-b11e-4b47560aa3d6
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	aa270b3d-a360-4527-8a81-1c066d1b49da
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	83bac4c0-b276-4620-b0da-06519840e9cd
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	5073ce13-4fc1-45f9-af56-2c065e78173f
550e8400-e29b-41d4-a716-446655440000	26	2	2026	Abdumannon	Fetched sale by ID: 4129506c-38e2-47ae-87d4-85f30cc50464	a85905c9-76c6-413a-a7da-928168c98fb8
Abdumannon	26	2	2026	0000	Fetched products for shop	e6512033-8d29-40a9-98ff-362607a53fff
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	9b65f55d-3809-49c4-8ef4-d1a0e5f5f6f1
Abdumannon	26	2	2026	0000	Fetched products for shop	ac7c00c3-603c-43d7-8e2d-0011b6c88953
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	dff68374-d1f0-4b02-b857-7f50c55e596e
\N	26	2	2026	Abdumannon	Fetched all wagons	eaa6a502-b808-45ae-ab2b-42ea5ee735c2
\N	26	2	2026	Abdumannon	Fetched all wagons	457b82a1-4d65-44a2-8a05-d0d856a6a905
\N	26	2	2026	Abdumannon	Fetched all wagons	80ef369e-d1de-4ee4-9eb7-d704a71fd34a
\N	26	2	2026	Abdumannon	Fetched all wagons	b0c46112-20b2-45d0-9ed5-4453ee86046e
\N	26	2	2026	Abdumannon	Fetched all wagons	5c2c8175-6583-43ac-99a8-6018ef59c6e3
\N	26	2	2026	Abdumannon	Fetched all wagons	233e6261-4f05-4da6-b192-6ad4cbda4930
\N	26	2	2026	Abdumannon	Fetched all wagons	727d49ec-c0ba-4ca2-ac5e-a25c6f9a9ba4
Abdumannon	26	2	2026	0000	Fetched products for shop	60da4f2a-b4b6-4f91-986b-e19fddb89ae8
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	baeee774-1054-4dab-8dfb-6d76210134ce
Abdumannon	26	2	2026	0000	Fetched products for shop	30e85bbd-3817-4df8-b6ca-ca2e316baafc
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	0d6aa723-170f-40ab-a688-6d67df2026c1
Abdumannon	26	2	2026	0000	Fetched products for shop	f77af4c8-79c9-4e05-b795-79c79b781e7b
Abdumannon	26	2	2026	0000	Fetched products for shop	493f43a2-f02f-4a6d-a254-a72daff44bfb
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	17a3fe0c-dadc-4eed-8dd0-816529dc340b
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	027fb42e-9aca-4440-8b9e-41204300e969
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	209c7322-d0d3-4eb0-9963-115aa996ccc3
Abdumannon	26	2	2026	0000	Fetched debt statistics	76167ebe-9dc6-445c-b425-85e6a610569c
Abdumannon	26	2	2026	0000	Fetched all debts - count: 4	32f4b945-f241-4ef0-9071-37cc41647446
Abdumannon	26	2	2026	0000	Fetched debt statistics	e2ba8038-cc7c-47fd-867e-8e028aea8b15
Abdumannon	26	2	2026	0000	Debt created successfully - customer: dfgd, amount: 300	2f018541-df60-4cf1-971f-dfdfe34c15dd
Abdumannon	26	2	2026	0000	Fetched debt statistics	7da6bf3c-0d9a-42fe-b090-e5ad2c603fd0
Abdumannon	26	2	2026	0000	Debt created successfully - customer: Qobil, amount: 3075	f5c9f562-8ed0-4778-b6fb-3ff9372cd44d
Abdumannon	26	2	2026	0000	Fetched debt statistics	5c3cf6ca-7cf7-44e8-969b-78625d338526
Abdumannon	26	2	2026	\N	Fetched debt by ID: b0ce10da-a0ad-4855-8433-42c34c402686	4e82b649-a862-41ea-a985-5e5eaf60f959
Abdumannon	26	2	2026	\N	Fetched debt by ID: 57a26376-8aa4-404b-85a0-b8421e7ed5b3	3a195b78-3cd2-44f1-815e-027ebef72c86
Abdumannon	26	2	2026	\N	Debt updated successfully: 57a26376-8aa4-404b-85a0-b8421e7ed5b3	00daa868-eab5-45be-9882-230fbdccdca6
Abdumannon	26	2	2026	0000	Fetched debt statistics	7c387a9e-06d4-4c0c-b16c-d1ef308fafd3
Abdumannon	26	2	2026	\N	Fetched debt by ID: 57a26376-8aa4-404b-85a0-b8421e7ed5b3	eafeec15-619b-429b-bc9c-878324d23d94
Abdumannon	26	2	2026	0000	Fetched products for shop	13570f0b-04ed-4bd9-b321-08af1497b3d8
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	c17e68ed-faba-4273-bf67-22bae153775b
Abdumannon	26	2	2026	0000	Fetched products for shop	887accce-f601-471f-a91f-5a53c867be39
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	f4e8324e-5e61-4a87-9a2c-2f3ba5d4e1d9
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	691dec58-bd86-47b5-a6eb-58c40760c20e
Abdumannon	26	2	2026	0000	Fetched debt statistics	a278906a-0780-420c-b9e4-944568c3fd22
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	dc208de7-c338-4850-a61d-e14e79ce3d58
Abdumannon	26	2	2026	0000	Fetched debt statistics	2dca67bd-0e14-4933-8f55-6b37c196c4f3
Abdumannon	26	2	2026	0000	Fetched products for shop	4516dbba-07e1-43fd-bdf7-30e6df1c703f
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	e24fa696-4504-4bb6-a30b-6845a3050c64
Abdumannon	26	2	2026	0000	Fetched products for shop	be21bca9-6e3e-4292-9a0b-454481b65e5f
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	2e73df42-b094-432f-98fb-dfdd53b0cfc1
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	42df5829-05b1-4041-bacd-b0600f8a5894
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	76dbcb39-4670-4c57-8726-61622ca3df65
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	613b77b1-4ea8-4668-a707-e6e538864906
Abdumannon	26	2	2026	0000	Fetched debt statistics	1cdb613b-d4d9-45ac-aa33-625071d45f97
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	92fc39ec-c991-41ed-a2df-63925dfdd5cd
Abdumannon	26	2	2026	0000	Fetched debt statistics	dce2948d-408a-4aa1-9ef1-17763071f443
\N	26	2	2026	Abdumannon	Fetched all wagons	bf83cbe0-cf9f-488c-9dd0-5d7fb681c5aa
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	6cd164fd-4a2b-499a-9f09-e1e8fec1011d
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	433b3c46-b956-4cd3-ad55-1c5f8f62f2a2
\N	26	2	2026	Abdumannon	Fetched all wagons	25031593-307d-4753-8fcb-73c763387075
\N	26	2	2026	Abdumannon	Fetched all wagons	5e37bf4e-dfc1-4be0-bb94-c2d8d5747eed
\N	26	2	2026	Abdumannon	Fetched all wagons	d37a30b2-e11e-4f7b-ab46-25bfa87b6763
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	4daf2103-72f7-41b4-b17b-310c3e33f47e
Abdumannon	26	2	2026	0000	Fetched debt statistics	a6103c96-677f-4e70-8118-868132a43e04
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	c4494cd9-ee75-41fa-8678-132f9ca5b5bd
Abdumannon	26	2	2026	0000	Fetched debt statistics	8cf7d383-4958-4626-a289-a8b207044ac3
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	c3c0cd13-caaa-4530-99ae-770d7ff137fb
Abdumannon	26	2	2026	0000	Fetched debt statistics	cb9f9c58-7043-4362-a26b-6ae9a713e1c2
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	bf788424-663d-49dc-9def-f511f22aa03d
Abdumannon	26	2	2026	0000	Fetched debt statistics	f85159a5-254f-40c0-acc4-a31f8a55029b
Abdumannon	26	2	2026	0000	Fetched products for shop	19bfe764-4793-459f-95bd-391cf5fa50ab
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	e80b3ac9-f92d-4424-8594-8c731958a6fc
Abdumannon	26	2	2026	0000	Fetched products for shop	b11fa534-8c50-41ed-bee0-f885594da5c2
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	1fc40b5f-bb8d-4093-ab89-a7fb381c81f2
\N	26	2	2026	Abdumannon	Fetched all wagons	b90909b2-ded8-4b5a-8b16-e047abdbd5f2
\N	26	2	2026	Abdumannon	Fetched all wagons	89ce1e5c-5ec4-4ea7-a906-9768ecd3b632
\N	26	2	2026	Abdumannon	Created wagon: Nima,dffdf	02c7377d-bc0e-40f7-ad6d-1360213ed992
\N	26	2	2026	Abdumannon	Fetched all wagons	6af1f2b1-f79c-46fe-a1fd-4a2be69d3b1c
\N	26	2	2026	Abdumannon	Fetched all wagons	7e9a82b4-de6f-4c88-8f55-491030544398
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	ad3ca08a-5078-48a7-8c56-cc69e7beee48
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	5ec36ba5-7fb9-4cfb-9cd6-28e1f555f40c
\N	26	2	2026	Abdumannon	Fetched all wagons	e9a2b9d9-5a9d-4ddd-8047-fe17aaaa3de0
\N	26	2	2026	Abdumannon	Fetched all wagons	fedb74e2-a53c-4ceb-8d63-c51c54aec6d7
\N	26	2	2026	Abdumannon	Fetched all wagons	2fe15acd-93ed-4954-80f9-20e3bdaac39e
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	0b027442-b1e7-4aa5-aeab-d8fde9e44b85
Abdumannon	26	2	2026	0000	Fetched debt statistics	4de15b7c-a96d-4992-b59c-599c15e96aa2
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	ca0b3302-871b-404a-a678-935ca00c103b
Abdumannon	26	2	2026	0000	Fetched debt statistics	c9389222-3167-44a5-8269-c0bcaa73493b
Abdumannon	26	2	2026	0000	Fetched products for shop	afc5f418-6c1e-4fd5-85fc-3479b56bf508
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	0eee8964-08f6-444f-85f0-30a7f2ce9fe9
Abdumannon	26	2	2026	0000	Fetched products for shop	35d7005f-5a9d-4939-b667-808378177d9a
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	073eb956-955a-4c60-bbcb-127dc81a72f4
Abdumannon	26	2	2026	0000	Super logged in	5c9e123d-5d28-4a84-985d-fd1928b03488
Abdumannon	26	2	2026	0000	Fetched products for shop	0a52ae07-7dd1-4a1a-b40b-d9646cd2e009
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	3246906f-eb55-4d04-965d-fb0bcc58af7d
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	b978e111-421d-499e-a159-d258b6e57bce
Abdumannon	26	2	2026	0000	Fetched products for shop	d37048d1-24bd-419a-b749-239d3c98fdee
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	bc40441a-758d-4104-98a3-8293a81d511d
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	3256c60d-e4e9-4276-83ff-326fb5d3af0d
Abdumannon	26	2	2026	0000	Fetched debt statistics	c73ae466-86e0-4e81-88af-06aa47900317
\N	26	2	2026	Abdumannon	Fetched all wagons	436a582d-0aec-4e89-bbde-d8b6d84d0c19
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	5e610621-6d88-4b44-babf-187ac5cf8e0b
Abdumannon	26	2	2026	0000	Fetched debt statistics	6ee688c8-c317-4a8b-8395-11c6d6eaf776
\N	26	2	2026	Abdumannon	Fetched all wagons	645758ce-6393-4668-a1d8-6fd03b0b4360
\N	26	2	2026	Abdumannon	Fetched all wagons	170563ad-28ac-477c-aa96-1624e1a187e4
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	a73dab47-880e-4876-8b69-dbeea7827493
\N	26	2	2026	Abdumannon	Fetched all wagons	b7c6b014-a3d5-47ce-9e91-26a7c2123fed
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	faf3337d-6c2f-41d7-9abe-7abef607125a
Abdumannon	26	2	2026	0000	Fetched debt statistics	d36c998c-3c2f-45cb-9cfa-c0cddfa44889
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	ee5d3954-79f8-4a90-8668-0e8ede38e390
Abdumannon	26	2	2026	0000	Fetched debt statistics	63492b3d-a7fe-42d8-92f7-44ff75812a86
Abdumannon	26	2	2026	0000	Fetched products for shop	d6fb9d6b-6f82-4c68-bd31-3a552883c949
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	351758ee-ad37-4adc-9808-1b0149b9289f
Abdumannon	26	2	2026	0000	Fetched products for shop	2d969cee-be99-46b5-8a9d-cf924014ff07
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	f3fd26bf-56bc-4720-8a41-5876b3133e49
Abdumannon	26	2	2026	0000	Fetched products for shop	adcfdd41-1128-4309-a797-43453e3e674d
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	398da3db-25e4-40b4-bb2f-91167124bd94
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	41a23c35-2f51-43f6-a267-31e499d7c38f
Abdumannon	26	2	2026	0000	Fetched products for shop	48d5a6f9-b3aa-4735-82fd-89c0a867beb7
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	7b1e8721-1f7e-4e04-b253-3e19ddadb176
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	7daac4a6-1384-4490-9d4f-5df525cc155c
Abdumannon	26	2	2026	0000	Fetched debt statistics	ba75eed7-933b-4944-beb0-a248f0a66c4e
\N	26	2	2026	Abdumannon	Fetched all wagons	d43e3cec-0e89-4121-9c51-eff509e5c0bb
\N	26	2	2026	Abdumannon	Fetched all wagons	d3f6d359-8ef1-4f50-b1ae-3e6976094af4
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	f07daed4-c735-40e5-951b-9de0fec51ca9
\N	26	2	2026	Abdumannon	Fetched all wagons	c73b1b11-1505-4e86-a070-ab84cd2f4ba3
\N	26	2	2026	Abdumannon	Fetched all wagons	ce935265-6ec7-486a-b29c-88bdd66a1ed3
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	1cff60b2-9203-46e2-b0e3-29b39ba1426c
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	989a07e0-2f20-4df1-8bf6-7760c7723f76
Abdumannon	26	2	2026	0000	Fetched debt statistics	ef0f17d7-3a6b-4876-b4eb-e84c4157d04c
Abdumannon	26	2	2026	0000	Fetched products for shop	a806b0e1-70a9-488a-bbee-115b0761ac31
Abdumannon	26	2	2026	\N	Fetched all categories - count: 1	68d31bca-e22f-44de-822d-576e14f99a1b
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	099a0ff2-cfe3-41c4-9cd3-e633d0cdd9fc
Abdumannon	26	2	2026	0000	Fetched products for shop	161e0c00-217b-469b-a232-f50e11930e41
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	10eb0f08-e2fd-4fce-8788-dc3b4d7ab417
Abdumannon	26	2	2026	Abdumannon	Fetched all sales	42a487ac-9f7b-4934-b75e-a5f84749e7bf
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	a666d4f0-d785-43f8-a200-17ce631f8912
Abdumannon	26	2	2026	0000	Fetched debt statistics	71989ede-bd0c-40cb-80e5-083bd460df40
\N	26	2	2026	Abdumannon	Fetched all wagons	7c0227a4-ef44-4d6a-8e3f-2e026741e690
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	508d0f66-1a43-43ae-9aad-b4ff170d4f13
Abdumannon	26	2	2026	0000	Fetched products for shop	86d4e31e-276e-43a9-96af-271aeb9fe069
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	927ae35a-584a-4760-8b0d-2dde41d60232
Abdumannon	26	2	2026	0000	Fetched products for shop	3e7aa480-38a9-454d-ac17-6bba572b9524
Abdumannon	26	2	2026	\N	Fetched all brands - count: 3	89c665ff-a517-4e0d-821e-e96ad7a5d485
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	8e8ad922-d2fd-4014-9ff3-77bca01beeb7
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	482b5466-60bb-45fc-9af8-7aca6e54edf9
\N	26	2	2026	Abdumannon	Fetched all wagons	1904f2cb-b3a4-4d17-8e76-435f5710735c
\N	26	2	2026	Abdumannon	Fetched all wagons	0fb7bc4e-fc44-432d-9b5d-25aca83e3364
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	11f1430d-5349-46b1-8fa4-c08f514840b7
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	7b20e6ae-1025-4a91-b07a-62961fe75047
Abdumannon	26	2	2026	0000	Fetched debt statistics	39c9f697-e10b-40fe-983d-04a25cbfce7f
Abdumannon	26	2	2026	0000	Fetched debt statistics	94a9d2e9-c6a1-40b2-b758-bbc634b5b0bf
\N	26	2	2026	Abdumannon	Fetched all wagons	8a0f04f7-9f1f-4800-b28c-cb52a22ec52c
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	e42c8f4a-bd61-4105-9ddd-744413a52590
Abdumannon	26	2	2026	0000	Fetched all debts - count: 6	63ce7875-8148-448d-81b1-e73aba8df155
\N	26	2	2026	Abdumannon	Fetched all wagons	ae31193a-6a21-41da-b662-a8bc6f354f2d
\.


--
-- Data for Name: sales; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.sales (id, sale_id, admin_number, admin_name, total_price, total_net_price, profit, sale_time, sale_day, sales_month, sales_year, createdat, updatedat, branch, shop_id, payment_method) FROM stdin;
135	4129506c-38e2-47ae-87d4-85f30cc50464	+998974088108	Abdumannon	10000	1000	10000	2026-01-18 20:15:45.165	19	1	2026	2026-01-19 01:15:45.181291	2026-01-19 01:15:45.181291	100	0000	Naqd
1	8f6f1748-4845-493a-b416-8f85c175a911	+998974088108	Muzaffar	35806	488	35318	2025-12-10 14:59:52.382	11	12	2025	2025-12-10 19:59:53.433	2025-12-10 19:59:53.433	0	\N	card
2	83698911-52f1-49f3-85be-928d30f6c70b	+998974088108	Muzaffar	35806	488	35318	2025-12-10 15:14:25.004	11	12	2025	2025-12-10 20:14:26.29	2025-12-10 20:14:26.29	0	\N	cash
3	327e54c0-ba77-4792-a6bb-36b206a628f7	+998974088108	Muzaffar	35806	488	35318	2025-12-10 15:44:53.325	11	12	2025	2025-12-10 20:44:54.637	2025-12-10 20:44:54.637	0	\N	card
4	c27382d8-453f-49bc-ba7a-a5f6a2047fd9	+998974088108	Muzaffar	352	34	318	2025-12-10 15:45:43.233	11	12	2025	2025-12-10 20:45:44.4	2025-12-10 20:45:44.4	0	\N	cash
5	ab286359-e009-4e00-a864-3e3eb9d0d6fa	+998974088108	Muzaffar	35454	454	35000	2025-12-10 15:46:04.073	11	12	2025	2025-12-10 20:46:05.289	2025-12-10 20:46:05.289	0	\N	mobile_pay
6	f59e30f0-82d1-44fc-9866-47e6cf720657	+998974088108	Muzaffar	352	34	318	2025-12-11 02:04:05.853	11	12	2025	2025-12-11 07:04:13.402	2025-12-11 07:04:13.402	0	\N	card
7	1194592b-c914-4bb0-be3f-f33326f2711f	+998974088108	Muzaffar	352	34	318	2025-12-11 02:04:11.519	11	12	2025	2025-12-11 07:04:14.132	2025-12-11 07:04:14.132	0	\N	mobile_pay
8	cfb4e91c-e58a-4770-bf5f-e75e9b33fb62	+998974088108	Muzaffar	352	34	318	2025-12-11 02:04:11.14	11	12	2025	2025-12-11 07:04:14.417	2025-12-11 07:04:14.417	0	\N	cash
9	857b7fad-ba08-48b6-bdfb-b4f4f28a36e5	+998974088108	Muzaffar	352	34	318	2025-12-11 02:04:13.255	11	12	2025	2025-12-11 07:04:14.419	2025-12-11 07:04:14.419	0	\N	cash
\.


--
-- Data for Name: shopname; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.shopname (id, name, superadmin, location) FROM stdin;
0000	Oqil boboda	550e8400-e29b-41d4-a716-446655440000	gus
\.


--
-- Data for Name: soldproduct; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.soldproduct (id, product_name, amount, net_price, sell_price, productid, salesid, shop_id) FROM stdin;
85	fdghfh	5	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	73a21334-f928-4345-b5e8-6d006f1b97c8	0000
86	Qizil Olma	4	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	1b112df1-a9d3-4319-be30-888acf11ae26	0000
87	Qizil Olma	5	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	1ef0bd11-30d2-46f8-b0e6-7c0c3ebe3e89	0000
88	Qizil Olma	10	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	399d091a-2afc-4133-aa76-197ac782f0c3	0000
89	Qizil Olma	2	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	f5e85c20-1282-43bf-a360-3ab2241a592e	0000
90	sifhbshu545	1	2516125	651	2b40b1b1-5532-422b-80f5-335513df1286	f5e85c20-1282-43bf-a360-3ab2241a592e	0000
91	sdfssdfsd	1	10000	11999.99	7dc6d744-d616-4927-aa66-22ed8373b7a7	f5e85c20-1282-43bf-a360-3ab2241a592e	0000
92	student	10	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	9b769fa1-29bf-4ad5-9d7a-4b8b623848ac	0000
93	Qizil Olma	20	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	3c2e6db1-c6c6-4a15-b01e-d671e02df760	0000
94	student	6	6000	100	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	9661769d-b01a-4d58-bf5d-f67bf6fc2b8c	0000
95	student	15	6000	500	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	3dd24248-ff2d-4783-a0e6-7275792c3f1e	0000
96	Qizil Olma	15	15000	250000	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	6ebe20c2-2c04-448c-ad28-55a749f69536	0000
105	Ruchka	15	1000	5000	117b6de9-167e-4092-8f77-43ed091bea0a	4a9d4b29-9ed3-4c77-abf5-e5d11c4d685b	0000
138	non	1	15	20	c1fa21a0-8889-4b05-8b80-da647ed38001	d1be09e3-e1ba-40c2-9b54-835d8beeb70b	0000
139	student	1	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	d1be09e3-e1ba-40c2-9b54-835d8beeb70b	0000
140	Qizil Olma	3	15000	82	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	ee2ace91-ecaf-4c4b-b892-2b720ad4ce5a	0000
141	Ruchka	14	1000	2031	117b6de9-167e-4092-8f77-43ed091bea0a	a9a36cf7-1091-44da-b1c9-55c0185058b7	0000
142	non	3	15	20	c1fa21a0-8889-4b05-8b80-da647ed38001	6987b7b2-531f-4164-aa77-45e74833cb06	0000
143	non	3	15	20	c1fa21a0-8889-4b05-8b80-da647ed38001	ecdc6bf0-73ab-4814-a51b-90d5a6e0e286	0000
144	Qizil Olma	2	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	8415afdb-5ee4-4417-a1a6-7f2d8edd8f16	0000
145	student	153	6000	10000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	44c0ce3f-b1c8-4fec-a702-b75396712a57	0000
146	student	7	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	ec616336-618d-496d-b7ae-b3feabd9ebd2	0000
147	student	4	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	6b270c55-fc71-4684-9492-591bd2cf748d	0000
148	student	5	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	9313157e-9bd2-48af-b160-a9a3d1e33473	0000
149	student	4	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	127d8faa-9bff-49a2-9f79-19efb09fc322	0000
150	student	3	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	232192b8-27fe-49d0-a456-6b3ae2e0de25	0000
151	student	4	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	ad664556-2909-439d-a69a-d4edc1ea1d4c	0000
153	student	15	6000	2370	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	731d3045-bc6a-4689-bf64-4856448ca4d2	0000
157	student	2	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	0674804c-59db-45b1-94c7-af7d34941f9d	0000
158	sdfssdfsd	1	10000	11999.99	7dc6d744-d616-4927-aa66-22ed8373b7a7	7f5e871b-c9df-479f-be45-5af4d5a82c88	0000
159	Lazer guruch*KG	1	1000	10000	fd3fffe6-044c-4ee3-b815-471be7dd887f	4129506c-38e2-47ae-87d4-85f30cc50464	0000
97	Ruchka	1	1000	2000	117b6de9-167e-4092-8f77-43ed091bea0a	b78c4dbc-a3d5-4829-be79-414893413efe	0000
98	sifhbshu545	25	2516125	55000	2b40b1b1-5532-422b-80f5-335513df1286	8ed4739d-d27f-4d1a-862d-6a0946d5d316	0000
152	student	2	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	4ca34cad-19a9-489d-9c9e-11055a25102b	0000
154	student	1	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	c4c0ca68-f406-4b16-ab25-fb737e31a505	0000
155	Lazer guruch	250	1500	15000	fd3fffe6-044c-4ee3-b815-471be7dd887f	f3d924a2-f918-4c27-84a5-524573cacd9d	0000
156	student	1	6000	9000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	52ff0e1c-8563-400a-a26b-a27b15b15f5a	0000
1	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	8f6f1748-4845-493a-b416-8f85c175a911	\N
2	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	8f6f1748-4845-493a-b416-8f85c175a911	\N
3	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	83698911-52f1-49f3-85be-928d30f6c70b	\N
4	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	83698911-52f1-49f3-85be-928d30f6c70b	\N
5	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	327e54c0-ba77-4792-a6bb-36b206a628f7	\N
6	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	327e54c0-ba77-4792-a6bb-36b206a628f7	\N
7	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	c27382d8-453f-49bc-ba7a-a5f6a2047fd9	\N
8	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	ab286359-e009-4e00-a864-3e3eb9d0d6fa	\N
9	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	f59e30f0-82d1-44fc-9866-47e6cf720657	\N
10	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	1194592b-c914-4bb0-be3f-f33326f2711f	\N
11	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	cfb4e91c-e58a-4770-bf5f-e75e9b33fb62	\N
12	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	857b7fad-ba08-48b6-bdfb-b4f4f28a36e5	\N
13	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	da339bba-194a-41a5-af13-f56b013ca35b	0000
14	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	da339bba-194a-41a5-af13-f56b013ca35b	0000
15	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	21957812-0c2f-4fe9-97f4-c41e0ae03e2d	0000
16	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	f7316cb2-0f72-48fc-8931-a3ec355b7a01	0000
17	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	f7316cb2-0f72-48fc-8931-a3ec355b7a01	0000
18	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	e00d63e5-b4b8-4aca-a27a-c2711ab122e6	0000
19	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	e00d63e5-b4b8-4aca-a27a-c2711ab122e6	0000
20	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	5975cb0a-3482-417b-b2b5-14df90b0ca05	0000
21	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	5975cb0a-3482-417b-b2b5-14df90b0ca05	0000
22	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	e36a8b06-8d00-4eb5-9d5f-6d4157af2bfa	0000
23	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	e36a8b06-8d00-4eb5-9d5f-6d4157af2bfa	0000
24	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	b8e6add6-e1bf-4cb9-8598-0f2b7d1374f9	0000
25	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	f3c0c46d-4b53-4dde-8fb9-4f052e41edbc	0000
26	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	f3c0c46d-4b53-4dde-8fb9-4f052e41edbc	0000
27	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	1363c8ad-64b8-4689-8269-0d884a885074	0000
28	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	1363c8ad-64b8-4689-8269-0d884a885074	0000
29	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	eab2e5f3-b929-4218-85c0-294ea25f2e8f	0000
30	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	eab2e5f3-b929-4218-85c0-294ea25f2e8f	0000
31	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	4b56bc9b-ab48-4596-bdeb-053c283bf030	0000
32	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	4b56bc9b-ab48-4596-bdeb-053c283bf030	0000
33	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	657e85cb-ff60-47c3-9597-7374c4a48e12	0000
34	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	1bb2bac3-3dde-4f53-a5b0-8e1b3861fd2c	0000
35	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	c0dd6072-006d-40a6-a4a6-6f7363edc670	0000
36	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	df62c835-ef45-4ef0-a6cf-adcd5308e0ab	0000
37	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	082ac668-07be-464e-b011-13ae49f0673b	0000
38	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	082ac668-07be-464e-b011-13ae49f0673b	0000
39	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	1597ec04-d461-44ca-87f5-64bd56877292	0000
40	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	1597ec04-d461-44ca-87f5-64bd56877292	0000
41	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	2d826d36-06b2-44da-b1c8-ff13e456f269	0000
42	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	c8178332-1b8e-4c18-8052-d71b231e923f	0000
43	fdgdfg	5	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	6701baaf-2d95-4059-b4be-85688b94ef0e	0000
44	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	6701baaf-2d95-4059-b4be-85688b94ef0e	0000
45	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	5846cf51-024e-402f-911b-3b136ef1db53	0000
46	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	d44dfc32-f7af-48e2-8941-2825fda09f38	0000
47	Qizil Olma	1	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	8878b4b4-07f3-49fe-aa62-caa30c8e6681	0000
48	Qizil Olma	1	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	3802513d-acd0-4225-8d39-cb3e6ca3b555	0000
49	fdghfh	7	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	3b73e0e6-0347-41e2-bfac-285785e66f6d	0000
50	Qizil Olma	4	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	9c005e83-33f1-49be-8125-51c800b3b256	0000
51	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	3e35a658-6812-433d-9258-28f2f564f85f	0000
52	Qizil Olma	1	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	301cf017-d0ee-4949-954d-4520774ddb72	0000
53	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	301cf017-d0ee-4949-954d-4520774ddb72	0000
54	sdfssdfsd	1	10000	11999.99	7dc6d744-d616-4927-aa66-22ed8373b7a7	3181a300-bda0-419d-af57-70941345f9b5	0000
55	Qizil Olma	1	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	3181a300-bda0-419d-af57-70941345f9b5	0000
56	Qizil Olma	10	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	4288378e-6020-4235-9a76-d18a4b56ecfb	0000
57	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	ef514f67-5538-47ba-9ede-7d2b891fe7f5	0000
58	sdfssdfsd	1	10000	11999.99	7dc6d744-d616-4927-aa66-22ed8373b7a7	24a705b8-af18-4d7d-ba31-34941ae61abc	0000
59	Qizil Olma	1	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	aad19139-4b8c-4dd8-b152-43063f46282c	0000
60	sifhbshu545	1	2516125	651	2b40b1b1-5532-422b-80f5-335513df1286	aad19139-4b8c-4dd8-b152-43063f46282c	0000
61	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	faeefa31-68a8-4ff0-ae03-c678565d49db	0000
62	sdfssdfsd	1	10000	11999.99	7dc6d744-d616-4927-aa66-22ed8373b7a7	faeefa31-68a8-4ff0-ae03-c678565d49db	0000
63	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	b240b112-9666-4c80-b612-26e055094d91	0000
64	sdfssdfsd	1	10000	11999.99	7dc6d744-d616-4927-aa66-22ed8373b7a7	b240b112-9666-4c80-b612-26e055094d91	0000
65	sifhbshu545	1	2516125	651	2b40b1b1-5532-422b-80f5-335513df1286	449fccaa-e68e-4fc7-a288-0da259760025	0000
66	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	dd9faf32-b915-4252-a250-e14c412a6d06	0000
67	Qizil Olma	1	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	bba120b3-8337-4326-9d56-39ed5523605d	0000
68	fdgdfg	1	34	352	f18ea1c0-14ba-4d44-be1e-376b743a62cc	df01c05a-1dbe-4fc4-b92c-352d5ba47017	0000
69	sifhbshu545	1	2516125	651	2b40b1b1-5532-422b-80f5-335513df1286	df01c05a-1dbe-4fc4-b92c-352d5ba47017	0000
70	sifhbshu545	1	2516125	651	2b40b1b1-5532-422b-80f5-335513df1286	44aef04f-6704-4a48-8232-46bb6c60fb6b	0000
71	sdfssdfsd	1	10000	11999.99	7dc6d744-d616-4927-aa66-22ed8373b7a7	44aef04f-6704-4a48-8232-46bb6c60fb6b	0000
72	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	44aef04f-6704-4a48-8232-46bb6c60fb6b	0000
73	sifhbshu545	2	2516125	651	2b40b1b1-5532-422b-80f5-335513df1286	c32bf7eb-1f9f-46e5-abff-1d63eccc0e8e	0000
74	sdfssdfsd	2	10000	11999.99	7dc6d744-d616-4927-aa66-22ed8373b7a7	4f895b3f-a048-4c23-8d27-f2d3614ec751	0000
75	fdghfh	4	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	c61a6ab2-849f-456e-874b-c6128fb82374	0000
76	sifhbshu545	1	2516125	651	2b40b1b1-5532-422b-80f5-335513df1286	38f74ef1-0593-4ed5-8043-178e76da5ec4	0000
77	Qizil Olma	3	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	17d3f1d4-4930-4bc6-8dc0-504faf222c1d	0000
78	Qizil Olma	4	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	4b18c951-a7e4-491f-94c4-c0657912b0de	0000
79	sdfssdfsd	1	10000	11999.99	7dc6d744-d616-4927-aa66-22ed8373b7a7	0ddf9c90-f21e-493f-b5db-e933712b78f2	0000
80	fdghfh	1	454	35454	da18ad8d-220d-45df-b208-df2d19cba6b9	0ddf9c90-f21e-493f-b5db-e933712b78f2	0000
81	sifhbshu545	1	2516125	651	2b40b1b1-5532-422b-80f5-335513df1286	e1fab004-8dec-472d-9541-6a59e3804ef9	0000
82	Qizil Olma	1	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	e1fab004-8dec-472d-9541-6a59e3804ef9	0000
83	non	1	15	20	c1fa21a0-8889-4b05-8b80-da647ed38001	e1fab004-8dec-472d-9541-6a59e3804ef9	0000
84	Qizil Olma	6	15000	18000.01	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	fcacfc45-4ffe-49e5-91f9-74d515a30501	0000
99	Qizil Olma	110	15000	15000	52b77d14-5bfb-428a-98ad-f4d2a6480fa5	1b9142c2-0b9f-4ca8-9b8e-7b42b978e1fd	0000
100	student	15	6000	95000	90efd7fc-1711-4e46-9a55-c7a40bbb41e5	41831bd7-76ae-4918-a12c-dd1d1894335c	0000
101	Ruchka	15	1000	5000	117b6de9-167e-4092-8f77-43ed091bea0a	528cef18-f13f-4f0e-b359-27992e1af384	0000
102	sifhbshu545	15	2516125	6551	2b40b1b1-5532-422b-80f5-335513df1286	2edabfdb-1608-4513-8709-aa082703786f	0000
103	sdfssdfsd	15	10000	15000	7dc6d744-d616-4927-aa66-22ed8373b7a7	adbc4bfe-bbfa-42f8-82e9-5be1217904dc	0000
104	Ruchka	15	1000	5000	117b6de9-167e-4092-8f77-43ed091bea0a	5faa2eda-98d7-483c-83f6-60d6c79a6838	0000
\.


--
-- Data for Name: superuser; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.superuser (id, uuid, name, lastname, email, phonenumber, isloggedin, password, refreshtoken, accesstoken, shopname, img_url, createdat, updatedat, shop_id) FROM stdin;
1	550e8400-e29b-41d4-a716-446655440000	Abdumannon	Abdumannon	abdumannon@gmail.com	+998974088108	t	123456789	\N	eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1dWlkIjoiNTUwZTg0MDAtZTI5Yi00MWQ0LWE3MTYtNDQ2NjU1NDQwMDAwIiwibmFtZSI6IkFiZHVtYW5ub24iLCJzaG9wX2lkIjoiMDAwMCIsInJvbGUiOiJzdXBlcnVzZXIiLCJpYXQiOjE3NzIxMDkxOTksImV4cCI6MTc3MjE5NTU5OX0.ZCo1BgR0CExzBkCLjujXTS6PKbx3fJeG_FZ1DfQBRXM	Oqil bobo	\N	2025-12-10 18:38:06.178	2025-12-10 18:38:06.178	0000
\.


--
-- Data for Name: wagons; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.wagons (id, wagon_number, products, total, indicator, shop_id, branch, created_at, updated_at, created_by) FROM stdin;
4217d269-8585-4803-8f32-5df865adb6c7	salom	[{"price": 10, "amount": 100, "subtotal": 1000, "product_id": "1", "paid_amount": 100, "product_name": "product"}]	1000.00	debt_taken	\N	\N	2026-01-12 19:00:49.266626	2026-01-12 19:00:49.266626	\N
f8cc0dc6-eab3-4b70-8613-f57ef92123ff	Vagon	[{"price": 100, "amount": 5, "subtotal": 500, "product_id": "1", "paid_amount": 400, "product_name": "d"}]	500.00	debt_given	\N	\N	2026-01-12 19:01:40.409117	2026-01-12 19:01:40.409117	\N
a971059e-9b02-4964-b077-4be55f048c33	Salom,23434	[{"price": 150, "amount": 150, "subtotal": 22500, "product_id": "1", "paid_amount": 0, "product_name": "Nono"}, {"price": 300, "amount": 200, "subtotal": 60000, "product_id": "2", "paid_amount": 0, "product_name": "nimadir"}]	82500.00	debt_taken	\N	\N	2026-02-26 13:52:37.929301	2026-02-26 14:33:04.189947	\N
ed6926d5-4555-47ff-841c-133559111389	Salom,1234	[{"price": 300, "amount": 200, "subtotal": 60000, "product_id": "2", "paid_amount": 0, "product_name": "Olma"}, {"price": 1500, "amount": 10, "subtotal": 15000, "product_id": "3", "paid_amount": 0, "product_name": "Non"}]	75000.00	none	\N	\N	2026-02-26 14:33:28.005004	2026-02-26 15:17:17.481848	\N
f5e42ce1-f54b-434b-892d-c63aaed384d4	Nima,dffdf	[{"price": 152, "amount": 233, "subtotal": 35416, "product_id": "1", "paid_amount": 0, "product_name": "sfs"}]	35416.00	none	\N	\N	2026-02-26 17:13:54.487008	2026-02-26 17:13:54.487008	\N
\.


--
-- Data for Name: weekstats; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.weekstats (id, month, net_sales, net_profit, createdat, updatedat, week_end, week_start) FROM stdin;
\.


--
-- Name: admin_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.admin_id_seq', 6, true);


--
-- Name: brand_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.brand_id_seq', 3, true);


--
-- Name: category_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.category_id_seq', 4, true);


--
-- Name: edu_centers_center_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.edu_centers_center_id_seq', 1, false);


--
-- Name: finance_records_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.finance_records_id_seq', 4, true);


--
-- Name: payments_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.payments_id_seq', 1, false);


--
-- Name: permission_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.permission_id_seq', 1, true);


--
-- Name: sale_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.sale_id_seq', 135, true);


--
-- Name: soldproduct_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.soldproduct_id_seq', 159, true);


--
-- Name: superuser_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.superuser_id_seq', 1, false);


--
-- Name: weekstats_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.weekstats_id_seq', 1, false);


--
-- Name: admin admin_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin
    ADD CONSTRAINT admin_pkey PRIMARY KEY (id);


--
-- Name: branches branches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.branches
    ADD CONSTRAINT branches_pkey PRIMARY KEY (id);


--
-- Name: brand brand_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.brand
    ADD CONSTRAINT brand_pkey PRIMARY KEY (id);


--
-- Name: category category_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.category
    ADD CONSTRAINT category_pkey PRIMARY KEY (id);


--
-- Name: debt_table debts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.debt_table
    ADD CONSTRAINT debts_pkey PRIMARY KEY (id);


--
-- Name: edu_centers edu_centers_center_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.edu_centers
    ADD CONSTRAINT edu_centers_center_code_key UNIQUE (center_code);


--
-- Name: edu_centers edu_centers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.edu_centers
    ADD CONSTRAINT edu_centers_pkey PRIMARY KEY (center_id);


--
-- Name: finance_records finance_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.finance_records
    ADD CONSTRAINT finance_records_pkey PRIMARY KEY (id);


--
-- Name: payments payments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payments
    ADD CONSTRAINT payments_pkey PRIMARY KEY (id);


--
-- Name: permission permission_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.permission
    ADD CONSTRAINT permission_pkey PRIMARY KEY (id);


--
-- Name: product product_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.product
    ADD CONSTRAINT product_pkey PRIMARY KEY (id);


--
-- Name: reports reports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reports
    ADD CONSTRAINT reports_pkey PRIMARY KEY (uuid);


--
-- Name: sales sale_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sales
    ADD CONSTRAINT sale_pkey PRIMARY KEY (id);


--
-- Name: shopname shopname_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shopname
    ADD CONSTRAINT shopname_pkey PRIMARY KEY (id);


--
-- Name: soldproduct soldproduct_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.soldproduct
    ADD CONSTRAINT soldproduct_pkey PRIMARY KEY (id);


--
-- Name: superuser superuser_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.superuser
    ADD CONSTRAINT superuser_pkey PRIMARY KEY (id);


--
-- Name: wagons unique_wagon_number; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wagons
    ADD CONSTRAINT unique_wagon_number UNIQUE (wagon_number);


--
-- Name: wagons wagons_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wagons
    ADD CONSTRAINT wagons_pkey PRIMARY KEY (id);


--
-- Name: weekstats weekstats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.weekstats
    ADD CONSTRAINT weekstats_pkey PRIMARY KEY (id);


--
-- Name: idx_center_code; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_center_code ON public.edu_centers USING btree (center_code);


--
-- Name: idx_finance_records_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_finance_records_created_at ON public.finance_records USING btree (created_at DESC);


--
-- Name: idx_payments_finance_record_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payments_finance_record_id ON public.payments USING btree (finance_record_id);


--
-- Name: idx_wagons_indicator; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wagons_indicator ON public.wagons USING btree (indicator);


--
-- Name: idx_wagons_shop_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wagons_shop_id ON public.wagons USING btree (shop_id);


--
-- Name: idx_wagons_wagon_number; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wagons_wagon_number ON public.wagons USING btree (wagon_number);


--
-- Name: wagons trigger_update_wagon_timestamp; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trigger_update_wagon_timestamp BEFORE UPDATE ON public.wagons FOR EACH ROW EXECUTE FUNCTION public.update_wagon_timestamp();


--
-- Name: payments payments_finance_record_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payments
    ADD CONSTRAINT payments_finance_record_id_fkey FOREIGN KEY (finance_record_id) REFERENCES public.finance_records(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict BB2WrxsohBdE62FraoJqHfWAvPxCwmk4xbqPlp9g0IukW7uJUCHuCA8EfPqJVm7

