--
-- PostgreSQL database dump
--

-- Dumped from database version 14.17
-- Dumped by pg_dump version 14.17

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: dev_daily_prices; Type: TABLE DATA; Schema: public; Owner: postgres
--

INSERT INTO public.dev_daily_prices VALUES (1, 'AAPL', '2025-01-01', 185.5000, 187.2500, 184.1000, 186.7500, 45000000, '2025-08-23 14:47:33.552166');
INSERT INTO public.dev_daily_prices VALUES (2, 'AAPL', '2025-01-02', 186.8000, 188.9000, 185.3000, 187.4500, 52000000, '2025-08-23 14:47:33.552166');
INSERT INTO public.dev_daily_prices VALUES (3, 'MSFT', '2025-01-01', 425.2000, 428.5000, 423.8000, 427.1000, 28000000, '2025-08-23 14:47:33.552166');
INSERT INTO public.dev_daily_prices VALUES (4, 'MSFT', '2025-01-02', 427.3000, 429.7500, 425.9000, 428.6500, 31000000, '2025-08-23 14:47:33.552166');
INSERT INTO public.dev_daily_prices VALUES (5, 'TSLA', '2025-01-01', 245.8000, 248.2000, 243.5000, 246.9000, 75000000, '2025-08-23 14:47:33.552166');


--
-- Data for Name: dev_products; Type: TABLE DATA; Schema: public; Owner: postgres
--

INSERT INTO public.dev_products VALUES (1, 'Laptop Pro', 1299.99, 1, 'High-performance laptop', '2025-08-23 14:47:33.552166');
INSERT INTO public.dev_products VALUES (2, 'Wireless Mouse', 29.99, 2, 'Ergonomic wireless mouse', '2025-08-23 14:47:33.552166');
INSERT INTO public.dev_products VALUES (3, 'Mechanical Keyboard', 149.99, 2, 'RGB mechanical keyboard', '2025-08-23 14:47:33.552166');


--
-- Data for Name: dev_users; Type: TABLE DATA; Schema: public; Owner: postgres
--

INSERT INTO public.dev_users VALUES (1, 'john_doe', 'john@example.com', '2025-08-23 14:47:33.552166', '2025-08-23 14:47:33.552166');
INSERT INTO public.dev_users VALUES (2, 'jane_smith', 'jane@example.com', '2025-08-23 14:47:33.552166', '2025-08-23 14:47:33.552166');
INSERT INTO public.dev_users VALUES (3, 'bob_wilson', 'bob@example.com', '2025-08-23 14:47:33.552166', '2025-08-23 14:47:33.552166');


--
-- Data for Name: dev_orders; Type: TABLE DATA; Schema: public; Owner: postgres
--

INSERT INTO public.dev_orders VALUES (1, 1, 1, 1, 1299.99, '2025-08-23 14:47:33.552166', 'completed');
INSERT INTO public.dev_orders VALUES (2, 2, 2, 2, 59.98, '2025-08-23 14:47:33.552166', 'pending');
INSERT INTO public.dev_orders VALUES (3, 3, 3, 1, 149.99, '2025-08-23 14:47:33.552166', 'shipped');
INSERT INTO public.dev_orders VALUES (4, 1, 2, 1, 29.99, '2025-08-23 14:47:33.552166', 'completed');


--
-- Data for Name: dev_training_dataset; Type: TABLE DATA; Schema: public; Owner: postgres
--

INSERT INTO public.dev_training_dataset VALUES (1, 'aapl_2024_daily', 'AAPL', 15.60, 252, '2025-08-23 14:47:33.552166', 'active');
INSERT INTO public.dev_training_dataset VALUES (2, 'msft_2024_daily', 'MSFT', 14.80, 252, '2025-08-23 14:47:33.552166', 'active');
INSERT INTO public.dev_training_dataset VALUES (3, 'tsla_2024_daily', 'TSLA', 16.20, 252, '2025-08-23 14:47:33.552166', 'active');
INSERT INTO public.dev_training_dataset VALUES (4, 'combined_tech_stocks', NULL, 125.40, 2520, '2025-08-23 14:47:33.552166', 'active');


--
-- Name: dev_daily_prices_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.dev_daily_prices_id_seq', 5, true);


--
-- Name: dev_orders_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.dev_orders_id_seq', 4, true);


--
-- Name: dev_products_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.dev_products_id_seq', 3, true);


--
-- Name: dev_training_dataset_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.dev_training_dataset_id_seq', 4, true);


--
-- Name: dev_users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.dev_users_id_seq', 3, true);


--
-- PostgreSQL database dump complete
--

