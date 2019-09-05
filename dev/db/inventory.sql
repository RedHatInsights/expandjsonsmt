CREATE TABLE public.hosts (
    id INT NOT NULL,
    account character varying(10),
    metadata jsonb,
    logs jsonb,
    PRIMARY KEY (id)
);

INSERT INTO public.hosts VALUES (1, '12234', '{"country":"USA"}', '{"info":"running"}');
INSERT INTO public.hosts VALUES (2, '34634', '{"address":{"code": 555}, "a":1}', '{"debug":"printed"}');
INSERT INTO public.hosts VALUES (3, '34523', '{"address":{"city":"Studenec"},"add":0}', '{"error":"null pointer"}');
