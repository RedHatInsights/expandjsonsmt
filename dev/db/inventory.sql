CREATE TABLE public.hosts (
    id INT NOT NULL,
    account character varying(10),
    metadata jsonb,
    PRIMARY KEY (id)
);

INSERT INTO public.hosts VALUES (1, '12234', '{"country":"USA"}');
INSERT INTO public.hosts VALUES (2, '34634', '{"address":{"code": 555}, "a":1}');
INSERT INTO public.hosts VALUES (3, '34523', '{"country":"Czech Republic","address":{"city":"Studenec","code":123},"add":0}');
