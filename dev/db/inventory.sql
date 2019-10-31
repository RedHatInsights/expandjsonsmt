CREATE TABLE public.hosts (
    id INT NOT NULL,
    account character varying(10),
    metadata jsonb,
    logs jsonb,
    tags character varying(200),
    PRIMARY KEY (id)
);

INSERT INTO public.hosts VALUES (1, '12234', '{"country":"USA"}', NULL, '[null, "tag1", "tag2"]');
INSERT INTO public.hosts VALUES (2, '34634', NULL, '{"debug":"printed"}', NULL);
INSERT INTO public.hosts VALUES (3, '34523', '{"address":{"city":"Studenec"}}', '{}', '[]');
INSERT INTO public.hosts VALUES (4, '23423', '{"address":{"city":"Studenec"}}', NULL, '[malformated]');
