DROP SCHEMA public CASCADE;
CREATE SCHEMA public;


create table country
(
  country_code  text not null
    constraint country_pk
    primary key,
  incomelevel   text,
  name          text,
  full_name     text,
  region        text,
  oecd          text,
  eu            text,
  eu_sub        text,
  imf2003       text,
  type          text
  	
);

alter table country
  owner to postgres;

create unique index country_country_code_uindex
  on country (country_code);



create table field
(
  field_code  text not null
    constraint field_pk
    primary key,
  name        text,
  level       text,
  scopus_code text,
  scopus_name text,
  leg_name    text
);

alter table field
  owner to postgres;

create unique index field_field_code_uindex
  on field (field_code);


create table method
(
  method_code text not null
    constraint method_pk
    primary key,
  minmax     text,
  name text,
  full_name  text,
  short_desc text,
  input  text,
  description  text,
  formula    text,
  source     text
);

alter table method
  owner to postgres;

create unique index method_id_uindex
  on method (method_code);

create table interindex
(
  country_code text not null
    constraint index_country_country_code_fk
    references country,
  field_code   text
    constraint index_field_field_code_fk
    references field,
  method_code  text       not null
    constraint index_method_method_code_fk
    references method,
  period       integer    not null,
  value        double precision,
  id           serial    not null
    constraint index_pk
    primary key
);

alter table interindex
  owner to postgres;

create unique index interindex_id_uindex
  on interindex (id);











