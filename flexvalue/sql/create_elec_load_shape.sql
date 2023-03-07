CREATE TABLE elec_load_shape (
    pk SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    state TEXT,
    utility TEXT,
    region TEXT,
    quarter INTEGER,
    month INTEGER,
    hour_of_day INTEGER,
    hour_of_year INTEGER,
    --hoy_util_st TEXT,
    load_shape_name TEXT,
    value FLOAT
);
