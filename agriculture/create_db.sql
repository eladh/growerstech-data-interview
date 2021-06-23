CREATE TABLE fields (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    longitude FLOAT,
    latitude FLOAT,
);

CREATE TABLE climate (
    field_id INT NOT NULL,
    et FLOAT,
    rain FLOAT,
    tmin FLOAT,
    tmax FLOAT,
    date date,
    FOREIGN KEY (field_id) REFERENCES fields(id)
);

CREATE TABLE sensors (
    field_id INT NOT NULL,
    date DATE,
    time TIMESTAMP,
    value FLOAT,
    level INT,
    FOREIGN KEY (field_id) REFERENCES fields(id)
);

CREATE TABLE samples_metadata (
    research_id INT NOT NULL,
    trial_id INT NOT NULL,
    treatment_id INT NOT NULL,
    treatment_timing VARCHAR(120)
);

CREATE TABLE samples_dates (
    research_id INT NOT NULL,
    trial_id INT NOT NULL,
    treatment_id INT NOT NULL,
    treatment_date DATE
);

CREATE TABLE samples (
    field_id INT NOT NULL,
    trial FLOAT,
    repetition FLOAT,
    treatment_id INT,
    n_timing VARCHAR(120),
    grain_yield VARCHAR(120),
    total_biomass FLOAT,
    total_n_content VARCHAR(120),
    total_ndff FLOAT,
    root_ndff FLOAT,
    shoot_ndff FLOAT,
    grain_ndff FLOAT,
    total_soil_ndff FLOAT,
    clay FLOAT,
    silt FLOAT,
    sand FLOAT,
    precipitation FLOAT,
    oat VARCHAR(120),
    oat_planting_date DATE,
    corn_hybrids VARCHAR(120),
    corn_planting_date DATE,
    plant_population FLOAT,
    fertilizer_n_rate FLOAT,
    product_name VARCHAR(120),
    n_date_application DATE,
    second_n_date_application VARCHAR(120),
    ph FLOAT,
    soil_organic_matter_som FLOAT,
    p_resin_as_extractor FLOAT,
    k FLOAT,
    FOREIGN KEY (field_id) REFERENCES fields(id)
);