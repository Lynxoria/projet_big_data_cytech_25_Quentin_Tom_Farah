

-- Type de Paiement
CREATE TABLE dim_payment_type
(
    payment_type_id INT PRIMARY KEY,
    payment_name    VARCHAR(50)
);

-- Codes Tarifs
CREATE TABLE dim_rate_code (
                               rate_code_id INT PRIMARY KEY,
                               rate_description VARCHAR(100)
);

-- Vendeurs
CREATE TABLE dim_vendor (
                            vendor_id INT PRIMARY KEY,
                            vendor_name VARCHAR(100)
);

-- ================================================================
-- 3. CRÉATION DE LA TABLE DES FAITS (COURSES)
-- ================================================================
CREATE TABLE fact_trips (
                            trip_id SERIAL PRIMARY KEY,
                            vendor_id INT,
                            tpep_pickup_datetime TIMESTAMP,
                            tpep_dropoff_datetime TIMESTAMP,
                            passenger_count INT,
                            trip_distance DOUBLE PRECISION,
                            rate_code_id INT,
                            store_and_fwd_flag VARCHAR(5),
                            pulocationid INT,
                            dolocationid INT,
                            payment_type_id INT,
                            fare_amount DOUBLE PRECISION,
                            extra DOUBLE PRECISION,
                            mta_tax DOUBLE PRECISION,
                            tip_amount DOUBLE PRECISION,
                            tolls_amount DOUBLE PRECISION,
                            improvement_surcharge DOUBLE PRECISION,
                            total_amount DOUBLE PRECISION,
                            congestion_surcharge DOUBLE PRECISION,
                            airport_fee DOUBLE PRECISION,

    -- Clés étrangères (Vérification stricte)
                            CONSTRAINT fk_payment FOREIGN KEY (payment_type_id) REFERENCES dim_payment_type(payment_type_id),
                            CONSTRAINT fk_rate FOREIGN KEY (rate_code_id) REFERENCES dim_rate_code(rate_code_id)
);