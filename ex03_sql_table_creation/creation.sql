-- Nettoyage préalable (au cas où)
DROP TABLE IF EXISTS fact_trips CASCADE;
DROP TABLE IF EXISTS dim_rate_code CASCADE;
DROP TABLE IF EXISTS dim_payment_type CASCADE;

-- 1. Dimension : Type de paiement
CREATE TABLE dim_payment_type (
                                  payment_type_id INT PRIMARY KEY,
                                  payment_name VARCHAR(50)
);

-- 2. Dimension : Tarif (Rate Code)
CREATE TABLE dim_rate_code (
                               rate_code_id INT PRIMARY KEY,
                               rate_description VARCHAR(100)
);

-- 3. Table de Faits : Les courses (Fact Table)
-- Elle contient les mesures et les clés étrangères
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

    -- Clés étrangères (Relations)
                            CONSTRAINT fk_payment FOREIGN KEY (payment_type_id) REFERENCES dim_payment_type(payment_type_id),
                            CONSTRAINT fk_rate FOREIGN KEY (rate_code_id) REFERENCES dim_rate_code(rate_code_id)
);