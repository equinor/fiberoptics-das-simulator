INSERT INTO fiber_optical_path(id, wellbore, switch_port, fiber_optical_path_info)
SELECT '25be8bc6-cd35-4c9f-9f8d-cabe43326163', 'Simulator box fiber path', 0, '{}'
    WHERE NOT EXISTS (SELECT id from fiber_optical_path WHERE id = '25be8bc6-cd35-4c9f-9f8d-cabe43326163');

-- vendor_code
INSERT INTO vendor(id, name, vendor_code, api_key)
SELECT '3a1e813f-a272-498c-8850-6188587369cd', 'Simulator vendor', 'Simulator', '1aa111a11aa11a0a1a1aa1111a1a1a1a'
    WHERE NOT EXISTS(SELECT id from vendor WHERE id = '3a1e813f-a272-498c-8850-6188587369cd');

-- instrument_box
INSERT INTO instrument_box(id, description, instrument_box_info, vendor_id)
SELECT '00528e45-06d0-4110-bba4-e904aaa02657', 'Simulator 1', '{}', '3a1e813f-a272-498c-8850-6188587369cd'
    WHERE NOT EXISTS(SELECT id from instrument_box WHERE id = '00528e45-06d0-4110-bba4-e904aaa02657');

-- instrument_box 2
INSERT INTO instrument_box(id, description, instrument_box_info, vendor_id)
SELECT '1deb5d57-2fb9-418a-990b-4cf7252a0450', 'Simulator 2', '{}', '3a1e813f-a272-498c-8850-6188587369cd'
WHERE NOT EXISTS(SELECT id from instrument_box WHERE id = '1deb5d57-2fb9-418a-990b-4cf7252a0450');

-- instrument_box 3
INSERT INTO instrument_box(id, description, instrument_box_info, vendor_id)
SELECT '1deb5d57-2fb9-418a-990b-4cf7252a0451', 'Simulator 3', '{}', '3a1e813f-a272-498c-8850-6188587369cd'
WHERE NOT EXISTS(SELECT id from instrument_box WHERE id = '1deb5d57-2fb9-418a-990b-4cf7252a0451');

-- instrument_box 4
INSERT INTO instrument_box(id, description, instrument_box_info, vendor_id)
SELECT '1deb5d57-2fb9-418a-990b-4cf7252a0452', 'Simulator 4', '{}', '3a1e813f-a272-498c-8850-6188587369cd'
WHERE NOT EXISTS(SELECT id from instrument_box WHERE id = '1deb5d57-2fb9-418a-990b-4cf7252a0452');

-- A test profile
INSERT INTO profile(id, description,
                    topic, pulse_rate,
                    pulse_width, pulse_width_unit, maximum_frequency,
                    optical_path_id, instrument_box_id,
                    spatial_sampling_interval, gauge_length,
                    measurement_start_time,
                    number_of_loci, start_locus_index,
                    vendor_code,
                    created_at, modified_at)
SELECT 'eceb17fc-355f-466e-b0a1-f1af59c3c51a', 'Canary test setup',
       '276172105-amp', 10000.0,
       100.5, 'ns',5000.0,
       '25be8bc6-cd35-4c9f-9f8d-cabe43326163', '00528e45-06d0-4110-bba4-e904aaa02657',
       100.1, 10.209524, 1584116799000000000,
       25, 2, 'Simulator',
       CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    WHERE NOT EXISTS(SELECT id from profile WHERE id = 'eceb17fc-355f-466e-b0a1-f1af59c3c51a');
