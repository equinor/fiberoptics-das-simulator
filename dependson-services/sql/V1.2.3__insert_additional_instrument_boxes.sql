-- Additional instrument boxes (kept in a new migration to avoid checksum changes)
INSERT INTO instrument_box(id, description, instrument_box_info, vendor_id)
SELECT '6fc8a265-1ec5-4daf-b6b5-f7649af2a07a', 'Simulator 5', '{}', '3a1e813f-a272-498c-8850-6188587369cd'
WHERE NOT EXISTS(SELECT id from instrument_box WHERE id = '6fc8a265-1ec5-4daf-b6b5-f7649af2a07a');

INSERT INTO instrument_box(id, description, instrument_box_info, vendor_id)
SELECT '1deb5d57-3fb9-418a-990b-4cf7252a0451', 'Simulator 6', '{}', '3a1e813f-a272-498c-8850-6188587369cd'
WHERE NOT EXISTS(SELECT id from instrument_box WHERE id = '1deb5d57-3fb9-418a-990b-4cf7252a0451');

INSERT INTO instrument_box(id, description, instrument_box_info, vendor_id)
SELECT '1deb5d57-4fb9-418a-990b-4cf7252a0452', 'Simulator 7', '{}', '3a1e813f-a272-498c-8850-6188587369cd'
WHERE NOT EXISTS(SELECT id from instrument_box WHERE id = '1deb5d57-4fb9-418a-990b-4cf7252a0452');
