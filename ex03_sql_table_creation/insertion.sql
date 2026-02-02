INSERT INTO dim_payment_type (payment_type_id, payment_name) VALUES
                                                                 (1, 'Credit card'), (2, 'Cash'), (3, 'No charge'),
                                                                 (4, 'Dispute'), (5, 'Unknown'), (6, 'Voided trip')
ON CONFLICT DO NOTHING;

INSERT INTO dim_rate_code (rate_code_id, rate_description) VALUES
                                                               (1, 'Standard rate'), (2, 'JFK'), (3, 'Newark'),
                                                               (4, 'Nassau or Westchester'), (5, 'Negotiated fare'), (6, 'Group ride'), (99, 'Unknown')
ON CONFLICT DO NOTHING;