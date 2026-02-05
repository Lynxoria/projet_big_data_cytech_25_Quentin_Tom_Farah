-- Paiements (On ajoute le 0 ici !)
INSERT INTO dim_payment_type (payment_type_id, payment_name) VALUES
                                                                 (0, 'Unknown / System Error'),  -- <--- LE SAUVEUR
                                                                 (1, 'Credit card'),
                                                                 (2, 'Cash'),
                                                                 (3, 'No charge'),
                                                                 (4, 'Dispute'),
                                                                 (5, 'Unknown'),
                                                                 (6, 'Voided trip');

-- Tarifs (On ajoute 0 et 99 par sécurité)
INSERT INTO dim_rate_code (rate_code_id, rate_description) VALUES
                                                               (0, 'Unknown'),                 -- <--- SÉCURITÉ
                                                               (1, 'Standard rate'),
                                                               (2, 'JFK'),
                                                               (3, 'Newark'),
                                                               (4, 'Nassau or Westchester'),
                                                               (5, 'Negotiated fare'),
                                                               (6, 'Group ride'),
                                                               (99, 'Unknown');                -- <--- SÉCURITÉ

-- Vendeurs
INSERT INTO dim_vendor (vendor_id, vendor_name) VALUES
                                                    (1, 'Creative Mobile Technologies, LLC'),
                                                    (2, 'VeriFone Inc.');

-- Message de confirmation
SELECT 'Base prête. Le code 0 est bien présent.' as status;