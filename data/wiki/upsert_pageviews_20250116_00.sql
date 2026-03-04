-- auto genertaed
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-16', 0, 'Google', 289) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-16', 0, 'Amazon', 7) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-16', 0, 'Apple', 70) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-16', 0, 'Microsoft', 141) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-16', 0, 'Facebook', 253) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
