-- auto genertaed
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-15', 0, 'Google', 275) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-15', 0, 'Amazon', 3) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-15', 0, 'Apple', 45) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-15', 0, 'Microsoft', 136) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
INSERT INTO pageview_counts(ds, hour, title, view_count) VALUES('2025-01-15', 0, 'Facebook', 278) ON CONFLICT (ds, hour, title) DO UPDATE SET view_count = EXCLUDED.view_count;
