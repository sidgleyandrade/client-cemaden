CREATE TABLE IF NOT EXISTS %s.%s (
    station_id char(20) NOT NULL,
    name text NULL,
	datetime timestamp NOT NULL,
	date date NOT NULL,
	state char(2) NULL,
	city text NULL,
	measure numeric(24,20) NULL,
	offset_level numeric(24,20) NULL
);

SELECT AddGeometryColumn ('%s','%s','coordinates', 4326, 'POINT', 2)
WHERE NOT EXISTS (SELECT column_name FROM information_schema.columns
		  WHERE	table_schema = '%s' AND
		        table_name ='%s' AND
			    column_name = 'coordinates');

DO $$
    BEGIN
        BEGIN
            ALTER TABLE %s.%s ADD CONSTRAINT %s_pk PRIMARY KEY (station_id, coordinates, datetime);
        EXCEPTION
            WHEN others THEN NULL;
        END;
    END;
$$;

CREATE INDEX IF NOT EXISTS date_%s_ix ON %s.%s (date);
