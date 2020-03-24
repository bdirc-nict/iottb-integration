SET optimizer=off;
SET enable_seqscan=off;

SELECT analysis.aggregate_raster_table (
  :output_table
  'overwrite',
  :event_table,
  :start_datetime,
  :end_datetime,
  (
    SELECT ST_Extent(geom)
    FROM analysis.jisx0410_mesh2
    WHERE code IN (
      '533912', '533913', '533914',
      '533902', '533903', '533904',
      '523972', '523973', '523974'
    )
    LIMIT 1
  ),
  'rain_map',
  1
);
