-- CREATE TYPE RasterMeshValue AS (x integer, y integer, lat double precision, lng double precision, meshcode text, v real);
CREATE TYPE RasterMeshValue AS (meshcode text, v real);

--
-- analysis.raster2mesh(rast Raster, band integer, extent Geometry)
--
-- returns setof (meshcode, value) of each pixels
--   in the <Raster> of band <band>,
--   which overlapps the <extent> (check by their box boundary).
--
-- SET enable_seqscan=false;
-- SELECT analysis.raster2mesh(rain_map, 1, ST_SetSRID(ST_MakeBox2D(ST_Point(134.0,34.0),ST_Point(134.5,34.5)),4326)) FROM event.rain_xrain WHERE start_datetime='2019-07-29 12:00:00+09';
-- 
CREATE OR REPLACE FUNCTION analysis.raster2mesh(
	_rast raster,
	_band integer,
	_spatial_extent geometry
)
RETURNS SETOF RasterMeshValue AS $$
DECLARE
  _rast2 raster;
  _meta RECORD;
  _row  RECORD;
  _rmv  RasterMeshValue;
  _x0   double precision;
  _x1   double precision;
  _y0   double precision;
  _y1   double precision;
  _xmin integer;
  _xmax integer;
  _ymin integer;
  _ymax integer;
BEGIN

  SELECT (ST_Metadata(_rast)).* INTO _meta;

  -- Calculate extent borders
  _x0 := ST_XMin(_spatial_extent);
  _y0 := ST_YMin(_spatial_extent);
  _x1 := ST_XMax(_spatial_extent);
  _y1 := ST_YMax(_spatial_extent);
  -- RAISE NOTICE 'extent:(%,%)-(%,%)', _x0, _y0, _x1, _y1;

  IF _meta.scalex > 0.0 THEN
    _xmin := ((_x0 - _meta.upperleftx) / _meta.scalex)::integer;
    _xmax := ((_x1 - _meta.upperleftx) / _meta.scalex)::integer;
  ELSE
    _xmax := ((_x0 - _meta.upperleftx) / _meta.scalex)::integer;
    _xmin := ((_x1 - _meta.upperleftx) / _meta.scalex)::integer;
  END IF;

  IF _meta.scaley > 0.0 THEN
    _ymin := ((_y0 - _meta.upperlefty) / _meta.scaley)::integer;
    _ymax := ((_y1 - _meta.upperlefty) / _meta.scaley)::integer;
  ELSE
    _ymax := ((_y0 - _meta.upperlefty) / _meta.scaley)::integer;
    _ymin := ((_y1 - _meta.upperlefty) / _meta.scaley)::integer;
  END IF;
  -- RAISE NOTICE 'range:(%,%)-(%,%)', _xmin, _ymin, _xmax, _ymax;

  IF _xmax < 0 OR _xmin > _meta.width
    OR _ymax < 0 OR _ymin > _meta.height THEN
    RETURN;
  END IF;

  SELECT ST_Clip(_rast, _spatial_extent, true) INTO _rast2;
  SELECT (ST_Metadata(_rast2)).* INTO _meta;

  FOR _row IN SELECT
    xyv.x AS x,
    xyv.y AS y,
    (_meta.upperleftx + _meta.scalex * xyv.x + _meta.scalex / 2.0)::double precision AS lon,
    (_meta.upperlefty + _meta.scaley * xyv.y + _meta.scaley / 2.0)::double precision AS lat,
    xyv.val AS val
  FROM
    (
      SELECT (unnestXY(ST_DumpValues(_rast2, _band))).*
    ) xyv
  -- WHERE
  --   xyv.x >= _xmin AND xyv.x < _xmax AND xyv.y >= _ymin AND xyv.y < _ymax
  LOOP

    -- _rmv.x = _row.x;
    -- _rmv.y = _row.y;
    -- _rmv.lat = _row.lat;
    -- _rmv.lng = _row.lon;
    _rmv.meshcode := analysis.evwh_loc2mesh(_row.lat, _row.lon, 5);
    _rmv.v := _row.val;
    RETURN NEXT _rmv;

  END LOOP;

  RETURN;

END $$
LANGUAGE plpgsql STABLE;

GRANT EXECUTE ON FUNCTION analysis.raster2mesh(raster, integer, geometry)
TO PUBLIC;
