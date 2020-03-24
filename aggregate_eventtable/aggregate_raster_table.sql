--
-- イベントのラスタ属性を時空間集約する
--

CREATE OR REPLACE FUNCTION analysis.aggregate_raster_table (
  output_table text,
  output_mode text,
  event_table_name text,
  start_datetime timestamp with time zone,
  end_datetime timestamp with time zone,
  spatial_extent geometry,
  raster_column_name text,
  raster_band integer
)
RETURNS text
AS $$
DECLARE
  _output_table text;
  _output_table_rc regclass;
  _event_table_rc regclass = event_table_name::regclass;
  _tmp_table_2 text;
  _tmp_table_2_rc regclass;
  _tmp_table_3 text;
  _tmp_table_3_rc regclass;
  _tmp_table_3_columns text;
  _tmp_table_m text;
  _tmp_table_m_rc regclass;
BEGIN

  -- 出力対象のテーブルが存在しなければ作成する
  _output_table := analysis.evwh_check_table(output_table, output_mode);
  IF NOT analysis.evwh_has_table(_output_table) THEN
    EXECUTE '
      CREATE TABLE ' || analysis.evwh_quote_ident(_output_table) || ' (
        start_datetime timestamp with time zone,
        end_datetime timestamp with time zone,
        location geometry,
        meshcode varchar,
        min_value double precision,
        avg_value double precision,
        max_value double precision
      )
      DISTRIBUTED RANDOMLY
    ';
  END IF;
  _output_table_rc := _output_table::regclass;

  -- 時空間ウィンドウによる分割
  _tmp_table_2 := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_2 || ' AS 
    SELECT dt_mesh_v.datetime, (dt_mesh_v.mesh_v).* FROM
      ( SELECT 
        date_trunc(''minute'', e.start_datetime)
        - extract(minute from e.start_datetime)::numeric % 5 * interval ''1 minute'' AS datetime,
      analysis.raster2mesh(
        ' || quote_ident(raster_column_name) || ',
        ' || raster_band || ',
        ST_SetSRID(ST_GeomFromText(''' || ST_AsText(spatial_extent) || '''), 4326)) AS mesh_v
      FROM
        ' || _event_table_rc || ' e
      WHERE
        e.start_datetime >= ''' || start_datetime || ''' AND e.start_datetime < ''' || end_datetime || '''
     ) dt_mesh_v
    DISTRIBUTED RANDOMLY
  ';
  _tmp_table_2_rc := _tmp_table_2::regclass;

  -- 集約処理
  _tmp_table_3 := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_3 || '
    AS SELECT
      e.datetime,
      e.meshcode,
      min(e.v) AS min_value,
      avg(e.v) AS avg_value,
      max(e.v) AS max_value
    FROM
      ' || _tmp_table_2 || ' e
    GROUP BY
      e.datetime,
      e.meshcode
    DISTRIBUTED RANDOMLY
  ';
  _tmp_table_3_rc := _tmp_table_3::regclass;

  -- メッシュコードテーブルから対象範囲のレコードを抽出
  _tmp_table_m := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_m || '
    AS SELECT
      m.*
    FROM
      analysis.jisx0410_mesh5 m
    WHERE
      ST_Intersects(m.geom, ST_SetSRID(ST_GeomFromText(''' || ST_AsText(spatial_extent) || '''), 4326))
    DISTRIBUTED RANDOMLY    
  ';
  _tmp_table_m_rc := _tmp_table_m::regclass;

  -- 時空間範囲を追加して出力テーブルに追加する
  EXECUTE '
    INSERT INTO ' || _output_table_rc || '
    SELECT
      e.datetime AS start_datetime,
      e.datetime + 5 * interval ''1 minutes'' AS end_datetime,
      m.geom AS location,
      e.meshcode,
      e.min_value,
      e.avg_value,
      e.max_value
    FROM
      ' || _tmp_table_3_rc || ' e,
      ' || _tmp_table_m_rc || ' m
    WHERE
      e.meshcode = m.code
  ';

  -- 出力テーブル名を返す
  RETURN _output_table;

END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION analysis.aggregate_raster_table(
  text, text, text, timestamp with time zone,
  timestamp with time zone, geometry, text, integer)
TO PUBLIC;
