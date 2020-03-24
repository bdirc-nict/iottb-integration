--
-- イベントのテーマ属性を時空間集約する
--

CREATE OR REPLACE FUNCTION analysis.aggregate_geometry_table (
  output_table text,
  output_mode text,
  event_table_name text,
  start_datetime timestamp with time zone,
  end_datetime timestamp with time zone,
  spatial_extent geometry,
  continuous_column_names text[],
  discrete_column_names text[]
)
RETURNS text
AS $$
DECLARE
  _output_table text;
  _output_table_rc regclass;
  _output_table_column_defs text;
  _output_table_columns text;
  _event_table_rc regclass = event_table_name::regclass;
  _event_table_schema_name text;
  _event_table_table_name text;
  _tmp_table_1 text;
  _tmp_table_1_rc regclass;
  _tmp_table_1_columns text;
  _tmp_table_2 text;
  _tmp_table_2_rc regclass;
  _tmp_table_3 text;
  _tmp_table_3_rc regclass;
  _tmp_table_3_columns text;
  _tmp_table_g text;
  _tmp_table_g_rc regclass;
  _tmp_table_k text;
  _tmp_table_k_rc regclass;
  _tmp_table_m text;
  _tmp_table_m_rc regclass;
  _tmp_table_t text;
  _tmp_table_t_rc regclass;
  _column_name text;
  _column_type text;
BEGIN

  -- 出力対象のテーブルを確認する
  _output_table := analysis.evwh_check_table(output_table, output_mode);

  -- 対象カラム名と型の対応をテーブルに格納する

  _tmp_table_t := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_t || ' (
      column_name text,
      column_type text
    )
    DISTRIBUTED RANDOMLY
  ';
  _tmp_table_t_rc := _tmp_table_t::regclass;

  _event_table_schema_name := analysis.evwh_get_schema_name(_event_table_rc::text);
  _event_table_table_name := analysis.evwh_trim_schema_name(_event_table_rc::text);

  FOR _column_name IN SELECT unnest(continuous_column_names)
  LOOP
    SELECT
      data_type
    FROM
      information_schema.columns
    WHERE
      table_schema = _event_table_schema_name
      AND table_name = _event_table_table_name
      AND column_name = _column_name
    LIMIT 1
    INTO _column_type;
    -- TODO 未対応の型の確認
    -- IF NOT _column_type IN ('integer') THEN
    --   RETURN; -- 未対応の連続値型
    -- END IF;
    EXECUTE '
      INSERT INTO ' || _tmp_table_t_rc || '
      VALUES (' || quote_literal(_column_name) || ', ' || quote_literal(_column_type) || ')
    ';
  END LOOP;

  FOR _column_name IN SELECT unnest(discrete_column_names)
  LOOP
    SELECT
      data_type
    FROM
      information_schema.columns
    WHERE
      table_schema = _event_table_schema_name
      AND table_name = _event_table_table_name
      AND column_name = _column_name
    LIMIT 1
    INTO _column_type;
    -- TODO 未対応の型の確認
    -- IF NOT _column_type IN ('integer', 'character varying', 'text') THEN
    --   RETURN; -- 未対応の連続値型
    -- END IF;
    EXECUTE '
      INSERT INTO ' || _tmp_table_t_rc || '
      VALUES (' || quote_literal(_column_name) || ', ' || quote_literal(_column_type) || ')
    ';
  END LOOP;

  -- 出力対象のテーブルが存在しなければ作成する

  IF NOT analysis.evwh_has_table(_output_table) THEN

    _output_table_column_defs := '';
    FOR _column_name IN SELECT unnest(continuous_column_names)
    LOOP
      EXECUTE '
        SELECT column_type FROM ' || _tmp_table_t_rc || ' WHERE column_name = ' || quote_literal(_column_name) || '
      ' INTO _column_type;
      _output_table_column_defs := _output_table_column_defs || ', min_' || _column_name || ' ' || _column_type;
      _output_table_column_defs := _output_table_column_defs || ', avg_' || _column_name || ' ' || _column_type;
      _output_table_column_defs := _output_table_column_defs || ', max_' || _column_name || ' ' || _column_type;
    END LOOP;
    FOR _column_name IN SELECT unnest(discrete_column_names)
    LOOP
      EXECUTE '
        SELECT column_type FROM ' || _tmp_table_t_rc || ' WHERE column_name = ' || quote_literal(_column_name) || '
      ' INTO _column_type;
      _output_table_column_defs := _output_table_column_defs || ', ' || _column_name || ' ' || _column_type || '[]';
    END LOOP;

    EXECUTE '
      CREATE TABLE ' || analysis.evwh_quote_ident(_output_table) || ' (
        start_datetime timestamp with time zone,
        end_datetime timestamp with time zone,
        location geometry,
        meshcode varchar
        ' || _output_table_column_defs || '
      )
      DISTRIBUTED RANDOMLY
    ';
  END IF;
  _output_table_rc := _output_table::regclass;

  -- 対象範囲をテーブル化する
  _tmp_table_g := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_g || '
    AS SELECT
      ST_SetSRID(ST_GeomFromText(''' || ST_AsText(spatial_extent) || '''), 4326) AS geom
    DISTRIBUTED RANDOMLY
  ';
  _tmp_table_g_rc := _tmp_table_g::regclass;

  -- イベントテーブルから対象範囲のレコードとカラムを抽出

  _tmp_table_1_columns := '';
  FOR _column_name IN SELECT unnest(continuous_column_names)
  LOOP
    _tmp_table_1_columns := _tmp_table_1_columns || ', e.' || _column_name;
  END LOOP;
  FOR _column_name IN SELECT unnest(discrete_column_names)
  LOOP
    _tmp_table_1_columns := _tmp_table_1_columns || ', e.' || _column_name;
  END LOOP;

  _tmp_table_1 := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_1 || '
    AS SELECT
      e.start_datetime,
      e.end_datetime,
      e.location
      ' || _tmp_table_1_columns || '
    FROM
      ' || _event_table_rc || ' e,
      ' || _tmp_table_g_rc || ' g
    WHERE
      e.start_datetime >= ''' || start_datetime || ''' AND e.start_datetime < ''' || end_datetime || '''
      AND ST_Intersects(e.location, g.geom)
    DISTRIBUTED RANDOMLY
  ';
  _tmp_table_1_rc := _tmp_table_1::regclass;

  -- メッシュコードテーブルから対象範囲のレコードを抽出
  _tmp_table_m := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_m || '
    AS SELECT
      m.*
    FROM
      analysis.jisx0410_mesh5 m,
      ' || _tmp_table_g_rc || ' g
    WHERE
      ST_Intersects(m.geom, g.geom)
    DISTRIBUTED RANDOMLY    
  ';
  _tmp_table_m_rc := _tmp_table_m::regclass;
  EXECUTE '
    CREATE INDEX idx_' || _tmp_table_m_rc || ' ON ' || _tmp_table_m_rc || ' USING gist (geom)
  ';

  -- 時空間ウィンドウによる分割
  -- 対象のカラムを抽出する (start, end, location は不要)
  _tmp_table_2 := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_2 || '
    AS SELECT DISTINCT
      date_trunc(''minute'', e.start_datetime)
        - extract(minute from e.start_datetime)::numeric % 5 * interval ''1 minute'' AS datetime,
      m.code AS meshcode,
      e.*
    FROM
      ' || _tmp_table_1_rc || ' e,
      ' || _tmp_table_m_rc || ' m
    WHERE
      ST_Intersects(e.location, m.geom)
    DISTRIBUTED RANDOMLY
  ';
  _tmp_table_2_rc := _tmp_table_2::regclass;

  -- キー項目を抽出
  _tmp_table_k := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_k || '
    AS SELECT DISTINCT
      e.datetime,
      e.meshcode
    FROM
      ' || _tmp_table_2_rc || ' e
    DISTRIBUTED RANDOMLY
  ';
  _tmp_table_k_rc := _tmp_table_k::regclass;

  -- 集約処理

  _tmp_table_3_columns := '';
  FOR _column_name IN SELECT unnest(continuous_column_names)
  LOOP
    EXECUTE '
      SELECT column_type FROM ' || _tmp_table_t_rc || ' WHERE column_name = ' || quote_literal(_column_name) || '
    ' INTO _column_type;
    _tmp_table_3_columns := _tmp_table_3_columns || ', min(e.' || _column_name || '::' || _column_type || ') AS min_' || _column_name;
    _tmp_table_3_columns := _tmp_table_3_columns || ', avg(e.' || _column_name || '::' || _column_type || ') AS avg_' || _column_name;
    _tmp_table_3_columns := _tmp_table_3_columns || ', max(e.' || _column_name || '::' || _column_type || ') AS max_' || _column_name;
  END LOOP;
  FOR _column_name IN SELECT unnest(discrete_column_names)
  LOOP
    EXECUTE '
      SELECT column_type FROM ' || _tmp_table_t_rc || ' WHERE column_name = ' || quote_literal(_column_name) || '
    ' INTO _column_type;
    _tmp_table_3_columns := _tmp_table_3_columns || ', array_agg(DISTINCT e.' || _column_name || '::' || _column_type || ') AS ' || _column_name;
  END LOOP;

  _tmp_table_3 := analysis.evwh_create_tmp_name();
  EXECUTE '
    CREATE TEMPORARY TABLE ' || _tmp_table_3 || '
    AS SELECT
      k.datetime,
      k.meshcode
      ' || _tmp_table_3_columns || '
    FROM
      ' || _tmp_table_k || ' k,
      ' || _tmp_table_2 || ' e
    WHERE
      k.datetime = e.datetime
      AND k.meshcode = e.meshcode
    GROUP BY
      k.datetime,
      k.meshcode
    DISTRIBUTED RANDOMLY
  ';
  _tmp_table_3_rc := _tmp_table_3::regclass;

  -- 時空間範囲を追加して出力テーブルに追加する

  _output_table_columns := '';
  FOR _column_name IN SELECT unnest(continuous_column_names)
  LOOP
    _output_table_columns := _output_table_columns || ', e.min_' || _column_name;
    _output_table_columns := _output_table_columns || ', e.avg_' || _column_name;
    _output_table_columns := _output_table_columns || ', e.max_' || _column_name;
  END LOOP;
  FOR _column_name IN SELECT unnest(discrete_column_names)
  LOOP
    _output_table_columns := _output_table_columns || ', e.' || _column_name;
  END LOOP;

  EXECUTE '
    INSERT INTO ' || _output_table_rc || '
    SELECT
      e.datetime AS start_datetime,
      e.datetime + 5 * interval ''1 minutes'' AS end_datetime,
      m.geom AS location,
      e.meshcode
      ' || _output_table_columns || '
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
