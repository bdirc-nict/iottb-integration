# iottb-integration

## イベントテーブルの集約処理

イベントテーブルに格納された geometry 型または raster 型カラムの値を時空間メッシュ単位に集約します。

時空間メッシュの大きさは次のとおりとします。

- 時間方向: 5 分
- 空間方向: 5 次メッシュ

### ベクタ形式のイベントテーブルを時空間集約
`aggregate_eventtable/aggregate_geometry_table.sql`

以下のスキーマを持つテーブルに対して、各属性の値を時空間メッシュ単位に集約します。

|    カラム名    |            型            |
|----------------|--------------------------|
| start_datetime | timestamp with time zone |
| end_datetime   | timestamp with time zone |
| location       | geometry                 |
| *(any_name)*   | *(any_type)*             |
| ...            | ...                      |


関数の定義と引数の意味は以下のとおりです。

```
analysis.aggregate_geometry_table(
  output_table text,
  output_mode text,
  event_table_name text,
  start_datetime timestamp with time zone,
  end_datetime timestamp with time zone,
  spatial_extent geometry,
  continuous_column_names text[],
  discrete_column_names text[]
)
```

|         引数名          |           意味          |
|-------------------------|-------------------------|
| output_table            | 出力テーブル名          |
| output_mode             | 出力モード              |
| event_table_name        | 入力テーブル名          |
| start_datetime          | 開始日時                |
| end_datetime            | 終了日時                |
| spatial_extent          | 空間範囲                |
| continuous_column_names | 集約対象カラム (連続値) |
| discrete_column_names   | 集約対象カラム (離散値) |

- output_mode は出力テーブルが存在するときの処理を指定します。以下のいずれかを指定可能です
    - error: エラー
    - overwrite: 上書き
    - append: 追加
- continuous_column_names に指定されたカラムは、最小、平均、最大の 3 値が出力されます
- discrete_column_names に指定されたカラムは、配列型要素として列挙されます

### イベントのラスタ属性 (raster 型カラム) を時空間集約
`aggregate_eventtable/aggregate_raster_table.sql`

以下のスキーマを持つテーブルに対して、raster 型属性の値を時空間メッシュ単位に集約します。

|    カラム名    |            型            |
|----------------|--------------------------|
| start_datetime | timestamp with time zone |
| end_datetime   | timestamp with time zone |
| *(any_name)*   | raster                   |
| ...            | ...                      |

関数の定義と引数の意味は以下のとおりです。

```
analysis.aggregate_raster_table (
  output_table text,
  output_mode text,
  event_table_name text,
  start_datetime timestamp with time zone,
  end_datetime timestamp with time zone,
  spatial_extent geometry,
  raster_column_name text,
  raster_band integer
)
```

|       引数名       |           意味          |
|--------------------|-------------------------|
| output_table       | 出力テーブル名          |
| output_mode        | 出力モード              |
| event_table_name   | 入力テーブル名          |
| start_datetime     | 開始日時                |
| end_datetime       | 終了日時                |
| spatial_extent     | 空間範囲                |
| raster_column_name | 集約対象カラム (raster) |
| raster_band        | 集約対象バンド          |

- output_mode は出力テーブルが存在するときの処理を指定します。以下のいずれかを指定可能です
    - error: エラー
    - overwrite: 上書き
    - append: 追加

### イベントのラスタ属性をメッシュ化
`aggregate_eventtable/raster2mesh.sql`

以下のスキーマを持つテーブルに対して、raster 型属性の値をメッシュ時空間メッシュ単位に集約します。
aggregate_raster_table 内で利用される関数ですが、単独で呼び出すこともできます。

|    カラム名    |            型            |
|----------------|--------------------------|
| start_datetime | timestamp with time zone |
| end_datetime   | timestamp with time zone |
| *(any_name)*   | raster                   |
| ...            | ...                      |

関数の定義と引数の意味は以下のとおりです。

```
analysis.raster2mesh(rast Raster, band integer, extent Geometry)

returns setof (meshcode, value) of each pixels
  in the <Raster> of band <band>,
  which overlapps the <extent> (check by their box boundary).
```

## ベンチマーク

### イベントのラスタ属性を時空間集約
`benchmark/create_riskmap.sql`

raster 型のイベントテーブルを時空間集約して処理速度を計測します。

実行方法
```
$ time psql -f create_riskmap.sql -v output_table="'aggregated_tbl'" -v event_table="'rain'" -v start_datetime="'2019-09-01 00:00:00+09'" -v end_datetime="'2019-09-02 00:00:00+09'"
```
