ALB / Classic ELBのログをAthenaで集計するテーブルを作るスクリプト

# 使い方
## ALB/ELBのテーブル作成＆パーティショニング設定
```
python3 alb.py \
  --albnames \
    <ALBの名前1>=<ALBの略称1> \
    <ALBの名前2=ALBの略称2> \
    ... \
  --result s3://YOUR-ATHENA_RESULT_BUCKET_NAME/path/to/somewhere \
  --database elblogs # 省略可能 \
  --region ap-northeast-1 # 省略可能 \
  --year 2020 # 省略可能 \
  --create-unioned-view # 省略可能 \
  --go
```

| #  | オプション     | 意味                                                                                | 必須 |
|----|---------------|-------------------------------------------------------------------------------------|------|
| 1  | `albnames`    | ALBの名前(ARNではない)と略称とを `=` で結んだものを渡す。略称は省略可能                  | ☑   |
| 2  | `result`      | Athenaのクエリ実行結果を出力するためのS3のパスを `s3://BUCKET-NAME/path` の形式で書く    | ☑  |
| 3  | `database`    | テーブル、ビューを作成するAthenaのデータベース（スキーマ）名。省略時は `default` になる   |      |
| 4  | `region`      | ALB, S3, Athenaのリージョン。省略時は `ap-northeast-1` になる                         |      |
| 5  | `year`        | どの年のログを集計対象とするかを示す。省略時は現在年になる                               |      |
| 6  | `create-unioned-view` | 作成したテーブルを全部UNIONで繋げたビューを作成する                             |      |
| 7  | `go`          | このオプションを明示的に与えたときにだけクエリを実行する。省略時はクエリ実行をすべてスキップする         |      |

* 作成されるテーブル名は `ALBの略称_accesslogs_2020` のようになる
  * ハイフンはアンダースコアに置換される
  * 略称に使える文字は英数字・ハイフン・アンダースコアのみ

## 例
```
python3 alb.py \
  --albnames \
     my-hogehoge-product-alb=hoge \
     my-fugafuga-alb \
     my-piyopiyo-alb=piyo \
  --result s3://YOUR-ATHENA_RESULT_BUCKET_NAME/path/to/somewhere \
  --year 2020
  --create-unioned-view
  --go
```

を実行すると、 `default` データベースに次の3つのテーブルが作成される。

| #  | ALBの名前                 | テーブル名               |
|----|---------------------------|-------------------------|
| 1  | my-hogehoge-product-alb   | hoge_2020               |
| 2  | my-fugafuga-alb           | my_fugafuga_alb_2020    |
| 3  | my-piyopiyo-alb           | piyo_2020               |

また、これらをUNIONした `alb_access_logs_2020` ビューが作成される。
このビューで2番目・3番目のテーブルだけを検索したいときは `lbname` にテーブル作成時に指定した略称を与えて

```sql
SELECT * FROM alb_access_logs_2020
WHERE
  lbname IN ('my_fugafuga_alb', 'piyo') AND
  month = 4 AND
  day = 17
```

とすればよい。
