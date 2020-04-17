ALB / Classic ELBのログをAthenaで集計するテーブルを作るスクリプト

# 使い方
## テーブル作成＆パーティショニング設定
```
python3 table.py \
  --albnames <ALBの名前1>=<ALBの略称1> <ALBの名前2=ALBの略称2> ... \
  --result s3://YOUR-ATHENA_RESULT_BUCKET_NAME/path/to/somewhere \
  --database elblogs # 省略可能 \
  --region ap-northeast-1 # 省略可能 \
  --year 2020 # 省略可能 \
  --create-unioned-view # 省略可能 \
  --go
```

で、指定した年のALBのログを検索するためのテーブルを作成し、さらにパーティショニングの設定を行い、最後にビューを作成する。

* 作成されるテーブル名は `ALBの略称_accesslogs_2020` のようになる
  * ハイフンはアンダースコアに置換される
  * 略称は省略可能
* `--year` は省略可（今年になる）
* `--go` を省略するとクエリだけを出力する（クエリ実行をスキップする）

## 例
```
python3 table.py \
  --albnames \
     my-hogehoge-product-alb=hoge \
     my-fugafuga-alb \
     my-piyopiyo-alb=piyo \
  --result s3://YOUR-ATHENA_RESULT_BUCKET_NAME/path/to/somewhere \
  --create-unioned-view
  --go
```

を実行すると、 `default` データベースに次の3つのテーブルが作成される。

| #  | ALBの名前                 | テーブル名               |
|----|---------------------------|-------------------------|
| 1  | my-hogehoge-product-alb   | hoge_2020               |
| 2  | my-fugafuga-alb           | my_fugafuga_alb_2020    |
| 3  | my-piyopiyo-alb           | piyo_2020               |

また、これらをUNIONした `alb_logs_2020` ビューが作成される。このビューで2番目・3番目のテーブルだけを検索したいときは

```sql
SELECT * FROM elb_access_logs_2020
WHERE
  lbname IN ('my_fugafuga_alb', 'piyo') AND
  month = 4 AND
  day = 17
```

とすればよい。
