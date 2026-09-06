# erlang-rdbms

Erlangで書いたリレーショナルデータベース。ページ単位のディスクストレージ、
バッファプール、カラムインデックス、トランザクション、REDOログによる
クラッシュリカバリを備えている。

OTPアプリケーション名は `transaction_db`。

## 動かす

```sh
cd erlang-rdbms
bin/sqlweb      # ブラウザUI  http://127.0.0.1:8080
bin/sqlsh       # 端末クライアント
```

初回は自動でビルドする。

必要なものは Erlang/OTP 25 以降。Debian/Ubuntu なら:

```sh
sudo apt install erlang-nox erlang-dev erlang-parsetools erlang-dialyzer
```

開発時のコマンド:

```sh
rebar3 compile
rebar3 eunit          # 365 tests
rebar3 dialyzer
rebar3 shell          # transaction_dbを起動した状態のErlangシェル
```

rebar3がない環境では同等のことがmakeでできる。

```sh
make        # ebin/ にビルド
make test   # EUnitを実行
make shell
```

### ブラウザUI

```sh
bin/sqlweb           # http://127.0.0.1:8080
bin/sqlweb 9000      # ポートを指定
```

左に表とカラム定義、上にSQL入力欄、下に実行結果が出る。`Ctrl+Enter` で実行、
複数文は `;` 区切りでまとめて流せる。`BEGIN` / `COMMIT` / `ROLLBACK` はボタンでも押せる。
いまトランザクションを開いているかが常に表示される。

外部依存は無い(OTP同梱の `inets` httpd を使う)。127.0.0.1 のみで待ち受ける。

接続はブラウザのセッションごとに1本持つ。**タブを2つ開いて片方で `BEGIN` した
ままにすると、もう片方のクエリが順番待ちで止まる。** 読み書きトランザクションが
直列に実行される様子がそのまま観察できる。`BEGIN READ ONLY` なら止まらない。

### SQLシェル

対話クライアントが付いている。

```sh
bin/sqlsh              # 対話シェルを開く
bin/sqlsh --tour       # 同梱のツアー(priv/tour.sql)を流す
bin/sqlsh FILE.sql     # ファイルの文を順に実行する
```

Erlangシェルからも起動できる。

```erlang
sql_shell:start().                      %% 対話
sql_shell:run_file("priv/tour.sql").    %% ファイル
sql_shell:start(#{data_dir => "/tmp/x"}).  %% データの置き場所を変える
```

```
transaction_db SQL shell
Type \? for help, \q to quit.  Statements end with ';'.

sql> CREATE TABLE emp (id INTEGER, name VARCHAR, active BOOLEAN);
OK
sql> \d emp
 column | type
--------+---------
 id     | INTEGER
 name   | VARCHAR
 active | BOOLEAN
sql> BEGIN;
OK
sql> INSERT INTO emp VALUES (1, 'ada', true);
OK (1 row inserted, oid={1788620056707150971,4})
sql> INSERT INTO emp (id) VALUES (3);
OK (1 row inserted, oid={1788620056707350428,8})
sql> SELECT * FROM emp;
 id | name | active
----+------+-------
 1  | ada  | true
 3  | NULL | NULL
(2 rows)
sql> COMMIT;
OK
```

メタコマンド:

| | |
| --- | --- |
| `\?` `\h` | ヘルプ |
| `\d` | テーブル一覧 |
| `\d NAME` | テーブル定義 |
| `\di` | インデックス一覧 |
| `\timing` | 実行時間の表示を切り替える |
| `\q` | 終了 |

文は `;` で区切る。空行でも溜まっている文を実行する。
データは `./data` に置かれる(消すには `rm -rf data`)。

`priv/tour.sql` は機能をひととおりなぞるスクリプトで、
成功する例だけでなく、型の不一致・未対応構文・トランザクション外のDMLなど
**エラーの出方も含めて**確認できるようにしてある。

### 使ってみる

```erlang
%% サーバ群を起動する
{ok, _} = sup:start_link(),

%% 接続を作る。1接続が1トランザクションを実行する
{ok, C} = gen_connection:connect(),

Q = fun(Sql) -> query_exec:exec_query(C, {sql, Sql}) end,

ok = Q("CREATE TABLE fruit (name VARCHAR, price INTEGER)"),
ok = Q("BEGIN"),
{ok, _}  = Q("INSERT INTO fruit VALUES ('apple', 100)"),
{ok, [[<<"apple">>, 100]]} = Q("SELECT * FROM fruit WHERE name = 'apple'"),
{ok, 1}  = Q("UPDATE fruit SET price = 120 WHERE name = 'apple'"),
ok = Q("COMMIT"),

ok = gen_connection:disconnect(C).
```

同梱のサンプル:

| モジュール | 内容 |
| --- | --- |
| `client_1:exec()` | ストレージエンジンを直接叩く例(トランザクションなし) |
| `client_query_exec:exec()` | トランザクションつきの一通りの操作 |
| `client_perf:exec(N)` | INSERT/SELECT/UPDATE/DELETEのスループット測定 |
| `client_query_tx_perf:exec(N)` | N個の接続が同じ行を並行に加算し、取りこぼしがないことを確認 |

## クエリ

`query_exec:exec_query/2` に渡すタプル。

SQL:

```sql
CREATE TABLE t (a VARCHAR, b INTEGER, c BOOLEAN);
DROP TABLE t;
CREATE INDEX i ON t (a);
DROP INDEX i;
ANALYZE [t];
EXPLAIN SELECT ...;
INSERT INTO t [(c, ...)] VALUES (v, ...);
UPDATE t SET c = expr, ... [WHERE expr];
DELETE FROM t [WHERE expr];
SELECT [DISTINCT] * | expr [AS name], ... FROM from_item [WHERE expr]
  [GROUP BY expr, ...] [HAVING expr]
  [ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST], ...]
  [LIMIT n] [OFFSET n];
<query> UNION|INTERSECT|EXCEPT [ALL] <query> [ORDER BY ...] [LIMIT n];

from_item は表名、結合、または導出表:
  FROM (SELECT ...) AS t
BEGIN;  BEGIN READ ONLY;  COMMIT;  ROLLBACK;
```

型は `INTEGER` / `FLOAT` / `VARCHAR` / `BOOLEAN`。暗黙変換はしない
(整数からFLOATへの格上げだけ許す)。指定しなかったカラムはNULLになる。

式に書けるもの:

| | |
| --- | --- |
| 比較 | `=` `<>`(`!=`) `<` `<=` `>` `>=` |
| 論理 | `AND` `OR` `NOT` `( )` |
| 算術 | `+` `-` `*` `/`(ゼロ除算はNULL) |
| NULL | `IS NULL` `IS NOT NULL` |
| 集合 | `IN (v, ...)` `NOT IN (...)` `IN (SELECT ...)` |
| 副問い合わせ | `(SELECT ...)`(スカラー) `EXISTS (SELECT ...)` |
| 集約 | `COUNT(*)` `COUNT(x)` `COUNT(DISTINCT x)` `SUM` `AVG` `MIN` `MAX` |
| 文字列 | `LIKE` `NOT LIKE`(`%` は任意の並び、`_` は任意の1文字) |
| 条件 | `CASE WHEN ... THEN ... [ELSE ...] END` |
| 関数 | 下記のスカラー関数 |

比較の一方がNULLなら結果はNULLになり、`WHERE` は通らない。
`NOT` をつけても通らない(3値論理)。

スカラー関数:

| | |
| --- | --- |
| 数値 | `ABS` `CEIL`/`CEILING` `FLOOR` `ROUND(x[,n])` `MOD` `POWER` `GREATEST` `LEAST` |
| 文字列 | `UPPER` `LOWER` `LENGTH` `SUBSTR(s,from[,len])` `TRIM` `LTRIM` `RTRIM` `CONCAT` `REPLACE` |
| NULL | `COALESCE(...)` `NULLIF(a,b)` |

**引数のどれかが NULL なら結果も NULL**が既定。例外は `COALESCE` と
`NULLIF` で、この2つは NULL を見て分岐するのが仕事。
型が合わないものは落とさず NULL にする(算術・比較と揃える)。

`GREATEST` / `LEAST` と `LIKE` は SQL の順序・文字単位で比べる。
Erlang の項順序だと `100 < <<"a">>` が通り、バイト単位だと多バイト文字で
`_` が1文字ぶんにならない。

タプルAPI(内部向け):

| クエリ | 戻り値 |
| --- | --- |
| `{begin_tx}` | `Txid` |
| `{commit_tx}` | `ok \| transaction_not_found` |
| `{rollback_tx}` | `ok \| transaction_not_found` |
| `{create_table, Table, Columns}` | `ok \| {error, Reason}` |
| `{drop_table, Table}` | `ok \| {error, Reason}` |
| `{insert, Table, Values}` | `{ok, Oid} \| {error, Reason}` |
| `{select, Table, Column, Value}` | `[Row] \| {error, Reason}` |
| `{update, Table, SetQuery, Column, Value}` | `{ok, Count} \| {error, Reason}` |
| `{delete, Table, Column, Value}` | `{ok, Count} \| {error, Reason}` |

`SetQuery` は `[{Column, NewValue}, ...]`。検索条件は等値のみ。
`create_table` と `drop_table` はトランザクションの対象外で、即座に反映される。

## 構成

```
   client
      |
 gen_connection ── query_exec_sup ── query_exec (接続ごと)
                                        |
                        +---------------+---------------+
                        |               |               |
                     tx_mng          lock_mng       log_util (REDOログ)
                        |
                  simple_db_server  ← ストレージエンジン
                        |
        +---------------+---------------+
        |               |               |
   sys_tbl_mng   simple_index / index   data_buffer
    (カタログ)      (インデックス)       (バッファプール)
                                            |
                                        file_mng (ページI/O)
```

| モジュール | 役割 |
| --- | --- |
| `sup` / `app` | スーパーバイザとアプリケーション |
| `gen_connection` | 接続の作成・切断 |
| `query_exec` | トランザクション中のクエリ実行。未コミットの変更をローカルに保持する |
| `tx_mng` | トランザクションの登録と、所有プロセスの監視 |
| `commit_latch` | DDLと走査を排他にするラッチ |
| `snapshot_mng` | スナップショットと undo。読み手を開始時点へ巻き戻す |
| `lock_mng` | オブジェクト単位の共有/排他ロック(2相ロック) |
| `log_util` | REDOログ(WAL) |
| `recover` | 起動時のクラッシュリカバリ |
| `simple_db_server` | ストレージエンジンのファサード |
| `sys_tbl_mng` | システムカタログ(DETSに永続化) |
| `simple_index` | ETSハッシュによるカラムインデックス(既定) |
| `index` | B+treeによるカラムインデックス。範囲検索ができる |
| `data_buffer` | バッファプール。Oidとページ内位置の対応を管理する |
| `file_mng` | データファイルへのページ単位の読み書き |
| `db_id` | Oid・トランザクションID・クエリIDの採番 |
| `sql_analyzer` | 意味解析。カラム参照を位置に束縛し、結合順序を決める |
| `sql_planner` | 論理プランの書き換えと物理プランへの変換 |
| `sql_stats` | 統計の採取と、選択率・費用の見積もり |
| `sql_explain` | 実行計画の描画 |
| `sql_exec` | Volcano(反復子)モデルの実行器 |

### トランザクション

**トランザクションは並行に走る。** 順番待ちは無い。分離は2つで担う。

| | |
| --- | --- |
| 読み | `snapshot_mng` のスナップショットと undo。**ロックは取らない** |
| 書き | `lock_mng` の行ロックと、コミット直前の衝突検査 |

以前は `tx_mng` が同時に1本しか active にせず、全トランザクションを
直列に実行していた。走査が読みロックを取らないので、直列でなければ
安全でなかった。スナップショットが入って、その前提が要らなくなった。

読みロックを取らないのは意図的で、走査は表の全行に触るため、
読みロックを取ると事実上の表ロックになって並行に走れない。

```sql
BEGIN;            -- スナップショットを取る
SELECT ...;       -- 開始時点の状態を読む。待たない
UPDATE ...;       -- その行の書き込みロックを取る。競合すれば待つ
COMMIT;           -- 衝突を確かめてから適用する
```

読み取り専用(`BEGIN READ ONLY`)は、これに加えて `tx_mng` にも
`lock_mng` にも触らない。**読み手と書き手は互いを待たない。**

コミットは変更前の値(undo)を `snapshot_mng` に積んでから適用する。
読み手は、自分のスナップショットより新しい undo を共有データに重ねて、
開始時点の値へ巻き戻して読む。

```
共有データ(いま)      undo(新しい順)          読み手が見るもの
  id=99            v51: id は 1 だった   →     id=1
  id=2             v50: 行Xは無かった    →     行Xは見えない
```

順序が要点で、**undo を積む → 適用する → 適用済みにする** の順に進む。
逆にすると、1行ずつ進む適用の途中を読み手が見てしまう。

書き手が並行に走ると、版を取る順と適用が終わる順が一致しない。版5と版6が
同時に進み、版6が先に終わることがある。だからスナップショットは1つの数
ではなく、**視界**として持つ(InnoDB の read view と同じ)。

```
limit    : 取った時点で採番済みだった最大の版
excluded : そのとき**まだ適用中**だった版の集合

見えるのは「S =< limit かつ excluded に無い」版
```

はじめは「連続して適用が終わっている所まで」という1つの数にしていたが、
それだと適用済みなのに誰にも見えない版ができる。その間に始まった
トランザクションはその版を「自分より後のもの」と見なすので、
**自分の直前のコミットにすら衝突と判定される**。実際その誤検出が出た。

* 読み手同士は並行に走る
* 読み手はコミットの全部を見るか、全部を見ないか
* コミットは読み手を待たない
* 同じトランザクションの中では何度読んでも同じ状態が見える

#### 書き込みの衝突

書き手もスナップショットで読むので、自分が読んだ後に他人が同じ行を変えても
気づかない。行の書き込みロックは、そのロックを**取った後**の変更しか防げない。
よってコミットの直前に「自分の視界の外のコミットが、自分の書く行を
変えていないか」を確かめる。変わっていたら自分を捨てる
(first-updater-wins、`serialization_failure`)。

行の待ちは閉路になりうるので、待ちに入る直前にデッドロックを検出する。
待ち行列そのものが「誰が誰を待つか」のグラフなので、要求元から辿って
自分に戻れれば閉路。見つけたら待たせずに `deadlock` を返し、
その文とトランザクションを捨てる。タイムアウトで気づく方式にしないのは、
待ち時間が長いほど正しいトランザクションまで巻き添えにするため。

行ロックのほかに**表の共有ロック**を取る。行ロックだけだと、書きかけの
トランザクションの下で `DROP TABLE` が通ってしまう。DDL は表の排他ロックを
取るので、その表の書き手が終わるまで待つ。共有ロックどうしは競合しないので、
同じ表への書き手が互いを待つことはない。

#### 何が保証され、何が保証されないか

**読み取り専用は直列化可能。** スナップショット分離の異常は書き込みを
伴って初めて起きるので、書かないトランザクションには現れない。

**読み書きは直列化可能ではない。** 得られるのはスナップショット分離で、
lost update は防げるが **write skew** は防げない。

```sql
-- 不変条件「vの合計は2以上」を2本が同時に壊す
T1: SELECT SUM(v) FROM t;      -- 3。片方を0にしても2残ると判断
T2: SELECT SUM(v) FROM t;      -- 3。同じ判断
T1: UPDATE t SET v=0 WHERE id=1;
T2: UPDATE t SET v=0 WHERE id=2;   -- 書く行が違うので衝突しない
両方コミット → 合計 0
```

書く行が重ならないので、行ロックにも衝突検査にも引っかからない。
直すには SSI(直列化可能スナップショット分離)か述語ロックが要る。
`test/tx_concurrent_tests.erl` に、防げないことを示す試験を置いてある。

undo は誰も要らなくなった時点で捨てる。上限(`max_undo`、既定1000コミット)を
超えると古いものから捨て、それを必要としていたスナップショットは無効になり、
以後の読みは `snapshot_too_old` で断られる。PostgreSQL の `snapshot too old` や
MySQL の `Rollback segment too small` と同じ性質のもので、
「古い読み手のために undo を無限に積む」を選ばないという判断。

**索引はスナップショットでも使える。** 索引は現在の値で引かれるので、
そのままでは足りない。索引付き列が更新されると、その行は古い鍵では
引けなくなり、新しい鍵で引けてしまう。索引そのものは版を持たない。

そこで候補を広げてから絞り直す。

```
候補 = いま索引に出てくる行 ∪ スナップショット以降に変わった行
     ↓ undo で巻き戻す
     ↓ ColName = Val で絞る
結果
```

後者は undo の重ね合わせの鍵そのもので、スナップショットを取ってからの
変更数しかない。表の大きさには比例しないので、索引を使う意味が残る。

走査中の DDL は `commit_latch` が止める。読み手は走査の間だけ共有ラッチを
持ち、DDL は排他ラッチを取る。表の共有ロックが守るのは「書きかけの
トランザクションの下で表が消えないこと」で、こちらが守るのは
「走査の途中で表が消えないこと」。時間の幅が違うので両方要る。

実測(2000行の表で6.2秒かかる自己結合を読み手が回し続けている間に、
別の接続が50回コミットする):

| | |
| --- | --- |
| 50コミットにかかった時間 | **161 ms** |

ラッチ方式では、コミットは走っている照会が終わるまで待っていた。

実測(8接続 × 各10クエリ、400行の表への集約):

| | |
| --- | --- |
| `BEGIN`(読み書き) | 94.4 ms |
| `BEGIN READ ONLY` | **14.1 ms** |

実測(N接続がそれぞれ別の行を50回ずつ更新。直列だった頃との比較):

| 接続数 | 直列(fsyncあり) | 並行(fsyncあり) | 直列(fsyncなし) | 並行(fsyncなし) |
| --- | --- | --- | --- | --- |
| 1 | 220 tx/s | 199 tx/s | 575 tx/s | 562 tx/s |
| 2 | 222 tx/s | 276 tx/s | 546 tx/s | 971 tx/s |
| 4 | 224 tx/s | 318 tx/s | 554 tx/s | 1274 tx/s |
| 8 | 218 tx/s | **362 tx/s** | 559 tx/s | **1762 tx/s** |

直列だった頃は接続を増やしても頭打ちだった。並行にすると接続数で伸びる。
fsync ありの伸びが鈍いのは、コミットごとに1回同期するため。ここは
グループコミット(複数のコミットのログを1回の fsync でまとめる)の領分で、
まだ入れていない。

読み取り専用では書き込みが断られる(`read_only_transaction`)。
書けてしまうと直列化の列に並ばないまま共有データを変えることになる。

トランザクション中の更新は共有データには書かず、`query_exec` プロセスが
持つローカル領域に溜める。SELECTは共有データの上に自分のローカルの変更を
重ねて返すので、コミット前の自分の変更は自分からだけ見える。

コミットは次の順に進む。

0. 全更新が適用可能かを検証する(通らなければ何も書かずに破棄)
1. このトランザクションの全更新をREDOログに書き、`disk_log:sync/1` で同期する
2. 共有データ(インデックスとデータファイル)へ反映する
3. データファイルとOid対応を `fsync` する
4. checkpointを書く
5. ロックを解放してトランザクションを終了する

1より前に、そのトランザクションの全変更が適用可能かを検証する。
検証を通らなければ何も書かずにトランザクションを破棄する。いったん共有データへ
適用すると戻す手段が無い(REDOのみでUNDOログを持たない)ので、
「適用が始まる前に確かめる」ことが原子性の担保になっている。

2の途中でプロセスやVMが落ちても、1が完了しているので次の起動時に
`recover` が最後のcheckpoint以降のログを再実行して追いつける。再実行は
Oid指定の上書き・削除なので、二重に適用しても結果は変わらない。

3のfsyncは省略できない。リカバリは最後のcheckpoint以降しか再実行しないため、
checkpointは「これより前は永続化済み」という宣言になる。ページキャッシュに
置いただけの状態でcheckpointを書くと、電源断のときにデータは失われるのに
リカバリは再実行せず、黙って消える。

ロールバックはローカル領域を捨てるだけでよい。共有データにはまだ何も
書いていないため。

トランザクションを開始した接続プロセスが異常終了した場合は、`tx_mng` が
monitorで検知してabortし、次のトランザクションに順番を渡す。

### ディスク上のページ

`file_mng` はテーブルごとのデータファイルを固定長ページ(4096バイト)で
読み書きする。1ページのレイアウト:

```
  0            12                12+8*SlotCount              4096
  +------------+-----------------+---------------+-----------+
  | header     | slot directory  | free space    | data      |
  | (12 bytes) | (8 bytes/slot)  |               | (末尾から) |
  +------------+-----------------+---------------+-----------+

  header : <<Magic:32, EmptySize:32, SlotCount:32>>
  slot N : <<Offset:32, Length:32>>   Length = 0 は未使用スロット
  data   : term_to_binary/1 でシリアライズした行
```

行は `term_to_binary/1` で保存するので、アトム・数値・文字列・タプルなど
任意のErlang項をそのまま格納できる。削除はスロットを未使用に戻すだけで、
そのスロット番号は次の挿入で再利用される。使われなくなったページは
`simple_db_server:vacuum/2` で回収できる。

`data_buffer` は8フレームのバッファプールを持ち、空きがなければ
最後に使われてから最も時間が経ったフレームを追い出す。書き込みは
ライトスルーなので、追い出す際にフラッシュする必要はない。

### インデックス

**索引は `CREATE INDEX` で明示的に作る。** 以前は `CREATE TABLE` が
全カラムに自動で索引を張っていたが、それだと「索引があるか」が常に真になり、
プランナのアクセスパス選択が退化する。

```sql
CREATE INDEX fruit_price ON fruit (price);
DROP INDEX fruit_price;
```

索引の無いカラムへの検索は**全表走査に落ちる**。落ちる先が無いと、
索引の無いカラムを条件にした検索が黙って空を返すことになる。

既定は `simple_index`(ETSハッシュ、等値検索のみ)。設定で `index`
(B+tree)に差し替えられる。B+treeは等値検索に加えて `index:select_range/4`
による範囲検索ができる。

```erlang
application:set_env(transaction_db, index_module, index).
```

インデックスはETS上にしかないため、`simple_db_server` は起動時に
カタログとデータファイルから読み直して再構築する。作り直すのは
カタログに宣言されているものだけ。

索引の定義は `ms_indexes.sys`(DETS)に永続化される。テーブルの行の形
`{Name, Columns}` を変えると既存のデータファイルとの互換が切れるので、
別のDETSに分けてある。

### 集合演算

```sql
SELECT k FROM a UNION     SELECT k FROM b ORDER BY 1;
SELECT k FROM a UNION ALL SELECT k FROM b;
SELECT k FROM a INTERSECT SELECT k FROM b;
SELECT k FROM a EXCEPT    SELECT k FROM b;
```

`INTERSECT` は `UNION` / `EXCEPT` より強く結合する(標準SQL)。
`ORDER BY` / `LIMIT` は演算全体に掛かり、結果の列名か序数で指す。
出力の列名は左に従う。

`ALL` は重複度を保つ。

```
a = [1, 2, 2, 3]、b = [2] のとき
  UNION ALL      → [1, 2, 2, 3, 2]
  INTERSECT ALL  → [2]        右の2は1つしか無いので1回だけ一致
  EXCEPT ALL     → [1, 2, 3]  右の2が左の2を1つだけ打ち消す
```

重複の判定は `sql_value:group_key/1` を通す。**`=` の意味論とは違い、
NULL 同士は等しいとみなす**(標準SQLの "not distinct from")。
`=` をそのまま使うと、NULL の行が `UNION` で重複除去されずに残る。

### 副問い合わせ

```sql
SELECT name FROM emp WHERE sal = (SELECT MAX(sal) FROM emp);
SELECT name FROM emp WHERE dept IN (SELECT id FROM dept);
SELECT name FROM emp WHERE EXISTS (SELECT 1 FROM dept WHERE id = 10);
SELECT name FROM emp WHERE dept IN (10, 20);
```

相関するもの(外側の列を参照するもの)も書ける。

```sql
SELECT e.name FROM emp e WHERE EXISTS
  (SELECT 1 FROM dept d WHERE d.id = e.dept);

SELECT e.name, (SELECT d.dname FROM dept d WHERE d.id = e.dept) AS dn FROM emp e;

SELECT e.name FROM emp e
 WHERE e.sal > (SELECT AVG(x.sal) FROM emp x WHERE x.dept = e.dept);
```

**相関しないものは本体を動かす前に1回だけ実行して定数へ畳む。**
相関するものは外側の行に依存するので、行ごとに実行し直す。
プランに `{outer, _, _}` が含まれるかどうかで見分ける。

名前解決は内側の層から順に探す。内側で解決できる名前を外側に
取られてはいけない。何段外かを覚えておき、実行時に外側の行の並びから引く。

スカラー副問い合わせは1行1列を要求する。0行なら NULL(標準SQL)、
2行以上はエラー。

`IN` は3値論理で評価する。

```
x が NULL          → unknown
一致がある         → true
一致が無くNULL有り → unknown(そのNULLが x かもしれない)
一致が無くNULL無し → false
```

だから `x NOT IN (10, NULL)` は決して真にならない。

### 導出表

```sql
SELECT * FROM (SELECT dept, SUM(sal) AS total FROM emp GROUP BY dept) AS d
 WHERE d.total > 600;
```

別名は必須(列を修飾するのに要る)。中に集合演算も別の導出表も書ける。

中の射影はリストを出すが、外側の演算子は位置参照(`element/2`)で引くので、
導出表がタプルへ戻す。述語は導出表の中へは落とさない。中の位置は射影の
出力位置であって、集約や `DISTINCT` が挟まると外の条件をそのまま
持ち込めないため。

### 実行計画

`EXPLAIN` で選ばれた計画が見られる。

```
sql> EXPLAIN SELECT * FROM emp WHERE id = 2 AND name = 'bob';
 Project (id, name, dept)
   Filter (name = 'bob')
     Index Scan on emp (id = 2)
```

索引を使うかどうかは**費用の比較**で決まる。「索引があるなら使う」ではない。

```
順次走査   費用 = 行数
索引走査   費用 = 1 + 行数 × 選択率 × 2      (索引経由の1行はランダム読み)
```

異なり値が2しかないカラムでは選択率が 0.5 になり、索引の費用が
順次走査を上回る。**索引を張っても使われない**のが正しい。

選択率は統計から求める。等値は `1/異なり値`、範囲は 1/3、
`AND` は積、`OR` は余事象の積の余、読めない述語は 0.5。

**結合アルゴリズム**は、等値で結べるかどうかで決まる。
`INNER` / `LEFT` / `RIGHT` / `FULL OUTER` のいずれでも同じ選択をする。

```
sql> EXPLAIN SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.id;
 Project (name, dname)
   Hash INNER Join on dept = id
     Seq Scan on emp
     Seq Scan on dept
```

`INNER` / `LEFT` / `RIGHT` / `FULL OUTER` に対応する。等値の条件が
1つでもあればハッシュ結合、無ければ入れ子ループ。
入れ子ループが |左|×|右| 回の比較をするのに対し、ハッシュ結合は
|左|+|右| で済む。右側はどちらの方式でもメモリに載せるので、
使うメモリは変わらない。

実測(500行 × 500行の等値結合):

| | |
| --- | --- |
| 入れ子ループ | 44.5 ms |
| ハッシュ結合 | **1.6 ms** |

等値以外の条件は、一致した組に対して後から評価する。

> **NULL の扱いに注意が要る。** `NULL = NULL` は unknown なので決して
> 一致しないが、素直にハッシュ表へ入れると NULL 同士が同じ鍵で衝突して
> 一致してしまう。鍵に NULL を含む行は表に入れず、引くときも一致無しとする。
> 値は `sql_value:group_key/1` で正規化する。素の項を鍵にすると
> 100 と 100.0 が別の鍵になるが、SQLの `=` では等しい。

**結合順序**も推定行数で決める。3表以上のときだけ動かす。

```
sql> EXPLAIN SELECT big.id FROM big JOIN mid ON big.m = mid.id
                                    JOIN small ON mid.s = small.id;
 Project (id)
   Nested Loop INNER Join on (m = id)
     Nested Loop INNER Join on (s = id)
       Seq Scan on small        ← 3行。書いた順では最後だった
       Seq Scan on mid          ← 10行
     Seq Scan on big            ← 40行
```

貪欲法で、**直積を後回しにする**のが主な効き目。条件でつながっている表を
優先し、つながっているものが複数あれば中間結果が小さくなる方を採る。
つながりは `ON` だけでなく `WHERE` からも読む
(`FROM a, b WHERE a.x = b.y` という書き方があるため)。

並べ替えは**アナライザで行う**。カラム参照は名前解決の時点で行タプル内の
位置に束縛されるので、プランナではもう動かせない。

### 統計

`ANALYZE` で採る。1回走査して、行数と各カラムの異なり値・NULL数・
最小最大を数え、`ms_stats.sys`(DETS)に置く。

```sql
ANALYZE fruit;   -- 1テーブル
ANALYZE;         -- 全テーブル
```

**挿入・削除では更新しない。** 更新すると1行ごとにDETSへの書き込みが増え、
ロールバックで戻す必要も出る。統計は古くてよい。見積もりが外れても
**結果は変わらず、遅くなるだけ**である(PostgreSQLの `ANALYZE` と同じ割り切り)。

採っていないテーブルは既定値(1000行)で見積もる。「統計が無いから
最適化しない」ではなく「分からないなりに見積もる」。

異なり値の計数は10000で打ち切る。全行ぶんの集合を持つと大きな表で
メモリを食う。選択率の見積もりに要るのは桁であって正確な値ではない。

## 設定

`transaction_db` のアプリケーション環境。

| キー | 既定値 | 内容 |
| --- | --- | --- |
| `data_dir` | `"./data"` | データファイル・カタログ・REDOログの置き場所 |
| `index_module` | `simple_index` | インデックス実装。`simple_index` または `index` |
| `durable_commit` | `true` | コミット時にディスクへ同期するか。`false` にするとfsyncが無くなり速くなるが、電源断でコミット済みのデータを失う |

## 今後の課題

### 並行制御

**段階1〜3は実装済み。** トランザクションは並行に走る。

| 段階 | 内容 | 効果 |
| --- | --- | --- |
| 1 | `BEGIN READ ONLY` が直列化の列に並ばない | 8接続の読み取りで 94.4ms → 14.1ms |
| 2 | スナップショット分離。読み手が書き手を待たせない | 6.2秒の照会中に50コミットが161msで完走 |
| 3 | 書き手の並行化。行ロック + 衝突検査 + デッドロック検出 | 8接続の書き込みで 218 → 362 tx/s(fsyncなしで 559 → 1762) |

段階2で選んだのは、行ごとのバージョン鎖(xmin/xmax)ではなく
MySQL/InnoDB と同じ **undo による巻き戻し**。既存の「行をその場で
書き換える」ストレージに手を入れずに済むため。代償は「古い読み手は
undo の上限を超えると断られる」こと。索引は版を持たないが、
候補を広げて絞り直すことで使えるようにした。

段階3で失ったのは直列化可能性で、得たのはスナップショット分離。
lost update は防げるが write skew は防げない。

残る課題:

* **SSI(直列化可能スナップショット分離)** — write skew を防ぐには、
  読んだ範囲を記録して危険な構造(rw依存の連鎖)を検出する必要がある。
  述語ロックでも直せるが、走査のたびに範囲ロックを取ることになる
* **グループコミット** — fsync ありの伸びが鈍いのは、コミットごとに
  1回同期しているため。複数のコミットのログを1回の fsync でまとめれば、
  接続数に対してもっと素直に伸びる
* **ファントム** — `INSERT` はロックの対象になる行が無い(新しいOid)。
  いまはスナップショットが読みを固定するので走査からは見えないが、
  述語ロックを入れるならここが対象になる

### 直列化に依存している箇所

現在の正しさのうち、以下は直列化から借りているもので、緩めると崩れる。

- ~~**読み手から見た原子性**~~ — `snapshot_mng` で解決済み。
  `apply_changes/1` は1行ずつ適用するが、undo を先に積むので、
  適用済みの行は巻き戻され、未適用の行はもともと古い。どちらも同じに見える
- ~~**検証と適用の間**~~ — 表の共有ロックで解決済み。DDL はその表を
  書きかけのトランザクションが終わるまで待つ
- **INSERTがロックを取っていない** — 新しいOidなのでロック対象が無い。
  いまはスナップショットが読みを固定するので走査からは見えないが、
  述語ロックを入れるならここが対象になる

一方、以下は直列化を外しても残る。

- 永続性
- 1行操作の内部整合性(`simple_db_server` の1つの `handle_call` で完結する)

### `lock_mng` の扱い

`tx_mng` が全トランザクションを直列化しているため、2つのトランザクションが
同時にロックを保持する状態は構造上到達不能で、競合で `can_lock/3` が
`false` を返す経路は実行されない。**現状では実証的に死コードである。**
上記3に着手するまでは、2相ロックが機能しているとは言えない。

### 分散化を見据えた設計上の制約

将来、分散DB（レプリケーション・パーティショニング）へ舵を切る可能性がある。
Erlangの価値が最も出るのはこの領域なので、いま安く、後で高くつく決定を
記録しておく。

**いま決めておくと安いもの**

- **オブジェクトIDにノード識別子が入っていない** —
  `db_id:new/0` は `{時刻, ノード内で一意な整数}` を返す。
  ノードをまたぐと一意性が保証されず、DETSとREDOログに永続化されるため、
  後から変えるとデータ移行が必要になる。分散化の可能性があるなら早いうちに
  ノード識別子を足しておくのが安い
- **決定的実行を壊す要素** — 状態機械レプリケーション（同じログを各ノードで
  再実行する方式）を採るなら、実行が決定的でなければならない。
  現在のオブジェクトID採番は `erlang:system_time/1` を含むため、
  ノードごとに違う値になり分岐する。Calvin型の決定的トランザクションを
  狙う場合はここが障害になる
- **REDOログの形式** — レプリケーションで送るのはこのログになる。
  `#redo_log{}` にバージョン欄が無い

**すでに都合が良いもの**

- **WALが存在する** — レプリケーションで送るべき成果物がすでにある
- **`tx_mng` が順序を決める役割を持っている** — Calvin型のシーケンサに
  そのまま対応する。順序を決めてから実行する構造は、分散化と相性が良い
- **スーパーバイザ木** — ノード内の障害回復はすでにOTPに載っている

### その他

- **Halloween problem** — `UPDATE` を実行器(Volcano)に載せると顕在化する。
  文レベルスナップショットが必要
- **DDLのロールバック** — カタログ変更のUNDOが無いため、明示的な
  トランザクション内のDDLは拒否している

## 制限

- `CREATE TABLE` / `DROP TABLE` は暗黙のトランザクションとして実行される。
  他のトランザクションとは直列化されるが、明示的なトランザクションの中では
  実行できない(カタログ変更を戻すUNDOログが無いため)
- 索引は単一カラムのみ。複合索引・一意索引は未実装
- 相関副問い合わせは**外側の行ごとに実行し直す**。結合への書き換え
  (デコリレーション)はしていないので、外側の行数だけ副問い合わせが走る
- 導出表は `LATERAL` ではない。外側の列は見せない
- 入れ子が深いとき、内側が使う外側の参照を見つけると外側も
  「相関している」とみなして畳まない。安全側だが、畳めるものを
  畳まないことがある
- ウィンドウ関数は未実装
- **`BETWEEN` は未実装。** `x BETWEEN a AND b` の規則は最後の終端記号が
  `AND` になり、yecc は規則の優先順位を最後の終端記号から取る
  (`%prec` に相当する指定が無い)。その結果 BETWEEN の `AND` が論理演算の
  `AND` と同じ優先度になり、`x BETWEEN 1 AND 2 AND y > 3` の切り方が
  決まらず衝突する。糖衣なので `x >= a AND x <= b` と書けばよい
- `CAST` は未実装
- 導出表の列の型は `any` になる(射影の式から型を推論しない)
- 列別名の `AS` は省略できない。省略を許すと `SELECT a b` が
  別名なのかカンマの書き忘れなのか区別できない
- 集合演算の `ORDER BY` は結果の列名か序数のみ。任意の式は書けない
  (結果には元のスコープが無いため)
- 結合の右側は、どちらの方式でも開始時にメモリへ載せる。
  右側が巨大だと載り切らない
- 集約は NULL を入力から外す(`COUNT(*)` だけが例外)。
  空集合では `COUNT` が 0、それ以外は NULL を返す
- 索引スキャンは**等値条件のみ**。範囲は未対応。未コミットの変更を
  重ねる仕組みが等値でしか動かないため、範囲で索引を引くと自分の
  変更が見えなくなる
- 結合順序の並べ替えは内部結合と直積のみ。`LEFT JOIN` が1つでも
  混ざっていたら書いた順のまま(可換でも結合的でもないため)
- 型宣言のないテーブル(タプルAPIで作ったもの)は全カラムが `any` 型になり、
  アトムをそのまま格納する。SQLの文字列リテラル(binary)とは一致しない
- コミットごとに fsync するため、書き込みの並行度は fsync で頭打ちになる。
  グループコミットは未実装
- 読み取り専用であることは宣言が要る。自動では判定しない
- デッドロックは断るだけで、再実行はしない。`deadlock` や
  `serialization_failure` を受けたらクライアントがやり直す
- 分離レベルはスナップショット分離。write skew は防げない(直列化可能ではない)。
  行ごとのバージョン鎖(xmin/xmax)を持つ本来のMVCCではなく、
  コミット単位の undo をメモリに積んで巻き戻す方式
- undo はメモリ上にしか無く、上限(`max_undo`、既定1000コミット)を超えると
  古い読み手は `snapshot_too_old` で断られる
- ロールバック用のUNDOログはディスクに無い。ロールバックは共有データに
  書く前に行われる前提。`snapshot_mng` の undo は読み手のためだけのもの
- カラムに型はなく、任意のErlang項を格納できる
