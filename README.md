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
| 集約 | `COUNT(*)` `COUNT(x)` `COUNT(DISTINCT x)` `SUM` `AVG` `MIN` `MAX` |

比較の一方がNULLなら結果はNULLになり、`WHERE` は通らない。
`NOT` をつけても通らない(3値論理)。

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
| `tx_mng` | 読み書きトランザクションの直列化 |
| `commit_latch` | コミットの適用を読み手から見て原子的にするラッチ |
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

`tx_mng` は同時に1つの**読み書き**トランザクションだけをactiveにする。
activeでないトランザクションからのクエリは、自分の順番が来るまでブロックする。

**読み取り専用トランザクションはこの列に並ばない。**

```sql
BEGIN READ ONLY;
SELECT ...;
COMMIT;
```

代わりに `commit_latch` の共有ラッチをトランザクションの間じゅう持つ。
コミットの適用は排他ラッチを取るので、

* 読み手同士は並行に走る
* 読み手が動いている間、どのコミットも適用されない
* 読み手はコミットの全部を見るか、全部を見ないか

**直列化可能性は保たれる。** 書き手の変更は適用まで共有データに現れず、
その適用が読み手の終わりまで待つので、読み手はその書き手の前か後ろの
どちらかに並べられる。

ラッチを文単位ではなくトランザクション単位で持つのが要点で、文単位だと
同じトランザクション内の2つのSELECTが違う状態を見る(反復不能読み取り)。

実測(8接続 × 各10クエリ、400行の表への集約):

| | |
| --- | --- |
| `BEGIN`(読み書き) | 94.4 ms |
| `BEGIN READ ONLY` | **14.1 ms** |

読み取り専用では書き込みが断られる(`read_only_transaction`)。
共有ラッチを持ったまま共有データを書くと、上の保証が崩れるため。

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

```
sql> EXPLAIN SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.id;
 Project (name, dname)
   Hash INNER Join on dept = id
     Seq Scan on emp
     Seq Scan on dept
```

等値の条件が1つでもあればハッシュ結合、無ければ入れ子ループ。
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

### 並行制御(最優先)

**段階1は実装済み**(`BEGIN READ ONLY`)。読み取り専用トランザクションは
直列化の列に並ばず、互いに並行に走る。8接続の読み取りで 94.4ms → 14.1ms。

残るのは書き込みの並行度で、1コミット約2.8ms(fsyncあり)が上限、
およそ350 tx/秒。ここは変わっていない。

直列実行そのものが悪いわけではない。VoltDBはパーティションごとに直列実行し、
SQLiteのWALモードは書き手を1本に保ったまま読み手を並行させる。実用になるのは
**パーティショニングか読み手の分離と組み合わせたとき**で、本実装はそのどちらも
持っていない。

**緩めるための前提は「コミットを原子的に可視化すること」。** 現状 `apply_changes/1`
は1行ずつ適用するため、並行する読み手はコミットの途中を見る。これを解かない限り、
どんな緩和も安全にならない。「読み取り専用トランザクションだけ並行にする」は
一見安全に見えるが、この理由で安全でない。

段階:

1. ~~**単一書き手 + 並行読み手**~~ — 実装済み。`commit_latch` が
   コミットの適用を囲い、読み手はその間だけ止まる。書き手は作業中
   ローカル領域しか触らず、共有データに触るのは適用フェーズだけなので
   排他区間は短い(REDOログのfsyncはラッチの外)。
   ただし読み取り専用であることを `BEGIN READ ONLY` で明示する必要がある。
   宣言なしに読み取り専用と判定する仕組みは無い
2. **スナップショット分離**。読み手が開始時点のバージョンを見る。
   追記専用ストレージかバージョン鎖が要る
3. **書き手の並行化**。ここで初めて `lock_mng` が仕事をする。
   同時に、デッドロック検出・走査のロック粒度(ファントム)・
   write skew が問題になる

### 直列化に依存している箇所

現在の正しさのうち、以下は直列化から借りているもので、緩めると崩れる。

- ~~**読み手から見た原子性**~~ — `commit_latch` で解決済み。
  `apply_changes/1` は1行ずつ適用するが、排他ラッチで囲ってある
- **検証と適用の間** — `validate_changes/1` を通ってから適用するまでに、
  他のトランザクションがテーブルを落とせるとコミットが千切れる
- **INSERTがロックを取っていない** — 新しいOidなのでロック対象が無い。
  行ロックでは並行する走査からコミット途中の挿入行を隠せない(ファントム)

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
- 副問い合わせは `FROM` の導出表のみ。`IN (SELECT ...)` / `EXISTS` /
  スカラー副問い合わせ・相関副問い合わせは未実装
- ウィンドウ関数・`RIGHT`/`FULL OUTER JOIN` は未実装
- 導出表の列の型は `any` になる(射影の式から型を推論しない)
- 列別名の `AS` は省略できない。省略を許すと `SELECT a b` が
  別名なのかカンマの書き忘れなのか区別できない
- 集合演算の `ORDER BY` は結果の列名か序数のみ。任意の式は書けない
  (結果には元のスコープが無いため)
- 結合の右側は、どちらの方式でも開始時にメモリへ載せる。
  外部結合の右側が巨大だと載り切らない
- 集約は NULL を入力から外す(`COUNT(*)` だけが例外)。
  空集合では `COUNT` が 0、それ以外は NULL を返す
- 索引スキャンは**等値条件のみ**。範囲は未対応。未コミットの変更を
  重ねる仕組みが等値でしか動かないため、範囲で索引を引くと自分の
  変更が見えなくなる
- 結合順序の並べ替えは内部結合と直積のみ。`LEFT JOIN` が1つでも
  混ざっていたら書いた順のまま(可換でも結合的でもないため)
- 外部結合のハッシュ結合は左外部のみ(`RIGHT`/`FULL` は構文自体が未実装)
- 型宣言のないテーブル(タプルAPIで作ったもの)は全カラムが `any` 型になり、
  アトムをそのまま格納する。SQLの文字列リテラル(binary)とは一致しない
- 読み書きトランザクションは1つずつ直列に実行されるため、書き込みの
  並行度は上がらない。読み取り専用(`BEGIN READ ONLY`)は並行に走る
- 読み取り専用であることは宣言が要る。自動では判定しない
- 分離レベルは直列実行と読み手ラッチによるもので、MVCCではない
- 長い読み取り専用トランザクションは書き手を待たせる(スナップショット
  分離が入るまでは避けられない)
- REDOログのみでUNDOログはない。ロールバックは共有データに書く前に行われる前提
- カラムに型はなく、任意のErlang項を格納できる
