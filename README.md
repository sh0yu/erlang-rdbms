# erlang-rdbms

Erlangで書いたリレーショナルデータベース。ページ単位のディスクストレージ、
バッファプール、カラムインデックス、トランザクション、REDOログによる
クラッシュリカバリを備えている。

OTPアプリケーション名は `transaction_db`。

## 動かす

必要なもの: Erlang/OTP 25 以降。

```sh
rebar3 compile
rebar3 eunit          # 146 tests
rebar3 shell          # transaction_dbを起動した状態のシェル
```

rebar3がない環境では同等のことがmakeでできる。

```sh
make        # ebin/ にビルド
make test   # EUnitを実行
make shell
```

### 使ってみる

```erlang
%% サーバ群を起動する
{ok, _} = sup:start_link(),

%% 接続を作る。1接続が1トランザクションを実行する
{ok, C} = gen_connection:connect(),

ok = query_exec:exec_query(C, {create_table, fruit, [name, price]}),

_Txid    = query_exec:exec_query(C, {begin_tx}),
{ok, Oid} = query_exec:exec_query(C, {insert, fruit, [apple, 100]}),
[[apple, 100]] = query_exec:exec_query(C, {select, fruit, name, apple}),
{ok, 1}  = query_exec:exec_query(C, {update, fruit, [{price, 120}], name, apple}),
ok       = query_exec:exec_query(C, {commit_tx}),

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
| `tx_mng` | トランザクションの直列化 |
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

### トランザクション

`tx_mng` は同時に1つのトランザクションだけをactiveにする。activeでない
トランザクションからのクエリは、自分の順番が来るまでブロックする。
これにより全トランザクションが直列に実行される。

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

既定は `simple_index`(ETSハッシュ、等値検索のみ)。設定で `index`
(B+tree)に差し替えられる。B+treeは等値検索に加えて `index:select_range/4`
による範囲検索ができる。

```erlang
application:set_env(transaction_db, index_module, index).
```

インデックスはETS上にしかないため、`simple_db_server` は起動時に
カタログとデータファイルから読み直して再構築する。

## 設定

`transaction_db` のアプリケーション環境。

| キー | 既定値 | 内容 |
| --- | --- | --- |
| `data_dir` | `"./data"` | データファイル・カタログ・REDOログの置き場所 |
| `index_module` | `simple_index` | インデックス実装。`simple_index` または `index` |
| `durable_commit` | `true` | コミット時にディスクへ同期するか。`false` にするとfsyncが無くなり速くなるが、電源断でコミット済みのデータを失う |

## 制限

- `CREATE TABLE` / `DROP TABLE` は暗黙のトランザクションとして実行される。
  他のトランザクションとは直列化されるが、明示的なトランザクションの中では
  実行できない(カタログ変更を戻すUNDOログが無いため)
- 検索条件は単一カラムの等値比較のみ。JOIN・集約は未実装
  (SQLは `SELECT ... FROM ... WHERE col = value` まで)
- トランザクションは1つずつ直列に実行されるため、書き込みの並行度は上がらない
- 分離レベルは直列実行によるもので、MVCCではない
- REDOログのみでUNDOログはない。ロールバックは共有データに書く前に行われる前提
- カラムに型はなく、任意のErlang項を格納できる
