%%%-------------------------------------------------------------------
%%% Hermitage の分離水準テスト。
%%%
%%%   https://github.com/ept/hermitage
%%%
%%% 「このDBはスナップショット分離である」と自分で言うだけでは、
%%% 自分で決めた定義を自分で確かめているにすぎない。Hermitage は
%%% Martin Kleppmann が各DBを同じ手順で並べるために書いたもので、
%%% 異常ごとに **どの文をどの順で流すか** が決まっている。
%%% ここではその手順をそのまま写し、結果を確かめる。
%%%
%%% 対応表(左が Hermitage の PostgreSQL 版、右がこのDB):
%%%
%%%   set transaction isolation level read committed  → BEGIN READ COMMITTED
%%%   set transaction isolation level repeatable read → BEGIN
%%%   abort                                           → ROLLBACK
%%%   value %% 3 = 0                                  → mod(value, 3) = 0
%%%
%%% 期待値は PostgreSQL 版のものをそのまま使う。15本のうち14本は
%%% PostgreSQL と同じ結果になった。**ずれたのは1本**で、
%%% pmp_write_predicate_rc がそれ。理由はそのテストのコメントに書いた。
%%%-------------------------------------------------------------------
-module(hermitage_tests).

-include_lib("eunit/include/eunit.hrl").

-define(RC, "BEGIN READ COMMITTED").
-define(RR, "BEGIN").

hermitage_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun g0_write_cycles/1,
      fun g1a_aborted_read/1,
      fun g1b_intermediate_read/1,
      fun g1c_circular_information_flow/1,
      fun otv_observed_transaction_vanishes/1,
      fun pmp_read_predicate_rc/1,
      fun pmp_read_predicate_rr/1,
      fun pmp_write_predicate_rc/1,
      fun pmp_write_predicate_rr/1,
      fun p4_lost_update_rc/1,
      fun p4_lost_update_rr/1,
      fun g_single_read_skew_rc/1,
      fun g_single_read_skew_rr/1,
      fun g2_item_write_skew_rr/1,
      fun g2_anti_dependency_rr/1]}.

%%%===================================================================
%%% G0 (Write Cycles) — 3水準すべてで防がれる
%%%
%%% 2本が同じ行を交互に書く。片方の書き込みだけが別の行に残る、
%%% という状態(dirty write)が起きてはいけない。
%%%===================================================================
g0_write_cycles(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RC),
        ok = q(T2, ?RC),
        {ok, 1} = q(T1, "UPDATE test SET value = 11 WHERE id = 1"),
        %% T2 は行ロックで止まる
        B = async(fun() -> q(T2, "UPDATE test SET value = 12 WHERE id = 1") end),
        ?assertEqual(timeout, await(B, 300)),
        {ok, 1} = q(T1, "UPDATE test SET value = 21 WHERE id = 2"),
        ok = q(T1, "COMMIT"),
        %% T1 のコミットで T2 が動く
        ?assertEqual({ok, {ok, 1}}, await(B, 5000)),
        {ok, 1} = q(T2, "UPDATE test SET value = 22 WHERE id = 2"),
        ok = q(T2, "COMMIT"),
        %% 1 => 12, 2 => 22。T1 の書き込みが混ざっていない
        ?assertEqual([[1, 12], [2, 22]], dump(T1))
    end.

%%%===================================================================
%%% G1a (Aborted Reads) — 3水準すべてで防がれる
%%% 捨てられたトランザクションの書き込みが見えてはいけない。
%%%===================================================================
g1a_aborted_read(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RC),
        ok = q(T2, ?RC),
        {ok, 1} = q(T1, "UPDATE test SET value = 101 WHERE id = 1"),
        ?assertEqual([[1, 10], [2, 20]], rows(T2)),
        ok = q(T1, "ROLLBACK"),
        ?assertEqual([[1, 10], [2, 20]], rows(T2)),
        ok = q(T2, "COMMIT")
    end.

%%%===================================================================
%%% G1b (Intermediate Reads) — 3水準すべてで防がれる
%%% トランザクションの途中の値(101)が見えてはいけない。
%%% 見えてよいのは最終の値(11)だけ。
%%%===================================================================
g1b_intermediate_read(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RC),
        ok = q(T2, ?RC),
        {ok, 1} = q(T1, "UPDATE test SET value = 101 WHERE id = 1"),
        ?assertEqual([[1, 10], [2, 20]], rows(T2)),
        {ok, 1} = q(T1, "UPDATE test SET value = 11 WHERE id = 1"),
        ok = q(T1, "COMMIT"),
        %% READ COMMITTED なので、コミット後の値は見える。101 は一度も見えない
        ?assertEqual([[1, 11], [2, 20]], rows(T2)),
        ok = q(T2, "COMMIT")
    end.

%%%===================================================================
%%% G1c (Circular Information Flow) — 3水準すべてで防がれる
%%% 互いに相手の未コミットの書き込みを読んではいけない。
%%%===================================================================
g1c_circular_information_flow(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RC),
        ok = q(T2, ?RC),
        {ok, 1} = q(T1, "UPDATE test SET value = 11 WHERE id = 1"),
        {ok, 1} = q(T2, "UPDATE test SET value = 22 WHERE id = 2"),
        ?assertEqual([[2, 20]], one(T1, 2)),
        ?assertEqual([[1, 10]], one(T2, 1)),
        ok = q(T1, "COMMIT"),
        ok = q(T2, "COMMIT")
    end.

%%%===================================================================
%%% OTV (Observed Transaction Vanishes) — 3水準すべてで防がれる
%%%
%%% T1 が2行を1つのコミットで変える。T3 から見て、片方だけが
%%% 適用された状態が見えてはいけない。
%%%===================================================================
otv_observed_transaction_vanishes(_) ->
    fun() ->
        T1 = seeded(), T2 = connect(), T3 = connect(),
        ok = q(T1, ?RC), ok = q(T2, ?RC), ok = q(T3, ?RC),
        {ok, 1} = q(T1, "UPDATE test SET value = 11 WHERE id = 1"),
        {ok, 1} = q(T1, "UPDATE test SET value = 19 WHERE id = 2"),
        B = async(fun() -> q(T2, "UPDATE test SET value = 12 WHERE id = 1") end),
        ?assertEqual(timeout, await(B, 300)),
        ok = q(T1, "COMMIT"),
        ?assertEqual({ok, {ok, 1}}, await(B, 5000)),

        ?assertEqual([[1, 11]], one(T3, 1)),
        {ok, 1} = q(T2, "UPDATE test SET value = 18 WHERE id = 2"),
        %% T1 のコミットは 1=>11 と 2=>19 の両方。片方だけは見えない
        ?assertEqual([[2, 19]], one(T3, 2)),
        ok = q(T2, "COMMIT"),
        ?assertEqual([[2, 18]], one(T3, 2)),
        ?assertEqual([[1, 12]], one(T3, 1)),
        ok = q(T3, "COMMIT")
    end.

%%%===================================================================
%%% PMP (Predicate-Many-Preceders) 読みの述語
%%%   READ COMMITTED  防がれない
%%%   REPEATABLE READ 防がれる
%%%===================================================================
pmp_read_predicate_rc(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RC),
        ok = q(T2, ?RC),
        ?assertMatch({ok, _, []}, q(T1, "SELECT * FROM test WHERE value = 30")),
        {ok, _} = q(T2, "INSERT INTO test (id, value) VALUES (3, 30)"),
        ok = q(T2, "COMMIT"),
        %% 文ごとにスナップショットを取り直すので、後から入った行が見える
        ?assertMatch({ok, _, [[3, 30]]},
                     q(T1, "SELECT * FROM test WHERE mod(value, 3) = 0")),
        ok = q(T1, "COMMIT")
    end.

pmp_read_predicate_rr(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RR),
        ok = q(T2, ?RC),
        ?assertMatch({ok, _, []}, q(T1, "SELECT * FROM test WHERE value = 30")),
        {ok, _} = q(T2, "INSERT INTO test (id, value) VALUES (3, 30)"),
        ok = q(T2, "COMMIT"),
        %% スナップショットは開始時点のまま。新しい行は見えない
        ?assertMatch({ok, _, []}, q(T1, "SELECT * FROM test WHERE mod(value, 3) = 0")),
        ok = q(T1, "COMMIT")
    end.

%%%===================================================================
%%% PMP 書きの述語
%%%   READ COMMITTED  PostgreSQL では防がれない。**このDBでは防がれる**
%%%   REPEATABLE READ 防がれる(直列化エラー)
%%%
%%% ここだけ PostgreSQL と結果が違う。違いは「ロックを待たされた後に
%%% 何をやり直すか」から来ている。
%%%
%%%   PostgreSQL  待っていた**その行だけ**を読み直して条件を当て直す
%%%               (EvalPlanQual)。走査そのものはやり直さない
%%%   このDB      **文を頭からやり直す**。走査もやり直す
%%%
%%% 初期値 1=>10, 2=>20 に T1 が全行 +10 して 1=>20, 2=>30 にする。
%%% T2 の DELETE ... WHERE value = 20 は、走査した時点では id=2 に
%%% 当たっていて、そこで止まる。
%%%
%%%   PostgreSQL  id=2 だけを読み直す。30 になっているので条件に合わず、
%%%               何も消さない。走査し直さないので、新たに 20 になった
%%%               id=1 は最後まで見ない。結果、消したはずの 20 が残る
%%%   このDB      走査からやり直す。1=>20, 2=>30 を見て id=1 を消す
%%%
%%% このDBの結果は「T1 の後に T2 を丸ごと実行した」場合と同じなので、
%%% この事例に関しては直列化可能な側に寄っている。文の途中まで進んで
%%% から待たされても、ロックを取るのは局所領域に書く前なので、
%%% やり直しても捨てるものが無い。だから丸ごとやり直せる。
%%%
%%% ただし**トランザクション全体が直列化可能になるわけではない**。
%%% 同じトランザクションの前の文は、古いスナップショットで書いた
%%% ままやり直されない。
%%%===================================================================
pmp_write_predicate_rc(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RC),
        ok = q(T2, ?RC),
        {ok, 2} = q(T1, "UPDATE test SET value = value + 10"),
        %% T2 は id=2 (値20) を消そうとして、T1 が持っている行で止まる
        B = async(fun() -> q(T2, "DELETE FROM test WHERE value = 20") end),
        ?assertEqual(timeout, await(B, 300)),
        ok = q(T1, "COMMIT"),
        %% 走査からやり直すので、いま 20 の行(id=1)を消す。
        %% PostgreSQL はここで 0 件。消したはずの 20 が残る
        ?assertEqual({ok, {ok, 1}}, await(B, 5000)),
        ok = q(T2, "COMMIT"),
        ?assertMatch({ok, _, []}, q_ro(T1, "SELECT * FROM test WHERE value = 20")),
        ?assertEqual([[2, 30]], dump(T1))
    end.

pmp_write_predicate_rr(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RR),
        ok = q(T2, ?RR),
        {ok, 2} = q(T1, "UPDATE test SET value = value + 10"),
        B = async(fun() -> q(T2, "DELETE FROM test WHERE value = 20") end),
        ?assertEqual(timeout, await(B, 300)),
        ok = q(T1, "COMMIT"),
        %% 読み直せないので断る
        ?assertEqual({ok, {error, serialization_failure}}, await(B, 5000)),
        ?assertEqual([[1, 20], [2, 30]], dump(T1))
    end.

%%%===================================================================
%%% P4 (Lost Update)
%%%   READ COMMITTED  防がれない(T2 が T1 の更新を踏み潰す)
%%%   REPEATABLE READ 防がれる(直列化エラー)
%%%===================================================================
p4_lost_update_rc(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RC),
        ok = q(T2, ?RC),
        ?assertEqual([[1, 10]], one(T1, 1)),
        ?assertEqual([[1, 10]], one(T2, 1)),
        {ok, 1} = q(T1, "UPDATE test SET value = 11 WHERE id = 1"),
        B = async(fun() -> q(T2, "UPDATE test SET value = 11 WHERE id = 1") end),
        ?assertEqual(timeout, await(B, 300)),
        ok = q(T1, "COMMIT"),
        %% 断らずに通る。これが「防がれない」ということ
        ?assertEqual({ok, {ok, 1}}, await(B, 5000)),
        ok = q(T2, "COMMIT")
    end.

p4_lost_update_rr(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RR),
        ok = q(T2, ?RR),
        ?assertEqual([[1, 10]], one(T1, 1)),
        ?assertEqual([[1, 10]], one(T2, 1)),
        {ok, 1} = q(T1, "UPDATE test SET value = 11 WHERE id = 1"),
        B = async(fun() -> q(T2, "UPDATE test SET value = 11 WHERE id = 1") end),
        ?assertEqual(timeout, await(B, 300)),
        ok = q(T1, "COMMIT"),
        ?assertEqual({ok, {error, serialization_failure}}, await(B, 5000))
    end.

%%%===================================================================
%%% G-single (Read Skew)
%%%   READ COMMITTED  防がれない(1つのトランザクションが食い違う2つの
%%%                   時点を見る)
%%%   REPEATABLE READ 防がれる
%%%===================================================================
g_single_read_skew_rc(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RC),
        ok = q(T2, ?RC),
        ?assertEqual([[1, 10]], one(T1, 1)),
        ?assertEqual([[1, 10]], one(T2, 1)),
        ?assertEqual([[2, 20]], one(T2, 2)),
        {ok, 1} = q(T2, "UPDATE test SET value = 12 WHERE id = 1"),
        {ok, 1} = q(T2, "UPDATE test SET value = 18 WHERE id = 2"),
        ok = q(T2, "COMMIT"),
        %% T1 は id=1 を 10 の時点で、id=2 を 18 の時点で見る。食い違っている
        ?assertEqual([[2, 18]], one(T1, 2)),
        ok = q(T1, "COMMIT")
    end.

g_single_read_skew_rr(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RR),
        ok = q(T2, ?RC),
        ?assertEqual([[1, 10]], one(T1, 1)),
        {ok, 1} = q(T2, "UPDATE test SET value = 12 WHERE id = 1"),
        {ok, 1} = q(T2, "UPDATE test SET value = 18 WHERE id = 2"),
        ok = q(T2, "COMMIT"),
        %% 同じ時点しか見えない
        ?assertEqual([[2, 20]], one(T1, 2)),
        ok = q(T1, "COMMIT")
    end.

%%%===================================================================
%%% G2-item (Write Skew) — REPEATABLE READ でも**防がれない**
%%%
%%% 互いに相手が読んだ行を書く。書く行が重ならないので、行ロックにも
%%% 衝突検査にも引っかからない。防ぐには SSI か述語ロックが要る。
%%%===================================================================
g2_item_write_skew_rr(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RR),
        ok = q(T2, ?RR),
        ?assertMatch({ok, _, [[1, 10], [2, 20]]}, q(T1, in_1_2())),
        ?assertMatch({ok, _, [[1, 10], [2, 20]]}, q(T2, in_1_2())),
        {ok, 1} = q(T1, "UPDATE test SET value = 11 WHERE id = 1"),
        {ok, 1} = q(T2, "UPDATE test SET value = 21 WHERE id = 2"),
        ok = q(T1, "COMMIT"),
        %% 両方通る = 防がれていない
        ok = q(T2, "COMMIT"),
        ?assertEqual([[1, 11], [2, 21]], dump(T1))
    end.

%%%===================================================================
%%% G2 (Anti-Dependency Cycles) — REPEATABLE READ でも**防がれない**
%%%===================================================================
g2_anti_dependency_rr(_) ->
    fun() ->
        {T1, T2} = two(),
        ok = q(T1, ?RR),
        ok = q(T2, ?RR),
        ?assertMatch({ok, _, []}, q(T1, mod3())),
        ?assertMatch({ok, _, []}, q(T2, mod3())),
        {ok, _} = q(T1, "INSERT INTO test (id, value) VALUES (3, 30)"),
        {ok, _} = q(T2, "INSERT INTO test (id, value) VALUES (4, 42)"),
        ok = q(T1, "COMMIT"),
        ok = q(T2, "COMMIT"),
        ?assertEqual([[3, 30], [4, 42]], dump_mod3())
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

in_1_2() -> "SELECT * FROM test WHERE id IN (1, 2) ORDER BY id".
mod3()   -> "SELECT * FROM test WHERE mod(value, 3) = 0 ORDER BY id".

%% トランザクションの中から全行を読む
rows(C) ->
    {ok, _, R} = q(C, "SELECT * FROM test ORDER BY id"),
    R.

one(C, Id) ->
    {ok, _, R} = q(C, "SELECT * FROM test WHERE id = " ++ integer_to_list(Id)),
    R.

%% トランザクションの外から現在の中身を読む
dump(C) ->
    ok = q(C, "BEGIN READ ONLY"),
    R = rows(C),
    ok = q(C, "COMMIT"),
    R.

%% トランザクションの外から1文だけ読む
q_ro(C, Sql) ->
    ok = q(C, "BEGIN READ ONLY"),
    R = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

dump_mod3() ->
    C = connect(),
    ok = q(C, "BEGIN READ ONLY"),
    {ok, _, R} = q(C, mod3()),
    ok = q(C, "COMMIT"),
    R.

two() -> {seeded(), connect()}.

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE test (id INTEGER, value INTEGER)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO test (id, value) VALUES (1, 10)"),
    {ok, _} = q(C, "INSERT INTO test (id, value) VALUES (2, 20)"),
    ok = q(C, "COMMIT"),
    C.

async(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    _ = spawn(fun() -> Parent ! {Ref, Fun()} end),
    Ref.

await(Ref, Ms) ->
    receive {Ref, R} -> {ok, R}
    after Ms -> timeout
    end.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
