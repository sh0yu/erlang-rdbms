%%%-------------------------------------------------------------------
%%% スナップショットと undo。
%%%
%%% ここは並行性の要なので、単体で先に確かめる。実際の走査と混ぜると
%%% 「どちらが壊れているか」が分からなくなる。
%%%-------------------------------------------------------------------
-module(snapshot_mng_tests).

-include_lib("eunit/include/eunit.hrl").

snapshot_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun no_undo_means_empty_overlay/1,
      fun overlay_restores_old_value/1,
      fun overlay_hides_rows_added_later/1,
      fun oldest_undo_wins/1,
      fun other_tables_are_not_mixed_in/1,
      fun newer_snapshot_sees_more/1,
      fun undo_is_trimmed_when_nobody_needs_it/1,
      fun undo_is_kept_while_a_reader_needs_it/1,
      fun too_old_snapshot_is_refused/1]}.

setup() ->
    application:set_env(transaction_db, max_undo, 3),
    {ok, Pid} = snapshot_mng:start_link(),
    Pid.

cleanup(Pid) ->
    application:unset_env(transaction_db, max_undo),
    gen_server:stop(Pid),
    ok.

no_undo_means_empty_overlay(_) ->
    fun() ->
        Snap = snapshot_mng:acquire(),
        ?assertEqual({ok, #{}}, snapshot_mng:overlay(Snap, t))
    end.

%% 変更前の値へ巻き戻せること。
overlay_restores_old_value(_) ->
    fun() ->
        Snap = snapshot_mng:acquire(),
        commit(#{{t, 1} => {row, [old]}}),
        ?assertEqual({ok, #{1 => {row, [old]}}}, snapshot_mng:overlay(Snap, t))
    end.

%% スナップショットより後に追加された行は見せない。
overlay_hides_rows_added_later(_) ->
    fun() ->
        Snap = snapshot_mng:acquire(),
        commit(#{{t, 9} => absent}),
        ?assertEqual({ok, #{9 => deleted}}, snapshot_mng:overlay(Snap, t))
    end.

%% 同じ行が何度も変わったら、いちばん古い undo の値がその時点の値。
oldest_undo_wins(_) ->
    fun() ->
        Snap = snapshot_mng:acquire(),
        commit(#{{t, 1} => {row, [v1]}}),   % 版1: 変更前は v1
        commit(#{{t, 1} => {row, [v2]}}),   % 版2: 変更前は v2
        ?assertEqual({ok, #{1 => {row, [v1]}}}, snapshot_mng:overlay(Snap, t))
    end.

other_tables_are_not_mixed_in(_) ->
    fun() ->
        Snap = snapshot_mng:acquire(),
        commit(#{{t, 1} => {row, [a]}, {u, 1} => {row, [b]}}),
        ?assertEqual({ok, #{1 => {row, [a]}}}, snapshot_mng:overlay(Snap, t)),
        ?assertEqual({ok, #{1 => {row, [b]}}}, snapshot_mng:overlay(Snap, u))
    end.

%% 後から取ったスナップショットは、それまでのコミットを見る。
newer_snapshot_sees_more(_) ->
    fun() ->
        Old = snapshot_mng:acquire(),
        S1 = commit(#{{t, 1} => {row, [v1]}}),
        ok = snapshot_mng:publish(S1),
        New = snapshot_mng:acquire(),
        %% 古い方は巻き戻しが要る。新しい方は要らない
        ?assertEqual({ok, #{1 => {row, [v1]}}}, snapshot_mng:overlay(Old, t)),
        ?assertEqual({ok, #{}}, snapshot_mng:overlay(New, t))
    end.

undo_is_trimmed_when_nobody_needs_it(_) ->
    fun() ->
        _ = commit(#{{t, 1} => {row, [a]}}),
        _ = commit(#{{t, 2} => {row, [b]}}),
        ?assertMatch(#{undo := 0}, snapshot_mng:status())
    end.

undo_is_kept_while_a_reader_needs_it(_) ->
    fun() ->
        Snap = snapshot_mng:acquire(),
        _ = commit(#{{t, 1} => {row, [a]}}),
        ?assertMatch(#{undo := 1}, snapshot_mng:status()),
        ok = snapshot_mng:release(Snap),
        ?assertMatch(#{undo := 0}, snapshot_mng:status())
    end.

%% 上限を超えたら古い undo を捨て、必要としていた読み手を断る。
%% 黙って古い値を返すよりは断る方がよい。
too_old_snapshot_is_refused(_) ->
    fun() ->
        Snap = snapshot_mng:acquire(),
        _ = [commit(#{{t, I} => {row, [I]}}) || I <- lists:seq(1, 10)],
        ?assertEqual({error, snapshot_too_old}, snapshot_mng:overlay(Snap, t)),
        %% 新しく取り直せば読める
        New = snapshot_mng:acquire(),
        ?assertEqual({ok, #{}}, snapshot_mng:overlay(New, t))
    end.

commit(Undo) ->
    Seq = snapshot_mng:commit(Undo),
    ok = snapshot_mng:publish(Seq),
    Seq.
