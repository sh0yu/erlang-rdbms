%%%-------------------------------------------------------------------
%%% sqllogictest のコーパスを流す。
%%%
%%% コーパス(数MB)は他所のものなので、この repo には入れていない。
%%%
%%%   bin/slt-fetch      test/slt/ に落とす
%%%   rebar3 eunit       置いてあれば流す。無ければ何もしない
%%%
%%% 見るのは **wrong が0であること**。
%%%
%%%   pass        期待どおり
%%%   wrong       実行できたが答えが違う ← バグ。0でなければならない
%%%   unsupported 構文や機能が無くて実行できない ← 足りないものの一覧
%%%
%%% unsupported は「まだ作っていない」なので落とさない。落とすべきなのは
%%% 「作ったつもりで間違っている」ほうで、それが wrong。
%%%-------------------------------------------------------------------
-module(slt_tests).

-include_lib("eunit/include/eunit.hrl").

%% ここまでは通る、という下限。下回ったら退行。
-define(MIN_PASS, 10700).

slt_test_() ->
    case filelib:wildcard("test/slt/*.test") of
        [] ->
            %% コーパスが無い。bin/slt-fetch を実行すれば流れる
            [];
        _ ->
            {setup, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
             fun(_) -> [{timeout, 600, fun corpus_has_no_wrong_answers/0}] end}
    end.

corpus_has_no_wrong_answers() ->
    #{pass := Pass, wrong := Wrong, unsupported := Unsup, wrongs := Wrongs} =
        slt:summary(slt:run_dir("test/slt")),
    ?debugFmt("sqllogictest: ~p pass, ~p wrong, ~p unsupported", [Pass, Wrong, Unsup]),
    %% 答えが違うものが1つでもあれば、それはバグ
    ?assertEqual([], lists:sublist(Wrongs, 10)),
    ?assertEqual(0, Wrong),
    %% 通る数が減っていたら退行
    ?assert(Pass >= ?MIN_PASS).
