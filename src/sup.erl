%%%-------------------------------------------------------------------
%%% @doc
%%% トップレベルのスーパーバイザ。
%%%
%%% 起動順に意味がある:
%%%   sys_tbl_mng     カタログ(DETS)。他のサーバがテーブル定義を引く
%%%   data_buffer     バッファプール。データファイルを開く
%%%   log_util        REDOログ。リカバリが読む
%%%   simple_db_server ストレージエンジン。起動時にインデックスを再構築する
%%%   tx_mng          初期化でrecover:recover/0を呼ぶため、上記が揃った後
%%%   lock_mng        ロック管理
%%%   commit_latch    DDLと走査を排他にするラッチ
%%%   snapshot_mng    スナップショットと undo
%%%   query_exec_sup  クライアント接続ごとのquery_execを起こす
%%%
%%% rest_for_oneにしているのは、下位のサーバが落ちたときに、その状態に
%%% 依存している上位のサーバも作り直す必要があるため。例えばdata_bufferが
%%% 落ちるとバッファ上のページが失われるので、simple_db_serverの
%%% インデックスも再構築しないと整合しない。
%%% @end
%%%-------------------------------------------------------------------
-module(sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    SupFlags = #{strategy => rest_for_one, intensity => 3, period => 5},
    ChildSpecs = [worker(sys_tbl_mng),
                  worker(data_buffer),
                  worker(log_util),
                  worker(simple_db_server),
                  worker(tx_mng),
                  worker(lock_mng),
                  worker(commit_latch),
                  worker(snapshot_mng),
                  #{id => query_exec_sup,
                    start => {query_exec_sup, start_link, []},
                    restart => permanent,
                    shutdown => 5000,
                    type => supervisor,
                    modules => [query_exec_sup]}],
    {ok, {SupFlags, ChildSpecs}}.

%% DETSやディスクログを閉じる時間が要るので、brutal_killではなく
%% 猶予を与えて終了させる。
worker(Module) ->
    #{id => Module,
      start => {Module, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [Module]}.
