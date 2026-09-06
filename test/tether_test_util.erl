-module(tether_test_util).
-export([tmpdir/0, rmrf/1, corrupt_byte/2, truncate_file/2, read/1]).

tmpdir() ->
    D = filename:join(["/tmp", "tether_test",
                       integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(D, "x")),
    D.

rmrf(D) ->
    _ = [file:delete(F) || F <- filelib:wildcard(filename:join(D, "*"))],
    _ = file:del_dir(D),
    ok.

read(P) -> {ok, B} = file:read_file(P), B.

corrupt_byte(P, Off) ->
    <<H:Off/binary, B:8, T/binary>> = read(P),
    ok = file:write_file(P, <<H/binary, (B bxor 16#FF):8, T/binary>>).

truncate_file(P, N) ->
    Bin = read(P),
    ok = file:write_file(P, binary:part(Bin, 0, N)).

-include_lib("eunit/include/eunit.hrl").

%% eunit は test/ の全モジュールを走らせるので、
%% 試験を1つも持たないモジュールがあると「中止」として報告される。
tmpdir_is_unique_test() ->
    A = tmpdir(), B = tmpdir(),
    ?assertNotEqual(A, B),
    ?assert(filelib:is_dir(A)),
    rmrf(A), rmrf(B).
