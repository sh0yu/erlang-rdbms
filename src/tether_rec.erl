%%%-------------------------------------------------------------------
%%% @doc
%%% ログレコードの表現。**このモジュールは純粋である。** ファイルを触らない。
%%%
%%% 版面:
%%%
%%%     <<Magic:32, Len:32, Crc:32, Payload:Len/binary>>
%%%
%%%     Magic  "TETH"。ここが合わなければ、そこから先はレコードではない
%%%     Len    Payload のバイト数。?MAX_PAYLOAD を超えていたら壊れている
%%%     Crc    <<Len:32, Payload/binary>> の crc32。
%%%            長さの欄も検査対象に含める
%%%
%%% ログは追記のみで、電源が落ちれば末尾が途中で切れる。**切れた末尾は
%%% 異常ではなく正常な状態である。** 復旧は「どこまでが健全か」を
%%% 知る必要があり、それがこのモジュールの仕事になる。
%%%
%%% 区別しなければならないものが2つある。
%%%
%%%   truncated  末尾が足りない。電源断で普通に起きる。
%%%              そこまでのレコードは有効
%%%   corrupt    長さは足りているのに中身が壊れている。
%%%              ディスクの故障か、書き込みが千切れたか。
%%%              **これは黙って切り捨ててはいけない**
%%%
%%% 両方を「読めるところまで読む」で済ませると、ディスクの故障が
%%% 「少しデータが古いだけ」に化けて表に出てこなくなる。
%%%
%%% ただし区別しきれない場合が一つある。**最後のレコードの長さの欄が
%%% 壊れて、実際より大きい値になった場合**、本体が足りなくなるので
%%% truncated として返る。これは安全である。どちらであっても取るべき
%%% 行動は同じ(末尾を捨てる)だから。途中のレコードで同じことが起きた
%%% 場合は、続きのバイトが存在するので CRC が合わず bad_crc になる。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_rec).

-export([encode/1, encode_all/1, scan/1, header_size/0, max_payload/0]).

-export_type([status/0]).

-define(MAGIC, 16#54455448).           % "TETH"
-define(HDR, 12).
%% 1レコードの上限。長さの欄が壊れて巨大な値になったとき、
%% それを信じて確保しに行かないための歯止め。
-define(MAX_PAYLOAD, 16#1000000).      % 16 MiB

-type status() :: complete
                | truncated
                | {corrupt, bad_magic | bad_crc | too_long}.

-spec header_size() -> pos_integer().
header_size() -> ?HDR.

-spec max_payload() -> pos_integer().
max_payload() -> ?MAX_PAYLOAD.

%%----------------------------------------------------------------------
%% @doc 1レコードに包む。
%%----------------------------------------------------------------------
-spec encode(binary()) -> binary().
encode(Payload) when byte_size(Payload) =< ?MAX_PAYLOAD ->
    Len = byte_size(Payload),
    <<?MAGIC:32, Len:32, (erlang:crc32(<<Len:32, Payload/binary>>)):32,
      Payload/binary>>.

-spec encode_all([binary()]) -> binary().
encode_all(Payloads) ->
    iolist_to_binary([encode(P) || P <- Payloads]).

%%----------------------------------------------------------------------
%% @doc 先頭から読めるだけ読む。
%%
%% 返り値の Offset は「健全な部分の終わり」。復旧はここでファイルを
%% 切り詰めれば、以後の追記が健全なログを作る。
%%
%% 途中で止まった理由を status() で返す。**呼び出し側が
%% truncated と corrupt を区別できることが重要**で、
%% ここで判断して握り潰さない。
%%----------------------------------------------------------------------
-spec scan(binary()) -> {[binary()], non_neg_integer(), status()}.
scan(Bin) ->
    scan(Bin, 0, []).

scan(<<>>, Off, Acc) ->
    {lists:reverse(Acc), Off, complete};
scan(Bin, Off, Acc) when byte_size(Bin) < ?HDR ->
    %% 表頭すら揃っていない。電源断の末尾として普通にありうる。
    {lists:reverse(Acc), Off, truncated};
scan(<<M:32, _/binary>>, Off, Acc) when M =/= ?MAGIC ->
    {lists:reverse(Acc), Off, {corrupt, bad_magic}};
scan(<<_:32, Len:32, _/binary>>, Off, Acc) when Len > ?MAX_PAYLOAD ->
    %% 長さの欄が壊れている。この値を信じて確保しに行かない。
    {lists:reverse(Acc), Off, {corrupt, too_long}};
scan(<<_:32, Len:32, Crc:32, Rest/binary>> = Bin, Off, Acc) ->
    case Rest of
        <<Payload:Len/binary, Tail/binary>> ->
            case erlang:crc32(<<Len:32, Payload/binary>>) of
                Crc -> scan(Tail, Off + ?HDR + Len, [Payload | Acc]);
                _   -> {lists:reverse(Acc), Off, {corrupt, bad_crc}}
            end;
        _ ->
            %% 本体が足りない。末尾が切れている。
            _ = Bin,
            {lists:reverse(Acc), Off, truncated}
    end.
