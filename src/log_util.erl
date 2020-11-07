-module(log_util).
-behavior(gen_server).
-compile(export_all).
-record(st, {
    writer
}).
-record(file, {
    file_path,
    fd,
    inode,
    last_check
}).
-record(log_entry, {
    level,
    pid,
    msg,
    msg_id,
    time_stamp
}).

-include_lib("kernel/include/file.hrl").
-include("../include/simple_db_server.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    {ok, _Log} = disk_log:open([{name, redo_log}]),
    {ok, Writer} = writer_init(),
    {ok, #st{writer=Writer}}.

writer_init() ->
    FilePath = "./query.log",
    Opts = [append, raw],
    case filelib:ensure_dir(FilePath) of
        ok ->
            case file:open(FilePath, Opts) of
                {ok, Fd} ->
                    case file:read_file_info(FilePath) of
                        {ok, FInfo} ->
                            {ok, #file{
                                file_path = FilePath,
                                fd = Fd,
                                inode = FInfo#file_info.inode,
                                last_check = os:timestamp()
                            }};
                        FInfoError ->
                            ok = file:close(Fd),
                            FInfoError
                    end;
                OpenError ->
                    OpenError
            end;
        EnsureDirError ->
            EnsureDirError
    end.

log(Entry) ->
    gen_server:call(?MODULE, {log, Entry}).

handle_call({log, Entry}, _From, #st{writer=Writer}=State) ->
    ok = write(Entry, Writer),
    {reply, ok, State}.

handle_cast(_Msg, _St) ->
    {stop, error}.

write(Entry, Writer) ->
    #log_entry{
        level = Level,
        pid = Pid,
        msg = Msg,
        msg_id = MsgId,
        time_stamp = TimeStamp
    } = Entry,
    Args = {
        Level,
        TimeStamp,
        node(),
        Pid,
        MsgId
    },
    Data = format(Args),
    ok = file:write(Writer#file.fd, [Data, Msg, "\n"]).

format({Level, Timestamp, Node, Pid, MsgId}) ->
    "[" ++ Level ++ "] " ++ Timestamp ++ " " ++ atom_to_list(Node) ++ " " ++ pid_to_list(Pid) ++ " " ++ MsgId.

redo_log_write(RedoLog) ->
    ok = disk_log:log(redo_log, RedoLog).

redo_log_chunk(Cont) ->
    disk_log:chunk(redo_log, Cont).

redo_log_put_checkpoint() ->
    ok = disk_log:log(redo_log, #redo_log{timestamp=erlang:system_time(), action=checkpoint}).

redo_log_truncate() ->
    ok = disk_log:truncate(redo_log).