-module(chord_api).

-behaviour(gen_server).

-include("../include/hyperparams.hrl").

-compile([export_all]).

-record(state,
        {num_backup_succs,
         backup_succs,
         num_nodes,
         temp_fail_prob,
         perm_fail_prob,
         num_requests_to_make,
         hash_num_bits}).

% Start API %
stop() ->
    gen_server:call({global, chord_api}, stop).

start_link(Args) ->
    gen_server:start_link({global, chord_api}, ?MODULE, Args, []).

spawn_and_join() ->
    % Spawns a new node and calls "join" on the first "anchor" node
    % that responds to our "ping".
    gen_server:cast({global, chord_api}, {spawn_and_join}).

% End API %

init({TempFailProb, PermFailProb, NumBackupSuccs, NumRequestsToMake, HashNumBits}) ->
    self() ! {start_chord_sup},
    {ok,
     #state{num_requests_to_make = NumRequestsToMake,
            num_backup_succs = NumBackupSuccs,
            backup_succs = [],
            num_nodes = 0,
            temp_fail_prob = TempFailProb,
            perm_fail_prob = PermFailProb,
            hash_num_bits = HashNumBits}}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call({find_successor, P, NodeH}, _From, State) ->
    Return = gen_server:call(P, {find_successor, NodeH}),
    {reply, Return, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({spawn_and_join},
            State =
                #state{num_requests_to_make = NumRequestsToMake,
                       num_backup_succs = NumBackupSuccs,
                       backup_succs = BackupSuccs,
                       num_nodes = NumNodes,
                       temp_fail_prob = TempFailProb,
                       perm_fail_prob = PermFailProb,
                       hash_num_bits = HashNumBits}) ->
    % Spawn new Chord node by associating a hash with it.
    NextId = NumNodes + 1,
    NodeHash = erlang:phash2(NextId, round(math:pow(2, HashNumBits))),
    {ok, NodePid} =
        supervisor:start_child({global, chord_sup},
                               [NextId,
                                NodeHash,
                                TempFailProb,
                                PermFailProb,
                                NumRequestsToMake,
                                HashNumBits]),

    % If this is one of the first 'R' nodes being added to the
    % network, add this to the list of backup successors.
    if NextId < NumBackupSuccs + 1 ->
           NewBackupSuccs = [{NodeHash, NodePid}] ++ BackupSuccs;
       true ->
           NewBackupSuccs = BackupSuccs
    end,

    if NextId == 1 ->
        chord_node:create({NodePid});
    true ->
        % Join Chord node into ring.
        {AnchorNodeHash, AnchorNodePid} = get_anchor_node(BackupSuccs),
        chord_node:join({NodePid, {AnchorNodeHash, AnchorNodePid}})
    end,

    {noreply, State#state{backup_succs = NewBackupSuccs, num_nodes = NumNodes + 1}};
handle_cast(stop, State=#state{num_nodes = NumNodes}) ->
    [supervisor:terminate({global, chord_sup}, list_to_atom(lists:flatten(
        io_lib:format("~B", [Id])))) || Id <- lists:seq(1, NumNodes)],
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

get_anchor_node(BackupSuccs) ->
    % Get the PID of the first responsive node (from the list of
    % backup_succs).
    AnchorNodePids =
        lists:filtermap(fun({SuccHash, SuccPid}) ->
                           Result = gen_server:call(SuccPid, {ping}),
                           case Result of
                               ok -> {true, {SuccHash, SuccPid}};
                               timeout -> false
                           end
                        end,
                        BackupSuccs),
    case length(AnchorNodePids) of
        0 ->
            no_node_found;
        _ ->
            lists:nth(1, AnchorNodePids)
    end.

handle_info({start_chord_sup}, State) ->
    {ok, Pid} = chord_sup:start_link({global, chord_sup}, {chord_node, start_link, []}),
    link(Pid),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
