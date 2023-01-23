-module(main).

-behavior(application).

-include("../include/hyperparams.hrl").

-compile([export_all]).

start(normal, _Args) ->
    NumNodes = application:get_env(chord, num_nodes, ?NUM_NODES),
    NumRequests = application:get_env(chord, num_nodes, ?NUM_REQUESTS),
    HashNumBits = application:get_env(chord, hash_num_bits, ?HASH_NUM_BITS),

    NumBackupSuccs = application:get_env(chord, num_backup_succs, ?NUM_BACKUP_SUCCS),
    TempFailProb = application:get_env(chord, temp_fail_prob, ?NODE_TEMP_FAILURE_PROB),
    PermFailProb = application:get_env(chord, perm_fail_prob, ?NODE_PERM_FAILURE_PROB),
    % Start the Chord API.
    chord_api:start_link({TempFailProb,
                          PermFailProb,
                          NumBackupSuccs,
                          NumRequests,
                          HashNumBits}),
    % Spawn nodes and simulate lookups.
    spawn_and_join_chord_nodes(NumNodes),
    simulate_lookups(),
    {ok, self()}.

spawn_and_join_chord_nodes(0) ->
    ok;
spawn_and_join_chord_nodes(N) ->
    chord_api:spawn_and_join(),
    spawn_and_join_chord_nodes(N - 1).

simulate_lookups() ->
    ok.

terminate(_Reason, _State, _Data) ->
    init:stop().
