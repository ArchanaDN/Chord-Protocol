-module(chord_node).

-behaviour(gen_server).

-compile([export_all]).

-record(state,
        {hash,
         pid,
         finger_table,
         next_finger_to_check = 1,
         predecessor,
         successor,
         num_requests_to_make,
         num_requests_made = 0,
         temp_fail_prob,
         num_hops = 0,
         perm_fail_prob,
         hash_num_bits,
         timer_ref}).

% Start API %

stop(Hash) ->
    io:format("Stopping"),
    gen_server:call({global, Hash}, stop).

start_link(NodeId, H, TempFailProb, PermFailProb, NumRequestsToMake, HashNumBits) ->
    {ok, Pid} =
        gen_server:start_link({global,
                               list_to_atom(lists:flatten(
                                                io_lib:format("~B", [NodeId])))},
                              ?MODULE,
                              {H, TempFailProb, PermFailProb, NumRequestsToMake, HashNumBits},
                              []),
    {ok, Pid}.

create({NodeP}) ->
    gen_server:call(NodeP, {create}).

join({NodeP, {NewH, NewP}}) ->
    gen_server:call(NodeP, {join, {NewH, NewP}}).

stabilize(Pid) ->
    gen_server:cast(Pid, {stabilize}).

perform_lookup({Pid, HashNumBits}) ->
    % Generate random hash
    RandH =
        erlang:phash2(
            rand:uniform(1000), round(math:pow(2, HashNumBits))),
    io:format("RandH = ~p~n", [RandH]),
    {TgtH, TgtP} = gen_server:call(Pid, {find_successor, external, RandH}),
    io:format("LOOKUP: ~p found in Node ~p~n", [RandH, TgtH]).

fix_fingers(Pid) ->
    PingReply = gen_server:call(Pid, {ping}),
    case PingReply of
        timeout ->
            nothing;
        _ ->
            gen_server:cast(Pid, {fix_fingers})
    end.

check_predecessor(Pid) ->
    PingReply = gen_server:call(Pid, {ping}),
    case PingReply of
        timeout ->
            nothing;
        _ ->
            gen_server:cast(Pid, {check_predecessor})
    end.

% End API %

init({Hash, TempFailProb, PermFailProb, NumRequestsToMake, HashNumBits}) ->
    % Start timers
    {ok, TimerRef} =
        timer:apply_interval(1000, ?MODULE, perform_lookup, [{self(), HashNumBits}]),
    timer:apply_interval(10, ?MODULE, stabilize, [self()]),
    timer:apply_interval(10, ?MODULE, fix_fingers, [self()]),
    timer:apply_interval(10, ?MODULE, check_predecessor, [self()]),

    {ok,
     #state{hash = Hash,
            pid = self(),
            num_requests_to_make = NumRequestsToMake,
            temp_fail_prob = TempFailProb,
            perm_fail_prob = PermFailProb,
            num_hops = 0,
            hash_num_bits = HashNumBits,
            finger_table = dict:new(),
            timer_ref = TimerRef}}.

handle_call({ping}, _From, State) ->
    {reply, ok, State};
handle_call({create},
            _From,
            State =
                #state{finger_table = FingerTable,
                       hash = NodeH,
                       pid = NodeP}) ->
    io:format("chord_node: create called on Node ~p~n", [NodeH]),
    {reply, ok, State#state{successor = {NodeH, NodeP}}};
handle_call({join, {H, P}},
            _From,
            State =
                #state{hash = NodeH,
                       pid = NodeP,
                       finger_table = FingerTable,
                       hash_num_bits = HashNumBits}) ->
    io:format("chord_node: join called on ~p with param ~p~n", [NodeH, H]),
    % {SuccH, SuccP} = gen_server:call({global, chord_api}, {find_successor, P, NodeH}),
    {SuccH, SuccP} = gen_server:call(P, {find_successor, internal, NodeH}),
    io:format("chord_node: Node ~p is now to Node ~p's successor~n", [SuccH, NodeH]),
    % NewFingerTable = dict:store(1, {SuccH, SuccP}, FingerTable),
    {reply, ok, State#state{successor = {SuccH, SuccP}}};
handle_call({get_predecessor}, _From, State = #state{predecessor = Predecessor}) ->
    {reply, Predecessor, State};
handle_call({find_successor, InternalOrExternal, H},
            _From,
            State =
                #state{hash = NodeH,
                       pid = NodeP,
                       successor = Successor,
                       finger_table = FingerTable,
                       hash_num_bits = HashNumBits,
                       num_requests_made = NumRequestsMade,
                       num_requests_to_make = NumRequestsToMake,
                       timer_ref = TimerRef}) ->
    find_successor(InternalOrExternal, H, State);
handle_call({notify, {H, P}},
            _From,
            State = #state{predecessor = Predecessor, hash = NodeH}) ->
    io:format("chord_node: notify called on ~p with param ~p~n", [NodeH, H]),
    case Predecessor of
        undefined ->
            {reply, ok, State#state{predecessor = {H, P}}};
        {PredH, PredP} ->
            if (H >= PredH) and (H =< NodeH) ->
                   {reply, ok, State#state{predecessor = {H, P}}};
               true ->
                   {reply, ok, State}
            end
    end,
    {reply, ok, State}.

handle_cast({stabilize},
            State =
                #state{successor = Successor,
                       hash = NodeH,
                       pid = Pid}) ->
    case Successor of
        undefined ->
            {noreply, State};
        {SuccH, SuccP} ->
            case SuccP =:= Pid of
                true ->
                    {noreply, State};
                false ->
                    io:format("~p and ~p~n", [Pid, SuccP]),
                    MaybeSucc = gen_server:call(SuccP, {get_predecessor}),
                    case MaybeSucc =:= undefined of
                        true ->
                            {noreply, State};
                        false ->
                            {MaybeSuccH, MaybeSuccP} = MaybeSucc,
                            if (MaybeSuccH >= NodeH) and (MaybeSuccH =< SuccH) ->
                                   {noreply, State#state{successor = {MaybeSuccH, MaybeSuccP}}};
                               true ->
                                   {noreply, State}
                            end
                    end
            end;
        _ ->
            {noreply, State}
    end;
handle_cast({fix_fingers},
            State =
                #state{next_finger_to_check = NextFingerToCheck,
                       hash_num_bits = HashNumBits,
                       hash = NodeH,
                       pid = NodeP,
                       finger_table = FingerTable}) ->
    NewNextFingerToCheck = (NextFingerToCheck + 1) rem HashNumBits,
    {reply, {FingerH, FingerP}, NewState} =
        find_successor(internal, NodeH + round(math:pow(2, NewNextFingerToCheck - 1)), State),
    % gen_server:call(NodeP,
    %                 {find_successor,
    %                  internal,
    %                  NodeH + round(math:pow(2, NewNextFingerToCheck - 1))}),
    NewFingerTable = dict:store(NewNextFingerToCheck, {FingerH, FingerP}, FingerTable),
    {noreply, NewState};
handle_cast({check_predecessor}, State = #state{predecessor = Predecessor}) ->
    case Predecessor of
        undefined -> {noreply, State};
        {PredH, PredP} ->
            PingReply = gen_server:call(PredP, {ping}),
            case PingReply of
                timeout ->
                    {noreply, State#state{predecessor = undefined}};
                ok ->
                    {noreply, State}
            end
    end.

terminate(_Reason, _State) ->
    ok.

% Internal functions
succ(FingerTable) ->
    dict:find(1, FingerTable).

find_successor(InternalOrExternal,
               H,
               State =
                   #state{finger_table = FingerTable,
                          num_requests_made = NumRequestsMade,
                          num_requests_to_make = NumRequestsToMake,
                          pid = NodeP,
                          num_hops = NumHops,
                          hash = NodeH,
                          timer_ref = TimerRef,
                          successor = Successor,
                          hash_num_bits = HashNumBits}) ->
    io:format("chord_node: find_successor called on ~p with param ~p~n", [NodeH, H]),
    case InternalOrExternal of
        internal ->
            NewNumRequestsMade = NumRequestsMade;
        external ->
            NewNumRequestsMade = NumRequestsMade + 1,
            case NewNumRequestsMade >= NumRequestsToMake of
                true ->
                    timer:cancel(TimerRef),
                    gen_server:cast({global, chord_api}, stop);
                false ->
                    nothing
            end
    end,

    {SuccH, SuccP} = Successor,
    if (H >= NodeH) and (H =< SuccH) ->
           {reply, {SuccH, SuccP}, State#state{num_requests_made = NewNumRequestsMade}};
       true ->
           ClosestPrecedingEntryIdx =
               util:first(
                   lists:seq(HashNumBits, 1, -1),
                   fun(FingerIdx) ->
                      FingerLookup = dict:find(FingerIdx, FingerTable),
                      case FingerLookup of
                          {ok, {FingerH, _}} -> (FingerH =< H) and (H >= NodeH);
                          error -> false
                      end
                   end,
                   table_empty),

           case ClosestPrecedingEntryIdx of
               table_empty ->
                   % If table is empty, just return self
                   {reply, {NodeH, NodeP}, State#state{num_requests_made = NewNumRequestsMade}};
               _ ->
                   {ok, {FingerH, FingerP}} = dict:find(ClosestPrecedingEntryIdx, FingerTable),
                   Result = gen_server:call(FingerP, {find_successor, H}),
                   {reply, {FingerH, FingerP}, State#state{num_requests_made = NewNumRequestsMade}}
           end
    end.
