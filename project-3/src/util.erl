-module(util).

-compile([export_all]).

% Helper function to return first element in a list
% that satisfies the condition. Returns a default value if
% no element satisfies it.
first([E | Rest], Condition, Default) ->
  case Condition(E) of
    true ->
      E;
    false ->
      first(Rest, Condition, Default)
  end;
first([], _Cond, Default) ->
  Default.
