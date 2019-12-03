-module(achlys_event).
-export([
  create_listener/1,
  event_listener/1
]).

% add_event(Var) ->
%     set_listeners(#{
%         variable => Var,
%         callbacks => [fun(V) -> 
%             io:format("~p~n", ["Hello World"])
%         end]
%     }).

% set_listeners(State) ->
%     case State of #{variable := Variable, callbacks := Callbacks} ->
%         lasp:stream(Variable, fun(Value) ->
%             lists:foreach(fun(Callback) ->
%                 Callback(Value) 
%             end, Callbacks)
%         end)
%     end.

event_listener(State) ->
    receive 
        {trigger, Value} ->
            case State of #{callbacks := Callbacks} ->
                lists:foreach(fun(Callback) ->
                    Callback(Value) 
                end, Callbacks)
            end,
            event_listener(State);
        {on_change, Callback} ->
            case State of #{callbacks := Callbacks} ->
                event_listener(State#{
                    callbacks := Callbacks ++ [Callback]
                })
            end
    end.

create_listener(Variable) ->
    PID = spawn(?MODULE, event_listener, [#{
        callbacks => []
    }]),
    lasp:stream(Variable, fun(Value) ->
        PID ! {trigger, Value}
    end),
    PID.
