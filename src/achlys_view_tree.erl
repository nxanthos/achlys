-module(achlys_view_tree).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-export([
    start_link/0,
    init/1,
    handle_cast/2,
    handle_call/3
]).

% API:

-export([
    add_variable/3,
    set_reduce/1,
    add_listener/1,
    debug/0
]).

% Starting function:

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #{
        pairs => [],
        mapping => [],
        roots => #{},
        nodes => #{},
        listeners => []
    }}.

% Cast:

handle_cast({add_variable, Variable, Map, Reduce}, State) ->
    Pairs = Map(Variable),
    mapreduce({Pairs}, State, Reduce);

handle_cast({reduce, Reduce}, State) ->
    case State of 
        #{pairs := Pairs, listeners := Listeners} ->
            Groups = shuffle(#{}, Pairs),
            Results = maps:fold(fun(Key, Values, Acc) ->
                maps:put(Key, reduce(Values, Reduce), Acc)
            end, #{}, Groups),
            lists:foreach(fun(Listener) ->
                Listener(Results)
            end, Listeners);
        _ ->
            {noreply, State}
    end,
    {noreply, State};

handle_cast({add_listener, Listener}, State) ->
    case State of #{listeners := Listeners} ->
        {noreply, State#{
            listeners := Listeners ++ [Listener]
        }}
    end;

handle_cast(debug, State) ->
    io:format("~p~n", ["Debugging"]),
    case State of #{pairs := Pairs, mapping := Mapping, roots := Roots, nodes := Nodes} ->
        io:format("Pairs: ~p~n, Mapping: ~p~n, Roots: ~p~n, Nodes: ~p~n", [Pairs, Mapping, Roots, Nodes]),
        {noreply, State}
    end.

% Call:

handle_call(_Request , _From , State) ->
    {reply, ok, State}.

% Helpers:

build_listener(Variable) ->
    fun(Value) ->
        { Name, _ } = Variable,
        io:format("Name of the variable that changed ~p~n", [Name]),
        io:format("Value of the variable~p~n", [Value])
    end.

shuffle(Groups, []) -> Groups;
shuffle(Groups, [{Key, Value}|T]) ->
    case maps:is_key(Key, Groups) of
        true ->
            L = [Value|maps:get(Key, Groups)],
            M = maps:put(Key, L, Groups),
            shuffle(M, T);
        false ->
            L = [Value],
            M = maps:put(Key, L, Groups),
            shuffle(M, T)
    end.

reduce([H], _) -> H;
reduce([H1|[H2|T]], Reduce) ->
    reduce([Reduce(H1, H2)|T], Reduce).

mapreduce({Key_Value_Pairs}, State, Reduce) ->
    case Key_Value_Pairs of 
        [Key_Value_Pair|T] ->
            case State of 
                #{listeners := Listeners, pairs := Pairs, mapping := Mapping, roots := Roots, nodes := Nodes} ->
                    % Get Key and Value
                    {Key, Value} = Key_Value_Pair,
                    %% Create a new node with an ID and a state with value = Value
                    % Format: {Node_ID, #{value => Value, children => [], parent => []}}
                    Node = create_node(Value),
                    {Node_ID, Node_State} = Node,
                    %% Mapping field
                    % Compute the hash of the pair
                    Hash_Pair = erlang:phash2(Key_Value_Pair),
                    % Associate the hash of the pair and the ID
                    Map = #{Hash_Pair => Node_ID},
                    % Get the root node of the tree associated to the key
                    case maps:is_key(Key, Roots) of 
                        true -> % The tree already exists, get the ID of the root node
                            Root_ID = maps:get(Key, Roots), % Get the root ID from Key
                            Root_State = maps:get(Root_ID, Nodes), % Get the State of root in Nodes
                            Root = {Root_ID, Root_State}, % return the root

                            % Find a leaf to insert the new Node
                            Leaf = find_leaf(Nodes, Root, Value),
                            {Leaf_ID, Leaf_State} = Leaf,
                            
                            % Get parent of the Leaf
                            Parent_List = maps:get(parent, Leaf_State),
                            % Generate an ID for the intermediate node
                            Interm_Node_ID = create_ID(),
                            case length(Parent_List) of
                                0 -> % No parent: Leaf == Root
                                    New_Nodes = add_nodes(create_intermediate_node(Interm_Node_ID, Leaf, Node, Reduce), Nodes),
                                    New_State = #{
                                        listeners => Listeners,
                                        pairs => Pairs ++ [Key_Value_Pair],
                                        mapping => Mapping ++ [Map],
                                        roots => Roots,
                                        nodes => New_Nodes
                                    };
                                _ ->
                                    [Parent_ID] = Parent_List,
                                    Parent = get_node(Parent_ID, Nodes),
                                    New_Nodes = add_nodes(create_intermediate_node(Interm_Node_ID, Leaf, Node, Parent, Reduce), Nodes),
                                    New_State = #{
                                        listeners => Listeners,
                                        pairs => Pairs ++ [Key_Value_Pair],
                                        mapping => Mapping ++ [Map],
                                        roots => Roots,
                                        nodes => New_Nodes
                                    }
                            end,
                            mapreduce({T}, update_values(Interm_Node_ID, New_State, Reduce), Reduce);

                        false -> % The tree does not exist, we create a new tree associated to the key
                            Root = Node,
                            {Root_ID, Root_State} = Node,
                            New_State = #{
                                listeners => Listeners,
                                pairs => Pairs ++ [Key_Value_Pair],
                                mapping => Mapping ++ [Map],
                                roots => maps:put(Key, Root_ID, Roots), % Add Node as Root of the tree
                                nodes => maps:put(Root_ID, Root_State, Nodes) % Add Node in Nodes
                            },
                            mapreduce({T}, New_State, Reduce)
                    end;
                _ -> 
                    {noreply, State}
            end;
        _ -> {noreply, State}
    end.

% Return a node with a Node_ID and a state with value = Value. No parent and no children
create_node(Value) ->
    Node_ID = create_ID(),
    Node_State = #{value => Value, children => [], parent => []},
    {Node_ID, Node_State}.

% Return a unique ID
create_ID() ->
    erlang:unique_integer().

%  Find and return a leaf
find_leaf(Nodes, Root, Value) ->
    {Root_ID, Root_State} = Root,
    case length(maps:get(children, Root_State)) of 
        0 -> Root; % Root is a leaf
        _ -> case Value > maps:get(value, Root_State) of 
            true ->
                Child_ID = lists:nth(2,maps:get(children, Root_State)),
                find_leaf(Nodes, {Child_ID, maps:get(Child_ID, Nodes)}, Value);
            false ->
                Child_ID = lists:nth(1,maps:get(children, Root)),
                find_leaf(Nodes, {Child_ID, maps:get(Child_ID, Nodes)}, Value)
            end
    end.

%Create an intermediate node for Node and Leaf.
create_intermediate_node(Interm_Node_ID, {Leaf_ID, Leaf_State}, {Node_ID, Node_State}, {Parent_ID, Parent_State}, Reduce) ->
    New_Node_State = maps:update(parent, [Interm_Node_ID], Node_State), % Update  the node's state: Parent of node is Intermediate Node
    New_Leaf_State = maps:update(parent, [Interm_Node_ID], Leaf_State), % Update the Leaf's state: Parent of leaf is Intermediate Node
    [Parents_Children_1, Parents_Children_2] = maps:get(children, Parent_State),
    case Parents_Children_1 of % Update children of Parent: Interm_Node replace Leaf
        Leaf_ID -> New_Parent_State = maps:update(children, [Interm_Node_ID, Parents_Children_2], Parent_State);
        _ -> New_Parent_State = maps:update(children, [Parents_Children_1, Interm_Node_ID], Parent_State)
    end,
    Node_Value = maps:get(value, Node_State),
    Leaf_Value = maps:get(value, Leaf_State),
    case Leaf_Value > Node_Value of % left child must have a smaller value
        true ->
            Interm_Node_State = #{value => Reduce(Node_Value, Leaf_Value), children => [Node_ID, Leaf_ID], parent => [Parent_ID]},
            Interm_Node = {Interm_Node_ID, Interm_Node_State},
            [{Node_ID, New_Node_State}, {Leaf_ID, New_Leaf_State}, Interm_Node, {Parent_ID, New_Parent_State}];
        false ->
            Interm_Node_State = #{value => Reduce(Node_Value, Leaf_Value), children => [Leaf_ID, Node_ID], parent => [Parent_ID]},
            Interm_Node = {Interm_Node_ID, Interm_Node_State},
            [{Node_ID, New_Node_State}, {Leaf_ID, New_Leaf_State}, Interm_Node, {Parent_ID, New_Parent_State}]
end.

% Create an intermediate node for Node and Leaf which have no parent (= Root)
create_intermediate_node(Interm_Node_ID, {Leaf_ID, Leaf_State}, {Node_ID, Node_State}, Reduce) ->
    New_Node_State = maps:update(parent, [Interm_Node_ID], Node_State),
    New_Leaf_State = maps:update(parent, [Interm_Node_ID], Leaf_State),
    Node_Value = maps:get(value, Node_State),
    Leaf_Value = maps:get(value, Leaf_State),
    case Leaf_Value > Node_Value of % left child must have a smaller value
        true ->
            Interm_Node_State = #{value => Reduce(Node_Value, Leaf_Value), children => [Node_ID, Leaf_ID], parent => []},
            Interm_Node = {Interm_Node_ID, Interm_Node_State},
            [{Node_ID, New_Node_State}, {Leaf_ID, New_Leaf_State}, Interm_Node];
        false ->
            Interm_Node_State = #{value => Reduce(Node_Value, Leaf_Value), children => [Leaf_ID, Node_ID], parent => []},
            Interm_Node = {Interm_Node_ID, Interm_Node_State},
            [{Node_ID, New_Node_State}, {Leaf_ID, New_Leaf_State}, Interm_Node]
    end.

update_values(Node_ID, State, Reduce) -> % Start the update from Node
    case State of #{listeners:= Listeners, pairs := Pairs, mapping := Mapping, roots := Roots, nodes := Nodes} ->
        Node = get_node(Node_ID, Nodes),
        {Node_ID, #{value := Value, children := Children, parent := Parent}} = Node,
        [Left_Child_ID, Right_Child_ID] = Children,
        Left_Child_State = maps:get(Left_Child_ID, Nodes),
        Right_Child_State = maps:get(Right_Child_ID, Nodes),
        Left_Child_Value = maps:get(value, Left_Child_State),
        Right_Child_Value = maps:get(value, Right_Child_State),
        
        Node_State = #{value => Reduce(Left_Child_Value, Right_Child_Value), children => Children, parent => Parent},
        New_Nodes = maps:update(Node_ID, Node_State, Nodes),

        New_State = #{
            listeners => Listeners,
            pairs => Pairs,
            mapping => Mapping,
            roots => Roots,
            nodes => New_Nodes
        },
        case length(Parent) of
            0 -> % We reach the root node => All nodes are updated
                New_State;
            _ ->
                [Parent_ID] = Parent,
                update_values(Parent_ID, New_State, Reduce)
        end
    end.

add_nodes(List_of_Nodes, Set) ->
    case List_of_Nodes of
        [Node|T] -> 
            {Node_ID, Node_State} = Node,
            New_Set = maps:put(Node_ID, Node_State, Set),
            add_nodes(T, New_Set);
        _ -> Set
    end.

get_node(Node_ID, Set) ->
    {Node_ID, maps:get(Node_ID, Set)}.

% API:

add_variable(Variable, Map, Reduce) ->
    gen_server:cast(?SERVER , {add_variable, Variable, Map, Reduce}).

set_reduce(Reduce) ->
    gen_server:cast(?SERVER , {reduce, Reduce}).

add_listener(Listener) ->
    gen_server:cast(?SERVER , {add_listener, Listener}).

debug() ->
    gen_server:cast(?SERVER, debug).