-module(achlys_ct).
-export([
    new/0,
    get/2,
    get_all/1,
    add/3,
    update/4,
    remove/3
]).

% Helpers:

new() -> #{
    mapping => #{},
    roots => #{},
    nodes => #{}
}.

generate_ID() ->
    erlang:unique_integer().

add_mapping(Mapping, Hash, Node_ID) ->
    case maps:is_key(Hash, Mapping) of
        true ->
            maps:put(Hash, [Node_ID|maps:get(Hash, Mapping)], Mapping);
        false ->
            maps:put(Hash, [Node_ID], Mapping)
    end.

get_closest_leaf(Nodes, Node_ID) ->
    case maps:get(Node_ID, Nodes) of #{
            n := {
                N1,
                N2
            },
            children := {
                Left_Node_ID,
                Right_Node_ID
            }
        } ->
            case N1 > N2 of
                true -> get_closest_leaf(Nodes, Right_Node_ID);
                false -> get_closest_leaf(Nodes, Left_Node_ID)
            end;
        _ -> Node_ID
    end.

get_root(Nodes, Node_ID) ->
    case maps:get(Node_ID, Nodes) of
        #{parent := Parent_Node_ID} ->
            get_root(Nodes, Parent_Node_ID);
        _ -> Node_ID
    end.

propagate_changes(Nodes, Node_ID, Reduce) ->
    case maps:is_key(Node_ID, Nodes) of true ->
        Node = maps:get(Node_ID, Nodes),
        case Node of #{
            children := {
                Left_Node_ID,
                Right_Node_ID
            }
        } ->
            Left_Node = maps:get(Left_Node_ID, Nodes),
            Right_Node = maps:get(Right_Node_ID, Nodes),
            V1 = maps:get(value, Left_Node),
            V2 = maps:get(value, Right_Node),
            {A, B} = maps:get(n, Left_Node, {1, 0}),
            {C, D} = maps:get(n, Right_Node, {1, 0}),
            Updated_Nodes = maps:merge(Nodes, #{
                Node_ID => maps:merge(Node, #{
                    n => {A + B, C + D},
                    value => erlang:apply(Reduce, [V1, V2])
                })
            }),
            case Node of
                #{parent := Parent_Node_ID} ->
                    propagate_changes(
                        Updated_Nodes,
                        Parent_Node_ID,
                        Reduce
                    );
                _ -> Updated_Nodes
            end
        end
    end.

add_node(Nodes, Leaf_Node_ID, Node_ID, Node_Value, Reduce) ->
    Intermediate_Node_ID = generate_ID(),
    case maps:get(Leaf_Node_ID, Nodes) of
        #{parent := Parent_Node_ID, value := Leaf_Node_Value} ->
            Parent_Node = maps:get(Parent_Node_ID, Nodes),
            Updated_Nodes = maps:merge(Nodes, #{
                Parent_Node_ID => maps:update_with(children, fun (Children) ->
                    case Children of {Left_Node_ID, Right_Node_ID} ->
                        case Left_Node_ID == Leaf_Node_ID of
                            true -> {Intermediate_Node_ID, Right_Node_ID};
                            false -> {Left_Node_ID, Intermediate_Node_ID}
                        end
                    end
                end, Parent_Node),
                Intermediate_Node_ID => #{
                    children => {Leaf_Node_ID, Node_ID},
                    parent => Parent_Node_ID
                },
                Leaf_Node_ID => #{
                    value => Leaf_Node_Value,
                    parent => Intermediate_Node_ID
                },
                Node_ID => #{
                    value => Node_Value,
                    parent => Intermediate_Node_ID
                }
            }),
            propagate_changes(
                Updated_Nodes,
                Intermediate_Node_ID,
                Reduce
            );
        Leaf_Node ->
            Leaf_Node_Value = maps:get(value, Leaf_Node),
            maps:merge(Nodes, #{
                Intermediate_Node_ID => #{
                    value => erlang:apply(Reduce, [
                        Leaf_Node_Value,
                        Node_Value
                    ]),
                    n => {1, 1},
                    children => {Leaf_Node_ID, Node_ID}
                },
                Leaf_Node_ID => #{
                    value => Leaf_Node_Value,
                    parent => Intermediate_Node_ID
                },
                Node_ID => #{
                    value => Node_Value,
                    parent => Intermediate_Node_ID
                }
            })
    end.

% API:

add(Tree, Pair, Reduce) ->
    case Tree of #{
        mapping := Mapping,
        roots := Roots,
        nodes := Nodes
    } ->
        case Pair of {Key, Value} ->
            Hash = erlang:phash2(Pair),
            Node_ID = generate_ID(),
            Updated_Mapping = add_mapping(
                Mapping,
                Hash,
                Node_ID
            ),
            case maps:is_key(Key, Roots) of
                false ->
                    Updated_Roots = maps:put(
                        Key,
                        Node_ID,
                        Roots
                    ),
                    Updated_Nodes = maps:put(
                        Node_ID,
                        #{value => Value},
                        Nodes
                    ),
                    #{
                        mapping => Updated_Mapping,
                        roots => Updated_Roots,
                        nodes => Updated_Nodes
                    };
                true ->
                    Root_Node_ID = maps:get(Key, Roots),
                    Leaf_Node_ID = get_closest_leaf(
                        Nodes,
                        Root_Node_ID
                    ),
                    Updated_Nodes = add_node(
                        Nodes,
                        Leaf_Node_ID,
                        Node_ID,
                        Value,
                        Reduce
                    ),
                    case Root_Node_ID == Leaf_Node_ID of
                        true ->
                            Updated_Roots = maps:update(
                                Key,
                                get_root(Nodes, Root_Node_ID),
                                Roots
                            ),
                            #{
                                mapping => Updated_Mapping,
                                roots => Updated_Roots,
                                nodes => Updated_Nodes
                            };
                        false -> #{
                            mapping => Updated_Mapping,
                            roots => Roots,
                            nodes => Updated_Nodes
                        }
                    end
            end
        end
    end.

update(Tree, Pair, Value, Reduce) ->
    case Tree of #{
        mapping := Mapping,
        nodes := Nodes
    } ->
        Hash = erlang:phash2(Pair),
        case maps:is_key(Hash, Mapping) of
            true ->
                [Node_ID|_] = maps:get(Hash, Mapping),
                case maps:get(Node_ID, Nodes) of Node ->
                    case Node of #{parent := Parent_Node_ID} ->
                        maps:update(nodes, propagate_changes(
                            maps:merge(Nodes, #{
                                Node_ID => maps:update(
                                    value,
                                    Value,
                                    Node
                                )
                            }),
                            Parent_Node_ID,
                            Reduce
                        ), Tree);
                    _ -> maps:update(nodes, maps:merge(Nodes, #{
                            Node_ID => maps:update(
                                value,
                                Value,
                                Node
                            )
                        }), Tree)
                    end
                end
            ;
            false -> Tree
        end
    end.

get(Tree, Key) ->
    case Tree of #{
        roots := Roots,
        nodes := Nodes
    } ->
        case maps:is_key(Key, Roots) of true ->
            Node_ID = maps:get(Key, Roots),
            case maps:get(Node_ID, Nodes) of #{value := Value} ->
                Value
            end
        end
    end.

get_all(Tree) ->
    case Tree of #{roots := Roots} ->
        get_all(Tree, maps:keys(Roots))
    end.
get_all(_, []) -> [];
get_all(Tree, [Key|Keys]) ->
    [{Key, get(Tree, Key)}|get_all(Tree, Keys)].

remove(Tree, Pair, Reduce) ->
    case Tree of #{
        mapping := Mapping,
        roots := Roots,
        nodes := Nodes
    } ->
        Hash = erlang:phash2(Pair),
        case maps:is_key(Hash, Mapping) of
            true ->
                [Node_ID|_] = maps:get(Hash, Mapping),
                case maps:get(Node_ID, Nodes) of Node ->
                    % maps:without(Nodes, #{})
                    ok
                end;
            false -> Tree
        end
    end.