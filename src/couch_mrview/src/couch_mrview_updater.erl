% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_mrview_updater).

-export([start_update/3, purge/4, process_doc/3, finish_update/1]).

-include("couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").


start_update(Partial, State, NumChanges) ->
    QueueOpts = [{max_size, 100000}, {max_items, 500}],
    {ok, DocQueue}     = couch_work_queue:new(QueueOpts),
    {ok, WriteQueue}   = couch_work_queue:new(QueueOpts),
    {ok, NestedMapQ}   = couch_work_queue:new(QueueOpts),
    {ok, NestedWriteQ} = couch_work_queue:new(QueueOpts),

    InitState = State#mrst{
        first_build=State#mrst.update_seq==0,
        partial_resp_pid=Partial,
        doc_acc=[],
        doc_queue=DocQueue,
        write_queue=WriteQueue,
        nested_map_queue=NestedMapQ,
        nested_write_queue=NestedWriteQ
    },

    Self = self(),
    MapFun = fun() ->
        couch_task_status:add_task([
            {type, indexer},
            {database, State#mrst.db_name},
            {design_document, State#mrst.idx_name},
            {progress, 0},
            {changes_done, 0},
            {total_changes, NumChanges}
        ]),
        couch_task_status:set_update_frequency(500),
        map_docs(Self, InitState)
    end,
    WriteFun = fun() -> write_results(Self, InitState) end,
    NestedViewFun = fun() -> nested_map_docs(Self, InitState) end,

    spawn_link(MapFun),
    spawn_link(WriteFun),
    spawn_link(NestedViewFun),

    {ok, InitState}.


purge(_Db, PurgeSeq, PurgedIdRevs, State) ->
    ?LOG_INFO("**********************************************~n", []),
    ?LOG_INFO("*** TODO: couch_mrview_updater:purge(...) ****~n", []),
    ?LOG_INFO("**********************************************~n", []),
    #mrst{
        id_btree=IdBtree,
        views=Views
    } = State,

    Ids = [Id || {Id, _Revs} <- PurgedIdRevs],
    {ok, Lookups, IdBtree2} = couch_btree:query_modify(IdBtree, Ids, [], Ids),

    MakeDictFun = fun
        ({ok, {DocId, ViewNumRowKeys}}, DictAcc) ->
            FoldFun = fun({ViewNum, RowKey}, DictAcc2) ->
                dict:append(ViewNum, {RowKey, DocId}, DictAcc2)
            end,
            lists:foldl(FoldFun, DictAcc, ViewNumRowKeys);
        ({not_found, _}, DictAcc) ->
            DictAcc
    end,
    KeysToRemove = lists:foldl(MakeDictFun, dict:new(), Lookups),

    RemKeysFun = fun(#mrview{id_num=Num, btree=Btree}=View) ->
        case dict:find(Num, KeysToRemove) of
            {ok, RemKeys} ->
                {ok, Btree2} = couch_btree:add_remove(Btree, [], RemKeys),
                NewPurgeSeq = case Btree2 /= Btree of
                    true -> PurgeSeq;
                    _ -> View#mrview.purge_seq
                end,
                View#mrview{btree=Btree2, purge_seq=NewPurgeSeq};
            error ->
                View
        end
    end,

    Views2 = lists:map(RemKeysFun, Views),
    {ok, State#mrst{
        id_btree=IdBtree2,
        views=Views2,
        purge_seq=PurgeSeq
    }}.


process_doc(Doc, Seq, #mrst{doc_acc=Acc}=State) when length(Acc) > 100 ->
    couch_work_queue:queue(State#mrst.doc_queue, lists:reverse(Acc)),
    process_doc(Doc, Seq, State#mrst{doc_acc=[]});
process_doc(nil, Seq, #mrst{doc_acc=Acc}=State) ->
    {ok, State#mrst{doc_acc=[{nil, Seq, nil} | Acc]}};
process_doc(#doc{id=Id, deleted=true}, Seq, #mrst{doc_acc=Acc}=State) ->
    {ok, State#mrst{doc_acc=[{Id, Seq, deleted} | Acc]}};
process_doc(#doc{id=Id}=Doc, Seq, #mrst{doc_acc=Acc}=State) ->
    {ok, State#mrst{doc_acc=[{Id, Seq, Doc} | Acc]}}.


finish_update(#mrst{doc_acc=Acc}=State) ->
    if Acc /= [] ->
        couch_work_queue:queue(State#mrst.doc_queue, Acc);
        true -> ok
    end,
    couch_work_queue:close(State#mrst.doc_queue),
    receive
        {new_state, NewState} ->
            {ok, NewState#mrst{
                first_build=undefined,
                partial_resp_pid=undefined,
                doc_acc=undefined,
                doc_queue=undefined,
                write_queue=undefined,
                qserver=nil
            }}
    end.


map_docs(Parent, State0) ->
    case couch_work_queue:dequeue(State0#mrst.doc_queue) of
        closed ->
            couch_query_servers:stop_doc_map(State0#mrst.qserver),
            couch_work_queue:close(State0#mrst.write_queue);
        {ok, Dequeued} ->
            State1 = with_query_server(State0, root_views(State0)),
            {ok, MapResults} = compute_map_results(State1, Dequeued),
            couch_work_queue:queue(State1#mrst.write_queue, MapResults),
            map_docs(Parent, State1)
    end.

compute_map_results(#mrst{qserver = Qs}, Dequeued) ->
    % Run all the non deleted docs through the view engine and
    % then pass the results on to the writer process.
    DocFun = fun
        ({nil, Seq, _}, {SeqAcc, AccDel, AccNotDel}) ->
            {erlang:max(Seq, SeqAcc), AccDel, AccNotDel};
        ({Id, Seq, deleted}, {SeqAcc, AccDel, AccNotDel}) ->
            {erlang:max(Seq, SeqAcc), [{Id, []} | AccDel], AccNotDel};
        ({_Id, Seq, Doc}, {SeqAcc, AccDel, AccNotDel}) ->
            {erlang:max(Seq, SeqAcc), AccDel, [Doc | AccNotDel]}
    end,
    FoldFun = fun(Docs, Acc) ->
        lists:foldl(DocFun, Acc, Docs)
    end,
    {MaxSeq, DeletedResults, Docs} =
        lists:foldl(FoldFun, {0, [], []}, Dequeued),
    {ok, MapResultList} = couch_query_servers:map_docs_raw(Qs, Docs),
    NotDeletedResults = lists:zipwith(
        fun(#doc{id = Id}, MapResults) -> {Id, MapResults} end,
        Docs,
        MapResultList),
    AllMapResults = DeletedResults ++ NotDeletedResults,
    update_task(length(AllMapResults)),
    {ok, {MaxSeq, AllMapResults}}.

with_query_server(State, Views) ->
    case State#mrst.qserver of
        nil -> start_query_server(State, Views);
        _ -> State
    end.

start_query_server(State, Views) ->
    #mrst{
        language=Language,
        lib=Lib
    } = State,
    Defs = [View#mrview.def || View <- Views],
    {ok, QServer} = couch_query_servers:start_doc_map(Language, Defs, Lib),
    State#mrst{qserver=QServer}.


nested_map_docs(Parent, State) ->
    case couch_work_queue:dequeue(State#mrst.nested_map_queue) of
        closed -> ok;
        {ok, [{Ref, Views, Docs}]} ->
            NewState = with_query_server(State, Views),
            {ok, MapResults} = compute_map_results(NewState, [Docs]),
            couch_work_queue:queue(NewState#mrst.nested_write_queue,
                                   {Ref, MapResults}),
            couch_query_servers:stop_doc_map(NewState#mrst.qserver),
            nested_map_docs(Parent, State)
    end.


write_results(Parent, State0) ->
    case couch_work_queue:dequeue(State0#mrst.write_queue) of
        closed ->
            couch_work_queue:close(State0#mrst.nested_map_queue),
            couch_work_queue:close(State0#mrst.nested_write_queue),
            Parent ! {new_state, State0};
        {ok, MapResults} ->
            State1 = write_nested_results(State0, MapResults),
            send_partial(State1#mrst.partial_resp_pid, State1),
            write_results(Parent, State1)
    end.

write_nested_results(State, Results) ->
    #mrst{id_btree=IdBtree} = State,
    write_nested_results(State, state, IdBtree, [], Results).
write_nested_results(State0, IdBtreeLoc, IdBtree, Path, MapResults) ->
    Views  = nested_views_for_path(Path, State0),
    Merged = merge_results(MapResults, Views),
    {State1, ToRemByView} = write_kvs(State0, IdBtreeLoc, IdBtree, Views, Merged),
    traverse_view_path(State1, Path, Merged, ToRemByView).


% TODO: make ViewKVs a dict, Key=ViewID; Value=ViewEntries.
% it's currently being accessed via lists:keyfind
%
% Info        = [{Seq, SeqResults}   | _Rest]
% SeqResults  = [{DocId, RawResults} | _Rest]
% RawResults  = string output from view server map_doc
% DocResults  = [FunResults          | _Rest]
% FunResults  = [Emit                | _Rest]
% Emit        = {Key, Value}
%
% returns: {Seq, ViewKVs, DocIdKeys}
% Seq         = highest Seq
% ViewKVs     = [{ViewId, ViewEntries} | _Rest]
% DocIdKeys   = [{DocId,  ViewIdKeys}  | _Rest]
%
% ViewEntries = [{{Key, DocID}, Value} | _Rest]
% ViewIdKeys  = {ViewId, ViewKey}      | _Rest]
% Value       = JsonObject or {dups, [JsonObject | _Rest]}
%
% So:
% Info        = [{Seq, [{DocId, [[ {K,V} ]]}]}]
% Return      = {Seq, [{ViewId, [ {{K,ID},V} ]}], [{DocId, [{ViewID,K}]}]}
merge_results(Info, Views) ->
    ViewKVs = [{V#mrview.id_num, []} || V <- Views],
    lists:foldl(fun merge_seq_results/2, {0, ViewKVs, []}, Info).

merge_seq_results({Seq, SeqResults}, {SeqAcc, ViewKVs, DocIdKeys}) ->
    {NewViewKVs, NewDocIdKeys} = lists:foldl(
        fun merge_doc_results/2, {ViewKVs, DocIdKeys}, SeqResults
    ),
    {erlang:max(Seq, SeqAcc), NewViewKVs, NewDocIdKeys}.

merge_doc_results({DocId, []}, {ViewKVs, DocIdKeys}) ->
    {ViewKVs, [{DocId, []} | DocIdKeys]};
merge_doc_results({DocId, RawResults}, {ViewKVs, DocIdKeys}) ->
    DocResults = parse_results(RawResults),
    {NewViewKVs, ViewIDKeys} = lists:foldl(
        merge_view_results(DocId), {[], []}, lists:zip(DocResults, ViewKVs)
    ),
    {lists:reverse(NewViewKVs), [{DocId, ViewIDKeys} | DocIdKeys]}.

parse_results(RawResults) ->
    DocResults = couch_query_servers:raw_to_ejson(RawResults),
    [[list_to_tuple(Emit) || Emit <- FunResults] || FunResults <- DocResults].

merge_view_results(DocId) ->
    fun ({DocViewResults, {ViewId, ThisViewKVs}}, {ViewKVs, ViewIdKeys}) ->
        {Duped, UniqueViewIdKeys} = lists:foldl(
            view_dup_reduction(ViewId),
            {[], ViewIdKeys}, lists:sort(DocViewResults)
        ),
        FinalKVs = [{{Key, DocId}, Val} || {Key, Val} <- Duped] ++ ThisViewKVs,
        {[{ViewId, FinalKVs} | ViewKVs], UniqueViewIdKeys}
    end.

view_dup_reduction(ViewId) ->
    fun ({Key, Val1}, {[{Key, Val2}         | Rest], IdKeys}) ->
            {[{Key, {dups, [Val1, Val2]}} | Rest], IdKeys};
        ({Key, Val},  {[{Key, {dups, Vals}} | Rest], IdKeys}) ->
            {[{Key, {dups, [Val | Vals]}} | Rest], IdKeys};
        ({Key, _}=KV, {                        Rest, IdKeys}) ->
            {[KV | Rest], [{ViewId, Key} | IdKeys]}
    end.


write_kvs(State0, IdBtreeLoc, IdBtree0, _Views, {UpdateSeq, ViewKVs, DocIdKeys}) ->
    FirstBuild  = State0#mrst.first_build,
    {ok, ToRemove, IdBtree1} = update_id_btree(IdBtree0, DocIdKeys, FirstBuild),
    ToRemByView = collapse_rem_keys(ToRemove),
    UpdatedViews = lists:map(
        fun(View) -> update_view(View, {IdBtreeLoc, IdBtree1}, {UpdateSeq, ViewKVs, ToRemByView}) end,
        State0#mrst.views
    ),
    State1 = State0#mrst{views=UpdatedViews, update_seq=UpdateSeq},
    State2 = case IdBtreeLoc of
        state -> State1#mrst{id_btree=IdBtree1};
        _ -> State1
    end,
    {State2, ToRemByView}.

update_id_btree(Btree, DocIdKeys, true) ->
    ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
    couch_btree:query_modify(Btree, [], ToAdd, []);
update_id_btree(Btree, DocIdKeys, _) ->
    ToFind = [Id || {Id, _} <- DocIdKeys],
    ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
    ToRem = [Id || {Id, DIKeys} <- DocIdKeys, DIKeys == []],
    couch_btree:query_modify(Btree, ToFind, ToAdd, ToRem).

collapse_rem_keys(ToRemove) -> collapse_rem_keys(ToRemove, dict:new()).
collapse_rem_keys([], Acc) -> Acc;
collapse_rem_keys([{ok, {DocId, ViewIdKeys}} | Rest], Acc) ->
    NewAcc = lists:foldl(fun({ViewId, Key}, Acc2) ->
        dict:append(ViewId, {Key, DocId}, Acc2)
    end, Acc, ViewIdKeys),
    collapse_rem_keys(Rest, NewAcc);
collapse_rem_keys([{not_found, _} | Rest], Acc) ->
    collapse_rem_keys(Rest, Acc).

update_view(#mrview{id_num=ViewID}=View, {#mrview{id_num=ViewID}, IdBtree}, _) ->
    View#mrview{nested_id_btree=IdBtree};
update_view(#mrview{id_num=ViewID}=View, _, {UpdateSeq, ViewKVs, ToRemByView}) ->
    case lists:keyfind(ViewID, 1, ViewKVs) of
        false -> View;
        {ViewId, KVs} ->
            ToRem = couch_util:dict_find(ViewId, ToRemByView, []),
            {ok, VBtree2} = couch_btree:add_remove(View#mrview.btree, KVs, ToRem),
            NewUpdateSeq = case VBtree2 =/= View#mrview.btree of
                true -> UpdateSeq;
                _ -> View#mrview.update_seq
            end,
            View#mrview{btree=VBtree2, update_seq=NewUpdateSeq}
    end.


% Depth first recursive traversal
traverse_view_path(State0, Path, ViewData, ToRemByView) ->
    Views = nested_views_for_path(Path, State0),
    lists:foldl(fun(View, State1) ->
            traverse_view(State1, View, ViewData, ToRemByView)
        end, State0, Views).

traverse_view(State, #mrview{id_num=ViewId}=View, ViewData, ToRemByView) ->
    {_Seq, ViewKVs, _DocIdKeys} = ViewData,
    {ViewId, KVs} = lists:keyfind(ViewId, 1, ViewKVs),
    RemovedKeys   = couch_util:dict_find(ViewId, ToRemByView, []),
    ChangedViewKeys = unique_view_keys(KVs, RemovedKeys),
    Paths = nested_paths_for_view(View, State),
    visit_nested_paths(State, View, Paths, ChangedViewKeys).

visit_nested_paths(State, _View, [], _Keys) -> State;
visit_nested_paths(State0, View, Paths, ChangedViewKeys) ->
    ?LOG_INFO("**** visit_nested_paths(... ~p ...)~n", [Paths]),
    lists:foldl(fun(Path, State1) ->
            ReducedResult = reduced_results(State1, View, Path, ChangedViewKeys),
            visit_nested_path(State1, View, Path, ReducedResult)
        end, State0, Paths).

visit_nested_path(State, _, _, todo_not_implemented_yet) -> State;
visit_nested_path(State0, ParentView, Path, ReducedResults) ->
    ?LOG_INFO("**** visit_nested_path(... ~p,~n   ~p)~n", [Path, ReducedResults]),
    NestedViews = nested_views_for_path(Path, State0),
    lists:foldl(fun(NestedView, State1) ->
            visit_nested_view(State1, ParentView, Path,
                              NestedView, ReducedResults)
        end, State0, NestedViews).

visit_nested_view(State, ParentView, Path, NestedViews, ReducedResults) ->
    ?LOG_INFO("**** visit_nested_view(... ~p ...)~n", [ParentView#mrview.id_num]),
    #mrview{nested_id_btree=IdBtree} = ParentView,
    MapResults = wait_for_nested_map(State, NestedViews, ReducedResults),
    write_nested_results(State, ParentView, IdBtree, Path, MapResults).

wait_for_nested_map(State, NestedViews, ReducedResults) ->
    ?LOG_INFO("**** wait_for_nested_map(State, ~p, ~p)~n",
      [[IdNum || #mrview{id_num=IdNum} <- NestedViews],
        length(ReducedResults)]),
    Ref           = make_ref(),
    NestedMapWork = {Ref, NestedViews, ReducedResults},
    couch_work_queue:queue(State#mrst.nested_map_queue, NestedMapWork),
    case couch_work_queue:dequeue(State#mrst.nested_write_queue) of
        closed -> error(nested_write_queue_closed_prematurely);
        {ok, [{Ref, MapResults}]} -> [MapResults]
    end.

nested_paths_for_view(#mrview{path=ParentPath}=View, State) ->
    MaybeNested = [[Name|ParentPath] || {Name,_Fun} <- View#mrview.reduce_funs],
    [NestedPath || #mrview{path=NestedPath} <- State#mrst.views,
        lists:member(NestedPath, MaybeNested)].

root_views(State) -> nested_views_for_path([], State).
nested_views_for_path(Path, State) ->
    [V || V <- State#mrst.views, V#mrview.path =:= Path].

unique_view_keys(KVs, RemovedKeys) ->
    Set0 = sets:from_list(RemovedKeys),
    Set1 = sets:from_list([Key || {{Key, _DocId}, _Value} <- KVs]),
    Set2 = sets:union([Set0, Set1]),
    sets:to_list(Set2).

% Returns a list of tuples:
%   * when reduced key is missing: {Id, Seq, deleted}
%   * when reduced key is present: {Id, Seq, Doc}
%     * Doc = #doc{id=?JSON_ENCODE(Key),
%                  body={[{<<"key">>, Key}, {<<"value">>, V}]}
%   * Id = json_bin_string(Key)
reduced_results(State, View, [ReduceName|_]=Path, Keys) ->
    Db      = State#mrst.db_name,
    Nth     = reduce_index(ReduceName, View#mrview.reduce_funs),
    RedView = {Nth, State#mrst.language, View},
    Results = couch_mrview:query_reduced_results(Db, RedView, Keys),
    ?LOG_INFO("*** reduced_results for ~p, ~p: ~p~n", [Path, Keys, Results]),
    %[
    %    {<<"1">>, State#mrst.update_seq, #doc{id= <<"1">>,
    %        body={[{<<"key">>, 1}, {<<"value">>, 23}]}}},
    %    {<<"2">>, State#mrst.update_seq, #doc{id= <<"1">>,
    %        body={[{<<"key">>, 2}, {<<"value">>, 13}]}}},
    %    {<<"3">>, State#mrst.update_seq, #doc{id= <<"1">>,
    %        body={[{<<"key">>, 3}, {<<"value">>,  3}]}}}
    %].
    todo_not_implemented_yet.

reduce_index(Name, List) -> reduce_index(Name, List, 1).
reduce_index(_, [], _)  -> not_found;
reduce_index(Name, [{Name,_}|_], Index) -> Index;
reduce_index(Name, [_|Tl], Index) -> reduce_index(Name, Tl, Index+1).

send_partial(Pid, State) when is_pid(Pid) ->
    gen_server:cast(Pid, {new_state, State});
send_partial(_, _) ->
    ok.


update_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = case Total of
        0 ->
            % updater restart after compaction finishes
            0;
        _ ->
            (Changes2 * 100) div Total
    end,
    couch_task_status:update([{progress, Progress}, {changes_done, Changes2}]).
