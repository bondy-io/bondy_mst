%% =============================================================================
%% Tests for the `bondy_oplog_fold` dispatch surface — specifically the
%% strategy validation helpers `is_known/1` and `validate/1` introduced
%% with F7 (FOLD_STRATEGY_DESIGN §6/§7) for instance-init-time config
%% checking.
%% =============================================================================

-module(bondy_oplog_fold_test).

-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% is_known/1
%% =============================================================================

is_known_recognises_builtin_shorthands_test() ->
    ?assert(bondy_oplog_fold:is_known(presence_basic)),
    ?assert(bondy_oplog_fold:is_known(lww_register)),
    ?assert(bondy_oplog_fold:is_known(strict_register)),
    ?assert(bondy_oplog_fold:is_known(orset)),
    ?assert(bondy_oplog_fold:is_known(ttl_presence)),
    ?assert(bondy_oplog_fold:is_known(map_of_fields)).

is_known_rejects_arbitrary_atoms_test() ->
    ?assertNot(bondy_oplog_fold:is_known(some_random_atom)),
    ?assertNot(bondy_oplog_fold:is_known(bondy_oplog_fold_lww_register)).

is_known_rejects_non_atoms_test() ->
    ?assertNot(bondy_oplog_fold:is_known(<<"lww_register">>)),
    ?assertNot(bondy_oplog_fold:is_known(42)),
    ?assertNot(bondy_oplog_fold:is_known([])).

%% =============================================================================
%% validate/1
%% =============================================================================

validate_accepts_all_builtin_shorthands_test() ->
    lists:foreach(
        fun(S) -> ?assertEqual(ok, bondy_oplog_fold:validate(S)) end,
        [presence_basic, lww_register, strict_register, orset,
         ttl_presence, map_of_fields]
    ).

validate_accepts_resolved_module_names_test() ->
    %% Passing the fully-qualified module name directly (rather than
    %% the shorthand) is the supported path for application-defined folds.
    ?assertEqual(ok, bondy_oplog_fold:validate(bondy_oplog_fold_lww_register)),
    ?assertEqual(ok, bondy_oplog_fold:validate(bondy_oplog_fold_map_of_fields)).

validate_rejects_unloadable_module_test() ->
    Result = bondy_oplog_fold:validate(nonexistent_fold_module_xyz),
    ?assertMatch({error, {module_not_loadable, nonexistent_fold_module_xyz, _}},
                 Result).

validate_rejects_module_missing_callbacks_test() ->
    %% `bondy_oplog_merge_strict_uniqueness` is a real loadable module
    %% but implements a different behaviour, so it lacks the
    %% bondy_oplog_fold callbacks.
    Result = bondy_oplog_fold:validate(bondy_oplog_merge_strict_uniqueness),
    ?assertMatch({error, {missing_callbacks, _, [_ | _]}}, Result),
    {error, {missing_callbacks, _, Missing}} = Result,
    %% At minimum apply_event/3 (the post-§3.1 contract) must be flagged.
    ?assert(lists:member({apply_event, 3}, Missing)).

validate_rejects_non_atom_strategy_test() ->
    ?assertEqual(
        {error, {unknown_strategy, <<"lww_register">>}},
        bondy_oplog_fold:validate(<<"lww_register">>)
    ),
    ?assertEqual(
        {error, {unknown_strategy, 42}},
        bondy_oplog_fold:validate(42)
    ).
