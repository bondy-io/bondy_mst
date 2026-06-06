%% =============================================================================
%% EUnit smoke tests for `bondy_oplog_index_key`.
%%
%% Concrete coverage of the order-preserving composite codec: PK
%% round-trip (incl. PKs containing 0x00), the prefix-of-another-term
%% ordering case the escape exists to fix, integer sign-bias ordering,
%% and the equality/range bounds.
%% =============================================================================

-module(bondy_oplog_index_key_test).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, bondy_oplog_index_key).

%% =============================================================================
%% encode / decode_pk
%% =============================================================================

encode_decode_pk_binary_term_test() ->
    K = ?MOD:encode(<<"active">>, <<"user/1">>),
    ?assertEqual(<<"user/1">>, ?MOD:decode_pk(K)).

encode_decode_pk_integer_term_test() ->
    K = ?MOD:encode(42, <<"user/1">>),
    ?assertEqual(<<"user/1">>, ?MOD:decode_pk(K)).

%% The primary key itself may contain the 0x00 separator byte; decode_pk
%% must scan to the *first* 0x00 (the separator, never a term byte).
decode_pk_with_null_in_primary_key_test() ->
    PK = <<"a", 0, "b", 0, "c">>,
    K = ?MOD:encode(<<"term">>, PK),
    ?assertEqual(PK, ?MOD:decode_pk(K)).

decode_pk_empty_primary_key_test() ->
    K = ?MOD:encode(<<"term">>, <<>>),
    ?assertEqual(<<>>, ?MOD:decode_pk(K)).

decode_pk_no_separator_is_badarg_test() ->
    ?assertError(badarg, ?MOD:decode_pk(<<1, 2, 3>>)).

%% =============================================================================
%% Ordering — the cases the escape exists to make correct
%% =============================================================================

%% "a" < "a\0" < "ab": a term that is a byte-prefix of another must order
%% before it regardless of the appended primary key.
prefix_term_ordering_test() ->
    Ka = ?MOD:encode(<<"a">>, <<255>>),
    Kz = ?MOD:encode(<<"a", 0>>, <<>>),
    Kab = ?MOD:encode(<<"a", "b">>, <<>>),
    ?assert(Ka < Kz),
    ?assert(Kz < Kab).

%% A term embedding 0x00 still orders correctly against a longer term.
embedded_null_term_ordering_test() ->
    K1 = ?MOD:encode(<<0>>, <<"pk">>),
    K2 = ?MOD:encode(<<0, 0>>, <<"pk">>),
    K3 = ?MOD:encode(<<1>>, <<"pk">>),
    ?assert(K1 < K2),
    ?assert(K2 < K3).

same_term_orders_by_primary_key_test() ->
    Ka = ?MOD:encode(<<"t">>, <<"a">>),
    Kb = ?MOD:encode(<<"t">>, <<"b">>),
    ?assert(Ka < Kb).

integer_terms_order_numerically_test() ->
    Neg = ?MOD:encode_term(-1),
    Zero = ?MOD:encode_term(0),
    One = ?MOD:encode_term(1),
    Big = ?MOD:encode_term(1000000),
    ?assert(Neg < Zero),
    ?assert(Zero < One),
    ?assert(One < Big).

integer_extremes_order_test() ->
    Min = ?MOD:encode_term(-(1 bsl 63)),
    NegOne = ?MOD:encode_term(-1),
    Max = ?MOD:encode_term((1 bsl 63) - 1),
    ?assert(Min < NegOne),
    ?assert(NegOne < Max).

integer_out_of_range_is_badarg_test() ->
    ?assertError(badarg, ?MOD:encode_term(1 bsl 63)),
    ?assertError(badarg, ?MOD:encode_term(-(1 bsl 63) - 1)).

%% =============================================================================
%% Bounds
%% =============================================================================

equality_bounds_cover_exactly_the_term_test() ->
    {Lo, Hi} = ?MOD:equality_bounds(<<"active">>),
    InTerm = ?MOD:encode(<<"active">>, <<"pk">>),
    Other = ?MOD:encode(<<"activf">>, <<>>),
    Before = ?MOD:encode(<<"activd">>, <<255>>),
    ?assert(Lo =< InTerm andalso InTerm < Hi),
    ?assertNot(Lo =< Other andalso Other < Hi),
    ?assertNot(Lo =< Before andalso Before < Hi).

range_bounds_half_open_test() ->
    {Lo, Hi} = ?MOD:range_bounds(<<"b">>, <<"d">>),
    Kb = ?MOD:encode(<<"b">>, <<>>),
    Kc = ?MOD:encode(<<"c">>, <<"x">>),
    Kd = ?MOD:encode(<<"d">>, <<>>),
    Ka = ?MOD:encode(<<"a">>, <<255>>),
    %% [b, d): b and c included, d excluded, a excluded
    ?assert(Lo =< Kb andalso Kb < Hi),
    ?assert(Lo =< Kc andalso Kc < Hi),
    ?assertNot(Lo =< Kd andalso Kd < Hi),
    ?assertNot(Lo =< Ka andalso Ka < Hi).
