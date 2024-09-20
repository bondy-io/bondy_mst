%% ===========================================================================
%%  bondy_mst_utils.erl -
%%
%%  Copyright (c) 2023-2024 Leapsight. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% ===========================================================================



-module(bondy_mst_utils).
-moduledoc """
""".

-export([hash/1]).
-export([hash/2]).



%% =============================================================================
%% API
%% =============================================================================

-doc """
""".
-spec hash(Term :: term()) -> Digest :: binary().

hash(Term) ->
    hash(Term, sha256).


-doc """
""".
-spec hash(Term :: term(), Algo :: atom()) -> Digest :: binary().

hash(Term, Algo) ->
    crypto:hash(Algo, erlang:term_to_binary(Term, [{minor_version, 2}])).



