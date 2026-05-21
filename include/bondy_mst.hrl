-if(?OTP_RELEASE >= 27).
-define(MODULEDOC(Str), -moduledoc(Str)).
-define(DOC(Str), -doc(Str)).
-else.
-define(MODULEDOC(Str), -compile([])).
-define(DOC(Str), -compile([])).
-endif.

-define(ROOT_KEY, <<"$root">>).

-define(T2B_OPTS, [{minor_version, 2}]).

%% Leveled object tag used for projection cell frames. The tag activates
%% the `bondy_oplog_leveled_tag` extractor + head-builder pair so
%% `leveled_bookie:book_head/4` returns the HEAD wire format
%% (`<<HlcLen:16, Hlc/binary, ValueBytes/binary>>`) instead of the full
%% V2 frame — see `_design/catalogue_expansion_plan.md` §3.4.
-define(BONDY_FOLD_TAG, o_fold).

-type optional(T) :: T | undefined.
-type hash() :: binary().
-type key() :: any().
-type value() :: any().
-type level() :: non_neg_integer().
-type epoch() :: integer().
