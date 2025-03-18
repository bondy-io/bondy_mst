-if(?OTP_RELEASE >= 27).
-define(MODULEDOC(Str), -moduledoc(Str)).
-define(DOC(Str), -doc(Str)).
-else.
-define(MODULEDOC(Str), -compile([])).
-define(DOC(Str), -compile([])).
-endif.

-define(ROOT_KEY, <<"$root">>).

-define(T2B_OPTS, [{minor_version, 2}]).

-type optional(T)   ::  T | undefined.
-type hash()        ::  binary().
-type key()         ::  any().
-type value()       ::  any().
-type level()       ::  non_neg_integer().
-type epoch()       ::  integer().