otp_path: "/Users/aramallo/otp/27.0.1/"
% otp_path: "${_KERL_ACTIVE_DIR}"
apps_dirs:
  - "apps/*"
  - "lib/*"
deps_dirs:
  - "_build/default/lib/*"
  - "_build/default/checkouts/*"
include_dirs:
- "include"
- "_build/default/lib/"
- "_build/default/lib/*/include"
- "_build/default/lib/*/apps/*/include"
- "_checkouts/*/include"
- "_checkouts/*/apps/*/include"
- "lib"
- "lib/*/include"
- "lib/*/apps/*/include"
diagnostics:
  enabled:
  disabled:
    - crossref
    - dialyzer
lenses:
  enabled:
    - function-references
  disabled:
    - ct-run-test
    - show-behaviour-usages
    - suggest-spec
    - server-info