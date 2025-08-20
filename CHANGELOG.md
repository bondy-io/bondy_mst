# CHANGELOG

## 0.4.0
### Changes:
- Added `callback_mfa` field to the record alongside existing `callback_mod`
- Updated type definitions to include callback_mfa => {module(), atom(), list()}
- Implemented validate_callback/1 function that handles both patterns
- Added call_callback/3 helper function that routes calls appropriately:
    - For callback_mod: calls Module:Function(Args)
    - For callback_mfa: calls Module:Fun(ExtraArgs ++ [Function, Args])
- Updated all callback invocations (send, broadcast, on_merge) to use the new pattern
- Maintained full backward compatibility