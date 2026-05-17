Bench.setup()

# CRDT folds — apply_event/2 and merge_states/2 across the
# implementations the substrate ships with. Hottest per-event
# functions in the system.

# Logical part is 16-bit, so use the physical part to spread events.
hlc = fn n -> :bondy_oplog_hlc.encode(1_700_000_000_000 + n, 0) end

orset_state = fn n_elements ->
  base = :bondy_oplog_fold_orset.initial_value()

  Enum.reduce(1..n_elements, base, fn i, s ->
    dot = {"node-a", i}
    :bondy_oplog_fold_orset.apply_event(s, {:add, hlc.(i), "e#{i}", dot})
  end)
end

lww_state = fn ->
  Enum.reduce(1..1000, :bondy_oplog_fold_lww_register.initial_value(), fn n, s ->
    :bondy_oplog_fold_lww_register.apply_event(s, {:set, hlc.(n), "v#{n}"})
  end)
end

presence_state =
  :bondy_oplog_fold_presence_basic.apply_event(
    :bondy_oplog_fold_presence_basic.initial_value(),
    {:create, hlc.(1), "payload"}
  )

strict_state =
  :bondy_oplog_fold_strict_register.apply_event(
    :bondy_oplog_fold_strict_register.initial_value(),
    {:set, hlc.(1), "value"}
  )

scenarios = %{
  "lww / apply_event (set newer)" =>
    fn _ -> :bondy_oplog_fold_lww_register.apply_event({:set, "old", hlc.(5)}, {:set, hlc.(100), "new"}) end,
  "lww / apply_event (rejected)" =>
    fn _ -> :bondy_oplog_fold_lww_register.apply_event({:set, "old", hlc.(100)}, {:set, hlc.(5), "new"}) end,
  "lww / merge_states (lhs wins)" =>
    fn _ -> :bondy_oplog_fold_lww_register.merge_states({:set, "a", hlc.(100)}, {:set, "b", hlc.(50)}) end,
  "orset / apply_event (add new dot, set=1k)" =>
    {fn {state, i} ->
       :bondy_oplog_fold_orset.apply_event(state, {:add, hlc.(i + 5_000), "new-e", {"node-b", i + 5_000}})
     end,
     before_each: fn state -> {state, System.unique_integer([:positive])} end,
     before_scenario: fn _ -> orset_state.(1_000) end},
  "orset / apply_event (remove existing, set=1k)" =>
    {fn {state, i} ->
       :bondy_oplog_fold_orset.apply_event(state, {:remove, hlc.(i + 100_000), "e#{rem(i, 1_000) + 1}", [{"node-a", rem(i, 1_000) + 1}]})
     end,
     before_each: fn state -> {state, System.unique_integer([:positive])} end,
     before_scenario: fn _ -> orset_state.(1_000) end},
  "orset / merge_states (1k × 1k)" =>
    {fn {a, b} -> :bondy_oplog_fold_orset.merge_states(a, b) end,
     before_scenario: fn _ ->
       a = orset_state.(1_000)
       b =
         Enum.reduce(1..1_000, :bondy_oplog_fold_orset.initial_value(), fn i, s ->
           :bondy_oplog_fold_orset.apply_event(s, {:add, hlc.(i + 500_000), "b#{i}", {"node-b", i}})
         end)
       {a, b}
     end},
  "presence_basic / apply_event" =>
    fn _ -> :bondy_oplog_fold_presence_basic.apply_event(presence_state, {:create, hlc.(50), "payload2"}) end,
  "strict_register / apply_event" =>
    fn _ -> :bondy_oplog_fold_strict_register.apply_event(strict_state, {:set, hlc.(50), "v2"}) end
}

Benchee.run(scenarios, [inputs: %{"folds" => :ok}] ++ Bench.benchee_opts("folds"))

# ----- Encoding/decoding (codec hot path) -----

state_to_encode = lww_state.()

codec_scenarios = %{
  "lww / encode_state (1k events absorbed)" =>
    fn _ -> :bondy_oplog_fold_lww_register.encode_state(state_to_encode) end,
  "lww / decode_state" =>
    {fn bin -> :bondy_oplog_fold_lww_register.decode_state(bin) end,
     before_scenario: fn _ -> :bondy_oplog_fold_lww_register.encode_state(state_to_encode) end},
  "lww / encode_event" =>
    fn _ -> :bondy_oplog_fold_lww_register.encode_event({:set, hlc.(1), "value"}) end,
  "lww / decode_event" =>
    {fn bin -> :bondy_oplog_fold_lww_register.decode_event(bin) end,
     before_scenario: fn _ ->
       :bondy_oplog_fold_lww_register.encode_event({:set, hlc.(1), "value"})
     end}
}

Benchee.run(codec_scenarios, [inputs: %{"codec" => :ok}] ++ Bench.benchee_opts("folds_codec"))
