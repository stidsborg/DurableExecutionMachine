# Assumptions

Requirements the machine places on flow code and callers that the type system does not enforce.

## Captured results and delivered messages are immutable

A value returned from `Context.Capture` or passed to `Messages.Deliver` must not be mutated afterwards.

`States` serializes lazily: it keeps a reference to the value and only serializes it when it is first needed
(on flush, or when read back). Values that are overwritten or removed before that are never serialized,
and a serialized value is reused by every later flush.

If a value is mutated after it was handed over, the persisted bytes reflect the value as it was at its
first serialization — not as it was when it was captured or delivered.

## Captured values must round-trip through System.Text.Json

Values are serialized with `System.Text.Json` using their runtime type (`result.GetType()`), and the type is
stored by name and resolved with `Type.GetType` on deserialization.

Because serialization is lazy, a value that cannot be serialized fails on the next flush (or when read back),
not inside the `Capture`/`Deliver` call that handed it over.

## Flows must issue durable calls in a deterministic order

Ids for `Capture`, `Delay` and `Parallel` are positional (e.g. `0.1.1`): they are assigned by call order within
the current scope. A replaying flow must make the same calls in the same order to find its stored results.

Concurrent durable work must go through `Context.Parallel`. Starting `Capture`/`Delay` calls concurrently by other
means (e.g. `Task.WhenAll` over helpers that share the context) makes their ids depend on scheduling and breaks replay.

Changing a flow's code — e.g. inserting a `Capture` before an existing one — shifts the ids of later calls and
breaks replay of executions that are already in flight. This is a known and accepted limitation.
