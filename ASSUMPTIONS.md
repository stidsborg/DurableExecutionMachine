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

## A scope must not complete while anything started inside it is still running

A `Capture`, a `Parallel` or the flow itself must not complete while a `Parallel` subflow, a `Context.Message` or a
`Context.Delay` started inside it is still running or waiting.

A running subflow can only be restarted by replaying the code that started it. When a `Capture` or `Parallel`
completes, its result is stored and its descendants are removed, and on replay its code is skipped. Anything still
running inside it is then lost when the machine restarts, and its later writes end up under a parent whose
descendants have already been removed.

Waits cannot be cancelled. An abandoned `Context.Message` stays subscribed and takes the next matching message, so a
later `Context.Message` never sees it. An abandoned wait also stays counted as suspended: the machine can then report
all subflows as suspended while the flow is running, and a sync after the flow has completed never returns.

`Task.WhenAny` over `Context.Message` or `Context.Delay` calls breaks this rule, as the losing wait is never awaited
again. To wait for one of several messages, use a single `Context.Message` whose filter matches all of them. To wait
for a message or a deadline, use its `timeout` parameter.

## Choices that depend on timing must be captured

When several tasks have already completed, `Task.WhenAny` returns the first of them in argument order. On replay,
stored results complete immediately, so a choice that depended on which task finished first in the original run can
turn out differently. Wrap such a choice in a `Context.Capture`, so it is stored and read back on replay:

```csharp
var first = await ctx.Capture(async () => await Task.WhenAny(a, b) == a ? "a" : "b");
```

As in .NET, the branches that lose a `Task.WhenAny` keep running. The enclosing scope must still not complete before
they have.
