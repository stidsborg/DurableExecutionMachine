using Wolverine.Persistence.Sagas;

namespace Dormouse.Messages;

// By convention Wolverine looks for a member named "{SagaType}Id" - "OrderId" does
// not match OrderFlow, so [SagaIdentity] names the identity member explicitly.
public record StartOrder([property: SagaIdentity] string OrderId, string Item);
