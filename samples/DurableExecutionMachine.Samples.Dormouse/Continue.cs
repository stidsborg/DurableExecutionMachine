using System.Text;
using Wolverine;
using Wolverine.Attributes;
using Wolverine.Persistence.Sagas;

namespace Dormouse;

/// <summary>
/// Sent by a flow to itself when a turn ends at a sync point, so the next turn runs on from the committed state.
/// It is published with the turn's message context, so it is committed together with the state, and it carries only the flow's id:
/// the state is the saga's, and a copy here could overwrite progress made by a message handled in between.
/// </summary>
/// <remarks>
/// It writes itself to bytes (<see cref="ISerializable"/>), so Wolverine uses its own binary serializer for it on every route,
/// whatever serializer the application configures. The identity keeps its name on the wire stable when the type is renamed or moved.
/// </remarks>
[MessageIdentity("dormouse-continue")]
public sealed record Continue([property: SagaIdentity] string FlowId) : ISerializable
{
    public byte[] Write() => Encoding.UTF8.GetBytes(FlowId);

    public static object Read(byte[] bytes) => new Continue(Encoding.UTF8.GetString(bytes));
}
