using System.Text;

namespace DurableExecutionMachine;

public static class TypeHelperExtensions
{
    public static string SimpleQualifiedName(this Type type)
    {
        var assemblyQualifiedName = type.AssemblyQualifiedName;
        if (assemblyQualifiedName == null)
            return type.FullName ?? type.Name;

        // Reflection escapes ',', '+', '[', ']', '*', '&' and '\\' with a backslash when they occur
        // inside a type name rather than as syntax - which C# cannot produce, but F# quoted
        // identifiers, Reflection.Emit and obfuscators can. ExtractSimplifiedName walks the name
        // character by character and does not honour those escapes, so an escaped comma is counted
        // as an assembly qualifier and the assembly name is dropped from the result. Refuse the name
        // here instead of handing back one that no longer resolves. Throwing inside the factory also
        // keeps it out of the cache: GetOrAdd stores nothing when the factory throws.
        if (assemblyQualifiedName.Contains('\\'))
            throw new ArgumentException(
                $"Type '{type}' cannot be simplified: its assembly-qualified name contains an escaped " +
                $"character - {assemblyQualifiedName}",
                nameof(type));

        var builder = new StringBuilder(assemblyQualifiedName.Length);
        TypeHelper.ExtractSimplifiedName(assemblyQualifiedName, 0, builder);
        return builder.ToString();
    }
}