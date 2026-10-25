using System.Text;

namespace DurableExecutionMachine.Serialization;

// The name a Type is written under in a flow's state: its assembly-qualified name without the
// Version/Culture/PublicKeyToken segments, so that it still resolves after the assembly's version changes.
public static class TypeExtensions
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
        // here instead of handing back one that no longer resolves.
        if (assemblyQualifiedName.Contains('\\'))
            throw new ArgumentException(
                $"Type '{type}' cannot be simplified: its assembly-qualified name contains an escaped " +
                $"character - {assemblyQualifiedName}",
                nameof(type));

        var builder = new StringBuilder(assemblyQualifiedName.Length);
        ExtractSimplifiedName(assemblyQualifiedName, 0, builder);
        return builder.ToString();
    }

    // Walks one bracket-scope of an assembly-qualified name, copying it while dropping
    // the Version/Culture/PublicKeyToken segments. Recurses on '[' so each nested generic
    // argument (and array suffix) is processed in its own scope; returns the index of the
    // ']' that closed this scope so the caller can continue past it.
    private static int ExtractSimplifiedName(string name, int i, StringBuilder stringBuilder)
    {
        // Within a scope the assembly qualifier is "TypeName, Assembly, Version=.., Culture=.., PublicKeyToken=..".
        // The first "real" comma separates the type name from the assembly name (kept); any further commas
        // begin Version/Culture/Token (dropped). Commas of the form "],[" separate generic arguments and are
        // not assembly qualifiers, so they are excluded from the count via the lookahead below.
        var ignore = false;
        var commas = 0;
        for (; i < name.Length; i++)
        {
            var letter = name[i];
            if (letter == '[')
            {
                stringBuilder.Append('[');
                i = ExtractSimplifiedName(name, i + 1, stringBuilder);
                continue;
            }
            else if (letter == ']')
            {
                stringBuilder.Append(']');
                return i;
            }
            else if (letter == ',')
            {
                // A comma directly before '[' is a generic-argument separator ("],["), not an assembly qualifier.
                if (i + 1 < name.Length && name[i + 1] != '[')
                    commas++;
                ignore = commas > 1;
            }
            if (ignore) continue;

            stringBuilder.Append(letter);
        }

        return i;
    }
}
