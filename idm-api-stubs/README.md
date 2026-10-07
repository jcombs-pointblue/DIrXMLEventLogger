# Identity Manager API stubs (compile-only)

Signature-only stand-ins for the seven Identity Manager driver API types the
Event Logger compiles against: `DriverShim`, `SubscriptionShim`, `PublicationShim`,
`XmlQueryProcessor`, `XmlCommandProcessor`, `XmlDocument` and `Trace`. They contain
no NetIQ / OpenText code and are never packaged: the driver jar declares them
`provided`, and the engine supplies the real classes at runtime.

The build adds them automatically when `lib/dirxml.jar` is missing and excludes
`com/novell/**` from the jar. They exist so CI can build and release the driver without the proprietary engine
jars. When the real jars are in `lib/`, the build uses them instead (see the
README's "Building the JAR").

Keep the stubs to exactly the members the driver uses. Adding a member here that
does not exist in the real API compiles fine but fails on the engine with
`NoSuchMethodError`.
