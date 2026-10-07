# Engine jars (not committed)

Optional. Copy `dirxml.jar` from an Identity Manager install (or your IDM Driver
Dependencies set) here to compile against the real engine API instead of the
stubs in `idm-api-stubs/`. These jars are proprietary and gitignored; never
commit them. You can also point at another directory with `-Didm.lib=/path`.
