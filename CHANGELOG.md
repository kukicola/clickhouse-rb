## Unreleased

- Make responses more user-friendly ([#1](https://github.com/kukicola/clickhouse-rb/pull/1))

## [0.2.0] - 2025-12-28

- Added instrumentation support with configurable `instrumenter` (defaults to `NullInstrumenter`)
- Query errors now raise `Clickhouse::QueryError` instead of returning a failed response
- Removed `Response#success?`, `Response#failure?`, and `Response#error` methods

## [0.1.0] - 2025-12-28

- Initial release
