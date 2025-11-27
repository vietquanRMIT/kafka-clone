# Repository Guidelines

## Project Structure & Module Organization

This is a Gradle multi-module project with three modules:

- **kafka-proto/**: Shared gRPC/Protocol Buffer definitions
  - `src/main/proto/api/kafka.proto`: gRPC contract. Regenerate stubs with `./gradlew :kafka-proto:generateProto` after edits.
  
- **kafka-server/**: Spring Boot gRPC broker (server)
  - `src/main/java/com/example/kafkaclone/`: Server application and service logic under `service/`.
  - `src/main/resources/application.yml`: Spring configuration.
  - `src/test/java/`: JUnit tests; mirror package paths from `src/main/java`.
  
- **kafka-client/**: CLI module using Picocli
  - `src/main/java/com/example/kafkaclient/cmd/`: Picocli commands for produce/consume operations.

## Build, Test, and Development Commands
- `./gradlew build`: Build all modules, run tests, and assemble JARs.
- `./gradlew :kafka-server:bootRun`: Launch the broker locally on `localhost:9090`.
- `./gradlew :kafka-client:bootRun --args='produce -t demo -p 0 -m hi'`: Execute CLI commands against a running broker.
- `./gradlew test`: Run all tests across all modules.
- `./gradlew :kafka-server:test`: Run only server tests.
- `./gradlew :kafka-client:test`: Run only client tests.

## Coding Style & Naming Conventions
- Java 21, 4-space indentation, braces on same line. Favor descriptive class names (e.g., `KafkaService`, `ProducerCommand`).
- gRPC/Proto enums use `UPPER_SNAKE_CASE`; message fields stay `lower_snake_case` per proto style.
- Keep CLI options in Picocli annotated fields named after their semantic role (`topic`, `partition`).

## Testing Guidelines
- Tests live under the matching package in `src/test/java`. Class names end with `Test` or `Tests` (e.g., `KafkaServiceTest`).
- Use Spring Boot test slices when wiring context; use pure JUnit + Mockito for service/unit layers.
- Validate produce/consume flows with integration tests that spin up the gRPC server in-memory.

## Commit & Pull Request Guidelines
- Follow conventional concise commits: `<type>: <summary>` (e.g., `feat: add consumer CLI`).
- Squash work into logical units, include proto or config regenerations in the same commit.
- PRs must describe motivation, testing commands run, and include CLI output/log snippets when relevant. Link issues/tickets and note breaking changes explicitly.

## Security & Configuration Tips
- Local development uses plaintext gRPC on `localhost:9090`; do not expose publicly without TLS/auth hardening.
- Store secrets outside the repo; prefer environment variables and Spring config placeholders.
