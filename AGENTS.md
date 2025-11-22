# Repository Guidelines

## Project Structure & Module Organization
- `src/main/java`: Spring Boot gRPC broker. Keep service logic under `service/`.
- `src/main/proto`: gRPC contract (`api/kafka.proto`). Regenerate stubs with `./gradlew generateProto` after edits.
- `src/main/resources`: Spring configuration (e.g., `application.yml`).
- `src/test/java`: JUnit tests; mirror package paths from `src/main/java`.
- `build/`: Gradle outputs. Do not commit.
- `kafka-client/`: Standalone CLI module. Its `src/main/java/com/example/kafkaclient/cmd` tree hosts Picocli commands, and it reuses the root proto via Gradle configuration.

## Build, Test, and Development Commands
- `./gradlew build`: Compile the broker, run tests, and assemble the bootable JAR.
- `./gradlew bootRun`: Launch the broker locally on `localhost:9090`.
- `./gradlew test`: Run the broker’s JUnit suite; required before any PR.
- `cd kafka-client && ./gradlew bootRun --args='kafka produce -t demo -p 0 -m hi'`: Execute CLI commands against a running broker. Replace args with `consume` to read messages.

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
