# Contributing

Contributions are welcome. This project is a learning-oriented reference
architecture, so clarity and simplicity are valued over feature count.

## Getting started

```bash
cp .env.example .env
./scripts/download-jars.sh
docker compose up -d
```

See the [README](README.md) for full setup instructions including Flink job
submission and balance seeding.

## Making changes

1. Fork the repo and create a branch from `main`.
2. Make your changes. Keep diffs focused — one logical change per PR.
3. Test locally with `docker compose up -d` and verify the affected services.
4. Open a pull request with a short description of what changed and why.

## Conventions

- All services run via Docker Compose — no external dependencies beyond Docker and Ollama.
- All configuration lives in `.env` — no hardcoded values in source.
- Python services use `python:3.12-slim` base images.
- Flink SQL files are self-contained and submitted via `sql-client.sh`.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/):
  `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`, etc.

## Reporting issues

Open an issue on GitHub. Include:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Output of `docker compose ps` and `docker compose logs <service>` if relevant

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE).
