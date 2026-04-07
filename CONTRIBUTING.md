# Contributing

Thanks for helping improve offerTrackML.

## Before you PR

- Run tests: `cd rust/offertrack-crawler && cargo test`.
- Do not commit secrets: use `.env` locally (see `.env.example`); never add real API keys or `state/*.db` / `out/*.json` outputs.
- Keep changes focused on the **open-source scope** in [README.md](README.md): job crawling and export only. Embeddings, RAG, batch jobs, and training belong in a private repository.

## Pull requests

- Describe what changed and why in plain language.
- Link related issues if any.
- Unless you state otherwise, contributions are accepted under the same terms as this project: **Apache License 2.0** (see [LICENSE](LICENSE)).
