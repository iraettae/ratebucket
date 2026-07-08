# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1] - 2026-07-08

The release pipeline, published before there is anything to release. This version
exists to prove that building, checking and publishing work end to end, on a real
index, with no API token anywhere. It contains no library.

### Added

- Packaging with uv and hatchling, `py.typed`, src-layout.
- `pyproject.toml` as the only configuration file.
- ruff (linter and formatter), mypy `--strict`, pytest.
- CI: lint, a mypy matrix over 3.11/3.12/3.13, a pytest matrix, and a build job
  that fails if `py.typed` is missing from the wheel.
- A secret scan over every commit, not just the tip.
- Trusted Publishing to TestPyPI, and to PyPI behind a human approval gate.

[Unreleased]: https://github.com/iraettae/ratebucket/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/iraettae/ratebucket/releases/tag/v0.0.1
