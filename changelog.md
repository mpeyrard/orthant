# Changelog
All notable changes to this project will be documented in this file.

This format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - UNRELEASED
### Added
- Initial project scaffolding (`orthant`).
- MkDocs + Material documentation site.
  - `index.md`, `quickstart.md`, `guides/*`, `changelog.md`.
  - `mkdocs.yml` with navigation, search, emoji, minify, and git-revision-date plugins.
- `uv`-managed environment pinned to Python **3.14t**.
  - Docs dependencies in a dedicated **docs** group and locked via `uv.lock`.
- GitHub Actions workflow to build and deploy docs to **GitHub Pages**.
- Pre-commit baseline hooks for YAML/EOF/trailing whitespace.

### Changed
- N/A

### Fixed
- N/A

### Deprecated
- N/A

### Removed
- N/A

### Security
- N/A
