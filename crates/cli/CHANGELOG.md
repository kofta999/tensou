# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/kofta999/tensou/releases/tag/v0.1.0) - 2026-07-12

### Added

- *(cli)* add transfer mode selection
- add reconnection
- add sender info to handshake and cli
- *(cli)* add multiple file sending
- *(gui/cli)* support resume progress bars starting at bytes_done and correct speed calculation
- add clipboard / text sending
- ability to send multiple files and folders

### Fixed

- improve error handling and resume
- make config changes reactive on UI changes

### Other

- use standard transfer id for resume and storage prep
- *(core)* remove single sendtype
- fix lints
- use crates
