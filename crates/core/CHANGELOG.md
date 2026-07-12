# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/kofta999/tensou/releases/tag/v0.1.0) - 2026-07-12

### Added

- *(core, gui)* add sync mode
- add reconnection
- add sender info to handshake and cli
- *(cli)* add multiple file sending
- *(gui/cli)* support resume progress bars starting at bytes_done and correct speed calculation
- add clipboard / text sending
- ability to send multiple files and folders
- add auto accept option
- add disk space verification
- use .tensou directory for staging files

### Fixed

- receiver cancel reconnects instead
- improve error handling and resume
- *(core/disk)* cancel transfer on hash failure
- add toast noti on transfer start
- *(core)* nest staging directory under target folder, fix daemon busy loop and chunk seek corruption
- *(state)* handle invalid state file case
- perf optimization for transfer (temp)
- make config changes reactive on UI changes
- broken resumes with folders when overwrite is disabled
- *(find_unique_path)* ignore extension if it's a folder
- catch connectionclose error on reject
- use cancel tokens with network chunks

### Other

- another lint
- fix fmt and lints
- fix clippy lints
- use standard transfer id for resume and storage prep
- *(core/net)* receiver structure and const names
- *(core)* remove single sendtype
- *(core)* improve code structure
- *(gui)* more minor refactorings
- fix lints
- format
- format
- add logging
- comment weird test
- fix lints
- add source comment for find_extension_start
- add a lot
- minor fixes
- remove sleep
- fix failing? test
- use crates
