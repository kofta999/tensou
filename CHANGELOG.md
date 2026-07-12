# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/kofta999/tensou/releases/tag/v0.1.0) - 2026-07-12

### Added

- add reconnection
- *(gui/cli)* support resume progress bars starting at bytes_done and correct speed calculation
- add clipboard / text sending
- ui overhaul
- add session transfer history
- add disk space verification
- improve settings page
- improve config and discovery
- add transfer cancellation
- *(gui)* add basic functionality
- prevent file overwrite
- impl graceful shutdown
- set custom ip and port when sending
- add transfer confirmation
- add progress bars
- improve cli experience
- add basic service discovery
- add multiple file transfers
- add quic networking

### Fixed

- add app icon to slint
- remove panic abort as tasks could panic
- hide console on windows
- perf optimization for transfer (temp)
- broken resumes with folders when overwrite is disabled
- *(ci)* move packager config to cargo.toml
- *(ci)* use correct packager package name
- ui improvments
- network optimizations

### Other

- add release plz
- add readme
- use standard transfer id for resume and storage prep
- remove macos from ci
- add logging
- *(ci)* make ci on manual trigger only (temp)
- fix lints
- use correct branch
- add release workflow
- use crates
- create background and callbacks modules
- improve UI
- create command files
- *(gui)* slint
- move receiverdaemon to recv.rs
- use config
- code structure and reduce duplication
- use observer pattern instead of broadcast channel for transferevents
- fix tests and use trait for consent
- remove clutter in types
- 10x performance improvments
- create net module
- initial commit
