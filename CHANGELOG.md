# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0](https://github.com/kofta999/tensou/compare/tensou-v0.1.0...tensou-v0.2.0) (2026-08-05)


### Features

* add basic small file batching ([f02328f](https://github.com/kofta999/tensou/commit/f02328f6ccb83d53799976b231403f79b8e3b87a))
* add basic small file batching ([6cdfe02](https://github.com/kofta999/tensou/commit/6cdfe025f5d66db4e12eff26f72f64f4550092fa))
* add modified time sync and observer integration ([134e0a7](https://github.com/kofta999/tensou/commit/134e0a765148253f907b28e6328630b67aa1fe07))


### Bug Fixes

* **bench:** use only source path for transfer benches ([c86cbc6](https://github.com/kofta999/tensou/commit/c86cbc616de4d25ba72fc0f7ed9e33f877a4c0b3))
* **ci:** define explicit version in subcrate Cargo.toml for release-please ([1de5e3d](https://github.com/kofta999/tensou/commit/1de5e3dc99e77c0cd594bc69da329c8cf421e02f))
* **ci:** fix release-please action repo and subcrate workspace version parsing ([22216ff](https://github.com/kofta999/tensou/commit/22216ffeeaede72e9443ab427bd4eed1a9310906))
* **ci:** pass custom secret token to release-please action ([5bc727c](https://github.com/kofta999/tensou/commit/5bc727cf0a770b7c2b5505c0dbe8a91175f44e7f))
* **ui:** bytes calculation in transfer bar ([d118337](https://github.com/kofta999/tensou/commit/d1183377cab696d044a3fb4d3e4019597a92b241))


### Performance Improvements

* improve file transfer performance ([ef1c235](https://github.com/kofta999/tensou/commit/ef1c235f430e18ad0400313028bc71a9cfc097d6))

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
