# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/kofta999/tensou/releases/tag/v0.1.0) - 2026-07-12

### Added

- *(core, gui)* add sync mode
- add reconnection
- *(gui)* add transfer pause/resume
- add sender info to handshake and cli
- *(cli)* add multiple file sending
- *(gui)* show peer device name and transfer timestamp in transfer list
- *(gui/cli)* support resume progress bars starting at bytes_done and correct speed calculation
- add clipboard / text sending
- ability to send multiple files and folders
- ui overhaul
- add auto accept option
- add session transfer history

### Fixed

- add app icon to slint
- pause token not saved when resuming
- improve error handling and resume
- *(gui)* use SERVER_PORT as default port in direct send
- remove drag and drop until upstream support
- add toast noti on transfer start
- *(gui)* use single font weight on changing tabs
- make config changes reactive on UI changes
- broken resumes with folders when overwrite is disabled

### Other

- comment out pulsing dot
- fix clippy lints
- use standard transfer id for resume and storage prep
- *(core)* remove single sendtype
- Merge branch 'master' of https://github.com/kofta999/tensou
- *(gui)* more minor refactorings
- *(gui)* minor refactorings
- add logging
- fix lints
- minor fixes
- use crates
