use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;

struct Fixture {
    name: &'static str,
    nesting_level: u64,
    files_per_level: u64,
}

impl Fixture {
    fn create_dir(&self) -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut current_path = dir.path().join(self.name);
        let dummy_data = vec![0u8; 4 * 1024];

        for level in 0..self.nesting_level {
            current_path = current_path.join(format!("level_{level}"));
            fs::create_dir_all(&current_path).unwrap();

            for i in 0..self.files_per_level {
                let file_path = current_path.join(format!("file_{i}.bin"));
                let mut f = File::create(file_path).unwrap();
                f.write_all(&dummy_data).unwrap();
            }
        }

        dir
    }
}

const FIXTURES: &[Fixture] = &[
    Fixture {
        nesting_level: 1,
        name: "Flat_Structure",
        files_per_level: 100,
    },
    Fixture {
        nesting_level: 100,
        name: "Deep_Structure",
        files_per_level: 100,
    },
    Fixture {
        nesting_level: 10,
        name: "Realistic_Large_Tree",
        files_per_level: 100_000,
    },
];

fn benchmark_manifest(c: &mut Criterion) {
    let mut group = c.benchmark_group("manifest");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

    for fixture in FIXTURES {
        let tempdir = fixture.create_dir();

        group.throughput(Throughput::Elements(
            fixture.files_per_level * fixture.nesting_level,
        ));

        group.bench_function(fixture.name, |b| {
            b.iter(|| {
                tensou_core::protocol::manifest::build(&[PathBuf::from(tempdir.path())]).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_manifest);
criterion_main!(benches);
