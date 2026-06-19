fn main() {
    println!("Hello, world!");

    // allocate_sparse_file("test.bin", 1024 * 1024 * 1024 * 10);

    // let file = fs::metadata("random_file.bin").unwrap();

    // let metadata = Metadata {
    //     chunk_size: 4 * 1024 * 1024,
    //     filename: "random_file.bin".into(),
    //     size: file.size(),
    // };

    // let mut session = ReceiveSession::new(metadata).unwrap();

    // session.save_state();

    // // let c = session.get_chunk(0).unwrap();

    // dbg!(c.hash, c.index, c.bytes.len());
}
