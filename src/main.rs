use test_sqlite_perf::test_path;

fn main() {
    let temp = std::env::temp_dir();
    test_path(temp);
}
