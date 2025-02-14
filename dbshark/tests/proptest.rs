mod common_tests;
use rand::Rng;

#[test]
fn proptest() {
    let mut rng = rand::thread_rng();
    loop {
        let choice = rng.gen_range(0..2);
        let seed = rng.gen::<u64>();
        match choice {
            0 => {
                println!("run simulate_db_crashing seed={seed}");
                common_tests::simulate_db_crashing(seed);
            }
            1 => {
                println!("run simulate_concurrent_checkpoint_and_rollback seed={seed}");
                common_tests::simulate_concurrent_checkpoint_and_rollback(seed);
            }
            _ => unreachable!(),
        }
    }
}

