// #![feature(test)]
//
// extern crate test;
//
// #[path = "../src/main.rs"]
// mod main;
//
// #[cfg(test)]
// mod tests {
//     use test::Bencher;
//
//     #[bench]
//     fn small(b: &mut Bencher) {
//         b.iter(|| {
//             crate::main::run("test_data/transactions.csv").unwrap();
//         });
//     }
//
//     #[bench]
//     fn medium(b: &mut Bencher) {
//         b.iter(|| {
//             crate::main::run("test_data/medium_test.csv").unwrap();
//         });
//     }
//
//     // #[bench]
//     // fn large(b: &mut Bencher) {
//     //     b.iter(|| {
//     //         crate::main::run("test_data/large_test.csv").unwrap();
//     //     });
//     // }
// }
