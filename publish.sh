pushd crates

pushd tdb-core
cargo publish --allow-dirty
popd

pushd tdb-server-core
cargo publish --allow-dirty
popd

pushd tdb-cli
cargo publish --allow-dirty
popd

popd

cargo publish --allow-dirty
