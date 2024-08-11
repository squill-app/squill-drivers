function codecov() {
  export LLVM_PROFILE_FILE=rsql-%p-%m.profraw
  export RUST_BACKTRACE=1
  export RUST_LOG="info"
  export RUST_LOG_SPAN_EVENTS=full
  export RUSTFLAGS=-Cinstrument-coverage
  export RUSTDOCFLAGS=-Cinstrument-coverage

  cargo test --workspace && \
  grcov $(find . -name "rsql-*.profraw" -print) \
    -s . \
    --branch \
    --ignore-not-existing \
    --ignore='./squill-core/src/mock.rs' \
    --ignore='target/*' \
    --ignore='benches/*' \
    --ignore='/*' \
    --binary-path ./target/debug/ \
    --excl-line='#\[derive' \
    -t html \
    -o ./target/coverage/
}

function clean() {
  local profile_files=($(find . -name "rsql-*.profraw" -print))
  for file in "${profile_files[@]}"; do
    rm $file
  done
  [ -f "lcov.info" ] && rm lcov.info
}

function ctest() {
  cargo test --workspace
}