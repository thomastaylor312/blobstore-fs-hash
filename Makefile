# blobstore-fs Makefile

CAPABILITY_ID = "wasmcloud:blobstore"
NAME = "Filesystem Hash Blobstore"
VENDOR = "wasmcloud"
PROJECT = blobstore_fs_hash
VERSION  = $(shell cargo metadata --no-deps --format-version 1 | jq -r '.packages[] .version' | head -1)
REVISION = 0

# builds are 'release'
# If using debug builds, change bin_path in provider_test_config.toml
TEST_FLAGS := --release -- --nocapture

include ./provider.mk

ifeq ($(shell nc -zt -w1 127.0.0.1 4222 || echo fail),fail)
test::
	@killall blobstore_fs || true
	docker run --rm -d --name fs-provider-test -p 127.0.0.1:4222:4222 nats:2.9.16-alpine -js
	RUST_BACKTRACE=1 RUST_LOG=debug cargo test $(TEST_FLAGS)
	docker stop fs-provider-test
else
test::
	@killall blobstore_fs || true
	cargo test $(TEST_FLAGS)
endif
