PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=arrow
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Client tests
DEBUG_EXT_PATH='$(PROJ_DIR)build/debug/extension/arrow/arrow.duckdb_extension'
RELDEBUG_EXT_PATH='$(PROJ_DIR)build/release/extension/arrow/arrow.duckdb_extension'
RELEASE_EXT_PATH='$(PROJ_DIR)build/release/extension/arrow/arrow.duckdb_extension'

test_js:
test_debug_js:
	ARROW_EXTENSION_BINARY_PATH=$(DEBUG_EXT_PATH) mocha -R spec --timeout 480000 -n expose-gc --exclude 'test/*.ts' -- "test/nodejs/**/*.js"
test_release_js:
	ARROW_EXTENSION_BINARY_PATH=$(RELEASE_EXT_PATH) NODE_PATH=/opt/homebrew/Cellar/node/23.11.0/lib/node_modules mocha -R spec --timeout 480000 -n expose-gc --exclude 'test/*.ts' -- "test/nodejs/**/*.js"

run_benchmark:
	python3 benchmark/lineitem.py $(RELEASE_EXT_PATH)
