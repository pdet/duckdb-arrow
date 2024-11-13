# nanoarrow for DuckDB

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, nanoarrow, allows you to read Arrow IPC streams.

## Building

To build the extension, clone the repository with submodules:

``` shell
git clone --recurse-submodules https://github.com/paleolimbot/duckdb-nanoarrow.git
```

...or if you forget to clone the submodules/you're using VSCode to do your checkout, you can run:

``` shell
git submodule init
git submodule update --checkout
```

A quick-and-dirty way to get your build up and running is to run `make`:

```sh
make

```
The main binaries that will be built are:

```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/nanoarrow/nanoarrow.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `nanoarrow.duckdb_extension` is the loadable binary as it would be distributed.

If you'd like to use VSCode with the integration provided by the CMake/clangd extension, you
can run:

``` shell
cp CMakeUserPresets.json duckdb/
```

...and ensure that `.vscode/settings.json` contains:

``` json
{
    "cmake.sourceDirectory": "${workspaceFolder}/duckdb"
}
```

Then choose *Developer: Reload window* from the command palette and choose the
*Extension (Debug build)* preset.

## Running the extension

To run the extension code, simply start the shell with `./build/release/duckdb`
(if you're using `make` to build) or `./build/duckdb` (if you're using CMake
via VSCode).

Now we can use the features from the extension directly in DuckDB.

## Running the tests

Different tests can be created for DuckDB extensions. Tests are written in
SQL  `./test/sql`. These SQL tests can be run using `make test` (if using
make) or `./test_local.sh` (if using CMake via VSCode).

## Debugging

You can debug an interactive SQL session by launching it with `gdb` or `lldb`:

``` shell
lldb build/duckdb
```

...or you can use the CodeLLDB extension (Command Palette: *LLDB: Attach to process*)
to launch a VSCode interactive debugger launched in a terminal.
