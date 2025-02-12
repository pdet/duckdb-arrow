//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/array_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_reader/base_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {
class IpcArrayStream {
 public:
  explicit IpcArrayStream(unique_ptr<IPCStreamReader> reader);

  IPCStreamReader& Reader() const;

  void ToArrayStream(ArrowArrayStream* stream);

  int GetSchema(ArrowSchema* schema);

  int GetNext(ArrowArray* array);

  const char* GetLastError() const;

  template <typename Func>
  int Wrap(Func&& func) {
    try {
      func();
      return NANOARROW_OK;
    } catch (IOException& e) {
      last_msg = std::string("IOException: ") + e.what();
      return EIO;
    } catch (InternalException& e) {
      last_msg = std::string("InternalException: ") + e.what();
      return EINVAL;
    } catch (nanoarrow::Exception& e) {
      last_msg = std::string("nanoarrow::Exception: ") + e.what();
      // Could probably find a way to pass on this code, usually ENOMEM
      return ENOMEM;
    } catch (std::exception& e) {
      last_msg = e.what();
      return EINVAL;
    }
  }

 private:
  unique_ptr<IPCStreamReader> reader;
  string last_msg;
};
}  // namespace ext_nanoarrow
}  // namespace duckdb
