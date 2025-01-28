//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// array_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_reader.hpp"

namespace duckdb{
namespace ext_nanoarrow {
class IpcArrayStream {
 public:
  IpcArrayStream(unique_ptr<IpcStreamReader> reader) : reader(std::move(reader)) {}

  IpcStreamReader& Reader() { return *reader; }

  void ToArrayStream(ArrowArrayStream* stream) {
    nanoarrow::ArrayStreamFactory<IpcArrayStream>::InitArrayStream(
        new IpcArrayStream(std::move(reader)), stream);
  }

  int GetSchema(ArrowSchema* schema) {
    return Wrap([&]() {
      NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(reader->GetOutputSchema(), schema));
    });
  }

  int GetNext(ArrowArray* array) {
    return Wrap([&]() { reader->GetNextBatch(array); });
  }

  const char* GetLastError() { return last_msg.c_str(); }

 private:
  unique_ptr<IpcStreamReader> reader;
  string last_msg;

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
};
}
}