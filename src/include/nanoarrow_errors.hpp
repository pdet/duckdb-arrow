//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// nanoarrow_errors.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#define _DUCKDB_NANOARROW_THROW_NOT_OK_IMPL(NAME, ExceptionCls, ERROR_PTR, EXPR,      \
                                            EXPR_STR)                                 \
  do {                                                                                \
    const int NAME = (EXPR);                                                          \
    if (NAME) {                                                                       \
      throw ExceptionCls(std::string(EXPR_STR) + std::string(" failed with errno ") + \
                         std::to_string(NAME) + std::string(": ") +                   \
                         std::string((ERROR_PTR)->message));                          \
    }                                                                                 \
  } while (0)

#define THROW_NOT_OK(ExceptionCls, ERROR_PTR, EXPR)                                     \
  _DUCKDB_NANOARROW_THROW_NOT_OK_IMPL(_NANOARROW_MAKE_NAME(errno_status_, __COUNTER__), \
                                      ExceptionCls, ERROR_PTR, EXPR, #EXPR)
