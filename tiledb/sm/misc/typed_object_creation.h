/**
 * @file   typed_object_creation.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file defines a way to allocate typed objects.
 */

#ifndef TILEDB_TYPED_OBJECT_ALLOCATION_H
#define TILEDB_TYPED_OBJECT_ALLOCATION_H

#include "tiledb/sm/enums/datatype.h"

namespace tiledb {
namespace sm {

#define INTEGER_CONVERSIONS                                   \
  case Datatype::INT8:                                        \
    return tdb_new(T<int8_t>, std::forward<Args>(args)...);   \
  case Datatype::INT16:                                       \
    return tdb_new(T<int16_t>, std::forward<Args>(args)...);  \
  case Datatype::INT32:                                       \
    return tdb_new(T<int32_t>, std::forward<Args>(args)...);  \
  case Datatype::INT64:                                       \
    return tdb_new(T<int64_t>, std::forward<Args>(args)...);  \
  case Datatype::UINT8:                                       \
    return tdb_new(T<uint8_t>, std::forward<Args>(args)...);  \
  case Datatype::UINT16:                                      \
    return tdb_new(T<uint16_t>, std::forward<Args>(args)...); \
  case Datatype::UINT32:                                      \
    return tdb_new(T<uint32_t>, std::forward<Args>(args)...); \
  case Datatype::UINT64:                                      \
    return tdb_new(T<uint64_t>, std::forward<Args>(args)...);

#define FLOATING_POINT_CONVERSIONS                         \
  case Datatype::FLOAT32:                                  \
    return tdb_new(T<float>, std::forward<Args>(args)...); \
  case Datatype::FLOAT64:                                  \
    return tdb_new(T<double>, std::forward<Args>(args)...);

#define TIME_CONVERSIONS         \
  case Datatype::DATETIME_YEAR:  \
  case Datatype::DATETIME_MONTH: \
  case Datatype::DATETIME_WEEK:  \
  case Datatype::DATETIME_DAY:   \
  case Datatype::DATETIME_HR:    \
  case Datatype::DATETIME_MIN:   \
  case Datatype::DATETIME_SEC:   \
  case Datatype::DATETIME_MS:    \
  case Datatype::DATETIME_US:    \
  case Datatype::DATETIME_NS:    \
  case Datatype::DATETIME_PS:    \
  case Datatype::DATETIME_FS:    \
  case Datatype::DATETIME_AS:    \
  case Datatype::TIME_HR:        \
  case Datatype::TIME_MIN:       \
  case Datatype::TIME_SEC:       \
  case Datatype::TIME_MS:        \
  case Datatype::TIME_US:        \
  case Datatype::TIME_NS:        \
  case Datatype::TIME_PS:        \
  case Datatype::TIME_FS:        \
  case Datatype::TIME_AS:        \
    return tdb_new(T<int64_t>, std::forward<Args>(args)...);

#define STRING_CONVERSIONS                                    \
  case Datatype::STRING_ASCII:                                \
  case Datatype::CHAR:                                        \
  case Datatype::STRING_UTF8:                                 \
    return tdb_new(T<char>, std::forward<Args>(args)...);     \
  case Datatype::STRING_UTF16:                                \
  case Datatype::STRING_UCS2:                                 \
    return tdb_new(T<char16_t>, std::forward<Args>(args)...); \
  case Datatype::STRING_UTF32:                                \
  case Datatype::STRING_UCS4:                                 \
    return tdb_new(T<char32_t>, std::forward<Args>(args)...);

#define OTHER_CONVERSIONS                                    \
  case Datatype::BOOL:                                       \
    return tdb_new(T<uint8_t>, std::forward<Args>(args)...); \
  case Datatype::BLOB:                                       \
  case Datatype::ANY:                                        \
    return tdb_new(T<std::byte>, std::forward<Args>(args)...);

template <class SupportedTypes>
struct typed_objects {
  template <template <class> class T, typename... Args>
  static void* allocate_for_type(Datatype type, Args&&... args);
};

#define TYPED_OBJECTS(tag, cases)                                   \
  template <>                                                       \
  struct typed_objects<tag> {                                       \
    template <template <class> class T, typename... Args>           \
    static void* allocate_for_type(Datatype type, Args&&... args) { \
      switch (type) {                                               \
        cases default : throw std::runtime_error("Unknown type");   \
      }                                                             \
    }                                                               \
  };

struct dense_dims_t {};
struct all_types_t {};

TYPED_OBJECTS(dense_dims_t, INTEGER_CONVERSIONS TIME_CONVERSIONS)
TYPED_OBJECTS(
    all_types_t,
    INTEGER_CONVERSIONS FLOATING_POINT_CONVERSIONS TIME_CONVERSIONS
        STRING_CONVERSIONS OTHER_CONVERSIONS)

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TYPED_OBJECT_ALLOCATION_H
