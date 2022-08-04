/**
 * @file   lidar_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file implements the lidar compressor class.
 */

#include "tiledb/sm/compressors/lidar_compressor.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/compressors/bzip_compressor.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/filter/xor_filter.h"

#include <cmath>
#include <vector>
#include <utility>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

template<typename W>
struct LidarSortCmp {
  bool operator()(const std::pair<W, size_t>& a, const std::pair<W, size_t>& b) const {
    return a.first < b.first;
  }
};

template<typename W>
Status Lidar::compress(
    Datatype type, int level, ConstBuffer* input_buffer, Buffer* output_buffer) {
  assert(sizeof(W) == 4 || sizeof(W) == 8);
  assert((input_buffer->size() % sizeof(W)) == 0 && input_buffer->size() > 0);
  std::vector<std::pair<W, size_t>> vals;
  for (size_t i = 0; i < input_buffer->size()/sizeof(W); ++i) {
    W val = input_buffer->value<W>(i * sizeof(W));
    vals.push_back(std::make_pair(val, i));
  }
  // Sort values
  parallel_sort(compute_tp_, vals.begin(), vals.end(), LidarSortCmp<W>());

  W first_val = vals[0].first;
  std::vector<W> num_vals;
  std::vector<size_t> positions;
  for (const auto &elem : vals) {
    num_vals.push_back(elem.first);
    positions.push_back(elem.second);
  }

  // Apply XOR filter.
  // convert std::vector to filter buffer?? 
  // examples of using xor filter in buffer
  Datatype int_type = type == Datatype::FLOAT32 ? Datatype::INT32 : Datatype::INT64;
  Tile tile;
    tile.init_unfiltered(
        constants::format_version, int_type, 0, 1, 0);
  FilterBuffer input;
  input.init(num_vals.data(), num_vals.size() * sizeof(W));
  FilterBuffer output;
  FilterBuffer input_metadata;
  FilterBuffer output_metadata;
  xor_filter_.run_forward(tile, nullptr, &input, &input_metadata, &output, &output_metadata);

  assert(output.num_buffers() == 1);
  Buffer bzip_output;
  RETURN_NOT_OK(BZip::compress(level, &output.buffers()[0], &bzip_output));

  // Write output to output buffer.
  RETURN_NOT_OK(output_buffer->write(&first_val, sizeof(W)));
  output_buffer->advance_offset(sizeof(W));
  RETURN_NOT_OK(output_buffer->write(positions.data(), positions.size() * sizeof(size_t)));
  output_buffer->advance_offset(positions.size() * sizeof(size_t));
  RETURN_NOT_OK(output_buffer->write(&bzip_output));
  return Status::Ok();
}

Status Lidar::compress(
    Datatype type, int level, ConstBuffer* input_buffer, Buffer* output_buffer) {
  switch (type) {
    case Datatype::FLOAT32: {
      return compress<int32_t>(type, level, input_buffer, output_buffer);
    }
    case Datatype::FLOAT64: {
      return compress<int64_t>(type, level, input_buffer, output_buffer);
    }
    default: {
      return Status_CompressionError("Lidar::compress: attribute type is not a floating point type.");
    }
  }
}

Status Lidar::compress(Datatype type, ConstBuffer* input_buffer, Buffer* output_buffer) {
    return compress(type, default_level_, input_buffer, output_buffer);
}

template<typename T, typename W>
Status Lidar::decompress(ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  (void)input_buffer;
  (void)output_buffer;
  return Status::Ok();
}

Status Lidar::decompress(
    Datatype type, ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  switch (type) {
    case Datatype::FLOAT32: {
      return decompress<float, int32_t>(input_buffer, output_buffer);
    }
    case Datatype::FLOAT64: {
      return decompress<double, int64_t>(input_buffer, output_buffer);
    }
    default: {
      return Status_CompressionError("Lidar::compress: attribute type is not a floating point type.");
    }
  }
}

uint64_t Lidar::overhead(uint64_t nbytes) {
  return GZip::overhead(nbytes);
}

}  // namespace sm
}  // namespace tiledb
