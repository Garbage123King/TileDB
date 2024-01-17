/**
 * @file   array_meta_consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file implements the ArrayMetaConsolidator class.
 */

#include "tiledb/sm/consolidator/array_meta_consolidator.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ****************************** */
/*          CONSTRUCTOR           */
/* ****************************** */

template <class RM>
ArrayMetaConsolidator<RM>::ArrayMetaConsolidator(
    const Config& config, StorageManager* storage_manager)
    : Consolidator<RM>(storage_manager) {
  auto st = set_config(config);
  if (!st.ok()) {
    throw std::logic_error(st.message());
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

template <class RM>
Status ArrayMetaConsolidator<RM>::consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer_se =
      Consolidator<RM>::stats_->start_timer("consolidate_array_meta");

  Consolidator<RM>::check_array_uri(array_name);

  // Open array for reading
  auto array_uri = URI(array_name);
  Array array_for_reads(array_uri, Consolidator<RM>::storage_manager_);
  RETURN_NOT_OK(array_for_reads.open(
      QueryType::READ,
      config_.timestamp_start_,
      config_.timestamp_end_,
      encryption_type,
      encryption_key,
      key_length));

  // Open array for writing
  Array array_for_writes(array_uri, Consolidator<RM>::storage_manager_);
  RETURN_NOT_OK_ELSE(
      array_for_writes.open(
          QueryType::WRITE, encryption_type, encryption_key, key_length),
      throw_if_not_ok(array_for_reads.close()));

  // Swap the in-memory metadata between the two arrays.
  // After that, the array for writes will store the (consolidated by
  // the way metadata loading works) metadata of the array for reads
  Metadata* metadata_r;
  auto st = array_for_reads.metadata(&metadata_r);
  if (!st.ok()) {
    throw_if_not_ok(array_for_reads.close());
    throw_if_not_ok(array_for_writes.close());
    return st;
  }
  Metadata* metadata_w;
  st = array_for_writes.metadata(&metadata_w);
  if (!st.ok()) {
    throw_if_not_ok(array_for_reads.close());
    throw_if_not_ok(array_for_writes.close());
    return st;
  }
  metadata_r->swap(metadata_w);

  // Metadata uris to delete
  const auto to_vacuum = metadata_w->loaded_metadata_uris();

  // Get the new URI name for consolidated metadata
  URI new_uri = metadata_w->get_uri(array_uri);

  // Write vac files relative to the array URI. This was fixed for reads in
  // version 19 so only do this for arrays starting with version 19.
  size_t base_uri_size = 0;
  if (array_for_reads.array_schema_latest_ptr() == nullptr ||
      array_for_reads.array_schema_latest().write_version() >= 19) {
    base_uri_size = array_for_reads.array_uri().to_string().size();
  }

  // Close arrays
  RETURN_NOT_OK_ELSE(
      array_for_reads.close(), throw_if_not_ok(array_for_writes.close()));
  throw_if_not_ok(array_for_writes.close());

  // Write vacuum file
  URI vac_uri = URI(new_uri.to_string() + constants::vacuum_file_suffix);

  std::stringstream ss;
  for (const auto& uri : to_vacuum) {
    ss << uri.to_string().substr(base_uri_size) << "\n";
  }

  auto data = ss.str();
  RETURN_NOT_OK(Consolidator<RM>::storage_manager_->vfs()->write(
      vac_uri, data.c_str(), data.size()));
  RETURN_NOT_OK(Consolidator<RM>::storage_manager_->vfs()->close_file(vac_uri));

  return Status::Ok();
}

template <class RM>
void ArrayMetaConsolidator<RM>::vacuum(const char* array_name) {
  if (array_name == nullptr) {
    throw Status_StorageManagerError(
        "Cannot vacuum array metadata; Array name cannot be null");
  }

  // Get the array metadata URIs and vacuum file URIs to be vacuum
  auto vfs = Consolidator<RM>::storage_manager_->vfs();
  auto compute_tp = Consolidator<RM>::storage_manager_->compute_tp();

  auto array_dir = ArrayDirectory(
      Consolidator<RM>::storage_manager_->resources(),
      URI(array_name),
      0,
      std::numeric_limits<uint64_t>::max());

  // Delete the array metadata and vacuum files
  vfs->remove_files(compute_tp, array_dir.array_meta_uris_to_vacuum());
  vfs->remove_files(compute_tp, array_dir.array_meta_vac_uris_to_vacuum());
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

template <class RM>
Status ArrayMetaConsolidator<RM>::set_config(const Config& config) {
  // Set the consolidation config for ease of use
  Config merged_config = Consolidator<RM>::storage_manager_->config();
  merged_config.inherit(config);
  bool found = false;
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_start", &config_.timestamp_start_, &found));
  assert(found);
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_end", &config_.timestamp_end_, &found));
  assert(found);

  return Status::Ok();
}

template class ArrayMetaConsolidator<context_bypass_RM>;

}  // namespace tiledb::sm
