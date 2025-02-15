/**
 * @file  array_schema_evolution.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB ArraySchemaEvolution object.
 */

#ifndef TILEDB_CPP_API_ARRAY_SCHEMA_EVOLUTION_H
#define TILEDB_CPP_API_ARRAY_SCHEMA_EVOLUTION_H

#include "array_schema.h"
#include "attribute.h"
#include "context.h"
#include "current_domain.h"
#include "deleter.h"
#include "domain.h"
#include "object.h"
#include "tiledb.h"
#include "tiledb_experimental.h"

#include <array>
#include <memory>
#include <string>
#include <unordered_map>

namespace tiledb {

/**
 * Evolve the schema on a tiledb::Array.
 *
 * @details
 * See examples for more usage details.
 *
 * **Example:**
 *
 * @code{.cpp}
 * // Open the array for writing
 * tiledb::Context ctx;
 * tiledb::ArraySchemaEvolution evolution(ctx);
 * evolution.drop_attribute("a1");
 * evolution.array_evolve("my_test_array");
 * @endcode
 */
class ArraySchemaEvolution {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructs the array schema evolution with the input C array
   * array schema evolution object.
   *
   * @param ctx TileDB context
   * @param evolution C API array schema evolution object
   */
  ArraySchemaEvolution(
      const Context& context, tiledb_array_schema_evolution_t* evolution)
      : ctx_(context) {
    evolution_ =
        std::shared_ptr<tiledb_array_schema_evolution_t>(evolution, deleter_);
  }

  /**
   * Constructs an array schema evolution object.
   *
   * @param ctx TileDB context
   */
  ArraySchemaEvolution(const Context& context)
      : ctx_(context) {
    tiledb_array_schema_evolution_t* evolution;
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_array_schema_evolution_alloc(ctx.ptr().get(), &evolution));
    evolution_ =
        std::shared_ptr<tiledb_array_schema_evolution_t>(evolution, deleter_);
  }

  ArraySchemaEvolution(const ArraySchemaEvolution&) = default;
  ArraySchemaEvolution(ArraySchemaEvolution&&) = default;
  ArraySchemaEvolution& operator=(const ArraySchemaEvolution&) = default;
  ArraySchemaEvolution& operator=(ArraySchemaEvolution&&) = default;
  virtual ~ArraySchemaEvolution() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Adds an Attribute to the array schema evolution.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::ArraySchemaEvolution schema_evolution(ctx);
   * schema_evolution.add_attribute(Attribute::create<int32_t>(ctx,
   * "attr_name"));
   * @endcode
   *
   * @param attr The Attribute to add
   * @return Reference to this `ArraySchemaEvolution` instance.
   */
  ArraySchemaEvolution& add_attribute(const Attribute& attr) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_evolution_add_attribute(
        ctx.ptr().get(), evolution_.get(), attr.ptr().get()));
    return *this;
  }

  /**
   * Drops an attribute.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::ArraySchemaEvolution schema_evolution(ctx);
   * schema_evolution.drop_attribute("attr_name");
   * @endcode
   *
   * @param attr The attribute to be dropped
   * @return Reference to this `ArraySchemaEvolution` instance.
   */
  ArraySchemaEvolution& drop_attribute(const std::string& attribute_name) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_evolution_drop_attribute(
        ctx.ptr().get(), evolution_.get(), attribute_name.c_str()));
    return *this;
  }

  /**
   * Adds an Enumeration to the array schema evolution.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::ArraySchemaEvolution schema_evolution(ctx);
   * std::vector<std::string> values = {"red", "green", "blue"};
   * schema_evolution.add_enumeration(Enumeration::create(ctx, "an_enumeration",
   * values));
   * @endcode
   *
   * @param enmr The Enumeration to add.
   * @return Reference to this `ArraySchemaEvolution` instance.
   */
  ArraySchemaEvolution& add_enumeration(const Enumeration& enmr) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_evolution_add_enumeration(
        ctx.ptr().get(), evolution_.get(), enmr.ptr().get()));
    return *this;
  }

  /**
   * Extends an Enumeration during array schema evolution.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Enumeration old_enmr = array->get_enumeration("some_enumeration");
   * std::vector<std::string> new_values = {"cyan", "magenta", "mauve"};
   * tiledb::Enumeration new_enmr = old_enmr->extend(new_values);
   * tiledb::ArraySchemaEvolution schema_evolution(ctx);
   * schema_evolution.extend_enumeration(new_enmr);
   * @endcode
   *
   * @param enmr The Enumeration to extend.
   * @return Reference to this `ArraySchemaEvolution` instance.
   */
  ArraySchemaEvolution& extend_enumeration(const Enumeration& enmr) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_evolution_extend_enumeration(
        ctx.ptr().get(), evolution_.get(), enmr.ptr().get()));
    return *this;
  }

  /**
   * Drops an enumeration.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::ArraySchemaEvolution schema_evolution(ctx);
   * schema_evolution.drop_enumeration("enumeration_name");
   * @endcode
   *
   * @param enumeration_name The enumeration to be dropped
   * @return Reference to this `ArraySchemaEvolution` instance.
   */
  ArraySchemaEvolution& drop_enumeration(const std::string& enumeration_name) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_evolution_drop_enumeration(
        ctx.ptr().get(), evolution_.get(), enumeration_name.c_str()));
    return *this;
  }

  /**
   * Expands the current domain during array schema evolution.
   * TileDB will enforce that the new current domain is expanding
   * on the current one and not contracting during `tiledb_array_evolve`.
   *
   * @param expanded_domain The current domain we want to expand the schema to.
   */
  ArraySchemaEvolution& expand_current_domain(
      const CurrentDomain& expanded_domain) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_evolution_expand_current_domain(
        ctx.ptr().get(), evolution_.get(), expanded_domain.ptr().get()));
    return *this;
  }

  /**
   * Sets timestamp range.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::ArraySchemaEvolution schema_evolution(ctx);
   * uint64_t now = tiledb_timestamp_now_ms()
   * schema_evolution.set_timestamp_range({now, now});
   * @endcode
   *
   * @param timestamp_range The timestamp range to be set
   */
  void set_timestamp_range(
      const std::pair<uint64_t, uint64_t>& timestamp_range) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_evolution_set_timestamp_range(
        ctx.ptr().get(),
        evolution_.get(),
        std::get<0>(timestamp_range),
        std::get<1>(timestamp_range)));
  }

  /**
   * Evolves the schema of an array.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::ArraySchemaEvolution schema_evolution(ctx);
   * schema_evolution.drop_attribute("attr_name");
   * schema_evolution.array_evolve("test_array_uri");
   * @endcode
   *
   * @param array_uri The uri of an array
   * @return Reference to this `ArraySchemaEvolution` instance.
   */
  ArraySchemaEvolution& array_evolve(const std::string& array_uri) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_evolve(
        ctx.ptr().get(), array_uri.c_str(), evolution_.get()));
    return *this;
  }

  /** Returns a shared pointer to the C TileDB array schema evolution object. */
  std::shared_ptr<tiledb_array_schema_evolution_t> ptr() const {
    return evolution_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** An auxiliary deleter. */
  impl::Deleter deleter_;

  /** The pointer to the C TileDB array schema evolution object. */
  std::shared_ptr<tiledb_array_schema_evolution_t> evolution_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ARRAY_SCHEMA_EVOLUTION_H
