/**
 * @file unit_resumable_nodes.cc
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
 */

#include "unit_resumable_nodes.h"
#include <future>
#include <type_traits>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/duffs.h"
#include "experimental/tiledb/common/dag/nodes/generators.h"
#include "experimental/tiledb/common/dag/nodes/resumable_nodes.h"
#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

#include "experimental/tiledb/common/dag/nodes/detail/resumable/mimo.h"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/reduce.h"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/broadcast.h"

using namespace tiledb::common;

using S = tiledb::common::DuffsScheduler<node>;
using R2_1_1 =
    mimo_node<DuffsMover2, std::tuple<size_t>, DuffsMover2, std::tuple<size_t>>;
using R2_3_1 = mimo_node<
    DuffsMover2,
    std::tuple<size_t, int, double>,
    DuffsMover2,
    std::tuple<size_t>>;
using R2_1_3 = mimo_node<
    DuffsMover2,
    std::tuple<size_t>,
    DuffsMover2,
    std::tuple<size_t, double, int>>;
using R2_3_3 = mimo_node<
    DuffsMover2,
    std::tuple<size_t, int, double>,
    DuffsMover2,
    std::tuple<size_t, double, int>>;

using R3_1_1 =
    mimo_node<DuffsMover3, std::tuple<size_t>, DuffsMover3, std::tuple<size_t>>;
using R3_3_1 = mimo_node<
    DuffsMover3,
    std::tuple<size_t, int, double>,
    DuffsMover3,
    std::tuple<size_t>>;
using R3_1_3 = mimo_node<
    DuffsMover3,
    std::tuple<size_t>,
    DuffsMover3,
    std::tuple<size_t, double, int>>;
using R3_3_3 = mimo_node<
    DuffsMover3,
    std::tuple<size_t, int, double>,
    DuffsMover3,
    std::tuple<size_t, double, int>>;

using reduce_3_1 = reducer_node<
    DuffsMover3,
    std::tuple<size_t, size_t, size_t>,
    DuffsMover3,
    std::tuple<size_t>>;
using reduce_3_3 = reducer_node<
    DuffsMover3,
    std::tuple<size_t, size_t, size_t>,
    DuffsMover3,
    std::tuple<size_t, size_t, size_t>>;
using broadcast_1_3 = broadcast_node<3,
    DuffsMover3,
    std::tuple<size_t>,
    DuffsMover3,
    std::tuple<size_t>>;


namespace tiledb::common {
// Tentative deduction guide
template <class... R, class... Args>
mimo_node(std::function<std::tuple<R...>(const std::tuple<Args...>&)>&&)
    -> mimo_node<
        DuffsMover3,
        std::tuple<std::remove_cv_t<std::remove_reference_t<Args>>...>,
        DuffsMover3,
        std::tuple<R>...>;
}  // namespace tiledb::common

TEST_CASE("ResumableNode: Verify Construction", "[resumable_node]") {
  SECTION("Test Construction") {
    R2_1_1 b2_1_1{[](std::tuple<size_t>) { return std::make_tuple(0UL); }};
    R2_1_3 b2_1_3{
        [](std::tuple<size_t>) { return std::make_tuple(0UL, 0.0, 0); }};
    R2_3_1 b2_3_1{
        [](std::tuple<size_t, int, double>) { return std::make_tuple(0UL); }};
    R2_3_3 b2_3_3{[](std::tuple<size_t, int, double>) {
      return std::make_tuple(0UL, 0.0, 0);
    }};

    R3_1_1 b3_1_1{[](std::tuple<size_t>) { return std::make_tuple(0UL); }};
    R3_1_3 b3_1_3{
        [](std::tuple<size_t>) { return std::make_tuple(0UL, 0.0, 0); }};
    R3_3_1 b3_3_1{
        [](std::tuple<size_t, int, double>) { return std::make_tuple(0UL); }};
    R3_3_3 b3_3_3{[](std::tuple<size_t, int, double>) {
      return std::make_tuple(0UL, 0.0, 0);
    }};

    // Deduction guide not working
    // mimo_node c_1_1 { [](std::tuple<size_t>) { return std::make_tuple(0UL); }
    // };
  }
}

TEST_CASE("Resumable Node: Construct reduce node", "[reducer_node]") {
  SECTION("Test Construction") {
    reduce_3_1 b3_3_1{[](const std::tuple<size_t, size_t, size_t>& a) {
      return std::make_tuple(std::get<0>(a) + std::get<1>(a) + std::get<2>(a));
    }};
    CHECK(b3_3_1->num_inputs() == 3);
    CHECK(b3_3_1->num_outputs() == 1);

    // Error: reduction goes M -> 1
    // reduce_3_3 b3_3_3{[](const std::tuple<size_t, size_t, size_t>& a) {
    //   return std::make_tuple(std::get<0>(a), std::get<1>(a), std::get<2>(a));
    // }};
  }
}

TEST_CASE("Resumable Node: Construct broadcast node", "[broadcast_node]") {
  SECTION("Test Construction") {
    broadcast_1_3 b3_1_3{[](const std::tuple<size_t>& a) {
      return std::make_tuple(5*std::get<0>(a));
    }};
    CHECK(b3_1_3->num_inputs() == 1);
    CHECK(b3_1_3->num_outputs() == 3);
  }
}

//make_proxy<0>(v)
TEST_CASE("Resumable Node: Connect broadcast to reduce", "[broadcast_node]") {
  SECTION("Test Connection") {
    broadcast_1_3 b3_1_3{[](const std::tuple<size_t>& a) {
      return std::make_tuple(5*std::get<0>(a));
    }};
    CHECK(b3_1_3->num_inputs() == 1);
    CHECK(b3_1_3->num_outputs() == 3);

    reduce_3_1 b3_3_1{[](const std::tuple<size_t, size_t, size_t>& a) {
      return std::make_tuple(std::get<0>(a) + std::get<1>(a) + std::get<2>(a));
    }};
    CHECK(b3_1_3->num_inputs() == 1);
    CHECK(b3_1_3->num_outputs() == 3);

    Edge e1{make_proxy<0>(b3_1_3), make_proxy<0>(b3_3_1)};
  }
}

#if 0

using S = tiledb::common::DuffsScheduler<node>;
using C2 = consumer_node<DuffsMover2, std::tuple<size_t, size_t, size_t>>;
using F2 = function_node<DuffsMover2, size_t>;
using T2 = tuple_maker_node<
    DuffsMover2,
    size_t,
    DuffsMover2,
    std::tuple<size_t, size_t, size_t>>;
using P2 = producer_node<DuffsMover2, size_t>;

using C3 = consumer_node<DuffsMover3, std::tuple<size_t, size_t, size_t>>;
using F3 = function_node<DuffsMover3, size_t>;
using T3 = tuple_maker_node<
    DuffsMover3,
    size_t,
    DuffsMover3,
    std::tuple<size_t, size_t, size_t>>;
using P3 = producer_node<DuffsMover3, size_t>;

/**
 * Verify various API approaches
 */
TEMPLATE_TEST_CASE(
    "ResumableNodes: Verify various API approaches",
    "[resumable_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  SECTION("Test Construction") {
    P a;
    P b([](std::stop_source&) { return 0; });
    C c([](size_t) {});
  }
  SECTION("Test Connection") {
    P b([](std::stop_source&) { return 0; });
    C c([](size_t) {});
    Edge g{*b, *c};
  }
  SECTION("Enable if fail") {
    // These will fail, with good diagnostics.  This should be commented out
    // from time to time and tested by hand that we get the right error
    // messages.
    //   producer_node<AsyncMover3, size_t> bb{0UL};
    //   consumer_node<AsyncMover3, size_t> cc{-1.1};
    //   Edge g{bb, cc};
  }
}

bool two_nodes(node_base&, node_base&) {
  return true;
}

bool two_nodes(const node&, const node&) {
  return true;
}

TEMPLATE_TEST_CASE(
    "Tasks: Extensive tests of tasks with nodes",
    "[tasks]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        function_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        function_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  using CI = typename C::Base::element_type;
  using FI = typename F::Base::element_type;
  using PI = typename P::Base::element_type;

  auto pro_node_impl = PI([](std::stop_source&) { return 0; });
  auto fun_node_impl = FI([](const size_t& i) { return i; });
  auto con_node_impl = CI([](const size_t&) {});

  auto pro_node = P([](std::stop_source&) { return 0; });
  auto fun_node = F([](const size_t& i) { return i; });
  auto con_node = C([](const size_t&) {});

  SECTION("Check specified and deduced are same types") {
  }

  SECTION("Check polymorphism to node&") {
    CHECK(two_nodes(pro_node_impl, con_node_impl));
    CHECK(two_nodes(pro_node_impl, fun_node_impl));
    CHECK(two_nodes(fun_node_impl, con_node_impl));

    // No conversion from producer_node to node
    CHECK(two_nodes(pro_node, con_node));
    CHECK(two_nodes(pro_node, fun_node));
    CHECK(two_nodes(fun_node, con_node));
  }

  SECTION("Check some copying node (godbolt)") {
    auto shared_pro = node{pro_node};
    auto shared_fun = node{fun_node};
    auto shared_con = node{con_node};

    auto shared_nil = node{};
    shared_nil = shared_pro;
    CHECK(shared_nil == shared_pro);
  }
}

/**
 * Some dummy functions and classes to test node constructors
 * with.
 */
size_t dummy_source(std::stop_source&) {
  return size_t{};
}

size_t dummy_function(size_t) {
  return size_t{};
}

void dummy_sink(size_t) {
}

size_t dummy_const_function(const size_t&) {
  return size_t{};
}

void dummy_const_sink(const size_t&) {
}

class dummy_source_class {
 public:
  size_t operator()(std::stop_source&) {
    return size_t{};
  }
};

class dummy_function_class {
 public:
  size_t operator()(const size_t&) {
    return size_t{};
  }
};

class dummy_sink_class {
 public:
  void operator()(size_t) {
  }
};

size_t dummy_bind_source(double) {
  return size_t{};
}

size_t dummy_bind_function(double, float, size_t) {
  return size_t{};
}

void dummy_bind_sink(size_t, float, const int&) {
}

/**
 * Some dummy function template and class templates to test node constructors
 * with.
 */
template <class Block = size_t>
size_t dummy_source_t(std::stop_source&) {
  return Block{};
}

template <class InBlock = size_t, class OutBlock = InBlock>
OutBlock dummy_function_t(InBlock) {
  return OutBlock{};
}

template <class Block = size_t>
void dummy_sink_t(const Block&) {
}

template <class Block = size_t>
class dummy_source_class_t {
 public:
  Block operator()() {
    return Block{};
  }
};

template <class InBlock = size_t, class OutBlock = InBlock>
class dummy_function_class_t {
 public:
  OutBlock operator()(const InBlock&) {
    return OutBlock{};
  }
};

template <class Block = size_t>
class dummy_sink_class_t {
 public:
  void operator()(Block) {
  }
};

template <class Block = size_t>
Block dummy_bind_source_t(double) {
  return Block{};
}

template <class InBlock = size_t, class OutBlock = InBlock>
OutBlock dummy_bind_function_t(double, float, InBlock) {
  return OutBlock{};
}

template <class Block = size_t>
void dummy_bind_sink_t(Block, float, const int&) {
}

/**
 * Verify initializing producer_node and consumer_node with function, lambda,
 * in-line lambda, function object, bind, and rvalue bind.
 */
TEMPLATE_TEST_CASE(
    "ResumableNodes: Verify numerous API approaches, with edges",
    "[resumable_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  SECTION("function") {
    P b{dummy_source};
    C c{dummy_sink};
    Edge g{*b, *c};
  }

  SECTION("function with const reference") {
    P b{dummy_source};
    C c{dummy_const_sink};
    Edge g{*b, *c};
  }

  SECTION("function, no star") {
    P b{dummy_source};
    C c{dummy_sink};
    Edge g{b, c};
  }

  SECTION("function with const reference, no star") {
    P b{dummy_source};
    C c{dummy_const_sink};
    Edge g{b, c};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    P b{dummy_source_lambda};
    C c{dummy_sink_lambda};

    Edge g{*b, *c};
  }

  SECTION("inline lambda") {
    P b([](std::stop_source&) { return 0; });
    C c([](size_t) {});

    Edge g{*b, *c};
  }

  SECTION("function object") {
    auto a = dummy_source_class{};
    dummy_sink_class d{};

    P b{a};
    C c{d};

    Edge g{*b, *c};
  }

  SECTION("inline function object") {
    P b{dummy_source_class{}};
    C c{dummy_sink_class{}};

    Edge g{*b, *c};
  }

  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, x);
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);

    P b{a};
    C c{d};

    Edge g{*b, *c};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    P b{std::bind(dummy_bind_source, x)};
    C c{std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge g{*b, *c};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, std::move(x));
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, std::move(z));

    P b{std::move(a)};
    C c{std::move(d)};

    Edge g{*b, *c};
  }
}

/**
 * Verify initializing producer_node, function_node, and consumer_node with
 * function, lambda, in-line lambda, function object, bind, and rvalue bind.
 * (This is a repeat of the previous test, but modified to include a
 * function_node.)
 */
TEMPLATE_TEST_CASE(
    "ResumableNodes: Verify various API approaches, including function_node",
    "[resumable_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        function_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        function_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  SECTION("function") {
    P a{dummy_source};
    F b{dummy_function};
    C c{dummy_sink};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    auto dummy_function_lambda = [](size_t) { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    P a{dummy_source_lambda};
    F b{dummy_function_lambda};
    C c{dummy_sink_lambda};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("inline lambda") {
    P a([](std::stop_source&) { return 0UL; });
    F b([](size_t) { return 0UL; });
    C c([](size_t) {});

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("function object") {
    auto ac = dummy_source_class{};
    dummy_function_class fc{};
    dummy_sink_class dc{};

    P a{ac};
    F b{fc};
    C c{dc};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("inline function object") {
    P a{dummy_source_class{}};
    F b{dummy_function_class{}};
    C c{dummy_sink_class{}};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }
  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source, x);
    auto dc = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);
    auto fc = std::bind(dummy_bind_function, x, y, std::placeholders::_1);

    P a{ac};
    F b{fc};
    C c{dc};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    P a{std::bind(dummy_bind_source, x)};
    F b{std::bind(dummy_bind_function, x, y, std::placeholders::_1)};
    C c{std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge i{*a, *b};
    Edge j{*b, *c};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source, std::move(x));
    auto dc = std::bind(
        dummy_bind_sink, std::move(y), std::placeholders::_1, std::move(z));
    auto fc = std::bind(
        dummy_bind_function, std::move(x), std::move(y), std::placeholders::_1);

    P a{std::move(ac)};
    F b{std::move(fc)};
    C c{std::move(dc)};

    Edge i{*a, *b};
    Edge j{*b, *c};
  }
}
#endif