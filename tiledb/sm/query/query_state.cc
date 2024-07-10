/**
 * @file query_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines a state machine for processing local queries.
 */

#include "query_state.h"

#include "tiledb/common/exception/exception.h"

namespace tiledb::sm {

class QueryStateException : public tiledb::common::StatusException {
 public:
  QueryStateException(std::string_view s)
      : StatusException("QueryState", std::string(s)) {
  }
};

LocalQueryStateMachine::LocalQueryStateMachine(LocalQueryState s)
    : state_(s) {
  if (!is_initial()) {
    throw QueryStateException("Argument is not an initial state");
  }
}

/**
 * A row in the transition table is a state indexed by events.
 */
using transition_table_row = LocalQueryState[n_local_query_events];
/*
 * A transition table is one row for each state.
 */
using transition_table = transition_table_row[n_local_query_states];

using enum LocalQueryState;

/**
 * Transition table for `LocalQueryStateMachine`
 */
constexpr transition_table local_query_tt{
    // uninitialized
    {
        everything_else,  // ready
        success,    // finish. In due course this should be `error`, since it
                    // should be impossible to complete a query without
                    // initializing it.
        aborted,    // abort,
        cancelled,  // cancel
    },
    // everything_else
    {
        everything_else,  // ready
        success,          // finish
        aborted,          // abort,
        cancelled,        // cancel
    },
    // success
    {
        success,  // ready
        success,  // finish. Arguably this might be `error`, since it's already
                  // finished once already.
        error,    // abort. There should be no occasion where a successful query
                  // aborts after completion.
        success,  // cancel. Cancelling a successful query has no effect.
                  // There's no longer any pending activity to cancel.
    },
    // aborted
    {
        aborted,  // ready
        error,    // finish. It's an error to try to complete an aborted query.
        aborted,  // abort. Self-transition is intentional
        aborted,  // cancel. Cancelling an aborted query has no effect. There's
                  // no longer any pending activity to cancel.
    },
    // cancelled
    {
        cancelled,  // ready
        error,      // finish. You can't complete a cancelled query.
        error,      // abort. A cancelled query shouldn't be doing anything that
                    // would give rise to an `abort`.
        cancelled,  // cancel
    },
    // error
    {
        error,  // ready
        error,  // finish
        error,  // abort,
        error,  // cancel
    },
};

/**
 * @section Implementation Maturity
 *
 * This state machine at present is quite simple. All it does is to process the
 * state transition. It does not have functions associated with events, nor with
 * entering or leaving states. Such functions must be able to throw. The query
 * processing code is not known to work correctly with exceptions in all cases,
 * so such functions are currently not used.
 */
void LocalQueryStateMachine::event(LocalQueryEvent e) {
  std::lock_guard<std::mutex> lg{m_};
  state_ = local_query_tt[index_of(state_)][index_of(e)];
}

LocalQueryState LocalQueryStateMachine::state() const {
  std::lock_guard<std::mutex> lg{m_};
  return state_;
}

}  // namespace tiledb::sm
