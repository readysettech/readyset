// Simple example test of building a stateful proptest for a counter API
use std::time::Duration;

use async_trait::async_trait;
use proptest::prelude::*;
use proptest_stateful::{ModelState, ProptestStatefulConfig};

// First, we have our example counter struct and associated API:

struct Counter {
    count: usize,
}

impl Counter {
    fn new(count: usize) -> Self {
        Counter { count }
    }

    fn inc(&mut self) {
        self.count += 1;
    }

    fn dec(&mut self) {
        self.count -= 1;
    }
}

// Everything after this is test code for the above

#[derive(Clone, Debug)]
enum CounterOp {
    Inc,
    Dec,
}

#[derive(Clone, Debug)]
struct TestState {
    model_count: usize,
}

struct TestContext {
    counter: Counter,
}

impl Default for TestState {
    fn default() -> Self {
        TestState { model_count: 3 } // Set to match initial test value
    }
}

#[async_trait(?Send)]
impl ModelState for TestState {
    type Operation = CounterOp;
    type RunContext = TestContext;
    type OperationStrategy = BoxedStrategy<Self::Operation>;

    fn op_generators(&self) -> Vec<Self::OperationStrategy> {
        let mut ops = vec![Just(CounterOp::Inc).boxed()];
        // There's a known issue with `Counter`, which is that it will panic and underflow if you
        // decrement below 0, so don't generate a decrement operation if we expect the counter
        // state to be 0:
        if self.model_count > 0 {
            ops.push(Just(CounterOp::Dec).boxed());
        }
        ops
    }

    fn preconditions_met(&self, op: &Self::Operation) -> bool {
        match op {
            CounterOp::Inc => true,
            // This may seem redundant with the logic in `op_generators`, but including this next
            // line in `preconditions_met` is essential to avoid issues when shrinking test cases
            // that have already been generated. Without it, we may accidentally create a test case
            // via shrinking that crashes due to underflow, when the original error that triggered
            // the failure may have been something completely different, and that could result in
            // us returning a minimal failing case that doesn't actually reproduce whatever bug the
            // test found.
            CounterOp::Dec => self.model_count > 0,
        }
    }

    fn next_state(&mut self, op: &Self::Operation) {
        match op {
            CounterOp::Inc => {
                self.model_count += 1;
            }
            CounterOp::Dec => {
                self.model_count -= 1;
            }
        }
    }

    async fn init_test_run(&self) -> Self::RunContext {
        let counter = Counter::new(3); // Start with 3 to make the failing cases more interesting
        TestContext { counter }
    }

    async fn run_op(&self, op: &Self::Operation, ctxt: &mut Self::RunContext) {
        match op {
            CounterOp::Inc => ctxt.counter.inc(),
            CounterOp::Dec => ctxt.counter.dec(),
        }
    }

    // If we wanted, we could implement `check_postconditions` to assert that the counter state
    // matches the model state, but it's not necessary for this simplified example, since we're
    // just concerned with whether the code runs without panicking.
    async fn check_postconditions(&self, _ctxt: &mut Self::RunContext) {}
    async fn clean_up_test_run(&self, _ctxt: &mut Self::RunContext) {}
}

#[test]
fn run_cases() {
    let config = ProptestStatefulConfig {
        min_ops: 10,
        max_ops: 20,
        test_case_timeout: Duration::from_secs(60),
        proptest_config: ProptestConfig::default(),
    };

    proptest_stateful::test::<TestState>(config);
}
