//! This framework builds on the [`proptest`] framework to make it easier to build stateful
//! property-based tests. By "stateful", we mean that the intent is to be able to test stateful
//! systems, which we do by modeling the state of the system under test and using that model to
//! assist in generating test cases and defining the expected results.
//!
//! The code in this framework generates test cases as sequences of operations, and shrinks test
//! cases by removing operations from the sequence. Individual operation themselves are not
//! currently shrunk. Adding the ability to shrink individual operations within a sequence is
//! non-trivial, since it tends to break preconditions in a way that is difficult to compensate
//! for. There may be ways around this, but in my personal experience, shrinking via just removing
//! operations has been sufficient to yield simple and useful minimal failing cases, so shrinking
//! individual ops is not a priority right now.
//!
//! There is currently no way to symbolically model the outputs of operations when generating test
//! cases, so the inputs to each operation must be able to have concrete values generated at test
//! case generation time. At some point, we may try to add the feature support necessary to remove
//! this limitation, but for now the current framework code has been sufficient for our needs.
use std::cell::Cell;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use async_trait::async_trait;
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;
use rand::distributions::{Distribution, Uniform};

/// Used by the caller of [`test`] for providing config options.
pub struct ProptestStatefulConfig {
    /// Minimum number of operations to generate for any given test case.
    pub min_ops: usize,
    /// Maximum number of operations to generate for any given test case.
    pub max_ops: usize,
    /// Amount of time to wait before timing out and failing a test case.
    pub test_case_timeout: Duration,
    /// Can be used to override default proptest configuration parameters.
    ///
    /// Note, however, that we ignore the provided value of [`ProptestConfig::max_shrink_iters`],
    /// and always set it to `max_ops * 2`, because the default value is not high enough to make
    /// sure we won't fail out of shrinking before we've had a chance to try removing and adding
    /// back in each individual op.
    pub proptest_config: ProptestConfig,
}

/// A trait for defining stateful property tests.
///
/// The [`ModelState`] trait includes callbacks that enable generating, shrinking, and running test
/// cases. Note that the same value is intended to be used both for planning test cases and
/// executing them; the [`ModelState::next_state`] callback is used during all three of test case
/// generation, execution, and shrinking, since all three of those processes require modeling the
/// state of the system under test in order to determine whether a given sequence of operations is
/// valid (in generation and shrinking) and to check postconditions (during test execution).
///
/// Note that the trait as it is currently implemented uses async callbacks. We may want to make a
/// non-async version of this at some point, but all the tests we've written so far are async, so
/// I'm leaving this as a TODO item.
#[async_trait(?Send)]
pub trait ModelState: Clone + Debug + Default {
    /// A type used to specify a single operation in the sequence of operations that constitute a
    /// test case.
    type Operation: Clone + Debug;
    /// A type used to hold on to any state that is only needed at runtime for executing cases (as
    /// opposed to the state held directly in an implementor of [`ModelState`], which is used for
    /// generation and shrinking).
    type RunContext;
    /// The type for proptest strategies used to generate [`Self::Operation`] values.
    ///
    /// Note that this type should no longer need to be explicitly named in this way once
    /// return_position_impl_trait_in_trait is stable.
    type OperationStrategy: Strategy<Value = Self::Operation>;

    // TODO once return_position_impl_trait_in_trait is stabilized, uncomment this and get rid of
    // the OperationStrategy type:
    // fn op_generators(&self) -> Vec<impl Strategy<Value = Self::Operation>>;
    /// Returns a [`Vec`] of [`Strategy`]s for generating [`Self::Operation`] values.
    ///
    /// The proptest-stateful code will pseudo-randomly pick a strategy based off of the random
    /// seed from the proptest [`TestRunner`], and generate a single [`Self::Operation`] value from
    /// the selected [`Strategy`].
    fn op_generators(&self) -> Vec<Self::OperationStrategy>;
    /// Determines whether preconditions have been met for an operation, based on the current model
    /// state.
    fn preconditions_met(&self, op: &Self::Operation) -> bool;

    /// Updates the internal model state based on the modeled result of executing of `op`.
    fn next_state(&mut self, op: &Self::Operation);

    /// Called once at the beginning of each test case's execution.
    ///
    /// After performing any necessary initialization work, the implementation returns a value of
    /// type [`Self::RunContext`] which is subsequently passed back into calls to [`run_op`],
    /// [`check_postconditions`], and [`clean_up_test_run`].
    async fn init_test_run(&self) -> Self::RunContext;
    /// Called to execute `op` during a test run.
    async fn run_op(&self, op: &Self::Operation, ctxt: &mut Self::RunContext);
    /// Called after each execution of [`run_op`] to check any postconditions that are expected to
    /// be upheld during the test run.
    ///
    /// Since the return type is `()`, postcondition failures must trigger a panic.
    async fn check_postconditions(&self, ctxt: &mut Self::RunContext);
    /// Called at the end of each test run to allow any necessary cleanup work to be done.
    async fn clean_up_test_run(&self, ctxt: &mut Self::RunContext);
}

/// This is used to store and shrink a list of operation values by implementing the [`ValueTree`]
/// trait.
///
/// Note that shrinking is entirely based around removing elements from the sequence of operations;
/// the individual operations themselves are not shrunk, as doing so would likely cause
/// preconditions to be violated later in the sequence of operations (unless we re-generated the
/// rest of the sequence, but this would likely result in a test case that no longer fails).
///
/// In practice, this is not a terrible limitation, as having a minimal sequence of operations
/// tends to make it relatively easy to understand the cause of a failing case, even if the inputs
/// to those operations are not necessarily minimal themselves.
struct TestTree<T, O>
where
    O: Clone + Debug,
    T: ModelState<Operation = O>,
{
    /// List of all operations initially included in this test case
    ops: Vec<O>,
    /// List of which operations are still included vs. which have been shrunk out
    currently_included: Vec<bool>,
    /// The last operation we removed via shrinking (which may or may not end up needing to be
    /// added back in, depending on whether the test continues to fail with that operation removed)
    last_shrank_idx: usize,
    /// The index of the next step we're planning to run. If the test fails, the last successful
    /// step will thus be `next_step_idx - 1`.
    next_step_idx: Rc<Cell<usize>>,
    /// Whether we've already used next_step_idx to trim out steps after the failure.
    have_trimmed_after_failure: bool,
    /// Used to suppress "unused type parameter" warnings.
    ///
    /// Even though we don't actually have any fields in this struct that implement [`ModelState`],
    /// we need the type parameter so that we can create the correct type of temporary state value
    /// when we perform shrinking in the [`ValueTree`] impl.
    state_type: PhantomData<T>,
}

impl<T, O> ValueTree for TestTree<T, O>
where
    O: Clone + Debug,
    T: ModelState<Operation = O>,
{
    type Value = Vec<O>;

    /// Returns a [`Vec`] of all values for this test case that have not been shrunk out.
    fn current(&self) -> Self::Value {
        self.ops
            .iter()
            .zip(self.currently_included.iter())
            .filter_map(|(v, include)| include.then_some(v))
            .cloned()
            .collect()
    }

    /// Attempts to remove one operation for shrinking purposes, starting at the end of the list
    /// and working backward for each invocation of this method. Operations are only removed if it
    /// is possible to do so without violating a precondition.
    ///
    /// There are some limitations in the current approach that I'd like to address at some point:
    ///  * It is inefficient to only remove one element at a time.
    ///    * It is likely that we can randomly try removing multiple elements at once, since most
    ///      minimal failing cases are much smaller than the original failing cases that produced
    ///      them. The math around this is a bit tricky though, as the ideal number of elements to
    ///      try removing at any given time depends on how big we expect a typical minimal failing
    ///      test case to be.
    ///  * Removing only one element at a time may prevent us from fully shrinking down to the
    ///    minimal failing case
    ///    * For example, if we create a table A, then drop A, then create A again with different
    ///      columns, then the third step may be the only one necessary to reproduce the failure,
    ///      but we will not be able to remove the first two steps. Removing the first step alone
    ///      would violate a precondition for the second step (since the second stop is to drop A),
    ///      and removing the second step alone would violate a precondition for the third step
    ///      (since we cannot create A again unless we've dropped the previous table named A). In
    ///      order to shrink this down completely, we must remove both of the first two steps in one
    ///      step, which the current shrinking algorithm cannot do.
    ///
    /// However, removing only one element at a time is much simpler from an implementation
    /// standpoint, so that is all that I've implemented for now. Designing and implementing a
    /// better shrinking algorithm could definitely be useful in other tests in the future, though.
    fn simplify(&mut self) -> bool {
        if !self.have_trimmed_after_failure {
            self.have_trimmed_after_failure = true; // Only run this block once per test case

            let next_step_idx = self.next_step_idx.get();

            // next_step_idx is the failing step, and we want to exclude steps *beyond* the failing
            // step, hence adding 1 to the start of the range:
            let steps_after_failure = next_step_idx + 1..self.ops.len();
            for i in steps_after_failure {
                self.currently_included[i] = false;
            }
            // Setting last_shrank_idx to next_step_idx causes us to skip trying to shrink the step
            // that failed, which should be good since the step that triggers the failure is
            // presumably needed to reproduce the failure:
            self.last_shrank_idx = next_step_idx;
        }

        self.try_removing_op()
    }

    /// Undoes the last call to [`simplify`], and attempts to remove another element instead.
    fn complicate(&mut self) -> bool {
        self.currently_included[self.last_shrank_idx] = true;
        self.try_removing_op()
    }
}

impl<T, O> TestTree<T, O>
where
    O: Clone + Debug,
    T: ModelState<Operation = O>,
{
    /// Used by both [`simplify`] and [`complicate`] to attempt the removal of another operation
    /// during shrinking. If we are able to find another operation we can remove without violating
    /// any preconditions, this returns `true`; otherwise, this function returns `false`.
    fn try_removing_op(&mut self) -> bool {
        // More efficient to iterate backward, since we only need to go over the list once
        // (preconditions are based only on the state changes caused by previous ops, so if we're
        // moving backward through the list then the only time we'll be unable to remove an op
        // based on a precondition is if we've previously had to add back in a later op that
        // depends on it due to failing to remove the later op via shrinking; in that case we'll
        // never be able to remove the current op either, so there's no need to check it more than
        // once).
        while self.last_shrank_idx > 0 {
            self.last_shrank_idx -= 1;

            // try removing next idx to try, and see if preconditions still hold
            self.currently_included[self.last_shrank_idx] = false;
            let candidate_ops = self.current();
            // if they still hold, leave it as removed, and we're done
            if preconditions_hold::<T, O>(&candidate_ops) {
                return true;
            }
            // if they don't still hold, add it back and try another run through the loop
            self.currently_included[self.last_shrank_idx] = true;
        }
        // We got all the way through and didn't remove anything
        false
    }
}

/// Tests whether a given sequence of operation elements maintains all of the required preconditions
/// as each step is executed according to [`TestModel`].
fn preconditions_hold<T, O>(ops: &[O]) -> bool
where
    T: ModelState<Operation = O>,
{
    let mut candidate_state = T::default();
    for op in ops {
        if !candidate_state.preconditions_met(op) {
            return false;
        } else {
            candidate_state.next_state(op);
        }
    }
    true
}

/// Holds the internal state of a stateful property test run, including both the supplied
/// [`ModelState`] value, and other fields that are used internally to the proptest-stateful
/// framework.
#[derive(Clone, Debug)]
struct TestState<T, O>
where
    T: ModelState<Operation = O>,
{
    /// Supplied by the caller
    model_state: T,
    /// Used during shrinking to optimize removal of steps after the failing step
    next_step_idx: Rc<Cell<usize>>,
    min_ops: usize,
    max_ops: usize,
}

impl<T, O> Strategy for TestState<T, O>
where
    T: ModelState<Operation = O>,
    O: Clone + Debug,
{
    type Value = Vec<O>;
    type Tree = TestTree<T, O>;

    /// Generates a new test case, consisting of a sequence of Operation values that conform to
    /// the preconditions defined for a instance of [`ModelState`].
    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        let mut symbolic_state = self.model_state.clone();

        let size = Uniform::new_inclusive(self.min_ops, self.max_ops).sample(runner.rng());
        let ops = (0..size)
            .map(|_| {
                let mut possible_ops = symbolic_state.op_generators();
                // Once generated, we do not shrink individual Operations; all shrinking is done via
                // removing individual operations from the test sequence. Thus, rather than
                // returning a Strategy to generate any of the possible ops from
                // `possible_ops`, it's simpler to just randomly pick one and return
                // it directly. It's important to use the RNG from `runner` though
                // to ensure test runs are deterministic based on the seed from proptest.
                let op_gen =
                    possible_ops.swap_remove(runner.rng().gen_range(0..possible_ops.len()));
                let next_op = op_gen.new_tree(runner).unwrap().current();
                symbolic_state.next_state(&next_op);
                next_op
            })
            .collect();

        // initially we include everything, then exclude different steps during shrinking:
        let currently_included = vec![true; size];
        Ok(TestTree {
            ops,
            currently_included,
            last_shrank_idx: size, // 1 past the end of the ops vec
            next_step_idx: self.next_step_idx.clone(),
            have_trimmed_after_failure: false,
            state_type: PhantomData,
        })
    }
}

/// Runs a single test case, as specified by `steps`.
async fn run<T, O>(steps: Vec<O>, next_step_idx: Rc<Cell<usize>>)
where
    T: ModelState<Operation = O>,
    O: Debug,
{
    let mut runtime_state = T::default();
    let mut ctxt = runtime_state.init_test_run().await;

    for (idx, op) in steps.iter().enumerate() {
        println!("Running op {idx}: {op:?}");
        runtime_state.run_op(op, &mut ctxt).await;

        // Note that if we were verifying results of any operations directly, it would be better to
        // wait until checking postconditions before we compute the next state; this would
        // complicate the postcondition code a bit, but would allow us to check that the op result
        // makes sense given the state *prior* to the operation being run. However, right now we
        // just check that the database contents match, so it's simpler to update the expected
        // state right away and then check the table contents based on that.
        runtime_state.next_state(op);

        runtime_state.check_postconditions(&mut ctxt).await;

        next_step_idx.set(next_step_idx.get() + 1);
    }

    runtime_state.clean_up_test_run(&mut ctxt).await;
}

/// Execute a property test for the specified test model, using the config values passed in.
pub fn test<T>(mut stateful_config: ProptestStatefulConfig)
where
    T: ModelState,
{
    // Always make sure we have max_shrink_iters high enough to fully shrink a max size test case:
    stateful_config.proptest_config.max_shrink_iters = stateful_config.max_ops as u32 * 2;

    // Used for optimizing shrinking by letting us immediately discard any steps after the failing
    // step. To enable this, we need to be able to mutate the value from the test itself, and then
    // access it from within the [`TestModel`], which is not easily doable within the constraints
    // of the proptest API, since the test passed to [`TestRunner`] is [`Fn`] (not [`FnMut`]).
    // Hence the need for interior mutability.
    let next_step_idx = Rc::new(Cell::new(0));

    let test_state = TestState {
        model_state: T::default(),
        next_step_idx: next_step_idx.clone(),
        min_ops: stateful_config.min_ops,
        max_ops: stateful_config.max_ops,
    };

    // We have to manually construct a [`TestRunner`] with the given config because using the
    // closure-style version of the [`proptest!`] macro causes the `source_file` value to be
    // unconditionally replaced with the result of `file!()`; this prevents other tests from
    // configuring the failure persistence features to output regressions into the same crate
    // directory structure as the actual test code.
    let mut runner = TestRunner::new(stateful_config.proptest_config);
    let result = runner.run(&test_state, |steps| {
        prop_assume!(preconditions_hold::<T, _>(&steps));
        next_step_idx.set(0);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            tokio::time::timeout(
                stateful_config.test_case_timeout,
                run::<T, _>(steps, next_step_idx.clone()),
            )
            .await
        })
        .unwrap();
        Ok(())
    });

    match result {
        Ok(_) => (),
        Err(e) => panic!("{}\n{}", e, runner),
    }
}
