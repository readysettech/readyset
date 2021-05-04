# Code Review

#### Goals of code review:

1. Increase the quality of the code. This can include (but is not limited to)
  * Reducing the number of bugs introduced by new code

  * Improving the readability of code so that future engineers are less dependent on the current author for an explanation

2. Recieve constructive feedback on our work in order to improve code quality on future PRs

#### Before you request a reviewer, please ensure the following hold true:

1. The changes you've made are organized into small, easy(-ish!) to understand commits. Each commit should:
   * Have a message in the form
   ~~~
   Commit subject line.

    A detailed commit body message. This will likely be several
    sentences long. Dont be shy about explaining intentions and
    potential trade offs you made during this code change.
   ~~~
   * Compile
   * Pass `cargo fmt --check`
   * Pass all tests

2. The name of the PR is a brief description of the change made

3. The PR description is filled out with the following information:
  * **Description** -- a description of the actual code changes themselves. It is ok for this to include duplicated info from the commit messages. This can also include any extra details that will help the reviewer understand the changes including design doc links, etc.
  * **Known Issues** -- A description of any known bugs (whether they were caused by your change or not) that have to do with the changes
  * **Suggested Review Plan** -- suggested strategy for reviewer. In almost all PRs, this will be `commit by commit`
  * **Starting Places** -- a brief decription (even just a filename) of where to start the review for each commit
  * **Testing** -- a description of the testing done around the changes

#### Once the PR has all required info:
1. Request main reviwer(s). In most cases, we try to make this a single person but sometimes multiple are needed. At least one of these reviewers should be someone who did NOT pair code with you on the PR.

2. Tag any other relevant people in a comment on the PR. This distinction between reviewers and "cc'd" helps us make sure PRs are not blocked waiting on too many people for CR if their review isnt necessary for merge.

#### Once you have received comments:
1. For each comment, leave some sort of acknowledgement that you made the change (I use the thumbs up reaction) OR a comment explaining why you think the change is unnecessary.

2. When you make code changes, use interactive rebase to make them in the relevant commit. We should not end up with commits with the message "Code Review changes."

3. Once you have made all changes, **re-request the reviewers**. This is very important and is the only way they know you have addressed everything.


#### During the actual code review phase, keep the following in mind:

1. Both reviewers and authors should approach code review with the goal of producing the best solution within reasonable time constraints.

2. It is normal for large PRs to get 20+ comments.

2. Code review on comments (or lack thereof) and commit messages is also common.

3. Code reviewers should try to always explain *why* their changes are better. This will help the PR author evaluate if the change is necessary and, if it is, improve their code style for the next PR.

4. If an author is not convinced about a reviewer's comment, they are not required to make the change. At the end of the day, the author has ownership over the work, not the reviewer.

#### Parting remarks:
1. The PR needs to pass the CI pipeeline in order to be merged. You can see the progress of this as well as all failing tests by using the link to Buildkite in the PR status dot.

2. To resolve conflicts with master, REBASE. Do not "merge" as this creates merge commits that pollute the git history.

2. Once it has been approved and passes CI, REBASE AND MERGE. Wooooo!