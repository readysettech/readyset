# Gerrit Social Conventions at ReadySet

# Summary

At ReadySet, we use Gerrit for code review. Because code review involves a
number of social interactions that are not clear from the interface or
other documentation, we have written them down for everyone to be on the
same page.

# Words

CL/Changelist: A unit of work being reviewed. Each Changelist maps onto
a single Git commit.

Attention Set: The list of people who will be notified via email and
have the CL put back into the "Your Turn" section on the user dashboard.
This is identified by the flag in front of the user's name.

# Commit Messages

Commit messages here are closer to what others would put inside of
the Pull Request message at other organizations. A reasonable
explanation of the formatting is at
[[https://chris.beams.io/posts/git-commit/]{.underline}](https://chris.beams.io/posts/git-commit/).
We do lint commit messages with softer line length limits but these are
good to aim for.

Subject line: \< 50 characters

Other lines: \< 80 characters

# Review Votes

Every review is associated with a vote of an integer between -2 and +2.
To have your review accepted you MUST have at least one +2 from one
reviewer who is not yourself.

The votes mean approximately the following.

**-2**: I do not believe this should be included in the code base as is
due to some blocking issue. Gerrit will not allow a merge if there is
any current -2 vote.

**-1**: I do not believe this should be included in the code base as is.
However, I do not wish to block merging if someone else does approve
this.

**0**: I want to leave a review but have no opinion either way on the
inclusion of this code.

**+1**: I do believe this should be included in the code base. However,
I do not feel comfortable giving final approval.

**+2**: I do believe this should be included in the code base AND I am
willing to accept responsibility as a reviewer. Those who vote +2 will
be included in the final commit message.

# Review Comment Responses

We have configured Gerrit to require all comments be resolved before
review can be completed. Gerrit gives two buttons to comment and resolve
with a single click. These responses have subtly different meanings.

**Done**: I have read your comment and implemented any actionable
feedback on it in the further iterations of this changelist.

**Ack**: I have read your comment and acknowledged your feedback.
However, if there is actionable feedback I have decided to not implement
it.

# Comment shorthand

Many reviewers at ReadySet use a common set of shorthand prefixes in
their review comments to indicate the context of the comment - while
this is not required or standardized, it can be a nice way of indicating
how important you think the comment is to resolve.

-   "nit:" short for "nitpick" - a pedantic or otherwise low-severity
    > comment that may improve the stylistic consistency or readability
    > of the code, but is otherwise not critical to the functionality.
    > The implication is that nits can be ignored by the author with
    > relatively low social cost.

# Development with Gerrit

**What is a changelist (CL)?**

A CL represents a single change under review. At ReadySet this is a
change that a developer wants to get merged into the main branch. It
requires a myriad of continuous integration tests, resolution of all
code review comments, and explicit +2 approval from another developer to
get merged. Each change receives a unique Change-Id on commit using the
Gerrit commit-msg hook. When a commit with a new Change-Id is pushed to
Gerrit, a new CL is created.

A CL may have multiple patchsets, one patchset represents one iteration
of the CL's code. Pushing a CL that already exists creates a new
patchset if the code has changed.

## Best Practice

-   If you make changes to the CL that don't have to do with open
    > comments, or are significant, remove existing +2s and put the
    > reviewers back in the attention set.

-   Don't merge another author's CL without explicit permission.

# Tooling

For helpful git tooling, please see [this document](git_tooling.md).
