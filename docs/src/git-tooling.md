# Git Tooling

## Git command line extensions

There are extensions to the git command line interface that can be installed
to help with the stacked diff workflow.

You can find a [list of various tools][0] but a couple are discussed here.

[0]: https://github.com/arxanas/git-branchless/wiki/Related-tools

### git-branchless

[git-branchless](https://github.com/arxanas/git-branchless) requires adding a
hook to the git repository you want it to work in but adds additional features
like a `git undo` command alongside other tooling.

### git-stack

[git-stack](https://github.com/gitext-rs/git-stack) requires marking some
branches as protected and and requires working off on another branch which
might be more familiar.

## Editor integrations

### VSCode

The following configuration helps with commit message writing by adding
a colored column at 50 and 72 characters to help keep the different lines within
the boundaries for the subject and the body.

```json
"[git-commit]": {
    "editor.rulers": [50, 72],
}
```

The [GitLens extension](https://gitlens.amod.io/) includes a [special editor][4]
for dealing with rebases.

[1]: https://github.com/gitkraken/vscode-gitlens#interactive-rebase-editor-

### Vim/NeoVim

The [fugitive](https://github.com/tpope/vim-fugitive) plugin includes
many tools for helping with commits and rebases.

#### Example Workflow

Let's walk through how one would apply a fix a few commits back in a commit
chain using Vim Fugitive.

##### Fixup method

An easy way to apply a fix when you are at the tip of a commit chain onto an
earlier commit in the chain, is to write the fix first. Then pull up Fugitive
using the `:Git` command. From here, we can add untracked or unstaged changes by
highlighting the file we want to stage, and hitting the `-` key on our keyboard.
We can similarly unstage changes by moving to a staged change and hitting the
same `-` key to move those changes back (more on line based committing using
Fugitive later).

Once we have staged the changes we would like to apply backwards, we can then
type `:Git log` to pull up our log. Simply move down in the log until you are
highlighting the commit you would like to apply the fix towards, and type `cF`
with the line highlighted (lower case c, upper case F) and Fugitive will
automatically create a fixup commit, and perform a rebase, autosquashing the
fixup commit for us.

One caveat to this is that the rebase will not successfully apply if you have
any unstaged changes. This is true in general of any rebase. This can be
unexpected when doing line based commiting and applying multiple fixup commits
at once using the `cF` shortcut in Fugitive. My general approach to this is to
not worry about it, because the very last fixup that I apply will result in no
remaining unstaged changes, and all fixup commits will be correctly squashed at
that point in time.

###### Rebase edit method

You can also apply a fix into an earlier commit, but performing an interactive
rebase, and choosing to `edit` the commit in question. This will drop you into
that commit. Once you have applied your edits, you can amend the commit with
`git commit --amend`, and then use `git rebase --continue` to finish the rebase
process. This can be a useful way to work, but can also in some cases make it
difficult to test how a change to an earlier commit effects future work in the
commit chain. In that case the fixup approach is more sensible.

##### Line-based commiting

Sometimes we will work on a larger body of work that we realize later should be
split into multiple commits. To handle this task, we can perform line based
commiting using Vim Fugitive.

Type `:Git` to pull up Fugitive. Highlight the file you would like to perform
line based commiting on, and type `dd`. Another approach is if you already have
that file open you can simply type `:Gdiffsplit`. This will bring up a split. On
the top will be the currently committed file, and on the bottom, your changed
version. To commit a line, or group of lines, use Vim's visual selection to
select the lines you would like to add, then type `:diffput` and hit enter. This
will commit just the selected lines. You should now see them appear in the top
split. If the change is a deletion, we instead will want to move to the top
split, select the lines in question and use `:diffobtain` instead.

We can confirm which lines we have staged by pulling up Fugitive with `:Git` and
hit the `=` key on the file in question. This will pop down a mini diff that
makes it very easy to see what you have staged.

When you are finished with all relevant files, write a commit and move onto the
segments you would like to stage for the next commit.

### Emacs

The [magit](https://magit.vc/) package includes many tools to help with
commits and rebasing.

### IntelliJ

TODO: Looking for help here.
