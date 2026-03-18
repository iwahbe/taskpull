---
repo: ~/src/pulumi-foo
repeat: true
branch_prefix: iwahbe/fix-expected-failure
---

Go to the expected failures list. Pick one expected failure that looks easy to fix.
Remove it from the expected failures list, fix the underlying issue, rerun the
test with `pulumi accept`, check in the updated test files, run all tests and
the linter to make sure everything passes, then finish.

If the expected failures list is empty or you cannot find any failure that is
appropriate to fix, signal that there is nothing left to do.
