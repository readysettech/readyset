# Gerrit admin runbook

## Merging CLs without CI

Sometimes, especially when working with changes to Terraform that don't
synchronize with state properly, it can be necessary to merge CLs to `main` that
are "technically" failing CI, but may not fail once merged.

Doing this requires being a member of the ["Administrators" group in
Gerrit][Administrators], and is a slightly involved process to reflect that it
should not be taken lightly.

1. Navigate to [Browse -> Groups -> CI -> Members in Gerrit][CI Members]
2. Enter your username or email in the "Name Or Email" text field, and click
   "Add" to add yourself to this group
3. Open the CL page for the CL you want to merge
4. If present, click the trash can icon next to the `Verified -1` label set by
   Buildkite on the CL to remove the CI failing status
5. Click "Reply", and label the CL `Verified +1` yourself. The CL can now be
   submitted. After it's submitted, the message will contained `Tested-By:
   <you>` to mark that CI was skipped on the CL.
6. Remember to remove yourself from the CI group after you're done!

[administrators]: https://gerrit.readyset.name/admin/groups/66d5d12f6df3f27745e697744d9135c484616974
[ci members]: https://gerrit.readyset.name/admin/groups/cbfca8ef35d08fe33fed2ca3ae791acc3bb23c88,members
