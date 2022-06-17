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

## Help! The apple build instance can't clone the repository!

Specifically something like [this failure][build-17636], where you're seeing ssh
timeouts on the clone from Gerrit.

The way the MacStadium instance is set up to clone from gerrit is as follows:

- We run an instance, owned by the network account but in the Build VPC, which
  connects to tailscale and advertises the VPC's subnet as a route to the
  tailnet. These instances, called "tailscale subnet routers", are created by
  `//ops/substrate/modules/tailscale-vpn`.
- Tailscale is installed on the macstadium instance, and run with `sudo
  tailscale up --accept-routes`.
- The private IP of the gerrit instance (`10.3.175.41` as of writing), is then
  routable via the tailscale subnet router in us-east-2.

What breaks this though is that sometimes when that tailscale subnet router
instance is restarted or recreated (eg via a `terraform apply` that forces its
replacement), tailscale thinks of it as a *different machine*, and creates it
via a deduplicated hostname (something like
`tailscale-build-default-us-east-2-1`), without any of the settings copied over,
including the setting to enable advertised routes! So some manual intervention
is required:

1. Log into the tailscale admin console and go to "Machines"
2. Find the duplicated machine (you're looking for one suffixed with a number
   that's connected, and any number either not suffixed with that number or
   suffixed with a lower number that're not connected)
3. Delete all but the connected machine by clicking the three dots on the right
   of the page and clicking "Remove..."
4. Enable route advertising on the connected machine by clicking the three dots
   and clicking "Edit route settings..." then turning on the "Subnet routes"
   switch
5. Disable key expiry on the connected machine by clicking the three dots and
   clicking "Disable key expiry"
6. (Optionally) rename the machine to remove the extra number suffix.

[build-17636]: https://buildkite.com/readyset/readyset/builds/17636#01814408-f7d9-42b8-9bc0-6ea05949b42e

## Mark a user inactive (for offboarding)

To disable an offboarded user from clogging up the suggested reviewers in
gerrit, administrators can run the following command:

```console
$ ssh -p 29418 gerrit gerrit set-account <email> --inactive
```
