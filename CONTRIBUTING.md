# Welcome to Ceph NVMe-oF Gateway contributing guide <!-- omit in toc -->

## New contributor guide

The Ceph NVMe-oF Gateway project pivots around 2 other major Open Source projects:
- [SPDK](https://spdk.io/) [[source code](https://github.com/spdk/spdk/)], which internally relies on the DPDK project.
- [Ceph](https://ceph.io/) [[source code](https://github.com/ceph/ceph)].

## Engage with the community

Besides the [regular Ceph community channels](https://ceph.io/en/community/connect/), the NVMe-oF
Gateway team can be specifically reached at:
- [Ceph Slack workspace](https://ceph-storage.slack.com/), `#nvmeof` channel,
- [Weekly Sync Meeting](https://pad.ceph.com/p/rbd_nvmeof)

## Report issues

If you find an issue, identify whether the issue comes from the NVMe-oF Gateway, or any of the underlying components:
* For NVMe-oF Gateway issues (usually a Python traceback), check if [the issue has already been reported](https://github.com/ceph/ceph-nvmeof/issues).
  Otherwise, [open a new one](https://github.com/ceph/ceph-nvmeof/issues/new).
* For SPDK-related issues, [open an issue in the SPDK GitHub Issues](https://github.com/spdk/spdk/issues/).
* For Ceph-related issues, [open an issue in the Ceph Tracker](https://tracker.ceph.com/).

## Submit changes

### Coding conventions

This project follows:
* Python coding guidelines ([PEP-8](https://peps.python.org/pep-0008/)).
* [gRPC and Protocol Buffers](https://grpc.io/docs/what-is-grpc/introduction/).

### Commit format

When committing your changes:
* Sign-off (`git commit -s`), which will automatically add the following trailer to your commit: `Signed-off-by: FirstName LastName <email@example.com>`.
  This constitutes your [Developer Certificate of Origin](https://en.wikipedia.org/wiki/Developer_Certificate_of_Origin), and is enforced by a [CI check](https://probot.github.io/apps/dco/). 
* Follow the [Conventional Commit syntax](https://www.conventionalcommits.org/en/v1.0.0/). This is not yet enforced via CI checks.

### Testing

TODO.

### Pull request

Refer to Ceph's ["Submitting Patches to Ceph"](https://github.com/ceph/ceph/blob/main/SubmittingPatches.rst) documentation, with the difference that:
* This project uses GitHub Issues instead of the Ceph Tracker,
* Therefore commits and pull requests should use the `Fixes: #12345` syntax (where `#12345` is the GitHub issue number), instead of the `Fixes: https://tracker.ceph.com...`.

### Documentation

TODO: No documentation is yet in place.
