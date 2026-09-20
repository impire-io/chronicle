# Contributing to chronicle

This repository is the open tenant plane of chronicle: everything that
runs, or is used, inside one tenant, on any NATS the operator already has
(chronicle-hq design
[11-the-two-forms](https://github.com/impire-io/chronicle-hq/blob/main/02-DESIGN/11-the-two-forms.md),
decision
[0031](https://github.com/impire-io/chronicle-hq/blob/main/03-DECISIONS/0031-open-is-one-tenant-the-service-is-managed.md)).
Design lives in `chronicle-hq`; capabilities land here through its build
handoff, never by invention in a pull request. A contribution that
changes a contract starts as a conversation there.

## The Developer Certificate of Origin

Contributions are accepted under the
[Developer Certificate of Origin, version 1.1](https://developercertificate.org/)
(decision 0031). By adding a `Signed-off-by` trailer to a commit you
certify that you wrote the change or otherwise have the right to submit
it under this repository's license, and that you understand the
contribution is public and will be redistributed. Sign every commit:

```sh
git commit -s
```

The trailer must name you and a real address — `Signed-off-by: Ada
Lovelace <ada@example.com>`. Continuous integration refuses a pull
request whose commits lack it. There is no contributor license
agreement and no copyright assignment.

## The quality gate

`make check` — format, tidy, build, race-detected tests, lint — is green
before a change is done. The wire contract is tested against a real
embedded NATS server; mocking the NATS client is not accepted. Every
addition names the present need it serves.

## License

The repository's license is the file `LICENSE`. Contributions are
licensed under it as it stands when they are merged.
