# henderson-tech/mochi-server: the Voke fork

A thin fork of [mochi-mqtt/server](https://github.com/mochi-mqtt/server). It
exists because upstream has not tagged a release since v2.7.9 (2025-03-01), and
because Voke's device broker needs a few fixes that upstream does not yet carry.
The branch is `voke` and releases are tagged `v2.8.0-voke.N`. The module path
stays `github.com/mochi-mqtt/server/v2`, so a consumer pins the fork with a
replace:

```
replace github.com/mochi-mqtt/server/v2 => github.com/henderson-tech/mochi-server/v2 v2.8.0-voke.1
```

Every patch is a generic bug fix with no Voke-specific code, and each one is
written so it could be offered upstream as-is. Anything specific to Voke lives
in Voke's own hooks. The decision record is the device-plane spec in
henderson-tech/voke-platform (`docs/specs/2026-10-18-device-plane-decisions.md`,
§12 "Fork patches" and owner decision Q2). Voke's notes on the fork are in
`docs/internal/mochi-fork.md` in that repository.

## Base

The base is upstream `release/2.8.0` at `3b7c3e6`. That is `main` plus 32 pull
requests with CI green, opened as [#543](https://github.com/mochi-mqtt/server/pull/543).
It is unmerged because the upstream ruleset needs 3 approving reviews and
nobody can give them. The base already carries three fixes Voke needs:

| Upstream | What | In the base as |
|---|---|---|
| [#525](https://github.com/mochi-mqtt/server/pull/525), [#489](https://github.com/mochi-mqtt/server/pull/489) | `Clients.GetByListener` took its read lock twice, so `Server.Close` could deadlock against a connect or a disconnect. | `5f08b8c`, merge `3f0d2f4` |
| [#547](https://github.com/mochi-mqtt/server/issues/547) via #520 | `Inflight.NextImmediate` took its read lock twice, so a client's read loop could deadlock against a QoS 1 publish to it. | `ec361f8` |
| [#501](https://github.com/mochi-mqtt/server/issues/501) via #524 | A truncated MQTT 5 SUBSCRIBE panicked the whole process. | `3d6b641` |

Read the release notes in #543 before you upgrade across the base. Thirteen of
its changes alter what a client sees on the wire. Examples: a QoS 1 PUBACK now
carries `0x00`, and a 3.1.1 client that publishes to `$SYS` at QoS ≥ 1 is
disconnected.

## Patches on `voke`

| Commit | What it fixes | Upstream |
|---|---|---|
| `4fb3d02` clients: read a packet's remaining length as it arrives | `ReadPacket` allocated the remaining length a fixed header declared, up to 256 MiB, before a byte of it arrived and before CONNECT was authenticated. The buffer now grows as bytes arrive. | [#502](https://github.com/mochi-mqtt/server/issues/502) |
| `d5996e9` test: a truncated v5 SUBSCRIBE ends only its connection | A test that sends the exact bytes from the #501 report over the wire. | [#501](https://github.com/mochi-mqtt/server/issues/501) |
| `96fb29a` clients: a resent session message passes the read ACL | A session is keyed by client id alone. On resume, its queued messages were written to whoever resumed it, with no read-ACL check. | [#504](https://github.com/mochi-mqtt/server/issues/504) |
| `f75e469` pebble: count the writes the store refused | A failed Set, Delete or encode was only logged. `Hook.Errors()` now counts them, so an embedder can alarm on them. | none |
| `746a556` inflight: a held delivery keeps its expiry and stays in flight | A delivery held for Receive Maximum lost its expiry (it was marked with `Expiry = -1`). Once released, it was deleted, so its PUBACK never returned the quota and every later delivery stayed held. A resume also re-sent deliveries that had never been sent. | none filed |
| `48819b9` expiry: absolute expiry for every protocol and write path | Expiry was honoured only for MQTT 5, and only by the 1 s tick. Resend and the quota release never checked it. `WritePacket` clamped an expired interval to 1 s and sent the message anyway. One rule (`messageExpired`) now covers every path and protocol version. A delayed Will no longer reads as expired. Each expiry fires `OnQosDropped` once, not twice. | none filed |
| `d40bbbf` storage: a stored message keeps its absolute expiry | `storage.Message` had no `Expiry` field, so a message restored after a restart had no deadline. | none |
| `feecf0e` test: PUBACK follows OnPublish; Ignore skips retain and fan-out | Tests that pin the publish-hook contract Voke builds on. | none |
| `b9dcc01` server: a connection lost at its CONNACK ends like any other | A connection that failed after authentication but before its read loop (the CONNACK write to a peer that reset the socket, or the resend of a resumed session) returned without `OnDisconnect`, without its Will, and left a clean session in `s.Clients`, for a 3.1.1 client by default forever. Every exit after authentication now runs the read loop's end bookkeeping, and an earlier connection's delayed Will is dropped before the CONNACK, not after it. | none filed |
| `854e7d2` server: one client id's takeover, end and expiry run one at a time | Two connections of one client id attaching at once each read the same old session and registered themselves, so the first lost its `s.Clients` entry while neither was taken over: a live, subscribed connection nothing was ever written to. A clean connection's end and the expiry sweep cleaned up by client id, removing a successor's subscriptions and entry. A per-id session lock now serialises takeover, the end's cleanup and expiry, and `Clients.DeleteClient` removes only the client it names. `935cffb` also runs `OnSessionEstablished` and `OnDisconnect` under that lock, so a storage hook never deletes or overwrites a successor's record; a hook the lock is held for must not attach, end or `DiscardClient` the same id. `Server.DiscardClient` ends an offline listed session only while it still holds the id, and fences a stopped client's pending end. | none filed |
| `c049232` storage: restoring the stored sessions writes none of them back | The pebble and badger hooks rewrote every restored client's record when the boot told it `OnDisconnect`: one write per stored session (an fsync each in Sync mode) before the server served, and each record lost its remote address. | none filed |

### Known gaps that stay unfixed

- [#545](https://github.com/mochi-mqtt/server/issues/545): inbound and outbound
  packet ids share one inflight keyspace, so an inbound PUBLISH can overwrite a
  server delivery that has the same id.
- [#553](https://github.com/mochi-mqtt/server/pull/553): a plain error from
  `OnPublish` still gets a PUBACK. Voke's hooks return `ErrRejectPacket` when
  they need to withhold one.
- A resumed session's send quota is reset to the client's full Receive
  Maximum. The deliveries it re-sends then take slots back out of that quota.
- No read deadline applies before CONNECT. The embedder's listener sets one.

## Rebasing onto a new upstream

1. `git fetch upstream`, then pick the new base: an upstream tag if one exists,
   otherwise the head of the release branch. Note its commit here.
2. `git rebase --onto <new-base> <old-base> voke`. Drop any patch upstream now
   carries, and keep its test if upstream's test does not cover the same case.
3. Run the whole suite with `-race`: `go vet ./... && go test -race ./...`.
   `hooks/storage/badger` is known to hang now and then in its gc loop. A
   rerun clears it.
4. Tag the next version, `v<base>-voke.<n+1>`. Push the branch and the tag
   (`git push --force-with-lease origin voke && git push origin <tag>`). A
   pushed tag is final, because the Go module proxy caches it. Never move one:
   cut the next number instead.
5. In each consumer, bump the replace, run `go mod tidy`, then run the broker
   suites.

The review budget is 1–2 person-days a month, covering mochi and nats-server
together. That covers reading upstream's new issues and pull requests, deciding
what to carry, and rebasing.
