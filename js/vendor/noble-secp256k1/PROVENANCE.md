# Vendored: @noble/secp256k1 2.3.0

Source: the `@noble/secp256k1` npm package, version 2.3.0
(https://github.com/paulmillr/noble-secp256k1), MIT license (see LICENSE).
Single-file, zero-dependency, audited ECDSA implementation.

Vendored subset: `index.js` only (unmodified ESM). Copied from swarmlite's
`js/vendor/noble-secp256k1/`, which vendored it first; same file, same
version.

SHA-256 of `index.js`:
`8ba6138826758ef7d13243f18f7fbf8b26c81c6abea706e692baa83dfe3827e1`

Used for feed updates (`src/feeds.js`): signing them (`signAsync`, RFC 6979
deterministic with low S, the same signatures libsecp256k1 makes for
Python) and recovering the signer of each one read, so a forged update is
caught even through an untrusted node or gateway.

Vendored (rather than npm-installed) so that a page published on Swarm can
import recordstore with relative URLs only.

To upgrade: `npm pack @noble/secp256k1`, extract, copy `index.js` and
LICENSE, update the version and hash here, run `npm test` (the feed
fixtures pin signatures byte for byte).
