# Vendored: js-sha3 0.9.3

Source: the `js-sha3` npm package, version 0.9.3
(https://github.com/emn178/js-sha3), MIT license (see LICENSE).

Vendored subset: `src/sha3.js` only (unmodified), plus the `index.js` ESM
facade. Copied from swarmlite's `js/vendor/js-sha3/`, which vendored it
first; same file, same version.

SHA-256 of `sha3.js`:
`6378cc474ad0a0db3e88c0c67b7bd4a154e1b318f44dbbd13927f6e220efaa38`

Used for keccak256 in Swarm (BMT) content addressing (`src/address.js`).

Vendored (rather than npm-installed) so that a page published on Swarm can
import recordstore with relative URLs only: no registry, no CDN, no build.

Sanity check: `keccak256('hello')` =
`1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8`
(the well-known Ethereum keccak vector; asserted in `test/address.test.js`).

To upgrade: `npm pack js-sha3`, extract, copy `src/sha3.js` and LICENSE,
update the version and hash here, run `npm test`.
