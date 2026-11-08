// A whole store on Swarm in one call, as Python's `swarm_store`: blobs on a
// Bee node, the latest root in a Swarm feed.

import { RecordStore } from './recordstore.js';
import { BeeBytesStore } from './stores.js';
import { SwarmFeedPointer } from './feeds.js';

/**
 * A RecordStore that lives on Swarm, at its feed's current root.
 *
 *   await swarmStore('my-notes', { signer: key, batch })  // publish your own
 *   await swarmStore('my-notes', { owner: address })      // follow someone else's
 *
 * Writing needs a postage batch of the node's (`batch`); unlike Python's
 * `stamp="auto"`, none is picked for you.
 */
export async function swarmStore(topic, {
  url = 'http://localhost:1633', batch = null, signer = null, owner = null, feedTtl = 15, concurrency = 32,
} = {}) {
  if (!signer && !owner) {
    throw new TypeError('swarmStore needs a signer (private key hex, to publish) or an owner (address hex, to follow)');
  }
  const blobs = new BeeBytesStore(url, { batch, concurrency });
  const pointer = new SwarmFeedPointer(url, topic, { signer, owner, batch: signer ? batch : null, feedTtl });
  return RecordStore.open(blobs, { pointer });
}
