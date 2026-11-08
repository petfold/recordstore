// Runs test/freedom/provider-check.html in real Freedom Browser, from a
// freedom-browser checkout, and prints what it found as JSON.
//
//   node test/freedom/freedom_check.cjs [FREEDOM_DIR] [--bee=URL] [--write]
//
// FREEDOM_DIR: a freedom-browser checkout with its node_modules (default:
// beside this repository). Freedom is launched the way its live tests and
// ontodag's check launch it: a throwaway profile, and either its own Ant
// node (default) or, with --bee=URL, that Bee node as its external Swarm
// node. The page is served from this package's directory on 127.0.0.1 and
// opened through the address bar, so it reads and writes through Freedom's
// real window.swarm provider.
//
// Without --write only the read check runs. With --write the page also
// writes a small store, which spends stamps: the check approves Freedom's
// connect prompt and its publish prompt (ticking "Always allow this site",
// so the commit's chunks need one approval). Freedom's node needs a usable
// postage batch for that; without one the write fails with no-usable-stamps.
//
// Report and screenshots go to a temporary directory, named in the output.

const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const { createRequire } = require('module');

const args = process.argv.slice(2);
const positional = args.filter((a) => !a.startsWith('--'));
const flag = (name) => args.find((a) => a === `--${name}` || a.startsWith(`--${name}=`));
const bee = (flag('bee') || '').split('=')[1] || null;
const doWrite = Boolean(flag('write'));
const PACKAGE = path.resolve(__dirname, '..', '..');
const freedomDir = path.resolve(positional[0] || path.join(PACKAGE, '..', '..', 'freedom-browser'));
const { _electron: electron } = createRequire(path.join(freedomDir, 'package.json'))('@playwright/test');

const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.json': 'application/json', '.css': 'text/css' };

/** Serve this package's directory (read-only, no listing) on 127.0.0.1. */
function serve() {
  const server = http.createServer((req, res) => {
    const rel = decodeURIComponent(new URL(req.url, 'http://x').pathname).replace(/^\/+/, '');
    const file = path.resolve(PACKAGE, rel);
    if (!file.startsWith(PACKAGE + path.sep) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404).end();
      return;
    }
    res.writeHead(200, { 'content-type': TYPES[path.extname(file)] || 'application/octet-stream', 'cache-control': 'no-store' });
    fs.createReadStream(file).pipe(res);
  });
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve(server)));
}

function inPage(window, code) {
  return window.evaluate(async (snippet) => {
    const wv = document.querySelector('webview:not(.hidden)');
    if (!wv || typeof wv.executeJavaScript !== 'function') return { error: 'no webview' };
    try { return await wv.executeJavaScript(snippet); } catch (e) { return { error: String(e) }; }
  }, code);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function visible(window, selector) {
  return window.locator(selector).isVisible().catch(() => false);
}

async function main() {
  const server = await serve();
  const url = `http://127.0.0.1:${server.address().port}/test/freedom/provider-check.html`;
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'recordstore-freedom-'));
  const dirs = Object.fromEntries(['userData', 'ant-data', 'ipfs-data', 'identity'].map((d) => {
    const p = path.join(tmp, d);
    fs.mkdirSync(p, { recursive: true });
    return [d, p];
  }));
  fs.writeFileSync(path.join(dirs.userData, 'settings.json'), JSON.stringify({
    startIpfsAtLaunch: false, startRadicleAtLaunch: false, startTorAtLaunch: false,
  }));
  fs.writeFileSync(path.join(dirs.userData, 'external-node-decisions.json'), JSON.stringify({
    bee: bee ? { choice: 'external', endpoints: [bee] } : { choice: 'managed', endpoints: ['http://127.0.0.1:1633'] },
  }));
  const app = await electron.launch({
    args: ['.'], cwd: freedomDir, timeout: 90_000,
    env: {
      ...process.env,
      FREEDOM_TEST_USER_DATA: dirs.userData, FREEDOM_ANT_DATA: dirs['ant-data'],
      FREEDOM_IPFS_DATA: dirs['ipfs-data'], FREEDOM_IDENTITY_DATA: dirs.identity,
      ELECTRON_DISABLE_SECURITY_WARNINGS: 'true', LANG: 'en_US.UTF-8',
    },
  });
  const out = { url, node: bee ? `external ${bee}` : "Freedom's own Ant", profile: tmp };
  try {
    const window = await app.firstWindow();
    await window.waitForLoadState('domcontentloaded');
    const input = window.locator('[data-test="address-input"]');
    await input.waitFor({ state: 'visible', timeout: 60_000 });
    await input.click();
    await input.fill(url);
    await input.press('Enter');
    let started = Date.now();
    while (Date.now() - started < 60_000) {
      if ((await inPage(window, 'Boolean(window.rsCheck && window.rsCheck.ready)')) === true) break;
      await sleep(1000);
    }
    out.provider = await inPage(window, 'window.rsCheck ? window.rsCheck.provider : null');

    // Reads may wait for the node to find peers: retry for a few minutes.
    started = Date.now();
    for (;;) {
      out.read = await inPage(window, 'window.rsCheck.read()');
      if (!out.read || !out.read.error || Date.now() - started > 300_000) break;
      out.readRetries = (out.readRetries || 0) + 1;
      await sleep(15_000);
    }
    await window.screenshot({ path: path.join(tmp, 'read.png') });

    if (doWrite) {
      await inPage(window, 'window.rsCheck.pendingWrite = window.rsCheck.write(), true');
      const prompts = [];
      started = Date.now();
      while (Date.now() - started < 600_000) {
        if (await visible(window, '#swarm-connect-approve')) {
          await window.waitForFunction(() => !document.getElementById('swarm-connect-approve').disabled, null, { timeout: 30_000 });
          await window.locator('#swarm-connect-approve').click();
          prompts.push('connect approved');
        } else if (await visible(window, '#swarm-publish-confirm')) {
          const always = window.locator('#swarm-publish-auto-approve');
          if (!(await always.isChecked().catch(() => false))) await always.check({ force: true }).catch(() => {});
          const ticked = await always.isChecked().catch(() => false);
          await window.locator('#swarm-publish-confirm').click();
          prompts.push(`publish approved (always allow ${ticked ? 'ticked' : 'NOT ticked'})`);
        }
        const result = await inPage(window, 'window.rsCheck.results.write || null');
        if (result) {
          out.write = result;
          break;
        }
        await sleep(1000);
      }
      out.prompts = prompts;
      await window.screenshot({ path: path.join(tmp, 'write.png') });
    }
  } finally {
    await app.close().catch(() => {});
    server.close();
  }
  fs.writeFileSync(path.join(tmp, 'report.json'), JSON.stringify(out, null, 1));
  console.log(JSON.stringify(out, null, 1));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
