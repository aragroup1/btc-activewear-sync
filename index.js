// index.js
const express = require('express');
const ftp = require('basic-ftp');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const multer = require('multer');
const cron = require('node-cron');
const { Readable, Writable } = require('stream');
require('dotenv').config();

const app = express();
app.use(express.json());

// Request logging
app.use((req, res, next) => {
  console.log(`[REQ] ${req.method} ${req.url}`);
  next();
});

// Health
app.get('/health', (req, res) => res.status(200).send('ok'));

// Optional API key
const API_KEY = process.env.API_KEY || '';
function requireApiKey(req, res, next) {
  if (!API_KEY) return next();
  if (req.headers['x-api-key'] === API_KEY) return next();
  return res.status(401).json({ error: 'Unauthorized' });
}

// Upload (future use)
const upload = multer({
  dest: path.join(__dirname, 'uploads/'),
  limits: { fileSize: 500 * 1024 * 1024 }
});

// ============================================
// CONFIGURATION
// ============================================

const config = {
  shopify: {
    domain: process.env.SHOPIFY_DOMAIN,
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
    locationIdNumber: process.env.SHOPIFY_LOCATION_ID,
    locationId: `gid://shopify/Location/${process.env.SHOPIFY_LOCATION_ID}`,
    baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-04`,
    graphqlUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-04/graphql.json`
  },
  ftp: {
    host: process.env.FTP_HOST || process.env.BTC_FTP_HOST || 'ftpdata.btcactivewear.co.uk',
    user: process.env.FTP_USERNAME || process.env.BTC_FTP_USERNAME || 'ara0010',
    password: process.env.FTP_PASSWORD || process.env.BTC_FTP_PASSWORD || '',
    secure: false
  },
  btc: {
    stockFilePath: '/webdata/stock_levels_stock_id.csv',
    csvSeparator: (process.env.BTC_CSV_SEPARATOR || 'auto').toLowerCase(),
    maxInventory: parseInt(process.env.BTC_MAX_INVENTORY || process.env.MAX_INVENTORY || '9999', 10)
  },
  telegram: {
    botToken: process.env.TELEGRAM_BOT_TOKEN,
    chatId: process.env.TELEGRAM_CHAT_ID
  },
  failsafe: {
    maxRuntime: parseInt(process.env.MAX_RUNTIME_HOURS || '4', 10) * 60 * 60 * 1000
  },
  rateLimit: {
    requestsPerSecond: 2,
    burstSize: 40
  },
  runtime: {
    runStartupSync: (process.env.RUN_STARTUP_SYNC || 'true').toLowerCase() !== 'false'
  }
};

const requiredEnv = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID'];
const missingEnv = requiredEnv.filter(k => !process.env[k]);
if (missingEnv.length) {
  console.warn(`WARNING: Missing env vars: ${missingEnv.join(', ')}. UI loads, but sync will fail.`);
}

console.log(`Using Shopify Location ID: ${config.shopify.locationIdNumber}`);
console.log(`Shopify domain (sanitized): ${config.shopify.domain ? config.shopify.domain.replace(/^[^.]+/, 'xxxxx') : 'not-set'}`);
console.log(`BTC Activewear FTP Host: ${config.ftp.host}`);
console.log(`BTC FTP User: ${config.ftp.user}`);
console.log(`BTC FTP Password: ${config.ftp.password ? '***SET***' : 'NOT SET'}`);

// ============================================
// STATE & HELPERS
// ============================================

let logs = [];
let isRunning = { inventory: false };
let failsafe = { isTriggered: false, reason: '', timestamp: null };
let isSystemPaused = false;

const DATA_DIR = process.env.DATA_DIR || __dirname;
const PAUSE_LOCK_FILE = path.join(DATA_DIR, '_paused.lock');
const HISTORY_FILE = path.join(DATA_DIR, '_history.json');
const UPLOADED_FILES_DIR = path.join(DATA_DIR, 'uploaded_catalogs');

[UPLOADED_FILES_DIR, path.join(DATA_DIR, 'uploads')].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

class RateLimiter {
  constructor(rps = 2, bs = 40) {
    this.rps = rps; this.bs = bs; this.tokens = bs;
    this.lastRefill = Date.now(); this.queue = []; this.processing = false;
  }
  async acquire() {
    return new Promise(r => { this.queue.push(r); this.process(); });
  }
  async process() {
    if (this.processing) return;
    this.processing = true;
    while (this.queue.length > 0) {
      const n = Date.now(); const p = (n - this.lastRefill) / 1000;
      this.tokens = Math.min(this.bs, this.tokens + p * this.rps);
      this.lastRefill = n;
      if (this.tokens >= 1) { this.tokens--; this.queue.shift()(); }
      else { await new Promise(r => setTimeout(r, (1 / this.rps) * 1000)); }
    }
    this.processing = false;
  }
}
const rateLimiter = new RateLimiter(config.rateLimit.requestsPerSecond, config.rateLimit.burstSize);

function addLog(message, type = 'info') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log); if (logs.length > 500) logs.pop();
  console.log(`[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}`);
}

async function notifyTelegram(message) {
  if (!config.telegram.botToken || !config.telegram.chatId) return;
  try {
    if (message.length > 4096) message = message.substring(0, 4086) + '...';
    await axios.post(`https://api.telegram.org/bot${config.telegram.botToken}/sendMessage`, {
      chat_id: config.telegram.chatId,
      text: `🏪 BTC Activewear Sync\n${message}`,
      parse_mode: 'HTML'
    });
  } catch (e) { addLog(`Telegram failed: ${e.message}`, 'warning'); }
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

function triggerFailsafe(reason) {
  if (failsafe.isTriggered) return;
  failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString() };
  const msg = `🚨 FAILSAFE ACTIVATED: ${reason}`;
  addLog(msg, 'error'); notifyTelegram(msg);
  Object.keys(isRunning).forEach(k => isRunning[k] = false);
}

const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: { 'X-Shopify-Access-Token': config.shopify.accessToken },
  timeout: 60000
});

let runHistory = [];
function loadHistory() { try { if (fs.existsSync(HISTORY_FILE)) runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); } catch (e) { addLog(`Could not load history: ${e.message}`, 'warning'); } }
function saveHistory() { try { if (runHistory.length > 100) runHistory.pop(); fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2)); } catch (e) { addLog(`Could not save history: ${e.message}`, 'warning'); } }
function addToHistory(runData) { runHistory.unshift(runData); saveHistory(); }
function checkPauseStateOnStartup() { if (fs.existsSync(PAUSE_LOCK_FILE)) isSystemPaused = true; loadHistory(); }

async function shopifyRequestWithRetry(method, url, data = null, retries = 5) {
  let lastError;
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      await rateLimiter.acquire();
      switch (method.toLowerCase()) {
        case 'get': return await shopifyClient.get(url);
        case 'post': return await shopifyClient.post(url, data);
        case 'put': return await shopifyClient.put(url, data);
        case 'delete': return await shopifyClient.delete(url);
      }
    } catch (error) {
      lastError = error;
      if (error.response?.status === 429) {
        const retryAfter = (parseInt(error.response.headers['retry-after'] || 2) * 1000);
        await delay(retryAfter + 500);
      } else if (error.response?.status >= 500) {
        await delay(1000 * Math.pow(2, attempt));
      } else {
        throw error;
      }
    }
  }
  throw lastError;
}

async function shopifyGraphQLRequest(query, variables) {
  try {
    await rateLimiter.acquire();
    const response = await axios.post(config.shopify.graphqlUrl, { query, variables }, {
      headers: { 'X-Shopify-Access-Token': config.shopify.accessToken }
    });
    if (response.data.errors) throw new Error(JSON.stringify(response.data.errors));
    return response.data;
  } catch (error) {
    addLog(`GraphQL error: ${error.message}`, 'error');
    throw error;
  }
}

// Utils
const normalizeSku = s => (s || '').toString().trim().toUpperCase();
const unique = arr => Array.from(new Set(arr.filter(Boolean)));
const sample = (arr, n = 10) => arr.slice(0, n);
const gidToId = gid => String(gid || '').split('/').pop();

// ============================================
// FTP + CSV
// ============================================

function detectDelimiterFromBuffer(buf) {
  const text = buf.toString('utf8', 0, Math.min(buf.length, 200000));
  const headerLine = (text.split(/\r?\n/)[0] || '').replace(/^\uFEFF/, '');
  const counts = {
    comma: (headerLine.match(/,/g) || []).length,
    tab: (headerLine.match(/\t/g) || []).length,
    semicolon: (headerLine.match(/;/g) || []).length,
    pipe: (headerLine.match(/\|/g) || []).length
  };
  let delim = ',', label = 'comma';
  if (counts.tab >= counts.comma && counts.tab >= counts.semicolon && counts.tab >= counts.pipe) { delim = '\t'; label = 'tab'; }
  else if (counts.semicolon >= counts.comma && counts.semicolon >= counts.tab && counts.semicolon >= counts.pipe) { delim = ';'; label = 'semicolon'; }
  else if (counts.pipe >= counts.comma && counts.pipe >= counts.tab && counts.pipe >= counts.semicolon) { delim = '|'; label = 'pipe'; }
  return { delimiter: delim, label, headerPreview: headerLine, firstLines: text.split(/\r?\n/).slice(0, 5).join('\n') };
}

async function fetchInventoryFileBuffer() {
  const client = new ftp.Client();
  try {
    addLog(`Connecting to FTP: ${config.ftp.host} as ${config.ftp.user}...`, 'info');
    await client.access(config.ftp);
    addLog(`FTP connected successfully. Downloading ${config.btc.stockFilePath}...`, 'info');
    const chunks = [];
    await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), config.btc.stockFilePath);
    addLog('FTP download completed successfully.', 'success');
    return Buffer.concat(chunks);
  } catch (e) {
    addLog(`FTP error: ${e.message}`, 'error');
    throw e;
  } finally { client.close(); }
}

async function parseInventoryCSV(buffer) {
  // delimiter
  let delimiter = ',', detected = { delimiter: ',', label: 'comma', headerPreview: '', firstLines: '' };
  if (config.btc.csvSeparator === 'auto') {
    detected = detectDelimiterFromBuffer(buffer);
    delimiter = detected.delimiter;
    addLog(`[CSV] Detected delimiter: ${detected.label}. Header: "${detected.headerPreview}"`, 'info');
  } else {
    delimiter = config.btc.csvSeparator === '\\t' ? '\t' : config.btc.csvSeparator;
    addLog(`[CSV] Using configured delimiter: "${config.btc.csvSeparator}"`, 'info');
  }

  return new Promise((resolve, reject) => {
    const inventory = new Map();
    const headersRaw = new Set();
    const normalizeHeader = h => String(h || '').replace(/^\uFEFF/, '').replace(/^["']|["']$/g, '').trim().toLowerCase();

    Readable.from(buffer)
      .pipe(csv({
        separator: delimiter,
        mapHeaders: ({ header }) => { const h = normalizeHeader(header); headersRaw.add(h); return h; }
      }))
      .on('data', row => {
        const stockIdRaw =
          row['stock_id'] ?? row['stockid'] ?? row['stock id'] ??
          row['sku'] ?? row['code'] ?? row['product_code'] ?? row['product code'];
        const qtyRaw =
          row['free_stock'] ?? row['free stock'] ??
          row['remaining_stock*'] ?? row['remaining_stock'] ?? row['remaining stock'] ??
          row['qty'] ?? row['quantity'] ?? row['available'] ?? row['stock'];

        const stockId = normalizeSku(stockIdRaw);
        const qty = Math.min(parseInt(qtyRaw, 10) || 0, config.btc.maxInventory);
        if (stockId && !Number.isNaN(qty)) {
          inventory.set(stockId, qty);
        }
      })
      .on('end', () => {
        if (inventory.size === 0) {
          addLog(`Parsed 0 Stock IDs from BTC CSV`, 'warning');
          addLog(`[CSV] Headers seen: ${Array.from(headersRaw).join(' | ')}`, 'warning');
          addLog(`[CSV] First lines:\n${detected.firstLines || buffer.toString('utf8', 0, Math.min(buffer.length, 1000))}`, 'warning');
        } else {
          addLog(`Parsed ${inventory.size} Stock IDs from BTC CSV`, 'info');
        }
        resolve(inventory);
      })
      .on('error', reject);
  });
}

// ============================================
// SHOPIFY: LOCATIONS & INVENTORY
// ============================================

async function fetchLocations() {
  const res = await shopifyRequestWithRetry('get', `/locations.json`);
  return res.data?.locations || [];
}

async function validateLocationOrThrow() {
  const locations = await fetchLocations();
  const idNum = Number(config.shopify.locationIdNumber);
  const found = locations.find(l => Number(l.id) === idNum);
  if (!found) {
    addLog(`[LOC] Provided SHOPIFY_LOCATION_ID=${config.shopify.locationIdNumber} not found. Available locations:`, 'error');
    locations.forEach(l => addLog(` - ${l.name} (id=${l.id})`, 'error'));
    throw new Error(`Invalid SHOPIFY_LOCATION_ID (${config.shopify.locationIdNumber}). See /api/debug/locations for valid IDs.`);
  }
  addLog(`[LOC] Using location: ${found.name} (id=${found.id})`, 'info');
  return found;
}

// GraphQL: find inventoryItems by SKU (read_inventory)
async function graphqlFindInventoryItemsBySkus(skus) {
  const CHUNK = 40;
  const results = new Map(); // skuUpper -> { gid, tracked }
  for (let i = 0; i < skus.length; i += CHUNK) {
    const chunk = skus.slice(i, i + CHUNK).map(s => s.replace(/"/g, '\\"'));
    const queryStr = chunk.map(s => `sku:"${s}"`).join(' OR ');
    const query = `
      query invItems($q: String!, $after: String) {
        inventoryItems(first: 250, query: $q, after: $after) {
          edges { cursor node { id sku tracked } }
          pageInfo { hasNextPage }
        }
      }`;
    let after = null;
    do {
      const data = await shopifyGraphQLRequest(query, { q: queryStr, after });
      const edges = data?.data?.inventoryItems?.edges || [];
      for (const e of edges) {
        const node = e.node;
        const skuUpper = normalizeSku(node.sku);
        results.set(skuUpper, { gid: node.id, tracked: !!node.tracked });
      }
      const pageInfo = data?.data?.inventoryItems?.pageInfo;
      after = pageInfo?.hasNextPage ? edges[edges.length - 1]?.cursor : null;
    } while (after);
  }
  return results;
}

// REST: current availability at location
async function getAvailableAtLocationMap(inventoryItemIds, locationIdNumber) {
  const chunkSize = 50;
  const result = new Map();
  const ids = unique(inventoryItemIds.map(id => String(id)));
  if (!ids.length) return result;

  addLog(`Fetching inventory levels for ${ids.length} items at location ${locationIdNumber}...`, 'info');

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const res = await shopifyRequestWithRetry('get', `/inventory_levels.json?inventory_item_ids=${chunk.join(',')}&location_ids=${locationIdNumber}`);
    const levels = res.data?.inventory_levels || [];
    levels.forEach(lvl => result.set(String(lvl.inventory_item_id), lvl.available ?? 0));
    addLog(`   ↳ Retrieved ${levels.length} levels (chunk ${i / chunkSize + 1}/${Math.ceil(ids.length / chunkSize)})`, 'info');
  }
  return result;
}

async function setInventoryItemsTrackedBulk(inventoryItemIds, tracked = true) {
  let updated = 0;
  for (const id of inventoryItemIds) {
    try {
      await shopifyRequestWithRetry('put', `/inventory_items/${id}.json`, { inventory_item: { id, tracked } });
      updated++;
    } catch (e) {
      addLog(`   [tracked] failed to set tracked for ${id}: ${e.message}`, 'warning');
    }
  }
  return updated;
}

async function connectInventoryLevelsBulk(inventoryItemIds, locationIdNumber) {
  let connected = 0;
  for (const id of inventoryItemIds) {
    try {
      await shopifyRequestWithRetry('post', `/inventory_levels/connect.json`, {
        location_id: Number(locationIdNumber),
        inventory_item_id: Number(id)
      });
      connected++;
    } catch (e) {
      if (e.response?.status !== 422) {
        addLog(`   [connect] failed to connect ${id} to location ${locationIdNumber}: ${e.message}`, 'warning');
      }
    }
  }
  return connected;
}

async function ensureItemsForSkus(skuSet) {
  const skus = unique(Array.from(skuSet));
  addLog(`[MAP] Resolving ${skus.length} SKUs to inventory items...`, 'info');

  const itemMap = await graphqlFindInventoryItemsBySkus(skus); // skuUpper -> { gid, tracked }
  const foundSkus = Array.from(itemMap.keys());
  const missingSkus = skus.filter(s => !itemMap.has(s));
  addLog(`[MAP] Found ${foundSkus.length}/${skus.length} SKUs in Shopify inventoryItems.`, foundSkus.length ? 'info' : 'warning');
  if (missingSkus.length) addLog(`[MAP] Sample missing SKUs: ${sample(missingSkus, 10).join(', ')}`, 'warning');

  // enable tracked if needed
  const needTrackedIds = [];
  for (const [sku, info] of itemMap.entries()) {
    if (!info.tracked) needTrackedIds.push(gidToId(info.gid));
  }
  if (needTrackedIds.length) {
    addLog(`[FIX] Enabling tracking for ${needTrackedIds.length} items...`, 'warning');
    const updated = await setInventoryItemsTrackedBulk(needTrackedIds, true);
    addLog(`[FIX] Tracking enabled for ${updated}/${needTrackedIds.length}`, updated === needTrackedIds.length ? 'success' : 'warning');
  }

  return itemMap; // skuUpper -> { gid, tracked }
}

// ============================================
// INVENTORY ADJUSTMENT (GraphQL + userErrors)
// ============================================

async function sendInventoryUpdatesInBatches(adjustments) {
  // IMPORTANT: name must be 'available' or Shopify will reject with ledger-doc error
  const INPUT_NAME = 'available';
  const INPUT_REASON = 'correction';

  const BATCH_SIZE = 250;
  if (!adjustments || adjustments.length === 0) return { attempted: 0, applied: 0, failed: 0, errorSamples: [] };

  const totalBatches = Math.ceil(adjustments.length / BATCH_SIZE);
  addLog(`Found ${adjustments.length} inventory changes. Sending in ${totalBatches} batches...`, 'info');

  let attempted = 0, applied = 0, failed = 0;
  const errorMsgCounts = new Map();
  const errorSamples = [];

  for (let i = 0; i < adjustments.length; i += BATCH_SIZE) {
    const batch = adjustments.slice(i, i + BATCH_SIZE);
    const currentBatchNum = Math.floor(i / BATCH_SIZE) + 1;
    attempted += batch.length;

    addLog(`   Processing batch ${currentBatchNum} of ${totalBatches}... (${batch.length} items)`, 'info');

    const mutation = `
      mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
        inventoryAdjustQuantities(input: $input) {
          userErrors { field message }
        }
      }`;

    try {
      const resp = await shopifyGraphQLRequest(mutation, {
        input: { name: INPUT_NAME, reason: INPUT_REASON, changes: batch }
      });

      const ues = resp?.data?.inventoryAdjustQuantities?.userErrors || [];
      if (ues.length) {
        const failedIdxs = new Set();
        ues.forEach(e => {
          const msg = e.message || 'Unknown error';
          errorMsgCounts.set(msg, (errorMsgCounts.get(msg) || 0) + 1);
          if (errorSamples.length < 10) errorSamples.push(msg);
          const idx = (e.field || []).map(x => Number(x)).find(Number.isInteger);
          if (Number.isInteger(idx)) failedIdxs.add(idx);
        });
        const failedThis = failedIdxs.size || ues.length;
        failed += failedThis;
        const appliedThis = Math.max(0, batch.length - failedThis);
        applied += appliedThis;
        addLog(`   ⚠️ Batch ${currentBatchNum}: userErrors=${ues.length}, applied≈${appliedThis}, failed≈${failedThis}`, 'warning');
      } else {
        applied += batch.length;
        addLog(`   ✅ Batch ${currentBatchNum} processed successfully.`, 'success');
      }
    } catch (error) {
      failed += batch.length;
      addLog(`   ❌ Batch ${currentBatchNum} failed entirely: ${error.message}`, 'error');
    }
  }

  if (errorMsgCounts.size) {
    const top = [...errorMsgCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 5).map(([m, c]) => `${m} (${c})`);
    addLog(`⚠️ Shopify userErrors summary: ${top.join(' | ')}`, 'warning');
  }

  addLog(`Apply summary: attempted=${attempted}, applied≈${applied}, failed≈${failed}`, failed > 0 ? 'warning' : 'success');
  return { attempted, applied, failed, errorSamples };
}

// ============================================
// BUSINESS LOGIC
// ============================================

const isSystemLocked = () => Object.values(isRunning).some(v => v) || isSystemPaused || failsafe.isTriggered;

async function syncInventory() {
  if (isSystemLocked()) { addLog('Sync skipped: System is locked.', 'warning'); return; }
  if (missingEnv.length) { addLog(`Cannot run sync: missing env vars: ${missingEnv.join(', ')}`, 'error'); return; }

  isRunning.inventory = true;
  addLog('🚀 Starting BTC Activewear inventory sync process...', 'info');
  let runResult = { type: 'Inventory Sync', status: 'failed', applied: 0, errors: 0 };

  try {
    // 0) Validate location (prevents "location not found" for every change)
    const loc = await validateLocationOrThrow();

    // 1) FTP fetch + parse
    const fileBuffer = await fetchInventoryFileBuffer();
    const ftpInventory = await parseInventoryCSV(fileBuffer); // Map SKU -> qty
    if (ftpInventory.size === 0) throw new Error('Parsed 0 SKUs from BTC file. Check [CSV] logs.');
    addLog(`Successfully fetched and parsed ${ftpInventory.size} Stock IDs from BTC FTP.`, 'success');

    // 2) Resolve inventory items by SKU and ensure tracked
    const ftpSkus = Array.from(ftpInventory.keys());
    const itemMap = await ensureItemsForSkus(ftpSkus); // skuUpper -> { gid, tracked }

    const matchedSkus = Array.from(itemMap.keys());
    const unmatchedSkus = ftpSkus.filter(s => !itemMap.has(s));
    addLog(`[MAP] Matched SKUs: ${matchedSkus.length}, Unmatched SKUs: ${unmatchedSkus.length}`, 'info');
    if (unmatchedSkus.length) addLog(`[MAP] Sample unmatched SKUs: ${sample(unmatchedSkus, 10).join(', ')}`, 'warning');

    // 3) Fetch current availability at our location
    const invItemIdsNum = Array.from(itemMap.values()).map(v => gidToId(v.gid));
    let availableMap = await getAvailableAtLocationMap(invItemIdsNum, config.shopify.locationIdNumber);

    // If we fetched 0 levels but have many items, try to connect then refetch
    if (availableMap.size === 0 && invItemIdsNum.length > 0) {
      addLog(`[FIX] No levels found at location ${config.shopify.locationIdNumber}. Connecting items to the location...`, 'warning');
      const connected = await connectInventoryLevelsBulk(invItemIdsNum, config.shopify.locationIdNumber);
      addLog(`[FIX] Connected ${connected}/${invItemIdsNum.length} items to location ${config.shopify.locationIdNumber}.`, 'warning');
      availableMap = await getAvailableAtLocationMap(invItemIdsNum, config.shopify.locationIdNumber);
    }

    // 4) Build adjustments at location
    const adjustments = [];
    let sameCount = 0;
    const diffSamples = [];
    for (const sku of matchedSkus) {
      const invGid = itemMap.get(sku).gid;
      const invIdNum = gidToId(invGid);
      const currentAvail = availableMap.get(invIdNum) ?? 0;
      const ftpQty = ftpInventory.get(sku) ?? 0;
      if (ftpQty !== currentAvail) {
        const delta = ftpQty - currentAvail;
        adjustments.push({ inventoryItemId: invGid, locationId: config.shopify.locationId, delta });
        if (diffSamples.length < 15) diffSamples.push(`${sku}: FTP=${ftpQty}, Loc=${currentAvail}, Δ=${delta}`);
      } else {
        sameCount++;
      }
    }

    addLog(`[COMPARE] Same=${sameCount}, Different=${adjustments.length}`, 'info');
    if (diffSamples.length) addLog(`[COMPARE] Example differences: ${diffSamples.join(' | ')}`, 'info');

    // 5) Send updates (name='available' to avoid ledger errors)
    const result = await sendInventoryUpdatesInBatches(adjustments);
    runResult.applied = result.applied;

    // 6) Done
    runResult.status = 'completed';
    const duration = ((Date.now() - Date.parse(runHistory[0]?.timestamp || new Date())) / 1000).toFixed(2);
    addLog(`BTC Activewear sync ${runResult.status}:
🔄 Applied ≈${runResult.applied} changes
📊 ${matchedSkus.length}/${ftpSkus.length} SKUs matched
⏱️ Completed at ${new Date().toLocaleString()}`, 'success');
    notifyTelegram(`Applied ≈${runResult.applied} changes • Matched ${matchedSkus.length}/${ftpSkus.length} SKUs`);

  } catch (error) {
    runResult.errors++;
    runResult.status = 'failed';
    triggerFailsafe(error.message || 'Inventory sync failed');
  } finally {
    isRunning.inventory = false;
    addToHistory({ ...runResult, timestamp: new Date().toISOString() });
  }
}

// ============================================
// DEBUG ENDPOINTS
// ============================================
app.get('/api/debug/locations', async (req, res) => {
  try {
    const locations = await fetchLocations();
    res.json({
      ok: true,
      currentEnvLocationId: config.shopify.locationIdNumber,
      locations: locations.map(l => ({
        id: l.id,
        name: l.name,
        active: l.active,
        legacy: l.legacy
      }))
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/api/debug/shopify', async (req, res) => {
  try {
    const r = await shopifyRequestWithRetry('get', `/shop.json`);
    res.json({ ok: true, shop: r.data?.shop || null, baseUrl: config.shopify.baseUrl });
  } catch (e) {
    res.status(500).json({
      ok: false,
      error: e.message,
      hint: 'Ensure SHOPIFY_DOMAIN is yourshop.myshopify.com and token has read_inventory + write_inventory scopes.',
      baseUrl: config.shopify.baseUrl
    });
  }
});

app.get('/api/debug/ftp', async (req, res) => {
  try {
    const buf = await fetchInventoryFileBuffer();
    const det = detectDelimiterFromBuffer(buf);
    res.json({
      ok: true,
      host: config.ftp.host,
      user: config.ftp.user,
      detectedDelimiter: det.label,
      headerPreview: det.headerPreview,
      firstLines: det.firstLines
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message, host: config.ftp.host, user: config.ftp.user });
  }
});

// ============================================
// API
// ============================================
app.post('/api/sync/inventory', requireApiKey, (req, res) => { syncInventory(); res.json({ success: true }); });

app.post('/api/pause/toggle', requireApiKey, (req, res) => {
  isSystemPaused = !isSystemPaused;
  if (isSystemPaused) fs.writeFileSync(PAUSE_LOCK_FILE, 'paused');
  else try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch (e) {}
  addLog(`System ${isSystemPaused ? 'PAUSED' : 'RESUMED'}.`, 'warning');
  res.json({ success: true });
});

app.post('/api/failsafe/clear', requireApiKey, (req, res) => {
  failsafe = { isTriggered: false, reason: '', timestamp: null };
  addLog('Failsafe cleared.', 'warning');
  res.json({ success: true });
});

// ============================================
// WEB UI
// ============================================
app.get('/', (req, res) => {
  const lastInventorySync = runHistory.find(r => r.type === 'Inventory Sync');
  const html = `<!DOCTYPE html><html lang="en"><head><title>BTC Activewear Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><style>
  body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;line-height:1.5;}
  .container{max-width:1400px;margin:auto;padding:1rem;}
  .card{background:#161b22;border:1px solid #30363d;padding:1.5rem;border-radius:6px;margin-bottom:1rem;}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;}
  .btn{padding:0.5rem 1rem;border:1px solid #30363d;border-radius:6px;cursor:pointer;background:#21262d;color:#c9d1d9;font-weight:600;}
  .btn-primary{background:#238636;color:white;border-color:#2ea043;}
  .btn:disabled{opacity:0.5;cursor:not-allowed;}
  .logs{background:#010409;padding:1rem;height:300px;overflow-y:auto;border-radius:6px;font-family:monospace;white-space:pre-wrap;font-size:0.875em;}
  .stat-card{text-align:center;}
  .stat-value{font-size:2rem;font-weight:600;}
  .stat-label{font-size:0.8rem;color:#8b949e;}
  .location-info{font-size:0.875em;color:#8b949e;margin-top:0.5rem;}
  .supplier-info{background:rgba(56,139,253,0.1);border:1px solid #388bfd;padding:0.5rem;border-radius:4px;margin-top:0.5rem;font-size:0.875em;}
  .debug{display:flex;gap:.5rem;margin-top:.5rem;flex-wrap:wrap}
  </style></head><body><div class="container"><h1>BTC Activewear Sync</h1>
  <div class="grid">
    <div class="card">
      <h2>System</h2>
      <p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': Object.values(isRunning).some(v => v) ? 'Busy' : 'Active'}</p>
      <button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>
      ${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}
      <div class="location-info">Location ID: ${config.shopify.locationIdNumber}</div>
      <div class="supplier-info">FTP Host: ${config.ftp.host}</div>
      <div class="debug">
        <a class="btn" href="/api/debug/ftp" target="_blank">Test FTP</a>
        <a class="btn" href="/api/debug/shopify" target="_blank">Test Shopify</a>
        <a class="btn" href="/api/debug/locations" target="_blank">List Locations</a>
      </div>
    </div>
    <div class="card">
      <h2>Inventory Sync</h2>
      <p>Status: ${isRunning.inventory?'Running':'Ready'}</p>
      <p>Updates inventory by SKU at your location using read_inventory/write_inventory.</p>
      <button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${Object.values(isRunning).some(v => v)||isSystemPaused||failsafe.isTriggered?'disabled':''}>Run Now</button>
    </div>
  </div>
  <div class="card">
    <h2>Last Inventory Sync</h2>
    <div class="grid" style="grid-template-columns:1fr 1fr;">
      <div class="stat-card"><div class="stat-value">${lastInventorySync?.applied ?? 'N/A'}</div><div class="stat-label">Changes Applied (≈)</div></div>
      <div class="stat-card"><div class="stat-value">${lastInventorySync?.discontinued ?? 'N/A'}</div><div class="stat-label">Products Discontinued</div></div>
    </div>
  </div>
  <div class="card"><h2>Logs</h2><div class="logs">${logs.map(log=>`<div class="log-entry log-${log.type}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div>
  </div><script>
  async function apiPost(url,confirmMsg){
    if(confirmMsg&&!confirm(confirmMsg))return;
    try{
      const btn=event?.target;if(btn)btn.disabled=true;
      const headers = {};
      ${API_KEY ? `headers['x-api-key'] = '${API_KEY}';` : ''}
      await fetch(url,{method:'POST',headers});
      setTimeout(()=>location.reload(),700);
    }catch(e){ alert('Error: '+e.message); if(btn)btn.disabled=false; }
  }
  </script></body></html>`;
  res.send(html);
});

// ============================================
// SCHEDULED TASKS & STARTUP
// ============================================
cron.schedule('0 2 * * *', () => syncInventory()); // Daily 2 AM (set TZ if needed)

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  checkPauseStateOnStartup();
  addLog(`✅ BTC Activewear Sync Server started on port ${PORT} (Location: ${config.shopify.locationIdNumber})`, 'success');
  console.log(`Server is listening on 0.0.0.0:${PORT}`);
  if (config.runtime.runStartupSync) setTimeout(() => { if (!isSystemLocked()) syncInventory(); }, 5000);
});

function shutdown(signal) { addLog(`Received ${signal}, shutting down...`, 'info'); saveHistory(); process.exit(0); }
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
