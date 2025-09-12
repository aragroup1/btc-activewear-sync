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

// Request logging (helps during debugging)
app.use((req, res, next) => {
  console.log(`[REQ] ${req.method} ${req.url}`);
  next();
});

// Health check
app.get('/health', (req, res) => res.status(200).send('ok'));

// Optional API key for POST endpoints
const API_KEY = process.env.API_KEY || '';
function requireApiKey(req, res, next) {
  if (!API_KEY) return next();
  if (req.headers['x-api-key'] === API_KEY) return next();
  return res.status(401).json({ error: 'Unauthorized' });
}

// File upload (kept for future)
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
    locationId: `gid://shopify/Location/${process.env.SHOPIFY_LOCATION_ID}`,
    locationIdNumber: process.env.SHOPIFY_LOCATION_ID,
    baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-04`,
    graphqlUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-04/graphql.json`
  },
  // Prefer BTC_* to avoid accidentally using old FTP_* vars
  ftp: {
    host: process.env.BTC_FTP_HOST || 'ftpdata.btcactivewear.co.uk',
    user: process.env.BTC_FTP_USERNAME || 'ara0010',
    password: process.env.BTC_FTP_PASSWORD || '',
    secure: false
  },
  btcactivewear: {
    supplierTag: 'Source_BTC Activewear',
    stockFilePath: '/webdata/stock_levels_stock_id.csv',
    maxInventory: parseInt(process.env.BTC_MAX_INVENTORY || process.env.MAX_INVENTORY || '9999', 10),
    csvSeparator: process.env.BTC_CSV_SEPARATOR || ',' // set '\t' if TSV
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

const requiredShopify = ['SHOPIFY_DOMAIN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_LOCATION_ID'];
const missingShopify = requiredShopify.filter(k => !process.env[k]);
if (missingShopify.length) {
  console.warn(`WARNING: Missing env vars: ${missingShopify.join(', ')}. UI will load, but sync will fail until these are set.`);
}

console.log(`Using Shopify Location ID: ${config.shopify.locationIdNumber}`);
console.log(`BTC Activewear FTP Host: ${config.ftp.host}`);
console.log(`[FTP] Using username: ${config.ftp.user}`);

// ============================================
// STATE MANAGEMENT & HELPERS
// ============================================

let logs = [];
let isRunning = { inventory: false, fullImport: false };
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
    this.rps = rps;
    this.bs = bs;
    this.tokens = bs;
    this.lastRefill = Date.now();
    this.queue = [];
    this.processing = false;
  }
  async acquire() {
    return new Promise(r => { this.queue.push(r); this.process(); });
  }
  async process() {
    if (this.processing) return;
    this.processing = true;
    while (this.queue.length > 0) {
      const n = Date.now();
      const p = (n - this.lastRefill) / 1000;
      this.tokens = Math.min(this.bs, this.tokens + p * this.rps);
      this.lastRefill = n;
      if (this.tokens >= 1) {
        this.tokens--;
        const r = this.queue.shift();
        r();
      } else {
        const w = (1 / this.rps) * 1000;
        await new Promise(r => setTimeout(r, w));
      }
    }
    this.processing = false;
  }
}
const rateLimiter = new RateLimiter(config.rateLimit.requestsPerSecond, config.rateLimit.burstSize);

function addLog(message, type = 'info') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 500) logs.pop();
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
  } catch (error) {
    addLog(`Telegram failed: ${error.message}`, 'warning');
  }
}

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function triggerFailsafe(reason) {
  if (failsafe.isTriggered) return;
  failsafe = { isTriggered: true, reason, timestamp: new Date().toISOString() };
  const msg = `🚨 FAILSAFE ACTIVATED: ${reason}`;
  addLog(msg, 'error');
  notifyTelegram(msg);
  Object.keys(isRunning).forEach(key => isRunning[key] = false);
}

const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: { 'X-Shopify-Access-Token': config.shopify.accessToken },
  timeout: 60000
});

let runHistory = [];
function loadHistory() {
  try {
    if (fs.existsSync(HISTORY_FILE)) {
      runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
    }
  } catch (e) {
    addLog(`Could not load history: ${e.message}`, 'warning');
  }
}
function saveHistory() {
  try {
    if (runHistory.length > 100) runHistory.pop();
    fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2));
  } catch (e) {
    addLog(`Could not save history: ${e.message}`, 'warning');
  }
}
function addToHistory(runData) {
  runHistory.unshift(runData);
  saveHistory();
}
function checkPauseStateOnStartup() {
  if (fs.existsSync(PAUSE_LOCK_FILE)) {
    isSystemPaused = true;
  }
  loadHistory();
}

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
    if (response.data.errors) {
      throw new Error(JSON.stringify(response.data.errors));
    }
    return response.data; // GraphQL payload
  } catch (error) {
    addLog(`GraphQL error: ${error.message}`, 'error');
    throw error;
  }
}

// Helpers
const normalizeSku = s => (s || '').toString().trim().toUpperCase();
const productHasTag = (product, tag) => {
  const tagStr = (product?.tags || '').toString();
  return new Set(tagStr.split(',').map(t => t.trim())).has(tag);
};
const unique = arr => Array.from(new Set(arr.filter(Boolean)));
const sample = (arr, n = 10) => arr.slice(0, n);

// ============================================
// CORE LOGIC - BTC ACTIVEWEAR
// ============================================

async function fetchInventoryFromFTP() {
  const client = new ftp.Client();
  try {
    if (!config.ftp.host || !config.ftp.user || !config.ftp.password) {
      throw new Error('FTP credentials missing: ensure BTC_FTP_HOST, BTC_FTP_USERNAME, BTC_FTP_PASSWORD are set.');
    }
    await client.access(config.ftp);
    const chunks = [];
    await client.downloadTo(new Writable({
      write(c, e, cb) { chunks.push(c); cb(); }
    }), config.btcactivewear.stockFilePath);
    return Readable.from(Buffer.concat(chunks));
  } catch (e) {
    addLog(`FTP error: ${e.message}`, 'error');
    throw e;
  } finally {
    client.close();
  }
}

// Debug-only: quick FTP test (login + file size)
async function testFtpConnection() {
  const client = new ftp.Client();
  try {
    await client.access(config.ftp);
    let size = null;
    try {
      size = await client.size(config.btcactivewear.stockFilePath);
    } catch {
      // No SIZE support or path issue; list directory
      const dir = path.posix.dirname(config.btcactivewear.stockFilePath);
      const list = await client.list(dir);
      return { connected: true, fileSize: null, dir, dirListing: list.slice(0, 20) };
    }
    return { connected: true, fileSize: size };
  } catch (e) {
    return { connected: false, error: e.message, host: config.ftp.host, user: config.ftp.user };
  } finally {
    client.close();
  }
}

// Robust parser for BTC CSV
async function parseInventoryCSV(stream) {
  return new Promise((resolve, reject) => {
    const inventory = new Map();
    stream
      .pipe(csv({
        separator: config.btcactivewear.csvSeparator,
        mapHeaders: ({ header }) => String(header || '')
          .replace(/^\uFEFF/, '') // strip BOM
          .trim()
      }))
      .on('data', row => {
        const stockIdRaw = row['Stock ID'] ?? row['StockID'] ?? row['Stock_Id'] ?? row['Stock_ID'];
        const qtyRaw = row['Remaining_Stock*'] ?? row['Remaining_Stock'] ?? row['Remaining Stock'] ?? row['Qty'] ?? row['Quantity'];
        const stockId = normalizeSku(stockIdRaw);
        const qty = Math.min(parseInt(qtyRaw, 10) || 0, config.btcactivewear.maxInventory);
        if (stockId) inventory.set(stockId, qty);
      })
      .on('end', () => {
        addLog(`Parsed ${inventory.size} Stock IDs from BTC Activewear CSV`, 'info');
        resolve(inventory);
      })
      .on('error', reject);
  });
}

async function getAllShopifyProducts() {
  let allProducts = [];
  let url = `/products.json?limit=250`;
  addLog('Fetching all Shopify products...', 'info');
  while (url) {
    try {
      const res = await shopifyRequestWithRetry('get', url);
      allProducts.push(...res.data.products);
      const linkHeader = res.headers.link;
      const nextLinkMatch = linkHeader ? linkHeader.match(/<([^>]+)>;\s*rel="next"/) : null;
      if (nextLinkMatch) {
        const nextUrl = new URL(nextLinkMatch[1]);
        url = nextUrl.pathname + nextUrl.search;
      } else {
        url = null;
      }
    } catch (error) {
      addLog(`Error fetching products: ${error.message}`, 'error');
      triggerFailsafe(`Failed to fetch products from Shopify`);
      return [];
    }
  }
  addLog(`Fetched ${allProducts.length} products.`, 'success');
  return allProducts;
}

// Per-location availability
async function getAvailableAtLocationMap(inventoryItemIds, locationIdNumber) {
  const chunkSize = 50;
  const result = new Map();
  const ids = unique(inventoryItemIds);
  addLog(`Fetching inventory levels for ${ids.length} inventory items at location ${locationIdNumber}...`, 'info');

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const qs = `inventory_item_ids=${chunk.join(',')}&location_ids=${locationIdNumber}`;
    try {
      const res = await shopifyRequestWithRetry('get', `/inventory_levels.json?${qs}`);
      const levels = res.data.inventory_levels || [];
      levels.forEach(lvl => {
        result.set(String(lvl.inventory_item_id), lvl.available ?? 0);
      });
      addLog(`   ↳ Retrieved ${levels.length} levels (chunk ${i / chunkSize + 1}/${Math.ceil(ids.length / chunkSize)})`, 'info');
    } catch (err) {
      addLog(`Error fetching inventory levels: ${err.message}`, 'error');
      throw err;
    }
  }

  return result;
}

async function sendInventoryUpdatesInBatches(adjustments, reason) {
  const BATCH_SIZE = 250;
  if (!adjustments || adjustments.length === 0) return;

  const totalBatches = Math.ceil(adjustments.length / BATCH_SIZE);
  addLog(`Found ${adjustments.length} inventory changes. Sending in ${totalBatches} batches of up to ${BATCH_SIZE}...`, 'info');

  for (let i = 0; i < adjustments.length; i += BATCH_SIZE) {
    const batch = adjustments.slice(i, i + BATCH_SIZE);
    const currentBatchNum = Math.floor(i / BATCH_SIZE) + 1;

    addLog(`   Processing batch ${currentBatchNum} of ${totalBatches}... (${batch.length} items)`, 'info');

    const mutation = `
      mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
        inventoryAdjustQuantities(input: $input) {
          userErrors {
            field
            message
          }
        }
      }`;

    try {
      const resp = await shopifyGraphQLRequest(mutation, {
        input: {
          name: 'btc_stock_update',
          reason,
          changes: batch
        }
      });
      const userErrors = resp?.data?.inventoryAdjustQuantities?.userErrors || [];
      if (userErrors.length) {
        addLog(`   ⚠️ Shopify userErrors (${userErrors.length}): ${userErrors.slice(0, 5).map(e => e.message).join(' | ')}`, 'warning');
      }
      addLog(`   ✅ Batch ${currentBatchNum} processed.`, 'success');
    } catch (error) {
      addLog(`   ❌ Error processing batch ${currentBatchNum}: ${error.message}`, 'error');
      throw error;
    }
  }
}

async function processAutomatedDiscontinuations(ftpInventory, allShopifyProducts) {
  addLog('Starting automated discontinuation process...', 'info');
  let discontinuedCount = 0;
  const ftpSkuSet = new Set(ftpInventory.keys());

  const productsToCheck = allShopifyProducts.filter(
    p => p.status === 'active' && productHasTag(p, config.btcactivewear.supplierTag)
  );
  addLog(`Found ${productsToCheck.length} active '${config.btcactivewear.supplierTag}' products to check.`, 'info');

  for (const product of productsToCheck) {
    const isProductInFtp = product.variants.some(v => ftpSkuSet.has(normalizeSku(v.sku)));
    if (!isProductInFtp) {
      addLog(`Discontinuing '${product.title}' (ID: ${product.id}) - not found in FTP stock file.`, 'warning');
      try {
        const inventoryAdjustments = product.variants
          .filter(v => v.inventory_quantity > 0)
          .map(v => ({
            inventoryItemId: `gid://shopify/InventoryItem/${v.inventory_item_id}`,
            locationId: config.shopify.locationId,
            delta: -v.inventory_quantity
          }));

        await sendInventoryUpdatesInBatches(inventoryAdjustments, 'discontinued');

        await shopifyRequestWithRetry('put', `/products/${product.id}.json`, {
          product: { id: product.id, status: 'draft' }
        });
        addLog(`   ✅ Set status to 'draft' for '${product.title}'.`, 'success');
        discontinuedCount++;
      } catch (error) {
        addLog(`❌ Failed to discontinue product '${product.title}' (ID: ${product.id}): ${error.message}`, 'error');
      }
    }
  }

  addLog(`Automated discontinuation process finished. Discontinued ${discontinuedCount} products.`, 'info');
  return discontinuedCount;
}

const isSystemLocked = () => Object.values(isRunning).some(v => v) || isSystemPaused || failsafe.isTriggered;

async function syncInventory() {
  if (isSystemLocked()) {
    addLog('Sync skipped: System is locked.', 'warning');
    return;
  }
  if (missingShopify.length) {
    addLog(`Cannot run sync: missing env vars: ${missingShopify.join(', ')}`, 'error');
    return;
  }

  isRunning.inventory = true;
  addLog('🚀 Starting BTC Activewear inventory sync process...', 'info');
  let runResult = { type: 'Inventory Sync', status: 'failed', updated: 0, discontinued: 0, errors: 0 };

  try {
    const startTime = Date.now();

    // 1) FTP fetch + parse
    const ftpStream = await fetchInventoryFromFTP();
    const ftpInventory = await parseInventoryCSV(ftpStream);
    addLog(`Successfully fetched and parsed ${ftpInventory.size} Stock IDs from BTC Activewear FTP.`, 'success');

    // 2) Shopify products
    const shopifyProducts = await getAllShopifyProducts();

    // 3) Discontinuations
    const discontinuedCount = await processAutomatedDiscontinuations(ftpInventory, shopifyProducts);
    runResult.discontinued = discontinuedCount;

    // 4) Mapping diagnostics
    const btcProducts = shopifyProducts.filter(p => p.status === 'active' && productHasTag(p, config.btcactivewear.supplierTag));
    const btcVariants = btcProducts.flatMap(p => p.variants || []);
    const totalVariants = btcVariants.length;

    const matchedVariants = [];
    const unmatchedSkus = [];
    let notTrackedCount = 0;

    for (const v of btcVariants) {
      const sku = normalizeSku(v.sku);
      if (!sku) continue;
      if (ftpInventory.has(sku)) matchedVariants.push(v);
      else unmatchedSkus.push(sku);
      if (v.inventory_management !== 'shopify') notTrackedCount++;
    }

    addLog(`[MAP] BTC products: ${btcProducts.length}, variants: ${totalVariants}`, 'info');
    addLog(`[MAP] Matched SKUs: ${matchedVariants.length}, Unmatched SKUs: ${unmatchedSkus.length}`, 'info');
    if (unmatchedSkus.length > 0) addLog(`[MAP] Sample unmatched SKUs: ${sample(unique(unmatchedSkus), 10).join(', ')}`, 'warning');
    if (notTrackedCount > 0) addLog(`[MAP] Variants not tracked by Shopify: ${notTrackedCount}`, 'warning');

    // 5) Location-level compare
    const inventoryItemIds = unique(matchedVariants.map(v => String(v.inventory_item_id)));
    const availableMap = await getAvailableAtLocationMap(inventoryItemIds, config.shopify.locationIdNumber);

    const adjustments = [];
    let sameCount = 0;
    const diffSamples = [];

    for (const v of matchedVariants) {
      const sku = normalizeSku(v.sku);
      const invItemIdStr = String(v.inventory_item_id);
      const ftpQty = ftpInventory.get(sku);
      const currentAvail = availableMap.get(invItemIdStr) ?? 0;

      if (ftpQty !== currentAvail) {
        const delta = ftpQty - currentAvail;
        adjustments.push({
          inventoryItemId: `gid://shopify/InventoryItem/${invItemIdStr}`,
          locationId: config.shopify.locationId,
          delta
        });
        if (diffSamples.length < 15) diffSamples.push(`${sku}: FTP=${ftpQty}, LocationAvail=${currentAvail}, Δ=${delta}`);
      } else {
        sameCount++;
      }
    }

    addLog(`[COMPARE] Location-level comparisons: Same=${sameCount}, Different=${adjustments.length}`, 'info');
    if (diffSamples.length > 0) addLog(`[COMPARE] Example differences: ${diffSamples.join(' | ')}`, 'info');

    // 6) Update
    await sendInventoryUpdatesInBatches(adjustments, 'stock_sync');
    runResult.updated = adjustments.length;

    if (adjustments.length > 0) addLog(`✅ Successfully sent bulk inventory updates for ${adjustments.length} variants.`, 'success');
    else addLog('ℹ️ No inventory updates were needed (at the configured location).', 'info');

    // 7) Done
    runResult.status = 'completed';
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const summary = `BTC Activewear sync ${runResult.status} in ${duration}s:
🔄 ${runResult.updated} variants updated
🗑️ ${runResult.discontinued} products discontinued`;
    addLog(summary, 'success');
    notifyTelegram(summary);

  } catch (error) {
    runResult.errors++;
    runResult.status = 'failed';
    triggerFailsafe(`Inventory sync failed: ${error.message}`);
  } finally {
    isRunning.inventory = false;
    addToHistory({ ...runResult, timestamp: new Date().toISOString() });
  }
}

// ============================================
// API Endpoints
// ============================================
app.get('/api/debug/ftp', async (req, res) => {
  const result = await testFtpConnection();
  res.json({
    ok: result.connected,
    ...result,
    // don’t expose password
    host: config.ftp.host,
    user: config.ftp.user
  });
});

app.post('/api/sync/inventory', requireApiKey, (req, res) => {
  syncInventory();
  res.json({ success: true });
});

app.post('/api/pause/toggle', requireApiKey, (req, res) => {
  isSystemPaused = !isSystemPaused;
  if (isSystemPaused) fs.writeFileSync(PAUSE_LOCK_FILE, 'paused');
  else try { fs.unlinkSync(PAUSE_LOCK_FILE); } catch (e) { /* ignore */ }
  addLog(`System ${isSystemPaused ? 'PAUSED' : 'RESUMED'}.`, 'warning');
  res.json({ success: true });
});

app.post('/api/failsafe/clear', requireApiKey, (req, res) => {
  failsafe = { isTriggered: false };
  addLog('Failsafe cleared.', 'warning');
  res.json({ success: true });
});

// ============================================
// WEB INTERFACE
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
  </style></head><body><div class="container"><h1>BTC Activewear Sync</h1>
  <div class="grid">
    <div class="card">
      <h2>System</h2>
      <p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': Object.values(isRunning).some(v => v) ? 'Busy' : 'Active'}</p>
      <button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>
      ${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}
      <div class="location-info">Location ID: ${config.shopify.locationIdNumber}</div>
      <div class="supplier-info">Supplier Tag: ${config.btcactivewear.supplierTag}<br>FTP Host: ${config.ftp.host}<br>FTP User: ${config.ftp.user}</div>
      <div style="margin-top:8px"><a class="btn" href="/api/debug/ftp" target="_blank">Test FTP</a></div>
    </div>
    <div class="card">
      <h2>Inventory Sync</h2>
      <p>Status: ${isRunning.inventory?'Running':'Ready'}</p>
      <p>Syncs stock levels and <b>automatically discontinues</b> products not in the FTP file.</p>
      <button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${Object.values(isRunning).some(v => v)||isSystemPaused||failsafe.isTriggered?'disabled':''}>Run Now</button>
    </div>
  </div>
  <div class="card">
    <h2>Last Inventory Sync</h2>
    <div class="grid" style="grid-template-columns:1fr 1fr;">
      <div class="stat-card"><div class="stat-value">${lastInventorySync?.updated ?? 'N/A'}</div><div class="stat-label">Variants Updated</div></div>
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
      setTimeout(()=>location.reload(),500);
    }catch(e){
      alert('Error: '+e.message);
      if(btn)btn.disabled=false;
      location.reload();
    }
  }
  </script></body></html>`;
  res.send(html);
});

// ============================================
// SCHEDULED TASKS & STARTUP
// ============================================
cron.schedule('0 2 * * *', () => syncInventory()); // Daily at 2 AM

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  checkPauseStateOnStartup();
  addLog(`✅ BTC Activewear Sync Server started on port ${PORT} (Location: ${config.shopify.locationIdNumber})`, 'success');
  console.log(`Server is listening on 0.0.0.0:${PORT}`);
  console.log(`Railway deployment successful - server ready to accept connections`);
  if (config.runtime.runStartupSync) {
    setTimeout(() => { if (!isSystemLocked()) { syncInventory(); } }, 5000);
  }
});

function shutdown(signal) {
  addLog(`Received ${signal}, shutting down...`, 'info');
  saveHistory();
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
