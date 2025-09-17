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

// Upload (kept for future)
const upload = multer({
  dest: path.join(__dirname, 'uploads/'),
  limits: { fileSize: 500 * 1024 * 1024 }
});

// ============================================
// CONFIGURATION
// ============================================

// Validate and clean Shopify domain
function validateShopifyDomain(domain) {
  if (!domain) return null;
  // Remove https://, trailing slashes, and paths
  let cleaned = domain.replace(/^https?:\/\//, '').replace(/\/.*$/, '').trim();
  
  // If it doesn't end with .myshopify.com, it's wrong
  if (!cleaned.endsWith('.myshopify.com')) {
    console.error(`ERROR: SHOPIFY_DOMAIN must be your-store.myshopify.com format`);
    console.error(`You provided: ${domain}`);
    console.error(`This should be your Shopify admin domain, not your customer-facing domain`);
    return null;
  }
  
  return cleaned;
}

const shopifyDomain = validateShopifyDomain(process.env.SHOPIFY_DOMAIN);

const config = {
  shopify: {
    domain: shopifyDomain,
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
    locationId: `gid://shopify/Location/${process.env.SHOPIFY_LOCATION_ID}`,
    locationIdNumber: process.env.SHOPIFY_LOCATION_ID,
    baseUrl: shopifyDomain ? `https://${shopifyDomain}/admin/api/2024-04` : null,
    graphqlUrl: shopifyDomain ? `https://${shopifyDomain}/admin/api/2024-04/graphql.json` : null
  },
  // BTC FTP
  ftp: {
    host: process.env.FTP_HOST || process.env.BTC_FTP_HOST || 'ftpdata.btcactivewear.co.uk',
    user: process.env.FTP_USERNAME || process.env.BTC_FTP_USERNAME || 'ara0010',
    password: process.env.FTP_PASSWORD || process.env.BTC_FTP_PASSWORD || '87(fJrD5y<S6',
    secure: false
  },
  btcactivewear: {
    supplierTag: 'Source_BTC Activewear',
    stockFilePath: '/webdata/stock_levels_stock_id.csv',
    maxInventory: parseInt(process.env.BTC_MAX_INVENTORY || process.env.MAX_INVENTORY || '9999', 10),
    csvSeparator: (process.env.BTC_CSV_SEPARATOR || 'auto').toLowerCase()
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
  console.error(`CRITICAL: Missing required environment variables: ${missingEnv.join(', ')}`);
  console.error(`Please set these in Railway's Variables tab`);
}

if (!shopifyDomain) {
  console.error(`CRITICAL: Invalid SHOPIFY_DOMAIN. Must be in format: your-store.myshopify.com`);
  console.error(`Example: If your admin URL is https://my-awesome-store.myshopify.com/admin`);
  console.error(`Then set SHOPIFY_DOMAIN=my-awesome-store.myshopify.com`);
}

// Log configuration
console.log(`========================================`);
console.log(`BTC Activewear Sync Configuration:`);
console.log(`  Shopify Domain: ${shopifyDomain || 'INVALID - FIX THIS!'}`);
console.log(`  Shopify Location ID: ${config.shopify.locationIdNumber || 'NOT SET'}`);
console.log(`  BTC FTP Host: ${config.ftp.host}`);
console.log(`  BTC FTP User: ${config.ftp.user}`);
console.log(`  BTC FTP Password: ${config.ftp.password ? '***SET***' : 'NOT SET'}`);
console.log(`========================================`);

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

const shopifyClient = config.shopify.baseUrl ? axios.create({
  baseURL: config.shopify.baseUrl,
  headers: { 'X-Shopify-Access-Token': config.shopify.accessToken },
  timeout: 60000
}) : null;

let runHistory = [];
function loadHistory() { 
  try { 
    if (fs.existsSync(HISTORY_FILE)) 
      runHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); 
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
  if (fs.existsSync(PAUSE_LOCK_FILE)) isSystemPaused = true; 
  loadHistory(); 
}

async function shopifyRequestWithRetry(method, url, data = null, retries = 5) {
  if (!shopifyClient) {
    throw new Error('Shopify client not initialized. Check SHOPIFY_DOMAIN configuration.');
  }
  
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
      
      if (error.response?.status === 404) {
        addLog(`Shopify 404 Error Details:`, 'error');
        addLog(`  Attempted URL: ${error.config?.url}`, 'error');
        addLog(`  Base URL: ${config.shopify.baseUrl}`, 'error');
        addLog(`  Full URL: ${error.config?.baseURL}${error.config?.url}`, 'error');
        addLog(`  SHOPIFY_DOMAIN must be your .myshopify.com domain`, 'error');
        throw error;
      }
      
      if (error.response?.status === 401) {
        addLog(`Shopify 401: Invalid access token or insufficient permissions`, 'error');
        throw error;
      }
      
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
  if (!config.shopify.graphqlUrl) {
    throw new Error('GraphQL URL not configured. Check SHOPIFY_DOMAIN.');
  }
  
  try {
    await rateLimiter.acquire();
    const response = await axios.post(config.shopify.graphqlUrl, { query, variables }, {
      headers: { 'X-Shopify-Access-Token': config.shopify.accessToken }
    });
    if (response.data.errors) {
      throw new Error(JSON.stringify(response.data.errors));
    }
    return response.data;
  } catch (error) {
    addLog(`GraphQL error: ${error.message}`, 'error');
    throw error;
  }
}

// Utils
const normalizeSku = s => (s || '').toString().trim().toUpperCase();
const productHasTag = (p, tag) => new Set(String(p?.tags || '').split(',').map(t => t.trim())).has(tag);
const unique = arr => Array.from(new Set(arr.filter(Boolean)));
const sample = (arr, n = 10) => arr.slice(0, n);

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
    addLog(`FTP connected successfully. Downloading ${config.btcactivewear.stockFilePath}...`, 'info');
    const chunks = [];
    await client.downloadTo(new Writable({ write(c, e, cb) { chunks.push(c); cb(); } }), config.btcactivewear.stockFilePath);
    addLog('FTP download completed successfully.', 'success');
    return Buffer.concat(chunks);
  } catch (e) {
    addLog(`FTP error: ${e.message}`, 'error');
    throw e;
  } finally { client.close(); }
}

async function parseInventoryCSV(buffer) {
  let delimiter = ',', detected = { delimiter: ',', label: 'comma', headerPreview: '', firstLines: '' };
  if (config.btcactivewear.csvSeparator === 'auto') {
    detected = detectDelimiterFromBuffer(buffer);
    delimiter = detected.delimiter;
    addLog(`[CSV] Detected delimiter: ${detected.label}. Header: "${detected.headerPreview}"`, 'info');
  } else {
    delimiter = config.btcactivewear.csvSeparator === '\\t' ? '\t' : config.btcactivewear.csvSeparator;
    addLog(`[CSV] Using configured delimiter: "${config.btcactivewear.csvSeparator}"`, 'info');
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
        const qty = Math.min(parseInt(qtyRaw, 10) || 0, config.btcactivewear.maxInventory);
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
// SHOPIFY
// ============================================

async function testShopifyConnection() {
  try {
    const response = await shopifyRequestWithRetry('get', '/shop.json');
    const shop = response.data?.shop;
    if (shop) {
      addLog(`Successfully connected to Shopify: ${shop.name}`, 'success');
      return true;
    }
  } catch (error) {
    addLog(`Failed to connect to Shopify: ${error.message}`, 'error');
    return false;
  }
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
        // Remove the base URL if it was included
        url = url.replace(/^https?:\/\/[^\/]+/, '');
        url = url.replace(/^\/admin\/api\/\d{4}-\d{2}/, '');
      } else {
        url = null;
      }
    } catch (e) {
      addLog(`Error fetching products: ${e.message}`, 'error');
      triggerFailsafe('Failed to fetch products from Shopify');
      return [];
    }
  }
  
  addLog(`Fetched ${allProducts.length} products.`, 'success');
  return allProducts;
}

async function getInventoryItemsTrackedMap(inventoryItemIds) {
  const chunkSize = 50;
  const ids = unique(inventoryItemIds.map(String));
  const map = new Map();
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const res = await shopifyRequestWithRetry('get', `/inventory_items.json?ids=${chunk.join(',')}`);
    const items = res.data?.inventory_items || [];
    items.forEach(it => map.set(String(it.id), Boolean(it.tracked)));
    addLog(`   [tracked] fetched ${items.length} items (chunk ${i / chunkSize + 1}/${Math.ceil(ids.length / chunkSize)})`, 'info');
  }
  return map;
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

async function getAvailableAtLocationMap(inventoryItemIds, locationIdNumber) {
  const chunkSize = 50;
  const result = new Map();
  const ids = unique(inventoryItemIds.map(String));
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

async function ensureTrackedAndConnected(inventoryItemIds) {
  const ids = unique(inventoryItemIds.map(String));

  const trackedMap = await getInventoryItemsTrackedMap(ids);
  const toTrack = ids.filter(id => trackedMap.get(id) !== true);
  if (toTrack.length) {
    addLog(`[FIX] Enabling tracking for ${toTrack.length} inventory items...`, 'warning');
    const updated = await setInventoryItemsTrackedBulk(toTrack, true);
    addLog(`[FIX] Tracking enabled for ${updated}/${toTrack.length}`, updated === toTrack.length ? 'success' : 'warning');
  }

  let availableMap = await getAvailableAtLocationMap(ids, config.shopify.locationIdNumber);
  const toConnect = ids.filter(id => !availableMap.has(id));
  if (toConnect.length) {
    addLog(`[FIX] Connecting ${toConnect.length} items to location ${config.shopify.locationIdNumber}...`, 'warning');
    const connected = await connectInventoryLevelsBulk(toConnect, config.shopify.locationIdNumber);
    addLog(`[FIX] Connected ${connected}/${toConnect.length} to location`, connected === toConnect.length ? 'success' : 'warning');
    availableMap = await getAvailableAtLocationMap(ids, config.shopify.locationIdNumber);
  }

  return availableMap;
}

async function sendInventoryUpdatesInBatches(adjustments, reason = 'correction') {
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

      const ues = resp?.data?.inventoryAdjustQuantities?.userErrors || [];
      if (ues.length) {
        const failedIdxs = new Set();
        ues.forEach(e => {
          const msg = e.message || 'Unknown error';
          errorMsgCounts.set(msg, (errorMsgCounts.get(msg) || 0) + 1);
          if (errorSamples.length < 10) errorSamples.push(msg);
          const field = e.field || [];
          for (const part of field) {
            const n = Number(part);
            if (Number.isInteger(n)) { failedIdxs.add(n); break; }
          }
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
    const top = [...errorMsgCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([m, c]) => `${m} (${c})`);
    addLog(`⚠️ Shopify userErrors summary: ${top.join(' | ')}`, 'warning');
  }

  addLog(`Apply summary: attempted=${attempted}, applied≈${applied}, failed≈${failed}`, failed > 0 ? 'warning' : 'success');
  return { attempted, applied, failed, errorSamples };
}

async function processAutomatedDiscontinuations(ftpInventory, allShopifyProducts) {
  addLog('Starting automated discontinuation process...', 'info');
  let discontinuedCount = 0;
  const ftpSkuSet = new Set(ftpInventory.keys());
  const productsToCheck = allShopifyProducts.filter(p => p.status === 'active' && productHasTag(p, config.btcactivewear.supplierTag));
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
        await sendInventoryUpdatesInBatches(inventoryAdjustments, 'correction');
        await shopifyRequestWithRetry('put', `/products/${product.id}.json`, { product: { id: product.id, status: 'draft' } });
        addLog(`   ✅ Set status to 'draft' for '${product.title}'.`, 'success');
        discontinuedCount++;
      } catch (e) {
        addLog(`❌ Failed to discontinue product '${product.title}' (ID: ${product.id}): ${e.message}`, 'error');
      }
    }
  }

  addLog(`Automated discontinuation finished. Discontinued ${discontinuedCount} products.`, 'info');
  return discontinuedCount;
}

const isSystemLocked = () => Object.values(isRunning).some(v => v) || isSystemPaused || failsafe.isTriggered;

async function syncInventory() {
  if (isSystemLocked()) {
    addLog('Sync skipped: System is locked.', 'warning');
    return;
  }
  
  if (!shopifyDomain) {
    addLog('Cannot run sync: SHOPIFY_DOMAIN is invalid. Must be your-store.myshopify.com format', 'error');
    triggerFailsafe('Invalid SHOPIFY_DOMAIN configuration');
    return;
  }
  
  if (missingEnv.length) {
    addLog(`Cannot run sync: missing env vars: ${missingEnv.join(', ')}`, 'error');
    return;
  }

  isRunning.inventory = true;
  addLog('🚀 Starting BTC Activewear inventory sync process...', 'info');
  let runResult = { type: 'Inventory Sync', status: 'failed', updated: 0, discontinued: 0, errors: 0 };

  try {
    const startTime = Date.now();

    // Test Shopify connection first
    const shopifyConnected = await testShopifyConnection();
    if (!shopifyConnected) {
      throw new Error('Cannot connect to Shopify. Check SHOPIFY_DOMAIN and SHOPIFY_ACCESS_TOKEN');
    }

    // 1) FTP fetch + parse
    const fileBuffer = await fetchInventoryFileBuffer();
    const ftpInventory = await parseInventoryCSV(fileBuffer);
    if (ftpInventory.size === 0) throw new Error('Parsed 0 SKUs from BTC file. Check headers/delimiter in [CSV] logs.');
    addLog(`Successfully fetched and parsed ${ftpInventory.size} Stock IDs from BTC FTP.`, 'success');

    // 2) Shopify products
    const shopifyProducts = await getAllShopifyProducts();
    if (!shopifyProducts.length) throw new Error('No products fetched from Shopify.');

    // 3) Discontinuations
    runResult.discontinued = await processAutomatedDiscontinuations(ftpInventory, shopifyProducts);

    // 4) Map BTC-tagged variants
    const btcProducts = shopifyProducts.filter(p => p.status === 'active' && productHasTag(p, config.btcactivewear.supplierTag));
    const btcVariants = btcProducts.flatMap(p => p.variants || []);
    const matchedVariants = [];
    const unmatchedSkus = [];
    let notTrackedCount = 0;

    for (const v of btcVariants) {
      const sku = normalizeSku(v.sku);
      if (!sku) continue;
      if (ftpInventory.has(sku)) matchedVariants.push(v); else unmatchedSkus.push(sku);
      if (v.inventory_management !== 'shopify') notTrackedCount++;
    }

    addLog(`[MAP] BTC products: ${btcProducts.length}, variants: ${btcVariants.length}`, 'info');
    addLog(`[MAP] Matched SKUs: ${matchedVariants.length}, Unmatched SKUs: ${unmatchedSkus.length}`, 'info');
    if (unmatchedSkus.length) addLog(`[MAP] Sample unmatched SKUs: ${sample(unique(unmatchedSkus), 10).join(', ')}`, 'warning');
    if (notTrackedCount) addLog(`[MAP] Variants with inventory_management != 'shopify': ${notTrackedCount}`, 'warning');

    // 5) Ensure tracked + connected, then fetch availability
    const invItemIds = unique(matchedVariants.map(v => String(v.inventory_item_id)));
    const availableMap = await ensureTrackedAndConnected(invItemIds);

    // 6) Build deltas
    const adjustments = [];
    let sameCount = 0;
    const diffSamples = [];
    for (const v of matchedVariants) {
      const sku = normalizeSku(v.sku);
      const invItemId = String(v.inventory_item_id);
      const ftpQty = ftpInventory.get(sku);
      const currentAvail = availableMap.get(invItemId) ?? 0;
      if (ftpQty !== currentAvail) {
        const delta = ftpQty - currentAvail;
        adjustments.push({
          inventoryItemId: `gid://shopify/InventoryItem/${invItemId}`,
          locationId: config.shopify.locationId,
          delta
        });
        if (diffSamples.length < 15) diffSamples.push(`${sku}: FTP=${ftpQty}, Loc=${currentAvail}, Δ=${delta}`);
      } else {
        sameCount++;
      }
    }

    addLog(`[COMPARE] Same=${sameCount}, Different=${adjustments.length}`, 'info');
    if (diffSamples.length) addLog(`[COMPARE] Example differences: ${diffSamples.join(' | ')}`, 'info');

    // 7) Send updates
    const result = await sendInventoryUpdatesInBatches(adjustments, 'correction');
    runResult.updated = result.applied;

    if (result.applied > 0) addLog(`✅ Applied ≈${result.applied}/${result.attempted} changes.`, 'success');
    else addLog('ℹ️ No changes applied.', 'warning');

    // 8) Done
    runResult.status = 'completed';
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const summary = `BTC Activewear sync ${runResult.status} in ${duration}s:
🔄 Applied ≈${runResult.updated} changes
🗑️ ${runResult.discontinued} products discontinued
📊 ${matchedVariants.length}/${btcVariants.length} variants matched`;
    addLog(summary, 'success');
    notifyTelegram(summary);

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
// API
// ============================================

app.get('/api/debug/config', (req, res) => {
  res.json({
    shopify: {
      domain: shopifyDomain || 'INVALID',
      domainIsValid: !!shopifyDomain,
      locationId: config.shopify.locationIdNumber,
      baseUrl: config.shopify.baseUrl
    },
    ftp: {
      host: config.ftp.host,
      user: config.ftp.user,
      passwordSet: !!config.ftp.password
    },
    missingEnvVars: missingEnv
  });
});

app.post('/api/sync/inventory', requireApiKey, (req, res) => { 
  syncInventory(); 
  res.json({ success: true }); 
});

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
  
  const errorBanner = !shopifyDomain ? `
    <div style="background:rgba(255,0,0,0.2);border:2px solid red;padding:1rem;margin:1rem 0;border-radius:6px;">
      <h3 style="margin:0 0 0.5rem 0;color:#ff6b6b;">⚠️ Configuration Error</h3>
      <p style="margin:0;">SHOPIFY_DOMAIN is invalid or not set correctly.</p>
      <p style="margin:0.5rem 0 0 0;">It must be your .myshopify.com domain (e.g., my-store.myshopify.com)</p>
      <p style="margin:0.5rem 0 0 0;">Current value: ${process.env.SHOPIFY_DOMAIN || 'NOT SET'}</p>
    </div>
  ` : '';
  
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
  .debug-link{margin-top:0.5rem;}
  </style></head><body><div class="container">
  <h1>BTC Activewear Sync</h1>
  ${errorBanner}
  <div class="grid">
    <div class="card">
      <h2>System</h2>
      <p>Status: ${isSystemPaused?'Paused':failsafe.isTriggered?'FAILSAFE': Object.values(isRunning).some(v => v) ? 'Busy' : 'Active'}</p>
      <button onclick="apiPost('/api/pause/toggle')" class="btn" ${failsafe.isTriggered?'disabled':''}>${isSystemPaused?'Resume':'Pause'}</button>
      ${failsafe.isTriggered?`<button onclick="apiPost('/api/failsafe/clear')" class="btn">Clear Failsafe</button>`:''}
      <div class="location-info">Location ID: ${config.shopify.locationIdNumber || 'NOT SET'}</div>
      <div class="supplier-info">
        Supplier Tag: ${config.btcactivewear.supplierTag}<br>
        FTP Host: ${config.ftp.host}<br>
        Shopify Domain: ${shopifyDomain || 'INVALID'}
      </div>
      <div class="debug-link">
        <a href="/api/debug/config" target="_blank" class="btn">View Config</a>
      </div>
    </div>
    <div class="card">
      <h2>Inventory Sync</h2>
      <p>Status: ${isRunning.inventory?'Running':'Ready'}</p>
      <p>Syncs stock levels and <b>automatically discontinues</b> products not in the FTP file.</p>
      <button onclick="apiPost('/api/sync/inventory','Run inventory sync?')" class="btn btn-primary" ${Object.values(isRunning).some(v => v)||isSystemPaused||failsafe.isTriggered||!shopifyDomain?'disabled':''}>Run Now</button>
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
      setTimeout(()=>location.reload(),700);
    }catch(e){
      alert('Error: '+e.message);
      if(btn)btn.disabled=false;
    }
  }
  </script></body></html>`;
  res.send(html);
});

// ============================================
// SCHEDULED TASKS & STARTUP
// ============================================
cron.schedule('0 2 * * *', () => syncInventory()); // Daily 2 AM

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  checkPauseStateOnStartup();
  addLog(`✅ BTC Activewear Sync Server started on port ${PORT} (Location: ${config.shopify.locationIdNumber})`, 'success');
  console.log(`Server is listening on 0.0.0.0:${PORT}`);
  
  if (!shopifyDomain) {
    console.error('\n========================================');
    console.error('⚠️  CRITICAL CONFIGURATION ERROR');
    console.error('========================================');
    console.error('SHOPIFY_DOMAIN is not set correctly!');
    console.error('');
    console.error('In Railway Variables, set:');
    console.error('SHOPIFY_DOMAIN=your-store.myshopify.com');
    console.error('');
    console.error('Example: If your admin URL is:');
    console.error('https://my-awesome-store.myshopify.com/admin');
    console.error('Then set: SHOPIFY_DOMAIN=my-awesome-store.myshopify.com');
    console.error('========================================\n');
  }
  
  if (config.runtime.runStartupSync && shopifyDomain) {
    setTimeout(() => { if (!isSystemLocked()) syncInventory(); }, 5000);
  }
});

function shutdown(signal) { 
  addLog(`Received ${signal}, shutting down...`, 'info'); 
  saveHistory(); 
  process.exit(0); 
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
