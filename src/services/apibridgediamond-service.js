const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const path = require('node:path');

class BridgeError extends Error {
    constructor(message, statusCode = 400) { super(message); this.name = 'BridgeError'; this.statusCode = statusCode; }
}

class APIBridgeDiamondService {
    constructor(options = {}) {
        this.dataFile = path.resolve(options.dataFile || '.apibridge/state.json');
        this.fetch = options.fetch || global.fetch;
        this.maxAttempts = integer(options.maxAttempts, 3, 1, 10, 'maxAttempts');
        this.timeoutMs = integer(options.timeoutMs, 5000, 100, 60000, 'timeoutMs');
        this.routes = {};
        this.deliveries = {};
        this.idempotency = {};
        this.running = false;
        this.ready = false;
        this.saveTail = Promise.resolve();
        this.lastError = null;
    }

    async init() {
        await fs.mkdir(path.dirname(this.dataFile), { recursive: true });
        try {
            const state = JSON.parse(await fs.readFile(this.dataFile, 'utf8'));
            this.routes = state.routes || {};
            this.deliveries = state.deliveries || {};
            this.idempotency = state.idempotency || {};
            for (const delivery of Object.values(this.deliveries)) if (delivery.status === 'delivering') delivery.status = 'queued';
        } catch (error) {
            if (error.code !== 'ENOENT') throw error;
            await this.save();
        }
        this.ready = true;
        this.scheduleDrain();
        return this;
    }

    async createRoute(input) {
        await this.ensureReady();
        if (!input || typeof input !== 'object') throw new BridgeError('Route body must be a JSON object');
        if (!/^[a-z][a-z0-9-]{1,62}$/.test(input.name || '')) throw new BridgeError('name must be 2-63 lowercase letters, digits, or hyphens');
        let target;
        try { target = new URL(input.targetUrl); } catch { throw new BridgeError('targetUrl must be a valid HTTP(S) URL'); }
        if (!['http:', 'https:'].includes(target.protocol)) throw new BridgeError('targetUrl must use HTTP or HTTPS');
        if (this.routes[input.name]) throw new BridgeError(`Route already exists: ${input.name}`, 409);
        const route = { name: input.name, targetUrl: target.toString(), secret: input.secret || null, active: true, createdAt: new Date().toISOString() };
        this.routes[route.name] = route;
        await this.save();
        return publicRoute(route);
    }

    async listRoutes() { await this.ensureReady(); return Object.values(this.routes).map(publicRoute); }

    async removeRoute(name) {
        await this.ensureReady();
        if (!this.routes[name]) return false;
        delete this.routes[name];
        await this.save();
        return true;
    }

    async enqueue(routeName, payload, options = {}) {
        await this.ensureReady();
        const route = this.routes[routeName];
        if (!route || !route.active) throw new BridgeError(`Active route not found: ${routeName}`, 404);
        if (payload == null) throw new BridgeError('Event payload is required');
        const key = options.idempotencyKey;
        if (key && this.idempotency[`${routeName}:${key}`]) return this.deliveries[this.idempotency[`${routeName}:${key}`]];
        const id = crypto.randomUUID();
        const delivery = { id, route: routeName, payload, status: 'queued', attempts: 0, responseStatus: null, error: null, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
        this.deliveries[id] = delivery;
        if (key) this.idempotency[`${routeName}:${key}`] = id;
        await this.save();
        this.scheduleDrain();
        return delivery;
    }

    async getDelivery(id) { await this.ensureReady(); return this.deliveries[id] || null; }
    async listDeliveries() { await this.ensureReady(); return Object.values(this.deliveries).sort((a, b) => b.createdAt.localeCompare(a.createdAt)); }

    async metrics() {
        await this.ensureReady();
        const counts = { queued: 0, delivering: 0, delivered: 0, failed: 0 };
        for (const item of Object.values(this.deliveries)) counts[item.status] += 1;
        return { routes: Object.keys(this.routes).length, deliveries: Object.keys(this.deliveries).length, ...counts, lastError: this.lastError };
    }

    scheduleDrain() {
        queueMicrotask(() => this.drain().catch(error => { this.lastError = error.message; }));
    }

    async drain() {
        if (this.running) return;
        this.running = true;
        try {
            let delivery;
            while ((delivery = Object.values(this.deliveries).find(item => item.status === 'queued'))) await this.deliver(delivery);
        } finally { this.running = false; }
    }

    async deliver(delivery) {
        const route = this.routes[delivery.route];
        if (!route) { delivery.status = 'failed'; delivery.error = 'Route was removed'; await this.save(); return; }
        const body = JSON.stringify({ id: delivery.id, createdAt: delivery.createdAt, data: delivery.payload });
        delivery.status = 'delivering';
        if (delivery.attempts >= this.maxAttempts) {
            delivery.status = 'failed';
            delivery.error = 'Delivery attempt limit reached before restart';
            await this.save();
            return;
        }
        for (let attempt = delivery.attempts + 1; attempt <= this.maxAttempts; attempt += 1) {
            delivery.attempts = attempt;
            await this.save();
            try {
                const headers = { 'content-type': 'application/json', 'x-api-bridge-delivery': delivery.id };
                if (route.secret) headers['x-api-bridge-signature-256'] = `sha256=${crypto.createHmac('sha256', route.secret).update(body).digest('hex')}`;
                const controller = new AbortController();
                const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
                let response;
                try { response = await this.fetch(route.targetUrl, { method: 'POST', headers, body, signal: controller.signal }); }
                finally { clearTimeout(timeout); }
                delivery.responseStatus = response.status;
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                delivery.status = 'delivered'; delivery.error = null; break;
            } catch (error) {
                delivery.error = error.name === 'AbortError' ? 'Request timed out' : error.message;
                delivery.status = attempt === this.maxAttempts ? 'failed' : 'delivering';
            }
        }
        delivery.updatedAt = new Date().toISOString();
        await this.save();
    }

    save() {
        const snapshot = `${JSON.stringify({ routes: this.routes, deliveries: this.deliveries, idempotency: this.idempotency }, null, 2)}\n`;
        const operation = this.saveTail.then(async () => {
            const temporary = `${this.dataFile}.${process.pid}.tmp`;
            await fs.writeFile(temporary, snapshot, { mode: 0o600 });
            await fs.rename(temporary, this.dataFile);
        });
        this.saveTail = operation.catch(() => {});
        return operation;
    }

    async ensureReady() { if (!this.ready) await this.init(); }
}

function publicRoute(route) { const { secret, ...safe } = route; return { ...safe, signed: Boolean(secret) }; }
function integer(value, fallback, min, max, name) { const parsed = value == null ? fallback : Number(value); if (!Number.isInteger(parsed) || parsed < min || parsed > max) throw new BridgeError(`${name} must be an integer from ${min} to ${max}`); return parsed; }

module.exports = { APIBridgeDiamondService, BridgeError };
