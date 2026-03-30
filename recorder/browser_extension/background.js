const DEFAULT_API_URL = "http://127.0.0.1:5000/api/ingest_line";
const DEFAULT_BASE_URL = "http://127.0.0.1:5000";
const QUEUE_KEY = "pendingApiQueue";
const FLUSH_ALARM = "flushPendingApiQueue";
const MAX_QUEUE_SIZE = 2000;
const MAX_ACTIVITY_ITEMS = 80;
const recentActivity = [];

function clipText(value, maxLength = 140) {
    const text = String(value || "").replace(/\s+/g, " ").trim();
    if (text.length <= maxLength) {
        return text;
    }
    return `${text.slice(0, maxLength - 1)}...`;
}

function itemPreview(item) {
    if (!item || typeof item !== "object") {
        return "";
    }
    if (item.type === "line") {
        return clipText(item.line || "");
    }
    if (item.type === "replay_bulk") {
        const count = Array.isArray(item.urls) ? item.urls.length : 0;
        return `${count} replay(s) voor team ${Number(item.teamId) || "?"}`;
    }
    if (item.type === "poke") {
        return `poke: ${clipText(item.reason || "unknown", 80)}`;
    }
    return clipText(JSON.stringify(item), 140);
}

function contextLabel(context) {
    if (!context || typeof context !== "object") {
        return "LIVE";
    }
    if (context.phase === "leads") {
        return "LEADS";
    }
    if (context.phase === "turn") {
        const turn = Number(context.turn);
        if (Number.isFinite(turn) && turn > 0) {
            return `TURN ${turn}`;
        }
        return "TURN";
    }
    return "LIVE";
}

function pushActivity(status, item, meta = {}) {
    const entry = {
        time: new Date().toISOString(),
        status: String(status || "info"),
        type: String(item?.type || "unknown"),
        preview: itemPreview(item),
        source: meta.source || "live",
        label: contextLabel(item?.context),
    };
    recentActivity.push(entry);
    if (recentActivity.length > MAX_ACTIVITY_ITEMS) {
        recentActivity.splice(0, recentActivity.length - MAX_ACTIVITY_ITEMS);
    }

    try {
        chrome.runtime.sendMessage({ action: "ACTIVITY_PUSH", entry }, () => {
            void chrome.runtime.lastError;
        });
    } catch (_error) {
    }
}

function getRecentActivity(limit = 25) {
    const safeLimit = Math.max(1, Math.min(Number(limit) || 25, MAX_ACTIVITY_ITEMS));
    return recentActivity.slice(-safeLimit).reverse();
}

function getSyncStorage(keys) {
    return new Promise((resolve) => {
        chrome.storage.sync.get(keys, (result) => resolve(result || {}));
    });
}

function getLocalStorage(keys) {
    return new Promise((resolve) => {
        chrome.storage.local.get(keys, (result) => resolve(result || {}));
    });
}

function setLocalStorage(values) {
    return new Promise((resolve) => {
        chrome.storage.local.set(values, () => resolve());
    });
}

function getBaseUrlFromIngestUrl(apiUrl) {
    const value = String(apiUrl || "").trim();
    if (!value) return DEFAULT_BASE_URL;
    if (value.includes("/api/ingest_line")) {
        return value.split("/api/ingest_line")[0] || DEFAULT_BASE_URL;
    }
    try {
        const parsed = new URL(value);
        return `${parsed.protocol}//${parsed.host}`;
    } catch (_error) {
        return DEFAULT_BASE_URL;
    }
}

async function getSettings() {
    const data = await getSyncStorage({
        apiUrl: DEFAULT_API_URL,
        enabled: true,
    });
    return {
        apiUrl: String(data.apiUrl || DEFAULT_API_URL).trim() || DEFAULT_API_URL,
        enabled: data.enabled !== false,
    };
}

async function readQueue() {
    const data = await getLocalStorage({ [QUEUE_KEY]: [] });
    return Array.isArray(data[QUEUE_KEY]) ? data[QUEUE_KEY] : [];
}

async function writeQueue(items) {
    const trimmed = Array.isArray(items) ? items.slice(-MAX_QUEUE_SIZE) : [];
    await setLocalStorage({ [QUEUE_KEY]: trimmed });
}

async function enqueue(item) {
    const queue = await readQueue();
    const normalized = {
        id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
        createdAt: new Date().toISOString(),
        ...item,
    };
    queue.push(normalized);
    await writeQueue(queue);
    return queue.length;
}

async function getQueueCount() {
    const queue = await readQueue();
    return queue.length;
}

async function trySendItem(item, settings) {
    if (item.type === "line") {
        if (!settings.enabled) {
            return { ok: true, dropped: true };
        }
        const response = await fetch(settings.apiUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ line: item.line }),
            cache: "no-store",
        });
        return { ok: response.ok };
    }

    if (item.type === "replay_bulk") {
        const baseUrl = getBaseUrlFromIngestUrl(settings.apiUrl);
        const endpoint = `${baseUrl}/api/ingest_replay_bulk`;
        const response = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                urls: item.urls || [],
                team_id: Number(item.teamId),
            }),
            cache: "no-store",
        });
        if (!response.ok) {
            return { ok: false };
        }
        const payload = await response.json().catch(() => ({}));
        return {
            ok: payload.status === "ok",
            payload,
        };
    }

    if (item.type === "poke") {
        const baseUrl = getBaseUrlFromIngestUrl(settings.apiUrl);
        const endpoint = `${baseUrl}/api/poke`;
        const response = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ source: "extension", reason: item.reason || "unknown" }),
            cache: "no-store",
        });
        return { ok: response.ok };
    }

    return { ok: true };
}

async function flushQueue(limit = 120) {
    const settings = await getSettings();
    const queue = await readQueue();
    if (!queue.length) {
        return { sent: 0, remaining: 0 };
    }

    const pending = [...queue];
    let sent = 0;
    let processed = 0;

    while (pending.length && processed < limit) {
        const item = pending[0];
        processed += 1;
        try {
            const result = await trySendItem(item, settings);
            if (!result.ok) {
                pushActivity("error", item, { source: "queue" });
                break;
            }
            pending.shift();
            sent += 1;
            pushActivity("sent", item, { source: "queue" });
        } catch (_error) {
            pushActivity("error", item, { source: "queue" });
            break;
        }
    }

    await writeQueue(pending);
    return { sent, remaining: pending.length };
}

async function sendOrQueueLine(line, context) {
    const normalized = String(line || "").trim();
    if (!normalized) {
        return { ok: false, reason: "empty-line" };
    }

    const lineItem = { type: "line", line: normalized, context: context || null };

    const settings = await getSettings();
    if (!settings.enabled) {
        pushActivity("dropped", lineItem, { source: "live" });
        return { ok: true, dropped: true, queued: false, queueCount: await getQueueCount() };
    }

    try {
        const result = await trySendItem(lineItem, settings);
        if (result.ok) {
            pushActivity("sent", lineItem, { source: "live" });
            return { ok: true, sent: true, queued: false, queueCount: await getQueueCount() };
        }
    } catch (_error) {
    }

    const queueCount = await enqueue(lineItem);
    pushActivity("queued", lineItem, { source: "live" });
    return { ok: true, sent: false, queued: true, queueCount };
}

async function sendOrQueueReplayBulk(urls, teamId) {
    const cleanedUrls = Array.isArray(urls)
        ? urls.map((item) => String(item || "").trim()).filter(Boolean)
        : [];

    if (!cleanedUrls.length) {
        return { ok: false, reason: "empty-urls" };
    }

    const teamValue = Number(teamId);
    if (!Number.isFinite(teamValue) || teamValue <= 0) {
        return { ok: false, reason: "invalid-team" };
    }

    const settings = await getSettings();
    try {
        const result = await trySendItem({ type: "replay_bulk", urls: cleanedUrls, teamId: teamValue }, settings);
        if (result.ok) {
            pushActivity("sent", { type: "replay_bulk", urls: cleanedUrls, teamId: teamValue }, { source: "popup" });
            return {
                ok: true,
                sent: true,
                queued: false,
                summary: result.payload?.summary || null,
                queueCount: await getQueueCount(),
            };
        }
    } catch (_error) {
    }

    const queueCount = await enqueue({
        type: "replay_bulk",
        urls: cleanedUrls,
        teamId: teamValue,
    });
    pushActivity("queued", { type: "replay_bulk", urls: cleanedUrls, teamId: teamValue }, { source: "popup" });

    return { ok: true, sent: false, queued: true, queueCount };
}

function ensureFlushAlarm() {
    chrome.alarms.create(FLUSH_ALARM, { periodInMinutes: 1 });
}

chrome.runtime.onInstalled.addListener(() => {
    ensureFlushAlarm();
});

chrome.runtime.onStartup.addListener(() => {
    ensureFlushAlarm();
    flushQueue();
});

chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name !== FLUSH_ALARM) return;
    flushQueue();
});

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    const action = message?.action;

    if (action === "SEND_OR_QUEUE_LINE") {
        sendOrQueueLine(message?.line, message?.context)
            .then((result) => sendResponse(result))
            .catch(() => sendResponse({ ok: false }));
        return true;
    }

    if (action === "SEND_OR_QUEUE_REPLAY_BULK") {
        sendOrQueueReplayBulk(message?.urls, message?.teamId)
            .then((result) => sendResponse(result))
            .catch(() => sendResponse({ ok: false }));
        return true;
    }

    if (action === "FLUSH_QUEUE_NOW") {
        flushQueue()
            .then((result) => sendResponse({ ok: true, ...result }))
            .catch(() => sendResponse({ ok: false }));
        return true;
    }

    if (action === "QUEUE_STATUS") {
        getQueueCount()
            .then((count) => sendResponse({ ok: true, count }))
            .catch(() => sendResponse({ ok: false, count: 0 }));
        return true;
    }

    if (action === "RECENT_ACTIVITY") {
        const limit = Number(message?.limit) || 25;
        sendResponse({ ok: true, entries: getRecentActivity(limit) });
        return false;
    }

    if (action === "POKE") {
        getSettings()
            .then((settings) => trySendItem({ type: "poke", reason: message?.reason || "manual" }, settings))
            .then(() => sendResponse({ ok: true }))
            .catch(() => sendResponse({ ok: false }));
        return true;
    }

    return false;
});

ensureFlushAlarm();
flushQueue();
