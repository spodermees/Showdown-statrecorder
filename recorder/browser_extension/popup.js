const LOCAL_BASE_URL = "http://127.0.0.1:5000";
const PREP_TEAMS_URL = `${LOCAL_BASE_URL}/api/prep_teams`;
const STORAGE_KEY_REPLAY_LIST = "replayList";
const STORAGE_KEY_TEAM_ID = "selectedTeamId";
const STORAGE_KEY_TEAM_NAME = "selectedTeamName";

const statusEl = document.getElementById("status");
const teamSelect = document.getElementById("team-select");
const replayInput = document.getElementById("replay-input");
const replayListInput = document.getElementById("replay-list");
const saveReplayButton = document.getElementById("save-replay");
const sendReplaysButton = document.getElementById("send-replays");
const activityListEl = document.getElementById("activity-list");
const activityEmptyEl = document.getElementById("activity-empty");
let activityIntervalId = null;

function setStatus(text, isError = false) {
    statusEl.textContent = text;
    statusEl.style.color = isError ? "#ffd0cc" : "#d5ffe3";
}

function sendMessage(message) {
    return new Promise((resolve) => {
        try {
            chrome.runtime.sendMessage(message, (response) => {
                if (chrome.runtime.lastError) {
                    resolve({ ok: false, error: chrome.runtime.lastError.message });
                    return;
                }
                resolve(response || { ok: false });
            });
        } catch (_error) {
            resolve({ ok: false });
        }
    });
}

function getStorage(keys) {
    return new Promise((resolve) => {
        chrome.storage.sync.get(keys, (result) => resolve(result || {}));
    });
}

function setStorage(values) {
    return new Promise((resolve) => {
        chrome.storage.sync.set(values, () => resolve());
    });
}

async function restore() {
    const data = await getStorage({
        [STORAGE_KEY_REPLAY_LIST]: "",
        [STORAGE_KEY_TEAM_ID]: "",
        [STORAGE_KEY_TEAM_NAME]: "",
    });
    replayListInput.value = (data[STORAGE_KEY_REPLAY_LIST] || "").trim();
    await loadTeams(data[STORAGE_KEY_TEAM_ID], data[STORAGE_KEY_TEAM_NAME]);
    await refreshQueueStatus();
    await refreshRecentActivity();
}

function formatTime(value) {
    const parsed = new Date(value || "");
    if (Number.isNaN(parsed.getTime())) {
        return "--:--:--";
    }
    return parsed.toLocaleTimeString();
}

function renderRecentActivity(entries) {
    activityListEl.innerHTML = "";
    const rows = Array.isArray(entries) ? entries : [];
    activityEmptyEl.style.display = rows.length ? "none" : "block";

    for (const entry of rows) {
        const item = document.createElement("li");
        item.className = "activity-item";

        const meta = document.createElement("div");
        meta.className = "activity-meta";

        const left = document.createElement("span");
        left.className = `activity-status ${String(entry?.status || "info").toLowerCase()}`;
        left.textContent = `${String(entry?.status || "info")} • ${String(entry?.source || "live")}`;

        const right = document.createElement("span");
        right.textContent = formatTime(entry?.time);

        meta.appendChild(left);
        meta.appendChild(right);

        const preview = document.createElement("div");
        preview.className = "activity-preview";
        const type = String(entry?.type || "unknown");
        const label = String(entry?.label || "LIVE").trim();
        const text = String(entry?.preview || "").trim();
        const prefix = `[${label}] [${type}]`;
        preview.textContent = text ? `${prefix} ${text}` : prefix;

        item.appendChild(meta);
        item.appendChild(preview);
        activityListEl.appendChild(item);
    }
}

async function refreshRecentActivity() {
    const response = await sendMessage({ action: "RECENT_ACTIVITY", limit: 30 });
    if (!response?.ok) {
        return;
    }
    renderRecentActivity(response.entries || []);
}

async function refreshQueueStatus() {
    const response = await sendMessage({ action: "QUEUE_STATUS" });
    if (!response?.ok) return;
    if (Number(response.count) > 0) {
        setStatus(`Wachtrij: ${response.count} pending`);
    }
}

function setTeamOptions(teams, preferredTeamId) {
    const preferred = String(preferredTeamId || "").trim();
    teamSelect.innerHTML = "";

    if (!teams.length) {
        const option = document.createElement("option");
        option.value = "";
        option.textContent = "Geen teams gevonden";
        teamSelect.appendChild(option);
        teamSelect.disabled = true;
        return;
    }

    teamSelect.disabled = false;
    for (const team of teams) {
        const option = document.createElement("option");
        option.value = String(team.id);
        option.textContent = team.name || `Team ${team.id}`;
        teamSelect.appendChild(option);
    }

    const hasPreferred = preferred && teams.some((team) => String(team.id) === preferred);
    teamSelect.value = hasPreferred ? preferred : String(teams[0].id);
}

async function loadTeams(preferredTeamId = "", preferredTeamName = "") {
    try {
        const response = await fetch(PREP_TEAMS_URL, { cache: "no-store" });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || !payload?.ok || !Array.isArray(payload?.teams)) {
            throw new Error("teams unavailable");
        }

        setTeamOptions(payload.teams, preferredTeamId);
        const selectedTeamId = String(teamSelect.value || "").trim();
        const selectedOption = teamSelect.selectedOptions?.[0];
        const selectedTeamName = String(selectedOption?.textContent || "").trim();
        if (selectedTeamId) {
            await setStorage({
                [STORAGE_KEY_TEAM_ID]: selectedTeamId,
                [STORAGE_KEY_TEAM_NAME]: selectedTeamName,
            });
        }
    } catch (_error) {
        const fallbackTeamId = String(preferredTeamId || "").trim();
        const fallbackTeamName = String(preferredTeamName || "").trim();
        if (fallbackTeamId) {
            teamSelect.innerHTML = "";
            const option = document.createElement("option");
            option.value = fallbackTeamId;
            option.textContent = fallbackTeamName
                ? `${fallbackTeamName} (offline)`
                : `Team ${fallbackTeamId} (offline)`;
            teamSelect.appendChild(option);
            teamSelect.disabled = false;
            teamSelect.value = fallbackTeamId;
            setStatus("Teams API offline, maar queue blijft beschikbaar.");
            return;
        }
        setTeamOptions([], "");
        setStatus("Kon teams niet laden. Start app.py of kies eerst een team.", true);
    }
}

function extractReplayUrls(text) {
    const pattern = /https?:\/\/\S+/gi;
    return Array.from(new Set((text.match(pattern) || []).map((item) => item.trim())));
}

async function persistReplayList() {
    const value = (replayListInput.value || "").trim();
    await setStorage({ [STORAGE_KEY_REPLAY_LIST]: value });
}

async function persistSelectedTeam() {
    const selectedTeamId = String(teamSelect.value || "").trim();
    const selectedOption = teamSelect.selectedOptions?.[0];
    const selectedTeamName = String(selectedOption?.textContent || "").trim();
    await setStorage({
        [STORAGE_KEY_TEAM_ID]: selectedTeamId,
        [STORAGE_KEY_TEAM_NAME]: selectedTeamName,
    });
}

async function saveReplay() {
    const inputText = (replayInput.value || "").trim();
    if (!inputText) {
        setStatus("Plak eerst replay URL(s).", true);
        return;
    }

    const foundUrls = extractReplayUrls(inputText);
    if (!foundUrls.length) {
        setStatus("Geen geldige replay URL gevonden.", true);
        return;
    }

    const currentLines = (replayListInput.value || "").split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    let added = 0;
    for (const replayUrl of foundUrls) {
        if (!currentLines.includes(replayUrl)) {
            currentLines.push(replayUrl);
            added += 1;
        }
    }

    replayListInput.value = currentLines.join("\n");
    replayInput.value = "";
    await persistReplayList();
    setStatus(`${added} replay(s) toegevoegd.`);
}

async function sendReplaysToApi() {
    const urls = extractReplayUrls(replayListInput.value || "");
    if (!urls.length) {
        setStatus("Geen replay URL's om te sturen.", true);
        return;
    }

    const selectedTeamId = String(teamSelect.value || "").trim();
    if (!selectedTeamId) {
        setStatus("Kies eerst een team.", true);
        return;
    }

    const result = await sendMessage({
        action: "SEND_OR_QUEUE_REPLAY_BULK",
        urls,
        teamId: Number(selectedTeamId),
    });

    if (!result?.ok) {
        setStatus("Kon replay request niet verwerken.", true);
        return;
    }

    replayListInput.value = "";
    await persistReplayList();

    if (result.sent) {
        const ok = result?.summary?.ok ?? urls.length;
        const failed = result?.summary?.failed ?? 0;
        setStatus(`Verstuurd: ${ok} ok, ${failed} failed.`);
    } else if (result.queued) {
        setStatus(`API offline: opgeslagen in wachtrij (${result.queueCount}).`);
    }
    await refreshRecentActivity();
}

async function flushQueueNow() {
    const response = await sendMessage({ action: "FLUSH_QUEUE_NOW" });
    if (!response?.ok) {
        setStatus("Wachtrij flush mislukt.", true);
        await refreshRecentActivity();
        return;
    }
    if (response.remaining > 0) {
        setStatus(`Nog ${response.remaining} in wachtrij.`);
        await refreshRecentActivity();
        return;
    }
    if (response.sent > 0) {
        setStatus(`Wachtrij verstuurd (${response.sent}).`);
        await refreshRecentActivity();
        return;
    }
    setStatus("Geen pending items in wachtrij.");
    await refreshRecentActivity();
}
saveReplayButton.addEventListener("click", saveReplay);
sendReplaysButton.addEventListener("click", sendReplaysToApi);
replayListInput.addEventListener("blur", persistReplayList);
teamSelect.addEventListener("change", persistSelectedTeam);
window.addEventListener("focus", flushQueueNow);
window.addEventListener("focus", refreshRecentActivity);
chrome.runtime.onMessage.addListener((message) => {
    if (message?.action !== "ACTIVITY_PUSH") {
        return;
    }
    refreshRecentActivity();
});
window.addEventListener("load", () => {
    if (activityIntervalId) {
        clearInterval(activityIntervalId);
    }
    activityIntervalId = setInterval(refreshRecentActivity, 1500);
});
window.addEventListener("unload", () => {
    if (!activityIntervalId) {
        return;
    }
    clearInterval(activityIntervalId);
    activityIntervalId = null;
});
restore();
