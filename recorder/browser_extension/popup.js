const LOCAL_BASE_URL = "http://127.0.0.1:5000";
const POKE_URL = `${LOCAL_BASE_URL}/api/poke`;
const PREP_TEAMS_URL = `${LOCAL_BASE_URL}/api/prep_teams`;
const REPLAY_BULK_URL = `${LOCAL_BASE_URL}/api/ingest_replay_bulk`;
const STORAGE_KEY_REPLAY_LIST = "replayList";
const STORAGE_KEY_TEAM_ID = "selectedTeamId";

const statusEl = document.getElementById("status");
const teamSelect = document.getElementById("team-select");
const replayInput = document.getElementById("replay-input");
const replayListInput = document.getElementById("replay-list");
const saveReplayButton = document.getElementById("save-replay");
const sendReplaysButton = document.getElementById("send-replays");

function setStatus(text, isError = false) {
    statusEl.textContent = text;
    statusEl.style.color = isError ? "#ff9d9d" : "#9ad08f";
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

async function isLocalAppRunning() {
    try {
        const response = await fetch(POKE_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ source: "popup", reason: "send-replays" }),
            cache: "no-store",
        });
        return response.ok;
    } catch (_error) {
        return false;
    }
}

async function restore() {
    const data = await getStorage({
        [STORAGE_KEY_REPLAY_LIST]: "",
        [STORAGE_KEY_TEAM_ID]: "",
    });
    replayListInput.value = (data[STORAGE_KEY_REPLAY_LIST] || "").trim();
    await loadTeams(data[STORAGE_KEY_TEAM_ID]);
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

async function loadTeams(preferredTeamId = "") {
    try {
        const response = await fetch(PREP_TEAMS_URL, { cache: "no-store" });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || !payload?.ok || !Array.isArray(payload?.teams)) {
            throw new Error("teams unavailable");
        }

        setTeamOptions(payload.teams, preferredTeamId);
        const selectedTeamId = String(teamSelect.value || "").trim();
        if (selectedTeamId) {
            await setStorage({ [STORAGE_KEY_TEAM_ID]: selectedTeamId });
        }
    } catch (_error) {
        setTeamOptions([], "");
        setStatus("Kon teams niet laden. Draait app.py?", true);
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
    await setStorage({ [STORAGE_KEY_TEAM_ID]: selectedTeamId });
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

    const appRunning = await isLocalAppRunning();
    if (!appRunning) {
        setStatus("app.py draait niet. Start met: python app.py", true);
        return;
    }

    try {
        const response = await fetch(REPLAY_BULK_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ urls, team_id: Number(selectedTeamId) }),
            cache: "no-store",
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || payload.status !== "ok") {
            const message = payload?.message || "Upload mislukt.";
            setStatus(message, true);
            return;
        }

        replayListInput.value = "";
        await persistReplayList();
        const ok = payload?.summary?.ok ?? 0;
        const failed = payload?.summary?.failed ?? 0;
        setStatus(`Verstuurd: ${ok} ok, ${failed} failed.`);
    } catch (_error) {
        setStatus("Kon API niet bereiken.", true);
    }
}
saveReplayButton.addEventListener("click", saveReplay);
sendReplaysButton.addEventListener("click", sendReplaysToApi);
replayListInput.addEventListener("blur", persistReplayList);
teamSelect.addEventListener("change", persistSelectedTeam);
restore();
