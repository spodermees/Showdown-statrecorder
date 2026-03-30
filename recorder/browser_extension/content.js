(() => {
    const STORAGE_KEY_AUTO_TRACK = "autoTrack";
    const DEFAULT_AUTO_TRACK = true;
    const TURN_LINE_PATTERN = /^Turn\s+\d+\b/i;
    const BATTLE_END_PATTERN = /\bwon the battle!?$/i;
    const LINE_NODE_SELECTOR = ".chat, .chatmessage, .message, .battle-history, .battle-log-message, h2, p, li, div";
    let sentCount = 0;
    let observer = null;
    let activeRoot = null;
    let autoTrackEnabled = DEFAULT_AUTO_TRACK;
    let lastLineByNode = new WeakMap();
    let pendingLeadLines = [];
    let pendingTurnLines = [];
    let pendingLeadFlushTimer = null;
    let pendingTurnFlushTimer = null;
    let sendChain = Promise.resolve();
    let activeTurnNumber = null;
    let hasSeenTurnStart = false;

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

    function ensureBadge() {
        let badge = document.getElementById("euic-recorder-bridge-badge");
        if (!badge) {
            badge = document.createElement("div");
            badge.id = "euic-recorder-bridge-badge";
            badge.style.cssText = [
                "position:fixed",
                "bottom:12px",
                "right:12px",
                "padding:6px 10px",
                "background:#1f2538",
                "color:#e6e9f2",
                "border:1px solid #2b334b",
                "border-radius:10px",
                "font:12px/1.2 Segoe UI, sans-serif",
                "z-index:2147483647",
                "pointer-events:none",
                "opacity:0.92"
            ].join(";");
            badge.textContent = "Recorder bridge: idle";
            (document.body || document.documentElement).appendChild(badge);
        }
        return badge;
    }

    function setBadge(text) {
        const extras = [];
        if (pendingLeadLines.length) {
            extras.push(`leads ${pendingLeadLines.length}`);
        }
        if (activeTurnNumber !== null) {
            extras.push(`turn ${activeTurnNumber}`);
        }
        if (pendingTurnLines.length) {
            extras.push(`queued ${pendingTurnLines.length}`);
        }
        const suffix = extras.length ? ` | ${extras.join(" | ")}` : "";
        ensureBadge().textContent = `${text}${suffix}`;
    }

    function cleanLine(text) {
        return String(text || "").replace(/\s+/g, " ").trim();
    }

    function isLeafMessage(node) {
        for (const child of node.children) {
            const tag = child.tagName.toLowerCase();
            if (["div", "p", "h1", "h2", "h3", "ul", "ol", "li", "table"].includes(tag)) {
                return false;
            }
        }
        return true;
    }

    function readAutoTrackSetting() {
        return new Promise((resolve) => {
            chrome.storage.sync.get({ [STORAGE_KEY_AUTO_TRACK]: DEFAULT_AUTO_TRACK }, (result) => {
                resolve(result[STORAGE_KEY_AUTO_TRACK] !== false);
            });
        });
    }

    function stopWatching() {
        flushPendingLeadLines();
        flushPendingTurnLines();
        if (observer) {
            observer.disconnect();
            observer = null;
        }
        if (pendingLeadFlushTimer) {
            clearTimeout(pendingLeadFlushTimer);
            pendingLeadFlushTimer = null;
        }
        if (pendingTurnFlushTimer) {
            clearTimeout(pendingTurnFlushTimer);
            pendingTurnFlushTimer = null;
        }
        pendingLeadLines = [];
        pendingTurnLines = [];
        lastLineByNode = new WeakMap();
        activeTurnNumber = null;
        hasSeenTurnStart = false;
        activeRoot = null;
    }

    function extractTurnNumber(line) {
        const match = String(line || "").match(TURN_LINE_PATTERN);
        if (!match) {
            return null;
        }
        const turn = Number(match[0].replace(/\D+/g, ""));
        return Number.isFinite(turn) ? turn : null;
    }

    async function pokeRecorder(reason) {
        await sendMessage({ action: "POKE", reason });
    }

    async function sendLine(line, context = null) {
        if (!line) {
            return;
        }

        const result = await sendMessage({ action: "SEND_OR_QUEUE_LINE", line, context });
        if (result?.ok && result?.sent) {
            sentCount += 1;
            setBadge(`Recorder bridge: sent ${sentCount}`);
            return;
        }

        if (result?.ok && result?.queued) {
            setBadge(`Recorder bridge: queued ${result.queueCount}`);
            return;
        }

        if (result?.ok && result?.dropped) {
            setBadge("Recorder bridge: disabled");
            return;
        }

        setBadge("Recorder bridge: error");
    }

    function queueLines(items, defaultContext = null) {
        if (!Array.isArray(items) || !items.length) {
            return;
        }
        sendChain = sendChain.then(async () => {
            for (const item of items) {
                if (typeof item === "string") {
                    await sendLine(item, defaultContext);
                    continue;
                }
                await sendLine(item.line, item.context || defaultContext);
            }
        });
    }

    function flushPendingTurnLines() {
        if (!pendingTurnLines.length) {
            return;
        }
        const turnNumber = activeTurnNumber;
        const toSend = pendingTurnLines.map((line) => ({
            line,
            context: { phase: "turn", turn: turnNumber },
        }));
        pendingTurnLines = [];
        queueLines(toSend);
        activeTurnNumber = null;
        setBadge(`Recorder bridge: sent ${sentCount}`);
    }

    function flushPendingLeadLines() {
        if (!pendingLeadLines.length) {
            return;
        }
        const toSend = pendingLeadLines.map((line) => ({
            line,
            context: { phase: "leads" },
        }));
        pendingLeadLines = [];
        queueLines(toSend);
        setBadge(`Recorder bridge: sent ${sentCount}`);
    }

    function schedulePendingLeadFlush() {
        if (pendingLeadFlushTimer) {
            clearTimeout(pendingLeadFlushTimer);
        }
        pendingLeadFlushTimer = setTimeout(() => {
            pendingLeadFlushTimer = null;
            flushPendingLeadLines();
        }, 500);
    }

    function schedulePendingTurnFlush() {
        if (pendingTurnFlushTimer) {
            clearTimeout(pendingTurnFlushTimer);
        }
        pendingTurnFlushTimer = setTimeout(() => {
            pendingTurnFlushTimer = null;
            flushPendingTurnLines();
        }, 500);
    }

    function nodeToLine(node) {
        if (!node || !(node instanceof Element)) {
            return null;
        }
        if (!node.matches(LINE_NODE_SELECTOR)) {
            return null;
        }
        if (!isLeafMessage(node)) {
            return null;
        }
        const line = cleanLine(node.textContent);
        if (!line || line.length < 2) {
            return null;
        }
        const lastLine = lastLineByNode.get(node);
        if (lastLine === line) {
            return null;
        }
        lastLineByNode.set(node, line);
        return line;
    }

    function extractLinesFromNodes(nodes) {
        const lines = [];
        for (const node of nodes) {
            if (!(node instanceof Element)) {
                continue;
            }

            const ownLine = nodeToLine(node);
            if (ownLine) {
                lines.push(ownLine);
            }

            const descendants = node.querySelectorAll(LINE_NODE_SELECTOR);
            for (const child of descendants) {
                const childLine = nodeToLine(child);
                if (!childLine) {
                    continue;
                }
                lines.push(childLine);
            }
        }
        return lines;
    }

    function processTurnBuffer(lines) {
        for (const line of lines) {
            if (TURN_LINE_PATTERN.test(line)) {
                hasSeenTurnStart = true;
                flushPendingLeadLines();
                flushPendingTurnLines();
                pendingTurnLines = [line];
                activeTurnNumber = extractTurnNumber(line);
                setBadge("Recorder bridge: watching");
                continue;
            }

            if (!hasSeenTurnStart) {
                pendingLeadLines.push(line);
                setBadge("Recorder bridge: watching");
                if (BATTLE_END_PATTERN.test(line)) {
                    flushPendingLeadLines();
                }
                continue;
            }

            if (pendingTurnLines.length) {
                pendingTurnLines.push(line);
                setBadge("Recorder bridge: watching");
                if (BATTLE_END_PATTERN.test(line)) {
                    flushPendingTurnLines();
                }
                continue;
            }

            queueLines([{ line, context: { phase: "live" } }]);
        }

        if (pendingTurnLines.length) {
            schedulePendingTurnFlush();
            return;
        }

        if (pendingLeadLines.length) {
            schedulePendingLeadFlush();
        }
    }

    function collectLinesFromLog(logRoot) {
        const lines = extractLinesFromNodes([logRoot]);
        if (!lines.length) {
            return;
        }
        processTurnBuffer(lines);
    }

    function collectLinesFromMutations(mutationList) {
        const candidateNodes = [];
        for (const mutation of mutationList) {
            if (mutation.type === "childList") {
                for (const added of mutation.addedNodes) {
                    if (added instanceof Element) {
                        candidateNodes.push(added);
                    } else if (added?.parentElement) {
                        candidateNodes.push(added.parentElement);
                    }
                }
            }
            if (mutation.type === "characterData") {
                if (mutation.target?.parentElement) {
                    candidateNodes.push(mutation.target.parentElement);
                }
            }
        }
        if (!candidateNodes.length) {
            return;
        }
        const lines = extractLinesFromNodes(candidateNodes);
        if (!lines.length) {
            return;
        }
        processTurnBuffer(lines);
    }

    function findLogRoot() {
        return (
            document.querySelector(".battle-log") ||
            document.querySelector(".chatlog") ||
            document.querySelector(".battle-history") ||
            document.querySelector(".chat")
        );
    }

    function watchLogRoot(logRoot) {
        flushPendingLeadLines();
        flushPendingTurnLines();
        if (pendingLeadFlushTimer) {
            clearTimeout(pendingLeadFlushTimer);
            pendingLeadFlushTimer = null;
        }
        if (pendingTurnFlushTimer) {
            clearTimeout(pendingTurnFlushTimer);
            pendingTurnFlushTimer = null;
        }
        pendingLeadLines = [];
        pendingTurnLines = [];
        activeTurnNumber = null;
        hasSeenTurnStart = false;
        collectLinesFromLog(logRoot);
        const nextObserver = new MutationObserver((mutationList) => collectLinesFromMutations(mutationList));
        nextObserver.observe(logRoot, { childList: true, subtree: true, characterData: true });
        setBadge("Recorder bridge: watching");
        return nextObserver;
    }

    function startWatchingIfPossible() {
        if (!autoTrackEnabled) {
            return;
        }
        const root = findLogRoot();
        if (!root) {
            setBadge("Recorder bridge: wacht op log");
            return;
        }
        if (root === activeRoot && observer) {
            return;
        }

        activeRoot = root;
        if (observer) {
            observer.disconnect();
        }
        observer = watchLogRoot(root);
    }

    async function boot() {
        ensureBadge();
        pokeRecorder("boot");

        autoTrackEnabled = await readAutoTrackSetting();
        if (!autoTrackEnabled) {
            stopWatching();
            setBadge("Recorder bridge: auto-track uit");
        } else {
            startWatchingIfPossible();
        }

        chrome.storage.onChanged.addListener((changes, area) => {
            if (area !== "sync" || !changes[STORAGE_KEY_AUTO_TRACK]) {
                return;
            }

            autoTrackEnabled = changes[STORAGE_KEY_AUTO_TRACK].newValue !== false;
            if (!autoTrackEnabled) {
                stopWatching();
                setBadge("Recorder bridge: auto-track uit");
                return;
            }

            startWatchingIfPossible();
        });

        setInterval(() => {
            if (!autoTrackEnabled) {
                readAutoTrackSetting().then((enabled) => {
                    if (!enabled) {
                        return;
                    }
                    autoTrackEnabled = true;
                    startWatchingIfPossible();
                });
                return;
            }

            startWatchingIfPossible();
        }, 1000);
    }

    boot();
})();
