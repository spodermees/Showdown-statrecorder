// ==UserScript==
// @name         Showdown Live Log Sender
// @namespace    https://github.com/spodermees
// @version      1.0.0
// @description  Streams Pokemon Showdown battle log lines to local recorder.
// @match        https://play.pokemonshowdown.com/*
// @match        https://psim.us/*
// @grant        none
// @run-at       document-idle
// ==/UserScript==

(() => {
    const API_URL = "http://127.0.0.1:5000/api/ingest_line";
    const lastLineByNode = new WeakMap();
    let sentCount = 0;

    function ensureBadge() {
        let badge = document.getElementById("ps-log-sender");
        if (!badge) {
            badge = document.createElement("div");
            badge.id = "ps-log-sender";
            badge.style.cssText = "position:fixed;bottom:12px;right:12px;padding:6px 10px;background:#1f2538;color:#e6e9f2;border:1px solid #2b334b;border-radius:10px;font:12px/1.2 Segoe UI, sans-serif;z-index:99999;";
            badge.textContent = "Log sender: idle";
            (document.body || document.documentElement).appendChild(badge);
        }
        return badge;
    }

    function cleanLine(text) {
        return text.replace(/\s+/g, " ").trim();
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

    async function sendLine(line) {
        if (!line) return;
        try {
            await fetch(API_URL, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ line })
            });
            sentCount += 1;
            const badge = ensureBadge();
            badge.textContent = `Log sender: sent ${sentCount}`;
        } catch (err) {
            const badge = ensureBadge();
            badge.textContent = "Log sender: error";
            console.warn("Showdown log sender error", err);
        }
    }

    function readLogLines(logRoot) {
        const nodes = logRoot.querySelectorAll(".chat, .chatmessage, .message, h2, p, li, div");
        nodes.forEach((node) => {
            if (!isLeafMessage(node)) return;
            const line = cleanLine(node.textContent || "");
            if (!line || line.length < 2) return;
            const lastLine = lastLineByNode.get(node);
            if (lastLine === line) return;
            lastLineByNode.set(node, line);
            sendLine(line);
        });
    }

    function attachObserver(logRoot) {
        readLogLines(logRoot);
        const observer = new MutationObserver(() => readLogLines(logRoot));
        observer.observe(logRoot, { childList: true, subtree: true, characterData: true });
        ensureBadge().textContent = "Log sender: watching";
    }

    function findLogRoot() {
        return document.querySelector(".battle-log") || document.querySelector(".battle-log") || document.querySelector(".chatlog") || document.querySelector(".chat") || null;
    }

    console.log("Showdown log sender loaded");

    const interval = setInterval(() => {
        const logRoot = findLogRoot();
        if (logRoot) {
            clearInterval(interval);
            attachObserver(logRoot);
        }
    }, 1000);
})();
