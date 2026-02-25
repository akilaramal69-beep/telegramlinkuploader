// Initialize Telegram Web App (Safe)
const tg = window.Telegram?.WebApp || {
    expand: () => { },
    initDataUnsafe: {},
    HapticFeedback: { notificationOccurred: () => { } },
    close: () => { window.close(); }
};
if (window.Telegram?.WebApp) {
    tg.expand(); // Expand to maximum height
}

// UI Elements
const urlForm = document.getElementById('urlForm');
const linkInput = document.getElementById('linkInput');
const checkBtn = document.getElementById('checkBtn');
const scanLoader = document.getElementById('scanLoader');
const errorMsg = document.getElementById('errorMsg');

const qualitySection = document.getElementById('qualitySection');
const qualityList = document.getElementById('qualityList');

const modeSection = document.getElementById('modeSection');
const modeBtns = document.querySelectorAll('.mode-btn');
const finalDownloadBtn = document.getElementById('finalDownloadBtn');

const progressSection = document.getElementById('progressSection');
const progressAction = document.getElementById('progressAction');
const progressPercent = document.getElementById('progressPercent');
const progressFill = document.getElementById('progressFill');
const progressBytes = document.getElementById('progressBytes');
const progressSpeed = document.getElementById('progressSpeed');
const openTgBtn = document.getElementById('openTgBtn');
const cancelBtn = document.getElementById('cancelBtn');
const filenameInput = document.getElementById('filenameInput');


// ── Adsgram SDK Initialization ──────────────────────────────────────────────
let adController = null;
function initAds() {
    try {
        const blockId = window.APP_CONFIG?.adsgram_block_id || "int-23574";

        if (window.Adsgram) {
            adController = window.Adsgram.init({ blockId: blockId });
            console.log("Adsgram initialized with:", blockId);
        }
    } catch (e) {
        console.error("Adsgram init error:", e);
    }
}
initAds();

// State Variables
let selectedFormat = null;
let selectedMode = 'media';
let currentFormats = [];

// Helper: Show error
function showError(msg) {
    errorMsg.textContent = msg;
    errorMsg.classList.remove('hidden');
}

function hideError() {
    errorMsg.classList.add('hidden');
}

// Helper: Format bytes
function humanbytes(size) {
    if (!size) return "0 B";
    const units = ["B", "KB", "MB", "GB"];
    let i = 0;
    while (size >= 1024 && i < units.length - 1) {
        size /= 1024;
        i++;
    }
    return i === 0 ? `${size} ${units[i]}` : `${size.toFixed(2)} ${units[i]}`;
}

// ── Step 1: Scan URL and fetch formats ──
urlForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const url = linkInput.value.trim();
    if (!url) return showError("Please enter a valid URL.");

    hideError();
    qualitySection.classList.add('hidden');
    modeSection.classList.add('hidden');

    // UI Loading state
    checkBtn.querySelector('#btnText').innerText = "⏳ Processing...";
    scanLoader.classList.remove('hidden');
    checkBtn.disabled = true;

    try {
        const response = await fetch('/api/formats', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url: url })
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "Failed to fetch media details.");
        }

        currentFormats = data.formats || [];
        const suggestedTitle = data.title || "";
        renderQualityButtons(currentFormats, suggestedTitle);

    } catch (err) {
        showError(err.message);
    } finally {
        checkBtn.querySelector('#btnText').innerText = "🚀 Download";
        scanLoader.classList.add('hidden');
        checkBtn.disabled = false;
    }
});

// ── Step 2: Render Quality Buttons ──
function renderQualityButtons(formats, title) {
    qualityList.innerHTML = '';
    selectedFormat = null;

    // Set suggested filename in input
    if (title) {
        // Simple sanitization for display
        filenameInput.value = title.replace(/[\\/*?:"<>|]/g, "") + ".mp4";
    }

    if (formats.length === 0) {
        // Direct media links (no yt-dlp formats) skip straight to Mode Selection
        selectedFormat = "direct";
        qualitySection.classList.add('hidden');
        modeSection.classList.remove('hidden');
        return;
    }

    // Generate yt-dlp quality buttons
    formats.forEach(f => {
        const btn = document.createElement('button');
        btn.className = 'qual-btn';
        btn.textContent = `${f.resolution} (${humanbytes(f.filesize)})`;
        btn.dataset.id = f.format_id;

        btn.addEventListener('click', () => {
            document.querySelectorAll('.qual-btn').forEach(b => b.classList.remove('selected'));
            btn.classList.add('selected');
            selectedFormat = f.format_id;
            modeSection.classList.remove('hidden'); // Show mode options
        });

        qualityList.appendChild(btn);
    });

    // Add Best Quality Auto button
    const bestFmt = formats[0].format_id; // formats are pre-sorted highest to lowest by backend
    const bestBtn = document.createElement('button');
    bestBtn.className = 'qual-btn';
    bestBtn.textContent = '✨ Best Quality (Auto)';
    bestBtn.dataset.id = `best_${bestFmt}`;
    bestBtn.style.gridColumn = "1 / -1"; // Make it full width

    bestBtn.addEventListener('click', () => {
        document.querySelectorAll('.qual-btn').forEach(b => b.classList.remove('selected'));
        bestBtn.classList.add('selected');
        selectedFormat = `best_${bestFmt}`;
        modeSection.classList.remove('hidden');
    });

    qualityList.appendChild(bestBtn);
    qualitySection.classList.remove('hidden');
}

// ── Step 3: Handle Mode Selection ──
modeBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        modeBtns.forEach(b => b.classList.remove('mode-selected'));
        btn.classList.add('mode-selected');
        selectedMode = btn.dataset.mode;
    });
});

// ── Step 4: Submit Download Task to Backend ──
finalDownloadBtn.addEventListener('click', async () => {
    const url = linkInput.value.trim();

    // Identify Telegram User
    // If testing outside telegram, fallback to 0
    const user = tg.initDataUnsafe?.user;
    const userId = user ? user.id : 0;

    // Show Adsgram Interstitial before final download
    if (adController) {
        adController.show().then((result) => {
            console.log("Ad finished:", result);
            startFinalDownload(url, userId);
        }).catch((err) => {
            console.warn("Ad error/skip:", err);
            startFinalDownload(url, userId); // Proceed anyway
        });
    } else {
        startFinalDownload(url, userId);
    }
});

async function startFinalDownload(url, userId) {
    if (!url) return;

    finalDownloadBtn.innerText = "⏳ Submitting...";
    finalDownloadBtn.disabled = true;

    try {
        const response = await fetch('/api/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                url: url,
                chat_id: userId,
                format_id: selectedFormat,
                mode: selectedMode,
                filename: filenameInput.value.trim()
            })
        });

        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Failed to submit task.");

        // Show Live Progress UI
        urlForm.classList.add('hidden');
        qualitySection.classList.add('hidden');
        modeSection.classList.add('hidden');
        progressSection.classList.remove('hidden');

        // Notify Telegram we're done (triggers haptic feedback)
        tg.HapticFeedback.notificationOccurred('success');

        // Start polling loop
        startProgressPolling(userId);

        // Show cancel button at the start of process
        cancelBtn.classList.remove('hidden');
        cancelBtn.disabled = false;
        cancelBtn.innerText = "✖️ Cancel Process";

    } catch (err) {
        alert("Error: " + err.message);
        finalDownloadBtn.innerText = "🚀 Beam to Chat";
        finalDownloadBtn.disabled = false;
    }
}

// ── Step 6: Process Cancellation ──
cancelBtn.addEventListener('click', async () => {
    const user = tg.initDataUnsafe?.user;
    const userId = user ? user.id : 0;

    cancelBtn.innerText = "⏳ Cancelling...";
    cancelBtn.disabled = true;

    try {
        const response = await fetch('/api/cancel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user_id: userId })
        });
        if (response.ok) {
            tg.HapticFeedback.notificationOccurred('warning');
        }
    } catch (e) {
        console.error("Cancel API error:", e);
    }
});

// ── Step 5: Live Progress Polling ──
function startProgressPolling(userId) {
    const pollInterval = setInterval(async () => {
        try {
            const resp = await fetch(`/api/progress?user_id=${userId}`);
            if (!resp.ok) return;

            const data = await resp.json();

            if (data.action === "idle") return; // Not started yet

            // Check for error state from backend
            if (data.action.startsWith("Error:") || data.action.toLowerCase().includes("cancelled") || data.action.toLowerCase().includes("error")) {
                clearInterval(pollInterval);
                progressAction.innerText = data.action;
                progressAction.style.color = "var(--danger)";
                progressPercent.innerText = "❌";
                progressFill.style.background = "var(--danger)";
                cancelBtn.classList.add('hidden');
                tg.HapticFeedback.notificationOccurred('error');
                return;
            }

            // Update DOM
            progressAction.style.color = "var(--button-color)";

            // If at low percentage and just initializing, show Preparing
            let actionText = data.action;
            if (data.percentage < 5 && actionText.toLowerCase().includes("initializing")) {
                actionText = "🚀 Preparing Download...";
            }
            progressAction.innerText = actionText;

            const displayPct = parseFloat(data.percentage).toFixed(1);
            progressPercent.innerText = `${displayPct}%`;
            progressFill.style.transform = `scaleX(${data.percentage / 100})`;

            if (data.current) {
                progressBytes.innerText = `${data.current} / ${data.total}`;
            }
            if (data.speed) {
                progressSpeed.innerText = data.speed;
            }

            // Check for completion
            if (data.action === "Complete" || data.percentage >= 100) {
                clearInterval(pollInterval);
                progressAction.innerText = "File is ready in your chat! ✅";
                openTgBtn.classList.remove('hidden');
                cancelBtn.classList.add('hidden');

                // Clicking "Get Processed File" closes the Mini App so the user can see the file in chat
                openTgBtn.addEventListener('click', () => {
                    tg.close();
                });
                tg.HapticFeedback.notificationOccurred('success');
            }

        } catch (e) {
            console.error("Polling error", e);
        }
    }, 1500); // 1.5 second UI updates
}
