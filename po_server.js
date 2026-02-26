const express = require('express');
const { generate } = require('youtube-po-token-generator');

const app = express();
const PORT = 4416;

// To support JSON body parsing if yt-dlp happens to send any
app.use(express.json());

// Background utility plugin for yt-dlp expects /ping to return 200 OK
app.get('/ping', (req, res) => {
    res.status(200).send('pong');
});

// The main endpoint the plugin fetches
app.post('/', async (req, res) => {
    try {
        console.log(`[PO Server] Request received for token. Generating...`);
        // generate() returns { poToken: "...", visitorData: "..." }
        const result = await generate();
        console.log(`[PO Server] Token successfully generated.`);
        res.status(200).json(result);
    } catch (error) {
        console.error(`[PO Server] Error generating token:`, error);
        res.status(500).json({ error: error.toString() });
    }
});

// Explicitly define the other endpoint formats in case bgutil switches
app.post('/v1/pots', async (req, res) => {
    try {
        console.log(`[PO Server] Request received at /v1/pots. Generating...`);
        const result = await generate();
        console.log(`[PO Server] Token successfully generated.`);
        res.status(200).json(result);
    } catch (error) {
        console.error(`[PO Server] Error generating token:`, error);
        res.status(500).json({ error: error.toString() });
    }
});

app.listen(PORT, '127.0.0.1', () => {
    console.log(`[PO Server] YouTube Proof-of-Origin Token server running on http://127.0.0.1:${PORT}`);
});
