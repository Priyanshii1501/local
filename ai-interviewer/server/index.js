const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const { WebSocketServer } = require('ws');
const PQueue = require('p-queue');
const { z } = require('zod');
const OpenAI = require('openai');

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '5mb' }));
app.use(express.static('public'));

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const FrameSchema = z.object({
	imageBase64: z.string(),
	timestamp: z.number(),
	sessionId: z.string(),
	cursorText: z.string().optional(),
	language: z.string().optional(),
});

const SummarySchema = z.object({
	sessionId: z.string(),
});

const sessions = new Map();

function getOrCreateSession(sessionId) {
	if (!sessions.has(sessionId)) {
		sessions.set(sessionId, {
			queue: new PQueue({ concurrency: 1, interval: 1000, intervalCap: 2 }),
			observations: [],
		});
	}
	return sessions.get(sessionId);
}

async function generateObservation({ imageBase64, cursorText }) {
	const systemPrompt = `You are an AI technical interviewer observing a live coding session. Follow the required output format strictly:\n\nOutput Format While Interview is Running:\n- Observation: Your live observation of the code.\n- Question/Challenge: One precise question or challenge related to the code you are seeing.\n- Hint (Optional): A gentle nudge if the candidate seems stuck.\n\nKeep it concise and actionable. Ask exactly one question at a time.`;

	const response = await client.chat.completions.create({
		model: 'gpt-4o-mini',
		messages: [
			{ role: 'system', content: systemPrompt },
			{
				role: 'user',
				content: [
					{ type: 'text', text: cursorText || 'No extra context.' },
					{ type: 'image_url', image_url: { url: `data:image/jpeg;base64,${imageBase64}` } },
				],
			},
		],
		temperature: 0.4,
	});
	return response.choices[0]?.message?.content || '';
}

async function generateSummary(observations) {
	const systemPrompt = `You are an AI technical interviewer. Produce the Final Output After Interview with the following structure:\n\nSummary: High-level evaluation of performance.\n\nDetailed Feedback: Line-by-line or section-based review of the code.\n\nRating: Score out of 10 for:\n- Problem Understanding\n- Code Quality\n- Algorithm Efficiency\n- Communication\n`;
	const userText = observations.join('\n---\n');
	const response = await client.chat.completions.create({
		model: 'gpt-4o-mini',
		messages: [
			{ role: 'system', content: systemPrompt },
			{ role: 'user', content: userText },
		],
		temperature: 0.3,
	});
	return response.choices[0]?.message?.content || '';
}

app.post('/api/frame', async (req, res) => {
	try {
		const parsed = FrameSchema.parse(req.body);
		const session = getOrCreateSession(parsed.sessionId);
		const job = session.queue.add(async () => {
			const text = await generateObservation({
				imageBase64: parsed.imageBase64,
				cursorText: parsed.cursorText,
			});
			session.observations.push(text);
			return text;
		});
		const text = await job;
		res.json({ ok: true, text });
	} catch (err) {
		console.error(err);
		res.status(400).json({ ok: false, error: err?.message || 'Bad Request' });
	}
});

app.post('/api/summary', async (req, res) => {
	try {
		const parsed = SummarySchema.parse(req.body);
		const session = getOrCreateSession(parsed.sessionId);
		const text = await generateSummary(session.observations);
		res.json({ ok: true, text });
	} catch (err) {
		console.error(err);
		res.status(400).json({ ok: false, error: err?.message || 'Bad Request' });
	}
});

const server = app.listen(process.env.PORT || 8787, () => {
	console.log('Server listening on http://localhost:' + (process.env.PORT || 8787));
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
	ws.on('message', async (raw) => {
		try {
			const data = JSON.parse(raw.toString());
			if (data.type === 'observe') {
				const { sessionId, imageBase64, cursorText } = data;
				const session = getOrCreateSession(sessionId);
				const text = await session.queue.add(() =>
					generateObservation({ imageBase64, cursorText })
				);
				session.observations.push(text);
				ws.send(JSON.stringify({ type: 'observation', text }));
			} else if (data.type === 'summary') {
				const { sessionId } = data;
				const session = getOrCreateSession(sessionId);
				const text = await generateSummary(session.observations);
				ws.send(JSON.stringify({ type: 'summary', text }));
			}
		} catch (e) {
			ws.send(JSON.stringify({ type: 'error', error: e?.message || 'invalid' }));
		}
	});
});