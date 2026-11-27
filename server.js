require("dotenv").config();
const express = require("express");
const crypto = require("crypto");

const app = express();

const PORT = process.env.PORT || 3000;
const BLACKPIX_BASE_URL = "https://blackpix.site/api/integrations/v1";
const BLACKPIX_API_KEY = process.env.BLACKPIX_API_KEY;
const BLACKPIX_WEBHOOK_SECRET = process.env.BLACKPIX_WEBHOOK_SECRET;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";

// Armazenamento em memória para exemplo (em produção, use banco de dados como Supabase/Postgres)
const idempotencyStore = new Map(); // orderOrRefKey -> uuid v4
const processedEventIds = new Set(); // event.id já processados
const ordersByDepositId = new Map(); // deposit.id -> { id, amountCents, status, paidAt, eventId }

// CORS simples para permitir chamadas do seu front-end (index.html)
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", ALLOWED_ORIGIN);
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization,Idempotency-Key");
  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }
  next();
});

// Guarda o corpo bruto EXATO para validação HMAC do webhook antes de JSON.parse
app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf.toString("utf8");
    }
  })
);

function generateUuidV4() {
  return crypto.randomUUID();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function sanitizeDescription(desc) {
  if (typeof desc !== "string") return undefined;
  const cleaned = desc.replace(/[\r\n]+/g, " ").trim();
  if (!cleaned) return undefined;
  return cleaned.slice(0, 140);
}

function getOrCreateIdempotencyKey(keyRef) {
  if (keyRef && idempotencyStore.has(keyRef)) {
    return idempotencyStore.get(keyRef);
  }
  const newKey = generateUuidV4();
  if (keyRef) {
    idempotencyStore.set(keyRef, newKey);
  }
  return newKey;
}

function mapBlackPixError(status, payload) {
  const errorMessage =
    (payload && typeof payload.error === "string" && payload.error) ||
    "Erro na comunicacao com a BlackPix";
  const code =
    (payload && typeof payload.code === "string" && payload.code) ||
    `HTTP_${status}`;

  return {
    httpStatus: status >= 400 && status < 600 ? status : 502,
    body: {
      success: false,
      error: errorMessage,
      code
    }
  };
}

/**
 * Cria cobrança Pix com idempotência e repetição segura (mesmo UUID em falhas de rede, 429 ou 503).
 * Nunca expõe BLACKPIX_API_KEY nem registra pixCopyPasteCode completo nos logs.
 */
async function criarCobrancaPix({ amountCents, description, orderRef }) {
  if (!BLACKPIX_API_KEY || !BLACKPIX_API_KEY.startsWith("bpx_live_")) {
    const err = new Error("Chave BLACKPIX_API_KEY nao configurada corretamente no servidor");
    err.statusCode = 500;
    err.code = "MISSING_API_KEY";
    throw err;
  }

  if (!Number.isInteger(amountCents) || amountCents <= 0) {
    const err = new Error("amountCents deve ser um numero inteiro positivo em centavos");
    err.statusCode = 400;
    err.code = "INVALID_AMOUNT";
    throw err;
  }

  const bodyObj = { amountCents };
  const cleanDesc = sanitizeDescription(description);
  if (cleanDesc) {
    bodyObj.description = cleanDesc;
  }

  const jsonBody = JSON.stringify(bodyObj);
  const idempotencyKey = getOrCreateIdempotencyKey(orderRef || `${amountCents}:${cleanDesc || ""}`);

  const maxAttempts = 4;
  let attempt = 0;

  while (attempt < maxAttempts) {
    attempt += 1;
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 12000);

      const response = await fetch(`${BLACKPIX_BASE_URL}/deposits`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${BLACKPIX_API_KEY}`,
          "Content-Type": "application/json",
          "Idempotency-Key": idempotencyKey
        },
        body: jsonBody,
        signal: controller.signal
      });

      clearTimeout(timeout);

      let data = null;
      try {
        data = await response.json();
      } catch (_parseErr) {
        data = null;
      }

      if (response.status === 201 || response.status === 200) {
        if (data && data.deposit && data.deposit.id) {
          ordersByDepositId.set(data.deposit.id, {
            depositId: data.deposit.id,
            amountCents: data.deposit.amountCents,
            status: data.deposit.status,
            paidAt: data.deposit.paidAt || null,
            description: data.deposit.description || null
          });
        }
        console.log(
          `[PIX] Cobranca processada | statusHttp=${response.status} | reused=${Boolean(data && data.reused)} | depositId=${data && data.deposit ? data.deposit.id : "desconhecido"}`
        );
        return data;
      }

      // 429: respeita Retry-After e repete com o MESMO UUID
      if (response.status === 429 && attempt < maxAttempts) {
        const retryAfterSec = Number(response.headers.get("Retry-After"));
        const waitMs = Number.isFinite(retryAfterSec) && retryAfterSec > 0
          ? Math.min(retryAfterSec * 1000, 15000)
          : attempt * 1000;
        console.warn(`[PIX] Limite 429 atingido | tentativa=${attempt} | aguardandoMs=${waitMs}`);
        await sleep(waitMs);
        continue;
      }

      // 503: indisponível ou resultado incerto -> repete com o MESMO UUID e espera crescente
      if (response.status === 503 && attempt < maxAttempts) {
        const waitMs = attempt * 1000;
        console.warn(`[PIX] Indisponivel 503 | tentativa=${attempt} | aguardandoMs=${waitMs}`);
        await sleep(waitMs);
        continue;
      }

      // 400, 401, 403, 404, 409 ou outros erros não repetíveis sem correção
      const mapped = mapBlackPixError(response.status, data);
      console.error(`[PIX] Erro na API | httpStatus=${mapped.httpStatus} | code=${mapped.body.code}`);
      const err = new Error(mapped.body.error);
      err.statusCode = mapped.httpStatus;
      err.code = mapped.body.code;
      throw err;
    } catch (networkOrErr) {
      if (networkOrErr.statusCode) {
        throw networkOrErr;
      }
      console.warn(`[PIX] Falha de rede/tempo esgotado | tentativa=${attempt} | idempotencyKey=${idempotencyKey}`);
      if (attempt < maxAttempts) {
        await sleep(attempt * 1000);
        continue;
      }
      const err = new Error("Falha de comunicacao com a BlackPix apos tentativas de repeticao");
      err.statusCode = 503;
      err.code = "NETWORK_ERROR";
      throw err;
    }
  }
}

/**
 * Consulta um depósito específico pelo ID ou lista depósitos com filtros opcionais.
 */
async function consultarDepositoPix({ depositId, status, limit, startingAfter }) {
  if (!BLACKPIX_API_KEY || !BLACKPIX_API_KEY.startsWith("bpx_live_")) {
    const err = new Error("Chave BLACKPIX_API_KEY nao configurada corretamente no servidor");
    err.statusCode = 500;
    err.code = "MISSING_API_KEY";
    throw err;
  }

  let url = `${BLACKPIX_BASE_URL}/deposits`;
  if (depositId) {
    url = `${BLACKPIX_BASE_URL}/deposits/${encodeURIComponent(depositId)}`;
  } else {
    const params = new URLSearchParams();
    if (status) params.set("status", status);
    if (limit !== undefined) params.set("limit", String(limit));
    if (startingAfter) params.set("startingAfter", startingAfter);
    const qs = params.toString();
    if (qs) url += `?${qs}`;
  }

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${BLACKPIX_API_KEY}`
    }
  });

  let data = null;
  try {
    data = await response.json();
  } catch (_err) {
    data = null;
  }

  if (!response.ok) {
    const mapped = mapBlackPixError(response.status, data);
    console.error(`[PIX] Erro na consulta | httpStatus=${mapped.httpStatus} | code=${mapped.body.code}`);
    const err = new Error(mapped.body.error);
    err.statusCode = mapped.httpStatus;
    err.code = mapped.body.code;
    throw err;
  }

  return data;
}

/**
 * Valida o cabeçalho X-BlackPix-Signature no formato "t=<segundos>,v1=<hex>"
 * usando HMAC-SHA256 sobre "<t>.<corpo bruto>" e comparação em tempo constante.
 */
function verifyBlackPixSignature(rawBody, signatureHeader, secret) {
  if (!secret || typeof signatureHeader !== "string" || typeof rawBody !== "string") {
    return false;
  }

  const parts = signatureHeader.split(",");
  let timestampStr = null;
  let v1Hex = null;

  for (const part of parts) {
    const trimmed = part.trim();
    if (trimmed.startsWith("t=")) {
      timestampStr = trimmed.slice(2);
    } else if (trimmed.startsWith("v1=")) {
      v1Hex = trimmed.slice(3);
    }
  }

  if (!timestampStr || !v1Hex) {
    return false;
  }

  const timestampSec = Number(timestampStr);
  if (!Number.isFinite(timestampSec)) {
    return false;
  }

  const nowSec = Math.floor(Date.now() / 1000);
  if (Math.abs(nowSec - timestampSec) > 300) {
    return false;
  }

  const signedPayload = `${timestampStr}.${rawBody}`;
  const expectedHex = crypto
    .createHmac("sha256", secret)
    .update(signedPayload)
    .digest("hex");

  const receivedBuf = Buffer.from(v1Hex, "utf8");
  const expectedBuf = Buffer.from(expectedHex, "utf8");

  if (receivedBuf.length !== expectedBuf.length) {
    return false;
  }

  return crypto.timingSafeEqual(receivedBuf, expectedBuf);
}

// Rota para o seu front-end (index.html) criar cobrança Pix
app.post("/api/pix/deposits", async (req, res) => {
  try {
    const { amountCents, description, orderRef } = req.body || {};
    const parsedAmount = Number(amountCents);
    const result = await criarCobrancaPix({
      amountCents: parsedAmount,
      description,
      orderRef
    });
    return res.status(201).json(result);
  } catch (err) {
    return res.status(err.statusCode || 500).json({
      success: false,
      error: err.message || "Erro interno ao criar cobranca Pix",
      code: err.code || "INTERNAL_ERROR"
    });
  }
});

// Rota para consultar um depósito Pix pelo ID
app.get("/api/pix/deposits/:id", async (req, res) => {
  try {
    const data = await consultarDepositoPix({ depositId: req.params.id });
    return res.status(200).json(data);
  } catch (err) {
    return res.status(err.statusCode || 500).json({
      success: false,
      error: err.message || "Erro ao consultar deposito Pix",
      code: err.code || "INTERNAL_ERROR"
    });
  }
});

// Rota para listar depósitos Pix
app.get("/api/pix/deposits", async (req, res) => {
  try {
    const { status, limit, startingAfter } = req.query || {};
    const parsedLimit = limit !== undefined ? Number(limit) : undefined;
    const data = await consultarDepositoPix({
      status,
      limit: parsedLimit,
      startingAfter
    });
    return res.status(200).json(data);
  } catch (err) {
    return res.status(err.statusCode || 500).json({
      success: false,
      error: err.message || "Erro ao listar depositos Pix",
      code: err.code || "INTERNAL_ERROR"
    });
  }
});

// Endpoint de Webhook da BlackPix
app.post("/api/webhooks/blackpix", (req, res) => {
  const sigHeader = req.header("X-BlackPix-Signature");
  const isValid = verifyBlackPixSignature(req.rawBody, sigHeader, BLACKPIX_WEBHOOK_SECRET);

  if (!isValid) {
    console.warn("[WEBHOOK] Assinatura invalida ou timestamp expirado");
    return res.status(401).json({ success: false, error: "Assinatura invalida" });
  }

  let event = null;
  try {
    event = JSON.parse(req.rawBody);
  } catch (_err) {
    return res.status(400).json({ success: false, error: "JSON invalido" });
  }

  const eventId = event && event.id;
  const eventType = event && event.type;
  const deposit = event && event.data && event.data.deposit;

  if (!eventId || !eventType) {
    return res.status(400).json({ success: false, error: "Evento incompleto" });
  }

  // Deduplicação: ignora eventos repetidos já recebidos
  if (processedEventIds.has(eventId)) {
    console.log(`[WEBHOOK] Evento repetido ignorado | eventId=${eventId}`);
    return res.status(200).json({ success: true, duplicate: true });
  }
  processedEventIds.add(eventId);

  if (eventType === "deposit.paid" && deposit && deposit.id && deposit.status === "paid") {
    const savedOrder = ordersByDepositId.get(deposit.id);

    // Confere id e amountCents com o pedido salvo quando houver registro salvo
    const amountMatches = savedOrder ? savedOrder.amountCents === deposit.amountCents : true;

    if (amountMatches) {
      ordersByDepositId.set(deposit.id, {
        ...(savedOrder || { depositId: deposit.id }),
        amountCents: deposit.amountCents,
        status: "paid",
        paidAt: deposit.paidAt || new Date().toISOString(),
        lastEventId: eventId
      });
      console.log(`[WEBHOOK] Pagamento confirmado | depositId=${deposit.id} | eventId=${eventId}`);
    } else {
      console.warn(`[WEBHOOK] Divergencia de valor | depositId=${deposit.id} | eventId=${eventId}`);
    }
  } else {
    console.log(`[WEBHOOK] Evento recebido | type=${eventType} | eventId=${eventId}`);
  }

  return res.status(200).json({ success: true });
});

app.get("/health", (_req, res) => {
  res.status(200).json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`[SERVER] Rodando na porta ${PORT}`);
});
