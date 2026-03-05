import os
import json
from datetime import datetime, timezone, timedelta
from html import escape as html_escape
import requests

# =========================
# Config via Secrets/ENV
# =========================
DT_URL = os.environ["DT_URL"].rstrip("/")  # https://.../api/v2/problems
DT_TOKEN = os.environ["DT_TOKEN"]

TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]

# Base do link Dynatrace (Managed). Deriva de DT_URL: https://{domain}/e/{env-id}
DT_UI_BASE = os.getenv("DT_UI_BASE", DT_URL.split("/api/v2/problems")[0])

FROM_TIMEFRAME = os.getenv("DT_FROM", "now-7d")
PAGE_SIZE = int(os.getenv("DT_PAGE_SIZE", "200"))  # agora pode ser > 10 pois NÃO usamos fields na lista

PERSIST_MIN = int(os.getenv("PERSIST_MINUTES", "15"))
PERSIST_MS = PERSIST_MIN * 60 * 1000

# Timezone fixo UTC-3 (BRT)
TZ_BRT = timezone(timedelta(hours=-3))

# Dedup (cache do Actions)
STATE_FILE = os.getenv("STATE_FILE", "sent.json")

# Telegram message limit (~4096). Folga:
TELEGRAM_MAX_CHARS = int(os.getenv("TELEGRAM_MAX_CHARS", "3800"))

# Limites para não poluir
MAX_TAGS = int(os.getenv("MAX_TAGS", "8"))
MAX_CAUSES = int(os.getenv("MAX_CAUSES", "2"))
MAX_SYMPTOMS = int(os.getenv("MAX_SYMPTOMS", "2"))

# Limite de enriquecimento por rodada (para não explodir chamadas no /details)
MAX_ENRICH = int(os.getenv("MAX_ENRICH", "25"))

# =========================
# Management Zones (OR)
# =========================
MZ_NAMES = [
    "AP14324 - PowerOn Sao Paulo Brazil",
    "AP14314 - GIS EO Sao Paulo Brazil",
    "AP14320 - Insight Sao Paulo Brazil",
    "AP04170 - SDE BRAZIL",
    "AP12613.03 - eOrder Features - São Paulo - Brazil",
    "AP36646 - eCallback São Paulo",
    "AP36845 - eCallback Rio de Janeiro",
    "AP36846 - eCallback Ceara",
    "AP14131 - SGE Sao Paulo Brazil",
    "AP14131.01 - SGE - AMANAJE Sao Paulo",
    "AP14131.02 - SGE - ARACE Sao Paulo",
    "AP14131.03 - SGE - AUTOFAT Sao Paulo",
    "AP14131.04 - SGE - AYMBERE Sao Paulo",
    "AP14131.05 - SGE - AYMBERE Campo Sao Paulo",
    "AP14131.06 - SGE - EMBEYBA Sao Paulo",
    "AP14131.07 - SGE - Suporte TI Sao Paulo",
    "AP14131.08 - SGE - SITE Sao Paulo",
    "AP32414.01 - SGE CEARA BRAZIL",
    "AP35075 - SGE RIO BRAZIL",
    "AP24559 - ENEL SP - SAP CRM",
    "AP24559.10 - SAP CRM SP - Suporte AMS",
    "AP24559.12 - SAP CRM SP - MKT Perfil (TI)",
    "AP06289.02 - Mulesoft Brasil",
    "AP04170.07 - SDE - GESTINFO RIO BRAZIL",
    "AP04170.13 - SDE - GESTINFO CEARA BRAZIL",
    "AP04170.09 - SDE - OPERBT RIO BRAZIL",
    "AP04170.12 - SDE - OPERBT CEARA BRAZIL",
]

def build_problem_selector():
    # Problems API v2: status("open") e managementZones("name1","name2",...) (OR) [1](https://www.pulumi.com/registry/packages/dynatrace/api-docs/getmanagementzonev2/)
    quoted_mz = ",".join([f"\"{mz}\"" for mz in MZ_NAMES])
    return f"status(\"open\"),managementZones({quoted_mz})"


# =========================
# State (dedup)
# =========================
def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data) if isinstance(data, list) else set()
    except FileNotFoundError:
        return set()

def save_state(sent_ids: set):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(list(sent_ids)), f, ensure_ascii=False, indent=2)


# =========================
# Dynatrace API (list + details)
# =========================
def _dt_headers():
    return {"Authorization": f"Api-Token {DT_TOKEN}", "Accept": "application/json"}

def dt_list_problems_page(next_page_key=None):
    """
    Lista problems (sem fields extras) para poder usar pageSize alto.
    GET /api/v2/problems suporta from/pageSize/problemSelector/nextPageKey [1](https://www.pulumi.com/registry/packages/dynatrace/api-docs/getmanagementzonev2/)
    """
    params = {}
    if next_page_key:
        params["nextPageKey"] = next_page_key
    else:
        params["from"] = FROM_TIMEFRAME
        params["pageSize"] = PAGE_SIZE
        params["problemSelector"] = build_problem_selector()

    r = requests.get(DT_URL, headers=_dt_headers(), params=params, timeout=40)
    if r.status_code >= 400:
        raise RuntimeError(f"Dynatrace LIST HTTP {r.status_code}: {r.text}")
    return r.json()

def dt_list_all_open_problems():
    problems = []
    payload = dt_list_problems_page()
    problems.extend(payload.get("problems", []))
    next_key = payload.get("nextPageKey")

    while next_key:
        payload = dt_list_problems_page(next_page_key=next_key)
        problems.extend(payload.get("problems", []))
        next_key = payload.get("nextPageKey")

    return problems

def dt_get_problem_details(problem_id: str):
    """
    Busca detalhes de um problem específico:
    GET /api/v2/problems/{problemId}?fields=evidenceDetails [2](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/entity-v2/entity-selector)
    """
    url = f"{DT_URL}/{problem_id}"
    params = {"fields": "evidenceDetails"}
    r = requests.get(url, headers=_dt_headers(), params=params, timeout=40)
    if r.status_code >= 400:
        raise RuntimeError(f"Dynatrace DETAILS HTTP {r.status_code}: {r.text}")
    return r.json()


# =========================
# Telegram
# =========================
def tg_send_html(text_html: str):
    """
    Telegram Bot API sendMessage com parse_mode=HTML [4](https://rishandigital.com/powerautomate/using-http-requests-in-power-automate-for-api-calls/)[5](https://www.youtube.com/watch?v=uZPuSMdcuLY)
    """
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    r = requests.post(url, json=payload, timeout=40)
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram HTTP {r.status_code}: {r.text}")


# =========================
# Helpers (tempo, link, parsing)
# =========================
def utc_ms_now():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def fmt_brt_datetime_from_utc_ms(ms_utc: int) -> str:
    dt_utc = datetime.fromtimestamp(ms_utc / 1000, tz=timezone.utc)
    dt_brt = dt_utc.astimezone(TZ_BRT)
    return dt_brt.strftime("%d/%m/%Y %H:%M:%S (UTC-03)")

def fmt_duration_ms(ms: int) -> str:
    if ms < 0:
        ms = 0
    total_minutes = ms // 60000
    days = total_minutes // (24 * 60)
    hours = (total_minutes % (24 * 60)) // 60
    mins = total_minutes % 60
    if days > 0:
        return f"{days}d {hours}h {mins}m"
    if hours > 0:
        return f"{hours}h {mins}m"
    return f"{mins}m"

def build_dynatrace_problem_link(problem_id: str) -> str:
    # Dynatrace Managed deep link clássico: /#problems/problemdetails;pid={ProblemID} [3](https://community.dynatrace.com/t5/Container-platforms/How-to-select-multiple-entityName-startsWith-statements/m-p/279026)
    return f"{DT_UI_BASE}/#problems/problemdetails;pid={problem_id}"

def extract_mz_names(problem: dict) -> str:
    mzs = problem.get("managementZones", []) or []
    names = [z.get("name") for z in mzs if isinstance(z, dict) and z.get("name")]
    return ", ".join(names) if names else "n/d"

def extract_tags(problem: dict) -> str:
    tags = problem.get("entityTags", []) or []
    out = []
    for t in tags:
        if not isinstance(t, dict):
            continue
        s = t.get("stringRepresentation")
        if not s:
            ctx = t.get("context")
            key = t.get("key")
            val = t.get("value")
            if key and val:
                s = f"[{ctx}]{key}:{val}" if ctx else f"{key}:{val}"
            elif key:
                s = f"[{ctx}]{key}" if ctx else key
        if s:
            out.append(s)
        if len(out) >= MAX_TAGS:
            break
    return ", ".join(out) if out else "n/d"

def extract_root_cause_and_symptom_from_details(problem_details: dict):
    """
    Usa evidenceDetails.details[].rootCauseRelevant (true/false) para separar causa vs sintoma. [2](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/entity-v2/entity-selector)
    """
    evidence = (problem_details.get("evidenceDetails") or {}).get("details", []) or []
    causes, symptoms = [], []

    for ev in evidence:
        if not isinstance(ev, dict):
            continue
        name = ev.get("displayName") or ""
        ent = ev.get("entity") or {}
        ent_name = ent.get("name") if isinstance(ent, dict) else None
        text = name
        if ent_name and ent_name not in name:
            text = f"{name} — {ent_name}"

        if ev.get("rootCauseRelevant") is True:
            causes.append(text)
        else:
            symptoms.append(text)

    return causes[:MAX_CAUSES], symptoms[:MAX_SYMPTOMS]

def chunk_messages(lines, header):
    chunks = []
    current = header
    for line in lines:
        if len(current) + len(line) + 2 > TELEGRAM_MAX_CHARS:
            chunks.append(current.strip())
            current = header + line + "\n\n"
        else:
            current += line + "\n\n"
    if current.strip() != header.strip():
        chunks.append(current.strip())
    return chunks


# =========================
# Mensagem (digest bonito) — MZ como prioridade
# =========================
def build_digest(enriched_items: list, now_ms: int):
    now_brt = datetime.now(TZ_BRT).strftime("%d/%m/%Y %H:%M:%S (UTC-03)")
    header = (
        f"<b>🚨 Dynatrace — Problems persistentes (≥ {PERSIST_MIN} min)</b>\n"
        f"<i>Atualização: {html_escape(now_brt)}</i>\n"
        f"<i>Total nesta rodada: {len(enriched_items)}</i>\n\n"
    )

    lines = []
    for i, item in enumerate(enriched_items, start=1):
        p = item["problem"]
        causes = item.get("causes") or []
        symptoms = item.get("symptoms") or []

        display_id = html_escape(p.get("displayId", ""))
        title = html_escape(p.get("title", ""))
        severity = html_escape(p.get("severityLevel", ""))
        impact = html_escape(p.get("impactLevel", ""))
        start_ms = p.get("startTime", 0)

        age_ms = max(0, now_ms - start_ms) if isinstance(start_ms, int) else 0
        opened_for = fmt_duration_ms(age_ms)
        start_brt = fmt_brt_datetime_from_utc_ms(start_ms) if isinstance(start_ms, int) else "n/d"

        mz_names = html_escape(extract_mz_names(p))  # ⭐ prioridade
        tags = html_escape(extract_tags(p))

        causes_txt = html_escape(" | ".join(causes)) if causes else "n/d"
        symptoms_txt = html_escape(" | ".join(symptoms)) if symptoms else "n/d"

        pid = p.get("problemId", "")
        link = build_dynatrace_problem_link(pid) if pid else DT_UI_BASE

        line = (
            f"<b>{i}) {display_id} — {title}</b>\n"
            f"MZ: <b>{mz_names}</b>\n"
            f"Sev: <b>{severity}</b> | Impacto: <b>{impact}</b>\n"
            f"Início (UTC-03): <code>{html_escape(start_brt)}</code>\n"
            f"Aberto há: <b>{html_escape(opened_for)}</b>\n"
            f"Tags: <code>{tags}</code>\n"
            f"Causa raiz (se houver): <code>{causes_txt}</code>\n"
            f"Sintoma (se houver): <code>{symptoms_txt}</code>\n"
            f"🔗 <a href=\"{html_escape(link)}\">Abrir no Dynatrace</a>"
        )
        lines.append(line)

    return header, lines


# =========================
# Main
# =========================
def main():
    sent_ids = load_state()
    now_ms = utc_ms_now()

    all_problems = dt_list_all_open_problems()

    # Filtra por persistência >= 15 min e dedup por problemId
    candidates = []
    for p in all_problems:
        pid = p.get("problemId")
        if not pid or pid in sent_ids:
            continue

        start_ms = p.get("startTime")
        if not isinstance(start_ms, int):
            continue

        if (now_ms - start_ms) >= PERSIST_MS:
            candidates.append(p)

    if not candidates:
        print("Nada para enviar.")
        return

    # Ordena por startTime (mais antigos primeiro)
    candidates.sort(key=lambda x: x.get("startTime", 0))

    # Enriquecimento (detalhes) só até MAX_ENRICH para evitar excesso
    enriched = []
    for p in candidates[:MAX_ENRICH]:
        pid = p["problemId"]
        try:
            details = dt_get_problem_details(pid)  # GET /api/v2/problems/{problemId} [2](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/entity-v2/entity-selector)
            causes, symptoms = extract_root_cause_and_symptom_from_details(details)
        except Exception as e:
            # Se falhar detalhes, não bloqueia envio
            causes, symptoms = [], []
            print(f"[WARN] Falha ao enriquecer {pid}: {e}")

        enriched.append({"problem": p, "causes": causes, "symptoms": symptoms})

    header, lines = build_digest(enriched, now_ms)
    chunks = chunk_messages(lines, header)

    for chunk in chunks:
        tg_send_html(chunk)  # sendMessage HTML [4](https://rishandigital.com/powerautomate/using-http-requests-in-power-automate-for-api-calls/)[5](https://www.youtube.com/watch?v=uZPuSMdcuLY)

    # Marca como enviado (para todos candidates, inclusive os não enriquecidos)
    for p in candidates:
        sent_ids.add(p["problemId"])
    save_state(sent_ids)

    print(f"Enviado: {len(candidates)} problems (enriquecidos: {len(enriched)}) em {len(chunks)} mensagem(ns).")

if __name__ == "__main__":
    main()
