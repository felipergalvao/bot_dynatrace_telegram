import os
import json
from datetime import datetime, timezone, timedelta
from html import escape as html_escape
import requests

# =========================
# Config via Secrets/ENV
# =========================
DT_URL = os.environ["DT_URL"].rstrip("/")  # ex: https://dynatrace-one.enel.com/e/<env>/api/v2/problems
DT_TOKEN = os.environ["DT_TOKEN"]

TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]

# Se não setar DT_UI_BASE, derivamos automaticamente a partir do DT_URL:
# https://{dominio}/e/{env-id}
DT_UI_BASE = os.getenv("DT_UI_BASE", DT_URL.split("/api/v2/problems")[0])

FROM_TIMEFRAME = os.getenv("DT_FROM", "now-7d")
PAGE_SIZE = int(os.getenv("DT_PAGE_SIZE", "200"))

PERSIST_MIN = int(os.getenv("PERSIST_MINUTES", "15"))
PERSIST_MS = PERSIST_MIN * 60 * 1000

# Timezone fixo UTC-3 (BRT)
TZ_BRT = timezone(timedelta(hours=-3))

# Dedup simples (arquivo no repo + cache do Actions)
STATE_FILE = os.getenv("STATE_FILE", "sent.json")

# Limites do Telegram (texto máximo ~4096 chars). Vamos chunkar com folga.
TELEGRAM_MAX_CHARS = 3800

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
    # Problems API v2: problemSelector aceita status("open") e managementZones("name1","name2",...) (OR) [1](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/problems-v2/problems/get-problems-list)
    quoted_mz = ",".join([f"\"{mz}\"" for mz in MZ_NAMES])
    return f"status(\"open\"),managementZones({quoted_mz})"


# =========================
# State (dedup)
# =========================
def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(data)
        return set()
    except FileNotFoundError:
        return set()

def save_state(sent_ids: set):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(list(sent_ids)), f, ensure_ascii=False, indent=2)


# =========================
# Dynatrace API
# =========================
def dt_get_problems_page(next_page_key=None):
    headers = {
        "Authorization": f"Api-Token {DT_TOKEN}",
        "Accept": "application/json",
    }

    # GET /api/v2/problems com filtros via from/pageSize/problemSelector (ou paginação via nextPageKey) [1](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/problems-v2/problems/get-problems-list)
    params = {}
    if next_page_key:
        params["nextPageKey"] = next_page_key
    else:
        params["from"] = FROM_TIMEFRAME
        params["pageSize"] = PAGE_SIZE
        params["problemSelector"] = build_problem_selector()

    r = requests.get(DT_URL, headers=headers, params=params, timeout=40)
    r.raise_for_status()
    return r.json()

def dt_get_all_open_problems_filtered():
    # Busca todas as páginas respeitando nextPageKey [1](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/problems-v2/problems/get-problems-list)
    problems = []
    payload = dt_get_problems_page()
    problems.extend(payload.get("problems", []))
    next_key = payload.get("nextPageKey")

    while next_key:
        payload = dt_get_problems_page(next_page_key=next_key)
        problems.extend(payload.get("problems", []))
        next_key = payload.get("nextPageKey")

    return problems


# =========================
# Telegram
# =========================
def tg_send_html(text_html: str):
    # sendMessage do Telegram Bot API com parse_mode HTML [2](https://www.burgersandbytes.nl/blog/20240212teamsannouncementmessage/)[3](https://community.dynatrace.com/t5/Dynatrace-API/Management-Zone-list-through-API/m-p/256644)
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=40)
    r.raise_for_status()

def chunk_messages(lines, header):
    """
    Junta linhas em blocos <= TELEGRAM_MAX_CHARS.
    Mantém o header no início de cada bloco.
    """
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
# Formatadores
# =========================
def fmt_brt_from_utc_ms(ms_utc: int) -> str:
    # A API entrega timestamps em ms UTC [1](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/problems-v2/problems/get-problems-list)
    dt_utc = datetime.fromtimestamp(ms_utc / 1000, tz=timezone.utc)
    dt_brt = dt_utc.astimezone(TZ_BRT)
    return dt_brt.strftime("%d/%m/%Y %H:%M:%S (UTC-03)")

def build_dynatrace_problem_link(problem_id: str) -> str:
    # Link direto (Managed): https://{domain}/e/{env-id}/#problems/problemdetails;pid={ProblemID} [4](https://community.dynatrace.com/t5/Dynatrace-API/API-to-get-problem-details-URL/m-p/196958)
    return f"{DT_UI_BASE}/#problems/problemdetails;pid={problem_id}"

def build_digest(problems_to_send):
    now_brt = datetime.now(TZ_BRT).strftime("%d/%m/%Y %H:%M:%S (UTC-03)")
    header = (
        f"<b>🚨 Dynatrace — Problems persistentes (≥ {PERSIST_MIN} min)</b>\n"
        f"<i>Atualização: {html_escape(now_brt)}</i>\n"
        f"<i>Total nesta rodada: {len(problems_to_send)}</i>\n\n"
    )

    lines = []
    for i, p in enumerate(problems_to_send, start=1):
        display_id = html_escape(p.get("displayId", ""))
        title = html_escape(p.get("title", ""))
        severity = html_escape(p.get("severityLevel", ""))
        impact = html_escape(p.get("impactLevel", ""))

        start_ms = p.get("startTime")
        start_brt = fmt_brt_from_utc_ms(start_ms) if isinstance(start_ms, int) else "n/d"

        # NÃO exibimos problemId, mas usamos para montar link clicável
        problem_id = p.get("problemId", "")
        link = build_dynatrace_problem_link(problem_id) if problem_id else DT_UI_BASE

        # HTML link clicável
        line = (
            f"<b>{i}) {display_id} — {title}</b>\n"
            f"Sev: <b>{severity}</b> | Impacto: <b>{impact}</b>\n"
            f"Início (UTC-03): <code>{html_escape(start_brt)}</code>\n"
            f"🔗 <a href=\"{html_escape(link)}\">Abrir no Dynatrace</a>"
        )
        lines.append(line)

    return header, lines


# =========================
# Main
# =========================
def main():
    sent_ids = load_state()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    all_problems = dt_get_all_open_problems_filtered()

    # Filtra por persistência >= 15 min e dedup por problemId
    to_send = []
    for p in all_problems:
        pid = p.get("problemId")
        if not pid or pid in sent_ids:
            continue

        start_ms = p.get("startTime")
        if not isinstance(start_ms, int):
            continue

        age_ms = now_ms - start_ms
        if age_ms >= PERSIST_MS:
            to_send.append(p)

    if not to_send:
        print("Nada para enviar.")
        return

    # Ordena por startTime (mais antigos primeiro)
    to_send.sort(key=lambda x: x.get("startTime", 0))

    header, lines = build_digest(to_send)
    chunks = chunk_messages(lines, header)

    for chunk in chunks:
        tg_send_html(chunk)

    # Marca como enviado
    for p in to_send:
        sent_ids.add(p["problemId"])

    save_state(sent_ids)
    print(f"Enviado: {len(to_send)} problems em {len(chunks)} mensagem(ns).")

if __name__ == "__main__":
    main()
