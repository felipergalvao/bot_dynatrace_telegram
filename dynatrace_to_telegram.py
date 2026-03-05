import os, time, json
from datetime import datetime, timezone
import requests

DT_URL = os.environ["DT_URL"]
DT_TOKEN = os.environ["DT_TOKEN"]
TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]

PERSIST_MIN = 15
PERSIST_MS = PERSIST_MIN * 60 * 1000
FROM_TIMEFRAME = "now-7d"

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
    "AP04170.12 - SDE - OPERBT CEARA BRAZIL"
]

# selector conforme Problems API v2 (status + managementZones com OR por múltiplos valores) [1](https://learn.microsoft.com/en-us/connectors/dynatrace/)
def build_problem_selector():
    quoted = ",".join([f"\"{mz}\"" for mz in MZ_NAMES])
    return f"status(\"open\"),managementZones({quoted})"

def utc_ms_now():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def dt_get_problems():
    headers = {"Authorization": f"Api-Token {DT_TOKEN}", "Accept": "application/json"}
    params = {
        "from": FROM_TIMEFRAME,
        "pageSize": 200,
        "problemSelector": build_problem_selector(),
    }
    r = requests.get(DT_URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("problems", [])

def tg_send(text):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()

def load_state():
    # Estado simples em arquivo (persistido via artifact do GitHub Actions)
    try:
        with open("sent.json", "r", encoding="utf-8") as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()

def save_state(sent_ids):
    with open("sent.json", "w", encoding="utf-8") as f:
        json.dump(sorted(list(sent_ids)), f)

def main():
    sent = load_state()
    now = utc_ms_now()

    problems = dt_get_problems()  # GET /api/v2/problems [1](https://learn.microsoft.com/en-us/connectors/dynatrace/)
    for p in problems:
        pid = p.get("problemId")
        if not pid or pid in sent:
            continue

        start_ms = p.get("startTime", now)  # startTime em ms UTC [1](https://learn.microsoft.com/en-us/connectors/dynatrace/)
        if (now - start_ms) < PERSIST_MS:
            continue

        display = p.get("displayId", "")
        title = p.get("title", "")
        sev = p.get("severityLevel", "")
        impact = p.get("impactLevel", "")
        start_iso = datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).isoformat()

        msg = (
            f"🚨 Dynatrace Problem (aberto ≥ {PERSIST_MIN} min)\n"
            f"• {display} — {title}\n"
            f"• Severidade: {sev} | Impacto: {impact}\n"
            f"• Início (UTC): {start_iso}\n"
            f"• ProblemId: {pid}"
        )

        tg_send(msg)  # sendMessage no Telegram Bot API [2](https://core.telegram.org/bots/api)[8](https://telegram-bot-sdk.readme.io/reference/sendmessage)
        sent.add(pid)

    save_state(sent)

if __name__ == "__main__":
    main()
