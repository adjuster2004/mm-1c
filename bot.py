import os
import sys
import json
import re
import io
import pandas as pd
import requests
import threading
import asyncio
from datetime import datetime, date
from mattermostdriver import Driver
import jinja2

# --- ИМПОРТ МОДУЛЯ TEAMS ---
try:
    import teams
except ImportError:
    print("⚠️ Модуль teams.py не найден. Функционал уведомления лидов будет недоступен.")
    teams = None

# --- ЗАГРУЗКА КОНФИГУРАЦИИ ---
def get_env(key, default=None, required=False):
    val = os.getenv(key, default)
    if required and not val:
        print(f"❌ Ошибка: Не задана переменная {key}")
        sys.exit(1)
    return val

MM_URL = get_env("MM_URL", required=True)
MM_TOKEN = get_env("MM_TOKEN", required=True)
MM_PORT = int(get_env("MM_PORT", "443"))
MM_SCHEME = get_env("MM_SCHEME", "https")
MM_TARGET_CHANNEL_ID = get_env("MM_TARGET_CHANNEL_ID", required=True)

JIRA_DOMAIN = get_env("JIRA_DOMAIN", required=True)
JIRA_TOKEN = get_env("JIRA_TOKEN", required=True)
AUTH_METHOD = get_env("JIRA_AUTH_METHOD", "Bearer")
VERIFY_SSL = get_env("VERIFY_SSL", "False").lower() in ('true', '1', 't')

if not VERIFY_SSL:
    requests.packages.urllib3.disable_warnings()

# Глобальные кэши
JIRA_LOOKUP_CACHE = {}
JIRA_KEY_CACHE = {}

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_headers():
    if AUTH_METHOD == "Bearer":
        return {"Authorization": f"Bearer {JIRA_TOKEN}", "Content-Type": "application/json"}
    return {"Cookie": f"JSESSIONID={JIRA_TOKEN}", "Content-Type": "application/json"}

def get_all_jira_users():
    print("⏳ Кэширование пользователей Jira...", flush=True)
    users = []
    search_queries = ['.', '@', '']
    for query in search_queries:
        print(f"🔎 Поиск пользователей API Jira по маске: '{query}'", flush=True)
        temp_users = []
        start_at = 0
        while True:
            url = f"https://{JIRA_DOMAIN}/rest/api/2/user/search"
            params = {"username": query, "startAt": start_at, "maxResults": 1000, "includeInactive": "false"}
            try:
                resp = requests.get(url, headers=get_headers(), params=params, verify=VERIFY_SSL, timeout=(10, 30))
                if resp.status_code != 200: break
                chunk = resp.json()
                if not chunk: break
                temp_users.extend(chunk)
                if len(chunk) < 1000: break
                start_at += len(chunk)
            except: break
        if temp_users:
            users = temp_users
            break

    lookup_map = {}
    key_map = {}
    for u in users:
        login = u.get('name')
        key = u.get('key')
        d_name = u.get('displayName')
        if not key: key = login
        user_obj = {'login': login, 'key': key, 'displayName': d_name}
        if key: key_map[key] = user_obj
        if d_name:
            lookup_map[d_name.lower()] = user_obj
            parts = d_name.split()
            if len(parts) == 2:
                rev_name = f"{parts[1]} {parts[0]}"
                lookup_map[rev_name.lower()] = user_obj
        if login: lookup_map[login.lower()] = user_obj
    return lookup_map, key_map

def update_progress_message(post_id, channel_id, message):
    try:
        driver.posts.update_post(post_id, options={'id': post_id, 'channel_id': channel_id, 'message': message})
    except Exception as e: print(f"Status update error: {e}", flush=True)

def parse_tempo_date(date_val):
    if not date_val: return None
    s = str(date_val).lower().strip().split('t')[0]
    try:
        if '-' in s: return datetime.strptime(s, '%Y-%m-%d').date()
        if '/' in s: return datetime.strptime(s, '%Y/%m/%d').date()
        if '.' in s: return datetime.strptime(s, '%d.%m.%Y').date()
    except: pass
    return None

def get_team_rank(team_name):
    tn = team_name.lower()
    if tn == 'other': return (99, 0, tn)
    if 'arch-team' in tn: return (1, 0, tn)
    if 'change-team' in tn: return (2, 0, tn)
    if 'stream' in tn:
        match = re.search(r'stream(\d+)', tn)
        num = int(match.group(1)) if match else 999
        return (3, num, tn)
    return (4, 0, tn)

def get_tempo_teams_assignments(report_start_date, report_end_date):
    print("⏳ Анализ команд Tempo...", flush=True)
    try:
        resp = requests.get(f"https://{JIRA_DOMAIN}/rest/tempo-teams/2/team", headers=get_headers(), verify=VERIFY_SSL, timeout=30)
        if resp.status_code != 200: return {}
        all_teams = resp.json()
    except: return {}

    target_teams = []
    pattern = re.compile(r"^(stream.*-team|change-team|arch-team)$", re.IGNORECASE)
    for team in all_teams:
        if pattern.match(team.get("name", "")): target_teams.append(team)

    user_team_map = {}
    for team in target_teams:
        try:
            m_resp = requests.get(f"https://{JIRA_DOMAIN}/rest/tempo-teams/2/team/{team.get('id')}/member", headers=get_headers(), verify=VERIFY_SSL, timeout=30)
            if m_resp.status_code == 200:
                for m in m_resp.json():
                    jira_key = m.get("member", {}).get("key")
                    if not jira_key: continue
                    ms = m.get("membership", {})
                    d_from = parse_tempo_date(ms.get('dateFromANSI') or ms.get('dateFrom')) or date(2000, 1, 1)
                    d_to = parse_tempo_date(ms.get('dateToANSI') or ms.get('dateTo')) or date(2099, 12, 31)
                    if d_from <= report_end_date and d_to >= report_start_date:
                        user_team_map[jira_key] = team.get("name")
        except: pass
    return user_team_map

def fetch_tempo_worklogs_for_users(start_date, end_date, worker_ids, progress_callback=None):
    all_worklogs = []
    chunks = [worker_ids[i:i + 25] for i in range(0, len(worker_ids), 25)]
    for i, chunk_workers in enumerate(chunks):
        if progress_callback: progress_callback(i + 1, len(chunks))
        payload = {"from": start_date.strftime("%Y-%m-%d"), "to": end_date.strftime("%Y-%m-%d"), "worker": chunk_workers}
        try:
            resp = requests.post(f"https://{JIRA_DOMAIN}/rest/tempo-timesheets/4/worklogs/search", headers=get_headers(), json=payload, verify=VERIFY_SSL, timeout=90)
            if resp.status_code == 200: all_worklogs.extend(resp.json().get('results', []) if isinstance(resp.json(), dict) else resp.json())
        except: pass
    return all_worklogs

def check_name_match(jira_name, excel_name):
    if not jira_name or not excel_name: return False
    j_parts = [p for p in str(jira_name).lower().replace('.', ' ').split() if len(p)>1]
    e_parts = [p for p in str(excel_name).lower().replace('.', ' ').split() if len(p)>1]
    return not set(j_parts).isdisjoint(set(e_parts))

def extract_period_from_excel(df_head):
    dates = []
    for _, row in df_head.iterrows():
        matches = re.findall(r'\d{2}\.\d{2}\.\d{4}', str(row.values))
        for m in matches:
            try: dates.append(datetime.strptime(m, '%d.%m.%Y').date())
            except: pass
    if len(dates) >= 2: return min(dates), max(dates)
    return None, None

# --- ТЯЖЕЛАЯ РАБОТА В ПОТОКЕ ---
def worker_process_file(file_id, channel_id, root_id):
    print(f"[THREAD] Поток запущен для файла {file_id}", flush=True)
    status_post_id = None
    try:
        status_post = driver.posts.create_post(options={'channel_id': channel_id, 'message': '⏳ Файл в очереди...', 'root_id': root_id})
        status_post_id = status_post['id']
    except: pass

    def update_status_text(text):
        if status_post_id: update_progress_message(status_post_id, channel_id, text)

    try:
        # 1. СКАЧИВАНИЕ ФАЙЛА
        raw_file_resp = requests.get(f"{MM_SCHEME}://{MM_URL}/api/v4/files/{file_id}", headers={"Authorization": f"Bearer {MM_TOKEN}"}, verify=VERIFY_SSL, timeout=60)
        if raw_file_resp.status_code != 200: return
        file_bytes = io.BytesIO(raw_file_resp.content)

        update_status_text("⏳ Читаю Excel...")
        try: df_raw = pd.read_excel(file_bytes, header=None)
        except Exception as e:
            update_status_text(f"❌ Ошибка Excel: {e}")
            return

        start_date, end_date = extract_period_from_excel(df_raw.head(20))
        if not start_date:
            update_status_text("⚠️ Не найден период дат.")
            return

        # 2. ПАРСИНГ EXCEL
        header_row_idx = None
        name_col_idx = None
        hours_col_idx = None
        absence_cols = []

        for idx, row in df_raw.iterrows():
            for c, val in enumerate(row):
                if str(val).lower().startswith("фамилия"):
                    header_row_idx, name_col_idx = idx, c
                    break
            if header_row_idx is not None: break

        if header_row_idx is None:
            update_status_text("❌ Не найдена колонка 'Фамилия'.")
            return

        for c in range(len(df_raw.columns)-1, -1, -1):
            if pd.notna(df_raw.iloc[len(df_raw)//2, c]):
                try:
                    float(str(df_raw.iloc[len(df_raw)//2, c]).replace(',','.'))
                    hours_col_idx = c
                    break
                except: pass

        for r in range(header_row_idx, min(header_row_idx+3, len(df_raw))):
            for c in range(len(df_raw.columns)):
                if c > name_col_idx and "код" in str(df_raw.iloc[r, c]).lower() and c not in absence_cols:
                    absence_cols.append(c)

        excel_data = []
        target_jira_keys = set()
        unique_users = {v['key']: v for v in JIRA_LOOKUP_CACHE.values()}.values()

        for i in range(header_row_idx + 1, len(df_raw)):
            row = df_raw.iloc[i]
            raw_name = row[name_col_idx]
            if pd.isna(raw_name) or len(str(raw_name)) < 2: continue
            if any(k in str(raw_name).lower() for k in ["итого", "подпись", "должность", "профессия"]): continue

            clean_name = str(raw_name).split('\n')[0].split('(')[0].strip()
            hours = 0
            absences = set()

            for offset in range(4):
                if i + offset >= len(df_raw): break
                if hours_col_idx:
                    try:
                        h = float(str(df_raw.iloc[i+offset, hours_col_idx]).replace(',', '.').replace(' ', ''))
                        if h > hours: hours = h
                    except: pass

                for ac in absence_cols:
                    val = df_raw.iloc[i+offset, ac]
                    if pd.notna(val):
                        s = str(val).strip()
                        if s and not s.isdigit() and len(s) < 5 and s.upper() != 'Я': absences.add(s)

            if hours > 0 or absences:
                found_u = next((u for u in unique_users if check_name_match(u['displayName'], clean_name)), None)
                if found_u: target_jira_keys.add(found_u['key'])
                excel_data.append({"name_1c": clean_name, "hours_1c": hours, "jira_user": found_u, "absences": sorted(list(absences))})

        # 3. ПОЛУЧЕНИЕ ДАННЫХ
        update_status_text("⏳ Определяю команды...")
        team_mapping = get_tempo_teams_assignments(start_date, end_date)

        # --- ЗАГРУЗКА ЛИДОВ ИЗ CONFLUENCE ---
        leads_mapping = {}
        if teams:
            update_status_text("⏳ Получаю лидов из Confluence...")
            leads_mapping = teams.fetch_team_leads_mapping()

        tempo_agg = {}
        if target_jira_keys:
            def pc(c, t): update_status_text(f"⏳ Tempo... {int(c/t*100)}%")
            logs = fetch_tempo_worklogs_for_users(start_date, end_date, list(target_jira_keys), pc)
            for l in logs:
                wid = l.get('worker')
                tempo_agg[wid] = tempo_agg.get(wid, 0) + l.get('timeSpentSeconds', 0)

        # 4. СБОРКА РЕЗУЛЬТАТА
        update_status_text("⏳ Формирую отчет...")
        report_rows = []
        for r in excel_data:
            t_sec = 0
            j_name, j_key, t_name = "—", "—", "Other"

            if r['jira_user']:
                j_name = r['jira_user']['displayName']
                j_key = r['jira_user']['key']
                j_login = r['jira_user']['login']
                t_sec = tempo_agg.get(j_key) or tempo_agg.get(j_login) or 0
                if j_key in team_mapping: t_name = team_mapping[j_key]

            t_hours = round(t_sec / 3600, 2)
            diff = round(t_hours - r['hours_1c'], 2)

            status = "✅ OK"
            if abs(diff) > 4: status = "⚠️ Расхождение"
            if j_name == "—": status = "❓ Не найден в Jira"

            report_rows.append({
                "Team": t_name, "Сотрудник (1C)": r['name_1c'], "Сотрудник (Jira)": j_name,
                "Jira Key": j_key, "Link": f"https://{JIRA_DOMAIN}/secure/Tempo.jspa#/my-work/timesheet?worker={j_key}&viewType=TIMESHEET" if j_key != "—" else None,
                "Часы 1С": r['hours_1c'], "Неявки (1С)": ", ".join(r['absences']), "Часы Tempo": t_hours,
                "Разница": diff, "Статус": status
            })

        df = pd.DataFrame(report_rows)
        df['SortRank'] = df['Team'].apply(lambda x: get_team_rank(x)[0])
        df['SortNum'] = df['Team'].apply(lambda x: get_team_rank(x)[1])
        df = df.sort_values(by=['SortRank', 'SortNum', 'Team', 'Сотрудник (1C)'])

        # 5. ГЕНЕРАЦИЯ ФАЙЛА
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            def hl(row):
                codes = [c.strip().upper() for c in (str(row['Неявки (1С)']) or "").split(',')]
                return ['background-color: #ffffcc'] * len(row) if any(c != 'В' and c for c in codes) else [''] * len(row)

            df.drop(columns=['SortRank', 'SortNum', 'Link']).style.apply(hl, axis=1).to_excel(writer, index=False, sheet_name='Sverka')
            wb = writer.book
            ws = writer.sheets['Sverka']
            l_fmt = wb.add_format({'font_color': 'blue', 'underline': 1})
            ws.set_column(0, 0, 20); ws.set_column(1, 2, 25); ws.set_column(3, 3, 20)

            for idx, row in df.iterrows():
                if row['Link']:
                    rn = df.index.get_loc(idx) + 1
                    ws.write_url(rn, 3, row['Link'], l_fmt, string=row['Jira Key'])
        output.seek(0)

        # 6. ОТПРАВКА В ЧАТ
        print("[THREAD] Отправка...", flush=True)
        msg_lines = [f"📊 **Итог сверки**", f"ℹ️ {start_date} — {end_date} (Сотрудников: {len(target_jira_keys)})", "", ":al:  - Tempo > 1C", ":bangbang:  - см табель", "🔻  -  1C > Tempo", ""]

        teams_with_issues = set() # Собираем команды с проблемами

        unique_teams = []
        for t in df['Team']:
            if t not in unique_teams: unique_teams.append(t)

        for team in unique_teams:
            team_df = df[df['Team'] == team]
            bad = team_df[team_df['Статус'].str.contains("⚠️")]
            if bad.empty:
                msg_lines.append(f"📁 **{team}**: ✅ Все ОК")
            else:
                teams_with_issues.add(team) # Запоминаем проблемную команду
                msg_lines.append(f"📁 **{team}**: ⚠️ Расхождений: **{len(bad)}**")
                for _, r in bad.iterrows():
                    icon = "🔻" if r['Разница'] < 0 else ":al:"
                    bang = " :bangbang:" if any(c != 'В' and c for c in [x.strip().upper() for x in str(r['Неявки (1С)']).split(',')]) else ""
                    abs_s = f" ({r['Неявки (1С)']})" if r['Неявки (1С)'] else ""
                    msg_lines.append(f"  - **{r['Сотрудник (1C)']}**: 1C=`{r['Часы 1С']}` | Tempo=`{r['Часы Tempo']}` | Diff: **{r['Разница']}** {icon}{bang}{abs_s}")

        not_found = df[df['Статус'].str.contains("❓")]
        if not not_found.empty:
            msg_lines.append(f"\n❓ **Не найдены ({len(not_found)}):**\n_{', '.join(not_found['Сотрудник (1C)'].head(5))}_...")

        up_file = driver.files.upload_file(channel_id=channel_id, files={'files': ('report.xlsx', output)})
        driver.posts.delete_post(status_post_id)
        driver.posts.create_post(options={'channel_id': channel_id, 'message': "\n".join(msg_lines), 'root_id': root_id, 'file_ids': [up_file['file_infos'][0]['id']]})

        # 7. ТЕГИРОВАНИЕ ЛИДОВ
        if teams_with_issues and leads_mapping:
            leads_to_tag = set()
            for t_name in teams_with_issues:
                if t_name in leads_mapping:
                    leads_to_tag.add(leads_mapping[t_name])

            if leads_to_tag:
                tags_str = ", ".join(sorted(list(leads_to_tag)))
                driver.posts.create_post(options={
                    'channel_id': channel_id,
                    'message': f"Внимание: {tags_str} — в ваших командах есть расхождения.",
                    'root_id': root_id
                })

        print("[THREAD] Готово!", flush=True)

    except Exception as e:
        print(f"[THREAD] Error: {e}", flush=True)
        update_status_text(f"💥 Ошибка: {e}")

# --- MATTERMOST HANDLER ---
async def my_event_handler(event):
    try: data = json.loads(event)
    except: return
    if 'event' not in data or data['event'] != 'posted': return
    try: post = json.loads(data['data']['post'])
    except: return
    if post.get('props', {}).get('from_bot') == 'true': return
    if post.get('channel_id') != MM_TARGET_CHANNEL_ID: return

    file_ids = post.get('file_ids', [])
    if len(file_ids) > 0:
        print(f"📥 Файл получен. Запускаю поток...", flush=True)
        for file_id in file_ids:
            t = threading.Thread(target=worker_process_file, args=(file_id, post['channel_id'], post['id']))
            t.start()

# --- INIT ---
if __name__ == "__main__":
    print(f"🚀 Запуск (v5.1 Fixed) для {MM_URL}...", flush=True)
    driver = Driver({'url': MM_URL, 'token': MM_TOKEN, 'scheme': MM_SCHEME, 'port': MM_PORT, 'verify': VERIFY_SSL})
    try:
        JIRA_LOOKUP_CACHE, JIRA_KEY_CACHE = get_all_jira_users()
        driver.login()
        driver.init_websocket(my_event_handler)
    except Exception as e:
        print(f"❌ Сбой: {e}", flush=True)
        sys.exit(1)
