import os
import re
from atlassian import Confluence
from bs4 import BeautifulSoup

# Кэш для хранения пар {user_key: username}, чтобы не делать лишние запросы
USER_KEY_CACHE = {}

def get_confluence_client():
    url = os.getenv("CONFLUENCE_URL")
    token = os.getenv("CONFLUENCE_TOKEN")
    c_type = os.getenv("CONFLUENCE_TYPE", "Server")
    user = os.getenv("CONFLUENCE_USER")

    if not url or not token:
        return None

    try:
        if c_type.upper() == "CLOUD":
            return Confluence(url=url, username=user, password=token, cloud=True)
        else:
            return Confluence(url=url, token=token)
    except Exception as e:
        print(f"❌ Ошибка подключения к Confluence: {e}")
        return None

def resolve_user_by_key(confluence, user_key):
    """
    Превращает userkey (например, '8af005...') в username (например, 'ivanov')
    делая запрос к API Confluence.
    """
    if not user_key:
        return None

    # Проверяем кэш
    if user_key in USER_KEY_CACHE:
        return USER_KEY_CACHE[user_key]

    try:
        # Метод API для получения данных пользователя по ключу
        user_info = confluence.get_user_details_by_userkey(user_key)

        if user_info and 'username' in user_info:
            username = user_info['username']
            # Сохраняем в кэш
            USER_KEY_CACHE[user_key] = username
            print(f"[DEBUG] API Resolved: {user_key[:5]}... -> {username}", flush=True)
            return username
        else:
            print(f"[DEBUG] API вернул пустой результат для ключа: {user_key}", flush=True)

    except Exception as e:
        print(f"[DEBUG] Ошибка при резолве ключа {user_key}: {e}", flush=True)

    return None

def extract_identity_from_tag(tag):
    """
    Возвращает кортеж (тип_идентификатора, значение).
    Типы: 'username' (готовый логин) или 'userkey' (нужен резолв).
    """
    # 1. User Key (Storage Format в новых версиях Server/DC)
    if tag.has_attr('ri:userkey'):
        return 'userkey', tag['ri:userkey']

    # 2. Username (Legacy Storage Format)
    if tag.has_attr('ri:username'):
        return 'username', tag['ri:username']

    # 3. Data-username (View Format / HTML Macro)
    if tag.has_attr('data-username'):
        return 'username', tag['data-username']

    # 4. Ссылка href (если вставлено как веб-ссылка)
    if tag.has_attr('href'):
        href = tag['href']
        # Ищем /display/~username
        match = re.search(r'/display/~([^/?#]+)', href)
        if match: return 'username', match.group(1)
        # Ищем /users/username
        match_users = re.search(r'/users/([^/?#]+)', href)
        if match_users: return 'username', match_users.group(1)

    return None, None

def fetch_team_leads_mapping():
    if os.getenv("CONFLUENCE_ENABLED", "False").lower() != "true":
        print("ℹ️ Confluence модуль отключен в .env")
        return {}

    page_id = os.getenv("CONFLUENCE_PAGE_ID")
    try:
        col_idx = int(os.getenv("CONFLUENCE_TABLE_COL_INDEX", "0"))
    except:
        col_idx = 0

    if not page_id:
        print("⚠️ Не задан CONFLUENCE_PAGE_ID")
        return {}

    confluence = get_confluence_client()
    if not confluence:
        return {}

    print(f"⏳ Скачивание данных о лидах со страницы {page_id}...", flush=True)

    try:
        # Получаем Storage Format (XML)
        page = confluence.get_page_by_id(page_id, expand='body.storage')
        html_content = page.get('body', {}).get('storage', {}).get('value', '')

        # Используем lxml для XML-тегов
        soup = BeautifulSoup(html_content, 'lxml')

        table = soup.find('table')
        if not table:
            print("⚠️ Таблица не найдена.")
            return {}

        leads_map = {}

        # Паттерн для поиска команд
        team_pattern = re.compile(r"^(stream.*-team|change-team|arch-team)$", re.IGNORECASE)

        rows = table.find_all('tr')
        print(f"[DEBUG] Найдено строк: {len(rows)}. Ищем в колонке #{col_idx}", flush=True)

        current_lead = None

        for i, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])
            if len(cells) > col_idx:
                target_cell = cells[col_idx]

                # --- ЛОГИРОВАНИЕ ПЕРВОЙ СТРОКИ (Для проверки) ---
                if i == 1:
                    raw_html = str(target_cell)[:100].replace('\n', '')
                    print(f"[DEBUG] HTML пример (стр 1): {raw_html}...", flush=True)

                found_username = None

                # 1. Проход по тегам (ищем ri:userkey или другие признаки)
                all_tags = target_cell.find_all(True)
                for tag in all_tags:
                    id_type, id_val = extract_identity_from_tag(tag)

                    if id_type == 'username':
                        found_username = id_val
                        print(f"[DEBUG] Нашел username напрямую: {found_username}", flush=True)
                        break
                    elif id_type == 'userkey':
                        # Если нашли ключ - делаем запрос к API для получения логина
                        resolved_login = resolve_user_by_key(confluence, id_val)
                        if resolved_login:
                            found_username = resolved_login
                            break

                # 2. Если тегов нет, ищем текстовый @username
                if not found_username:
                    cell_text = target_cell.get_text(" ", strip=True).replace('\xa0', ' ')
                    for word in cell_text.split():
                        if word.startswith('@') and len(word) > 1:
                            found_username = word.strip().lstrip('@')
                            print(f"[DEBUG] Нашел текстовый username: {found_username}", flush=True)
                            break

                # Запоминаем текущего лида
                if found_username:
                    current_lead = found_username

                # Ищем названия команд в этой же ячейке
                cell_text_clean = target_cell.get_text(" ", strip=True).replace('\xa0', ' ')
                parts = [p.strip().strip(',;') for p in cell_text_clean.split()]

                for p in parts:
                    if team_pattern.match(p):
                        if current_lead:
                            # Добавляем @ перед логином для тега в Mattermost
                            leads_map[p] = f"@{current_lead}"
                            print(f"[DEBUG] MATCH: {p} -> @{current_lead}", flush=True)
                        else:
                            print(f"[DEBUG] ⚠️ Найдена команда {p}, но лид не определен.", flush=True)

        print(f"✅ Загружено {len(leads_map)} привязок.", flush=True)
        return leads_map

    except Exception as e:
        print(f"❌ Ошибка парсинга Confluence: {e}", flush=True)
        return {}
