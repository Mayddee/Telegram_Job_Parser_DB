import re

def clean_hashtags(text):
    """Remove all hashtags"""
    return re.sub(r'#\w+', '', text).strip()


def extract_position(text):
    text_clean = clean_hashtags(text)
    match = re.search(r'Должность:\s*([^\n\r]+)', text_clean, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    patterns = [
        r'Вакансия:\s*([^\n]+)',
        r'Позиция:\s*([^\n]+)',
        r'Ищем\s+([^\n]+)',
        r'Требуется\s+([^\n]+)',
    ]
    for pat in patterns:
        match = re.search(pat, text_clean, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    job_pattern = (
        r'\b(Backend|Frontend|Full[- ]?stack|Mobile|iOS|Android|Flutter|React|Python|Java|Golang|\.NET|'
        r'DevOps|QA|Tester|UI/UX|Designer|Data Scientist|ML Engineer|Product Manager|Project Manager|'
        r'HR|Recruiter|System Administrator|Helpdesk|Software Engineer|Team Lead|Tech Lead|CTO|Developer|'
        r'Разработчик|Программист|Дизайнер|Тестировщик|Аналитик|Менеджер)\b'
    )
    match = re.search(job_pattern, text_clean, re.IGNORECASE)
    if match:
        return match.group(0).strip()
    return None


def extract_company(text):
    text_clean = clean_hashtags(text)
    match = re.search(r'Компания:\s*([^\n\r]+)', text_clean, re.IGNORECASE)
    if match:
        return re.sub(r'https?://\S+', '', match.group(1)).strip()
    match = re.search(r'(?:О компании|В компани[ию])\s+([^\n]+)', text_clean, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def extract_location(text):
    text_clean = clean_hashtags(text)
    match = re.search(r'(?:Город|Локация):\s*([^\n\r]+)', text_clean, re.IGNORECASE)
    if match:
        return match.group(1).split(',')[0].strip()

    cities = [
        'Алматы', 'Астана', 'Нур-Султан', 'Шымкент', 'Караганда', 'Актобе', 'Тараз', 'Павлодар',
        'Усть-Каменогорск', 'Семей', 'Атырау', 'Костанай', 'Кызылорда', 'Уральск',
        'Петропавловск', 'Актау', 'Темиртау', 'Туркестан'
    ]
    for city in cities:
        if city.lower() in text.lower():
            return city

    work_format = re.search(r'\b(офис|удален[ка|но]|гибрид|remote|office|hybrid)\b', text, re.IGNORECASE)
    if work_format:
        return work_format.group(0).capitalize()
    return None


def extract_salary(text):
    text_clean = clean_hashtags(text)
    match = re.search(r'(?:Оплата|Зарплата|ЗП):\s*([^\n\r]+)', text_clean, re.IGNORECASE)
    if match:
        return match.group(1).split(',')[0].strip()
    return None


def extract_contacts(text):
    text_clean = clean_hashtags(text)
    contacts = []
    contacts.extend([f'@{u}' for u in re.findall(r'@(\w+)', text_clean)])
    contacts.extend(re.findall(r'(?:https?://)?t\.me/(\w+)', text_clean))
    contacts.extend(re.findall(r'\+7\d{10}', text_clean))
    contacts.extend(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', text_clean))
    return ', '.join(dict.fromkeys(contacts)) or None


def extract_description(text):
    text = clean_hashtags(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip() if text else None


def extract_fields(text):
    return {
        'position': extract_position(text),
        'company': extract_company(text),
        'location': extract_location(text),
        'salary': extract_salary(text),
        'contacts': extract_contacts(text),
        'description': extract_description(text)
    }

