import asyncio
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from config import api_id, api_hash, CHANNELS
from text_processing import extract_fields
import pandas as pd


async def scrape_channel(client, channel_username):
    print(f"Parsing channel: @{channel_username}")
    messages = []
    try:
        channel = await client.get_entity(channel_username)
        async for message in client.iter_messages(channel, limit=200):
            if message.message and any(
                w in message.message.lower()
                for w in ['вакансия', 'работа', 'позиция', 'ищем', 'требуется', 'должность']
            ):
                messages.append({
                    'channel': channel_username,
                    'date': message.date.strftime('%Y-%m-%d %H:%M'),
                    'text': message.message
                })
        print(f"Collected {len(messages)} messages from @{channel_username}")
    except FloodWaitError as e:
        print(f"⏳ FloodWait: sleeping for {e.seconds} seconds...")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        print(f"Error in @{channel_username}: {e}")
    return messages


async def collect_all():
    async with TelegramClient('session', api_id, api_hash) as client:
        all_messages = []
        for channel in CHANNELS:
            msgs = await scrape_channel(client, channel)
            all_messages.extend(msgs)
        return all_messages


def process_vacancies(messages):
    rows = []
    for msg in messages:
        data = extract_fields(msg['text'])
        data['channel'] = msg['channel']
        data['date'] = msg['date']
        rows.append(data)

    df = pd.DataFrame(rows)
    df = df[df['position'].notnull()].reset_index(drop=True)
    print(f"\nFound {len(df)} vacancies with positions")
    return df
