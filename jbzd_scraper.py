import aiohttp
import asyncio
import time
import csv
from asyncio import Queue

START_ID = 1100001
END_ID = 1200000
OUTPUT_CSV = "jbzd_users_selected.csv"

CONCURRENT_WORKERS = 50
RETRY_DELAY = 1
PROGRESS_INTERVAL = 100

queue = Queue()
total_downloaded = 0
total_404 = 0
start_time = time.time()
batch_data = []

FIELDS = ['id', 'name', 'slug', 'rank', 'active', 'banned', 'is_admin', 'is_moderator']

for user_id in range(START_ID, END_ID + 1):
    queue.put_nowait(user_id)

async def fetch_user(session, user_id):
    url = f"https://jbzd.com.pl/mikroblog/user/profile/{user_id}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                data = await response.json()
                if data.get("status") == "success":
                    user = data["user"]
                    return {k: user.get(k) for k in FIELDS}
            elif response.status == 404:
                return "NOT_FOUND"
    except Exception:
        return None
    return None

async def save_batch(batch):
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writerows(batch)

async def worker(worker_id, session, queue, file_lock, counter_lock):
    global total_downloaded, total_404, batch_data
    while not queue.empty():
        user_id = await queue.get()
        user_data = await fetch_user(session, user_id)
        retried = False

        if user_data == "NOT_FOUND":
            total_404 += 1
        elif user_data:
            batch_data.append(user_data)
        else:
            await asyncio.sleep(RETRY_DELAY)
            await queue.put(user_id)
            retried = True

        async with counter_lock:
            if not retried:
                total_downloaded += 1

        if total_downloaded % PROGRESS_INTERVAL == 0 and batch_data:
            async with file_lock:
                await save_batch(batch_data)
                batch_data = []

            remaining_normal = max(0, END_ID - START_ID + 1 - total_downloaded - total_404)
            retry_in_queue = max(0, queue.qsize() - remaining_normal)

            elapsed = time.time() - start_time
            avg_time_per_profile = elapsed / total_downloaded
            remaining = (END_ID - total_downloaded) * avg_time_per_profile / CONCURRENT_WORKERS
            hrs, rem = divmod(remaining, 3600)
            mins, secs = divmod(rem, 60)

            print(f"[Progress] Pobrano: {total_downloaded}, 404: {total_404}, "
                  f"Retry w kolejce: {retry_in_queue}, "
                  f"Szacowany czas do końca: {int(hrs)}h {int(mins)}m {int(secs)}s",
                  flush=True)

        queue.task_done()

async def main():
    file_lock = asyncio.Lock()
    counter_lock = asyncio.Lock()

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()

    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(worker(i, session, queue, file_lock, counter_lock)) for i in range(CONCURRENT_WORKERS)]
        await queue.join()
        for task in tasks:
            task.cancel()

    if batch_data:
        await save_batch(batch_data)

if __name__ == "__main__":
    asyncio.run(main())
