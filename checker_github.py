import asyncio
import aiohttp
import csv
import sys
import os
import random

FILE_PATH = 'urlchecker.csv'
# Bajamos un poco la velocidad para evitar el 503
URLS_POR_SEGUNDO = 1.0 

async def check_url(session, url, semaphore):
    async with semaphore:
        for intento in range(3): # Reintenta hasta 3 veces si falla
            try:
                headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}
                async with session.get(url, timeout=20, allow_redirects=True, headers=headers) as resp:
                    if resp.status == 503 or resp.status == 429:
                        wait = (intento + 1) * 30 # Espera 30, 60, 90 segundos
                        await asyncio.sleep(wait)
                        continue
                    return resp.status
            except:
                await asyncio.sleep(5)
        return "Error/503-Persistente"

async def main():
    batch_idx, total_batches = int(sys.argv[1]), int(sys.argv[2])
    with open(FILE_PATH, mode='r', encoding='utf-8-sig') as f:
        rows = list(csv.reader(f))
        data = [row[0] for row in rows[1:] if row]

    chunk_size = len(data) // total_batches
    start = batch_idx * chunk_size
    end = start + chunk_size if batch_idx < total_batches - 1 else len(data)
    mi_segmento = data[start:end]

    semaphore = asyncio.Semaphore(3) # Menos conexiones simultáneas para ser más discretos
    async with aiohttp.ClientSession() as session:
        results = []
        for url in mi_segmento:
            status = await check_url(session, url, semaphore)
            results.append([url, status])
            await asyncio.sleep(1 / URLS_POR_SEGUNDO)

    with open(f"resultado_batch_{batch_idx}.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Status_Real"])
        writer.writerows(results)

if __name__ == "__main__":
    asyncio.run(main())
