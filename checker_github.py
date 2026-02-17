import asyncio, aiohttp, csv, sys, os

FILE_PATH = 'urlchecker.csv'
URLS_POR_SEGUNDO = 1.0 
# Idiomas a omitir
BLACKLIST = ['/nl/', '/da/']

async def check_url(session, url, semaphore):
    async with semaphore:
        # Reintentos inteligentes para evitar el 503
        for intento in range(3):
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'}
                async with session.get(url, timeout=20, allow_redirects=True, headers=headers) as resp:
                    if resp.status in [503, 429]:
                        await asyncio.sleep(30 * (intento + 1))
                        continue
                    return resp.status
            except:
                await asyncio.sleep(5)
        return "Error/503"

async def main():
    batch_idx, total_batches = int(sys.argv[1]), int(sys.argv[2])
    with open(FILE_PATH, mode='r', encoding='utf-8-sig') as f:
        rows = list(csv.reader(f))
        # FILTRO DE IDIOMAS: Solo URLs que no tengan /nl/ ni /da/
        raw_data = [row[0] for row in rows[1:] if row]
        data = [u for u in raw_data if not any(lang in u for lang in BLACKLIST)]

    chunk_size = len(data) // total_batches
    start, end = batch_idx * chunk_size, (batch_idx + 1) * chunk_size if batch_idx < total_batches - 1 else len(data)
    mi_segmento = data[start:end]

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(3) # Menos presión al servidor
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
