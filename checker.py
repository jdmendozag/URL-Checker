import asyncio
import aiohttp
import csv
import sys
import os
import random

# --- CONFIGURACIÓN ---
FILE_PATH = 'urlchecker.csv'
TIMEOUT_SECONDS = 15
URLS_POR_SEGUNDO = 1.5 # Velocidad conservadora por cada máquina

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1"
]

async def check_url(session, url, semaphore):
    async with semaphore:
        for intento in range(2):
            try:
                headers = {'User-Agent': random.choice(USER_AGENTS)}
                async with session.get(url, timeout=TIMEOUT_SECONDS, allow_redirects=True, headers=headers) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(30)
                        continue
                    return resp.status
            except Exception:
                await asyncio.sleep(2)
        return "Error/Timeout"

async def main():
    # Obtener el número de lote desde los argumentos de GitHub
    batch_idx = int(sys.argv[1]) # 0 a 9
    total_batches = int(sys.argv[2]) # 10

    # Leer todas las URLs
    with open(FILE_PATH, mode='r', encoding='utf-8') as f:
        rows = list(csv.reader(f))
        header = rows[0]
        data = [row[0] for row in rows[1:]]

    # Calcular el segmento que le toca a esta máquina
    chunk_size = len(data) // total_batches
    start = batch_idx * chunk_size
    end = start + chunk_size if batch_idx < total_batches - 1 else len(data)
    mi_segmento = data[start:end]

    print(f"Máquina {batch_idx}: Procesando {len(mi_segmento)} URLs (del índice {start} al {end})")

    semaphore = asyncio.Semaphore(5) # Máximo 5 conexiones simultáneas por máquina
    connector = aiohttp.TCPConnector(limit=5, force_close=True)
    
    results = []
    async with aiohttp.ClientSession(connector=connector) as session:
        for i, url in enumerate(mi_segmento):
            status = await check_url(session, url, semaphore)
            results.append([url, status])
            
            # Control de velocidad (1.5 URLs por segundo aprox)
            await asyncio.sleep(1 / URLS_POR_SEGUNDO)
            
            if i % 100 == 0:
                print(f"Máquina {batch_idx}: {i}/{len(mi_segmento)} completadas...")

    # Guardar el mini-reporte de esta máquina
    output_name = f"resultado_batch_{batch_idx}.csv"
    with open(output_name, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Status_Real"])
        writer.writerows(results)

if __name__ == "__main__":
    asyncio.run(main())