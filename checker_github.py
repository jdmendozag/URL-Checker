import asyncio, aiohttp, csv, sys, os
from bs4 import BeautifulSoup

FILE_PATH = 'urlchecker.csv'
URLS_POR_SEGUNDO = 1.0 
BLACKLIST = ['/nl/', '/da/']

def extraer_idioma(url):
    # Intenta extraer el idioma de la URL (ej: /es-mx/, /fr/, etc)
    parts = url.split('/')
    if len(parts) > 3:
        lang_part = parts[3]
        if len(lang_part) <= 5 and '-' in lang_part or len(lang_part) == 2:
            return lang_part
    return "en" # Default

async def fetch_data(session, url, semaphore):
    async with semaphore:
        for intento in range(3):
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'}
                async with session.get(url, timeout=25, allow_redirects=True, headers=headers) as resp:
                    status = resp.status
                    if status in [503, 429]:
                        await asyncio.sleep(30 * (intento + 1))
                        continue
                    
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Extracción de datos
                    h1 = soup.find('h1').get_text(strip=True) if soup.find('h1') else ""
                    h2 = soup.find('h2').get_text(strip=True) if soup.find('h2') else ""
                    m_title = soup.title.string.strip() if soup.title else ""
                    m_desc = ""
                    meta_d = soup.find('meta', attrs={'name': 'description'})
                    if meta_d: m_desc = meta_d.get('content', '').strip()

                    return [status, extraer_idioma(url), h1, h2, m_title, m_desc]
            except:
                await asyncio.sleep(5)
        return ["Error", extraer_idioma(url), "", "", "", ""]

async def main():
    batch_idx, total_batches = int(sys.argv[1]), int(sys.argv[2])
    with open(FILE_PATH, mode='r', encoding='utf-8-sig') as f:
        rows = list(csv.reader(f))
        raw_data = [row[0] for row in rows[1:] if row]
        data = [u for u in raw_data if not any(lang in u for lang in BLACKLIST)]

    chunk_size = len(data) // total_batches
    start, end = batch_idx * chunk_size, (batch_idx + 1) * chunk_size if batch_idx < total_batches - 1 else len(data)
    mi_segmento = data[start:end]

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(2) # Bajamos a 2 para procesar HTML con calma
        results = []
        for url in mi_segmento:
            res_fields = await fetch_data(session, url, semaphore)
            results.append([url] + res_fields)
            await asyncio.sleep(1 / URLS_POR_SEGUNDO)

    with open(f"resultado_batch_{batch_idx}.csv", "w", newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Status", "Idioma", "H1", "H2", "MetaTitle", "MetaDescription"])
        writer.writerows(results)

if __name__ == "__main__":
    asyncio.run(main())
