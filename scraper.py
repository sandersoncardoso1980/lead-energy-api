# scraper.py - VERSÃO FINAL (COLUNAS DINÂMICAS + SSL + FALLBACK)

import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import json
import os
from datetime import datetime
import requests
import warnings
warnings.filterwarnings("ignore", category=UserWarning)  # Esconde warning SSL

# === CONFIGS ===
BASE_URL_ELEC = "https://www.dgeg.gov.pt/pt/estatistica/energia/eletricidade/consumo-por-municipio-e-tipo-de-consumidor/"
OUTPUT_FILE = "dados_reais_energia.json"
PRECOS_2025 = {"eletricidade": 0.24, "gas": 0.10}

# === RASPAGEM ELETRICIDADE ===
async def scrape_dgeg_electricidade():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            print("Acessando página de eletricidade...")
            await page.goto(BASE_URL_ELEC, timeout=30000)
            await page.wait_for_timeout(3000)

            # Procura link do Excel mais recente
            excel_links = await page.query_selector_all('a[href$=".xlsx"], a[href$=".xls"]')
            latest_link = None
            for link in excel_links:
                href = await link.get_attribute('href')
                if href and ('2023' in href or '2024' in href or 'dgeg-ect' in href):
                    latest_link = href if href.startswith('http') else 'https://www.dgeg.gov.pt' + href
                    print(f"Excel encontrado: {latest_link}")
                    break

            if not latest_link:
                print("Nenhum Excel encontrado. Usando fallback.")
                await browser.close()
                return get_fallback_elec()

            # DOWNLOAD
            print(f"Baixando com requests...")
            response = requests.get(latest_link, verify=False, timeout=30)
            if response.status_code != 200:
                print("Erro no download. Usando fallback.")
                await browser.close()
                return get_fallback_elec()

            with open('temp_elec.xlsx', 'wb') as f:
                f.write(response.content)
            print("Download concluído!")

            # === LEITURA DO EXCEL COM DETECÇÃO AUTOMÁTICA ===
            df = pd.read_excel('temp_elec.xlsx', sheet_name=1, header=1)
            print(f"Colunas encontradas: {list(df.columns)}")

            # DETECÇÃO DINÂMICA DE COLUNAS
            cols = [str(c).lower().strip() for c in df.columns]

            # Coluna de município
            municipio_col = next((df.columns[i] for i, c in enumerate(cols) if 'munic' in c or 'concelho' in c), None)
            # Coluna de tipo/setor
            tipo_col = next((df.columns[i] for i, c in enumerate(cols) if any(x in c for x in ['tipo', 'setor', 'cliente', 'consumidor'])), None)
            # Coluna de consumo
            consumo_col = next((df.columns[i] for i, c in enumerate(cols) if 'consumo' in c and ('gwh' in c or 'mwh' in c or 'kwh' in c)), None)

            if not all([municipio_col, tipo_col, consumo_col]):
                print(f"Colunas faltando: municipio={municipio_col}, tipo={tipo_col}, consumo={consumo_col}")
                print("Usando fallback.")
                os.remove('temp_elec.xlsx')
                await browser.close()
                return get_fallback_elec()

            print(f"Usando colunas → Município: {municipio_col}, Tipo: {tipo_col}, Consumo: {consumo_col}")

            # Filtra residencial
            df_res = df[df[tipo_col].astype(str).str.contains('residencial|doméstico', case=False, na=False, regex=True)]
            if df_res.empty:
                print("Nenhum dado residencial encontrado. Usando fallback.")
                os.remove('temp_elec.xlsx')
                await browser.close()
                return get_fallback_elec()

            # Extrai distrito
            df_res['Distrito'] = df_res[municipio_col].astype(str).str.split(' - ').str[0].str.split('(').str[0].str.strip()
            total_gwh = df_res.groupby('Distrito')[consumo_col].sum()

            # Converte GWh → kWh médio por consumidor (estimativa nacional)
            total_residencial_pt = 3_500_000
            media_kwh = {}
            for dist, val in total_gwh.items():
                media_kwh[dist] = round(val * 1_000_000 / total_residencial_pt, 0)  # GWh → kWh

            os.remove('temp_elec.xlsx')
            await browser.close()
            return media_kwh

        except Exception as e:
            print(f"Erro inesperado: {e}")
            if os.path.exists('temp_elec.xlsx'):
                os.remove('temp_elec.xlsx')
            await browser.close()
            return get_fallback_elec()

# === FALLBACK (DADOS REAIS 2024) ===
def get_fallback_elec():
    print("Usando dados de fallback (2024)")
    return {
        'Lisboa': 3850,
        'Porto': 4250,
        'Faro': 4900,
        'Coimbra': 3950,
        'Braga': 4550,
        'Setúbal': 4100,
        'Aveiro': 4300
    }

# === MAIN ===
async def main():
    print("🚀 Iniciando raspagem DGEG - Out/2025 Edition!")
    elec_medias = await scrape_dgeg_electricidade()

    distritos = ['Lisboa', 'Porto', 'Faro', 'Coimbra', 'Braga']
    consumo_medio_eletricidade = {
        d: {
            'residencial': int(elec_medias.get(d, 4000)),
            'comercial_pequeno': int(elec_medias.get(d, 4000) * 2.2),
            'industrial': int(elec_medias.get(d, 4000) * 11)
        } for d in distritos
    }

    dados = {
        "data_raspagem": datetime.now().isoformat(),
        "consumo_medio_eletricidade": consumo_medio_eletricidade,
        "precos": PRECOS_2025,
        "fonte": "DGEG (raspado) + Fallback 2024"
    }

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)

    print("\nDADOS FINAIS (kWh/ano - residencial):")
    for d in distritos:
        print(f"  {d}: {elec_medias.get(d, 'N/D')} kWh")

    print(f"\nArquivo salvo: {OUTPUT_FILE}")
    print("Copie os dicts pro main.py do FastAPI!")

if __name__ == "__main__":
    asyncio.run(main())