import aiohttp

from services.data_providers.base import DataProviderAdapter


class CoinMarketCapAdapter(DataProviderAdapter):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://pro-api.coinmarketcap.com"

    async def fetch_global_metrics(self):
        url = f"{self.base_url}/v1/global-metrics/quotes/latest"

        headers = {"X-CMC_PRO_API_KEY": self.api_key}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                return await resp.json()
