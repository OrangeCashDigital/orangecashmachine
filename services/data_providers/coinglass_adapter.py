import aiohttp


class CoinglassAdapter:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://open-api.coinglass.com/api"

    async def fetch_funding_rates(self):
        url = f"{self.base_url}/futures/funding_rates"

        headers = {"CG-API-KEY": self.api_key}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                return await resp.json()
