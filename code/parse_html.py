
import aiohttp
import asyncio
import json
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict


async def fetch_page(session: aiohttp.ClientSession,
                     url: str) -> str:
    """Fetch a single page asynchronously"""
    try:
        async with session.get(url) as response:
            return await response.text()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return ""

async def process_item(session: aiohttp.ClientSession,
                       row: pd.Series) -> Dict:
    """Process a single item asynchronously"""
    item_id = row["id"]
    url = f"https://prodinfo.wolt.com/sk/#marketid/{item_id}"
    
    html = await fetch_page(session, url)
    if not html:
        return {}
    
    soup = BeautifulSoup(html, features="html.parser")
    page_headers = soup.find_all("h3")
    page_paragraphs = soup.find_all("p")
    
    headers = [header.get_text(strip=True) for header in page_headers]
    ps = [para.get_text(strip=True) for para in page_paragraphs]
    headers.insert(0, "category")
    
    # Create dictionary
    page_data = dict(zip(headers, ps * len(headers)))
    
    try:
        ingredients = page_data["ingredient"].split(",") if page_data["ingredient"] else []
        page_data["ingredient_count"] = 1 if len(ingredients) == 1 else len(ingredients)

    except KeyError:
        page_data["ingredient_count"] = pd.NA
        
    return page_data

async def get_item_data(data: pd.DataFrame) -> pd.DataFrame:
    """Process all items asynchronously"""
    async with aiohttp.ClientSession() as session:
        # Create tasks for all items
        tasks = [process_item(session, row) for _, row in data.iterrows()]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks)
        
    # Process results
    headers = list(results[0].keys()) if results and results[0] else []
    if headers:
        for col in headers:
            data[col] = [result.get(col) for result in results]
    
    return data

async def main():
    # Load and prepare data
    with open("#jsonfile", mode='r', encoding="utf8") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data=data["categories"])
    df = df.explode('items').reset_index(drop=True)
    df2 = pd.json_normalize(df["items"])
    ultimate_df = pd.merge(
        left=df, 
        right=df2, 
        left_index=True, 
        right_index=True
    ).drop(['items', 'description', 'barcode'], axis=1)
    
    # Process data asynchronously
    result_df = await get_item_data(ultimate_df)
    print("Processing complete!")
    
    # Convert dataframe to csv
    pd.DataFrame.to_csv(result_df, 'preprocessing_df.csv', index=False)
    return result_df


# Run the async code
if __name__ == "__main__":
    result = asyncio.run(main())
    print(result)
