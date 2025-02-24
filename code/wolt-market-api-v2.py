import asyncio
from pywolt.api import Wolt
from typing import List, Dict
import json
import logging
import time
from datetime import datetime

class WoltMarketAPI:
    def __init__(self):
        """Initialize WoltMarketAPI specifically for # location"""
        self.wolt = Wolt(lat="#", lon="#")
        self.base_url = "https://consumer-api.wolt.com/consumer-api/consumer-assortment/v1"
        self.venue_slug = "#slug"
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('wolt_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    async def get_categories(self) -> List[Dict]:
        """Get categories for #Wolt Market"""
        try:
            response = await self.wolt.sesh.get(
                f"{self.base_url}/venues/slug/{self.venue_slug}/assortment"
            )
            data = response.json()
            
            categories = []
            for category in data.get('categories', []):
                categories.append({
                    'id': category.get('id'),
                    'name': category.get('name'),
                    'description': category.get('description', ''),
                    'slug': category.get('slug')
                })
            
            self.logger.info(f"Found {len(categories)} categories")
            return categories
        except Exception as e:
            self.logger.error(f"Error fetching categories: {str(e)}")
            return []

    async def get_category_items(self, category_slug: str, retries: int = 3) -> List[Dict]:
        """Get items for a specific category with retries"""
        for attempt in range(retries):
            try:
                self.logger.info(f"Fetching items for category {category_slug} (attempt {attempt + 1})")
                
                response = await self.wolt.sesh.get(
                    f"{self.base_url}/venues/slug/{self.venue_slug}/assortment/categories/slug/{category_slug}",
                    params={'language': 'sk'}
                )
                    
                    
                if response.status_code != 200:
                    self.logger.warning(f"Got status code {response.status_code} for category {category_slug}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2)  # Wait before retry
                        continue
                    return []

                data = response.json()
                items = data.get('items', [])
                   
                
                formatted_items = []
                for item in items:
                    try:
                        formatted_item = {
                            'id': item.get('id'),
                            'name': item.get('name'),
                            'price': item.get('price', 0) / 100,
                            'barcode': item.get('barcode_gtin'),
                            'unit_info': item.get('unit_info'),

                        }
                        formatted_items.append(formatted_item)
                    except Exception as item_error:
                        self.logger.error(f"Error processing item in {category_slug}: {str(item_error)}")
                
                self.logger.info(f"Successfully fetched {len(formatted_items)} items from category {category_slug}")
                return formatted_items

            except Exception as e:
                self.logger.error(f"Error fetching items for category {category_slug} (attempt {attempt + 1}): {str(e)}")
                if attempt < retries - 1:
                    await asyncio.sleep(2)  # Wait before retry
                    continue
                return []

    async def fetch_all_data(self):
        """Fetch all categories and their items from Wolt Market Ružinov"""
        start_time = time.time()
        self.logger.info("Starting data fetch for Wolt Market Ružinov")
        
        all_data = {
            'venue_slug': self.venue_slug,
            'categories': [],
            'timestamp': datetime.now().isoformat(),
            'stats': {
                'total_categories': 0,
                'total_items': 0,
                'categories_with_items': 0,
                'empty_categories': 0
            }
        }
        
        # Get all categories
        categories = await self.get_categories()
        all_data['stats']['total_categories'] = len(categories)
        empty = False
        
        # Get items for each category with delay between requests
        for category in categories:
            # Add delay between requests to avoid rate limiting
            await asyncio.sleep(1)
            
            items = await self.get_category_items(category['slug'])
            
          
                
            category_data = {
                'name': category['name'],
                'description': category['description'],
                'slug': category['slug'],
                'items': items
            }
            
            # Update statistics

            all_data['stats']['categories_with_items'] += 1
            all_data['stats']['total_items'] += len(items)
            
            all_data['categories'].append(category_data)
            
            if "ovocie" in category["name"].strip().lower():
                empty = True
                break
        
        if empty:
            slug_list = list()
    
            # opening categories.txt, where I had specific slugs, that are outside of normal category scopes
            # Wolt treats subcategories as separate categories
            with open("cats.txt", mode="r") as file:
                cat_list = list(file)
                for slug in cat_list:
                    slug = slug.split("/")
                    slug_list.append(slug[-1].removesuffix("\n").strip())
                for category in slug_list:
                    # Add delay between requests to avoid rate limiting
                    await asyncio.sleep(1)
                    
                    items = await self.get_category_items(category)
                    
                    if len(items) == 0:
                        empty = True
                        break
                    
                    
                    category_data = {
                        # Creating name from slug
                        'name': (lambda x: x[:-4] if x[:-4].lstrip('-').isdigit()
                                 and int(x[:-4]) > 0 else x[:-3])(category),
                        'slug': category,
                        'items': items
                    }
                    
                    all_data['stats']['categories_with_items'] += 1
                    all_data['stats']['total_items'] += len(items)
                
                    all_data['categories'].append(category_data)
                    
        # Add execution time to stats
        all_data['stats']['execution_time'] = time.time() - start_time
        
        # Save data
        self.save_to_json(all_data, '#jasoooooon')
        
        # Log summary
        self.logger.info(f"Fetch completed in {all_data['stats']['execution_time']:.2f} seconds")
        self.logger.info(f"Total categories: {all_data['stats']['total_categories']}")
        self.logger.info(f"Categories with items: {all_data['stats']['categories_with_items']}")
        self.logger.info(f"Empty categories: {all_data['stats']['empty_categories']}")
        self.logger.info(f"Total items: {all_data['stats']['total_items']}")
        
        return all_data

    def save_to_json(self, data: Dict, filename: str):
        """Save data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Data saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving to {filename}: {str(e)}")

async def main():
    api = WoltMarketAPI()
    data = await api.fetch_all_data()
    
    # Print summary
    print("\nSummary:")
    print(f"Total categories: {data['stats']['total_categories']}")
    print(f"Categories with items: {data['stats']['categories_with_items']}")
    print(f"Empty categories: {data['stats']['empty_categories']}")
    print(f"Total items: {data['stats']['total_items']}")
    print(f"Execution time: {data['stats']['execution_time']:.2f} seconds")
    
    # Print categories with no items
    print("\nCategories with no items:")
    for category in data['categories']:
        if not category['items']:
            print(f"- {category['name']} (slug: {category['slug']})")

if __name__ == "__main__":
    asyncio.run(main())
