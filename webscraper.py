import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
import boto3
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Starting web scraper")

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

def extract_date_from_title(title):
    try:
        date_str = title.split("day, ")[-1]
        date_obj = datetime.strptime(date_str, '%B %d, %Y')
        return date_obj
    except Exception as e:
        print(f"Error extracting date from title: {e}")
        return None

async def scrapeGame(game_id, semaphore, retries=3):
    url = f'https://j-archive.com/showgame.php?game_id={game_id}'
    async with semaphore, aiohttp.ClientSession() as session:
        for attempt in range(retries):
            try:
                logger.info(f"Fetching game ID {game_id}, attempt {attempt + 1}")
                pageToScrape = await fetch(session, url)
                soup = BeautifulSoup(pageToScrape, "html.parser")

                no_game = soup.find('p', attrs={'class': 'error'})
                if no_game:
                    logger.warning(f"No game {game_id} in database")
                    return {'game_id': game_id, 'error': f'No game {game_id} in database'}

                game_title = soup.find('div', attrs={'id': 'game_title'})
                game_title_text = game_title.get_text(strip=True) if game_title else "Title not found"

                game_date = extract_date_from_title(game_title_text)

                game_comments = soup.find('div', attrs={'id': 'game_comments'})
                game_comments_text = game_comments.get_text(strip=True) if game_comments else "Comments not found"

                categories = [cat.get_text(strip=True) for cat in soup.findAll('td', attrs={'class': 'category_name'})]
                category_comments = [com.get_text(strip=True) for com in soup.findAll('td', attrs={'class': 'category_comments'})]

                jeopardy_cells = []
                jeopardy_clues = []
                jeopardy_responses = []
                double_jeopardy_cells = []
                double_jeopardy_clues = []
                double_jeopardy_responses = []

                for y in range(1, 6):
                    for x in range(1, 7):
                        clue = soup.find('td', attrs={'id': f'clue_J_{x}_{y}'})
                        if clue:
                            jeopardy_clues.append(clue.get_text(strip=True))
                            jeopardy_cells.append(f'J_{x}_{y}')

                        double_clue = soup.find('td', attrs={'id': f'clue_DJ_{x}_{y}'})
                        if double_clue:
                            double_jeopardy_clues.append(double_clue.get_text(strip=True))
                            double_jeopardy_cells.append(f'DJ_{x}_{y}')

                final_jeopardy_clue = soup.find('td', attrs={'id': 'clue_FJ'})
                final_jeopardy_clue_text = final_jeopardy_clue.get_text(strip=True) if final_jeopardy_clue else "Final Jeopardy clue not found"

                final_jeopardy_response = "Final Jeopardy response not found"
                responses = soup.findAll('em', attrs={'class': 'correct_response'})
                for count, response in enumerate(responses, start=1):
                    if count <= len(jeopardy_clues):
                        jeopardy_responses.append(response.get_text(strip=True))
                    elif len(jeopardy_clues) < count <= len(jeopardy_clues) + len(double_jeopardy_clues):
                        double_jeopardy_responses.append(response.get_text(strip=True))
                    else:
                        final_jeopardy_response = response.get_text(strip=True)

                logger.info(f"Successfully scraped game ID {game_id}")
                return {
                    'game_id': game_id,
                    'game_title': game_title_text,
                    'game_date': game_date.isoformat() if game_date else None,
                    'game_comments': game_comments_text,
                    'categories': categories,
                    'category_comments': category_comments,
                    'jeopardy_round': {
                        'clues': jeopardy_clues,
                        'responses': jeopardy_responses,
                        'cells': jeopardy_cells
                    },
                    'double_jeopardy_round': {
                        'clues': double_jeopardy_clues,
                        'responses': double_jeopardy_responses,
                        'cells': double_jeopardy_cells
                    },
                    'final_jeopardy': {
                        'clue': final_jeopardy_clue_text,
                        'response': final_jeopardy_response
                    }
                }

            except aiohttp.ClientError as e:
                logger.error(f"Game ID {game_id} generated an exception: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return {'game_id': game_id, 'error': 'Failed after multiple retries'}


async def scrapeGames(game_ids):
    semaphore = asyncio.Semaphore(10)
    tasks = [scrapeGame(game_id, semaphore) for game_id in game_ids]

    results = await asyncio.gather(*tasks)
    return results

def sort_key(entry):
    date_str = entry.get('game_date')
    if date_str:
        try:
            return (datetime.strptime(date_str.split("T")[0], "%Y-%m-%d"), 0)
        except ValueError:
            pass
    return (datetime.min, entry.get('game_id'))

def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                             aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        return False
    return True

def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                             aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        logger.info(f"Successfully uploaded {file_name} to {bucket}/{object_name}")
        return True
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        return False

def update_json_file():
    game_ids = range(1, 10001)
    scraped_data = asyncio.run(scrapeGames(game_ids))
    sorted_jeopardy_games = sorted(scraped_data, key=sort_key, reverse=True)
    file_name = 'jeopardy_games.json'
    
    with open(file_name, 'w') as f:
        json.dump(sorted_jeopardy_games, f, indent=4)
    
    logger.info("Data has been written to jeopardy_games.json")

    if upload_to_s3(file_name, os.environ['S3_BUCKET_NAME']):
        logger.info("Data successfully uploaded to S3")
    else:
        logger.error("Failed to upload data to S3")

if __name__ == "__main__":
    try:
        update_json_file()
        logger.info("Web scraper finished successfully")
    except Exception as e:
        logger.error(f"Web scraper encountered an error: {e}")