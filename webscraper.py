import json
import time
import atexit
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from flask import Flask, jsonify, request
import os
from apscheduler.schedulers.background import BackgroundScheduler
import aiohttp
import asyncio
import ijson

app = Flask(__name__)

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
                pageToScrape = await fetch(session, url)
                soup = BeautifulSoup(pageToScrape, "html.parser")

                no_game = soup.find('p', attrs={'class': 'error'})
                if no_game:
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
                print(f"Game ID {game_id} generated an exception: {e}")
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

def update_json_file():
    game_ids = range(1, 10000)

    scraped_data = asyncio.run(scrapeGames(game_ids))

    sorted_jeopardy_games = sorted(scraped_data, key=sort_key, reverse=True)

    with open('jeopardy_games.json', 'w') as f:
        json.dump(sorted_jeopardy_games, f, indent=4)

    print("Data has been written to jeopardy_games.json")

update_json_file()

@app.route('/api/games', methods=['GET'])
def get_all_games():
    page = request.args.get('page', default=1, type=int)
    size = request.args.get('size', default=10, type=int)

    start = (page - 1) * size
    end = start + size

    games_data = []
    with open('jeopardy_games.json', 'r') as f:
        parser = ijson.parse(f)
        for prefix, event, value in parser:
            if prefix.endswith('.game_id'):
                if len(games_data) >= end:
                    break
                games_data.append(value)

    paginated_games = games_data[start:end]

    response = {
        'page': page,
        'size': size,
        'total_games': len(games_data),
        'total_pages': (len(games_data) + size - 1) // size,
        'games': paginated_games
    }

    return jsonify(response)

@app.route('/api/games/ids/<int:game_id>', methods=['GET'])
def get_game_by_id(game_id):
    with open('jeopardy_games.json', 'r') as f:
        games_data = json.load(f)

    for game in games_data:
        if 'error' not in game and game['game_title'].endswith(str(game_id)):
            return jsonify(game)
    return jsonify({'error': f'Game {game_id} not found'}), 404

@app.route('/api/games/date/<string:game_date>', methods=['GET'])
def get_games_by_date(game_date):
    with open('jeopardy_games.json', 'r') as f:
        games_data = json.load(f)

    games_by_date = [game for game in games_data if game.get('game_date', '').startswith(game_date)]
    
    if not games_by_date:
        return jsonify({'error': f'No games found for date {game_date}'}), 404

    return jsonify(games_by_date)

scheduler = BackgroundScheduler()
scheduler.add_job(func=update_json_file, trigger="interval", hours=24)
scheduler.start()

atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)