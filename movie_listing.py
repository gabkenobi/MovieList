import concurrent.futures
import csv
import requests
import random
import time

from bs4 import BeautifulSoup


headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

MAX_THREADS = 10


def extract_movie(url: str) -> None:
  res = requests.get(url, headers=headers)

  if res.status_code == 200:
    soup = BeautifulSoup(res.content, 'html.parser')

    title = soup.find('h1').get_text()
    date = (soup
      .find('ul', attrs={'data-testid': 'hero-title-block__metadata'})
      .find('a').get_text()
    )

    try:
      rating = (soup
        .find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
        .find('span').get_text()
      )
    except AttributeError as err:
        print(err)
        rating = None

    plot = soup.find('span', attrs={'data-testid': 'plot-xs_to_m'}).get_text()
  else:
    title = None
    date = None
    rating = None
    plot = None

  write(title, date, rating, plot)

def write(title: str, date: str, rating: str, plot: str) -> None:
  with open('movies.csv', mode='a+') as file:
    movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    if all([title, date, rating, plot]):
      print(
        f'''Title: {title}
        Date: {date}
        Rating: {rating}
        Plot: {plot}'''
      )

      movie_writer.writerow([title, date, rating, plot])

def extract_list_singlethreading(soup):
  base_url = 'https://imdb.com'

  movies = (soup
    .find('table').find('tbody')
    .find_all('tr')
  )

  urls = [base_url + movie.find('a')['href'] for movie in movies]

  for url in urls:
    extract_movie(url)

def extract_list_multithreading(soup):
  base_url = 'https://imdb.com'

  movies = (soup
    .find('table').find('tbody')
    .find_all('tr')
  )

  urls = [base_url + movie.find('a')['href'] for movie in movies]

  threads = min(MAX_THREADS, len(urls))
  with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    executor.map(extract_movie, urls)


def main():
  popular_movies_url = 'https://www.imdb.com/chart/moviemeter/'

  # Multi threaded call
  multi_threads_start = time.perf_counter()

  res = requests.get(popular_movies_url, headers=headers)
  soup = BeautifulSoup(res.content, 'html.parser')
  extract_list_multithreading(soup)

  multi_threads_end = time.perf_counter()

  # Single threaded call
  single_thread_start = time.perf_counter()

  res = requests.get(popular_movies_url, headers=headers)
  soup = BeautifulSoup(res.content, 'html.parser')
  extract_list_singlethreading(soup)

  single_thread_end = time.perf_counter()
  print(f'total time taken (single thread): {single_thread_end - single_thread_start}')
  print(f'Total time taken (multi threads): {multi_threads_end - multi_threads_start}')


if __name__ == '__main__':
    main()