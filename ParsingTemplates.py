import asyncio
import aiofiles
from aiohttp import ClientSession
from time import time as current_time
import os.path

import safe_requests

# parse_function Ч функци€ дл€ разбора html-текста
# urls Ч итерабельный объект со строками-урлами
def parse_site(parse_function, urls, output_filename = None, wait_time = DEFAULT_WAIT_TIME, print_progress=False):
    async with ClientSession() as session:
        queue = asyncio.Queue()
        # list of producers for async page loading
        producers = [asyncio.create_task(download_and_parse(parse_function, url, session, queue, print_progress)) for url in urls]
        file_loader = asyncio.create_task(load_parsed_data_to_file(queue, output_filename, print_progress))
        await asyncio.gather(*producers)
        await queue.join()
        file_loader.cancel()
        await asyncio.gather(file_loader, return_exceptions=True)

async def load_parsed_data_to_file(queue, output_filename, print_progress):
    if print_progress:
        while True:
            data, url = await queue.get()
            async with aiofiles.open(filename, "a") as f:
                for element in arr:
                    await f.write(element + separator)
            print("Saved data from " + url)
            queue.task_done()
    else:
        while True:
            data = await queue.get()
            async with aiofiles.open(filename, "a") as f:
                for element in arr:
                    await f.write(element + separator)
            queue.task_done()

async def download_and_parse(parse_function, page, session, queue, print_progress):
    html_text = await fetch_html(url, session)
    if print_progress:
        print("Downloaded " + url)
    html = parse_html(html_text)
    result = parse_function(html)
    if print_progress:
        await queue.put((html, page))
    else:
        await queue.put(html)


LAST_REQUEST_TIME = current_time()
DEFAULT_WAIT_TIME = 0.1

async def safe_get(url, session, wait_time=DEFAULT_WAIT_TIME):
    global LAST_REQUEST_TIME
    sleep_time = 0
    if wait_time != -1 and abs(current_time() - LAST_REQUEST_TIME) < wait_time:
        sleep_time = abs(current_time() - LAST_REQUEST_TIME)
    LAST_REQUEST_TIME = current_time() + sleep_time
    if sleep_time > 0:
        await asyncio.sleep(sleep_time)
    return await fetch_html(url, session)

async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    html = await resp.text()
    return html