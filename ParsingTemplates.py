import asyncio
import aiofiles
from aiohttp import ClientSession
from time import time as current_time
import os.path

from bs4 import BeautifulSoup as BS

LAST_REQUEST_TIME = current_time()
DEFAULT_WAIT_TIME = 0.1

# parse_function Ч функци€ дл€ разбора html-текста
# urls Ч итерабельный объект со строками-урлами
async def parse_site(
 parse_function,
 urls,
 filename = None, 
 wait_time = DEFAULT_WAIT_TIME, 
 print_progress=True):
    async with ClientSession() as session:
        queue = asyncio.Queue()
        # list of producers for async page loading
        producers = [asyncio.create_task(download_and_parse(parse_function, url, session, queue, print_progress)) for url in urls]
        if filename is not None:
            consumer = asyncio.create_task(load_parsed_data_to_file(queue, filename, print_progress))
        else:
            output_list = []
            consumer = asyncio.create_task(collect_parsed_data_to_list(queue, output_list, print_progress))
        await asyncio.gather(*producers)
        await queue.join()
        consumer.cancel()
        #await asyncio.gather(consumer, return_exceptions=True)
        #print(100500)
        if filename is None:
            return output_list

async def load_parsed_data_to_file(queue, output_filename, print_progress):
    if print_progress:
        while True:
            data, url = await queue.get()
            async with aiofiles.open(filename, "a") as f:
                if type(data) == str:
                    await f.write(data + '\n')
                else:
                    await f.writelines('\n'.join(data))
                
            print("Saved data from " + url)
            queue.task_done()
            print(queue.qsize())
    else:
        while True:
            data = await queue.get()
            async with aiofiles.open(filename, "a") as f:
                if type(data) == str:
                    await f.write(data + '\n')
                else:
                    await f.writelines('\n'.join(data))
            queue.task_done()
            print(queue.qsize())

async def collect_parsed_data_to_list(queue, output_list, print_progress):
    if print_progress:
        while True:
            data, url = await queue.get()
            if type(data) == str:
                output_list.append(data)
            else:
                output_list += data
            print("Saved data from " + url)
            queue.task_done()
    else:
        while True:
            data = await queue.get()
            if type(data) == str:
                output_list.append(data)
            else:
                output_list += data
            queue.task_done()

async def download_and_parse(parse_function, page, session, queue, print_progress):
    html_text = await fetch_html(page, session)
    html = BS(html_text, 'html.parser')
    result = parse_function(html)
    if print_progress:
        await queue.put((result, page))
    else:
        await queue.put(result)
    print(queue.qsize())
    if print_progress:
        print("Downloaded " + page)

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