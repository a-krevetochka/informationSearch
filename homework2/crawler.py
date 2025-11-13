import asyncio
import hashlib
import logging
import signal
import time
from argparse import ArgumentParser
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl, urljoin

import aiohttp
import motor.motor_asyncio
import yaml
import re

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger('crawler')

def normalize_url(url: str) -> str:
    p = urlparse(url)
    scheme = p.scheme.lower() or 'http'
    netloc = p.netloc.lower()
    path = p.path or '/'
    query = urlencode(sorted(parse_qsl(p.query, keep_blank_values=True)))
    normalized = urlunparse((scheme, netloc, path, '', query, ''))
    return normalized

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

class MongoCrawler:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        mongo_cfg = cfg['db']
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_cfg['uri'])
        self.db = self.client[mongo_cfg.get('name', 'crawler_db')]
        self.cfg_logic = cfg.get('logic', {})
        self.delay = float(self.cfg_logic.get('delay', 1.0))
        self.max_concurrency = int(self.cfg_logic.get('max_concurrency', 3))
        self.recheck_interval = int(self.cfg_logic.get('recheck_interval', 24*3600))
        self.session: aiohttp.ClientSession | None = None
        self._stop = False
        self._workers = []

        self.gutenberg_docs = self.db[self.cfg['gutenberg']['docs_collection']]
        self.gutenberg_queue = self.db[self.cfg['gutenberg']['queue_collection']]
        self.gutenberg_min_id = self.cfg['gutenberg']['min_id']
        self.gutenberg_max_id = self.cfg['gutenberg']['max_id']

        self.allrecipes_docs = self.db[self.cfg['allrecipes']['docs_collection']]
        self.allrecipes_queue = self.db[self.cfg['allrecipes']['queue_collection']]
        self.allrecipes_seed = self.cfg['allrecipes']['seed']
        self.allrecipes_allowed_regex = re.compile(self.cfg['allrecipes'].get('allowed_regex', r'^https?://www\.allrecipes\.com/'))

    async def init(self):
        self.session = aiohttp.ClientSession()
        for coll in [self.gutenberg_docs, self.allrecipes_docs]:
            await coll.create_index('url', unique=True)
            await coll.create_index('content_hash')
        for coll in [self.gutenberg_queue, self.allrecipes_queue]:
            await coll.create_index('url', unique=True)

    async def close(self):
        if self.session:
            await self.session.close()
        self.client.close()

    async def seed_gutenberg_range(self):
        for book_id in range(self.gutenberg_min_id, self.gutenberg_max_id + 1):
            url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
            try:
                await self.gutenberg_queue.update_one(
                    {"url": url},
                    {"$setOnInsert": {"added_ts": int(time.time())}},
                    upsert=True
                )
            except Exception:
                pass

    async def crawl_gutenberg_worker(self, wid: int):
        logger.info(f'Gutenberg Worker {wid} started')
        while not self._stop:
            job = await self.gutenberg_queue.find_one_and_delete({})
            if not job:
                await asyncio.sleep(1)
                continue
            url = job['url']
            try:
                await self.fetch_and_store(url, 'gutenberg', self.gutenberg_docs)
            except Exception as e:
                logger.exception(f'Gutenberg Worker error {url}: {e}')
            await asyncio.sleep(self.delay)
        logger.info(f'Gutenberg Worker {wid} stopped')

    async def seed_allrecipes(self):
        try:
            await self.allrecipes_queue.update_one(
                {"url": self.allrecipes_seed},
                {"$setOnInsert": {"added_ts": int(time.time()), "source": "allrecipes"}},
                upsert=True
            )
        except Exception:
            pass

    async def crawl_allrecipes_worker(self, wid: int):
        logger.info(f'Allrecipes Worker {wid} started')
        while not self._stop:
            job = await self.allrecipes_queue.find_one_and_delete({})
            if not job:
                await asyncio.sleep(1)
                continue
            url = job['url']
            try:
                await self.fetch_and_store(url, 'allrecipes', self.allrecipes_docs, enqueue_links=True, queue_coll=self.allrecipes_queue)
            except Exception as e:
                logger.exception(f'Allrecipes Worker error {url}: {e}')
            await asyncio.sleep(self.delay)
        logger.info(f'Allrecipes Worker {wid} stopped')

    async def fetch_and_store(self, url: str, source: str, docs_coll, recheck=False, enqueue_links=False, queue_coll=None):
        logger.info(f'Fetching {url} (source={source}, recheck={recheck})')
        doc = await docs_coll.find_one({'url': url})
        headers = {'User-Agent': self.cfg_logic.get('user_agent', 'SimpleCrawler/1.0')}
        if doc:
            if 'etag' in doc and doc['etag']:
                headers['If-None-Match'] = doc['etag']
            if 'last_modified' in doc and doc['last_modified']:
                headers['If-Modified-Since'] = doc['last_modified']
        try:
            async with self.session.get(url, headers=headers, timeout=self.cfg_logic.get('timeout', 30)) as resp:
                if resp.status == 304:
                    await docs_coll.update_one({'url': url}, {'$set': {'last_checked': int(time.time())}})
                    return
                if resp.status != 200:
                    logger.warning(f'Status {resp.status} for {url}')
                    return
                body = await resp.read()
                content_hash = sha256_hex(body)
                etag = resp.headers.get('ETag')
                last_mod = resp.headers.get('Last-Modified')
                now_ts = int(time.time())

                changed = True
                if doc:
                    if etag and doc.get('etag') and etag == doc.get('etag'):
                        changed = False
                    elif content_hash == doc.get('content_hash'):
                        changed = False

                if not doc or changed:
                    payload = {
                        'url': url,
                        'raw_html': body.decode('utf-8', errors='ignore'),
                        'source': source,
                        'crawled_ts': now_ts,
                        'etag': etag,
                        'last_modified': last_mod,
                        'content_hash': content_hash,
                        'last_checked': now_ts,
                    }
                    await docs_coll.update_one({'url': url}, {'$set': payload}, upsert=True)
                    logger.info(f'Stored/updated {url} (changed={changed})')
                else:
                    await docs_coll.update_one({'url': url}, {'$set': {'last_checked': now_ts}})
                    logger.info(f'Unchanged {url}, updated last_checked')

                if enqueue_links and queue_coll:
                    await self.enqueue_links_from_body(url, body, queue_coll)
        except asyncio.TimeoutError:
            logger.warning(f'Timeout fetching {url}')
        except aiohttp.ClientError as e:
            logger.warning(f'Client error fetching {url}: {e}')

    async def enqueue_links_from_body(self, base_url: str, body: bytes, queue_coll):
        text = body.decode('utf-8', errors='ignore')
        hrefs = set(re.findall(r'href=[\"\']([^\"\']+)[\"\']', text))
        parsed_base = urlparse(base_url)
        domain = parsed_base.netloc
        added = 0
        for h in hrefs:
            if h.startswith('http'):
                p = urlparse(h)
                if p.netloc != domain:
                    continue
                norm = normalize_url(h)
            elif h.startswith('/'):
                norm = normalize_url(f'{parsed_base.scheme}://{domain}{h}')
            else:
                norm = normalize_url(urljoin(base_url, h))
            if not self.allrecipes_allowed_regex.match(norm):
                continue
            try:
                await queue_coll.update_one({'url': norm}, {'$setOnInsert': {'added_ts': int(time.time()), 'source': 'allrecipes'}}, upsert=True)
                added += 1
            except Exception:
                pass
        logger.info(f'Enqueued {added} links from {base_url}')

    async def recheck_scheduler(self):
        logger.info('Recheck scheduler started')
        while not self._stop:
            cutoff = int(time.time()) - self.recheck_interval
            for docs_coll, queue_coll in [(self.gutenberg_docs, self.gutenberg_queue), (self.allrecipes_docs, self.allrecipes_queue)]:
                cursor = docs_coll.find({'last_checked': {'$lt': cutoff}}, projection=['url'])
                count = 0
                async for d in cursor:
                    try:
                        await queue_coll.update_one({'url': d['url']}, {'$setOnInsert': {'added_ts': int(time.time()), 'recheck': True}}, upsert=True)
                        count += 1
                    except Exception:
                        pass
                logger.info(f'Recheck queued {count} documents for {docs_coll.name}')
            await asyncio.sleep(self.recheck_interval)
        logger.info('Recheck scheduler stopped')

    async def run(self):
        await self.init()
        await self.seed_gutenberg_range()
        await self.seed_allrecipes()

        sem = asyncio.Semaphore(self.max_concurrency)
        for i in range(self.max_concurrency):
            self._workers.append(asyncio.create_task(self._worker_wrapper_gutenberg(i+1, sem)))
            self._workers.append(asyncio.create_task(self._worker_wrapper_allrecipes(i+1, sem)))

        recheck_task = asyncio.create_task(self.recheck_scheduler())

        try:
            while not self._stop:
                await asyncio.sleep(0.5)
        finally:
            self._stop = True
            for w in self._workers:
                w.cancel()
            recheck_task.cancel()
            await self.close()

    async def _worker_wrapper_gutenberg(self, wid: int, sem: asyncio.Semaphore):
        async with sem:
            try:
                await self.crawl_gutenberg_worker(wid)
            except asyncio.CancelledError:
                return

    async def _worker_wrapper_allrecipes(self, wid: int, sem: asyncio.Semaphore):
        async with sem:
            try:
                await self.crawl_allrecipes_worker(wid)
            except asyncio.CancelledError:
                return

def load_config(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def main():
    parser = ArgumentParser()
    parser.add_argument('config', help='path to YAML config')
    args = parser.parse_args()
    cfg = load_config(args.config)

    crawler = MongoCrawler(cfg)

    loop = asyncio.get_event_loop()

    def _on_stop(*_):
        logger.info('Получен сигнал остановки — останавливаемся...')
        crawler._stop = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_stop)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(crawler.run())
    except KeyboardInterrupt:
        logger.info('Interrupted by user')
    finally:
        loop.close()

if __name__ == '__main__':
    main()
