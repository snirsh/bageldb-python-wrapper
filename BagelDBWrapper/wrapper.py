import asyncio
from typing import Mapping

import aiohttp
import requests
import json
from math import ceil

from aiohttp import ClientSession, ClientError
from tqdm import asyncio as tqdm_aio
from tqdm import tqdm
from urllib.parse import quote_plus

# type structures
MASTER_URL = 'https://api.bagelstudio.co/api/public'
GENERIC_PATH = "/collection/{collection_name}/items"
HEADERS_FORMAT = {"Authorization": "Bearer {}", "Accept-Version": "v1"}


class BagelDBWrapper:
    def __init__(self, api_token: str, enable_tqdm: bool = False):
        """
        Initializer for the BagelDB python wrapper.
        :param api_token: for the Authorization: Added as authorization headers Authorization: Bearer <<API_TOKEN>>
        :param enable_tqdm: if true, it will enable console logging of the  when doing 'get collection'
        """
        self.enable_tqdm = enable_tqdm
        self.path = MASTER_URL + GENERIC_PATH
        self.headers = HEADERS_FORMAT
        self.headers['Authorization'] = self.headers['Authorization'].replace('{}', api_token)

    def get_collection_parallel(self, collection_name: str, per_page: int = 100, project_on: [str] = None,
                                queries: [tuple] = None, extra_params: [str] = None):
        if extra_params is None:
            extra_params = []
        path_to_fetch_from = self.path.replace('{collection_name}', collection_name)
        symbol = '?'
        extra_arguments = ""
        for arg in extra_params:
            extra_arguments += f"{symbol}{arg}"
            symbol = "&"
        if project_on:
            extra_arguments += f"{symbol}projectOn={','.join(project_on)}"
            symbol = "&"
        if queries:
            extra_arguments += f"{symbol}query="
            symbol = "&"
            query_strings = []
            for query in queries:
                if len(query) == 3:
                    query_strings.append(f"{query[0]}:{query[1]}:{quote_plus(str(query[2]))}")
                else:
                    query_strings.append(f"{query[0]}:{quote_plus(str(query[1]))}")
            extra_arguments += quote_plus('+').join(query_strings)
        path_to_fetch_from += f"{extra_arguments}{symbol}perPage={per_page}"
        response = requests.get(f"{path_to_fetch_from}&pageNumber=1", headers=self.headers)
        item_count = int(response.headers.get('item-count'))
        end_page = ceil(item_count / per_page)
        items = asyncio.run(
            self.parallel_fetching(set([f"{path_to_fetch_from}&pageNumber={i}" for i in range(1, end_page + 1)])))
        return [j for jobs in items for j in jobs]

    @staticmethod
    async def _fetch_json(url: str, session: ClientSession) -> tuple:
        data = None
        retries = 10
        while data is None or retries == 0:
            try:
                async with session.get(url) as response:
                    retries -= 1
                    data = await response.json()
            except ClientError:
                await asyncio.sleep(1)
            except asyncio.exceptions.TimeoutError:
                await asyncio.sleep(1)
        return url, data

    async def parallel_fetching(self, urls: set) -> []:
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=10)
        async with ClientSession(connector=connector, headers=self.headers) as session:
            tasks = []
            for url in urls:
                tasks.append(
                    BagelDBWrapper._fetch_json(url=url, session=session)
                )
            if self.enable_tqdm:
                results = await tqdm_aio.tqdm.gather(*tasks)
            else:
                results = await asyncio.gather(*tasks)
        jsons = []
        for result in results:
            jsons.append(result[1])
        return jsons

    def get_collection(self, collection_name: str, pagination: bool = True, per_page: int = 100,
                       project_on: [str] = None, queries: [tuple] = None, extra_params: [str] = None):
        """
        Retrieve multiple items from a collection

        By default, this will return the first 100 items in the collection, in order to get a specific set of items,
        this is why there's a pagination boolean.
        Nested collection fields will not be retrieved unless they are specifically projected on using the projectOn
        feature.

        :param collection_name: as the example in docs.bageldb suggests, i.e "articles".
        :param pagination: True as default, if false you'll only receive the first 100 items
        :param per_page: and Int with number of items per page on pagination, default is 100
        :param project_on: Parameters to project on, as docs.bageldb suggests, "title,name", this should be a list of
        strings, so ["title", "name"]
        :param queries: this parameter is for querying and should be passed as a list of tuples
                      [("author.itemRefID", "=", "5e89a0a573c14625b8850a05,5ed9a0a573c14625ry830v52")]
        :param extra_params: you can create your own parameters and pass them here as a list of strings.
        :return: response dictionary with all the items.
        """
        if extra_params is None:
            extra_params = []
        path_to_fetch_from = self.path.replace('{collection_name}', collection_name)
        symbol = '?'
        extra_arguments = ""
        for arg in extra_params:
            extra_arguments += f"{symbol}{arg}"
            symbol = "&"
        if project_on:
            extra_arguments += f"{symbol}projectOn={','.join(project_on)}"
            symbol = "&"
        if queries:
            for query in queries:
                if len(query) == 3:
                    extra_arguments += f"{symbol}query={query[0]}:{query[1]}:{quote_plus(str(query[2]))}"
                else:
                    extra_arguments += f"{symbol}query={query[0]}:{quote_plus(str(query[1]))}"
                symbol = "&"
        if not pagination:
            response = requests.get(path_to_fetch_from + extra_arguments, headers=self.headers)
            return json.loads(response.content)
        else:
            path_to_fetch_from += f"{extra_arguments}{symbol}pageNumber=1&perPage={per_page}"
            response = requests.get(path_to_fetch_from, headers=self.headers)
            items = json.loads(response.content)
            item_count = int(response.headers.get('item-count'))
            number_of_pages = ceil(item_count / per_page)
            for page in tqdm(range(2, number_of_pages + 1), desc="Getting bagel pages", disable=not self.enable_tqdm):
                path_to_fetch_from = path_to_fetch_from.replace(f'pageNumber={page - 1}', f'pageNumber={page}')
                page_response = requests.get(path_to_fetch_from, headers=self.headers)
                if page_response.status_code == 200:
                    items += json.loads(page_response.content)
                else:
                    print(f"ERROR FETCHING {collection_name})! {page_response.status_code} {page_response.content}")
                    break
            return items

    def create_item(self, collection_name: str, object_dict: dict):
        """
        creating item in the given collectino name, item should be dictionary

        DATE SHOULD BE AN ISO-8601
        :param collection_name: i.e articles
        :param object_dict: {'name':'my new item'}
        :return: requests response
        """
        path_to_write_to = self.path.replace('{collection_name}', collection_name)
        return requests.post(path_to_write_to, json.dumps(object_dict), headers=self.headers)

    def update_item(self, collection_name: str, item_id: str, dict_to_write: Mapping[str, any]):
        """
        updates an item inside collection_name with dict_to_write given item_id

        :param collection_name: 'articles'
        :param item_id: some bagelDB item_id
        :param dict_to_write: {'name':'my new item'}
        :return:
        """
        path_to_put_to = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', '/items/' + item_id)
        return requests.put(path_to_put_to, json.dumps(dict_to_write), headers=self.headers)

    def delete_item(self, collection_name: str, item_id: str):
        """
        delete an item from collection_name with item_id

        :param collection_name: 'articles'
        :param item_id: bagel's  item_id
        :return: requests response
        """
        path_to_delete = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', '/items/' + item_id)
        return requests.delete(path_to_delete, headers=self.headers)

    def write_to_nested_collection(self, collection_name: str, item_id: str, nested_collection_name: str,
                                   dict_to_post: dict):
        """
        Writing to dict_to_post into the nested_collection_name of item_id in collection_name
        :param collection_name: 'articles'
        :param item_id: bagel's item_id
        :param nested_collection_name: 'chapters'
        :param dict_to_post: a dictionary representing the nested item
        :return: requests response
        """
        path_to_post = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', f'/items/{item_id}?nestedID={nested_collection_name}')
        item_to_post = json.dumps(dict_to_post)
        return requests.post(path_to_post, item_to_post, headers=self.headers)

    def update_item_in_nested_collection(self, collection_name: str, item_id: str, nested_collection_name: str,
                                         nested_item_id: str, dict_to_put: dict):
        """
        This updates the nested item with dict_to_put that's in:
        /<<collection_name>>/<<item_id>>/<<nested_collection_name>>/<<nested_item_ref_id>>/
        :param collection_name: 'articles'
        :param item_id: bageldb item_id
        :param nested_collection_name: 'chapters'
        :param nested_item_id: chapter ID, under '_id' of requested item
        :param dict_to_put: a dictionary representing the data you want to put into it
        :return: requests response
        """
        path_to_put_to = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', f'/items/{item_id}?nestedID={nested_collection_name}.{nested_item_id}')
        return requests.put(path_to_put_to, json.dumps(dict_to_put), headers=self.headers)

    def add_image_to_item(self, collection_name: str, item_id: str, image_slug: str, image_url: str):
        """
        adds an image to an existing item

        :param collection_name: 'articles'
        :param item_id: bageldb item_id
        :param image_slug: 'logo'
        :param image_url: a url containing the image
        :return: requests response
        """
        path_to_post = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', '/items/' + item_id)
        path_to_post += f"/image?imageSlug={image_slug}"
        files = {'imageLink': image_url}
        return requests.put(path_to_post, data=files, headers=self.headers)

    def add_local_image_to_item(self, collection_name: str, item_id: str, image_slug: str, image_path: str):
        """
        adds a local image to an existing item

        :param collection_name: 'articles'
        :param item_id: bageldb item_id
        :param image_slug: 'logo'
        :param image_path: a path for the image, i.e. /home/usr/username/Pictures/to_upload.jpg
        :return: requests response
        """
        path_to_post = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', '/items/' + item_id)
        path_to_post += f"/image?imageSlug={image_slug}"
        image_file_descriptor = open(image_path, "rb")
        image_file = image_file_descriptor.read()
        files = {'imageFile': image_file}
        return requests.put(path_to_post, files=files, headers=self.headers)

    def get_single_item(self, collection_name: str, item_id: str):
        """
        Retrieve a single item

        :param collection_name: 'articles'
        :param item_id: bageldb item_id
        :return: requests response
        """
        path_for_item = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', '/items/' + item_id)
        return requests.get(path_for_item, headers=self.headers)

    def delete_nested_item(self, collection_name: str, item_id: str, nested_collection_name: str, nested_item_id: str):
        """
        deletes a nested item
        :param collection_name: 'articles'
        :param item_id: bageldb item_id
        :param nested_collection_name: 'chapters'
        :param nested_item_id: bageldb item_id
        :return: requests response
        """
        path_for_item = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', f'/items/{item_id}?nestedID={nested_collection_name}.{nested_item_id}')
        return requests.delete(path_for_item, headers=self.headers)
