import requests
import json
import datetime
from math import ceil
from re import sub
from tqdm import tqdm

# type structures
MASTER_URL = 'https://api.bagelstudio.co/api/public'
GENERIC_PATH = "/collection/{collection_name}/items"
HEADERS_FORMAT = {"Authorization": "Bearer {}", "Accept-Version": "v1"}


class BagelDBWrapper:
    def __init__(self, api_token: str, enable_tqdm: bool=False):
        """
        Initializer for the BagelDB python wrapper.
        :param api_token: for the Authorization: Added as authorization headers Authorization: Bearer <<API_TOKEN>>
        :param enable_tqdm: if true, it will enable console logging of the  when doing 'get collection'
        """
        self.enable_tqdm = enable_tqdm
        self.path = MASTER_URL + GENERIC_PATH
        self.headers = HEADERS_FORMAT
        self.headers['Authorization'] = self.headers['Authorization'].replace('{}', api_token)

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
        pathToFetchFrom = self.path.replace('{collection_name}', collection_name)
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
                    extra_arguments += f"{symbol}query={query[0]}:{query[1]}:{query[2]}"
                else:
                    extra_arguments += f"{symbol}query={query[0]}:{query[1]}"
                symbol = "&"
        if not pagination:
            response = requests.get(pathToFetchFrom + extra_arguments, headers=self.headers)
            return json.loads(response.content)
        else:
            pathToFetchFrom += f"{extra_arguments}{symbol}pageNumber=1&perPage={per_page}"
            response = requests.get(pathToFetchFrom, headers=self.headers)
            items = json.loads(response.content)
            item_count = int(response.headers.get('item-count'))
            number_of_pages = ceil(item_count / per_page)
            for page in tqdm(range(2, number_of_pages + 1), desc="Getting bagel pages", disable=self.enable_tqdm):
                pathToFetchFrom = pathToFetchFrom.replace(f'pageNumber={page - 1}', f'pageNumber={page}')
                page_response = requests.get(pathToFetchFrom, headers=self.headers)
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
        pathToWriteTo = self.path.replace('{collection_name}', collection_name)
        return requests.post(pathToWriteTo, json.dumps(object_dict), headers=self.headers)

    def update_item(self, collection_name: str, item_id: str, dict_to_write: dict):
        """
        updates an item inside collection_name with dict_to_write given item_id

        :param collection_name: 'articles'
        :param item_id: some bagelDB item_id
        :param dict_to_write: {'name':'my new item'}
        :return:
        """
        pathToPutTO = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', '/items/' + item_id)
        return requests.put(pathToPutTO, json.dumps(dict_to_write), headers=self.headers)

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
        pathToPost = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', f'/items/{item_id}?nestedID={nested_collection_name}')
        item_to_post = json.dumps(dict_to_post)
        return requests.post(pathToPost, item_to_post, headers=self.headers)

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
        pathToPutTo = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', f'/items/{item_id}?nestedID={nested_collection_name}.{nested_item_id}')
        return requests.put(pathToPutTo, json.dumps(dict_to_put), headers=self.headers)

    def add_image_to_item(self, collection_name: str, item_id: str, image_slug: str, image_url: str):
        """
        adds an image to an existing item

        :param collection_name: 'articles'
        :param item_id: bageldb item_id
        :param image_slug: 'logo'
        :param image_url: a url containing the image
        :return: requests response
        """
        pathToPost = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', '/items/' + item_id)
        pathToPost += f"/image?imageSlug={image_slug}"
        files = {'imageLink': image_url}
        return requests.put(pathToPost, data=files, headers=self.headers)

    def get_single_item(self, collection_name: str, item_id: str):
        """
        Retrieve a single item

        :param collection_name: 'articles'
        :param item_id: bageldb item_id
        :return: requests response
        """
        pathForItem = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', '/items/' + item_id)
        return requests.get(pathForItem, headers=self.headers)

    def delete_nested_item(self, collection_name: str, item_id: str, nested_collection_name: str, nested_item_id: str):
        """
        deletes a nested item
        :param collection_name: 'articles'
        :param item_id: bageldb item_id
        :param nested_collection_name: 'chapters'
        :param nested_item_id: bageldb item_id
        :return: requests response
        """
        pathForItem = self.path \
            .replace('{collection_name}', collection_name) \
            .replace('/items', f'/items/{item_id}?nestedID={nested_collection_name}.{nested_item_id}')
        return requests.delete(pathForItem, headers=self.headers)
