# bageldb-python-wrapper
A Python Wrapper for BagelDB.
Please refer first to the official docs on [BagelDB docs](https://docs.bageldb.com).

## Installation and updating
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install wrapper as suggested below. 
Rerun the command to check and install updates.
```bash
pip install git+https://github.com/snirsh/bageldb-python-wrapper
```

## Usage
This is a wrapper for the [BagelDB docs](https://docs.bageldb.com).
Most of the features there are implemented fully here.

Features:
*  get_collection --> get collection
*  get_single_item --> get single item in collection
*  create_item --> create a new item
*  update_item --> updates an existing item
*  delete_item --> deletes an existing item
*  write_to_nested_collection --> create item in a nested collection
*  update_item_in_nested_collection --> update an existing item in a nested collection
*  delete_nested_item --> delete an existing nested item
*  add_image_to_item --> add image from existing URL to item
*  add_local_image_to_item --> adds a local image to item

#### Demo of some of the features:
```python
from BagelDBWrapper import BagelDBWrapper

# Don't forget your token!
wrapper = BagelDBWrapper(api_token="<<API_TOKEN>>", enable_tqdm=True)  # enabling progress logging

items = wrapper.get_collection(collection_name='articles', per_page=400, project_on="name,title", queries=[("name","!=","some")])
item_to_add = {"name": "new article"}
# remember that functions return a python-requests response
response = wrapper.create_item('articles', item_to_add)
id_of_created_item = response.json().get('id')
wrapper.delete_item(id_of_created_item)
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)
