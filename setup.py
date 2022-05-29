import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='bageldb-python-wrapper',
    version='1.4.1',
    author='Snir Sharristh',
    author_email='snir@madeinjlm.org',
    description='A Python wrapper for BagelDB',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/snirsh/bageldb-python-wrapper',
    project_urls={
        "Bug Tracker": "https://github.com/snirsh/bageldb-python-wrapper/issues"
    },
    license='MIT',
    packages=['BagelDBWrapper'],
    install_requires=['requests', 'tqdm', 'aiohttp'],
)
