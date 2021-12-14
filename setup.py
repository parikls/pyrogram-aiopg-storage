import pathlib
import re
from setuptools import setup

here = pathlib.Path(__file__).parent
init = here / "pyrogram_aiopg_storage" / "__init__.py"
readme_path = here / "README.md"

with init.open() as fp:
    try:
        version = re.findall(r"^__version__ = '([^']+)'$", fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine version.')


with readme_path.open() as f:
    README = f.read()

setup(
    name='pyrogram-aiopg-storage',
    version=version,
    description='aiopg storage for pyrogram',
    long_description=README,
    long_description_content_type='text/markdown',
    author='Dmytro Smyk',
    author_email='porovozls@gmail.com',
    url='https://github.com/parikls/pyrogram-aiopg-storage',
    packages=["pyrogram_aiopg_storage", ],
    classifiers=[
        "Operating System :: OS Independent",
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires='>=3.6.0'
)
