from setuptools import setup, find_packages
from toml import load as load_toml
from typing import Tuple


def extract_author_and_email(pyproject_entry: str) -> Tuple[str, str]:
    author, author_email = pyproject_entry.split('<')
    author = author.strip()
    author_email = author_email.replace('>', '')
    return author, author_email


with open('LICENSE', 'r') as f:
    LICENCE = f.read()

with open('README.md', 'r') as f:
    README = f.read()

pyproject_info = load_toml('pyproject.toml')['tool']['poetry']
author, author_email = extract_author_and_email(pyproject_info['authors'][0])

setup(
    name=pyproject_info['name'],
    version=pyproject_info['version'],
    description=pyproject_info['description'],
    long_description=README,
    author=author,
    author_email=author_email,
    url='https://github.com/andrewsonin/gateway',
    license=LICENCE,
    packages=find_packages(exclude=('tests', 'docs'))
)
