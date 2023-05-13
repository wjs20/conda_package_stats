#!usr/bin/env python

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import os
from pathlib import Path
from collections import defaultdict

from concurrent.futures import ThreadPoolExecutor, as_completed

import bs4
import pandas as pd
import requests
from tqdm import tqdm, trange

logging.basicConfig(filename='scraping.log', encoding='utf-8', level=logging.ERROR)

N_PAGES=198
BASE_URL="https://anaconda.org/bioconda"


def get_name_list_from_package_table(num):
    page = requests.get(f"{BASE_URL}/repo?page={num}")
    package_table, = pd.read_html(page.content.decode('utf-8'))
    return package_table['Package Name'].to_list()


def get_package_names(n_workers: int, limit: int | None = None) -> list[str]:
    """Fetch the names of all the packages on the bioconda channel and return them as a list."""
    package_names = []
    with ThreadPoolExecutor(max_workers=n_workers) as exc:
        futures = [exc.submit(get_name_list_from_package_table, page_num) for page_num in range(limit or N_PAGES)]
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                package_names.extend(future.result())
            except Exception as e:
                logging.ERROR(f'failed to retrieve package list page for {num}')
    return package_names


def url_for(package_name: str) -> str:
    """Format the url for a bioconda package from its name and return it as a str."""
    return f'{BASE_URL}/{package_name}'


class PackageInfoPage:
    """Homepage for a bioconda package.

    Args:
        name (str): Name of the package.
        soup (BeautifulSoup): a soup object containing the parsed page html.
    """
    def __init__(self, name: str, soup: bs4.BeautifulSoup) -> None:
        self.name = name
        self.soup = soup

    def get_tag_with(self, title: str) -> bs4.Tag | None:
        """Find the tags with a specific title and return the first one if found."""
        if (tags := self.soup.find_all(title=title)):
            return tags[0]
        else:
            return None

    def parse_times(self, time_since: str) -> dict[str, int] | None:
        """Extract the amount of time since a package has been updated into a dictionary."""
        units = ('years', 'months', 'days')
        times = {}
        for unit in units:
            search_str = r"(\d+) %s" % unit
            match = re.search(search_str, time_since)
            if match:
                times[unit] = int(match.group(1))
            else:
                times[unit] = 0
        return times or None

    @property
    def download_count(self) -> int | None:
        """Return the download count as an integer if present."""
        tag = self.get_tag_with(title='Download Count')
        return int(tag.span.string) if tag else None

    @property
    def homepage(self) -> str | None:
        tag = self.get_tag_with(title='Home Page')
        """Return the link the the code repository if present"""
        return tag.attrs['href'] if tag else None

    @property
    def last_upload(self) -> dict[str, int] | None:
        tag = self.get_tag_with(title='Last upload')
        if tag:
            return self.parse_times(tag.contents[2]) if tag else None

    @classmethod
    def from_name(cls, name: str) -> PackageInfoPage | None:
        url = url_for(name)
        try:
            page = requests.get(url)
            soup = bs4.BeautifulSoup(page.content, features='lxml')
            return cls(name, soup)
        except Exception as e:
            logging.debug(f'{e}: Could not get soup from package page {url}')
            return None


def parse_user_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', required=False, type=int)
    parser.add_argument('--sort_by_downloads', required=False, action='store_true', default=True)
    parser.add_argument('--max_workers', required=False, default=8, type=int)
    return parser.parse_args()


def collect_package_info(package_data, name, info_page):
    package_data[name]['downloads'] = info_page.download_count
    package_data[name]['homepage'] = info_page.homepage
    package_data[name]['last_upload'] = info_page.last_upload


def main():
    args = parse_user_args()
    parent_dir = Path(__file__).resolve().parent


    if os.path.exists('package_names.txt'):
        package_names = open('package_names.txt').read().splitlines()
    else:
        package_names = get_package_names(args.max_workers, args.limit)
        if not package_names:
            print('Could not parse package name list', file=sys.stderr)
            return 1
        else:
            names_path = parent_dir/'package_names.txt' 
            names_path.write_text('\n'.join(package_names))

    try:
        with ThreadPoolExecutor(max_workers=args.max_workers) as exc:
            futures = {exc.submit(PackageInfoPage.from_name, name): name for name in package_names}
            pages = {}
            for future in tqdm(as_completed(futures), total=len(futures)):
                try:
                    name = futures[future]
                    pages[name] = future.result()
                except Exception as e:
                    logging.ERROR(f'failed to retrieve page for {package}')

            package_data = {
                name: {'downloads': page.download_count, 'homepage': page.homepage, 'last_upload': page.last_upload}
                for name, page in pages.items()
            }

            if args.sort_by_downloads:
                package_data = dict(sorted(
                    package_data.items(), 
                    key=lambda package: package[1].get('downloads')
                ))

            print(json.dumps(package_data), file=sys.stdout)

    except Exception as e:
        print(f'Could not get package counts due to {e}', file=sys.stderr)
        return 1

if __name__ == "__main__":
    main()


