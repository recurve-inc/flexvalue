#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

   Copyright 2021 Recurve Analytics, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""
# This setup.py inspired by https://github.com/kennethreitz/setup.py

# Note: To use the 'upload' functionality of this file, you must:
#   $ pip install twine

import os
import glob
import sys
from shutil import rmtree

from setuptools import find_packages, setup, Command

NAME = "flexvalue"

# TODO: fix psycopg-binary; use correct dependency for distributing
INSTALL_REQUIRES = [
    "click>=8.0.0",
    "toml>=0.10.2",
    "sqlalchemy==2.0",
    "jinja2==3.1.6",
    "psycopg[binary]==3.1.8",
    "google-cloud-bigquery>=2.34.3",
]

here = os.path.abspath(os.path.dirname(__file__))

# Load the package's __version__.py module as a dictionary.
about = {}
with open(os.path.join(here, NAME, "__version__.py")) as f:
    exec(f.read(), about)


class UploadCommand(Command):
    """Support setup.py upload."""

    description = "Build and publish the package."
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print("\033[1m{0}\033[0m".format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status("Removing previous builds…")
            rmtree(os.path.join(here, "dist"))
        except OSError:
            pass

        self.status("Building Source and Wheel (universal) distribution…")
        os.system("{0} setup.py sdist bdist_wheel --universal".format(sys.executable))

        self.status("Uploading the package to PyPi via Twine…")
        os.system("twine upload dist/*")

        sys.exit()

template_files = glob.glob('flexvalue/templates/*sql')
sql_files = glob.glob('flexvalue/sql/*sql')

# Where the magic happens:
setup(
    name=NAME,
    version=about["__version__"],
    description=about["__description__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    url=about["__url__"],
    license=about["__license__"],
    packages=find_packages(exclude=("tests", "db", "notebooks", "docs")),
    entry_points={"console_scripts": ["flexvalue=flexvalue.cli:cli"]},
    install_requires=INSTALL_REQUIRES,
    include_package_data=True,
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    # $ setup.py publish support.
    cmdclass={"upload": UploadCommand},
    data_files=[
        ('templates', template_files),
        ('sql', sql_files)
    ]
)
