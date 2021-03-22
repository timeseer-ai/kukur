"""Package Kukur for publishing."""

import os
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kukur",
    version=os.environ.get('KUKUR_VERSION', '0.0.0'),
    author="Timeseer.AI",
    author_email="pypi@timeseer.ai",
    description="Kukur makes time series data and metadata available to the Apache Arrow ecosystem.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Apache-2.0",
    url="https://kukur.timeseer.ai/",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages=setuptools.find_packages(exclude="tests"),
    install_requires=[
          'pyarrow',
          'python-dateutil',
          'pytz',
          'toml',
      ],
    python_requires='>=3.6',
    package_data = {
        'kukur': ['py.typed'],
    },
)
