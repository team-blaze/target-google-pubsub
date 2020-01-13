#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-google-pubsub",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_google_pubsub"],
    install_requires=[
        "singer-python>=5.0.12",
        "google-cloud-pubsub"
    ],
    entry_points="""
    [console_scripts]
    target-google-pubsub=target_google_pubsub:main
    """,
    packages=["target_google_pubsub"],
    package_data = {},
    include_package_data=True,
)
