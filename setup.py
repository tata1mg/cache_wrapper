from setuptools import find_packages, setup

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="cache_wrapper",
    version="1.0.0",
    author="1mg",
    author_email="devops@1mg.com",
    url="https://github.com/tata1mg/cache_wrapper",
    description="This module serves as an abstraction layer over popular caching technologies like Redis.",
    packages=find_packages(exclude="requirements"),
    install_requires=requirements,
)
