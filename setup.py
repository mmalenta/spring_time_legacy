from spring_time import __version__ 
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="spring_time",
    version=__version__,
    author="Mateusz Malenta",
    author_email="mateusz.malenta@gmail.com",
    description="MeerTRAP supervisor for the Spring post-processing pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "spring_time"},
    packages=["supervisor"],
    scripts=["bin/spring_supervisor.py"]
)
