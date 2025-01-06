from distutils.core import setup
from setuptools import setup, find_packages

setup(
    name='Globant’s Data Engineering Coding Challenge',
    version='0.1',
    author='Poul Edson Bardales Rios', 
    author_email='pouledson@gmail.com',
    packages=find_packages(),
    long_description=open('README.md').read()
)