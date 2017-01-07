#!/usr/bin/env python
from distutils.core import setup
from setuptools import find_packages

setup(name='handle_geodata',
      version='0.0.1',
      description='Yet Another GeoAggregator',
      author='BST',
      author_email='bst@gmail.com',
      url='https://github.com/BigStructureTeam/YAGA',
      packages=find_packages(exclude=['*.tests']),
      install_requires=['py4j==0.9.0'])