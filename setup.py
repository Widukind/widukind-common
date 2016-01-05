# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

from widukind_common import version

setup(
    name='widukind-common',
    version=version.version_str(),
    description='Shared tools for Widukind Projects',
    author='Widukind team',
    url='https://github.com/Widukind/widukind-common',
    license='AGPLv3',
    packages=find_packages(),
    include_package_data=True
)
