#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""
import os
import re
import shutil
from pathlib import Path

from Cython.Build import cythonize
from Cython.Distutils import build_ext
from setuptools import find_packages, setup
from setuptools.extension import Extension

HERE = os.path.dirname(os.path.abspath(__file__))


def get_version():
    filename = os.path.join(HERE, 'pgsync', '__init__.py')
    with open(filename) as f:
        contents = f.read()
    pattern = r"^__version__ = '(.*?)'$"
    return re.search(pattern, contents, re.MULTILINE).group(1)


# Package meta-data.
NAME = 'pgsync'
DESCRIPTION = 'Postgres to elasticsearch sync'
URL = 'https://github.com/toluaina/pg-sync'
AUTHOR = MAINTAINER = 'Tolu Aina'
AUTHOR_EMAIL = MAINTAINER_EMAIL = 'tolu@pgsync.com'
PYTHON_REQUIRES = '>=3.6.0'
VERSION = get_version()
INSTALL_REQUIRES = []
KEYWORDS = [
    'pgsync',
    'elasticsearch',
    'postgres',
    'change data capture',
]
CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Natural Language :: English',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
]
SCRIPTS = [
    'bin/pgsync',
    'bin/bootstrap',
]
SETUP_REQUIRES = ['pytest-runner']
TESTS_REQUIRE = ['pytest']

# if building the source dist then add the sources
PACKAGES = find_packages(
    include=['pgsync']
)
PACKAGES = []

with open('README.rst') as fp:
    README = fp.read()

with open('HISTORY.rst') as fp:
    HISTORY = fp.read()

with open('requirements/prod.txt') as fp:
    INSTALL_REQUIRES = fp.read()

for target_dir in ['dist', 'build', 'PGSync.egg-info']:
    try:
        shutil.rmtree(target_dir)
    except OSError:
        pass


class Builder(build_ext):

    def run(self):

        build_ext.run(self)

        build_dir = Path(self.build_lib)
        root_dir = Path(__file__).parent

        target_dir = build_dir if not self.inplace else root_dir

        self.copy_file(
            Path('pgsync') / '__init__.py', root_dir, target_dir
        )

    def copy_file(self, path, source_dir, destination_dir):
        if not (source_dir / path).exists():
            return
        shutil.copyfile(
            str(source_dir / path),
            str(destination_dir / path),
        )


setup(
    name=NAME,
    author=AUTHOR,
    maintainer=MAINTAINER,
    maintainer_email=MAINTAINER_EMAIL,
    author_email=AUTHOR_EMAIL,
    classifiers=CLASSIFIERS,
    python_requires=PYTHON_REQUIRES,
    description=DESCRIPTION,
    long_description=README + '\n\n' + HISTORY,
    install_requires=INSTALL_REQUIRES,
    include_package_data=True,
    keywords=KEYWORDS,
    packages=PACKAGES,
    setup_requires=SETUP_REQUIRES,
    scripts=SCRIPTS,
    test_suite='tests',
    tests_require=TESTS_REQUIRE,
    url=URL,
    version=VERSION,
    zip_safe=False,
    cmdclass={'build_ext': Builder},
    ext_modules=cythonize(
        [
            Extension(
                'pgsync.*', ['pgsync/*.py']
            )
        ],
        build_dir='build',
        language_level=3,
    ),
    extra_compile_args=['-finline-functions -s'],
    project_urls={
        'Bug Reports': 'https://github.com/toluaina/pg-sync/issues',
        'Funding': 'https://patreon.com/toluaina',
        'Source': URL,
        'Web': 'https://pgsync.com',
    },
)
