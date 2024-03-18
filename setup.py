from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()


packages = ['monkeypod']

requires = [
    'requests',
    'click',
    'pyyaml',
    'attrs',
#    'pydash',
    'arrow',
]

setup(
    name='monkeypod-python',
    version="0.0.1",
    description='MonkeyPod Python Client',
    long_description=long_description,
    url='https://github.com/bostrick/monkeypod-python',

    # Author details
    author='Bowe Strickland',
    author_email='bowe@yak.net',

    # Choose your license
    license='GPLv2',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Red Hat Internal',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='monkeypod',
    packages=packages,
    include_package_data=True,
    install_requires=requires,
    entry_points={
        'console_scripts': [
            'monkepod=monkeypod.cli:monkeypod',
        ],
    },
)
