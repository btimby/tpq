"""Trivial Postgres Queue"""
import os

from subprocess import check_output, CalledProcessError
from setuptools import find_packages
from setuptools import setup


with open('requirements.txt') as f:
    required = f.read().splitlines()

required = [r for r in required if not r.startswith('git')]

package_name = 'tpq'
version_msg = '# Do not edit. See setup.py.{nl}__version__ = "{ver}"{nl}'
version_py = os.path.join(os.path.dirname(__file__), package_name, 'version.py')

# Get version from GIT or version.py.
try:
    version_git = check_output(['git', 'describe', '--tags']).rstrip()
    version_git = version_git.decode('utf-8')
except CalledProcessError:
    with open(version_py, 'rt') as f:
        version_git = f.read().strip().split('=')[-1].replace('"', '')

# Write out version.py.
with open(version_py, 'wt') as f:
    f.write(version_msg.format(ver=version_git, nl=os.linesep))


setup(
    name=package_name,
    version='{ver}'.format(ver=version_git),
    description='Trivial Postgres Queue',
    author='Ben Timby',
    author_email='btimby@smartfile.com',
    url='https://github.com/btimby/tpq/',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
    scripts=['bin/tpq'],
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Framework :: Django',
    ],
)
