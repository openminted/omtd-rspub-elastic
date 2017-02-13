from setuptools import setup

setup(
    name='omtd-rspub-elastic',
    version=0.1,
    packages=['omtdrspub.elastic'],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    author='Giorgio Basile',
    author_email='giorgio.basile@open.ac.uk',
    description='ResourceSync documents generation library based on Elasticsearch, provided by the OpenMinTeD project',
    long_description=open('README.md').read(),
    install_requires=[
        "pyyaml",
        "elasticsearch>=1.0.0,<2.0.0"
    ],
    dependency_links=["https://github.com/EHRI/rspub-core/tarball/master#egg=rspub-core",
                      "https://github.com/EHRI/resync/tarball/ehribranch#egg=resyncehri"],
    test_suite="omtdrspub.elastic.test",
)