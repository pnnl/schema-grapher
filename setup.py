from setuptools import setup
from dunamai import Version


setup(
    name='schema_grapher',
    version=Version.from_any_vcs().serialize(),
    packages=['schema_grapher', 'schema_grapher.parser', 'schema_grapher.util'],
    long_description=open('README.md').read(),
    scripts=['scripts/schema_grapher'],
    install_requires=[
        "matplotlib",
        "rdflib",
        "geographiclib==1.50"
    ]
)
