from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='neo4j-backup',
    version='0.1.0',
    url='https://github.com/andreshyer/neo4j-backup',
    author='Andres Hyer',
    author_email='andreshyer@gmail.com',
    description='A simple way to backup and restore Neo4j databases without using dump files.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='Apache Software License',
    packages=['neo4j_backup'],
    install_requires=['neo4j>=4.3.0',
                      'tqdm>=4.10.0',
                      ],

    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires=">=3.5",
)
