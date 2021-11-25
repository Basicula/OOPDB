import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="oopdb",
    version="0.0.1",
    author="Basicula",
    author_email="maksim.basum@gmail.com",
    description="OOP abstraction/wrapper for work with SQL data bases",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Basicula/OOPDB",
    project_urls={
        "Bug Tracker": "https://github.com/Basicula/OOPDB/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "oopdb"},
    packages=setuptools.find_packages(where="oopdb"),
    install_requires=["sqlite3"],
)