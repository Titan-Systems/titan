from setuptools import find_packages, setup

setup(
    name="titan",
    version="0.0.3",
    description="The easy way to automate data warehouse infrastructure",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/teej/titan",
    author="TJ Murphy",
    packages=find_packages(include=["titan", "titan.*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: SQL",
        "Topic :: Database",
        "Framework :: Titan",
    ],
    install_requires=[
        "click",
        "inflection",
        "pydantic",
        "pyparsing",
        "pyyaml",
        "snowflake-connector-python",
        "snowflake-snowpark-python",
    ],
    extras_require={
        "dev": [
            "tabulate",
            "pytest",
        ]
    },
)
