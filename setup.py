from setuptools import find_packages, setup

setup(
    name="titan",
    version="0.0.15",
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
        "click==8.1.7",
        "inflection==0.5.1",
        "pydantic>=2.0",
        "pyparsing==3.0.9",
        "pyyaml",
        "snowflake-connector-python",
        "snowflake-snowpark-python",
        "pygithub==1.55",
    ],
    extras_require={
        "dev": [
            "black",
            "tabulate",
            "pytest>=6.0",
            "ruff",
            "snowflake-cli-labs",
            "pytest-xdist",
        ]
    },
)
