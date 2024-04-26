from setuptools import find_packages, setup

setup(
    name="titan",
    version="0.2.0",
    description="Snowflake infrastructure as code",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/teej/titan",
    author="TJ Murphy",
    packages=find_packages(include=["titan", "titan.*"]),
    python_requires=">=3.9",
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
        "pyparsing==3.0.9",
        "pyyaml",
        "snowflake-connector-python==3.5.0",
        "snowflake-snowpark-python==1.11.1",
        "pygithub==1.55",
    ],
    extras_require={
        "dev": [
            "black",
            "codespell==2.2.6",
            "pytest-cov",
            "pytest-profiling",
            "pytest-xdist",
            "pytest>=6.0",
            "ruff",
            "snowflake-cli-labs",
            "tabulate",
        ]
    },
)
