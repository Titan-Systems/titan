from setuptools import find_packages, setup


setup(
    name="titan-core",
    # Package version is managed by the string inside version.md. By default,
    # setuptools doesnt copy this file into the build package. So we direct
    # setuptools to include it using the `include_package_data=True` option
    # as well as the MANIFEST.in file which has the `include version.md` directive.
    version=open("version.md", encoding="utf-8").read().split(" ")[2],
    include_package_data=True,
    description="Titan Core: Snowflake infrastructure as code",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Titan-Systems/titan",
    author="TJ Murphy",
    packages=find_packages(include=["titan", "titan.*"]),
    python_requires=">=3.9",
    project_urls={
        "Homepage": "https://github.com/Titan-Systems/titan",
    },
    entry_points={
        "console_scripts": [
            "titan=titan.cli:titan_cli",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: SQL",
        "Topic :: Database",
    ],
    install_requires=[
        "click>=8.1.7",
        "inflection>=0.5.1",
        "pyparsing>=3.0.9",
        "pyyaml",
        "snowflake-connector-python>=3.7.0",
        "snowflake-snowpark-python>=1.14.0",
        "jinja2",
    ],
    extras_require={
        "dev": [
            "black",
            "build",
            "codespell==2.2.6",
            "pytest-cov",
            "pytest-profiling",
            "pytest-xdist",
            "pytest>=6.0",
            "python-dotenv",
            "ruff",
            "snowflake-cli-labs",
            "tabulate",
            "twine!=5.1.0",
        ]
    },
)
