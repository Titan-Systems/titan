from setuptools import find_packages, setup

setup(
    name="Titan",
    version="0.0.1",
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
    ],
)
