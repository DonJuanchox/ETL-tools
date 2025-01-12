from setuptools import setup, find_packages
import pathlib

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# Text of the README file
README = (HERE / "README.md").read_text()

setup(
    name="etl_tools",  # Your distribution package name; choose something unique if publishing to PyPI
    version="0.1.0",
    description="ETL tools for data pipelines",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/YourUser/ETL-TOOLS",
    author="Your Name",
    author_email="your.email@example.com",
    license="MIT",  # or any other license (Apache 2.0, etc.)
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(where="src"),  # Automatically finds 'etl' in src/
    package_dir={"": "src"},              # Tells setuptools that packages are under "src/"
    python_requires=">=3.7",             # Adjust as needed
    install_requires=[
        # List runtime dependencies here, for example:
        # "pandas>=1.3.0",
        # "requests>=2.25.1",
    ],
    extras_require={
        "dev": [
            "pytest>=6.2",
            # Add other dev/test dependencies if needed
        ],
    },
    include_package_data=True,  # If you have data files to include (specified in MANIFEST.in)
)