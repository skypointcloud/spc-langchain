import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="spc-langchain",
    version="0.0.48",
    author="Arun raja",
    author_email="arun.raja@skypointcloud.com",
    description="SkyPoint fork of the LangChain library.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/skypointcloud/spc-langchain.git",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "libs/langchain/src"},
    package_data={"": ["*.txt"]},
    packages=setuptools.find_packages(where="libs/langchain/src"),
    python_requires=">=3.8",
    install_requires=[
        "tenacity==8.2.3",
        "langsmith==0.0.27",
        "numexpr==2.8.5",
    ],
)
