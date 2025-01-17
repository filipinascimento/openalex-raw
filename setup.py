import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
   requirements = fh.readlines()

setuptools.setup(
    name="openalex-raw",
    version="0.3.2",
    author="Filipi N. Silva",
    author_email="filipi@iu.edu",
    description="Python library to access OpenAlex Snapshot files",
    install_requires=[req for req in requirements if req[:2] != "# "],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/filipinascimento/openalex-raw",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
)