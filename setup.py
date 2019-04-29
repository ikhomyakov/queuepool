import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="queuepool",
    version="0.1.0",
    author="ikh software, inc.",
    author_email="ikh@ikhsoftware.com",
    description="A simple resource pool based on synchronized queue",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://bitbucket.org/ikh/pool",
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
    ],
)

