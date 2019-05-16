import setuptools

with open("README.rst", "r") as f:
    long_description = f.read()

setuptools.setup(
   name="queuepool",
   version="1.1.0",
   author="ikh software, inc.",
   author_email="ikh@ikhsoftware.com",
   description="A multithread-safe resource pool based on synchronized queue",
   long_description=long_description,
   long_description_content_type="text/x-rst",
   url="https://bitbucket.org/ikh/queuepool",
   packages=setuptools.find_packages(),
   classifiers=[
      'Programming Language :: Python :: 3',
      'License :: OSI Approved :: BSD License',
      'Operating System :: OS Independent',
   ],
   python_requires = '>= 3.7',
   install_requires=[
      'psycopg2 >= 2.8.2',
   ],
   project_urls={
        'Bug Reports': 'https://bitbucket.org/ikh/queuepool/issues',
   },
)

