from setuptools import setup, find_packages

setup(
    name="dscache",
    version="0.0.1",
    packages=find_packages(),
    url="http://github.com/vendasta/appengine-dscache",
    install_requires=[
        "appengine-python-standard==1.1.5",
        "Flask>=3.0.0",
    ],
    package_data={},
)