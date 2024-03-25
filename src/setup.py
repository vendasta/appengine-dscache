from setuptools import setup, find_packages

setup(
    name="dscache",
    version="0.0.2",
    packages=find_packages(),
    url="http://github.com/vendasta/appengine-dscache",
    install_requires=[
        "appengine-python-standard==1.1.5"
    ],
    package_data={},
)