from setuptools import setup, find_packages

VERSION = "0.0.1"
DESCRIPTION = "Compute bid-ask spread from the Bitso websocket interface"
LONG_DESCRIPTION = "Compute bid-ask spread from the Bitso websocket interface"

# Setting up
setup(
    # the name must match the folder name 'verysimplemodule'
    name="bidaskspread",
    version=VERSION,
    author="André Gomes Lamas Otero",
    author_email="lamas3000@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=["websocket-client"],
    # needs to be installed along with your package. Eg: 'caer'
    keywords=["python", "bitso", "bid ask spread"],
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
)
