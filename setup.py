from setuptools import find_packages, setup

PYTHON_VERSION = '>=3.7'

REQUIREMENTS = [
    "websocket-client==0.56.0",
    "requests>=2.20,<3",
    "signalr-client-threads==0.0.12",
    "python-dateutil>=2.8,<3",
]

setup(name="hyperquant-framework",
      version="0.1",
      description='HyperQuant Crypto-Trading Framework',
      author='HyperQuant',
      author_email='support@hyperquant.net',
      python_requires=PYTHON_VERSION,
      install_requires=REQUIREMENTS,
      packages=find_packages())
