"""
src/common/setup.py
"""

from setuptools import setup, find_packages

setup(
    name="common",  # Choose a unique name to avoid conflicts
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        # List any dependencies your package needs
        # e.g., 'requests', 'numpy'
    ],
    description="Common utilities and validators for the project.",
    author="David Powell",
    author_email="david.powell@hexa.co.nz",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  # Choose your license
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",  # Specify the Python versions you support
)
