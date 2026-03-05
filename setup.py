from setuptools import setup, find_packages

setup(
    name="db_dict",
    version="0.32",
    description="Easy to use user-friendly sqlite db interface",
    packages=find_packages(),
    requires=[
        "aiosqlite"
    ]
)