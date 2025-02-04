from setuptools import setup, find_packages

# Read requirements from requirements.txt
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='Sales_data_pipeline',
    version='1.0',
    packages=find_packages(),
    include_package_data=True,
    author="Charles",
    install_requires=required,  # Use the list of dependencies from requirements.txt
    entry_points={
        'console_scripts': [
            'batch_upload=src.batch_upload:main',
            'incremental_upload=src.incremental_upload:main'
        ],
    },
)