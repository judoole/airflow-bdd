import setuptools

setuptools.setup(
    name="airflow-bdd",
    version="0.1.0",    
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
)