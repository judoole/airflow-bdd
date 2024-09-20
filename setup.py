import setuptools

setuptools.setup(
    name="airflow-bdd",
    version="0.2.0",    
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
)