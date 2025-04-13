import setuptools

setuptools.setup(
    name="airflow-bdd",
    version="0.3.0",    
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
)