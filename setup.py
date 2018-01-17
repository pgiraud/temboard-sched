from setuptools import setup, find_packages
import os

# Load version number
setup_path = os.path.dirname(os.path.realpath(__file__))
exec(open(os.path.join(setup_path,'temboardsched','version.py')).read())

SETUP_KWARGS = dict(
    name='temboard-sched',
    version=__version__,
    author='Julien Tachoires',
    author_email='julmon@gmail.com',
    license='PostgreSQL',
    description='Task scheduler for temboard.',
)

if __name__ == '__main__':
    setup(**dict(
        SETUP_KWARGS,
        packages=find_packages(),
    ))
