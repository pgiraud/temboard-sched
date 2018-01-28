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
    license='BSD',
    description='Task scheduler for temBoard.',
    url='https://github.com/dalibo/temboard-sched',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Operating System :: POSIX :: Linux',
    ],
)

if __name__ == '__main__':
    setup(**dict(
        SETUP_KWARGS,
        packages=find_packages(),
    ))
