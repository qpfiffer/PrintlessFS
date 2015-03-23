from setuptools import setup, find_packages

setup(name    = 'printlessfs',
      version = '0.1',
      description = 'Simple FUSE bindings to RethinkDB.',
      author='Quinlan Pfiffer',
      author_email='qpfiffer@gmail.com',
      packages=find_packages('src'),
      package_dir = {'':'src'},
      install_requires = [
          'rethinkdb',
          'fusepy',
      ],
      entry_points={
          'console_scripts': [
              'printlessfsd = printlessfs.main:main',
              ]
          },
      )
