from distutils.core import setup

setup(name='traildb',
      version='0.0.1',
      description='TrailDB stores and queries cookie trails from raw logs.',
      author='AdRoll.com',
      packages=['traildb'],
      package_data={'traildb': ['libtraildb.so']})