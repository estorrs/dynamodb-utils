from setuptools import setup

setup(name='dynamodb_utils',
      version='0.0.1',
      description='A wrapper for dynamodb',
      author='Erik Storrs',
      author_email='epstorrs@gmail.com',
      packages=['dynamodb_utils'],
      install_requires=[
          'boto3==1.4.7',
          'requests==2.9.1',
          'requests-aws4auth==0.9',
      ],
      zip_safe=False)
