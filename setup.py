from setuptools import setup

import tkv

setup(
	name='tkv',
	version=tkv.__version__,    
	description='table-key-value adapter to sqlite and other db engines',
	url='https://github.com/mobarski/tkv',
	author='Maciej Obarski',
	author_email='mobarski@gmail.com',
	license='MIT',
	py_modules=['tkv','tkv_redis'],
	platforms='any',
)
