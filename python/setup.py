from setuptools import setup, find_packages

setup(
    name='event-stream-engine',
    version='1.0.0',
    description='Python event streaming engine with producer, consumer, and analytics',
    author='Kishan Borad',
    packages=find_packages(),
    install_requires=['websockets>=12.0', 'aiohttp>=3.9.0'],
    entry_points={
        'console_scripts': [
            'es-produce=event_producer:main',
            'es-consume=event_consumer:main',
            'es-analyze=stream_analyzer:main',
            'es-loadtest=load_generator:main',
        ],
    },
    python_requires='>=3.9',
)
