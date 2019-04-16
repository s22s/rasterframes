# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from os import path, environ
from io import open
from pathlib import Path
import distutils.cmd
import importlib

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

#pyspark_ver = 'pyspark>=2.2.0,<2.3'
pyspark_ver = 'pyspark==2.3.2'

class RunExamples(distutils.cmd.Command):
    """A custom command to run pyrasterframes examples."""

    description = 'run pyrasterframes examples'
    user_options = [
        # The format is (long option, short option, description).
        ('examples=', 'e', 'examples to run'),
    ]

    def _extract_module(self, mod):
        module = importlib.import_module(mod)

        if hasattr(module, '__all__'):
            globals().update({n: getattr(module, n) for n in module.__all__})
        else:
            globals().update({k: v for (k, v) in module.__dict__.items() if not k.startswith('_')})

    def initialize_options(self):
        """Set default values for options."""
        # Each user option must be listed here with their default value.
        self.examples = filter(lambda x: not x.name.startswith('_'),
                               list(Path('./examples').resolve().glob('*.py')))

    def _check_ex_path(self, ex):
        file = Path(ex)
        if not file.suffix:
            file = file.with_suffix('.py')
        file = (Path('./examples') / file).resolve()

        assert file.is_file(), ('Invalid example %s' % file)
        return file

    def finalize_options(self):
        """Post-process options."""
        import re
        if isinstance(self.examples, str):
            self.examples = re.split('\W+', self.examples)
        self.examples = map(lambda x: 'examples.' + x.stem,
                            map(self._check_ex_path, self.examples))

    def run(self):
        """Run the examples."""
        import traceback
        for ex in self.examples:
            print(('-' * 50) + '\nRunning %s' % ex + '\n' + ('-' * 50))
            try:
                self._extract_module(ex)
            except Exception:
                print(('-' * 50) + '\n%s Failed:' % ex + '\n' + ('-' * 50))
                print(traceback.format_exc())



setup(
    name='pyrasterframes',
    description='RasterFrames for PySpark',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    version=environ.get('RASTERFRAMES_VERSION', 'NONE'),
    author='Astraea, Inc.',
    author_email='info@astraea.earth',
    license='Apache 2',
    url='https://rasterframes.io',
    project_urls={
        'Bug Reports': 'https://github.com/locationtech/rasterframes/issues',
        'Source': 'https://github.com/locationtech/rasterframes',
    },
    setup_requires=[
        'pytest-runner',
        pyspark_ver,
        'setuptools >= 0.8'
    ],
    install_requires=[
        'pytz',
        'shapely',
        'pip >= 1.4'
        # pyspark_ver,
        # 'pathlib'
    ],
    tests_require=[
        pyspark_ver,
        'pytest==3.4.2',
        'pypandoc',
        'numpy>=1.7',
        'pandas',
    ],
    packages=[
        'pyrasterframes',
        'geomesa_pyspark'
    ],
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries'
    ],
    zip_safe=False,
    test_suite="pytest-runner",
    cmdclass={
        'examples': RunExamples
    }
    # entry_points={
    #     "console_scripts": ['pyrasterframes=pyrasterframes:console']
    # }
)
