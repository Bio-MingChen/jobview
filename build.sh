rm -rf dist build *egg-info

python setup.py sdist bdist_wheel

rm -rf build *egg-info