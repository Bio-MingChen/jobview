rm -rf dist build *egg-info

python setup.py sdist bdist_wheel

twine upload dist/*

rm -rf build *egg-info