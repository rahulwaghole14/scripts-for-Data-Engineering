# About
Python project that has ELT scripts for Data Engineering
(that used to run in alteryx server in /src folder and now run in EKS pods in /src_docker folder).
There is also `dbt` code for the data models in `src_docker/dbtcdwarehouse` folder.

## setting up
### install extensions if using vscode
there is a pre-commit hook that runs several linters and formatters on the code before it is committed
to make sure that the code is formatted correctly and has no errors we can install the following
vscode extensions also and resolve problems in IDE:

- https://marketplace.visualstudio.com/items?itemName=ms-python.flake8
- https://marketplace.visualstudio.com/items?itemName=ms-python.pylint

### install pyenv
```
pyenv activate {envname}
python3 -m pip install -r requirements.txt
```
## demo pytest (unittest example)
why do unittests? or spend more time to set them up: (Davids opinion:)
 pros:
 - it will improve code quality and reduce the chance of future bugs (reducing future technical debt)
   so that developers can spend more time building new features instead of maintaining existing code
 - it will make developers more confident in their code
 - it will make developers think of how to make their code better
 cons:
 - it will make developers have to spend more time setting it up
 - developers and it leaders need to convince exec and product people to allow them to spend time on it
 - it could potentially make us overconfident in bad code if we write bad tests

we have some python code in src/idm_api__user_profiles_to_cdw/
the code will connect to the idm drupal api and collect user profile data, and then save it to sqlserver
there are unittests we want to run to test the apps internal logic (tests touching on external systems are integration tests)

for the tests we setup new python files with the prefix "test_" so that it is obvious what is the test file to humans.
we can see this in src/idm_api__user_profiles_to_cdw/api_idm/
the test will do something like, create some fake data, and run the function against it to see if it produced the expected output
the test is only as effective as the range of possible scenarios we cover in it, it could be a good test or a bad test.
coverage reports will probably not solve for the above issue, but they are a good starting point

run pytest:
```bash

$ pytest src/idm_api__user_profiles_to_cdw/

```
it will automatically find all files that have tests in them and run them producing a report in the cmdline

## demo: pytest exit codes (for using in ci/cd):
- Exit code 0: All tests were collected and passed successfully.
- Exit code 1: Tests were collected and run but some of the tests failed.
- Exit code 2: Test execution was interrupted by the user.
- Exit code 3: Internal error happened while executing tests.
- Exit code 4: pytest command line usage error.

## demo: produce testing reports with pytest-cov (coverage reports)
we can create a report that shows us exactely the lines that are not being tested.
it will produce html coverage report here: htmlcov/index.html (open in browser)

```bash

 $ pytest --cov=src/idm_api__user_profiles_to_cdw --cov-report html src/idm_api__user_profiles_to_cdw/

```



# existing repos for specific workflows:

# testing (run tests)
## pytest check the test coverage:
```
python3 -m pytest --cov=src/
```
## run pytest
```
python3 -m pytest
```
## generate html coverage report
```
coverage html
```
