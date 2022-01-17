# Readme

## Setup instructions

1. To setup, install Docker.
2. Copy source/env.example file and create a new file .env in the same folder.
3. Then run the command -
   ```
   docker-compose up -d --build
   ```
   to do a build for airflow and selenium together.
4. In the browser, open http://localhost:8081/ to see the dags list.

## Run dags:-

- To run two sample parallel dags, replace 'URL' in sample_dag_helpers.py with the page which is needed to be opened and trigger the sample dag from the UI.
- If there are exceptions of browser tabs crashing or issue with tasks not executing properly, check the memory assigned to Docker and check the memory required by the worker when the tasks are running.

## TO enable authentication in airflow

- In airflow.cfg file change value of variable authenticate into True from False
- Add this line below authenticate variable in airflow.cfg file: auth_backend = airflow.contrib.auth.backends.password_auth

## To create new user in airflow[local]

- get into bash shell of webserver
- Execute these below commands in python shell

```python
import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
user = PasswordUser(models.User())
user.username = 'new_user_name'
user.email = 'new_user_email@example.com'
user.password = 'set_the_password'
session = settings.Session()
session.add(user)
session.commit()
session.close()
exit()
```

## flask-bcrypt

- Flask-Bcrypt is a Flask extension that provides bcrypt hashing utilities for your application.
- This package is required for airflow.contrib.auth.backends.password_auth to authenticate the user on airflow to access the DAGs and variables.
