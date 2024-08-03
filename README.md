# Capstone MAS Crew AI

Welcome to the CapstoneMas Crew project, powered by [crewAI](https://crewai.com).


## Setup Instructions

Follow the steps below to set up your environment and install poetry.

##### 1. Create a Conda Environment

First, create a fresh and empty Conda environment:

```sh
conda create -n poetry-env
```

##### 2.  Activate the newly created environment
```sh
conda activate poetry-env
```

##### 3.  Install poetry
```sh
conda install poetry -y
```

##### 4. Clone this github repository.

Once the environment is set up and poetry is installed, poetry will take care of the dependencies.

##### 5. This will install all dependencies from `pyproject.toml`

```sh
poetry install
```

##### 6. Run the crewai project.

```sh
poetry run weekly_update
```

### License

This project is licensed under the MIT License.