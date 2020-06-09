# project-implicit
Data and analysis from the Harvard Implicit Bias study


## How to run

1. Open your terminal and enter the following:

```
docker run --rm -p 8888:8888 -e JUPYTER_ENABLE_LAB=yes -v "$PWD":/home/jovyan/work jupyter/scipy-notebook
```

2. Copy and paste the URL posted in the terminal output. This will
create a Jupyter Lab interface. 

Open the terminal within Jupyter Lab and enter the following code
to connect your Docker account.

```
git config --global user.email "you@example.com"
git config --global user.name "Your Name"
```

And then use the following code to clone and initialize the project.

```
git clone https://github.com/gordonsilvera/project-implicit.git
pip install -U -r /home/jovyan/project-implicit/requirements.txt
```

