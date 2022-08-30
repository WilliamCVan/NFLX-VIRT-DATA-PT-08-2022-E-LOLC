# Create and Set Up the PythonDataSci Environment

1. Open Terminal on macOS or Anaconda Prompt on Windows and to then run the following command:

    `conda create -n PythonDataSci python=3.8.12 anaconda`

    The installation might take a few minutes.

2. At the command line, run `conda activate PythonDataSci` to activate the environment. When `(PythonDataSci)$` appears, you’re in the conda environment.

3. At the command line, run `python -m ipykernel install --user --name PythonDataSci` to add the `PythonDataSci` to the Jupyter kernel. This will allow you to use the `PythonDataSci` environment in Jupyter Notebook.

