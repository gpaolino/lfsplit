# LFSplit
This is a very simple Python3 project that aims to create a tool for splitting very large text files into small partitioned ones.
This program is based on Dask. It also constitutes a testing ground to improve my skills with the Python language.

## lfsplit-project setup!

### Windows OS
Prerequisites: Windows 10 (or higher) with Python v3.12 properly installed, don't forget to add Python to the PATH variable. <br/>
You can check the Python version installed running this command: <br/>
&emsp; $> py --version <br/>
Depending on how the environment variable is set it could be: "py", "python" or "python3" <br/>

Once you have done that, you can clone this repository in your local environment: <br/>
&emsp; $> git clone https://github.com/gpaolino/lfsplit.git

In order to run this project, you need to create a virtual environment and populate it with the dependencies listed in requirements.txt <br/>
Run these commands to get started quickly: <br/>
&emsp; $> cd .\lfsplit\ <br/>
&emsp; $> py -m venv .venv <br/>
&emsp; $> .\\.venv\Scripts\activate <br/>

Optionally upgrade pip package manager: $> python.exe -m pip install --upgrade pip <br/>

&emsp; $> python -m pip install -r requirements.txt <br/>

### Linux OS
Prerequisites: Ubuntu 22.04.4 LTS (or higher) with Python v3.10.12 properly installed.

Once you have done that, you can clone this repository in your local environment: <br/>
&emsp; $> git clone https://github.com/gpaolino/lfsplit.git

In order to run this project, you need to create a virtual environment and populate it with the dependencies listed in requirements.txt <br/>
Run these commands to get started quickly: <br/>
&emsp; $> cd lfsplit/ <br/>
&emsp; $> sudo apt install python3.10-venv <br/>
&emsp; $> python3 -m venv .venv <br/>
&emsp; $> source .venv/bin/activate <br/>
&emsp; $> python3 -m pip install -r requirements.txt <br/>
&emsp; $> sudo apt-get install python3-tk python3-dev <br/>

Make some small changes to the Python script: <br/>
&emsp; Change the shebang line in the Python script as following: #!.venv/bin/python <br/>
&emsp; Rename the Python script as *.py <br/>
&emsp; Make it executable using the command: $> chmod +x lfsplit.py <br/>

### Install LFSplit as a Local Package
Navigate to the src directory and install your local package: <br/>
&emsp; $> cd .\src\ <br/>
&emsp; $> python -m pip install . <br/>

Run the program: <br/>
&emsp; $> python lfsplit -ifp data/input/ -ifn test_file.txt -s '|' -c col57 -f '%d.%m.%Y' -ofp data/output/ -ofn test_file-*.txt <br/>
