# kommatipara project
## Set the Python 3.8 venv
1. Install python3.8 in ubuntu

    `cd /opt`

    `wget https://www.python.org/ftp/python/3.8.9/Python-3.8.9.tgz`

    `tar xfv Python-3.8.9.tgz`

    `cd /Python-3.8.9`

    `sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev libgdbm-dev libnss3-dev libedit-dev libc6-dev`

2. Switch the python versions
    `./configure --enable-optimizations`

    `sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1`

    `sudo update-alternatives --install /usr/bin/python python /usr/local/bin/python3.8 2`

3. Set the current python version

    `sudo update-alternatives --config python`

    There are 2 choices for the alternative python (providing /usr/bin/python).

    Selection    Path                      Priority   Status

------------------------------------------------------------

* 0            /usr/local/bin/python3.8   2         auto mode

  1            /usr/bin/python3.10        1         manual mode

  2            /usr/local/bin/python3.8   2         manual mode

    Press <enter> to keep the current choice[*], or type selection number:

4. Initialize the project

    cd to the project folder and install pdm 

    `pip3 install pdm`

    `pdm init`

    `source .venv/bin/activate`

5. Build the project

    `pdm build`

## Run the data loading code
    1. install the package `pip3 install -r requirements.txt` and `pip3 install -e .`
    2. cd to the `scripts` folder
    3. in the terminal run 
        `python3 load_client_data.py ./resources/dataset_one.csv ./resources/dataset_two.csv "United Kingdom,Netherlands"`
    4. check the exported data in `client_data` folder in the project folder
    5. the logs are created in etl_demo.log in the project folder
