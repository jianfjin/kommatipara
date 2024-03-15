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

## Structure of the project
1. scripts
    
        reources

        `dataset_one.csv`
        
        the source file of dataset one.

        `dataset_two.csv`
        
        is the source file of dataset two.

        `config.json`
        
        contains the meta data of the source files and is used to create the schema of the source data and further data processing.

    `load_client_data.py` 
    
        runs the loading process. It parses the input arguments, creates the logger, starts the spark session and consumes the configuration from config.json. Then the `load_raw_data` function in src/raw.py is called to read and write the source data to the data lake. After that the `output_data` function is called to transform the source data and export the processed data.    

2. src

    `models.py`

    contains a pydantic class `DatasetConfig` to validate the configuration data loaded from config.json.

    `raw.py`

    contains the functions for the staging process.
    
        `read_from_file` 
        
        reads the source file.
        
        `save_to_file` 
        
        writes the data to the data lake.
        
        `load_raw_data` 
        
        calls the read and save functions sequentially.

    `transform.py`

    contains the functions for the transformation process.

        `read_table`

        reads the data from the data lake.

        `filter table`

        filters the data frame by the condition provided.

        `join table`

        joins the data frames on the designated column.

        `rename columns`

        renames the selected columns.

        `output_data`

        calls the transformation functions sequentially and export the processed data.

    `utilities`
    contains the helping functions.
        `load_config_model`

        loads the configuration data from json and validate the fields and data types.

        `create_schema`

        creates the schema for the source data to be read by spark.

        `handle_errors`

        a error handling decorator for the data processing functions.

3. tests

    `test_raw.py`

    contains the unit tests of the functions in raw.py.

    `test_transform.py`

    contains the unit tests of the functions in transform.py.

    `test_utilities.py`

    contains the unit tests of the functions in utilities.py.


## Run the data loading code
1. install the package

    `sudo apt-get install python3-pip`

    `python3.8 -m pip install -r requirements.txt`

    `python3.8 pip install -e .`

2. cd to the `scripts` folder

3. in the terminal run

    `python3 load_client_data.py ./resources/dataset_one.csv ./resources/dataset_two.csv "United Kingdom,Netherlands"`

4. check the exported data in `client_data` folder in the project folder

5. the logs are created in etl_demo.log in the project folder

6. the loaded data can be queried in the jupyter notebook `query_tables.ipynb`