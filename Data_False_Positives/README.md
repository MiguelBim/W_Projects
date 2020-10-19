# Find False Positives
________
Script to find false positives in BigID results
## Getting Started

### Installing
Using virtualenv
~~~
virtualenv -p /usr/bin/python3.6 env
source activate env
pip install -r requirements.txt
~~~
### Run
Rename [dotenv](dotenv) file to .env and replace example values by real  values

First activate the virtualenv
Run with default configuration
~~~shell
source activate env

python find_false_positives.py --help
python find_false_positives.py
~~~

### Configuration 
[config.yaml](config.yaml) has the configuration of the project some of these values can be also customizable by CLI 