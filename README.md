# Schema Grapher

Schema Grapher is an application that will convert raw data (typically in CSV format) to n-triples RDF.

## How to Install
To install Schema Grapher run the following in a shell within the "schema\_grapher" folder:

    python3 setup.py install

or if you would like to manage with pip (easier uninstall):

    pip3 install ./
	
## How to Run
In order to run Schema Grapher a config file must be created that describes the datasets to be converted and the specification files that map the fields to the schema. Examples are given within the "example_configs" directory.

Once you have a config (in this example "config.json) you can run Schema Grapher by running the following in your shell:

    schema_grapher -i config.json --single
	
If you would like to use multiprocessing (faster than single thread mode) and have the parameter configured in the JSON file:

    schema_grapher -i config.json --multiprocess
