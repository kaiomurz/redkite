# redkite
Kaio Motawara - Redkite data engineer assignment

- Instead of doing a code review, I've taken the liberty to refactor and restructure the project to reflect the approach I would have taken. The changes from and bug fixes in the provided script effectively are the review.
- To run the pipeline, please create the environment specified in the `environment.yml'` file and run `main.py`, which orchestrates the pipeline. 
- There are folders for schemas and processing functions. I've used separate `.py` files to store the three schemas. I've used a similar structure for processing functions. While it might be overkill for this problem, it's meant to represent a real-world situation where I might have to deal with a large number of complex schemas and processing functions. 
- While I have created the prescribed schemas for the parquet files, when I try to load the files with those schemas, I get an error that I haven't been able to resolve. So I've gone ahead and loaded it without enforcing the schema just so that I could continue with the rest of the assignment.
- the final step of the pipeline provided includes .coalesce() and writes to a databricks .csv. Since the number of partitions is already 1 and since I don't have access to databricks, I would have replaced the step with a simple save to .csv. 
However, when I tried to write the  dataframe to csv, Spark threw a Java error that I couldn't resolve. Therefore, in order to complete the assignment, I converted the Spark dataframe to a Pandas dataframe and then saved it as a tab-separated file as required.
- I've created one unittest in the tests folder just to demonstrate how tests might be written. To run the test, please run `python -m unittest discover tests` from the root directory of the project (ie the same directory that contains `main.py`).
