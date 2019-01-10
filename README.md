# SPARCS-Downloader
Utility script to make downloading, formatting, and pruning data downloaded from the New York SPARCS database easy as pie

## Use
Using the script should be fairly straightforward. It uses the abseil app engine, so a few command line parameters are needed. Run the program like so:

download_sparcs.py --output_dir OUTPUT_DIRECTORY --token API_TOKEN

where OUTPUT_DIRECTORY is where the program should save your files and API_TOKEN is your API token given to you by SPARCS. Getting and API token is free and easy. Go to the (official developer site)[https://dev.socrata.com/docs/app-tokens.html] to learn more about registering your application and getting an API token.

By design, you need to set a few parameters within the download_sparcs.py file before it will filter the patient records. This is highly recommended since the files are large (100s of MB to > 1 GB per year). Within the file, add your Clinical Classification Software (CCS) procedure and diagnosis codes to the list (between the brackets with a comma between integers, no quotation marks are needed, hence the map(str... part):

ccs_diag_codes = map(str, [])
ccs_proc_codes = map(str, [])

and add your All Patient Refined Diagnosis Related Groups (APR-DRG) codes to:

apr_drg_codes = map(str, [])

Thats it! Let her rip and enjoy your cleaned and processed files, ready for whatever clinical question you can think of. 


## Processing
This script does several processing steps that I use in my clinical research workflow with SPARCS data (mainly machine learning and outcomes research). In order, this script:

1. Downloads raw SPARCS csv files from the NY SPARCS database
2. Standardizes each years columns (e.g., payment_typology to source_of_payment)
3. Adds a "Medicare" column and codes each patient yes or no
4. Adjusts total_costs and total_charges for inflation by converting into 2009 USD. Note: This uses the consumer price index from the Bureau of Labor and Statistics. I looked up the average value ("value" ) of medical care from 2009 and 2016, took the yearly average value, and divided the 2009 value by this average value to get a ratio multiplier that will convert yearly prices to 2009 prices (i.e., deflate the monetary value of medical costs to 2009 USD). 
5. Convert all numeric columns to pandas numeric values. (This is backend stuff, carry on)
6. Subset the data into "all_patients" and "medicare". "all_patients" is then saved into a csv. I also save a file with only certain columns kept (change columns_to_keep in the python file to adjust this). Its a good idea to confirm that the file saved correctly (this is research, after all) and this feature helps the files open smoothly on excel.
7. IMPORTANT: additional cleaning for medicare only patients. This is a critical step that is handled by the additional_cleaning function in the file. You can add different cleaning steps depending on the question that you're answering. For example, in one of my papers, I wanted to only keep patients of traditional medicare age (i.e. >= 65 years old) for a paper, so I only kept patients in the 50-69 and 70+ age groups (thus exlcuding complicated patients with ESRD, etc). I left some examples commented out in this function to help provide some inspiration. 
8. IMPORTANT: Outlier removal. removeOutliers removes the bottom 0.5th percentile and the upper 99.5th percentile from the total_costs_inflation_adjusted column to help provide bounds for my machine learning algorithms. Check out the function and either comment everything out or uncomment length of stay (LOS) to fit your needs. 
