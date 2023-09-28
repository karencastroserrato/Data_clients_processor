# Data_clients_processor

This project consists of analyzing the data processing of the dataframe "Records_DataFrame(1).xlsx" by performing tasks such as:

* Segment the location of each record, indicating whether the location is a city or a country
* Graph the number of records found by city and country
* Assign the correct format for phone numbers, putting the country code before the phone number
* Format the emails, leaving exclusively the email without any additional characters to it
* Handle duplicate records: Duplicate records are eliminated, taking into account that the most recent record should be left and the oldest ones should be deleted. Additionally, if there is missing data in records that are duplicates, these spaces are filled with the existing data from the duplicate records. Regarding the type of industry managed in the dataframe, the information contained in the different cells of the duplicate records are unified in a single cell.
