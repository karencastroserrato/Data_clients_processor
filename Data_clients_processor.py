import pandas as pd
import requests
import json
import matplotlib.pyplot as plt 
import geocoder
import re as re
from collections import OrderedDict

"""1. Country Recognition: In the Country column you can find some countries and city names, 
create a function that recognizes if the value in each cell corresponds to a country or a city, 
the values should be relocated in new columns called "Country detected" and "City detected"."""

def load_data(file_name):
    first_df = pd.read_excel(file_name)
    first_df.rename(columns={"Record ID":"Record_ID", "First Name":"First_Name", "Last Name":"Last_Name","Phone Number":"Phone_Number", 
                             "Street Address":"Street_Address", "Created Date":"Created_Date"}, inplace=True,)
    return first_df    

def get_info_countries():
    url = "https://country-list5.p.rapidapi.com/countrylist/"
    headers = {"X-RapidAPI-Key": "cad1f406c7msh796ff172e492b73p1f6e85jsn8a6a3daea35f","X-RapidAPI-Host": "country-list5.p.rapidapi.com"}
    response = requests.get(url, headers=headers)
    country_info_list = (response.json()['country'])
    json_object = json.dumps(country_info_list, indent=4)
    with open("country_info_list.json", "w") as outfile:
        outfile.write(json_object)
    return country_info_list

def get_list_with_country_names():
    countries_list = []
    with open("country_info_list.json", 'r') as openfile:
        json_object = json.load(openfile)
    for i in json_object:
        country_name =i["nicename"]
        countries_list.append(country_name)
    return countries_list

def separete_cities_and_countries(first_df, countries_list):
    first_df["Country_detected"] = ""
    first_df["City_detected"] = ""
    for i, location in zip(first_df.index, first_df["Country"]):
        if location in countries_list:
            first_df["Country_detected"].iloc[i] = location
        else:
            first_df["City_detected"].iloc[i] = location
    return first_df

def country_recognition(file_name):
    print("Runing country recognition")
    uploaded_data = load_data(file_name)
    list_with_country_info = get_info_countries()
    list_with_countries = get_list_with_country_names()
    df_with_country_recognition = separete_cities_and_countries(uploaded_data, list_with_countries)
    return (df_with_country_recognition)


"""2. Generate a graph to display the number of records found by country and city."""

def graph_records_by_country(df):
    print("Runing graph records by country")
    country_mask = df['Country_detected'] != ''
    df2 = df[country_mask]
    country_count = df2['Country_detected'].value_counts().sum()
    city_mask = df['City_detected'] != ''
    df3 = df[city_mask]
    city_count = df3['City_detected'].value_counts().sum()
    plt.style.use("fivethirtyeight") 
    plt.bar( x = ["Countries", "Cities"], height = [country_count , city_count], color = ["red", "orange"])
    plt.title("Number of records found by country and city")
    plt.xlabel("Categories")
    plt.ylabel("Values")
    plt.savefig("Records_by_country_and_city.png", bbox_inches='tight', pad_inches=0.2,)
    

"""3. Fix Phone Numbers: Create a function that allows:
a. According to the detected country, add the "country phone code" to the phone number. 
Please note that any number should begin with zero before applying the country code, e.g., England Phone Numbers.
b. The format of the numbers should be arranged """

def get_phonecode():
    final_phonecode_dict = {}
    with open("country_info_list.json", 'r') as openfile:
        json_object = json.load(openfile)
    for i in json_object:
        phonecode =i["phonecode"]
        country_name =i["nicename"]
        final_phonecode_dict.update({country_name:phonecode})
    return final_phonecode_dict

def get_country_by_city(df):
    unique_countries = df["Country"].dropna().unique()
    city_equal_country = {}
    for i in unique_countries:
        g = geocoder.geonames(i, key='karencastro', featureClass='P')
        city_equal_country.update({i:g.country})
    return city_equal_country

def map_all_cities_with_countries(df, city_equal_country, final_phonecode_dict,):
    df["Phone_Number"] = df["Phone_Number"].replace(to_replace='^0-', value='  ', regex=True)
    df['Final_Country'] = df['Country'].map(city_equal_country)
    df["Phone_Number"] ='(+' + df['Final_Country'].map(final_phonecode_dict) +') '+ df["Phone_Number"].astype(str)
    return df

def fix_phone_number(df_with_country_recognition):
    print("Runing format phone number")
    phonecode_by_country = get_phonecode()
    city_by_country = get_country_by_city(df_with_country_recognition)
    df_with_corrected_phone_number = map_all_cities_with_countries(df_with_country_recognition, city_by_country, phonecode_by_country)
    return (df_with_corrected_phone_number)


"""4. Found Emails: Create a function that evaluates the data within a cell and extracts only the email, 
place the value found in a new column called "Email Found"."""

def find_emails(df):
    print("Runing find emails")
    email_list = []
    for i in df["Email"]:
        separate_email = re.findall(r'[\w\.-]+@[\w\.-]+',str(i)) #    [\w\.-]*?    Start with a mix of letters, digits, -, ., or _
        email = ''.join(separate_email)
        email_list.append(email)
    df["Email_Found"] = email_list
    df_with_found_emails = df 
    return df_with_found_emails 


"""5. Duplicates Management: For this exercise, two or more records will be considered duplicates if they have the same email or the same name.
The code must choose the most recent record and then:
a. If an old record contains data that the new one does not, please add it to the most recent record.
b. Concatenate the values of the Industry property with a semicolon (“ ; ”), without repeating options, 
and also add a semicolon at the beginning of the string."""

def duplicate_management(df):
    print("Runing duplicate management")
    df["Full_Name"] = df["First_Name"] + " " + df["Last_Name"]
    df.update(df.groupby("Email").ffill())
    df.update(df.groupby("Full_Name").ffill())
    df["Industry"] = df.groupby(["Full_Name"])["Industry"].transform(lambda x: ';'.join(x))
    df["Industry"] = df["Industry"].astype(str).str.split(';').apply(lambda x: ';'.join(OrderedDict.fromkeys(x).keys()))
    df["Industry"] =';' + df["Industry"].astype(str)
    df = df.sort_values("Created_Date").drop_duplicates(["Full_Name"], keep='last')
    df = df.sort_values("Created_Date").drop_duplicates(["Email"], keep='last')
    df.sort_index()
    return df


"""6. Integrate all the actions above into a single data pipeline and run the DataFrame as input, then print the output as a .xlsx file."""

def run_pipeline(file_name):
    city_or_country = country_recognition(file_name)
    graph_records_by_country(city_or_country)
    correct_phone_number = fix_phone_number(city_or_country)
    emails_found = find_emails(correct_phone_number)
    final_df = duplicate_management(emails_found)
    return (final_df.to_excel("Data_clients_processor.xlsx", index = False))


file_name = "Records_DataFrame(1).xlsx"
run_pipeline(file_name)