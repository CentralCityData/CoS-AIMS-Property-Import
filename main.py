from load_data import load_data
import pandas as pd
from datetime import datetime


global max_trn

def pad_account_number(elm):

    if len(str(elm)) == 12:
        return f"{elm}"
    return f"0{elm}"


def create_owner_name(row):

    if str(row['owner_first_name']).strip() != '' and str(row['owner_first_name']).strip() != "nan":
        if str(row['owner_initial_name']).strip() != '' and str(row['owner_initial_name']).strip() != "nan":
            return f"{str(row['owner_first_name']).strip()} {str(row['owner_initial_name']).strip()} {str(row['owner_last_name']).strip()}"
        return f"{str(row['owner_first_name']).strip()} {str(row['owner_last_name']).strip()}"
    return f"{str(row['owner_last_name']).strip()}"


def create_property_address_street(row):

    # THIS WAS NEVER IMPLEMENTED?
    
    # If there is no record in the county data,
    # We will have to fall back to the RPS address.
    # This is the case for about 10k records.
    # if str(row["properties.ST_NAME"]).strip() == "nan":
    #     create_property_street_address_rps(row)
    
    

    second_line = f"{row['properties.ST_NUMBER']} {row['properties.ST_NAME']}"
    if not pd.isnull(row['properties.SUFFIX']):
        second_line = (f"{second_line} {row['properties.SUFFIX']}")

    return second_line


def create_mailing_address_county(row):
    
    first_line = create_owner_name(row)
    second_line = create_property_address_street(row)
    third_line = f"Syracuse, NY {row['properties.ZIPCODE']}"

    address = f"{first_line}\n{second_line}\n{third_line}"
    return address


def create_street_address_owner(row):

    line_pts = []
    for val in ['mail_st_nbr', 'prefix_dir', 'mail_st_rt', 'own_mail_st_suff', 'post_dir']:
        if str(row[val]).strip() != "" and str(row[val]).strip() != "nan":
            if val == 'own_mail_st_suff':
                try:
                    if str(row["mail_st_rt"]).split()[-1].strip().upper() == row[val].strip().upper():
                        continue
                except AttributeError:
                    print(row["mail_st_rt"])
            line_pts.append(str(row[val]).strip())
    second_line = " ".join(line_pts)
    return second_line


def create_mailing_address_owner(row):

    first_line = create_owner_name(row)
    second_line = create_street_address_owner(row)

    line_pts = []
    for val in ['mail_city', 'own_mail_state', 'mail_zip', 'own_unit_name', 'own_unit_nbr']:
        if str(row[val]).strip() != "" and str(row[val]).strip() != "nan":
            if val == 'mail_city':
                line_pts.append(f"{str(row[val]).strip()},")
            else:
                line_pts.append(f"{str(row[val]).strip()}")
    third_line = " ".join(line_pts)

    return f"{first_line}\n{second_line}\n{third_line}"


def get_TRN_record(row, mapper):
    """This is kind of critial to document.
    
    TRN was originally just dumbly built off of the index of a file.

    We should at some point build it to be a pointer to some other value,
    but for now. We will want to ensure that the TRN remains MAPPED to the prior
    value it was mapped to when the first ADDRESSS BASED Property Import File was generated.

    WHat does this mean? It means we check to see if the county address record has a match to 
    the original property import (1-23-2024) and if not we build a new TRN using the current
    MAX Trn + 1.

    Slopppy, but does work.
    """

    if row["id"] in mapper.keys():
        return mapper[row["id"]]
    else:
        global max_trn
        max_trn = max_trn + 1
        return max_trn




def create_export_columns(df):
    
    # For AIMS property import 
    df["proprerty_street"] = df.apply(lambda x: create_property_address_street(x), axis=1, result_type="expand")

    # WARNING: THERE WAS NO GOOD PLACE TO PUT THIS.
    df.drop_duplicates(subset="proprerty_street", inplace=True)

    # Make this cleaner, and swap file to new file when finished.
    # -----------------------------------------------------------
    map_df = pd.read_csv("./outputs/aims_property_import_1-23-2024.tsv", sep="\t")
    mapper = dict(zip(map_df["county_record_id"], map_df["aims_account_number"]))
    # -----------------------------------------------------------
    global max_trn
    max_trn = max([int(elm.replace("TRN", "")) for elm in mapper.values()])

    # df["aims_account_number"] = "TRN" + df.index.astype(str)
    df["aims_account_number"] = df.apply(lambda x: get_TRN_record(x, mapper), axis=1)
    df["county_record_id"] = df["id"].astype(str)
    df["old_aims_account_number"] = "TR" + df["parcel_id"].astype(str)
    df["rps_account_number"] = df["account_number"].apply(lambda x: pad_account_number(x))
    df["property_city"] = ["Syracuse" for x in range(df.shape[0])]
    df["property_state"] = ["NY" for x in range(df.shape[0])]
    df["property_zip"] = df["properties.ZIPCODE"]
    df["owner_street"] = df.apply(lambda x: create_street_address_owner(x), axis=1, result_type="expand")
    df["owner_city"] = df["mail_city"].str.strip()
    df["owner_zip"] = df["mail_zip"].str.strip()
    df["owner_state"] = df["own_mail_state"].str.strip()
    df["account_type"] = ["PROP" for x in range(df.shape[0])]
    # For Back of House / sanity checking / mail merge.
    df["property_mailing_address"] = df.apply(lambda x: create_mailing_address_county(x), axis=1, result_type="expand")

    df["owner_mailing_address"] = df.apply(lambda x: create_mailing_address_owner(x), axis=1, result_type="expand")
    df["county_id"] = df["id"]
    
    # df["aims_tr_account_number"] = "TR" + df["parcel_id"].astype(str)

    return df


def main():

    OUTPUT_DIR = "./outputs/"
    
    OUT_COLUMNS_PROPERTY_IMPORT = ['aims_account_number', 'county_record_id', 'parcel_id', 'print_key', 
                                   'rps_account_number', 'owner_last_name', 'owner_first_name',	
                                   'proprerty_street',	'property_city', 'property_state', 'property_zip', 'owner_street', 
                                   'owner_city', 'owner_zip', 'owner_state', 'account_type']

    OUT_COLUMNS_BACK_OF_HOUSE = ['county_id', 'parcel_id', 'aims_account_number', 'print_key', 'account_number',
                                'property_mailing_address', 'owner_mailing_address']


    # Attempts to locate files on disk, if files do
    # not exist -- loads from source systems.
    print("Loading RPS and County Data.")
    df_rps, df_county = load_data()
    print("Data Loaded.")

    # There are, for some reason, duplicate rows created
    # from the Hamer Query.
    df_rps.drop_duplicates(subset="print_key", inplace=True)

    # Name these the same thing so that the merge is successful.
    df_county["print_key"] = df_county["properties.PRINTKEY"]
    df = df_county.merge(df_rps)

    print("Performing Column Tranformations... ... ")
    df = create_export_columns(df)
    
    print("Exporting Data")
    df[OUT_COLUMNS_PROPERTY_IMPORT].to_csv(f"{OUTPUT_DIR}AIMS_property_import_{datetime.now().month}-{datetime.now().day}-{datetime.now().year}.tsv", sep="\t", index=False)
    df[OUT_COLUMNS_BACK_OF_HOUSE].to_excel(f"{OUTPUT_DIR}property_join_mailing_addresses.xlsx", index=False)
    
    print("Data Exported")


if __name__ == "__main__":
    main()