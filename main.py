from load_data import load_data
from column_sets import out_cols_prop_import, out_cols_back_of_house

import pandas as pd
from datetime import datetime

def create_link_to_old_property_join(new):

    old = pd.read_csv("./inputs/first_prop_import.tsv", sep='\t')
    new.drop(["parcel_id"], axis=1, inplace=True)
    new["parcel_id"] = new["old_aims_account_number"]
    df = old.merge(new, on="parcel_id", how="left")
    df = df.groupby(["parcel_id"]).aggregate("first")
    df.to_csv("./outputs/old_account_update.tsv", sep="\t")
    return


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

    # If there is no record in the county data,
    # We will have to fall back to the RPS address.
    # This is the case for about 10k records.
    if str(row["properties.ST_NAME"]).strip() == "nan":
        create_property_street_address_rps(row)

    second_line = f"{row['properties.ST_NUMBER']} {row['properties.ST_NAME']}"
    if not pd.isnull(row['properties.SUFFIX']):
        second_line = (f"{second_line} {row['properties.SUFFIX']}")

    return second_line


def create_property_street_address_rps(row):

    cols = ['loc_prefix_dir', 'loc_st_nbr',
            'loc_st_name', 'loc_mail_st_suff', 'loc_post_dir', 'loc_unit_name',
            'loc_unit_nbr', 'loc_muni_name', 'loc_zip', 'loc_zip4']

    print(row['loc_st_name'])
    print(row['loc_st_nbr'])


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
                if str(row["mail_st_rt"]).split()[-1].strip().upper() == row[val].strip().upper():
                    continue
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


def create_export_columns(df):
    
    # For AIMS property import 
    df["proprerty_street"] = df.apply(lambda x: create_property_address_street(x), axis=1, result_type="expand")

    # WARNING: THERE WAS NO GOOD PLACE TO PUT THIS.
    df.drop_duplicates(subset="proprerty_street", inplace=True)

    df["aims_account_number"] = "TRN" + df.index.astype(str)
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
    df["aims_tr_account_number"] = "TR" + df["parcel_id"].astype(str)
    df["owner_mailing_address"] = df.apply(lambda x: create_mailing_address_owner(x), axis=1, result_type="expand")
    df["county_id"] = df["id"]

    return df


def main():

    OUTPUT_DIR = "./outputs/"

    # Attempts to locate files on disk, if files do
    # not exist -- loads from source systems.
    df_rps, df_county = load_data()

    # There are, for some reason, duplicate rows created
    # from the Hamer Query.
    df_rps.drop_duplicates(subset="print_key", inplace=True)

    # Name these the same thing so that the merge is successful.
    df_county["print_key"] = df_county["properties.PRINTKEY"]
    df = df_county.merge(df_rps)

    df = create_export_columns(df)

    df[out_cols_prop_import].to_csv(f"{OUTPUT_DIR}aims_property_import_{datetime.now().month}-{datetime.now().day}-{datetime.now().year}.tsv", sep="\t", index=False)
    df[out_cols_back_of_house].to_excel(f"{OUTPUT_DIR}property_join_mailing_addresses.xlsx", index=False)

    create_link_to_old_property_join(df)

    print(f"Ouput Files Generated to directory: '{OUTPUT_DIR}'")

if __name__ == "__main__":
    main()