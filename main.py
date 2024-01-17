from load_data import load_data
from column_sets import out_cols_prop_import, out_cols_back_of_house

import pandas as pd

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
    df["parcel_id_new"] = "TRC" + df["id"].astype(str)
    df["parcel_id_old"] = "TR" + df["parcel_id"].astype(str)
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

    # Attempts to locate files on disk, if files do
    # not exist -- loads from source systems.
    df_rps, df_county = load_data()

    df_rps.drop_duplicates(subset="print_key", inplace=True)
    df_county["print_key"] = df_county["properties.PRINTKEY"]
    df = df_county.merge(df_rps)
    df = create_export_columns(df)

    df[out_cols_prop_import].to_csv("./outputs/property_join_tr_new.tsv", sep="\t")
    df[out_cols_back_of_house].to_excel("./outputs/property_join_mailing_addresses.xlsx")

    print("Ouput Files Generated to directory: './outputs/'")

if __name__ == "__main__":
    main()